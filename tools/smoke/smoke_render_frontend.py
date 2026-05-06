from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _env_first(*names: str) -> str:
    for name in names:
        value = str(os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def _infer_render_base_url() -> str:
    explicit = _env_first(
        "MLB_BETTING_BASE_URL",
        "BASE_URL",
        "RENDER_URL",
        "RENDER_EXTERNAL_URL",
    )
    if explicit:
        return explicit
    render_yaml_path = (_ROOT / "render.yaml").resolve()
    try:
        text = render_yaml_path.read_text(encoding="utf-8")
    except Exception:
        return ""
    for raw_line in text.splitlines():
        line = str(raw_line or "").strip()
        if not line.startswith("name:"):
            continue
        service_name = str(line.split(":", 1)[1] or "").strip()
        if service_name:
            return f"https://{service_name}.onrender.com"
    return ""


def _normalize_base_url(value: str) -> str:
    base_url = str(value or "").strip()
    if not base_url:
        return ""
    if "://" not in base_url:
        return f"https://{base_url}"
    return base_url.rstrip("/")


def _fetch_json(base_url: str, path: str, *, timeout_seconds: int) -> Tuple[Dict[str, str], Dict[str, Any]]:
    url = f"{base_url}{path}"
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            response = requests.get(
                url,
                timeout=max(1, int(timeout_seconds)),
                headers={"User-Agent": "mlb-betting-v2-render-smoke/1.0"},
            )
            response.raise_for_status()
            raw_headers = dict(response.headers.items())
            body = response.json()
            break
        except requests.RequestException as exc:
            last_error = exc
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = status_code in {502, 503, 504} or status_code is None
            if attempt >= 2 or not retryable:
                raise
            time.sleep(1.0)
    else:
        raise RuntimeError(f"{path} request failed") from last_error
    if not isinstance(body, dict):
        raise RuntimeError(f"{path} response was not a JSON object")
    headers = {str(key).lower(): str(value) for key, value in raw_headers.items()}
    return headers, body


def _header(headers: Dict[str, str], name: str) -> str:
    return str(headers.get(str(name).lower()) or "").strip()


def _body_app(payload: Dict[str, Any]) -> Dict[str, Any]:
    app = payload.get("app")
    return app if isinstance(app, dict) else {}


def _collect_card_badge_stats(cards_payload: Dict[str, Any]) -> List[str]:
    cards = cards_payload.get("cards")
    if not isinstance(cards, list):
        return []
    stats: List[str] = []
    for card in cards:
        if not isinstance(card, dict):
            continue
        probable = card.get("probable")
        if not isinstance(probable, dict):
            continue
        for side_key in ("away", "home"):
            side = probable.get(side_key)
            if not isinstance(side, dict):
                continue
            badges = side.get("ladderBadges")
            if not isinstance(badges, list):
                continue
            for badge in badges:
                if not isinstance(badge, dict):
                    continue
                stat = str(badge.get("stat") or "").strip()
                if stat:
                    stats.append(stat)
    return sorted(set(stats))


def _iter_live_prop_rows(live_payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    games = live_payload.get("games")
    if not isinstance(games, list):
        return []
    rows: List[Dict[str, Any]] = []
    for game in games:
        if not isinstance(game, dict):
            continue
        for key in ("liveProps", "props", "trackedProps"):
            bucket = game.get(key)
            if not isinstance(bucket, list):
                continue
            for row in bucket:
                if isinstance(row, dict):
                    rows.append(row)
    return rows


def _live_prop_labels(live_payload: Dict[str, Any]) -> List[str]:
    labels: List[str] = []
    for row in _iter_live_prop_rows(live_payload):
        label = str(row.get("marketLabel") or row.get("prop") or "").strip()
        if label:
            labels.append(label)
    return sorted(set(labels))


def _comma_join(values: Iterable[str]) -> str:
    items = [str(value).strip() for value in values if str(value).strip()]
    return ", ".join(items)


def _check_endpoint(
    name: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    *,
    expected_commit: str,
    require_no_store: bool,
) -> Tuple[List[str], Dict[str, Any]]:
    failures: List[str] = []
    app = _body_app(payload)
    body_commit = str(app.get("commit") or "").strip()
    header_commit = _header(headers, "X-App-Commit")
    body_instance = str(app.get("instanceId") or "").strip()
    header_instance = _header(headers, "X-App-Instance")
    cache_control = _header(headers, "Cache-Control")
    runtime = app.get("runtime") if isinstance(app.get("runtime"), dict) else {}
    snapshot = {
        "commit": body_commit,
        "header_commit": header_commit,
        "instance_id": body_instance,
        "header_instance": header_instance,
        "commit_source": str(app.get("commitSource") or "").strip(),
        "branch_source": str(app.get("branchSource") or "").strip(),
        "cache_control": cache_control,
        "booted_at": str(runtime.get("bootedAt") or "").strip(),
    }

    if not body_commit:
        failures.append(f"{name}: missing body app.commit")
    if not header_commit:
        failures.append(f"{name}: missing X-App-Commit header")
    if body_commit and header_commit and body_commit != header_commit:
        failures.append(f"{name}: body/header commit mismatch ({body_commit} != {header_commit})")
    if expected_commit and body_commit and body_commit != expected_commit:
        failures.append(f"{name}: commit {body_commit} did not match expected {expected_commit}")
    if not body_instance:
        failures.append(f"{name}: missing body app.instanceId")
    if not header_instance:
        failures.append(f"{name}: missing X-App-Instance header")
    if body_instance and header_instance and body_instance != header_instance:
        failures.append(f"{name}: body/header instance mismatch")
    if require_no_store:
        expected_tokens = ("no-store", "no-cache", "must-revalidate")
        for token in expected_tokens:
            if token not in cache_control.lower():
                failures.append(f"{name}: Cache-Control missing {token}")
        if _header(headers, "Pragma").lower() != "no-cache":
            failures.append(f"{name}: Pragma header was not no-cache")
        if _header(headers, "Expires") != "0":
            failures.append(f"{name}: Expires header was not 0")
    return failures, snapshot


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke-check the Render frontend APIs for metadata consistency and key betting-card/live-lens surfaces.")
    ap.add_argument("--base-url", default="", help="Override Render base URL (defaults from env vars or render.yaml).")
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    ap.add_argument("--season", type=int, default=datetime.now().year)
    ap.add_argument("--cron-token", default="", help="Optional cron token for /api/cron/config.")
    ap.add_argument("--timeout-seconds", type=int, default=30)
    ap.add_argument("--expected-commit", default="", help="Fail if any checked endpoint reports a different app.commit.")
    ap.add_argument(
        "--require-card-badges",
        default="hits_allowed,walks_allowed",
        help="Comma-separated card badge stats that must be present in /api/cards. Use empty string to disable.",
    )
    args = ap.parse_args()

    base_url = _normalize_base_url(str(args.base_url or "").strip() or _infer_render_base_url())
    if not base_url:
        print("Missing Render base URL. Set --base-url or one of MLB_BETTING_BASE_URL/BASE_URL/RENDER_URL/RENDER_EXTERNAL_URL.")
        return 2

    cron_token = str(args.cron_token or "").strip() or _env_first("MLB_BETTING_CRON_TOKEN", "MLB_CRON_TOKEN", "CRON_TOKEN")
    expected_commit = str(args.expected_commit or "").strip()
    required_card_badges = [
        token.strip()
        for token in str(args.require_card_badges or "").split(",")
        if token.strip()
    ]

    endpoint_specs: List[Tuple[str, str]] = [
        ("cards", f"/api/cards?date={urllib.parse.quote(str(args.date))}"),
        ("live_lens", f"/api/live-lens?date={urllib.parse.quote(str(args.date))}"),
        ("season", f"/api/season/{int(args.season)}"),
        ("season_live_lens", f"/api/season/{int(args.season)}/live-lens?date={urllib.parse.quote(str(args.date))}"),
    ]
    if cron_token:
        endpoint_specs.append(("cron_config", f"/api/cron/config?token={urllib.parse.quote(cron_token)}"))

    failures: List[str] = []
    endpoint_report: Dict[str, Any] = {}
    bodies: Dict[str, Dict[str, Any]] = {}
    instance_ids: List[str] = []
    commits: List[str] = []

    for name, path in endpoint_specs:
        try:
            headers, payload = _fetch_json(base_url, path, timeout_seconds=int(args.timeout_seconds))
        except Exception as exc:
            failures.append(f"{name}: request failed: {type(exc).__name__}: {exc}")
            continue
        bodies[name] = payload
        endpoint_failures, snapshot = _check_endpoint(
            name,
            headers,
            payload,
            expected_commit=expected_commit,
            require_no_store=True,
        )
        failures.extend(endpoint_failures)
        endpoint_report[name] = snapshot
        if snapshot.get("commit"):
            commits.append(str(snapshot["commit"]))
        if snapshot.get("instance_id"):
            instance_ids.append(str(snapshot["instance_id"]))

    unique_commits = sorted(set(commits))
    unique_instances = sorted(set(instance_ids))
    if len(unique_commits) > 1:
        failures.append(f"endpoint commit mismatch: {_comma_join(unique_commits)}")
    if len(unique_instances) > 1:
        failures.append(f"endpoint instance mismatch: {_comma_join(unique_instances)}")

    cards_payload = bodies.get("cards") or {}
    badge_stats = _collect_card_badge_stats(cards_payload) if cards_payload else []
    if cards_payload:
        missing_card_badges = [stat for stat in required_card_badges if stat not in badge_stats]
        if missing_card_badges:
            failures.append(f"cards: missing required badge stats: {_comma_join(missing_card_badges)}")

    live_payload = bodies.get("live_lens") or {}
    live_labels = _live_prop_labels(live_payload) if live_payload else []

    report = {
        "baseUrl": base_url,
        "date": str(args.date),
        "season": int(args.season),
        "expectedCommit": expected_commit or None,
        "endpoints": endpoint_report,
        "cards": {
            "badgeStats": badge_stats,
            "requiredBadgeStats": required_card_badges,
        },
        "liveLens": {
            "marketLabels": live_labels,
            "hasHitsAllowed": "Hits Allowed" in live_labels,
            "hasWalksAllowed": "Walks Allowed" in live_labels,
        },
        "failures": failures,
    }

    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())