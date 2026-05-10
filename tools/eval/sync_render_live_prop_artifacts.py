from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.eval.sync_render_live_lens_history import _env_first, _infer_render_base_url  # noqa: E402


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")))
            handle.write("\n")


def _daterange(start: dt.date, end: dt.date) -> List[dt.date]:
    days: List[dt.date] = []
    current = start
    while current <= end:
        days.append(current)
        current += dt.timedelta(days=1)
    return days


def _fetch_payload(
    *,
    base_url: str,
    token: str,
    date_str: str,
    timeout_seconds: int,
    include_observation_log: bool,
    include_registry_log: bool,
) -> Dict[str, Any]:
    query = urllib.parse.urlencode(
        {
            "date": str(date_str),
            "includeObservationLog": "on" if include_observation_log else "off",
            "includeRegistryLog": "on" if include_registry_log else "off",
        }
    )
    url = f"{str(base_url).rstrip('/')}/api/cron/live-prop-artifacts?{query}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=max(1, int(timeout_seconds))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("live-prop artifacts response was not a JSON object")
    return payload


def sync_render_live_prop_artifacts(
    *,
    base_url: str,
    cron_token: str,
    start_date: dt.date,
    end_date: dt.date,
    timeout_seconds: int = 45,
    out_dir: Path,
    overwrite: bool = False,
    include_observation_log: bool = True,
    include_registry_log: bool = False,
) -> Dict[str, Any]:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    results: List[Dict[str, Any]] = []
    prop_registry_dir = out_dir / "prop_registry"
    recap_dir = out_dir / "recaps"
    prop_registry_dir.mkdir(parents=True, exist_ok=True)
    recap_dir.mkdir(parents=True, exist_ok=True)

    for day in _daterange(start_date, end_date):
        date_str = day.isoformat()
        slug = date_str.replace("-", "_")
        registry_path = prop_registry_dir / f"live_prop_registry_{slug}.json"
        observation_path = prop_registry_dir / f"live_prop_observations_{slug}.jsonl"
        registry_log_path = prop_registry_dir / f"live_prop_registry_{slug}.jsonl"
        recap_path = recap_dir / f"live_lens_daily_recap_{slug}.json"
        if registry_path.exists() and not bool(overwrite):
            try:
                registry_doc = json.loads(registry_path.read_text(encoding="utf-8"))
            except Exception:
                registry_doc = {}
            entry_count = len((registry_doc.get("entries") or {})) if isinstance(registry_doc, dict) else 0
            results.append(
                {
                    "date": date_str,
                    "status": "skipped",
                    "registryPath": str(registry_path.relative_to(_ROOT)).replace("\\", "/"),
                    "entryCount": int(entry_count),
                }
            )
            continue
        try:
            payload = _fetch_payload(
                base_url=base_url,
                token=cron_token,
                date_str=date_str,
                timeout_seconds=int(timeout_seconds),
                include_observation_log=bool(include_observation_log),
                include_registry_log=bool(include_registry_log),
            )
            registry_doc = payload.get("registry") if isinstance(payload.get("registry"), dict) else {}
            observation_rows = payload.get("observationLog") if isinstance(payload.get("observationLog"), list) else []
            registry_log_rows = payload.get("registryLog") if isinstance(payload.get("registryLog"), list) else []
            daily_recap = payload.get("dailyRecap") if isinstance(payload.get("dailyRecap"), dict) else {}
            _write_json(registry_path, registry_doc)
            if include_observation_log:
                _write_jsonl(observation_path, (row for row in observation_rows if isinstance(row, dict)))
            if include_registry_log:
                _write_jsonl(registry_log_path, (row for row in registry_log_rows if isinstance(row, dict)))
            if daily_recap:
                _write_json(recap_path, daily_recap)
            entry_count = len((registry_doc.get("entries") or {})) if isinstance(registry_doc, dict) else 0
            results.append(
                {
                    "date": date_str,
                    "status": "ok",
                    "registryPath": str(registry_path.relative_to(_ROOT)).replace("\\", "/"),
                    "observationPath": str(observation_path.relative_to(_ROOT)).replace("\\", "/") if include_observation_log else None,
                    "entryCount": int(entry_count),
                    "observationRowCount": len(observation_rows),
                    "archiveCount": len(payload.get("firstObservationArchive") or []),
                }
            )
        except urllib.error.HTTPError as exc:
            results.append({"date": date_str, "status": "http_error", "code": int(exc.code), "reason": str(exc.reason)})
        except Exception as exc:
            results.append({"date": date_str, "status": "error", "error": f"{type(exc).__name__}: {exc}"})

    return {
        "ok": True,
        "baseUrl": base_url,
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "results": results,
        "okCount": sum(1 for row in results if str(row.get("status")) == "ok"),
        "skippedCount": sum(1 for row in results if str(row.get("status")) == "skipped"),
        "errorCount": sum(1 for row in results if str(row.get("status")) not in {"ok", "skipped"}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync raw Render live prop registry and observation artifacts into a local mirror.")
    parser.add_argument("--base-url", default="", help="Render base URL; defaults to env or render.yaml service host")
    parser.add_argument("--cron-token", default="", help="Cron bearer token; defaults to MLB_BETTING_CRON_TOKEN/MLB_CRON_TOKEN/CRON_TOKEN env")
    parser.add_argument("--start-date", required=True, help="Start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date inclusive (YYYY-MM-DD)")
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--out-dir", default="data/live_lens/render_truth", help="Output directory for synced prop artifacts")
    parser.add_argument("--overwrite", choices=("on", "off"), default="off")
    parser.add_argument("--include-observation-log", choices=("on", "off"), default="on")
    parser.add_argument("--include-registry-log", choices=("on", "off"), default="off")
    args = parser.parse_args()

    start_date = dt.date.fromisoformat(str(args.start_date))
    end_date = dt.date.fromisoformat(str(args.end_date))
    if end_date < start_date:
        raise SystemExit("end-date must be on or after start-date")

    base_url = str(args.base_url or "").strip() or _infer_render_base_url()
    cron_token = str(args.cron_token or "").strip() or _env_first("MLB_BETTING_CRON_TOKEN", "MLB_CRON_TOKEN", "CRON_TOKEN")
    if not base_url:
        raise SystemExit("Missing base URL. Pass --base-url or set MLB_BETTING_BASE_URL/RENDER_URL.")
    if not cron_token:
        raise SystemExit("Missing cron token. Pass --cron-token or set MLB_BETTING_CRON_TOKEN/MLB_CRON_TOKEN/CRON_TOKEN.")

    out_dir = Path(str(args.out_dir))
    if not out_dir.is_absolute():
        out_dir = (_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = sync_render_live_prop_artifacts(
        base_url=base_url,
        cron_token=cron_token,
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=int(args.timeout_seconds),
        out_dir=out_dir,
        overwrite=str(args.overwrite) == "on",
        include_observation_log=str(args.include_observation_log) == "on",
        include_registry_log=str(args.include_registry_log) == "on",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())