from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.tune.fit_live_prop_ranking import (  # noqa: E402
    _iter_first_observation_rows,
    _iter_render_sync_report_fallback_rows,
    _load_first_observations,
    _read_json,
)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except Exception:
        return None
    return number if number == number else None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _slug_to_date(token: str) -> str:
    parts = str(token or "").split("_")
    if len(parts) == 3:
        return "-".join(parts)
    return str(token or "")


def _summarize(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    settled = [row for row in rows if row.get("odds") is not None]
    n = len(settled)
    wins = sum(int(row.get("label") or 0) for row in settled)
    profit = 0.0
    for row in settled:
        odds = int(row.get("odds") or 0)
        if int(row.get("label") or 0) == 1:
            profit += (float(odds) / 100.0) if odds > 0 else (100.0 / abs(float(odds)))
        else:
            profit -= 1.0
    return {
        "n": n,
        "win_rate": round(wins / n, 4) if n else None,
        "roi": round(profit / n, 4) if n else None,
    }


def _reason_flags(texts: Iterable[str]) -> List[str]:
    haystack = " ".join(str(text or "").strip().lower() for text in texts if str(text or "").strip())
    if not haystack:
        return []
    checks = {
        "starter_matchup_live": ["original matchup is still the live path", "still lined up against the starter"],
        "pitch_count_runway": ["leash near", "working at only", "room for more workload"],
        "hook_risk": ["hook risk", "third trip through the order", "third time through", "shorter leash", "burn through the leash"],
        "k_pace": ["k pace", "strikeouts through", "bat-missing", "swing-and-miss"],
        "pitch_mix": ["pitch mix", "secondary mix", "four-seam", "curveball", "slider", "changeup", "cutter", "sinker"],
        "contact_quality": ["contact quality", "hard contact"],
        "damage_quality": ["damage quality", "extra-base ceiling", "power risk"],
        "strikeout_pressure": ["strikeout pressure"],
        "remaining_volume": ["remaining volume", "projected for about", "plate appearances", "lineup spot"],
        "current_actual_context": ["current actual sits", "already has", "still at 0", "has only reached"],
        "game_script": ["trailing by", "club is trailing", "protects regular late-game at-bats", "score is still tight"],
    }
    out = [flag for flag, needles in checks.items() if any(needle in haystack for needle in needles)]
    return sorted(out)


def _progress_bucket(row: Dict[str, Any]) -> str:
    progress = _safe_float(row.get("progress_fraction"))
    if progress is None:
        return "unknown"
    if progress < 0.33:
        return "early_third"
    if progress < 0.55:
        return "mid_game"
    return "later"


def _rank_bucket(row: Dict[str, Any]) -> str:
    rank = int(row.get("rank") or 999)
    if rank <= 5:
        return "top5"
    if rank <= 10:
        return "top10"
    return "after10"


def _stability_bucket(row: Dict[str, Any]) -> str:
    seen_count = int(_safe_int(row.get("seen_count")) or 0)
    if seen_count <= 1:
        return "one_tick"
    if seen_count <= 3:
        return "two_to_three_ticks"
    if seen_count <= 6:
        return "four_to_six_ticks"
    return "seven_plus_ticks"


def _odds_move_bucket(row: Dict[str, Any]) -> str:
    first_odds = _safe_int(row.get("first_odds"))
    last_odds = _safe_int(row.get("last_odds"))
    if first_odds is None or last_odds is None:
        return "unknown"
    if last_odds == first_odds:
        return "unchanged"
    selection = str(row.get("selection") or "").strip().lower()
    first_prob = _safe_float(row.get("first_implied_prob")) or 0.0
    last_prob = _safe_float(row.get("last_implied_prob")) or 0.0
    if selection in {"over", "under"}:
        return "price_better" if last_prob < first_prob else "price_worse"
    return "changed"


def _american_implied_prob(odds: Any) -> float | None:
    value = _safe_int(odds)
    if value is None or value == 0:
        return None
    if value > 0:
        return 100.0 / (float(value) + 100.0)
    return abs(float(value)) / (abs(float(value)) + 100.0)


def _load_report_index(live_lens_dir: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for report_path in sorted(live_lens_dir.glob("live_lens_report_*.json")):
        token = report_path.stem.replace("live_lens_report_", "")
        date_str = _slug_to_date(token)
        try:
            payload = _read_json(report_path)
        except Exception:
            continue
        if isinstance(payload, dict):
            out[date_str] = {
                "source": "report",
                "path": str(report_path.relative_to(_ROOT)).replace("\\", "/"),
                "counts": dict(payload.get("counts") or {}),
                "performance": dict(payload.get("performance") or {}),
            }
    recap_dir = live_lens_dir / "recaps"
    if recap_dir.exists():
        for recap_path in sorted(recap_dir.glob("live_lens_daily_recap_*.json")):
            token = recap_path.stem.replace("live_lens_daily_recap_", "")
            date_str = _slug_to_date(token)
            if date_str in out:
                continue
            try:
                payload = _read_json(recap_path)
            except Exception:
                continue
            if isinstance(payload, dict):
                raw_artifacts = dict((payload.get("rawArtifacts") or {}))
                summary = dict((payload.get("summary") or {}))
                out[date_str] = {
                    "source": "recap",
                    "path": str(recap_path.relative_to(_ROOT)).replace("\\", "/"),
                    "counts": dict(summary.get("counts") or {}),
                    "performance": dict(summary.get("performance") or {}),
                    "rawArtifacts": raw_artifacts,
                    "registrySummary": dict(payload.get("registrySummary") or {}),
                }
    render_sync_dir = live_lens_dir / "render_sync"
    if render_sync_dir.exists():
        for sync_path in sorted(render_sync_dir.glob("live_lens_reports_*.json")):
            token = sync_path.stem.replace("live_lens_reports_", "")
            date_str = _slug_to_date(token)
            if date_str in out:
                continue
            try:
                payload = _read_json(sync_path)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            latest_report = payload.get("latestReport") if isinstance(payload.get("latestReport"), dict) else {}
            registry_summary = payload.get("registrySummary") if isinstance(payload.get("registrySummary"), dict) else {}
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            counts = latest_report.get("counts") if isinstance(latest_report.get("counts"), dict) else {}
            performance = latest_report.get("performance") if isinstance(latest_report.get("performance"), dict) else {}
            raw_artifacts = payload.get("rawArtifacts") if isinstance(payload.get("rawArtifacts"), dict) else {}
            if not counts and isinstance(summary.get("counts"), dict):
                counts = dict(summary.get("counts") or {})
            if not performance and isinstance(summary.get("performance"), dict):
                performance = dict(summary.get("performance") or {})
            out[date_str] = {
                "source": "render_sync",
                "path": str(sync_path.relative_to(_ROOT)).replace("\\", "/"),
                "counts": dict(counts or {}),
                "performance": dict(performance or {}),
                "rawArtifacts": dict(raw_artifacts or {}),
                "registrySummary": dict(registry_summary or {}),
            }
    return out


def _load_rich_registry_rows(live_lens_dir: Path) -> Dict[str, Dict[str, Any]]:
    registry_dir = live_lens_dir / "prop_registry"
    report_index = _load_report_index(live_lens_dir)
    out: Dict[str, Dict[str, Any]] = {}
    def _merge_row(date_str: str, key: str, entry: Dict[str, Any], observation: Dict[str, Any]) -> None:
        first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
        last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
        observed_snapshot = observation.get("snapshot") if isinstance(observation.get("snapshot"), dict) else {}
        game_state = observation.get("gameState") if isinstance(observation.get("gameState"), dict) else {}
        day_report = report_index.get(date_str) or {}
        first_odds = _safe_int(first_snapshot.get("odds"))
        last_odds = _safe_int(last_snapshot.get("odds"))
        first_live_edge = _safe_float(first_snapshot.get("liveEdge"))
        last_live_edge = _safe_float(last_snapshot.get("liveEdge"))
        first_reasons = [str(reason).strip() for reason in (first_snapshot.get("reasons") or []) if str(reason).strip()]
        last_reasons = [str(reason).strip() for reason in (last_snapshot.get("reasons") or []) if str(reason).strip()]
        first_summary = str(first_snapshot.get("reasonSummary") or "").strip()
        last_summary = str(last_snapshot.get("reasonSummary") or "").strip()
        row = {
            "key": str(key),
            "date": date_str,
            "seen_count": _safe_int(entry.get("seenCount")),
            "first_seen_at": entry.get("firstSeenAt"),
            "last_seen_at": entry.get("lastSeenAt"),
            "first_odds": first_odds,
            "last_odds": last_odds,
            "first_implied_prob": _american_implied_prob(first_odds),
            "last_implied_prob": _american_implied_prob(last_odds),
            "first_live_edge": first_live_edge,
            "last_live_edge": last_live_edge,
            "live_edge_delta": (None if first_live_edge is None or last_live_edge is None else round(float(last_live_edge) - float(first_live_edge), 4)),
            "first_live_projection": _safe_float(first_snapshot.get("liveProjection")),
            "last_live_projection": _safe_float(last_snapshot.get("liveProjection")),
            "first_actual_so_far": _safe_float(first_snapshot.get("actualSoFar") if first_snapshot.get("actualSoFar") is not None else first_snapshot.get("actual")),
            "last_actual_so_far": _safe_float(last_snapshot.get("actualSoFar") if last_snapshot.get("actualSoFar") is not None else last_snapshot.get("actual")),
            "first_summary": first_summary,
            "last_summary": last_summary,
            "first_reasons": first_reasons,
            "last_reasons": last_reasons,
            "first_reason_flags": _reason_flags([first_summary, *first_reasons]),
            "last_reason_flags": _reason_flags([last_summary, *last_reasons]),
            "game_state_live_text": game_state.get("liveText"),
            "report_source": day_report.get("source"),
            "report_path": day_report.get("path"),
            "report_counts": dict(day_report.get("counts") or {}),
            "report_performance": dict(day_report.get("performance") or {}),
            "report_raw_artifacts": dict(day_report.get("rawArtifacts") or {}),
            "registry_summary": dict(day_report.get("registrySummary") or {}),
            "observation_changed_fields": list(observation.get("changedFields") or []),
            "observation_snapshot_changed": bool(observation.get("snapshotChanged")),
            "observed_snapshot": observed_snapshot,
        }
        out[str(key)] = row

    if registry_dir.exists():
        for registry_path in sorted(registry_dir.glob("live_prop_registry_*.json")):
            token = registry_path.stem.replace("live_prop_registry_", "")
            date_str = _slug_to_date(token)
            try:
                doc = _read_json(registry_path)
            except Exception:
                continue
            if not isinstance(doc, dict):
                continue
            observations = _load_first_observations(registry_dir / f"live_prop_observations_{token}.jsonl")
            entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
            if not isinstance(entries, dict):
                continue
            for key, entry in entries.items():
                if not isinstance(entry, dict):
                    continue
                observation = observations.get(str(key)) if isinstance(observations.get(str(key)), dict) else {}
                _merge_row(date_str, str(key), entry, observation)

    render_sync_dir = live_lens_dir / "render_sync"
    if render_sync_dir.exists():
        for sync_path in sorted(render_sync_dir.glob("live_lens_reports_*.json")):
            try:
                payload = _read_json(sync_path)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            archive_rows = payload.get("firstObservationArchive") if isinstance(payload.get("firstObservationArchive"), list) else []
            if not archive_rows:
                continue
            date_str = str(payload.get("date") or _slug_to_date(sync_path.stem.replace("live_lens_reports_", ""))).strip()
            for archive_row in archive_rows:
                if not isinstance(archive_row, dict):
                    continue
                key = str(archive_row.get("key") or "").strip()
                if not key or key in out:
                    continue
                entry = {
                    "seenCount": archive_row.get("seenCount"),
                    "firstSeenAt": archive_row.get("firstSeenAt"),
                    "lastSeenAt": archive_row.get("lastSeenAt"),
                    "firstSeenSnapshot": archive_row.get("firstSeenSnapshot"),
                    "lastSeenSnapshot": archive_row.get("lastSeenSnapshot"),
                }
                observation = {
                    "snapshot": archive_row.get("snapshot"),
                    "gameState": archive_row.get("gameState"),
                    "changedFields": archive_row.get("changedFields"),
                    "snapshotChanged": archive_row.get("snapshotChanged"),
                }
                _merge_row(date_str, key, entry, observation)
    return out


def _bucket_summary(rows: List[Dict[str, Any]], values: Iterable[str], key_fn) -> Dict[str, Any]:
    return {value: _summarize([row for row in rows if key_fn(row) == value]) for value in values}


def _build_summary_payload(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {
            "overall": {"n": 0, "win_rate": None, "roi": None},
            "dates": [],
            "counts": {"rows": 0, "markets": {}, "props": {}, "reportDates": 0},
            "by_progress": {},
            "by_rank": {},
            "by_selection": {},
            "by_market": {},
            "by_prop": {},
            "by_stability": {},
            "by_odds_move": {},
            "by_first_reason_flag": {},
            "by_last_reason_flag": {},
            "rich_archive": {"coverage": {}, "day_reports": {}},
            "examples": {"most_stable": [], "largest_first_edges": [], "largest_edge_growth": []},
        }

    markets = sorted({str(row.get("market") or "") for row in rows})
    props = sorted({str(row.get("prop") or "") for row in rows})
    selections = sorted({str(row.get("selection") or "") for row in rows})
    dates = sorted({str(row.get("date") or "") for row in rows})
    first_reason_flags = sorted({flag for row in rows for flag in (row.get("first_reason_flags") or [])})
    last_reason_flags = sorted({flag for row in rows for flag in (row.get("last_reason_flags") or [])})

    report_day_rows = [row for row in rows if isinstance(row.get("report_performance"), dict) and row.get("report_performance")]
    report_dates = sorted({str(row.get("date") or "") for row in report_day_rows})
    day_reports: Dict[str, Any] = {}
    for date_str in report_dates:
        sample = next((row for row in report_day_rows if str(row.get("date") or "") == date_str), None)
        if not sample:
            continue
        raw_artifacts = dict(sample.get("report_raw_artifacts") or {})
        day_reports[date_str] = {
            "source": sample.get("report_source"),
            "path": sample.get("report_path"),
            "counts": dict(sample.get("report_counts") or {}),
            "performance": dict(sample.get("report_performance") or {}),
            "raw_total_bytes": _safe_int(raw_artifacts.get("total_bytes")),
            "raw_total_bytes_text": raw_artifacts.get("total_bytes_text"),
        }

    return {
        "overall": _summarize(rows),
        "dates": dates,
        "counts": {
            "rows": len(rows),
            "markets": {market: sum(1 for row in rows if str(row.get("market") or "") == market) for market in markets},
            "props": {prop: sum(1 for row in rows if str(row.get("prop") or "") == prop) for prop in props},
            "reportDates": len(day_reports),
        },
        "by_progress": _bucket_summary(rows, ("early_third", "mid_game", "later", "unknown"), _progress_bucket),
        "by_rank": _bucket_summary(rows, ("top5", "top10", "after10"), _rank_bucket),
        "by_selection": {selection: _summarize([row for row in rows if str(row.get("selection") or "") == selection]) for selection in selections},
        "by_market": {market: _summarize([row for row in rows if str(row.get("market") or "") == market]) for market in markets},
        "by_prop": {prop: _summarize([row for row in rows if str(row.get("prop") or "") == prop]) for prop in props},
        "by_stability": _bucket_summary(rows, ("one_tick", "two_to_three_ticks", "four_to_six_ticks", "seven_plus_ticks"), _stability_bucket),
        "by_odds_move": _bucket_summary(rows, ("price_better", "price_worse", "unchanged", "unknown"), _odds_move_bucket),
        "by_first_reason_flag": {flag: _summarize([row for row in rows if flag in (row.get("first_reason_flags") or [])]) for flag in first_reason_flags},
        "by_last_reason_flag": {flag: _summarize([row for row in rows if flag in (row.get("last_reason_flags") or [])]) for flag in last_reason_flags},
        "rich_archive": {
            "coverage": {
                "rows_with_seen_count": sum(1 for row in rows if row.get("seen_count") is not None),
                "rows_with_reason_flags": sum(1 for row in rows if row.get("first_reason_flags") or row.get("last_reason_flags")),
                "rows_with_report_metadata": sum(1 for row in rows if row.get("report_source")),
                "rows_with_odds_drift": sum(1 for row in rows if row.get("first_odds") is not None and row.get("last_odds") is not None),
            },
            "day_reports": day_reports,
        },
        "examples": {
            "most_stable": sorted(rows, key=lambda row: (-int(_safe_int(row.get("seen_count")) or 0), -abs(float(_safe_float(row.get("last_live_edge")) or 0.0))))[:10],
            "largest_first_edges": sorted(rows, key=lambda row: -abs(float(_safe_float(row.get("first_live_edge")) or 0.0)))[:10],
            "largest_edge_growth": sorted(rows, key=lambda row: -abs(float(_safe_float(row.get("live_edge_delta")) or 0.0)))[:10],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze first-surface live-lens timing, reason, and stability splits from the rich archive")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Path to the live_lens directory")
    parser.add_argument("--out", default="", help="Optional JSON output path")
    args = parser.parse_args()

    live_lens_dir = Path(args.live_lens_dir)
    if not live_lens_dir.is_absolute():
        live_lens_dir = (_ROOT / live_lens_dir).resolve()

    rows = list(_iter_first_observation_rows(live_lens_dir))
    if not rows:
        raise SystemExit("No final-settled first-observation live-lens rows found")

    rich_by_key = _load_rich_registry_rows(live_lens_dir)
    enriched_rows: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item.update(rich_by_key.get(str(row.get("key") or ""), {}))
        enriched_rows.append(item)

    supplemental_rows = list(_iter_render_sync_report_fallback_rows(live_lens_dir))
    supplemental_enriched: List[Dict[str, Any]] = []
    for row in supplemental_rows:
        item = dict(row)
        item.update(rich_by_key.get(str(row.get("key") or ""), {}))
        supplemental_enriched.append(item)

    out: Dict[str, Any] = _build_summary_payload(enriched_rows)
    out["supplemental_report_fallback"] = {
        "counts": {
            "rows": len(supplemental_enriched),
            "dates": sorted({str(row.get("date") or "") for row in supplemental_enriched}),
        },
        "summary": _build_summary_payload(supplemental_enriched),
    }

    text = json.dumps(out, indent=2)
    if str(args.out).strip():
        out_path = Path(str(args.out))
        if not out_path.is_absolute():
            out_path = (_ROOT / out_path).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")
        print(f"Wrote report: {out_path}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
