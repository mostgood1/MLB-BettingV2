from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.tune.export_live_pitcher_projection_dataset import _collect_projection_rows, _maybe_sync_render_history  # noqa: E402


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _mean(values: Sequence[float]) -> Optional[float]:
    numbers = [float(value) for value in values]
    if not numbers:
        return None
    return float(sum(numbers) / len(numbers))


def _median(values: Sequence[float]) -> Optional[float]:
    numbers = sorted(float(value) for value in values)
    if not numbers:
        return None
    mid = len(numbers) // 2
    if len(numbers) % 2 == 1:
        return float(numbers[mid])
    return float((numbers[mid - 1] + numbers[mid]) / 2.0)


def _progress_bucket(row: Dict[str, Any]) -> str:
    progress = _safe_float(row.get("progress_fraction"))
    if progress is None:
        return "unknown"
    if progress < 0.33:
        return "early"
    if progress < 0.66:
        return "mid"
    return "late"


def _times_through_order_bucket(row: Dict[str, Any]) -> str:
    value = _safe_float(row.get("times_through_order"))
    if value is None:
        return "unknown"
    if value < 1.5:
        return "first_pass"
    if value < 2.5:
        return "second_pass"
    return "third_plus"


def _pitch_count_bucket(row: Dict[str, Any]) -> str:
    value = _safe_int(row.get("pitch_count"))
    if value is None:
        return "unknown"
    if value < 50:
        return "under_50"
    if value < 75:
        return "50_to_74"
    if value < 95:
        return "75_to_94"
    return "95_plus"


def _line_gap_bucket(row: Dict[str, Any]) -> str:
    value = _safe_float(row.get("line_gap"))
    if value is None:
        return "unknown"
    if value <= -1.0:
        return "under_by_1_plus"
    if value < 0.0:
        return "under_small"
    if value < 1.0:
        return "over_small"
    return "over_by_1_plus"


def _summarize_group(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    projection_errors = [
        float(value)
        for value in (_safe_float(row.get("projection_error")) for row in rows)
        if value is not None
    ]
    line_gaps = [
        float(value)
        for value in (_safe_float(row.get("line_gap")) for row in rows)
        if value is not None
    ]
    wins = sum(1 for row in rows if int(row.get("label") or 0) == 1)
    return {
        "n": len(rows),
        "win_rate": round(wins / len(rows), 4) if rows else None,
        "mean_projection_error": round(_mean(projection_errors), 3) if projection_errors else None,
        "median_projection_error": round(_median(projection_errors), 3) if projection_errors else None,
        "mean_line_gap": round(_mean(line_gaps), 3) if line_gaps else None,
    }


def _group_summary(rows: Sequence[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = str(key_fn(row) or "unknown")
        groups.setdefault(key, []).append(row)
    out: List[Dict[str, Any]] = []
    for key, group_rows in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        summary = _summarize_group(group_rows)
        summary["bucket"] = key
        out.append(summary)
    return out


def _extreme_rows(rows: Sequence[Dict[str, Any]], *, limit: int = 5) -> Dict[str, List[Dict[str, Any]]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in rows:
        error = _safe_float(row.get("projection_error"))
        if error is None:
            continue
        scored.append((float(error), row))
    scored.sort(key=lambda item: item[0])
    most_under = [
        {
            "date": row.get("date"),
            "owner": row.get("owner"),
            "prop": row.get("prop"),
            "selection": row.get("selection"),
            "market_line": row.get("market_line"),
            "live_projection": row.get("live_projection"),
            "final_actual": row.get("final_actual"),
            "projection_error": round(error, 3),
            "pitch_count": row.get("pitch_count"),
            "times_through_order": row.get("times_through_order"),
            "reason_summary": row.get("reason_summary"),
            "source": row.get("source"),
        }
        for error, row in scored[:limit]
    ]
    most_over = [
        {
            "date": row.get("date"),
            "owner": row.get("owner"),
            "prop": row.get("prop"),
            "selection": row.get("selection"),
            "market_line": row.get("market_line"),
            "live_projection": row.get("live_projection"),
            "final_actual": row.get("final_actual"),
            "projection_error": round(error, 3),
            "pitch_count": row.get("pitch_count"),
            "times_through_order": row.get("times_through_order"),
            "reason_summary": row.get("reason_summary"),
            "source": row.get("source"),
        }
        for error, row in scored[-limit:][::-1]
    ]
    return {"most_under_projected": most_under, "most_over_projected": most_over}


def _analyze_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    by_prop: Dict[str, Any] = {}
    prop_keys = sorted({str(row.get("prop") or "") for row in rows if str(row.get("prop") or "")})
    for prop in prop_keys:
        prop_rows = [row for row in rows if str(row.get("prop") or "") == prop]
        by_prop[prop] = {
            "overall": _summarize_group(prop_rows),
            "by_progress": _group_summary(prop_rows, _progress_bucket),
            "by_pitch_count": _group_summary(prop_rows, _pitch_count_bucket),
            "by_times_through_order": _group_summary(prop_rows, _times_through_order_bucket),
            "by_line_gap": _group_summary(prop_rows, _line_gap_bucket),
            "extremes": _extreme_rows(prop_rows),
        }
    return {
        "rows": len(rows),
        "sources": {
            source_name: sum(1 for row in rows if str(row.get("source") or "") == source_name)
            for source_name in sorted({str(row.get("source") or "") for row in rows})
        },
        "by_prop": by_prop,
    }


def _filter_rows_by_date(
    rows: Sequence[Dict[str, Any]],
    *,
    min_date: str = "",
    max_date: str = "",
    exclude_date: str = "",
) -> List[Dict[str, Any]]:
    lower = str(min_date or "").strip()
    upper = str(max_date or "").strip()
    excluded = str(exclude_date or "").strip()
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        date_text = str(row.get("date") or "").strip()
        if not date_text:
            continue
        if lower and date_text < lower:
            continue
        if upper and date_text > upper:
            continue
        if excluded and date_text == excluded:
            continue
        filtered.append(row)
    return filtered


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze live pitcher outs/strikeouts projection bias from live-lens history.")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Root live_lens directory.")
    parser.add_argument("--source", choices=("render-sync", "local", "both"), default="render-sync", help="History source to analyze.")
    parser.add_argument("--props", default="outs,strikeouts", help="Comma-separated pitcher props to include.")
    parser.add_argument("--include-pregame", action="store_true", help="Include pregame first-seen rows.")
    parser.add_argument("--min-date", default="", help="Optional lower date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--max-date", default="", help="Optional upper date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--exclude-date", default="", help="Optional date to exclude (YYYY-MM-DD).")
    parser.add_argument("--exclude-today", action="store_true", help="Exclude the current local date from analysis.")
    parser.add_argument("--sync-render", choices=("on", "off"), default="on", help="Auto-refresh render_sync history before analysis when using render-sync sources.")
    parser.add_argument("--render-base-url", default="", help="Optional Render base URL for auto-sync.")
    parser.add_argument("--render-cron-token", default="", help="Optional Render cron token for auto-sync.")
    parser.add_argument("--render-timeout-seconds", type=int, default=45, help="Timeout for Render auto-sync requests.")
    parser.add_argument("--out", default="", help="Optional path to write JSON analysis output.")
    args = parser.parse_args()

    props = {token.strip().lower() for token in str(args.props or "").split(",") if token.strip()}
    live_lens_dir = Path(str(args.live_lens_dir)).resolve()
    sync_summary = _maybe_sync_render_history(
        live_lens_dir,
        source=str(args.source or "render-sync"),
        sync_render=str(args.sync_render or "on") == "on",
        min_date=str(args.min_date or ""),
        max_date=str(args.max_date or ""),
        render_base_url=str(args.render_base_url or ""),
        render_cron_token=str(args.render_cron_token or ""),
        render_timeout_seconds=int(args.render_timeout_seconds or 45),
        require_archive=True,
    )
    rows = _collect_projection_rows(
        live_lens_dir,
        props,
        source=str(args.source or "render-sync"),
        include_pregame=bool(args.include_pregame),
    )
    exclude_date = str(args.exclude_date or "").strip()
    if bool(args.exclude_today) and not exclude_date:
        exclude_date = dt.date.today().isoformat()
    rows = _filter_rows_by_date(
        rows,
        min_date=str(args.min_date or ""),
        max_date=str(args.max_date or ""),
        exclude_date=exclude_date,
    )
    analysis = _analyze_rows(rows)
    analysis.update(
        {
            "live_lens_dir": str(live_lens_dir),
            "source": str(args.source or "render-sync"),
            "props": sorted(props),
            "min_date": str(args.min_date or ""),
            "max_date": str(args.max_date or ""),
            "exclude_date": exclude_date,
        }
    )
    if isinstance(sync_summary, dict):
        analysis["renderSync"] = {
            "startDate": sync_summary.get("startDate"),
            "endDate": sync_summary.get("endDate"),
            "archiveOkCount": sync_summary.get("archiveOkCount"),
            "errorCount": sync_summary.get("errorCount"),
        }
    output = json.dumps(analysis, indent=2)
    if str(args.out or "").strip():
        out_path = Path(str(args.out)).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())