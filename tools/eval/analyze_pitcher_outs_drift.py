from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.eval.analyze_live_prop_projection_shapes import (  # noqa: E402
    _collect_shape_rows,
    _filter_rows_by_date,
    _mean,
    _median,
    _safe_float,
)


def _bucket_inning(value: Any) -> str:
    inning = _safe_float(value)
    if inning is None:
        return "unknown"
    if inning <= 1:
        return "inning_1"
    if inning <= 3:
        return "inning_2_3"
    if inning <= 5:
        return "inning_4_5"
    return "inning_6_plus"


def _bucket_pitch_count(value: Any) -> str:
    pitch_count = _safe_float(value)
    if pitch_count is None:
        return "unknown"
    if pitch_count < 20:
        return "pitch_lt_20"
    if pitch_count < 40:
        return "pitch_20_39"
    if pitch_count < 60:
        return "pitch_40_59"
    if pitch_count < 80:
        return "pitch_60_79"
    return "pitch_80_plus"


def _bucket_times_through_order(value: Any) -> str:
    tto = _safe_float(value)
    if tto is None:
        return "unknown"
    if tto < 1.0:
        return "tto_lt_1"
    if tto < 2.0:
        return "tto_1_to_2"
    if tto < 3.0:
        return "tto_2_to_3"
    return "tto_3_plus"


def _bucket_pitch_stress(row: Dict[str, Any]) -> str:
    actual = _safe_float(row.get("first_pitches_per_batter"))
    expected = _safe_float(row.get("first_expected_pitches_per_batter"))
    if actual is None or expected is None or expected <= 0:
        return "unknown"
    ratio = float(actual) / float(expected)
    if ratio < 0.9:
        return "low"
    if ratio <= 1.1:
        return "neutral"
    return "high"


def _summarize_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    first_abs_errors = [
        float(value) for value in (_safe_float(row.get("first_abs_error")) for row in rows) if value is not None
    ]
    last_abs_errors = [
        float(value) for value in (_safe_float(row.get("last_abs_error")) for row in rows) if value is not None
    ]
    deltas = [
        float(value) for value in (_safe_float(row.get("abs_error_delta")) for row in rows) if value is not None
    ]
    upward_rows = [row for row in rows if str(row.get("projection_move_direction") or "") == "up"]
    downward_rows = [row for row in rows if str(row.get("projection_move_direction") or "") == "down"]
    worsened = [row for row in rows if str(row.get("shape_result") or "") == "worsened"]
    return {
        "n": len(rows),
        "improvement_rate": round(sum(1 for row in rows if row.get("distance_improved") is True) / len(rows), 4) if rows else None,
        "worsened_rate": round(len(worsened) / len(rows), 4) if rows else None,
        "mean_first_abs_error": round(_mean(first_abs_errors), 3) if first_abs_errors else None,
        "mean_last_abs_error": round(_mean(last_abs_errors), 3) if last_abs_errors else None,
        "median_first_abs_error": round(_median(first_abs_errors), 3) if first_abs_errors else None,
        "median_last_abs_error": round(_median(last_abs_errors), 3) if last_abs_errors else None,
        "mean_abs_error_delta": round(_mean(deltas), 3) if deltas else None,
        "median_abs_error_delta": round(_median(deltas), 3) if deltas else None,
        "projection_up_rate": round(len(upward_rows) / len(rows), 4) if rows else None,
        "projection_down_rate": round(len(downward_rows) / len(rows), 4) if rows else None,
        "projection_up_improvement_rate": round(sum(1 for row in upward_rows if row.get("distance_improved") is True) / len(upward_rows), 4) if upward_rows else None,
        "projection_down_improvement_rate": round(sum(1 for row in downward_rows if row.get("distance_improved") is True) / len(downward_rows), 4) if downward_rows else None,
    }


def _group_summary(rows: Sequence[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(key_fn(row) or "unknown"), []).append(row)
    out: List[Dict[str, Any]] = []
    for bucket, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        summary = _summarize_rows(group_rows)
        summary["bucket"] = bucket
        out.append(summary)
    return out


def _top_examples(rows: Sequence[Dict[str, Any]], *, limit: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    def _compact(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "date": row.get("date"),
            "owner": row.get("owner"),
            "selection": row.get("selection"),
            "first_inning": row.get("first_inning"),
            "first_progress_fraction": row.get("first_progress_fraction"),
            "first_pitch_count": row.get("first_pitch_count"),
            "first_times_through_order": row.get("first_times_through_order"),
            "first_live_projection": row.get("first_live_projection"),
            "last_live_projection": row.get("last_live_projection"),
            "final_actual": row.get("final_actual"),
            "projection_delta": row.get("projection_delta"),
            "abs_error_delta": row.get("abs_error_delta"),
            "source": row.get("source"),
        }

    up_rows = [row for row in rows if str(row.get("projection_move_direction") or "") == "up"]
    worst_up = sorted(
        up_rows,
        key=lambda row: (_safe_float(row.get("abs_error_delta")) if _safe_float(row.get("abs_error_delta")) is not None else 0.0),
    )[:limit]
    biggest_up = sorted(
        up_rows,
        key=lambda row: (_safe_float(row.get("projection_delta")) if _safe_float(row.get("projection_delta")) is not None else 0.0),
        reverse=True,
    )[:limit]
    best_down = sorted(
        [row for row in rows if str(row.get("projection_move_direction") or "") == "down"],
        key=lambda row: (_safe_float(row.get("abs_error_delta")) if _safe_float(row.get("abs_error_delta")) is not None else 0.0),
        reverse=True,
    )[:limit]
    return {
        "worst_upward_moves": [_compact(row) for row in worst_up],
        "largest_upward_moves": [_compact(row) for row in biggest_up],
        "best_downward_moves": [_compact(row) for row in best_down],
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze pitcher outs drift across live projection snapshots.")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Root live_lens directory.")
    parser.add_argument("--source", choices=("render-sync", "local", "both"), default="local", help="Row source selection.")
    parser.add_argument("--min-date", default="", help="Optional lower date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--max-date", default="", help="Optional upper date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--exclude-date", default="", help="Optional date to exclude (YYYY-MM-DD).")
    parser.add_argument("--include-pregame", action="store_true", help="Include rows without live-state evidence.")
    parser.add_argument("--out", default="", help="Optional path to write JSON summary output.")
    args = parser.parse_args()

    rows = _collect_shape_rows(
        Path(str(args.live_lens_dir)).resolve(),
        source=str(args.source or "local"),
        include_pregame=bool(args.include_pregame),
        markets={"pitcher_props"},
        props={"outs"},
    )
    rows = _filter_rows_by_date(
        rows,
        min_date=str(args.min_date or ""),
        max_date=str(args.max_date or ""),
        exclude_date=str(args.exclude_date or ""),
    )

    summary = {
        "rows": len(rows),
        "overall": _summarize_rows(rows),
        "by_source": _group_summary(rows, lambda row: row.get("source")),
        "by_progress": _group_summary(rows, lambda row: row.get("progress_bucket")),
        "by_inning_bucket": _group_summary(rows, lambda row: _bucket_inning(row.get("first_inning"))),
        "by_pitch_count_bucket": _group_summary(rows, lambda row: _bucket_pitch_count(row.get("first_pitch_count"))),
        "by_tto_bucket": _group_summary(rows, lambda row: _bucket_times_through_order(row.get("first_times_through_order"))),
        "by_pitch_stress": _group_summary(rows, _bucket_pitch_stress),
        "by_projection_move": _group_summary(rows, lambda row: row.get("projection_move_direction")),
        "examples": _top_examples(rows),
    }
    if str(args.out or "").strip():
        _write_json(Path(str(args.out)).resolve(), summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())