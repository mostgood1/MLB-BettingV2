from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
import sys
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Sequence, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.eval.analyze_live_prop_projection_shapes import (  # noqa: E402
    _direction_bucket,
    _local_source_name,
    _mean,
    _median,
    _progress_bucket,
    _read_json,
    _safe_float,
    _safe_int,
)


def _read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = str(line or "").strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _slug_to_date(token: str) -> str:
    return str(token or "").strip().replace("_", "-")


def _final_actual_from_entry(entry: Dict[str, Any]) -> Optional[float]:
    final_actual = _safe_float(
        entry.get("finalActual")
        if entry.get("finalActual") is not None
        else entry.get("actual")
        if entry.get("actual") is not None
        else entry.get("lastSeenActual")
    )
    if final_actual is not None:
        return final_actual
    last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
    return _safe_float(last_snapshot.get("actual"))


def _build_trajectory_rows_for_key(
    *,
    date_str: str,
    key: str,
    entry: Dict[str, Any],
    observations: Sequence[Dict[str, Any]],
    source_name: str,
) -> List[Dict[str, Any]]:
    market = str(entry.get("market") or "").strip().lower()
    prop = str(entry.get("prop") or "").strip().lower()
    selection = str(entry.get("selection") or "").strip().lower()
    if market not in {"pitcher_props", "hitter_props"}:
        return []
    if selection not in {"over", "under"} or not prop:
        return []
    final_actual = _final_actual_from_entry(entry)
    if final_actual is None:
        return []

    rows: List[Dict[str, Any]] = []
    previous_projection: Optional[float] = None
    previous_abs_error: Optional[float] = None
    sorted_observations = sorted(observations, key=lambda row: str(row.get("recordedAt") or row.get("firstSeenAt") or ""))
    for index, observation in enumerate(sorted_observations, start=1):
        snapshot = observation.get("snapshot") if isinstance(observation.get("snapshot"), dict) else {}
        game_state = observation.get("gameState") if isinstance(observation.get("gameState"), dict) else {}
        score = game_state.get("score") if isinstance(game_state.get("score"), dict) else {}
        live_projection = _safe_float(snapshot.get("liveProjection"))
        if live_projection is None:
            continue
        abs_error = abs(float(final_actual) - float(live_projection))
        projection_delta = None if previous_projection is None else float(live_projection) - float(previous_projection)
        abs_error_delta = None if previous_abs_error is None else float(previous_abs_error) - float(abs_error)
        row: Dict[str, Any] = {
            "date": date_str,
            "key": key,
            "game_pk": _safe_int(entry.get("gamePk") if entry.get("gamePk") is not None else observation.get("gamePk")),
            "owner": str(entry.get("owner") or observation.get("owner") or "").strip(),
            "market": market,
            "prop": prop,
            "selection": selection,
            "market_line": _safe_float(entry.get("marketLine") if entry.get("marketLine") is not None else snapshot.get("marketLine")),
            "final_actual": final_actual,
            "actual_so_far": _safe_float(snapshot.get("actualSoFar") if snapshot.get("actualSoFar") is not None else snapshot.get("actual")),
            "live_projection": live_projection,
            "model_mean": _safe_float(snapshot.get("modelMean")),
            "live_edge": _safe_float(snapshot.get("liveEdge")),
            "observation_index": index,
            "seen_count": _safe_int(observation.get("seenCount") if observation.get("seenCount") is not None else entry.get("seenCount")),
            "recorded_at": observation.get("recordedAt"),
            "first_seen_at": observation.get("firstSeenAt") if observation.get("firstSeenAt") is not None else entry.get("firstSeenAt"),
            "previous_seen_at": observation.get("previousSeenAt"),
            "progress_fraction": _safe_float(game_state.get("progressFraction")),
            "inning": _safe_int(game_state.get("inning")),
            "half_inning": game_state.get("halfInning"),
            "outs": _safe_int(game_state.get("outs")),
            "score_away": _safe_int(score.get("away")),
            "score_home": _safe_int(score.get("home")),
            "pitch_count": _safe_int(snapshot.get("pitchCount")),
            "batters_faced": _safe_int(snapshot.get("battersFaced")),
            "outs_recorded": _safe_int(snapshot.get("outsRecorded")),
            "pitches_per_batter": _safe_float(snapshot.get("pitchesPerBatter")),
            "expected_pitches_per_batter": _safe_float(snapshot.get("expectedPitchesPerBatter")),
            "strike_rate": _safe_float(snapshot.get("strikeRate")),
            "strikeout_rate": _safe_float(snapshot.get("strikeoutRate")),
            "times_through_order": _safe_float(snapshot.get("timesThroughOrder")),
            "snapshot_changed": bool(observation.get("snapshotChanged")),
            "changed_fields": list(observation.get("changedFields") or []),
            "source": source_name,
            "projection_error": float(final_actual) - float(live_projection),
            "abs_error": abs_error,
            "projection_delta": projection_delta,
            "abs_error_delta": abs_error_delta,
            "distance_improved_vs_prev": None if abs_error_delta is None else bool(float(abs_error_delta) > 1e-9),
        }
        row["progress_bucket"] = _progress_bucket({"first_progress_fraction": row.get("progress_fraction")})
        row["projection_move_direction"] = _direction_bucket(row.get("projection_delta"))
        rows.append(row)
        previous_projection = live_projection
        previous_abs_error = abs_error
    return rows


def _collect_trajectory_rows(
    live_lens_dir: Path,
    *,
    min_date: str,
    max_date: str,
    markets: Sequence[str],
    props: Sequence[str],
) -> List[Dict[str, Any]]:
    registry_dir = live_lens_dir / "prop_registry"
    if not registry_dir.exists():
        return []
    source_name = _local_source_name(live_lens_dir).replace("registry", "trajectory")
    market_filter = {str(token).strip().lower() for token in markets if str(token).strip()}
    prop_filter = {str(token).strip().lower() for token in props if str(token).strip()}
    rows: List[Dict[str, Any]] = []
    for registry_path in sorted(registry_dir.glob("live_prop_registry_*.json")):
        token = registry_path.stem.replace("live_prop_registry_", "")
        date_str = _slug_to_date(token)
        if min_date and date_str < min_date:
            continue
        if max_date and date_str > max_date:
            continue
        try:
            registry_doc = _read_json(registry_path)
        except Exception:
            continue
        entries = registry_doc.get("entries") if isinstance(registry_doc.get("entries"), dict) else {}
        if not isinstance(entries, dict) or not entries:
            continue
        grouped_observations: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        for observation in _read_jsonl(registry_dir / f"live_prop_observations_{token}.jsonl"):
            key = str(observation.get("key") or "").strip()
            if key:
                grouped_observations[key].append(observation)
        for key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            market = str(entry.get("market") or "").strip().lower()
            prop = str(entry.get("prop") or "").strip().lower()
            if market_filter and market not in market_filter:
                continue
            if prop_filter and prop not in prop_filter:
                continue
            rows.extend(
                _build_trajectory_rows_for_key(
                    date_str=date_str,
                    key=str(key),
                    entry=entry,
                    observations=grouped_observations.get(str(key), []),
                    source_name=source_name,
                )
            )
    return rows


def _summarize_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    abs_errors = [float(value) for value in (_safe_float(row.get("abs_error")) for row in rows) if value is not None]
    deltas = [float(value) for value in (_safe_float(row.get("abs_error_delta")) for row in rows) if value is not None]
    moved = [row for row in rows if str(row.get("projection_move_direction") or "") in {"up", "down"}]
    improved = [row for row in rows if row.get("distance_improved_vs_prev") is True]
    worsened = [row for row in rows if row.get("abs_error_delta") is not None and float(row.get("abs_error_delta")) < -1e-9]
    update_rows = [row for row in rows if row.get("abs_error_delta") is not None]
    return {
        "n": len(rows),
        "update_n": len(update_rows),
        "move_rate": round(len(moved) / len(rows), 4) if rows else None,
        "improvement_rate": round(len(improved) / len(rows), 4) if rows else None,
        "worsened_rate": round(len(worsened) / len(rows), 4) if rows else None,
        "update_improvement_rate": round(len(improved) / len(update_rows), 4) if update_rows else None,
        "update_worsened_rate": round(len(worsened) / len(update_rows), 4) if update_rows else None,
        "mean_abs_error": round(_mean(abs_errors), 3) if abs_errors else None,
        "median_abs_error": round(_median(abs_errors), 3) if abs_errors else None,
        "mean_abs_error_delta": round(_mean(deltas), 3) if deltas else None,
        "median_abs_error_delta": round(_median(deltas), 3) if deltas else None,
    }


def _group_summary(rows: Sequence[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    grouped: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(key_fn(row) or "unknown")].append(row)
    out: List[Dict[str, Any]] = []
    for bucket, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        summary = _summarize_rows(group_rows)
        summary["bucket"] = bucket
        out.append(summary)
    return out


def _examples(rows: Sequence[Dict[str, Any]], *, limit: int = 8) -> Dict[str, List[Dict[str, Any]]]:
    def _compact(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "date": row.get("date"),
            "owner": row.get("owner"),
            "market": row.get("market"),
            "prop": row.get("prop"),
            "selection": row.get("selection"),
            "observation_index": row.get("observation_index"),
            "progress_fraction": row.get("progress_fraction"),
            "live_projection": row.get("live_projection"),
            "final_actual": row.get("final_actual"),
            "projection_delta": row.get("projection_delta"),
            "abs_error": row.get("abs_error"),
            "abs_error_delta": row.get("abs_error_delta"),
            "source": row.get("source"),
        }

    movers = [row for row in rows if _safe_float(row.get("abs_error_delta")) is not None]
    best = sorted(movers, key=lambda row: float(_safe_float(row.get("abs_error_delta")) or 0.0), reverse=True)[:limit]
    worst = sorted(movers, key=lambda row: float(_safe_float(row.get("abs_error_delta")) or 0.0))[:limit]
    biggest_moves = sorted(
        [row for row in rows if _safe_float(row.get("projection_delta")) is not None],
        key=lambda row: abs(float(_safe_float(row.get("projection_delta")) or 0.0)),
        reverse=True,
    )[:limit]
    return {
        "best_updates": [_compact(row) for row in best],
        "worst_updates": [_compact(row) for row in worst],
        "largest_projection_moves": [_compact(row) for row in biggest_moves],
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze full live prop trajectories from observation logs.")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Root live_lens directory.")
    parser.add_argument("--min-date", default="", help="Optional lower date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--max-date", default="", help="Optional upper date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--markets", default="pitcher_props,hitter_props", help="Comma-separated markets to include.")
    parser.add_argument("--props", default="", help="Optional comma-separated prop keys to include.")
    parser.add_argument("--out", default="", help="Optional path to write JSON summary output.")
    args = parser.parse_args()

    rows = _collect_trajectory_rows(
        Path(str(args.live_lens_dir)).resolve(),
        min_date=str(args.min_date or "").strip(),
        max_date=str(args.max_date or "").strip(),
        markets=[token.strip() for token in str(args.markets or "").split(",") if token.strip()],
        props=[token.strip() for token in str(args.props or "").split(",") if token.strip()],
    )
    summary = {
        "rows": len(rows),
        "overall": _summarize_rows(rows),
        "by_source": _group_summary(rows, lambda row: row.get("source")),
        "by_market": _group_summary(rows, lambda row: row.get("market")),
        "by_prop": _group_summary(rows, lambda row: row.get("prop")),
        "by_progress": _group_summary(rows, lambda row: row.get("progress_bucket")),
        "by_move_direction": _group_summary(rows, lambda row: row.get("projection_move_direction")),
        "by_observation_index": _group_summary(rows, lambda row: row.get("observation_index")),
        "examples": _examples(rows),
    }
    if str(args.out or "").strip():
        _write_json(Path(str(args.out)).resolve(), summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())