from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.tune.export_live_pitcher_projection_dataset import _maybe_sync_render_history  # noqa: E402


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _slug_to_date(token: str) -> str:
    return str(token or "").strip().replace("_", "-")


def _local_source_name(live_lens_dir: Path) -> str:
    parts = {part.lower() for part in live_lens_dir.parts}
    if "render_truth" in parts:
        return "render_truth_registry"
    return "local_registry"


def _load_first_observations(path: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not path.exists() or not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        text = str(line or "").strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").strip()
        if key and key not in out:
            out[key] = row
    return out


def _is_live_snapshot(game_state: Dict[str, Any], snapshot: Dict[str, Any]) -> bool:
    progress_fraction = _safe_float(game_state.get("progressFraction"))
    actual_so_far = _safe_float(snapshot.get("actualSoFar") if snapshot.get("actualSoFar") is not None else snapshot.get("actual"))
    pitch_count = _safe_int(snapshot.get("pitchCount"))
    batters_faced = _safe_int(snapshot.get("battersFaced"))
    outs_recorded = _safe_int(snapshot.get("outsRecorded"))
    return any(
        value is not None and float(value) > 0.0
        for value in (progress_fraction, actual_so_far, pitch_count, batters_faced, outs_recorded)
    )


def _shape_metrics(first_projection: Optional[float], last_projection: Optional[float], final_actual: Optional[float]) -> Dict[str, Any]:
    first_error = None if first_projection is None or final_actual is None else float(final_actual) - float(first_projection)
    last_error = None if last_projection is None or final_actual is None else float(final_actual) - float(last_projection)
    first_abs_error = None if first_error is None else abs(float(first_error))
    last_abs_error = None if last_error is None else abs(float(last_error))
    abs_error_delta = None
    convergence_ratio = None
    distance_improved = None
    crossed_final = None
    if first_abs_error is not None and last_abs_error is not None:
        abs_error_delta = float(first_abs_error) - float(last_abs_error)
        distance_improved = bool(float(last_abs_error) + 1e-9 < float(first_abs_error))
        if float(first_abs_error) > 1e-9:
            convergence_ratio = float(last_abs_error) / float(first_abs_error)
    if first_error is not None and last_error is not None:
        crossed_final = bool(float(first_error) * float(last_error) < 0.0)
    return {
        "first_error": first_error,
        "last_error": last_error,
        "first_abs_error": first_abs_error,
        "last_abs_error": last_abs_error,
        "abs_error_delta": abs_error_delta,
        "convergence_ratio": convergence_ratio,
        "distance_improved": distance_improved,
        "crossed_final": crossed_final,
    }


def _progress_bucket(row: Dict[str, Any]) -> str:
    progress = _safe_float(row.get("first_progress_fraction"))
    if progress is None:
        return "unknown"
    if progress < 0.2:
        return "very_early"
    if progress < 0.45:
        return "early"
    if progress < 0.75:
        return "mid"
    return "late"


def _direction_bucket(value: Any, *, epsilon: float = 0.05) -> str:
    number = _safe_float(value)
    if number is None:
        return "unknown"
    if number > float(epsilon):
        return "up"
    if number < -float(epsilon):
        return "down"
    return "flat"


def _improvement_bucket(row: Dict[str, Any], *, epsilon: float = 0.1) -> str:
    delta = _safe_float(row.get("abs_error_delta"))
    if delta is None:
        return "unknown"
    if delta > float(epsilon):
        return "improved"
    if delta < -float(epsilon):
        return "worsened"
    return "flat"


def _build_shape_row(
    *,
    date_str: str,
    key: str,
    entry: Dict[str, Any],
    first_snapshot: Dict[str, Any],
    last_snapshot: Dict[str, Any],
    game_state: Dict[str, Any],
    team_side: Any,
    source_name: str,
) -> Optional[Dict[str, Any]]:
    market = str(entry.get("market") or "").strip().lower()
    prop = str(entry.get("prop") or "").strip().lower()
    selection = str(entry.get("selection") or "").strip().lower()
    if market not in {"pitcher_props", "hitter_props"}:
        return None
    if not prop or selection not in {"over", "under"}:
        return None

    market_line = _safe_float(entry.get("marketLine") if entry.get("marketLine") is not None else entry.get("market_line"))
    first_projection = _safe_float(first_snapshot.get("liveProjection"))
    last_projection = _safe_float(last_snapshot.get("liveProjection") if last_snapshot.get("liveProjection") is not None else first_snapshot.get("liveProjection"))
    first_actual_so_far = _safe_float(first_snapshot.get("actualSoFar") if first_snapshot.get("actualSoFar") is not None else first_snapshot.get("actual"))
    last_actual_so_far = _safe_float(last_snapshot.get("actualSoFar") if last_snapshot.get("actualSoFar") is not None else last_snapshot.get("actual"))
    final_actual = _safe_float(
        entry.get("finalActual")
        if entry.get("finalActual") is not None
        else entry.get("actual")
        if entry.get("actual") is not None
        else entry.get("lastSeenActual")
        if entry.get("lastSeenActual") is not None
        else last_snapshot.get("actual")
    )
    if final_actual is None:
        return None

    metrics = _shape_metrics(first_projection, last_projection, final_actual)
    projection_delta = None if first_projection is None or last_projection is None else float(last_projection) - float(first_projection)
    actual_gain = None if first_actual_so_far is None or last_actual_so_far is None else float(last_actual_so_far) - float(first_actual_so_far)
    score = game_state.get("score") if isinstance(game_state.get("score"), dict) else {}
    row: Dict[str, Any] = {
        "date": str(date_str),
        "key": str(key),
        "game_pk": _safe_int(entry.get("gamePk") if entry.get("gamePk") is not None else entry.get("game_pk")),
        "owner": str(entry.get("owner") or "").strip(),
        "market": market,
        "prop": prop,
        "selection": selection,
        "market_line": market_line,
        "first_live_projection": first_projection,
        "last_live_projection": last_projection,
        "first_actual_so_far": first_actual_so_far,
        "last_actual_so_far": last_actual_so_far,
        "final_actual": final_actual,
        "first_model_mean": _safe_float(first_snapshot.get("modelMean")),
        "last_model_mean": _safe_float(last_snapshot.get("modelMean") if last_snapshot.get("modelMean") is not None else first_snapshot.get("modelMean")),
        "first_live_edge": _safe_float(first_snapshot.get("liveEdge")),
        "last_live_edge": _safe_float(last_snapshot.get("liveEdge") if last_snapshot.get("liveEdge") is not None else first_snapshot.get("liveEdge")),
        "projection_delta": projection_delta,
        "actual_gain_during_window": actual_gain,
        "first_progress_fraction": _safe_float(game_state.get("progressFraction")),
        "first_inning": _safe_int(game_state.get("inning")),
        "first_half_inning": game_state.get("halfInning"),
        "first_outs": _safe_int(game_state.get("outs")),
        "first_pitch_count": _safe_int(first_snapshot.get("pitchCount")),
        "last_pitch_count": _safe_int(last_snapshot.get("pitchCount") if last_snapshot.get("pitchCount") is not None else first_snapshot.get("pitchCount")),
        "first_batters_faced": _safe_int(first_snapshot.get("battersFaced")),
        "last_batters_faced": _safe_int(last_snapshot.get("battersFaced") if last_snapshot.get("battersFaced") is not None else first_snapshot.get("battersFaced")),
        "first_outs_recorded": _safe_int(first_snapshot.get("outsRecorded")),
        "last_outs_recorded": _safe_int(last_snapshot.get("outsRecorded") if last_snapshot.get("outsRecorded") is not None else first_snapshot.get("outsRecorded")),
        "first_pitches_per_batter": _safe_float(first_snapshot.get("pitchesPerBatter")),
        "last_pitches_per_batter": _safe_float(last_snapshot.get("pitchesPerBatter") if last_snapshot.get("pitchesPerBatter") is not None else first_snapshot.get("pitchesPerBatter")),
        "first_expected_pitches_per_batter": _safe_float(first_snapshot.get("expectedPitchesPerBatter")),
        "last_expected_pitches_per_batter": _safe_float(last_snapshot.get("expectedPitchesPerBatter") if last_snapshot.get("expectedPitchesPerBatter") is not None else first_snapshot.get("expectedPitchesPerBatter")),
        "first_strike_rate": _safe_float(first_snapshot.get("strikeRate")),
        "last_strike_rate": _safe_float(last_snapshot.get("strikeRate") if last_snapshot.get("strikeRate") is not None else first_snapshot.get("strikeRate")),
        "first_strikeout_rate": _safe_float(first_snapshot.get("strikeoutRate")),
        "last_strikeout_rate": _safe_float(last_snapshot.get("strikeoutRate") if last_snapshot.get("strikeoutRate") is not None else first_snapshot.get("strikeoutRate")),
        "first_times_through_order": _safe_float(first_snapshot.get("timesThroughOrder")),
        "last_times_through_order": _safe_float(last_snapshot.get("timesThroughOrder") if last_snapshot.get("timesThroughOrder") is not None else first_snapshot.get("timesThroughOrder")),
        "score_away": _safe_int(score.get("away")),
        "score_home": _safe_int(score.get("home")),
        "team_side": team_side,
        "first_reason_summary": str(first_snapshot.get("reasonSummary") or "").strip(),
        "last_reason_summary": str(last_snapshot.get("reasonSummary") or first_snapshot.get("reasonSummary") or "").strip(),
        "first_seen_at": entry.get("firstSeenAt"),
        "last_seen_at": entry.get("lastSeenAt"),
        "seen_count": _safe_int(entry.get("seenCount")),
        "source": source_name,
    }
    row.update(metrics)
    row["projection_move_direction"] = _direction_bucket(row.get("projection_delta"))
    row["shape_result"] = _improvement_bucket(row)
    row["progress_bucket"] = _progress_bucket(row)
    return row


def _iter_render_sync_shape_rows(live_lens_dir: Path, *, include_pregame: bool) -> Iterable[Dict[str, Any]]:
    render_sync_dir = live_lens_dir / "render_sync"
    if not render_sync_dir.exists():
        return
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
            first_snapshot = archive_row.get("snapshot") if isinstance(archive_row.get("snapshot"), dict) else archive_row.get("firstSeenSnapshot") if isinstance(archive_row.get("firstSeenSnapshot"), dict) else {}
            last_snapshot = archive_row.get("lastSeenSnapshot") if isinstance(archive_row.get("lastSeenSnapshot"), dict) else {}
            game_state = archive_row.get("gameState") if isinstance(archive_row.get("gameState"), dict) else {}
            if not bool(include_pregame) and not _is_live_snapshot(game_state, first_snapshot):
                continue
            row = _build_shape_row(
                date_str=date_str,
                key=str(archive_row.get("key") or ""),
                entry=archive_row,
                first_snapshot=first_snapshot,
                last_snapshot=last_snapshot,
                game_state=game_state,
                team_side=archive_row.get("teamSide"),
                source_name="render_sync_archive",
            )
            if row:
                yield row


def _iter_local_shape_rows(live_lens_dir: Path, *, include_pregame: bool) -> Iterable[Dict[str, Any]]:
    registry_dir = live_lens_dir / "prop_registry"
    if not registry_dir.exists():
        return
    source_name = _local_source_name(live_lens_dir)
    for registry_path in sorted(registry_dir.glob("live_prop_registry_*.json")):
        try:
            doc = _read_json(registry_path)
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
        if not isinstance(entries, dict):
            continue
        token = registry_path.stem.replace("live_prop_registry_", "")
        date_str = str(doc.get("date") or _slug_to_date(token)).strip()
        observations = _load_first_observations(registry_dir / f"live_prop_observations_{token}.jsonl")
        for key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            observation = observations.get(str(key)) if isinstance(observations.get(str(key)), dict) else {}
            observed_snapshot = observation.get("snapshot") if isinstance(observation.get("snapshot"), dict) else {}
            first_snapshot = observed_snapshot if observed_snapshot else entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
            last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
            game_state = observation.get("gameState") if isinstance(observation.get("gameState"), dict) else {}
            if not bool(include_pregame) and not _is_live_snapshot(game_state, first_snapshot):
                continue
            row = _build_shape_row(
                date_str=date_str,
                key=str(key),
                entry=entry,
                first_snapshot=first_snapshot,
                last_snapshot=last_snapshot,
                game_state=game_state,
                team_side=observation.get("teamSide"),
                source_name=source_name,
            )
            if row:
                yield row


def _collect_shape_rows(
    live_lens_dir: Path,
    *,
    source: str,
    include_pregame: bool,
    markets: Set[str],
    props: Set[str],
) -> List[Dict[str, Any]]:
    normalized = str(source or "both").strip().lower() or "both"
    iterators: List[Iterable[Dict[str, Any]]] = []
    if normalized in {"render-sync", "both"}:
        iterators.append(_iter_render_sync_shape_rows(live_lens_dir, include_pregame=include_pregame))
    if normalized in {"local", "both"}:
        iterators.append(_iter_local_shape_rows(live_lens_dir, include_pregame=include_pregame))
    seen: Set[str] = set()
    rows: List[Dict[str, Any]] = []
    for iterator in iterators:
        for row in iterator:
            dedupe_key = f"{str(row.get('date') or '')}|{str(row.get('key') or '')}"
            if dedupe_key in seen:
                continue
            if markets and str(row.get("market") or "") not in markets:
                continue
            if props and str(row.get("prop") or "") not in props:
                continue
            seen.add(dedupe_key)
            rows.append(row)
    return rows


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


def _summarize_group(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    first_abs_errors = [
        float(value)
        for value in (_safe_float(row.get("first_abs_error")) for row in rows)
        if value is not None
    ]
    last_abs_errors = [
        float(value)
        for value in (_safe_float(row.get("last_abs_error")) for row in rows)
        if value is not None
    ]
    error_deltas = [
        float(value)
        for value in (_safe_float(row.get("abs_error_delta")) for row in rows)
        if value is not None
    ]
    convergence = [
        float(value)
        for value in (_safe_float(row.get("convergence_ratio")) for row in rows)
        if value is not None
    ]
    improved = sum(1 for row in rows if row.get("distance_improved") is True)
    crossed = sum(1 for row in rows if row.get("crossed_final") is True)
    return {
        "n": len(rows),
        "improvement_rate": round(improved / len(rows), 4) if rows else None,
        "crossed_final_rate": round(crossed / len(rows), 4) if rows else None,
        "mean_first_abs_error": round(_mean(first_abs_errors), 3) if first_abs_errors else None,
        "mean_last_abs_error": round(_mean(last_abs_errors), 3) if last_abs_errors else None,
        "median_first_abs_error": round(_median(first_abs_errors), 3) if first_abs_errors else None,
        "median_last_abs_error": round(_median(last_abs_errors), 3) if last_abs_errors else None,
        "mean_abs_error_delta": round(_mean(error_deltas), 3) if error_deltas else None,
        "median_abs_error_delta": round(_median(error_deltas), 3) if error_deltas else None,
        "mean_convergence_ratio": round(_mean(convergence), 3) if convergence else None,
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


def _example_rows(rows: Sequence[Dict[str, Any]], field: str, *, reverse: bool, limit: int = 8) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in rows:
        value = _safe_float(row.get(field))
        if value is None:
            continue
        scored.append((float(value), row))
    scored.sort(key=lambda item: item[0], reverse=bool(reverse))
    out: List[Dict[str, Any]] = []
    for value, row in scored[:limit]:
        out.append(
            {
                "date": row.get("date"),
                "owner": row.get("owner"),
                "market": row.get("market"),
                "prop": row.get("prop"),
                "selection": row.get("selection"),
                "first_live_projection": row.get("first_live_projection"),
                "last_live_projection": row.get("last_live_projection"),
                "final_actual": row.get("final_actual"),
                "first_abs_error": row.get("first_abs_error"),
                "last_abs_error": row.get("last_abs_error"),
                "abs_error_delta": row.get("abs_error_delta"),
                field: round(value, 3),
                "source": row.get("source"),
            }
        )
    return out


def _build_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "rows": len(rows),
        "sources": {
            source_name: sum(1 for row in rows if str(row.get("source") or "") == source_name)
            for source_name in sorted({str(row.get("source") or "") for row in rows})
        },
        "markets": {
            market_name: sum(1 for row in rows if str(row.get("market") or "") == market_name)
            for market_name in sorted({str(row.get("market") or "") for row in rows})
        },
        "props": {
            prop_name: sum(1 for row in rows if str(row.get("prop") or "") == prop_name)
            for prop_name in sorted({str(row.get("prop") or "") for row in rows})
        },
        "overall": _summarize_group(rows),
        "by_market": _group_summary(rows, lambda row: row.get("market")),
        "by_prop": _group_summary(rows, lambda row: row.get("prop")),
        "by_source": _group_summary(rows, lambda row: row.get("source")),
        "by_progress": _group_summary(rows, _progress_bucket),
        "by_shape_result": _group_summary(rows, lambda row: row.get("shape_result")),
        "by_projection_move": _group_summary(rows, lambda row: row.get("projection_move_direction")),
        "examples": {
            "largest_improvements": _example_rows(rows, "abs_error_delta", reverse=True),
            "largest_regressions": _example_rows(rows, "abs_error_delta", reverse=False),
            "largest_first_misses": _example_rows(rows, "first_abs_error", reverse=True),
            "largest_last_misses": _example_rows(rows, "last_abs_error", reverse=True),
        },
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")))
            handle.write("\n")


def _has_existing_render_sync_history(live_lens_dir: Path) -> bool:
    render_sync_dir = live_lens_dir / "render_sync"
    if not render_sync_dir.exists():
        return False
    return any(render_sync_dir.glob("live_lens_reports_*.json"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze live prop projection shape versus realized finals across hitter and pitcher markets.")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Root live_lens directory.")
    parser.add_argument("--source", choices=("render-sync", "local", "both"), default="both", help="Data source. 'both' prefers Render-synced archive rows and fills gaps from local registry artifacts.")
    parser.add_argument("--markets", default="pitcher_props,hitter_props", help="Comma-separated markets to include.")
    parser.add_argument("--props", default="", help="Optional comma-separated prop keys to include.")
    parser.add_argument("--include-pregame", action="store_true", help="Include rows whose first snapshot does not contain live-state evidence.")
    parser.add_argument("--min-date", default="", help="Optional lower date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--max-date", default="", help="Optional upper date bound inclusive (YYYY-MM-DD).")
    parser.add_argument("--exclude-date", default="", help="Optional date to exclude (YYYY-MM-DD).")
    parser.add_argument("--exclude-today", action="store_true", help="Exclude the current local date from analysis.")
    parser.add_argument("--sync-render", choices=("on", "off"), default="on", help="Auto-refresh render_sync history before analysis when using render-sync sources.")
    parser.add_argument("--render-base-url", default="", help="Optional Render base URL for auto-sync.")
    parser.add_argument("--render-cron-token", default="", help="Optional Render cron token for auto-sync.")
    parser.add_argument("--render-timeout-seconds", type=int, default=45, help="Timeout for Render auto-sync requests.")
    parser.add_argument("--out", default="", help="Optional path to write JSON summary output.")
    parser.add_argument("--rows-out", default="", help="Optional path to write row-level JSONL output.")
    args = parser.parse_args()

    live_lens_dir = Path(str(args.live_lens_dir)).resolve()
    sync_summary: Optional[Dict[str, Any]] = None
    sync_warning = ""
    try:
        sync_summary = _maybe_sync_render_history(
            live_lens_dir,
            source=str(args.source or "both"),
            sync_render=str(args.sync_render or "on") == "on",
            min_date=str(args.min_date or ""),
            max_date=str(args.max_date or ""),
            render_base_url=str(args.render_base_url or ""),
            render_cron_token=str(args.render_cron_token or ""),
            render_timeout_seconds=int(args.render_timeout_seconds or 45),
            require_archive=False,
        )
    except RuntimeError as exc:
        if str(args.source or "both") in {"render-sync", "both"} and _has_existing_render_sync_history(live_lens_dir):
            sync_warning = f"auto-sync skipped: {exc}"
        else:
            raise
    markets = {token.strip().lower() for token in str(args.markets or "").split(",") if token.strip()}
    props = {token.strip().lower() for token in str(args.props or "").split(",") if token.strip()}
    rows = _collect_shape_rows(
        live_lens_dir,
        source=str(args.source or "both"),
        include_pregame=bool(args.include_pregame),
        markets=markets,
        props=props,
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
    summary = _build_summary(rows)
    summary.update(
        {
            "live_lens_dir": str(live_lens_dir),
            "source": str(args.source or "both"),
            "markets_requested": sorted(markets),
            "props_requested": sorted(props),
            "min_date": str(args.min_date or ""),
            "max_date": str(args.max_date or ""),
            "exclude_date": exclude_date,
        }
    )
    if isinstance(sync_summary, dict):
        summary["renderSync"] = {
            "startDate": sync_summary.get("startDate"),
            "endDate": sync_summary.get("endDate"),
            "archiveOkCount": sync_summary.get("archiveOkCount"),
            "errorCount": sync_summary.get("errorCount"),
        }
    if sync_warning:
        summary["renderSyncWarning"] = sync_warning
    if str(args.out or "").strip():
        _write_json(Path(str(args.out)).resolve(), summary)
    if str(args.rows_out or "").strip():
        _write_jsonl(Path(str(args.rows_out)).resolve(), rows)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())