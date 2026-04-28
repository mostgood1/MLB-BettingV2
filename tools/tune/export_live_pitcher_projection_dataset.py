from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.eval.sync_render_live_lens_history import _env_first, _infer_render_base_url, sync_render_live_lens_history


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


def _load_first_observations(observation_path: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not observation_path.exists() or not observation_path.is_file():
        return out
    for line in observation_path.read_text(encoding="utf-8").splitlines():
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


def _result_label(selection: Any, line: Any, actual: Any) -> str:
    line_value = _safe_float(line)
    actual_value = _safe_float(actual)
    if line_value is None or actual_value is None:
        return "pending"
    if abs(float(actual_value) - float(line_value)) < 1e-9:
        return "push"
    selection_text = str(selection or "").strip().lower()
    if selection_text == "under":
        return "win" if float(actual_value) < float(line_value) else "loss"
    return "win" if float(actual_value) > float(line_value) else "loss"


def _write_jsonl(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")))
            handle.write("\n")


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(str(key))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _slug_to_date(token: str) -> str:
    return str(token or "").strip().replace("_", "-")


_GAME_STATE_RE = re.compile(
    r"Game state:\s*(Top|Bottom)\s+(\d+)\s*\|\s*(\d+)\s*-\s*(\d+)\s*,\s*(\d+)\s*out",
    re.IGNORECASE,
)


def _estimated_progress_fraction(inning: Optional[int], half_inning: str, outs: Optional[int]) -> Optional[float]:
    if inning is None or inning <= 0 or outs is None:
        return None
    half = str(half_inning or "").strip().lower()
    half_offset = 0 if half == "top" else 3 if half == "bottom" else None
    if half_offset is None:
        return None
    completed_outs = max(0, (int(inning) - 1) * 6 + half_offset + max(0, min(3, int(outs))))
    return float(completed_outs / 54.0)


def _parse_reason_game_state(reason_summary: str) -> Dict[str, Any]:
    text = str(reason_summary or "").strip()
    if not text:
        return {}
    match = _GAME_STATE_RE.search(text)
    if not match:
        return {}
    half_inning = str(match.group(1) or "").strip().lower()
    inning = _safe_int(match.group(2))
    score_away = _safe_int(match.group(3))
    score_home = _safe_int(match.group(4))
    outs = _safe_int(match.group(5))
    return {
        "half_inning": half_inning,
        "inning": inning,
        "game_outs": outs,
        "score_away": score_away,
        "score_home": score_home,
        "progress_fraction": _estimated_progress_fraction(inning, half_inning, outs),
        "game_state_parsed": True,
    }


def _with_reason_enrichment(row: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(row)
    parsed = _parse_reason_game_state(str(enriched.get("reason_summary") or ""))
    if not parsed:
        enriched["game_state_parsed"] = False
        return enriched
    for field, value in parsed.items():
        if field == "game_state_parsed":
            continue
        if enriched.get(field) is None:
            enriched[field] = value
    enriched["game_state_parsed"] = True
    return enriched


def _resolve_sync_window(live_lens_dir: Path, *, min_date: str = "", max_date: str = "", exclude_today: bool = True) -> tuple[dt.date, dt.date]:
    today = dt.date.today()
    end_date = dt.date.fromisoformat(str(max_date).strip()) if str(max_date).strip() else (today - dt.timedelta(days=1) if exclude_today else today)
    if str(min_date).strip():
        start_date = dt.date.fromisoformat(str(min_date).strip())
    else:
        render_sync_dir = live_lens_dir / "render_sync"
        existing_dates: List[dt.date] = []
        if render_sync_dir.exists():
            for path in render_sync_dir.glob("live_lens_reports_*.json"):
                token = path.stem.replace("live_lens_reports_", "")
                try:
                    existing_dates.append(dt.date.fromisoformat(_slug_to_date(token)))
                except Exception:
                    continue
        start_date = min(existing_dates) if existing_dates else (end_date - dt.timedelta(days=30))
    if end_date < start_date:
        start_date = end_date
    return start_date, end_date


def _maybe_sync_render_history(
    live_lens_dir: Path,
    *,
    source: str,
    sync_render: bool,
    min_date: str = "",
    max_date: str = "",
    render_base_url: str = "",
    render_cron_token: str = "",
    render_timeout_seconds: int = 45,
    require_archive: bool = True,
) -> Optional[Dict[str, Any]]:
    normalized = str(source or "").strip().lower()
    if normalized not in {"render-sync", "both"} or not bool(sync_render):
        return None
    base_url = str(render_base_url or "").strip() or _infer_render_base_url()
    token = str(render_cron_token or "").strip() or _env_first("MLB_BETTING_CRON_TOKEN", "MLB_CRON_TOKEN", "CRON_TOKEN")
    if not base_url or not token:
        raise RuntimeError("render-sync source requested but Render base URL or cron token is unavailable for auto-sync")
    start_date, end_date = _resolve_sync_window(live_lens_dir, min_date=min_date, max_date=max_date, exclude_today=True)
    return sync_render_live_lens_history(
        base_url=base_url,
        cron_token=token,
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=int(render_timeout_seconds),
        out_dir=(live_lens_dir / "render_sync"),
        overwrite=True,
        require_archive=bool(require_archive),
    )


def _is_live_snapshot(game_state: Dict[str, Any], snapshot: Dict[str, Any]) -> bool:
    progress_fraction = _safe_float(game_state.get("progressFraction"))
    actual_so_far = _safe_float(snapshot.get("actual"))
    pitch_count = _safe_int(snapshot.get("pitchCount"))
    batters_faced = _safe_int(snapshot.get("battersFaced"))
    outs_recorded = _safe_int(snapshot.get("outsRecorded"))
    return any(
        value is not None and float(value) > 0.0
        for value in (
            progress_fraction,
            actual_so_far,
            pitch_count,
            batters_faced,
            outs_recorded,
        )
    )


def _build_projection_row(
    *,
    date_str: str,
    key: str,
    owner: Any,
    prop: str,
    selection: str,
    market_line: Any,
    entry: Dict[str, Any],
    snapshot: Dict[str, Any],
    game_state: Dict[str, Any],
    team_side: Any,
    source_name: str,
) -> Optional[Dict[str, Any]]:
    score = game_state.get("score") if isinstance(game_state.get("score"), dict) else {}
    market_line_value = _safe_float(market_line)
    first_actual = _safe_float(snapshot.get("actual"))
    final_actual = _safe_float(entry.get("finalActual") if entry.get("finalActual") is not None else entry.get("actual") if entry.get("actual") is not None else entry.get("lastSeenActual"))
    if final_actual is None:
        last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
        final_actual = _safe_float(last_snapshot.get("actual"))
    result = _result_label(selection, market_line_value, final_actual)
    if result not in {"win", "loss"}:
        return None

    live_projection = _safe_float(snapshot.get("liveProjection"))
    model_mean = _safe_float(snapshot.get("modelMean"))
    row: Dict[str, Any] = {
        "date": date_str,
        "key": str(key),
        "game_pk": _safe_int(entry.get("gamePk")),
        "owner": str(owner or "").strip(),
        "market": "pitcher_props",
        "prop": prop,
        "selection": selection,
        "market_line": market_line_value,
        "odds": _safe_int(snapshot.get("odds")),
        "live_projection": live_projection,
        "model_mean": model_mean,
        "live_edge": _safe_float(snapshot.get("liveEdge")),
        "actual_so_far": first_actual,
        "final_actual": final_actual,
        "remaining_actual": (float(final_actual) - float(first_actual)) if final_actual is not None and first_actual is not None else None,
        "projection_error": (float(final_actual) - float(live_projection)) if final_actual is not None and live_projection is not None else None,
        "line_gap": (float(live_projection) - float(market_line_value)) if live_projection is not None and market_line_value is not None else None,
        "model_gap": (float(live_projection) - float(model_mean)) if live_projection is not None and model_mean is not None else None,
        "result": result,
        "label": 1 if result == "win" else 0,
        "first_seen_at": entry.get("firstSeenAt"),
        "last_seen_at": entry.get("lastSeenAt"),
        "seen_count": _safe_int(entry.get("seenCount")),
        "team_side": team_side,
        "inning": _safe_int(game_state.get("inning")),
        "half_inning": game_state.get("halfInning"),
        "game_outs": _safe_int(game_state.get("outs")),
        "progress_fraction": _safe_float(game_state.get("progressFraction")),
        "score_away": _safe_int(score.get("away")),
        "score_home": _safe_int(score.get("home")),
        "pitch_count": _safe_int(snapshot.get("pitchCount")),
        "batters_faced": _safe_int(snapshot.get("battersFaced")),
        "strikes": _safe_int(snapshot.get("strikes")),
        "outs_recorded": _safe_int(snapshot.get("outsRecorded")),
        "strike_rate": _safe_float(snapshot.get("strikeRate")),
        "strikeout_rate": _safe_float(snapshot.get("strikeoutRate")),
        "pitches_per_batter": _safe_float(snapshot.get("pitchesPerBatter")),
        "expected_pitches_per_batter": _safe_float(snapshot.get("expectedPitchesPerBatter")),
        "stamina_pitches": _safe_int(snapshot.get("staminaPitches")),
        "pitch_count_buffer": _safe_int(snapshot.get("pitchCountBuffer")),
        "times_through_order": _safe_float(snapshot.get("timesThroughOrder")),
        "reason_summary": str(snapshot.get("reasonSummary") or "").strip(),
        "reasons": list(snapshot.get("reasons") or []),
        "source": source_name,
    }
    return row


def _iter_render_sync_projection_rows(live_lens_dir: Path, props: Set[str], *, include_pregame: bool = False) -> Iterable[Dict[str, Any]]:
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
            if str(archive_row.get("market") or "").strip().lower() != "pitcher_props":
                continue
            prop = str(archive_row.get("prop") or "").strip().lower()
            if prop not in props:
                continue
            selection = str(archive_row.get("selection") or "").strip().lower()
            if selection not in {"over", "under"}:
                continue
            snapshot = archive_row.get("firstSeenSnapshot") if isinstance(archive_row.get("firstSeenSnapshot"), dict) else {}
            game_state = archive_row.get("gameState") if isinstance(archive_row.get("gameState"), dict) else {}
            if not include_pregame and not _is_live_snapshot(game_state, snapshot):
                continue
            row = _build_projection_row(
                date_str=date_str,
                key=str(archive_row.get("key") or ""),
                owner=archive_row.get("owner"),
                prop=prop,
                selection=selection,
                market_line=archive_row.get("marketLine"),
                entry=archive_row,
                snapshot=snapshot,
                game_state=game_state,
                team_side=archive_row.get("teamSide"),
                source_name="render_sync_archive",
            )
            if row:
                yield _with_reason_enrichment(row)


def _iter_projection_rows(live_lens_dir: Path, props: set[str], *, include_pregame: bool = False) -> Iterable[Dict[str, Any]]:
    registry_dir = live_lens_dir / "prop_registry"
    if not registry_dir.exists():
        return
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
        suffix = registry_path.stem.replace("live_prop_registry_", "")
        date_str = str(doc.get("date") or suffix.replace("_", "-")).strip()
        observations = _load_first_observations(registry_dir / f"live_prop_observations_{suffix}.jsonl")
        for key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("market") or "").strip().lower() != "pitcher_props":
                continue
            prop = str(entry.get("prop") or "").strip().lower()
            if prop not in props:
                continue
            selection = str(entry.get("selection") or "").strip().lower()
            if selection not in {"over", "under"}:
                continue

            first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
            last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
            observation = observations.get(str(key)) if isinstance(observations.get(str(key)), dict) else {}
            observation_snapshot = observation.get("snapshot") if isinstance(observation.get("snapshot"), dict) else {}
            game_state = observation.get("gameState") if isinstance(observation.get("gameState"), dict) else {}
            score = game_state.get("score") if isinstance(game_state.get("score"), dict) else {}
            base_snapshot = observation_snapshot if observation_snapshot else first_snapshot
            if not include_pregame and not _is_live_snapshot(game_state, base_snapshot):
                continue

            market_line = _safe_float(entry.get("marketLine"))
            first_actual = _safe_float(observation_snapshot.get("actual") if observation_snapshot.get("actual") is not None else first_snapshot.get("actual"))
            final_actual = _safe_float(last_snapshot.get("actual"))
            result = _result_label(selection, market_line, final_actual)
            if result not in {"win", "loss"}:
                continue

            live_projection = _safe_float(observation_snapshot.get("liveProjection") if observation_snapshot.get("liveProjection") is not None else first_snapshot.get("liveProjection"))
            model_mean = _safe_float(observation_snapshot.get("modelMean") if observation_snapshot.get("modelMean") is not None else first_snapshot.get("modelMean"))
            live_edge = _safe_float(observation_snapshot.get("liveEdge") if observation_snapshot.get("liveEdge") is not None else first_snapshot.get("liveEdge"))

            row: Dict[str, Any] = {
                "date": date_str,
                "key": str(key),
                "game_pk": _safe_int(entry.get("gamePk")),
                "owner": str(entry.get("owner") or "").strip(),
                "market": "pitcher_props",
                "prop": prop,
                "selection": selection,
                "market_line": market_line,
                "odds": _safe_int(observation_snapshot.get("odds") if observation_snapshot.get("odds") is not None else first_snapshot.get("odds")),
                "live_projection": live_projection,
                "model_mean": model_mean,
                "live_edge": live_edge,
                "actual_so_far": first_actual,
                "final_actual": final_actual,
                "remaining_actual": (float(final_actual) - float(first_actual)) if final_actual is not None and first_actual is not None else None,
                "projection_error": (float(final_actual) - float(live_projection)) if final_actual is not None and live_projection is not None else None,
                "line_gap": (float(live_projection) - float(market_line)) if live_projection is not None and market_line is not None else None,
                "model_gap": (float(live_projection) - float(model_mean)) if live_projection is not None and model_mean is not None else None,
                "result": result,
                "label": 1 if result == "win" else 0,
                "first_seen_at": entry.get("firstSeenAt"),
                "last_seen_at": entry.get("lastSeenAt"),
                "seen_count": _safe_int(entry.get("seenCount")),
                "team_side": observation.get("teamSide"),
                "inning": _safe_int(game_state.get("inning")),
                "half_inning": game_state.get("halfInning"),
                "game_outs": _safe_int(game_state.get("outs")),
                "progress_fraction": _safe_float(game_state.get("progressFraction")),
                "score_away": _safe_int(score.get("away")),
                "score_home": _safe_int(score.get("home")),
                "pitch_count": _safe_int(observation_snapshot.get("pitchCount") if observation_snapshot.get("pitchCount") is not None else first_snapshot.get("pitchCount")),
                "batters_faced": _safe_int(observation_snapshot.get("battersFaced") if observation_snapshot.get("battersFaced") is not None else first_snapshot.get("battersFaced")),
                "strikes": _safe_int(observation_snapshot.get("strikes") if observation_snapshot.get("strikes") is not None else first_snapshot.get("strikes")),
                "outs_recorded": _safe_int(observation_snapshot.get("outsRecorded") if observation_snapshot.get("outsRecorded") is not None else first_snapshot.get("outsRecorded")),
                "strike_rate": _safe_float(observation_snapshot.get("strikeRate") if observation_snapshot.get("strikeRate") is not None else first_snapshot.get("strikeRate")),
                "strikeout_rate": _safe_float(observation_snapshot.get("strikeoutRate") if observation_snapshot.get("strikeoutRate") is not None else first_snapshot.get("strikeoutRate")),
                "pitches_per_batter": _safe_float(observation_snapshot.get("pitchesPerBatter") if observation_snapshot.get("pitchesPerBatter") is not None else first_snapshot.get("pitchesPerBatter")),
                "expected_pitches_per_batter": _safe_float(observation_snapshot.get("expectedPitchesPerBatter") if observation_snapshot.get("expectedPitchesPerBatter") is not None else first_snapshot.get("expectedPitchesPerBatter")),
                "stamina_pitches": _safe_int(observation_snapshot.get("staminaPitches") if observation_snapshot.get("staminaPitches") is not None else first_snapshot.get("staminaPitches")),
                "pitch_count_buffer": _safe_int(observation_snapshot.get("pitchCountBuffer") if observation_snapshot.get("pitchCountBuffer") is not None else first_snapshot.get("pitchCountBuffer")),
                "times_through_order": _safe_float(observation_snapshot.get("timesThroughOrder") if observation_snapshot.get("timesThroughOrder") is not None else first_snapshot.get("timesThroughOrder")),
                "reason_summary": str(observation_snapshot.get("reasonSummary") or first_snapshot.get("reasonSummary") or "").strip(),
                "reasons": list(observation_snapshot.get("reasons") or first_snapshot.get("reasons") or []),
                "source": "local_registry",
            }
            yield _with_reason_enrichment(row)


def _collect_projection_rows(live_lens_dir: Path, props: Set[str], *, source: str, include_pregame: bool) -> List[Dict[str, Any]]:
    normalized = str(source or "both").strip().lower() or "both"
    rows: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    iterators: List[Iterable[Dict[str, Any]]] = []
    if normalized in {"render-sync", "both"}:
        iterators.append(_iter_render_sync_projection_rows(live_lens_dir, props, include_pregame=include_pregame))
    if normalized in {"local", "both"}:
        iterators.append(_iter_projection_rows(live_lens_dir, props, include_pregame=include_pregame))
    for iterator in iterators:
        for row in iterator:
            dedupe_key = f"{str(row.get('date') or '')}|{str(row.get('key') or '')}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Export live-lens pitcher outs/strikeouts projection rows for modeling.")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Root live_lens directory containing prop_registry artifacts.")
    parser.add_argument("--out", default="data/eval/live_pitcher_projection_dataset.jsonl", help="Output dataset path (.jsonl or .csv).")
    parser.add_argument("--props", default="outs,strikeouts", help="Comma-separated pitcher prop keys to export.")
    parser.add_argument("--include-pregame", action="store_true", help="Include first-seen rows that do not have live-state context yet.")
    parser.add_argument("--source", choices=("render-sync", "local", "both"), default="both", help="Dataset source: Render-synced payloads, local registry artifacts, or both with Render sync preferred.")
    parser.add_argument("--sync-render", choices=("on", "off"), default="on", help="Auto-refresh render_sync history before using render-sync sources.")
    parser.add_argument("--min-date", default="", help="Optional lower date bound used to scope auto-sync range.")
    parser.add_argument("--max-date", default="", help="Optional upper date bound used to scope auto-sync range.")
    parser.add_argument("--render-base-url", default="", help="Optional Render base URL for auto-sync.")
    parser.add_argument("--render-cron-token", default="", help="Optional Render cron token for auto-sync.")
    parser.add_argument("--render-timeout-seconds", type=int, default=45, help="Timeout for Render auto-sync requests.")
    args = parser.parse_args()

    props = {token.strip().lower() for token in str(args.props or "").split(",") if token.strip()}
    if not props:
        raise SystemExit("No props requested.")

    live_lens_dir = Path(str(args.live_lens_dir)).resolve()
    sync_summary = _maybe_sync_render_history(
        live_lens_dir,
        source=str(args.source or "both"),
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
        source=str(args.source or "both"),
        include_pregame=bool(args.include_pregame),
    )
    out_path = Path(str(args.out)).resolve()
    suffix = out_path.suffix.lower()
    if suffix == ".csv":
        _write_csv(out_path, rows)
    else:
        _write_jsonl(out_path, rows)

    summary = {
        "rows": len(rows),
        "props": sorted(props),
        "out": str(out_path),
        "by_prop": {
            prop: sum(1 for row in rows if str(row.get("prop") or "") == prop)
            for prop in sorted(props)
        },
        "by_source": {
            source_name: sum(1 for row in rows if str(row.get("source") or "") == source_name)
            for source_name in sorted({str(row.get("source") or "") for row in rows})
        },
        "parsedGameStateRows": sum(1 for row in rows if bool(row.get("game_state_parsed"))),
    }
    if isinstance(sync_summary, dict):
        summary["renderSync"] = {
            "startDate": sync_summary.get("startDate"),
            "endDate": sync_summary.get("endDate"),
            "archiveOkCount": sync_summary.get("archiveOkCount"),
            "errorCount": sync_summary.get("errorCount"),
        }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())