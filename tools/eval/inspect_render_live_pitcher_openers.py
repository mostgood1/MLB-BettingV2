from __future__ import annotations

import argparse
import datetime as dt
import json
from collections import Counter, defaultdict
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Optional


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.eval.sync_render_live_prop_artifacts import _fetch_payload  # noqa: E402
from tools.eval.sync_render_live_lens_history import _env_first, _infer_render_base_url  # noqa: E402


def _parse_iso_datetime(value: Any) -> Optional[dt.datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _projection_delta(snapshot: Dict[str, Any]) -> Optional[float]:
    live_projection = _safe_float(snapshot.get("liveProjection"))
    model_mean = _safe_float(snapshot.get("modelMean"))
    if live_projection is None or model_mean is None:
        return None
    return round(float(live_projection) - float(model_mean), 3)


def _first_pitcher_observations(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    observation_log = payload.get("observationLog") if isinstance(payload.get("observationLog"), list) else []
    first_by_key: Dict[str, Dict[str, Any]] = {}
    for row in observation_log:
        if not isinstance(row, dict) or str(row.get("market") or "") != "pitcher_props":
            continue
        key = str(row.get("key") or "").strip()
        if not key:
            continue
        recorded_at = _parse_iso_datetime(row.get("recordedAt"))
        if recorded_at is None:
            continue
        current = first_by_key.get(key)
        if current is None or recorded_at < current["_recorded_at"]:
            first_by_key[key] = {"_recorded_at": recorded_at, **row}

    rows = list(first_by_key.values())
    rows.sort(key=lambda row: row["_recorded_at"])
    return rows


def _strict_opener_like(snapshot: Dict[str, Any]) -> bool:
    batters_faced = _safe_int(snapshot.get("battersFaced"))
    outs_recorded = _safe_int(snapshot.get("outsRecorded"))
    pitch_count = _safe_int(snapshot.get("pitchCount"))
    actual_so_far = _safe_float(snapshot.get("actualSoFar"))
    return (
        batters_faced in {None, 0}
        and outs_recorded in {None, 0}
        and pitch_count in {None, 0}
        and actual_so_far in {None, 0.0}
    )


def _zero_workload_like(snapshot: Dict[str, Any]) -> bool:
    batters_faced = _safe_int(snapshot.get("battersFaced"))
    outs_recorded = _safe_int(snapshot.get("outsRecorded"))
    actual_so_far = _safe_float(snapshot.get("actualSoFar"))
    return batters_faced in {None, 0} and outs_recorded in {None, 0} and actual_so_far in {None, 0.0}


def _observation_row_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = row.get("snapshot") if isinstance(row.get("snapshot"), dict) else {}
    projection_delta = _projection_delta(snapshot)
    recorded_at = row.get("_recorded_at")
    return {
        "recordedAt": recorded_at.isoformat() if isinstance(recorded_at, dt.datetime) else None,
        "owner": row.get("owner"),
        "prop": row.get("prop"),
        "selection": row.get("selection"),
        "liveProjection": _safe_float(snapshot.get("liveProjection")),
        "modelMean": _safe_float(snapshot.get("modelMean")),
        "projectionDelta": projection_delta,
        "actualSoFar": _safe_float(snapshot.get("actualSoFar")),
        "battersFaced": _safe_int(snapshot.get("battersFaced")),
        "pitchCount": _safe_int(snapshot.get("pitchCount")),
        "outsRecorded": _safe_int(snapshot.get("outsRecorded")),
        "strictOpenerLike": _strict_opener_like(snapshot),
        "zeroWorkloadLike": _zero_workload_like(snapshot),
    }


def _bucket_summary(rows: Iterable[Dict[str, Any]], *, flag_key: str) -> List[Dict[str, Any]]:
    by_prop: Dict[str, List[float]] = defaultdict(list)
    for row in rows:
        if not bool(row.get(flag_key)):
            continue
        delta = _safe_float(row.get("projectionDelta"))
        if delta is None:
            continue
        by_prop[str(row.get("prop") or "")].append(float(delta))

    summary: List[Dict[str, Any]] = []
    for prop, deltas in sorted(by_prop.items()):
        summary.append(
            {
                "prop": prop,
                "n": len(deltas),
                "meanProjectionDelta": round(sum(deltas) / float(len(deltas)), 3),
                "minProjectionDelta": round(min(deltas), 3),
                "maxProjectionDelta": round(max(deltas), 3),
                "deltas": [round(value, 3) for value in deltas],
            }
        )
    return summary


def build_render_live_pitcher_openers_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    first_rows = _first_pitcher_observations(payload)
    summarized_rows = [_observation_row_summary(row) for row in first_rows]
    archive_rows = [
        row
        for row in (payload.get("firstObservationArchive") if isinstance(payload.get("firstObservationArchive"), list) else [])
        if isinstance(row, dict) and str(row.get("market") or "") == "pitcher_props"
    ]
    observation_rows = [
        row
        for row in (payload.get("observationLog") if isinstance(payload.get("observationLog"), list) else [])
        if isinstance(row, dict) and str(row.get("market") or "") == "pitcher_props"
    ]
    return {
        "date": payload.get("date"),
        "generatedAt": payload.get("generatedAt"),
        "pitcherArchiveCount": len(archive_rows),
        "pitcherObservationCount": len(observation_rows),
        "firstObservationCount": len(summarized_rows),
        "firstObservationProps": dict(Counter(str(row.get("prop") or "") for row in summarized_rows)),
        "strictOpenerLikeCount": sum(1 for row in summarized_rows if row.get("strictOpenerLike")),
        "zeroWorkloadLikeCount": sum(1 for row in summarized_rows if row.get("zeroWorkloadLike")),
        "strictOpenerLikeByProp": _bucket_summary(summarized_rows, flag_key="strictOpenerLike"),
        "zeroWorkloadLikeByProp": _bucket_summary(summarized_rows, flag_key="zeroWorkloadLike"),
        "strictOpenerLikeRows": [row for row in summarized_rows if row.get("strictOpenerLike")],
        "zeroWorkloadLikeRows": [row for row in summarized_rows if row.get("zeroWorkloadLike")],
        "firstRows": summarized_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect Render-backed live pitcher opener projections from live-prop artifacts.")
    parser.add_argument("--date", required=True, help="Date to inspect (YYYY-MM-DD).")
    parser.add_argument("--base-url", default="", help="Render base URL; defaults to env or render.yaml service host.")
    parser.add_argument("--cron-token", default="", help="Cron bearer token; defaults to MLB_BETTING_CRON_TOKEN/MLB_CRON_TOKEN/CRON_TOKEN env.")
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--out", default="", help="Optional path to write JSON summary output.")
    args = parser.parse_args()

    base_url = str(args.base_url or "").strip() or _infer_render_base_url()
    cron_token = str(args.cron_token or "").strip() or _env_first("MLB_BETTING_CRON_TOKEN", "MLB_CRON_TOKEN", "CRON_TOKEN")
    if not base_url:
        raise SystemExit("Missing base URL. Pass --base-url or set MLB_BETTING_BASE_URL/RENDER_URL.")
    if not cron_token:
        raise SystemExit("Missing cron token. Pass --cron-token or set MLB_BETTING_CRON_TOKEN/MLB_CRON_TOKEN/CRON_TOKEN.")

    payload = _fetch_payload(
        base_url=base_url,
        token=cron_token,
        date_str=str(args.date),
        timeout_seconds=int(args.timeout_seconds or 45),
        include_observation_log=True,
        include_registry_log=False,
    )
    summary = build_render_live_pitcher_openers_summary(payload)
    if str(args.out or "").strip():
        out_path = Path(str(args.out)).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())