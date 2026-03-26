from __future__ import annotations

from bisect import bisect_left
import gzip
import json
import math
import os
import shutil
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from zoneinfo import ZoneInfo

from flask import Flask, Response, abort, jsonify, render_template, request

# Ensure the project root (MLB-BettingV2/) is importable when running directly.
_ROOT = Path(__file__).resolve().parents[2]
_WEB_DIR = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import (
    StatsApiClient,
    fetch_game_feed_live,
    fetch_schedule_date_buckets,
    fetch_schedule_for_date,
)
from sim_engine.market_pitcher_props import market_side_probabilities, normalize_pitcher_name
from tools.oddsapi.fetch_daily_oddsapi_markets import fetch_and_write_live_odds_for_date
from tools.eval.settle_locked_policy_cards import _settle_card


app = Flask(
    __name__,
    template_folder=str(_WEB_DIR / "templates"),
    static_folder=str(_WEB_DIR / "static"),
)


_ROOT_DIR = Path(__file__).resolve().parents[2]
_TRACKED_DATA_DIR = _ROOT_DIR / "data"
_DATA_ROOT_ENV = str(os.environ.get("MLB_BETTING_DATA_ROOT") or "").strip()
_DATA_DIR = (Path(_DATA_ROOT_ENV).resolve() if _DATA_ROOT_ENV else _TRACKED_DATA_DIR.resolve())
_DAILY_DIR = _DATA_DIR / "daily"
_MARKET_DIR = _DATA_DIR / "market" / "oddsapi"
_LIVE_LENS_DIR = Path(
    str(os.environ.get("MLB_LIVE_LENS_DIR") or os.environ.get("LIVE_LENS_DIR") or (_DATA_DIR / "live_lens")).strip()
).resolve()
_TRACKED_DAILY_SNAPSHOT_DIR = _TRACKED_DATA_DIR / "daily" / "snapshots"
_CRON_TOKEN = str(os.environ.get("MLB_CRON_TOKEN") or os.environ.get("CRON_TOKEN") or "").strip()
_USER_TIMEZONE_NAME = str(os.environ.get("MLB_USER_TIMEZONE") or "America/Chicago").strip() or "America/Chicago"
try:
    _USER_TIMEZONE = ZoneInfo(_USER_TIMEZONE_NAME)
except Exception:
    _USER_TIMEZONE = ZoneInfo("America/Chicago")
_DEMO_DATE = "2025-06-04"
_CARDS_PRESEASON_DEFAULT_WINDOW_DAYS = 21
_LIVE_PROP_MARKET_MAX_AGE_SECONDS = 60
_PITCHER_LADDER_PROPS: Dict[str, Dict[str, Any]] = {
    "strikeouts": {
        "label": "Strikeouts",
        "dist_key": "so_dist",
        "mean_key": "so_mean",
        "market_key": "strikeouts",
        "unit": "K",
    },
    "outs": {
        "label": "Outs Recorded",
        "dist_key": "outs_dist",
        "mean_key": "outs_mean",
        "market_key": "outs",
        "unit": "Outs",
    },
    "pitches": {
        "label": "Pitches",
        "dist_key": "pitches_dist",
        "mean_key": "pitches_mean",
        "market_key": None,
        "unit": "Pitches",
    },
    "hits": {
        "label": "Hits Allowed",
        "dist_key": "hits_dist",
        "mean_key": "hits_mean",
        "market_key": None,
        "unit": "Hits",
    },
    "earned_runs": {
        "label": "Earned Runs",
        "dist_key": "earned_runs_dist",
        "mean_key": "er_mean",
        "market_key": "earned_runs",
        "unit": "ER",
    },
    "walks": {
        "label": "Walks Allowed",
        "dist_key": "walks_dist",
        "mean_key": "walks_mean",
        "market_key": None,
        "unit": "Walks",
    },
    "batters_faced": {
        "label": "Batters Faced",
        "dist_key": "batters_faced_dist",
        "mean_key": "batters_faced_mean",
        "market_key": None,
        "unit": "BF",
    },
}

_HITTER_LADDER_PROPS: Dict[str, Dict[str, Any]] = {
    "hits": {
        "label": "Hits",
        "dist_key": "hits_dist",
        "mean_key": "h_mean",
        "market_key": "batter_hits",
        "unit": "Hits",
        "thresholds": (
            {"total": 1, "section_key": "hits_1plus", "prob_key": "p_h_1plus"},
            {"total": 2, "section_key": "hits_2plus", "prob_key": "p_h_2plus"},
            {"total": 3, "section_key": "hits_3plus", "prob_key": "p_h_3plus"},
        ),
    },
    "home_runs": {
        "label": "Home Runs",
        "dist_key": "home_runs_dist",
        "mean_key": "hr_mean",
        "market_key": "batter_home_runs",
        "unit": "HR",
        "thresholds": (
            {"total": 1, "section_key": "hr_1plus", "prob_key": "p_hr_1plus"},
        ),
    },
    "total_bases": {
        "label": "Total Bases",
        "dist_key": "total_bases_dist",
        "mean_key": "tb_mean",
        "market_key": "batter_total_bases",
        "unit": "TB",
        "thresholds": (
            {"total": 1, "section_key": "total_bases_1plus", "prob_key": "p_tb_1plus"},
            {"total": 2, "section_key": "total_bases_2plus", "prob_key": "p_tb_2plus"},
            {"total": 3, "section_key": "total_bases_3plus", "prob_key": "p_tb_3plus"},
            {"total": 4, "section_key": "total_bases_4plus", "prob_key": "p_tb_4plus"},
            {"total": 5, "section_key": "total_bases_5plus", "prob_key": "p_tb_5plus"},
        ),
    },
    "runs": {
        "label": "Runs",
        "dist_key": "runs_dist",
        "mean_key": "r_mean",
        "market_key": "batter_runs_scored",
        "unit": "Runs",
        "thresholds": (
            {"total": 1, "section_key": "runs_1plus", "prob_key": "p_r_1plus"},
            {"total": 2, "section_key": "runs_2plus", "prob_key": "p_r_2plus"},
            {"total": 3, "section_key": "runs_3plus", "prob_key": "p_r_3plus"},
        ),
    },
    "rbi": {
        "label": "RBIs",
        "dist_key": "rbi_dist",
        "mean_key": "rbi_mean",
        "market_key": "batter_rbis",
        "unit": "RBI",
        "thresholds": (
            {"total": 1, "section_key": "rbi_1plus", "prob_key": "p_rbi_1plus"},
            {"total": 2, "section_key": "rbi_2plus", "prob_key": "p_rbi_2plus"},
            {"total": 3, "section_key": "rbi_3plus", "prob_key": "p_rbi_3plus"},
            {"total": 4, "section_key": "rbi_4plus", "prob_key": "p_rbi_4plus"},
        ),
    },
    "doubles": {
        "label": "Doubles",
        "dist_key": "doubles_dist",
        "mean_key": "2b_mean",
        "market_key": None,
        "unit": "2B",
        "thresholds": (
            {"total": 1, "section_key": "doubles_1plus", "prob_key": "p_2b_1plus"},
        ),
    },
    "triples": {
        "label": "Triples",
        "dist_key": "triples_dist",
        "mean_key": "3b_mean",
        "market_key": None,
        "unit": "3B",
        "thresholds": (
            {"total": 1, "section_key": "triples_1plus", "prob_key": "p_3b_1plus"},
        ),
    },
    "stolen_bases": {
        "label": "Stolen Bases",
        "dist_key": "stolen_bases_dist",
        "mean_key": "sb_mean",
        "market_key": None,
        "unit": "SB",
        "thresholds": (
            {"total": 1, "section_key": "sb_1plus", "prob_key": "p_sb_1plus"},
        ),
    },
}


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _round_stat(x: Any, digits: int = 2) -> Optional[float]:
    value = _safe_float(x)
    if value is None:
        return None
    return round(float(value), int(digits))


def _date_slug(d: str) -> str:
    return str(d or "").strip().replace("-", "_")


def _data_roots() -> List[Path]:
    roots: List[Path] = []
    for candidate in (_DATA_DIR, _TRACKED_DATA_DIR.resolve()):
        resolved = candidate.resolve()
        if resolved not in roots:
            roots.append(resolved)
    return roots


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json_file(path: Path, payload: Any) -> None:
    _ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def _append_jsonl(path: Path, payload: Any) -> None:
    _ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=False) + "\n")


def _local_now() -> datetime:
    return datetime.now(_USER_TIMEZONE)


def _local_today() -> date:
    return _local_now().date()


def _local_timestamp_text(value: Optional[datetime] = None) -> str:
    stamp = value.astimezone(_USER_TIMEZONE) if isinstance(value, datetime) else _local_now()
    return stamp.isoformat(timespec="seconds")


def _daily_snapshot_dir(d: str) -> Path:
    return _DAILY_DIR / "snapshots" / str(d)


def _daily_sim_dir(d: str) -> Path:
    return _DAILY_DIR / "sims" / str(d)


def _market_refresh_archive_root() -> Path:
    return _ensure_dir(_MARKET_DIR / "refresh_history")


def _market_refresh_archive_dir(d: str, recorded_at: datetime) -> Path:
    stamp = recorded_at.strftime("%Y%m%dT%H%M%S_%fZ")
    return _ensure_dir(_market_refresh_archive_root() / _date_slug(d) / stamp)


def _archive_oddsapi_refresh_outputs(d: str, result: Dict[str, Any], *, recorded_at: datetime) -> Dict[str, Any]:
    archive_dir = _market_refresh_archive_dir(d, recorded_at)
    copied: Dict[str, str] = {}
    files: Dict[str, str] = {}

    for key in ("game_lines_path", "pitcher_props_path", "hitter_props_path"):
        source_path = Path(str(result.get(key) or "")).resolve() if result.get(key) else None
        if not source_path or not source_path.exists() or not source_path.is_file():
            continue
        destination = archive_dir / source_path.name
        shutil.copy2(source_path, destination)
        copied[source_path.name] = _relative_path_str(destination) or str(destination)
        files[key] = _relative_path_str(destination) or str(destination)

    archive_meta = {
        "recordedAt": _local_timestamp_text(recorded_at),
        "date": str(d),
        "dataRoot": _relative_path_str(_DATA_DIR),
        "marketDir": _relative_path_str(_MARKET_DIR),
        "archiveDir": _relative_path_str(archive_dir),
        "result": result,
        "files": files,
    }
    _write_json_file(archive_dir / "refresh_meta.json", archive_meta)
    _append_jsonl(_market_refresh_archive_root() / f"refresh_log_{_date_slug(d)}.jsonl", archive_meta)
    return {
        "archiveDir": _relative_path_str(archive_dir),
        "files": files,
        "copied": copied,
    }


def _cron_meta_dir() -> Path:
    return _ensure_dir(_LIVE_LENS_DIR / "cron_meta")


def _live_lens_log_path(d: str) -> Path:
    return _LIVE_LENS_DIR / f"live_lens_{_date_slug(d)}.jsonl"


def _live_lens_report_path(d: str) -> Path:
    return _LIVE_LENS_DIR / f"live_lens_report_{_date_slug(d)}.json"


def _live_prop_registry_path(d: str) -> Path:
    return _ensure_dir(_LIVE_LENS_DIR / "prop_registry") / f"live_prop_registry_{_date_slug(d)}.json"


def _live_prop_registry_log_path(d: str) -> Path:
    return _ensure_dir(_LIVE_LENS_DIR / "prop_registry") / f"live_prop_registry_{_date_slug(d)}.jsonl"


def _live_prop_tracking_key(row: Dict[str, Any]) -> str:
    owner = normalize_pitcher_name(_prop_owner_name(row) or "")
    market = str(row.get("market") or "").strip().lower()
    prop = str(row.get("prop") or "").strip().lower()
    selection = str(row.get("selection") or "").strip().lower()
    line = _safe_float(row.get("market_line"))
    game_pk = _safe_int(row.get("game_pk") or row.get("gamePk")) or 0
    return "|".join(
        [
            str(game_pk),
            owner,
            market,
            prop,
            selection,
            "" if line is None else f"{float(line):.3f}",
        ]
    )


def _live_prop_capture_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "selection": str(row.get("selection") or ""),
        "marketLine": _safe_float(row.get("market_line")),
        "odds": _safe_int(row.get("odds")),
        "liveProjection": _safe_float(row.get("live_projection")),
        "liveEdge": _safe_float(row.get("live_edge")),
        "modelMean": _safe_float(row.get("model_mean")),
        "actual": _safe_float(row.get("actual")),
    }


def _load_live_prop_registry(d: str) -> Dict[str, Any]:
    doc = _load_json_file(_live_prop_registry_path(d)) or {}
    entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
    return {
        "date": str(doc.get("date") or d),
        "updatedAt": doc.get("updatedAt"),
        "entries": dict(entries),
    }


def _enrich_live_prop_rows_with_registry(rows: List[Dict[str, Any]], d: str, *, recorded_at: Optional[datetime] = None) -> List[Dict[str, Any]]:
    if not rows:
        return []

    stamp = recorded_at.astimezone(_USER_TIMEZONE) if isinstance(recorded_at, datetime) else _local_now()
    stamp_text = _local_timestamp_text(stamp)
    registry = _load_live_prop_registry(d)
    entries = registry.get("entries") if isinstance(registry.get("entries"), dict) else {}
    changed = False
    out: List[Dict[str, Any]] = []

    for row in rows:
        item = dict(row)
        key = _live_prop_tracking_key(item)
        snapshot = _live_prop_capture_snapshot(item)
        entry = entries.get(key) if isinstance(entries.get(key), dict) else None
        if not entry:
            entry = {
                "key": key,
                "date": str(d),
                "gamePk": _safe_int(item.get("game_pk") or item.get("gamePk")),
                "owner": _prop_owner_name(item),
                "market": item.get("market"),
                "prop": item.get("prop"),
                "selection": item.get("selection"),
                "marketLine": _safe_float(item.get("market_line")),
                "firstSeenAt": stamp_text,
                "firstSeenSnapshot": snapshot,
                "lastSeenAt": stamp_text,
                "lastSeenSnapshot": snapshot,
                "seenCount": 1,
            }
            entries[key] = entry
            _append_jsonl(
                _live_prop_registry_log_path(d),
                {
                    "recordedAt": stamp_text,
                    "event": "first_seen",
                    "date": str(d),
                    "key": key,
                    "owner": _prop_owner_name(item),
                    "market": item.get("market"),
                    "prop": item.get("prop"),
                    "selection": item.get("selection"),
                    "snapshot": snapshot,
                },
            )
            changed = True
        else:
            entry["lastSeenAt"] = stamp_text
            entry["lastSeenSnapshot"] = snapshot
            entry["seenCount"] = int(_safe_int(entry.get("seenCount")) or 0) + 1
            changed = True

        first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
        item["first_seen_at"] = entry.get("firstSeenAt")
        item["last_seen_at"] = entry.get("lastSeenAt")
        item["first_seen_odds"] = _safe_int(first_snapshot.get("odds"))
        item["first_seen_line"] = _safe_float(first_snapshot.get("marketLine"))
        item["first_seen_live_projection"] = _safe_float(first_snapshot.get("liveProjection"))
        item["first_seen_live_edge"] = _safe_float(first_snapshot.get("liveEdge"))
        item["first_seen_actual"] = _safe_float(first_snapshot.get("actual"))
        item["seen_count"] = _safe_int(entry.get("seenCount"))
        out.append(item)

    if changed:
        registry["updatedAt"] = stamp_text
        registry["entries"] = entries
        _write_json_file(_live_prop_registry_path(d), registry)
    return out


def _require_cron_auth() -> Optional[Response]:
    if not _CRON_TOKEN:
        return None
    auth_header = str(request.headers.get("Authorization") or "").strip()
    supplied = ""
    if auth_header.lower().startswith("bearer "):
        supplied = auth_header[7:].strip()
    if not supplied:
        supplied = str(request.args.get("token") or request.headers.get("X-Cron-Token") or "").strip()
    if supplied == _CRON_TOKEN:
        return None
    return jsonify({"ok": False, "error": "unauthorized"}), 401


def _path_from_maybe_relative(value: Any) -> Optional[Path]:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = _ROOT_DIR / path
    return path


def _relative_path_str(path: Optional[Path]) -> Optional[str]:
    if not path:
        return None
    try:
        return str(path.relative_to(_ROOT_DIR)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _resolve_oddsapi_market_file(d: str, prefix: str) -> Optional[Path]:
    slug = _date_slug(d)
    filename = f"{prefix}_{slug}.json"
    preferred: List[Path] = []
    for data_root in _data_roots():
        preferred.append(data_root / "daily" / "snapshots" / str(d) / filename)
        preferred.append(data_root / "market" / "oddsapi" / filename)
    return _find_candidate_file(
        preferred=preferred,
        recursive_pattern=f"**/{filename}",
    )


def _file_age_seconds(path: Optional[Path]) -> Optional[float]:
    if not path or not path.exists() or not path.is_file():
        return None
    try:
        return max(0.0, float(time.time()) - float(path.stat().st_mtime))
    except Exception:
        return None


def _maybe_refresh_live_oddsapi_markets(d: str, *, max_age_seconds: int = _LIVE_PROP_MARKET_MAX_AGE_SECONDS) -> bool:
    if str(d) != str(_today_iso()):
        return False

    pitcher_path = _resolve_oddsapi_market_file(d, "oddsapi_pitcher_props")
    hitter_path = _resolve_oddsapi_market_file(d, "oddsapi_hitter_props")
    game_lines_path = _resolve_oddsapi_market_file(d, "oddsapi_game_lines")
    ages = [
        _file_age_seconds(pitcher_path),
        _file_age_seconds(hitter_path),
        _file_age_seconds(game_lines_path),
    ]
    fresh = [age for age in ages if age is not None and float(age) <= float(max_age_seconds)]
    if len(fresh) == 3:
        return False

    try:
        _refresh_oddsapi_markets(d, overwrite=True)
        return True
    except Exception:
        return False


def _find_candidate_file(*, preferred: List[Path], recursive_pattern: str) -> Optional[Path]:
    seen: set[str] = set()
    for p in preferred:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        if p.exists() and p.is_file():
            return p

    for data_dir in _data_roots():
        try:
            for p in sorted(data_dir.glob(recursive_pattern)):
                key = str(p)
                if key in seen:
                    continue
                seen.add(key)
                if p.exists() and p.is_file():
                    return p
        except Exception:
            continue
    return None


def _load_json_file_cached(path_str: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(Path(path_str).read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(obj, dict):
        return obj
    return None


def _load_json_file(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists() or not path.is_file():
        return None
    return _load_json_file_cached(str(path))


def _load_json_or_gz_file(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists() or not path.is_file():
        return None
    try:
        if str(path).lower().endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                obj = json.load(handle)
        else:
            obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(obj, dict):
        return obj
    return None


def _count_game_line_markets(games: Any) -> Dict[str, int]:
    rows = games if isinstance(games, list) else []
    return {
        "games": int(len(rows)),
        "h2h_games": int(sum(1 for row in rows if isinstance(((row or {}).get("markets") or {}).get("h2h"), dict))),
        "totals_games": int(sum(1 for row in rows if isinstance(((row or {}).get("markets") or {}).get("totals"), dict))),
        "spreads_games": int(sum(1 for row in rows if isinstance(((row or {}).get("markets") or {}).get("spreads"), dict))),
    }


def _count_prop_market_rows(props_by_name: Any) -> Dict[str, Any]:
    players = 0
    markets: Dict[str, int] = {}
    if not isinstance(props_by_name, dict):
        return {"players": 0, "markets": {}}
    for market_rows in props_by_name.values():
        if not isinstance(market_rows, dict):
            continue
        player_has_market = False
        for market_name, row in market_rows.items():
            if not isinstance(row, dict):
                continue
            if row.get("line") is None:
                continue
            player_has_market = True
            key = str(market_name or "").strip()
            if not key:
                continue
            markets[key] = int(markets.get(key, 0) + 1)
        if player_has_market:
            players += 1
    return {
        "players": int(players),
        "markets": {key: int(value) for key, value in sorted(markets.items())},
    }


def _market_file_summary(path: Optional[Path], *, root_key: str) -> Dict[str, Any]:
    doc = _load_json_file(path)
    counts: Dict[str, Any] = {}
    if isinstance(doc, dict):
        counts = dict(((doc.get("meta") or {}).get("counts") or {}))
        if not counts:
            if root_key == "games":
                counts = _count_game_line_markets(doc.get("games") or [])
            else:
                counts = _count_prop_market_rows(doc.get(root_key) or {})
    available = int(counts.get("games") or 0) > 0 if root_key == "games" else int(counts.get("players") or 0) > 0
    return {
        "exists": bool(path and path.exists() and path.is_file()),
        "available": bool(available),
        "path": _relative_path_str(path),
        "mode": str((doc or {}).get("mode") or "") if isinstance(doc, dict) else "",
        "retrievedAt": (doc or {}).get("retrieved_at") if isinstance(doc, dict) else None,
        "counts": counts,
    }


def _load_market_availability(d: str) -> Dict[str, Any]:
    game_lines = _market_file_summary(_resolve_oddsapi_market_file(d, "oddsapi_game_lines"), root_key="games")
    pitcher_props = _market_file_summary(_resolve_oddsapi_market_file(d, "oddsapi_pitcher_props"), root_key="pitcher_props")
    hitter_props = _market_file_summary(_resolve_oddsapi_market_file(d, "oddsapi_hitter_props"), root_key="hitter_props")

    warnings: List[str] = []
    game_counts = dict(game_lines.get("counts") or {})
    if int(game_counts.get("games") or 0) > 0 and int(game_counts.get("h2h_games") or 0) > 0 and int(game_counts.get("totals_games") or 0) <= 0:
        warnings.append("game lines currently show moneylines without totals")
    if game_lines.get("exists") and int(game_counts.get("games") or 0) <= 0:
        warnings.append("game lines file exists but has no captured games")
    if pitcher_props.get("exists") and int(((pitcher_props.get("counts") or {}).get("players") or 0)) <= 0:
        warnings.append("pitcher props file exists but has no captured players")
    if hitter_props.get("exists") and int(((hitter_props.get("counts") or {}).get("players") or 0)) <= 0:
        warnings.append("hitter props file exists but has no captured players")

    return {
        "gameLines": game_lines,
        "pitcherProps": pitcher_props,
        "hitterProps": hitter_props,
        "warnings": warnings,
    }


def _lineup_health_summary(lineups_path: Optional[Path], lineups_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    summary = dict(((lineups_doc or {}).get("summary") or {})) if isinstance(lineups_doc, dict) else {}
    adjusted_teams = int(summary.get("adjusted_teams") or 0)
    partial_teams = int(summary.get("partial_teams") or 0)
    return {
        "exists": bool(lineups_path and lineups_path.exists() and lineups_path.is_file()),
        "path": _relative_path_str(lineups_path),
        "status": ("warning" if adjusted_teams > 0 or partial_teams > 0 else "ok"),
        "summary": summary,
        "projectedTeams": int(summary.get("projected_teams") or 0),
        "adjustedTeams": adjusted_teams,
        "partialTeams": partial_teams,
        "fallbackPoolTeams": int(summary.get("fallback_pool_teams") or 0),
    }


def _workflow_summary(ops_report_path: Optional[Path], ops_report_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    warnings = list((ops_report_doc or {}).get("warnings") or []) if isinstance(ops_report_doc, dict) else []
    errors = list((ops_report_doc or {}).get("errors") or []) if isinstance(ops_report_doc, dict) else []
    sims_per_game = None
    stages = (ops_report_doc or {}).get("stages") if isinstance(ops_report_doc, dict) else None
    if isinstance(stages, dict):
        for stage in stages.values():
            command = (stage or {}).get("command") if isinstance(stage, dict) else None
            if not isinstance(command, list):
                continue
            try:
                sims_idx = command.index("--sims")
            except ValueError:
                continue
            if sims_idx + 1 >= len(command):
                continue
            sims_per_game = _safe_int(command[sims_idx + 1])
            if sims_per_game is not None:
                break
    return {
        "exists": bool(ops_report_path and ops_report_path.exists() and ops_report_path.is_file()),
        "path": _relative_path_str(ops_report_path),
        "status": str((ops_report_doc or {}).get("status") or "") if isinstance(ops_report_doc, dict) else "",
        "simsPerGame": sims_per_game,
        "warningCount": int(len(warnings)),
        "errorCount": int(len(errors)),
        "warnings": [str(msg) for msg in warnings[:6]],
        "errors": [str(msg) for msg in errors[:6]],
    }


def _load_cards_artifacts(d: str) -> Dict[str, Any]:
    slug = _date_slug(d)
    data_dir = _DATA_DIR
    canonical_daily_dir = data_dir / "daily"
    canonical_profile_bundle_path = canonical_daily_dir / f"daily_summary_{slug}_profile_bundle.json"
    canonical_locked_policy_path = canonical_daily_dir / f"daily_summary_{slug}_locked_policy.json"
    canonical_game_summary_path = canonical_daily_dir / f"daily_summary_{slug}.json"
    canonical_sim_dir = canonical_daily_dir / "sims" / str(d)
    canonical_snapshot_dir = canonical_daily_dir / "snapshots" / str(d)
    tracked_daily_dir = _TRACKED_DATA_DIR / "daily"

    profile_bundle_path = _find_candidate_file(
        preferred=[
            canonical_profile_bundle_path,
            tracked_daily_dir / f"daily_summary_{slug}_profile_bundle.json",
            data_dir / "_tmp_live_subcap_random_day" / f"daily_summary_{slug}_profile_bundle.json",
            data_dir / "_tmp_live_subcap_smoke" / f"daily_summary_{slug}_profile_bundle.json",
        ],
        recursive_pattern=f"**/daily_summary_{slug}_profile_bundle.json",
    )
    profile_bundle = _load_json_file(profile_bundle_path)

    locked_policy_path = _find_candidate_file(
        preferred=[
            canonical_locked_policy_path,
            tracked_daily_dir / f"daily_summary_{slug}_locked_policy.json",
            data_dir / "_tmp_live_subcap_random_day" / f"daily_summary_{slug}_locked_policy.json",
            data_dir / "_tmp_live_subcap_smoke" / f"daily_summary_{slug}_locked_policy.json",
            data_dir / f"daily_summary_{slug}_locked_policy.json",
        ],
        recursive_pattern=f"**/daily_summary_{slug}_locked_policy.json",
    )
    if not locked_policy_path and isinstance(profile_bundle, dict):
        locked_policy_path = _path_from_maybe_relative(
            ((profile_bundle.get("official_locked_policy") or {}).get("card_path"))
        )
    locked_policy = _load_json_file(locked_policy_path)

    game_summary_path: Optional[Path] = canonical_game_summary_path if canonical_game_summary_path.exists() and canonical_game_summary_path.is_file() else None
    if not game_summary_path:
        tracked_game_summary_path = tracked_daily_dir / f"daily_summary_{slug}.json"
        if tracked_game_summary_path.exists() and tracked_game_summary_path.is_file():
            game_summary_path = tracked_game_summary_path
    sim_dir: Optional[Path] = canonical_sim_dir if canonical_sim_dir.exists() and canonical_sim_dir.is_dir() else None
    if not sim_dir:
        tracked_sim_dir = tracked_daily_dir / "sims" / str(d)
        if tracked_sim_dir.exists() and tracked_sim_dir.is_dir():
            sim_dir = tracked_sim_dir
    snapshot_dir: Optional[Path] = canonical_snapshot_dir if canonical_snapshot_dir.exists() and canonical_snapshot_dir.is_dir() else None
    if not snapshot_dir:
        tracked_snapshot_dir = tracked_daily_dir / "snapshots" / str(d)
        if tracked_snapshot_dir.exists() and tracked_snapshot_dir.is_dir():
            snapshot_dir = tracked_snapshot_dir
    for artifact in (locked_policy, profile_bundle):
        if not isinstance(artifact, dict):
            continue
        game_profile = ((artifact.get("profiles") or {}).get("game_recos") or {})
        if not game_summary_path:
            candidate = _path_from_maybe_relative(game_profile.get("summary_path"))
            if candidate and candidate.exists() and candidate.is_file():
                game_summary_path = candidate
        if not sim_dir:
            candidate = _path_from_maybe_relative(game_profile.get("sim_dir"))
            if candidate and candidate.exists() and candidate.is_dir():
                sim_dir = candidate
        if not snapshot_dir:
            candidate = _path_from_maybe_relative(game_profile.get("snapshot_dir"))
            if candidate and candidate.exists() and candidate.is_dir():
                snapshot_dir = candidate

    if not game_summary_path:
        game_summary_path = _find_candidate_file(
            preferred=[canonical_game_summary_path],
            recursive_pattern=f"**/daily_summary_{slug}.json",
        )
    if not sim_dir and canonical_sim_dir.exists() and canonical_sim_dir.is_dir():
        sim_dir = canonical_sim_dir
    if not snapshot_dir and canonical_snapshot_dir.exists() and canonical_snapshot_dir.is_dir():
        snapshot_dir = canonical_snapshot_dir

    preferred_ops_paths: List[Path] = []
    preferred_ops_paths.append(canonical_daily_dir / "ops" / f"daily_ops_{slug}.json")
    if game_summary_path:
        preferred_ops_paths.append(game_summary_path.parent / "ops" / f"daily_ops_{slug}.json")
    ops_report_path = _find_candidate_file(
        preferred=preferred_ops_paths,
        recursive_pattern=f"**/daily_ops_{slug}.json",
    )
    ops_report = _load_json_file(ops_report_path)

    lineups_path = (snapshot_dir / "lineups.json") if snapshot_dir else None
    lineups = _load_json_file(lineups_path)
    market_availability = _load_market_availability(d)

    return {
        "profile_bundle_path": profile_bundle_path,
        "profile_bundle": profile_bundle,
        "locked_policy_path": locked_policy_path,
        "locked_policy": locked_policy,
        "game_summary_path": game_summary_path,
        "game_summary": _load_json_file(game_summary_path),
        "sim_dir": sim_dir,
        "snapshot_dir": snapshot_dir,
        "lineups_path": lineups_path,
        "lineups": lineups,
        "ops_report_path": ops_report_path,
        "ops_report": ops_report,
        "market_availability": market_availability,
        "canonical_daily": bool(
            (canonical_locked_policy_path.exists() and canonical_locked_policy_path.is_file())
            or (canonical_game_summary_path.exists() and canonical_game_summary_path.is_file())
        ),
    }


def _pitcher_ladder_prop_options() -> List[Dict[str, str]]:
    return [
        {"value": key, "label": str(cfg.get("label") or key.title())}
        for key, cfg in _PITCHER_LADDER_PROPS.items()
    ]


def _normalize_pitcher_ladder_prop(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "": "strikeouts",
        "k": "strikeouts",
        "ks": "strikeouts",
        "so": "strikeouts",
        "strikeout": "strikeouts",
        "strikeouts": "strikeouts",
        "out": "outs",
        "outs": "outs",
        "hit": "hits",
        "hits": "hits",
        "er": "earned_runs",
        "earned_run": "earned_runs",
        "earned_runs": "earned_runs",
        "earnedruns": "earned_runs",
        "bb": "walks",
        "walk": "walks",
        "walks": "walks",
        "bf": "batters_faced",
        "batter_faced": "batters_faced",
        "batters_faced": "batters_faced",
        "battersfaced": "batters_faced",
        "pitch": "pitches",
        "pitches": "pitches",
    }
    normalized = aliases.get(token, token)
    if normalized not in _PITCHER_LADDER_PROPS:
        return "strikeouts"
    return normalized


def _hitter_ladder_prop_options() -> List[Dict[str, str]]:
    return [
        {"value": key, "label": str(cfg.get("label") or key.title())}
        for key, cfg in _HITTER_LADDER_PROPS.items()
    ]


def _normalize_hitter_ladder_prop(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "": "hits",
        "hit": "hits",
        "hits": "hits",
        "hr": "home_runs",
        "home_run": "home_runs",
        "home_runs": "home_runs",
        "homer": "home_runs",
        "homers": "home_runs",
        "tb": "total_bases",
        "total_base": "total_bases",
        "total_bases": "total_bases",
        "bases": "total_bases",
        "run": "runs",
        "runs": "runs",
        "rbi": "rbi",
        "rbis": "rbi",
        "double": "doubles",
        "doubles": "doubles",
        "triple": "triples",
        "triples": "triples",
        "sb": "stolen_bases",
        "stolen_base": "stolen_bases",
        "stolen_bases": "stolen_bases",
        "steals": "stolen_bases",
    }
    normalized = aliases.get(token, token)
    if normalized not in _HITTER_LADDER_PROPS:
        return "hits"
    return normalized


def _load_pitcher_prop_market_lines(d: str) -> Tuple[Optional[Path], Dict[str, Dict[str, Dict[str, Any]]]]:
    path = _resolve_oddsapi_market_file(d, "oddsapi_pitcher_props")
    doc = _load_json_file(path)
    raw = (doc or {}).get("pitcher_props") or {}
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    if not isinstance(raw, dict):
        return path, out

    for raw_name, markets in raw.items():
        nk = normalize_pitcher_name(str(raw_name))
        if not nk or not isinstance(markets, dict):
            continue
        for market_key in ("strikeouts", "outs", "earned_runs"):
            market = markets.get(market_key)
            if not isinstance(market, dict):
                continue
            line = _safe_float(market.get("line"))
            if line is None:
                continue
            out.setdefault(nk, {})[market_key] = {
                "line": float(line),
                "over_odds": _safe_int(market.get("over_odds")),
                "under_odds": _safe_int(market.get("under_odds")),
                "alternates": list(market.get("alternates") or []),
            }
    return path, out


def _load_hitter_prop_market_lines(d: str) -> Tuple[Optional[Path], Dict[str, Dict[str, Dict[str, Any]]]]:
    path = _resolve_oddsapi_market_file(d, "oddsapi_hitter_props")
    doc = _load_json_file(path)
    raw = (doc or {}).get("hitter_props") or {}
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    if not isinstance(raw, dict):
        return path, out

    for raw_name, markets in raw.items():
        nk = normalize_pitcher_name(str(raw_name))
        if not nk or not isinstance(markets, dict):
            continue
        for market_key, market in markets.items():
            if not isinstance(market, dict):
                continue
            line = _safe_float(market.get("line"))
            if line is None:
                continue
            out.setdefault(nk, {})[str(market_key)] = {
                "line": float(line),
                "over_odds": _safe_int(market.get("over_odds")),
                "under_odds": _safe_int(market.get("under_odds")),
                "alternates": list(market.get("alternates") or []),
            }
    return path, out


def _market_line_entry(*, stat_key: str, label: str, unit: str, market: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(market, dict):
        return None
    line = _safe_float(market.get("line"))
    if line is None:
        return None
    return {
        "stat": str(stat_key),
        "label": str(label),
        "unit": str(unit),
        "line": float(line),
        "overOdds": _safe_int(market.get("over_odds")),
        "underOdds": _safe_int(market.get("under_odds")),
        "alternates": list(market.get("alternates") or []),
    }


def _pitcher_market_lines_by_stat(markets: Any) -> List[Dict[str, Any]]:
    if not isinstance(markets, dict):
        return []
    out: List[Dict[str, Any]] = []
    for stat_key, cfg in _PITCHER_LADDER_PROPS.items():
        market_key = str(cfg.get("market_key") or "").strip()
        if not market_key:
            continue
        entry = _market_line_entry(
            stat_key=str(stat_key),
            label=str(cfg.get("label") or stat_key),
            unit=str(cfg.get("unit") or ""),
            market=markets.get(market_key),
        )
        if entry:
            out.append(entry)
    return out


def _hitter_market_lines_by_stat(markets: Any) -> List[Dict[str, Any]]:
    if not isinstance(markets, dict):
        return []
    out: List[Dict[str, Any]] = []
    for stat_key, cfg in _HITTER_LADDER_PROPS.items():
        market_key = str(cfg.get("market_key") or "").strip()
        if not market_key:
            continue
        entry = _market_line_entry(
            stat_key=str(stat_key),
            label=str(cfg.get("label") or stat_key),
            unit=str(cfg.get("unit") or ""),
            market=markets.get(market_key),
        )
        if entry:
            out.append(entry)
    return out


def _dist_to_ladder_rows(raw_dist: Any) -> Tuple[List[Dict[str, Any]], int, Optional[int], Optional[int]]:
    if not isinstance(raw_dist, dict):
        return [], 0, None, None

    counts: Dict[int, int] = {}
    for raw_total, raw_count in raw_dist.items():
        total = _safe_int(raw_total)
        count = _safe_int(raw_count)
        if total is None or count is None or count <= 0:
            continue
        counts[int(total)] = int(count)
    if not counts:
        return [], 0, None, None

    sim_count = int(sum(counts.values()))
    min_total = int(min(counts.keys()))
    max_total = int(max(counts.keys()))
    running = 0
    rows_desc: List[Dict[str, Any]] = []
    denom = float(max(1, sim_count))
    for total in range(max_total, min_total - 1, -1):
        exact_count = int(counts.get(int(total), 0))
        running += exact_count
        rows_desc.append(
            {
                "total": int(total),
                "exactCount": int(exact_count),
                "exactProb": float(exact_count / denom),
                "hitCount": int(running),
                "hitProb": float(running / denom),
            }
        )
    rows_desc.reverse()
    return rows_desc, sim_count, min_total, max_total


def _normalize_pitcher_selector(value: Any) -> str:
    return str(value or "").strip()


def _normalize_game_selector(value: Any) -> str:
    token = str(value or "").strip()
    return token if token.isdigit() else ""


def _threshold_prob_to_count(prob: Any, sim_count: int) -> int:
    value = _safe_float(prob)
    if value is None or sim_count <= 0:
        return 0
    return int(round(float(value) * float(sim_count)))


def _threshold_ladder_rows(prob_by_total: Dict[int, float], sim_count: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for total in sorted(int(key) for key in prob_by_total.keys()):
        prob = float(prob_by_total.get(int(total), 0.0) or 0.0)
        rows.append(
            {
                "total": int(total),
                "hitCount": _threshold_prob_to_count(prob, sim_count),
                "hitProb": float(prob),
            }
        )
    return rows


def _normalize_hitter_selector(value: Any) -> str:
    return str(value or "").strip()


def _normalize_hitter_team_selector(value: Any) -> str:
    return str(value or "").strip().upper()


def _pitcher_ladder_sort_options() -> List[Dict[str, str]]:
    return [
        {"value": "team", "label": "Team"},
        {"value": "mean", "label": "Mean"},
        {"value": "mode", "label": "Mode"},
    ]


def _normalize_pitcher_ladder_sort(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token not in {"team", "mean", "mode"}:
        return "team"
    return token


def _sort_pitcher_ladder_rows(rows: List[Dict[str, Any]], sort_key: str) -> List[Dict[str, Any]]:
    normalized = _normalize_pitcher_ladder_sort(sort_key)
    if normalized == "mean":
        return sorted(
            rows,
            key=lambda row: (
                -float(_safe_float(row.get("mean")) or float("-inf")),
                str(row.get("team") or ""),
                str(row.get("pitcherName") or ""),
            ),
        )
    if normalized == "mode":
        return sorted(
            rows,
            key=lambda row: (
                -int(_safe_int(row.get("mode")) or -1),
                -float(_safe_float(row.get("modeProb")) or -1.0),
                -float(_safe_float(row.get("mean")) or float("-inf")),
                str(row.get("team") or ""),
                str(row.get("pitcherName") or ""),
            ),
        )
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("team") or ""),
            str(row.get("opponent") or ""),
            str(row.get("pitcherName") or ""),
        ),
    )


def _hitter_ladder_sort_options() -> List[Dict[str, str]]:
    return [
        {"value": "team", "label": "Team"},
        {"value": "mean", "label": "Mean"},
        {"value": "mode", "label": "Mode"},
    ]


def _normalize_hitter_ladder_sort(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token not in {"team", "mean", "mode"}:
        return "team"
    return token


def _sort_hitter_ladder_rows(rows: List[Dict[str, Any]], sort_key: str) -> List[Dict[str, Any]]:
    normalized = _normalize_hitter_ladder_sort(sort_key)
    if normalized == "mean":
        return sorted(
            rows,
            key=lambda row: (
                -float(_safe_float(row.get("mean")) or float("-inf")),
                str(row.get("team") or ""),
                int(_safe_int(row.get("lineupOrder")) or 99),
                str(row.get("hitterName") or ""),
            ),
        )
    if normalized == "mode":
        return sorted(
            rows,
            key=lambda row: (
                -int(_safe_int(row.get("mode")) or -1),
                -float(_safe_float(row.get("modeProb")) or -1.0),
                -float(_safe_float(row.get("mean")) or float("-inf")),
                str(row.get("team") or ""),
                str(row.get("hitterName") or ""),
            ),
        )
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("team") or ""),
            int(_safe_int(row.get("lineupOrder")) or 99),
            str(row.get("hitterName") or ""),
        ),
    )


def _pitcher_ladders_payload(d: str, prop_value: Any, sort_value: Any) -> Dict[str, Any]:
    prop = _normalize_pitcher_ladder_prop(prop_value)
    selected_game = _normalize_game_selector(request.args.get("game"))
    selected_pitcher = _normalize_pitcher_selector(request.args.get("pitcher"))
    sort_key = _normalize_pitcher_ladder_sort(sort_value)
    prop_cfg = _PITCHER_LADDER_PROPS[prop]
    artifacts = _load_cards_artifacts(d)
    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    market_path, market_lines = _load_pitcher_prop_market_lines(d)
    nav = _cards_nav_from_schedule(d) or {"season": _season_from_date_str(d)}

    payload: Dict[str, Any] = {
        "date": str(d),
        "prop": prop,
        "propLabel": str(prop_cfg.get("label") or prop.title()),
        "propUnit": str(prop_cfg.get("unit") or ""),
        "propOptions": _pitcher_ladder_prop_options(),
        "selectedGame": selected_game,
        "selectedPitcher": selected_pitcher,
        "gameOptions": [],
        "selectedSort": sort_key,
        "pitcherOptions": [],
        "sortOptions": _pitcher_ladder_sort_options(),
        "defaultSims": 1000,
        "found": False,
        "sourceDir": _relative_path_str(sim_dir),
        "marketSource": _relative_path_str(market_path),
        "nav": nav,
        "rows": [],
    }
    if not sim_dir or not sim_dir.exists() or not sim_dir.is_dir():
        payload["error"] = "sim_dir_missing"
        return payload

    sim_files = sorted(path for path in sim_dir.glob("sim_*.json") if path.is_file())
    rows: List[Dict[str, Any]] = []
    for sim_path in sim_files:
        sim_obj = _load_json_file(sim_path)
        if not isinstance(sim_obj, dict):
            continue
        starters = sim_obj.get("starters") or {}
        starter_names = sim_obj.get("starter_names") or {}
        sim_pitcher_props = ((sim_obj.get("sim") or {}).get("pitcher_props") or {})
        away_team_id = _safe_int((sim_obj.get("away") or {}).get("team_id"))
        home_team_id = _safe_int((sim_obj.get("home") or {}).get("team_id"))
        away_abbr = _first_text((sim_obj.get("away") or {}).get("abbreviation"), (sim_obj.get("away") or {}).get("name"), "AWAY")
        home_abbr = _first_text((sim_obj.get("home") or {}).get("abbreviation"), (sim_obj.get("home") or {}).get("name"), "HOME")
        game_pk = _safe_int(sim_obj.get("game_pk"))

        for side in ("away", "home"):
            starter_id = _safe_int(starters.get(side))
            starter_name = _first_text(starter_names.get(side))
            if starter_id is None or not starter_name:
                continue
            pred = sim_pitcher_props.get(str(int(starter_id)))
            if not isinstance(pred, dict):
                continue
            ladder_rows, sim_count, min_total, max_total = _dist_to_ladder_rows(pred.get(str(prop_cfg.get("dist_key"))))
            if not ladder_rows:
                continue
            team = away_abbr if side == "away" else home_abbr
            opponent = home_abbr if side == "away" else away_abbr
            team_id = away_team_id if side == "away" else home_team_id
            opponent_team_id = home_team_id if side == "away" else away_team_id
            player_market_lines = market_lines.get(normalize_pitcher_name(starter_name)) or {}
            market = {}
            market_key = prop_cfg.get("market_key")
            if market_key:
                market = (player_market_lines.get(str(market_key)) or {})
            market_line = _safe_float(market.get("line")) if isinstance(market, dict) else None
            over_line_count = None
            over_line_prob = None
            if market_line is not None:
                over_line_count = int(sum(row.get("exactCount") or 0 for row in ladder_rows if float(row.get("total") or 0) > float(market_line)))
                over_line_prob = float(over_line_count / float(max(1, sim_count)))
            mode_row = max(
                ladder_rows,
                key=lambda row: (int(row.get("exactCount") or 0), -int(row.get("total") or 0)),
            )
            rows.append(
                {
                    "gamePk": int(game_pk) if game_pk is not None else None,
                    "pitcherId": int(starter_id),
                    "pitcherName": starter_name,
                    "headshotUrl": _mlb_headshot_url(int(starter_id)),
                    "team": team,
                    "teamId": int(team_id) if team_id is not None else None,
                    "teamLogoUrl": (_mlb_logo_url(int(team_id)) if team_id is not None else None),
                    "opponent": opponent,
                    "opponentTeamId": int(opponent_team_id) if opponent_team_id is not None else None,
                    "opponentLogoUrl": (_mlb_logo_url(int(opponent_team_id)) if opponent_team_id is not None else None),
                    "side": side,
                    "matchup": f"{team} @ {opponent}" if side == "away" else f"{opponent} @ {team}",
                    "mean": _safe_float(pred.get(str(prop_cfg.get("mean_key")))),
                    "mode": int(mode_row.get("total") or 0),
                    "modeCount": int(mode_row.get("exactCount") or 0),
                    "modeProb": float(mode_row.get("exactProb") or 0.0),
                    "simCount": int(sim_count),
                    "minTotal": min_total,
                    "maxTotal": max_total,
                    "marketLine": market_line,
                    "marketLinesByStat": _pitcher_market_lines_by_stat(player_market_lines),
                    "overLineCount": over_line_count,
                    "overLineProb": over_line_prob,
                    "ladder": ladder_rows,
                    "sourceFile": _relative_path_str(sim_path),
                }
            )

    payload["gameOptions"] = [
        {
            "value": str(int(row.get("gamePk") or 0)),
            "label": str(row.get("matchup") or f"Game {int(row.get('gamePk') or 0)}"),
            "gamePk": int(row.get("gamePk") or 0),
            "matchup": str(row.get("matchup") or ""),
        }
        for row in sorted(
            {
                int(row.get("gamePk") or 0): {
                    "gamePk": int(row.get("gamePk") or 0),
                    "matchup": str(row.get("matchup") or ""),
                }
                for row in rows
                if _safe_int(row.get("gamePk")) is not None
            }.values(),
            key=lambda item: (str(item.get("matchup") or ""), int(item.get("gamePk") or 0)),
        )
    ]

    if selected_game:
        rows = [row for row in rows if str(int(row.get("gamePk") or 0)) == selected_game]

    rows = _sort_pitcher_ladder_rows(rows, sort_key)
    payload["pitcherOptions"] = [
        {
            "value": str(int(row.get("pitcherId") or 0)),
            "label": f"{row.get('pitcherName') or 'Unknown'} ({row.get('team') or '-'} vs {row.get('opponent') or '-'})",
            "pitcherId": int(row.get("pitcherId") or 0),
            "pitcherName": str(row.get("pitcherName") or ""),
            "headshotUrl": row.get("headshotUrl"),
            "teamLogoUrl": row.get("teamLogoUrl"),
            "opponentLogoUrl": row.get("opponentLogoUrl"),
        }
        for row in rows
    ]

    if selected_pitcher:
        if selected_pitcher.isdigit():
            rows = [row for row in rows if str(int(row.get("pitcherId") or 0)) == selected_pitcher]
        else:
            target_name = normalize_pitcher_name(selected_pitcher)
            rows = [row for row in rows if normalize_pitcher_name(row.get("pitcherName")) == target_name]

    if not rows:
        payload["error"] = "pitcher_ladders_missing"
        if selected_game:
            payload["error"] = "pitcher_ladders_game_missing"
        if selected_pitcher:
            payload["error"] = "pitcher_ladders_pitcher_missing"
        payload["summary"] = {
            "games": int(len(sim_files)),
            "starters": 0,
            "simCounts": [],
            "availableGames": int(len(payload.get("gameOptions") or [])),
            "availableStarters": int(len(payload.get("pitcherOptions") or [])),
        }
        return payload

    payload["found"] = True
    payload["rows"] = rows
    payload["summary"] = {
        "games": int(len(sim_files)),
        "starters": int(len(rows)),
        "simCounts": sorted({int(row.get("simCount") or 0) for row in rows if int(row.get("simCount") or 0) > 0}),
        "availableGames": int(len(payload.get("gameOptions") or [])),
        "availableStarters": int(len(payload.get("pitcherOptions") or [])),
    }
    return payload


def _hitter_ladders_payload(d: str, prop_value: Any) -> Dict[str, Any]:
    prop = _normalize_hitter_ladder_prop(prop_value)
    selected_game = _normalize_game_selector(request.args.get("game"))
    selected_team = _normalize_hitter_team_selector(request.args.get("team"))
    selected_hitter = _normalize_hitter_selector(request.args.get("hitter"))
    sort_key = _normalize_hitter_ladder_sort(request.args.get("sort"))
    prop_cfg = _HITTER_LADDER_PROPS[prop]
    artifacts = _load_cards_artifacts(d)
    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    market_path, market_lines = _load_hitter_prop_market_lines(d)
    nav = _cards_nav_from_schedule(d) or {"season": _season_from_date_str(d)}

    payload: Dict[str, Any] = {
        "date": str(d),
        "prop": prop,
        "propLabel": str(prop_cfg.get("label") or prop.title()),
        "propUnit": str(prop_cfg.get("unit") or ""),
        "propOptions": _hitter_ladder_prop_options(),
        "selectedGame": selected_game,
        "selectedTeam": selected_team,
        "selectedHitter": selected_hitter,
        "gameOptions": [],
        "teamOptions": [],
        "selectedSort": sort_key,
        "hitterOptions": [],
        "sortOptions": _hitter_ladder_sort_options(),
        "defaultSims": 1000,
        "found": False,
        "ladderShape": "threshold",
        "sourceDir": _relative_path_str(sim_dir),
        "marketSource": _relative_path_str(market_path),
        "nav": nav,
        "rows": [],
    }
    if not sim_dir or not sim_dir.exists() or not sim_dir.is_dir():
        payload["error"] = "sim_dir_missing"
        return payload

    sim_files = sorted(path for path in sim_dir.glob("sim_*.json") if path.is_file())
    threshold_specs = list(prop_cfg.get("thresholds") or [])
    rows: List[Dict[str, Any]] = []
    topn_limits: List[int] = []
    for sim_path in sim_files:
        sim_obj = _load_json_file(sim_path)
        if not isinstance(sim_obj, dict):
            continue
        sim_payload = sim_obj.get("sim") or {}
        sim_count = _safe_int(sim_payload.get("sims")) or 0
        exact_hitter_props = sim_payload.get("hitter_props") or {}
        hitter_topn = sim_payload.get("hitter_props_likelihood_topn") or {}
        hitter_hr_topn = sim_payload.get("hitter_hr_likelihood_topn") or {}
        if isinstance(hitter_topn, dict):
            topn_limit = _safe_int(hitter_topn.get("n"))
            if topn_limit is not None and topn_limit > 0:
                topn_limits.append(int(topn_limit))
        if prop == "home_runs" and isinstance(hitter_hr_topn, dict):
            topn_limit = _safe_int(hitter_hr_topn.get("n"))
            if topn_limit is not None and topn_limit > 0:
                topn_limits.append(int(topn_limit))

        away_team_id = _safe_int((sim_obj.get("away") or {}).get("team_id"))
        home_team_id = _safe_int((sim_obj.get("home") or {}).get("team_id"))
        away_abbr = _first_text((sim_obj.get("away") or {}).get("abbreviation"), (sim_obj.get("away") or {}).get("name"), "AWAY")
        home_abbr = _first_text((sim_obj.get("home") or {}).get("abbreviation"), (sim_obj.get("home") or {}).get("name"), "HOME")
        game_pk = _safe_int(sim_obj.get("game_pk"))

        exact_rows_added = False
        if isinstance(exact_hitter_props, dict):
            for batter_key, raw_row in exact_hitter_props.items():
                if not isinstance(raw_row, dict):
                    continue
                batter_id = _safe_int(raw_row.get("batter_id"))
                if batter_id is None:
                    batter_id = _safe_int(batter_key)
                hitter_name = _first_text(raw_row.get("name"))
                team = _first_text(raw_row.get("team"))
                if batter_id is None or not hitter_name or not team:
                    continue
                ladder_rows, row_sim_count, min_total, max_total = _dist_to_ladder_rows(raw_row.get(str(prop_cfg.get("dist_key"))))
                if not ladder_rows:
                    continue
                side = "away" if team == away_abbr else ("home" if team == home_abbr else "")
                opponent = home_abbr if side == "away" else (away_abbr if side == "home" else "")
                team_id = away_team_id if side == "away" else (home_team_id if side == "home" else None)
                opponent_team_id = home_team_id if side == "away" else (away_team_id if side == "home" else None)
                player_market_lines = market_lines.get(normalize_pitcher_name(hitter_name)) or {}
                market = {}
                market_key = prop_cfg.get("market_key")
                if market_key:
                    market = (player_market_lines.get(str(market_key)) or {})
                market_line = _safe_float(market.get("line")) if isinstance(market, dict) else None
                over_line_count = None
                over_line_prob = None
                if market_line is not None:
                    over_line_count = int(sum(row.get("exactCount") or 0 for row in ladder_rows if float(row.get("total") or 0) > float(market_line)))
                    over_line_prob = float(over_line_count / float(max(1, row_sim_count)))
                mode_row = max(
                    ladder_rows,
                    key=lambda row: (int(row.get("exactCount") or 0), -int(row.get("total") or 0)),
                )
                rows.append(
                    {
                        "gamePk": int(game_pk) if game_pk is not None else None,
                        "batterId": int(batter_id),
                        "hitterId": int(batter_id),
                        "hitterName": hitter_name,
                        "playerName": hitter_name,
                        "headshotUrl": _mlb_headshot_url(int(batter_id)),
                        "team": team,
                        "teamId": int(team_id) if team_id is not None else None,
                        "teamLogoUrl": (_mlb_logo_url(int(team_id)) if team_id is not None else None),
                        "opponent": opponent,
                        "opponentTeamId": int(opponent_team_id) if opponent_team_id is not None else None,
                        "opponentLogoUrl": (_mlb_logo_url(int(opponent_team_id)) if opponent_team_id is not None else None),
                        "side": side,
                        "matchup": f"{team} @ {opponent}" if side == "away" else (f"{opponent} @ {team}" if side == "home" else team),
                        "mean": _safe_float(raw_row.get(str(prop_cfg.get("mean_key")))),
                        "paMean": _safe_float(raw_row.get("pa_mean")),
                        "abMean": _safe_float(raw_row.get("ab_mean")),
                        "lineupOrder": _safe_int(raw_row.get("lineup_order")),
                        "isLineupBatter": bool(raw_row.get("is_lineup_batter")),
                        "mode": int(mode_row.get("total") or 0),
                        "modeCount": int(mode_row.get("exactCount") or 0),
                        "modeProb": float(mode_row.get("exactProb") or 0.0),
                        "simCount": int(row_sim_count),
                        "minTotal": min_total,
                        "maxTotal": max_total,
                        "marketLine": market_line,
                        "marketLinesByStat": _hitter_market_lines_by_stat(player_market_lines),
                        "overLineCount": over_line_count,
                        "overLineProb": over_line_prob,
                        "ladder": ladder_rows,
                        "ladderShape": "exact",
                        "sourceFile": _relative_path_str(sim_path),
                    }
                )
                exact_rows_added = True

        if exact_rows_added:
            continue

        if prop == "home_runs":
            if not isinstance(hitter_hr_topn, dict):
                continue
            raw_rows = hitter_hr_topn.get("overall") or []
            if not isinstance(raw_rows, list):
                continue
            for raw_row in raw_rows:
                if not isinstance(raw_row, dict):
                    continue
                batter_id = _safe_int(raw_row.get("batter_id"))
                hitter_name = _first_text(raw_row.get("name"))
                if batter_id is None or not hitter_name:
                    continue
                team = _first_text(raw_row.get("team"))
                side = "away" if team == away_abbr else ("home" if team == home_abbr else "")
                opponent = home_abbr if side == "away" else (away_abbr if side == "home" else "")
                team_id = away_team_id if side == "away" else (home_team_id if side == "home" else None)
                opponent_team_id = home_team_id if side == "away" else (away_team_id if side == "home" else None)
                player_market_lines = market_lines.get(normalize_pitcher_name(hitter_name)) or {}
                market = (player_market_lines.get("batter_home_runs") or {})
                market_line = _safe_float(market.get("line")) if isinstance(market, dict) else None
                hit_prob = float(_safe_float(raw_row.get("p_hr_1plus")) or 0.0)
                hit_count = _threshold_prob_to_count(hit_prob, sim_count)
                over_line_count = hit_count if market_line is not None and float(market_line) < 1.0 else None
                over_line_prob = hit_prob if over_line_count is not None else None
                rows.append(
                    {
                        "gamePk": int(game_pk) if game_pk is not None else None,
                        "batterId": int(batter_id),
                        "hitterId": int(batter_id),
                        "hitterName": hitter_name,
                        "playerName": hitter_name,
                        "headshotUrl": _mlb_headshot_url(int(batter_id)),
                        "team": team,
                        "teamId": int(team_id) if team_id is not None else None,
                        "teamLogoUrl": (_mlb_logo_url(int(team_id)) if team_id is not None else None),
                        "opponent": opponent,
                        "opponentTeamId": int(opponent_team_id) if opponent_team_id is not None else None,
                        "opponentLogoUrl": (_mlb_logo_url(int(opponent_team_id)) if opponent_team_id is not None else None),
                        "side": side,
                        "matchup": f"{team} @ {opponent}" if side == "away" else (f"{opponent} @ {team}" if side == "home" else team),
                        "mean": _safe_float(raw_row.get("hr_mean")),
                        "paMean": _safe_float(raw_row.get("pa_mean")),
                        "abMean": _safe_float(raw_row.get("ab_mean")),
                        "lineupOrder": _safe_int(raw_row.get("lineup_order")),
                        "isLineupBatter": bool(raw_row.get("is_lineup_batter")),
                        "mode": None,
                        "modeCount": None,
                        "modeProb": None,
                        "simCount": int(sim_count),
                        "marketLine": market_line,
                        "marketLinesByStat": _hitter_market_lines_by_stat(player_market_lines),
                        "overLineCount": over_line_count,
                        "overLineProb": over_line_prob,
                        "ladder": [{"total": 1, "hitCount": hit_count, "hitProb": hit_prob}],
                        "ladderShape": "threshold",
                        "sourceFile": _relative_path_str(sim_path),
                    }
                )
            continue

        if not isinstance(hitter_topn, dict):
            continue

        batter_rows: Dict[int, Dict[str, Any]] = {}
        for threshold_spec in threshold_specs:
            total = _safe_int(threshold_spec.get("total"))
            section_key = str(threshold_spec.get("section_key") or "")
            prob_key = str(threshold_spec.get("prob_key") or "")
            if total is None or not section_key or not prob_key:
                continue
            raw_rows = hitter_topn.get(section_key) or []
            if not isinstance(raw_rows, list):
                continue
            for raw_row in raw_rows:
                if not isinstance(raw_row, dict):
                    continue
                batter_id = _safe_int(raw_row.get("batter_id"))
                hitter_name = _first_text(raw_row.get("name"))
                if batter_id is None or not hitter_name:
                    continue
                team = _first_text(raw_row.get("team"))
                side = "away" if team == away_abbr else ("home" if team == home_abbr else "")
                opponent = home_abbr if side == "away" else (away_abbr if side == "home" else "")
                team_id = away_team_id if side == "away" else (home_team_id if side == "home" else None)
                opponent_team_id = home_team_id if side == "away" else (away_team_id if side == "home" else None)
                entry = batter_rows.setdefault(
                    int(batter_id),
                    {
                        "gamePk": int(game_pk) if game_pk is not None else None,
                        "batterId": int(batter_id),
                        "hitterId": int(batter_id),
                        "hitterName": hitter_name,
                        "playerName": hitter_name,
                        "headshotUrl": _mlb_headshot_url(int(batter_id)),
                        "team": team,
                        "teamId": int(team_id) if team_id is not None else None,
                        "teamLogoUrl": (_mlb_logo_url(int(team_id)) if team_id is not None else None),
                        "opponent": opponent,
                        "opponentTeamId": int(opponent_team_id) if opponent_team_id is not None else None,
                        "opponentLogoUrl": (_mlb_logo_url(int(opponent_team_id)) if opponent_team_id is not None else None),
                        "side": side,
                        "matchup": f"{team} @ {opponent}" if side == "away" else (f"{opponent} @ {team}" if side == "home" else team),
                        "mean": _safe_float(raw_row.get(str(prop_cfg.get("mean_key")))),
                        "paMean": _safe_float(raw_row.get("pa_mean")),
                        "abMean": _safe_float(raw_row.get("ab_mean")),
                        "lineupOrder": _safe_int(raw_row.get("lineup_order")),
                        "isLineupBatter": bool(raw_row.get("is_lineup_batter")),
                        "simCount": int(sim_count),
                        "thresholdProbs": {},
                        "sourceFile": _relative_path_str(sim_path),
                    },
                )
                entry["thresholdProbs"][int(total)] = float(_safe_float(raw_row.get(prob_key)) or 0.0)
                if entry.get("mean") is None:
                    entry["mean"] = _safe_float(raw_row.get(str(prop_cfg.get("mean_key"))))
                if entry.get("paMean") is None:
                    entry["paMean"] = _safe_float(raw_row.get("pa_mean"))
                if entry.get("abMean") is None:
                    entry["abMean"] = _safe_float(raw_row.get("ab_mean"))
                if entry.get("lineupOrder") is None:
                    entry["lineupOrder"] = _safe_int(raw_row.get("lineup_order"))

        for batter in batter_rows.values():
            prob_by_total = {
                int(_safe_int(spec.get("total")) or 0): float(((batter.get("thresholdProbs") or {}).get(int(_safe_int(spec.get("total")) or 0), 0.0)) or 0.0)
                for spec in threshold_specs
                if _safe_int(spec.get("total")) is not None
            }
            ladder_rows = _threshold_ladder_rows(prob_by_total, int(batter.get("simCount") or 0))
            if not ladder_rows:
                continue
            player_market_lines = market_lines.get(normalize_pitcher_name(batter.get("hitterName"))) or {}
            market = {}
            market_key = prop_cfg.get("market_key")
            if market_key:
                market = (player_market_lines.get(str(market_key)) or {})
            market_line = _safe_float(market.get("line")) if isinstance(market, dict) else None
            over_line_count = None
            over_line_prob = None
            if market_line is not None:
                required_total = int(float(market_line)) + 1
                line_row = next((row for row in ladder_rows if int(row.get("total") or 0) == required_total), None)
                if line_row is not None:
                    over_line_count = int(line_row.get("hitCount") or 0)
                    over_line_prob = float(line_row.get("hitProb") or 0.0)
            batter["marketLine"] = market_line
            batter["marketLinesByStat"] = _hitter_market_lines_by_stat(player_market_lines)
            batter["overLineCount"] = over_line_count
            batter["overLineProb"] = over_line_prob
            batter["ladder"] = ladder_rows
            batter["ladderShape"] = "threshold"
            rows.append(batter)

    payload["gameOptions"] = [
        {
            "value": str(int(row.get("gamePk") or 0)),
            "label": str(row.get("matchup") or f"Game {int(row.get('gamePk') or 0)}"),
            "gamePk": int(row.get("gamePk") or 0),
            "matchup": str(row.get("matchup") or ""),
        }
        for row in sorted(
            {
                int(row.get("gamePk") or 0): {
                    "gamePk": int(row.get("gamePk") or 0),
                    "matchup": str(row.get("matchup") or ""),
                }
                for row in rows
                if _safe_int(row.get("gamePk")) is not None
            }.values(),
            key=lambda item: (str(item.get("matchup") or ""), int(item.get("gamePk") or 0)),
        )
    ]

    if selected_game:
        rows = [row for row in rows if str(int(row.get("gamePk") or 0)) == selected_game]

    payload["teamOptions"] = [
        {
            "value": str(team_key),
            "label": str(team_key),
            "team": str(team_key),
            "teamId": int(team_data.get("teamId")) if _safe_int(team_data.get("teamId")) is not None else None,
            "teamLogoUrl": team_data.get("teamLogoUrl"),
        }
        for team_key, team_data in sorted(
            {
                str(row.get("team") or "").upper(): {
                    "teamId": row.get("teamId"),
                    "teamLogoUrl": row.get("teamLogoUrl"),
                }
                for row in rows
                if str(row.get("team") or "").strip()
            }.items(),
            key=lambda item: item[0],
        )
    ]

    if selected_team:
        rows = [row for row in rows if str(row.get("team") or "").strip().upper() == selected_team]

    rows = _sort_hitter_ladder_rows(rows, sort_key)
    payload["hitterOptions"] = [
        {
            "value": str(int(row.get("hitterId") or 0)),
            "label": f"{row.get('hitterName') or 'Unknown'} ({row.get('team') or '-'} vs {row.get('opponent') or '-'})",
            "hitterId": int(row.get("hitterId") or 0),
            "hitterName": str(row.get("hitterName") or ""),
            "headshotUrl": row.get("headshotUrl"),
            "teamLogoUrl": row.get("teamLogoUrl"),
            "opponentLogoUrl": row.get("opponentLogoUrl"),
        }
        for row in rows
    ]

    if selected_hitter:
        if selected_hitter.isdigit():
            rows = [row for row in rows if str(int(row.get("hitterId") or 0)) == selected_hitter]
        else:
            target_name = normalize_pitcher_name(selected_hitter)
            rows = [row for row in rows if normalize_pitcher_name(row.get("hitterName")) == target_name]

    if not rows:
        payload["error"] = "hitter_ladders_missing"
        if selected_game:
            payload["error"] = "hitter_ladders_game_missing"
        if selected_team:
            payload["error"] = "hitter_ladders_team_missing"
        if selected_hitter:
            payload["error"] = "hitter_ladders_hitter_missing"
        payload["summary"] = {
            "games": int(len(sim_files)),
            "hitters": 0,
            "simCounts": [],
            "availableGames": int(len(payload.get("gameOptions") or [])),
            "availableTeams": int(len(payload.get("teamOptions") or [])),
            "availableHitters": int(len(payload.get("hitterOptions") or [])),
            "topN": int(max(topn_limits)) if topn_limits else None,
        }
        return payload

    payload["found"] = True
    payload["rows"] = rows
    payload["ladderShape"] = "exact" if any(str(row.get("ladderShape") or "") == "exact" for row in rows) else "threshold"
    payload["summary"] = {
        "games": int(len(sim_files)),
        "hitters": int(len(rows)),
        "simCounts": sorted({int(row.get("simCount") or 0) for row in rows if int(row.get("simCount") or 0) > 0}),
        "availableGames": int(len(payload.get("gameOptions") or [])),
        "availableTeams": int(len(payload.get("teamOptions") or [])),
        "availableHitters": int(len(payload.get("hitterOptions") or [])),
        "topN": int(max(topn_limits)) if topn_limits else None,
    }
    return payload


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _recommendations_by_game(locked_policy: Optional[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    if not isinstance(locked_policy, dict):
        return {}

    grouped: Dict[int, Dict[str, Any]] = defaultdict(
        lambda: {
            "totals": None,
            "ml": None,
            "pitcher_props": [],
            "hitter_props": [],
            "extra_pitcher_props": [],
            "extra_hitter_props": [],
        }
    )
    markets = locked_policy.get("markets") or {}
    if not isinstance(markets, dict):
        return {}

    def _append_reco(bucket: Dict[str, Any], market_name: str, reco: Dict[str, Any], *, tier: str) -> None:
        item = dict(reco)
        item["recommendation_tier"] = tier
        if market_name == "totals":
            bucket["totals"] = item
        elif market_name == "ml":
            bucket["ml"] = item
        elif market_name == "pitcher_props":
            bucket["extra_pitcher_props" if tier == "candidate" else "pitcher_props"].append(item)
        else:
            bucket["extra_hitter_props" if tier == "candidate" else "hitter_props"].append(item)

    for market_name, section in markets.items():
        if not isinstance(section, dict):
            continue
        recos = section.get("recommendations") or []
        extra_recos = section.get("other_playable_candidates") or []
        if not isinstance(recos, list):
            continue
        for reco in recos:
            if not isinstance(reco, dict):
                continue
            game_pk = _safe_int(reco.get("game_pk"))
            if not game_pk or int(game_pk) <= 0:
                continue
            bucket = grouped[int(game_pk)]
            _append_reco(bucket, market_name, reco, tier="official")

        if not isinstance(extra_recos, list):
            continue
        for reco in extra_recos:
            if not isinstance(reco, dict):
                continue
            game_pk = _safe_int(reco.get("game_pk"))
            if not game_pk or int(game_pk) <= 0:
                continue
            bucket = grouped[int(game_pk)]
            _append_reco(bucket, market_name, reco, tier="candidate")

    for bucket in grouped.values():
        bucket["pitcher_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
        bucket["hitter_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
        bucket["extra_pitcher_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
        bucket["extra_hitter_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
    return dict(grouped)


def _game_outputs_by_game(game_summary: Optional[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    if not isinstance(game_summary, dict):
        return {}
    outputs = game_summary.get("outputs") or []
    if not isinstance(outputs, list):
        return {}
    out: Dict[int, Dict[str, Any]] = {}
    for row in outputs:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if not game_pk or int(game_pk) <= 0:
            continue
        out[int(game_pk)] = row
    return out


def _normalized_probable_entry(value: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    player_id = _safe_int(value.get("id"))
    full_name = _first_text(value.get("fullName"), value.get("name"))
    if player_id is None and not full_name:
        return None
    return {"id": player_id, "fullName": full_name}


def _season_report_outputs_by_game(day_report: Optional[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    if not isinstance(day_report, dict):
        return {}
    out: Dict[int, Dict[str, Any]] = {}
    for raw_game in day_report.get("games") or []:
        if not isinstance(raw_game, dict):
            continue
        game_pk = _safe_int(raw_game.get("game_pk"))
        if not game_pk or int(game_pk) <= 0:
            continue
        away = raw_game.get("away") or {}
        home = raw_game.get("home") or {}
        starter_names = raw_game.get("starter_names") or {}
        starters = raw_game.get("starters") or {}
        segments = raw_game.get("segments") or {}
        full = dict(segments.get("full") or {})
        actual_full = full.get("actual") or {}
        away_abbr = _first_text(away.get("abbr"), away.get("name"))
        home_abbr = _first_text(home.get("abbr"), home.get("name"))
        away_score = _safe_int(actual_full.get("away"))
        home_score = _safe_int(actual_full.get("home"))
        if away_score is not None and home_score is not None:
            status_detailed = f"Final · {away_abbr} {away_score}, {home_abbr} {home_score}"
        else:
            status_detailed = "Archived final"
        out[int(game_pk)] = {
            "game_pk": int(game_pk),
            "away_id": _safe_int(away.get("id")),
            "home_id": _safe_int(home.get("id")),
            "away_abbr": away_abbr,
            "home_abbr": home_abbr,
            "away": _first_text(away.get("name"), away_abbr),
            "home": _first_text(home.get("name"), home_abbr),
            "full": _normalized_full_game_probs(full),
            "first5": dict(segments.get("first5") or {}),
            "first3": dict(segments.get("first3") or {}),
            "probable": {
                "away": _normalized_probable_entry({"id": _safe_int(starters.get("away")), "fullName": starter_names.get("away")}),
                "home": _normalized_probable_entry({"id": _safe_int(starters.get("home")), "fullName": starter_names.get("home")}),
            },
            "status_abstract": "Final",
            "status_detailed": status_detailed,
        }
    return out


def _cards_list_from_sources(
    *,
    d: str,
    schedule_games: List[Dict[str, Any]],
    outputs_by_game: Dict[int, Dict[str, Any]],
    recos_by_game: Dict[int, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cards_by_game: Dict[int, Dict[str, Any]] = {}

    def _ensure_card(game_pk: int, sort_order: int) -> Dict[str, Any]:
        card = cards_by_game.get(int(game_pk))
        if card is None:
            card = {
                "sortOrder": int(sort_order),
                "gamePk": int(game_pk),
                "gameType": "",
                "gameDate": "",
                "startTime": "",
                "officialDate": d,
                "status": {"abstract": "", "detailed": ""},
                "away": {"id": None, "abbr": "AWAY", "name": "Away", "logo": None},
                "home": {"id": None, "abbr": "HOME", "name": "Home", "logo": None},
                "probable": {"away": None, "home": None},
                "predictions": None,
                "markets": {"totals": None, "ml": None, "pitcherProps": [], "hitterProps": []},
            }
            cards_by_game[int(game_pk)] = card
            return card
        card["sortOrder"] = min(int(card.get("sortOrder") or sort_order), int(sort_order))
        return card

    for idx, g in enumerate(schedule_games):
        game_pk = _safe_int(g.get("gamePk"))
        if not game_pk or int(game_pk) <= 0:
            continue
        card = _ensure_card(int(game_pk), idx)
        away_side = ((g.get("teams") or {}).get("away") or {})
        home_side = ((g.get("teams") or {}).get("home") or {})
        away = _team_from_schedule(away_side)
        home = _team_from_schedule(home_side)
        status = (g.get("status") or {})
        game_date = str(g.get("gameDate") or "")
        card.update(
            {
                "gameType": str(g.get("gameType") or ""),
                "gameDate": game_date,
                "startTime": _format_start_time_local(game_date),
                "officialDate": str(g.get("officialDate") or d),
                "status": {
                    "abstract": str(status.get("abstractGameState") or ""),
                    "detailed": str(status.get("detailedState") or ""),
                },
                "away": {"id": away.id, "abbr": away.abbr, "name": away.name, "logo": _mlb_logo_url(away.id)},
                "home": {"id": home.id, "abbr": home.abbr, "name": home.name, "logo": _mlb_logo_url(home.id)},
                "probable": {
                    "away": _probable_pitcher_from_schedule(away_side),
                    "home": _probable_pitcher_from_schedule(home_side),
                },
            }
        )

    for idx, (game_pk, row) in enumerate(outputs_by_game.items()):
        card = _ensure_card(int(game_pk), 1000 + idx)
        away_abbr = _first_text(row.get("away_abbr"), row.get("away"), card["away"].get("abbr"))
        home_abbr = _first_text(row.get("home_abbr"), row.get("home"), card["home"].get("abbr"))
        if not card["away"].get("name") or card["away"].get("name") == "Away":
            card["away"]["name"] = away_abbr or card["away"]["name"]
        if not card["home"].get("name") or card["home"].get("name") == "Home":
            card["home"]["name"] = home_abbr or card["home"]["name"]
        if away_abbr and card["away"].get("abbr") in (None, "", "AWAY"):
            card["away"]["abbr"] = away_abbr
        if home_abbr and card["home"].get("abbr") in (None, "", "HOME"):
            card["home"]["abbr"] = home_abbr

        away_id = _safe_int(row.get("away_id"))
        home_id = _safe_int(row.get("home_id"))
        if away_id and not card["away"].get("id"):
            card["away"]["id"] = int(away_id)
            card["away"]["logo"] = _mlb_logo_url(int(away_id))
        if home_id and not card["home"].get("id"):
            card["home"]["id"] = int(home_id)
            card["home"]["logo"] = _mlb_logo_url(int(home_id))

        probable = row.get("probable") or {}
        if isinstance(probable, dict):
            if not card["probable"].get("away"):
                card["probable"]["away"] = _normalized_probable_entry(probable.get("away"))
            if not card["probable"].get("home"):
                card["probable"]["home"] = _normalized_probable_entry(probable.get("home"))

        if not card["status"].get("abstract"):
            card["status"]["abstract"] = _first_text(row.get("status_abstract"))
        if not card["status"].get("detailed"):
            card["status"]["detailed"] = _first_text(row.get("status_detailed"))

        if not card["gameType"]:
            card["gameType"] = _first_text(row.get("game_type"), card.get("gameType"))
        if not card["gameDate"]:
            card["gameDate"] = _first_text(row.get("game_date"), row.get("commence_time"))
            card["startTime"] = _format_start_time_local(card["gameDate"])

        card["predictions"] = {
            "full": _normalized_full_game_probs(row.get("full") or {}),
            "first5": row.get("first5") or {},
            "first3": row.get("first3") or {},
        }

    for idx, (game_pk, bucket) in enumerate(recos_by_game.items()):
        card = _ensure_card(int(game_pk), 2000 + idx)
        lead_reco = (
            bucket.get("totals")
            or bucket.get("ml")
            or ((bucket.get("pitcher_props") or [None])[0])
            or ((bucket.get("hitter_props") or [None])[0])
        )
        if isinstance(lead_reco, dict):
            away_name = _first_text(lead_reco.get("away"), card["away"].get("name"), card["away"].get("abbr"))
            home_name = _first_text(lead_reco.get("home"), card["home"].get("name"), card["home"].get("abbr"))
            away_abbr = _first_text(lead_reco.get("away_abbr"), card["away"].get("abbr"), away_name)
            home_abbr = _first_text(lead_reco.get("home_abbr"), card["home"].get("abbr"), home_name)
            card["away"]["name"] = away_name or card["away"]["name"]
            card["home"]["name"] = home_name or card["home"]["name"]
            card["away"]["abbr"] = away_abbr or card["away"].get("abbr")
            card["home"]["abbr"] = home_abbr or card["home"].get("abbr")
            if not card["gameDate"]:
                card["gameDate"] = str(lead_reco.get("commence_time") or "")
                card["startTime"] = _format_start_time_local(card["gameDate"])
        card["markets"] = {
            "totals": bucket.get("totals"),
            "ml": bucket.get("ml"),
            "pitcherProps": bucket.get("pitcher_props") or [],
            "hitterProps": bucket.get("hitter_props") or [],
            "extraPitcherProps": bucket.get("extra_pitcher_props") or [],
            "extraHitterProps": bucket.get("extra_hitter_props") or [],
        }

    cards = sorted(cards_by_game.values(), key=lambda row: (int(row.get("sortOrder") or 999999), int(row.get("gamePk") or 0)))
    for card in cards:
        card.pop("sortOrder", None)
        card["flags"] = {
            "hasAnyRecommendations": bool(
                card["markets"].get("totals")
                or card["markets"].get("ml")
                or card["markets"].get("pitcherProps")
                or card["markets"].get("hitterProps")
            ),
            "hasPitcherProps": bool(card["markets"].get("pitcherProps")),
            "hasHitterProps": bool(card["markets"].get("hitterProps")),
        }
    return cards


def _season_report_game(day_report: Optional[Dict[str, Any]], game_pk: int) -> Optional[Dict[str, Any]]:
    if not isinstance(day_report, dict):
        return None
    target = int(game_pk)
    for raw_game in day_report.get("games") or []:
        if not isinstance(raw_game, dict):
            continue
        current_pk = _safe_int(raw_game.get("game_pk"))
        if current_pk and int(current_pk) == target:
            return raw_game
    return None


def _report_player_meta(report_game: Optional[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    if not isinstance(report_game, dict):
        return {}

    meta: Dict[int, Dict[str, Any]] = {}

    def _ensure(pid: Any, *, side: Optional[str] = None, order: Optional[int] = None, name: str = "") -> None:
        player_id = _safe_int(pid)
        if not player_id or int(player_id) <= 0:
            return
        row = meta.setdefault(int(player_id), {"id": int(player_id)})
        if side in ("away", "home") and not row.get("side"):
            row["side"] = side
        if order is not None and row.get("order") is None:
            row["order"] = int(order)
        if name and not row.get("name"):
            row["name"] = str(name)

    starters = report_game.get("starters") or {}
    starter_names = report_game.get("starter_names") or {}
    for side in ("away", "home"):
        _ensure(starters.get(side), side=side, name=_first_text(starter_names.get(side)))

    for side in ("away", "home"):
        confirmed = list(((report_game.get("confirmed_lineup_ids") or {}).get(side) or []))
        projected = list(((report_game.get("projected_lineup_ids") or {}).get(side) or []))
        lineup_ids = confirmed or projected
        for idx, player_id in enumerate(lineup_ids, start=1):
            _ensure(player_id, side=side, order=idx)

    hitter_props = report_game.get("hitter_props_likelihood") or {}
    if isinstance(hitter_props, dict):
        for rows in hitter_props.values():
            if not isinstance(rows, list):
                continue
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                _ensure(raw.get("batter_id"), name=_first_text(raw.get("name")))
    return meta


def _report_game_to_sim_obj(report_game: Dict[str, Any], sim_count: Optional[int]) -> Dict[str, Any]:
    full = ((report_game.get("segments") or {}).get("full") or {})
    mean_total = _safe_float(full.get("mean_total_runs"))
    margin = _safe_float(full.get("mean_run_margin_home_minus_away"))
    away_runs_mean: Optional[float] = None
    home_runs_mean: Optional[float] = None
    if mean_total is not None and margin is not None:
        home_runs_mean = round((float(mean_total) + float(margin)) / 2.0, 3)
        away_runs_mean = round(float(mean_total) - float(home_runs_mean), 3)

    pitcher_props_out: Dict[str, Dict[str, Optional[float]]] = {}
    pitcher_props = report_game.get("pitcher_props") or {}
    if isinstance(pitcher_props, dict):
        for side in ("away", "home"):
            side_row = pitcher_props.get(side) or {}
            if not isinstance(side_row, dict):
                continue
            starter_id = _safe_int(side_row.get("starter_id")) or _safe_int(((side_row.get("actual") or {}).get("pitcher_id")))
            pred = side_row.get("pred") or {}
            if not starter_id or not isinstance(pred, dict):
                continue
            pitcher_props_out[str(int(starter_id))] = {
                "outs_mean": _safe_float(pred.get("outs_mean")),
                "so_mean": _safe_float(pred.get("so_mean")),
                "pitches_mean": _safe_float(pred.get("pitches_mean")),
            }

    return {
        "game_pk": _safe_int(report_game.get("game_pk")),
        "away": report_game.get("away") or {},
        "home": report_game.get("home") or {},
        "starters": report_game.get("starters") or {},
        "starter_names": report_game.get("starter_names") or {},
        "sim": {
            "sims": _safe_int(sim_count),
            "segments": {
                "full": {
                    "away_runs_mean": away_runs_mean,
                    "home_runs_mean": home_runs_mean,
                }
            },
            "pitcher_props": pitcher_props_out,
            "hitter_props_likelihood_topn": report_game.get("hitter_props_likelihood") or {},
            "hitter_hr_likelihood_topn": {"overall": []},
        },
    }


def _default_cards_date() -> str:
    today = _today_iso()
    nav = _cards_nav_from_schedule(today)
    min_date = str(nav.get("minDate") or "").strip()
    max_date = str(nav.get("maxDate") or "").strip()
    if min_date and today < min_date:
        try:
            days_until = (date.fromisoformat(min_date) - date.fromisoformat(today)).days
        except Exception:
            days_until = None
        if days_until is not None and 0 <= days_until <= _CARDS_PRESEASON_DEFAULT_WINDOW_DAYS:
            return min_date
    elif max_date and today <= max_date:
        return today

    artifacts = _load_cards_artifacts(_DEMO_DATE)
    if artifacts.get("locked_policy") or artifacts.get("game_summary"):
        return _DEMO_DATE
    return today


def _find_season_manifest_path(season: int) -> Optional[Path]:
    season_dir = _ROOT_DIR / "data" / "eval" / "seasons" / str(int(season))
    candidates = [
        season_dir / "season_eval_manifest.json",
        season_dir / "manifest.json",
    ]
    for path in candidates:
        if path.exists() and path.is_file():
            return path
    if not season_dir.exists() or not season_dir.is_dir():
        return None
    try:
        extra = sorted(
            [path for path in season_dir.glob("*.json") if path.is_file()],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return None
    return extra[0] if extra else None


def _load_season_manifest(season: int) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    path = _find_season_manifest_path(int(season))
    return path, _load_json_file(path)


def _season_from_date_str(date_str: str) -> Optional[int]:
    text = str(date_str or "").strip()
    if len(text) < 4:
        return None
    return _safe_int(text[:4])


def _shift_iso_date_str(date_str: str, days: int) -> Optional[str]:
    text = str(date_str or "").strip()
    if not text:
        return None
    try:
        shifted = date.fromisoformat(text) + timedelta(days=int(days))
    except Exception:
        return None
    return shifted.isoformat()


def _is_historical_date(date_str: str) -> bool:
    text = str(date_str or "").strip()
    if not text:
        return False
    try:
        return date.fromisoformat(text) < _local_today()
    except Exception:
        return False


def _cards_nav_from_season_manifest(date_str: str, season_manifest: Dict[str, Any]) -> Dict[str, Any]:
    overview = season_manifest.get("overview") or {}
    min_date = str(overview.get("first_date") or "").strip() or None
    max_date = str(overview.get("last_date") or "").strip() or None
    season = _safe_int((season_manifest.get("meta") or {}).get("season"))
    current = str(date_str or "").strip()

    prev_date = _shift_iso_date_str(current, -1)
    next_date = _shift_iso_date_str(current, 1)
    if min_date and current and current < min_date:
        prev_date = None
        next_date = min_date
    elif max_date and current and current > max_date:
        prev_date = max_date
        next_date = None
    else:
        if prev_date and min_date and prev_date < min_date:
            prev_date = None
        if next_date and max_date and next_date > max_date:
            next_date = None

    return {
        "season": season,
        "minDate": min_date,
        "maxDate": max_date,
        "prevDate": prev_date,
        "nextDate": next_date,
    }


def _regular_season_schedule_dates(season: int) -> Tuple[str, ...]:
    try:
        buckets = fetch_schedule_date_buckets(_client(), int(season), game_type="R") or []
    except Exception:
        return tuple()

    out: List[str] = []
    seen: set[str] = set()
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        bucket_date = str(bucket.get("date") or "").strip()
        if not bucket_date or bucket_date in seen:
            continue
        games = bucket.get("games") or []
        if not isinstance(games, list) or not games:
            continue
        seen.add(bucket_date)
        out.append(bucket_date)
    return tuple(sorted(out))


def _cards_nav_from_schedule(date_str: str) -> Dict[str, Any]:
    season = _season_from_date_str(date_str)
    if not season:
        return {}

    schedule_dates = _regular_season_schedule_dates(int(season))
    if not schedule_dates:
        return {}

    current = str(date_str or "").strip()
    min_date = schedule_dates[0]
    max_date = schedule_dates[-1]
    idx = bisect_left(schedule_dates, current)

    if current and current < min_date:
        prev_date = None
        next_date = min_date
    elif current and current > max_date:
        prev_date = max_date
        next_date = None
    elif idx < len(schedule_dates) and schedule_dates[idx] == current:
        prev_date = schedule_dates[idx - 1] if idx > 0 else None
        next_date = schedule_dates[idx + 1] if (idx + 1) < len(schedule_dates) else None
    else:
        prev_date = schedule_dates[idx - 1] if idx > 0 else None
        next_date = schedule_dates[idx] if idx < len(schedule_dates) else None

    return {
        "season": season,
        "minDate": min_date,
        "maxDate": max_date,
        "prevDate": prev_date,
        "nextDate": next_date,
    }


def _raw_feed_live_path(game_pk: int, date_str: str) -> Optional[Path]:
    season = _season_from_date_str(date_str)
    if not season:
        return None
    day_dir = _ROOT_DIR / "data" / "raw" / "statsapi" / "feed_live" / str(int(season)) / str(date_str)
    for suffix in (".json.gz", ".json"):
        candidate = day_dir / f"{int(game_pk)}{suffix}"
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _load_game_feed_for_date(game_pk: int, date_str: str) -> Optional[Dict[str, Any]]:
    return _load_json_or_gz_file(_raw_feed_live_path(int(game_pk), str(date_str or "")))


def _load_cards_archive_context(date_str: str) -> Dict[str, Any]:
    season = _season_from_date_str(date_str)
    out: Dict[str, Any] = {
        "season": season,
        "manifest_path": None,
        "manifest": None,
        "report_path": None,
        "report": None,
        "betting_manifest_path": None,
        "profile": None,
        "available_profiles": {},
        "card_path": None,
        "card": None,
        "nav": {},
    }
    if not season:
        return out

    manifest_path, manifest = _load_season_manifest(int(season))
    out["manifest_path"] = manifest_path
    out["manifest"] = manifest
    if not manifest_path or not isinstance(manifest, dict):
        return out

    out["nav"] = _cards_nav_from_season_manifest(str(date_str), manifest)
    report_path = _resolve_season_day_report_path(manifest, str(date_str))
    out["report_path"] = report_path
    out["report"] = _load_json_file(report_path)

    profile_name, betting_manifest_path, betting_manifest, available_profiles = _load_season_betting_manifest(int(season), "")
    out["profile"] = profile_name
    out["betting_manifest_path"] = betting_manifest_path
    out["available_profiles"] = available_profiles
    if betting_manifest_path and isinstance(betting_manifest, dict):
        card_path = _resolve_season_betting_day_card_path(betting_manifest, str(date_str))
        out["card_path"] = card_path
        out["card"] = _load_json_file(card_path)
    return out


def _season_betting_manifest_candidates(season: int) -> Dict[str, List[Path]]:
    season_dir = _ROOT_DIR / "data" / "eval" / "seasons" / str(int(season))
    return {
        "baseline": [season_dir / "season_betting_cards_manifest.json"],
        "retuned": [season_dir / "season_betting_cards_retuned_manifest.json"],
    }


def _available_season_betting_profiles(season: int) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for profile_name, candidates in _season_betting_manifest_candidates(int(season)).items():
        for path in candidates:
            if path.exists() and path.is_file():
                out[str(profile_name)] = _relative_path_str(path) or str(path)
                break
    return out


def _load_season_betting_manifest(
    season: int,
    requested_profile: str,
) -> Tuple[str, Optional[Path], Optional[Dict[str, Any]], Dict[str, str]]:
    available = _available_season_betting_profiles(int(season))
    requested = str(requested_profile or "").strip().lower()
    if requested in ("baseline", "retuned"):
        selected_profile = requested
    elif requested in ("", "default", "current", "live"):
        selected_profile = "retuned" if "retuned" in available else "baseline"
    else:
        selected_profile = requested or ("retuned" if "retuned" in available else "baseline")

    manifest_rel = available.get(selected_profile)
    manifest_path = _path_from_maybe_relative(manifest_rel)
    return selected_profile, manifest_path, _load_json_file(manifest_path), available


def _resolve_season_betting_day_card_path(betting_manifest: Dict[str, Any], date_str: str) -> Optional[Path]:
    for row in betting_manifest.get("days") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("date") or "").strip() != str(date_str or "").strip():
            continue
        candidate = _path_from_maybe_relative(row.get("card_path"))
        if candidate and candidate.exists() and candidate.is_file():
            return candidate
    return None


def _resolve_season_day_report_path(season_manifest: Dict[str, Any], date_str: str) -> Optional[Path]:
    for row in season_manifest.get("days") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("date") or "").strip() != str(date_str or "").strip():
            continue
        candidate = _path_from_maybe_relative(row.get("report_path"))
        if candidate and candidate.exists() and candidate.is_file():
            return candidate

    batch_dir = _path_from_maybe_relative(((season_manifest.get("meta") or {}).get("batch_dir")))
    if batch_dir and batch_dir.exists() and batch_dir.is_dir():
        candidate = batch_dir / f"sim_vs_actual_{date_str}.json"
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _season_day_cards_link(season_manifest: Dict[str, Any], date_str: str) -> Optional[str]:
    for row in season_manifest.get("days") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("date") or "").strip() != str(date_str or "").strip():
            continue
        cards_url = str(row.get("cards_url") or "").strip()
        return cards_url or None
    return None


_SETTLED_HITTER_MARKETS = {
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
}

_BETTING_COUNT_KEYS = (
    "totals",
    "ml",
    "pitcher_props",
    "hitter_props",
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
    "combined",
)


def _settled_rows_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    stake_u = sum(float(row.get("stake_u") or 0.0) for row in rows)
    profit_u = sum(float(row.get("profit_u") or 0.0) for row in rows)
    wins = sum(1 for row in rows if row.get("result") == "win")
    count = len(rows)
    return {
        "n": int(count),
        "wins": int(wins),
        "losses": int(count - wins),
        "stake_u": round(float(stake_u), 4),
        "profit_u": round(float(profit_u), 4),
        "roi": (round(float(profit_u) / float(stake_u), 4) if float(stake_u) > 0 else None),
    }


def _settled_results_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_market: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    hitter_rows: List[Dict[str, Any]] = []
    for row in rows:
        market = str(row.get("market") or "")
        if not market:
            continue
        by_market[market].append(row)
        if market in _SETTLED_HITTER_MARKETS:
            hitter_rows.append(row)

    results = {market: _settled_rows_summary(market_rows) for market, market_rows in sorted(by_market.items())}
    if hitter_rows:
        results["hitter_props"] = _settled_rows_summary(hitter_rows)
    results["combined"] = _settled_rows_summary(rows)
    return results


def _betting_selected_counts_with_defaults(counts: Any) -> Dict[str, int]:
    out: Dict[str, int] = {key: 0 for key in _BETTING_COUNT_KEYS}
    if isinstance(counts, dict):
        for key in out:
            out[key] = int(counts.get(key) or 0)
    if out["hitter_props"] <= 0:
        out["hitter_props"] = int(
            out["hitter_home_runs"]
            + out["hitter_hits"]
            + out["hitter_total_bases"]
            + out["hitter_runs"]
            + out["hitter_rbis"]
        )
    if out["combined"] <= 0:
        out["combined"] = int(out["totals"] + out["ml"] + out["pitcher_props"] + out["hitter_props"])
    return out


def _settlement_player_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _settlement_line_key(value: Any) -> Optional[float]:
    line = _safe_float(value)
    if line is None:
        return None
    return round(float(line), 4)


def _settlement_lookup_key(item: Dict[str, Any]) -> Tuple[Optional[int], str, str, Optional[float], str]:
    market = str(item.get("market") or "").strip().lower()
    line_key = _settlement_line_key(item.get("market_line"))
    player_key = _settlement_player_key(item.get("player_name") or item.get("pitcher_name"))
    if market == "ml":
        line_key = None
        player_key = ""
    elif market == "totals":
        player_key = ""
    return (
        _safe_int(item.get("game_pk")),
        market,
        str(item.get("selection") or "").strip().lower(),
        line_key,
        player_key,
    )


def _annotated_reco(
    reco: Dict[str, Any],
    settled_lookup: Dict[Tuple[Optional[int], str, str, Optional[float], str], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    item = dict(reco)
    matches = settled_lookup.get(_settlement_lookup_key(item)) or []
    if matches:
        item["settlement"] = dict(matches.pop(0))
    return item


def _empty_game_betting() -> Dict[str, Any]:
    return {
        "markets": {
            "totals": None,
            "ml": None,
            "pitcherProps": [],
            "hitterProps": [],
            "extraPitcherProps": [],
            "extraHitterProps": [],
        },
        "results": {"combined": _settled_rows_summary([])},
        "playable_results": {"combined": _settled_rows_summary([])},
        "all_results": {"combined": _settled_rows_summary([])},
        "settled_rows": [],
        "playable_settled_rows": [],
        "all_settled_rows": [],
        "unresolved_rows": [],
        "playable_unresolved_rows": [],
        "all_unresolved_rows": [],
        "counts": {
            "official": 0,
            "playable": 0,
            "pitcher": 0,
            "hitter": 0,
            "extra_pitcher": 0,
            "extra_hitter": 0,
        },
        "flags": {
            "hasAnyRecommendations": False,
            "hasOfficialRecommendations": False,
            "hasPlayableCandidates": False,
        },
    }


def _season_betting_games_payload(card_obj: Dict[str, Any], settled_card: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    recos_by_game = _recommendations_by_game(card_obj)
    settled_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    playable_settled_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    unresolved_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    playable_unresolved_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    settled_lookup: Dict[Tuple[Optional[int], str, str, Optional[float], str], List[Dict[str, Any]]] = defaultdict(list)
    playable_lookup: Dict[Tuple[Optional[int], str, str, Optional[float], str], List[Dict[str, Any]]] = defaultdict(list)

    for row in settled_card.get("_settled_rows") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            settled_rows_by_game[int(game_pk)].append(dict(row))
        settled_lookup[_settlement_lookup_key(row)].append(dict(row))

    for row in settled_card.get("_playable_settled_rows") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            playable_settled_rows_by_game[int(game_pk)].append(dict(row))
        playable_lookup[_settlement_lookup_key(row)].append(dict(row))

    for row in settled_card.get("unresolved_recommendations") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            unresolved_rows_by_game[int(game_pk)].append(dict(row))

    for row in settled_card.get("playable_unresolved_recommendations") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            playable_unresolved_rows_by_game[int(game_pk)].append(dict(row))

    out: Dict[int, Dict[str, Any]] = {}
    for game_pk, bucket in recos_by_game.items():
        totals = bucket.get("totals")
        ml = bucket.get("ml")
        totals_item = _annotated_reco(totals, settled_lookup) if isinstance(totals, dict) else None
        ml_item = _annotated_reco(ml, settled_lookup) if isinstance(ml, dict) else None
        pitcher_props = [_annotated_reco(row, settled_lookup) for row in (bucket.get("pitcher_props") or [])]
        hitter_props = [_annotated_reco(row, settled_lookup) for row in (bucket.get("hitter_props") or [])]
        extra_pitcher_props = [_annotated_reco(row, playable_lookup) for row in (bucket.get("extra_pitcher_props") or [])]
        extra_hitter_props = [_annotated_reco(row, playable_lookup) for row in (bucket.get("extra_hitter_props") or [])]
        settled_rows = list(settled_rows_by_game.get(int(game_pk), []))
        playable_settled_rows = list(playable_settled_rows_by_game.get(int(game_pk), []))
        all_settled_rows = list(settled_rows) + list(playable_settled_rows)
        unresolved_rows = list(unresolved_rows_by_game.get(int(game_pk), []))
        playable_unresolved_rows = list(playable_unresolved_rows_by_game.get(int(game_pk), []))
        all_unresolved_rows = list(unresolved_rows) + list(playable_unresolved_rows)
        official_count = int(bool(totals_item)) + int(bool(ml_item)) + len(pitcher_props) + len(hitter_props)
        playable_count = len(extra_pitcher_props) + len(extra_hitter_props)

        out[int(game_pk)] = {
            "markets": {
                "totals": totals_item,
                "ml": ml_item,
                "pitcherProps": pitcher_props,
                "hitterProps": hitter_props,
                "extraPitcherProps": extra_pitcher_props,
                "extraHitterProps": extra_hitter_props,
            },
            "results": _settled_results_from_rows(settled_rows),
            "playable_results": _settled_results_from_rows(playable_settled_rows),
            "all_results": _settled_results_from_rows(all_settled_rows),
            "settled_rows": settled_rows,
            "playable_settled_rows": playable_settled_rows,
            "all_settled_rows": all_settled_rows,
            "unresolved_rows": unresolved_rows,
            "playable_unresolved_rows": playable_unresolved_rows,
            "all_unresolved_rows": all_unresolved_rows,
            "counts": {
                "official": int(official_count),
                "playable": int(playable_count),
                "pitcher": int(len(pitcher_props)),
                "hitter": int(len(hitter_props)),
                "extra_pitcher": int(len(extra_pitcher_props)),
                "extra_hitter": int(len(extra_hitter_props)),
            },
            "flags": {
                "hasAnyRecommendations": bool(official_count or playable_count),
                "hasOfficialRecommendations": bool(official_count),
                "hasPlayableCandidates": bool(playable_count),
            },
        }
    return out


def _season_betting_day_payload(season: int, date_str: str, requested_profile: str) -> Dict[str, Any]:
    profile_name, manifest_path, manifest, available_profiles = _load_season_betting_manifest(
        int(season),
        requested_profile,
    )
    payload: Dict[str, Any] = {
        "season": int(season),
        "date": str(date_str),
        "profile": profile_name,
        "available_profiles": available_profiles,
        "found": False,
        "games": {},
    }

    daily_artifacts = _load_cards_artifacts(str(date_str))
    canonical_card_path = daily_artifacts.get("locked_policy_path")
    canonical_card_obj = daily_artifacts.get("locked_policy")

    def _finalize_from_card(
        *,
        card_path: Optional[Path],
        card_obj: Optional[Dict[str, Any]],
        summary: Optional[Dict[str, Any]] = None,
        manifest_source: Optional[Path] = None,
        source_kind: str,
    ) -> Dict[str, Any]:
        if not card_path or not isinstance(card_obj, dict):
            return payload
        try:
            settled_card = _settle_card(card_path)
        except Exception as exc:
            if manifest_source is not None:
                payload["manifest_source"] = _relative_path_str(manifest_source)
            payload["card_source"] = _relative_path_str(card_path)
            if isinstance(summary, dict):
                payload["summary"] = summary
            payload["error"] = "season_betting_day_settle_failed"
            payload["detail"] = str(exc)
            return payload

        payload.update(
            {
                "found": True,
                "source_kind": str(source_kind),
                "manifest_source": _relative_path_str(manifest_source) if manifest_source is not None else None,
                "card_source": _relative_path_str(card_path),
                "summary": summary if isinstance(summary, dict) else None,
                "cap_profile": card_obj.get("cap_profile"),
                "selected_counts": _betting_selected_counts_with_defaults(
                    (summary.get("selected_counts") if isinstance(summary, dict) else None)
                    or settled_card.get("selected_counts")
                    or {}
                ),
                "playable_selected_counts": _betting_selected_counts_with_defaults(
                    settled_card.get("playable_selected_counts") or {}
                ),
                "all_selected_counts": _betting_selected_counts_with_defaults(
                    settled_card.get("all_selected_counts") or {}
                ),
                "results": _settled_results_from_rows(list(settled_card.get("_settled_rows") or [])),
                "playable_results": _settled_results_from_rows(list(settled_card.get("_playable_settled_rows") or [])),
                "all_results": _settled_results_from_rows(
                    list(settled_card.get("_all_settled_rows") or [])
                ),
                "games": _season_betting_games_payload(card_obj, settled_card),
            }
        )
        return payload

    if not _is_historical_date(str(date_str)) and canonical_card_path and isinstance(canonical_card_obj, dict):
        return _finalize_from_card(
            card_path=canonical_card_path,
            card_obj=canonical_card_obj,
            source_kind="canonical_daily",
        )

    if not manifest_path or not isinstance(manifest, dict):
        if canonical_card_path and isinstance(canonical_card_obj, dict):
            return _finalize_from_card(
                card_path=canonical_card_path,
                card_obj=canonical_card_obj,
                source_kind="canonical_daily_fallback",
            )
        payload["error"] = "season_betting_cards_missing"
        return payload

    day_row = None
    for row in manifest.get("days") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("date") or "").strip() == str(date_str or "").strip():
            day_row = row
            break
    if not isinstance(day_row, dict):
        if canonical_card_path and isinstance(canonical_card_obj, dict):
            return _finalize_from_card(
                card_path=canonical_card_path,
                card_obj=canonical_card_obj,
                manifest_source=manifest_path,
                source_kind="canonical_daily_fallback",
            )
        payload["manifest_source"] = _relative_path_str(manifest_path)
        payload["error"] = "season_betting_day_missing"
        return payload

    card_path = _resolve_season_betting_day_card_path(manifest, str(date_str))
    card_obj = _load_json_file(card_path)
    if not card_path or not isinstance(card_obj, dict):
        if canonical_card_path and isinstance(canonical_card_obj, dict):
            return _finalize_from_card(
                card_path=canonical_card_path,
                card_obj=canonical_card_obj,
                summary=day_row,
                manifest_source=manifest_path,
                source_kind="canonical_daily_fallback",
            )
        payload["manifest_source"] = _relative_path_str(manifest_path)
        payload["summary"] = day_row
        payload["error"] = "season_betting_day_card_missing"
        return payload

    return _finalize_from_card(
        card_path=card_path,
        card_obj=card_obj,
        summary=day_row,
        manifest_source=manifest_path,
        source_kind="season_manifest",
    )


def _season_day_payload(
    *,
    season: int,
    season_manifest: Dict[str, Any],
    day_report: Dict[str, Any],
    report_path: Path,
    betting_profile: str,
) -> Dict[str, Any]:
    meta = day_report.get("meta") or {}
    assessment = ((day_report.get("assessment") or {}).get("full_game") or {})
    aggregate = day_report.get("aggregate") or {}
    date_str = str(meta.get("date") or report_path.stem.replace("sim_vs_actual_", "")).strip()
    cards_url = _season_day_cards_link(season_manifest, date_str)
    betting_payload = _season_betting_day_payload(int(season), date_str, betting_profile)
    betting_games = betting_payload.get("games") if isinstance(betting_payload.get("games"), dict) else {}
    cards_by_game_pk: Dict[int, Dict[str, Any]] = {}

    try:
        cards_by_game_pk = {
            int(card.get("gamePk")): dict(card)
            for card in (_load_live_lens_cards(date_str) or [])
            if isinstance(card, dict) and _safe_int(card.get("gamePk"))
        }
    except Exception:
        cards_by_game_pk = {}

    games_out: List[Dict[str, Any]] = []
    for raw_game in day_report.get("games") or []:
        if not isinstance(raw_game, dict):
            continue
        game_pk = _safe_int(raw_game.get("game_pk"))
        game_betting = None
        if betting_payload.get("found"):
            game_betting = dict(betting_games.get(int(game_pk or 0)) or _empty_game_betting())
        card_row = dict(cards_by_game_pk.get(int(game_pk or 0)) or {})
        card_status = card_row.get("status") if isinstance(card_row.get("status"), dict) else {}
        games_out.append(
            {
                "game_pk": game_pk,
                "game_date": card_row.get("gameDate") or raw_game.get("game_date") or raw_game.get("commence_time"),
                "start_time": card_row.get("startTime") or "",
                "official_date": card_row.get("officialDate") or date_str,
                "status": {
                    "abstract": str(card_status.get("abstract") or ""),
                    "detailed": str(card_status.get("detailed") or ""),
                },
                "away": raw_game.get("away") or {},
                "home": raw_game.get("home") or {},
                "starter_names": raw_game.get("starter_names") or {},
                "segments": raw_game.get("segments") or {},
                "pitcher_props": raw_game.get("pitcher_props") or {},
                "betting": game_betting,
            }
        )

    betting_payload.pop("games", None)

    return {
        "season": int(season),
        "date": date_str,
        "cards_available": bool(cards_url),
        "cards_url": cards_url,
        "source_file": _relative_path_str(report_path),
        "meta": {
            "sims_per_game": _safe_int(meta.get("sims_per_game")),
            "season": _safe_int(meta.get("season")),
            "generated_at": meta.get("generated_at"),
            "use_raw": meta.get("use_raw"),
            "jobs": _safe_int(meta.get("jobs")),
            "skipped_games": _safe_int(meta.get("skipped_games")),
        },
        "summary": {
            "aggregate": {
                "full": aggregate.get("full") or {},
                "first5": aggregate.get("first5") or {},
                "first3": aggregate.get("first3") or {},
            },
            "full_game": {
                "totals": assessment.get("totals") or {},
                "moneyline": assessment.get("moneyline") or {},
                "runline_fav_minus_1_5": assessment.get("ats_runline_fav_minus_1_5") or {},
                "pitcher_props_starters": assessment.get("pitcher_props_starters") or {},
                "pitcher_props_at_market_lines": assessment.get("pitcher_props_at_market_lines") or {},
            },
        },
        "betting": betting_payload,
        "games": games_out,
    }


def _find_sim_file(*, game_pk: int, d: str, day_dir: Optional[Path] = None) -> Optional[Path]:
    day_dir = day_dir or (_ROOT_DIR / "data" / "daily" / "sims" / str(d))
    if not day_dir.exists() or not day_dir.is_dir():
        return None

    # Fast path: file name includes pk.
    pk_tag = f"pk{int(game_pk)}"
    for p in day_dir.glob(f"*{pk_tag}*.json"):
        if p.is_file():
            return p

    # Fallback: scan small set of json files for matching game_pk field.
    for p in day_dir.glob("*.json"):
        if not p.is_file():
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if _safe_int(obj.get("game_pk")) == int(game_pk):
            return p
    return None


def _sim_predicted_score(sim_obj: Dict[str, Any]) -> Dict[str, Any]:
    """Return mean predicted final score across all full-game sim samples."""
    try:
        seg = (((sim_obj.get("sim") or {}).get("segments") or {}).get("full") or {})
        away_mean = _safe_float(seg.get("away_runs_mean"))
        home_mean = _safe_float(seg.get("home_runs_mean"))
        if away_mean is not None or home_mean is not None:
            return {
                "away": round(float(away_mean), 1) if away_mean is not None else None,
                "home": round(float(home_mean), 1) if home_mean is not None else None,
            }
        samples = seg.get("samples") or []
        if isinstance(samples, list) and samples:
            away_vals: List[float] = []
            home_vals: List[float] = []
            for raw in samples:
                if not isinstance(raw, dict):
                    continue
                away_value = _safe_float(raw.get("away"))
                home_value = _safe_float(raw.get("home"))
                if away_value is not None:
                    away_vals.append(float(away_value))
                if home_value is not None:
                    home_vals.append(float(home_value))
            if away_vals or home_vals:
                return {
                    "away": round(sum(away_vals) / len(away_vals), 1) if away_vals else None,
                    "home": round(sum(home_vals) / len(home_vals), 1) if home_vals else None,
                }
    except Exception:
        pass
    return {"away": None, "home": None}


def _normalized_full_game_probs(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    home_prob = _safe_float(out.get("home_win_prob"))
    away_prob = _safe_float(out.get("away_win_prob"))
    denom = float((home_prob or 0.0) + (away_prob or 0.0))
    if denom <= 0.0:
        return out
    out["home_win_prob"] = float(home_prob or 0.0) / denom
    out["away_win_prob"] = float(away_prob or 0.0) / denom
    out["tie_prob"] = 0.0
    return out


def _ip_from_outs(outs: int) -> str:
    outs = max(0, int(outs))
    return f"{outs // 3}.{outs % 3}"


def _find_roster_snapshot_for_sim(
    *,
    d: str,
    sim_file: Path,
    sim_obj: Dict[str, Any],
    day_dir: Optional[Path] = None,
) -> Optional[Path]:
    day_dir = day_dir or (_ROOT_DIR / "data" / "daily" / "snapshots" / str(d))
    if not day_dir.exists() or not day_dir.is_dir():
        return None

    # Strongest match: same basename as the sim output, but roster_*
    try:
        roster_name = sim_file.name.replace("sim_", "roster_", 1)
        p = day_dir / roster_name
        if p.exists() and p.is_file():
            return p
    except Exception:
        pass

    # Next: match by away/home abbreviations (+ optional pk tag).
    away_abbr = str((sim_obj.get("away") or {}).get("abbreviation") or "").strip()
    home_abbr = str((sim_obj.get("home") or {}).get("abbreviation") or "").strip()
    if not away_abbr or not home_abbr:
        return None

    pk_tag = f"pk{_safe_int(sim_obj.get('game_pk'))}" if _safe_int(sim_obj.get("game_pk")) else None
    candidates = sorted(day_dir.glob(f"roster_*_{away_abbr}_at_{home_abbr}*.json"))
    if not candidates:
        return None
    if pk_tag:
        for c in candidates:
            if pk_tag in c.name:
                return c
    if len(candidates) == 1:
        return candidates[0]
    # Fall back to the shortest name (usually the non-pk variant).
    candidates.sort(key=lambda x: len(x.name))
    return candidates[0]


def _player_meta_from_roster_snapshot(roster_obj: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    meta: Dict[int, Dict[str, Any]] = {}

    def _add(side: str, obj: Any, *, order: Optional[int] = None) -> None:
        if not isinstance(obj, dict):
            return
        pid = _safe_int(obj.get("id"))
        name = str(obj.get("name") or "").strip()
        if not pid or not name:
            return
        row = meta.setdefault(int(pid), {"id": int(pid)})
        row.setdefault("name", name)
        row.setdefault("side", side)
        pos = str(obj.get("pos") or "").strip()
        if pos and not row.get("pos"):
            row["pos"] = pos
        if order is not None and row.get("order") is None:
            row["order"] = int(order)

    for side in ("away", "home"):
        t = roster_obj.get(side) or {}
        if not isinstance(t, dict):
            continue
        _add(side, t.get("starter"))
        bullpen = t.get("bullpen") or []
        if isinstance(bullpen, list):
            for it in bullpen:
                _add(side, it)
        lineup = t.get("lineup") or []
        if isinstance(lineup, list):
            for idx, it in enumerate(lineup, start=1):
                _add(side, it, order=idx)
        bench = t.get("bench") or []
        if isinstance(bench, list):
            for it in bench:
                _add(side, it)
    return meta


def _decimal_ip_from_outs(outs: Any) -> Optional[float]:
    outs_value = _safe_float(outs)
    if outs_value is None:
        return None
    return round(float(outs_value) / 3.0, 2)


def _sum_row_stat(rows: List[Dict[str, Any]], key: str, *, digits: int = 2) -> Optional[float]:
    total = 0.0
    seen = False
    for row in rows:
        value = _safe_float((row or {}).get(key))
        if value is None:
            continue
        total += float(value)
        seen = True
    return round(total, int(digits)) if seen else None


def _reconcile_aggregate_boxscore_display(boxscore: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "away": {
            "totals": dict(((boxscore.get("away") or {}).get("totals") or {})),
            "batting": [dict(row or {}) for row in (((boxscore.get("away") or {}).get("batting") or []))],
            "pitching": [dict(row or {}) for row in (((boxscore.get("away") or {}).get("pitching") or []))],
        },
        "home": {
            "totals": dict(((boxscore.get("home") or {}).get("totals") or {})),
            "batting": [dict(row or {}) for row in (((boxscore.get("home") or {}).get("batting") or []))],
            "pitching": [dict(row or {}) for row in (((boxscore.get("home") or {}).get("pitching") or []))],
        },
    }

    def _adjust_pitching_side(batting_side: str, key: str) -> None:
        opponent_side = "home" if batting_side == "away" else "away"
        batting_rows = (out.get(batting_side) or {}).get("batting") or []
        pitching_rows = (out.get(opponent_side) or {}).get("pitching") or []
        if not batting_rows or not pitching_rows:
            return

        batting_total = _sum_row_stat(batting_rows, key)
        pitching_total = _sum_row_stat(pitching_rows, key)
        if batting_total is None or pitching_total is None:
            return

        delta = round(float(batting_total) - float(pitching_total), 2)
        if abs(delta) < 0.01 or abs(delta) > 0.05:
            return

        target_idx: Optional[int] = None
        target_value = -1.0
        for idx, row in enumerate(pitching_rows):
            value = _safe_float((row or {}).get(key))
            outs = _safe_float((row or {}).get("OUTS")) or 0.0
            if value is None:
                continue
            if value > target_value or (abs(value - target_value) < 1e-9 and outs > (_safe_float((pitching_rows[target_idx] or {}).get("OUTS")) or 0.0 if target_idx is not None else -1.0)):
                target_idx = idx
                target_value = float(value)
        if target_idx is None:
            return

        current_value = _safe_float((pitching_rows[target_idx] or {}).get(key)) or 0.0
        pitching_rows[target_idx][key] = round(max(0.0, float(current_value) + float(delta)), 2)

    for side in ("away", "home"):
        batting_rows = (out.get(side) or {}).get("batting") or []
        totals = (out.get(side) or {}).get("totals") or {}
        totals["H"] = _sum_row_stat(batting_rows, "H")
        totals["R"] = _sum_row_stat(batting_rows, "R")

    for batting_side in ("away", "home"):
        for key in ("H", "R", "BB", "SO", "HR", "HBP"):
            _adjust_pitching_side(batting_side, key)

    return out


def _sim_boxscore_from_aggregate_means(
    sim_obj: Dict[str, Any],
    *,
    player_meta: Optional[Dict[int, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    sim_data = sim_obj.get("sim") or {}
    aggregate_boxscore = sim_data.get("aggregate_boxscore")
    if isinstance(aggregate_boxscore, dict):
        away_box = aggregate_boxscore.get("away") or {}
        home_box = aggregate_boxscore.get("home") or {}
        if isinstance(away_box, dict) or isinstance(home_box, dict):
            return _reconcile_aggregate_boxscore_display({
                "away": {
                    "totals": (away_box.get("totals") or {}),
                    "batting": (away_box.get("batting") or []),
                    "pitching": (away_box.get("pitching") or []),
                },
                "home": {
                    "totals": (home_box.get("totals") or {}),
                    "batting": (home_box.get("batting") or []),
                    "pitching": (home_box.get("pitching") or []),
                },
            })

    hitter_topn = sim_data.get("hitter_props_likelihood_topn") or {}
    hitter_hr_topn = sim_data.get("hitter_hr_likelihood_topn") or {}
    pitcher_props = sim_data.get("pitcher_props") or {}
    if not isinstance(hitter_topn, dict) and not isinstance(hitter_hr_topn, dict) and not isinstance(pitcher_props, dict):
        return None

    away_team = sim_obj.get("away") or {}
    home_team = sim_obj.get("home") or {}
    starters = sim_obj.get("starters") or {}
    starter_names = sim_obj.get("starter_names") or {}

    away_tokens = {
        str(away_team.get("abbreviation") or "").strip().lower(),
        str(away_team.get("name") or "").strip().lower(),
    }
    home_tokens = {
        str(home_team.get("abbreviation") or "").strip().lower(),
        str(home_team.get("name") or "").strip().lower(),
    }
    away_tokens.discard("")
    home_tokens.discard("")

    def _meta(pid: int) -> Dict[str, Any]:
        if player_meta and int(pid) in player_meta:
            return player_meta[int(pid)] or {}
        return {}

    def _side_from_team(team_value: Any) -> Optional[str]:
        token = str(team_value or "").strip().lower()
        if not token:
            return None
        if token in away_tokens:
            return "away"
        if token in home_tokens:
            return "home"
        return None

    batter_acc: Dict[int, Dict[str, Any]] = {}

    def _ensure_batter(pid: int) -> Dict[str, Any]:
        meta_row = _meta(int(pid))
        return batter_acc.setdefault(
            int(pid),
            {
                "id": int(pid),
                "name": str(meta_row.get("name") or pid),
                "side": meta_row.get("side"),
                "pos": str(meta_row.get("pos") or ""),
                "order": _safe_int(meta_row.get("order")),
                "AB": None,
                "H": None,
                "2B": None,
                "3B": None,
                "HR": None,
                "R": None,
                "RBI": None,
                "TB": None,
                "SB": None,
            },
        )

    def _apply_hitter_row(raw: Dict[str, Any]) -> None:
        pid = _safe_int(raw.get("batter_id"))
        if not pid or int(pid) <= 0:
            return
        row = _ensure_batter(int(pid))
        name = str(raw.get("name") or "").strip()
        if name and (not row.get("name") or str(row.get("name")).isdigit()):
            row["name"] = name
        if row.get("side") not in ("away", "home"):
            side = _side_from_team(raw.get("team"))
            if side:
                row["side"] = side
        for raw_key, out_key in (
            ("ab_mean", "AB"),
            ("h_mean", "H"),
            ("2b_mean", "2B"),
            ("3b_mean", "3B"),
            ("hr_mean", "HR"),
            ("r_mean", "R"),
            ("rbi_mean", "RBI"),
            ("tb_mean", "TB"),
            ("sb_mean", "SB"),
        ):
            value = _round_stat(raw.get(raw_key), 2)
            if value is not None:
                row[out_key] = value

    if isinstance(hitter_topn, dict):
        for prop_key, rows in hitter_topn.items():
            if prop_key == "n" or not isinstance(rows, list):
                continue
            for raw in rows:
                if isinstance(raw, dict):
                    _apply_hitter_row(raw)

    if isinstance(hitter_hr_topn, dict):
        for raw in hitter_hr_topn.get("overall") or []:
            if isinstance(raw, dict):
                _apply_hitter_row(raw)

    bat: Dict[str, List[Dict[str, Any]]] = {"away": [], "home": []}
    for pid, acc in batter_acc.items():
        side = acc.get("side")
        if side not in ("away", "home"):
            continue
        hits = _safe_float(acc.get("H"))
        doubles = _safe_float(acc.get("2B")) or 0.0
        triples = _safe_float(acc.get("3B")) or 0.0
        homers = _safe_float(acc.get("HR"))
        total_bases = _safe_float(acc.get("TB"))
        if homers is None and hits is not None and total_bases is not None:
            derived_hr = (float(total_bases) - float(hits) - float(doubles) - (2.0 * float(triples))) / 3.0
            max_hr = max(0.0, float(hits) - float(doubles) - float(triples))
            homers = min(max(0.0, derived_hr), max_hr)
        if total_bases is None and hits is not None:
            singles = max(0.0, float(hits) - float(doubles) - float(triples) - float(homers or 0.0))
            total_bases = singles + (2.0 * float(doubles)) + (3.0 * float(triples)) + (4.0 * float(homers or 0.0))
        bat[side].append(
            {
                "id": int(pid),
                "name": str(acc.get("name") or pid),
                "pos": str(acc.get("pos") or ""),
                "PA": None,
                "AB": _round_stat(acc.get("AB"), 2),
                "H": _round_stat(hits, 2),
                "R": _round_stat(acc.get("R"), 2),
                "RBI": _round_stat(acc.get("RBI"), 2),
                "BB": None,
                "SO": None,
                "HR": _round_stat(homers, 2),
                "TB": _round_stat(total_bases, 2),
                "SB": _round_stat(acc.get("SB"), 2),
                "_order": _safe_int(acc.get("order")),
            }
        )

    for side in ("away", "home"):
        bat[side].sort(
            key=lambda row: (
                row.get("_order") if row.get("_order") is not None else 999,
                -(_safe_float(row.get("AB")) or 0.0),
                str(row.get("name") or ""),
            )
        )
        for row in bat[side]:
            row.pop("_order", None)

    pit: Dict[str, List[Dict[str, Any]]] = {"away": [], "home": []}
    if isinstance(pitcher_props, dict):
        away_starter = _safe_int(starters.get("away"))
        home_starter = _safe_int(starters.get("home"))
        for pid_s, raw in pitcher_props.items():
            pid = _safe_int(pid_s)
            if not pid or not isinstance(raw, dict):
                continue
            meta_row = _meta(int(pid))
            side = meta_row.get("side")
            if side not in ("away", "home"):
                if away_starter is not None and int(pid) == int(away_starter):
                    side = "away"
                elif home_starter is not None and int(pid) == int(home_starter):
                    side = "home"
            if side not in ("away", "home"):
                continue
            outs_mean = _round_stat(raw.get("outs_mean"), 2)
            pit[side].append(
                {
                    "id": int(pid),
                    "name": str(meta_row.get("name") or starter_names.get(side) or pid),
                    "OUTS": outs_mean,
                    "IP": _decimal_ip_from_outs(outs_mean),
                    "H": None,
                    "R": None,
                    "BB": None,
                    "SO": _round_stat(raw.get("so_mean"), 2),
                    "HR": None,
                    "BF": None,
                    "P": _round_stat(raw.get("pitches_mean"), 2),
                }
            )

    for side in ("away", "home"):
        pit[side].sort(key=lambda row: (-(_safe_float(row.get("OUTS")) or 0.0), str(row.get("name") or "")))

    predicted = _sim_predicted_score(sim_obj)
    has_any_rows = any(bat[side] or pit[side] for side in ("away", "home"))
    if not has_any_rows and predicted.get("away") is None and predicted.get("home") is None:
        return None

    return {
        "away": {
            "totals": {"R": predicted.get("away"), "H": _sum_row_stat(bat["away"], "H"), "E": None},
            "batting": bat["away"],
            "pitching": pit["away"],
        },
        "home": {
            "totals": {"R": predicted.get("home"), "H": _sum_row_stat(bat["home"], "H"), "E": None},
            "batting": bat["home"],
            "pitching": pit["home"],
        },
    }


def _sim_boxscore_from_sim_boxscore(
    sim_obj: Dict[str, Any],
    *,
    player_meta: Optional[Dict[int, Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    sim_box = ((sim_obj.get("pbp") or {}).get("boxscore") or {})
    batter_stats = sim_box.get("batter_stats")
    pitcher_stats = sim_box.get("pitcher_stats")
    if not isinstance(batter_stats, dict) and not isinstance(pitcher_stats, dict):
        return None

    away_team_id = _safe_int((sim_obj.get("away") or {}).get("team_id"))
    home_team_id = _safe_int((sim_obj.get("home") or {}).get("team_id"))

    inferred_side: Dict[int, str] = {}
    pbp = ((sim_obj.get("pbp") or {}).get("pbp") or [])
    if isinstance(pbp, list) and (away_team_id or home_team_id):
        for ev in pbp:
            if not isinstance(ev, dict):
                continue
            if ev.get("type") == "PA":
                bid = _safe_int(ev.get("batter_id"))
                pid = _safe_int(ev.get("pitcher_id"))
                bt = _safe_int(ev.get("batting_team_id"))
                ft = _safe_int(ev.get("fielding_team_id"))
                if bid and bt and bid not in inferred_side:
                    if away_team_id is not None and bt == away_team_id:
                        inferred_side[int(bid)] = "away"
                    elif home_team_id is not None and bt == home_team_id:
                        inferred_side[int(bid)] = "home"
                if pid and ft and pid not in inferred_side:
                    if away_team_id is not None and ft == away_team_id:
                        inferred_side[int(pid)] = "away"
                    elif home_team_id is not None and ft == home_team_id:
                        inferred_side[int(pid)] = "home"
            elif ev.get("type") == "PITCHING_CHANGE":
                ft = _safe_int(ev.get("fielding_team_id"))
                for k in ("from_pitcher_id", "to_pitcher_id"):
                    pid = _safe_int(ev.get(k))
                    if pid and ft and pid not in inferred_side:
                        if away_team_id is not None and ft == away_team_id:
                            inferred_side[int(pid)] = "away"
                        elif home_team_id is not None and ft == home_team_id:
                            inferred_side[int(pid)] = "home"

    def _meta(pid: int) -> Dict[str, Any]:
        if player_meta and int(pid) in player_meta:
            return player_meta[int(pid)] or {}
        return {}

    def _name(pid: int) -> str:
        m = _meta(pid)
        nm = m.get("name")
        return str(nm) if nm else str(pid)

    def _side(pid: int) -> Optional[str]:
        m = _meta(pid)
        s = m.get("side")
        if s in ("away", "home"):
            return str(s)
        return inferred_side.get(int(pid))

    def _pos(pid: int) -> str:
        m = _meta(pid)
        return str(m.get("pos") or "")

    bat: Dict[str, List[Dict[str, Any]]] = {"away": [], "home": []}
    pit: Dict[str, List[Dict[str, Any]]] = {"away": [], "home": []}

    if isinstance(batter_stats, dict):
        for pid_s, st in batter_stats.items():
            pid = _safe_int(pid_s)
            if not pid or not isinstance(st, dict):
                continue
            side = _side(int(pid))
            if side not in ("away", "home"):
                continue
            singles = _safe_int(st.get("1B")) or 0
            doubles = _safe_int(st.get("2B")) or 0
            triples = _safe_int(st.get("3B")) or 0
            homers = _safe_int(st.get("HR")) or 0
            bat[side].append(
                {
                    "id": int(pid),
                    "name": _name(int(pid)),
                    "pos": _pos(int(pid)),
                    "PA": _safe_int(st.get("PA")),
                    "AB": _safe_int(st.get("AB")),
                    "H": _safe_int(st.get("H")),
                    "BB": _safe_int(st.get("BB")),
                    "SO": _safe_int(st.get("SO")),
                    "HR": homers,
                    "R": _safe_int(st.get("R")),
                    "RBI": _safe_int(st.get("RBI")),
                    "HBP": _safe_int(st.get("HBP")),
                    "TB": _safe_int(st.get("TB")) or (singles + 2 * doubles + 3 * triples + 4 * homers),
                }
            )

    if isinstance(pitcher_stats, dict):
        for pid_s, st in pitcher_stats.items():
            pid = _safe_int(pid_s)
            if not pid or not isinstance(st, dict):
                continue
            side = _side(int(pid))
            if side not in ("away", "home"):
                continue
            outs = _safe_int(st.get("OUTS")) or 0
            pit[side].append(
                {
                    "id": int(pid),
                    "name": _name(int(pid)),
                    "IP": _ip_from_outs(int(outs)),
                    "H": _safe_int(st.get("H")),
                    "R": _safe_int(st.get("R")),
                    "BB": _safe_int(st.get("BB")),
                    "SO": _safe_int(st.get("SO")),
                    "HR": _safe_int(st.get("HR")),
                    "BF": _safe_int(st.get("BF")),
                    "P": _safe_int(st.get("P")),
                    "HBP": _safe_int(st.get("HBP")),
                }
            )

    bat["away"].sort(key=lambda r: (-(r.get("PA") or 0), r.get("name") or ""))
    bat["home"].sort(key=lambda r: (-(r.get("PA") or 0), r.get("name") or ""))
    pit["away"].sort(key=lambda r: (-(r.get("P") or 0), r.get("name") or ""))
    pit["home"].sort(key=lambda r: (-(r.get("P") or 0), r.get("name") or ""))

    return {"away": {"batting": bat["away"], "pitching": pit["away"]}, "home": {"batting": bat["home"], "pitching": pit["home"]}}


def _sim_boxscore_from_pbp(sim_obj: Dict[str, Any], *, name_lookup: Optional[Dict[int, str]] = None) -> Dict[str, Any]:
    pbp = ((sim_obj.get("pbp") or {}).get("pbp") or [])
    if not isinstance(pbp, list) or not pbp:
        return {"away": {"batting": [], "pitching": []}, "home": {"batting": [], "pitching": []}}

    away_team_id = _safe_int((sim_obj.get("away") or {}).get("team_id"))
    home_team_id = _safe_int((sim_obj.get("home") or {}).get("team_id"))

    def _side_from_team_id(tid: Optional[int]) -> Optional[str]:
        if tid is None:
            return None
        if away_team_id is not None and int(tid) == int(away_team_id):
            return "away"
        if home_team_id is not None and int(tid) == int(home_team_id):
            return "home"
        return None

    # Batter stats: AB/H/BB/SO/HR + PA
    bat: Dict[str, Dict[int, Dict[str, Any]]] = {"away": {}, "home": {}}
    pit: Dict[str, Dict[int, Dict[str, Any]]] = {"away": {}, "home": {}}

    def _name(pid: int) -> str:
        if name_lookup and int(pid) in name_lookup and name_lookup[int(pid)]:
            return str(name_lookup[int(pid)])
        return str(pid)

    for ev in pbp:
        if not isinstance(ev, dict) or ev.get("type") != "PA":
            continue
        batting_side = _side_from_team_id(_safe_int(ev.get("batting_team_id")))
        fielding_side = _side_from_team_id(_safe_int(ev.get("fielding_team_id")))
        batter_id = _safe_int(ev.get("batter_id"))
        pitcher_id = _safe_int(ev.get("pitcher_id"))
        result = str(ev.get("result") or "").upper()
        pitches = _safe_int(ev.get("pitches")) or 0
        outs_before = _safe_int(ev.get("outs_before")) or 0
        outs_after = _safe_int(ev.get("outs_after")) or outs_before
        outs_delta = max(0, outs_after - outs_before)

        if batting_side in ("away", "home") and batter_id:
            row = bat[batting_side].setdefault(
                int(batter_id),
                {
                    "id": int(batter_id),
                    "name": _name(int(batter_id)),
                    "pos": "",
                    "PA": 0,
                    "AB": 0,
                    "H": 0,
                    "BB": 0,
                    "SO": 0,
                    "HR": 0,
                    "TB": 0,
                },
            )
            row["PA"] += 1
            if result in ("BB",):
                row["BB"] += 1
            elif result in ("SO",):
                row["SO"] += 1
                row["AB"] += 1
            elif result in ("OUT",):
                row["AB"] += 1
            elif result in ("1B", "2B", "3B", "HR"):
                row["AB"] += 1
                row["H"] += 1
                if result == "1B":
                    row["TB"] += 1
                elif result == "2B":
                    row["TB"] += 2
                elif result == "3B":
                    row["TB"] += 3
                elif result == "HR":
                    row["TB"] += 4
                if result == "HR":
                    row["HR"] += 1
            elif result in ("HBP",):
                # Not counted as AB; ignore for now.
                pass
            else:
                # Unknown result: treat as PA but not AB.
                pass

        if fielding_side in ("away", "home") and pitcher_id:
            row = pit[fielding_side].setdefault(
                int(pitcher_id),
                {
                    "id": int(pitcher_id),
                    "name": _name(int(pitcher_id)),
                    "GS": 0,
                    "IP": "0.0",
                    "H": 0,
                    "R": None,
                    "ER": None,
                    "BB": 0,
                    "SO": 0,
                    "HR": 0,
                    "BF": 0,
                    "P": 0,
                    "S": None,
                    "_outs": 0,
                },
            )
            row["BF"] += 1
            row["P"] += int(pitches)
            row["_outs"] += int(outs_delta)
            if result == "SO":
                row["SO"] += 1
            elif result == "BB":
                row["BB"] += 1
            elif result in ("1B", "2B", "3B", "HR"):
                row["H"] += 1
                if result == "HR":
                    row["HR"] += 1

    def _finalize_pitch_rows(rows: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for _pid, r in rows.items():
            r = dict(r)
            outs = int(r.pop("_outs", 0) or 0)
            r["IP"] = _ip_from_outs(outs)
            out.append(r)
        out.sort(key=lambda x: -(x.get("P") or 0))
        return out

    def _finalize_bat_rows(rows: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = list(rows.values())
        out.sort(key=lambda x: (-(x.get("PA") or 0), x.get("name") or ""))
        return out

    return {
        "away": {"batting": _finalize_bat_rows(bat["away"]), "pitching": _finalize_pitch_rows(pit["away"])},
        "home": {"batting": _finalize_bat_rows(bat["home"]), "pitching": _finalize_pitch_rows(pit["home"])},
    }


@dataclass(frozen=True)
class TeamMini:
    id: int
    abbr: str
    name: str


def _today_iso() -> str:
    return _local_today().isoformat()


def _mlb_logo_url(team_id: int) -> str:
    # Public MLB static logo endpoint. If this ever changes, we can swap to a local asset map.
    return f"https://www.mlbstatic.com/team-logos/{int(team_id)}.svg"


def _mlb_headshot_url(player_id: int) -> str:
    return f"https://img.mlbstatic.com/mlb-photos/image/upload/w_180,q_auto:best/v1/people/{int(player_id)}/headshot/67/current"


def _abbr(team_obj: dict) -> str:
    return str(team_obj.get("abbreviation") or team_obj.get("teamName") or team_obj.get("name") or "UNK")


def _team_from_schedule(side_obj: dict) -> TeamMini:
    team_obj = (side_obj.get("team") or {}) if isinstance(side_obj, dict) else {}
    tid = int(team_obj.get("id") or 0)
    name = str(team_obj.get("name") or "")
    abbr = _abbr(team_obj)
    return TeamMini(id=tid, abbr=abbr, name=name)


def _probable_pitcher_from_schedule(side_obj: dict) -> Optional[Dict[str, Any]]:
    if not isinstance(side_obj, dict):
        return None
    pp = side_obj.get("probablePitcher") or {}
    if not isinstance(pp, dict) or not pp:
        return None
    try:
        pid = int(pp.get("id") or 0)
    except Exception:
        pid = 0
    if pid <= 0:
        return None
    return {"id": pid, "fullName": str(pp.get("fullName") or "")}


def _get_box_starting_pitcher_id(feed: Dict[str, Any], side: str) -> Optional[int]:
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        players = t.get("players") or {}
        if isinstance(players, dict):
            for _k, pobj in players.items():
                if not isinstance(pobj, dict):
                    continue
                person = pobj.get("person") or {}
                try:
                    pid = int(person.get("id") or 0)
                except Exception:
                    pid = 0
                if pid <= 0:
                    continue
                pitching = ((pobj.get("stats") or {}).get("pitching") or {})
                try:
                    gs = int(pitching.get("gamesStarted") or 0)
                except Exception:
                    gs = 0
                if gs == 1:
                    return int(pid)
    except Exception:
        pass

    # Fallback: first pitcher id if present.
    try:
        pitchers = (((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}).get(str(side), {}).get("pitchers") or []
        if isinstance(pitchers, list) and pitchers:
            return int(pitchers[0])
    except Exception:
        pass
    return None


def _player_name_from_box(feed: Dict[str, Any], pid: int) -> str:
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        for side in ("away", "home"):
            t = teams.get(side) or {}
            players = t.get("players") or {}
            pobj = players.get(f"ID{int(pid)}") or {}
            person = pobj.get("person") or {}
            name = str(person.get("fullName") or "")
            if name:
                return name
    except Exception:
        pass
    return ""


def _lineup_from_box(feed: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    """Best-effort batting order from boxscore players[].battingOrder."""
    out: List[Tuple[int, int, str]] = []
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        players = t.get("players") or {}
        if not isinstance(players, dict):
            return []
        for _k, pobj in players.items():
            if not isinstance(pobj, dict):
                continue
            try:
                pid = int(((pobj.get("person") or {}).get("id") or 0))
            except Exception:
                continue
            if pid <= 0:
                continue
            bo = pobj.get("battingOrder")
            if bo is None:
                continue
            try:
                bo_i = int(str(bo))
            except Exception:
                continue
            # battingOrder is like 100, 200, ... for slots.
            name = str(((pobj.get("person") or {}).get("fullName") or ""))
            out.append((bo_i, pid, name))
    except Exception:
        return []

    out.sort(key=lambda t: t[0])
    lineup: List[Dict[str, Any]] = []
    for bo_i, pid, name in out:
        lineup.append({"order": bo_i, "id": pid, "name": name})
    return lineup


def _current_matchup(feed: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort current batter/pitcher from currentPlay."""
    try:
        cur = (((feed.get("liveData") or {}).get("plays") or {}).get("currentPlay") or {})
        about = cur.get("about") or {}
        count = cur.get("count") or {}
        matchup = cur.get("matchup") or {}
        batter = matchup.get("batter") or {}
        pitcher = matchup.get("pitcher") or {}

        def _i(x: Any) -> Optional[int]:
            try:
                if x is None:
                    return None
                return int(x)
            except Exception:
                return None

        return {
            "inning": _i(about.get("inning")),
            "halfInning": about.get("halfInning"),
            "count": {
                "balls": _i(count.get("balls")),
                "strikes": _i(count.get("strikes")),
                "outs": _i(count.get("outs")),
            },
            "batter": {"id": batter.get("id"), "fullName": batter.get("fullName")},
            "pitcher": {"id": pitcher.get("id"), "fullName": pitcher.get("fullName")},
        }
    except Exception:
        return {"inning": None, "halfInning": None, "count": None, "batter": None, "pitcher": None}


def _iter_team_players(feed: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        players = t.get("players") or {}
        if not isinstance(players, dict):
            return []
        out: List[Dict[str, Any]] = []
        for _k, pobj in players.items():
            if isinstance(pobj, dict):
                out.append(pobj)
        return out
    except Exception:
        return []


def _pos_abbr(player_obj: Dict[str, Any]) -> str:
    try:
        pos = player_obj.get("position") or {}
        if isinstance(pos, dict):
            return str(pos.get("abbreviation") or "")
    except Exception:
        pass
    return ""


def _positions_from_box(feed: Dict[str, Any], side: str) -> Dict[int, str]:
    """Map player_id -> position abbreviation for a team from boxscore."""
    out: Dict[int, str] = {}
    for pobj in _iter_team_players(feed, side):
        try:
            person = pobj.get("person") or {}
            pid = _safe_int(person.get("id"))
            if not pid or int(pid) <= 0:
                continue
            pos = _pos_abbr(pobj)
            if pos:
                out[int(pid)] = str(pos)
        except Exception:
            continue
    return out


def _boxscore_batting(feed: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    rows: List[Tuple[int, int, Dict[str, Any]]] = []
    for pobj in _iter_team_players(feed, side):
        try:
            batting = ((pobj.get("stats") or {}).get("batting") or {})
            if not isinstance(batting, dict) or not batting:
                continue
            person = pobj.get("person") or {}
            pid = int(person.get("id") or 0)
            if pid <= 0:
                continue
            name = str(person.get("fullName") or "")
            bo_raw = pobj.get("battingOrder")
            try:
                bo = int(str(bo_raw)) if bo_raw is not None else 999999
            except Exception:
                bo = 999999

            def _i(x: Any) -> Optional[int]:
                try:
                    if x is None:
                        return None
                    return int(float(x))
                except Exception:
                    return None

            row = {
                "id": pid,
                "name": name,
                "pos": _pos_abbr(pobj),
                "battingOrder": None if bo == 999999 else bo,
                "AB": _i(batting.get("atBats")),
                "R": _i(batting.get("runs")),
                "H": _i(batting.get("hits")),
                "2B": _i(batting.get("doubles")),
                "3B": _i(batting.get("triples")),
                "TB": _i(batting.get("totalBases")),
                "RBI": _i(batting.get("rbi")),
                "BB": _i(batting.get("baseOnBalls")),
                "SO": _i(batting.get("strikeOuts")),
                "HR": _i(batting.get("homeRuns")),
                "SB": _i(batting.get("stolenBases")),
                "LOB": _i(batting.get("leftOnBase")),
            }
            rows.append((bo, pid, row))
        except Exception:
            continue

    rows.sort(key=lambda t: (t[0], t[1]))
    return [r for _bo, _pid, r in rows]


def _boxscore_pitching(feed: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
    rows: List[Tuple[int, Dict[str, Any]]] = []
    for pobj in _iter_team_players(feed, side):
        try:
            pitching = ((pobj.get("stats") or {}).get("pitching") or {})
            if not isinstance(pitching, dict) or not pitching:
                continue
            person = pobj.get("person") or {}
            pid = int(person.get("id") or 0)
            if pid <= 0:
                continue
            name = str(person.get("fullName") or "")

            def _i(x: Any) -> Optional[int]:
                try:
                    if x is None:
                        return None
                    return int(float(x))
                except Exception:
                    return None

            def _s(x: Any) -> str:
                return str(x) if x is not None else ""

            # Sort starters first, then by pitchesThrown desc.
            try:
                gs = int(pitching.get("gamesStarted") or 0)
            except Exception:
                gs = 0
            sort_key = 0 if gs == 1 else 1
            pitches = _i(pitching.get("pitchesThrown"))
            row = {
                "id": pid,
                "name": name,
                "GS": gs,
                "IP": _s(pitching.get("inningsPitched")),
                "H": _i(pitching.get("hits")),
                "R": _i(pitching.get("runs")),
                "ER": _i(pitching.get("earnedRuns")),
                "BB": _i(pitching.get("baseOnBalls")),
                "SO": _i(pitching.get("strikeOuts")),
                "HR": _i(pitching.get("homeRuns")),
                "BF": _i(pitching.get("battersFaced")),
                "P": pitches,
                "S": _i(pitching.get("strikes")),
            }
            rows.append((sort_key, row))
        except Exception:
            continue

    rows.sort(key=lambda t: (t[0], -(t[1].get("P") or 0), t[1].get("name") or ""))
    return [r for _k, r in rows]


def _team_totals(feed: Dict[str, Any], side: str) -> Dict[str, Any]:
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        ts = t.get("teamStats") or {}
        batting = ts.get("batting") or {}
        pitching = ts.get("pitching") or {}

        # For many games (esp. spring), the canonical score lives in linescore.
        ls_side = (
            ((feed.get("liveData") or {}).get("linescore") or {}).get("teams") or {}
        ).get(str(side)) or {}
        runs_val = ls_side.get("runs")
        hits_val = ls_side.get("hits")
        errors_val = ls_side.get("errors")
        lob_val = ls_side.get("leftOnBase")
        if runs_val is None:
            runs_val = batting.get("runs")
        if hits_val is None:
            hits_val = batting.get("hits")

        def _i(x: Any) -> Optional[int]:
            try:
                if x is None:
                    return None
                return int(float(x))
            except Exception:
                return None

        return {
            "R": _i(runs_val),
            "H": _i(hits_val),
            "E": _i(errors_val),
            "LOB": _i(lob_val),
            "HR": _i(batting.get("homeRuns")),
            "SO_bat": _i(batting.get("strikeOuts")),
            "BB_bat": _i(batting.get("baseOnBalls")),
            "SO_pit": _i(pitching.get("strikeOuts")),
            "BB_pit": _i(pitching.get("baseOnBalls")),
        }
    except Exception:
        return {}


def _plays_since(feed: Dict[str, Any], *, since_index: int) -> Tuple[int, List[Dict[str, Any]]]:
    """Return (new_index, plays[]) where plays are simplified for UI."""
    try:
        all_plays = (((feed.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])
        if not isinstance(all_plays, list):
            return since_index, []
    except Exception:
        return since_index, []

    plays_out: List[Dict[str, Any]] = []
    new_index = since_index


def _normalize_person_name(value: Any) -> str:
    return normalize_pitcher_name(str(value or ""))


def _lookup_boxscore_row(rows: Any, player_name: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(rows, list):
        return None
    target = _normalize_person_name(player_name)
    if not target:
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _normalize_person_name(row.get("name")) == target:
            return row
    return None


def _prop_owner_name(reco: Dict[str, Any]) -> str:
    return str(reco.get("player_name") or reco.get("pitcher_name") or "").strip()


def _prop_side(card: Dict[str, Any], reco: Dict[str, Any]) -> Optional[str]:
    explicit = str(reco.get("team_side") or "").strip().lower()
    if explicit in {"away", "home"}:
        return explicit
    team_value = str(reco.get("team") or "").strip().lower()
    away_team = card.get("away") or {}
    home_team = card.get("home") or {}
    away_values = {str(away_team.get("abbr") or "").strip().lower(), str(away_team.get("name") or "").strip().lower()}
    home_values = {str(home_team.get("abbr") or "").strip().lower(), str(home_team.get("name") or "").strip().lower()}
    if team_value and team_value in away_values:
        return "away"
    if team_value and team_value in home_values:
        return "home"
    return None


def _parse_ip_to_outs(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    parts = text.split(".", 1)
    try:
        whole = int(parts[0])
        frac = int(parts[1] if len(parts) > 1 else 0)
    except Exception:
        return None
    return int(whole * 3 + frac)


def _live_stat_value(row: Optional[Dict[str, Any]], reco: Dict[str, Any]) -> Optional[float]:
    if not isinstance(row, dict):
        return None
    market = str(reco.get("market") or "").strip().lower()
    prop = str(reco.get("prop") or "").strip().lower()
    if "home_runs" in market or "home_runs" in prop:
        return _safe_float(row.get("HR"))
    if "total_bases" in market or "total_bases" in prop:
        return _safe_float(row.get("TB"))
    if "rbis" in market or prop == "rbi":
        return _safe_float(row.get("RBI"))
    if "hitter_runs" in market or "runs_scored" in prop:
        return _safe_float(row.get("R"))
    if "hitter_hits" in market or prop.endswith("hits"):
        return _safe_float(row.get("H"))
    if prop == "strikeouts":
        return _safe_float(row.get("SO"))
    if "earned_runs" in market or prop == "earned_runs":
        return _safe_float(row.get("ER"))
    if prop == "outs":
        outs = _safe_float(row.get("OUTS"))
        if outs is not None:
            return float(outs)
        ip_outs = _parse_ip_to_outs(row.get("IP"))
        return float(ip_outs) if ip_outs is not None else None
    return None


def _prop_result_state(reco: Dict[str, Any], actual_value: Optional[float], status_text: Any) -> str:
    status_token = str(status_text or "").strip().lower()
    if actual_value is None:
        return "live" if status_token in {"live", "in progress", "manager challenge"} or "live" in status_token else "pending"
    if status_token in {"live", "in progress", "manager challenge"} or "live" in status_token:
        return "live"
    line = _safe_float(reco.get("market_line"))
    selection = str(reco.get("selection") or "over").strip().lower()
    if line is None:
        return "pending"
    if abs(float(actual_value) - float(line)) < 1e-9:
        return "push"
    did_win = float(actual_value) < float(line) if selection == "under" else float(actual_value) > float(line)
    return "win" if did_win else "loss"


def _american_odds_implied_prob(value: Any) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        odds = int(text)
    except Exception:
        try:
            odds = int(float(text))
        except Exception:
            return None
    if odds == 0:
        return None
    if odds > 0:
        return 100.0 / (float(odds) + 100.0)
    return abs(float(odds)) / (abs(float(odds)) + 100.0)


def _normalize_american_odds(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(round(float(value)))
    except Exception:
        return None


def _prop_price_allowed(odds: Any, *, max_favorite_odds: int = -200) -> bool:
    odds_value = _normalize_american_odds(odds)
    if odds_value is None:
        return True
    if odds_value >= 0:
        return True
    return odds_value >= int(max_favorite_odds)


def _prob_over_line_from_dist(dist: Dict[str, Any], line: float) -> Optional[float]:
    total = 0
    over = 0
    for raw_bucket, raw_count in (dist or {}).items():
        try:
            bucket = float(raw_bucket)
            count = int(raw_count)
        except Exception:
            continue
        total += count
        if float(bucket) > float(line):
            over += count
    if total <= 0:
        return None
    return float(over / float(total))


def _selection_live_edge(selection: str, live_projection: Optional[float], line: Optional[float]) -> Optional[float]:
    projection_value = _safe_float(live_projection)
    line_value = _safe_float(line)
    if projection_value is None or line_value is None:
        return None
    if str(selection) == "under":
        return round(float(line_value) - float(projection_value), 3)
    if str(selection) == "over":
        return round(float(projection_value) - float(line_value), 3)
    return None


def _live_prop_market_resolved(actual_value: Optional[float], market_line: Optional[float]) -> bool:
    actual = _safe_float(actual_value)
    line = _safe_float(market_line)
    if actual is None or line is None:
        return False
    return float(actual) > float(line) + 1e-9


def _select_live_prop_side(
    *,
    model_prob_over: Optional[float],
    live_projection: Optional[float],
    line: Optional[float],
    over_odds: Any,
    under_odds: Any,
) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    line_value = _safe_float(line)
    side_probs = market_side_probabilities(over_odds, under_odds)
    market_prob_over = _safe_float(side_probs.get("over"))
    market_prob_under = _safe_float(side_probs.get("under"))

    for selection, odds in (("over", over_odds), ("under", under_odds)):
        if _safe_int(odds) is None:
            continue
        if not _prop_price_allowed(odds, max_favorite_odds=-200):
            continue
        live_edge = _selection_live_edge(selection, live_projection, line_value)
        projection_gap = abs(float(live_edge)) if live_edge is not None else None
        market_edge = None
        if model_prob_over is not None:
            if selection == "over" and market_prob_over is not None:
                market_edge = round(float(model_prob_over) - float(market_prob_over), 4)
            elif selection == "under" and market_prob_under is not None:
                market_edge = round((1.0 - float(model_prob_over)) - float(market_prob_under), 4)
        score = live_edge if live_edge is not None else market_edge
        if score is None or float(score) <= 0.0:
            continue
        candidates.append(
            {
                "selection": selection,
                "odds": _safe_int(odds),
                "liveEdge": live_edge,
                "projectionGap": projection_gap,
                "marketEdge": market_edge,
                "marketProbOver": market_prob_over,
                "marketProbUnder": market_prob_under,
                "selectedSideMarketProb": market_prob_over if selection == "over" else market_prob_under,
                "marketProbMode": str(side_probs.get("mode") or "unknown"),
                "score": float(score),
            }
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            float(item.get("liveEdge") or float("-inf")),
            float(item.get("projectionGap") or float("-inf")),
            float(item.get("marketEdge") or float("-inf")),
            1 if item.get("selection") == "over" else 0,
        ),
    )


def _sim_prop_models(sim_context: Optional[Dict[str, Any]], kind: str) -> Dict[str, Dict[str, Any]]:
    out = ((sim_context or {}).get("propModels") or {}).get(kind) or {}
    return out if isinstance(out, dict) else {}


def _live_prop_market_label(market: str, prop: str) -> str:
    market_text = str(market or "").strip().lower()
    prop_text = str(prop or "").strip().lower()
    if market_text == "pitcher_props":
        cfg = _PITCHER_LADDER_PROPS.get(prop_text) or {}
        return str(cfg.get("label") or market or "Pitcher prop")
    cfg = _HITTER_LADDER_PROPS.get(prop_text) or {}
    return str(cfg.get("label") or market or "Hitter prop")


def _final_live_prop_rows_from_registry(
    card: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    d: str,
) -> List[Dict[str, Any]]:
    game_pk = _safe_int(card.get("gamePk"))
    if game_pk is None:
        return []

    registry = _load_live_prop_registry(d)
    entries = registry.get("entries") if isinstance(registry.get("entries"), dict) else {}
    if not entries:
        return []

    actual_teams = ((snapshot or {}).get("teams") or {})
    rows: List[Dict[str, Any]] = []
    for entry in entries.values():
        if not isinstance(entry, dict):
            continue
        if _safe_int(entry.get("gamePk")) != game_pk:
            continue

        owner = str(entry.get("owner") or "").strip()
        market = str(entry.get("market") or "").strip().lower()
        prop = str(entry.get("prop") or "").strip().lower()
        selection = str(entry.get("selection") or "").strip().lower()
        market_line = _safe_float(entry.get("marketLine"))
        if not owner or not market or not prop or not selection or market_line is None:
            continue

        first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
        last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
        row_type = "pitching" if market == "pitcher_props" else "batting"
        actual_row = None
        team_side = None
        for candidate_side in ("away", "home"):
            candidate_row = _lookup_boxscore_row((((actual_teams.get(candidate_side) or {}).get("boxscore") or {}).get(row_type) or []), owner)
            if candidate_row:
                actual_row = candidate_row
                team_side = candidate_side
                break

        live_edge = _safe_float(first_snapshot.get("liveEdge"))
        actual_value = _live_stat_value(actual_row, {"market": market, "prop": prop})
        team_info = (card.get(team_side) or {}) if team_side in {"away", "home"} else {}
        item: Dict[str, Any] = {
            "recommendation_tier": "live",
            "source": "live_registry",
            "market": market,
            "market_label": _live_prop_market_label(market, prop),
            "prop": prop,
            "selection": selection,
            "market_line": float(market_line),
            "odds": _safe_int(first_snapshot.get("odds")),
            "over_odds": None,
            "under_odds": None,
            "model_prob_over": None,
            "market_prob_over": None,
            "market_prob_under": None,
            "market_prob_mode": "archived_first_seen",
            "selected_side_market_prob": None,
            "edge": live_edge,
            "live_edge": live_edge,
            "projection_gap": abs(float(live_edge)) if live_edge is not None else None,
            "model_mean": _safe_float(first_snapshot.get("modelMean")),
            "actual": actual_value if actual_value is not None else _safe_float(last_snapshot.get("actual")),
            "actual_value": actual_value if actual_value is not None else _safe_float(last_snapshot.get("actual")),
            "live_projection": _safe_float(first_snapshot.get("liveProjection")),
            "first_seen_at": entry.get("firstSeenAt"),
            "last_seen_at": entry.get("lastSeenAt"),
            "first_seen_odds": _safe_int(first_snapshot.get("odds")),
            "first_seen_line": _safe_float(first_snapshot.get("marketLine")),
            "first_seen_live_projection": _safe_float(first_snapshot.get("liveProjection")),
            "first_seen_live_edge": live_edge,
            "first_seen_actual": _safe_float(first_snapshot.get("actual")),
            "seen_count": _safe_int(entry.get("seenCount")),
            "game_pk": int(game_pk),
            "archived_for_reconciliation": True,
        }
        if market == "pitcher_props":
            item["pitcher_name"] = owner
            item["outs_mean"] = _safe_float(first_snapshot.get("modelMean")) if prop == "outs" else None
        else:
            item["player_name"] = owner
        if team_side:
            item["team_side"] = team_side
        if team_info:
            item["team"] = team_info.get("abbr") or team_info.get("name")
        rows.append(item)

    rows.sort(
        key=lambda row: (
            -float(_safe_float(row.get("live_edge")) or -999.0),
            str(row.get("first_seen_at") or ""),
            str(_prop_owner_name(row) or ""),
            str(row.get("market") or ""),
        )
    )
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        item = dict(row)
        item["rank"] = int(idx)
        out.append(item)
    return out


def _current_live_prop_rows(card: Dict[str, Any], snapshot: Optional[Dict[str, Any]], sim_context: Optional[Dict[str, Any]], d: str) -> List[Dict[str, Any]]:
    if not isinstance(snapshot, dict) or not isinstance(sim_context, dict) or not sim_context.get("found"):
        return []

    status = (snapshot or {}).get("status") or {}
    abstract = str(status.get("abstractGameState") or ((card or {}).get("status") or {}).get("abstract") or "").strip().lower()
    if abstract == "final":
        return _final_live_prop_rows_from_registry(card, snapshot, d)

    _maybe_refresh_live_oddsapi_markets(d)

    progress_fraction = float((_live_game_progress(snapshot, card).get("fraction") or 0.0))
    actual_teams = ((snapshot or {}).get("teams") or {})
    _, pitcher_market_lines = _load_pitcher_prop_market_lines(d)
    _, hitter_market_lines = _load_hitter_prop_market_lines(d)
    pitcher_models = _sim_prop_models(sim_context, "pitchers")
    hitter_models = _sim_prop_models(sim_context, "hitters")
    rows: List[Dict[str, Any]] = []

    hitter_market_names = {
        "hits": "hitter_hits",
        "home_runs": "hitter_home_runs",
        "total_bases": "hitter_total_bases",
        "runs": "hitter_runs",
        "rbi": "hitter_rbis",
    }

    for side in ("away", "home"):
        starter_name = _first_text((((actual_teams.get(side) or {}).get("starter") or {}).get("name")))
        starter_key = normalize_pitcher_name(starter_name)
        model_entry = pitcher_models.get(starter_key) if starter_key else None
        market_entry = pitcher_market_lines.get(starter_key) if starter_key else None
        actual_row = _lookup_boxscore_row((((actual_teams.get(side) or {}).get("boxscore") or {}).get("pitching") or []), starter_name)
        if isinstance(model_entry, dict) and isinstance(market_entry, dict):
            for prop_key, cfg in _PITCHER_LADDER_PROPS.items():
                market_key = cfg.get("market_key")
                if not market_key:
                    continue
                market = market_entry.get(str(market_key))
                if not isinstance(market, dict):
                    continue
                line_value = _safe_float(market.get("line"))
                if line_value is None:
                    continue
                model_row = model_entry.get("model") or {}
                model_mean = _safe_float(model_row.get(str(cfg.get("mean_key"))))
                model_prob_over = _prob_over_line_from_dist(model_row.get(str(cfg.get("dist_key"))) or {}, float(line_value))
                actual_value = _live_stat_value(actual_row, {"market": "pitcher_props", "prop": prop_key})
                if _live_prop_market_resolved(actual_value, line_value):
                    continue
                live_projection = _project_live_value(actual_value, model_mean, progress_fraction)
                side_pick = _select_live_prop_side(
                    model_prob_over=model_prob_over,
                    live_projection=live_projection,
                    line=float(line_value),
                    over_odds=market.get("over_odds"),
                    under_odds=market.get("under_odds"),
                )
                if side_pick is None:
                    continue
                rows.append(
                    {
                        "recommendation_tier": "live",
                        "source": "current_market",
                        "market": "pitcher_props",
                        "market_label": cfg.get("label"),
                        "prop": prop_key,
                        "pitcher_name": starter_name,
                        "team": model_entry.get("team"),
                        "team_side": side,
                        "selection": side_pick.get("selection"),
                        "market_line": float(line_value),
                        "odds": side_pick.get("odds"),
                        "over_odds": _safe_int(market.get("over_odds")),
                        "under_odds": _safe_int(market.get("under_odds")),
                        "model_prob_over": model_prob_over,
                        "market_prob_over": side_pick.get("marketProbOver"),
                        "market_prob_under": side_pick.get("marketProbUnder"),
                        "market_prob_mode": side_pick.get("marketProbMode"),
                        "selected_side_market_prob": side_pick.get("selectedSideMarketProb"),
                        "edge": side_pick.get("marketEdge"),
                        "live_edge": side_pick.get("liveEdge"),
                        "projection_gap": side_pick.get("projectionGap"),
                        "outs_mean": model_mean if prop_key == "outs" else None,
                        "model_mean": model_mean,
                        "actual": actual_value,
                        "actual_value": actual_value,
                        "live_projection": live_projection,
                    }
                )

        batting_rows = (((actual_teams.get(side) or {}).get("boxscore") or {}).get("batting") or [])
        for actual_row in batting_rows:
            player_name = _first_text(actual_row.get("name"))
            player_key = normalize_pitcher_name(player_name)
            model_entry = hitter_models.get(player_key) if player_key else None
            market_entry = hitter_market_lines.get(player_key) if player_key else None
            if not isinstance(model_entry, dict) or not isinstance(market_entry, dict):
                continue
            model_row = model_entry.get("model") or {}
            for prop_key, cfg in _HITTER_LADDER_PROPS.items():
                market_key = cfg.get("market_key")
                market_name = hitter_market_names.get(str(prop_key))
                if not market_key or not market_name:
                    continue
                market = market_entry.get(str(market_key))
                if not isinstance(market, dict):
                    continue
                line_value = _safe_float(market.get("line"))
                if line_value is None:
                    continue
                model_mean = _safe_float(model_row.get(str(cfg.get("mean_key"))))
                model_prob_over = _prob_over_line_from_dist(model_row.get(str(cfg.get("dist_key"))) or {}, float(line_value))
                actual_value = _live_stat_value(actual_row, {"market": market_name, "prop": prop_key})
                if _live_prop_market_resolved(actual_value, line_value):
                    continue
                live_projection = _project_live_value(actual_value, model_mean, progress_fraction)
                side_pick = _select_live_prop_side(
                    model_prob_over=model_prob_over,
                    live_projection=live_projection,
                    line=float(line_value),
                    over_odds=market.get("over_odds"),
                    under_odds=market.get("under_odds"),
                )
                if side_pick is None:
                    continue
                rows.append(
                    {
                        "recommendation_tier": "live",
                        "source": "current_market",
                        "market": market_name,
                        "market_label": cfg.get("label"),
                        "prop": prop_key,
                        "player_name": player_name,
                        "team": model_entry.get("team"),
                        "team_side": side,
                        "selection": side_pick.get("selection"),
                        "market_line": float(line_value),
                        "odds": side_pick.get("odds"),
                        "over_odds": _safe_int(market.get("over_odds")),
                        "under_odds": _safe_int(market.get("under_odds")),
                        "model_prob_over": model_prob_over,
                        "market_prob_over": side_pick.get("marketProbOver"),
                        "market_prob_under": side_pick.get("marketProbUnder"),
                        "market_prob_mode": side_pick.get("marketProbMode"),
                        "selected_side_market_prob": side_pick.get("selectedSideMarketProb"),
                        "edge": side_pick.get("marketEdge"),
                        "live_edge": side_pick.get("liveEdge"),
                        "projection_gap": side_pick.get("projectionGap"),
                        "model_mean": model_mean,
                        "actual": actual_value,
                        "actual_value": actual_value,
                        "live_projection": live_projection,
                    }
                )

    rows.sort(
        key=lambda row: (
            -float(_safe_float(row.get("live_edge")) or -999.0),
            -float(_safe_float(row.get("projection_gap")) or -999.0),
            -float(_safe_float(row.get("edge")) or -999.0),
            str(_prop_owner_name(row) or ""),
            str(row.get("market") or ""),
        )
    )
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        item = dict(row)
        item["rank"] = int(idx)
        item["game_pk"] = _safe_int(card.get("gamePk"))
        out.append(item)
    return _enrich_live_prop_rows_with_registry(out, d)


def _normalize_two_way_probs(first_prob: Optional[float], second_prob: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if first_prob is None or second_prob is None:
        return None, None
    denom = float(first_prob) + float(second_prob)
    if denom <= 0.0:
        return None, None
    return float(first_prob) / denom, float(second_prob) / denom


def _live_margin_win_prob(home_margin: Optional[float]) -> Optional[float]:
    margin = _safe_float(home_margin)
    if margin is None:
        return None
    return 1.0 / (1.0 + math.exp(-0.65 * float(margin)))


def _normalize_team_key(value: Any) -> str:
    return " ".join(part for part in normalize_pitcher_name(str(value or "")).split() if part)


def _card_matchup_key(card: Dict[str, Any]) -> Tuple[str, str]:
    away = _normalize_team_key(((card.get("away") or {}).get("name") or (card.get("away") or {}).get("abbr") or ""))
    home = _normalize_team_key(((card.get("home") or {}).get("name") or (card.get("home") or {}).get("abbr") or ""))
    return away, home


def _card_event_id(card: Dict[str, Any]) -> Optional[str]:
    markets = card.get("markets") or {}
    candidates: List[Any] = [
        (markets.get("totals") or {}).get("event_id"),
        (markets.get("ml") or {}).get("event_id"),
    ]
    for key in ("pitcherProps", "hitterProps", "extraPitcherProps", "extraHitterProps"):
        rows = markets.get(key) or []
        if isinstance(rows, list) and rows:
            candidates.append((rows[0] or {}).get("event_id"))
    for raw in candidates:
        text = str(raw or "").strip()
        if text:
            return text
    return None


def _load_game_line_market_index(d: str) -> Dict[str, Any]:
    path = _resolve_oddsapi_market_file(d, "oddsapi_game_lines")
    doc = _load_json_file(path)
    by_event_id: Dict[str, Dict[str, Any]] = {}
    by_matchup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if isinstance(doc, dict):
        for row in doc.get("games") or []:
            if not isinstance(row, dict):
                continue
            event_id = str(row.get("event_id") or "").strip()
            away_key = _normalize_team_key(row.get("away_team"))
            home_key = _normalize_team_key(row.get("home_team"))
            if event_id:
                by_event_id[event_id] = row
            if away_key and home_key:
                by_matchup[(away_key, home_key)] = row
    return {
        "path": path,
        "by_event_id": by_event_id,
        "by_matchup": by_matchup,
    }


def _game_line_market_for_card(card: Dict[str, Any], index: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    event_id = _card_event_id(card)
    if event_id:
        row = (index.get("by_event_id") or {}).get(str(event_id))
        if isinstance(row, dict):
            return row
    return (index.get("by_matchup") or {}).get(_card_matchup_key(card))


def _live_game_progress(snapshot: Optional[Dict[str, Any]], card: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    status = ((snapshot or {}).get("status") or {}) if isinstance(snapshot, dict) else {}
    abstract = str(status.get("abstractGameState") or ((card or {}).get("status") or {}).get("abstract") or "")
    detailed = str(status.get("detailedState") or ((card or {}).get("status") or {}).get("detailed") or "")
    if abstract.lower() == "final":
        return {"fraction": 1.0, "inning": 9, "half": "final", "outs": 3, "label": detailed or "Final"}
    if abstract.lower() != "live":
        return {"fraction": 0.0, "inning": None, "half": None, "outs": 0, "label": detailed or abstract or "Pregame"}

    current = ((snapshot or {}).get("current") or {}) if isinstance(snapshot, dict) else {}
    inning = _safe_int(current.get("inning")) or 1
    half = str(current.get("halfInning") or "").strip().lower()
    outs = _safe_int(((current.get("count") or {}).get("outs"))) or 0
    outs = int(max(0, min(2, outs)))
    outs_recorded = int(max(0, ((inning - 1) * 6) + (3 if half == "bottom" else 0) + outs))
    fraction = max(0.0, min(1.0, float(outs_recorded) / 54.0))
    label = f"{half.title()} {inning}".strip() if half else f"Inning {inning}"
    return {"fraction": fraction, "inning": inning, "half": half, "outs": outs, "label": label}


def _project_live_value(actual_value: Optional[float], model_mean: Optional[float], progress_fraction: float) -> Optional[float]:
    mean = _safe_float(model_mean)
    if mean is None:
        return None
    actual = float(_safe_float(actual_value) or 0.0)
    progress = max(0.0, min(1.0, float(progress_fraction or 0.0)))
    expected_to_date = float(mean) * progress
    remaining = max(float(mean) - expected_to_date, 0.0)
    return round(actual + remaining, 3)


def _segment_projection(
    *,
    pregame_away: Optional[float],
    pregame_home: Optional[float],
    actual_away: Optional[float],
    actual_home: Optional[float],
    progress_fraction: float,
    target_innings: int,
) -> Dict[str, Any]:
    away_mean = _safe_float(pregame_away)
    home_mean = _safe_float(pregame_home)
    if away_mean is None or home_mean is None:
        return {"away": None, "home": None, "total": None, "homeMargin": None, "closed": False}
    target_fraction = max(0.0, min(1.0, float(target_innings) / 9.0))
    if progress_fraction > target_fraction + 1e-9:
        return {"away": None, "home": None, "total": None, "homeMargin": None, "closed": True}
    away_target = float(away_mean) * target_fraction
    home_target = float(home_mean) * target_fraction
    expected_away_to_date = min(float(away_mean) * progress_fraction, away_target)
    expected_home_to_date = min(float(home_mean) * progress_fraction, home_target)
    away_projection = float(_safe_float(actual_away) or 0.0) + max(away_target - expected_away_to_date, 0.0)
    home_projection = float(_safe_float(actual_home) or 0.0) + max(home_target - expected_home_to_date, 0.0)
    return {
        "away": round(away_projection, 2),
        "home": round(home_projection, 2),
        "total": round(away_projection + home_projection, 2),
        "homeMargin": round(home_projection - away_projection, 2),
        "closed": False,
    }


def _load_sim_context_for_game(
    game_pk: int,
    d: str,
    *,
    artifacts: Optional[Dict[str, Any]] = None,
    archive: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    artifacts = artifacts or _load_cards_artifacts(d)
    archive = archive or _load_cards_archive_context(d)
    sim_dir = artifacts.get("sim_dir")
    snapshot_dir = artifacts.get("snapshot_dir")

    p = _find_sim_file(game_pk=int(game_pk), d=d, day_dir=sim_dir)
    source_path: Optional[Path] = p
    if p:
        try:
            sim_obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {"gamePk": int(game_pk), "date": d, "found": False, "error": "read_failed"}
    else:
        report_obj = archive.get("report") if isinstance(archive.get("report"), dict) else None
        report_game = _season_report_game(report_obj, int(game_pk))
        if not isinstance(report_game, dict):
            return {"gamePk": int(game_pk), "date": d, "found": False}
        sim_obj = _report_game_to_sim_obj(
            report_game,
            _safe_int(((report_obj.get("meta") or {}).get("sims_per_game")) if isinstance(report_obj, dict) else None),
        )
        source_path = archive.get("report_path") if isinstance(archive.get("report_path"), Path) else None

    player_meta: Dict[int, Dict[str, Any]] = {}
    if p:
        try:
            roster_path = _find_roster_snapshot_for_sim(
                d=d,
                sim_file=p,
                sim_obj=sim_obj,
                day_dir=snapshot_dir,
            )
            if roster_path:
                roster_obj = json.loads(roster_path.read_text(encoding="utf-8"))
                if isinstance(roster_obj, dict):
                    player_meta.update(_player_meta_from_roster_snapshot(roster_obj))
        except Exception:
            pass
    else:
        player_meta.update(_report_player_meta(_season_report_game(archive.get("report"), int(game_pk))))

    name_lookup: Dict[int, str] = {}
    try:
        feed = _load_game_feed_for_date(int(game_pk), d) if _is_historical_date(d) else None
        if not isinstance(feed, dict) or not feed:
            feed = fetch_game_feed_live(_client(), int(game_pk))
        if isinstance(feed, dict) and feed:
            box = (feed.get("liveData") or {}).get("boxscore") or {}
            teams = box.get("teams") or {}
            for side in ("away", "home"):
                players = (teams.get(side) or {}).get("players") or {}
                if isinstance(players, dict):
                    for _k, pobj in players.items():
                        if not isinstance(pobj, dict):
                            continue
                        person = pobj.get("person") or {}
                        pid = _safe_int(person.get("id"))
                        nm = str(person.get("fullName") or "")
                        if pid and nm:
                            name_lookup[int(pid)] = nm
                            row = player_meta.setdefault(int(pid), {"id": int(pid)})
                            row.setdefault("name", nm)
                            row.setdefault("side", side)
                            pos = _pos_abbr(pobj)
                            if pos and not row.get("pos"):
                                row["pos"] = pos
                            batting_order = pobj.get("battingOrder")
                            try:
                                order_value = int(str(batting_order)) if batting_order is not None else None
                            except Exception:
                                order_value = None
                            if order_value is not None and row.get("order") is None:
                                row["order"] = int(order_value)
    except Exception:
        pass

    for pid, nm in name_lookup.items():
        if not pid or not nm:
            continue
        row = player_meta.setdefault(int(pid), {"id": int(pid)})
        row.setdefault("name", str(nm))

    pbp_list = ((sim_obj.get("pbp") or {}).get("pbp") or [])
    has_pbp = isinstance(pbp_list, list) and len(pbp_list) > 0
    sim_data = sim_obj.get("sim") or {}
    away_abbr = _first_text((sim_obj.get("away") or {}).get("abbreviation"), (sim_obj.get("away") or {}).get("name"))
    home_abbr = _first_text((sim_obj.get("home") or {}).get("abbreviation"), (sim_obj.get("home") or {}).get("name"))
    prop_models: Dict[str, Dict[str, Dict[str, Any]]] = {"pitchers": {}, "hitters": {}}
    starter_names = sim_obj.get("starter_names") or {}
    starters = sim_obj.get("starters") or {}
    pitcher_props = sim_data.get("pitcher_props") or {}
    for side in ("away", "home"):
        starter_name = _first_text(starter_names.get(side))
        starter_id = _safe_int(starters.get(side))
        if not starter_name or starter_id is None:
            continue
        pred = pitcher_props.get(str(int(starter_id)))
        if not isinstance(pred, dict):
            continue
        normalized = normalize_pitcher_name(starter_name)
        if not normalized:
            continue
        prop_models["pitchers"][normalized] = {
            "name": starter_name,
            "team": away_abbr if side == "away" else home_abbr,
            "team_side": side,
            "model": dict(pred),
        }

    hitter_props = sim_data.get("hitter_props") or {}
    if isinstance(hitter_props, dict):
        for _player_id, pred in hitter_props.items():
            if not isinstance(pred, dict):
                continue
            hitter_name = _first_text(pred.get("name"))
            team = _first_text(pred.get("team"))
            normalized = normalize_pitcher_name(hitter_name)
            if not normalized or not hitter_name:
                continue
            side = "away" if team == away_abbr else ("home" if team == home_abbr else "")
            prop_models["hitters"][normalized] = {
                "name": hitter_name,
                "team": team,
                "team_side": side,
                "model": dict(pred),
            }

    sim_count = _safe_int(sim_data.get("sims"))
    aggregate_boxscore_present = isinstance(sim_data.get("aggregate_boxscore"), dict)
    sim_boxscore = _sim_boxscore_from_aggregate_means(sim_obj, player_meta=player_meta)
    boxscore_mode = "aggregate" if sim_boxscore is not None else None
    pitching_scope = ("full_staff" if aggregate_boxscore_present else "starters_only") if sim_boxscore is not None else None
    if sim_boxscore is None:
        sim_boxscore = _sim_boxscore_from_sim_boxscore(sim_obj, player_meta=player_meta)
        if sim_boxscore is not None:
            boxscore_mode = "representative"
            pitching_scope = None
    if sim_boxscore is None:
        sim_boxscore = _sim_boxscore_from_pbp(
            sim_obj,
            name_lookup={pid: (m.get("name") or "") for pid, m in player_meta.items() if m.get("name")},
        )
        boxscore_mode = "representative_pbp"
        pitching_scope = None

    return {
        "gamePk": int(game_pk),
        "date": d,
        "found": True,
        "sourceFile": _relative_path_str(source_path),
        "simCount": sim_count,
        "away": sim_obj.get("away") or {},
        "home": sim_obj.get("home") or {},
        "predicted": _sim_predicted_score(sim_obj),
        "predictedMode": "aggregate_mean",
        "hasPbp": bool(has_pbp),
        "note": None if has_pbp else "sim_output_has_no_pbp",
        "propModels": prop_models,
        "boxscoreMode": boxscore_mode,
        "pitchingScope": pitching_scope,
        "boxscore": sim_boxscore,
    }


def _live_matchup_text(snapshot: Optional[Dict[str, Any]]) -> str:
    if not isinstance(snapshot, dict):
        return ""
    current = snapshot.get("current") or {}
    inning = current.get("inning")
    half = str(current.get("halfInning") or "").title()
    batter = ((current.get("batter") or {}).get("fullName") or "")
    pitcher = ((current.get("pitcher") or {}).get("fullName") or "")
    pieces = []
    if inning:
        pieces.append(f"{half} {inning}".strip())
    count = current.get("count") or {}
    balls = _safe_int(count.get("balls"))
    strikes = _safe_int(count.get("strikes"))
    outs = _safe_int(count.get("outs"))
    if balls is not None and strikes is not None and outs is not None:
        pieces.append(f"{balls}-{strikes}, {outs} out")
    if batter and pitcher:
        pieces.append(f"{batter} vs {pitcher}")
    return " | ".join(piece for piece in pieces if piece)


def _load_live_lens_cards(d: str) -> List[Dict[str, Any]]:
    artifacts = _load_cards_artifacts(d)
    archive = _load_cards_archive_context(d)

    if isinstance(artifacts.get("locked_policy"), dict):
        recos_by_game = _recommendations_by_game(artifacts.get("locked_policy"))
    elif isinstance(archive.get("card"), dict):
        recos_by_game = _recommendations_by_game(archive.get("card"))
    else:
        recos_by_game = {}

    if isinstance(artifacts.get("game_summary"), dict):
        outputs_by_game = _game_outputs_by_game(artifacts.get("game_summary"))
    elif isinstance(archive.get("report"), dict):
        outputs_by_game = _season_report_outputs_by_game(archive.get("report"))
    else:
        outputs_by_game = {}

    schedule_games: List[Dict[str, Any]] = []
    try:
        schedule_games = fetch_schedule_for_date(_client(), d) or []
    except Exception:
        schedule_games = []

    return _cards_list_from_sources(
        d=d,
        schedule_games=schedule_games,
        outputs_by_game=outputs_by_game,
        recos_by_game=recos_by_game,
    )


def _load_live_lens_snapshot(game_pk: int, d: str) -> Optional[Dict[str, Any]]:
    try:
        use_archive = _is_historical_date(d)
        feed = _load_game_feed_for_date(int(game_pk), d) if use_archive else None
        if not isinstance(feed, dict) or not feed:
            feed = fetch_game_feed_live(_client(), int(game_pk))
        if not isinstance(feed, dict) or not feed:
            return None
        away_sp = _get_box_starting_pitcher_id(feed, "away")
        home_sp = _get_box_starting_pitcher_id(feed, "home")
        return {
            "gamePk": int(game_pk),
            "status": (feed.get("gameData") or {}).get("status") or {},
            "current": _current_matchup(feed),
            "teams": {
                "away": {
                    "starter": {"id": away_sp, "name": _player_name_from_box(feed, away_sp) if away_sp else ""},
                    "totals": _team_totals(feed, "away"),
                    "boxscore": {
                        "batting": _boxscore_batting(feed, "away"),
                        "pitching": _boxscore_pitching(feed, "away"),
                    },
                },
                "home": {
                    "starter": {"id": home_sp, "name": _player_name_from_box(feed, home_sp) if home_sp else ""},
                    "totals": _team_totals(feed, "home"),
                    "boxscore": {
                        "batting": _boxscore_batting(feed, "home"),
                        "pitching": _boxscore_pitching(feed, "home"),
                    },
                },
            },
        }
    except Exception:
        return None


def _sim_boxscore_rows(sim_context: Optional[Dict[str, Any]], side: str, kind: str) -> List[Dict[str, Any]]:
    return (((((sim_context or {}).get("boxscore") or {}).get("teams") or {}).get(side) or {}).get(kind) or [])


def _prop_model_mean_value(reco: Dict[str, Any], sim_row: Optional[Dict[str, Any]]) -> Optional[float]:
    value = _live_stat_value(sim_row, reco)
    if value is not None:
        return value
    if str(reco.get("market") or "") == "pitcher_props":
        return _safe_float(reco.get("outs_mean"))
    return None


def _build_game_lens(card: Dict[str, Any], snapshot: Optional[Dict[str, Any]], sim_context: Optional[Dict[str, Any]], market_row: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    predicted = (sim_context or {}).get("predicted") or {}
    pregame_away = _safe_float(predicted.get("away"))
    pregame_home = _safe_float(predicted.get("home"))
    away_score = _safe_float((((snapshot or {}).get("teams") or {}).get("away") or {}).get("totals", {}).get("R"))
    home_score = _safe_float((((snapshot or {}).get("teams") or {}).get("home") or {}).get("totals", {}).get("R"))
    progress = _live_game_progress(snapshot, card)
    predictions = card.get("predictions") or {}
    markets = (market_row or {}).get("markets") or {}
    h2h = markets.get("h2h") if isinstance(markets.get("h2h"), dict) else {}
    spreads = markets.get("spreads") if isinstance(markets.get("spreads"), dict) else {}
    totals = markets.get("totals") if isinstance(markets.get("totals"), dict) else {}

    lanes = [
        {"key": "live", "label": progress.get("label") or "Live", "innings": 9, "baseline": False},
        {"key": "first3", "label": "F3", "innings": 3, "baseline": True},
        {"key": "first5", "label": "F5", "innings": 5, "baseline": True},
        {"key": "first7", "label": "F7", "innings": 7, "baseline": False},
        {"key": "full", "label": "Full Game", "innings": 9, "baseline": True},
    ]
    rows: List[Dict[str, Any]] = []
    for lane in lanes:
        projection = _segment_projection(
            pregame_away=pregame_away,
            pregame_home=pregame_home,
            actual_away=away_score,
            actual_home=home_score,
            progress_fraction=float(progress.get("fraction") or 0.0),
            target_innings=int(lane["innings"]),
        )
        baseline_probs = predictions.get(lane["key"]) if isinstance(predictions.get(lane["key"]), dict) else {}
        baseline_home_prob = None
        if baseline_probs:
            if lane["key"] == "full":
                baseline_home_prob = _safe_float(baseline_probs.get("homeWin"))
            else:
                baseline_home_prob = _safe_float(baseline_probs.get("homeWin"))
                away_prob = _safe_float(baseline_probs.get("awayWin"))
                baseline_home_prob, _ = _normalize_two_way_probs(baseline_home_prob, away_prob)
        model_home_prob = _live_margin_win_prob(projection.get("homeMargin")) if not projection.get("closed") else None

        total_line = _safe_float(totals.get("line"))
        total_over_odds = totals.get("over_odds") or totals.get("overOdds")
        total_under_odds = totals.get("under_odds") or totals.get("underOdds")
        total_edge = None
        total_pick = None
        if projection.get("total") is not None and total_line is not None:
            total_edge = round(float(projection["total"]) - float(total_line), 3)
            total_pick = "over" if total_edge > 0 else ("under" if total_edge < 0 else None)

        spread_line = _safe_float(spreads.get("home_line") or spreads.get("homeLine"))
        spread_home_odds = spreads.get("home_odds") or spreads.get("homeOdds")
        spread_away_odds = spreads.get("away_odds") or spreads.get("awayOdds")
        spread_edge = None
        spread_pick = None
        if projection.get("homeMargin") is not None and spread_line is not None:
            spread_edge = round(float(projection["homeMargin"]) + float(spread_line), 3)
            spread_pick = "home" if spread_edge > 0 else ("away" if spread_edge < 0 else None)

        home_odds = h2h.get("home_odds") or h2h.get("homeOdds")
        away_odds = h2h.get("away_odds") or h2h.get("awayOdds")
        home_prob_market = _american_odds_implied_prob(home_odds)
        away_prob_market = _american_odds_implied_prob(away_odds)
        home_prob_market, away_prob_market = _normalize_two_way_probs(home_prob_market, away_prob_market)
        ml_pick = None
        ml_edge = None
        if model_home_prob is not None and home_prob_market is not None and away_prob_market is not None:
            home_delta = float(model_home_prob) - float(home_prob_market)
            away_delta = (1.0 - float(model_home_prob)) - float(away_prob_market)
            if abs(home_delta) >= abs(away_delta):
                ml_pick = "home" if home_delta > 0 else None
                ml_edge = round(home_delta, 4) if home_delta > 0 else None
            else:
                ml_pick = "away" if away_delta > 0 else None
                ml_edge = round(away_delta, 4) if away_delta > 0 else None

        rows.append(
            {
                "key": lane["key"],
                "label": lane["label"],
                "closed": bool(projection.get("closed")),
                "projection": projection,
                "progress": progress,
                "baselineHomeWinProb": baseline_home_prob,
                "modelHomeWinProb": model_home_prob,
                "source": "live_projection" if lane["key"] == "live" else "segment_projection",
                "markets": {
                    "moneyline": {
                        "homeOdds": home_odds,
                        "awayOdds": away_odds,
                        "marketHomeProb": home_prob_market,
                        "pick": ml_pick,
                        "edge": ml_edge,
                    },
                    "spread": {
                        "homeLine": spread_line,
                        "homeOdds": spread_home_odds,
                        "awayOdds": spread_away_odds,
                        "pick": spread_pick,
                        "edge": spread_edge,
                    },
                    "total": {
                        "line": total_line,
                        "overOdds": total_over_odds,
                        "underOdds": total_under_odds,
                        "pick": total_pick,
                        "edge": total_edge,
                    },
                },
            }
        )
    return rows


def _prop_lens_rows(card: Dict[str, Any], snapshot: Optional[Dict[str, Any]], sim_context: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    status_text = ((snapshot or {}).get("status") or {}).get("abstractGameState") or ((card.get("status") or {}).get("abstract") or "")
    progress_fraction = float((_live_game_progress(snapshot, card).get("fraction") or 0.0))
    rows: List[Dict[str, Any]] = []
    actual_teams = ((snapshot or {}).get("teams") or {})
    for key, tier in (("pitcherProps", "official"), ("hitterProps", "official"), ("extraPitcherProps", "playable"), ("extraHitterProps", "playable")):
        for reco in card.get("markets", {}).get(key) or []:
            if not isinstance(reco, dict):
                continue
            if not _prop_price_allowed(reco.get("odds"), max_favorite_odds=-200):
                continue
            owner_name = _prop_owner_name(reco)
            if not owner_name:
                continue
            side = _prop_side(card, reco)
            is_pitcher = str(reco.get("market") or "") == "pitcher_props"
            type_key = "pitching" if is_pitcher else "batting"
            search_sets: List[Any] = []
            sim_sets: List[Any] = []
            if side in {"away", "home"}:
                search_sets.append((((actual_teams.get(side) or {}).get("boxscore") or {}).get(type_key) or []))
                sim_sets.append(_sim_boxscore_rows(sim_context, side, type_key))
            else:
                search_sets.append((((actual_teams.get("away") or {}).get("boxscore") or {}).get(type_key) or []))
                search_sets.append((((actual_teams.get("home") or {}).get("boxscore") or {}).get(type_key) or []))
                sim_sets.append(_sim_boxscore_rows(sim_context, "away", type_key))
                sim_sets.append(_sim_boxscore_rows(sim_context, "home", type_key))
            actual_row = None
            for row_set in search_sets:
                actual_row = _lookup_boxscore_row(row_set, owner_name)
                if actual_row:
                    break
            sim_row = None
            for row_set in sim_sets:
                sim_row = _lookup_boxscore_row(row_set, owner_name)
                if sim_row:
                    break
            actual_value = _live_stat_value(actual_row, reco)
            model_mean = _prop_model_mean_value(reco, sim_row)
            live_projection = _project_live_value(actual_value, model_mean, progress_fraction)
            market_line = _safe_float(reco.get("market_line"))
            rows.append(
                {
                    "tier": tier,
                    "market": reco.get("market"),
                    "prop": reco.get("prop"),
                    "playerName": owner_name,
                    "teamSide": side,
                    "selection": reco.get("selection"),
                    "line": market_line,
                    "actual": actual_value,
                    "modelMean": model_mean,
                    "liveProjection": live_projection,
                    "liveEdge": (float(live_projection) - float(market_line)) if live_projection is not None and market_line is not None else None,
                    "delta": (float(actual_value) - float(market_line)) if actual_value is not None and market_line is not None else None,
                    "status": _prop_result_state(reco, actual_value, status_text),
                    "edge": _safe_float(reco.get("edge")),
                    "odds": _safe_int(reco.get("odds")),
                    "modelProbOver": _safe_float(reco.get("model_prob_over")),
                    "outsMean": _safe_float(reco.get("outs_mean")),
                    "marketLabel": reco.get("market_label") or reco.get("prop") or reco.get("market"),
                }
            )
    rows.sort(
        key=lambda row: (
            0 if row.get("tier") == "official" else 1,
            -abs(_safe_float(row.get("liveEdge")) or 0.0),
            -(_safe_float(row.get("edge")) or -999.0),
            str(row.get("playerName") or ""),
        )
    )
    return rows


def _live_lens_payload(d: str, *, persist: bool = False) -> Dict[str, Any]:
    cards = _load_live_lens_cards(d)
    artifacts = _load_cards_artifacts(d)
    archive = _load_cards_archive_context(d)
    game_line_index = _load_game_line_market_index(d)
    games_out: List[Dict[str, Any]] = []
    counts = {
        "games": 0,
        "live": 0,
        "final": 0,
        "pregame": 0,
        "props": 0,
    }
    for card in cards:
        game_pk = _safe_int(card.get("gamePk"))
        if not game_pk:
            continue
        snapshot = _load_live_lens_snapshot(int(game_pk), d)
        sim_context = _load_sim_context_for_game(int(game_pk), d, artifacts=artifacts, archive=archive)
        status = ((snapshot or {}).get("status") or {})
        status_abstract = str(status.get("abstractGameState") or ((card.get("status") or {}).get("abstract") or ""))
        prop_rows = _prop_lens_rows(card, snapshot, sim_context if sim_context.get("found") else None)
        game_lens = _build_game_lens(card, snapshot, sim_context if sim_context.get("found") else None, _game_line_market_for_card(card, game_line_index))
        if status_abstract.lower() == "live":
            counts["live"] += 1
        elif status_abstract.lower() == "final":
            counts["final"] += 1
        else:
            counts["pregame"] += 1
        counts["games"] += 1
        counts["props"] += len(prop_rows)
        away_totals = ((((snapshot or {}).get("teams") or {}).get("away") or {}).get("totals") or {})
        home_totals = ((((snapshot or {}).get("teams") or {}).get("home") or {}).get("totals") or {})
        games_out.append(
            {
                "gamePk": int(game_pk),
                "status": {
                    "abstract": status_abstract,
                    "detailed": str(status.get("detailedState") or ((card.get("status") or {}).get("detailed") or "")),
                },
                "startTime": card.get("startTime"),
                "matchup": {
                    "away": card.get("away") or {},
                    "home": card.get("home") or {},
                    "score": {
                        "away": _safe_int(away_totals.get("R")),
                        "home": _safe_int(home_totals.get("R")),
                    },
                    "liveText": _live_matchup_text(snapshot),
                },
                "predictions": card.get("predictions"),
                "gameMarkets": {
                    "totals": ((card.get("markets") or {}).get("totals")),
                    "ml": ((card.get("markets") or {}).get("ml")),
                },
                "gameLens": game_lens,
                "props": prop_rows,
                "simContextAvailable": bool(sim_context.get("found")),
                "snapshotAvailable": bool(snapshot),
            }
        )

    payload = {
        "date": str(d),
        "generatedAt": _local_timestamp_text(),
        "dataRoot": _relative_path_str(_DATA_DIR),
        "liveLensDir": _relative_path_str(_LIVE_LENS_DIR),
        "counts": counts,
        "games": games_out,
    }

    if persist:
        log_entry = {
            "recordedAt": payload.get("generatedAt"),
            "date": payload.get("date"),
            "counts": counts,
            "games": [
                {
                    "gamePk": game.get("gamePk"),
                    "status": ((game.get("status") or {}).get("abstract")),
                    "score": ((game.get("matchup") or {}).get("score")),
                    "liveText": ((game.get("matchup") or {}).get("liveText")),
                    "propCount": len(game.get("props") or []),
                    "topProps": (game.get("props") or [])[:5],
                }
                for game in games_out
            ],
        }
        _append_jsonl(_live_lens_log_path(d), log_entry)
        _write_json_file(_live_lens_report_path(d), payload)

    return payload


def _refresh_oddsapi_markets(d: str, *, overwrite: bool = True) -> Dict[str, Any]:
    recorded_at = _local_now()
    result = fetch_and_write_live_odds_for_date(
        d,
        out_dir=_MARKET_DIR,
        overwrite=overwrite,
    )
    snapshot_dir = _daily_snapshot_dir(d)
    copied: Dict[str, str] = {}
    _ensure_dir(snapshot_dir)
    for key in ("game_lines_path", "pitcher_props_path", "hitter_props_path"):
        source_path = Path(str(result.get(key) or "")).resolve() if result.get(key) else None
        if not source_path or not source_path.exists() or not source_path.is_file():
            continue
        destination = snapshot_dir / source_path.name
        shutil.copy2(source_path, destination)
        copied[source_path.name] = _relative_path_str(destination) or str(destination)
    archived = _archive_oddsapi_refresh_outputs(d, result, recorded_at=recorded_at)
    meta = {
        "recordedAt": _local_timestamp_text(recorded_at),
        "date": str(d),
        "overwrite": bool(overwrite),
        "result": result,
        "copied": copied,
        "archived": archived,
    }
    _write_json_file(_cron_meta_dir() / "latest_refresh_oddsapi.json", meta)
    return {
        "ok": True,
        "date": str(d),
        "marketDir": _relative_path_str(_MARKET_DIR),
        "snapshotDir": _relative_path_str(snapshot_dir),
        "result": result,
        "copied": copied,
        "archived": archived,
    }


def _live_lens_reports_payload(d: str) -> Dict[str, Any]:
    log_path = _live_lens_log_path(d)
    latest_report = _load_json_file(_live_lens_report_path(d)) or {}
    entries = 0
    latest_entry: Optional[Dict[str, Any]] = None
    if log_path.exists() and log_path.is_file():
        try:
            with log_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = str(line).strip()
                    if not text:
                        continue
                    entries += 1
                    try:
                        latest_entry = json.loads(text)
                    except Exception:
                        continue
        except Exception:
            pass
    return {
        "ok": True,
        "date": str(d),
        "logPath": _relative_path_str(log_path),
        "reportPath": _relative_path_str(_live_lens_report_path(d)),
        "entries": int(entries),
        "latestEntry": latest_entry,
        "latestReport": latest_report,
    }


@app.get("/live-lens")
def live_lens_view() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    return render_template("live_lens.html", date=d)


@app.get("/api/live-lens")
def api_live_lens() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    persist = str(request.args.get("persist") or "off").strip().lower() == "on"
    return jsonify(_live_lens_payload(d, persist=persist))


@app.get("/api/cron/ping")
def api_cron_ping() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    return jsonify(
        {
            "ok": True,
            "service": "mlb-betting-v2",
            "time": _local_timestamp_text(),
            "dataRoot": _relative_path_str(_DATA_DIR),
            "liveLensDir": _relative_path_str(_LIVE_LENS_DIR),
        }
    )


@app.get("/api/cron/config")
def api_cron_config() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    return jsonify(
        {
            "ok": True,
            "cronTokenConfigured": bool(_CRON_TOKEN),
            "dataRoot": _relative_path_str(_DATA_DIR),
            "marketDir": _relative_path_str(_MARKET_DIR),
            "dailyDir": _relative_path_str(_DAILY_DIR),
            "liveLensDir": _relative_path_str(_LIVE_LENS_DIR),
        }
    )


@app.get("/api/cron/refresh-oddsapi-markets")
def api_cron_refresh_oddsapi_markets() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    d = str(request.args.get("date") or "").strip() or _today_iso()
    overwrite = str(request.args.get("overwrite") or "on").strip().lower() != "off"
    try:
        return jsonify(_refresh_oddsapi_markets(d, overwrite=overwrite))
    except Exception as exc:
        return jsonify({"ok": False, "date": d, "error": f"{type(exc).__name__}: {exc}"}), 500


@app.get("/api/cron/live-lens-tick")
def api_cron_live_lens_tick() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    d = str(request.args.get("date") or "").strip() or _today_iso()
    try:
        payload = _live_lens_payload(d, persist=True)
        meta = {
            "recordedAt": _local_timestamp_text(),
            "date": str(d),
            "counts": payload.get("counts"),
            "reportPath": _relative_path_str(_live_lens_report_path(d)),
            "logPath": _relative_path_str(_live_lens_log_path(d)),
        }
        _write_json_file(_cron_meta_dir() / "latest_live_lens_tick.json", meta)
        return jsonify({"ok": True, "date": d, "counts": payload.get("counts"), "report": meta})
    except Exception as exc:
        return jsonify({"ok": False, "date": d, "error": f"{type(exc).__name__}: {exc}"}), 500


@app.get("/api/cron/live-lens-reports")
def api_cron_live_lens_reports() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    d = str(request.args.get("date") or "").strip() or _today_iso()
    return jsonify(_live_lens_reports_payload(d))
    for i in range(max(0, since_index), len(all_plays)):
        p = all_plays[i]
        if not isinstance(p, dict):
            continue
        about = p.get("about") or {}
        result = p.get("result") or {}
        matchup = p.get("matchup") or {}
        plays_out.append(
            {
                "atBatIndex": i,
                "inning": about.get("inning"),
                "halfInning": about.get("halfInning"),
                "event": result.get("event"),
                "description": result.get("description"),
                "isScoringPlay": about.get("isScoringPlay"),
                "batter": (matchup.get("batter") or {}).get("fullName"),
                "pitcher": (matchup.get("pitcher") or {}).get("fullName"),
            }
        )
        new_index = i + 1

    return new_index, plays_out


def _client() -> StatsApiClient:
    # Short-ish cache; frontend should reflect changes.
    return StatsApiClient.with_default_cache(ttl_seconds=60)


def _format_start_time_local(game_date_iso: str) -> str:
    """Format StatsAPI gameDate (ISO) into a compact local-time string."""
    try:
        s = str(game_date_iso or "").strip()
        if not s:
            return ""
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        dt_local = dt.astimezone(_USER_TIMEZONE)
        # Windows-friendly: strip leading zero manually.
        return dt_local.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return ""


@app.get("/")
def index() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    return render_template("cards.html", date=d)


@app.get("/pitcher-ladders")
def pitcher_ladders_view() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    prop = _normalize_pitcher_ladder_prop(request.args.get("prop"))
    game = _normalize_game_selector(request.args.get("game"))
    pitcher = _normalize_pitcher_selector(request.args.get("pitcher"))
    sort = _normalize_pitcher_ladder_sort(request.args.get("sort"))
    return render_template(
        "pitcher_ladders.html",
        date=d,
        prop=prop,
        game=game,
        pitcher=pitcher,
        sort=sort,
        prop_options=_pitcher_ladder_prop_options(),
        sort_options=_pitcher_ladder_sort_options(),
    )


@app.get("/hitter-ladders")
def hitter_ladders_view() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    prop = _normalize_hitter_ladder_prop(request.args.get("prop"))
    game = _normalize_game_selector(request.args.get("game"))
    team = _normalize_hitter_team_selector(request.args.get("team"))
    hitter = _normalize_hitter_selector(request.args.get("hitter"))
    sort = _normalize_hitter_ladder_sort(request.args.get("sort"))
    return render_template(
        "hitter_ladders.html",
        date=d,
        prop=prop,
        game=game,
        team=team,
        hitter=hitter,
        sort=sort,
        prop_options=_hitter_ladder_prop_options(),
        sort_options=_hitter_ladder_sort_options(),
    )


@app.get("/season/<int:season>")
def season_view(season: int) -> str:
    d = str(request.args.get("date") or "").strip()
    return render_template("season.html", season=int(season), date=d)


@app.get("/api/cards")
def api_cards() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    artifacts = _load_cards_artifacts(d)
    archive = _load_cards_archive_context(d)
    game_line_index = _load_game_line_market_index(d)

    if isinstance(artifacts.get("locked_policy"), dict):
        recos_by_game = _recommendations_by_game(artifacts.get("locked_policy"))
    elif isinstance(archive.get("card"), dict):
        recos_by_game = _recommendations_by_game(archive.get("card"))
    else:
        recos_by_game = {}

    if isinstance(artifacts.get("game_summary"), dict):
        outputs_by_game = _game_outputs_by_game(artifacts.get("game_summary"))
    elif isinstance(archive.get("report"), dict):
        outputs_by_game = _season_report_outputs_by_game(archive.get("report"))
    else:
        outputs_by_game = {}

    schedule_games: List[Dict[str, Any]] = []
    try:
        schedule_games = fetch_schedule_for_date(_client(), d) or []
    except Exception:
        schedule_games = []

    cards = _cards_list_from_sources(
        d=d,
        schedule_games=schedule_games,
        outputs_by_game=outputs_by_game,
        recos_by_game=recos_by_game,
    )
    for card in cards:
        if not isinstance(card, dict):
            continue
        market_row = _game_line_market_for_card(card, game_line_index)
        card["trackedGameLines"] = (market_row.get("markets") or {}) if isinstance(market_row, dict) else None

    has_legacy_data = bool(artifacts.get("locked_policy") or artifacts.get("game_summary"))
    has_archive_data = bool(archive.get("report") or archive.get("card"))
    view_mode = "legacy_daily" if has_legacy_data else ("season_archive" if has_archive_data else "schedule_only")
    nav = dict(archive.get("nav") or {})
    if not nav:
        nav = _cards_nav_from_schedule(d)
    if not nav:
        nav = {
            "season": _season_from_date_str(d),
            "minDate": None,
            "maxDate": None,
            "prevDate": _shift_iso_date_str(d, -1),
            "nextDate": _shift_iso_date_str(d, 1),
        }

    return jsonify(
        {
            "date": d,
            "hasSampleData": bool(has_legacy_data or has_archive_data),
            "view": {
                "mode": view_mode,
                "season": archive.get("season"),
                "profile": archive.get("profile"),
            },
            "nav": nav,
            "sources": {
                "mode": view_mode,
                "locked_policy": _relative_path_str(artifacts.get("locked_policy_path")),
                "game_summary": _relative_path_str(artifacts.get("game_summary_path")),
                "sim_dir": _relative_path_str(artifacts.get("sim_dir")),
                "snapshot_dir": _relative_path_str(artifacts.get("snapshot_dir")),
                "lineups": _relative_path_str(artifacts.get("lineups_path")),
                "ops_report": _relative_path_str(artifacts.get("ops_report_path")),
                "oddsapi_game_lines": ((artifacts.get("market_availability") or {}).get("gameLines") or {}).get("path"),
                "oddsapi_pitcher_props": ((artifacts.get("market_availability") or {}).get("pitcherProps") or {}).get("path"),
                "oddsapi_hitter_props": ((artifacts.get("market_availability") or {}).get("hitterProps") or {}).get("path"),
                "season_manifest": _relative_path_str(archive.get("manifest_path")),
                "season_report": _relative_path_str(archive.get("report_path")),
                "season_betting_manifest": _relative_path_str(archive.get("betting_manifest_path")),
                "season_card": _relative_path_str(archive.get("card_path")),
            },
            "marketAvailability": artifacts.get("market_availability") or {},
            "lineupHealth": _lineup_health_summary(artifacts.get("lineups_path"), artifacts.get("lineups")),
            "workflow": _workflow_summary(artifacts.get("ops_report_path"), artifacts.get("ops_report")),
            "cards": cards,
        }
    )


@app.get("/api/pitcher-ladders")
def api_pitcher_ladders() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _pitcher_ladders_payload(d, request.args.get("prop"), request.args.get("sort"))
    status_code = 200 if payload.get("found") else 404
    return jsonify(payload), status_code


@app.get("/api/hitter-ladders")
def api_hitter_ladders() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _hitter_ladders_payload(d, request.args.get("prop"))
    status_code = 200 if payload.get("found") else 404
    return jsonify(payload), status_code


@app.get("/api/season/<int:season>")
def api_season_manifest(season: int) -> Response:
    manifest_path, manifest = _load_season_manifest(int(season))
    if not manifest_path or not isinstance(manifest, dict):
        return jsonify(
            {
                "season": int(season),
                "found": False,
                "error": "season_manifest_missing",
            }
        ), 404

    payload = dict(manifest)
    meta = dict(payload.get("meta") or {})
    sources = dict(meta.get("sources") or {})
    sources["manifest"] = _relative_path_str(manifest_path)
    meta["sources"] = sources
    payload["meta"] = meta
    payload["found"] = True
    return jsonify(payload)


@app.get("/api/season/<int:season>/betting-cards")
def api_season_betting_cards(season: int) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    profile_name, manifest_path, manifest, available_profiles = _load_season_betting_manifest(
        int(season),
        requested_profile,
    )
    if not manifest_path or not isinstance(manifest, dict):
        return jsonify(
            {
                "season": int(season),
                "profile": profile_name,
                "found": False,
                "available_profiles": available_profiles,
                "error": "season_betting_cards_missing",
            }
        ), 404

    payload = dict(manifest)
    meta = dict(payload.get("meta") or {})
    sources = dict(meta.get("sources") or {})
    sources["manifest"] = _relative_path_str(manifest_path)
    meta["sources"] = sources
    payload["meta"] = meta
    payload["profile"] = profile_name
    payload["available_profiles"] = available_profiles
    payload["found"] = True
    return jsonify(payload)


@app.get("/api/season/<int:season>/betting-cards/day/<date_str>")
def api_season_betting_cards_day(season: int, date_str: str) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    payload = _season_betting_day_payload(int(season), str(date_str), requested_profile)
    if payload.get("found"):
        card_path = _path_from_maybe_relative(payload.get("card_source"))
        payload["card"] = _load_json_file(card_path)
        return jsonify(payload)

    status_code = 500 if payload.get("error") == "season_betting_day_settle_failed" else 404
    return jsonify(payload), status_code


@app.get("/api/season/<int:season>/day/<date_str>")
def api_season_day(season: int, date_str: str) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    manifest_path, manifest = _load_season_manifest(int(season))
    if not manifest_path or not isinstance(manifest, dict):
        return jsonify(
            {
                "season": int(season),
                "date": str(date_str),
                "found": False,
                "error": "season_manifest_missing",
            }
        ), 404

    report_path = _resolve_season_day_report_path(manifest, str(date_str))
    if not report_path or not report_path.exists() or not report_path.is_file():
        return jsonify(
            {
                "season": int(season),
                "date": str(date_str),
                "found": False,
                "error": "season_day_missing",
            }
        ), 404

    report_obj = _load_json_file(report_path)
    if not isinstance(report_obj, dict):
        return jsonify(
            {
                "season": int(season),
                "date": str(date_str),
                "found": False,
                "error": "season_day_read_failed",
            }
        ), 500

    payload = _season_day_payload(
        season=int(season),
        season_manifest=manifest,
        day_report=report_obj,
        report_path=report_path,
        betting_profile=requested_profile,
    )
    payload["found"] = True
    payload["manifest_source"] = _relative_path_str(manifest_path)
    return jsonify(payload)


@app.get("/game/<int:game_pk>")
def game_view(game_pk: int) -> str:
    d = str(request.args.get("date") or "").strip() or _today_iso()
    today = _today_iso()
    is_historical = _is_historical_date(d)
    return render_template(
        "game.html",
        game_pk=int(game_pk),
        date=d,
        pitcher_ladders_href=f"/pitcher-ladders?date={d}",
        hitter_ladders_href=f"/hitter-ladders?date={d}",
        today=today,
        is_historical=is_historical,
        stream_enabled=(str(d) == str(today)),
    )


@app.get("/api/schedule")
def api_schedule() -> Response:
    d = str(request.args.get("date") or "").strip() or _today_iso()
    c = _client()
    games = fetch_schedule_for_date(c, d)
    out: List[Dict[str, Any]] = []
    for g in games or []:
        try:
            game_pk = int(g.get("gamePk") or 0)
        except Exception:
            continue
        if game_pk <= 0:
            continue
        away_side = ((g.get("teams") or {}).get("away") or {})
        home_side = ((g.get("teams") or {}).get("home") or {})
        away = _team_from_schedule(away_side)
        home = _team_from_schedule(home_side)
        status = (g.get("status") or {})
        out.append(
            {
                "gamePk": game_pk,
                "gameType": str(g.get("gameType") or ""),
                "gameDate": str(g.get("gameDate") or ""),
                "officialDate": str(g.get("officialDate") or d),
                "status": {
                    "abstract": str(status.get("abstractGameState") or ""),
                    "detailed": str(status.get("detailedState") or ""),
                },
                "away": {"id": away.id, "abbr": away.abbr, "name": away.name, "logo": _mlb_logo_url(away.id)},
                "home": {"id": home.id, "abbr": home.abbr, "name": home.name, "logo": _mlb_logo_url(home.id)},
                "probable": {
                    "away": _probable_pitcher_from_schedule(away_side),
                    "home": _probable_pitcher_from_schedule(home_side),
                },
            }
        )
    return jsonify({"date": d, "games": out})


@app.get("/api/game/<int:game_pk>/snapshot")
def api_game_snapshot(game_pk: int) -> Response:
    d = str(request.args.get("date") or "").strip()
    use_archive = _is_historical_date(d)
    feed = _load_game_feed_for_date(int(game_pk), d) if use_archive else None
    if not isinstance(feed, dict) or not feed:
        c = _client()
        feed = fetch_game_feed_live(c, int(game_pk))
    if not isinstance(feed, dict) or not feed:
        abort(404)

    away_sp = _get_box_starting_pitcher_id(feed, "away")
    home_sp = _get_box_starting_pitcher_id(feed, "home")

    out = {
        "gamePk": int(game_pk),
        "date": d or None,
        "archived": bool(use_archive),
        "streamAvailable": bool(not use_archive and d == _today_iso()),
        "generatedAt": _local_timestamp_text(),
        "status": (feed.get("gameData") or {}).get("status") or {},
        "current": _current_matchup(feed),
        "teams": {
            "away": {
                "lineup": _lineup_from_box(feed, "away"),
                "starter": {"id": away_sp, "name": _player_name_from_box(feed, away_sp) if away_sp else ""},
                "totals": _team_totals(feed, "away"),
                "boxscore": {
                    "batting": _boxscore_batting(feed, "away"),
                    "pitching": _boxscore_pitching(feed, "away"),
                },
            },
            "home": {
                "lineup": _lineup_from_box(feed, "home"),
                "starter": {"id": home_sp, "name": _player_name_from_box(feed, home_sp) if home_sp else ""},
                "totals": _team_totals(feed, "home"),
                "boxscore": {
                    "batting": _boxscore_batting(feed, "home"),
                    "pitching": _boxscore_pitching(feed, "home"),
                },
            },
        },
    }
    return jsonify(out)


@app.get("/api/game/<int:game_pk>/stream")
def api_game_stream(game_pk: int) -> Response:
    """Server-Sent Events stream of play-by-play logs and periodic stat snapshots."""

    d = str(request.args.get("date") or "").strip()
    if _is_historical_date(d):
        archived_feed = _load_game_feed_for_date(int(game_pk), d)
        if not isinstance(archived_feed, dict) or not archived_feed:
            return Response("event: error\ndata: {}\n\n", mimetype="text/event-stream", headers={"Cache-Control": "no-cache"})

        def archived_gen() -> Generator[str, None, None]:
            last_idx, plays = _plays_since(archived_feed, since_index=0)
            if last_idx >= 0 and plays:
                yield "event: plays\n" + "data: " + json.dumps({"plays": plays}) + "\n\n"

            away_bat = _boxscore_batting(archived_feed, "away")
            home_bat = _boxscore_batting(archived_feed, "home")
            away_pit = _boxscore_pitching(archived_feed, "away")
            home_pit = _boxscore_pitching(archived_feed, "home")
            matchup = _current_matchup(archived_feed)
            yield (
                "event: stats\n"
                + "data: "
                + json.dumps(
                    {
                        "away": _team_totals(archived_feed, "away"),
                        "home": _team_totals(archived_feed, "home"),
                        "status": (archived_feed.get("gameData") or {}).get("status") or {},
                        "current": matchup,
                        "boxscore": {
                            "away": {"batting": away_bat, "pitching": away_pit},
                            "home": {"batting": home_bat, "pitching": home_pit},
                        },
                    }
                )
                + "\n\n"
            )
            yield "event: end\n" + "data: " + json.dumps({"archived": True, "date": d}) + "\n\n"

        return Response(archived_gen(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache"})

    def gen() -> Generator[str, None, None]:
        c = _client()
        last_idx = 0
        last_seen = {
            "away_bat": set(),
            "home_bat": set(),
            "away_pit": set(),
            "home_pit": set(),
            "pitcher_id": None,
            "away_pos": {},
            "home_pos": {},
        }
        seeded = False
        while True:
            try:
                feed = fetch_game_feed_live(c, int(game_pk))
                if not isinstance(feed, dict) or not feed:
                    yield "event: error\n" + "data: {}\n\n"
                    time.sleep(2.0)
                    continue

                last_idx, plays = _plays_since(feed, since_index=last_idx)
                if plays:
                    yield "event: plays\n" + "data: " + json.dumps({"plays": plays}) + "\n\n"

                # Detect roster/boxscore changes and pitcher changes.
                away_bat = _boxscore_batting(feed, "away")
                home_bat = _boxscore_batting(feed, "home")
                away_pit = _boxscore_pitching(feed, "away")
                home_pit = _boxscore_pitching(feed, "home")
                away_pos = _positions_from_box(feed, "away")
                home_pos = _positions_from_box(feed, "home")

                away_bat_ids = {int(r.get("id") or 0) for r in away_bat if int(r.get("id") or 0) > 0}
                home_bat_ids = {int(r.get("id") or 0) for r in home_bat if int(r.get("id") or 0) > 0}
                away_pit_ids = {int(r.get("id") or 0) for r in away_pit if int(r.get("id") or 0) > 0}
                home_pit_ids = {int(r.get("id") or 0) for r in home_pit if int(r.get("id") or 0) > 0}

                if not seeded:
                    last_seen["away_bat"] = away_bat_ids
                    last_seen["home_bat"] = home_bat_ids
                    last_seen["away_pit"] = away_pit_ids
                    last_seen["home_pit"] = home_pit_ids
                    last_seen["away_pos"] = away_pos
                    last_seen["home_pos"] = home_pos
                    seeded = True
                else:

                    added = {
                        "away_batting": sorted(list(away_bat_ids - last_seen["away_bat"])),
                        "home_batting": sorted(list(home_bat_ids - last_seen["home_bat"])),
                        "away_pitching": sorted(list(away_pit_ids - last_seen["away_pit"])),
                        "home_pitching": sorted(list(home_pit_ids - last_seen["home_pit"])),
                    }
                    if any(added.values()):
                        def _name(pid: int) -> str:
                            return _player_name_from_box(feed, pid)

                        payload = {
                            "added": {
                                k: [{"id": pid, "name": _name(pid)} for pid in v]
                                for k, v in added.items()
                                if v
                            }
                        }
                        yield "event: changes\n" + "data: " + json.dumps(payload) + "\n\n"

                    last_seen["away_bat"] = away_bat_ids
                    last_seen["home_bat"] = home_bat_ids
                    last_seen["away_pit"] = away_pit_ids
                    last_seen["home_pit"] = home_pit_ids

                    # Position changes (defensive swaps/subs): pid seen before but pos changed.
                    pos_changes: List[Dict[str, Any]] = []
                    for side, cur_map in (("away", away_pos), ("home", home_pos)):
                        prev_map = last_seen.get(f"{side}_pos") or {}
                        if not isinstance(prev_map, dict):
                            prev_map = {}
                        for pid, cur_pos in cur_map.items():
                            prev_pos = prev_map.get(pid)
                            if prev_pos and cur_pos and prev_pos != cur_pos:
                                pos_changes.append(
                                    {
                                        "side": side,
                                        "id": int(pid),
                                        "name": _player_name_from_box(feed, int(pid)),
                                        "from": str(prev_pos),
                                        "to": str(cur_pos),
                                    }
                                )
                    if pos_changes:
                        yield "event: position_change\n" + "data: " + json.dumps({"changes": pos_changes}) + "\n\n"

                    last_seen["away_pos"] = away_pos
                    last_seen["home_pos"] = home_pos

                matchup = _current_matchup(feed)
                cur_pitcher_id = None
                try:
                    cur_pitcher_id = int(((matchup.get("pitcher") or {}).get("id") or 0))
                except Exception:
                    cur_pitcher_id = None
                if cur_pitcher_id and cur_pitcher_id != last_seen.get("pitcher_id"):
                    yield (
                        "event: pitcher_change\n"
                        + "data: "
                        + json.dumps({"pitcher": {"id": cur_pitcher_id, "name": _player_name_from_box(feed, cur_pitcher_id)}})
                        + "\n\n"
                    )
                    last_seen["pitcher_id"] = cur_pitcher_id

                # Always send a small stats heartbeat so the UI can update.
                yield (
                    "event: stats\n"
                    + "data: "
                    + json.dumps(
                        {
                            "away": _team_totals(feed, "away"),
                            "home": _team_totals(feed, "home"),
                            "status": (feed.get("gameData") or {}).get("status") or {},
                            "current": matchup,
                            "boxscore": {
                                "away": {"batting": away_bat, "pitching": away_pit},
                                "home": {"batting": home_bat, "pitching": home_pit},
                            },
                        }
                    )
                    + "\n\n"
                )

                time.sleep(2.0)
            except GeneratorExit:
                return
            except Exception:
                yield "event: error\n" + "data: {}\n\n"
                time.sleep(2.0)

    return Response(gen(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.get("/api/game/<int:game_pk>/sim")
def api_game_sim(game_pk: int) -> Response:
    d = str(request.args.get("date") or "").strip()
    if not d:
        return jsonify({"gamePk": int(game_pk), "found": False, "error": "missing_date"}), 400
    out = _load_sim_context_for_game(int(game_pk), d)
    if out.get("found"):
        snapshot = _load_live_lens_snapshot(int(game_pk), d)
        live_card = {
            "gamePk": int(game_pk),
            "status": {
                "abstract": str((((snapshot or {}).get("status") or {}).get("abstractGameState") or "")),
            },
        }
        out["livePropRows"] = _current_live_prop_rows(live_card, snapshot, out, d)
        out.pop("propModels", None)
    status = 200 if out.get("found") else 404
    if out.get("error") == "read_failed":
        status = 500
    return jsonify(out), status


if __name__ == "__main__":
    host = str(os.environ.get("HOST") or "0.0.0.0").strip() or "0.0.0.0"
    port_raw = os.environ.get("PORT")
    try:
        port = int(port_raw or 5000)
    except Exception:
        port = 5000

    debug_env = str(os.environ.get("FLASK_DEBUG") or "").strip().lower()
    debug = debug_env in {"1", "true", "yes", "on"}
    app.run(host=host, port=port, debug=debug, threaded=True)
