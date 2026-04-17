from __future__ import annotations

from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
from functools import lru_cache
import copy
import gzip
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from flask import Flask, Response, abort, jsonify, render_template, request

# Ensure the project root (MLB-BettingV2/) is importable when running directly.
_ROOT = Path(__file__).resolve().parents[2]
_WEB_DIR = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import (
    StatsApiClient,
    fetch_person,
    fetch_game_feed_live,
    fetch_person_gamelog,
    fetch_person_season_hitting,
    fetch_person_season_pitching,
    fetch_schedule_date_buckets,
    fetch_schedule_for_date,
)
from sim_engine import BaseState
from sim_engine.data.roster_artifact import roster_from_dict
from sim_engine.live_mc import LiveSituation, estimate_live
from sim_engine.forward_tuning import should_use_forward_tuning
from sim_engine.live_mc import LiveSituation, estimate_live, _forward_live_cfg_kwargs
from sim_engine.live_prop_ranking import predict_live_prop_win_probability
from sim_engine.market_pitcher_props import market_side_probabilities, normalize_pitcher_name
from tools.daily_update_multi_profile import (
    _hitter_bvp_reason,
    _hitter_pitch_mix_reason,
    _hitter_platoon_reason,
    _hitter_statcast_quality_reason,
    _opponent_lineup_reason,
    _pitch_mix_reason,
    _pitcher_bvp_reason,
    _pitcher_statcast_quality_reason,
    _pitcher_workload_reason,
)
from tools.eval.build_season_eval_manifest import build_manifest as build_season_eval_manifest
from tools.eval.build_season_eval_manifest import write_manifest_artifacts as write_season_eval_manifest_artifacts
from tools.oddsapi.fetch_daily_oddsapi_markets import fetch_and_write_live_odds_for_date
from tools.eval.settle_locked_policy_cards import (
    _feed_is_final as _settlement_feed_is_final,
    _load_feed as _load_settlement_feed,
    _player_stats as _settlement_player_stats,
    _settle_card,
    _settle_over_under as _settlement_over_under,
)


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
_CRON_TOKEN = str(
    os.environ.get("MLB_BETTING_CRON_TOKEN")
    or os.environ.get("MLB_CRON_TOKEN")
    or os.environ.get("CRON_TOKEN")
    or ""
).strip()
_USER_TIMEZONE_NAME = str(os.environ.get("MLB_USER_TIMEZONE") or "America/Chicago").strip() or "America/Chicago"
try:
    _USER_TIMEZONE = ZoneInfo(_USER_TIMEZONE_NAME)
except Exception:
    _USER_TIMEZONE = ZoneInfo("America/Chicago")


def _env_int(name: str, default: int, *, minimum: Optional[int] = None) -> int:
    raw = str(os.environ.get(name) or "").strip()
    try:
        value = int(raw or default)
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), int(value))
    return int(value)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _live_lens_optimization_regime(d: Any) -> Dict[str, Any]:
    date_str = str(d or "").strip()
    regime = {
        "date": date_str,
        "kind": "unclassified",
        "label": "Unclassified",
        "isLegacyComparison": False,
        "isCleanBaseline": False,
        "baselineStartDate": "2026-03-31",
        "legacyWindowStartDate": "2026-03-25",
        "recommendedUse": "inspect_only",
    }
    if not date_str:
        return regime
    if "2026-03-25" <= date_str < "2026-03-31":
        regime.update(
            {
                "kind": "legacy_comparison",
                "label": "Legacy comparison",
                "isLegacyComparison": True,
                "recommendedUse": "diagnostic_comparison_only",
            }
        )
        return regime
    if date_str >= "2026-03-31":
        regime.update(
            {
                "kind": "clean_baseline",
                "label": "Clean baseline",
                "isCleanBaseline": True,
                "recommendedUse": "optimization_baseline",
            }
        )
    if should_use_forward_tuning(date_str):
        regime.update(
            {
                "kind": "forward_tuned_live",
                "label": "Forward tuned live",
                "isCleanBaseline": False,
                "forwardTuningStartDate": "2026-04-14",
                "recommendedUse": "post_retune_monitoring",
            }
        )
    return regime


_CARDS_PRESEASON_DEFAULT_WINDOW_DAYS = 21
_LIVE_PROP_MARKET_MAX_AGE_SECONDS = 90
_LIVE_FEED_CACHE_TTL_SECONDS = float(_env_int("MLB_LIVE_FEED_CACHE_TTL_SECONDS", 5, minimum=1))
_LIVE_GAME_MC_SIMS = _env_int("MLB_LIVE_GAME_MC_SIMS", 120, minimum=20)
_LIVE_HITTER_PROP_MIN_MARKET_EDGE = 0.05
_LIVE_PROP_RANKING_CONFIG_PATH = Path(
    str(os.environ.get("MLB_LIVE_PROP_RANKING_CONFIG") or (_ROOT_DIR / "data" / "tuning" / "live_prop_ranking" / "default.json")).strip()
).resolve()
_PERSON_CACHE_MAXSIZE = _env_int("MLB_PERSON_CACHE_MAXSIZE", 1024, minimum=64)
_PERSON_GAMELOG_CACHE_MAXSIZE = _env_int("MLB_PERSON_GAMELOG_CACHE_MAXSIZE", 512, minimum=64)
_PERSON_SEASON_CACHE_MAXSIZE = _env_int("MLB_PERSON_SEASON_CACHE_MAXSIZE", 1024, minimum=64)
_JSON_FILE_CACHE_MAXSIZE = _env_int("MLB_JSON_FILE_CACHE_MAXSIZE", 256, minimum=32)
_JSON_FILE_CACHE_MAX_BYTES = _env_int("MLB_JSON_FILE_CACHE_MAX_BYTES", 786432, minimum=0)
_SCHEDULE_FETCH_CACHE_MAXSIZE = _env_int("MLB_SCHEDULE_FETCH_CACHE_MAXSIZE", 32, minimum=4)
_SCHEDULE_REMOTE_CACHE_TTL_SECONDS = float(_env_int("MLB_SCHEDULE_REMOTE_CACHE_TTL_SECONDS", 15, minimum=5))
_LADDERS_CACHE_TTL_SECONDS = float(_env_int("MLB_LADDERS_CACHE_TTL_SECONDS", 60, minimum=1))
_TOP_PROPS_CACHE_TTL_SECONDS = float(_env_int("MLB_TOP_PROPS_CACHE_TTL_SECONDS", 60, minimum=1))
_CARDS_CACHE_TTL_SECONDS = float(_env_int("MLB_CARDS_CACHE_TTL_SECONDS", 60, minimum=1))
_CARDS_CONTEXT_CACHE_TTL_SECONDS = float(_env_int("MLB_CARDS_CONTEXT_CACHE_TTL_SECONDS", 60, minimum=1))
_LIVE_ROUTE_CACHE_TTL_SECONDS = float(_env_int("MLB_LIVE_ROUTE_CACHE_TTL_SECONDS", 5, minimum=1))
_LIVE_LENS_LOOP_DEFAULT_INTERVAL_SECONDS = 30
_LIVE_LENS_LOOP_MIN_INTERVAL_SECONDS = 5
_LIVE_ODDSAPI_REFRESH_MIN_INTERVAL_SECONDS = 15
_LIVE_LENS_REPORT_REFRESH_DEFAULT_INTERVAL_SECONDS = 120
_LIVE_LENS_REPORT_MAX_AGE_DEFAULT_SECONDS = 180
_LIVE_LENS_LOOP_THREAD: Optional[threading.Thread] = None
_LIVE_LENS_LOOP_LOCK = threading.Lock()
_LIVE_LENS_LOOP_STOP = threading.Event()
_SCHEDULE_REMOTE_CACHE_LOCK = threading.Lock()
_SCHEDULE_REMOTE_CACHE: Dict[str, Tuple[float, Tuple[Dict[str, Any], ...]]] = {}
_LIVE_FEED_CACHE_LOCK = threading.Lock()
_LIVE_FEED_CACHE: Dict[Tuple[str, int], Tuple[float, Dict[str, Any]]] = {}
_PAYLOAD_CACHE_LOCK = threading.Lock()
_PAYLOAD_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_LIVE_PROP_MARKET_REFRESH_LOCK = threading.Lock()
_LIVE_PROP_MARKET_REFRESH_IN_PROGRESS: set[str] = set()
_LIVE_PROP_MARKET_REFRESH_LAST_ATTEMPT: Dict[str, float] = {}
_PITCHER_LADDER_PROPS: Dict[str, Dict[str, Any]] = {
    "strikeouts": {
        "label": "Strikeouts",
        "dist_key": "so_dist",
        "mean_key": "so_mean",
        "market_key": "strikeouts",
        "unit": "K",
        "ladder_min_hit_prob": 0.2,
        "ladder_max_rungs": 4,
    },
    "outs": {
        "label": "Outs Recorded",
        "dist_key": "outs_dist",
        "mean_key": "outs_mean",
        "market_key": "outs",
        "unit": "Outs",
        "ladder_min_hit_prob": 0.24,
        "ladder_max_rungs": 2,
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


@lru_cache(maxsize=1)
def _statsapi_client_cached() -> StatsApiClient:
    return StatsApiClient.with_default_cache()


@lru_cache(maxsize=_PERSON_CACHE_MAXSIZE)
def _fetch_person_cached(person_id: int) -> Dict[str, Any]:
    try:
        return fetch_person(_statsapi_client_cached(), int(person_id)) or {}
    except Exception:
        return {}


@lru_cache(maxsize=_PERSON_GAMELOG_CACHE_MAXSIZE)
def _fetch_person_gamelog_cached(person_id: int, season: int, group: str) -> Tuple[Dict[str, Any], ...]:
    try:
        rows = fetch_person_gamelog(_statsapi_client_cached(), int(person_id), int(season), str(group)) or []
    except Exception:
        rows = []
    return tuple(row for row in rows if isinstance(row, dict))


@lru_cache(maxsize=_PERSON_SEASON_CACHE_MAXSIZE)
def _fetch_person_season_cached(person_id: int, season: int, group: str) -> Dict[str, Any]:
    try:
        if str(group) == "pitching":
            return fetch_person_season_pitching(_statsapi_client_cached(), int(person_id), int(season)) or {}
        return fetch_person_season_hitting(_statsapi_client_cached(), int(person_id), int(season)) or {}
    except Exception:
        return {}


def _career_start_season(person_id: int, fallback_season: int) -> int:
    person = _fetch_person_cached(int(person_id))
    debut = str(person.get("mlbDebutDate") or "").strip()
    try:
        if debut:
            return max(2000, min(int(fallback_season), int(debut[:4])))
    except Exception:
        pass
    draft_year = _safe_int(person.get("draftYear"))
    if draft_year is not None:
        return max(2000, min(int(fallback_season), int(draft_year)))
    return max(2000, int(fallback_season) - 6)


def _history_metric_value(group: str, prop: str, stat: Dict[str, Any]) -> Optional[float]:
    if not isinstance(stat, dict):
        return None
    if str(group) == "pitching":
        mapping = {
            "strikeouts": "strikeOuts",
            "outs": "outs",
            "pitches": "numberOfPitches",
            "hits": "hits",
            "earned_runs": "earnedRuns",
            "walks": "baseOnBalls",
            "batters_faced": "battersFaced",
        }
    else:
        mapping = {
            "hits": "hits",
            "home_runs": "homeRuns",
            "total_bases": "totalBases",
            "runs": "runs",
            "rbi": "rbi",
            "doubles": "doubles",
            "triples": "triples",
            "stolen_bases": "stolenBases",
        }
    stat_key = mapping.get(str(prop or "").strip().lower())
    if not stat_key:
        return None
    return _safe_float(stat.get(stat_key))


def _average_metric_from_logs(group: str, prop: str, rows: Sequence[Dict[str, Any]]) -> Optional[float]:
    values: List[float] = []
    for row in rows:
        stat = (row.get("stat") or {}) if isinstance(row, dict) else {}
        metric = _history_metric_value(group, prop, stat)
        if metric is None:
            continue
        values.append(float(metric))
    if not values:
        return None
    return float(sum(values) / float(len(values)))


def _season_average_metric(person_id: int, season: int, group: str, prop: str) -> Optional[float]:
    stat = _fetch_person_season_cached(int(person_id), int(season), str(group))
    if not isinstance(stat, dict) or not stat:
        return None
    metric = _history_metric_value(group, prop, stat)
    if metric is None:
        return None
    games = _safe_float(stat.get("gamesPlayed"))
    if games is None or games <= 0:
        return float(metric)
    return float(metric / games)


def _player_history_summary(person_id: Any, season: Any, group: str, prop: str, opponent_team_id: Any, opponent_label: Any) -> List[Dict[str, Any]]:
    pid = _safe_int(person_id)
    season_i = _safe_int(season)
    opp_id = _safe_int(opponent_team_id)
    if pid is None or season_i is None:
        return []

    current_logs = list(_fetch_person_gamelog_cached(int(pid), int(season_i), str(group)))
    previous_season = int(season_i) - 1
    previous_logs = list(_fetch_person_gamelog_cached(int(pid), int(previous_season), str(group))) if previous_season > 0 else []
    out: List[Dict[str, Any]] = []

    def _append(label: str, value: Optional[float], games: int) -> None:
        if value is None or games <= 0:
            return
        out.append({"label": str(label), "value": round(float(value), 2), "games": int(games)})

    tail5 = current_logs[-5:]
    _append("L5", _average_metric_from_logs(group, prop, tail5), len(tail5))
    tail10 = current_logs[-10:]
    _append("L10", _average_metric_from_logs(group, prop, tail10), len(tail10))
    _append(f"{int(season_i)} avg", _season_average_metric(int(pid), int(season_i), str(group), prop), len(current_logs))
    if previous_season > 0:
        _append(f"{int(previous_season)} avg", _season_average_metric(int(pid), int(previous_season), str(group), prop), len(previous_logs))

    if opp_id is not None:
        opp_logs: List[Dict[str, Any]] = []
        for yr in range(_career_start_season(int(pid), int(season_i)), int(season_i) + 1):
            for row in _fetch_person_gamelog_cached(int(pid), int(yr), str(group)):
                opponent = (row.get("opponent") or {}) if isinstance(row, dict) else {}
                if _safe_int(opponent.get("id")) == int(opp_id):
                    opp_logs.append(row)
        _append(f"Vs {str(opponent_label or 'opp').strip() or 'opp'}", _average_metric_from_logs(group, prop, opp_logs), len(opp_logs))

    return out


def _attach_history_summary(row: Dict[str, Any], *, season: int, group: str, prop: str) -> Dict[str, Any]:
    item = dict(row)
    person_id = row.get("pitcherId") if str(group) == "pitching" else row.get("hitterId")
    history_rows = _player_history_summary(person_id, season, group, prop, row.get("opponentTeamId"), row.get("opponent"))
    if not history_rows:
        return item
    reference = _safe_float(row.get("marketLine"))
    if reference is None:
        reference = _safe_float(row.get("mean"))
    for history_row in history_rows:
        value = _safe_float(history_row.get("value"))
        if value is None or reference is None:
            history_row["trend"] = "neutral"
        elif value >= reference + 0.15:
            history_row["trend"] = "above"
        elif value <= reference - 0.15:
            history_row["trend"] = "below"
        else:
            history_row["trend"] = "neutral"
    item["historyRows"] = history_rows
    return item


def _attach_history_summary_rows(rows: List[Dict[str, Any]], *, season: int, group: str, prop: str) -> List[Dict[str, Any]]:
    return [_attach_history_summary(row, season=season, group=group, prop=prop) for row in rows if isinstance(row, dict)]


def _should_attach_ladder_history(*, selected_player: Any) -> bool:
    return bool(str(selected_player or "").strip())


def _date_slug(d: str) -> str:
    return str(d or "").strip().replace("-", "_")


def _oddsapi_market_root_key(prefix: str) -> str:
    if str(prefix or "").endswith("game_lines"):
        return "games"
    if str(prefix or "").endswith("pitcher_props"):
        return "pitcher_props"
    return "hitter_props"


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


def _format_bytes(num_bytes: Any) -> str:
    try:
        value = float(num_bytes or 0)
    except Exception:
        value = 0.0
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while value >= 1024.0 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    decimals = 0 if idx == 0 else 2
    return f"{value:.{decimals}f} {units[idx]}"


def _safe_file_mtime(path: Path) -> Optional[float]:
    try:
        return float(path.stat().st_mtime)
    except Exception:
        return None


def _collect_tree_usage(root: Path, *, largest_file_limit: int = 20) -> Dict[str, Any]:
    root = Path(root).resolve()
    total_bytes = 0
    total_files = 0
    total_dirs = 0
    largest_files: List[Dict[str, Any]] = []

    if not root.exists():
        return {
            "path": _relative_path_str(root),
            "exists": False,
            "bytes": 0,
            "bytes_text": _format_bytes(0),
            "file_count": 0,
            "dir_count": 0,
            "largest_files": [],
        }

    for current_root, dir_names, file_names in os.walk(root, topdown=True):
        total_dirs += len(dir_names)
        current_dir = Path(current_root)
        for file_name in file_names:
            file_path = current_dir / file_name
            try:
                stat = file_path.stat()
            except Exception:
                continue
            size = int(stat.st_size or 0)
            total_bytes += size
            total_files += 1
            largest_files.append(
                {
                    "path": _relative_path_str(file_path),
                    "bytes": size,
                    "bytes_text": _format_bytes(size),
                    "modified_at": datetime.fromtimestamp(float(stat.st_mtime), tz=_USER_TIMEZONE).isoformat(timespec="seconds"),
                }
            )

    largest_files.sort(key=lambda item: (-int(item.get("bytes") or 0), str(item.get("path") or "")))
    return {
        "path": _relative_path_str(root),
        "exists": True,
        "bytes": int(total_bytes),
        "bytes_text": _format_bytes(total_bytes),
        "file_count": int(total_files),
        "dir_count": int(total_dirs),
        "largest_files": largest_files[: max(1, int(largest_file_limit))],
    }


def _data_disk_report(*, largest_file_limit: int = 20) -> Dict[str, Any]:
    usage = shutil.disk_usage(_DATA_DIR)
    roots = {
        "data": _DATA_DIR,
        "daily": _DAILY_DIR,
        "market": _MARKET_DIR,
        "live_lens": _LIVE_LENS_DIR,
        "eval": _DATA_DIR / "eval",
        "raw": _DATA_DIR / "raw",
        "statcast": _DATA_DIR / "statcast",
    }
    sections: Dict[str, Any] = {}
    for name, path in roots.items():
        sections[name] = _collect_tree_usage(path, largest_file_limit=largest_file_limit)
    top_level = sorted(
        [
            {
                "name": name,
                "path": section.get("path"),
                "bytes": int(section.get("bytes") or 0),
                "bytes_text": section.get("bytes_text"),
                "file_count": int(section.get("file_count") or 0),
            }
            for name, section in sections.items()
        ],
        key=lambda item: (-int(item.get("bytes") or 0), str(item.get("name") or "")),
    )
    return {
        "ok": True,
        "time": _local_timestamp_text(),
        "data_root": _relative_path_str(_DATA_DIR),
        "disk": {
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
            "free_bytes": int(usage.free),
            "total_text": _format_bytes(usage.total),
            "used_text": _format_bytes(usage.used),
            "free_text": _format_bytes(usage.free),
            "used_pct": round((float(usage.used) / float(usage.total) * 100.0), 2) if usage.total else 0.0,
        },
        "sections": sections,
        "top_level": top_level,
        "cleanup_targets": {
            "live-lens": {
                "path": _relative_path_str(_LIVE_LENS_DIR),
                "description": "Daily live-lens logs, reports, prop registry, observation logs, and cron metadata.",
            },
            "market-refresh-history": {
                "path": _relative_path_str(_market_refresh_archive_root()),
                "description": "Archived OddsAPI refresh snapshots copied on each refresh event.",
            },
            "eval-batches": {
                "path": _relative_path_str(_DATA_DIR / "eval" / "batches"),
                "description": "Historical eval batch and tuning run outputs under data/eval/batches.",
            },
            "eval-temp-files": {
                "path": _relative_path_str(_DATA_DIR / "eval"),
                "description": "Top-level disposable eval comparison and temporary files matching _tmp/_compare/_cmp/_attrib prefixes.",
            },
        },
    }


_EVAL_TEMP_FILE_PREFIXES: Tuple[str, ...] = (
    "_tmp_",
    "_compare_",
    "_cmp_",
    "_attrib_",
)


def _cleanup_target_paths(target: str) -> List[Path]:
    normalized = str(target or "live-lens").strip().lower()
    if normalized == "live-lens":
        return [_LIVE_LENS_DIR]
    if normalized == "market-refresh-history":
        return [_market_refresh_archive_root()]
    if normalized == "eval-batches":
        return [_DATA_DIR / "eval" / "batches"]
    if normalized == "eval-temp-files":
        return [_DATA_DIR / "eval"]
    if normalized == "eval-ephemeral":
        return [_DATA_DIR / "eval" / "batches", _DATA_DIR / "eval"]
    if normalized == "all":
        return [_LIVE_LENS_DIR, _market_refresh_archive_root()]
    raise ValueError(f"unsupported_cleanup_target: {normalized}")


def _should_skip_cleanup_path(path: Path, *, root: Path, target: str, include_today: bool) -> bool:
    normalized = str(target or "").strip().lower()
    resolved = path.resolve()
    if resolved == root.resolve():
        return True
    try:
        relative = resolved.relative_to(root.resolve())
    except Exception:
        return True
    parts = tuple(str(part) for part in relative.parts)
    if normalized == "eval-temp-files":
        if len(parts) != 1:
            return True
        filename = str(path.name or "")
        if not any(filename.startswith(prefix) for prefix in _EVAL_TEMP_FILE_PREFIXES):
            return True
    if parts and parts[0] in {"cron_meta", "recaps"}:
        return True
    if include_today:
        return False
    today_slug = _date_slug(_today_iso())
    return today_slug in resolved.name or today_slug in "/".join(parts)


def _cleanup_old_files(
    *,
    target: str,
    retention_days: int,
    apply_changes: bool,
    include_today: bool,
    prune_empty_dirs: bool,
    largest_file_limit: int = 20,
) -> Dict[str, Any]:
    normalized_target = str(target or "live-lens").strip().lower() or "live-lens"
    cutoff = _local_now().timestamp() - max(0, int(retention_days)) * 86400
    candidate_roots = _cleanup_target_paths(normalized_target)
    deleted_files: List[Dict[str, Any]] = []
    deleted_dirs: List[str] = []
    bytes_reclaimed = 0
    scanned_files = 0
    kept_recent = 0
    skipped_today = 0

    for root in candidate_roots:
        if not root.exists():
            continue
        for current_root, dir_names, file_names in os.walk(root, topdown=False):
            current_dir = Path(current_root)
            for file_name in file_names:
                file_path = current_dir / file_name
                if _should_skip_cleanup_path(file_path, root=root, target=normalized_target, include_today=include_today):
                    skipped_today += 1
                    continue
                scanned_files += 1
                modified = _safe_file_mtime(file_path)
                if modified is None or modified >= cutoff:
                    kept_recent += 1
                    continue
                try:
                    size = int(file_path.stat().st_size or 0)
                except Exception:
                    size = 0
                deleted_files.append(
                    {
                        "path": _relative_path_str(file_path),
                        "bytes": size,
                        "bytes_text": _format_bytes(size),
                        "modified_at": datetime.fromtimestamp(float(modified), tz=_USER_TIMEZONE).isoformat(timespec="seconds"),
                    }
                )
                bytes_reclaimed += size
                if apply_changes:
                    try:
                        file_path.unlink(missing_ok=True)
                    except Exception:
                        continue
            can_prune_dir = prune_empty_dirs and normalized_target != "eval-temp-files"
            if can_prune_dir and current_dir != root and not any(current_dir.iterdir()):
                if apply_changes:
                    try:
                        current_dir.rmdir()
                        deleted_dirs.append(_relative_path_str(current_dir) or str(current_dir))
                    except Exception:
                        pass
                else:
                    deleted_dirs.append(_relative_path_str(current_dir) or str(current_dir))

    deleted_files.sort(key=lambda item: (-int(item.get("bytes") or 0), str(item.get("path") or "")))
    return {
        "ok": True,
        "target": normalized_target,
        "apply": bool(apply_changes),
        "retention_days": int(retention_days),
        "include_today": bool(include_today),
        "prune_empty_dirs": bool(prune_empty_dirs),
        "time": _local_timestamp_text(),
        "data_root": _relative_path_str(_DATA_DIR),
        "scanned_files": int(scanned_files),
        "kept_recent_files": int(kept_recent),
        "skipped_today_files": int(skipped_today),
        "candidate_delete_count": int(len(deleted_files)),
        "candidate_delete_bytes": int(bytes_reclaimed),
        "candidate_delete_bytes_text": _format_bytes(bytes_reclaimed),
        "deleted_files": deleted_files[: max(1, int(largest_file_limit))],
        "deleted_dirs": deleted_dirs[: max(1, int(largest_file_limit))],
        "post_cleanup_disk": _data_disk_report(largest_file_limit=10).get("disk"),
    }


def _live_lens_daily_recap_dir() -> Path:
    return _ensure_dir(_LIVE_LENS_DIR / "recaps")


def _live_lens_daily_recap_path(d: str) -> Path:
    return _live_lens_daily_recap_dir() / f"live_lens_daily_recap_{_date_slug(d)}.json"


def _file_stat_summary(path: Path) -> Dict[str, Any]:
    try:
        stat = path.stat()
    except Exception:
        return {
            "path": _relative_path_str(path),
            "exists": False,
            "bytes": 0,
            "bytes_text": _format_bytes(0),
        }
    return {
        "path": _relative_path_str(path),
        "exists": True,
        "bytes": int(stat.st_size or 0),
        "bytes_text": _format_bytes(int(stat.st_size or 0)),
        "modified_at": datetime.fromtimestamp(float(stat.st_mtime), tz=_USER_TIMEZONE).isoformat(timespec="seconds"),
    }


def _live_lens_artifact_paths(d: str) -> Dict[str, Path]:
    return {
        "log": _live_lens_log_path(d),
        "report": _live_lens_report_path(d),
        "registry": _live_prop_registry_path(d),
        "registry_log": _live_prop_registry_log_path(d),
        "observation_log": _live_prop_observation_log_path(d),
        "daily_recap": _live_lens_daily_recap_path(d),
    }


def _live_lens_log_snapshot(d: str) -> Tuple[int, Optional[Dict[str, Any]]]:
    log_path = _live_lens_log_path(d)
    entries = 0
    latest_entry: Optional[Dict[str, Any]] = None
    if not log_path.exists() or not log_path.is_file():
        return entries, latest_entry
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
        return 0, None
    return int(entries), latest_entry if isinstance(latest_entry, dict) else None


def _live_lens_daily_recap_payload(d: str) -> Dict[str, Any]:
    artifact_paths = _live_lens_artifact_paths(d)
    latest_report = _load_json_file(artifact_paths.get("report")) or {}
    registry_summary: Dict[str, Any] = {}
    registry_path = artifact_paths.get("registry")
    if isinstance(registry_path, Path) and registry_path.exists():
        registry_summary = _live_prop_registry_summary(d)
    entries, latest_entry = _live_lens_log_snapshot(d)
    files = {name: _file_stat_summary(path) for name, path in artifact_paths.items() if name != "daily_recap"}
    total_bytes = sum(int((item or {}).get("bytes") or 0) for item in files.values())
    latest_entry_games = []
    if isinstance(latest_entry, dict):
        latest_entry_games = [row for row in (latest_entry.get("games") or []) if isinstance(row, dict)][:20]
    report_counts = (latest_report.get("counts") or {}) if isinstance(latest_report.get("counts"), dict) else {}
    report_performance = (latest_report.get("performance") or {}) if isinstance(latest_report.get("performance"), dict) else {}
    return {
        "ok": True,
        "date": str(d),
        "generatedAt": _local_timestamp_text(),
        "source": "daily_recap",
        "optimizationRegime": _live_lens_optimization_regime(d),
        "dailyRecapPath": _relative_path_str(_live_lens_daily_recap_path(d)),
        "logPath": _relative_path_str(artifact_paths.get("log")),
        "propObservationLogPath": _relative_path_str(artifact_paths.get("observation_log")),
        "registryPath": _relative_path_str(artifact_paths.get("registry")),
        "registryLogPath": _relative_path_str(artifact_paths.get("registry_log")),
        "reportPath": _relative_path_str(artifact_paths.get("report")),
        "entries": int(entries),
        "latestEntry": latest_entry,
        "latestReport": latest_report,
        "registrySummary": registry_summary,
        "summary": {
            "counts": report_counts,
            "performance": report_performance,
            "topStable": list((registry_summary.get("topStable") or []))[:10] if isinstance(registry_summary, dict) else [],
            "topEdges": list((registry_summary.get("topEdges") or []))[:10] if isinstance(registry_summary, dict) else [],
            "latestGames": latest_entry_games,
        },
        "rawArtifacts": {
            "total_bytes": int(total_bytes),
            "total_bytes_text": _format_bytes(total_bytes),
            "files": files,
        },
    }


def _compact_live_lens_day(d: str, *, apply_changes: bool) -> Dict[str, Any]:
    artifact_paths = _live_lens_artifact_paths(d)
    recap_path = artifact_paths["daily_recap"]
    recap_payload = _live_lens_daily_recap_payload(d)
    reclaimable_bytes = int((((recap_payload.get("rawArtifacts") or {}).get("total_bytes")) or 0))
    deleted_files: List[str] = []
    if apply_changes:
        _write_json_file(recap_path, recap_payload)
        for key in ("log", "report", "registry", "registry_log", "observation_log"):
            path = artifact_paths.get(key)
            if not isinstance(path, Path) or not path.exists() or not path.is_file():
                continue
            try:
                path.unlink(missing_ok=True)
                deleted_files.append(_relative_path_str(path) or str(path))
            except Exception:
                continue
    return {
        "ok": True,
        "date": str(d),
        "apply": bool(apply_changes),
        "dailyRecapPath": _relative_path_str(recap_path),
        "reclaimable_bytes": int(reclaimable_bytes),
        "reclaimable_bytes_text": _format_bytes(reclaimable_bytes),
        "deleted_files": deleted_files,
        "summary": recap_payload.get("summary") or {},
    }


def _compact_live_lens_days(
    *,
    retention_days: int,
    apply_changes: bool,
    include_today: bool,
    max_days: int = 30,
) -> Dict[str, Any]:
    cutoff = _local_now().timestamp() - max(0, int(retention_days)) * 86400
    candidate_dates: set[str] = set()
    today_str = _today_iso()
    artifact_roots = [_LIVE_LENS_DIR, _ensure_dir(_LIVE_LENS_DIR / "prop_registry")]
    for root in artifact_roots:
        if not root.exists():
            continue
        for path in root.iterdir():
            if not path.is_file():
                continue
            stem = str(path.stem or "")
            parts = stem.split("_")
            if len(parts) < 4:
                continue
            maybe_date = "-".join(parts[-3:])
            if not maybe_date[:4].isdigit():
                continue
            if not include_today and maybe_date == today_str:
                continue
            mtime = _safe_file_mtime(path)
            if mtime is None or mtime >= cutoff:
                continue
            candidate_dates.add(maybe_date)

    days_out: List[Dict[str, Any]] = []
    total_reclaimable = 0
    for date_str in sorted(candidate_dates)[: max(1, int(max_days))]:
        day_result = _compact_live_lens_day(str(date_str), apply_changes=apply_changes)
        total_reclaimable += int(day_result.get("reclaimable_bytes") or 0)
        days_out.append(day_result)
    return {
        "ok": True,
        "apply": bool(apply_changes),
        "retention_days": int(retention_days),
        "include_today": bool(include_today),
        "candidate_days": int(len(candidate_dates)),
        "processed_days": int(len(days_out)),
        "reclaimable_bytes": int(total_reclaimable),
        "reclaimable_bytes_text": _format_bytes(total_reclaimable),
        "days": days_out,
        "post_cleanup_disk": _data_disk_report(largest_file_limit=10).get("disk"),
    }


def _local_now() -> datetime:
    return datetime.now(_USER_TIMEZONE)


def _local_today() -> date:
    return _local_now().date()


def _is_current_local_date(date_str: str) -> bool:
    try:
        return date.fromisoformat(str(date_str or "").strip()) == _local_today()
    except Exception:
        return False


def _cards_cache_ttl_seconds_for_date(d: str) -> float:
    if _is_current_local_date(d):
        return min(float(_CARDS_CACHE_TTL_SECONDS), 15.0)
    return float(_CARDS_CACHE_TTL_SECONDS)


def _cards_context_cache_ttl_seconds_for_date(d: str) -> float:
    if _is_current_local_date(d):
        return min(float(_CARDS_CONTEXT_CACHE_TTL_SECONDS), 15.0)
    return float(_CARDS_CONTEXT_CACHE_TTL_SECONDS)


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


def _is_live_lens_loop_enabled() -> bool:
    return _env_bool("MLB_ENABLE_LIVE_LENS_LOOP", default=False)


def _is_live_lens_background_report_enabled() -> bool:
    return _env_bool("MLB_ENABLE_LIVE_LENS_BACKGROUND_REPORTS", default=True)


def _is_inline_season_manifest_rebuild_enabled() -> bool:
    return _env_bool("MLB_ENABLE_INLINE_SEASON_MANIFEST_REBUILD", default=False)


def _live_lens_loop_interval_seconds() -> int:
    raw = str(os.environ.get("MLB_LIVE_LENS_LOOP_INTERVAL_SECONDS") or "").strip()
    try:
        value = int(raw or _LIVE_LENS_LOOP_DEFAULT_INTERVAL_SECONDS)
    except Exception:
        value = _LIVE_LENS_LOOP_DEFAULT_INTERVAL_SECONDS
    return max(_LIVE_LENS_LOOP_MIN_INTERVAL_SECONDS, int(value))


def _live_oddsapi_refresh_interval_seconds() -> int:
    raw = str(os.environ.get("MLB_LIVE_ODDSAPI_REFRESH_INTERVAL_SECONDS") or "").strip()
    try:
        value = int(raw or _LIVE_PROP_MARKET_MAX_AGE_SECONDS)
    except Exception:
        value = _LIVE_PROP_MARKET_MAX_AGE_SECONDS
    return max(_LIVE_ODDSAPI_REFRESH_MIN_INTERVAL_SECONDS, int(value))


def _live_lens_report_refresh_interval_seconds() -> int:
    raw = str(os.environ.get("MLB_LIVE_LENS_REPORT_REFRESH_INTERVAL_SECONDS") or "").strip()
    try:
        value = int(raw or _LIVE_LENS_REPORT_REFRESH_DEFAULT_INTERVAL_SECONDS)
    except Exception:
        value = _LIVE_LENS_REPORT_REFRESH_DEFAULT_INTERVAL_SECONDS
    return max(int(_live_lens_loop_interval_seconds()), int(value))


def _live_lens_report_max_age_seconds() -> int:
    raw = str(os.environ.get("MLB_LIVE_LENS_REPORT_MAX_AGE_SECONDS") or "").strip()
    default_value = max(
        _LIVE_LENS_REPORT_MAX_AGE_DEFAULT_SECONDS,
        int(_live_lens_report_refresh_interval_seconds()) + int(_live_lens_loop_interval_seconds()) + 15,
    )
    try:
        value = int(raw or default_value)
    except Exception:
        value = default_value
    return max(int(_live_lens_loop_interval_seconds()), int(value))


def _live_lens_loop_thread_alive() -> bool:
    thread = _LIVE_LENS_LOOP_THREAD
    return bool(thread is not None and thread.is_alive())


def _live_lens_loop_status_payload() -> Dict[str, Any]:
    status_path = _cron_meta_dir() / "live_lens_loop_status.json"
    latest_tick_path = _cron_meta_dir() / "latest_live_lens_tick.json"
    latest_oddsapi_refresh_path = _cron_meta_dir() / "latest_refresh_oddsapi.json"
    status = _load_json_file(status_path) or {}
    latest_tick = _load_json_file(latest_tick_path) or {}
    latest_oddsapi_refresh = _load_json_file(latest_oddsapi_refresh_path) or {}
    werkzeug_run_main = str(os.environ.get("WERKZEUG_RUN_MAIN") or "").strip()
    flask_debug = str(os.environ.get("FLASK_DEBUG") or "").strip()
    return {
        "enabled": _is_live_lens_loop_enabled(),
        "backgroundReportEnabled": _is_live_lens_background_report_enabled(),
        "intervalSeconds": int(_live_lens_loop_interval_seconds()),
        "oddsapiRefreshIntervalSeconds": int(_live_oddsapi_refresh_interval_seconds()),
        "threadAlive": _live_lens_loop_thread_alive(),
        "werkzeugRunMain": werkzeug_run_main,
        "flaskDebug": flask_debug,
        "statusPath": _relative_path_str(status_path),
        "latestTickPath": _relative_path_str(latest_tick_path),
        "latestOddsapiRefreshPath": _relative_path_str(latest_oddsapi_refresh_path),
        "status": status,
        "latestTick": latest_tick,
        "latestOddsapiRefresh": latest_oddsapi_refresh,
    }


def _ensure_live_lens_background_loop_running() -> Dict[str, Any]:
    was_alive = _live_lens_loop_thread_alive()
    started = False
    if not was_alive:
        started = start_live_lens_background_loop()
    out = _live_lens_loop_status_payload()
    out["restartAttempted"] = bool(not was_alive)
    out["restartStarted"] = bool(started)
    return out


def _live_lens_log_path(d: str) -> Path:
    return _LIVE_LENS_DIR / f"live_lens_{_date_slug(d)}.jsonl"


def _live_lens_report_path(d: str) -> Path:
    return _LIVE_LENS_DIR / f"live_lens_report_{_date_slug(d)}.json"


def _live_prop_registry_path(d: str) -> Path:
    return _ensure_dir(_LIVE_LENS_DIR / "prop_registry") / f"live_prop_registry_{_date_slug(d)}.json"


def _live_prop_registry_log_path(d: str) -> Path:
    return _ensure_dir(_LIVE_LENS_DIR / "prop_registry") / f"live_prop_registry_{_date_slug(d)}.jsonl"


def _live_prop_observation_log_path(d: str) -> Path:
    return _ensure_dir(_LIVE_LENS_DIR / "prop_registry") / f"live_prop_observations_{_date_slug(d)}.jsonl"


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
        "reasonSummary": str(row.get("reason_summary") or "").strip(),
        "reasons": [
            str(reason).strip()
            for reason in (row.get("reasons") or [])
            if str(reason).strip()
        ],
    }


def _live_prop_snapshot_changed_fields(previous: Dict[str, Any], current: Dict[str, Any]) -> List[str]:
    changed: List[str] = []
    keys = sorted({str(key) for key in (previous or {}).keys()} | {str(key) for key in (current or {}).keys()})
    for key in keys:
        if (previous or {}).get(key) != (current or {}).get(key):
            changed.append(str(key))
    return changed


def _live_prop_observation_event(
    item: Dict[str, Any],
    *,
    d: str,
    key: str,
    recorded_at: str,
    snapshot: Dict[str, Any],
    previous_snapshot: Optional[Dict[str, Any]],
    previous_seen_at: Optional[str],
    entry: Dict[str, Any],
) -> Dict[str, Any]:
    previous = previous_snapshot if isinstance(previous_snapshot, dict) else {}
    changed_fields = _live_prop_snapshot_changed_fields(previous, snapshot)
    return {
        "recordedAt": recorded_at,
        "event": "observed",
        "date": str(d),
        "key": key,
        "gamePk": _safe_int(item.get("game_pk") or item.get("gamePk")),
        "owner": _prop_owner_name(item),
        "market": item.get("market"),
        "prop": item.get("prop"),
        "selection": item.get("selection"),
        "team": item.get("team"),
        "teamSide": item.get("team_side"),
        "rank": _safe_int(item.get("rank")),
        "source": item.get("source"),
        "recommendationTier": item.get("recommendation_tier"),
        "status": {
            "abstract": str(item.get("status_abstract") or ""),
            "detailed": str(item.get("status_detailed") or ""),
        },
        "gameState": {
            "inning": _safe_int(item.get("inning")),
            "halfInning": item.get("half_inning"),
            "outs": _safe_int(item.get("outs")),
            "progressFraction": _safe_float(item.get("progress_fraction")),
            "liveText": item.get("live_text"),
            "score": {
                "away": _safe_int(item.get("score_away")),
                "home": _safe_int(item.get("score_home")),
            },
        },
        "seenCount": int(_safe_int(entry.get("seenCount")) or 0),
        "firstSeenAt": entry.get("firstSeenAt"),
        "previousSeenAt": previous_seen_at,
        "snapshotChanged": bool(changed_fields),
        "changedFields": changed_fields,
        "snapshot": snapshot,
    }


def _load_live_prop_registry(d: str) -> Dict[str, Any]:
    doc = _load_json_file(_live_prop_registry_path(d)) or {}
    entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
    return {
        "date": str(doc.get("date") or d),
        "updatedAt": doc.get("updatedAt"),
        "entries": dict(entries),
    }


def _live_prop_registry_result(selection: Any, market_line: Any, actual_value: Any) -> str:
    line = _safe_float(market_line)
    actual = _safe_float(actual_value)
    side = str(selection or "over").strip().lower()
    if line is None or actual is None:
        return "pending"
    if abs(float(actual) - float(line)) < 1e-9:
        return "push"
    did_win = float(actual) < float(line) if side == "under" else float(actual) > float(line)
    return "win" if did_win else "loss"


def _live_prop_registry_summary(d: str) -> Dict[str, Any]:
    registry = _load_live_prop_registry(d)
    entries = registry.get("entries") if isinstance(registry.get("entries"), dict) else {}
    by_prop: Dict[str, int] = {}
    by_selection: Dict[str, int] = {}
    result_counts: Dict[str, int] = {"win": 0, "loss": 0, "push": 0, "pending": 0}
    unique_games: set[int] = set()
    unique_owners: set[str] = set()
    summarized_rows: List[Dict[str, Any]] = []

    for entry in entries.values():
        if not isinstance(entry, dict):
            continue
        prop = str(entry.get("prop") or "").strip().lower()
        selection = str(entry.get("selection") or "").strip().lower()
        owner = str(entry.get("owner") or "").strip()
        game_pk = _safe_int(entry.get("gamePk"))
        seen_count = int(_safe_int(entry.get("seenCount")) or 0)
        first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
        last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
        market_line = _safe_float(entry.get("marketLine"))
        first_live_edge = _safe_float(first_snapshot.get("liveEdge"))
        last_live_edge = _safe_float(last_snapshot.get("liveEdge"))
        actual_value = _safe_float(last_snapshot.get("actual"))
        result = _live_prop_registry_result(selection, market_line, actual_value)

        if prop:
            by_prop[prop] = int(by_prop.get(prop, 0) + 1)
        if selection:
            by_selection[selection] = int(by_selection.get(selection, 0) + 1)
        result_counts[result] = int(result_counts.get(result, 0) + 1)
        if game_pk is not None:
            unique_games.add(int(game_pk))
        if owner:
            unique_owners.add(owner)

        summarized_rows.append(
            {
                "gamePk": int(game_pk) if game_pk is not None else None,
                "owner": owner,
                "market": str(entry.get("market") or "").strip().lower(),
                "prop": prop,
                "selection": selection,
                "marketLine": market_line,
                "seenCount": seen_count,
                "firstSeenAt": entry.get("firstSeenAt"),
                "lastSeenAt": entry.get("lastSeenAt"),
                "firstSeenLiveEdge": first_live_edge,
                "lastSeenLiveEdge": last_live_edge,
                "actual": actual_value,
                "result": result,
            }
        )

    top_stable = sorted(
        summarized_rows,
        key=lambda row: (
            -int(row.get("seenCount") or 0),
            -abs(float(_safe_float(row.get("lastSeenLiveEdge")) or 0.0)),
            str(row.get("firstSeenAt") or ""),
            str(row.get("owner") or ""),
        ),
    )[:5]
    top_edges = sorted(
        summarized_rows,
        key=lambda row: (
            -abs(float(_safe_float(row.get("lastSeenLiveEdge")) or 0.0)),
            -int(row.get("seenCount") or 0),
            str(row.get("firstSeenAt") or ""),
            str(row.get("owner") or ""),
        ),
    )[:5]

    return {
        "date": str(registry.get("date") or d),
        "updatedAt": registry.get("updatedAt"),
        "totalEntries": int(len(summarized_rows)),
        "uniqueGames": int(len(unique_games)),
        "uniqueOwners": int(len(unique_owners)),
        "settledEntries": int(result_counts.get("win", 0) + result_counts.get("loss", 0) + result_counts.get("push", 0)),
        "resultCounts": result_counts,
        "byProp": dict(sorted(by_prop.items(), key=lambda item: (-int(item[1]), str(item[0])))),
        "bySelection": dict(sorted(by_selection.items(), key=lambda item: (-int(item[1]), str(item[0])))),
        "topStable": top_stable,
        "topEdges": top_edges,
    }


def _enrich_live_prop_rows_with_registry(
    rows: List[Dict[str, Any]],
    d: str,
    *,
    recorded_at: Optional[datetime] = None,
    write_observation_log: bool = False,
) -> List[Dict[str, Any]]:
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
        previous_snapshot = (entry.get("lastSeenSnapshot") if isinstance(entry, dict) and isinstance(entry.get("lastSeenSnapshot"), dict) else None)
        previous_seen_at = entry.get("lastSeenAt") if isinstance(entry, dict) else None
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

        if write_observation_log:
            observation_event = _live_prop_observation_event(
                item,
                d=str(d),
                key=key,
                recorded_at=stamp_text,
                snapshot=snapshot,
                previous_snapshot=previous_snapshot,
                previous_seen_at=previous_seen_at,
                entry=entry,
            )
            if previous_snapshot and observation_event.get("snapshotChanged"):
                _append_jsonl(
                    _live_prop_observation_log_path(d),
                    observation_event,
                )

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
    if path.is_absolute():
        return path

    normalized = raw.replace("\\", "/").lstrip("./")
    candidates: List[Path] = []
    if normalized == "data" or normalized.startswith("data/"):
        suffix = normalized[5:] if normalized.startswith("data/") else ""
        relative_suffix = Path(suffix) if suffix else Path()
        candidates.extend((data_root / relative_suffix) for data_root in _data_roots())
    candidates.append(_ROOT_DIR / Path(normalized or raw))

    seen: set[str] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        if resolved.exists():
            return resolved

    return candidates[0].resolve() if candidates else None


def _relative_path_str(path: Optional[Path]) -> Optional[str]:
    if not path:
        return None
    try:
        return str(path.relative_to(_ROOT_DIR)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _detect_app_build_info() -> Dict[str, Any]:
    commit = (
        str(
            os.environ.get("RENDER_GIT_COMMIT")
            or os.environ.get("GIT_COMMIT")
            or os.environ.get("COMMIT_SHA")
            or ""
        ).strip()
        or None
    )
    branch = (
        str(
            os.environ.get("RENDER_GIT_BRANCH")
            or os.environ.get("GIT_BRANCH")
            or os.environ.get("BRANCH")
            or ""
        ).strip()
        or None
    )

    if commit is None:
        git_dir = _ROOT_DIR / ".git"
        if git_dir.exists():
            try:
                commit = subprocess.check_output(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(_ROOT_DIR),
                    stderr=subprocess.DEVNULL,
                    text=True,
                ).strip() or None
            except Exception:
                commit = None
    if branch is None:
        git_dir = _ROOT_DIR / ".git"
        if git_dir.exists():
            try:
                branch = subprocess.check_output(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                    cwd=str(_ROOT_DIR),
                    stderr=subprocess.DEVNULL,
                    text=True,
                ).strip() or None
            except Exception:
                branch = None

    return {
        "service": str(os.environ.get("RENDER_SERVICE_NAME") or "mlb-betting-v2").strip() or "mlb-betting-v2",
        "commit": commit,
        "branch": branch,
        "root": _relative_path_str(_ROOT_DIR),
        "dataRoot": _relative_path_str(_DATA_DIR),
    }


_APP_BUILD_INFO = _detect_app_build_info()


def _with_app_build(payload: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(payload)
    merged["app"] = dict(_APP_BUILD_INFO)
    return merged


def _jsonify_no_store(payload: Any, status_code: int = 200) -> Response:
    response = jsonify(payload)
    response.status_code = int(status_code)
    response.headers["Cache-Control"] = "no-store, no-cache, max-age=0, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def _resolve_oddsapi_market_file(d: str, prefix: str) -> Optional[Path]:
    slug = _date_slug(d)
    filename = f"{prefix}_{slug}.json"
    preferred: List[Path] = []
    for data_root in _data_roots():
        preferred.append(data_root / "daily" / "snapshots" / str(d) / filename)
        preferred.append(data_root / "market" / "oddsapi" / filename)
    return _find_preferred_available_oddsapi_file(
        preferred=preferred,
        recursive_pattern=f"**/{filename}",
        prefix=prefix,
    )


def _pregame_oddsapi_market_filename(d: str, prefix: str) -> str:
    return f"{prefix}_pregame_{_date_slug(d)}.json"


def _resolve_pregame_oddsapi_market_file(d: str, prefix: str) -> Optional[Path]:
    filename = _pregame_oddsapi_market_filename(d, prefix)
    preferred: List[Path] = []
    for data_root in _data_roots():
        preferred.append(data_root / "daily" / "snapshots" / str(d) / filename)
        preferred.append(data_root / "market" / "oddsapi" / filename)
    return _find_candidate_file(
        preferred=preferred,
        recursive_pattern=f"**/{filename}",
    )


def _freeze_oddsapi_pregame_markets(d: str) -> Dict[str, str]:
    slug = _date_slug(d)
    snapshot_dir = _daily_snapshot_dir(d)
    _ensure_dir(snapshot_dir)
    copied: Dict[str, str] = {}

    for prefix in ("oddsapi_game_lines", "oddsapi_pitcher_props", "oddsapi_hitter_props"):
        source_path = _MARKET_DIR / f"{prefix}_{slug}.json"
        if not source_path.exists() or not source_path.is_file():
            continue

        source_doc = _load_json_file(source_path)
        mode = str((source_doc or {}).get("mode") or "").strip().lower()
        if mode == "live":
            continue

        frozen_name = _pregame_oddsapi_market_filename(d, prefix)
        for destination in (_MARKET_DIR / frozen_name, snapshot_dir / frozen_name):
            shutil.copy2(source_path, destination)
            copied[destination.name] = _relative_path_str(destination) or str(destination)

    return copied


def _resolve_earliest_archived_oddsapi_market_file(d: str, prefix: str) -> Optional[Path]:
    archive_root = _market_refresh_archive_root() / _date_slug(d)
    if not archive_root.exists() or not archive_root.is_dir():
        return None
    filename = f"{prefix}_{_date_slug(d)}.json"
    for child in sorted(path for path in archive_root.iterdir() if path.is_dir()):
        candidate = child / filename
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


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

    now = time.time()
    refresh_key = str(d)
    with _LIVE_PROP_MARKET_REFRESH_LOCK:
        last_attempt = _LIVE_PROP_MARKET_REFRESH_LAST_ATTEMPT.get(refresh_key)
        if refresh_key in _LIVE_PROP_MARKET_REFRESH_IN_PROGRESS:
            return False
        if last_attempt is not None and (float(now) - float(last_attempt)) < float(max_age_seconds):
            return False
        _LIVE_PROP_MARKET_REFRESH_IN_PROGRESS.add(refresh_key)
        _LIVE_PROP_MARKET_REFRESH_LAST_ATTEMPT[refresh_key] = float(now)

    try:
        _refresh_oddsapi_markets(d, overwrite=True)
        return True
    except Exception:
        return False
    finally:
        with _LIVE_PROP_MARKET_REFRESH_LOCK:
            _LIVE_PROP_MARKET_REFRESH_IN_PROGRESS.discard(refresh_key)


def _load_live_lens_feed(game_pk: int, d: str) -> Optional[Dict[str, Any]]:
    try:
        use_archive = _is_historical_date(d)
        cache_key = (str(d), int(game_pk))
        if not use_archive:
            now = time.time()
            with _LIVE_FEED_CACHE_LOCK:
                cached = _LIVE_FEED_CACHE.get(cache_key)
                if cached is not None and (now - float(cached[0])) <= _LIVE_FEED_CACHE_TTL_SECONDS:
                    return cached[1]

        feed = _load_game_feed_for_date(int(game_pk), d) if use_archive else None
        if not isinstance(feed, dict) or not feed:
            feed = fetch_game_feed_live(_client(), int(game_pk))
        if not use_archive and isinstance(feed, dict) and feed:
            now = time.time()
            with _LIVE_FEED_CACHE_LOCK:
                _LIVE_FEED_CACHE[cache_key] = (now, feed)
                expired_keys = [
                    key
                    for key, value in _LIVE_FEED_CACHE.items()
                    if (now - float(value[0])) > (_LIVE_FEED_CACHE_TTL_SECONDS * 4.0)
                ]
                for key in expired_keys:
                    _LIVE_FEED_CACHE.pop(key, None)
        if isinstance(feed, dict) and feed:
            return feed
    except Exception:
        return None
    return None


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


def _find_preferred_available_oddsapi_file(*, preferred: List[Path], recursive_pattern: str, prefix: str) -> Optional[Path]:
    root_key = _oddsapi_market_root_key(prefix)
    seen: set[str] = set()
    fallback: Optional[Path] = None

    def _consider(path: Path) -> Optional[Path]:
        nonlocal fallback
        key = str(path)
        if key in seen:
            return None
        seen.add(key)
        if not path.exists() or not path.is_file():
            return None
        if fallback is None:
            fallback = path
        summary = _market_file_summary(path, root_key=root_key)
        if bool(summary.get("available")):
            return path
        return None

    for path in preferred:
        chosen = _consider(path)
        if chosen is not None:
            return chosen

    for data_dir in _data_roots():
        try:
            for path in sorted(data_dir.glob(recursive_pattern)):
                chosen = _consider(path)
                if chosen is not None:
                    return chosen
        except Exception:
            continue
    return fallback


def _market_file_summary_from_doc(
    path: Optional[Path],
    doc: Any,
    *,
    root_key: str,
    path_override: Optional[str] = None,
) -> Dict[str, Any]:
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
        "path": path_override if path_override is not None else _relative_path_str(path),
        "mode": str((doc or {}).get("mode") or "") if isinstance(doc, dict) else "",
        "retrievedAt": (doc or {}).get("retrieved_at") if isinstance(doc, dict) else None,
        "counts": counts,
    }


def _find_preferred_file(preferred: Sequence[Path]) -> Optional[Path]:
    seen: set[str] = set()
    for p in preferred:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        if p.exists() and p.is_file():
            return p
    return None


@lru_cache(maxsize=_JSON_FILE_CACHE_MAXSIZE)
def _load_json_file_cached(path_str: str, mtime_ns: int, size_bytes: int) -> Optional[Dict[str, Any]]:
    return _read_json_file_dict(path_str)


def _read_json_file_dict(path_str: str) -> Optional[Dict[str, Any]]:
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
    try:
        stat = path.stat()
    except Exception:
        return None
    size_bytes = int(getattr(stat, "st_size", 0))
    if _JSON_FILE_CACHE_MAX_BYTES > 0 and size_bytes > _JSON_FILE_CACHE_MAX_BYTES:
        return _read_json_file_dict(str(path))
    return _load_json_file_cached(str(path), int(getattr(stat, "st_mtime_ns", 0)), size_bytes)


def _path_signature(path: Optional[Path]) -> Tuple[str, bool, int, int]:
    if not isinstance(path, Path):
        return ("", False, 0, 0)
    try:
        stat = path.stat()
    except Exception:
        return (_relative_path_str(path) or str(path), False, 0, 0)
    return (
        _relative_path_str(path) or str(path),
        True,
        int(getattr(stat, "st_mtime_ns", 0)),
        int(getattr(stat, "st_size", 0)),
    )


def _dir_signature(path: Optional[Path], pattern: str = "*.json") -> Tuple[Any, ...]:
    base = _path_signature(path)
    if not isinstance(path, Path) or not path.exists() or not path.is_dir():
        return (base,)
    try:
        children = sorted(child for child in path.glob(pattern) if child.is_file())
    except Exception:
        children = []
    return tuple([base, *(_path_signature(child) for child in children)])


def _payload_cache_get_or_build(
    cache_name: str,
    cache_key: str,
    *,
    signature: Any = None,
    signature_factory: Any = None,
    max_age_seconds: Optional[float] = None,
    builder: Any,
) -> Dict[str, Any]:
    now = time.time()
    full_key = (str(cache_name), str(cache_key))
    with _PAYLOAD_CACHE_LOCK:
        entry = _PAYLOAD_CACHE.get(full_key)
        if entry is not None:
            created_at = float(entry.get("createdAt") or 0.0)
            cached_signature = entry.get("signature")
            age_matches = max_age_seconds is None or (now - created_at) <= float(max_age_seconds)
            if age_matches and isinstance(entry.get("payload"), dict):
                return entry["payload"]
            has_signature_check = signature_factory is not None or signature is not None
            if signature_factory is not None:
                try:
                    signature = signature_factory()
                except Exception:
                    signature = None
            if has_signature_check:
                signature_matches = signature is None or cached_signature == signature
                if signature_matches and isinstance(entry.get("payload"), dict):
                    entry["createdAt"] = now
                    return entry["payload"]

    payload = builder()
    if signature_factory is not None and signature is None:
        try:
            signature = signature_factory()
        except Exception:
            signature = None
    with _PAYLOAD_CACHE_LOCK:
        _PAYLOAD_CACHE[full_key] = {
            "signature": signature,
            "createdAt": now,
            "payload": payload,
        }
        if len(_PAYLOAD_CACHE) > 128:
            oldest_key = min(
                _PAYLOAD_CACHE,
                key=lambda key: float((_PAYLOAD_CACHE.get(key) or {}).get("createdAt") or 0.0),
            )
            if oldest_key != full_key:
                _PAYLOAD_CACHE.pop(oldest_key, None)
    return payload


def _path_age_seconds(path: Optional[Path]) -> Optional[float]:
    if not isinstance(path, Path) or not path.exists() or not path.is_file():
        return None
    try:
        return max(0.0, float(time.time()) - float(path.stat().st_mtime))
    except Exception:
        return None


def _logical_path_str(path: Optional[Path]) -> Optional[str]:
    if not path:
        return None
    try:
        resolved = path.resolve()
    except Exception:
        resolved = path
    for data_root in _data_roots():
        try:
            return str(resolved.relative_to(data_root)).replace("\\", "/")
        except Exception:
            continue
    try:
        return str(resolved.relative_to(_ROOT_DIR)).replace("\\", "/")
    except Exception:
        return str(resolved).replace("\\", "/")


def _load_live_prop_ranking_cfg() -> Optional[Dict[str, Any]]:
    raw = str(_LIVE_PROP_RANKING_CONFIG_PATH or "").strip()
    if not raw or raw.lower() == "off":
        return None
    try:
        path = Path(raw)
        if not path.exists() or not path.is_file():
            return None
        loaded = _load_json_file(path)
        return loaded if isinstance(loaded, dict) else None
    except Exception:
        try:
            app.logger.exception("live prop ranking config load failed: %s", raw)
        except Exception:
            pass
        return None


def _apply_live_prop_ranking_scores(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    cfg: Optional[Dict[str, Any]] = None
    try:
        cfg = _load_live_prop_ranking_cfg()
    except Exception:
        try:
            app.logger.exception("live prop ranking setup failed")
        except Exception:
            pass
        cfg = None
    scored_rows: List[Dict[str, Any]] = []
    ranking_failed = False
    for row in rows:
        item = dict(row)
        if cfg:
            try:
                probability = predict_live_prop_win_probability(item, cfg, prop_key=str(item.get("prop") or ""))
            except Exception:
                probability = None
                if not ranking_failed:
                    ranking_failed = True
                    try:
                        app.logger.exception("live prop ranking failed for current payload; falling back to baseline ordering")
                    except Exception:
                        pass
            if probability is not None:
                item["estimated_win_prob"] = float(probability)
                item["ranking_score"] = float(probability)
        scored_rows.append(item)
    scored_rows.sort(
        key=lambda row: (
            -float(_safe_float(row.get("ranking_score")) or -1.0),
            -float(_safe_float(row.get("edge")) or -999.0),
            -float(_safe_float(row.get("live_edge")) or -999.0),
            -float(_safe_float(row.get("projection_gap")) or -999.0),
            str(_prop_owner_name(row) or ""),
            str(row.get("market") or ""),
        )
    )
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(scored_rows, start=1):
        item = dict(row)
        item["rank"] = int(idx)
        out.append(item)
    return out


def _same_daily_card_path(left: Optional[Path], right: Optional[Path]) -> bool:
    if not left or not right:
        return False
    try:
        if left.resolve() == right.resolve():
            return True
    except Exception:
        pass
    left_logical = _logical_path_str(left)
    right_logical = _logical_path_str(right)
    return bool(left_logical and right_logical and left_logical == right_logical)


def _prefer_newer_file(primary: Optional[Path], challenger: Optional[Path]) -> Optional[Path]:
    if challenger and challenger.exists() and challenger.is_file():
        if not primary or not primary.exists() or not primary.is_file():
            return challenger
        try:
            primary_stat = primary.stat()
            challenger_stat = challenger.stat()
        except Exception:
            return primary
        primary_mtime = int(getattr(primary_stat, "st_mtime_ns", 0) or 0)
        challenger_mtime = int(getattr(challenger_stat, "st_mtime_ns", 0) or 0)
        if challenger_mtime > primary_mtime:
            return challenger
    return primary


def _synthetic_settlement_from_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    settlement: Dict[str, Any] = {
        "_settled_rows": [],
        "_playable_settled_rows": [],
        "_all_settled_rows": [],
        "unresolved_recommendations": [],
        "playable_unresolved_recommendations": [],
    }
    for key in (
        "status",
        "date",
        "card_path",
        "settlement_path",
        "selected_counts",
        "playable_selected_counts",
        "all_selected_counts",
        "results",
        "playable_results",
        "all_results",
    ):
        value = summary.get(key)
        if isinstance(value, dict):
            settlement[key] = dict(value)
        elif value is not None:
            settlement[key] = value
    return settlement


def _prior_day_settlement_from_ops_report(
    ops_report: Optional[Dict[str, Any]],
    *,
    target_date: str,
    target_card_path: Optional[Path],
) -> Optional[Dict[str, Any]]:
    if not isinstance(ops_report, dict):
        return None
    summary = ops_report.get("prior_day_card_settlement")
    if not isinstance(summary, dict):
        summary = ((ops_report.get("stages") or {}).get("prior_day_card_settlement"))
    if not isinstance(summary, dict):
        return None
    if str(summary.get("date") or "").strip() != str(target_date or "").strip():
        return None
    summary_card_path = _path_from_maybe_relative(summary.get("card_path"))
    if target_card_path and summary_card_path and not _same_daily_card_path(summary_card_path, target_card_path):
        return None
    return summary


def _prior_day_settlement_from_ops_candidates(
    ops_paths: Sequence[Optional[Path]],
    *,
    target_date: str,
    target_card_path: Optional[Path],
) -> Tuple[Optional[Path], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    seen: set[str] = set()
    first_report_path: Optional[Path] = None
    first_report_obj: Optional[Dict[str, Any]] = None
    for candidate in ops_paths:
        if not candidate:
            continue
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        ops_report = _load_json_file(resolved)
        if first_report_path is None and isinstance(ops_report, dict):
            first_report_path = resolved
            first_report_obj = ops_report
        summary = _prior_day_settlement_from_ops_report(
            ops_report,
            target_date=str(target_date),
            target_card_path=target_card_path,
        )
        if isinstance(summary, dict):
            return resolved, ops_report, summary
    return first_report_path, first_report_obj, None


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
    return _market_file_summary_from_doc(path, doc, root_key=root_key)


def _extract_game_line_rows(doc: Any) -> List[Dict[str, Any]]:
    rows = (doc or {}).get("games") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _load_game_line_market_context(d: str) -> Dict[str, Any]:
    current_path = _resolve_oddsapi_market_file(d, "oddsapi_game_lines")
    current_doc = _load_json_file(current_path)
    current_mode = str((current_doc or {}).get("mode") or "").strip().lower()
    current_rows = _extract_game_line_rows(current_doc)

    pregame_path = _resolve_pregame_oddsapi_market_file(d, "oddsapi_game_lines")
    pregame_doc = _load_json_file(pregame_path) if pregame_path else None
    pregame_rows = _extract_game_line_rows(pregame_doc) if isinstance(pregame_doc, dict) else []
    pregame_source = _relative_path_str(pregame_path) if pregame_path else None
    if not pregame_rows:
        archived_path = _resolve_earliest_archived_oddsapi_market_file(d, "oddsapi_game_lines")
        archived_doc = _load_json_file(archived_path) if archived_path else None
        archived_rows = _extract_game_line_rows(archived_doc) if isinstance(archived_doc, dict) else []
        if archived_rows:
            pregame_path = archived_path
            pregame_doc = archived_doc
            pregame_rows = archived_rows
            pregame_source = _relative_path_str(archived_path)

    effective_mode = current_mode
    schedule_counts = _schedule_status_counts(d) if current_mode == "live" else {"known": False, "live": 0}
    if current_mode == "live" and bool(schedule_counts.get("known")) and int(schedule_counts.get("live") or 0) <= 0:
        effective_mode = "pregame"

    use_pregame = bool(pregame_rows) and (effective_mode != "live" or not current_rows)
    display_doc = pregame_doc if use_pregame else current_doc
    display_path = pregame_path if use_pregame else current_path
    display_rows = pregame_rows if use_pregame else current_rows
    display_source = pregame_source if use_pregame else _relative_path_str(current_path)

    return {
        "currentPath": current_path,
        "currentDoc": current_doc,
        "currentMode": current_mode,
        "currentRows": current_rows,
        "effectiveMode": effective_mode,
        "displayDoc": display_doc,
        "displayPath": display_path,
        "displayRows": display_rows,
        "displaySource": display_source,
        "pregamePath": pregame_path,
        "pregameDoc": pregame_doc,
        "pregameRows": pregame_rows,
        "pregameSource": pregame_source,
        "scheduleCounts": schedule_counts,
    }


def _load_market_availability(d: str) -> Dict[str, Any]:
    game_ctx = _load_game_line_market_context(d)
    pitcher_ctx = _load_pitcher_ladder_market_context(d)
    hitter_ctx = _load_hitter_ladder_market_context(d)

    game_lines = _market_file_summary_from_doc(
        game_ctx.get("displayPath") if isinstance(game_ctx.get("displayPath"), Path) else None,
        game_ctx.get("displayDoc"),
        root_key="games",
        path_override=str(game_ctx.get("displaySource") or "") or None,
    )
    pitcher_props = _market_file_summary_from_doc(
        pitcher_ctx.get("displayPath") if isinstance(pitcher_ctx.get("displayPath"), Path) else None,
        pitcher_ctx.get("displayDoc"),
        root_key="pitcher_props",
        path_override=str(pitcher_ctx.get("displaySource") or "") or None,
    )
    hitter_props = _market_file_summary_from_doc(
        hitter_ctx.get("displayPath") if isinstance(hitter_ctx.get("displayPath"), Path) else None,
        hitter_ctx.get("displayDoc"),
        root_key="hitter_props",
        path_override=str(hitter_ctx.get("displaySource") or "") or None,
    )

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
    canonical_hr_targets_path = canonical_daily_dir / f"daily_summary_{slug}_hr_targets.json"
    canonical_locked_policy_path = canonical_daily_dir / f"daily_summary_{slug}_locked_policy.json"
    canonical_game_summary_path = canonical_daily_dir / f"daily_summary_{slug}.json"
    canonical_sim_dir = canonical_daily_dir / "sims" / str(d)
    canonical_snapshot_dir = canonical_daily_dir / "snapshots" / str(d)
    tracked_daily_dir = _TRACKED_DATA_DIR / "daily"

    tracked_profile_bundle_path = tracked_daily_dir / f"daily_summary_{slug}_profile_bundle.json"
    tracked_hr_targets_path = tracked_daily_dir / f"daily_summary_{slug}_hr_targets.json"

    profile_bundle_path = _find_preferred_file([
        canonical_profile_bundle_path,
        tracked_profile_bundle_path,
        data_dir / "_tmp_live_subcap_random_day" / f"daily_summary_{slug}_profile_bundle.json",
        data_dir / "_tmp_live_subcap_smoke" / f"daily_summary_{slug}_profile_bundle.json",
    ])
    profile_bundle_path = _prefer_newer_file(profile_bundle_path, tracked_profile_bundle_path)
    profile_bundle = _load_json_file(profile_bundle_path)

    hr_targets_path = None
    hr_targets = None
    if isinstance(profile_bundle, dict):
        hr_targets_path = _path_from_maybe_relative(((profile_bundle.get("hr_targets") or {}).get("artifact_path")))
        hr_targets_path = _prefer_newer_file(hr_targets_path, canonical_hr_targets_path)
        hr_targets_path = _prefer_newer_file(hr_targets_path, tracked_hr_targets_path)
    if not hr_targets_path:
        hr_targets_path = _find_preferred_file([
            canonical_hr_targets_path,
            tracked_hr_targets_path,
        ])
        hr_targets_path = _prefer_newer_file(hr_targets_path, tracked_hr_targets_path)
    hr_targets = _load_json_file(hr_targets_path)

    settlement_path = _find_preferred_file([
        canonical_daily_dir / "settlements" / f"daily_summary_{slug}_locked_policy_settlement.json",
        tracked_daily_dir / "settlements" / f"daily_summary_{slug}_locked_policy_settlement.json",
        data_dir / f"daily_summary_{slug}_locked_policy_settlement.json",
    ])
    settlement = _load_json_file(settlement_path)

    tracked_locked_policy_path = tracked_daily_dir / f"daily_summary_{slug}_locked_policy.json"

    locked_policy_path = _find_preferred_file([
        canonical_locked_policy_path,
        tracked_locked_policy_path,
        data_dir / "_tmp_live_subcap_random_day" / f"daily_summary_{slug}_locked_policy.json",
        data_dir / "_tmp_live_subcap_smoke" / f"daily_summary_{slug}_locked_policy.json",
        data_dir / f"daily_summary_{slug}_locked_policy.json",
    ])
    locked_policy_path = _prefer_newer_file(locked_policy_path, tracked_locked_policy_path)
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
        game_summary_path = _find_preferred_file([
            canonical_game_summary_path,
            tracked_daily_dir / f"daily_summary_{slug}.json",
        ])
    if not sim_dir and canonical_sim_dir.exists() and canonical_sim_dir.is_dir():
        sim_dir = canonical_sim_dir
    if not snapshot_dir and canonical_snapshot_dir.exists() and canonical_snapshot_dir.is_dir():
        snapshot_dir = canonical_snapshot_dir

    preferred_ops_paths: List[Path] = []
    preferred_ops_paths.append(canonical_daily_dir / "ops" / f"daily_ops_{slug}.json")
    preferred_ops_paths.append(tracked_daily_dir / "ops" / f"daily_ops_{slug}.json")
    if game_summary_path:
        preferred_ops_paths.append(game_summary_path.parent / "ops" / f"daily_ops_{slug}.json")
    ops_report_candidates: List[Path] = []
    seen_ops_candidates: set[str] = set()
    for candidate in preferred_ops_paths:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        key = str(resolved)
        if key in seen_ops_candidates:
            continue
        seen_ops_candidates.add(key)
        ops_report_candidates.append(resolved)

    ops_report_path, ops_report, embedded_settlement_summary = _prior_day_settlement_from_ops_candidates(
        ops_report_candidates,
        target_date=str(d),
        target_card_path=locked_policy_path,
    )
    if not isinstance(embedded_settlement_summary, dict):
        next_date = _shift_iso_date_str(str(d), 1)
        if next_date:
            next_slug = _date_slug(next_date)
            next_ops_report_path, next_ops_report, embedded_settlement_summary = _prior_day_settlement_from_ops_candidates(
                [
                    canonical_daily_dir / "ops" / f"daily_ops_{next_slug}.json",
                    tracked_daily_dir / "ops" / f"daily_ops_{next_slug}.json",
                ],
                target_date=str(d),
                target_card_path=locked_policy_path,
            )
            if not ops_report_path and next_ops_report_path is not None:
                ops_report_path = next_ops_report_path
                ops_report = next_ops_report
    if not isinstance(settlement, dict) and isinstance(embedded_settlement_summary, dict):
        embedded_settlement_path = _path_from_maybe_relative(embedded_settlement_summary.get("settlement_path"))
        if embedded_settlement_path and embedded_settlement_path.exists() and embedded_settlement_path.is_file():
            settlement_path = embedded_settlement_path
            settlement = _load_json_file(settlement_path)
        if not isinstance(settlement, dict):
            settlement = _synthetic_settlement_from_summary(embedded_settlement_summary)
            settlement_path = embedded_settlement_path or settlement_path

    lineups_path = (snapshot_dir / "lineups.json") if snapshot_dir else None
    lineups = _load_json_file(lineups_path)
    market_availability = _load_market_availability(d)
    daily_ladders_path, daily_ladders = _load_daily_ladders_artifact(str(d))

    return {
        "profile_bundle_path": profile_bundle_path,
        "profile_bundle": profile_bundle,
        "hr_targets_path": hr_targets_path,
        "hr_targets": hr_targets,
        "embedded_settlement_summary": embedded_settlement_summary,
        "settlement_path": settlement_path,
        "settlement": settlement,
        "locked_policy_path": locked_policy_path,
        "locked_policy": locked_policy,
        "game_summary_path": game_summary_path,
        "game_summary": _load_json_file(game_summary_path),
        "sim_dir": sim_dir,
        "snapshot_dir": snapshot_dir,
        "lineups_path": lineups_path,
        "lineups": lineups,
        "daily_ladders_path": daily_ladders_path,
        "daily_ladders": daily_ladders,
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


def _extract_pitcher_prop_market_lines(doc: Any) -> Dict[str, Dict[str, Dict[str, Any]]]:
    raw = (doc or {}).get("pitcher_props") or {}
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    if not isinstance(raw, dict):
        return out

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
    return out


def _load_pitcher_prop_market_lines(d: str) -> Tuple[Optional[Path], Dict[str, Dict[str, Dict[str, Any]]]]:
    path = _resolve_oddsapi_market_file(d, "oddsapi_pitcher_props")
    doc = _load_json_file(path)
    return path, _extract_pitcher_prop_market_lines(doc)


def _extract_hitter_prop_market_lines(doc: Any) -> Dict[str, Dict[str, Dict[str, Any]]]:
    raw = (doc or {}).get("hitter_props") or {}
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    if not isinstance(raw, dict):
        return out

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
    return out


def _load_hitter_prop_market_lines(d: str) -> Tuple[Optional[Path], Dict[str, Dict[str, Dict[str, Any]]]]:
    path = _resolve_oddsapi_market_file(d, "oddsapi_hitter_props")
    doc = _load_json_file(path)
    return path, _extract_hitter_prop_market_lines(doc)


_MARKET_NAME_FIRST_TOKEN_ALIASES: Dict[str, Tuple[str, ...]] = {
    "chris": ("christopher",),
    "christopher": ("chris",),
    "isiah": ("isaiah",),
    "isaiah": ("isiah",),
    "jeff": ("jeffrey",),
    "jeffrey": ("jeff",),
    "matt": ("matthew",),
    "matthew": ("matt",),
    "mike": ("michael",),
    "michael": ("mike",),
    "nate": ("nathaniel",),
    "nathaniel": ("nate",),
    "nick": ("nicholas",),
    "nicholas": ("nick",),
}


def _market_name_lookup_variants(name: Any) -> List[str]:
    base = normalize_pitcher_name(str(name or ""))
    if not base:
        return []

    out: List[str] = []
    seen: set[str] = set()

    def _add(value: Any) -> None:
        token = normalize_pitcher_name(str(value or ""))
        if token and token not in seen:
            seen.add(token)
            out.append(token)

    _add(base)
    tokens = base.split()
    if len(tokens) >= 2:
        compact_tokens: List[str] = []
        compacted = False
        idx = 0
        while idx < len(tokens):
            token = tokens[idx]
            if len(token) == 1:
                letters: List[str] = []
                inner = idx
                while inner < len(tokens) and len(tokens[inner]) == 1:
                    letters.append(tokens[inner])
                    inner += 1
                if len(letters) >= 2:
                    compact_tokens.append("".join(letters))
                    compacted = True
                    idx = inner
                    continue
            compact_tokens.append(token)
            idx += 1
        if compacted:
            _add(" ".join(compact_tokens))
        first_token = tokens[0]
        if len(first_token) == 2 and first_token.isalpha():
            _add(" ".join(list(first_token) + tokens[1:]))

    for variant in list(out):
        variant_tokens = variant.split()
        if not variant_tokens:
            continue
        for alias in _MARKET_NAME_FIRST_TOKEN_ALIASES.get(variant_tokens[0], ()):
            _add(" ".join([alias] + variant_tokens[1:]))
    return out


def _market_lines_for_name(all_lines: Any, name: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(all_lines, dict):
        return {}
    for variant in _market_name_lookup_variants(name):
        candidate = all_lines.get(variant)
        if isinstance(candidate, dict) and candidate:
            return candidate
    return {}


@lru_cache(maxsize=64)
def _schedule_status_counts(date_str: str) -> Dict[str, Any]:
    counts: Dict[str, Any] = {
        "known": False,
        "games": 0,
        "live": 0,
        "final": 0,
        "pregame": 0,
    }
    d = str(date_str or "").strip()
    if not d:
        return counts
    try:
        schedule_games = fetch_schedule_for_date(_client(), d) or []
    except Exception:
        return counts

    counts["known"] = True
    for game in schedule_games:
        if not isinstance(game, dict):
            continue
        status = game.get("status") or {}
        abstract = str(status.get("abstractGameState") or "")
        if _status_is_live(status):
            counts["live"] += 1
        elif _status_is_final(abstract):
            counts["final"] += 1
        else:
            counts["pregame"] += 1
        counts["games"] += 1
    return counts


def _market_line_stage_entry(market: Any, *, stage: str, source: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(market, dict):
        return None
    line = _safe_float(market.get("line"))
    if line is None:
        return None
    return {
        "stage": str(stage),
        "line": float(line),
        "overOdds": _safe_int(market.get("over_odds")),
        "underOdds": _safe_int(market.get("under_odds")),
        "alternates": list(market.get("alternates") or []),
        "source": str(source or ""),
    }


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


def _first_seen_pitcher_market_lines_from_registry(d: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    registry = _load_live_prop_registry(d)
    entries = registry.get("entries") if isinstance(registry.get("entries"), dict) else {}
    grouped: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

    for entry in entries.values():
        if not isinstance(entry, dict):
            continue
        if str(entry.get("market") or "").strip().lower() != "pitcher_props":
            continue

        owner_key = normalize_pitcher_name(str(entry.get("owner") or ""))
        prop_key = str(entry.get("prop") or "").strip().lower()
        if not owner_key or prop_key not in {"strikeouts", "outs", "earned_runs"}:
            continue

        first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
        line = _safe_float(first_snapshot.get("marketLine"))
        selection = str(first_snapshot.get("selection") or entry.get("selection") or "").strip().lower()
        if line is None or selection not in {"over", "under"}:
            continue

        line_key = f"{float(line):.3f}"
        stamp = str(entry.get("firstSeenAt") or "")
        bucket = grouped.setdefault(owner_key, {}).setdefault(prop_key, {}).setdefault(
            line_key,
            {
                "line": float(line),
                "over_odds": None,
                "under_odds": None,
                "alternates": [],
                "source": "live_registry_first_seen",
                "firstSeenAt": stamp,
            },
        )
        if stamp and (not bucket.get("firstSeenAt") or stamp < str(bucket.get("firstSeenAt"))):
            bucket["firstSeenAt"] = stamp
        bucket[f"{selection}_odds"] = _safe_int(first_snapshot.get("odds"))

    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for owner_key, owner_markets in grouped.items():
        for prop_key, line_groups in owner_markets.items():
            if not isinstance(line_groups, dict) or not line_groups:
                continue
            chosen = min(
                line_groups.values(),
                key=lambda item: (
                    str(item.get("firstSeenAt") or "9999-12-31T23:59:59"),
                    float(_safe_float(item.get("line")) or 999.0),
                ),
            )
            out.setdefault(owner_key, {})[prop_key] = {
                "line": float(_safe_float(chosen.get("line")) or 0.0),
                "over_odds": _safe_int(chosen.get("over_odds")),
                "under_odds": _safe_int(chosen.get("under_odds")),
                "alternates": [],
                "source": str(chosen.get("source") or "live_registry_first_seen"),
            }
    return out


def _load_pitcher_ladder_market_context(d: str) -> Dict[str, Any]:
    current_path = _resolve_oddsapi_market_file(d, "oddsapi_pitcher_props")
    current_doc = _load_json_file(current_path)
    current_mode = str((current_doc or {}).get("mode") or "").strip().lower()
    current_lines = _extract_pitcher_prop_market_lines(current_doc)

    pregame_path = _resolve_pregame_oddsapi_market_file(d, "oddsapi_pitcher_props")
    pregame_doc = _load_json_file(pregame_path) if pregame_path else None
    pregame_lines = _extract_pitcher_prop_market_lines(pregame_doc) if isinstance(pregame_doc, dict) else {}
    pregame_source = _relative_path_str(pregame_path) if pregame_path else None
    if not pregame_lines:
        archived_path = _resolve_earliest_archived_oddsapi_market_file(d, "oddsapi_pitcher_props")
        archived_doc = _load_json_file(archived_path) if archived_path else None
        pregame_lines = _extract_pitcher_prop_market_lines(archived_doc) if isinstance(archived_doc, dict) else {}
        if pregame_lines:
            pregame_source = _relative_path_str(archived_path)
    if not pregame_lines:
        pregame_lines = _first_seen_pitcher_market_lines_from_registry(d)
        if pregame_lines:
            pregame_source = "live_registry_first_seen"

    effective_mode = current_mode
    schedule_counts = _schedule_status_counts(d) if current_mode == "live" else {"known": False, "live": 0}
    if current_mode == "live" and bool(schedule_counts.get("known")) and int(schedule_counts.get("live") or 0) <= 0:
        effective_mode = "pregame"
    use_pregame = bool(pregame_lines) and (effective_mode != "live" or not current_lines)
    display_lines = pregame_lines if use_pregame else current_lines
    display_source = pregame_source if use_pregame else _relative_path_str(current_path)
    display_doc = pregame_doc if use_pregame else current_doc
    display_path = pregame_path if use_pregame else current_path

    return {
        "currentPath": current_path,
        "currentDoc": current_doc,
        "currentMode": current_mode,
        "effectiveMode": effective_mode,
        "currentLines": current_lines,
        "displayLines": display_lines,
        "displayDoc": display_doc,
        "displayPath": display_path,
        "displaySource": display_source,
        "pregamePath": pregame_path,
        "pregameDoc": pregame_doc,
        "pregameLines": pregame_lines,
        "pregameSource": pregame_source,
        "scheduleCounts": schedule_counts,
    }


def _load_hitter_ladder_market_context(d: str) -> Dict[str, Any]:
    current_path = _resolve_oddsapi_market_file(d, "oddsapi_hitter_props")
    current_doc = _load_json_file(current_path)
    current_mode = str((current_doc or {}).get("mode") or "").strip().lower()
    current_lines = _extract_hitter_prop_market_lines(current_doc)

    pregame_path = _resolve_pregame_oddsapi_market_file(d, "oddsapi_hitter_props")
    pregame_doc = _load_json_file(pregame_path) if pregame_path else None
    pregame_lines = _extract_hitter_prop_market_lines(pregame_doc) if isinstance(pregame_doc, dict) else {}
    pregame_source = _relative_path_str(pregame_path) if pregame_path else None
    if not pregame_lines:
        archived_path = _resolve_earliest_archived_oddsapi_market_file(d, "oddsapi_hitter_props")
        archived_doc = _load_json_file(archived_path) if archived_path else None
        pregame_lines = _extract_hitter_prop_market_lines(archived_doc) if isinstance(archived_doc, dict) else {}
        if pregame_lines:
            pregame_source = _relative_path_str(archived_path)

    effective_mode = current_mode
    schedule_counts = _schedule_status_counts(d) if current_mode == "live" else {"known": False, "live": 0}
    if current_mode == "live" and bool(schedule_counts.get("known")) and int(schedule_counts.get("live") or 0) <= 0:
        effective_mode = "pregame"
    use_pregame = bool(pregame_lines) and (effective_mode != "live" or not current_lines)
    display_lines = pregame_lines if use_pregame else current_lines
    display_source = pregame_source if use_pregame else _relative_path_str(current_path)
    display_doc = pregame_doc if use_pregame else current_doc
    display_path = pregame_path if use_pregame else current_path

    return {
        "currentPath": current_path,
        "currentDoc": current_doc,
        "currentMode": current_mode,
        "effectiveMode": effective_mode,
        "currentLines": current_lines,
        "displayLines": display_lines,
        "displayDoc": display_doc,
        "displayPath": display_path,
        "displaySource": display_source,
        "pregamePath": pregame_path,
        "pregameDoc": pregame_doc,
        "pregameLines": pregame_lines,
        "pregameSource": pregame_source,
        "scheduleCounts": schedule_counts,
    }


def _pitcher_market_lines_by_stat(
    markets: Any,
    *,
    pregame_markets: Any = None,
    live_markets: Any = None,
    current_mode: str = "",
) -> List[Dict[str, Any]]:
    if not isinstance(markets, dict) and not isinstance(pregame_markets, dict) and not isinstance(live_markets, dict):
        return []
    out: List[Dict[str, Any]] = []
    for stat_key, cfg in _PITCHER_LADDER_PROPS.items():
        market_key = str(cfg.get("market_key") or "").strip()
        if not market_key:
            continue
        current_market = markets.get(market_key) if isinstance(markets, dict) else None
        pregame_market = pregame_markets.get(market_key) if isinstance(pregame_markets, dict) else None
        live_market = live_markets.get(market_key) if isinstance(live_markets, dict) else None
        entry = _market_line_entry(
            stat_key=str(stat_key),
            label=str(cfg.get("label") or stat_key),
            unit=str(cfg.get("unit") or ""),
            market=live_market or current_market or pregame_market,
        )
        if entry:
            entry["pregame"] = _market_line_stage_entry(
                pregame_market,
                stage="pregame",
                source=(pregame_market or {}).get("source") if isinstance(pregame_market, dict) else None,
            )
            entry["live"] = _market_line_stage_entry(
                live_market,
                stage="live",
                source="oddsapi_live" if str(current_mode or "").lower() == "live" else None,
            )
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


def _normalize_top_props_limit(value: Any) -> int:
    limit = _safe_int(value)
    if limit is None:
        return 15
    return max(10, min(25, int(limit)))


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


def _top_props_market_line_for_stat(entries: Any, stat_key: str) -> Dict[str, Any]:
    if not isinstance(entries, list):
        return {}
    target = str(stat_key or "").strip().lower()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("stat") or "").strip().lower() == target:
            return entry
    return {}


def _top_props_target_label(line: Any, selection: str) -> str:
    line_value = _safe_float(line)
    if line_value is None:
        return ""
    normalized = str(selection or "").strip().lower()
    if normalized == "under":
        max_total = max(0, int(math.ceil(float(line_value)) - 1))
        return "0" if max_total <= 0 else f"{max_total} or fewer"
    required_total = int(math.floor(float(line_value))) + 1
    return f"{required_total}+"


def _top_props_side_choice(*, over_prob: Any, market_entry: Any, allow_under: bool = True) -> Optional[Dict[str, Any]]:
    over_prob_value = _safe_float(over_prob)
    line_value = _safe_float((market_entry or {}).get("line"))
    if over_prob_value is None or line_value is None or not isinstance(market_entry, dict):
        return None

    over_odds = _safe_int(market_entry.get("overOdds"))
    under_odds = _safe_int(market_entry.get("underOdds"))
    side_probs = market_side_probabilities(over_odds, under_odds)
    market_prob_over = _safe_float(side_probs.get("over"))
    market_prob_under = _safe_float(side_probs.get("under"))

    candidates: List[Dict[str, Any]] = []
    if market_prob_over is not None:
        candidates.append(
            {
                "selection": "over",
                "selectionLabel": "Over",
                "targetLabel": _top_props_target_label(line_value, "over"),
                "simProb": float(over_prob_value),
                "marketProb": float(market_prob_over),
                "rawEdge": float(over_prob_value) - float(market_prob_over),
                "odds": over_odds,
                "line": float(line_value),
            }
        )
    if allow_under and market_prob_under is not None:
        under_prob_value = max(0.0, min(1.0, 1.0 - float(over_prob_value)))
        candidates.append(
            {
                "selection": "under",
                "selectionLabel": "Under",
                "targetLabel": _top_props_target_label(line_value, "under"),
                "simProb": float(under_prob_value),
                "marketProb": float(market_prob_under),
                "rawEdge": float(under_prob_value) - float(market_prob_under),
                "odds": under_odds,
                "line": float(line_value),
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            -float(item.get("rawEdge") or 0.0),
            -float(item.get("simProb") or 0.0),
            0 if str(item.get("selection") or "") == "over" else 1,
        )
    )
    return candidates[0]


_TOP_PROPS_HITTER_ACTUAL_KEYS: Dict[str, str] = {
    "hits": "hits",
    "home_runs": "homeRuns",
    "total_bases": "totalBases",
    "runs": "runs",
    "rbi": "rbi",
}


_TOP_PROPS_PITCHER_ACTUAL_KEYS: Dict[str, str] = {
    "strikeouts": "strikeOuts",
    "outs": "outs",
    "earned_runs": "earnedRuns",
}


def _top_props_supports_reconciliation(d: str) -> bool:
    try:
        return date.fromisoformat(str(d or "")) < _local_today()
    except Exception:
        return False


def _top_props_player_stats(
    *,
    feed: Dict[str, Any],
    player_name: str,
    stat_group: str,
    side_hint: str,
    cache: Dict[Tuple[int, str, str, str], Optional[Dict[str, Any]]],
    game_pk: int,
) -> Optional[Dict[str, Any]]:
    normalized_name = str(player_name or "").strip()
    if not normalized_name:
        return None
    candidate_sides = [str(side_hint or "").strip().lower(), "away", "home"]
    seen: set[str] = set()
    for side in candidate_sides:
        if side not in {"away", "home"} or side in seen:
            continue
        seen.add(side)
        cache_key = (int(game_pk), side, normalized_name.lower(), str(stat_group))
        if cache_key not in cache:
            stats = _settlement_player_stats(feed, side, normalized_name, stat_group)
            cache[cache_key] = dict(stats) if isinstance(stats, dict) else None
        if isinstance(cache.get(cache_key), dict):
            return dict(cache[cache_key] or {})
    return None


def _reconcile_top_props_row(
    row: Dict[str, Any],
    *,
    d: str,
    group: str,
    feed_cache: Dict[int, Optional[Dict[str, Any]]],
    stats_cache: Dict[Tuple[int, str, str, str], Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    game_pk = _safe_int(row.get("gamePk"))
    player_name = _first_text(row.get("playerName"), row.get("ownerName"))
    stat_key = str(row.get("stat") or "").strip().lower()
    line_value = _safe_float(row.get("line"))
    side = str(row.get("side") or "").strip().lower()
    if game_pk is None or not player_name or not stat_key or line_value is None:
        return {"status": "unavailable", "label": "Unavailable"}

    feed = feed_cache.get(int(game_pk))
    if int(game_pk) not in feed_cache:
        try:
            feed = _load_settlement_feed(str(d), int(game_pk))
        except Exception:
            feed = None
        feed_cache[int(game_pk)] = dict(feed) if isinstance(feed, dict) else None
    if not isinstance(feed, dict):
        return {"status": "unavailable", "label": "Unavailable"}
    if not _settlement_feed_is_final(feed):
        return {"status": "pending", "label": "Pending"}

    stat_group = "pitching" if str(group) == "pitcher" else "batting"
    actual_key = (_TOP_PROPS_PITCHER_ACTUAL_KEYS if str(group) == "pitcher" else _TOP_PROPS_HITTER_ACTUAL_KEYS).get(stat_key)
    if not actual_key:
        return {"status": "unavailable", "label": "Unavailable"}
    stats = _top_props_player_stats(
        feed=feed,
        player_name=player_name,
        stat_group=stat_group,
        side_hint=side,
        cache=stats_cache,
        game_pk=int(game_pk),
    )
    actual_value = _safe_float((stats or {}).get(actual_key))
    if actual_value is None:
        return {"status": "unavailable", "label": "DNP/scratched"}

    won = _settlement_over_under(float(actual_value), float(line_value), str(row.get("selection") or ""))
    if won is None:
        return {"status": "unavailable", "label": "Unavailable", "actual": float(actual_value)}
    if abs(float(actual_value) - float(line_value)) < 1e-9:
        return {"status": "push", "label": "Push", "actual": float(actual_value)}
    return {
        "status": "win" if bool(won) else "loss",
        "label": "Right" if bool(won) else "Wrong",
        "actual": float(actual_value),
    }


def _reconcile_top_props_sections(sections: List[Dict[str, Any]], *, d: str, group: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not _top_props_supports_reconciliation(d):
        return sections, {"enabled": False}

    feed_cache: Dict[int, Optional[Dict[str, Any]]] = {}
    stats_cache: Dict[Tuple[int, str, str, str], Optional[Dict[str, Any]]] = {}
    result_counts: Dict[str, int] = {"win": 0, "loss": 0, "push": 0, "pending": 0, "unavailable": 0}
    out_sections: List[Dict[str, Any]] = []
    for section in sections:
        rows_out: List[Dict[str, Any]] = []
        section_counts: Dict[str, int] = {"win": 0, "loss": 0, "push": 0, "pending": 0, "unavailable": 0}
        for row in section.get("rows") or []:
            if not isinstance(row, dict):
                continue
            row_out = dict(row)
            reconciliation = _reconcile_top_props_row(
                row_out,
                d=str(d),
                group=str(group),
                feed_cache=feed_cache,
                stats_cache=stats_cache,
            )
            status = str((reconciliation or {}).get("status") or "unavailable")
            row_out["reconciliation"] = dict(reconciliation or {})
            row_out["actual"] = _safe_float((reconciliation or {}).get("actual"))
            result_counts[status] = int(result_counts.get(status) or 0) + 1
            section_counts[status] = int(section_counts.get(status) or 0) + 1
            rows_out.append(row_out)
        section_out = dict(section)
        section_out["rows"] = rows_out
        section_out["reconciliation"] = {
            "enabled": True,
            "resultCounts": dict(section_counts),
            "settledCount": int(section_counts.get("win", 0) + section_counts.get("loss", 0) + section_counts.get("push", 0)),
        }
        out_sections.append(section_out)
    return out_sections, {
        "enabled": True,
        "resultCounts": dict(result_counts),
        "settledCount": int(result_counts.get("win", 0) + result_counts.get("loss", 0) + result_counts.get("push", 0)),
    }


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


def _pitcher_ladder_roster_snapshot(
    d: str,
    sim_path: Path,
    sim_obj: Dict[str, Any],
    snapshot_cache: Dict[Path, Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    if sim_path in snapshot_cache:
        return snapshot_cache.get(sim_path)
    roster_path = _find_roster_snapshot_for_sim(d=str(d), sim_file=sim_path, sim_obj=sim_obj)
    roster_obj = _load_json_file(roster_path) if isinstance(roster_path, Path) else None
    roster_snapshot = roster_obj if isinstance(roster_obj, dict) else None
    snapshot_cache[sim_path] = roster_snapshot
    return roster_snapshot


def _pitcher_ladder_lineup_k_rate(opponent_lineup: List[Dict[str, Any]]) -> Optional[float]:
    values: List[float] = []
    for batter in opponent_lineup:
        if not isinstance(batter, dict):
            continue
        k_rate = _safe_float(batter.get("k_rate"))
        if k_rate is None:
            continue
        values.append(float(k_rate))
    if not values:
        return None
    return float(sum(values) / float(len(values)))


def _pitcher_ladder_pitch_type_k_factor(
    pitcher_profile: Dict[str, Any],
    opponent_lineup: List[Dict[str, Any]],
) -> Optional[float]:
    if not isinstance(pitcher_profile, dict) or not isinstance(opponent_lineup, list):
        return None
    arsenal_map = pitcher_profile.get("arsenal") if isinstance(pitcher_profile.get("arsenal"), dict) else {}
    whiff_map = pitcher_profile.get("pitch_type_whiff_mult") if isinstance(pitcher_profile.get("pitch_type_whiff_mult"), dict) else {}

    pitch_weights: List[Tuple[str, float]] = []
    for pitch_type, usage_raw in arsenal_map.items():
        usage = _safe_float(usage_raw)
        if usage is None or float(usage) <= 0.0:
            continue
        pitch_weights.append((str(pitch_type), float(usage)))
    if not pitch_weights:
        for pitch_type in whiff_map.keys():
            pitch_weights.append((str(pitch_type), 1.0))
    total_weight = float(sum(weight for _, weight in pitch_weights))
    if total_weight <= 0.0:
        return None

    batter_factors: List[float] = []
    for batter in opponent_lineup:
        if not isinstance(batter, dict):
            continue
        vs_pitch_type = batter.get("vs_pitch_type") if isinstance(batter.get("vs_pitch_type"), dict) else {}
        if not isinstance(vs_pitch_type, dict):
            continue
        factor_sum = 0.0
        matched = False
        for pitch_type, weight in pitch_weights:
            whiff_mult = _safe_float(whiff_map.get(pitch_type))
            batter_mult = _safe_float(vs_pitch_type.get(pitch_type))
            whiff_value = float(whiff_mult) if whiff_mult is not None else 1.0
            batter_value = float(batter_mult) if batter_mult is not None else 1.0
            batter_value = max(0.4, min(1.6, batter_value))
            factor_sum += (float(weight) / total_weight) * (whiff_value / batter_value)
            matched = True
        if matched:
            batter_factors.append(float(factor_sum))
    if not batter_factors:
        return None
    return float(sum(batter_factors) / float(len(batter_factors)))


def _pitcher_ladder_bvp_k_context(
    pitcher_id: int,
    opponent_lineup: List[Dict[str, Any]],
) -> Dict[str, Any]:
    total_pa = 0.0
    hitter_matches = 0
    total_so = 0.0
    weighted_k = 0.0
    for batter in opponent_lineup:
        if not isinstance(batter, dict):
            continue
        history_map = batter.get("vs_pitcher_history")
        if not isinstance(history_map, dict):
            continue
        history = history_map.get(str(int(pitcher_id))) if str(int(pitcher_id)) in history_map else history_map.get(int(pitcher_id))
        if not isinstance(history, dict):
            continue
        pa = _safe_float(history.get("pa"))
        if pa is None or float(pa) <= 0.0:
            continue
        total_pa += float(pa)
        hitter_matches += 1
        total_so += float(_safe_float(history.get("so")) or 0.0)
        weighted_k += float(pa) * float(_safe_float(history.get("k_mult")) or 1.0)
    avg_k_mult = float(weighted_k / total_pa) if total_pa > 0.0 else None
    so_rate = float(total_so / total_pa) if total_pa > 0.0 else None
    return {
        "totalPa": total_pa,
        "hitterMatches": int(hitter_matches),
        "totalSo": total_so,
        "avgKMult": avg_k_mult,
        "soRate": so_rate,
    }


def _pitcher_ladder_strikeout_matchup_summary(
    d: str,
    sim_path: Path,
    sim_obj: Dict[str, Any],
    pitcher_side: str,
    pitcher_id: int,
    snapshot_cache: Dict[Path, Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    roster_snapshot = _pitcher_ladder_roster_snapshot(d, sim_path, sim_obj, snapshot_cache)
    if not isinstance(roster_snapshot, dict) or pitcher_side not in {"away", "home"}:
        return None
    opponent_side = "home" if pitcher_side == "away" else "away"
    side_doc = roster_snapshot.get(pitcher_side) if isinstance(roster_snapshot.get(pitcher_side), dict) else None
    opponent_doc = roster_snapshot.get(opponent_side) if isinstance(roster_snapshot.get(opponent_side), dict) else None
    if not isinstance(side_doc, dict) or not isinstance(opponent_doc, dict):
        return None

    pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else None
    opponent_lineup = [row for row in (opponent_doc.get("lineup") or []) if isinstance(row, dict)]
    if not isinstance(pitcher_profile, dict) or not opponent_lineup:
        return None

    lineup_k_rate = _pitcher_ladder_lineup_k_rate(opponent_lineup)
    pitch_type_k_factor = _pitcher_ladder_pitch_type_k_factor(pitcher_profile, opponent_lineup)
    bvp_ctx = _pitcher_ladder_bvp_k_context(int(pitcher_id), opponent_lineup)

    reasons: List[str] = []
    metrics: Dict[str, Any] = {}
    if lineup_k_rate is not None:
        metrics["lineupKRate"] = round(float(lineup_k_rate), 4)
        lineup_pct = 100.0 * float(lineup_k_rate)
        if float(lineup_k_rate) >= 0.235:
            reasons.append(f"Projected lineup baseline K rate is {lineup_pct:.1f}%, which is above neutral for a strikeout ceiling.")
        elif float(lineup_k_rate) <= 0.205:
            reasons.append(f"Projected lineup baseline K rate is only {lineup_pct:.1f}%, so this is a more contact-heavy draw.")
        else:
            reasons.append(f"Projected lineup baseline K rate is {lineup_pct:.1f}%.")
    if pitch_type_k_factor is not None:
        metrics["pitchTypeKFactor"] = round(float(pitch_type_k_factor), 3)
        delta_pct = (float(pitch_type_k_factor) - 1.0) * 100.0
        if float(pitch_type_k_factor) >= 1.04:
            reasons.append(f"Pitch-type matchup grades {delta_pct:.0f}% above neutral on whiff pressure against this lineup.")
        elif float(pitch_type_k_factor) <= 0.96:
            reasons.append(f"Pitch-type matchup grades {abs(delta_pct):.0f}% below neutral on whiff pressure against this lineup.")
    total_pa = float(bvp_ctx.get("totalPa") or 0.0)
    hitter_matches = int(bvp_ctx.get("hitterMatches") or 0)
    avg_k_mult = _safe_float(bvp_ctx.get("avgKMult"))
    total_so = float(bvp_ctx.get("totalSo") or 0.0)
    if total_pa >= 12.0 and hitter_matches >= 2:
        metrics["bvpPa"] = int(round(total_pa))
        metrics["bvpSo"] = int(round(total_so))
        metrics["bvpKFactor"] = round(float(avg_k_mult or 1.0), 3)
        if avg_k_mult is not None and float(avg_k_mult) >= 1.05:
            reasons.append(
                f"Prior matchup history logged {int(round(total_so))} strikeouts in {int(round(total_pa))} PA, a {float(avg_k_mult):.2f}x K multiplier versus baseline."
            )
        elif avg_k_mult is not None and float(avg_k_mult) <= 0.95:
            reasons.append(
                f"Prior matchup history logged only {int(round(total_so))} strikeouts in {int(round(total_pa))} PA, a {float(avg_k_mult):.2f}x K multiplier versus baseline."
            )
    reasons = _dedupe_reason_texts(reasons)
    if not reasons:
        return None
    return {
        "summary": " ".join(reasons[:3]).strip(),
        "reasons": reasons,
        "metrics": metrics,
    }


def _pitcher_ladders_payload(
    d: str,
    prop_value: Any,
    sort_value: Any,
    *,
    selected_game_value: Any = None,
    selected_pitcher_value: Any = None,
) -> Dict[str, Any]:
    prop = _normalize_pitcher_ladder_prop(prop_value)
    if selected_game_value is None:
        selected_game_value = request.args.get("game")
    if selected_pitcher_value is None:
        selected_pitcher_value = request.args.get("pitcher")
    selected_game = _normalize_game_selector(selected_game_value)
    selected_pitcher = _normalize_pitcher_selector(selected_pitcher_value)
    sort_key = _normalize_pitcher_ladder_sort(sort_value)
    prop_cfg = _PITCHER_LADDER_PROPS[prop]
    artifacts = _load_cards_artifacts(d)
    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    market_ctx = _load_pitcher_ladder_market_context(d)
    market_path = market_ctx.get("currentPath") if isinstance(market_ctx.get("currentPath"), Path) else None
    market_lines = market_ctx.get("displayLines") if isinstance(market_ctx.get("displayLines"), dict) else {}
    current_market_lines = market_ctx.get("currentLines") if isinstance(market_ctx.get("currentLines"), dict) else {}
    pregame_market_lines = market_ctx.get("pregameLines") if isinstance(market_ctx.get("pregameLines"), dict) else {}
    market_mode = str(market_ctx.get("effectiveMode") or market_ctx.get("currentMode") or "")
    pregame_market_source = str(market_ctx.get("pregameSource") or "")
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
        "marketSource": str(market_ctx.get("displaySource") or _relative_path_str(market_path) or ""),
        "marketMode": market_mode,
        "pregameMarketSource": pregame_market_source,
        "nav": nav,
        "rows": [],
    }
    if not sim_dir or not sim_dir.exists() or not sim_dir.is_dir():
        payload["error"] = "sim_dir_missing"
        return payload

    sim_files = _list_unique_sim_files(sim_dir)
    roster_snapshot_cache: Dict[Path, Optional[Dict[str, Any]]] = {}
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
            player_market_lines = _market_lines_for_name(market_lines, starter_name)
            player_current_market_lines = _market_lines_for_name(current_market_lines, starter_name)
            player_pregame_market_lines = _market_lines_for_name(pregame_market_lines, starter_name)
            market = {}
            market_key = prop_cfg.get("market_key")
            if market_key:
                market = (player_market_lines.get(str(market_key)) or {})
            pregame_market = (player_pregame_market_lines.get(str(market_key)) or {}) if market_key else {}
            market_line = _safe_float(market.get("line")) if isinstance(market, dict) else None
            pregame_market_line = _safe_float(pregame_market.get("line")) if isinstance(pregame_market, dict) else None
            over_line_count = None
            over_line_prob = None
            if market_line is not None:
                over_line_count = int(sum(row.get("exactCount") or 0 for row in ladder_rows if float(row.get("total") or 0) > float(market_line)))
                over_line_prob = float(over_line_count / float(max(1, sim_count)))
            matchup_summary = None
            if prop == "strikeouts":
                matchup_summary = _pitcher_ladder_strikeout_matchup_summary(
                    str(d),
                    sim_path,
                    sim_obj,
                    str(side),
                    int(starter_id),
                    roster_snapshot_cache,
                )
            mode_row = max(
                ladder_rows,
                key=lambda row: (int(row.get("exactCount") or 0), -int(row.get("total") or 0)),
            )
            row_out = {
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
                "pregameMarketLine": pregame_market_line,
                "marketLinesByStat": _pitcher_market_lines_by_stat(
                    player_market_lines,
                    pregame_markets=player_pregame_market_lines,
                    live_markets=player_current_market_lines if market_mode == "live" else None,
                    current_mode=market_mode,
                ),
                "overLineCount": over_line_count,
                "overLineProb": over_line_prob,
                "ladder": ladder_rows,
                "sourceFile": _relative_path_str(sim_path),
            }
            if isinstance(matchup_summary, dict):
                row_out["matchupSummary"] = str(matchup_summary.get("summary") or "").strip()
                row_out["matchupReasons"] = [
                    str(reason).strip()
                    for reason in (matchup_summary.get("reasons") or [])
                    if str(reason).strip()
                ]
                row_out["matchupMetrics"] = dict(matchup_summary.get("metrics") or {})
            rows.append(row_out)

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

    attach_history = _should_attach_ladder_history(selected_player=selected_pitcher)
    if attach_history:
        rows = _attach_history_summary_rows(rows, season=_season_from_date_str(d), group="pitching", prop=prop)

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
    if rows:
        payload["featuredRow"] = (
            _attach_history_summary(rows[0], season=_season_from_date_str(d), group="pitching", prop=prop)
            if attach_history
            else dict(rows[0])
        )
    payload["historyMode"] = "selected_player" if attach_history else "deferred"
    payload["summary"] = {
        "games": int(len(sim_files)),
        "starters": int(len(rows)),
        "simCounts": sorted({int(row.get("simCount") or 0) for row in rows if int(row.get("simCount") or 0) > 0}),
        "availableGames": int(len(payload.get("gameOptions") or [])),
        "availableStarters": int(len(payload.get("pitcherOptions") or [])),
    }
    return payload


def _hitter_ladders_payload(
    d: str,
    prop_value: Any,
    *,
    selected_game_value: Any = None,
    selected_team_value: Any = None,
    selected_hitter_value: Any = None,
    sort_value: Any = None,
) -> Dict[str, Any]:
    prop = _normalize_hitter_ladder_prop(prop_value)
    if selected_game_value is None:
        selected_game_value = request.args.get("game")
    if selected_team_value is None:
        selected_team_value = request.args.get("team")
    if selected_hitter_value is None:
        selected_hitter_value = request.args.get("hitter")
    if sort_value is None:
        sort_value = request.args.get("sort")
    selected_game = _normalize_game_selector(selected_game_value)
    selected_team = _normalize_hitter_team_selector(selected_team_value)
    selected_hitter = _normalize_hitter_selector(selected_hitter_value)
    sort_key = _normalize_hitter_ladder_sort(sort_value)
    prop_cfg = _HITTER_LADDER_PROPS[prop]
    artifacts = _load_cards_artifacts(d)
    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    market_ctx = _load_hitter_ladder_market_context(d)
    market_path = market_ctx.get("currentPath") if isinstance(market_ctx.get("currentPath"), Path) else None
    market_lines = market_ctx.get("displayLines") if isinstance(market_ctx.get("displayLines"), dict) else {}
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
        "marketSource": str(market_ctx.get("displaySource") or _relative_path_str(market_path) or ""),
        "marketMode": str(market_ctx.get("effectiveMode") or market_ctx.get("currentMode") or ""),
        "nav": nav,
        "rows": [],
    }
    if not sim_dir or not sim_dir.exists() or not sim_dir.is_dir():
        payload["error"] = "sim_dir_missing"
        return payload

    sim_files = _list_unique_sim_files(sim_dir)
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
                player_market_lines = _market_lines_for_name(market_lines, hitter_name)
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
                player_market_lines = _market_lines_for_name(market_lines, hitter_name)
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

    attach_history = _should_attach_ladder_history(selected_player=selected_hitter)
    if attach_history:
        rows = _attach_history_summary_rows(rows, season=_season_from_date_str(d), group="hitting", prop=prop)

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
    if rows:
        payload["featuredRow"] = (
            _attach_history_summary(rows[0], season=_season_from_date_str(d), group="hitting", prop=prop)
            if attach_history
            else dict(rows[0])
        )
    payload["historyMode"] = "selected_player" if attach_history else "deferred"
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


def _pitcher_ladders_signature(d: str) -> Tuple[Any, ...]:
    artifacts = _load_cards_artifacts(d)
    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    market_ctx = _load_pitcher_ladder_market_context(d)
    return (
        str(d),
        _dir_signature(sim_dir),
        _path_signature(market_ctx.get("displayPath") if isinstance(market_ctx.get("displayPath"), Path) else None),
        _path_signature(market_ctx.get("currentPath") if isinstance(market_ctx.get("currentPath"), Path) else None),
        _path_signature(market_ctx.get("pregamePath") if isinstance(market_ctx.get("pregamePath"), Path) else None),
    )


def _hitter_ladders_signature(d: str) -> Tuple[Any, ...]:
    artifacts = _load_cards_artifacts(d)
    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    market_ctx = _load_hitter_ladder_market_context(d)
    return (
        str(d),
        _dir_signature(sim_dir),
        _path_signature(market_ctx.get("displayPath") if isinstance(market_ctx.get("displayPath"), Path) else None),
        _path_signature(market_ctx.get("currentPath") if isinstance(market_ctx.get("currentPath"), Path) else None),
        _path_signature(market_ctx.get("pregamePath") if isinstance(market_ctx.get("pregamePath"), Path) else None),
    )


def _pitcher_ladder_game_options(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
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


def _pitcher_ladder_pitcher_options(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
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


def _hitter_ladder_game_options(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
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


def _hr_target_schedule_game_index(d: str) -> Dict[int, Dict[str, Any]]:
    schedule_games = _schedule_games_for_date(d)
    indexed: Dict[int, Dict[str, Any]] = {}
    for order_idx, game in enumerate(schedule_games):
        if not isinstance(game, dict):
            continue
        game_pk = _safe_int(game.get("gamePk"))
        if game_pk is None:
            continue
        away = dict(game.get("teams", {}).get("away") or {})
        home = dict(game.get("teams", {}).get("home") or {})
        away_team_id = _safe_int(away.get("team", {}).get("id"))
        home_team_id = _safe_int(home.get("team", {}).get("id"))
        away_abbr = _first_text(away.get("team", {}).get("abbreviation"), away.get("team", {}).get("name"), away.get("team", {}).get("clubName"))
        home_abbr = _first_text(home.get("team", {}).get("abbreviation"), home.get("team", {}).get("name"), home.get("team", {}).get("clubName"))
        game_date = str(game.get("gameDate") or "")
        start_time = _format_start_time_local(game_date)
        matchup = " @ ".join(part for part in (away_abbr, home_abbr) if part)
        indexed[int(game_pk)] = {
            "gamePk": int(game_pk),
            "orderIndex": int(order_idx),
            "gameDate": game_date,
            "startTime": start_time,
            "matchup": matchup,
            "awayTeamId": int(away_team_id) if away_team_id is not None else None,
            "homeTeamId": int(home_team_id) if home_team_id is not None else None,
            "awayAbbr": away_abbr,
            "homeAbbr": home_abbr,
            "label": f"{start_time} - {matchup}" if start_time and matchup else (matchup or f"Game {int(game_pk)}"),
        }
    return indexed


def _hr_target_resolved_team_ids(
    row: Dict[str, Any],
    schedule_row: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[int], Optional[int]]:
    team_id = _safe_int(row.get("team_id")) or _safe_int(row.get("teamId"))
    opponent_team_id = _safe_int(row.get("opponent_team_id")) or _safe_int(row.get("opponentTeamId"))
    if not isinstance(schedule_row, dict) or not schedule_row:
        return team_id, opponent_team_id

    away_team_id = _safe_int(schedule_row.get("awayTeamId"))
    home_team_id = _safe_int(schedule_row.get("homeTeamId"))
    away_abbr = str(schedule_row.get("awayAbbr") or "").strip().upper()
    home_abbr = str(schedule_row.get("homeAbbr") or "").strip().upper()
    team_abbr = str(row.get("team") or "").strip().upper()
    opponent_abbr = str(row.get("opponent") or "").strip().upper()

    if team_abbr and away_abbr and team_abbr == away_abbr:
        return away_team_id, home_team_id
    if team_abbr and home_abbr and team_abbr == home_abbr:
        return home_team_id, away_team_id
    if opponent_abbr and away_abbr and opponent_abbr == away_abbr:
        return home_team_id, away_team_id
    if opponent_abbr and home_abbr and opponent_abbr == home_abbr:
        return away_team_id, home_team_id

    if team_id is None and opponent_team_id is not None:
        if away_team_id is not None and opponent_team_id == away_team_id:
            return home_team_id, opponent_team_id
        if home_team_id is not None and opponent_team_id == home_team_id:
            return away_team_id, opponent_team_id
    if opponent_team_id is None and team_id is not None:
        if away_team_id is not None and team_id == away_team_id:
            return team_id, home_team_id
        if home_team_id is not None and team_id == home_team_id:
            return team_id, away_team_id

    return team_id, opponent_team_id


def _hr_target_game_options(rows: List[Dict[str, Any]], d: str) -> List[Dict[str, Any]]:
    schedule_index = _hr_target_schedule_game_index(d)
    game_map: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        game_pk = _safe_int(row.get("gamePk"))
        if game_pk is None:
            game_pk = _safe_int(row.get("game_pk"))
        if game_pk is None:
            continue
        game_pk = int(game_pk)
        schedule_row = dict(schedule_index.get(game_pk) or {})
        matchup = _first_text(row.get("matchup"), schedule_row.get("matchup"))
        start_time = _first_text(schedule_row.get("startTime"))
        game_map[game_pk] = {
            "value": str(game_pk),
            "label": f"{start_time} - {matchup}" if start_time and matchup else (matchup or f"Game {game_pk}"),
            "gamePk": game_pk,
            "matchup": matchup,
            "startTime": start_time,
            "orderIndex": int(schedule_row.get("orderIndex")) if _safe_int(schedule_row.get("orderIndex")) is not None else 9999,
        }
    return sorted(
        game_map.values(),
        key=lambda item: (
            int(item.get("orderIndex") or 9999),
            str(item.get("startTime") or ""),
            str(item.get("matchup") or ""),
            int(item.get("gamePk") or 0),
        ),
    )


def _hitter_ladder_team_options(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
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


def _hitter_ladder_hitter_options(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
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


def daily_ladders_artifact_path(d: str, *, data_root: Optional[Path] = None) -> Path:
    root = data_root.resolve() if isinstance(data_root, Path) else _DATA_DIR
    return root / "daily" / "ladders" / f"daily_ladders_{_date_slug(d)}.json"


def _load_daily_ladders_artifact(d: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    candidates: List[Path] = []
    for root in _data_roots():
        candidate = daily_ladders_artifact_path(d, data_root=root)
        if candidate not in candidates:
            candidates.append(candidate)
    artifact_path = _find_preferred_file(candidates)
    if not artifact_path:
        return None, None
    return artifact_path, _load_json_file(artifact_path)


def _prebuilt_pitcher_ladders_payload(
    d: str,
    prop_value: Any,
    sort_value: Any,
    *,
    selected_game_value: Any = None,
    selected_pitcher_value: Any = None,
) -> Optional[Dict[str, Any]]:
    prop = _normalize_pitcher_ladder_prop(prop_value)
    sort_key = _normalize_pitcher_ladder_sort(sort_value)
    selected_game = _normalize_game_selector(selected_game_value)
    selected_pitcher = _normalize_pitcher_selector(selected_pitcher_value)
    artifact_path, artifact_doc = _load_daily_ladders_artifact(d)
    if not artifact_path or not isinstance(artifact_doc, dict):
        return None
    groups = artifact_doc.get("groups") or {}
    pitcher_group = groups.get("pitcher") if isinstance(groups, dict) else None
    base_payload = pitcher_group.get(prop) if isinstance(pitcher_group, dict) else None
    if not isinstance(base_payload, dict):
        return None

    rows_all = [dict(row) for row in (base_payload.get("rows") or []) if isinstance(row, dict)]
    payload = copy.deepcopy(base_payload)
    payload["date"] = str(d)
    payload["prop"] = prop
    payload["selectedGame"] = selected_game
    payload["selectedPitcher"] = selected_pitcher
    payload["selectedSort"] = sort_key
    payload["gameOptions"] = _pitcher_ladder_game_options(rows_all)
    payload["artifactPath"] = _relative_path_str(artifact_path)
    payload["artifactGeneratedAt"] = artifact_doc.get("generatedAt")
    payload["artifactSource"] = "daily_update"

    rows = list(rows_all)
    if selected_game:
        rows = [row for row in rows if str(int(row.get("gamePk") or 0)) == selected_game]
    rows = _sort_pitcher_ladder_rows(rows, sort_key)
    payload["pitcherOptions"] = _pitcher_ladder_pitcher_options(rows)

    if selected_pitcher:
        if selected_pitcher.isdigit():
            rows = [row for row in rows if str(int(row.get("pitcherId") or 0)) == selected_pitcher]
        else:
            target_name = normalize_pitcher_name(selected_pitcher)
            rows = [row for row in rows if normalize_pitcher_name(row.get("pitcherName")) == target_name]

    attach_history = _should_attach_ladder_history(selected_player=selected_pitcher)
    if attach_history:
        rows = _attach_history_summary_rows(rows, season=_season_from_date_str(d), group="pitching", prop=prop)

    if not rows:
        payload["found"] = False
        payload["rows"] = []
        payload.pop("featuredRow", None)
        payload["error"] = "pitcher_ladders_missing"
        if selected_game:
            payload["error"] = "pitcher_ladders_game_missing"
        if selected_pitcher:
            payload["error"] = "pitcher_ladders_pitcher_missing"
        payload["historyMode"] = "selected_player" if attach_history else "deferred"
        payload["summary"] = {
            "games": int((((base_payload.get("summary") or {}).get("games")) or len(payload.get("gameOptions") or []))),
            "starters": 0,
            "simCounts": [],
            "availableGames": int(len(payload.get("gameOptions") or [])),
            "availableStarters": int(len(payload.get("pitcherOptions") or [])),
        }
        return payload

    payload["found"] = True
    payload.pop("error", None)
    payload["rows"] = rows
    if rows:
        payload["featuredRow"] = (
            _attach_history_summary(rows[0], season=_season_from_date_str(d), group="pitching", prop=prop)
            if attach_history
            else dict(rows[0])
        )
    payload["historyMode"] = "selected_player" if attach_history else "deferred"
    payload["summary"] = {
        "games": int((((base_payload.get("summary") or {}).get("games")) or len(payload.get("gameOptions") or []))),
        "starters": int(len(rows)),
        "simCounts": sorted({int(row.get("simCount") or 0) for row in rows if int(row.get("simCount") or 0) > 0}),
        "availableGames": int(len(payload.get("gameOptions") or [])),
        "availableStarters": int(len(payload.get("pitcherOptions") or [])),
    }
    return payload


def _prebuilt_hitter_ladders_payload(
    d: str,
    prop_value: Any,
    *,
    selected_game_value: Any = None,
    selected_team_value: Any = None,
    selected_hitter_value: Any = None,
    sort_value: Any = None,
) -> Optional[Dict[str, Any]]:
    prop = _normalize_hitter_ladder_prop(prop_value)
    sort_key = _normalize_hitter_ladder_sort(sort_value)
    selected_game = _normalize_game_selector(selected_game_value)
    selected_team = _normalize_hitter_team_selector(selected_team_value)
    selected_hitter = _normalize_hitter_selector(selected_hitter_value)
    artifact_path, artifact_doc = _load_daily_ladders_artifact(d)
    if not artifact_path or not isinstance(artifact_doc, dict):
        return None
    groups = artifact_doc.get("groups") or {}
    hitter_group = groups.get("hitter") if isinstance(groups, dict) else None
    base_payload = hitter_group.get(prop) if isinstance(hitter_group, dict) else None
    if not isinstance(base_payload, dict):
        return None

    rows_all = [dict(row) for row in (base_payload.get("rows") or []) if isinstance(row, dict)]
    payload = copy.deepcopy(base_payload)
    payload["date"] = str(d)
    payload["prop"] = prop
    payload["selectedGame"] = selected_game
    payload["selectedTeam"] = selected_team
    payload["selectedHitter"] = selected_hitter
    payload["selectedSort"] = sort_key
    payload["gameOptions"] = _hitter_ladder_game_options(rows_all)
    payload["artifactPath"] = _relative_path_str(artifact_path)
    payload["artifactGeneratedAt"] = artifact_doc.get("generatedAt")
    payload["artifactSource"] = "daily_update"

    rows = list(rows_all)
    if selected_game:
        rows = [row for row in rows if str(int(row.get("gamePk") or 0)) == selected_game]
    payload["teamOptions"] = _hitter_ladder_team_options(rows)

    if selected_team:
        rows = [row for row in rows if str(row.get("team") or "").strip().upper() == selected_team]
    rows = _sort_hitter_ladder_rows(rows, sort_key)
    payload["hitterOptions"] = _hitter_ladder_hitter_options(rows)

    if selected_hitter:
        if selected_hitter.isdigit():
            rows = [row for row in rows if str(int(row.get("hitterId") or 0)) == selected_hitter]
        else:
            target_name = normalize_pitcher_name(selected_hitter)
            rows = [row for row in rows if normalize_pitcher_name(row.get("hitterName")) == target_name]

    attach_history = _should_attach_ladder_history(selected_player=selected_hitter)
    if attach_history:
        rows = _attach_history_summary_rows(rows, season=_season_from_date_str(d), group="hitting", prop=prop)

    top_n = (base_payload.get("summary") or {}).get("topN")
    if not rows:
        payload["found"] = False
        payload["rows"] = []
        payload.pop("featuredRow", None)
        payload["error"] = "hitter_ladders_missing"
        if selected_game:
            payload["error"] = "hitter_ladders_game_missing"
        if selected_team:
            payload["error"] = "hitter_ladders_team_missing"
        if selected_hitter:
            payload["error"] = "hitter_ladders_hitter_missing"
        payload["historyMode"] = "selected_player" if attach_history else "deferred"
        payload["summary"] = {
            "games": int((((base_payload.get("summary") or {}).get("games")) or len(payload.get("gameOptions") or []))),
            "hitters": 0,
            "simCounts": [],
            "availableGames": int(len(payload.get("gameOptions") or [])),
            "availableTeams": int(len(payload.get("teamOptions") or [])),
            "availableHitters": int(len(payload.get("hitterOptions") or [])),
            "topN": int(top_n) if _safe_int(top_n) is not None else None,
        }
        return payload

    payload["found"] = True
    payload.pop("error", None)
    payload["rows"] = rows
    payload["ladderShape"] = "exact" if any(str(row.get("ladderShape") or "") == "exact" for row in rows) else "threshold"
    if rows:
        payload["featuredRow"] = (
            _attach_history_summary(rows[0], season=_season_from_date_str(d), group="hitting", prop=prop)
            if attach_history
            else dict(rows[0])
        )
    payload["historyMode"] = "selected_player" if attach_history else "deferred"
    payload["summary"] = {
        "games": int((((base_payload.get("summary") or {}).get("games")) or len(payload.get("gameOptions") or []))),
        "hitters": int(len(rows)),
        "simCounts": sorted({int(row.get("simCount") or 0) for row in rows if int(row.get("simCount") or 0) > 0}),
        "availableGames": int(len(payload.get("gameOptions") or [])),
        "availableTeams": int(len(payload.get("teamOptions") or [])),
        "availableHitters": int(len(payload.get("hitterOptions") or [])),
        "topN": int(top_n) if _safe_int(top_n) is not None else None,
    }
    return payload


def _build_daily_pitcher_ladders_artifact_group(d: str) -> Dict[str, Any]:
    return {
        prop_key: _pitcher_ladders_payload(
            d,
            prop_key,
            "team",
            selected_game_value="",
            selected_pitcher_value="",
        )
        for prop_key in _PITCHER_LADDER_PROPS.keys()
    }


def _build_daily_hitter_ladders_artifact_group(d: str) -> Dict[str, Any]:
    return {
        prop_key: _hitter_ladders_payload(
            d,
            prop_key,
            selected_game_value="",
            selected_team_value="",
            selected_hitter_value="",
            sort_value="team",
        )
        for prop_key in _HITTER_LADDER_PROPS.keys()
    }


def build_daily_ladders_artifact(d: str) -> Dict[str, Any]:
    date_str = str(d or "").strip()
    return {
        "date": date_str,
        "generatedAt": _local_timestamp_text(),
        "groups": {
            "pitcher": _build_daily_pitcher_ladders_artifact_group(date_str),
            "hitter": _build_daily_hitter_ladders_artifact_group(date_str),
        },
    }


def write_daily_ladders_artifact(d: str, *, out_path: Optional[Path] = None) -> Dict[str, Any]:
    date_str = str(d or "").strip()
    destination = out_path.resolve() if isinstance(out_path, Path) else daily_ladders_artifact_path(date_str)
    artifact = build_daily_ladders_artifact(date_str)
    _write_json_file(destination, artifact)
    groups = artifact.get("groups") if isinstance(artifact.get("groups"), dict) else {}
    return {
        "date": date_str,
        "path": destination,
        "groupSummaries": {
            str(group): {
                str(prop): {
                    "found": bool((payload or {}).get("found")),
                    "rowCount": int(len((payload or {}).get("rows") or [])),
                    "error": (payload or {}).get("error"),
                }
                for prop, payload in dict(group_payload or {}).items()
                if isinstance(payload, dict)
            }
            for group, group_payload in groups.items()
            if isinstance(group_payload, dict)
        },
    }


def _pitcher_ladders_payload_cached(
    d: str,
    prop_value: Any,
    sort_value: Any,
    *,
    selected_game_value: Any = None,
    selected_pitcher_value: Any = None,
) -> Dict[str, Any]:
    prop = _normalize_pitcher_ladder_prop(prop_value)
    sort_key = _normalize_pitcher_ladder_sort(sort_value)
    selected_game = _normalize_game_selector(selected_game_value)
    selected_pitcher = _normalize_pitcher_selector(selected_pitcher_value)
    prebuilt_payload = _prebuilt_pitcher_ladders_payload(
        d,
        prop,
        sort_key,
        selected_game_value=selected_game,
        selected_pitcher_value=selected_pitcher,
    )
    if isinstance(prebuilt_payload, dict):
        return prebuilt_payload
    cache_key = f"{str(d)}:{prop}:{sort_key}:{selected_game}:{selected_pitcher}"
    return _payload_cache_get_or_build(
        "pitcher_ladders",
        cache_key,
        signature_factory=lambda: _pitcher_ladders_signature(d),
        max_age_seconds=_LADDERS_CACHE_TTL_SECONDS,
        builder=lambda: _pitcher_ladders_payload(
            d,
            prop,
            sort_key,
            selected_game_value=selected_game,
            selected_pitcher_value=selected_pitcher,
        ),
    )


def _hitter_ladders_payload_cached(
    d: str,
    prop_value: Any,
    *,
    selected_game_value: Any = None,
    selected_team_value: Any = None,
    selected_hitter_value: Any = None,
    sort_value: Any = None,
) -> Dict[str, Any]:
    prop = _normalize_hitter_ladder_prop(prop_value)
    sort_key = _normalize_hitter_ladder_sort(sort_value)
    selected_game = _normalize_game_selector(selected_game_value)
    selected_team = _normalize_hitter_team_selector(selected_team_value)
    selected_hitter = _normalize_hitter_selector(selected_hitter_value)
    prebuilt_payload = _prebuilt_hitter_ladders_payload(
        d,
        prop,
        selected_game_value=selected_game,
        selected_team_value=selected_team,
        selected_hitter_value=selected_hitter,
        sort_value=sort_key,
    )
    if isinstance(prebuilt_payload, dict):
        return prebuilt_payload
    cache_key = f"{str(d)}:{prop}:{sort_key}:{selected_game}:{selected_team}:{selected_hitter}"
    return _payload_cache_get_or_build(
        "hitter_ladders",
        cache_key,
        signature_factory=lambda: _hitter_ladders_signature(d),
        max_age_seconds=_LADDERS_CACHE_TTL_SECONDS,
        builder=lambda: _hitter_ladders_payload(
            d,
            prop,
            selected_game_value=selected_game,
            selected_team_value=selected_team,
            selected_hitter_value=selected_hitter,
            sort_value=sort_key,
        ),
    )


def _hr_target_sort_options() -> List[Dict[str, str]]:
    return [
        {"value": "score", "label": "Target score"},
        {"value": "prob", "label": "HR probability"},
        {"value": "support", "label": "Support score"},
        {"value": "team", "label": "Team"},
    ]


def _normalize_hr_target_sort(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"probability", "p_hr", "hr_prob"}:
        return "prob"
    if raw in {"support_score", "confidence", "label"}:
        return "support"
    if raw in {"club", "abbr"}:
        return "team"
    if raw not in {"score", "prob", "support", "team"}:
        return "score"
    return raw


def _sort_hr_target_rows(rows: List[Dict[str, Any]], sort_key: str) -> List[Dict[str, Any]]:
    normalized = _normalize_hr_target_sort(sort_key)
    if normalized == "prob":
        return sorted(
            rows,
            key=lambda row: (
                float(row.get("p_hr_1plus") or 0.0),
                float(row.get("hr_target_score") or 0.0),
                float(row.get("hr_support_score") or 0.0),
            ),
            reverse=True,
        )
    if normalized == "support":
        return sorted(
            rows,
            key=lambda row: (
                float(row.get("hr_support_score") or 0.0),
                float(row.get("p_hr_1plus") or 0.0),
                float(row.get("hr_target_score") or 0.0),
            ),
            reverse=True,
        )
    if normalized == "team":
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("team") or ""),
                str(row.get("player_name") or ""),
                -float(row.get("hr_target_score") or 0.0),
            ),
        )
    return sorted(
        rows,
        key=lambda row: (
            float(row.get("hr_target_score") or 0.0),
            float(row.get("p_hr_1plus") or 0.0),
            float(row.get("hr_support_score") or 0.0),
        ),
        reverse=True,
    )


def _daily_hr_targets_signature(d: str) -> Tuple[Any, ...]:
    artifacts = _load_cards_artifacts(d)
    hr_targets_path = artifacts.get("hr_targets_path") if isinstance(artifacts.get("hr_targets_path"), Path) else None
    profile_bundle_path = artifacts.get("profile_bundle_path") if isinstance(artifacts.get("profile_bundle_path"), Path) else None
    return (
        _path_signature(hr_targets_path),
        _path_signature(profile_bundle_path),
        str(d),
    )


def _hr_target_page_row_payload(d: str, row: Dict[str, Any], *, schedule_row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    game_pk = _safe_int(row.get("game_pk"))
    batter_id = _safe_int(row.get("batter_id"))
    team_id, opponent_team_id = _hr_target_resolved_team_ids(row, schedule_row)
    return {
        "gamePk": int(game_pk) if game_pk is not None else None,
        "batterId": int(batter_id) if batter_id is not None else None,
        "playerName": _first_text(row.get("player_name"), row.get("hitterName")),
        "team": _first_text(row.get("team")),
        "teamId": int(team_id) if team_id is not None else None,
        "opponent": _first_text(row.get("opponent")),
        "opponentTeamId": int(opponent_team_id) if opponent_team_id is not None else None,
        "matchup": _first_text(row.get("matchup")),
        "opponentPitcherName": _first_text(row.get("opponent_pitcher_name")),
        "headshotUrl": (_mlb_headshot_url(int(batter_id)) if batter_id is not None else None),
        "teamLogoUrl": (_mlb_logo_url(int(team_id)) if team_id is not None else None),
        "opponentLogoUrl": (_mlb_logo_url(int(opponent_team_id)) if opponent_team_id is not None else None),
        "lineupOrder": _safe_int(row.get("lineup_order")),
        "paMean": _safe_float(row.get("pa_mean")),
        "pHr1Plus": _safe_float(row.get("p_hr_1plus")),
        "supportScore": _safe_float(row.get("hr_support_score")),
        "supportLabel": _first_text(row.get("hr_support_label")),
        "summary": _first_text(row.get("hr_target_summary")),
        "drivers": _cards_hr_target_driver_payload(row),
        "writeup": _cards_hr_target_writeup(row),
        "detailHref": f"/hr-targets?date={d}" + (f"&game={int(game_pk)}" if game_pk is not None else ""),
    }


def _daily_hr_targets_payload(
    d: str,
    *,
    selected_game_value: Any = None,
    selected_team_value: Any = None,
    selected_hitter_value: Any = None,
    sort_value: Any = None,
) -> Dict[str, Any]:
    selected_game = _normalize_game_selector(selected_game_value)
    selected_team = _normalize_hitter_team_selector(selected_team_value)
    selected_hitter = _normalize_hitter_selector(selected_hitter_value)
    sort_key = _normalize_hr_target_sort(sort_value)
    schedule_index = _hr_target_schedule_game_index(d)
    artifacts = _load_cards_artifacts(d)
    nav = _cards_nav_from_schedule(d) or {"season": _season_from_date_str(d)}
    doc = artifacts.get("hr_targets") if isinstance(artifacts.get("hr_targets"), dict) else None
    artifact_path = artifacts.get("hr_targets_path") if isinstance(artifacts.get("hr_targets_path"), Path) else None
    rows_all = list((doc or {}).get("rows") or []) if isinstance(doc, dict) else []
    rows_all = [row for row in rows_all if isinstance(row, dict)]

    filtered_rows: List[Dict[str, Any]] = []
    for row in rows_all:
        game_pk = str(int(row.get("game_pk") or 0)) if _safe_int(row.get("game_pk")) is not None else ""
        team = str(row.get("team") or "").strip().upper()
        hitter_id = str(int(row.get("batter_id") or 0)) if _safe_int(row.get("batter_id")) is not None else ""
        if selected_game and game_pk != selected_game:
            continue
        if selected_team and team != selected_team:
            continue
        if selected_hitter and hitter_id != selected_hitter:
            continue
        filtered_rows.append(dict(row))

    rows = _sort_hr_target_rows(filtered_rows, sort_key)
    for idx, row in enumerate(rows, start=1):
        schedule_row = dict(schedule_index.get(int(row.get("game_pk") or 0)) or {}) if _safe_int(row.get("game_pk")) is not None else {}
        row["rank"] = int(idx)
        row.update(_hr_target_page_row_payload(d, row, schedule_row=schedule_row))
        row["hitterId"] = row.get("batterId")
        row["hitterName"] = row.get("playerName")
        row["teamId"] = _safe_int(row.get("teamId"))
        row["opponentTeamId"] = _safe_int(row.get("opponentTeamId"))

    hitter_options: List[Dict[str, Any]] = []
    for row in rows_all:
        batter_id = _safe_int(row.get("batter_id"))
        player_name = str(row.get("player_name") or "").strip()
        if batter_id is None or not player_name:
            continue
        schedule_row = schedule_index.get(int(row.get("game_pk") or 0)) if _safe_int(row.get("game_pk")) is not None else None
        team_id, opponent_team_id = _hr_target_resolved_team_ids(row, schedule_row)
        hitter_options.append(
            {
                "hitterId": row.get("batter_id"),
                "hitterName": row.get("player_name"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "headshotUrl": _mlb_headshot_url(int(batter_id)),
                "teamId": team_id,
                "opponentTeamId": opponent_team_id,
                "teamLogoUrl": (_mlb_logo_url(int(team_id)) if team_id is not None else None),
                "opponentLogoUrl": (_mlb_logo_url(int(opponent_team_id)) if opponent_team_id is not None else None),
            }
        )

    payload: Dict[str, Any] = {
        "date": str(d),
        "selectedGame": selected_game,
        "selectedTeam": selected_team,
        "selectedHitter": selected_hitter,
        "selectedSort": sort_key,
        "sortOptions": _hr_target_sort_options(),
        "gameOptions": _hr_target_game_options(rows_all, d),
        "teamOptions": _hitter_ladder_team_options(rows_all),
        "hitterOptions": _hitter_ladder_hitter_options(hitter_options),
        "rows": rows,
        "sourcePath": _relative_path_str(artifact_path),
        "policy": dict((doc or {}).get("policy") or {}) if isinstance(doc, dict) else {},
        "counts": {
            "totalRows": int(len(rows_all)),
            "filteredRows": int(len(rows)),
            "games": int(len({int(row.get("game_pk") or 0) for row in rows_all if _safe_int(row.get("game_pk")) is not None})),
        },
        "nav": nav,
        "found": bool(isinstance(doc, dict) and bool(rows_all)),
    }

    grouped: List[Dict[str, Any]] = []
    seen_games = {
        int(game_pk)
        for game_pk in (_safe_int(row.get("gamePk")) for row in rows)
        if game_pk is not None
    }
    ordered_game_pks = sorted(
        seen_games,
        key=lambda game_pk: (
            int((schedule_index.get(int(game_pk)) or {}).get("orderIndex") or 9999),
            str((schedule_index.get(int(game_pk)) or {}).get("startTime") or ""),
            str((schedule_index.get(int(game_pk)) or {}).get("matchup") or ""),
            int(game_pk),
        ),
    )
    for game_pk in ordered_game_pks:
        game_rows = [row for row in rows if _safe_int(row.get("gamePk")) == int(game_pk)]
        if not game_rows:
            continue
        first_row = game_rows[0]
        schedule_row = dict(schedule_index.get(int(game_pk)) or {})
        grouped.append(
            {
                "gamePk": int(game_pk),
                "matchup": _first_text(schedule_row.get("matchup"), first_row.get("matchup")),
                "startTime": _first_text(schedule_row.get("startTime")),
                "away": first_row.get("away"),
                "home": first_row.get("home"),
                "awayAbbr": first_row.get("away_abbr"),
                "homeAbbr": first_row.get("home_abbr"),
                "rows": game_rows,
            }
        )
    payload["games"] = grouped
    if not payload["found"]:
        payload["error"] = "hr_targets_missing"
    return payload


def _daily_hr_targets_payload_cached(
    d: str,
    *,
    selected_game_value: Any = None,
    selected_team_value: Any = None,
    selected_hitter_value: Any = None,
    sort_value: Any = None,
) -> Dict[str, Any]:
    selected_game = _normalize_game_selector(selected_game_value)
    selected_team = _normalize_hitter_team_selector(selected_team_value)
    selected_hitter = _normalize_hitter_selector(selected_hitter_value)
    sort_key = _normalize_hr_target_sort(sort_value)
    cache_key = f"{str(d)}:{sort_key}:{selected_game}:{selected_team}:{selected_hitter}"
    return _payload_cache_get_or_build(
        "daily_hr_targets",
        cache_key,
        signature_factory=lambda: _daily_hr_targets_signature(d),
        max_age_seconds=_LADDERS_CACHE_TTL_SECONDS,
        builder=lambda: _daily_hr_targets_payload(
            d,
            selected_game_value=selected_game,
            selected_team_value=selected_team,
            selected_hitter_value=selected_hitter,
            sort_value=sort_key,
        ),
    )


def _daily_top_props_row(row: Dict[str, Any], *, stat_key: str, stat_label: str, group: str) -> Optional[Dict[str, Any]]:
    market_entry = _top_props_market_line_for_stat(row.get("marketLinesByStat"), stat_key)
    side_choice = _top_props_side_choice(
        over_prob=row.get("overLineProb"),
        market_entry=market_entry,
        allow_under=(str(stat_key) != "home_runs"),
    )
    if side_choice is None:
        return None

    owner_name = _first_text(row.get("playerName"), row.get("pitcherName"), row.get("hitterName"))
    if not owner_name:
        return None

    return {
        "stat": str(stat_key),
        "statLabel": str(stat_label),
        "group": str(group),
        "side": str(row.get("side") or ""),
        "ownerId": _safe_int(row.get("pitcherId") if group == "pitcher" else row.get("hitterId")),
        "ownerName": owner_name,
        "playerName": owner_name,
        "headshotUrl": row.get("headshotUrl"),
        "team": _first_text(row.get("team")),
        "teamId": _safe_int(row.get("teamId")),
        "teamLogoUrl": row.get("teamLogoUrl"),
        "opponent": _first_text(row.get("opponent")),
        "opponentTeamId": _safe_int(row.get("opponentTeamId")),
        "opponentLogoUrl": row.get("opponentLogoUrl"),
        "matchup": _first_text(row.get("matchup")),
        "gamePk": _safe_int(row.get("gamePk")),
        "mean": _safe_float(row.get("mean")),
        "line": _safe_float(side_choice.get("line")),
        "selection": str(side_choice.get("selection") or ""),
        "selectionLabel": str(side_choice.get("selectionLabel") or ""),
        "targetLabel": str(side_choice.get("targetLabel") or ""),
        "simProb": _safe_float(side_choice.get("simProb")),
        "marketProb": _safe_float(side_choice.get("marketProb")),
        "rawEdge": _safe_float(side_choice.get("rawEdge")),
        "odds": _safe_int(side_choice.get("odds")),
        "marketLine": _safe_float(market_entry.get("line")),
        "sourceFile": row.get("sourceFile"),
    }


def daily_top_props_artifact_path(d: str, *, data_root: Optional[Path] = None) -> Path:
    root = data_root.resolve() if isinstance(data_root, Path) else _DATA_DIR
    return root / "daily" / "top_props" / f"daily_top_props_{_date_slug(d)}.json"


def _load_daily_top_props_artifact(d: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    candidates: List[Path] = []
    for root in _data_roots():
        candidate = daily_top_props_artifact_path(d, data_root=root)
        if candidate not in candidates:
            candidates.append(candidate)
    artifact_path = _find_preferred_file(candidates)
    if not artifact_path:
        return None, None
    return artifact_path, _load_json_file(artifact_path)


def _prebuilt_daily_top_props_payload(d: str, group: str) -> Optional[Dict[str, Any]]:
    normalized_group = "pitcher" if str(group or "").strip().lower() == "pitcher" else "hitter"
    artifact_path, artifact_doc = _load_daily_top_props_artifact(d)
    if not artifact_path or not isinstance(artifact_doc, dict):
        return None
    groups = artifact_doc.get("groups") or {}
    payload = groups.get(normalized_group) if isinstance(groups, dict) else None
    if not isinstance(payload, dict):
        return None
    out = dict(payload)
    out["artifactPath"] = _relative_path_str(artifact_path)
    out["artifactGeneratedAt"] = artifact_doc.get("generatedAt")
    out["artifactSource"] = "daily_update"
    return out


def _persist_daily_top_props_group_payload(d: str, group: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_group = "pitcher" if str(group or "").strip().lower() == "pitcher" else "hitter"
    artifact_path, artifact_doc = _load_daily_top_props_artifact(d)
    groups: Dict[str, Any] = {}
    if isinstance(artifact_doc, dict):
        existing_groups = artifact_doc.get("groups")
        if isinstance(existing_groups, dict):
            groups = {str(key): value for key, value in existing_groups.items() if isinstance(value, dict)}

    groups[normalized_group] = dict(payload)
    out_doc = {
        "date": str(d),
        "generatedAt": _local_timestamp_text(),
        "groups": groups,
    }
    destination = artifact_path if isinstance(artifact_path, Path) else daily_top_props_artifact_path(str(d))
    _write_json_file(destination, out_doc)

    out = dict(payload)
    out["artifactPath"] = _relative_path_str(destination)
    out["artifactGeneratedAt"] = out_doc.get("generatedAt")
    out["artifactSource"] = "historical_request_cache"
    return out


def _daily_top_props_signature(d: str, group: str) -> Tuple[Any, ...]:
    normalized_group = "pitcher" if str(group or "").strip().lower() == "pitcher" else "hitter"
    artifacts = _load_cards_artifacts(d)
    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    market_ctx = _load_pitcher_ladder_market_context(d) if normalized_group == "pitcher" else _load_hitter_ladder_market_context(d)
    return (
        str(d),
        normalized_group,
        _dir_signature(sim_dir),
        _path_signature(market_ctx.get("displayPath") if isinstance(market_ctx.get("displayPath"), Path) else None),
        _path_signature(market_ctx.get("currentPath") if isinstance(market_ctx.get("currentPath"), Path) else None),
        _path_signature(market_ctx.get("pregamePath") if isinstance(market_ctx.get("pregamePath"), Path) else None),
    )


def _build_live_daily_top_props_payload(d: str, normalized_group: str) -> Dict[str, Any]:
    nav = _cards_nav_from_schedule(d) or {
        "season": _season_from_date_str(d),
        "minDate": None,
        "maxDate": None,
        "prevDate": _shift_iso_date_str(d, -1),
        "nextDate": _shift_iso_date_str(d, 1),
    }

    if normalized_group == "pitcher":
        prop_items = [(key, cfg) for key, cfg in _PITCHER_LADDER_PROPS.items() if str(cfg.get("market_key") or "").strip()]
    else:
        prop_items = [(key, cfg) for key, cfg in _HITTER_LADDER_PROPS.items() if str(cfg.get("market_key") or "").strip()]

    def _load_prop_payload(prop_key: str) -> Tuple[str, Dict[str, Any]]:
        if normalized_group == "pitcher":
            return (
                prop_key,
                _pitcher_ladders_payload(
                    d,
                    prop_key,
                    "mean",
                    selected_game_value="",
                    selected_pitcher_value="",
                ),
            )
        return (
            prop_key,
            _hitter_ladders_payload(
                d,
                prop_key,
                selected_game_value="",
                selected_team_value="",
                selected_hitter_value="",
                sort_value="team",
            ),
        )

    payload_by_prop: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(4, max(1, len(prop_items)))) as executor:
        futures = {executor.submit(_load_prop_payload, prop_key): prop_key for prop_key, _ in prop_items}
        for future in as_completed(futures):
            prop_key = futures[future]
            try:
                loaded_prop_key, prop_payload = future.result()
            except Exception as exc:
                app.logger.exception("daily top props load failed for %s %s", normalized_group, prop_key)
                payload_by_prop[prop_key] = {"found": False, "error": f"{type(exc).__name__}: {exc}"}
                continue
            payload_by_prop[str(loaded_prop_key)] = prop_payload if isinstance(prop_payload, dict) else {"found": False}

    sections: List[Dict[str, Any]] = []
    market_mode = ""
    market_source = None
    total_candidates = 0
    total_positive = 0
    total_displayed = 0
    default_stat = "strikeouts" if normalized_group == "pitcher" else "home_runs"
    game_options_map: Dict[int, Dict[str, Any]] = {}
    for prop_key, prop_cfg in prop_items:
        prop_payload = payload_by_prop.get(prop_key) or {}
        if not market_mode:
            market_mode = str(prop_payload.get("marketMode") or "")
        if not market_source:
            market_source = prop_payload.get("marketPath")
        section_rows: List[Dict[str, Any]] = []
        for row in prop_payload.get("rows") or []:
            if not isinstance(row, dict):
                continue
            top_row = _daily_top_props_row(
                row,
                stat_key=prop_key,
                stat_label=str(prop_cfg.get("label") or prop_key.title()),
                group=normalized_group,
            )
            if top_row is not None:
                section_rows.append(top_row)
                game_pk = _safe_int(top_row.get("gamePk"))
                matchup = _first_text(top_row.get("matchup"))
                if game_pk is not None and matchup:
                    game_options_map[int(game_pk)] = {
                        "value": str(int(game_pk)),
                        "gamePk": int(game_pk),
                        "label": str(matchup),
                        "matchup": str(matchup),
                    }
        section_rows.sort(
            key=lambda item: (
                -float(item.get("rawEdge") or 0.0),
                -float(item.get("simProb") or 0.0),
                str(item.get("playerName") or ""),
            )
        )
        for idx, item in enumerate(section_rows, start=1):
            item["rank"] = idx
        positive_count = sum(1 for item in section_rows if float(item.get("rawEdge") or 0.0) > 0.0)
        displayed_rows = list(section_rows)
        total_candidates += len(section_rows)
        total_positive += positive_count
        total_displayed += len(displayed_rows)
        sections.append(
            {
                "stat": str(prop_key),
                "label": str(prop_cfg.get("label") or prop_key.title()),
                "unit": str(prop_cfg.get("unit") or ""),
                "marketKey": str(prop_cfg.get("market_key") or ""),
                "rows": displayed_rows,
                "candidateCount": int(len(section_rows)),
                "positiveEdgeCount": int(positive_count),
                "found": bool(prop_payload.get("found")),
                "error": prop_payload.get("error"),
            }
        )

    sections_out, reconciliation = _reconcile_top_props_sections(sections, d=str(d), group=normalized_group)

    return {
        "found": bool(any(section.get("candidateCount") for section in sections_out)),
        "date": str(d),
        "season": int(_season_from_date_str(d)),
        "group": normalized_group,
        "groupLabel": "Pitcher" if normalized_group == "pitcher" else "Hitter",
        "title": "Daily Top Pitcher Props" if normalized_group == "pitcher" else "Daily Top Hitter Props",
        "defaultStat": default_stat,
        "defaultGame": "",
        "gameOptions": sorted(game_options_map.values(), key=lambda item: (str(item.get("label") or ""), int(item.get("gamePk") or 0))),
        "nav": nav,
        "marketMode": market_mode,
        "marketSource": _relative_path_str(market_source),
        "reconciliation": reconciliation,
        "sections": sections_out,
        "summary": {
            "sectionCount": int(len(sections_out)),
            "candidateCount": int(total_candidates),
            "positiveEdgeCount": int(total_positive),
            "displayedCount": int(total_displayed),
        },
    }


def build_daily_top_props_artifact(d: str) -> Dict[str, Any]:
    date_str = str(d or "").strip()
    return {
        "date": date_str,
        "generatedAt": _local_timestamp_text(),
        "groups": {
            "pitcher": _build_live_daily_top_props_payload(date_str, "pitcher"),
            "hitter": _build_live_daily_top_props_payload(date_str, "hitter"),
        },
    }


def write_daily_top_props_artifact(d: str, *, out_path: Optional[Path] = None) -> Dict[str, Any]:
    date_str = str(d or "").strip()
    destination = out_path.resolve() if isinstance(out_path, Path) else daily_top_props_artifact_path(date_str)
    artifact = build_daily_top_props_artifact(date_str)
    _write_json_file(destination, artifact)
    groups = artifact.get("groups") if isinstance(artifact.get("groups"), dict) else {}
    return {
        "date": date_str,
        "path": destination,
        "groupSummaries": {
            str(group): {
                "found": bool((payload or {}).get("found")),
                "candidateCount": int((((payload or {}).get("summary") or {}).get("candidateCount") or 0)),
                "displayedCount": int((((payload or {}).get("summary") or {}).get("displayedCount") or 0)),
                "sectionCount": int((((payload or {}).get("summary") or {}).get("sectionCount") or 0)),
            }
            for group, payload in groups.items()
            if isinstance(payload, dict)
        },
    }


def _daily_top_props_payload(d: str, group: str, limit_value: Any) -> Dict[str, Any]:
    normalized_group = "pitcher" if str(group or "").strip().lower() == "pitcher" else "hitter"
    prebuilt_payload = _prebuilt_daily_top_props_payload(d, normalized_group)
    if isinstance(prebuilt_payload, dict):
        return prebuilt_payload
    cache_key = f"{str(d)}:{normalized_group}"

    payload = _payload_cache_get_or_build(
        "daily_top_props",
        cache_key,
        signature_factory=lambda: _daily_top_props_signature(d, normalized_group),
        max_age_seconds=_TOP_PROPS_CACHE_TTL_SECONDS,
        builder=lambda: _build_live_daily_top_props_payload(d, normalized_group),
    )
    if _top_props_supports_reconciliation(d) and isinstance(payload, dict):
        try:
            return _persist_daily_top_props_group_payload(d, normalized_group, payload)
        except Exception:
            app.logger.exception("failed to persist historical top props payload for %s %s", normalized_group, d)
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
            "game_date": _first_text(raw_game.get("game_date"), raw_game.get("commence_time")),
            "away_abbr": away_abbr,
            "home_abbr": home_abbr,
            "away": _first_text(away.get("name"), away_abbr),
            "home": _first_text(home.get("name"), home_abbr),
            "full": _normalized_full_game_probs(full),
            "first1": dict(segments.get("first1") or {}),
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


def _schedule_context_by_game_pk(d: str) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    schedule_games = _schedule_games_for_date(d)

    for game in schedule_games:
        if not isinstance(game, dict):
            continue
        game_pk = _safe_int(game.get("gamePk"))
        if not game_pk or int(game_pk) <= 0:
            continue
        status = game.get("status") or {}
        game_date = str(game.get("gameDate") or "")
        away_side = (game.get("teams") or {}).get("away") or {}
        home_side = (game.get("teams") or {}).get("home") or {}
        away_team = _team_from_schedule(away_side)
        home_team = _team_from_schedule(home_side)
        out[int(game_pk)] = {
            "gameDate": game_date,
            "startTime": _format_start_time_local(game_date),
            "officialDate": str(game.get("officialDate") or d),
            "status": {
                "abstract": str(status.get("abstractGameState") or ""),
                "detailed": str(status.get("detailedState") or ""),
            },
            "away": {"id": away_team.id, "abbr": away_team.abbr, "name": away_team.name},
            "home": {"id": home_team.id, "abbr": home_team.abbr, "name": home_team.name},
            "score": {
                "away": _safe_int(away_side.get("score")),
                "home": _safe_int(home_side.get("score")),
            },
            "probable": {
                "away": _normalized_probable_entry(_probable_pitcher_from_schedule(away_side)),
                "home": _normalized_probable_entry(_probable_pitcher_from_schedule(home_side)),
            },
        }
    return out


def _fetch_schedule_games_remote_cached(d: str) -> Tuple[Dict[str, Any], ...]:
    cache_key = str(d or "").strip()
    if not cache_key:
        return tuple()
    now = time.time()
    with _SCHEDULE_REMOTE_CACHE_LOCK:
        cached = _SCHEDULE_REMOTE_CACHE.get(cache_key)
        if cached is not None and (now - float(cached[0])) <= _SCHEDULE_REMOTE_CACHE_TTL_SECONDS:
            return cached[1]
    try:
        schedule_games = fetch_schedule_for_date(_client(), cache_key) or []
    except Exception:
        schedule_games = []
    payload = tuple(game for game in schedule_games if isinstance(game, dict))
    with _SCHEDULE_REMOTE_CACHE_LOCK:
        _SCHEDULE_REMOTE_CACHE[cache_key] = (now, payload)
        expired_keys = [
            key
            for key, value in _SCHEDULE_REMOTE_CACHE.items()
            if (now - float(value[0])) > (_SCHEDULE_REMOTE_CACHE_TTL_SECONDS * 4.0)
        ]
        for key in expired_keys:
            _SCHEDULE_REMOTE_CACHE.pop(key, None)
        while len(_SCHEDULE_REMOTE_CACHE) > int(_SCHEDULE_FETCH_CACHE_MAXSIZE):
            oldest_key = min(_SCHEDULE_REMOTE_CACHE, key=lambda key: float((_SCHEDULE_REMOTE_CACHE.get(key) or (0.0, tuple()))[0]))
            _SCHEDULE_REMOTE_CACHE.pop(oldest_key, None)
    return payload


def _schedule_games_for_date(d: str) -> List[Dict[str, Any]]:
    schedule_snapshot_path: Optional[Path] = None
    for candidate in (
        _DATA_DIR / "daily" / "snapshots" / str(d) / "schedule_raw.json",
        _TRACKED_DATA_DIR / "daily" / "snapshots" / str(d) / "schedule_raw.json",
    ):
        if candidate.exists() and candidate.is_file():
            schedule_snapshot_path = candidate
            break
    if not _is_historical_date(str(d)):
        remote_games = [dict(game) for game in _fetch_schedule_games_remote_cached(str(d))]
        if remote_games:
            return remote_games
    schedule_snapshot = _load_json_file(schedule_snapshot_path)
    if isinstance(schedule_snapshot, list):
        return [game for game in schedule_snapshot if isinstance(game, dict)]
    if isinstance(schedule_snapshot, dict):
        games = (((schedule_snapshot.get("dates") or [{}])[0]).get("games") or [])
        return [game for game in games if isinstance(game, dict)]
    return [dict(game) for game in _fetch_schedule_games_remote_cached(str(d))]


def _supplement_card_status_from_live_feed(
    card: Dict[str, Any],
    d: str,
    *,
    feed: Optional[Dict[str, Any]] = None,
) -> None:
    if not _is_current_local_date(str(d or "")):
        return
    if not isinstance(card, dict):
        return
    game_pk = _safe_int(card.get("gamePk"))
    if not game_pk or int(game_pk) <= 0:
        return

    current_status = card.get("status") if isinstance(card.get("status"), dict) else {}
    current_abstract = str(current_status.get("abstract") or "").strip()
    current_detailed = str(current_status.get("detailed") or "").strip()

    if not isinstance(feed, dict) or not feed:
        feed = _load_live_lens_feed(int(game_pk), str(d))
    if not isinstance(feed, dict) or not feed:
        return

    feed_status = (((feed.get("gameData") or {}).get("status") or {}))
    feed_abstract = str(feed_status.get("abstractGameState") or "").strip()
    feed_detailed = str(feed_status.get("detailedState") or "").strip()
    if not feed_abstract and not feed_detailed:
        return

    should_override = False
    if _status_is_final({"abstract": feed_abstract, "detailed": feed_detailed}):
        should_override = True
    elif _status_is_live({"abstract": feed_abstract, "detailed": feed_detailed}):
        should_override = True
    elif str(feed_detailed).strip().lower() == "warmup":
        should_override = True
    elif not current_abstract and not current_detailed:
        should_override = True

    if not should_override:
        return

    card["status"] = {
        "abstract": feed_abstract or current_abstract,
        "detailed": feed_detailed or current_detailed,
    }

    away_totals = _team_totals(feed, "away")
    home_totals = _team_totals(feed, "home")
    away_runs = _safe_int(away_totals.get("R"))
    home_runs = _safe_int(home_totals.get("R"))
    if away_runs is not None or home_runs is not None:
        card["score"] = {"away": away_runs, "home": home_runs}


def _cards_list_from_sources(
    *,
    d: str,
    schedule_games: List[Dict[str, Any]],
    outputs_by_game: Dict[int, Dict[str, Any]],
    recos_by_game: Dict[int, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    cards_by_game: Dict[int, Dict[str, Any]] = {}
    sim_segment_cache: Dict[int, Dict[str, Any]] = {}

    def _sim_segments_for_game(game_pk: int) -> Dict[str, Any]:
        cached = sim_segment_cache.get(int(game_pk))
        if cached is not None:
            return cached
        out: Dict[str, Any] = {}
        sim_path = _find_sim_file(game_pk=int(game_pk), d=d)
        if sim_path is not None:
            try:
                sim_obj = json.loads(sim_path.read_text(encoding="utf-8"))
            except Exception:
                sim_obj = {}
            sim_segments = (((sim_obj.get("sim") or {}).get("segments") or {})) if isinstance(sim_obj, dict) else {}
            if isinstance(sim_segments, dict):
                out = {
                    str(key): dict(value)
                    for key, value in sim_segments.items()
                    if isinstance(value, dict)
                }
        sim_segment_cache[int(game_pk)] = out
        return out

    def _merge_prediction_row(base_row: Any, sim_segments: Dict[str, Any], key: str, *, normalize_full: bool = False) -> Dict[str, Any]:
        base = dict(base_row) if isinstance(base_row, dict) else {}
        sim_row = dict(sim_segments.get(str(key)) or {}) if isinstance(sim_segments.get(str(key)), dict) else {}
        merged = dict(base)
        for field in (
            "home_win_prob",
            "away_win_prob",
            "tie_prob",
            "away_runs_mean",
            "home_runs_mean",
            "total_runs_dist",
            "run_margin_dist",
            "samples",
        ):
            if field not in merged or merged.get(field) in (None, {}, []):
                if sim_row.get(field) not in (None, {}, []):
                    merged[field] = sim_row.get(field)
        return _normalized_full_game_probs(merged) if normalize_full else merged

    def _card_sort_key(card: Dict[str, Any]) -> Tuple[int, int, str, int]:
        status_block = card.get("status") or {}
        abstract = str(status_block.get("abstract") or "").strip().lower()
        detailed = str(status_block.get("detailed") or "").strip().lower()
        if any(token in abstract or token in detailed for token in ("live", "in progress", "manager challenge")):
            status_weight = 0
        elif abstract == "final" or "game over" in detailed or "completed early" in detailed:
            status_weight = 2
        else:
            status_weight = 1
        game_date = str(card.get("gameDate") or "").strip()
        has_game_date = 0 if game_date else 1
        return (status_weight, has_game_date, game_date, int(card.get("gamePk") or 0))

    def _merge_output_status(card_status: Dict[str, Any], row: Dict[str, Any]) -> None:
        row_status = {
            "abstract": _first_text(row.get("status_abstract")),
            "detailed": _first_text(row.get("status_detailed")),
        }
        if not row_status["abstract"] and not row_status["detailed"]:
            return
        if not card_status.get("abstract"):
            card_status["abstract"] = row_status["abstract"]
        if not card_status.get("detailed"):
            card_status["detailed"] = row_status["detailed"]
        if _status_is_final(row_status) and not _status_is_final(card_status):
            card_status["abstract"] = row_status["abstract"]
            card_status["detailed"] = row_status["detailed"]

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
        sim_segments = _sim_segments_for_game(int(game_pk))
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

        _merge_output_status(card["status"], row)

        if not card["gameType"]:
            card["gameType"] = _first_text(row.get("game_type"), card.get("gameType"))
        if not card["gameDate"]:
            card["gameDate"] = _first_text(row.get("game_date"), row.get("commence_time"))
            card["startTime"] = _format_start_time_local(card["gameDate"])

        card["predictions"] = {
            "full": _merge_prediction_row(row.get("full") or {}, sim_segments, "full", normalize_full=True),
            "first1": _merge_prediction_row(row.get("first1") or {}, sim_segments, "first1"),
            "first5": _merge_prediction_row(row.get("first5") or {}, sim_segments, "first5"),
            "first3": _merge_prediction_row(row.get("first3") or {}, sim_segments, "first3"),
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

    cards = sorted(cards_by_game.values(), key=_card_sort_key)
    for card in cards:
        card.pop("sortOrder", None)
        first1_signal = _cards_first1_bet_signal(card)
        if isinstance(first1_signal, dict):
            card["first1BetSignal"] = first1_signal
        else:
            card.pop("first1BetSignal", None)
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


def _cards_first1_zero_run_prob(row: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(row, dict):
        return None
    direct = _safe_float(row.get("nrfi_prob"))
    if direct is not None:
        return max(0.0, min(1.0, float(direct)))
    dist = row.get("total_runs_dist") or {}
    if not isinstance(dist, dict) or not dist:
        return None
    total_weight = 0.0
    zero_weight = 0.0
    for raw_key, raw_value in dist.items():
        weight = _safe_float(raw_value)
        if weight is None or weight < 0:
            continue
        total_weight += float(weight)
        key_int = _safe_int(raw_key)
        if key_int is not None and int(key_int) == 0:
            zero_weight += float(weight)
    if total_weight <= 0.0:
        return None
    return max(0.0, min(1.0, zero_weight / total_weight))


def _cards_first1_bet_signal(card: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(card, dict):
        return None
    predictions = card.get("predictions") or {}
    first1 = predictions.get("first1") if isinstance(predictions.get("first1"), dict) else {}
    if not isinstance(first1, dict) or not first1:
        return None

    nrfi_prob = _cards_first1_zero_run_prob(first1)
    if nrfi_prob is None:
        return None
    yrfi_prob = max(0.0, min(1.0, 1.0 - float(nrfi_prob)))
    away_runs_mean = _safe_float(first1.get("away_runs_mean"))
    home_runs_mean = _safe_float(first1.get("home_runs_mean"))
    mean_total_runs = None
    if away_runs_mean is not None or home_runs_mean is not None:
        mean_total_runs = float(away_runs_mean or 0.0) + float(home_runs_mean or 0.0)
    away_win_prob = _safe_float(first1.get("away_win_prob"))
    home_win_prob = _safe_float(first1.get("home_win_prob"))
    max_side_prob = max(float(away_win_prob or 0.0), float(home_win_prob or 0.0))

    if mean_total_runs is None:
        return None

    label = None
    tone = None
    summary = None
    detail = None

    if float(nrfi_prob) >= 0.56 and float(mean_total_runs) <= 0.75:
        label = "F1 NRFI"
        tone = "nrfi"
        summary = f"0-run sim {float(nrfi_prob) * 100.0:.1f}% | F1 mean {float(mean_total_runs):.2f}"
        detail = (
            f"Season filter qualified: simulated scoreless first inning {float(nrfi_prob) * 100.0:.1f}% "
            f"with only {float(mean_total_runs):.2f} expected runs in the opening frame."
        )
    elif float(nrfi_prob) <= 0.55 and float(mean_total_runs) >= 1.0 and float(max_side_prob) >= 0.29:
        label = "F1 YRFI"
        tone = "yrfi"
        summary = (
            f"F1 mean {float(mean_total_runs):.2f} | side lead {float(max_side_prob) * 100.0:.1f}%"
        )
        detail = (
            f"Season filter qualified: only {float(nrfi_prob) * 100.0:.1f}% simulated NRFI, "
            f"{float(mean_total_runs):.2f} expected first-inning runs, and one side reaches a "
            f"{float(max_side_prob) * 100.0:.1f}% chance to be ahead after one."
        )
    else:
        return None

    return {
        "label": label,
        "tone": tone,
        "summary": summary,
        "detail": detail,
        "nrfiProb": round(float(nrfi_prob), 4),
        "yrfiProb": round(float(yrfi_prob), 4),
        "meanTotalRuns": round(float(mean_total_runs), 3),
        "maxSideLeadProb": round(float(max_side_prob), 4),
    }


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
    report_segments = report_game.get("segments") if isinstance(report_game.get("segments"), dict) else {}
    full = (report_segments.get("full") or {})
    mean_total = _safe_float(full.get("mean_total_runs"))
    margin = _safe_float(full.get("mean_run_margin_home_minus_away"))
    away_runs_mean: Optional[float] = None
    home_runs_mean: Optional[float] = None
    if mean_total is not None and margin is not None:
        home_runs_mean = round((float(mean_total) + float(margin)) / 2.0, 3)
        away_runs_mean = round(float(mean_total) - float(home_runs_mean), 3)

    segments_out = {
        key: dict(report_segments.get(key) or {})
        for key in ("full", "first1", "first3", "first5")
    }
    full_out = segments_out.get("full") if isinstance(segments_out.get("full"), dict) else {}
    if away_runs_mean is not None and full_out.get("away_runs_mean") is None:
        full_out["away_runs_mean"] = away_runs_mean
    if home_runs_mean is not None and full_out.get("home_runs_mean") is None:
        full_out["home_runs_mean"] = home_runs_mean

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
                "batters_faced_mean": _safe_float(pred.get("batters_faced_mean")),
            }

    return {
        "game_pk": _safe_int(report_game.get("game_pk")),
        "away": report_game.get("away") or {},
        "home": report_game.get("home") or {},
        "starters": report_game.get("starters") or {},
        "starter_names": report_game.get("starter_names") or {},
        "sim": {
            "sims": _safe_int(sim_count),
            "segments": segments_out,
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
    return today


def _season_batch_dir(season: int) -> Path:
    return _DATA_DIR / "eval" / "batches" / f"season_{int(season)}_ui_daily_live"


def _season_output_dir(season: int) -> Path:
    return _DATA_DIR / "eval" / "seasons" / str(int(season))


def _path_mtime(path: Optional[Path]) -> Optional[float]:
    if not path or not path.exists():
        return None
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return None


def _latest_report_mtime(batch_dir: Path) -> Optional[float]:
    if not batch_dir.exists() or not batch_dir.is_dir():
        return None
    latest: Optional[float] = None
    try:
        for path in batch_dir.glob("sim_vs_actual_*.json"):
            if not path.is_file():
                continue
            value = _path_mtime(path)
            if value is None:
                continue
            latest = value if latest is None else max(latest, value)
    except OSError:
        return None
    return latest


def _ensure_fresh_season_manifests(season: int, betting_profile: str = "retuned") -> None:
    if not _is_inline_season_manifest_rebuild_enabled():
        return
    batch_dir = _season_batch_dir(int(season))
    season_dir = _season_output_dir(int(season))
    latest_report = _latest_report_mtime(batch_dir)
    if latest_report is None:
        return

    normalized_profile = str(betting_profile or "retuned").strip().lower()
    if normalized_profile not in ("baseline", "retuned"):
        normalized_profile = "retuned"
    betting_manifest_name = (
        "season_betting_cards_retuned_manifest.json"
        if normalized_profile == "retuned"
        else "season_betting_cards_manifest.json"
    )
    season_manifest_path = season_dir / "season_eval_manifest.json"
    betting_manifest_path = season_dir / betting_manifest_name
    season_manifest_mtime = _path_mtime(season_manifest_path)
    betting_manifest_mtime = _path_mtime(betting_manifest_path)
    needs_rebuild = (
        season_manifest_mtime is None
        or betting_manifest_mtime is None
        or season_manifest_mtime < latest_report
        or betting_manifest_mtime < latest_report
    )
    if not needs_rebuild:
        return
    try:
        _publish_season_manifests(
            season=int(season),
            batch_dir=batch_dir,
            betting_profile=normalized_profile,
            season_dir=season_dir,
        )
    except Exception:
        return


def _find_season_manifest_path(season: int) -> Optional[Path]:
    _ensure_fresh_season_manifests(int(season), "retuned")
    season_dirs = [data_root / "eval" / "seasons" / str(int(season)) for data_root in _data_roots()]
    seen: set[str] = set()
    for season_dir in season_dirs:
        resolved_dir = season_dir.resolve()
        key = str(resolved_dir)
        if key in seen:
            continue
        seen.add(key)
        candidates = [
            resolved_dir / "season_eval_manifest.json",
            resolved_dir / "manifest.json",
        ]
        for path in candidates:
            if path.exists() and path.is_file():
                return path
        if not resolved_dir.exists() or not resolved_dir.is_dir():
            continue
        try:
            extra = sorted(
                [path for path in resolved_dir.glob("*.json") if path.is_file()],
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            continue
        if extra:
            return extra[0]
    return None


def _load_season_manifest(season: int) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    path = _find_season_manifest_path(int(season))
    return path, _load_json_file(path)


def _season_frontend_profile_slug(requested_profile: str) -> str:
    token = str(requested_profile or "").strip().lower()
    if token in {"", "default", "current", "live"}:
        return "retuned"
    return token


def daily_season_frontend_dir(*, data_root: Optional[Path] = None) -> Path:
    root = data_root.resolve() if isinstance(data_root, Path) else _DATA_DIR
    return root / "daily" / "season_frontend"


def daily_season_manifest_artifact_path(season: int, date_str: str, *, data_root: Optional[Path] = None) -> Path:
    return daily_season_frontend_dir(data_root=data_root) / f"season_manifest_{int(season)}_{_date_slug(date_str)}.json"


def daily_season_day_artifact_path(season: int, date_str: str, *, profile: str = "retuned", data_root: Optional[Path] = None) -> Path:
    profile_slug = _season_frontend_profile_slug(profile)
    return daily_season_frontend_dir(data_root=data_root) / f"season_day_{int(season)}_{_date_slug(date_str)}_{profile_slug}.json"


def daily_season_betting_day_artifact_path(season: int, date_str: str, *, profile: str = "retuned", data_root: Optional[Path] = None) -> Path:
    profile_slug = _season_frontend_profile_slug(profile)
    return daily_season_frontend_dir(data_root=data_root) / f"season_betting_day_{int(season)}_{_date_slug(date_str)}_{profile_slug}.json"


def daily_official_betting_card_day_artifact_path(season: int, date_str: str, *, profile: str = "retuned", data_root: Optional[Path] = None) -> Path:
    profile_slug = _season_frontend_profile_slug(profile)
    return daily_season_frontend_dir(data_root=data_root) / f"season_official_betting_day_{int(season)}_{_date_slug(date_str)}_{profile_slug}.json"


def _load_daily_season_frontend_artifact(path_factory: Any) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    candidates: List[Path] = []
    for root in _data_roots():
        candidate = path_factory(root)
        if candidate not in candidates:
            candidates.append(candidate)
    artifact_path = _find_preferred_file(candidates)
    if not artifact_path:
        return None, None
    return artifact_path, _load_json_file(artifact_path)


def _prebuilt_season_manifest_payload(season: int) -> Optional[Dict[str, Any]]:
    if int(season) != int(_season_from_date_str(_today_iso()) or 0):
        return None
    artifact_path, artifact_doc = _load_daily_season_frontend_artifact(
        lambda root: daily_season_manifest_artifact_path(int(season), _today_iso(), data_root=root)
    )
    if not artifact_path or not isinstance(artifact_doc, dict) or not artifact_doc.get("found"):
        return None
    out = dict(artifact_doc)
    out["artifactPath"] = _relative_path_str(artifact_path)
    out["artifactSource"] = "daily_update"
    return out


def _prebuilt_season_day_payload(season: int, date_str: str, requested_profile: str) -> Optional[Dict[str, Any]]:
    if str(date_str or "").strip() != _today_iso():
        return None
    artifact_path, artifact_doc = _load_daily_season_frontend_artifact(
        lambda root: daily_season_day_artifact_path(int(season), str(date_str), profile=requested_profile, data_root=root)
    )
    if not artifact_path or not isinstance(artifact_doc, dict) or not artifact_doc.get("found"):
        return None
    out = dict(artifact_doc)
    out["artifactPath"] = _relative_path_str(artifact_path)
    out["artifactSource"] = "daily_update"
    return out


def _prebuilt_season_betting_day_payload(season: int, date_str: str, requested_profile: str) -> Optional[Dict[str, Any]]:
    if str(date_str or "").strip() != _today_iso():
        return None
    artifact_path, artifact_doc = _load_daily_season_frontend_artifact(
        lambda root: daily_season_betting_day_artifact_path(int(season), str(date_str), profile=requested_profile, data_root=root)
    )
    if not artifact_path or not isinstance(artifact_doc, dict) or not artifact_doc.get("found"):
        return None
    out = dict(artifact_doc)
    out["artifactPath"] = _relative_path_str(artifact_path)
    out["artifactSource"] = "daily_update"
    return out


def _prebuilt_official_betting_card_day_payload(season: int, date_str: str, requested_profile: str) -> Optional[Dict[str, Any]]:
    if str(date_str or "").strip() != _today_iso():
        return None
    artifact_path, artifact_doc = _load_daily_season_frontend_artifact(
        lambda root: daily_official_betting_card_day_artifact_path(int(season), str(date_str), profile=requested_profile, data_root=root)
    )
    if not artifact_path or not isinstance(artifact_doc, dict) or not artifact_doc.get("found"):
        return None
    out = dict(artifact_doc)
    out["artifactPath"] = _relative_path_str(artifact_path)
    out["artifactSource"] = "daily_update"
    return out


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


def _available_daily_locked_card_dates(season: int) -> Tuple[str, ...]:
    prefix = "daily_summary_"
    suffix = "_locked_policy.json"
    out: List[str] = []
    seen: set[str] = set()
    for data_root in _data_roots():
        daily_dir = data_root / "daily"
        if not daily_dir.exists() or not daily_dir.is_dir():
            continue
        try:
            candidates = sorted(daily_dir.glob(f"daily_summary_{int(season)}_*_locked_policy.json"))
        except OSError:
            continue
        for path in candidates:
            name = path.name
            if not name.startswith(prefix) or not name.endswith(suffix):
                continue
            date_slug = name[len(prefix):-len(suffix)]
            date_str = str(date_slug).replace("_", "-")
            try:
                parsed = date.fromisoformat(date_str)
            except Exception:
                continue
            if parsed.year != int(season):
                continue
            normalized = parsed.isoformat()
            if normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
    return tuple(sorted(out))


def _lightweight_betting_cards_hint(season: int, daily_artifacts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    artifacts = daily_artifacts if isinstance(daily_artifacts, dict) else {}
    available_profiles = sorted(_available_season_betting_profiles(int(season)).keys())
    locked_policy_path = artifacts.get("locked_policy_path") if isinstance(artifacts.get("locked_policy_path"), Path) else None
    has_betting_card = bool(locked_policy_path and locked_policy_path.exists() and locked_policy_path.is_file())
    default_profile = None
    if available_profiles:
        default_profile = "retuned" if "retuned" in available_profiles else available_profiles[0]
    elif has_betting_card:
        default_profile = "retuned"

    profiles: Dict[str, Any] = {}
    if has_betting_card and default_profile:
        profiles[str(default_profile)] = {
            "available": True,
            "card_path": _relative_path_str(locked_policy_path),
            "selected_counts": _betting_selected_counts_with_defaults({}),
            "playable_counts": _betting_selected_counts_with_defaults({}),
            "results": {"combined": _blank_settled_summary()},
        }

    return {
        "available": bool(has_betting_card),
        "available_profiles": available_profiles,
        "default_profile": default_profile,
        "profiles": profiles,
    }


def _daily_artifact_game_count(daily_artifacts: Optional[Dict[str, Any]] = None) -> int:
    artifacts = daily_artifacts if isinstance(daily_artifacts, dict) else {}
    game_summary = artifacts.get("game_summary") if isinstance(artifacts.get("game_summary"), dict) else {}
    games = _safe_int(game_summary.get("games"))
    if games is not None and int(games) >= 0:
        return int(games)

    sim_dir = artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None
    if sim_dir and sim_dir.exists() and sim_dir.is_dir():
        try:
            return int(sum(1 for path in sim_dir.glob("sim_*.json") if path.is_file()))
        except OSError:
            return 0
    return 0


def _supplemental_season_day_row(season: int, date_str: str) -> Optional[Dict[str, Any]]:
    daily_artifacts = _load_cards_artifacts(str(date_str))
    has_cards = bool(daily_artifacts.get("locked_policy") or daily_artifacts.get("game_summary"))
    if not has_cards:
        return None
    game_count = _daily_artifact_game_count(daily_artifacts)
    betting_day_payload = _season_betting_day_payload(int(season), str(date_str), "")
    betting_payload_found = bool(betting_day_payload.get("found"))
    betting_summary = betting_day_payload.get("summary") if isinstance(betting_day_payload.get("summary"), dict) else {}
    selected_counts = _betting_selected_counts_with_defaults(
        betting_day_payload.get("selected_counts") or betting_summary.get("selected_counts") or {}
    )
    playable_selected_counts = _betting_selected_counts_with_defaults(
        betting_day_payload.get("playable_selected_counts") or betting_summary.get("playable_selected_counts") or {}
    )
    results = _merge_settled_results_blocks([betting_day_payload.get("results") or {}])
    combined = results.get("combined") or _blank_settled_summary()
    betting_cards = _lightweight_betting_cards_hint(int(season), daily_artifacts)
    default_profile = str(betting_cards.get("default_profile") or betting_day_payload.get("profile") or "").strip()
    if betting_payload_found and default_profile:
        merged_profiles = dict(betting_cards.get("profiles") or {})
        merged_profiles[default_profile] = {
            **dict(merged_profiles.get(default_profile) or {}),
            "available": True,
            "card_path": betting_day_payload.get("card_source"),
            "selected_counts": selected_counts,
            "playable_counts": playable_selected_counts,
            "results": results,
        }
        betting_cards["profiles"] = merged_profiles
        available_profiles = list(betting_cards.get("available_profiles") or [])
        if default_profile not in available_profiles:
            available_profiles.append(default_profile)
            available_profiles.sort()
            betting_cards["available_profiles"] = available_profiles
        betting_cards["default_profile"] = default_profile
        betting_cards["available"] = True

    return {
        "date": str(date_str),
        "month": str(date_str)[:7],
        "games": int(game_count),
        "cards_available": bool(has_cards),
        "legacy_cards_available": bool(has_cards),
        "cards_url": f"/?date={date_str}" if has_cards else None,
        "legacy_cards_url": f"/?date={date_str}" if has_cards else None,
        "betting_cards": betting_cards,
        "full_game": {
            "moneyline": {},
            "totals": {},
            "runline_fav_minus_1_5": {},
            "pitcher_props_starters": {},
            "pitcher_props_at_market_lines": {},
        },
        "aggregate": {
            "full": {"games": int(game_count)},
            "first5": {"games": int(game_count)},
            "first3": {"games": int(game_count)},
        },
        "cap_profile": betting_day_payload.get("cap_profile"),
        "card_path": betting_day_payload.get("card_source"),
        "selected_counts": selected_counts,
        "results": results,
        "profit_u": round(float(combined.get("profit_u") or 0.0), 4),
        "roi": combined.get("roi"),
        "settled_n": int(combined.get("n") or 0),
        "unresolved_n": int(betting_summary.get("unresolved_n") or 0),
        "source_kind": betting_day_payload.get("source_kind"),
        "_betting_payload_found": bool(betting_payload_found),
        "report_path": None,
    }


def _season_day_row_with_refreshed_cards(season: int, row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    date_str = str(out.get("date") or "").strip()
    if not date_str:
        return out
    supplemental = _supplemental_season_day_row(int(season), date_str)
    if not isinstance(supplemental, dict):
        return out
    betting_payload_found = bool(supplemental.get("_betting_payload_found"))
    out["month"] = str(out.get("month") or supplemental.get("month") or date_str[:7])
    out["games"] = int(out.get("games") or supplemental.get("games") or 0)
    out["cards_available"] = bool(supplemental.get("cards_available"))
    out["legacy_cards_available"] = bool(supplemental.get("legacy_cards_available"))
    out["cards_url"] = supplemental.get("cards_url")
    out["legacy_cards_url"] = supplemental.get("legacy_cards_url")
    if betting_payload_found:
        out["cap_profile"] = supplemental.get("cap_profile")
        out["card_path"] = supplemental.get("card_path")
        out["selected_counts"] = supplemental.get("selected_counts")
        out["results"] = supplemental.get("results")
        out["profit_u"] = supplemental.get("profit_u")
        out["roi"] = supplemental.get("roi")
        out["settled_n"] = supplemental.get("settled_n")
        out["unresolved_n"] = supplemental.get("unresolved_n")
        out["source_kind"] = supplemental.get("source_kind")
    existing_betting_cards = dict(out.get("betting_cards") or {})
    supplemental_betting_cards = dict(supplemental.get("betting_cards") or {})
    if existing_betting_cards:
        merged_profiles = dict(existing_betting_cards.get("profiles") or {})
        for profile_name, profile_info in (supplemental_betting_cards.get("profiles") or {}).items():
            if not isinstance(profile_info, dict):
                continue
            if betting_payload_found or profile_name not in merged_profiles:
                merged_profiles[str(profile_name)] = {
                    **dict(merged_profiles.get(profile_name) or {}),
                    **dict(profile_info),
                }
        if merged_profiles:
            existing_betting_cards["profiles"] = merged_profiles
        if supplemental_betting_cards.get("available"):
            existing_betting_cards["available"] = True
        if supplemental_betting_cards.get("available_profiles"):
            merged_available_profiles = sorted(
                {
                    *[str(name) for name in (existing_betting_cards.get("available_profiles") or [])],
                    *[str(name) for name in (supplemental_betting_cards.get("available_profiles") or [])],
                }
            )
            existing_betting_cards["available_profiles"] = merged_available_profiles
        if not existing_betting_cards.get("default_profile") and supplemental_betting_cards.get("default_profile"):
            existing_betting_cards["default_profile"] = supplemental_betting_cards.get("default_profile")
        out["betting_cards"] = existing_betting_cards
    else:
        out["betting_cards"] = supplemental_betting_cards
    return out


def _should_refresh_season_day_row(season: int, row: Dict[str, Any]) -> bool:
    date_str = str((row or {}).get("date") or "").strip()
    if not date_str:
        return False
    if int(season) == int(_season_from_date_str(date_str) or 0) and date_str == _today_iso():
        return True

    if not bool(row.get("cards_available") or row.get("legacy_cards_available")):
        return True

    betting_cards = row.get("betting_cards") if isinstance(row.get("betting_cards"), dict) else {}
    if not bool(betting_cards.get("available")):
        return True

    return False


def _supplement_season_manifest_payload(season: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    existing_days: List[Dict[str, Any]] = []
    for raw_row in out.get("days") or []:
        if not isinstance(raw_row, dict):
            continue
        row = dict(raw_row)
        if _should_refresh_season_day_row(int(season), row):
            row = _season_day_row_with_refreshed_cards(int(season), row)
        existing_days.append(row)
    seen_dates = {str(row.get("date") or "").strip() for row in existing_days if str(row.get("date") or "").strip()}
    manifest_floor = min(seen_dates) if seen_dates else None
    supplemental_dates = [
        d for d in _available_daily_locked_card_dates(int(season))
        if d not in seen_dates and (not manifest_floor or d >= manifest_floor)
    ]

    for date_str in supplemental_dates:
        row = _supplemental_season_day_row(int(season), date_str)
        if isinstance(row, dict):
            existing_days.append(row)

    existing_days.sort(key=lambda row: str(row.get("date") or ""))
    out["days"] = existing_days

    months = [dict(row) for row in (out.get("months") or []) if isinstance(row, dict)]
    month_counts: Dict[str, int] = defaultdict(int)
    for row in existing_days:
        month_counts[str(row.get("month") or str(row.get("date") or "")[:7])] += 1
    months_by_key = {str(row.get("month") or ""): row for row in months}
    for month_key, count in month_counts.items():
        if month_key in months_by_key:
            months_by_key[month_key]["days"] = int(count)
        else:
            months_by_key[month_key] = {
                "month": str(month_key),
                "label": _season_betting_month_label(str(month_key)),
                "days": int(count),
                "full_game": {},
                "hitter_hr_likelihood_topn": {},
                "hitter_props_likelihood_topn": {},
                "segments": {},
            }
    out["months"] = [months_by_key[key] for key in sorted(months_by_key)]
    return out


def build_current_day_season_manifest_artifact(season: int, date_str: str) -> Dict[str, Any]:
    manifest_path, manifest = _load_season_manifest(int(season))
    if not manifest_path or not isinstance(manifest, dict):
        return {
            "season": int(season),
            "date": str(date_str),
            "found": False,
            "error": "season_manifest_missing",
        }
    payload = _supplement_season_manifest_payload(int(season), dict(manifest))
    meta = dict(payload.get("meta") or {})
    sources = dict(meta.get("sources") or {})
    sources["manifest"] = _relative_path_str(manifest_path)
    meta["sources"] = sources
    payload["meta"] = meta
    payload["found"] = True
    payload["artifactDate"] = str(date_str)
    return payload


def _season_day_fallback_payload(season: int, date_str: str, betting_profile: str) -> Dict[str, Any]:
    daily_artifacts = _load_cards_artifacts(str(date_str))
    has_cards = bool(daily_artifacts.get("locked_policy") or daily_artifacts.get("game_summary"))
    betting_payload = _season_betting_day_payload(int(season), str(date_str), betting_profile)
    betting_games = betting_payload.get("games") if isinstance(betting_payload.get("games"), dict) else {}

    schedule_games: List[Dict[str, Any]] = []
    try:
        schedule_games = fetch_schedule_for_date(_client(), str(date_str)) or []
    except Exception:
        schedule_games = []
    recos_by_game = _recommendations_by_game(daily_artifacts.get("locked_policy") or {})
    if betting_payload.get("found"):
        for raw_game_pk, raw_game_betting in betting_games.items():
            game_pk = _safe_int(raw_game_pk)
            game_betting = raw_game_betting if isinstance(raw_game_betting, dict) else {}
            markets = game_betting.get("markets") if isinstance(game_betting.get("markets"), dict) else {}
            if not game_pk or int(game_pk) <= 0 or int(game_pk) in recos_by_game:
                continue
            recos_by_game[int(game_pk)] = {
                "totals": dict(markets.get("totals") or {}) if isinstance(markets.get("totals"), dict) else None,
                "ml": dict(markets.get("ml") or {}) if isinstance(markets.get("ml"), dict) else None,
                "pitcher_props": [dict(row) for row in (markets.get("pitcherProps") or []) if isinstance(row, dict)],
                "hitter_props": [dict(row) for row in (markets.get("hitterProps") or []) if isinstance(row, dict)],
                "extra_pitcher_props": [dict(row) for row in (markets.get("extraPitcherProps") or []) if isinstance(row, dict)],
                "extra_hitter_props": [dict(row) for row in (markets.get("extraHitterProps") or []) if isinstance(row, dict)],
            }
    cards = _cards_list_from_sources(
        d=str(date_str),
        schedule_games=schedule_games,
        outputs_by_game={},
        recos_by_game=recos_by_game,
    )

    games_out: List[Dict[str, Any]] = []
    for card in cards:
        if not isinstance(card, dict):
            continue
        game_pk = _safe_int(card.get("gamePk"))
        probable = card.get("probable") or {}
        game_betting = None
        if betting_payload.get("found"):
            game_betting = dict(betting_games.get(int(game_pk or 0)) or _empty_game_betting())
        games_out.append(
            {
                "game_pk": game_pk,
                "game_date": card.get("gameDate") or "",
                "start_time": card.get("startTime") or "",
                "official_date": card.get("officialDate") or str(date_str),
                "status": {
                    "abstract": str(((card.get("status") or {}).get("abstract") or "")),
                    "detailed": str(((card.get("status") or {}).get("detailed") or "")),
                },
                "away": card.get("away") or {},
                "home": card.get("home") or {},
                "starter_names": {
                    "away": _first_text((probable.get("away") or {}).get("fullName")),
                    "home": _first_text((probable.get("home") or {}).get("fullName")),
                },
                "segments": {},
                "pitcher_props": {},
                "betting": game_betting,
            }
        )

    betting_payload.pop("games", None)
    return {
        "season": int(season),
        "date": str(date_str),
        "cards_available": bool(has_cards),
        "cards_url": (f"/?date={date_str}" if has_cards else None),
        "source_file": None,
        "meta": {
            "sims_per_game": None,
            "season": int(season),
            "generated_at": _local_timestamp_text(),
            "use_raw": None,
            "jobs": None,
            "skipped_games": 0,
        },
        "summary": {
            "aggregate": {"full": {}, "first5": {}, "first3": {}},
            "full_game": {
                "totals": {},
                "moneyline": {},
                "runline_fav_minus_1_5": {},
                "pitcher_props_starters": {},
                "pitcher_props_at_market_lines": {},
            },
        },
        "betting": betting_payload,
        "games": games_out,
    }


def build_current_day_season_day_artifact(season: int, date_str: str, betting_profile: str) -> Dict[str, Any]:
    manifest_path, manifest = _load_season_manifest(int(season))
    if manifest_path and isinstance(manifest, dict):
        report_path = _resolve_season_day_report_path(manifest, str(date_str))
        if report_path and report_path.exists() and report_path.is_file():
            report_obj = _load_json_file(report_path)
            if isinstance(report_obj, dict):
                payload = _season_day_payload(
                    season=int(season),
                    season_manifest=manifest,
                    day_report=report_obj,
                    report_path=report_path,
                    betting_profile=betting_profile,
                )
                payload["found"] = True
                payload["manifest_source"] = _relative_path_str(manifest_path)
                payload["artifactDate"] = str(date_str)
                return payload

    fallback_payload = _season_day_fallback_payload(int(season), str(date_str), betting_profile)
    if fallback_payload.get("cards_available") or ((fallback_payload.get("betting") or {}).get("found")):
        fallback_payload["found"] = True
        fallback_payload["manifest_source"] = _relative_path_str(manifest_path)
        fallback_payload["artifactDate"] = str(date_str)
        return fallback_payload
    return {
        "season": int(season),
        "date": str(date_str),
        "found": False,
        "error": "season_day_missing" if manifest_path else "season_manifest_missing",
        "manifest_source": _relative_path_str(manifest_path),
        "artifactDate": str(date_str),
    }


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
    today = _today_iso()
    if _season_from_date_str(today) == int(season) and today > max_date:
        max_date = today
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
    for data_root in _data_roots():
        day_dir = data_root / "raw" / "statsapi" / "feed_live" / str(int(season)) / str(date_str)
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


def _should_load_cards_archive_context(date_str: str, artifacts: Optional[Dict[str, Any]] = None) -> bool:
    if _is_historical_date(str(date_str or "")):
        return True
    artifact_map = artifacts if isinstance(artifacts, dict) else _load_cards_artifacts(str(date_str or ""))
    return not bool(artifact_map.get("locked_policy") or artifact_map.get("game_summary"))


def _season_betting_manifest_candidates(season: int) -> Dict[str, List[Path]]:
    season_dirs = [data_root / "eval" / "seasons" / str(int(season)) for data_root in _data_roots()]
    return {
        "baseline": [season_dir / "season_betting_cards_manifest.json" for season_dir in season_dirs],
        "retuned": [season_dir / "season_betting_cards_retuned_manifest.json" for season_dir in season_dirs],
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
    requested = str(requested_profile or "").strip().lower()
    if requested in ("baseline", "retuned"):
        selected_profile = requested
    elif requested in ("", "default", "current", "live"):
        selected_profile = "retuned"
    else:
        selected_profile = requested or "retuned"

    _ensure_fresh_season_manifests(int(season), selected_profile)
    available = _available_season_betting_profiles(int(season))
    if requested in ("", "default", "current", "live"):
        selected_profile = "retuned" if "retuned" in available else "baseline"
    elif selected_profile not in available and available:
        selected_profile = "retuned" if "retuned" in available else next(iter(available.keys()))

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


def _resolve_season_betting_day_payload_path(betting_manifest: Dict[str, Any], date_str: str) -> Optional[Path]:
    for row in betting_manifest.get("days") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("date") or "").strip() != str(date_str or "").strip():
            continue
        candidate = _path_from_maybe_relative(row.get("payload_path"))
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


def _infer_betting_selected_counts_from_card(card_obj: Any, reco_key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {key: 0 for key in _BETTING_COUNT_KEYS}
    markets = (card_obj or {}).get("markets") if isinstance(card_obj, dict) else {}
    if not isinstance(markets, dict):
        return counts

    for raw_market_name, market_info in markets.items():
        if not isinstance(market_info, dict):
            continue
        market_name = str(raw_market_name or "").strip().lower()
        if market_name not in counts:
            continue
        recs = market_info.get(reco_key) or []
        if not isinstance(recs, list):
            continue
        counts[market_name] += int(sum(1 for rec in recs if isinstance(rec, dict)))

    if counts["hitter_props"] <= 0:
        counts["hitter_props"] = int(
            counts["hitter_home_runs"]
            + counts["hitter_hits"]
            + counts["hitter_total_bases"]
            + counts["hitter_runs"]
            + counts["hitter_rbis"]
        )
    counts["combined"] = int(counts["totals"] + counts["ml"] + counts["pitcher_props"] + counts["hitter_props"])
    return counts


def _blank_settled_summary() -> Dict[str, Any]:
    return {
        "n": 0,
        "wins": 0,
        "losses": 0,
        "stake_u": 0.0,
        "profit_u": 0.0,
        "roi": None,
    }


def _merge_settled_summary_blocks(blocks: Sequence[Any]) -> Dict[str, Any]:
    total_n = 0
    total_wins = 0
    total_losses = 0
    total_stake = 0.0
    total_profit = 0.0
    for block in blocks:
        if not isinstance(block, dict):
            continue
        total_n += int(block.get("n") or 0)
        total_wins += int(block.get("wins") or 0)
        total_losses += int(block.get("losses") or 0)
        total_stake += float(block.get("stake_u") or 0.0)
        total_profit += float(block.get("profit_u") or 0.0)
    return {
        "n": int(total_n),
        "wins": int(total_wins),
        "losses": int(total_losses),
        "stake_u": round(float(total_stake), 4),
        "profit_u": round(float(total_profit), 4),
        "roi": (round(float(total_profit) / float(total_stake), 4) if float(total_stake) > 0.0 else None),
    }


def _merge_settled_results_blocks(blocks: Sequence[Any]) -> Dict[str, Any]:
    by_market: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for block in blocks:
        if not isinstance(block, dict):
            continue
        for market_name, summary in block.items():
            if isinstance(summary, dict):
                by_market[str(market_name)].append(summary)

    out: Dict[str, Any] = {}
    for market_name, summaries in sorted(by_market.items()):
        out[str(market_name)] = _merge_settled_summary_blocks(summaries)
    if "combined" not in out:
        out["combined"] = _blank_settled_summary()
    return out


def _season_betting_month_label(month_key: str) -> str:
    try:
        return datetime.strptime(str(month_key), "%Y-%m").strftime("%b %Y")
    except Exception:
        return str(month_key)


def _median_float(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    size = len(ordered)
    midpoint = size // 2
    if (size % 2) == 1:
        return float(ordered[midpoint])
    return float((ordered[midpoint - 1] + ordered[midpoint]) / 2.0)


def _season_betting_daily_stats(day_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    profits = [float(((row.get("results") or {}).get("combined") or {}).get("profit_u") or 0.0) for row in day_rows]
    cards_with_bets = [row for row in day_rows if int((((row.get("results") or {}).get("combined") or {}).get("n") or 0)) > 0]
    best_day = max(
        day_rows,
        key=lambda row: float((((row.get("results") or {}).get("combined") or {}).get("profit_u") or 0.0)),
        default=None,
    )
    worst_day = min(
        day_rows,
        key=lambda row: float((((row.get("results") or {}).get("combined") or {}).get("profit_u") or 0.0)),
        default=None,
    )
    mean_u = (round(float(sum(profits) / len(profits)), 4) if profits else None)
    median_u = _median_float(profits)
    if profits:
        mean_raw = float(sum(profits) / len(profits))
        variance = sum((float(value) - mean_raw) ** 2 for value in profits) / len(profits)
        std_u = round(float(math.sqrt(variance)), 4)
    else:
        std_u = None
    return {
        "cards": int(len(day_rows)),
        "cards_with_bets": int(len(cards_with_bets)),
        "cards_without_bets": int(len(day_rows) - len(cards_with_bets)),
        "positive_days": int(sum(1 for value in profits if value > 0.0)),
        "negative_days": int(sum(1 for value in profits if value < 0.0)),
        "flat_days": int(sum(1 for value in profits if abs(value) <= 1e-12)),
        "mean_u": mean_u,
        "median_u": (round(float(median_u), 4) if median_u is not None else None),
        "std_u": std_u,
        "best_day": (
            {
                "date": str(best_day.get("date") or ""),
                "profit_u": round(float((((best_day.get("results") or {}).get("combined") or {}).get("profit_u") or 0.0)), 4),
            }
            if isinstance(best_day, dict)
            else {"date": None, "profit_u": None}
        ),
        "worst_day": (
            {
                "date": str(worst_day.get("date") or ""),
                "profit_u": round(float((((worst_day.get("results") or {}).get("combined") or {}).get("profit_u") or 0.0)), 4),
            }
            if isinstance(worst_day, dict)
            else {"date": None, "profit_u": None}
        ),
    }


def _season_betting_aggregate_selected_counts(day_rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    totals: Dict[str, int] = {key: 0 for key in _BETTING_COUNT_KEYS}
    for row in day_rows:
        counts = _betting_selected_counts_with_defaults(row.get("selected_counts") or {})
        for key in totals:
            totals[key] += int(counts.get(key) or 0)
    return totals


def _season_betting_unresolved_count(settled_card: Any) -> int:
    if not isinstance(settled_card, dict):
        return 0
    unresolved_rows = [row for row in (settled_card.get("unresolved_recommendations") or []) if isinstance(row, dict)]
    if unresolved_rows:
        return int(len(unresolved_rows))
    unresolved_n = _safe_int(settled_card.get("unresolved_n"))
    if unresolved_n is not None and int(unresolved_n) > 0:
        return int(unresolved_n)
    selected_counts = _betting_selected_counts_with_defaults(settled_card.get("selected_counts") or {})
    combined = _merge_settled_results_blocks([settled_card.get("results") or {}]).get("combined") or _blank_settled_summary()
    missing = int(selected_counts.get("combined") or 0) - int(combined.get("n") or 0)
    return max(int(missing), 0)


def _normalized_season_betting_summary(
    *,
    date_str: str,
    card_path: Optional[Path],
    source_summary: Optional[Dict[str, Any]],
    selected_counts: Any,
    results: Any,
    settled_card: Any,
) -> Dict[str, Any]:
    summary = dict(source_summary or {})
    merged_results = _merge_settled_results_blocks([results or {}])
    combined = merged_results.get("combined") or _blank_settled_summary()
    summary["date"] = str(summary.get("date") or date_str)
    summary["month"] = str(summary.get("month") or str(date_str)[:7])
    summary["card_path"] = _relative_path_str(card_path) if card_path is not None else summary.get("card_path")
    summary["selected_counts"] = _betting_selected_counts_with_defaults(selected_counts or {})
    summary["results"] = merged_results
    summary["profit_u"] = round(float(combined.get("profit_u") or 0.0), 4)
    summary["roi"] = combined.get("roi")
    summary["settled_n"] = int(combined.get("n") or 0)
    summary["unresolved_n"] = _season_betting_unresolved_count(settled_card)
    return summary


def _season_betting_manifest_day_row_from_payload(day_payload: Dict[str, Any]) -> Dict[str, Any]:
    date_str = str(day_payload.get("date") or "").strip()
    summary = dict(day_payload.get("summary") or {})
    results = _merge_settled_results_blocks([day_payload.get("results") or {}])
    combined = results.get("combined") or _blank_settled_summary()
    selected_counts = _betting_selected_counts_with_defaults(
        day_payload.get("selected_counts") or summary.get("selected_counts") or {}
    )
    row = dict(summary)
    row["date"] = date_str
    row["month"] = str(row.get("month") or date_str[:7])
    row["cap_profile"] = day_payload.get("cap_profile") or row.get("cap_profile")
    row["card_path"] = day_payload.get("card_source") or row.get("card_path")
    row["selected_counts"] = selected_counts
    row["results"] = results
    row["profit_u"] = round(float(combined.get("profit_u") or 0.0), 4)
    row["roi"] = combined.get("roi")
    row["settled_n"] = int(combined.get("n") or 0)
    row["unresolved_n"] = int(summary.get("unresolved_n") or 0)
    row["source_kind"] = day_payload.get("source_kind") or row.get("source_kind")
    return row


def _embedded_settlement_is_usable(summary: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(summary, dict):
        return False
    selected_counts = _betting_selected_counts_with_defaults(summary.get("selected_counts") or {})
    if int(selected_counts.get("combined") or 0) > 0:
        return True
    results = _merge_settled_results_blocks([summary.get("results") or {}])
    combined = results.get("combined") or _blank_settled_summary()
    return int(combined.get("n") or 0) > 0


def _rebuild_season_betting_manifest_payload(
    season: int,
    profile_name: str,
    manifest_path: Path,
    manifest: Dict[str, Any],
    available_profiles: Sequence[str],
) -> Dict[str, Any]:
    days_out: List[Dict[str, Any]] = []
    corrected_days: List[Dict[str, Any]] = []
    manifest_dates: set[str] = set()
    today_str = _today_iso()
    for raw_row in manifest.get("days") or []:
        if not isinstance(raw_row, dict):
            continue
        day_row = dict(raw_row)
        date_str = str(day_row.get("date") or "").strip()
        if not date_str:
            days_out.append(day_row)
            continue
        manifest_dates.add(date_str)
        row_counts = _betting_selected_counts_with_defaults(day_row.get("selected_counts") or {})
        refresh_day = bool(date_str == today_str or int(row_counts.get("combined") or 0) <= 0)
        if not refresh_day:
            day_row["selected_counts"] = row_counts
            day_row["results"] = _merge_settled_results_blocks([day_row.get("results") or {}])
            combined = (day_row.get("results") or {}).get("combined") or _blank_settled_summary()
            day_row["profit_u"] = round(float(combined.get("profit_u") or day_row.get("profit_u") or 0.0), 4)
            day_row["roi"] = combined.get("roi")
            day_row["settled_n"] = int(combined.get("n") or day_row.get("settled_n") or 0)
            day_row["unresolved_n"] = int(day_row.get("unresolved_n") or 0)
            day_row.setdefault("month", date_str[:7])
            corrected_days.append(day_row)
            days_out.append(day_row)
            continue
        day_payload = _season_betting_day_payload(int(season), date_str, profile_name)
        if not day_payload.get("found"):
            day_row["selected_counts"] = _betting_selected_counts_with_defaults(day_row.get("selected_counts") or {})
            day_row["results"] = _merge_settled_results_blocks([day_row.get("results") or {}])
            combined = (day_row.get("results") or {}).get("combined") or {}
            day_row["profit_u"] = round(float(combined.get("profit_u") or day_row.get("profit_u") or 0.0), 4)
            day_row["roi"] = combined.get("roi")
            day_row["settled_n"] = int(combined.get("n") or day_row.get("settled_n") or 0)
            day_row["unresolved_n"] = int(day_row.get("unresolved_n") or 0)
            day_row.setdefault("month", date_str[:7])
            days_out.append(day_row)
            continue

        corrected_results = _merge_settled_results_blocks([day_payload.get("results") or {}])
        corrected_combined = corrected_results.get("combined") or _blank_settled_summary()
        day_row["selected_counts"] = _betting_selected_counts_with_defaults(day_payload.get("selected_counts") or {})
        day_row["results"] = corrected_results
        day_row["profit_u"] = round(float(corrected_combined.get("profit_u") or 0.0), 4)
        day_row["roi"] = corrected_combined.get("roi")
        day_row["settled_n"] = int(corrected_combined.get("n") or 0)
        summary_block = day_payload.get("summary") if isinstance(day_payload.get("summary"), dict) else {}
        day_row["unresolved_n"] = int(summary_block.get("unresolved_n") or day_row.get("unresolved_n") or 0)
        day_row["card_path"] = day_payload.get("card_source") or day_row.get("card_path")
        day_row["source_kind"] = day_payload.get("source_kind") or day_row.get("source_kind")
        day_row.setdefault("month", date_str[:7])
        corrected_days.append(day_row)
        days_out.append(day_row)

    manifest_floor = min(manifest_dates) if manifest_dates else None
    supplemental_dates = [
        d for d in _available_daily_locked_card_dates(int(season))
        if d not in manifest_dates and (not manifest_floor or d >= manifest_floor)
    ]
    for date_str in supplemental_dates:
        day_payload = _season_betting_day_payload(int(season), date_str, profile_name)
        if not day_payload.get("found"):
            continue
        supplemental_day = _season_betting_manifest_day_row_from_payload(day_payload)
        corrected_days.append(supplemental_day)
        days_out.append(supplemental_day)
        manifest_dates.add(date_str)

    if _season_from_date_str(today_str) == int(season) and today_str not in manifest_dates:
        today_payload = _season_betting_day_payload(int(season), today_str, profile_name)
        if today_payload.get("found"):
            supplemental_day = _season_betting_manifest_day_row_from_payload(today_payload)
            corrected_days.append(supplemental_day)
            days_out.append(supplemental_day)

    corrected_days.sort(key=lambda row: str(row.get("date") or ""))
    days_out.sort(key=lambda row: str(row.get("date") or ""))

    corrected_results = _merge_settled_results_blocks([row.get("results") or {} for row in corrected_days])
    summary_selected_counts = _season_betting_aggregate_selected_counts(corrected_days)
    summary_daily = _season_betting_daily_stats(corrected_days)

    month_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for day_row in corrected_days:
        month_buckets[str(day_row.get("month") or "")].append(day_row)

    months_out: List[Dict[str, Any]] = []
    for month_key in sorted(month_buckets):
        month_days = list(month_buckets[month_key])
        month_results = _merge_settled_results_blocks([row.get("results") or {} for row in month_days])
        months_out.append(
            {
                "month": str(month_key),
                "label": _season_betting_month_label(str(month_key)),
                "selected_counts": _season_betting_aggregate_selected_counts(month_days),
                "results": month_results,
                "daily": _season_betting_daily_stats(month_days),
            }
        )

    payload = dict(manifest)
    meta = dict(payload.get("meta") or {})
    meta["available_reports"] = max(int(meta.get("available_reports") or 0), len(days_out))
    meta["processed_reports"] = max(int(meta.get("processed_reports") or 0), len(days_out))
    sources = dict(meta.get("sources") or {})
    sources["manifest"] = _relative_path_str(manifest_path)
    meta["sources"] = sources
    payload["meta"] = meta
    payload["summary"] = {
        **dict(payload.get("summary") or {}),
        "cards": int(len(corrected_days)),
        "cards_processed": int(len(corrected_days)),
        "selected_counts": summary_selected_counts,
        "settled_recommendations": int((corrected_results.get("combined") or {}).get("n") or 0),
        "unresolved_recommendations": int(sum(int(row.get("unresolved_n") or 0) for row in corrected_days)),
        "results": corrected_results,
        "daily": summary_daily,
        "combined": corrected_results.get("combined") or _blank_settled_summary(),
        "market_results": {
            key: value
            for key, value in corrected_results.items()
            if key not in {"combined", "hitter_props"}
        },
    }
    payload["months"] = months_out
    payload["days"] = days_out
    payload["profile"] = profile_name
    payload["available_profiles"] = list(available_profiles)
    payload["source_kind"] = "season_manifest_rebuilt"
    payload["found"] = True
    return payload


def _season_betting_manifest_needs_refresh(season: int, manifest: Dict[str, Any]) -> bool:
    if not isinstance(manifest, dict):
        return False

    manifest_dates: List[str] = []
    for raw_row in manifest.get("days") or []:
        if not isinstance(raw_row, dict):
            continue
        date_str = str(raw_row.get("date") or "").strip()
        if date_str:
            manifest_dates.append(date_str)
        selected_counts = _betting_selected_counts_with_defaults(raw_row.get("selected_counts") or {})
        if int(selected_counts.get("combined") or 0) <= 0:
            return True

    available_daily_dates = _available_daily_locked_card_dates(int(season))
    if not available_daily_dates:
        return False

    latest_manifest_date = max(manifest_dates) if manifest_dates else ""
    latest_daily_date = str(available_daily_dates[-1] or "")
    if not latest_manifest_date:
        return True
    return latest_daily_date > latest_manifest_date


def _season_betting_manifest_response_payload(
    season: int,
    profile_name: str,
    manifest_path: Path,
    manifest: Dict[str, Any],
    available_profiles: Sequence[str],
) -> Dict[str, Any]:
    if _season_betting_manifest_needs_refresh(int(season), manifest):
        return _rebuild_season_betting_manifest_payload(
            int(season),
            profile_name,
            manifest_path,
            manifest,
            available_profiles,
        )
    return _season_betting_manifest_static_payload(
        int(season),
        profile_name,
        manifest_path,
        manifest,
        available_profiles,
    )


def _official_betting_card_manifest_response_payload(
    season: int,
    profile_name: str,
    manifest_path: Path,
    manifest: Dict[str, Any],
    available_profiles: Sequence[str],
) -> Dict[str, Any]:
    if _season_betting_manifest_needs_refresh(int(season), manifest):
        return _official_betting_card_manifest_payload(
            int(season),
            profile_name,
            manifest_path,
            manifest,
            available_profiles,
        )
    return _official_betting_card_manifest_static_payload(
        int(season),
        profile_name,
        manifest_path,
        manifest,
        available_profiles,
    )


def _season_betting_manifest_static_payload(
    season: int,
    profile_name: str,
    manifest_path: Path,
    manifest: Dict[str, Any],
    available_profiles: Sequence[str],
) -> Dict[str, Any]:
    # Season betting-card manifests are already published as frontend-ready JSON.
    # Keep this route on the artifact fast path instead of rehydrating season-day
    # state from daily files on every request.
    payload = dict(manifest)
    meta = dict(payload.get("meta") or {})
    sources = dict(meta.get("sources") or {})
    sources["manifest"] = _relative_path_str(manifest_path)
    meta["sources"] = sources
    payload["meta"] = meta
    payload["profile"] = profile_name
    payload["available_profiles"] = list(available_profiles)
    payload["source_kind"] = "season_manifest_static"
    payload["found"] = True
    return payload


def _official_betting_card_manifest_static_payload(
    season: int,
    profile_name: str,
    manifest_path: Path,
    manifest: Dict[str, Any],
    available_profiles: Sequence[str],
) -> Dict[str, Any]:
    payload = _season_betting_manifest_static_payload(
        int(season),
        profile_name,
        manifest_path,
        manifest,
        available_profiles,
    )
    active_days = _official_betting_card_active_days(payload.get("days") or [])
    active_results = _merge_settled_results_blocks([row.get("results") or {} for row in active_days])
    active_combined = active_results.get("combined") or _blank_settled_summary()

    summary = dict(payload.get("summary") or {})
    summary["cards"] = int(len(active_days))
    summary["cards_processed"] = int(len(active_days))
    summary["selected_counts"] = _season_betting_aggregate_selected_counts(active_days)
    summary["settled_recommendations"] = int(active_combined.get("n") or 0)
    summary["unresolved_recommendations"] = int(sum(int(row.get("unresolved_n") or 0) for row in active_days))
    summary["results"] = active_results
    summary["daily"] = _season_betting_daily_stats(active_days)
    summary["combined"] = active_combined
    summary["market_results"] = {
        key: value
        for key, value in active_results.items()
        if key not in {"combined", "hitter_props"}
    }

    payload["summary"] = summary
    payload["days"] = active_days
    payload["months"] = _official_betting_card_month_rows(active_days)
    payload["view"] = "official_betting_card"
    return payload


def _official_betting_card_active_days(day_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw_row in day_rows:
        if not isinstance(raw_row, dict):
            continue
        row = dict(raw_row)
        selected_counts = _betting_selected_counts_with_defaults(row.get("selected_counts") or {})
        if int(selected_counts.get("combined") or 0) <= 0:
            continue
        results = _merge_settled_results_blocks([row.get("results") or {}])
        combined = results.get("combined") or _blank_settled_summary()
        row["month"] = str(row.get("month") or str(row.get("date") or "")[:7])
        row["selected_counts"] = selected_counts
        row["results"] = results
        row["profit_u"] = round(float(combined.get("profit_u") or row.get("profit_u") or 0.0), 4)
        row["roi"] = combined.get("roi")
        row["settled_n"] = int(combined.get("n") or row.get("settled_n") or 0)
        row["unresolved_n"] = int(row.get("unresolved_n") or 0)
        out.append(row)
    out.sort(key=lambda row: str(row.get("date") or ""))
    return out


def _official_betting_card_month_rows(day_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    month_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in day_rows:
        month_buckets[str(row.get("month") or "")].append(dict(row))

    months_out: List[Dict[str, Any]] = []
    for month_key in sorted(month_buckets):
        month_days = list(month_buckets[month_key])
        month_results = _merge_settled_results_blocks([row.get("results") or {} for row in month_days])
        months_out.append(
            {
                "month": str(month_key),
                "label": _season_betting_month_label(str(month_key)),
                "days": int(len(month_days)),
                "selected_counts": _season_betting_aggregate_selected_counts(month_days),
                "results": month_results,
                "daily": _season_betting_daily_stats(month_days),
            }
        )
    return months_out


def _official_betting_card_manifest_payload(
    season: int,
    profile_name: str,
    manifest_path: Path,
    manifest: Dict[str, Any],
    available_profiles: Sequence[str],
) -> Dict[str, Any]:
    payload = _rebuild_season_betting_manifest_payload(
        int(season),
        profile_name,
        manifest_path,
        manifest,
        available_profiles,
    )
    active_days = _official_betting_card_active_days(payload.get("days") or [])
    active_results = _merge_settled_results_blocks([row.get("results") or {} for row in active_days])
    active_combined = active_results.get("combined") or _blank_settled_summary()

    summary = dict(payload.get("summary") or {})
    summary["cards"] = int(len(active_days))
    summary["cards_processed"] = int(len(active_days))
    summary["selected_counts"] = _season_betting_aggregate_selected_counts(active_days)
    summary["settled_recommendations"] = int(active_combined.get("n") or 0)
    summary["unresolved_recommendations"] = int(sum(int(row.get("unresolved_n") or 0) for row in active_days))
    summary["results"] = active_results
    summary["daily"] = _season_betting_daily_stats(active_days)
    summary["combined"] = active_combined
    summary["market_results"] = {
        key: value
        for key, value in active_results.items()
        if key not in {"combined", "hitter_props"}
    }

    payload["summary"] = summary
    payload["days"] = active_days
    payload["months"] = _official_betting_card_month_rows(active_days)
    payload["view"] = "official_betting_card"
    payload["found"] = True
    return payload


def _official_betting_card_games_payload(date_str: str, betting_games: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _lead_betting_row(bucket: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(bucket, dict):
            return None
        markets = bucket.get("markets") if isinstance(bucket.get("markets"), dict) else {}
        for key in ("totals", "ml"):
            item = markets.get(key)
            if isinstance(item, dict):
                return item
        for key in ("pitcherProps", "hitterProps", "extraPitcherProps", "extraHitterProps"):
            rows = markets.get(key) or []
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        return row
        return None

    def _historical_schedule_context() -> Dict[int, Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}
        for raw_game_pk, raw_game_betting in (betting_games or {}).items():
            game_pk = _safe_int(raw_game_pk)
            if not game_pk or int(game_pk) <= 0 or not isinstance(raw_game_betting, dict):
                continue
            lead_row = _lead_betting_row(raw_game_betting)
            if not isinstance(lead_row, dict):
                continue
            game_date = str(lead_row.get("commence_time") or lead_row.get("game_date") or "")
            away_name = _first_text(lead_row.get("away"), lead_row.get("away_abbr"), "Away")
            home_name = _first_text(lead_row.get("home"), lead_row.get("home_abbr"), "Home")
            away_abbr = _first_text(lead_row.get("away_abbr"), away_name, "Away")
            home_abbr = _first_text(lead_row.get("home_abbr"), home_name, "Home")
            out[int(game_pk)] = {
                "gameDate": game_date,
                "startTime": _format_start_time_local(game_date),
                "officialDate": str(lead_row.get("date") or date_str),
                "status": {
                    "abstract": "Final",
                    "detailed": "Final",
                },
                "away": {"id": None, "abbr": away_abbr, "name": away_name},
                "home": {"id": None, "abbr": home_abbr, "name": home_name},
                "score": {},
                "probable": {
                    "away": _normalized_probable_entry(None),
                    "home": _normalized_probable_entry(None),
                },
            }
        return out

    schedule_by_game_pk = (
        _historical_schedule_context()
        if _is_historical_date(str(date_str))
        else _schedule_context_by_game_pk(str(date_str))
    )
    games_out: List[Dict[str, Any]] = []
    live_matchup_candidates: List[Tuple[int, str, Dict[str, Any]]] = []

    for raw_game_pk, raw_game_betting in (betting_games or {}).items():
        game_pk = _safe_int(raw_game_pk)
        if not game_pk or int(game_pk) <= 0 or not isinstance(raw_game_betting, dict):
            continue
        counts = raw_game_betting.get("counts") if isinstance(raw_game_betting.get("counts"), dict) else {}
        flags = raw_game_betting.get("flags") if isinstance(raw_game_betting.get("flags"), dict) else {}
        if int(counts.get("official") or 0) <= 0 and not bool(flags.get("hasOfficialRecommendations")):
            continue

        schedule_row = dict(schedule_by_game_pk.get(int(game_pk)) or {})
        probable = schedule_row.get("probable") if isinstance(schedule_row.get("probable"), dict) else {}
        status = schedule_row.get("status") if isinstance(schedule_row.get("status"), dict) else {}
        game_date = str(schedule_row.get("gameDate") or "")
        away = dict(schedule_row.get("away") or {"id": None, "abbr": "Away", "name": "Away"})
        home = dict(schedule_row.get("home") or {"id": None, "abbr": "Home", "name": "Home"})
        official_date = str(schedule_row.get("officialDate") or date_str)
        game_row = {
            "game_pk": int(game_pk),
            "game_date": game_date,
            "start_time": schedule_row.get("startTime") or _format_start_time_local(game_date),
            "official_date": official_date,
            "status": {
                "abstract": str(status.get("abstract") or ""),
                "detailed": str(status.get("detailed") or ""),
            },
            "away": away,
            "home": home,
            "starter_names": {
                "away": _first_text(((probable.get("away") or {}).get("fullName"))),
                "home": _first_text(((probable.get("home") or {}).get("fullName"))),
            },
            "matchup": _official_betting_game_matchup_payload(
                int(game_pk),
                official_date,
                status,
                score=schedule_row.get("score") if isinstance(schedule_row.get("score"), dict) else None,
                fetch_feed=bool(_status_is_live(status)),
            ),
            "betting": dict(raw_game_betting),
        }
        games_out.append(game_row)
        if bool(_status_is_live(status)):
            live_matchup_candidates.append((int(game_pk), official_date, status))

    if live_matchup_candidates:
        row_by_game_pk = {
            int(row.get("game_pk") or 0): row
            for row in games_out
            if isinstance(row, dict) and int(row.get("game_pk") or 0) > 0
        }
        max_workers = max(1, min(8, len(live_matchup_candidates)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_official_betting_game_matchup_payload, game_pk, official_date, status, None, True): game_pk
                for game_pk, official_date, status in live_matchup_candidates
            }
            for future in as_completed(futures):
                game_pk = int(futures[future])
                try:
                    matchup = future.result()
                except Exception:
                    matchup = None
                if isinstance(matchup, dict) and game_pk in row_by_game_pk:
                    row_by_game_pk[game_pk]["matchup"] = matchup

    games_out.sort(key=lambda row: (str(row.get("game_date") or ""), int(row.get("game_pk") or 0)))
    return games_out


def _official_betting_card_day_payload(season: int, date_str: str, requested_profile: str) -> Dict[str, Any]:
    betting_payload = _season_betting_day_payload(int(season), str(date_str), requested_profile)
    payload: Dict[str, Any] = {
        "season": int(season),
        "date": str(date_str),
        "profile": betting_payload.get("profile"),
        "available_profiles": betting_payload.get("available_profiles") or [],
        "found": False,
        "games": [],
    }
    if not betting_payload.get("found"):
        payload.update(
            {
                "error": betting_payload.get("error"),
                "detail": betting_payload.get("detail"),
                "manifest_source": betting_payload.get("manifest_source"),
            }
        )
        return payload

    selected_counts = _betting_selected_counts_with_defaults(betting_payload.get("selected_counts") or {})
    if int(selected_counts.get("combined") or 0) <= 0:
        payload["error"] = "official_betting_card_day_missing"
        return payload

    daily_artifacts = _load_cards_artifacts(str(date_str))
    has_cards = bool(daily_artifacts.get("locked_policy") or daily_artifacts.get("game_summary"))
    betting_games = betting_payload.get("games") if isinstance(betting_payload.get("games"), dict) else {}
    payload.update(
        {
            "found": True,
            "manifest_source": betting_payload.get("manifest_source"),
            "card_source": betting_payload.get("card_source"),
            "source_kind": betting_payload.get("source_kind"),
            "summary": dict(betting_payload.get("summary") or {}),
            "cap_profile": betting_payload.get("cap_profile"),
            "selected_counts": selected_counts,
            "results": _merge_settled_results_blocks([betting_payload.get("results") or {}]),
            "cards_available": bool(has_cards),
            "cards_url": (f"/?date={date_str}" if has_cards else None),
            "games": _official_betting_card_games_payload(str(date_str), betting_games),
        }
    )
    return payload


def write_current_day_season_frontend_artifacts(
    season: int,
    date_str: str,
    *,
    betting_profile: str = "retuned",
    out_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    target_dir = out_dir.resolve() if isinstance(out_dir, Path) else daily_season_frontend_dir()
    profile_slug = _season_frontend_profile_slug(betting_profile)

    manifest_payload = build_current_day_season_manifest_artifact(int(season), str(date_str))
    manifest_path = target_dir / daily_season_manifest_artifact_path(int(season), str(date_str)).name
    _write_json_file(manifest_path, manifest_payload)

    season_day_payload = build_current_day_season_day_artifact(int(season), str(date_str), profile_slug)
    season_day_path = target_dir / daily_season_day_artifact_path(int(season), str(date_str), profile=profile_slug).name
    _write_json_file(season_day_path, season_day_payload)

    betting_day_payload = _season_betting_day_payload(int(season), str(date_str), profile_slug)
    if betting_day_payload.get("found"):
        card_path = _path_from_maybe_relative(betting_day_payload.get("card_source"))
        betting_day_payload = dict(betting_day_payload)
        betting_day_payload["card"] = _load_json_file(card_path)
        betting_day_payload["artifactDate"] = str(date_str)
    betting_day_path = target_dir / daily_season_betting_day_artifact_path(int(season), str(date_str), profile=profile_slug).name
    _write_json_file(betting_day_path, betting_day_payload)

    official_betting_payload = _official_betting_card_day_payload(int(season), str(date_str), profile_slug)
    if official_betting_payload.get("found"):
        official_betting_payload = dict(official_betting_payload)
        official_betting_payload["artifactDate"] = str(date_str)
    official_betting_path = target_dir / daily_official_betting_card_day_artifact_path(int(season), str(date_str), profile=profile_slug).name
    _write_json_file(official_betting_path, official_betting_payload)

    return {
        "season": int(season),
        "date": str(date_str),
        "profile": profile_slug,
        "dir": target_dir,
        "artifacts": {
            "season_manifest": {
                "path": manifest_path,
                "found": bool(manifest_payload.get("found")),
                "error": manifest_payload.get("error"),
            },
            "season_day": {
                "path": season_day_path,
                "found": bool(season_day_payload.get("found")),
                "error": season_day_payload.get("error"),
            },
            "season_betting_day": {
                "path": betting_day_path,
                "found": bool(betting_day_payload.get("found")),
                "error": betting_day_payload.get("error"),
            },
            "season_official_betting_day": {
                "path": official_betting_path,
                "found": bool(official_betting_payload.get("found")),
                "error": official_betting_payload.get("error"),
            },
        },
    }


def _settlement_player_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _settlement_line_key(value: Any) -> Optional[float]:
    line = _safe_float(value)
    if line is None:
        return None
    return round(float(line), 4)


def _settlement_market_key(item: Dict[str, Any]) -> str:
    market = str(item.get("market") or "").strip().lower()
    prop = str(item.get("prop") or "").strip().lower()
    if market == "hitter_props" and prop in _SETTLED_HITTER_MARKETS:
        return prop
    return market


def _settlement_lookup_key(item: Dict[str, Any]) -> Tuple[Optional[int], str, str, Optional[float], str]:
    market = _settlement_market_key(item)
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


def _pending_settlement_from_card(
    card_path: Optional[Path],
    card_obj: Dict[str, Any],
    *,
    reason: str,
) -> Dict[str, Any]:
    date_str = str(card_obj.get("date") or "").strip()
    selected_counts = _betting_selected_counts_with_defaults(card_obj.get("selected_counts") or {})
    playable_selected_counts = _betting_selected_counts_with_defaults(card_obj.get("playable_selected_counts") or {})
    all_selected_counts = _betting_selected_counts_with_defaults(card_obj.get("all_selected_counts") or {})
    inferred_selected_counts = _infer_betting_selected_counts_from_card(card_obj, "recommendations")
    inferred_playable_counts = _infer_betting_selected_counts_from_card(card_obj, "other_playable_candidates")
    if int(selected_counts.get("combined") or 0) <= 0:
        selected_counts = inferred_selected_counts
    if int(playable_selected_counts.get("combined") or 0) <= 0:
        playable_selected_counts = inferred_playable_counts
    if int(all_selected_counts.get("combined") or 0) <= 0:
        all_selected_counts = _betting_selected_counts_with_defaults(
            {
                key: int(selected_counts.get(key) or 0) + int(playable_selected_counts.get(key) or 0)
                for key in _BETTING_COUNT_KEYS
            }
        )

    unresolved_rows: List[Dict[str, Any]] = []
    playable_unresolved_rows: List[Dict[str, Any]] = []
    markets = (card_obj.get("markets") or {}) if isinstance(card_obj, dict) else {}
    for market_name, market_info in markets.items():
        if not isinstance(market_info, dict):
            continue
        for reco_key, tier_name, target_rows in (
            ("recommendations", "official", unresolved_rows),
            ("other_playable_candidates", "candidate", playable_unresolved_rows),
        ):
            recs = market_info.get(reco_key) or []
            if not isinstance(recs, list):
                continue
            for rec in recs:
                if not isinstance(rec, dict):
                    continue
                player_label = rec.get("player_name") or rec.get("pitcher_name") or None
                target_rows.append(
                    {
                        "path": str(card_path) if card_path else None,
                        "date": date_str,
                        "game_pk": _safe_int(rec.get("game_pk")) or 0,
                        "market": str(rec.get("market") or market_name),
                        "player_name": player_label,
                        "selection": rec.get("selection"),
                        "market_line": rec.get("market_line"),
                        "reason": str(reason),
                        "recommendation_tier": tier_name,
                    }
                )

    all_unresolved_rows = list(unresolved_rows) + list(playable_unresolved_rows)
    return {
        "path": str(card_path) if card_path else None,
        "date": date_str,
        "cap_profile": card_obj.get("cap_profile"),
        "selected_counts": selected_counts,
        "playable_selected_counts": playable_selected_counts,
        "all_selected_counts": all_selected_counts,
        "results": {"combined": _blank_settled_summary()},
        "playable_results": {"combined": _blank_settled_summary()},
        "all_results": {"combined": _blank_settled_summary()},
        "settled_n": 0,
        "playable_settled_n": 0,
        "all_settled_n": 0,
        "unresolved_n": int(len(unresolved_rows)),
        "playable_unresolved_n": int(len(playable_unresolved_rows)),
        "all_unresolved_n": int(len(all_unresolved_rows)),
        "unresolved_recommendations": unresolved_rows,
        "playable_unresolved_recommendations": playable_unresolved_rows,
        "all_unresolved_recommendations": all_unresolved_rows,
        "_settled_rows": [],
        "_playable_settled_rows": [],
        "_all_settled_rows": [],
    }


def _fallback_recommendation_from_settled_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    game_pk = _safe_int(row.get("game_pk"))
    if not game_pk or int(game_pk) <= 0:
        return None
    raw_market = str(row.get("market") or "").strip().lower()
    if not raw_market:
        return None
    market = raw_market
    prop = str(row.get("prop") or "").strip().lower()
    if raw_market.startswith("hitter_") and raw_market != "hitter_props":
        market = "hitter_props"
        prop = raw_market
    item: Dict[str, Any] = dict(row)
    item.update(
        {
            "game_pk": int(game_pk),
            "market": market,
            "prop": prop,
            "player_name": row.get("player_name"),
            "pitcher_name": row.get("pitcher_name") or row.get("player_name"),
            "selection": row.get("selection"),
            "market_line": row.get("market_line"),
            "odds": row.get("odds"),
            "recommendation_tier": row.get("recommendation_tier") or "official",
        }
    )
    if any(key in row for key in ("result", "profit_u", "actual", "stake_u")):
        item["settlement"] = {
            "result": row.get("result"),
            "profit_u": row.get("profit_u"),
            "actual": row.get("actual"),
            "stake_u": row.get("stake_u"),
        }
    return item


def _fallback_recommendations_by_game_from_settled_card(settled_card: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
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
    rows: List[Dict[str, Any]] = []
    for key in (
        "_settled_rows",
        "_playable_settled_rows",
        "unresolved_recommendations",
        "playable_unresolved_recommendations",
    ):
        rows.extend([row for row in (settled_card.get(key) or []) if isinstance(row, dict)])

    for row in rows:
        item = _fallback_recommendation_from_settled_row(row)
        if not isinstance(item, dict):
            continue
        game_pk = int(item.get("game_pk") or 0)
        if game_pk <= 0:
            continue
        bucket = grouped[game_pk]
        market = str(item.get("market") or "").strip().lower()
        tier = str(item.get("recommendation_tier") or "official").strip().lower()
        if market == "totals":
            bucket["totals"] = item
        elif market == "ml":
            bucket["ml"] = item
        elif market == "pitcher_props":
            if tier == "candidate":
                bucket["extra_pitcher_props"].append(item)
            else:
                bucket["pitcher_props"].append(item)
        else:
            if tier == "candidate":
                bucket["extra_hitter_props"].append(item)
            else:
                bucket["hitter_props"].append(item)

    for bucket in grouped.values():
        bucket["pitcher_props"].sort(key=lambda reco: (str(reco.get("pitcher_name") or ""), _safe_float(reco.get("market_line")) or 0.0))
        bucket["hitter_props"].sort(key=lambda reco: (str(reco.get("player_name") or ""), _safe_float(reco.get("market_line")) or 0.0))
        bucket["extra_pitcher_props"].sort(key=lambda reco: (str(reco.get("pitcher_name") or ""), _safe_float(reco.get("market_line")) or 0.0))
        bucket["extra_hitter_props"].sort(key=lambda reco: (str(reco.get("player_name") or ""), _safe_float(reco.get("market_line")) or 0.0))
    return dict(grouped)


def _season_betting_games_payload(card_obj: Dict[str, Any], settled_card: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    recos_by_game = _recommendations_by_game(card_obj)
    if not recos_by_game:
        recos_by_game = _fallback_recommendations_by_game_from_settled_card(settled_card)
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


def _payload_has_row_settlement(games: Any) -> bool:
    if not isinstance(games, dict):
        return False

    def _has_settlement(item: Any) -> bool:
        return isinstance(item, dict) and isinstance(item.get("settlement"), dict)

    for game in games.values():
        if not isinstance(game, dict):
            continue
        markets = game.get("markets") or {}
        if not isinstance(markets, dict):
            continue
        if _has_settlement(markets.get("totals")) or _has_settlement(markets.get("ml")):
            return True
        for key in ("pitcherProps", "hitterProps", "extraPitcherProps", "extraHitterProps"):
            rows = markets.get(key) or []
            if isinstance(rows, list) and any(_has_settlement(row) for row in rows):
                return True
    return False


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
    canonical_settlement_path = daily_artifacts.get("settlement_path")
    canonical_settlement = daily_artifacts.get("settlement")
    embedded_settlement_summary = daily_artifacts.get("embedded_settlement_summary")
    historical_date = _is_historical_date(str(date_str))

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
        effective_card_path = card_path
        effective_card_obj = card_obj
        effective_source_kind = str(source_kind)
        settled_card: Optional[Dict[str, Any]] = None

        def _prefer_more_complete_settlement(
            current_settlement: Optional[Dict[str, Any]],
            candidate_settlement: Optional[Dict[str, Any]],
        ) -> Optional[Dict[str, Any]]:
            if not isinstance(candidate_settlement, dict):
                return current_settlement
            if not isinstance(current_settlement, dict):
                return candidate_settlement
            current_unresolved = _season_betting_unresolved_count(current_settlement)
            candidate_unresolved = _season_betting_unresolved_count(candidate_settlement)
            current_all_settled_n = int(current_settlement.get("all_settled_n") or 0)
            candidate_all_settled_n = int(candidate_settlement.get("all_settled_n") or 0)
            if (
                candidate_unresolved < current_unresolved
                or (
                    candidate_unresolved == current_unresolved
                    and candidate_all_settled_n > current_all_settled_n
                )
            ):
                return candidate_settlement
            return current_settlement

        if (
            canonical_settlement_path
            and canonical_card_path
            and _same_daily_card_path(card_path, canonical_card_path)
            and canonical_settlement_path.exists()
            and isinstance(canonical_settlement, dict)
        ):
            settled_card = canonical_settlement

        embedded_settlement: Optional[Dict[str, Any]] = None
        if _embedded_settlement_is_usable(embedded_settlement_summary):
            embedded_card_path = _path_from_maybe_relative(embedded_settlement_summary.get("card_path"))
            if not embedded_card_path or _same_daily_card_path(embedded_card_path, card_path):
                embedded_settlement = _synthetic_settlement_from_summary(embedded_settlement_summary)

        if not isinstance(settled_card, dict) and isinstance(embedded_settlement, dict):
            settled_card = embedded_settlement

        if not isinstance(settled_card, dict):
            if historical_date:
                settled_card = None
            else:
                settled_card = _pending_settlement_from_card(
                    effective_card_path,
                    effective_card_obj,
                    reason="game not final",
                )

        if not isinstance(settled_card, dict):
            if manifest_source is not None:
                payload["manifest_source"] = _relative_path_str(manifest_source)
            payload["card_source"] = _relative_path_str(card_path)
            if isinstance(summary, dict):
                payload["summary"] = summary
            payload["error"] = "season_betting_day_settlement_missing"
            return payload

        settled_results_preview = _merge_settled_results_blocks([settled_card.get("results") or {}])
        settled_combined_preview = settled_results_preview.get("combined") or _blank_settled_summary()
        if (
            isinstance(embedded_settlement, dict)
            and int(settled_combined_preview.get("n") or 0) <= 0
            and _season_betting_unresolved_count(settled_card) > 0
        ):
            embedded_results_preview = _merge_settled_results_blocks([embedded_settlement.get("results") or {}])
            embedded_combined_preview = embedded_results_preview.get("combined") or _blank_settled_summary()
            if int(embedded_combined_preview.get("n") or 0) > 0:
                settled_card = embedded_settlement

        settled_counts = _betting_selected_counts_with_defaults(settled_card.get("selected_counts") or {})
        settled_preview = _merge_settled_results_blocks([settled_card.get("results") or {}])
        settled_combined_preview = settled_preview.get("combined") or _blank_settled_summary()

        if canonical_card_path and not _same_daily_card_path(canonical_card_path, card_path):
            if (
                canonical_settlement_path
                and canonical_settlement_path.exists()
                and isinstance(canonical_settlement, dict)
            ):
                canonical_settled = canonical_settlement
            else:
                canonical_settled = None
                if not historical_date and isinstance(canonical_card_obj, dict):
                    canonical_settled = _pending_settlement_from_card(
                        canonical_card_path,
                        canonical_card_obj,
                        reason="game not final",
                    )
            if isinstance(canonical_settled, dict):
                canonical_counts_preview = _betting_selected_counts_with_defaults(canonical_settled.get("selected_counts") or {})
            canonical_counts = _betting_selected_counts_with_defaults(
                ((canonical_settled or {}).get("selected_counts") if isinstance(canonical_settled, dict) else None) or {}
            )
            canonical_should_override = int(canonical_counts.get("combined") or 0) > int(settled_counts.get("combined") or 0)
            if not canonical_should_override and isinstance(canonical_settled, dict):
                canonical_unresolved = _season_betting_unresolved_count(canonical_settled)
                settled_unresolved = _season_betting_unresolved_count(settled_card)
                canonical_all_settled_n = int(canonical_settled.get("all_settled_n") or 0)
                settled_all_settled_n = int(settled_card.get("all_settled_n") or 0)
                canonical_should_override = (
                    canonical_unresolved < settled_unresolved
                    or (
                        canonical_unresolved == settled_unresolved
                        and canonical_all_settled_n > settled_all_settled_n
                    )
                )
            if canonical_should_override:
                settled_card = _prefer_more_complete_settlement(settled_card, canonical_settled)
                settled_counts = canonical_counts
                effective_card_path = canonical_card_path
                effective_source_kind = "canonical_daily_override"
                if isinstance(canonical_card_obj, dict):
                    effective_card_obj = canonical_card_obj

        summary_counts = _betting_selected_counts_with_defaults(
            (summary.get("selected_counts") if isinstance(summary, dict) else None) or {}
        )
        summary_card_path = _path_from_maybe_relative(summary.get("card_path")) if isinstance(summary, dict) else None
        if (
            int(settled_counts.get("combined") or 0) <= 0
            and int(summary_counts.get("combined") or 0) > 0
            and (
                summary_card_path is None
                or _same_daily_card_path(summary_card_path, effective_card_path)
            )
        ):
            selected_counts = summary_counts
        else:
            selected_counts = settled_counts

        official_rows = list(settled_card.get("_settled_rows") or [])
        playable_rows = list(settled_card.get("_playable_settled_rows") or [])
        all_rows = list(settled_card.get("_all_settled_rows") or [])
        official_results = (
            _settled_results_from_rows(official_rows)
            if official_rows
            else dict(settled_card.get("results") or {}) if isinstance(settled_card.get("results"), dict) else {}
        )
        playable_results = (
            _settled_results_from_rows(playable_rows)
            if playable_rows
            else dict(settled_card.get("playable_results") or {})
            if isinstance(settled_card.get("playable_results"), dict)
            else {}
        )
        all_results = (
            _settled_results_from_rows(all_rows)
            if all_rows
            else dict(settled_card.get("all_results") or {}) if isinstance(settled_card.get("all_results"), dict) else {}
        )
        normalized_summary = _normalized_season_betting_summary(
            date_str=str(date_str),
            card_path=effective_card_path,
            source_summary=summary if isinstance(summary, dict) else None,
            selected_counts=selected_counts,
            results=official_results,
            settled_card=settled_card,
        )

        payload.update(
            {
                "found": True,
                "source_kind": effective_source_kind,
                "manifest_source": _relative_path_str(manifest_source) if manifest_source is not None else None,
                "card_source": _relative_path_str(effective_card_path),
                "summary": normalized_summary,
                "cap_profile": effective_card_obj.get("cap_profile"),
                "selected_counts": selected_counts,
                "playable_selected_counts": _betting_selected_counts_with_defaults(
                    settled_card.get("playable_selected_counts") or {}
                ),
                "all_selected_counts": _betting_selected_counts_with_defaults(
                    settled_card.get("all_selected_counts") or {}
                ),
                "results": official_results,
                "playable_results": playable_results,
                "all_results": all_results,
                "games": _season_betting_games_payload(effective_card_obj, settled_card),
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

    static_payload_path = _resolve_season_betting_day_payload_path(manifest, str(date_str))
    static_payload = _load_json_file(static_payload_path)
    if isinstance(static_payload, dict) and static_payload.get("found"):
        static_card_path = _path_from_maybe_relative(static_payload.get("card_source"))
        static_card_obj = _load_json_file(static_card_path)
        static_summary = static_payload.get("summary") if isinstance(static_payload.get("summary"), dict) else None
        static_source_kind = str(static_payload.get("source_kind") or "season_manifest_static")
        if historical_date and not _payload_has_row_settlement(static_payload.get("games")):
            if static_card_path and isinstance(static_card_obj, dict):
                return _finalize_from_card(
                    card_path=static_card_path,
                    card_obj=static_card_obj,
                    summary=static_summary,
                    manifest_source=manifest_path,
                    source_kind=f"{static_source_kind}_rebuilt",
                )
        payload.update(dict(static_payload))
        payload["season"] = int(season)
        payload["date"] = str(date_str)
        payload["profile"] = profile_name
        payload["available_profiles"] = available_profiles
        payload["manifest_source"] = _relative_path_str(manifest_path)
        payload["payload_source"] = _relative_path_str(static_payload_path)
        if not payload.get("card_source"):
            payload["card_source"] = _relative_path_str(_resolve_season_betting_day_card_path(manifest, str(date_str)))
        payload["source_kind"] = str(payload.get("source_kind") or "season_manifest_static")
        payload["found"] = True
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
    schedule_by_game_pk = _schedule_context_by_game_pk(date_str)
    report_by_game_pk = _season_report_outputs_by_game(day_report)

    games_out: List[Dict[str, Any]] = []
    seen_game_pks: set[int] = set()
    for raw_game in day_report.get("games") or []:
        if not isinstance(raw_game, dict):
            continue
        game_pk = _safe_int(raw_game.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            seen_game_pks.add(int(game_pk))
        game_betting = None
        if betting_payload.get("found"):
            game_betting = dict(betting_games.get(int(game_pk or 0)) or _empty_game_betting())
        schedule_row = dict(schedule_by_game_pk.get(int(game_pk or 0)) or {})
        schedule_status = schedule_row.get("status") if isinstance(schedule_row.get("status"), dict) else {}
        report_row = dict(report_by_game_pk.get(int(game_pk or 0)) or {})
        game_date = (
            schedule_row.get("gameDate")
            or report_row.get("game_date")
            or raw_game.get("game_date")
            or raw_game.get("commence_time")
            or ""
        )
        start_time = schedule_row.get("startTime") or _format_start_time_local(str(game_date or ""))
        games_out.append(
            {
                "game_pk": game_pk,
                "game_date": game_date,
                "start_time": start_time,
                "official_date": schedule_row.get("officialDate") or date_str,
                "status": {
                    "abstract": str(schedule_status.get("abstract") or report_row.get("status_abstract") or ""),
                    "detailed": str(schedule_status.get("detailed") or report_row.get("status_detailed") or ""),
                },
                "away": raw_game.get("away") or {},
                "home": raw_game.get("home") or {},
                "starter_names": raw_game.get("starter_names") or {},
                "segments": raw_game.get("segments") or {},
                "pitcher_props": raw_game.get("pitcher_props") or {},
                "betting": game_betting,
            }
        )

    if betting_payload.get("found"):
        for raw_game_pk, raw_game_betting in betting_games.items():
            game_pk = _safe_int(raw_game_pk)
            if not game_pk or int(game_pk) <= 0 or int(game_pk) in seen_game_pks:
                continue
            schedule_row = dict(schedule_by_game_pk.get(int(game_pk)) or {})
            schedule_status = schedule_row.get("status") if isinstance(schedule_row.get("status"), dict) else {}
            probable = schedule_row.get("probable") if isinstance(schedule_row.get("probable"), dict) else {}
            games_out.append(
                {
                    "game_pk": int(game_pk),
                    "game_date": schedule_row.get("gameDate") or "",
                    "start_time": schedule_row.get("startTime") or _format_start_time_local(str(schedule_row.get("gameDate") or "")),
                    "official_date": schedule_row.get("officialDate") or date_str,
                    "status": {
                        "abstract": str(schedule_status.get("abstract") or ""),
                        "detailed": str(schedule_status.get("detailed") or ""),
                    },
                    "away": dict(schedule_row.get("away") or {}),
                    "home": dict(schedule_row.get("home") or {}),
                    "starter_names": {
                        "away": _first_text((probable.get("away") or {}).get("fullName")),
                        "home": _first_text((probable.get("home") or {}).get("fullName")),
                    },
                    "segments": {},
                    "pitcher_props": {},
                    "betting": dict(raw_game_betting) if isinstance(raw_game_betting, dict) else _empty_game_betting(),
                }
            )

    games_out.sort(
        key=lambda row: (
            str(row.get("game_date") or ""),
            str(row.get("start_time") or ""),
            int(_safe_int(row.get("game_pk")) or 0),
        )
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
                "first1": aggregate.get("first1") or {},
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
    candidates = [p for p in day_dir.glob(f"*{pk_tag}*.json") if p.is_file()]
    if candidates:
        return max(candidates, key=lambda p: (int(getattr(p.stat(), "st_mtime_ns", 0)), p.name))

    # Fallback: scan small set of json files for matching game_pk field.
    matches: List[Path] = []
    for p in day_dir.glob("*.json"):
        if not p.is_file():
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if _safe_int(obj.get("game_pk")) == int(game_pk):
            matches.append(p)
    if matches:
        return max(matches, key=lambda p: (int(getattr(p.stat(), "st_mtime_ns", 0)), p.name))
    return None


_SIM_FILE_PK_RE = re.compile(r"pk(\d+)", re.IGNORECASE)


def _sim_file_game_pk(path: Path) -> Optional[int]:
    match = _SIM_FILE_PK_RE.search(path.name)
    if match is not None:
        try:
            return int(match.group(1))
        except Exception:
            pass
    obj = _load_json_file(path)
    if isinstance(obj, dict):
        return _safe_int(obj.get("game_pk"))
    return None


def _list_unique_sim_files(day_dir: Path) -> List[Path]:
    latest_by_game: Dict[int, Path] = {}
    for path in sorted(day_dir.glob("sim_*.json")):
        if not path.is_file():
            continue
        game_pk = _sim_file_game_pk(path)
        if game_pk is None:
            continue
        current = latest_by_game.get(int(game_pk))
        if current is None:
            latest_by_game[int(game_pk)] = path
            continue
        try:
            current_mtime = int(getattr(current.stat(), "st_mtime_ns", 0))
        except Exception:
            current_mtime = 0
        try:
            next_mtime = int(getattr(path.stat(), "st_mtime_ns", 0))
        except Exception:
            next_mtime = 0
        if (next_mtime, path.name) >= (current_mtime, current.name):
            latest_by_game[int(game_pk)] = path
    return sorted(latest_by_game.values(), key=lambda path: path.name)


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
                    "BF": _round_stat(raw.get("batters_faced_mean"), 2),
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


def _pitching_row_has_appearance(row: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(row, dict):
        return False
    for key in ("OUTS", "BF", "P", "SO", "ER", "R", "H", "BB"):
        value = _safe_float(row.get(key))
        if value is not None and float(value) > 0.0:
            return True
    outs = _parse_ip_to_outs(row.get("IP"))
    return outs is not None and int(outs) > 0


def _current_pitching_side(snapshot: Optional[Dict[str, Any]]) -> Optional[str]:
    half = str((((snapshot or {}).get("current") or {}).get("halfInning") or "")).strip().lower()
    if half == "top":
        return "home"
    if half == "bottom":
        return "away"
    return None


def _starter_removed_from_snapshot(snapshot: Optional[Dict[str, Any]], side: str) -> bool:
    if side not in {"away", "home"} or not isinstance(snapshot, dict):
        return False

    team = (((snapshot.get("teams") or {}).get(side)) or {})
    starter = (team.get("starter") or {}) if isinstance(team.get("starter"), dict) else {}
    starter_id = _safe_int(starter.get("id"))
    starter_name = _first_text(starter.get("name"))
    if starter_id is None and not starter_name:
        return False

    current_pitching_side = _current_pitching_side(snapshot)
    current_pitcher = (((snapshot.get("current") or {}).get("pitcher")) or {})
    current_pitcher_id = _safe_int(current_pitcher.get("id"))
    current_pitcher_name = _first_text(current_pitcher.get("fullName"), current_pitcher.get("name"))
    if current_pitching_side == side:
        if starter_id is not None and current_pitcher_id is not None and int(current_pitcher_id) != int(starter_id):
            return True
        if starter_name and current_pitcher_name and normalize_pitcher_name(current_pitcher_name) != normalize_pitcher_name(starter_name):
            return True

    pitching_rows = (((team.get("boxscore") or {}).get("pitching")) or [])
    for row in pitching_rows:
        if not isinstance(row, dict):
            continue
        row_id = _safe_int(row.get("id"))
        row_name = _first_text(row.get("name"))
        is_starter_row = False
        if starter_id is not None and row_id is not None:
            is_starter_row = int(row_id) == int(starter_id)
        elif starter_name and row_name:
            is_starter_row = normalize_pitcher_name(row_name) == normalize_pitcher_name(starter_name)
        if is_starter_row:
            continue
        if _pitching_row_has_appearance(row):
            return True
    return False


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


def _status_is_live(status_text: Any) -> bool:
    if isinstance(status_text, dict):
        abstract = str(
            status_text.get("abstract")
            or status_text.get("abstractGameState")
            or ""
        ).strip().lower()
        detailed = str(
            status_text.get("detailed")
            or status_text.get("detailedState")
            or ""
        ).strip().lower()
    else:
        abstract = str(status_text or "").strip().lower()
        detailed = ""
    if _status_is_final({"abstract": abstract, "detailed": detailed}):
        return False
    if detailed == "warmup":
        return False
    if detailed in {"in progress", "manager challenge"}:
        return True
    if detailed.startswith(("top ", "bottom ", "mid ", "end ")):
        return True
    return abstract in {"live", "in progress", "manager challenge"}


def _status_is_final(status_text: Any) -> bool:
    if isinstance(status_text, dict):
        abstract = str(
            status_text.get("abstract")
            or status_text.get("abstractGameState")
            or ""
        ).strip().lower()
        detailed = str(
            status_text.get("detailed")
            or status_text.get("detailedState")
            or ""
        ).strip().lower()
        return _is_final_game_status(abstract) or _is_final_game_status(detailed)
    return _is_final_game_status(status_text)


def _format_matchup_live_text(current: Dict[str, Any]) -> str:
    inning = _safe_int(current.get("inning"))
    half = str(current.get("halfInning") or "").strip().lower()
    if inning is None:
        return ""
    if half == "top":
        return f"Top {int(inning)}"
    if half == "bottom":
        return f"Bot {int(inning)}"
    return f"Inning {int(inning)}"


def _official_betting_game_matchup_payload(
    game_pk: int,
    date_str: str,
    status: Dict[str, Any],
    score: Optional[Dict[str, Any]] = None,
    fetch_feed: bool = True,
) -> Dict[str, Any]:
    abstract = str((status or {}).get("abstract") or "").strip()
    detailed = str((status or {}).get("detailed") or "").strip()
    schedule_score = score if isinstance(score, dict) else {}
    out: Dict[str, Any] = {
        "isLive": bool(_status_is_live({"abstract": abstract, "detailed": detailed})),
        "isFinal": bool(_status_is_final(abstract)),
        "liveText": "",
        "displayState": detailed or abstract,
        "inning": None,
        "halfInning": "",
        "count": {"balls": None, "strikes": None, "outs": None},
        "batter": "",
        "pitcher": "",
        "score": {
            "away": _safe_int(schedule_score.get("away")),
            "home": _safe_int(schedule_score.get("home")),
        },
    }

    if int(game_pk or 0) <= 0:
        return out

    if not fetch_feed:
        if (not out.get("isLive")) and (not out.get("isFinal")):
            away_score = _safe_int((out.get("score") or {}).get("away"))
            home_score = _safe_int((out.get("score") or {}).get("home"))
            if int(away_score or 0) == 0 and int(home_score or 0) == 0:
                out["score"] = {"away": None, "home": None}
        return out

    feed = _load_game_feed_for_date(int(game_pk), str(date_str or "")) if _is_historical_date(str(date_str or "")) else None
    if not isinstance(feed, dict) or not feed:
        try:
            feed = fetch_game_feed_live(_client(), int(game_pk))
        except Exception:
            feed = None
    if not isinstance(feed, dict) or not feed:
        return out

    feed_status = ((feed.get("gameData") or {}).get("status") or {})
    abstract = str(feed_status.get("abstractGameState") or abstract).strip()
    detailed = str(feed_status.get("detailedState") or detailed).strip()
    current = _current_matchup(feed)
    away_totals = _team_totals(feed, "away")
    home_totals = _team_totals(feed, "home")
    count = current.get("count") if isinstance(current.get("count"), dict) else {}
    batter = current.get("batter") if isinstance(current.get("batter"), dict) else {}
    pitcher = current.get("pitcher") if isinstance(current.get("pitcher"), dict) else {}

    out.update(
        {
            "isLive": bool(_status_is_live({"abstract": abstract, "detailed": detailed})),
            "isFinal": bool(_status_is_final(abstract)),
            "liveText": _format_matchup_live_text(current),
            "displayState": detailed or abstract,
            "inning": _safe_int(current.get("inning")),
            "halfInning": str(current.get("halfInning") or ""),
            "count": {
                "balls": _safe_int(count.get("balls")),
                "strikes": _safe_int(count.get("strikes")),
                "outs": _safe_int(count.get("outs")),
            },
            "batter": str(batter.get("fullName") or ""),
            "pitcher": str(pitcher.get("fullName") or ""),
            "score": {
                "away": _safe_int(away_totals.get("R")),
                "home": _safe_int(home_totals.get("R")),
            },
        }
    )
    if not out.get("isLive"):
        out["liveText"] = ""
        out["inning"] = None
        out["halfInning"] = ""
        out["count"] = {"balls": None, "strikes": None, "outs": None}
        out["batter"] = ""
        out["pitcher"] = ""
    if (not out.get("isLive")) and (not out.get("isFinal")):
        score = out.get("score") if isinstance(out.get("score"), dict) else {}
        away_score = _safe_int(score.get("away"))
        home_score = _safe_int(score.get("home"))
        if int(away_score or 0) == 0 and int(home_score or 0) == 0:
            out["score"] = {"away": None, "home": None}
    return out


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


def _live_linescore(feed: Dict[str, Any]) -> Dict[str, Any]:
    linescore = ((feed.get("liveData") or {}).get("linescore") or {}) if isinstance(feed, dict) else {}
    innings_raw = linescore.get("innings") or []
    innings: List[Dict[str, int]] = []
    if isinstance(innings_raw, list):
        for inning in innings_raw:
            if not isinstance(inning, dict):
                continue
            away_runs = _safe_int(((inning.get("away") or {}).get("runs")))
            home_runs = _safe_int(((inning.get("home") or {}).get("runs")))
            if away_runs is None or home_runs is None:
                continue
            innings.append({"away": int(away_runs), "home": int(home_runs)})

    teams = (linescore.get("teams") or {}) if isinstance(linescore, dict) else {}
    away_runs = _safe_int(((teams.get("away") or {}).get("runs")))
    home_runs = _safe_int(((teams.get("home") or {}).get("runs")))
    if away_runs is None and innings:
        away_runs = sum(int(row.get("away") or 0) for row in innings)
    if home_runs is None and innings:
        home_runs = sum(int(row.get("home") or 0) for row in innings)
    if away_runs is None and home_runs is None and not innings:
        return {}

    def _segment_score(target_innings: int) -> Dict[str, int]:
        subset = innings[: max(0, int(target_innings))]
        return {
            "away": int(sum(int(row.get("away") or 0) for row in subset)),
            "home": int(sum(int(row.get("home") or 0) for row in subset)),
        }

    return {
        "innings": innings,
        "full": {
            "away": int(away_runs or 0),
            "home": int(home_runs or 0),
        },
        "first1": _segment_score(1),
        "first3": _segment_score(3),
        "first5": _segment_score(5),
        "first7": _segment_score(7),
    }


def _live_offense_state(feed: Dict[str, Any]) -> Dict[str, Any]:
    try:
        offense = (((feed.get("liveData") or {}).get("linescore") or {}).get("offense") or {})
        if not isinstance(offense, dict):
            return {}

        def _runner(base_key: str) -> Dict[str, Any]:
            row = offense.get(base_key) or {}
            if not isinstance(row, dict):
                return {"id": None, "fullName": ""}
            return {
                "id": _safe_int(row.get("id")),
                "fullName": str(row.get("fullName") or ""),
            }

        return {
            "first": _runner("first"),
            "second": _runner("second"),
            "third": _runner("third"),
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
    if "hitter_runs" in market or "runs_scored" in prop or prop == "runs":
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
        return "live" if _is_live_game_status(status_token) else "pending"
    if _is_live_game_status(status_token):
        return "live"
    if not _is_final_game_status(status_token):
        return "pending"
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


def _is_live_hitter_prop_market(market: Any) -> bool:
    token = str(market or "").strip().lower()
    return token == "hitter_props" or token.startswith("hitter_")


def _live_hitter_prop_row_actionable(row: Dict[str, Any]) -> bool:
    if not _is_live_hitter_prop_market(row.get("market")):
        return True
    pa_mean = _safe_float(row.get("pa_mean"))
    model_mean = _safe_float(row.get("model_mean"))
    live_projection = _safe_float(row.get("live_projection"))
    actual_value = _safe_float(row.get("actual_value"))
    if pa_mean is not None and float(pa_mean) <= 0.0:
        return False
    if model_mean is not None and float(model_mean) <= 0.0:
        return False
    if live_projection is not None and float(live_projection) <= 0.0 and (actual_value is None or float(actual_value) <= 0.0):
        return False
    return True


def _is_live_game_status(status_text: Any) -> bool:
    token = str(status_text or "").strip().lower()
    return token in {"live", "in progress", "manager challenge"} or "live" in token


def _is_final_game_status(status_text: Any) -> bool:
    token = str(status_text or "").strip().lower()
    if not token:
        return False
    return token in {"final", "completed early", "game over"} or token.startswith("final") or token.startswith("completed")


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
    min_market_edge: float = 0.0,
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
        if live_edge is None or float(live_edge) <= 0.0:
            continue
        projection_gap = abs(float(live_edge)) if live_edge is not None else None
        min_live_edge = 0.08 if selection == "over" else 0.18
        min_required_market_edge = float(min_market_edge) if selection == "over" else max(float(min_market_edge), 0.025)
        if projection_gap is None or float(projection_gap) < float(min_live_edge):
            continue
        market_edge = None
        if model_prob_over is not None:
            if selection == "over" and market_prob_over is not None:
                market_edge = round(float(model_prob_over) - float(market_prob_over), 4)
            elif selection == "under" and market_prob_under is not None:
                market_edge = round((1.0 - float(model_prob_over)) - float(market_prob_under), 4)
        score = market_edge
        if score is None or float(score) <= float(min_required_market_edge):
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
            float(item.get("marketEdge") or float("-inf")),
            float(item.get("liveEdge") or float("-inf")),
            float(item.get("projectionGap") or float("-inf")),
            1 if item.get("selection") == "over" else 0,
        ),
    )


def _sim_prop_models(sim_context: Optional[Dict[str, Any]], kind: str) -> Dict[str, Dict[str, Any]]:
    out = ((sim_context or {}).get("propModels") or {}).get(kind) or {}
    return out if isinstance(out, dict) else {}


def _live_pitcher_model_entry(
    pitcher_models: Optional[Dict[str, Dict[str, Any]]],
    *,
    team_side: str,
    starter_name: str,
) -> Optional[Dict[str, Any]]:
    if team_side not in {"away", "home"} or not isinstance(pitcher_models, dict):
        return None
    starter_key = normalize_pitcher_name(starter_name)
    if starter_key:
        exact_entry = pitcher_models.get(starter_key)
        if isinstance(exact_entry, dict) and str(exact_entry.get("team_side") or "").strip().lower() == team_side:
            return exact_entry
    return None


def _live_pitcher_model_mismatch(
    pitcher_models: Optional[Dict[str, Dict[str, Any]]],
    *,
    team_side: str,
    starter_name: str,
) -> Optional[Dict[str, Any]]:
    if team_side not in {"away", "home"} or not isinstance(pitcher_models, dict):
        return None
    starter_key = normalize_pitcher_name(starter_name)
    if starter_key and isinstance(pitcher_models.get(starter_key), dict):
        return None
    team_entries = [
        entry
        for entry in pitcher_models.values()
        if isinstance(entry, dict) and str(entry.get("team_side") or "").strip().lower() == team_side
    ]
    if len(team_entries) != 1:
        return None
    entry = team_entries[0]
    sim_starter_name = _first_text(entry.get("name"))
    if not sim_starter_name:
        return None
    if starter_key and normalize_pitcher_name(sim_starter_name) == starter_key:
        return None
    return {
        "team_side": team_side,
        "live_starter_name": starter_name,
        "sim_starter_name": sim_starter_name,
    }


def _live_prop_market_label(market: str, prop: str) -> str:
    market_text = str(market or "").strip().lower()
    prop_text = str(prop or "").strip().lower()
    if market_text == "pitcher_props":
        cfg = _PITCHER_LADDER_PROPS.get(prop_text) or {}
        return str(cfg.get("label") or market or "Pitcher prop")
    cfg = _HITTER_LADDER_PROPS.get(prop_text) or {}
    return str(cfg.get("label") or market or "Hitter prop")


def _live_prop_reason_subject(row: Dict[str, Any]) -> str:
    market = str(row.get("market") or "").strip().lower()
    if market == "pitcher_props":
        return str(row.get("pitcher_name") or "the starter").strip() or "the starter"
    return str(row.get("player_name") or "the hitter").strip() or "the hitter"


def _live_prop_reason_stat_label(row: Dict[str, Any]) -> str:
    label = str(row.get("market_label") or _live_prop_market_label(str(row.get("market") or ""), str(row.get("prop") or ""))).strip()
    if not label:
        return "the prop"
    label = label.replace("Hitter ", "").replace("Pitcher ", "")
    return label[:1].lower() + label[1:] if label else "the prop"


def _live_hitter_boxscore_reason(
    row: Dict[str, Any],
    actual_row: Optional[Dict[str, Any]],
) -> Optional[str]:
    if not isinstance(actual_row, dict):
        return None
    actual_value = _live_stat_value(actual_row, row)
    if actual_value is None:
        return None
    choice = str(row.get("selection") or "").strip().lower()
    line_value = _safe_float(row.get("market_line"))
    at_bats = _safe_int(actual_row.get("AB"))
    subject = _live_prop_reason_subject(row)
    stat_label = _live_prop_reason_stat_label(row)
    actual_text = _format_live_reason_value(actual_value)

    if choice == "over":
        if line_value is not None and float(actual_value) > float(line_value):
            return f"{subject} already has {actual_text} {stat_label} on the board, so this over is already home."
        if float(actual_value) > 0.0:
            return f"{subject} already has {actual_text} {stat_label} on the board, so the over is now part-way there before the remaining plate appearances."
        if at_bats is not None and int(at_bats) <= 1:
            return f"{subject} is only {int(at_bats)} at-bat into the game, so the live over is still leaning on remaining volume rather than a dead profile."
        return None

    if float(actual_value) <= 0.0 and at_bats is not None and int(at_bats) >= 2:
        return f"{subject} is still at {actual_text} {stat_label} through {int(at_bats)} at-bats, which keeps the under live if the remaining trips stay quiet."
    if line_value is not None and float(actual_value) < float(line_value):
        return f"{subject} has only reached {actual_text} {stat_label} so far, so the under is still ahead of the current number."
    return None


def _live_hitter_pitcher_hook_reason(
    row: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    matchup_profile: Optional[Dict[str, Any]],
    *,
    current_is_starter: bool,
    opponent_side: str,
) -> Optional[str]:
    if not isinstance(snapshot, dict) or not isinstance(matchup_profile, dict) or not current_is_starter:
        return None
    pitching_rows = (((snapshot.get("teams") or {}).get(opponent_side) or {}).get("boxscore") or {}).get("pitching") or []
    current_row = _lookup_boxscore_row(pitching_rows, matchup_profile.get("name"))
    if not isinstance(current_row, dict):
        return None

    pitch_count = _safe_int(current_row.get("P"))
    outs_recorded = _safe_int(current_row.get("OUTS"))
    if outs_recorded is None:
        outs_recorded = _parse_ip_to_outs(current_row.get("IP"))
    stamina = _safe_int(matchup_profile.get("stamina_pitches"))
    choice = str(row.get("selection") or "").strip().lower()
    subject = str(matchup_profile.get("name") or "the starter").strip() or "the starter"

    if choice == "over" and pitch_count is not None and stamina is not None and pitch_count >= max(1, int(stamina) - 10):
        return f"{subject} is already up to {int(pitch_count)} pitches against a leash around {int(stamina)}, so the plate-appearance path may flip to the bullpen soon."
    if choice == "under" and pitch_count is not None and stamina is not None and pitch_count <= max(0, int(stamina) - 20):
        return f"{subject} is still working at only {int(pitch_count)} pitches against a leash near {int(stamina)}, so the original starter matchup is likely to stay in place a bit longer."
    if choice == "under" and outs_recorded is not None and int(outs_recorded) >= 9 and pitch_count is not None and int(pitch_count) <= 55:
        return f"{subject} is still fairly efficient through {int(outs_recorded)} outs on {int(pitch_count)} pitches, which supports the tougher starter matchup holding."
    return None


def _live_pitcher_k_performance_reason(
    row: Dict[str, Any],
    actual_row: Optional[Dict[str, Any]],
) -> Optional[str]:
    if not isinstance(actual_row, dict):
        return None
    prop = str(row.get("prop") or "").strip().lower()
    choice = str(row.get("selection") or "").strip().lower()
    strikeouts = _safe_int(actual_row.get("SO"))
    batters_faced = _safe_int(actual_row.get("BF"))
    strikes = _safe_int(actual_row.get("S"))
    pitch_count = _safe_int(actual_row.get("P"))
    subject = _live_prop_reason_subject(row)

    if prop == "strikeouts" and strikeouts is not None:
        if choice == "over" and batters_faced is not None and int(batters_faced) > 0:
            k_rate = float(strikeouts) / float(batters_faced)
            if strikeouts >= 3 and k_rate >= 0.28:
                return f"{subject} already has {int(strikeouts)} strikeouts through {int(batters_faced)} batters, so the in-game bat-missing pace is still live enough for the over."
        if choice == "under" and batters_faced is not None and int(batters_faced) >= 9 and strikeouts <= 2:
            return f"{subject} only has {int(strikeouts)} strikeouts through {int(batters_faced)} batters, so the in-game K pace is still lagging the current number."

    if prop == "outs" and pitch_count is not None and strikes is not None and int(pitch_count) > 0:
        strike_rate = float(strikes) / float(pitch_count)
        outs_recorded = _safe_int(actual_row.get("OUTS"))
        if outs_recorded is None:
            outs_recorded = _parse_ip_to_outs(actual_row.get("IP"))
        if choice == "over" and outs_recorded is not None and pitch_count <= max(1, int(outs_recorded) * 6):
            return f"{subject} has banked {int(outs_recorded)} outs on only {int(pitch_count)} pitches, which is efficient enough to keep the over path open."
        if choice == "under" and strike_rate <= 0.61 and pitch_count >= 50:
            return f"{subject} is only around a {strike_rate * 100.0:.0f}% strike rate on {int(pitch_count)} pitches, which is the kind of traffic that can bring the hook in faster."
    return None


def _live_pitcher_manager_hook_reason(
    row: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    actual_row: Optional[Dict[str, Any]],
    pitcher_profile: Optional[Dict[str, Any]],
) -> Optional[str]:
    if not isinstance(actual_row, dict):
        return None
    prop = str(row.get("prop") or "").strip().lower()
    if prop not in {"outs", "strikeouts", "earned_runs"}:
        return None
    choice = str(row.get("selection") or "").strip().lower()
    pitch_count = _safe_int(actual_row.get("P"))
    stamina = _safe_int((pitcher_profile or {}).get("stamina_pitches")) if isinstance(pitcher_profile, dict) else None
    batters_faced = _safe_int(actual_row.get("BF"))
    side = str(row.get("team_side") or "").strip().lower()
    team_score = _safe_int(row.get("score_away")) if side == "away" else _safe_int(row.get("score_home"))
    opp_score = _safe_int(row.get("score_home")) if side == "away" else _safe_int(row.get("score_away"))
    subject = _live_prop_reason_subject(row)

    if choice == "under":
        if pitch_count is not None and stamina is not None and pitch_count >= max(1, int(stamina) - 5):
            return f"{subject} is basically at the edge of a normal leash at {int(pitch_count)} pitches versus roughly {int(stamina)}, so the manager-hook risk is now real."
        if batters_faced is not None and int(batters_faced) >= 21:
            return f"{subject} is already deep into the lineup for a third time through at {int(batters_faced)} batters faced, which is a common manager decision point."
        if team_score is not None and opp_score is not None and int(team_score) < int(opp_score) - 3:
            return f"{subject}'s club is already down {int(opp_score) - int(team_score)}, which makes a shorter leash more likely if one more jam shows up."
    elif choice == "over":
        if pitch_count is not None and stamina is not None and pitch_count <= max(0, int(stamina) - 18):
            return f"{subject} is still well short of a normal hook point at {int(pitch_count)} pitches against a leash near {int(stamina)}, so the manager can still leave him out there."
    return None


def _format_live_reason_value(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    text = f"{float(number):.1f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def _pitcher_role_reason_label(value: Any) -> str:
    role = str(value or "").strip().upper()
    labels = {
        "SP": "starter",
        "CL": "closer",
        "SU": "setup arm",
        "MR": "middle reliever",
        "LR": "long reliever",
        "RP": "reliever",
    }
    return str(labels.get(role) or "reliever")


def _dedupe_reason_texts(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _live_hitter_profile_for_row(
    roster_snapshot: Optional[Dict[str, Any]],
    side: str,
    player_name: str,
    lineup_order: Optional[int],
) -> Optional[Dict[str, Any]]:
    if side not in {"away", "home"} or not isinstance(roster_snapshot, dict):
        return None
    side_doc = roster_snapshot.get(side)
    if not isinstance(side_doc, dict):
        return None
    lineup = side_doc.get("lineup") if isinstance(side_doc.get("lineup"), list) else []
    target_name = normalize_pitcher_name(player_name)
    fallback: Optional[Dict[str, Any]] = None
    for row in lineup:
        if not isinstance(row, dict):
            continue
        if target_name and normalize_pitcher_name(str(row.get("name") or "")) == target_name:
            return row
        try:
            if fallback is None and lineup_order is not None and int(row.get("lineup_order") or 0) == int(lineup_order):
                fallback = row
        except Exception:
            continue
    return fallback


def _live_pitcher_profile_matches(profile: Optional[Dict[str, Any]], pitcher_id: Optional[int], pitcher_name: str) -> bool:
    if not isinstance(profile, dict):
        return False
    profile_id = _safe_int(profile.get("id"))
    if pitcher_id is not None and profile_id is not None and int(profile_id) == int(pitcher_id):
        return True
    profile_name = normalize_pitcher_name(str(profile.get("name") or ""))
    return bool(profile_name and pitcher_name and profile_name == normalize_pitcher_name(pitcher_name))


def _live_pitcher_profile_for_game_state(
    roster_snapshot: Optional[Dict[str, Any]],
    snapshot: Optional[Dict[str, Any]],
    opponent_side: str,
) -> Dict[str, Any]:
    if opponent_side not in {"away", "home"} or not isinstance(roster_snapshot, dict):
        return {}
    opp_doc = roster_snapshot.get(opponent_side)
    if not isinstance(opp_doc, dict):
        return {}

    starter_profile = opp_doc.get("starter_profile") if isinstance(opp_doc.get("starter_profile"), dict) else None
    bullpen_profiles = [row for row in (opp_doc.get("bullpen_profiles") or []) if isinstance(row, dict)]
    current_pitching_side = _current_pitching_side(snapshot)
    current_pitcher = (((snapshot or {}).get("current") or {}).get("pitcher") or {}) if isinstance(snapshot, dict) else {}
    current_pitcher_id = _safe_int(current_pitcher.get("id"))
    current_pitcher_name = _first_text(current_pitcher.get("fullName"), current_pitcher.get("name"))

    current_profile: Optional[Dict[str, Any]] = None
    if current_pitching_side == opponent_side and (current_pitcher_id is not None or current_pitcher_name):
        if _live_pitcher_profile_matches(starter_profile, current_pitcher_id, current_pitcher_name):
            current_profile = starter_profile
        else:
            for profile in bullpen_profiles:
                if _live_pitcher_profile_matches(profile, current_pitcher_id, current_pitcher_name):
                    current_profile = profile
                    break

    current_is_starter = bool(
        current_profile is not None
        and starter_profile is not None
        and _live_pitcher_profile_matches(starter_profile, _safe_int(current_profile.get("id")), str(current_profile.get("name") or ""))
    )
    return {
        "starter_profile": starter_profile,
        "current_profile": current_profile,
        "current_is_starter": current_is_starter,
        "starter_removed": _starter_removed_from_snapshot(snapshot, opponent_side),
    }


def _live_hitter_matchup_delta_reason(
    batter_profile: Dict[str, Any],
    starter_profile: Optional[Dict[str, Any]],
    current_profile: Optional[Dict[str, Any]],
    *,
    selection: str,
) -> Optional[str]:
    if not isinstance(batter_profile, dict) or not isinstance(starter_profile, dict) or not isinstance(current_profile, dict):
        return None
    batter_side = str(batter_profile.get("bat") or "").strip().upper()
    if batter_side not in {"L", "R"}:
        return None
    platoon_key = "platoon_mult_vs_lhb" if batter_side == "L" else "platoon_mult_vs_rhb"
    starter_platoon = starter_profile.get(platoon_key) if isinstance(starter_profile.get(platoon_key), dict) else {}
    current_platoon = current_profile.get(platoon_key) if isinstance(current_profile.get(platoon_key), dict) else {}
    if not isinstance(starter_platoon, dict) or not isinstance(current_platoon, dict):
        return None

    starter_inplay = _safe_float(starter_platoon.get("inplay"))
    starter_hr = _safe_float(starter_platoon.get("hr"))
    starter_k = _safe_float(starter_platoon.get("k"))
    current_inplay = _safe_float(current_platoon.get("inplay"))
    current_hr = _safe_float(current_platoon.get("hr"))
    current_k = _safe_float(current_platoon.get("k"))
    if None in {starter_inplay, starter_hr, starter_k, current_inplay, current_hr, current_k}:
        return None

    starter_score = float(starter_inplay) + (0.8 * float(starter_hr)) - (0.7 * float(starter_k))
    current_score = float(current_inplay) + (0.8 * float(current_hr)) - (0.7 * float(current_k))
    delta = float(current_score) - float(starter_score)
    current_name = str(current_profile.get("name") or "the current reliever").strip()
    role_label = _pitcher_role_reason_label(current_profile.get("role"))
    choice = str(selection or "").strip().lower()

    if delta >= 0.08 and choice == "over":
        return f"The starter is out, and the matchup has shifted to {current_name}, a {role_label} whose contact profile is softer for this bat than the opening starter."
    if delta <= -0.08 and choice == "under":
        return f"The starter is out, and the game has turned over to {current_name}, a {role_label} who grades tougher for this bat than the opening starter."
    return None


def _live_hitter_game_state_reason(row: Dict[str, Any], snapshot: Optional[Dict[str, Any]]) -> Optional[str]:
    side = str(row.get("team_side") or "").strip().lower()
    if side not in {"away", "home"}:
        return None
    team_score = _safe_int(row.get("score_away")) if side == "away" else _safe_int(row.get("score_home"))
    opp_score = _safe_int(row.get("score_home")) if side == "away" else _safe_int(row.get("score_away"))
    if team_score is None or opp_score is None:
        return None
    progress = _live_game_progress(snapshot)
    remaining_outs = _game_lens_remaining_outs(progress)
    margin = int(team_score) - int(opp_score)
    choice = str(row.get("selection") or "").strip().lower()

    if abs(margin) <= 2 and remaining_outs <= 18:
        return f"The game is still within {int(abs(margin))} run{'s' if abs(margin) != 1 else ''}, so regular high-leverage plate appearances should stay intact."
    if margin < 0 and choice == "over":
        return f"His club is trailing by {int(abs(margin))}, which keeps the offense pressing and protects regular late-game at-bats."
    if margin > 0 and choice == "under" and remaining_outs <= 15:
        return f"His club is ahead by {int(margin)}, and with limited outs left the under gets some help from shrinking offensive volume."
    return None


def _live_hitter_inning_reason(row: Dict[str, Any], snapshot: Optional[Dict[str, Any]]) -> Optional[str]:
    progress = _live_game_progress(snapshot)
    inning = _safe_int(progress.get("inning"))
    if inning is None:
        return None
    remaining_outs = _game_lens_remaining_outs(progress)
    choice = str(row.get("selection") or "").strip().lower()
    if remaining_outs <= 12:
        if choice == "under":
            return f"With only {int(remaining_outs)} outs left in the game, remaining plate-appearance volume is getting tight, which supports the under path."
        if choice == "over":
            return f"Even with only {int(remaining_outs)} outs left, the live projection still stays beyond the current number."
    if int(inning) >= 7 and choice == "over":
        return "This has turned into a late-game prop, so the over now needs impact quality more than a full game of volume."
    if int(inning) >= 7 and choice == "under":
        return "The game is already in the late innings, which naturally squeezes the remaining volume for the under path."
    return None


def _live_hitter_matchup_reasons(
    row: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    sim_context: Optional[Dict[str, Any]],
) -> List[str]:
    if not isinstance(sim_context, dict):
        return []
    roster_snapshot = sim_context.get("roster_snapshot") if isinstance(sim_context.get("roster_snapshot"), dict) else None
    if not isinstance(roster_snapshot, dict):
        return []

    side = str(row.get("team_side") or "").strip().lower()
    if side not in {"away", "home"}:
        return []
    opponent_side = "home" if side == "away" else "away"
    batter_profile = _live_hitter_profile_for_row(
        roster_snapshot,
        side,
        str(row.get("player_name") or ""),
        _safe_int(row.get("lineup_order")),
    )
    if not isinstance(batter_profile, dict):
        return []

    pitcher_ctx = _live_pitcher_profile_for_game_state(roster_snapshot, snapshot, opponent_side)
    starter_profile = pitcher_ctx.get("starter_profile") if isinstance(pitcher_ctx.get("starter_profile"), dict) else None
    current_profile = pitcher_ctx.get("current_profile") if isinstance(pitcher_ctx.get("current_profile"), dict) else None
    current_is_starter = bool(pitcher_ctx.get("current_is_starter"))
    starter_removed = bool(pitcher_ctx.get("starter_removed"))
    choice = str(row.get("selection") or "").strip().lower()
    prop = str(row.get("prop") or "")
    actual_row = _lookup_boxscore_row(
        ((((snapshot or {}).get("teams") or {}).get(side) or {}).get("boxscore") or {}).get("batting") or [],
        str(row.get("player_name") or ""),
    )

    reasons: List[str] = []
    matchup_profile = current_profile if isinstance(current_profile, dict) else starter_profile
    if isinstance(current_profile, dict):
        current_name = str(current_profile.get("name") or "the current pitcher").strip()
        if current_is_starter:
            reasons.append(f"He is still lined up against the starter, {current_name}, so the original matchup is still the live path.")
        else:
            role_label = _pitcher_role_reason_label(current_profile.get("role"))
            reasons.append(f"The starter is already out, so the remaining plate appearances are now running through {current_name}, a {role_label}.")
            delta_reason = _live_hitter_matchup_delta_reason(batter_profile, starter_profile, current_profile, selection=choice)
            if delta_reason:
                reasons.append(delta_reason)
    elif starter_removed:
        reasons.append("The opposing starter is already out, so the remaining path is bullpen-based rather than the original starter look.")

    hook_reason = _live_hitter_pitcher_hook_reason(
        row,
        snapshot,
        matchup_profile,
        current_is_starter=current_is_starter,
        opponent_side=opponent_side,
    )
    if hook_reason:
        reasons.append(hook_reason)

    if isinstance(batter_profile, dict) and isinstance(matchup_profile, dict):
        reasons.extend(
            reason
            for reason in (
                _hitter_bvp_reason(
                    batter_profile,
                    matchup_profile,
                    season=_safe_int(row.get("season")),
                    prop=prop,
                    selection=choice,
                    line_value=_safe_float(row.get("market_line")),
                ),
                _hitter_pitch_mix_reason(batter_profile, matchup_profile, prop=prop, selection=choice),
                _hitter_platoon_reason(batter_profile, matchup_profile, prop=prop, selection=choice),
                _hitter_statcast_quality_reason(batter_profile, prop=prop, selection=choice),
            )
            if str(reason or "").strip()
        )

    progress_reason = _live_hitter_boxscore_reason(row, actual_row if isinstance(actual_row, dict) else None)
    if progress_reason:
        reasons.append(progress_reason)

    game_state_reason = _live_hitter_game_state_reason(row, snapshot)
    if game_state_reason:
        reasons.append(game_state_reason)
    inning_reason = _live_hitter_inning_reason(row, snapshot)
    if inning_reason:
        reasons.append(inning_reason)
    return _dedupe_reason_texts(reasons)


def _live_pitcher_matchup_context(
    row: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    sim_context: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not isinstance(sim_context, dict):
        return {}
    roster_snapshot = sim_context.get("roster_snapshot") if isinstance(sim_context.get("roster_snapshot"), dict) else None
    if not isinstance(roster_snapshot, dict):
        return {}
    side = str(row.get("team_side") or "").strip().lower()
    if side not in {"away", "home"}:
        return {}
    opp_side = "home" if side == "away" else "away"
    side_doc = roster_snapshot.get(side)
    opp_doc = roster_snapshot.get(opp_side)
    if not isinstance(side_doc, dict) or not isinstance(opp_doc, dict):
        return {}
    pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else None
    bullpen_profiles = [item for item in (side_doc.get("bullpen_profiles") or []) if isinstance(item, dict)]
    opponent_lineup = opp_doc.get("lineup") if isinstance(opp_doc.get("lineup"), list) else []
    pitcher_models = _sim_prop_models(sim_context, "pitchers")
    pitcher_name = str(row.get("pitcher_name") or (pitcher_profile or {}).get("name") or "").strip()
    pitcher_entry = pitcher_models.get(normalize_pitcher_name(pitcher_name)) if pitcher_name else None
    pitcher_model = pitcher_entry.get("model") if isinstance(pitcher_entry, dict) and isinstance(pitcher_entry.get("model"), dict) else None
    pitcher_state = _live_pitcher_profile_for_game_state(roster_snapshot, snapshot, side)
    return {
        "pitcher_profile": pitcher_profile,
        "bullpen_profiles": bullpen_profiles,
        "opponent_lineup": [item for item in opponent_lineup if isinstance(item, dict)],
        "pitcher_model": pitcher_model,
        "current_profile": pitcher_state.get("current_profile") if isinstance(pitcher_state.get("current_profile"), dict) else None,
        "current_is_starter": bool(pitcher_state.get("current_is_starter")),
    }


def _live_pitcher_efficiency_reason(
    row: Dict[str, Any],
    actual_row: Optional[Dict[str, Any]],
    pitcher_model: Optional[Dict[str, Any]],
) -> Optional[str]:
    if not isinstance(actual_row, dict) or not isinstance(pitcher_model, dict):
        return None
    prop = str(row.get("prop") or "").strip().lower()
    if prop not in {"outs", "strikeouts"}:
        return None

    pitch_count = _safe_float(actual_row.get("P"))
    batters_faced = _safe_float(actual_row.get("BF"))
    expected_pitches = _safe_float(pitcher_model.get("pitches_mean"))
    expected_bf = _safe_float(pitcher_model.get("batters_faced_mean"))
    if pitch_count is None or batters_faced is None or expected_pitches is None or expected_bf is None:
        return None
    if float(batters_faced) <= 0.0 or float(expected_bf) <= 0.0:
        return None

    actual_ppbf = float(pitch_count) / float(batters_faced)
    expected_ppbf = float(expected_pitches) / float(expected_bf)
    if expected_ppbf <= 0.0:
        return None

    choice = str(row.get("selection") or "").strip().lower()
    subject = _live_prop_reason_subject(row)
    ratio = float(actual_ppbf / expected_ppbf)

    if choice == "over" and float(batters_faced) >= 9.0 and ratio <= 0.94:
        return (
            f"{subject} is only around {actual_ppbf:.1f} pitches per batter so far versus a pregame pace near {expected_ppbf:.1f}, "
            f"which leaves more pitch-count runway than the baseline expected."
        )
    if choice == "under" and float(pitch_count) >= 35.0 and ratio >= 1.08:
        return (
            f"{subject} is already around {actual_ppbf:.1f} pitches per batter versus a pregame pace near {expected_ppbf:.1f}, "
            f"which is the kind of grind that can burn through the leash faster."
        )
    return None


def _live_pitcher_count_reason(
    row: Dict[str, Any],
    actual_row: Optional[Dict[str, Any]],
    pitcher_profile: Optional[Dict[str, Any]],
) -> Optional[str]:
    if not isinstance(actual_row, dict):
        return None
    choice = str(row.get("selection") or "").strip().lower()
    pitch_count = _safe_int(actual_row.get("P"))
    batters_faced = _safe_int(actual_row.get("BF"))
    outs_recorded = _safe_int(actual_row.get("OUTS"))
    if outs_recorded is None:
        outs_recorded = _parse_ip_to_outs(actual_row.get("IP"))
    stamina = _safe_int((pitcher_profile or {}).get("stamina_pitches")) if isinstance(pitcher_profile, dict) else None

    if choice == "over":
        if pitch_count is not None and stamina is not None and pitch_count <= max(0, int(stamina) - 15):
            return f"He is only at {int(pitch_count)} pitches against a leash closer to {int(stamina)}, so there is still room for more workload."
        if batters_faced is not None and batters_faced <= 18 and (outs_recorded or 0) >= 9:
            return f"He is only {float(batters_faced) / 9.0:.1f} times through the order, which keeps the deeper-outing path available."
    elif choice == "under":
        if pitch_count is not None and stamina is not None and pitch_count >= max(1, int(stamina) - 8):
            return f"He is already up to {int(pitch_count)} pitches against a leash around {int(stamina)}, so the hook risk is climbing."
        if batters_faced is not None and batters_faced >= 19:
            return "He is already into the third trip through the order, which raises the chance the outing gets cut shorter from here."
    return None


def _live_pitcher_game_state_reason(row: Dict[str, Any], snapshot: Optional[Dict[str, Any]]) -> Optional[str]:
    side = str(row.get("team_side") or "").strip().lower()
    if side not in {"away", "home"}:
        return None
    team_score = _safe_int(row.get("score_away")) if side == "away" else _safe_int(row.get("score_home"))
    opp_score = _safe_int(row.get("score_home")) if side == "away" else _safe_int(row.get("score_away"))
    if team_score is None or opp_score is None:
        return None
    margin = int(team_score) - int(opp_score)
    choice = str(row.get("selection") or "").strip().lower()
    remaining_outs = _game_lens_remaining_outs(_live_game_progress(snapshot))
    prop = str(row.get("prop") or "").strip().lower()

    if prop in {"outs", "strikeouts"} and choice == "over" and abs(margin) <= 2 and remaining_outs >= 9:
        return "The score is still tight enough that a normal starter leash is more likely to stay in place."
    if prop in {"outs", "strikeouts"} and choice == "under" and margin <= -4:
        return f"His club is trailing by {int(abs(margin))}, which raises the chance the manager shortens the outing from here."
    if prop == "earned_runs" and choice == "over" and margin <= -2:
        return "The game script is already leaning against him, so more run pressure is still in play."
    if prop == "earned_runs" and choice == "under" and margin >= 2 and remaining_outs <= 15:
        return "With his club protecting a lead late, the clean finish path is still intact if he avoids one damaging inning."
    return None


def _live_pitcher_matchup_reasons(
    row: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    sim_context: Optional[Dict[str, Any]],
    actual_row: Optional[Dict[str, Any]],
) -> List[str]:
    ctx = _live_pitcher_matchup_context(row, snapshot, sim_context)
    pitcher_profile = ctx.get("pitcher_profile") if isinstance(ctx.get("pitcher_profile"), dict) else None
    opponent_lineup = ctx.get("opponent_lineup") if isinstance(ctx.get("opponent_lineup"), list) else []
    pitcher_model = ctx.get("pitcher_model") if isinstance(ctx.get("pitcher_model"), dict) else None
    if not isinstance(pitcher_profile, dict):
        return []
    choice = str(row.get("selection") or "").strip().lower()
    prop = str(row.get("prop") or "")

    reasons: List[str] = []
    pitch_count_reason = _live_pitcher_count_reason(row, actual_row, pitcher_profile)
    if pitch_count_reason:
        reasons.append(pitch_count_reason)
    efficiency_reason = _live_pitcher_efficiency_reason(row, actual_row, pitcher_model)
    if efficiency_reason:
        reasons.append(efficiency_reason)
    k_perf_reason = _live_pitcher_k_performance_reason(row, actual_row)
    if k_perf_reason:
        reasons.append(k_perf_reason)
    hook_reason = _live_pitcher_manager_hook_reason(row, snapshot, actual_row, pitcher_profile)
    if hook_reason:
        reasons.append(hook_reason)
    reasons.extend(
        reason
        for reason in (
            _pitch_mix_reason(pitcher_profile, prop=prop, selection=choice),
            _opponent_lineup_reason(pitcher_profile, opponent_lineup, prop=prop, selection=choice),
            _pitcher_statcast_quality_reason(pitcher_profile, prop=prop, selection=choice),
            _pitcher_workload_reason(pitcher_profile, prop=prop, selection=choice),
            _pitcher_bvp_reason(pitcher_profile, opponent_lineup),
        )
        if str(reason or "").strip()
    )
    game_state_reason = _live_pitcher_game_state_reason(row, snapshot)
    if game_state_reason:
        reasons.append(game_state_reason)
    return _dedupe_reason_texts(reasons)


def _live_staff_state(snapshot: Optional[Dict[str, Any]]) -> Dict[str, bool]:
    return {
        "awayStarterOut": _starter_removed_from_snapshot(snapshot, "away"),
        "homeStarterOut": _starter_removed_from_snapshot(snapshot, "home"),
    }


def _game_lens_leader_text(selected_side: str, actual_home: Optional[float], actual_away: Optional[float], remaining_outs: int) -> str:
    current_home_margin = float(_safe_float(actual_home) or 0.0) - float(_safe_float(actual_away) or 0.0)
    current_side_margin = current_home_margin if selected_side == "home" else -current_home_margin
    return _game_lens_score_phrase(current_side_margin, remaining_outs)


def _game_lens_bullpen_context(snapshot: Optional[Dict[str, Any]]) -> str:
    staff = _live_staff_state(snapshot)
    if staff.get("awayStarterOut") and staff.get("homeStarterOut"):
        return "Both clubs are already into the bullpens."
    if staff.get("awayStarterOut"):
        return "The away starter is already out, while the home starter is still carrying the game."
    if staff.get("homeStarterOut"):
        return "The home starter is already out, while the away starter is still carrying the game."
    return "Both starters are still shaping the run environment."


def _game_lens_score_text(actual_home: Optional[float], actual_away: Optional[float]) -> str:
    home_score = _safe_float(actual_home)
    away_score = _safe_float(actual_away)
    if home_score is None or away_score is None:
        return ""
    return f"home {home_score:.0f}, away {away_score:.0f}"


def _game_lens_segment_result_text(label: str, actual_home: Optional[float], actual_away: Optional[float], *, closed: bool) -> str:
    home_score = _safe_float(actual_home)
    away_score = _safe_float(actual_away)
    if home_score is None or away_score is None:
        return ""
    score_text = _game_lens_score_text(home_score, away_score)
    if closed:
        if home_score > away_score:
            return f"{label} closed with the home side winning at {score_text}."
        if away_score > home_score:
            return f"{label} closed with the away side winning at {score_text}."
        return f"{label} closed tied at {score_text}."
    return f"{label} currently sits at {score_text}."


def _game_lens_reason_label(label: str) -> str:
    text = str(label or "").strip()
    if text.startswith("F") or text == "Full Game":
        return text
    return "live"


def _game_lens_markets_for_lane(markets: Optional[Dict[str, Any]], lane_key: str) -> Dict[str, Any]:
    if not isinstance(markets, dict):
        return {}
    segments = markets.get("segments") if isinstance(markets.get("segments"), dict) else {}
    if lane_key in {"live", "full"}:
        full_bucket = segments.get("full") if isinstance(segments.get("full"), dict) else None
        return dict(full_bucket) if isinstance(full_bucket, dict) else markets
    bucket = segments.get(lane_key)
    return dict(bucket) if isinstance(bucket, dict) else {}


def _game_lens_actual_segment(snapshot: Optional[Dict[str, Any]], target_innings: int) -> Dict[str, Optional[float]]:
    linescore = (snapshot or {}).get("linescore") if isinstance(snapshot, dict) else {}
    key = {1: "first1", 3: "first3", 5: "first5", 7: "first7", 9: "full"}.get(int(target_innings), "full")
    if isinstance(linescore, dict) and isinstance(linescore.get(key), dict):
        segment = linescore.get(key) or {}
        away_score = _safe_float(segment.get("away"))
        home_score = _safe_float(segment.get("home"))
    else:
        away_score = _safe_float((((snapshot or {}).get("teams") or {}).get("away") or {}).get("totals", {}).get("R"))
        home_score = _safe_float((((snapshot or {}).get("teams") or {}).get("home") or {}).get("totals", {}).get("R"))
    total = None if away_score is None or home_score is None else float(away_score) + float(home_score)
    margin = None if away_score is None or home_score is None else float(home_score) - float(away_score)
    return {
        "away": away_score,
        "home": home_score,
        "total": total,
        "homeMargin": margin,
    }


def _annotate_live_prop_reason_fields(
    row: Dict[str, Any],
    *,
    snapshot: Optional[Dict[str, Any]] = None,
    sim_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    item = dict(row)
    selection = str(item.get("selection") or "").strip().lower()
    market = str(item.get("market") or "").strip().lower()
    label = str(item.get("market_label") or _live_prop_market_label(market, str(item.get("prop") or ""))).strip()
    model_prob_over = _safe_float(item.get("model_prob_over"))
    selected_market_prob = _safe_float(item.get("selected_side_market_prob"))
    selected_model_prob: Optional[float] = None
    if model_prob_over is not None and selection == "over":
        selected_model_prob = float(model_prob_over)
    elif model_prob_over is not None and selection == "under":
        selected_model_prob = 1.0 - float(model_prob_over)

    common_reasons: List[str] = []
    reasons: List[str] = []
    if selected_model_prob is not None and selected_market_prob is not None:
        common_reasons.append(
            f"The model lands on the {selection} side in {selected_model_prob * 100.0:.1f}% of sims, while the market is pricing it closer to {selected_market_prob * 100.0:.1f}%."
        )

    actual_text = _format_live_reason_value(item.get("actual"))
    projection_text = _format_live_reason_value(item.get("live_projection"))
    line_text = _format_live_reason_value(item.get("market_line"))
    if projection_text and line_text:
        actual_clause = f" Current actual sits at {actual_text}." if actual_text else ""
        common_reasons.append(f"{actual_clause.strip() or 'Current actual is still pending.'} The live projection is {projection_text} against a line of {line_text} for {label.lower()}.")

    actual_row = None
    if market == "pitcher_props" and isinstance(snapshot, dict):
        team_side = str(item.get("team_side") or "").strip().lower()
        if team_side in {"away", "home"}:
            actual_row = _lookup_boxscore_row(
                ((((snapshot.get("teams") or {}).get(team_side)) or {}).get("boxscore") or {}).get("pitching") or [],
                str(item.get("pitcher_name") or ""),
            )

    if market == "pitcher_props":
        reasons.extend(_live_pitcher_matchup_reasons(item, snapshot, sim_context, actual_row if isinstance(actual_row, dict) else None))
        reasons.extend(common_reasons)
        model_mean_text = _format_live_reason_value(item.get("model_mean"))
        if model_mean_text:
            reasons.append(f"The pregame model mean sat around {model_mean_text} for this prop.")
    else:
        reasons.extend(_live_hitter_matchup_reasons(item, snapshot, sim_context))
        lineup_order = _safe_int(item.get("lineup_order"))
        pa_mean = _safe_float(item.get("pa_mean"))
        if lineup_order is not None and pa_mean is not None:
            reasons.append(f"He opened in lineup spot {int(lineup_order)} and was projected for about {float(pa_mean):.1f} plate appearances.")
        elif pa_mean is not None:
            reasons.append(f"He was projected for about {float(pa_mean):.1f} plate appearances pregame.")
        reasons.extend(common_reasons)

    live_text = str(item.get("live_text") or "").strip()
    if live_text:
        reasons.append(f"Game state: {live_text}.")

    reasons = _dedupe_reason_texts(reasons)
    summary = " ".join(reasons[:4]).strip()
    if summary:
        item["reason_summary"] = summary
        item["reasons"] = reasons
    return item


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
        if market == "pitcher_props" and team_side in {"away", "home"} and _starter_removed_from_snapshot(snapshot, str(team_side)):
            continue

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
        if not _live_hitter_prop_row_actionable(item):
            continue
        if team_side:
            item["team_side"] = team_side
        if team_info:
            item["team"] = team_info.get("abbr") or team_info.get("name")
        reason_summary = str(first_snapshot.get("reasonSummary") or "").strip()
        reasons = [
            str(reason).strip()
            for reason in (first_snapshot.get("reasons") or [])
            if str(reason).strip()
        ]
        if reason_summary:
            item["reason_summary"] = reason_summary
        if reasons:
            item["reasons"] = reasons
        elif reason_summary:
            item["reasons"] = [reason_summary]
        rows.append(item)
    return _apply_live_prop_ranking_scores(rows)


def _current_live_prop_rows(
    card: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    sim_context: Optional[Dict[str, Any]],
    d: str,
    *,
    write_observation_log: bool = False,
    ensure_market_fresh: bool = True,
) -> List[Dict[str, Any]]:
    if not isinstance(snapshot, dict):
        return []

    status = (snapshot or {}).get("status") or {}
    abstract = str(status.get("abstractGameState") or ((card or {}).get("status") or {}).get("abstract") or "").strip().lower()
    if abstract == "final":
        return _final_live_prop_rows_from_registry(card, snapshot, d)
    if not isinstance(sim_context, dict) or not sim_context.get("found"):
        return []
    if abstract != "live":
        return []

    if ensure_market_fresh:
        _maybe_refresh_live_oddsapi_markets(d)

    progress_fraction = float((_live_game_progress(snapshot, card).get("fraction") or 0.0))
    actual_teams = ((snapshot or {}).get("teams") or {})
    pitcher_market_ctx = _load_pitcher_ladder_market_context(d)
    hitter_market_ctx = _load_hitter_ladder_market_context(d)
    pitcher_market_lines = pitcher_market_ctx.get("displayLines") if isinstance(pitcher_market_ctx.get("displayLines"), dict) else {}
    hitter_market_lines = hitter_market_ctx.get("displayLines") if isinstance(hitter_market_ctx.get("displayLines"), dict) else {}
    pitcher_market_source = str(pitcher_market_ctx.get("displaySource") or "")
    hitter_market_source = str(hitter_market_ctx.get("displaySource") or "")
    pitcher_models = _sim_prop_models(sim_context, "pitchers")
    hitter_models = _sim_prop_models(sim_context, "hitters")
    rows: List[Dict[str, Any]] = []
    pitcher_model_mismatches: List[Dict[str, Any]] = []

    for side in ("away", "home"):
        if _starter_removed_from_snapshot(snapshot, side):
            continue
        starter_name = _first_text((((actual_teams.get(side) or {}).get("starter") or {}).get("name")))
        starter_key = normalize_pitcher_name(starter_name)
        model_entry = _live_pitcher_model_entry(pitcher_models, team_side=side, starter_name=starter_name)
        market_entry = pitcher_market_lines.get(starter_key) if starter_key else None
        mismatch = _live_pitcher_model_mismatch(pitcher_models, team_side=side, starter_name=starter_name)
        if isinstance(mismatch, dict):
            pitcher_model_mismatches.append(mismatch)
        actual_row = _lookup_boxscore_row((((actual_teams.get(side) or {}).get("boxscore") or {}).get("pitching") or []), starter_name)
        if isinstance(model_entry, dict) and isinstance(market_entry, dict):
            pitcher_ctx = _live_pitcher_matchup_context({"team_side": side}, snapshot, sim_context)
            pitcher_profile = pitcher_ctx.get("pitcher_profile") if isinstance(pitcher_ctx.get("pitcher_profile"), dict) else None
            current_profile = pitcher_ctx.get("current_profile") if isinstance(pitcher_ctx.get("current_profile"), dict) else None
            bullpen_profiles = pitcher_ctx.get("bullpen_profiles") if isinstance(pitcher_ctx.get("bullpen_profiles"), list) else []
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
                live_projection = _project_live_pitcher_value(
                    prop=prop_key,
                    team_side=side,
                    actual_value=actual_value,
                    model_mean=model_mean,
                    progress_fraction=progress_fraction,
                    actual_row=actual_row,
                    model_row=model_row,
                    pitcher_profile=pitcher_profile,
                    current_profile=current_profile,
                    bullpen_profiles=bullpen_profiles,
                    snapshot=snapshot,
                )
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
                        "source": "current_market" if not pitcher_market_source or pitcher_market_source.endswith("oddsapi_pitcher_props_" + _date_slug(d) + ".json") else "market_fallback",
                        "market_source": pitcher_market_source or None,
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

    for model_entry in hitter_models.values():
        if not isinstance(model_entry, dict):
            continue
        hitter_name = _first_text(model_entry.get("name"))
        side = str(model_entry.get("team_side") or "").strip().lower()
        if not hitter_name or side not in {"away", "home"}:
            continue
        actual_row = _lookup_boxscore_row((((actual_teams.get(side) or {}).get("boxscore") or {}).get("batting") or []), hitter_name)
        player_market_lines = _market_lines_for_name(hitter_market_lines, hitter_name)
        if not isinstance(player_market_lines, dict) or not player_market_lines:
            continue
        model_row = model_entry.get("model") or {}
        for prop_key, cfg in _HITTER_LADDER_PROPS.items():
            market_key = cfg.get("market_key")
            if not market_key:
                continue
            market = player_market_lines.get(str(market_key))
            if not isinstance(market, dict):
                continue
            line_value = _safe_float(market.get("line"))
            if line_value is None:
                continue
            model_mean = _safe_float(model_row.get(str(cfg.get("mean_key"))))
            model_prob_over = _prob_over_line_from_dist(model_row.get(str(cfg.get("dist_key"))) or {}, float(line_value))
            actual_value = _live_stat_value(actual_row, {"market": "hitter_props", "prop": prop_key})
            if _live_prop_market_resolved(actual_value, line_value):
                continue
            live_projection = _project_live_hitter_value(
                prop=prop_key,
                player_name=hitter_name,
                team_side=side,
                actual_value=actual_value,
                model_mean=model_mean,
                progress_fraction=progress_fraction,
                actual_row=actual_row,
                model_row=model_row,
                snapshot=snapshot,
            )
            side_pick = _select_live_prop_side(
                model_prob_over=model_prob_over,
                live_projection=live_projection,
                line=float(line_value),
                over_odds=market.get("over_odds"),
                under_odds=market.get("under_odds"),
                min_market_edge=_LIVE_HITTER_PROP_MIN_MARKET_EDGE,
            )
            if side_pick is None:
                continue
            item = {
                "recommendation_tier": "live",
                "source": "current_market" if not hitter_market_source or hitter_market_source.endswith("oddsapi_hitter_props_" + _date_slug(d) + ".json") else "market_fallback",
                "market_source": hitter_market_source or None,
                "market": "hitter_props",
                "market_label": cfg.get("label"),
                "prop": prop_key,
                "player_name": hitter_name,
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
                "lineup_order": _safe_int(model_row.get("lineup_order")),
                "pa_mean": _safe_float(model_row.get("pa_mean")),
                "ab_mean": _safe_float(model_row.get("ab_mean")),
            }
            if not _live_hitter_prop_row_actionable(item):
                continue
            rows.append(item)

    out: List[Dict[str, Any]] = []
    current = (snapshot or {}).get("current") or {}
    count = current.get("count") or {}
    status = (snapshot or {}).get("status") or {}
    away_totals = ((((snapshot or {}).get("teams") or {}).get("away") or {}).get("totals") or {})
    home_totals = ((((snapshot or {}).get("teams") or {}).get("home") or {}).get("totals") or {})
    for row in rows:
        item = dict(row)
        item["game_pk"] = _safe_int(card.get("gamePk"))
        item["status_abstract"] = str(status.get("abstractGameState") or ((card.get("status") or {}).get("abstract") or ""))
        item["status_detailed"] = str(status.get("detailedState") or ((card.get("status") or {}).get("detailed") or ""))
        item["inning"] = _safe_int(current.get("inning"))
        item["half_inning"] = str(current.get("halfInning") or "").strip().lower() or None
        item["outs"] = _safe_int(count.get("outs"))
        item["progress_fraction"] = _safe_float(progress_fraction)
        item["score_away"] = _safe_int(away_totals.get("R"))
        item["score_home"] = _safe_int(home_totals.get("R"))
        item["live_text"] = _live_matchup_text(snapshot)
        out.append(item)
    ranked_rows = _apply_live_prop_ranking_scores(out)
    annotated_rows = [_annotate_live_prop_reason_fields(item, snapshot=snapshot, sim_context=sim_context) for item in ranked_rows]
    enriched_rows = _enrich_live_prop_rows_with_registry(annotated_rows, d, write_observation_log=write_observation_log)
    if pitcher_model_mismatches:
        if isinstance(sim_context, dict):
            sim_context["livePitcherModelMismatches"] = pitcher_model_mismatches
        for item in enriched_rows:
            item.setdefault("meta", {})
            if isinstance(item.get("meta"), dict):
                item["meta"].setdefault("livePitcherModelMismatches", pitcher_model_mismatches)
    return enriched_rows


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


def _game_lens_remaining_outs(progress: Dict[str, Any]) -> int:
    remaining = _safe_int(progress.get("remainingOuts"))
    if remaining is not None:
        return max(0, int(remaining))
    fraction = max(0.0, min(1.0, float(_safe_float(progress.get("fraction")) or 0.0)))
    return max(0, int(round((1.0 - fraction) * 54.0)))


def _game_lens_trailing_cap(progress: Dict[str, Any]) -> int:
    remaining_outs = _game_lens_remaining_outs(progress)
    return max(1, min(3, int(math.ceil(float(remaining_outs) / 12.0))))


def _game_lens_min_ml_win_prob(progress: Dict[str, Any]) -> float:
    fraction = max(0.0, min(1.0, float(_safe_float(progress.get("fraction")) or 0.0)))
    return 0.53 + (0.03 * fraction)


def _game_lens_min_ml_edge(progress: Dict[str, Any]) -> float:
    fraction = max(0.0, min(1.0, float(_safe_float(progress.get("fraction")) or 0.0)))
    return 0.015 + (0.01 * fraction)


def _game_lens_min_margin(progress: Dict[str, Any]) -> float:
    fraction = max(0.0, min(1.0, float(_safe_float(progress.get("fraction")) or 0.0)))
    return 0.6 + (0.35 * fraction)


def _game_lens_min_spread_cushion(progress: Dict[str, Any]) -> float:
    fraction = max(0.0, min(1.0, float(_safe_float(progress.get("fraction")) or 0.0)))
    return 0.75 + (0.25 * fraction)


def _game_lens_min_total_cushion(progress: Dict[str, Any]) -> float:
    fraction = max(0.0, min(1.0, float(_safe_float(progress.get("fraction")) or 0.0)))
    return 0.6 + (0.25 * fraction)


def _game_lens_score_phrase(side_margin: Optional[float], remaining_outs: int) -> str:
    margin = _safe_float(side_margin)
    if margin is None:
        return f"{int(max(0, remaining_outs))} outs left"
    if margin > 0:
        return f"ahead by {int(round(margin))} with {int(max(0, remaining_outs))} outs left"
    if margin < 0:
        return f"trailing by {int(round(abs(margin)))} with {int(max(0, remaining_outs))} outs left"
    return f"tied game with {int(max(0, remaining_outs))} outs left"


def _game_lens_moneyline_market(
    *,
    label: str,
    model_home_prob: Optional[float],
    projection_home_margin: Optional[float],
    progress: Dict[str, Any],
    actual_home: Optional[float],
    actual_away: Optional[float],
    closed: bool,
    home_odds: Any,
    away_odds: Any,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    home_prob_market = _american_odds_implied_prob(home_odds)
    away_prob_market = _american_odds_implied_prob(away_odds)
    home_prob_market, away_prob_market = _normalize_two_way_probs(home_prob_market, away_prob_market)
    out = {
        "homeOdds": home_odds,
        "awayOdds": away_odds,
        "marketHomeProb": home_prob_market,
        "pick": None,
        "edge": None,
        "reason": None,
    }
    segment_text = _game_lens_segment_result_text(label, actual_home, actual_away, closed=closed)
    reason_label = _game_lens_reason_label(label)
    home_prob = _safe_float(model_home_prob)
    home_margin = _safe_float(projection_home_margin)
    if home_prob is None or home_margin is None:
        out["reason"] = segment_text or None
        return out
    if home_prob_market is None or away_prob_market is None:
        out["reason"] = " ".join(piece for piece in [segment_text, f"No tracked {reason_label.lower()} moneyline is attached yet."] if piece).strip() or None
        return out

    current_home_margin = float(_safe_float(actual_home) or 0.0) - float(_safe_float(actual_away) or 0.0)
    selected_side = "home" if float(home_prob) >= 0.5 else "away"
    selected_prob = float(home_prob) if selected_side == "home" else (1.0 - float(home_prob))
    selected_market_prob = float(home_prob_market) if selected_side == "home" else float(away_prob_market)
    selected_projection_margin = float(home_margin) if selected_side == "home" else -float(home_margin)
    current_side_margin = current_home_margin if selected_side == "home" else -current_home_margin
    remaining_outs = _game_lens_remaining_outs(progress)
    if current_side_margin < 0 and abs(current_side_margin) > float(_game_lens_trailing_cap(progress)):
        out["reason"] = " ".join(piece for piece in [segment_text, f"The model leaned {selected_side} at {selected_prob:.1%} against a market price near {selected_market_prob:.1%}, but they were already chasing too much of the segment for the moneyline lane to stay live."] if piece).strip() or None
        return out

    if selected_prob < _game_lens_min_ml_win_prob(progress):
        out["reason"] = " ".join(piece for piece in [segment_text, f"The model edge only leaned {selected_side} to {selected_prob:.1%}, which was too thin to promote a moneyline side."] if piece).strip() or None
        return out
    if selected_projection_margin < _game_lens_min_margin(progress):
        out["reason"] = " ".join(piece for piece in [segment_text, f"The projected margin on the {reason_label.lower()} slice was only {selected_projection_margin:+.2f}, so there was not enough separation for a side."] if piece).strip() or None
        return out

    edge = float(selected_prob) - float(selected_market_prob)
    if edge < _game_lens_min_ml_edge(progress):
        out["reason"] = " ".join(piece for piece in [segment_text, f"The model leaned {selected_side} at {selected_prob:.1%} versus {selected_market_prob:.1%} in the market, but that gap was not large enough to surface a moneyline edge."] if piece).strip() or None
        return out

    out["pick"] = selected_side
    out["edge"] = round(edge, 4)
    side_label = "home" if selected_side == "home" else "away"
    out["reason"] = " ".join(
        piece
        for piece in [
            segment_text if closed else "",
            f"The model still favors the {side_label} side at {selected_prob:.1%}, with a projected margin of {selected_projection_margin:+.2f}.",
            None if closed else f"Right now they are {_game_lens_leader_text(selected_side, actual_home, actual_away, remaining_outs)}.",
            None if closed else _game_lens_bullpen_context(snapshot),
        ]
        if piece
    ).strip()
    return out


def _game_lens_spread_market(
    *,
    label: str,
    projection_home_margin: Optional[float],
    progress: Dict[str, Any],
    actual_home: Optional[float],
    actual_away: Optional[float],
    closed: bool,
    spread_line: Optional[float],
    spread_home_odds: Any,
    spread_away_odds: Any,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = {
        "homeLine": spread_line,
        "homeOdds": spread_home_odds,
        "awayOdds": spread_away_odds,
        "pick": None,
        "edge": None,
        "reason": None,
    }
    segment_text = _game_lens_segment_result_text(label, actual_home, actual_away, closed=closed)
    reason_label = _game_lens_reason_label(label)
    home_margin = _safe_float(projection_home_margin)
    home_line = _safe_float(spread_line)
    if home_margin is None:
        out["reason"] = segment_text or None
        return out
    if home_line is None:
        out["reason"] = " ".join(piece for piece in [segment_text, f"No tracked {reason_label.lower()} spread is attached yet."] if piece).strip() or None
        return out

    spread_edge = float(home_margin) + float(home_line)
    if abs(spread_edge) <= 1e-9:
        out["reason"] = " ".join(piece for piece in [segment_text, f"The projected {reason_label.lower()} margin landed almost exactly on the spread."] if piece).strip() or None
        return out

    selected_side = "home" if spread_edge > 0 else "away"
    cover_cushion = abs(float(spread_edge))
    if cover_cushion < _game_lens_min_spread_cushion(progress):
        out["reason"] = " ".join(piece for piece in [segment_text, f"The projected cover cushion was only {cover_cushion:.2f} runs, so the spread lane stayed below threshold."] if piece).strip() or None
        return out

    current_home_margin = float(_safe_float(actual_home) or 0.0) - float(_safe_float(actual_away) or 0.0)
    current_cover_margin = current_home_margin + float(home_line)
    current_side_cover = current_cover_margin if selected_side == "home" else -current_cover_margin
    remaining_outs = _game_lens_remaining_outs(progress)
    if current_side_cover < 0 and remaining_outs <= 9 and cover_cushion < 1.25:
        out["reason"] = " ".join(piece for piece in [segment_text, "The chosen side was already behind the live spread path late, and the cushion was too thin to keep the run-line play active."] if piece).strip() or None
        return out

    out["pick"] = selected_side
    out["edge"] = round(spread_edge, 3)
    side_label = "home" if selected_side == "home" else "away"
    out["reason"] = " ".join(
        piece
        for piece in [
            segment_text if closed else "",
            f"The {side_label} side still projects to cover with about {cover_cushion:.2f} runs of cushion off a {float(home_margin):+.2f} margin forecast.",
            None if closed else f"At the moment they are {_game_lens_leader_text(selected_side, actual_home, actual_away, remaining_outs)}.",
            None if closed else _game_lens_bullpen_context(snapshot),
        ]
        if piece
    ).strip()
    return out


def _game_lens_total_market(
    *,
    label: str,
    projection_total: Optional[float],
    progress: Dict[str, Any],
    actual_home: Optional[float],
    actual_away: Optional[float],
    closed: bool,
    total_line: Optional[float],
    total_over_odds: Any,
    total_under_odds: Any,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = {
        "line": total_line,
        "overOdds": total_over_odds,
        "underOdds": total_under_odds,
        "pick": None,
        "edge": None,
        "reason": None,
    }
    segment_text = _game_lens_segment_result_text(label, actual_home, actual_away, closed=closed)
    reason_label = _game_lens_reason_label(label)
    projected_total = _safe_float(projection_total)
    live_total_line = _safe_float(total_line)
    if projected_total is None:
        out["reason"] = segment_text or None
        return out
    if live_total_line is None:
        out["reason"] = " ".join(piece for piece in [segment_text, f"No tracked {reason_label.lower()} total is attached yet."] if piece).strip() or None
        return out

    total_edge = float(projected_total) - float(live_total_line)
    if abs(total_edge) < _game_lens_min_total_cushion(progress):
        out["reason"] = " ".join(piece for piece in [segment_text, f"The projected total of {float(projected_total):.2f} was too close to {float(live_total_line):.1f} to surface a totals play."] if piece).strip() or None
        return out

    selected_side = "over" if total_edge > 0 else "under"
    current_total = float(_safe_float(actual_home) or 0.0) + float(_safe_float(actual_away) or 0.0)
    remaining_outs = _game_lens_remaining_outs(progress)
    if selected_side == "under" and current_total > float(live_total_line) and remaining_outs <= 9:
        out["reason"] = " ".join(piece for piece in [segment_text, "The game had already run past the posted total too late for the under path to remain actionable."] if piece).strip() or None
        return out

    out["pick"] = selected_side
    out["edge"] = round(total_edge, 3)
    pace_text = f"{int(max(0, remaining_outs))} outs left with {current_total:.0f} runs already on the board"
    out["reason"] = " ".join(
        piece
        for piece in [
            segment_text if closed else "",
            f"The live total still leans {selected_side} because the projection sits at {float(projected_total):.2f} against {float(live_total_line):.1f}.",
            None if closed else f"There are {pace_text}.",
            None if closed else _game_lens_bullpen_context(snapshot),
        ]
        if piece
    ).strip()
    return out


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
    market_ctx = _load_game_line_market_context(d)
    path = market_ctx.get("displayPath") if isinstance(market_ctx.get("displayPath"), Path) else market_ctx.get("currentPath")
    doc = market_ctx.get("displayDoc") if isinstance(market_ctx.get("displayDoc"), dict) else _load_json_file(path)
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
        return {"fraction": 1.0, "inning": 9, "half": "final", "outs": 3, "outsRecorded": 54, "remainingOuts": 0, "label": detailed or "Final"}
    if abstract.lower() != "live":
        return {"fraction": 0.0, "inning": None, "half": None, "outs": 0, "outsRecorded": 0, "remainingOuts": 54, "label": detailed or abstract or "Pregame"}

    current = ((snapshot or {}).get("current") or {}) if isinstance(snapshot, dict) else {}
    inning = _safe_int(current.get("inning")) or 1
    half = str(current.get("halfInning") or "").strip().lower()
    outs = _safe_int(((current.get("count") or {}).get("outs"))) or 0
    outs = int(max(0, min(2, outs)))
    outs_recorded = int(max(0, ((inning - 1) * 6) + (3 if half == "bottom" else 0) + outs))
    fraction = max(0.0, min(1.0, float(outs_recorded) / 54.0))
    label = f"{half.title()} {inning}".strip() if half else f"Inning {inning}"
    return {"fraction": fraction, "inning": inning, "half": half, "outs": outs, "outsRecorded": outs_recorded, "remainingOuts": max(0, 54 - outs_recorded), "label": label}


def _project_live_value(actual_value: Optional[float], model_mean: Optional[float], progress_fraction: float) -> Optional[float]:
    mean = _safe_float(model_mean)
    if mean is None:
        return None
    actual = float(_safe_float(actual_value) or 0.0)
    progress = max(0.0, min(1.0, float(progress_fraction or 0.0)))
    expected_to_date = float(mean) * progress
    remaining = max(float(mean) - expected_to_date, 0.0)
    return round(actual + remaining, 3)


def _current_batting_side(snapshot: Optional[Dict[str, Any]]) -> Optional[str]:
    half = str((((snapshot or {}).get("current") or {}).get("halfInning") or "")).strip().lower()
    if half == "top":
        return "away"
    if half == "bottom":
        return "home"
    return None


def _lineup_slot_from_snapshot(snapshot: Optional[Dict[str, Any]], side: str, player_name: str) -> Optional[int]:
    lineup_rows = ((((snapshot or {}).get("teams") or {}).get(side) or {}).get("lineup") or [])
    if not isinstance(lineup_rows, list):
        return None
    target_name = normalize_pitcher_name(player_name)
    if not target_name:
        return None
    for row in lineup_rows:
        if not isinstance(row, dict):
            continue
        if normalize_pitcher_name(str(row.get("name") or "")) != target_name:
            continue
        order_value = _safe_int(row.get("order"))
        if order_value is None:
            return None
        if int(order_value) >= 100:
            return max(1, int(order_value) // 100)
        return max(1, int(order_value))
    return None


def _current_batter_slot(snapshot: Optional[Dict[str, Any]], side: str) -> Optional[int]:
    current_batting_side = _current_batting_side(snapshot)
    if current_batting_side != side:
        return None
    batter = (((snapshot or {}).get("current") or {}).get("batter") or {}) if isinstance(snapshot, dict) else {}
    batter_name = _first_text(batter.get("fullName"), batter.get("name"))
    return _lineup_slot_from_snapshot(snapshot, side, batter_name)


def _live_hitter_actual_pa(actual_row: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(actual_row, dict):
        return None
    ab = _safe_float(actual_row.get("AB"))
    bb = _safe_float(actual_row.get("BB"))
    hbp = _safe_float(actual_row.get("HBP"))
    sf = _safe_float(actual_row.get("SF"))
    sh = _safe_float(actual_row.get("SH"))
    components = [value for value in (ab, bb, hbp, sf, sh) if value is not None]
    if not components:
        return None
    return float(sum(float(value) for value in components))


def _project_live_hitter_value(
    *,
    prop: str,
    player_name: str,
    team_side: str,
    actual_value: Optional[float],
    model_mean: Optional[float],
    progress_fraction: float,
    actual_row: Optional[Dict[str, Any]] = None,
    model_row: Optional[Dict[str, Any]] = None,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    mean = _safe_float(model_mean)
    if mean is None:
        return None
    if not isinstance(model_row, dict) or team_side not in {"away", "home"}:
        return _project_live_value(actual_value, model_mean, progress_fraction)

    actual = float(_safe_float(actual_value) or 0.0)
    prop_key = str(prop or "").strip().lower()
    pa_mean = _safe_float(model_row.get("pa_mean"))
    ab_mean = _safe_float(model_row.get("ab_mean"))
    actual_pa = _live_hitter_actual_pa(actual_row)
    actual_ab = _safe_float((actual_row or {}).get("AB"))

    use_ab_opportunity = prop_key in {"hits", "home_runs", "total_bases", "doubles", "triples"}
    opportunity_mean = ab_mean if use_ab_opportunity else pa_mean
    actual_opportunity = actual_ab if use_ab_opportunity else actual_pa
    if opportunity_mean is None or float(opportunity_mean) <= 0.0:
        return _project_live_value(actual_value, model_mean, progress_fraction)

    remaining_opportunity = max(float(opportunity_mean) - float(actual_opportunity or 0.0), 0.0)
    lineup_slot = _safe_int(model_row.get("lineup_order"))
    current_batting_side = _current_batting_side(snapshot)
    current_slot = _current_batter_slot(snapshot, team_side)
    if current_batting_side == team_side:
        if current_slot is not None and lineup_slot is not None:
            normalized_player_slot = max(1, int(lineup_slot))
            steps_until = (normalized_player_slot - int(current_slot)) % 9
            if steps_until == 0:
                remaining_opportunity = max(remaining_opportunity, 0.6 if use_ab_opportunity else 0.8)
            elif steps_until == 1:
                remaining_opportunity += 0.2
            elif steps_until == 2:
                remaining_opportunity += 0.1
        elif normalize_pitcher_name(player_name) == normalize_pitcher_name(_first_text((((snapshot or {}).get("current") or {}).get("batter") or {}).get("fullName"))):
            remaining_opportunity = max(remaining_opportunity, 0.6 if use_ab_opportunity else 0.8)

    current = (snapshot or {}).get("current") if isinstance(snapshot, dict) else {}
    inning = _safe_int(current.get("inning")) or 0
    away_score = _safe_float((((snapshot or {}).get("teams") or {}).get("away") or {}).get("totals", {}).get("R"))
    home_score = _safe_float((((snapshot or {}).get("teams") or {}).get("home") or {}).get("totals", {}).get("R"))
    if inning >= 9 and away_score is not None and home_score is not None:
        if team_side == "home" and current_batting_side == "top" and float(home_score) > float(away_score):
            remaining_opportunity = 0.0
        if team_side == "away" and current_batting_side == "bottom" and abs(float(home_score) - float(away_score)) > 1e-9:
            remaining_opportunity = 0.0

    rate = float(mean) / float(max(float(opportunity_mean), 1e-6))
    projection = float(actual) + max(0.0, float(remaining_opportunity)) * float(rate)
    return round(max(float(actual), projection), 3)


def _model_pitcher_stat(model_row: Optional[Dict[str, Any]], *keys: str) -> Optional[float]:
    if not isinstance(model_row, dict):
        return None
    for key in keys:
        value = _safe_float(model_row.get(key))
        if value is not None:
            return float(value)
    return None


def _project_live_pitcher_value(
    *,
    prop: str,
    team_side: str,
    actual_value: Optional[float],
    model_mean: Optional[float],
    progress_fraction: float,
    actual_row: Optional[Dict[str, Any]] = None,
    model_row: Optional[Dict[str, Any]] = None,
    pitcher_profile: Optional[Dict[str, Any]] = None,
    current_profile: Optional[Dict[str, Any]] = None,
    bullpen_profiles: Optional[List[Dict[str, Any]]] = None,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    prop_key = str(prop or "").strip().lower()
    mean = _safe_float(model_mean)
    if mean is None or prop_key not in {"outs", "strikeouts"}:
        return _project_live_value(actual_value, model_mean, progress_fraction)

    actual = float(_safe_float(actual_value) or 0.0)
    if not isinstance(actual_row, dict):
        return _project_live_value(actual_value, model_mean, progress_fraction)

    actual_bf = _safe_float(actual_row.get("BF"))
    actual_pitches = _safe_float(actual_row.get("P"))
    if actual_bf is None and actual_pitches is None:
        return _project_live_value(actual_value, model_mean, progress_fraction)

    model_bf = _model_pitcher_stat(model_row, "BF", "batters_faced_mean")
    model_pitches = _model_pitcher_stat(model_row, "P", "pitches_mean")
    if model_bf is None or model_bf <= 0.0:
        return _project_live_value(actual_value, model_mean, progress_fraction)

    remaining_bf = None
    if actual_bf is not None:
        remaining_bf = max(float(model_bf) - float(actual_bf), 0.0)

    pitches_per_bf = None
    if model_pitches is not None and float(model_pitches) > 0.0:
        pitches_per_bf = float(model_pitches) / float(max(model_bf, 1e-6))

    workload_limit = float(model_pitches) if model_pitches is not None else None
    stamina = _safe_float((pitcher_profile or {}).get("stamina_pitches")) if isinstance(pitcher_profile, dict) else None
    if stamina is not None:
        workload_limit = float(stamina) if workload_limit is None else min(float(workload_limit), float(stamina))

    if pitches_per_bf is not None and actual_pitches is not None:
        if workload_limit is not None:
            pitch_headroom = max(float(workload_limit) - float(actual_pitches), 0.0)
            bf_from_pitch_budget = float(pitch_headroom) / float(max(pitches_per_bf, 1e-6))
            bf_from_pitch_budget = max(0.0, bf_from_pitch_budget + 0.75)
            remaining_bf = bf_from_pitch_budget if remaining_bf is None else min(float(remaining_bf), bf_from_pitch_budget)

        if actual_bf is not None and float(actual_bf) > 0.0:
            actual_pitches_per_bf = float(actual_pitches) / float(actual_bf)
            pace_ratio = float(actual_pitches_per_bf) / float(max(pitches_per_bf, 1e-6))
            if remaining_bf is not None and pace_ratio > 1.05:
                remaining_bf = max(0.0, float(remaining_bf) / min(float(pace_ratio), 1.35))

    if actual_bf is not None and float(actual_bf) >= 21.0 and remaining_bf is not None:
        remaining_bf = min(float(remaining_bf), 3.5)

    if remaining_bf is None:
        return _project_live_value(actual_value, model_mean, progress_fraction)

    hook_factor = 1.0
    if actual_pitches is not None and workload_limit is not None and float(workload_limit) > 0.0:
        usage_ratio = float(actual_pitches) / float(workload_limit)
        if usage_ratio >= 0.75:
            hook_factor *= max(0.35, 1.0 - ((usage_ratio - 0.75) * 1.45))
    if actual_bf is not None:
        if float(actual_bf) >= 18.0:
            hook_factor *= 0.92
        if float(actual_bf) >= 21.0:
            hook_factor *= 0.78
        if float(actual_bf) >= 24.0:
            hook_factor *= 0.72

    progress = _live_game_progress(snapshot)
    inning = _safe_int(progress.get("inning")) or 0
    remaining_outs = _safe_int(progress.get("remainingOuts")) or 0
    if inning >= 6:
        hook_factor *= 0.93
    if inning >= 7:
        hook_factor *= 0.84
    if remaining_outs <= 9:
        hook_factor *= 0.88

    team_score = _safe_float((((snapshot or {}).get("teams") or {}).get(team_side) or {}).get("totals", {}).get("R")) if team_side in {"away", "home"} else None
    opp_side = "home" if team_side == "away" else ("away" if team_side == "home" else "")
    opp_score = _safe_float((((snapshot or {}).get("teams") or {}).get(opp_side) or {}).get("totals", {}).get("R")) if opp_side else None
    if team_score is not None and opp_score is not None:
        margin = float(team_score) - float(opp_score)
        if margin <= -4.0:
            hook_factor *= 0.72
        elif margin <= -2.0 and inning >= 5:
            hook_factor *= 0.84
        elif margin >= 4.0 and inning >= 6:
            hook_factor *= 0.86

    bullpen_availability = []
    bullpen_leverage = []
    for profile in bullpen_profiles or []:
        if not isinstance(profile, dict):
            continue
        avail = _safe_float(profile.get("availability_mult"))
        lev = _safe_float(profile.get("leverage_skill"))
        if avail is not None:
            bullpen_availability.append(float(avail))
        if lev is not None:
            bullpen_leverage.append(float(lev))
    if bullpen_availability:
        avg_avail = sum(bullpen_availability) / float(len(bullpen_availability))
        avg_lev = sum(bullpen_leverage) / float(len(bullpen_leverage)) if bullpen_leverage else 0.5
        if avg_avail >= 0.92 and avg_lev >= 0.58 and inning >= 6:
            hook_factor *= 0.9
        elif avg_avail <= 0.78:
            hook_factor *= 1.06

    if isinstance(current_profile, dict):
        current_id = _safe_int(current_profile.get("id"))
        starter_id = _safe_int((pitcher_profile or {}).get("id")) if isinstance(pitcher_profile, dict) else None
        if current_id is not None and starter_id is not None and int(current_id) != int(starter_id):
            hook_factor = 0.0

    remaining_bf = max(0.0, float(remaining_bf) * max(0.0, min(1.1, float(hook_factor))))

    per_bf_rate = float(mean) / float(max(model_bf, 1e-6))
    if prop_key == "strikeouts" and actual_bf is not None and float(actual_bf) >= 3.0:
        actual_k_rate = float(actual) / float(max(float(actual_bf), 1e-6))
        weight = min(0.55, max(0.12, float(actual_bf) / 36.0))
        per_bf_rate = ((1.0 - weight) * float(per_bf_rate)) + (weight * float(actual_k_rate))
    projection = float(actual) + max(0.0, float(remaining_bf)) * float(per_bf_rate)
    return round(max(float(actual), projection), 3)


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
    feed: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    artifacts = artifacts or _load_cards_artifacts(d)
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
        archive = archive or _load_cards_archive_context(d)
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
    roster_snapshot: Optional[Dict[str, Any]] = None
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
                    roster_snapshot = roster_obj
                    player_meta.update(_player_meta_from_roster_snapshot(roster_obj))
        except Exception:
            pass
    else:
        player_meta.update(_report_player_meta(_season_report_game(archive.get("report"), int(game_pk))))

    name_lookup: Dict[int, str] = {}
    try:
        game_feed = feed if isinstance(feed, dict) and feed else _load_live_lens_feed(int(game_pk), d)
        if isinstance(game_feed, dict) and game_feed:
            feed = game_feed
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
        "roster_snapshot": roster_snapshot,
        "simCount": sim_count,
        "away": sim_obj.get("away") or {},
        "home": sim_obj.get("home") or {},
        "predicted": _sim_predicted_score(sim_obj),
        "segments": sim_data.get("segments") or {},
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
    status = (snapshot.get("status") or {}) if isinstance(snapshot.get("status"), dict) else {}
    if _status_is_final(status) or (not _status_is_live(status)):
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


def _load_live_lens_cards(
    d: str,
    *,
    artifacts: Optional[Dict[str, Any]] = None,
    archive: Optional[Dict[str, Any]] = None,
    schedule_games: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    artifacts = artifacts or _load_cards_artifacts(d)
    archive = archive or (_load_cards_archive_context(d) if _should_load_cards_archive_context(d, artifacts) else {})

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

    if not isinstance(schedule_games, list):
        schedule_games = _schedule_games_for_date(d)

    return _cards_list_from_sources(
        d=d,
        schedule_games=schedule_games,
        outputs_by_game=outputs_by_game,
        recos_by_game=recos_by_game,
    )
def _load_live_lens_snapshot(game_pk: int, d: str, *, feed: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        game_feed = feed if isinstance(feed, dict) and feed else _load_live_lens_feed(int(game_pk), d)
        if isinstance(game_feed, dict) and game_feed:
            feed = game_feed
        if not isinstance(feed, dict) or not feed:
            return None
        away_sp = _get_box_starting_pitcher_id(feed, "away")
        home_sp = _get_box_starting_pitcher_id(feed, "home")
        return {
            "gamePk": int(game_pk),
            "status": (feed.get("gameData") or {}).get("status") or {},
            "current": _current_matchup(feed),
            "offense": _live_offense_state(feed),
            "linescore": _live_linescore(feed),
            "teams": {
                "away": {
                    "starter": {"id": away_sp, "name": _player_name_from_box(feed, away_sp) if away_sp else ""},
                    "lineup": _lineup_from_box(feed, "away"),
                    "totals": _team_totals(feed, "away"),
                    "boxscore": {
                        "batting": _boxscore_batting(feed, "away"),
                        "pitching": _boxscore_pitching(feed, "away"),
                    },
                },
                "home": {
                    "starter": {"id": home_sp, "name": _player_name_from_box(feed, home_sp) if home_sp else ""},
                    "lineup": _lineup_from_box(feed, "home"),
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


def _live_base_state(snapshot: Optional[Dict[str, Any]]) -> Tuple[BaseState, int, int, int]:
    offense = (snapshot or {}).get("offense") if isinstance(snapshot, dict) else {}
    first_id = _safe_int(((offense.get("first") or {}).get("id")) if isinstance(offense, dict) else None) or 0
    second_id = _safe_int(((offense.get("second") or {}).get("id")) if isinstance(offense, dict) else None) or 0
    third_id = _safe_int(((offense.get("third") or {}).get("id")) if isinstance(offense, dict) else None) or 0
    if first_id and second_id and third_id:
        bases = BaseState.LOADED
    elif first_id and second_id:
        bases = BaseState.FIRST_SECOND
    elif first_id and third_id:
        bases = BaseState.FIRST_THIRD
    elif second_id and third_id:
        bases = BaseState.SECOND_THIRD
    elif first_id:
        bases = BaseState.FIRST
    elif second_id:
        bases = BaseState.SECOND
    elif third_id:
        bases = BaseState.THIRD
    else:
        bases = BaseState.EMPTY
    return bases, int(first_id), int(second_id), int(third_id)


def _live_team_next_batter_index(snapshot: Optional[Dict[str, Any]], roster: Any, side: str, *, current_batter_id: Optional[int] = None, batting_side: Optional[str] = None) -> int:
    batters = list(getattr(getattr(roster, "lineup", None), "batters", []) or [])
    if not batters:
        return 0

    if batting_side == side and current_batter_id is not None:
        for idx, batter in enumerate(batters):
            try:
                if int(getattr(getattr(batter, "player", None), "mlbam_id", 0) or 0) == int(current_batter_id):
                    return idx
            except Exception:
                continue
        lineup_rows = ((((snapshot or {}).get("teams") or {}).get(side) or {}).get("lineup") or [])
        if isinstance(lineup_rows, list):
            for row in lineup_rows:
                if not isinstance(row, dict):
                    continue
                if int(_safe_int(row.get("id")) or 0) != int(current_batter_id):
                    continue
                order_value = _safe_int(row.get("order"))
                if order_value is not None:
                    slot = max(0, int(order_value) // 100 - 1)
                    return slot % len(batters)

    batting_rows = (((((snapshot or {}).get("teams") or {}).get(side) or {}).get("boxscore") or {}).get("batting") or [])
    approx_pa_total = 0
    if isinstance(batting_rows, list):
        for row in batting_rows:
            if not isinstance(row, dict):
                continue
            approx_pa_total += int(_safe_int(row.get("AB")) or 0)
            approx_pa_total += int(_safe_int(row.get("BB")) or 0)
            approx_pa_total += int(_safe_int(row.get("HBP")) or 0)
    return int(max(0, approx_pa_total)) % len(batters)


def _live_pitcher_usage(snapshot: Optional[Dict[str, Any]]) -> Tuple[Dict[int, int], Dict[int, int]]:
    pitch_counts: Dict[int, int] = {}
    batters_faced: Dict[int, int] = {}
    teams = ((snapshot or {}).get("teams") or {}) if isinstance(snapshot, dict) else {}
    for side in ("away", "home"):
        rows = ((((teams.get(side) or {}).get("boxscore") or {}).get("pitching")) or [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            pid = _safe_int(row.get("id"))
            if pid is None or int(pid) <= 0:
                continue
            pitch_counts[int(pid)] = int(_safe_int(row.get("P")) or 0)
            batters_faced[int(pid)] = int(_safe_int(row.get("BF")) or 0)
    return pitch_counts, batters_faced


def _snapshot_batter_to_roster_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "player": {
            "mlbam_id": int(_safe_int(row.get("id")) or 0),
            "full_name": str(row.get("name") or ""),
            "primary_position": str(row.get("pos") or ""),
            "bat_side": str(row.get("bat") or "R"),
            "throw_side": str(row.get("throw") or "R"),
        }
    }
    for key in (
        "k_rate",
        "bb_rate",
        "hbp_rate",
        "hr_rate",
        "inplay_hit_rate",
        "xb_hit_share",
        "triple_share_of_xb",
        "sb_attempt_rate",
        "sb_success_rate",
        "bb_gb_rate",
        "bb_fb_rate",
        "bb_ld_rate",
        "bb_pu_rate",
        "bb_inplay_n",
    ):
        if key in row:
            out[key] = row.get(key)
    for key in (
        "vs_pitch_type",
        "platoon_mult_vs_lhp",
        "platoon_mult_vs_rhp",
        "venue_mult_home",
        "venue_mult_away",
        "statcast_quality_mult",
        "vs_pitcher_hr_mult",
        "vs_pitcher_k_mult",
        "vs_pitcher_bb_mult",
        "vs_pitcher_inplay_mult",
        "vs_pitcher_history",
    ):
        if isinstance(row.get(key), dict):
            out[key] = dict(row.get(key) or {})
    return out


def _snapshot_pitcher_to_roster_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "player": {
            "mlbam_id": int(_safe_int(row.get("id")) or 0),
            "full_name": str(row.get("name") or ""),
            "primary_position": "P",
            "bat_side": "R",
            "throw_side": str(row.get("throw") or "R"),
        },
        "role": str(row.get("role") or "RP"),
    }
    for key in (
        "k_rate",
        "bb_rate",
        "hbp_rate",
        "hr_rate",
        "inplay_hit_rate",
        "batters_faced",
        "balls_in_play",
        "availability_mult",
        "bb_gb_rate",
        "bb_fb_rate",
        "bb_ld_rate",
        "bb_pu_rate",
        "leverage_skill",
        "stamina_pitches",
        "statcast_splits_n_pitches",
        "arsenal_sample_size",
        "bb_inplay_n",
    ):
        if key in row:
            out[key] = row.get(key)
    for key in (
        "arsenal_source",
        "statcast_splits_source",
        "statcast_splits_start_date",
        "statcast_splits_end_date",
    ):
        if key in row:
            out[key] = row.get(key)
    for key in (
        "arsenal",
        "pitch_type_whiff_mult",
        "pitch_type_inplay_mult",
        "platoon_mult_vs_lhb",
        "platoon_mult_vs_rhb",
        "venue_mult_home",
        "venue_mult_away",
        "statcast_quality_mult",
    ):
        if isinstance(row.get(key), dict):
            out[key] = dict(row.get(key) or {})
    return out


def _snapshot_side_to_roster_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    starter_profile = doc.get("starter_profile") if isinstance(doc.get("starter_profile"), dict) else {}
    bullpen_profiles = [row for row in (doc.get("bullpen_profiles") or []) if isinstance(row, dict)]
    lineup_rows = [row for row in (doc.get("lineup") or []) if isinstance(row, dict)]
    bench_rows = [row for row in (doc.get("bench") or []) if isinstance(row, dict)]
    team = doc.get("team") if isinstance(doc.get("team"), dict) else {}
    manager = doc.get("manager") if isinstance(doc.get("manager"), dict) else {}
    return {
        "schema_version": 4,
        "team": {
            "team_id": int(_safe_int(team.get("team_id")) or 0),
            "name": str(team.get("name") or ""),
            "abbreviation": str(team.get("abbreviation") or ""),
        },
        "manager": dict(manager),
        "lineup": {
            "batters": [_snapshot_batter_to_roster_dict(row) for row in lineup_rows],
            "pitcher": _snapshot_pitcher_to_roster_dict(starter_profile),
            "bench": [_snapshot_batter_to_roster_dict(row) for row in bench_rows],
            "bullpen": [_snapshot_pitcher_to_roster_dict(row) for row in bullpen_profiles],
        },
    }


def _roster_from_snapshot_side(doc: Optional[Dict[str, Any]]) -> Any:
    if not isinstance(doc, dict):
        raise ValueError("missing_roster_doc")
    try:
        return roster_from_dict(doc)
    except Exception:
        return roster_from_dict(_snapshot_side_to_roster_doc(doc))


def _live_mc_projection(snapshot: Optional[Dict[str, Any]], sim_context: Optional[Dict[str, Any]], *, date_str: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(snapshot, dict) or not isinstance(sim_context, dict):
        return None
    if not sim_context.get("found"):
        return None
    status = (snapshot.get("status") or {}) if isinstance(snapshot.get("status"), dict) else {}
    if not _status_is_live(status):
        return None

    roster_snapshot = sim_context.get("roster_snapshot") if isinstance(sim_context.get("roster_snapshot"), dict) else None
    if not isinstance(roster_snapshot, dict):
        return None
    away_doc = roster_snapshot.get("away") if isinstance(roster_snapshot.get("away"), dict) else None
    home_doc = roster_snapshot.get("home") if isinstance(roster_snapshot.get("home"), dict) else None
    if not isinstance(away_doc, dict) or not isinstance(home_doc, dict):
        return None

    try:
        away_roster = _roster_from_snapshot_side(away_doc)
        home_roster = _roster_from_snapshot_side(home_doc)
    except Exception:
        return None

    current = snapshot.get("current") if isinstance(snapshot.get("current"), dict) else {}
    inning = _safe_int(current.get("inning"))
    half = str(current.get("halfInning") or "").strip().lower()
    outs = _safe_int(((current.get("count") or {}).get("outs")))
    if inning is None or half not in {"top", "bottom"} or outs is None:
        return None

    away_score = _safe_int((((snapshot.get("teams") or {}).get("away") or {}).get("totals") or {}).get("R"))
    home_score = _safe_int((((snapshot.get("teams") or {}).get("home") or {}).get("totals") or {}).get("R"))
    if away_score is None or home_score is None:
        return None

    batting_side = "away" if half == "top" else "home"
    fielding_side = "home" if batting_side == "away" else "away"
    current_batter_id = _safe_int(((current.get("batter") or {}).get("id")))
    current_pitcher_id = _safe_int(((current.get("pitcher") or {}).get("id")))
    away_next_index = _live_team_next_batter_index(snapshot, away_roster, "away", current_batter_id=current_batter_id, batting_side=batting_side)
    home_next_index = _live_team_next_batter_index(snapshot, home_roster, "home", current_batter_id=current_batter_id, batting_side=batting_side)
    pitch_counts, batters_faced = _live_pitcher_usage(snapshot)
    bases, runner_on_1b, runner_on_2b, runner_on_3b = _live_base_state(snapshot)

    try:
        result = estimate_live(
            away_roster,
            home_roster,
            LiveSituation(
                inning=int(inning),
                top=(batting_side == "away"),
                outs=int(outs),
                bases=bases,
                away_score=int(away_score),
                home_score=int(home_score),
                runner_on_1b=int(runner_on_1b),
                runner_on_2b=int(runner_on_2b),
                runner_on_3b=int(runner_on_3b),
                away_next_batter_index=int(away_next_index),
                home_next_batter_index=int(home_next_index),
                away_pitcher_id=int(current_pitcher_id) if fielding_side == "away" and current_pitcher_id is not None else None,
                home_pitcher_id=int(current_pitcher_id) if fielding_side == "home" and current_pitcher_id is not None else None,
                pitcher_pitch_count=pitch_counts,
                pitcher_batters_faced=batters_faced,
            ),
            sims=int(_LIVE_GAME_MC_SIMS),
            seed=int(_safe_int(sim_context.get("gamePk")) or 0) or None,
            cfg_kwargs=_forward_live_cfg_kwargs(date_str),
        )
    except Exception:
        return None

    return {
        "away": round(float(result.avg_away_runs), 2),
        "home": round(float(result.avg_home_runs), 2),
        "total": round(float(result.avg_total_runs), 2),
        "homeMargin": round(float(result.avg_home_runs) - float(result.avg_away_runs), 2),
        "homeWinProb": round(float(result.home_win_prob), 4),
        "awayWinProb": round(float(result.away_win_prob), 4),
        "closed": False,
        "source": "live_mc",
    }


def _build_game_lens(card: Dict[str, Any], snapshot: Optional[Dict[str, Any]], sim_context: Optional[Dict[str, Any]], market_row: Optional[Dict[str, Any]], *, date_str: Optional[str] = None) -> List[Dict[str, Any]]:
    predicted = (sim_context or {}).get("predicted") or {}
    pregame_away = _safe_float(predicted.get("away"))
    pregame_home = _safe_float(predicted.get("home"))
    away_score = _safe_float((((snapshot or {}).get("teams") or {}).get("away") or {}).get("totals", {}).get("R"))
    home_score = _safe_float((((snapshot or {}).get("teams") or {}).get("home") or {}).get("totals", {}).get("R"))
    progress = _live_game_progress(snapshot, card)
    live_mc_projection = _live_mc_projection(snapshot, sim_context, date_str=date_str)
    predictions = card.get("predictions") or {}
    markets = (market_row or {}).get("markets") or {}

    lanes = [
        {"key": "live", "label": progress.get("label") or "Live", "innings": 9, "baseline": False},
        {"key": "first1", "label": "F1", "innings": 1, "baseline": True},
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
        if lane["key"] in {"live", "full"} and isinstance(live_mc_projection, dict):
            projection = {
                "away": _safe_float(live_mc_projection.get("away")),
                "home": _safe_float(live_mc_projection.get("home")),
                "total": _safe_float(live_mc_projection.get("total")),
                "homeMargin": _safe_float(live_mc_projection.get("homeMargin")),
                "closed": False,
            }
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
        if lane["key"] in {"live", "full"} and isinstance(live_mc_projection, dict):
            model_home_prob = _safe_float(live_mc_projection.get("homeWinProb"))

        lane_markets = _game_lens_markets_for_lane(markets, str(lane["key"]))
        h2h = lane_markets.get("h2h") if isinstance(lane_markets.get("h2h"), dict) else {}
        spreads = lane_markets.get("spreads") if isinstance(lane_markets.get("spreads"), dict) else {}
        totals = lane_markets.get("totals") if isinstance(lane_markets.get("totals"), dict) else {}
        segment_actual = _game_lens_actual_segment(snapshot, int(lane["innings"]))

        home_odds = h2h.get("home_odds") or h2h.get("homeOdds")
        away_odds = h2h.get("away_odds") or h2h.get("awayOdds")
        spread_line = _safe_float(spreads.get("home_line") or spreads.get("homeLine"))
        spread_home_odds = spreads.get("home_odds") or spreads.get("homeOdds")
        spread_away_odds = spreads.get("away_odds") or spreads.get("awayOdds")
        total_line = _safe_float(totals.get("line"))
        total_over_odds = totals.get("over_odds") or totals.get("overOdds")
        total_under_odds = totals.get("under_odds") or totals.get("underOdds")

        moneyline_market = _game_lens_moneyline_market(
            label=str(lane["label"]),
            model_home_prob=model_home_prob,
            projection_home_margin=projection.get("homeMargin"),
            progress=progress,
            actual_home=segment_actual.get("home"),
            actual_away=segment_actual.get("away"),
            closed=bool(projection.get("closed")),
            home_odds=home_odds,
            away_odds=away_odds,
            snapshot=snapshot,
        )
        spread_market = _game_lens_spread_market(
            label=str(lane["label"]),
            projection_home_margin=projection.get("homeMargin"),
            progress=progress,
            actual_home=segment_actual.get("home"),
            actual_away=segment_actual.get("away"),
            closed=bool(projection.get("closed")),
            spread_line=spread_line,
            spread_home_odds=spread_home_odds,
            spread_away_odds=spread_away_odds,
            snapshot=snapshot,
        )
        total_market = _game_lens_total_market(
            label=str(lane["label"]),
            projection_total=projection.get("total"),
            progress=progress,
            actual_home=segment_actual.get("home"),
            actual_away=segment_actual.get("away"),
            closed=bool(projection.get("closed")),
            total_line=total_line,
            total_over_odds=total_over_odds,
            total_under_odds=total_under_odds,
            snapshot=snapshot,
        )

        rows.append(
            {
                "key": lane["key"],
                "label": lane["label"],
                "closed": bool(projection.get("closed")),
                "projection": projection,
                "actualSegment": segment_actual,
                "progress": progress,
                "baselineHomeWinProb": baseline_home_prob,
                "modelHomeWinProb": model_home_prob,
                "source": str(live_mc_projection.get("source") or "live_projection") if lane["key"] in {"live", "full"} and isinstance(live_mc_projection, dict) else ("live_projection" if lane["key"] == "live" else "segment_projection"),
                "markets": {
                    "moneyline": moneyline_market,
                    "spread": spread_market,
                    "total": total_market,
                },
            }
        )
    return rows


def _prop_lens_rows(card: Dict[str, Any], snapshot: Optional[Dict[str, Any]], sim_context: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    status_text = ((snapshot or {}).get("status") or {}).get("abstractGameState") or ((card.get("status") or {}).get("abstract") or "")
    progress_fraction = float((_live_game_progress(snapshot, card).get("fraction") or 0.0))
    rows: List[Dict[str, Any]] = []
    actual_teams = ((snapshot or {}).get("teams") or {})
    for key, tier in (("pitcherProps", "official"), ("extraPitcherProps", "playable")):
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
            if is_pitcher and side in {"away", "home"} and _starter_removed_from_snapshot(snapshot, str(side)):
                continue
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
            pitcher_profile = None
            if is_pitcher:
                pitcher_ctx = _live_pitcher_matchup_context({"team_side": side}, snapshot, sim_context)
                if isinstance(pitcher_ctx.get("pitcher_profile"), dict):
                    pitcher_profile = pitcher_ctx.get("pitcher_profile")
                current_profile = pitcher_ctx.get("current_profile") if isinstance(pitcher_ctx.get("current_profile"), dict) else None
                bullpen_profiles = pitcher_ctx.get("bullpen_profiles") if isinstance(pitcher_ctx.get("bullpen_profiles"), list) else []
            live_projection = _project_live_pitcher_value(
                prop=str(reco.get("prop") or ""),
                team_side=str(side or ""),
                actual_value=actual_value,
                model_mean=model_mean,
                progress_fraction=progress_fraction,
                actual_row=actual_row,
                model_row=sim_row,
                pitcher_profile=pitcher_profile,
                current_profile=current_profile,
                bullpen_profiles=bullpen_profiles,
                snapshot=snapshot,
            ) if is_pitcher else _project_live_hitter_value(
                prop=str(reco.get("prop") or ""),
                player_name=owner_name,
                team_side=str(side or ""),
                actual_value=actual_value,
                model_mean=model_mean,
                progress_fraction=progress_fraction,
                actual_row=actual_row,
                model_row=sim_row,
                snapshot=snapshot,
            )
            market_line = _safe_float(reco.get("market_line"))
            selection = str(reco.get("selection") or "").strip().lower()
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
                    "liveEdge": _selection_live_edge(selection, live_projection, market_line),
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


def _normalize_live_lens_live_prop_row(row: Dict[str, Any], snapshot: Optional[Dict[str, Any]], card: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    status_text = ((snapshot or {}).get("status") or {}).get("abstractGameState") or (((card or {}).get("status") or {}).get("abstract") or "")
    actual_value = _safe_float(row.get("actual_value"))
    if actual_value is None:
        actual_value = _safe_float(row.get("actual"))
    model_mean = _safe_float(row.get("model_mean"))
    market_line = _safe_float(row.get("market_line"))
    live_projection = _safe_float(row.get("live_projection"))
    live_edge = _safe_float(row.get("live_edge"))
    return {
        "tier": "live",
        "source": row.get("source") or row.get("recommendation_tier") or "live",
        "playerName": _prop_owner_name(row),
        "teamSide": row.get("team_side"),
        "selection": row.get("selection"),
        "line": market_line,
        "actual": actual_value,
        "modelMean": model_mean,
        "liveProjection": live_projection,
        "liveEdge": live_edge,
        "delta": (float(actual_value) - float(market_line)) if actual_value is not None and market_line is not None else None,
        "status": _prop_result_state(
            {
                "market": row.get("market"),
                "prop": row.get("prop"),
                "selection": row.get("selection"),
                "market_line": market_line,
            },
            actual_value,
            status_text,
        ),
        "edge": _safe_float(row.get("edge")),
        "odds": _safe_int(row.get("odds")),
        "modelProbOver": _safe_float(row.get("model_prob_over")),
        "outsMean": _safe_float(row.get("outs_mean")),
        "marketLabel": row.get("market_label") or row.get("prop") or row.get("market"),
        "reason_summary": str(row.get("reason_summary") or "").strip(),
        "reasons": [
            str(reason).strip()
            for reason in (row.get("reasons") or [])
            if str(reason).strip()
        ],
        "firstSeenAt": row.get("first_seen_at"),
        "lastSeenAt": row.get("last_seen_at"),
        "firstSeenLine": _safe_float(row.get("first_seen_line")),
        "firstSeenLiveProjection": _safe_float(row.get("first_seen_live_projection")),
        "firstSeenLiveEdge": _safe_float(row.get("first_seen_live_edge")),
        "seenCount": _safe_int(row.get("seen_count")),
        "estimatedWinProb": _safe_float(row.get("estimated_win_prob")),
        "rankingScore": _safe_float(row.get("ranking_score")),
    }


def _live_lens_payload(d: str, *, persist: bool = False, refresh_markets: bool = False) -> Dict[str, Any]:
    started_at = time.perf_counter()
    market_refresh_started_at = time.perf_counter()
    markets_refreshed = _maybe_refresh_live_oddsapi_markets(d) if refresh_markets else False
    market_refresh_ms = round((time.perf_counter() - market_refresh_started_at) * 1000.0, 1)
    artifacts = _load_cards_artifacts(d)
    archive = _load_cards_archive_context(d) if _should_load_cards_archive_context(d, artifacts) else {}
    schedule_games = _schedule_games_for_date(d)
    cards = _load_live_lens_cards(d, artifacts=artifacts, archive=archive, schedule_games=schedule_games)
    game_line_index = _load_game_line_market_index(d)
    feed_cache: Dict[int, Optional[Dict[str, Any]]] = {}
    games_out: List[Dict[str, Any]] = []
    counts = {
        "games": 0,
        "live": 0,
        "final": 0,
        "pregame": 0,
        "props": 0,
        "archivedLiveProps": 0,
    }
    snapshot_ms_total = 0.0
    sim_context_ms_total = 0.0
    prop_eval_ms_total = 0.0
    game_lens_ms_total = 0.0
    for card in cards:
        game_pk = _safe_int(card.get("gamePk"))
        if not game_pk:
            continue
        game_feed = feed_cache.get(int(game_pk))
        if int(game_pk) not in feed_cache:
            game_feed = _load_live_lens_feed(int(game_pk), d)
            feed_cache[int(game_pk)] = game_feed
        snapshot_started_at = time.perf_counter()
        snapshot = _load_live_lens_snapshot(int(game_pk), d, feed=game_feed)
        snapshot_ms_total += (time.perf_counter() - snapshot_started_at) * 1000.0
        sim_context_started_at = time.perf_counter()
        sim_context = _load_sim_context_for_game(int(game_pk), d, artifacts=artifacts, archive=archive, feed=game_feed)
        sim_context_ms_total += (time.perf_counter() - sim_context_started_at) * 1000.0
        status = ((snapshot or {}).get("status") or {})
        status_detailed = str(status.get("detailedState") or ((card.get("status") or {}).get("detailed") or ""))
        status_abstract = str(status.get("abstractGameState") or ((card.get("status") or {}).get("abstract") or ""))
        status_is_live = _status_is_live({"abstract": status_abstract, "detailed": status_detailed})
        status_is_final = _status_is_final({"abstract": status_abstract, "detailed": status_detailed})
        tracked_prop_rows = _prop_lens_rows(card, snapshot, sim_context if sim_context.get("found") else None)
        prop_eval_started_at = time.perf_counter()
        live_prop_rows = [
            _normalize_live_lens_live_prop_row(row, snapshot, card)
            for row in _current_live_prop_rows(
                card,
                snapshot,
                sim_context if sim_context.get("found") else None,
                d,
                write_observation_log=bool(persist),
                ensure_market_fresh=False,
            )
            if isinstance(row, dict)
        ]
        archived_live_prop_rows: List[Dict[str, Any]] = []
        if status_is_final and live_prop_rows:
            archived_live_prop_rows = list(live_prop_rows)
            live_prop_rows = []
        prop_eval_ms_total += (time.perf_counter() - prop_eval_started_at) * 1000.0
        game_lens_started_at = time.perf_counter()
        game_lens = _build_game_lens(card, snapshot, sim_context if sim_context.get("found") else None, _game_line_market_for_card(card, game_line_index), date_str=str(d))
        game_lens_ms_total += (time.perf_counter() - game_lens_started_at) * 1000.0
        if status_is_final:
            counts["final"] += 1
        elif status_is_live:
            counts["live"] += 1
        else:
            counts["pregame"] += 1
        counts["games"] += 1
        counts["props"] += len(live_prop_rows) if live_prop_rows else len(tracked_prop_rows)
        counts["archivedLiveProps"] += len(archived_live_prop_rows)
        away_totals = ((((snapshot or {}).get("teams") or {}).get("away") or {}).get("totals") or {})
        home_totals = ((((snapshot or {}).get("teams") or {}).get("home") or {}).get("totals") or {})
        games_out.append(
            {
                "gamePk": int(game_pk),
                "status": {
                    "abstract": status_abstract,
                    "detailed": status_detailed,
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
                "props": live_prop_rows if live_prop_rows else tracked_prop_rows,
                "liveProps": live_prop_rows,
                "archivedLiveProps": archived_live_prop_rows,
                "trackedProps": tracked_prop_rows,
                "simContextAvailable": bool(sim_context.get("found")),
                "snapshotAvailable": bool(snapshot),
            }
        )

    payload = {
        "date": str(d),
        "generatedAt": _local_timestamp_text(),
        "dataRoot": _relative_path_str(_DATA_DIR),
        "liveLensDir": _relative_path_str(_LIVE_LENS_DIR),
        "optimizationRegime": _live_lens_optimization_regime(d),
        "counts": counts,
        "performance": {
            "marketsRefreshed": bool(markets_refreshed),
            "marketRefreshMs": market_refresh_ms,
            "totalMs": round((time.perf_counter() - started_at) * 1000.0, 1),
            "snapshotLoadMs": round(snapshot_ms_total, 1),
            "simContextLoadMs": round(sim_context_ms_total, 1),
            "propEvalMs": round(prop_eval_ms_total, 1),
            "gameLensMs": round(game_lens_ms_total, 1),
            "gameCount": int(counts["games"]),
            "liveGameCount": int(counts["live"]),
            "feedFetchCount": int(len(feed_cache)),
        },
        "games": games_out,
    }

    if persist:
        persist_started_at = time.perf_counter()
        perf = payload.get("performance") if isinstance(payload.get("performance"), dict) else {}
        perf["persistMs"] = None
        payload["performance"] = perf
        perf["persistMs"] = round((time.perf_counter() - persist_started_at) * 1000.0, 1)
        payload["performance"] = perf
        log_entry = {
            "recordedAt": payload.get("generatedAt"),
            "date": payload.get("date"),
            "counts": counts,
            "performance": payload.get("performance"),
            "games": [
                {
                    "gamePk": game.get("gamePk"),
                    "status": ((game.get("status") or {}).get("abstract")),
                    "score": ((game.get("matchup") or {}).get("score")),
                    "liveText": ((game.get("matchup") or {}).get("liveText")),
                    "propCount": len(game.get("liveProps") or game.get("props") or []),
                    "topProps": (game.get("liveProps") or game.get("props") or [])[:5],
                }
                for game in games_out
            ],
        }
        _append_jsonl(_live_lens_log_path(d), log_entry)
        _write_json_file(_live_lens_report_path(d), payload)

    return payload


def _refresh_oddsapi_markets(d: str, *, overwrite: bool = True) -> Dict[str, Any]:
    recorded_at = _local_now()
    frozen_pregame = _freeze_oddsapi_pregame_markets(d)
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
        "frozenPregame": frozen_pregame,
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
        "frozenPregame": frozen_pregame,
        "result": result,
        "copied": copied,
        "archived": archived,
    }


def _live_lens_reports_payload(d: str) -> Dict[str, Any]:
    log_path = _live_lens_log_path(d)
    observation_log_path = _live_prop_observation_log_path(d)
    registry_path = _live_prop_registry_path(d)
    registry_log_path = _live_prop_registry_log_path(d)
    recap_path = _live_lens_daily_recap_path(d)
    latest_report = _load_json_file(_live_lens_report_path(d)) or {}
    registry_summary = _live_prop_registry_summary(d)
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
    if not latest_report and not registry_summary and recap_path.exists():
        recap_payload = _load_json_file(recap_path) or {}
        if isinstance(recap_payload, dict) and recap_payload:
            return {
                "ok": True,
                "date": str(d),
                "optimizationRegime": recap_payload.get("optimizationRegime") or _live_lens_optimization_regime(d),
                "logPath": recap_payload.get("logPath") or _relative_path_str(log_path),
                "propObservationLogPath": recap_payload.get("propObservationLogPath") or _relative_path_str(observation_log_path),
                "registryPath": recap_payload.get("registryPath") or _relative_path_str(registry_path),
                "registryLogPath": recap_payload.get("registryLogPath") or _relative_path_str(registry_log_path),
                "reportPath": recap_payload.get("reportPath") or _relative_path_str(_live_lens_report_path(d)),
                "dailyRecapPath": _relative_path_str(recap_path),
                "entries": int(recap_payload.get("entries") or 0),
                "latestEntry": recap_payload.get("latestEntry") if isinstance(recap_payload.get("latestEntry"), dict) else None,
                "latestReport": recap_payload.get("latestReport") if isinstance(recap_payload.get("latestReport"), dict) else {},
                "registrySummary": recap_payload.get("registrySummary") if isinstance(recap_payload.get("registrySummary"), dict) else {},
                "source": "daily_recap",
            }
    return {
        "ok": True,
        "date": str(d),
        "optimizationRegime": _live_lens_optimization_regime(d),
        "logPath": _relative_path_str(log_path),
        "propObservationLogPath": _relative_path_str(observation_log_path),
        "registryPath": _relative_path_str(registry_path),
        "registryLogPath": _relative_path_str(registry_log_path),
        "reportPath": _relative_path_str(_live_lens_report_path(d)),
        "dailyRecapPath": _relative_path_str(recap_path),
        "entries": int(entries),
        "latestEntry": latest_entry,
        "latestReport": latest_report,
        "registrySummary": registry_summary,
    }


def _persist_live_lens_tick(d: str, *, trigger: str = "api", refresh_markets: bool = True) -> Dict[str, Any]:
    payload = _live_lens_payload(d, persist=True, refresh_markets=refresh_markets)
    performance = payload.get("performance") if isinstance(payload.get("performance"), dict) else {}
    meta = {
        "recordedAt": _local_timestamp_text(),
        "date": str(d),
        "counts": payload.get("counts"),
        "marketsRefreshed": bool(performance.get("marketsRefreshed")),
        "reportPath": _relative_path_str(_live_lens_report_path(d)),
        "logPath": _relative_path_str(_live_lens_log_path(d)),
        "propObservationLogPath": _relative_path_str(_live_prop_observation_log_path(d)),
        "trigger": str(trigger),
    }
    _write_json_file(_cron_meta_dir() / "latest_live_lens_tick.json", meta)
    return {"ok": True, "date": str(d), "counts": payload.get("counts"), "report": meta}


def _live_lens_background_loop() -> None:
    interval_seconds = _live_lens_loop_interval_seconds()
    oddsapi_refresh_interval_seconds = _live_oddsapi_refresh_interval_seconds()
    report_refresh_interval_seconds = _live_lens_report_refresh_interval_seconds()
    background_report_enabled = _is_live_lens_background_report_enabled()
    status_path = _cron_meta_dir() / "live_lens_loop_status.json"
    next_oddsapi_refresh_at = 0.0
    next_report_refresh_at = 0.0
    while not _LIVE_LENS_LOOP_STOP.is_set():
        started_at = time.time()
        refresh_markets = bool(started_at >= next_oddsapi_refresh_at)
        refresh_report = bool(background_report_enabled and started_at >= next_report_refresh_at)
        result: Dict[str, Any] = {}
        markets_refreshed = False
        try:
            if refresh_report:
                result = _persist_live_lens_tick(
                    _today_iso(),
                    trigger="background_loop",
                    refresh_markets=refresh_markets,
                )
                next_report_refresh_at = float(started_at) + float(report_refresh_interval_seconds)
                markets_refreshed = bool((result.get("report") or {}).get("marketsRefreshed"))
            elif refresh_markets:
                markets_refreshed = bool(_maybe_refresh_live_oddsapi_markets(_today_iso()))
            cards_payload = _warm_cards_api_cache(_today_iso())
            if refresh_markets:
                next_oddsapi_refresh_at = float(started_at) + float(oddsapi_refresh_interval_seconds)
            latest_tick = _load_json_file(_cron_meta_dir() / "latest_live_lens_tick.json") or {}
            _write_json_file(
                status_path,
                {
                    "ok": True,
                    "recordedAt": _local_timestamp_text(),
                    "intervalSeconds": int(interval_seconds),
                    "oddsapiRefreshIntervalSeconds": int(oddsapi_refresh_interval_seconds),
                    "backgroundReportEnabled": bool(background_report_enabled),
                    "reportRefreshIntervalSeconds": int(report_refresh_interval_seconds),
                    "reportRefreshTriggered": bool(refresh_report),
                    "marketsRefreshTriggered": bool(refresh_markets),
                    "marketsRefreshed": bool(markets_refreshed),
                    "date": result.get("date") or latest_tick.get("date"),
                    "counts": result.get("counts") or latest_tick.get("counts"),
                    "cardsCacheDate": str(cards_payload.get("date") or ""),
                    "cardsCacheCount": int(len(cards_payload.get("cards") or [])) if isinstance(cards_payload.get("cards"), list) else 0,
                },
            )
        except Exception as exc:
            if refresh_markets:
                next_oddsapi_refresh_at = float(started_at) + float(oddsapi_refresh_interval_seconds)
            if refresh_report:
                next_report_refresh_at = float(started_at) + float(report_refresh_interval_seconds)
            _write_json_file(
                status_path,
                {
                    "ok": False,
                    "recordedAt": _local_timestamp_text(),
                    "intervalSeconds": int(interval_seconds),
                    "oddsapiRefreshIntervalSeconds": int(oddsapi_refresh_interval_seconds),
                    "backgroundReportEnabled": bool(background_report_enabled),
                    "reportRefreshIntervalSeconds": int(report_refresh_interval_seconds),
                    "reportRefreshTriggered": bool(refresh_report),
                    "marketsRefreshTriggered": bool(refresh_markets),
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
        elapsed = max(0.0, float(time.time()) - float(started_at))
        wait_seconds = max(1.0, float(interval_seconds) - float(elapsed))
        _LIVE_LENS_LOOP_STOP.wait(wait_seconds)


def start_live_lens_background_loop() -> bool:
    global _LIVE_LENS_LOOP_THREAD
    if not _is_live_lens_loop_enabled():
        return False
    werkzeug_run_main = str(os.environ.get("WERKZEUG_RUN_MAIN") or "").strip().lower()
    if werkzeug_run_main == "false" and _env_bool("FLASK_DEBUG", default=False):
        return False
    with _LIVE_LENS_LOOP_LOCK:
        if _LIVE_LENS_LOOP_THREAD is not None and _LIVE_LENS_LOOP_THREAD.is_alive():
            return False
        _LIVE_LENS_LOOP_STOP.clear()
        _LIVE_LENS_LOOP_THREAD = threading.Thread(
            target=_live_lens_background_loop,
            name="mlb-live-lens-loop",
            daemon=True,
        )
        _LIVE_LENS_LOOP_THREAD.start()
        return True


def _season_live_lens_payload(season: int, d: str) -> Dict[str, Any]:
    date_str = str(d or "").strip() or _default_cards_date()
    payload: Dict[str, Any] = {
        "season": int(season),
        "date": date_str,
        "found": False,
    }
    date_season = _season_from_date_str(date_str)
    if date_season is not None and int(date_season) != int(season):
        payload["error"] = "season_live_lens_date_mismatch"
        payload["detail"] = f"Date {date_str} belongs to season {date_season}, not {int(season)}"
        return payload

    live_payload = _live_lens_payload(date_str, persist=False, refresh_markets=False)
    counts = dict(live_payload.get("counts") or {})
    live_payload.update(
        {
            "season": int(season),
            "date": date_str,
            "found": bool(int(counts.get("games") or 0) > 0),
            "isHistorical": bool(_is_historical_date(date_str)),
            "hasLiveGames": bool(int(counts.get("live") or 0) > 0),
            "hasTrackedProps": bool(int(counts.get("props") or 0) > 0),
        }
    )
    return live_payload


def _publish_season_manifests(
    *,
    season: int,
    batch_dir: Path,
    betting_profile: str,
    season_dir: Path,
) -> Dict[str, Any]:
    _ensure_dir(season_dir)

    report_files = sorted(batch_dir.glob("sim_vs_actual_*.json")) if batch_dir.exists() and batch_dir.is_dir() else []
    if not report_files:
        raise FileNotFoundError(f"No sim_vs_actual_*.json reports found under: {batch_dir}")

    season_manifest = build_season_eval_manifest(
        season=int(season),
        batch_dir=batch_dir,
        title=f"MLB {int(season)} Rolling Season Eval",
        game_types="R",
    )
    season_manifest_path, season_recap_path = write_season_eval_manifest_artifacts(
        season_manifest,
        season=int(season),
        out=str(season_dir / "season_eval_manifest.json"),
        recap_md=str(season_dir / "season_eval_recap.md"),
    )

    normalized_profile = str(betting_profile or "retuned").strip().lower()
    if normalized_profile not in ("baseline", "retuned"):
        normalized_profile = "retuned"
    betting_manifest_path = season_dir / (
        "season_betting_cards_retuned_manifest.json"
        if normalized_profile == "retuned"
        else "season_betting_cards_manifest.json"
    )
    betting_recap_path = season_dir / (
        "season_betting_cards_retuned_recap.md"
        if normalized_profile == "retuned"
        else "season_betting_cards_recap.md"
    )
    betting_cards_dir = season_dir / (
        "locked_cards_retuned"
        if normalized_profile == "retuned"
        else "locked_cards"
    )
    cmd = [
        str(Path(sys.executable).resolve()),
        str((_ROOT / "tools" / "eval" / "build_season_betting_cards_manifest.py").resolve()),
        "--season",
        str(int(season)),
        "--batch-dir",
        str(batch_dir),
        "--out",
        str(betting_manifest_path),
        "--recap-md",
        str(betting_recap_path),
        "--cards-dir",
        str(betting_cards_dir),
        "--title",
        f"MLB {int(season)} Betting Card Recap",
    ]
    if normalized_profile == "retuned":
        cmd.extend(["--prefer-canonical-daily", "on"])
    betting_rc = subprocess.run(cmd, check=False, capture_output=True, text=True)

    return {
        "ok": betting_rc.returncode == 0,
        "season": int(season),
        "batch_dir": _relative_path_str(batch_dir),
        "season_dir": _relative_path_str(season_dir),
        "report_count": int(len(report_files)),
        "betting_profile": normalized_profile,
        "season_eval_manifest": _relative_path_str(season_manifest_path),
        "season_eval_recap": _relative_path_str(season_recap_path),
        "season_eval_status": str((season_manifest.get("meta") or {}).get("status") or "unknown"),
        "season_eval_partial": bool((season_manifest.get("meta") or {}).get("partial")),
        "season_eval_days": int((season_manifest.get("overview") or {}).get("days") or 0),
        "season_betting_manifest": _relative_path_str(betting_manifest_path),
        "season_betting_recap": _relative_path_str(betting_recap_path),
        "season_betting_cards_dir": _relative_path_str(betting_cards_dir),
        "season_betting_exit_code": int(betting_rc.returncode),
        "season_betting_stdout": str((betting_rc.stdout or "").strip()),
        "season_betting_stderr": str((betting_rc.stderr or "").strip()),
        "season_betting_manifest_exists": bool(betting_manifest_path.exists()),
    }


def _rebuild_season_day_report(
    *,
    season: int,
    date_str: str,
    sims: int,
    workers: int,
    spring_mode: bool,
) -> Dict[str, Any]:
    from tools.eval import eval_sim_day_vs_actual as eval_sim_day_vs_actual_mod

    batch_dir = _DATA_DIR / "eval" / "batches" / f"season_{int(season)}_ui_daily_live"
    out_path = batch_dir / f"sim_vs_actual_{str(date_str)}.json"
    _ensure_dir(batch_dir)

    lineups_last_known_path = _DAILY_DIR / "lineups_last_known_by_team.json"
    argv = [
        "eval_sim_day_vs_actual.py",
        "--date",
        str(date_str),
        "--season",
        str(int(season)),
        "--spring-mode",
        ("on" if bool(spring_mode) else "off"),
        "--stats-season",
        str(int(season) - 1 if bool(spring_mode) else int(season)),
        "--use-daily-snapshots",
        "on",
        "--daily-snapshots-root",
        str(_DAILY_DIR / "snapshots"),
        "--use-roster-artifacts",
        "on",
        "--write-roster-artifacts",
        "on",
        "--sims-per-game",
        str(int(sims)),
        "--bvp-hr",
        "off",
        "--bvp-days-back",
        "365",
        "--bvp-min-pa",
        "10",
        "--bvp-shrink-pa",
        "50.0",
        "--bvp-clamp-lo",
        "0.8",
        "--bvp-clamp-hi",
        "1.25",
        "--hitter-hr-topn",
        "0",
        "--hitter-props-topn",
        "24",
        "--seed",
        "1337",
        "--jobs",
        str(max(1, int(workers))),
        "--use-raw",
        "on",
        "--write-missing-raw",
        "on",
        "--prop-lines-source",
        "auto",
        "--cache-ttl-hours",
        "24",
        "--umpire-shrink",
        "0.75",
        "--pitch-model-overrides",
        "",
        "--manager-pitching",
        "v2",
        "--manager-pitching-overrides",
        "",
        "--pitcher-rate-sampling",
        "on",
        "--bip-baserunning",
        "on",
        "--out",
        str(out_path),
    ]
    if lineups_last_known_path.exists():
        argv.extend(["--lineups-last-known", str(lineups_last_known_path)])

    original_argv = list(sys.argv)
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    try:
        sys.argv = list(argv)
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exit_code = int(eval_sim_day_vs_actual_mod.main())
    finally:
        sys.argv = original_argv
    return {
        "ok": int(exit_code) == 0 and out_path.exists(),
        "season": int(season),
        "date": str(date_str),
        "batch_dir": _relative_path_str(batch_dir),
        "report_path": _relative_path_str(out_path),
        "sims": int(sims),
        "workers": max(1, int(workers)),
        "spring_mode": bool(spring_mode),
        "command": [str(part) for part in argv],
        "exit_code": int(exit_code),
        "stdout": str(stdout_buffer.getvalue().strip()),
        "stderr": str(stderr_buffer.getvalue().strip()),
        "report_exists": bool(out_path.exists()),
    }


@app.get("/live-lens")
def live_lens_view() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    return render_template(
        "live_lens.html",
        date=d,
        season=None,
        api_path="/api/live-lens",
        form_action="/live-lens",
        back_href=f"/?date={d}",
        back_label="Back to cards",
        page_title=f"MLB Live Lens - {d}",
        page_heading=f"MLB Live Lens - {d}",
    )


@app.get("/season/<int:season>/live-lens")
def season_live_lens_view(season: int) -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    return render_template(
        "live_lens.html",
        date=d,
        season=int(season),
        api_path=f"/api/season/{int(season)}/live-lens",
        form_action=f"/season/{int(season)}/live-lens",
        back_href=f"/season/{int(season)}?date={d}",
        back_label=f"Back to season {int(season)}",
        page_title=f"MLB {int(season)} Live Lens - {d}",
        page_heading=f"MLB {int(season)} Live Lens - {d}",
    )


@app.get("/api/live-lens")
def api_live_lens() -> Response:
    _ensure_live_lens_background_loop_running()
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    persist = str(request.args.get("persist") or "off").strip().lower() == "on"
    report_path = _live_lens_report_path(d)
    report_age_seconds = _path_age_seconds(report_path)
    serve_report_max_age_seconds = float(_LIVE_ROUTE_CACHE_TTL_SECONDS)
    if not _is_historical_date(d) and _is_live_lens_loop_enabled():
        serve_report_max_age_seconds = float(_live_lens_report_max_age_seconds())
    if (
        not persist
        and report_age_seconds is not None
        and report_age_seconds <= float(serve_report_max_age_seconds)
    ):
        report_payload = _load_json_file(report_path)
        if isinstance(report_payload, dict) and report_payload:
            return jsonify(_with_app_build(report_payload))
    payload = _payload_cache_get_or_build(
        "live_lens_api",
        str(d),
        max_age_seconds=_LIVE_ROUTE_CACHE_TTL_SECONDS,
        builder=lambda: _live_lens_payload(d, persist=persist, refresh_markets=not _is_historical_date(d)),
    )
    return jsonify(_with_app_build(payload))


@app.get("/api/season/<int:season>/live-lens")
def api_season_live_lens(season: int) -> Response:
    _ensure_live_lens_background_loop_running()
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _season_live_lens_payload(int(season), d)
    if payload.get("found"):
        return jsonify(payload)
    status_code = 400 if payload.get("error") == "season_live_lens_date_mismatch" else 404
    return jsonify(payload), status_code


@app.get("/api/cron/ping")
def api_cron_ping() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    return jsonify(
        _with_app_build(
            {
                "ok": True,
                "service": "mlb-betting-v2",
                "time": _local_timestamp_text(),
                "dataRoot": _relative_path_str(_DATA_DIR),
                "liveLensDir": _relative_path_str(_LIVE_LENS_DIR),
            }
        )
    )


@app.get("/api/cron/config")
def api_cron_config() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    loop_status = _ensure_live_lens_background_loop_running()
    return jsonify(
        _with_app_build(
            {
                "ok": True,
                "cronTokenConfigured": bool(_CRON_TOKEN),
                "dataRoot": _relative_path_str(_DATA_DIR),
                "marketDir": _relative_path_str(_MARKET_DIR),
                "dailyDir": _relative_path_str(_DAILY_DIR),
                "liveLensDir": _relative_path_str(_LIVE_LENS_DIR),
                "liveLensLoop": loop_status,
                "diskUsageEndpoint": "/api/cron/disk-usage",
                "cleanupEndpoint": "/api/cron/cleanup-data?target=live-lens&retentionDays=3&apply=off",
                "compactLiveLensEndpoint": "/api/cron/compact-live-lens?retentionDays=3&apply=off",
                "seasonRebuildEndpoint": "/api/cron/rebuild-season-report?season=YYYY&date=YYYY-MM-DD",
                "seasonRepublishEndpoint": "/api/cron/republish-season?season=YYYY&profile=retuned",
            }
        )
    )


@app.get("/api/cron/disk-usage")
def api_cron_disk_usage() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    largest_file_limit = max(1, int(_safe_int(request.args.get("largest")) or 20))
    return jsonify(_with_app_build(_data_disk_report(largest_file_limit=largest_file_limit)))


@app.get("/api/cron/cleanup-data")
def api_cron_cleanup_data() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error

    target = str(request.args.get("target") or "live-lens").strip().lower() or "live-lens"
    retention_days = max(0, int(_safe_int(request.args.get("retentionDays")) or 3))
    apply_changes = str(request.args.get("apply") or "off").strip().lower() == "on"
    include_today = str(request.args.get("includeToday") or "off").strip().lower() == "on"
    prune_empty_dirs = str(request.args.get("pruneEmptyDirs") or "on").strip().lower() != "off"
    largest_file_limit = max(1, int(_safe_int(request.args.get("largest")) or 20))

    try:
        payload = _cleanup_old_files(
            target=target,
            retention_days=retention_days,
            apply_changes=apply_changes,
            include_today=include_today,
            prune_empty_dirs=prune_empty_dirs,
            largest_file_limit=largest_file_limit,
        )
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc), "target": target}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}", "target": target}), 500
    return jsonify(_with_app_build(payload))


@app.get("/api/cron/compact-live-lens")
def api_cron_compact_live_lens() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error

    retention_days = max(0, int(_safe_int(request.args.get("retentionDays")) or 3))
    apply_changes = str(request.args.get("apply") or "off").strip().lower() == "on"
    include_today = str(request.args.get("includeToday") or "off").strip().lower() == "on"
    max_days = max(1, int(_safe_int(request.args.get("maxDays")) or 30))

    try:
        payload = _compact_live_lens_days(
            retention_days=retention_days,
            apply_changes=apply_changes,
            include_today=include_today,
            max_days=max_days,
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 500
    return jsonify(_with_app_build(payload))


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
    _ensure_live_lens_background_loop_running()
    d = str(request.args.get("date") or "").strip() or _today_iso()
    try:
        return jsonify(_persist_live_lens_tick(d, trigger="api"))
    except Exception as exc:
        return jsonify({"ok": False, "date": d, "error": f"{type(exc).__name__}: {exc}"}), 500


@app.get("/api/cron/live-lens-reports")
def api_cron_live_lens_reports() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    _ensure_live_lens_background_loop_running()
    d = str(request.args.get("date") or "").strip() or _today_iso()
    return jsonify(_live_lens_reports_payload(d))


@app.get("/api/cron/live-lens-loop-status")
def api_cron_live_lens_loop_status() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error
    return jsonify(_with_app_build({"ok": True, "liveLensLoop": _ensure_live_lens_background_loop_running()}))


@app.get("/api/cron/republish-season")
def api_cron_republish_season() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error

    season = _safe_int(request.args.get("season")) or _season_from_date_str(_today_iso()) or _local_today().year
    profile = str(request.args.get("profile") or "retuned").strip().lower() or "retuned"
    batch_dir_raw = str(request.args.get("batchDir") or "").strip()
    season_dir_raw = str(request.args.get("seasonDir") or "").strip()
    batch_dir = _path_from_maybe_relative(batch_dir_raw) if batch_dir_raw else (_DATA_DIR / "eval" / "batches" / f"season_{int(season)}_ui_daily_live")
    season_dir = _path_from_maybe_relative(season_dir_raw) if season_dir_raw else (_DATA_DIR / "eval" / "seasons" / str(int(season)))

    try:
        payload = _publish_season_manifests(
            season=int(season),
            batch_dir=batch_dir,
            betting_profile=profile,
            season_dir=season_dir,
        )
    except FileNotFoundError as exc:
        return jsonify({
            "ok": False,
            "season": int(season),
            "profile": profile,
            "batch_dir": _relative_path_str(batch_dir),
            "season_dir": _relative_path_str(season_dir),
            "error": f"{type(exc).__name__}: {exc}",
        }), 404
    except Exception as exc:
        return jsonify({
            "ok": False,
            "season": int(season),
            "profile": profile,
            "batch_dir": _relative_path_str(batch_dir),
            "season_dir": _relative_path_str(season_dir),
            "error": f"{type(exc).__name__}: {exc}",
        }), 500

    status_code = 200 if bool(payload.get("ok")) else 500
    return jsonify(payload), status_code


@app.get("/api/cron/rebuild-season-report")
def api_cron_rebuild_season_report() -> Response:
    auth_error = _require_cron_auth()
    if auth_error is not None:
        return auth_error

    season = _safe_int(request.args.get("season")) or _season_from_date_str(_today_iso()) or _local_today().year
    date_str = str(request.args.get("date") or "").strip()
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date", "season": int(season)}), 400
    sims = _safe_int(request.args.get("sims")) or 1000
    workers = _safe_int(request.args.get("workers")) or 4
    spring_mode = str(request.args.get("springMode") or "on").strip().lower() != "off"
    profile = str(request.args.get("profile") or "retuned").strip().lower() or "retuned"
    publish_after = str(request.args.get("publish") or "on").strip().lower() != "off"

    try:
        rebuild_payload = _rebuild_season_day_report(
            season=int(season),
            date_str=str(date_str),
            sims=int(sims),
            workers=int(workers),
            spring_mode=bool(spring_mode),
        )
    except Exception as exc:
        return jsonify({
            "ok": False,
            "season": int(season),
            "date": str(date_str),
            "error": f"{type(exc).__name__}: {exc}",
        }), 500

    if not bool(rebuild_payload.get("ok")):
        return jsonify(rebuild_payload), 500

    payload: Dict[str, Any] = {"ok": True, "rebuild": rebuild_payload}
    if publish_after:
        try:
            payload["republish"] = _publish_season_manifests(
                season=int(season),
                batch_dir=_DATA_DIR / "eval" / "batches" / f"season_{int(season)}_ui_daily_live",
                betting_profile=profile,
                season_dir=_DATA_DIR / "eval" / "seasons" / str(int(season)),
            )
            payload["ok"] = bool((payload.get("republish") or {}).get("ok"))
        except Exception as exc:
            payload["ok"] = False
            payload["republish_error"] = f"{type(exc).__name__}: {exc}"
            return jsonify(payload), 500
    return jsonify(payload), 200
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


@app.get("/hr-targets")
def hr_targets_view() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    game = _normalize_game_selector(request.args.get("game"))
    team = _normalize_hitter_team_selector(request.args.get("team"))
    hitter = _normalize_hitter_selector(request.args.get("hitter"))
    sort = _normalize_hr_target_sort(request.args.get("sort"))
    season = _season_from_date_str(d) or _season_from_date_str(_today_iso()) or date.today().year
    return render_template(
        "hr_targets.html",
        date=d,
        game=game,
        team=team,
        hitter=hitter,
        sort=sort,
        sort_options=_hr_target_sort_options(),
        season=int(season),
    )


@app.get("/pitcher-top-props")
def pitcher_top_props_view() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    season = _season_from_date_str(d) or _season_from_date_str(_today_iso()) or date.today().year
    return render_template(
        "daily_top_props.html",
        title="Pitcher Top Props",
        date=d,
        group="pitcher",
        groupLabel="Pitcher",
        season=int(season),
    )


@app.get("/hitter-top-props")
def hitter_top_props_view() -> str:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    season = _season_from_date_str(d) or _season_from_date_str(_today_iso()) or date.today().year
    return render_template(
        "daily_top_props.html",
        title="Hitter Top Props",
        date=d,
        group="hitter",
        groupLabel="Hitter",
        season=int(season),
    )


@app.get("/season/<int:season>")
def season_view(season: int) -> str:
    d = str(request.args.get("date") or "").strip()
    return render_template("season.html", season=int(season), date=d)


@app.get("/season/<int:season>/betting-card")
def season_betting_card_view(season: int) -> str:
    d = str(request.args.get("date") or "").strip()
    profile = str(request.args.get("profile") or "retuned").strip().lower() or "retuned"
    return render_template("betting_card.html", season=int(season), date=d, profile=profile)


def _cards_api_payload(
    d: str,
    *,
    artifacts: Optional[Dict[str, Any]],
    archive: Optional[Dict[str, Any]],
    cards: List[Dict[str, Any]],
    fallback_error: Optional[str] = None,
) -> Dict[str, Any]:
    artifacts = artifacts if isinstance(artifacts, dict) else {}
    archive = archive if isinstance(archive, dict) else {}
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

    payload = {
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
        "hrTargets": _cards_hr_targets_summary_payload(d, artifacts),
        "cards": cards,
    }
    if fallback_error:
        payload["warning"] = fallback_error
    return payload


def _cards_hr_targets_summary_payload(d: str, artifacts: Dict[str, Any]) -> Dict[str, Any]:
    doc = artifacts.get("hr_targets") if isinstance(artifacts.get("hr_targets"), dict) else None
    artifact_path = artifacts.get("hr_targets_path") if isinstance(artifacts.get("hr_targets_path"), Path) else None
    rows = [row for row in ((doc or {}).get("rows") or []) if isinstance(row, dict)]
    top_rows: List[Dict[str, Any]] = []

    for row in rows[:5]:
        top_rows.append(_hr_target_page_row_payload(d, row))

    counts = (doc or {}).get("counts") if isinstance((doc or {}).get("counts"), dict) else {}
    return {
        "found": bool(doc and rows),
        "sourcePath": _relative_path_str(artifact_path),
        "games": int(counts.get("games") or 0),
        "rows": int(counts.get("rows") or 0),
        "topRows": top_rows,
        "pageHref": f"/hr-targets?date={d}",
    }


def _cards_hr_target_driver_payload(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    metrics = row.get("hr_target_metrics") if isinstance(row.get("hr_target_metrics"), dict) else {}
    candidates: List[Dict[str, Any]] = []

    def _append(label: str, value: Any) -> None:
        numeric = _safe_float(value)
        if numeric is None:
            return
        delta = float(numeric) - 1.0
        if abs(delta) < 0.015:
            return
        candidates.append(
            {
                "label": label,
                "value": round(float(numeric), 3),
                "delta": round(delta, 3),
                "display": f"{float(numeric):.2f}x",
            }
        )

    _append("HR quality", metrics.get("batterHrQuality"))
    _append("Batter platoon", metrics.get("batterPlatoonHr"))
    _append("Pitcher HR carry", metrics.get("pitcherHrQuality"))
    _append("Pitcher platoon", metrics.get("pitcherPlatoonHr"))
    _append("Park carry", metrics.get("parkHr"))
    _append("Weather carry", metrics.get("weatherHr"))

    positives = [item for item in candidates if float(item.get("delta") or 0.0) > 0]
    negatives = [item for item in candidates if float(item.get("delta") or 0.0) < 0]
    positives.sort(key=lambda item: abs(float(item.get("delta") or 0.0)), reverse=True)
    negatives.sort(key=lambda item: abs(float(item.get("delta") or 0.0)), reverse=True)

    selected = positives[:3]
    if len(selected) < 3:
        selected.extend(negatives[: 3 - len(selected)])
    if not selected:
        selected = sorted(candidates, key=lambda item: abs(float(item.get("delta") or 0.0)), reverse=True)[:3]
    return selected[:3]


def _cards_hr_target_join_phrases(parts: List[str]) -> str:
    filtered = [str(part).strip() for part in parts if str(part).strip()]
    if not filtered:
        return ""
    if len(filtered) == 1:
        return filtered[0]
    if len(filtered) == 2:
        return f"{filtered[0]} and {filtered[1]}"
    return ", ".join(filtered[:-1]) + f", and {filtered[-1]}"


def _cards_hr_target_ordinal(value: Any) -> str:
    number = _safe_int(value)
    if number is None:
        return ""
    remainder_100 = number % 100
    remainder_10 = number % 10
    if 11 <= remainder_100 <= 13:
        suffix = "th"
    elif remainder_10 == 1:
        suffix = "st"
    elif remainder_10 == 2:
        suffix = "nd"
    elif remainder_10 == 3:
        suffix = "rd"
    else:
        suffix = "th"
    return f"{number}{suffix}"


def _cards_hr_target_driver_phrase(driver: Dict[str, Any]) -> str:
    label = _first_text((driver or {}).get("label"))
    display = _first_text((driver or {}).get("display"), "1.00x")
    delta = _safe_float((driver or {}).get("delta")) or 0.0
    positive = delta >= 0
    if label == "HR quality":
        return f"his HR-quality contact is running at {display} of baseline"
    if label == "Batter platoon":
        return f"the hitter-side handedness split is {'boosting' if positive else 'trimming'} power to {display}"
    if label == "Pitcher HR carry":
        return f"the opposing pitcher's damage profile is {'allowing' if positive else 'holding'} HR carry around {display}"
    if label == "Pitcher platoon":
        return f"the pitcher-side handedness split is {'adding' if positive else 'reducing'} damage at {display}"
    if label == "Park carry":
        return f"the park is playing at {display} for HR carry"
    if label == "Weather carry":
        return f"weather is landing around {display} for HR carry"
    return f"{label.lower()} is checking in at {display}"


def _cards_hr_target_highlights(row: Dict[str, Any]) -> List[str]:
    reasons = [str(reason).strip() for reason in (row.get("hr_target_reasons") or []) if str(reason).strip()]
    filtered = [
        reason
        for reason in reasons
        if "Expected opportunity" not in reason and "lineup slot" not in reason
    ]
    return filtered[:2]


def _cards_hr_target_bvp_support_phrase(row: Dict[str, Any]) -> str:
    pitcher_name = _first_text(row.get("opponent_pitcher_name"), "this pitcher")
    career_pa = _safe_int(row.get("bvp_career_pa"))
    career_hr = _safe_int(row.get("bvp_career_hr"))
    career_hr_mult = _safe_float(row.get("bvp_career_hr_mult"))

    if career_pa is None or career_pa < 3:
        return ""

    detail_bits: List[str] = []
    if career_hr is not None and career_hr > 0:
        homer_label = "HR" if int(career_hr) == 1 else "HRs"
        detail_bits.append(f"{int(career_hr)} {homer_label} in {int(career_pa)} career PA off {pitcher_name}")
    elif career_pa >= 8 and career_hr_mult is not None and float(career_hr_mult) >= 1.08:
        detail_bits.append(f"{int(career_pa)} career PA off {pitcher_name}")

    if career_hr_mult is not None and float(career_hr_mult) >= 1.08:
        detail_bits.append(f"damage in that sample grading {float(career_hr_mult):.2f}x his baseline")

    if detail_bits:
        return f"the career BvP sample is supportive too, with {_cards_hr_target_join_phrases(detail_bits)}"

    if career_hr is not None:
        if int(career_hr) == 0:
            if career_pa >= 8:
                return f"there is direct BvP history too, with no HR yet in {int(career_pa)} career PA off {pitcher_name}, even if that sample reads more neutral than decisive"
            return f"there is at least some direct BvP history too, with {int(career_pa)} career PA off {pitcher_name}"
        homer_label = "HR" if int(career_hr) == 1 else "HRs"
        return f"there is direct BvP history too, with {int(career_hr)} {homer_label} in {int(career_pa)} career PA off {pitcher_name}"

    if career_pa >= 8:
        return f"there is direct BvP history too, with {int(career_pa)} career PA off {pitcher_name}, even if that sample reads more neutral than decisive"
    return f"there is at least some direct BvP history too, with {int(career_pa)} career PA off {pitcher_name}"


def _cards_hr_target_support_sentence(row: Dict[str, Any]) -> str:
    metrics = row.get("hr_target_metrics") if isinstance(row.get("hr_target_metrics"), dict) else {}
    support_score = _safe_float(row.get("hr_support_score"))
    support_label = _first_text(row.get("hr_support_label")).lower()
    pa_mean = _safe_float(row.get("pa_mean"))
    lineup_order = _safe_int(row.get("lineup_order"))
    lineup_status = _first_text(row.get("lineup_status"), metrics.get("lineupStatus")).lower()
    batter_hand = _first_text(row.get("batter_hand")).upper()
    pitcher_hand = _first_text(row.get("opponent_pitcher_hand")).upper()
    primary_pitch_type = _first_text(metrics.get("primaryPitchType")).upper()

    summary_bits: List[str] = []
    if pa_mean is not None and lineup_order is not None:
        status_prefix = f"{lineup_status} " if lineup_status else ""
        summary_bits.append(f"about {pa_mean:.1f} PA from the {status_prefix}{_cards_hr_target_ordinal(lineup_order)} spot")
    elif pa_mean is not None:
        summary_bits.append(f"about {pa_mean:.1f} PA")

    batter_platoon = _safe_float(metrics.get("batterPlatoonHr"))
    pitcher_platoon = _safe_float(metrics.get("pitcherPlatoonHr"))
    if batter_hand and pitcher_hand:
        if batter_platoon is not None and float(batter_platoon) >= 1.05:
            summary_bits.append(f"a favorable {batter_hand}-on-{pitcher_hand} split for his power")
        elif pitcher_platoon is not None and float(pitcher_platoon) >= 1.05:
            summary_bits.append(f"a pitcher-side {batter_hand}-on-{pitcher_hand} split that is allowing extra damage")

    pitcher_hr_quality = _safe_float(metrics.get("pitcherHrQuality"))
    if pitcher_hr_quality is not None and float(pitcher_hr_quality) >= 1.05:
        summary_bits.append("an opposing starter who is allowing more HR carry than neutral")

    batter_primary_pitch_hr = _safe_float(metrics.get("batterPrimaryPitchHr"))
    pitcher_primary_pitch_hr = _safe_float(metrics.get("pitcherPrimaryPitchHr"))
    if primary_pitch_type and (
        (batter_primary_pitch_hr is not None and float(batter_primary_pitch_hr) >= 1.05)
        or (pitcher_primary_pitch_hr is not None and float(pitcher_primary_pitch_hr) >= 1.05)
    ):
        summary_bits.append(f"a {primary_pitch_type} matchup that grades well for HR damage")

    park_hr = _safe_float(metrics.get("parkHr"))
    if park_hr is not None and float(park_hr) >= 1.03:
        summary_bits.append("a park that is a little better than neutral for HR carry")

    score_text = f"{support_score:.0f}" if support_score is not None else "high"
    if summary_bits:
        return f"He profiles as a {score_text}-grade HR target because the setup is stacking in his favor: {_cards_hr_target_join_phrases(summary_bits)}."
    if support_label in {"strong", "solid"}:
        return f"He profiles as a high-end HR target because the volume, matchup, and environment are all lining up in his favor."
    return f"He stays live as an HR target because the volume, matchup, and environment still give him a real one-swing path."


def _cards_hr_target_writeup(row: Dict[str, Any]) -> str:
    metrics = row.get("hr_target_metrics") if isinstance(row.get("hr_target_metrics"), dict) else {}
    drivers = _cards_hr_target_driver_payload(row)
    highlights = _cards_hr_target_highlights(row)
    pa_mean = _safe_float(row.get("pa_mean"))
    lineup_order = _safe_int(row.get("lineup_order"))
    lineup_status = _first_text(row.get("lineup_status"), metrics.get("lineupStatus"))
    lineup_prefix = f"the {lineup_status} " if lineup_status else "the "
    lineup_slot = _cards_hr_target_ordinal(lineup_order)
    support_label = _first_text(row.get("hr_support_label")).lower()

    volume_bits: List[str] = []
    if pa_mean is not None:
        volume_bits.append(f"about {pa_mean:.1f} PA")
    if lineup_slot:
        volume_bits.append(f"out of {lineup_prefix}{lineup_slot} spot")

    setup_parts: List[str] = []
    if volume_bits:
        setup_parts.append(_cards_hr_target_join_phrases(volume_bits))

    batter_hr_quality = _safe_float(metrics.get("batterHrQuality"))
    if batter_hr_quality is not None and float(batter_hr_quality) >= 1.03:
        setup_parts.append("above-baseline HR contact")

    batter_hand = _first_text(row.get("batter_hand"))
    pitcher_hand = _first_text(row.get("opponent_pitcher_hand"))
    batter_platoon = _safe_float(metrics.get("batterPlatoonHr"))
    pitcher_platoon = _safe_float(metrics.get("pitcherPlatoonHr"))
    handedness_mult = batter_platoon if batter_platoon is not None else pitcher_platoon
    if batter_hand and pitcher_hand and handedness_mult is not None and abs(float(handedness_mult) - 1.0) >= 0.03:
        if float(handedness_mult) > 1.0:
            setup_parts.append("a favorable handedness matchup")
        else:
            setup_parts.append("a tougher handedness matchup than neutral")

    setup_text = _cards_hr_target_join_phrases(setup_parts)
    sentence_one = _cards_hr_target_support_sentence(row)

    driver_phrases = [_cards_hr_target_driver_phrase(driver) for driver in drivers[:3]]
    if driver_phrases:
        sentence_two = f"The biggest modeled lifts are {_cards_hr_target_join_phrases(driver_phrases)}, which is why the one-swing upside is showing up so clearly in the model."
    else:
        sentence_two = "The biggest modeled lifts still come from a mix of contact quality, matchup context, and environment."

    context_parts: List[str] = []
    if setup_text and support_label not in {"strong", "solid"}:
        context_parts.append(setup_text)
    if pa_mean is not None and pa_mean >= 4.4:
        context_parts.append("the plate-appearance floor is high enough that he should still get multiple chances to cash the power upside")
    if lineup_slot and lineup_order is not None and lineup_order <= 4:
        context_parts.append("the lineup slot keeps the volume ceiling elevated")
    if batter_hand and pitcher_hand and handedness_mult is not None and abs(float(handedness_mult) - 1.0) >= 0.03:
        matchup_tone = "leans his way" if float(handedness_mult) > 1.0 else "is a little less friendly than neutral"
        context_parts.append(f"the {batter_hand}-on-{pitcher_hand} matchup {matchup_tone}")
    bvp_support = _cards_hr_target_bvp_support_phrase(row)
    if bvp_support:
        context_parts.append(bvp_support)
    if highlights:
        highlight_text = _cards_hr_target_join_phrases([reason.rstrip(".") for reason in highlights[:2]]).lower()
        if highlight_text:
            context_parts.append(highlight_text)

    if context_parts:
        sentence_three = _cards_hr_target_join_phrases(context_parts)
        sentence_three = sentence_three[:1].upper() + sentence_three[1:] + "."
        return f"{sentence_one} {sentence_two} {sentence_three}"
    return f"{sentence_one} {sentence_two}"


def _cards_payload_context(d: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    context_payload = _payload_cache_get_or_build(
        "cards_api_context",
        str(d),
        max_age_seconds=_cards_context_cache_ttl_seconds_for_date(d),
        builder=lambda: _build_cards_payload_context(d),
    )
    if not isinstance(context_payload, dict):
        return {}, {}, {}
    artifacts = context_payload.get("artifacts") if isinstance(context_payload.get("artifacts"), dict) else {}
    archive = context_payload.get("archive") if isinstance(context_payload.get("archive"), dict) else {}
    game_line_index = context_payload.get("game_line_index") if isinstance(context_payload.get("game_line_index"), dict) else {}
    return artifacts, archive, game_line_index


def _build_cards_payload_context(d: str) -> Dict[str, Any]:
    artifacts = _load_cards_artifacts(d)
    archive = _load_cards_archive_context(d) if _should_load_cards_archive_context(d, artifacts) else {}
    game_line_index = _load_game_line_market_index(d)
    signature = _cards_payload_signature(d, artifacts, archive, game_line_index)
    return {
        "artifacts": artifacts,
        "archive": archive,
        "game_line_index": game_line_index,
        "signature": signature,
    }


def _warm_cards_api_cache(d: str) -> Dict[str, Any]:
    context_payload = _payload_cache_get_or_build(
        "cards_api_context",
        str(d),
        max_age_seconds=_cards_context_cache_ttl_seconds_for_date(d),
        builder=lambda: _build_cards_payload_context(d),
    )
    artifacts = context_payload.get("artifacts") if isinstance(context_payload.get("artifacts"), dict) else {}
    archive = context_payload.get("archive") if isinstance(context_payload.get("archive"), dict) else {}
    game_line_index = context_payload.get("game_line_index") if isinstance(context_payload.get("game_line_index"), dict) else {}
    context_signature = context_payload.get("signature")
    payload = _payload_cache_get_or_build(
        "cards_api",
        str(d),
        signature=context_signature,
        max_age_seconds=_cards_cache_ttl_seconds_for_date(d),
        builder=lambda: _build_cards_api_payload(
            d,
            artifacts=artifacts,
            archive=archive,
            game_line_index=game_line_index,
        ),
    )
    return payload if isinstance(payload, dict) else {}


def _cards_payload_signature(d: str, artifacts: Dict[str, Any], archive: Dict[str, Any], game_line_index: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        str(d),
        _path_signature(artifacts.get("profile_bundle_path") if isinstance(artifacts.get("profile_bundle_path"), Path) else None),
        _path_signature(artifacts.get("hr_targets_path") if isinstance(artifacts.get("hr_targets_path"), Path) else None),
        _path_signature(artifacts.get("locked_policy_path") if isinstance(artifacts.get("locked_policy_path"), Path) else None),
        _path_signature(artifacts.get("game_summary_path") if isinstance(artifacts.get("game_summary_path"), Path) else None),
        _path_signature(artifacts.get("daily_ladders_path") if isinstance(artifacts.get("daily_ladders_path"), Path) else None),
        _path_signature(artifacts.get("settlement_path") if isinstance(artifacts.get("settlement_path"), Path) else None),
        _path_signature(artifacts.get("ops_report_path") if isinstance(artifacts.get("ops_report_path"), Path) else None),
        _path_signature(artifacts.get("lineups_path") if isinstance(artifacts.get("lineups_path"), Path) else None),
        _dir_signature(artifacts.get("sim_dir") if isinstance(artifacts.get("sim_dir"), Path) else None),
        _path_signature(archive.get("report_path") if isinstance(archive.get("report_path"), Path) else None),
        _path_signature(archive.get("card_path") if isinstance(archive.get("card_path"), Path) else None),
        _path_signature(game_line_index.get("path") if isinstance(game_line_index.get("path"), Path) else None),
    )


def _build_cards_api_payload(
    d: str,
    *,
    artifacts: Optional[Dict[str, Any]] = None,
    archive: Optional[Dict[str, Any]] = None,
    game_line_index: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not _is_historical_date(d):
        _maybe_refresh_live_oddsapi_markets(d)

    artifacts = artifacts if isinstance(artifacts, dict) else _load_cards_artifacts(d)
    archive = archive if isinstance(archive, dict) else (_load_cards_archive_context(d) if _should_load_cards_archive_context(d, artifacts) else {})
    game_line_index = game_line_index if isinstance(game_line_index, dict) else _load_game_line_market_index(d)

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

    schedule_games = _schedule_games_for_date(d)
    cards = _cards_list_from_sources(
        d=d,
        schedule_games=schedule_games,
        outputs_by_game=outputs_by_game,
        recos_by_game=recos_by_game,
    )
    _attach_cards_starter_ladder_badges(cards, artifacts.get("daily_ladders"))
    pitcher_market_ctx = _load_pitcher_ladder_market_context(d)
    feed_cache: Dict[int, Optional[Dict[str, Any]]] = {}
    for card in cards:
        if not isinstance(card, dict):
            continue
        game_pk = _safe_int(card.get("gamePk"))
        if game_pk:
            feed = feed_cache.get(int(game_pk))
            if int(game_pk) not in feed_cache:
                feed = _load_live_lens_feed(int(game_pk), d)
                feed_cache[int(game_pk)] = feed
            _supplement_card_status_from_live_feed(card, d, feed=feed)
            _attach_cards_final_starter_ladder_badges(
                card,
                d=d,
                feed=feed,
            )
            _attach_cards_live_starter_ladder_badges(
                card,
                d=d,
                artifacts=artifacts,
                archive=archive,
                feed=feed,
                pitcher_market_ctx=pitcher_market_ctx,
            )
        market_row = _game_line_market_for_card(card, game_line_index)
        card["trackedGameLines"] = (market_row.get("markets") or {}) if isinstance(market_row, dict) else None
    return _cards_api_payload(d, artifacts=artifacts, archive=archive, cards=cards)


def _starter_ladder_badge_from_row(
    row: Optional[Dict[str, Any]],
    *,
    stat_key: Optional[str] = None,
    short_label: str,
    min_hit_prob: float = 0.2,
    include_base_over: bool = True,
    max_rungs: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    market_line = _safe_float(row.get("marketLine"))
    ladder_rows = [entry for entry in (row.get("ladder") or []) if isinstance(entry, dict)]
    if market_line is None or not ladder_rows:
        return None
    base_over_total = int(math.floor(float(market_line))) + 1

    supported_totals: List[int] = []
    last_supported_prob: Optional[float] = None
    for entry in ladder_rows:
        total = _safe_int(entry.get("total"))
        hit_prob = _safe_float(entry.get("hitProb"))
        if total is None or hit_prob is None:
            continue
        if float(total) <= float(market_line):
            continue
        if not include_base_over and int(total) <= int(base_over_total):
            continue
        if float(hit_prob) < float(min_hit_prob):
            continue
        supported_totals.append(int(total))
        last_supported_prob = float(hit_prob)

    if max_rungs is not None and int(max_rungs) > 0 and len(supported_totals) > int(max_rungs):
        supported_totals = supported_totals[: int(max_rungs)]
        last_supported_prob = _safe_float(
            next(
                (
                    entry.get("hitProb")
                    for entry in ladder_rows
                    if _safe_int(entry.get("total")) == int(supported_totals[-1])
                ),
                last_supported_prob,
            )
        )

    if not supported_totals or last_supported_prob is None:
        return None

    tone = "soft"
    if float(last_supported_prob) >= 0.35:
        tone = "strong"
    elif float(last_supported_prob) >= 0.25:
        tone = "solid"

    supported_label = "/".join(str(int(total)) for total in supported_totals)
    if short_label == "O" and len(supported_totals) > 1:
        label = f"{short_label} {supported_label}"
    elif len(supported_totals) == 1:
        label = f"{short_label} up to {int(supported_totals[0])}"
    else:
        label = f"{short_label} up to {int(supported_totals[-1])}"

    detail_parts: List[str] = []
    matchup_summary = str(row.get("matchupSummary") or "").strip()
    if matchup_summary:
        detail_parts.append(matchup_summary)
    detail_parts.append(f"Supported ladders: {supported_label}")
    detail_parts.append(f"Last supported rung: {int(supported_totals[-1])}")

    return {
        "label": label,
        "stat": str(stat_key or "").strip().lower() or None,
        "target": int(supported_totals[-1]),
        "targets": supported_totals,
        "hitProb": round(float(last_supported_prob), 3),
        "tone": tone,
        "detail": " ".join(detail_parts).strip(),
    }


def _starter_ladder_badge_from_supported_totals(
    supported_totals: List[int],
    *,
    stat_key: Optional[str] = None,
    short_label: str,
    last_supported_prob: Optional[float],
    detail_parts: Optional[List[str]] = None,
    max_rungs: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    cleaned = [int(total) for total in supported_totals if _safe_int(total) is not None]
    if max_rungs is not None and int(max_rungs) > 0 and len(cleaned) > int(max_rungs):
        cleaned = cleaned[: int(max_rungs)]
    if not cleaned:
        return None

    probability = _safe_float(last_supported_prob)
    tone = "soft"
    if probability is not None:
        if float(probability) >= 0.35:
            tone = "strong"
        elif float(probability) >= 0.25:
            tone = "solid"

    supported_label = "/".join(str(int(total)) for total in cleaned)
    if short_label == "O" and len(cleaned) > 1:
        label = f"{short_label} {supported_label}"
    elif len(cleaned) == 1:
        label = f"{short_label} up to {int(cleaned[0])}"
    else:
        label = f"{short_label} up to {int(cleaned[-1])}"

    parts = [str(part).strip() for part in (detail_parts or []) if str(part).strip()]
    parts.append(f"Supported ladders: {supported_label}")
    parts.append(f"Last supported rung: {int(cleaned[-1])}")

    out: Dict[str, Any] = {
        "label": label,
        "stat": str(stat_key or "").strip().lower() or None,
        "target": int(cleaned[-1]),
        "targets": cleaned,
        "tone": tone,
        "detail": " ".join(parts).strip(),
    }
    if probability is not None:
        out["hitProb"] = round(float(probability), 3)
    return out


def _live_pitcher_ladder_market_candidates(market: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(market, dict):
        return []

    candidates: List[Dict[str, Any]] = []
    base_line = _safe_float(market.get("line"))
    if base_line is not None:
        candidates.append(
            {
                "line": float(base_line),
                "over_odds": _safe_int(market.get("over_odds")),
                "under_odds": _safe_int(market.get("under_odds")),
            }
        )

    for alt in (market.get("alternates") or []):
        if not isinstance(alt, dict):
            continue
        line_value = _safe_float(alt.get("line"))
        if line_value is None:
            continue
        candidates.append(
            {
                "line": float(line_value),
                "over_odds": _safe_int(alt.get("over_odds")),
                "under_odds": _safe_int(alt.get("under_odds")),
            }
        )

    deduped: Dict[float, Dict[str, Any]] = {}
    for item in candidates:
        line_value = _safe_float(item.get("line"))
        if line_value is None:
            continue
        deduped[float(line_value)] = dict(item)
    return [deduped[key] for key in sorted(deduped.keys())]


def _live_starter_ladder_badges_for_side(
    *,
    side: str,
    snapshot: Optional[Dict[str, Any]],
    sim_context: Optional[Dict[str, Any]],
    pitcher_market_ctx: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if side not in {"away", "home"}:
        return []
    if not isinstance(snapshot, dict) or not isinstance(sim_context, dict) or not sim_context.get("found"):
        return []
    if _starter_removed_from_snapshot(snapshot, side):
        return []

    actual_teams = (snapshot.get("teams") or {}) if isinstance(snapshot.get("teams"), dict) else {}
    starter = (((actual_teams.get(side) or {}).get("starter")) or {}) if isinstance(actual_teams.get(side), dict) else {}
    starter_name = _first_text(starter.get("name"))
    starter_key = normalize_pitcher_name(starter_name)
    if not starter_name or not starter_key:
        return []

    pitcher_models = _sim_prop_models(sim_context, "pitchers")
    model_entry = _live_pitcher_model_entry(pitcher_models, team_side=side, starter_name=starter_name)
    market_lines = (
        pitcher_market_ctx.get("displayLines")
        if isinstance(pitcher_market_ctx, dict) and isinstance(pitcher_market_ctx.get("displayLines"), dict)
        else {}
    )
    market_entry = market_lines.get(starter_key) if starter_key else None
    if not isinstance(model_entry, dict) or not isinstance(market_entry, dict):
        return []

    actual_row = _lookup_boxscore_row((((actual_teams.get(side) or {}).get("boxscore") or {}).get("pitching") or []), starter_name)
    pitcher_ctx = _live_pitcher_matchup_context({"team_side": side}, snapshot, sim_context)
    pitcher_profile = pitcher_ctx.get("pitcher_profile") if isinstance(pitcher_ctx.get("pitcher_profile"), dict) else None
    current_profile = pitcher_ctx.get("current_profile") if isinstance(pitcher_ctx.get("current_profile"), dict) else None
    bullpen_profiles = pitcher_ctx.get("bullpen_profiles") if isinstance(pitcher_ctx.get("bullpen_profiles"), list) else []
    model_row = model_entry.get("model") or {}
    progress_fraction = float((_live_game_progress(snapshot).get("fraction") or 0.0))
    live_projection_slack = 1.0

    badges: List[Dict[str, Any]] = []
    for prop_key, short_label in (("strikeouts", "K"), ("outs", "O")):
        cfg = _PITCHER_LADDER_PROPS.get(prop_key) or {}
        market_key = str(cfg.get("market_key") or "").strip()
        dist_key = str(cfg.get("dist_key") or "").strip()
        mean_key = str(cfg.get("mean_key") or "").strip()
        min_hit_prob = float(_safe_float(cfg.get("ladder_min_hit_prob")) or 0.2)
        max_rungs = _safe_int(cfg.get("ladder_max_rungs"))
        market = market_entry.get(market_key) if market_key else None
        if not isinstance(market, dict) or not dist_key:
            continue

        base_line = _safe_float(market.get("line"))
        if base_line is None:
            continue
        base_over_total = int(math.floor(float(base_line))) + 1
        model_mean = _safe_float(model_row.get(mean_key)) if mean_key else None
        actual_value = _live_stat_value(actual_row, {"market": "pitcher_props", "prop": prop_key})
        live_projection = _project_live_pitcher_value(
            prop=prop_key,
            team_side=side,
            actual_value=actual_value,
            model_mean=model_mean,
            progress_fraction=progress_fraction,
            actual_row=actual_row,
            model_row=model_row,
            pitcher_profile=pitcher_profile,
            current_profile=current_profile,
            bullpen_profiles=bullpen_profiles,
            snapshot=snapshot,
        )

        supported_totals: List[int] = []
        last_supported_prob: Optional[float] = None
        for candidate in _live_pitcher_ladder_market_candidates(market):
            line_value = _safe_float(candidate.get("line"))
            if line_value is None:
                continue
            target_total = int(math.floor(float(line_value))) + 1
            if actual_value is not None and int(target_total) <= int(math.floor(float(actual_value))):
                continue
            model_prob_over = _prob_over_line_from_dist(model_row.get(dist_key) or {}, float(line_value))
            if model_prob_over is None or float(model_prob_over) < float(min_hit_prob):
                continue
            if live_projection is None or float(live_projection) + float(live_projection_slack) < float(target_total):
                continue
            supported_totals.append(int(target_total))
            last_supported_prob = float(model_prob_over)

        if short_label == "O":
            higher_alts = [int(total) for total in supported_totals if int(total) > int(base_over_total)]
            if higher_alts:
                supported_totals = higher_alts
        if max_rungs is not None and int(max_rungs) > 0 and len(supported_totals) > int(max_rungs):
            supported_totals = supported_totals[: int(max_rungs)]
        if not supported_totals:
            continue

        detail_parts: List[str] = []
        if live_projection is not None:
            if actual_value is not None:
                detail_parts.append(f"Live projection {float(live_projection):.1f} from current {float(actual_value):.1f}.")
            else:
                detail_parts.append(f"Live projection {float(live_projection):.1f}.")
        detail_parts.append("Starter still active in live game state.")

        badge = _starter_ladder_badge_from_supported_totals(
            supported_totals,
            stat_key=prop_key,
            short_label=short_label,
            last_supported_prob=last_supported_prob,
            detail_parts=detail_parts,
            max_rungs=max_rungs,
        )
        if isinstance(badge, dict):
            badge["source"] = "live"
            badges.append(badge)
    return badges


def _attach_cards_live_starter_ladder_badges(
    card: Dict[str, Any],
    *,
    d: str,
    artifacts: Optional[Dict[str, Any]],
    archive: Optional[Dict[str, Any]],
    feed: Optional[Dict[str, Any]],
    pitcher_market_ctx: Optional[Dict[str, Any]],
) -> None:
    if not isinstance(card, dict):
        return
    if not _status_is_live(card.get("status") if isinstance(card.get("status"), dict) else {}):
        return
    probable = card.get("probable") if isinstance(card.get("probable"), dict) else None
    if not isinstance(probable, dict):
        return
    game_pk = _safe_int(card.get("gamePk"))
    if game_pk is None or int(game_pk) <= 0:
        return

    snapshot = _load_live_lens_snapshot(int(game_pk), str(d), feed=feed)
    sim_context = _load_sim_context_for_game(int(game_pk), str(d), artifacts=artifacts, archive=archive, feed=feed)
    for side in ("away", "home"):
        entry = probable.get(side)
        if not isinstance(entry, dict):
            continue
        live_badges = _live_starter_ladder_badges_for_side(
            side=side,
            snapshot=snapshot,
            sim_context=sim_context,
            pitcher_market_ctx=pitcher_market_ctx,
        )
        if live_badges:
            entry["ladderBadges"] = live_badges
        else:
            entry.pop("ladderBadges", None)


def _starter_ladder_badge_stat_key(badge: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(badge, dict):
        return None
    stat_key = str(badge.get("stat") or "").strip().lower()
    if stat_key in _PITCHER_LADDER_PROPS:
        return stat_key
    label = str(badge.get("label") or "").strip().upper()
    if label.startswith("K"):
        return "strikeouts"
    if label.startswith("O"):
        return "outs"
    return None


def _final_starter_ladder_badges_for_side(
    *,
    side: str,
    entry: Optional[Dict[str, Any]],
    feed: Optional[Dict[str, Any]],
    stats_cache: Dict[Tuple[int, str, str, str], Optional[Dict[str, Any]]],
    game_pk: int,
) -> List[Dict[str, Any]]:
    if side not in {"away", "home"} or not isinstance(entry, dict) or not isinstance(feed, dict):
        return []
    ladder_badges = [badge for badge in (entry.get("ladderBadges") or []) if isinstance(badge, dict)]
    if not ladder_badges:
        return []

    starter_name = _first_text(entry.get("fullName"), entry.get("name"))
    if not starter_name:
        return []
    stats = _top_props_player_stats(
        feed=feed,
        player_name=starter_name,
        stat_group="pitching",
        side_hint=side,
        cache=stats_cache,
        game_pk=int(game_pk),
    )
    if not isinstance(stats, dict):
        return []

    grouped_badges: Dict[str, Dict[str, Any]] = {}
    for badge in ladder_badges:
        stat_key = _starter_ladder_badge_stat_key(badge)
        actual_key = _TOP_PROPS_PITCHER_ACTUAL_KEYS.get(str(stat_key or ""))
        if not actual_key:
            continue
        actual_value = _safe_float(stats.get(actual_key))
        if actual_value is None:
            continue

        targets = [int(total) for total in (badge.get("targets") or []) if _safe_int(total) is not None]
        if not targets:
            target_total = _safe_int(badge.get("target"))
            if target_total is not None:
                targets = [int(target_total)]
        if not targets:
            continue

        wins = sum(1 for total in targets if float(actual_value) + 1e-9 >= float(total))
        losses = max(0, len(targets) - int(wins))
        short_label = "K" if stat_key == "strikeouts" else "O" if stat_key == "outs" else str(badge.get("label") or "").split(" ", 1)[0]
        stat_label = str((_PITCHER_LADDER_PROPS.get(str(stat_key or "")) or {}).get("label") or short_label).strip()
        supported_label = "/".join(str(int(total)) for total in targets)
        entry = grouped_badges.setdefault(
            str(stat_key or short_label),
            {
                "short_label": short_label,
                "stat": stat_key,
                "stat_label": stat_label,
                "actual": int(round(float(actual_value))),
                "targets": [],
            },
        )
        existing_targets = {int(total) for total in (entry.get("targets") or []) if _safe_int(total) is not None}
        for total in targets:
            if int(total) not in existing_targets:
                cast_targets = entry.get("targets") if isinstance(entry.get("targets"), list) else []
                cast_targets.append(int(total))
                entry["targets"] = cast_targets
                existing_targets.add(int(total))

    resolved_badges: List[Dict[str, Any]] = []
    for entry in grouped_badges.values():
        targets = sorted(int(total) for total in (entry.get("targets") or []) if _safe_int(total) is not None)
        if not targets:
            continue
        actual_value = int(entry.get("actual") or 0)
        wins = sum(1 for total in targets if float(actual_value) + 1e-9 >= float(total))
        losses = max(0, len(targets) - int(wins))
        short_label = str(entry.get("short_label") or "").strip() or str(entry.get("stat") or "").strip() or "L"
        stat_label = str(entry.get("stat_label") or short_label).strip()
        supported_label = "/".join(str(int(total)) for total in targets)
        total_rungs = len(targets)
        tone = "win" if losses == 0 else "loss" if wins == 0 else "split"
        if tone == "win":
            label = f"{short_label} +{int(wins)} ({int(actual_value)})"
        elif tone == "loss":
            label = f"{short_label} -{int(losses)} ({int(actual_value)})"
        else:
            label = f"{short_label} {int(wins)}/{int(total_rungs)} ({int(actual_value)})"
        detail = (
            f"Final {stat_label}: {int(actual_value)}. "
            f"Supported ladders: {supported_label}. Correct ladders: {int(wins)}. Missed ladders: {int(losses)}."
        )
        resolved_badges.append(
            {
                "label": label,
                "stat": entry.get("stat"),
                "tone": tone,
                "detail": detail,
                "count": int(wins if tone != "loss" else losses),
                "wins": int(wins),
                "losses": int(losses),
                "targetCount": int(total_rungs),
                "actual": int(actual_value),
                "targets": targets,
                "source": "final",
            }
        )
    return resolved_badges


def _attach_cards_final_starter_ladder_badges(
    card: Dict[str, Any],
    *,
    d: str,
    feed: Optional[Dict[str, Any]],
) -> None:
    if not isinstance(card, dict):
        return
    if not _status_is_final(card.get("status") if isinstance(card.get("status"), dict) else {}):
        return
    probable = card.get("probable") if isinstance(card.get("probable"), dict) else None
    if not isinstance(probable, dict):
        return
    game_pk = _safe_int(card.get("gamePk"))
    if game_pk is None or int(game_pk) <= 0:
        return

    settlement_feed = dict(feed) if isinstance(feed, dict) and _settlement_feed_is_final(feed) else None
    if not isinstance(settlement_feed, dict):
        try:
            loaded_feed = _load_settlement_feed(str(d), int(game_pk))
        except Exception:
            loaded_feed = None
        settlement_feed = dict(loaded_feed) if isinstance(loaded_feed, dict) and _settlement_feed_is_final(loaded_feed) else None

    stats_cache: Dict[Tuple[int, str, str, str], Optional[Dict[str, Any]]] = {}
    for side in ("away", "home"):
        entry = probable.get(side)
        if not isinstance(entry, dict):
            continue
        if not isinstance(settlement_feed, dict):
            entry.pop("ladderBadges", None)
            continue
        settled_badges = _final_starter_ladder_badges_for_side(
            side=side,
            entry=entry,
            feed=settlement_feed,
            stats_cache=stats_cache,
            game_pk=int(game_pk),
        )
        if settled_badges:
            entry["ladderBadges"] = settled_badges
        else:
            entry.pop("ladderBadges", None)


def _starter_ladder_badges_for_pitcher(
    game_groups: Optional[Dict[str, Any]],
    *,
    game_pk: Optional[int],
    pitcher_id: Optional[int],
    pitcher_name: str,
) -> List[Dict[str, Any]]:
    if not isinstance(game_groups, dict) or game_pk is None or int(game_pk) <= 0:
        return []

    def _resolve_row(group_key: str) -> Optional[Dict[str, Any]]:
        group = game_groups.get(group_key)
        rows = (group or {}).get("rows") if isinstance(group, dict) else None
        if not isinstance(rows, list):
            return None
        normalized_target_name = normalize_pitcher_name(pitcher_name)
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_game_pk = _safe_int(row.get("gamePk"))
            if row_game_pk is None or int(row_game_pk) != int(game_pk):
                continue
            row_pitcher_id = _safe_int(row.get("pitcherId"))
            if pitcher_id is not None and row_pitcher_id is not None and int(row_pitcher_id) == int(pitcher_id):
                return row
            if normalized_target_name and normalize_pitcher_name(row.get("pitcherName")) == normalized_target_name:
                return row
        return None

    badges: List[Dict[str, Any]] = []
    strikeout_badge = _starter_ladder_badge_from_row(
        _resolve_row("strikeouts"),
        stat_key="strikeouts",
        short_label="K",
        min_hit_prob=float(_safe_float((_PITCHER_LADDER_PROPS.get("strikeouts") or {}).get("ladder_min_hit_prob")) or 0.2),
        max_rungs=_safe_int((_PITCHER_LADDER_PROPS.get("strikeouts") or {}).get("ladder_max_rungs")),
    )
    if isinstance(strikeout_badge, dict):
        badges.append(strikeout_badge)
    outs_badge = _starter_ladder_badge_from_row(
        _resolve_row("outs"),
        stat_key="outs",
        short_label="O",
        min_hit_prob=float(_safe_float((_PITCHER_LADDER_PROPS.get("outs") or {}).get("ladder_min_hit_prob")) or 0.2),
        include_base_over=False,
        max_rungs=_safe_int((_PITCHER_LADDER_PROPS.get("outs") or {}).get("ladder_max_rungs")),
    )
    if isinstance(outs_badge, dict):
        badges.append(outs_badge)
    return badges


def daily_ladder_audit_artifact_path(d: str, *, data_root: Optional[Path] = None) -> Path:
    root = data_root.resolve() if isinstance(data_root, Path) else _DATA_DIR
    return root / "daily" / "ladders" / f"daily_ladder_audit_{_date_slug(d)}.json"


def build_daily_ladder_audit_artifact(d: str) -> Dict[str, Any]:
    date_str = str(d or "").strip()
    payload = _build_cards_api_payload(date_str)
    cards = [card for card in (payload.get("cards") or []) if isinstance(card, dict)]
    badge_rows: List[Dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    tone_counts: Counter[str] = Counter()
    stat_counts: Counter[str] = Counter()
    stat_rung_wins: Counter[str] = Counter()
    stat_rung_losses: Counter[str] = Counter()
    loss_size_counts: Counter[int] = Counter()
    win_size_counts: Counter[int] = Counter()
    largest_losses: List[Dict[str, Any]] = []
    mixed_rows: List[Dict[str, Any]] = []

    for card in cards:
        game_pk = _safe_int(card.get("gamePk"))
        probable = card.get("probable") if isinstance(card.get("probable"), dict) else {}
        away_info = card.get("away") if isinstance(card.get("away"), dict) else {}
        home_info = card.get("home") if isinstance(card.get("home"), dict) else {}
        matchup = f"{str(away_info.get('abbrev') or away_info.get('team') or '?').strip()} @ {str(home_info.get('abbrev') or home_info.get('team') or '?').strip()}"
        status = card.get("status") if isinstance(card.get("status"), dict) else {}
        status_text = str(status.get("display") or status.get("gameState") or "").strip()
        for side in ("away", "home"):
            entry = probable.get(side) if isinstance(probable, dict) else None
            if not isinstance(entry, dict):
                continue
            starter_name = _first_text(entry.get("fullName"), entry.get("name"))
            for badge in (entry.get("ladderBadges") or []):
                if not isinstance(badge, dict):
                    continue
                stat_key = str(badge.get("stat") or "unknown").strip().lower() or "unknown"
                tone = str(badge.get("tone") or "none").strip().lower() or "none"
                source = str(badge.get("source") or "none").strip().lower() or "none"
                wins = int(_safe_int(badge.get("wins")) or 0)
                losses = int(_safe_int(badge.get("losses")) or 0)
                target_count = int(_safe_int(badge.get("targetCount")) or len([t for t in (badge.get("targets") or []) if _safe_int(t) is not None]))
                row = {
                    "gamePk": int(game_pk) if game_pk is not None else None,
                    "matchup": matchup,
                    "status": status_text,
                    "side": side,
                    "starter": starter_name,
                    "label": str(badge.get("label") or "").strip(),
                    "stat": stat_key,
                    "tone": tone,
                    "source": source,
                    "actual": _safe_int(badge.get("actual")),
                    "wins": int(wins),
                    "losses": int(losses),
                    "targetCount": int(target_count),
                    "targets": [int(total) for total in (badge.get("targets") or []) if _safe_int(total) is not None],
                    "detail": str(badge.get("detail") or "").strip(),
                }
                badge_rows.append(row)
                source_counts[source] += 1
                tone_counts[tone] += 1
                stat_counts[stat_key] += 1
                stat_rung_wins[stat_key] += int(wins)
                stat_rung_losses[stat_key] += int(losses)
                if int(losses) > 0:
                    loss_size_counts[int(losses)] += 1
                    largest_losses.append(row)
                if int(wins) > 0:
                    win_size_counts[int(wins)] += 1
                if int(wins) > 0 and int(losses) > 0:
                    mixed_rows.append(row)

    final_rows = [row for row in badge_rows if str(row.get("source") or "") == "final"]
    final_badges = len(final_rows)
    final_wins = sum(int(row.get("wins") or 0) for row in final_rows)
    final_losses = sum(int(row.get("losses") or 0) for row in final_rows)
    final_rungs = final_wins + final_losses
    return {
        "date": date_str,
        "generatedAt": _local_timestamp_text(),
        "summary": {
            "cards": int(len(cards)),
            "badges": int(len(badge_rows)),
            "sources": dict(source_counts),
            "tones": dict(tone_counts),
            "stats": {
                stat_key: {
                    "badges": int(stat_counts.get(stat_key) or 0),
                    "rungWins": int(stat_rung_wins.get(stat_key) or 0),
                    "rungLosses": int(stat_rung_losses.get(stat_key) or 0),
                }
                for stat_key in sorted(stat_counts.keys())
            },
            "final": {
                "badges": int(final_badges),
                "rungWins": int(final_wins),
                "rungLosses": int(final_losses),
                "rungHitRate": round(float(final_wins) / float(final_rungs), 4) if final_rungs > 0 else None,
                "mixedBadgeCount": int(len([row for row in final_rows if int(row.get("wins") or 0) > 0 and int(row.get("losses") or 0) > 0])),
            },
            "lossSizeCounts": {str(int(size)): int(count) for size, count in sorted(loss_size_counts.items())},
            "winSizeCounts": {str(int(size)): int(count) for size, count in sorted(win_size_counts.items())},
        },
        "largestLosses": sorted(largest_losses, key=lambda row: (-int(row.get("losses") or 0), str(row.get("starter") or "")))[:10],
        "mixedFinalBadges": [row for row in final_rows if int(row.get("wins") or 0) > 0 and int(row.get("losses") or 0) > 0][:10],
        "rows": badge_rows,
    }


def write_daily_ladder_audit_artifact(d: str, *, out_path: Optional[Path] = None) -> Dict[str, Any]:
    date_str = str(d or "").strip()
    destination = out_path.resolve() if isinstance(out_path, Path) else daily_ladder_audit_artifact_path(date_str)
    artifact = build_daily_ladder_audit_artifact(date_str)
    _write_json_file(destination, artifact)
    return {
        "date": date_str,
        "path": destination,
        "summary": dict(artifact.get("summary") or {}),
    }


def _attach_cards_starter_ladder_badges(cards: Any, daily_ladders: Any) -> None:
    if not isinstance(cards, list) or not isinstance(daily_ladders, dict):
        return
    groups = daily_ladders.get("groups") if isinstance(daily_ladders.get("groups"), dict) else {}
    pitcher_groups = groups.get("pitcher") if isinstance(groups.get("pitcher"), dict) else None
    if not isinstance(pitcher_groups, dict):
        return

    for card in cards:
        if not isinstance(card, dict):
            continue
        game_pk = _safe_int(card.get("gamePk"))
        probable = card.get("probable") if isinstance(card.get("probable"), dict) else None
        if game_pk is None or not isinstance(probable, dict):
            continue
        for side in ("away", "home"):
            entry = probable.get(side)
            if not isinstance(entry, dict):
                continue
            starter_name = _first_text(entry.get("fullName"), entry.get("name"))
            starter_id = _safe_int(entry.get("id"))
            badges = _starter_ladder_badges_for_pitcher(
                pitcher_groups,
                game_pk=int(game_pk),
                pitcher_id=starter_id,
                pitcher_name=starter_name,
            )
            if badges:
                entry["ladderBadges"] = badges


@app.get("/api/cards")
def api_cards() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    try:
        context_payload = _payload_cache_get_or_build(
            "cards_api_context",
            str(d),
            max_age_seconds=_cards_context_cache_ttl_seconds_for_date(d),
            builder=lambda: _build_cards_payload_context(d),
        )
        artifacts = context_payload.get("artifacts") if isinstance(context_payload.get("artifacts"), dict) else {}
        archive = context_payload.get("archive") if isinstance(context_payload.get("archive"), dict) else {}
        game_line_index = context_payload.get("game_line_index") if isinstance(context_payload.get("game_line_index"), dict) else {}
        context_signature = context_payload.get("signature")
        payload = _payload_cache_get_or_build(
            "cards_api",
            str(d),
            signature=context_signature,
            max_age_seconds=_cards_cache_ttl_seconds_for_date(d),
            builder=lambda: _build_cards_api_payload(
                d,
                artifacts=artifacts,
                archive=archive,
                game_line_index=game_line_index,
            ),
        )
        return jsonify(payload)
    except Exception as exc:
        app.logger.exception("cards api failed for %s", d)
        try:
            schedule_games = _schedule_games_for_date(d)
        except Exception:
            schedule_games = []
        fallback_cards = _cards_list_from_sources(
            d=d,
            schedule_games=schedule_games,
            outputs_by_game={},
            recos_by_game={},
        )
        payload = _cards_api_payload(
            d,
            artifacts={},
            archive={},
            cards=fallback_cards,
            fallback_error=f"cards_api_fallback: {type(exc).__name__}: {exc}",
        )
        return jsonify(payload)


@app.get("/api/pitcher-ladders")
def api_pitcher_ladders() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _pitcher_ladders_payload_cached(
        d,
        request.args.get("prop"),
        request.args.get("sort"),
        selected_game_value=request.args.get("game"),
        selected_pitcher_value=request.args.get("pitcher"),
    )
    status_code = 200 if payload.get("found") else 404
    return jsonify(payload), status_code


@app.get("/api/hitter-ladders")
def api_hitter_ladders() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _hitter_ladders_payload_cached(
        d,
        request.args.get("prop"),
        selected_game_value=request.args.get("game"),
        selected_team_value=request.args.get("team"),
        selected_hitter_value=request.args.get("hitter"),
        sort_value=request.args.get("sort"),
    )
    status_code = 200 if payload.get("found") else 404
    return jsonify(payload), status_code


@app.get("/api/hr-targets")
def api_hr_targets() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _with_app_build(
        _daily_hr_targets_payload_cached(
            d,
            selected_game_value=request.args.get("game"),
            selected_team_value=request.args.get("team"),
            selected_hitter_value=request.args.get("hitter"),
            sort_value=request.args.get("sort"),
        )
    )
    status_code = 200 if payload.get("found") else 404
    return jsonify(payload), status_code


@app.get("/api/pitcher-top-props")
def api_pitcher_top_props() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _with_app_build(_daily_top_props_payload(d, "pitcher", request.args.get("limit")))
    return jsonify(payload)


@app.get("/api/hitter-top-props")
def api_hitter_top_props() -> Response:
    d = str(request.args.get("date") or "").strip() or _default_cards_date()
    payload = _with_app_build(_daily_top_props_payload(d, "hitter", request.args.get("limit")))
    return jsonify(payload)


@app.get("/api/season/<int:season>")
def api_season_manifest(season: int) -> Response:
    prebuilt_payload = _prebuilt_season_manifest_payload(int(season))
    if isinstance(prebuilt_payload, dict):
        return jsonify(_with_app_build(prebuilt_payload))

    manifest_path, manifest = _load_season_manifest(int(season))
    if not manifest_path or not isinstance(manifest, dict):
        return jsonify(
            _with_app_build(
                {
                    "season": int(season),
                    "found": False,
                    "error": "season_manifest_missing",
                }
            )
        ), 404

    payload = _supplement_season_manifest_payload(int(season), dict(manifest))
    meta = dict(payload.get("meta") or {})
    sources = dict(meta.get("sources") or {})
    sources["manifest"] = _relative_path_str(manifest_path)
    meta["sources"] = sources
    meta["app"] = dict(_APP_BUILD_INFO)
    payload["meta"] = meta
    payload["found"] = True
    return jsonify(_with_app_build(payload))


@app.get("/api/season/<int:season>/betting-cards")
def api_season_betting_cards(season: int) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    profile_name, manifest_path, manifest, available_profiles = _load_season_betting_manifest(
        int(season),
        requested_profile,
    )
    if not manifest_path or not isinstance(manifest, dict):
        return _jsonify_no_store(
            _with_app_build(
                {
                    "season": int(season),
                    "profile": profile_name,
                    "found": False,
                    "available_profiles": available_profiles,
                    "error": "season_betting_cards_missing",
                }
            ),
            404,
        )

    payload = _season_betting_manifest_response_payload(
        int(season),
        profile_name,
        manifest_path,
        manifest,
        available_profiles,
    )
    meta = dict(payload.get("meta") or {})
    meta["app"] = dict(_APP_BUILD_INFO)
    payload["meta"] = meta
    return _jsonify_no_store(_with_app_build(payload))


@app.get("/api/season/<int:season>/betting-card")
def api_season_official_betting_card(season: int) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    profile_name, manifest_path, manifest, available_profiles = _load_season_betting_manifest(
        int(season),
        requested_profile,
    )
    if not manifest_path or not isinstance(manifest, dict):
        return _jsonify_no_store(
            _with_app_build(
                {
                    "season": int(season),
                    "profile": profile_name,
                    "found": False,
                    "available_profiles": available_profiles,
                    "error": "season_betting_cards_missing",
                }
            ),
            404,
        )

    payload = _official_betting_card_manifest_response_payload(
        int(season),
        profile_name,
        manifest_path,
        manifest,
        available_profiles,
    )
    meta = dict(payload.get("meta") or {})
    meta["app"] = dict(_APP_BUILD_INFO)
    payload["meta"] = meta
    return _jsonify_no_store(_with_app_build(payload))


@app.get("/api/season/<int:season>/betting-cards/day/<date_str>")
def api_season_betting_cards_day(season: int, date_str: str) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    prebuilt_payload = _prebuilt_season_betting_day_payload(int(season), str(date_str), requested_profile)
    if isinstance(prebuilt_payload, dict):
        return _jsonify_no_store(_with_app_build(prebuilt_payload))

    payload = _season_betting_day_payload(int(season), str(date_str), requested_profile)
    if payload.get("found"):
        card_path = _path_from_maybe_relative(payload.get("card_source"))
        payload["card"] = _load_json_file(card_path)
        return _jsonify_no_store(_with_app_build(payload))

    return _jsonify_no_store(_with_app_build(payload), 404)


@app.get("/api/season/<int:season>/betting-card/day/<date_str>")
def api_season_official_betting_card_day(season: int, date_str: str) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    prebuilt_payload = _prebuilt_official_betting_card_day_payload(int(season), str(date_str), requested_profile)
    if isinstance(prebuilt_payload, dict):
        return _jsonify_no_store(_with_app_build(prebuilt_payload))

    payload = _official_betting_card_day_payload(int(season), str(date_str), requested_profile)
    if payload.get("found"):
        return _jsonify_no_store(_with_app_build(payload))

    return _jsonify_no_store(_with_app_build(payload), 404)


@app.get("/api/season/<int:season>/day/<date_str>")
def api_season_day(season: int, date_str: str) -> Response:
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    prebuilt_payload = _prebuilt_season_day_payload(int(season), str(date_str), requested_profile)
    if isinstance(prebuilt_payload, dict):
        return _jsonify_no_store(_with_app_build(prebuilt_payload))

    manifest_path, manifest = _load_season_manifest(int(season))
    if not manifest_path or not isinstance(manifest, dict):
        return _jsonify_no_store(
            _with_app_build(
                {
                    "season": int(season),
                    "date": str(date_str),
                    "found": False,
                    "error": "season_manifest_missing",
                }
            ),
            404,
        )

    report_path = _resolve_season_day_report_path(manifest, str(date_str))
    if not report_path or not report_path.exists() or not report_path.is_file():
        fallback_payload = _season_day_fallback_payload(int(season), str(date_str), requested_profile)
        if fallback_payload.get("cards_available") or ((fallback_payload.get("betting") or {}).get("found")):
            fallback_payload["found"] = True
            fallback_payload["manifest_source"] = _relative_path_str(manifest_path)
            return _jsonify_no_store(_with_app_build(fallback_payload))
        return _jsonify_no_store(
            _with_app_build(
                {
                    "season": int(season),
                    "date": str(date_str),
                    "found": False,
                    "error": "season_day_missing",
                }
            ),
            404,
        )

    report_obj = _load_json_file(report_path)
    if not isinstance(report_obj, dict):
        return _jsonify_no_store(
            _with_app_build(
                {
                    "season": int(season),
                    "date": str(date_str),
                    "found": False,
                    "error": "season_day_read_failed",
                }
            ),
            500,
        )

    payload = _season_day_payload(
        season=int(season),
        season_manifest=manifest,
        day_report=report_obj,
        report_path=report_path,
        betting_profile=requested_profile,
    )
    payload["found"] = True
    payload["manifest_source"] = _relative_path_str(manifest_path)
    return _jsonify_no_store(_with_app_build(payload))


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
    feed = _load_live_lens_feed(int(game_pk), d)
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


def _build_game_card_detail_payload(game_pk: int, d: str) -> Dict[str, Any]:
    feed = _load_live_lens_feed(int(game_pk), d)
    if not isinstance(feed, dict) or not feed:
        return {
            "gamePk": int(game_pk),
            "date": d or None,
            "found": False,
            "error": "missing_feed",
            "snapshot": None,
            "sim": {"found": False, "error": "missing_feed"},
        }

    away_sp = _get_box_starting_pitcher_id(feed, "away")
    home_sp = _get_box_starting_pitcher_id(feed, "home")
    snapshot = {
        "gamePk": int(game_pk),
        "date": d or None,
        "archived": bool(_is_historical_date(d)),
        "streamAvailable": bool(not _is_historical_date(d) and d == _today_iso()),
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
    sim = _build_game_sim_payload(int(game_pk), d, feed=feed)
    return {
        "gamePk": int(game_pk),
        "date": d or None,
        "found": True,
        "generatedAt": _local_timestamp_text(),
        "snapshot": snapshot,
        "sim": sim,
    }


@app.get("/api/game/<int:game_pk>/card-detail")
def api_game_card_detail(game_pk: int) -> Response:
    d = str(request.args.get("date") or "").strip()
    if not d:
        return jsonify({"gamePk": int(game_pk), "found": False, "error": "missing_date"}), 400
    out = _payload_cache_get_or_build(
        "game_card_detail_api",
        f"{str(d)}:{int(game_pk)}",
        max_age_seconds=_LIVE_ROUTE_CACHE_TTL_SECONDS,
        builder=lambda: _build_game_card_detail_payload(int(game_pk), d),
    )
    status = 200 if out.get("found") else 404
    return jsonify(out), status


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

def _build_game_sim_payload(
    game_pk: int,
    d: str,
    *,
    feed: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    artifacts, archive, game_line_index = _cards_payload_context(d)
    feed = feed if isinstance(feed, dict) else _load_live_lens_feed(int(game_pk), d)
    out = _load_sim_context_for_game(int(game_pk), d, artifacts=artifacts, archive=archive, feed=feed)
    if out.get("found"):
        snapshot = _load_live_lens_snapshot(int(game_pk), d, feed=feed)
        schedule_games = _schedule_games_for_date(d)
        live_card = next(
            (
                card
                for card in _load_live_lens_cards(d, artifacts=artifacts, archive=archive, schedule_games=schedule_games)
                if _safe_int((card or {}).get("gamePk")) == int(game_pk)
            ),
            None,
        )
        if not isinstance(live_card, dict):
            live_card = {
                "gamePk": int(game_pk),
                "status": {
                    "abstract": str((((snapshot or {}).get("status") or {}).get("abstractGameState") or "")),
                },
            }
        out["livePropRows"] = _current_live_prop_rows(
            live_card,
            snapshot,
            out,
            d,
            ensure_market_fresh=not _is_historical_date(d),
        )
        out["gameLens"] = _build_game_lens(
            live_card,
            snapshot,
            out,
            _game_line_market_for_card(live_card, game_line_index),
            date_str=str(d),
        )
        out["livePitcherModelMismatches"] = list(out.get("livePitcherModelMismatches") or [])
        out.pop("propModels", None)
    return out


@app.get("/api/game/<int:game_pk>/sim")
def api_game_sim(game_pk: int) -> Response:
    d = str(request.args.get("date") or "").strip()
    if not d:
        return jsonify({"gamePk": int(game_pk), "found": False, "error": "missing_date"}), 400
    out = _payload_cache_get_or_build(
        "game_sim_api",
        f"{str(d)}:{int(game_pk)}",
        max_age_seconds=_LIVE_ROUTE_CACHE_TTL_SECONDS,
        builder=lambda: _build_game_sim_payload(int(game_pk), d),
    )
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
    start_live_lens_background_loop()
    app.run(host=host, port=port, debug=debug, threaded=True)
