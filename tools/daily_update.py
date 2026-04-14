from __future__ import annotations

import argparse
import gzip
import json
import math
import multiprocessing
import multiprocessing.spawn
import os
import re
import shutil
import subprocess
import sys
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, get_type_hints


# On Windows, ensure ProcessPoolExecutor workers use this interpreter (venv).
if sys.platform.startswith("win"):
    try:
        multiprocessing.spawn.set_executable(sys.executable)
    except Exception:
        pass

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[1]
_TRACKED_DATA_DIR = (_ROOT / "data").resolve()
_DATA_ROOT_ENV = str(os.environ.get("MLB_BETTING_DATA_ROOT") or "").strip()
_DATA_DIR = (Path(_DATA_ROOT_ENV).resolve() if _DATA_ROOT_ENV else _TRACKED_DATA_DIR)
_OFFICIAL_CARD_MIN_PUBLISH_SIMS = 250
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import (
    StatsApiClient,
    extract_team_pitcher_pitches_thrown,
    fetch_person,
    fetch_official_starting_lineups_for_date,
    fetch_game_context,
    fetch_game_feed_live,
    fetch_mlb_teams,
    fetch_rotowire_batting_orders_for_team,
    fetch_schedule_for_date,
    fetch_team_roster,
    load_feed_live_from_raw,
    parse_confirmed_lineup_ids,
)
from sim_engine.data.build_roster import build_team, build_team_roster
from sim_engine.data.statcast_bvp import apply_starter_bvp_hr_multipliers, default_bvp_cache
from sim_engine.data.statcast_pitch_splits import default_statcast_cache
from sim_engine.models import GameConfig
from sim_engine.simulate import simulate_game
from sim_engine.pitch_model import PitchModelConfig
from sim_engine.prob_calibration import apply_prop_prob_calibration
from sim_engine.data.roster_artifact import read_game_roster_artifact, write_game_roster_artifact
from sim_engine.data.roster_registry import build_roster_events_for_date, update_team_roster_registry
from tools.eval.build_season_eval_manifest import build_manifest as build_season_eval_manifest
from tools.eval.build_season_eval_manifest import write_manifest_artifacts as write_season_eval_manifest_artifacts
from tools.eval.settle_locked_policy_cards import _feed_is_final, _load_feed, _settle_card
from tools.oddsapi.fetch_daily_oddsapi_markets import fetch_and_write_live_odds_for_date
from tools.web.flask_frontend import write_current_day_season_frontend_artifacts, write_daily_ladders_artifact, write_daily_top_props_artifact


# --- multiprocessing helpers (must be top-level for Windows spawn pickling) ---
_SIMW_AWAY = None
_SIMW_HOME = None
_SIMW_WEATHER = None
_SIMW_PARK = None
_SIMW_UMPIRE = None
_SIMW_SEED = 0
_SIMW_WANT_HITTER = False
_SIMW_BATTER_IDS: List[int] = []
_SIMW_PROP_IDS: List[int] = []
_SIMW_CFG_KWARGS: Dict[str, Any] = {}
_PITCHER_BOX_KEYS: Tuple[str, ...] = ("BF", "P", "OUTS", "H", "R", "ER", "BB", "SO", "HR", "HBP")
_PITCHER_PROP_DIST_SPECS: Tuple[Tuple[str, str, str], ...] = (
    ("so", "SO", "so_mean"),
    ("outs", "OUTS", "outs_mean"),
    ("pitches", "P", "pitches_mean"),
    ("hits", "H", "hits_mean"),
    ("earned_runs", "ER", "er_mean"),
    ("walks", "BB", "walks_mean"),
    ("batters_faced", "BF", "batters_faced_mean"),
)
_HITTER_PROP_DIST_SPECS: Tuple[Tuple[str, str, str], ...] = (
    ("hits", "H", "h_mean"),
    ("home_runs", "HR", "hr_mean"),
    ("total_bases", "TB", "tb_mean"),
    ("runs", "R", "r_mean"),
    ("rbi", "RBI", "rbi_mean"),
    ("doubles", "2B", "2b_mean"),
    ("triples", "3B", "3b_mean"),
    ("stolen_bases", "SB", "sb_mean"),
)


def _simw_init(
    away_roster,
    home_roster,
    seed: int,
    weather=None,
    park=None,
    umpire=None,
    want_hitter: bool = False,
    batter_ids: Optional[List[int]] = None,
    pitcher_prop_ids: Optional[List[int]] = None,
    cfg_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    global _SIMW_AWAY, _SIMW_HOME, _SIMW_WEATHER, _SIMW_PARK, _SIMW_UMPIRE, _SIMW_SEED, _SIMW_WANT_HITTER, _SIMW_BATTER_IDS, _SIMW_PROP_IDS, _SIMW_CFG_KWARGS
    _SIMW_AWAY = away_roster
    _SIMW_HOME = home_roster
    _SIMW_WEATHER = weather
    _SIMW_PARK = park
    _SIMW_UMPIRE = umpire
    _SIMW_SEED = int(seed or 0)
    _SIMW_WANT_HITTER = bool(want_hitter)
    _SIMW_BATTER_IDS = [int(x) for x in (batter_ids or []) if int(x or 0) > 0]
    _SIMW_PROP_IDS = [int(x) for x in (pitcher_prop_ids or []) if int(x or 0) > 0]
    _SIMW_CFG_KWARGS = dict(cfg_kwargs or {})


def _build_batter_meta(away_roster, home_roster) -> Dict[int, Dict[str, Any]]:
    meta: Dict[int, Dict[str, Any]] = {}

    def _add(
        side: str,
        team_abbr: str,
        prof,
        *,
        order: Optional[int] = None,
        is_lineup_batter: bool = False,
    ) -> None:
        try:
            player = prof.player
            pid = int(player.mlbam_id)
        except Exception:
            return
        row = meta.setdefault(int(pid), {"id": int(pid)})
        row.setdefault("name", str(getattr(player, "full_name", "") or ""))
        row.setdefault("team", str(team_abbr or ""))
        row.setdefault("side", str(side))
        pos = str(getattr(player, "primary_position", "") or "")
        if pos and not row.get("pos"):
            row["pos"] = pos
        if is_lineup_batter:
            row["is_lineup_batter"] = True
        elif "is_lineup_batter" not in row:
            row["is_lineup_batter"] = False
        if order is not None and row.get("order") is None:
            row["order"] = int(order)

    for side, roster in (("away", away_roster), ("home", home_roster)):
        team_abbr = str(getattr(getattr(roster, "team", None), "abbreviation", "") or "")
        for idx, prof in enumerate((getattr(getattr(roster, "lineup", None), "batters", None) or []), start=1):
            _add(side, team_abbr, prof, order=idx, is_lineup_batter=True)
        for prof in (getattr(getattr(roster, "lineup", None), "bench", None) or []):
            _add(side, team_abbr, prof, is_lineup_batter=False)
    return meta


def _build_pitcher_meta(away_roster, home_roster) -> Dict[int, Dict[str, Any]]:
    meta: Dict[int, Dict[str, Any]] = {}

    def _add(side: str, team_abbr: str, prof, *, order: int) -> None:
        try:
            player = prof.player
            pid = int(player.mlbam_id)
        except Exception:
            return
        row = meta.setdefault(int(pid), {"id": int(pid)})
        row.setdefault("name", str(getattr(player, "full_name", "") or ""))
        row.setdefault("team", str(team_abbr or ""))
        row.setdefault("side", str(side))
        row.setdefault("role", str(getattr(prof, "role", "") or ""))
        if row.get("order") is None:
            row["order"] = int(order)

    for side, roster in (("away", away_roster), ("home", home_roster)):
        team_abbr = str(getattr(getattr(roster, "team", None), "abbreviation", "") or "")
        lineup = getattr(roster, "lineup", None)
        starter = getattr(lineup, "pitcher", None)
        if starter is not None:
            _add(side, team_abbr, starter, order=0)
        for idx, prof in enumerate((getattr(lineup, "bullpen", None) or []), start=1):
            _add(side, team_abbr, prof, order=idx)
    return meta


def _new_pitcher_box_acc_row() -> Dict[str, Any]:
    row = {key: 0.0 for key in _PITCHER_BOX_KEYS}
    row["appearances"] = 0
    return row


def _accumulate_pitcher_box_row(dst: Dict[int, Dict[str, Any]], pid: int, row: Dict[str, Any]) -> None:
    acc = dst.setdefault(int(pid), _new_pitcher_box_acc_row())
    appeared = False
    for key in _PITCHER_BOX_KEYS:
        try:
            value = float((row or {}).get(key) or 0.0)
        except Exception:
            value = 0.0
        acc[key] = float(acc.get(key, 0.0)) + float(value)
        if key in ("BF", "P", "OUTS") and value > 0.0:
            appeared = True
    if appeared:
        acc["appearances"] = int(acc.get("appearances", 0) or 0) + 1


def _build_aggregate_boxscore(
    *,
    sims: int,
    full_segment: Dict[str, Any],
    batter_meta: Dict[int, Dict[str, Any]],
    sum_stats: Dict[int, Dict[str, float]],
    pitcher_meta: Dict[int, Dict[str, Any]],
    pitcher_box_acc: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    denom = float(max(1, int(sims)))
    out: Dict[str, Any] = {
        "away": {"totals": {"R": None, "H": None, "E": None}, "batting": [], "pitching": []},
        "home": {"totals": {"R": None, "H": None, "E": None}, "batting": [], "pitching": []},
    }

    def _mean(value: Any) -> float:
        return round(float(value or 0.0) / denom, 2)

    def _sum_side(side: str, key: str) -> Optional[float]:
        total = 0.0
        seen = False
        for row in out.get(side, {}).get("batting") or []:
            val = row.get(key)
            if not isinstance(val, (int, float)):
                continue
            total += float(val)
            seen = True
        return round(total, 2) if seen else None

    def _sum_rows(side: str, bucket: str, key: str) -> Optional[float]:
        total = 0.0
        seen = False
        for row in out.get(side, {}).get(bucket) or []:
            val = row.get(key)
            if not isinstance(val, (int, float)):
                continue
            total += float(val)
            seen = True
        return round(total, 2) if seen else None

    def _adjust_opponent_pitching(batting_side: str, key: str) -> None:
        opponent_side = "home" if batting_side == "away" else "away"
        batting_total = _sum_rows(batting_side, "batting", key)
        pitching_total = _sum_rows(opponent_side, "pitching", key)
        if batting_total is None or pitching_total is None:
            return
        delta = round(float(batting_total) - float(pitching_total), 2)
        if abs(delta) < 0.01 or abs(delta) > 0.05:
            return

        target_row: Optional[Dict[str, Any]] = None
        target_value = -1.0
        target_outs = -1.0
        for row in out.get(opponent_side, {}).get("pitching") or []:
            value = row.get(key)
            outs = row.get("OUTS")
            if not isinstance(value, (int, float)):
                continue
            value_f = float(value)
            outs_f = float(outs) if isinstance(outs, (int, float)) else 0.0
            if value_f > target_value or (abs(value_f - target_value) < 1e-9 and outs_f > target_outs):
                target_row = row
                target_value = value_f
                target_outs = outs_f
        if target_row is None:
            return

        current_value = target_row.get(key)
        try:
            target_row[key] = round(max(0.0, float(current_value or 0.0) + float(delta)), 2)
        except Exception:
            return

    for pid, meta in batter_meta.items():
        side = str(meta.get("side") or "")
        if side not in ("away", "home"):
            continue
        stats = sum_stats.get(int(pid)) or {}
        row = {
            "id": int(pid),
            "name": str(meta.get("name") or ""),
            "pos": str(meta.get("pos") or ""),
            "PA": _mean(stats.get("PA")),
            "AB": _mean(stats.get("AB")),
            "H": _mean(stats.get("H")),
            "R": _mean(stats.get("R")),
            "RBI": _mean(stats.get("RBI")),
            "BB": _mean(stats.get("BB")),
            "SO": _mean(stats.get("SO")),
            "HR": _mean(stats.get("HR")),
            "HBP": _mean(stats.get("HBP")),
            "SB": _mean(stats.get("SB")),
            "TB": _mean(stats.get("TB")),
            "_order": meta.get("order"),
        }
        has_usage = any(float(row.get(k) or 0.0) > 0.0 for k in ("PA", "AB", "H", "R", "RBI", "BB", "SO", "HR", "TB"))
        if not has_usage and row.get("_order") is None:
            continue
        out[side]["batting"].append(row)

    for side in ("away", "home"):
        out[side]["batting"].sort(
            key=lambda row: (
                row.get("_order") if isinstance(row.get("_order"), int) else 999,
                -(float(row.get("PA") or 0.0)),
                str(row.get("name") or ""),
            )
        )
        for row in out[side]["batting"]:
            row.pop("_order", None)

    all_pitcher_ids = set(int(pid) for pid in pitcher_meta.keys()) | set(int(pid) for pid in pitcher_box_acc.keys())
    for pid in sorted(all_pitcher_ids):
        meta = pitcher_meta.get(int(pid)) or {}
        side = str(meta.get("side") or "")
        if side not in ("away", "home"):
            continue
        acc = pitcher_box_acc.get(int(pid)) or _new_pitcher_box_acc_row()
        appearances = int(acc.get("appearances", 0) or 0)
        row = {
            "id": int(pid),
            "name": str(meta.get("name") or ""),
            "role": str(meta.get("role") or ""),
            "appearance_rate": round(float(appearances) / denom, 3),
            "BF": _mean(acc.get("BF")),
            "P": _mean(acc.get("P")),
            "OUTS": _mean(acc.get("OUTS")),
            "IP": round(float(acc.get("OUTS") or 0.0) / 3.0 / denom, 2),
            "H": _mean(acc.get("H")),
            "R": _mean(acc.get("R")),
            "ER": _mean(acc.get("ER")),
            "BB": _mean(acc.get("BB")),
            "SO": _mean(acc.get("SO")),
            "HR": _mean(acc.get("HR")),
            "HBP": _mean(acc.get("HBP")),
            "_order": meta.get("order"),
        }
        has_usage = any(float(row.get(k) or 0.0) > 0.0 for k in ("BF", "P", "OUTS", "H", "R", "ER", "BB", "SO", "HR", "HBP"))
        if not has_usage and row.get("_order") != 0:
            continue
        out[side]["pitching"].append(row)

    for side in ("away", "home"):
        out[side]["pitching"].sort(
            key=lambda row: (
                row.get("_order") if isinstance(row.get("_order"), int) else 999,
                -(float(row.get("OUTS") or 0.0)),
                str(row.get("name") or ""),
            )
        )
        for row in out[side]["pitching"]:
            row.pop("_order", None)

    for batting_side in ("away", "home"):
        for key in ("H", "R", "BB", "SO", "HR", "HBP"):
            _adjust_opponent_pitching(batting_side, key)

    out["away"]["totals"] = {
        "R": _sum_side("away", "R"),
        "H": _sum_side("away", "H"),
        "E": None,
    }
    out["home"]["totals"] = {
        "R": _sum_side("home", "R"),
        "H": _sum_side("home", "H"),
        "E": None,
    }
    return out


def _simw_chunk(start_i: int, n: int) -> Dict[str, Any]:
    """Run a chunk of sims and return raw counts for aggregation."""
    away_roster = _SIMW_AWAY
    home_roster = _SIMW_HOME
    seed = int(_SIMW_SEED)
    weather = _SIMW_WEATHER
    park = _SIMW_PARK
    umpire = _SIMW_UMPIRE
    want_hitter = bool(_SIMW_WANT_HITTER)
    batter_ids = list(_SIMW_BATTER_IDS)
    prop_ids = list(_SIMW_PROP_IDS)
    cfg_kwargs = dict(_SIMW_CFG_KWARGS or {})

    def init_seg():
        return {
            "home_wins": 0,
            "away_wins": 0,
            "ties": 0,
            "away_runs_sum": 0.0,
            "home_runs_sum": 0.0,
            "totals": {},
            "margins": {},
            "samples": [],
        }

    seg_full = init_seg()
    seg_f1 = init_seg()
    seg_f5 = init_seg()
    seg_f3 = init_seg()

    sum_stats: Dict[int, Dict[str, float]] = {}
    ge_counts: Dict[str, Dict[int, int]] = {}
    prop_acc: Dict[int, Dict[str, Any]] = {}
    hitter_prop_acc: Dict[int, Dict[str, Any]] = {}
    pitcher_box_acc: Dict[int, Dict[str, Any]] = {}
    for pid in prop_ids:
        acc: Dict[str, Any] = {}
        for dist_key, _row_key, mean_key in _PITCHER_PROP_DIST_SPECS:
            acc[str(dist_key)] = {}
            acc[str(mean_key)] = 0.0
        prop_acc[int(pid)] = acc
    if want_hitter and batter_ids:
        for pid in batter_ids:
            acc = {}
            for dist_key, _row_key, mean_key in _HITTER_PROP_DIST_SPECS:
                acc[str(dist_key)] = {}
                acc[str(mean_key)] = 0.0
            hitter_prop_acc[int(pid)] = acc

    def _inc_sum(pid: int, key: str, v: float) -> None:
        row = sum_stats.setdefault(int(pid), {})
        row[key] = float(row.get(key, 0.0)) + float(v)

    def _inc_ge(prop_key: str, pid: int) -> None:
        m = ge_counts.setdefault(str(prop_key), {})
        m[int(pid)] = int(m.get(int(pid), 0) + 1)

    def _inc_ge_thresholds(prop_base: str, pid: int, value: int, max_threshold: int) -> None:
        ivalue = int(value)
        for threshold in range(1, int(max_threshold) + 1):
            if ivalue < threshold:
                break
            _inc_ge(f"{prop_base}_{threshold}plus", pid)

    def seg_score(r, innings: int) -> Dict[str, int]:
        a = sum((r.away_inning_runs or [])[:innings])
        h = sum((r.home_inning_runs or [])[:innings])
        return {"away": int(a), "home": int(h)}

    for i in range(int(start_i), int(start_i) + int(n)):
        cfg = GameConfig(rng_seed=seed + int(i), weather=weather, park=park, umpire=umpire, **cfg_kwargs)
        r = simulate_game(away_roster, home_roster, cfg)

        ps = r.pitcher_stats or {}
        if ps:
            for pid_raw, row in ps.items():
                try:
                    pid = int(pid_raw)
                except Exception:
                    continue
                if pid <= 0 or not isinstance(row, dict):
                    continue
                _accumulate_pitcher_box_row(pitcher_box_acc, int(pid), row)

        if prop_ids:
            for pid in prop_ids:
                row = ps.get(int(pid)) or {}
                acc = prop_acc[int(pid)]
                for dist_key, row_key, mean_key in _PITCHER_PROP_DIST_SPECS:
                    try:
                        value = int(round(float(row.get(str(row_key)) or 0.0)))
                    except Exception:
                        value = 0
                    dist = acc.setdefault(str(dist_key), {})
                    dist[int(value)] = int(dist.get(int(value), 0) + 1)
                    acc[str(mean_key)] = float(acc.get(str(mean_key), 0.0) or 0.0) + float(value)

        if want_hitter and batter_ids:
            bs = r.batter_stats or {}
            for pid in batter_ids:
                row = bs.get(int(pid)) or {}
                try:
                    pa = int(row.get("PA") or 0)
                    ab = int(row.get("AB") or 0)
                    h = int(row.get("H") or 0)
                    d2 = int(row.get("2B") or 0)
                    d3 = int(row.get("3B") or 0)
                    hr = int(row.get("HR") or 0)
                    rr = int(row.get("R") or 0)
                    rbi = int(row.get("RBI") or 0)
                    bb = int(row.get("BB") or 0)
                    so = int(row.get("SO") or 0)
                    hbp = int(row.get("HBP") or 0)
                    sb = int(row.get("SB") or 0)
                except Exception:
                    continue
                tb = int(h + d2 + 2 * d3 + 3 * hr)
                hitter_stat_values = {
                    "H": h,
                    "HR": hr,
                    "TB": tb,
                    "R": rr,
                    "RBI": rbi,
                    "2B": d2,
                    "3B": d3,
                    "SB": sb,
                }

                _inc_sum(pid, "PA", pa)
                _inc_sum(pid, "AB", ab)
                _inc_sum(pid, "H", h)
                _inc_sum(pid, "2B", d2)
                _inc_sum(pid, "3B", d3)
                _inc_sum(pid, "HR", hr)
                _inc_sum(pid, "R", rr)
                _inc_sum(pid, "RBI", rbi)
                _inc_sum(pid, "BB", bb)
                _inc_sum(pid, "SO", so)
                _inc_sum(pid, "HBP", hbp)
                _inc_sum(pid, "SB", sb)
                _inc_sum(pid, "TB", tb)

                acc = hitter_prop_acc.setdefault(int(pid), {})
                for dist_key, row_key, mean_key in _HITTER_PROP_DIST_SPECS:
                    value = int(hitter_stat_values.get(str(row_key), 0))
                    dist = acc.setdefault(str(dist_key), {})
                    dist[int(value)] = int(dist.get(int(value), 0) + 1)
                    acc[str(mean_key)] = float(acc.get(str(mean_key), 0.0) or 0.0) + float(value)

                _inc_ge_thresholds("hits", pid, h, 3)
                if d2 >= 1:
                    _inc_ge("doubles_1plus", pid)
                if d3 >= 1:
                    _inc_ge("triples_1plus", pid)
                if hr >= 1:
                    _inc_ge("hr_1plus", pid)
                _inc_ge_thresholds("runs", pid, rr, 3)
                _inc_ge_thresholds("rbi", pid, rbi, 4)
                _inc_ge_thresholds("total_bases", pid, tb, 5)
                if sb >= 1:
                    _inc_ge("sb_1plus", pid)

        full = {"away": int(r.away_score), "home": int(r.home_score)}
        f1 = seg_score(r, 1)
        f5 = seg_score(r, 5)
        f3 = seg_score(r, 3)

        for seg, score in ((seg_full, full), (seg_f5, f5), (seg_f3, f3), (seg_f1, f1)):
            if len(seg["samples"]) < 50:
                seg["samples"].append(score)
            seg["away_runs_sum"] = float(seg.get("away_runs_sum", 0.0)) + float(score["away"])
            seg["home_runs_sum"] = float(seg.get("home_runs_sum", 0.0)) + float(score["home"])
            tot = int(score["away"] + score["home"])
            seg["totals"][tot] = seg["totals"].get(tot, 0) + 1
            margin = int(score["home"] - score["away"])
            seg["margins"][margin] = seg["margins"].get(margin, 0) + 1
            if score["home"] > score["away"]:
                seg["home_wins"] += 1
            elif score["away"] > score["home"]:
                seg["away_wins"] += 1
            else:
                seg["ties"] += 1

    return {
        "seg_full": seg_full,
        "seg_f1": seg_f1,
        "seg_f5": seg_f5,
        "seg_f3": seg_f3,
        "sum_stats": sum_stats,
        "ge_counts": ge_counts,
        "prop_acc": prop_acc,
        "hitter_prop_acc": hitter_prop_acc,
        "pitcher_box_acc": pitcher_box_acc,
    }


def _abbr(team_obj: dict) -> str:
    return (team_obj.get("abbreviation") or team_obj.get("teamName") or team_obj.get("name") or "UNK")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    tmp.replace(path)


def _write_gz_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(obj, f)
    tmp.replace(path)


def _resolve_path_arg(value: str, *, default: Path) -> Path:
    raw = str(value or "").strip()
    path = Path(raw) if raw else default
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


def _relative_path_str(path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(_ROOT.resolve())).replace("\\", "/")
    except Exception:
        return str(path.resolve()).replace("\\", "/")


def _sync_oddsapi_market_snapshots(date_str: str, snapshot_dir: Path) -> Dict[str, str]:
    token = str(date_str or "").strip().replace("-", "_")
    market_dir = (_ROOT / "data" / "market" / "oddsapi").resolve()
    names = (
        f"oddsapi_game_lines_{token}.json",
        f"oddsapi_pitcher_props_{token}.json",
        f"oddsapi_hitter_props_{token}.json",
    )
    copied: Dict[str, str] = {}
    _ensure_dir(snapshot_dir)
    for name in names:
        src = market_dir / name
        if not src.exists() or not src.is_file():
            continue
        dst = snapshot_dir / name
        try:
            shutil.copy2(src, dst)
        except Exception:
            continue
        copied[name] = str(dst)
    return copied


def _date_plus_days(date_str: str, days: int) -> str:
    try:
        base = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
    except Exception as exc:
        raise SystemExit(f"Invalid date (expected YYYY-MM-DD): {date_str}") from exc
    return (base + timedelta(days=int(days))).strftime("%Y-%m-%d")


def _parse_date_str(date_str: str) -> datetime.date:
    text = str(date_str or "").strip()
    try:
        return datetime.fromisoformat(text).date()
    except Exception:
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except Exception as exc:
            raise SystemExit(f"Invalid date (expected YYYY-MM-DD): {date_str}") from exc


def _season_from_date_str(date_str: str, fallback: int) -> int:
    text = str(date_str or "").strip()
    try:
        return int(text.split("-", 1)[0])
    except Exception:
        return int(fallback)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _load_json_if_exists(path: Path) -> Dict[str, Any]:
    try:
        if path.exists() and path.is_file():
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}
    return {}


def _env_first(*names: str) -> str:
    for name in names:
        value = str(os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def _format_metric_number(value: Any, *, decimals: int = 1) -> str:
    number = _safe_float(value)
    if number is None:
        return "n/a"
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:.{int(decimals)}f}"


def _live_lens_entry_result(selection: Any, market_line: Any, actual_value: Any) -> str:
    line = _safe_float(market_line)
    actual = _safe_float(actual_value)
    side = str(selection or "over").strip().lower()
    if line is None or actual is None:
        return "pending"
    if abs(float(actual) - float(line)) < 1e-9:
        return "push"
    did_win = float(actual) < float(line) if side == "under" else float(actual) > float(line)
    return "win" if did_win else "loss"


def _summarize_live_lens_registry_doc(doc: Dict[str, Any], *, date_str: str) -> Dict[str, Any]:
    entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
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
        result = _live_lens_entry_result(selection, market_line, actual_value)

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
        "date": str(doc.get("date") or date_str),
        "updatedAt": doc.get("updatedAt"),
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


def _format_live_lens_entry_label(row: Dict[str, Any]) -> str:
    owner = str(row.get("owner") or "Unknown").strip()
    prop = str(row.get("prop") or "prop").strip().replace("_", " ")
    selection = str(row.get("selection") or "").strip().lower()
    line = _safe_float(row.get("marketLine"))
    line_text = _format_metric_number(line, decimals=1) if line is not None else "?"
    return f"{owner} {prop} {selection} {line_text}".strip()


def _build_live_lens_readout(
    *,
    date_str: str,
    latest_report: Dict[str, Any],
    registry_summary: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:
    counts = latest_report.get("counts") if isinstance(latest_report.get("counts"), dict) else {}
    performance = latest_report.get("performance") if isinstance(latest_report.get("performance"), dict) else {}
    total_entries = int(registry_summary.get("totalEntries") or 0)
    unique_games = int(registry_summary.get("uniqueGames") or 0)
    unique_owners = int(registry_summary.get("uniqueOwners") or 0)
    result_counts = registry_summary.get("resultCounts") if isinstance(registry_summary.get("resultCounts"), dict) else {}
    by_prop = registry_summary.get("byProp") if isinstance(registry_summary.get("byProp"), dict) else {}
    top_stable = registry_summary.get("topStable") if isinstance(registry_summary.get("topStable"), list) else []
    top_edges = registry_summary.get("topEdges") if isinstance(registry_summary.get("topEdges"), list) else []
    wins = int(result_counts.get("win") or 0)
    losses = int(result_counts.get("loss") or 0)
    pushes = int(result_counts.get("push") or 0)
    settled = int(wins + losses + pushes)
    graded = int(wins + losses)
    win_rate = round((float(wins) / float(graded)) * 100.0, 1) if graded > 0 else None

    headline = f"No prior-day live-lens activity found for {date_str}."
    if total_entries > 0:
        prop_summary = ", ".join(f"{prop}:{count}" for prop, count in list(by_prop.items())[:3]) or "no active lanes"
        headline = (
            f"Prior-day live lens tracked {total_entries} opportunities across {unique_games} games and "
            f"{unique_owners} owners; active lanes were {prop_summary}."
        )

    learnings: List[str] = []
    performance_lines: List[str] = []
    if total_entries > 0:
        if settled > 0 and win_rate is not None:
            learnings.append(
                f"Resolved prior-day live spots finished {wins}-{losses}-{pushes} with a {win_rate:.1f}% win rate excluding pushes."
            )
        strongest_prop = next(iter(by_prop.keys()), "")
        if strongest_prop:
            learnings.append(
                f"The most active live lane was {strongest_prop.replace('_', ' ')} with {int(by_prop.get(strongest_prop) or 0)} tracked opportunities."
            )
        if top_stable:
            stable_row = top_stable[0] if isinstance(top_stable[0], dict) else {}
            learnings.append(
                f"Most persistent opportunity: {_format_live_lens_entry_label(stable_row)} stayed on the board for {int(stable_row.get('seenCount') or 0)} observations."
            )
        if top_edges:
            edge_row = top_edges[0] if isinstance(top_edges[0], dict) else {}
            learnings.append(
                f"Largest closing live edge: {_format_live_lens_entry_label(edge_row)} at {_format_metric_number(edge_row.get('lastSeenLiveEdge'), decimals=2)}."
            )

    total_ms = _safe_float(performance.get("totalMs"))
    market_refresh_ms = _safe_float(performance.get("marketRefreshMs"))
    game_count = _safe_int(performance.get("gameCount") or counts.get("games"))
    archived_props = _safe_int(counts.get("archivedLiveProps"))
    if total_ms is not None or market_refresh_ms is not None:
        performance_lines.append(
            f"{source} processed {int(game_count or 0)} games in {_format_metric_number(total_ms, decimals=1)} ms with {_format_metric_number(market_refresh_ms, decimals=1)} ms spent refreshing markets."
        )
    if archived_props:
        performance_lines.append(
            f"The latest persisted live-lens report archived {int(archived_props)} finalized live prop rows for {date_str}."
        )

    return {
        "headline": headline,
        "learnings": learnings,
        "performance": performance_lines,
    }


def _fetch_live_lens_reports_payload(
    *,
    base_url: str,
    token: str,
    date_str: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    query = urllib.parse.urlencode({"date": str(date_str)})
    url = f"{str(base_url).rstrip('/')}/api/cron/live-lens-reports?{query}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=max(1, int(timeout_seconds))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("live-lens reports response was not a JSON object")
    return payload


def _prior_day_live_lens_stage(args: argparse.Namespace, date_str: str) -> Dict[str, Any]:
    slug = str(date_str).strip().replace("-", "_")
    default_sync_path = (_DATA_DIR / "live_lens" / "render_sync" / f"live_lens_reports_{slug}.json").resolve()
    sync_out_arg = str(getattr(args, "live_lens_sync_out", "") or "").strip()
    sync_out_path = _resolve_path_arg(sync_out_arg, default=default_sync_path)
    live_lens_dir = (_DATA_DIR / "live_lens").resolve()
    local_report_path = live_lens_dir / f"live_lens_report_{slug}.json"
    local_registry_path = live_lens_dir / "prop_registry" / f"live_prop_registry_{slug}.json"
    stage: Dict[str, Any] = {
        "status": "skipped",
        "date": str(date_str),
        "sync_requested": bool(str(getattr(args, "sync_live_lens", "on") or "on") == "on"),
        "sync_out_path": _relative_path_str(sync_out_path),
        "local_report_path": _relative_path_str(local_report_path),
        "local_registry_path": _relative_path_str(local_registry_path),
    }

    payload: Dict[str, Any] = {}
    if stage["sync_requested"]:
        base_url = str(getattr(args, "live_lens_base_url", "") or "").strip() or _env_first(
            "MLB_BETTING_BASE_URL",
            "BASE_URL",
            "RENDER_URL",
            "RENDER_EXTERNAL_URL",
        )
        token = str(getattr(args, "live_lens_cron_token", "") or "").strip() or _env_first(
            "MLB_BETTING_CRON_TOKEN",
            "CRON_TOKEN",
        )
        stage["render_base_url"] = str(base_url or "")
        if base_url and token:
            try:
                payload = _fetch_live_lens_reports_payload(
                    base_url=base_url,
                    token=token,
                    date_str=str(date_str),
                    timeout_seconds=int(getattr(args, "live_lens_timeout_seconds", 45) or 45),
                )
                _ensure_dir(sync_out_path.parent)
                _write_json(sync_out_path, payload)
                stage["status"] = "ok"
                stage["source"] = "render_api"
                stage["synced_report_path"] = _relative_path_str(sync_out_path)
            except urllib.error.HTTPError as exc:
                stage["status"] = "warning"
                stage["error"] = f"HTTPError: {exc.code} {exc.reason}"
            except Exception as exc:
                stage["status"] = "warning"
                stage["error"] = f"{type(exc).__name__}: {exc}"
        else:
            stage["status"] = "warning"
            stage["error"] = "live-lens sync requested but Render base URL or cron token is unavailable"
    else:
        stage["reason"] = "sync_live_lens=off"

    latest_report = payload.get("latestReport") if isinstance(payload.get("latestReport"), dict) else {}
    registry_summary = payload.get("registrySummary") if isinstance(payload.get("registrySummary"), dict) else {}
    source_label = "Render live-lens report"

    if not latest_report:
        latest_report = _load_json_if_exists(local_report_path)
        if latest_report:
            stage["used_local_report"] = True
            if str(stage.get("source") or "") != "render_api":
                stage["source"] = "local_files"
            source_label = "Local live-lens report"

    if not registry_summary:
        local_registry_doc = _load_json_if_exists(local_registry_path)
        if local_registry_doc:
            registry_summary = _summarize_live_lens_registry_doc(local_registry_doc, date_str=str(date_str))
            stage["used_local_registry"] = True
            if str(stage.get("source") or "") not in {"render_api", "local_files"}:
                stage["source"] = "local_files"

    if latest_report or registry_summary:
        stage["status"] = "ok" if str(stage.get("status") or "") in {"", "skipped", "ok"} else stage["status"]
        if not stage.get("source"):
            stage["source"] = "local_files"
        stage["registry_summary"] = registry_summary
        stage["readout"] = _build_live_lens_readout(
            date_str=str(date_str),
            latest_report=latest_report,
            registry_summary=registry_summary,
            source=source_label,
        )
    elif str(stage.get("status") or "") == "skipped":
        stage["reason"] = "no prior-day live-lens report or registry data found"

    return stage


def _default_ui_profile_out_dirs(game_out: Path) -> Tuple[Path, Path]:
    default_game = (_DATA_DIR / "daily").resolve()
    try:
        resolved_game = game_out.resolve()
    except Exception:
        resolved_game = game_out
    if resolved_game == default_game:
        return (
            (_DATA_DIR / "daily_pitcher_props").resolve(),
            (_DATA_DIR / "daily_hitter_props").resolve(),
        )
    base_name = str(game_out.name or "daily").strip() or "daily"
    return (
        (game_out.parent / f"{base_name}_pitcher_props").resolve(),
        (game_out.parent / f"{base_name}_hitter_props").resolve(),
    )


def _strip_cli_args(
    argv: List[str],
    *,
    flags_with_values: Tuple[str, ...],
    flags_no_values: Tuple[str, ...],
) -> List[str]:
    out: List[str] = []
    skip_next = False
    value_flags = set(flags_with_values)
    bare_flags = set(flags_no_values)
    all_flags = value_flags | bare_flags
    for raw in argv:
        if skip_next:
            skip_next = False
            continue
        arg = str(raw or "")
        if not arg:
            continue
        if arg.startswith("--") and "=" in arg:
            flag = arg.split("=", 1)[0]
            if flag in all_flags:
                continue
        if arg in value_flags:
            skip_next = True
            continue
        if arg in bare_flags:
            continue
        out.append(arg)
    return out


def _argv_has_flag(argv: List[str], flag: str) -> bool:
    needle = str(flag or "").strip()
    if not needle:
        return False
    for raw in argv:
        arg = str(raw or "")
        if arg == needle:
            return True
        if arg.startswith(f"{needle}="):
            return True
    return False


def _fresh_auto_seed() -> int:
    return max(1, int.from_bytes(os.urandom(8), "big") % 2147483647)


def _resolve_effective_seed(args: argparse.Namespace, raw_argv: List[str]) -> Tuple[int, str, bool]:
    seed_source = str(getattr(args, "seed_source", "") or "").strip()
    if seed_source:
        return int(args.seed), seed_source, (seed_source == "explicit_cli")

    explicit_cli = _argv_has_flag(raw_argv, "--seed")
    if explicit_cli:
        return int(args.seed), "explicit_cli", True

    target_date = _parse_date_str(str(args.date))
    if target_date > datetime.now().date():
        return _fresh_auto_seed(), "auto_future_date", False

    return int(args.seed), "default_fixed", False


def _refresh_feed_live_cache_for_date(
    *,
    client: StatsApiClient,
    date_str: str,
    season: int,
) -> Dict[str, Any]:
    games = fetch_schedule_for_date(client, str(date_str))
    out_dir = (_DATA_DIR / "raw" / "statsapi" / "feed_live" / str(int(season)) / str(date_str)).resolve()
    _ensure_dir(out_dir)

    scheduled = 0
    regular_scheduled = 0
    wrote = 0
    errors: List[Dict[str, Any]] = []
    seen_game_pks = set()
    for game in games:
        game_pk = game.get("gamePk")
        try:
            game_pk_i = int(game_pk)
        except Exception:
            continue
        if game_pk_i in seen_game_pks:
            continue
        seen_game_pks.add(game_pk_i)
        scheduled += 1
        if str(game.get("gameType") or "").strip().upper() == "R":
            regular_scheduled += 1
        try:
            payload = fetch_game_feed_live(client, game_pk_i)
            _write_gz_json(out_dir / f"{game_pk_i}.json.gz", payload)
            wrote += 1
        except Exception as exc:
            errors.append(
                {
                    "game_pk": int(game_pk_i),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    status = "ok" if not errors else "warning"
    return {
        "status": status,
        "date": str(date_str),
        "season": int(season),
        "games_scheduled": int(scheduled),
        "regular_season_games_scheduled": int(regular_scheduled),
        "games_written": int(wrote),
        "errors": errors,
        "error_count": int(len(errors)),
        "out_dir": _relative_path_str(out_dir),
        "pbp_source": "statsapi_feed_live",
    }


def _build_prior_eval_command(
    *,
    args: argparse.Namespace,
    prior_date: str,
    prior_season: int,
    out_path: Path,
    daily_snapshots_root: Path,
    lineups_last_known_path: Optional[Path],
) -> List[str]:
    reconcile_mode = str(getattr(args, "prior_reconcile_mode", "artifact") or "artifact").strip().lower()
    if reconcile_mode == "artifact":
        return [
            str(Path(sys.executable).resolve()),
            str((_ROOT / "tools" / "eval" / "reconcile_daily_sim_artifacts.py").resolve()),
            "--date",
            str(prior_date),
            "--season",
            str(int(prior_season)),
            "--out",
            str(out_path),
            "--prop-lines-source",
            str(getattr(args, "prior_eval_prop_lines_source", "auto") or "auto"),
            "--hitter-hr-prob-calibration",
            str(getattr(args, "hitter_hr_prob_calibration", "") or ""),
            "--hitter-props-prob-calibration",
            str(getattr(args, "hitter_props_prob_calibration", "") or ""),
        ]
    cmd = [
        str(Path(sys.executable).resolve()),
        str((_ROOT / "tools" / "eval" / "eval_sim_day_vs_actual.py").resolve()),
        "--date",
        str(prior_date),
        "--season",
        str(int(prior_season)),
        "--spring-mode",
        ("on" if bool(getattr(args, "spring_mode", False)) else "off"),
        "--stats-season",
        str(int(getattr(args, "stats_season", 0) or 0)),
        "--use-daily-snapshots",
        "on",
        "--daily-snapshots-root",
        str(daily_snapshots_root),
        "--use-roster-artifacts",
        str(getattr(args, "use_roster_artifacts", "on") or "on"),
        "--write-roster-artifacts",
        str(getattr(args, "write_roster_artifacts", "on") or "on"),
        "--sims-per-game",
        str(int(getattr(args, "prior_eval_sims", 0) or getattr(args, "sims", 0) or 1000)),
        "--bvp-hr",
        str(getattr(args, "bvp_hr", "off") or "off"),
        "--bvp-days-back",
        str(int(getattr(args, "bvp_days_back", 365) or 365)),
        "--bvp-min-pa",
        str(int(getattr(args, "bvp_min_pa", 10) or 10)),
        "--bvp-shrink-pa",
        str(float(getattr(args, "bvp_shrink_pa", 50.0) or 50.0)),
        "--bvp-clamp-lo",
        str(float(getattr(args, "bvp_clamp_lo", 0.80) or 0.80)),
        "--bvp-clamp-hi",
        str(float(getattr(args, "bvp_clamp_hi", 1.25) or 1.25)),
        "--hitter-hr-topn",
        str(int(getattr(args, "hitter_hr_topn", 0) or 0)),
        "--hitter-props-topn",
        str(int(getattr(args, "hitter_props_topn", 24) or 24)),
        "--seed",
        str(int(getattr(args, "seed", 1337) or 1337)),
        "--jobs",
        str(int(getattr(args, "workers", 1) or 1)),
        "--use-raw",
        "on",
        "--write-missing-raw",
        "on",
        "--prop-lines-source",
        str(getattr(args, "prior_eval_prop_lines_source", "auto") or "auto"),
        "--cache-ttl-hours",
        str(int(getattr(args, "cache_ttl_hours", 24) or 24)),
        "--umpire-shrink",
        str(float(getattr(args, "umpire_shrink", 0.75) or 0.75)),
        "--pitch-model-overrides",
        str(getattr(args, "pitch_model_overrides", "") or ""),
        "--manager-pitching",
        str(getattr(args, "manager_pitching", "v2") or "v2"),
        "--manager-pitching-overrides",
        str(getattr(args, "manager_pitching_overrides", "") or ""),
        "--pitcher-rate-sampling",
        str(getattr(args, "pitcher_rate_sampling", "on") or "on"),
        "--bip-baserunning",
        str(getattr(args, "bip_baserunning", "on") or "on"),
        "--out",
        str(out_path),
    ]
    if lineups_last_known_path is not None and lineups_last_known_path.exists():
        cmd.extend(["--lineups-last-known", str(lineups_last_known_path)])
    if getattr(args, "bip_dp_rate", None) is not None:
        cmd.extend(["--bip-dp-rate", str(float(args.bip_dp_rate))])
    if getattr(args, "bip_sf_rate_flypop", None) is not None:
        cmd.extend(["--bip-sf-rate-flypop", str(float(args.bip_sf_rate_flypop))])
    if getattr(args, "bip_sf_rate_line", None) is not None:
        cmd.extend(["--bip-sf-rate-line", str(float(args.bip_sf_rate_line))])
    if getattr(args, "bip_1b_p2_scores_mult", None) is not None:
        cmd.extend(["--bip-1b-p2-scores-mult", str(float(args.bip_1b_p2_scores_mult))])
    if getattr(args, "bip_2b_p1_scores_mult", None) is not None:
        cmd.extend(["--bip-2b-p1-scores-mult", str(float(args.bip_2b_p1_scores_mult))])
    if getattr(args, "bip_1b_p1_to_3b_rate", None) is not None:
        cmd.extend(["--bip-1b-p1-to-3b-rate", str(float(args.bip_1b_p1_to_3b_rate))])
    if getattr(args, "bip_ground_rbi_out_rate", None) is not None:
        cmd.extend(["--bip-ground-rbi-out-rate", str(float(args.bip_ground_rbi_out_rate))])
    if getattr(args, "bip_out_2b_to_3b_rate", None) is not None:
        cmd.extend(["--bip-out-2b-to-3b-rate", str(float(args.bip_out_2b_to_3b_rate))])
    if getattr(args, "bip_out_1b_to_2b_rate", None) is not None:
        cmd.extend(["--bip-out-1b-to-2b-rate", str(float(args.bip_out_1b_to_2b_rate))])
    if getattr(args, "bip_misc_advance_pitch_rate", None) is not None:
        cmd.extend(["--bip-misc-advance-pitch-rate", str(float(args.bip_misc_advance_pitch_rate))])
    if getattr(args, "bip_roe_rate", None) is not None:
        cmd.extend(["--bip-roe-rate", str(float(args.bip_roe_rate))])
    if getattr(args, "bip_fc_rate", None) is not None:
        cmd.extend(["--bip-fc-rate", str(float(args.bip_fc_rate))])
    return cmd


def _current_day_inputs_stage(*, game_out: Path, date_str: str) -> Dict[str, Any]:
    snapshot_dir = game_out / "snapshots" / str(date_str)
    roster_path = snapshot_dir / "team_rosters_raw.json"
    injuries_path = snapshot_dir / "injuries_raw.json"
    roster_events_path = snapshot_dir / "roster_events.json"
    lineups_path = snapshot_dir / "lineups.json"
    probables_path = snapshot_dir / "probables.json"
    last_known_path = game_out / "lineups_last_known_by_team.json"

    lineups_games = None
    lineups_summary: Dict[str, Any] = {}
    probables_games = None
    if lineups_path.exists() and lineups_path.is_file():
        try:
            lineups_doc = json.loads(lineups_path.read_text(encoding="utf-8")) or {}
            lineups_games = int(len((lineups_doc or {}).get("games") or []))
            if isinstance((lineups_doc or {}).get("summary"), dict):
                lineups_summary = dict((lineups_doc or {}).get("summary") or {})
        except Exception:
            lineups_games = None
            lineups_summary = {}
    if probables_path.exists() and probables_path.is_file():
        try:
            probables_games = int(len((json.loads(probables_path.read_text(encoding="utf-8")) or {}).get("games") or []))
        except Exception:
            probables_games = None

    roster_ok = roster_path.exists() and injuries_path.exists()
    lineups_ok = lineups_path.exists()
    probables_ok = probables_path.exists()

    return {
        "roster_snapshot": {
            "status": ("ok" if roster_ok else "missing"),
            "snapshot_dir": _relative_path_str(snapshot_dir),
            "team_rosters_raw": _relative_path_str(roster_path),
            "injuries_raw": _relative_path_str(injuries_path),
            "roster_events": _relative_path_str(roster_events_path),
            "lineups_last_known": _relative_path_str(last_known_path if last_known_path.exists() else None),
        },
        "batting_lineups": {
            "status": ("ok" if lineups_ok else "missing"),
            "path": _relative_path_str(lineups_path),
            "games": lineups_games,
            "summary": lineups_summary,
            "adjusted_teams": int(lineups_summary.get("adjusted_teams") or 0),
            "partial_teams": int(lineups_summary.get("partial_teams") or 0),
            "fallback_pool_teams": int(lineups_summary.get("fallback_pool_teams") or 0),
        },
        "probable_pitchers": {
            "status": ("ok" if probables_ok else "missing"),
            "path": _relative_path_str(probables_path),
            "games": probables_games,
        },
    }


def _publish_live_season_manifests(
    *,
    season: int,
    batch_dir: Path,
    betting_profile: str,
    season_dir: Path,
) -> Dict[str, Any]:
    _ensure_dir(season_dir)

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
    betting_day_payload_dir = season_dir / (
        "betting_day_payloads_retuned"
        if normalized_profile == "retuned"
        else "betting_day_payloads"
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
        "--day-payload-dir",
        str(betting_day_payload_dir),
        "--profile-name",
        str(normalized_profile),
        "--title",
        f"MLB {int(season)} Betting Card Recap",
    ]
    if normalized_profile == "retuned":
        cmd.extend(["--prefer-canonical-daily", "on"])
    betting_rc = subprocess.run(cmd, check=False).returncode

    return {
        "season_eval_manifest": _relative_path_str(season_manifest_path),
        "season_eval_recap": _relative_path_str(season_recap_path),
        "season_eval_status": str((season_manifest.get("meta") or {}).get("status") or "unknown"),
        "season_eval_partial": bool((season_manifest.get("meta") or {}).get("partial")),
        "season_eval_days": int((season_manifest.get("overview") or {}).get("days") or 0),
        "betting_profile": normalized_profile,
        "season_betting_manifest": _relative_path_str(betting_manifest_path),
        "season_betting_recap": _relative_path_str(betting_recap_path),
        "season_betting_cards_dir": _relative_path_str(betting_cards_dir),
        "season_betting_day_payload_dir": _relative_path_str(betting_day_payload_dir),
        "season_betting_exit_code": int(betting_rc),
        "season_betting_manifest_exists": bool(betting_manifest_path.exists()),
    }


def _git_run(repo_root: Path, args: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo_root), *[str(part) for part in args]],
        check=False,
        capture_output=True,
        text=True,
    )


def _git_current_change_set(repo_root: Path) -> set[str]:
    paths: set[str] = set()
    commands = (
        ["diff", "--name-only", "--relative"],
        ["diff", "--cached", "--name-only", "--relative"],
        ["ls-files", "--others", "--exclude-standard"],
    )
    for cmd in commands:
        result = _git_run(repo_root, list(cmd))
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout or "git command failed").strip())
        for raw_line in str(result.stdout or "").splitlines():
            line = raw_line.strip().replace("\\", "/")
            if line:
                paths.add(line)
    return paths


def _git_current_branch(repo_root: Path) -> str:
    result = _git_run(repo_root, ["rev-parse", "--abbrev-ref", "HEAD"])
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "git branch lookup failed").strip())
    branch = str(result.stdout or "").strip()
    if not branch:
        raise RuntimeError("git branch lookup returned empty branch name")
    return branch


def _maybe_git_push_daily_update(
    *,
    repo_root: Path,
    date_str: str,
    workflow: str,
    preexisting_changes: Optional[set[str]],
    enabled: bool,
    remote: str,
    branch: str,
    commit_message: str,
) -> Dict[str, Any]:
    if not enabled:
        return {"status": "skipped", "reason": "git_push=off"}

    before = set(preexisting_changes or set())
    after = _git_current_change_set(repo_root)
    candidate_paths = sorted(path for path in after if path not in before)
    if not candidate_paths:
        return {
            "status": "skipped",
            "reason": "no new repository changes to commit",
            "preexisting_change_count": int(len(before)),
        }

    add_result = _git_run(repo_root, ["add", "-A", "--", *candidate_paths])
    if add_result.returncode != 0:
        raise RuntimeError((add_result.stderr or add_result.stdout or "git add failed").strip())

    staged_result = _git_run(repo_root, ["diff", "--cached", "--name-only", "--relative"])
    if staged_result.returncode != 0:
        raise RuntimeError((staged_result.stderr or staged_result.stdout or "git staged diff failed").strip())
    staged_paths = [line.strip().replace("\\", "/") for line in str(staged_result.stdout or "").splitlines() if line.strip()]
    if not staged_paths:
        return {
            "status": "skipped",
            "reason": "git add produced no staged changes",
            "candidate_paths": candidate_paths,
        }

    normalized_message = str(commit_message or "").format(date=str(date_str), workflow=str(workflow or "core")).strip()
    if not normalized_message:
        normalized_message = f"Daily update {date_str}"
    commit_result = _git_run(repo_root, ["commit", "-m", normalized_message])
    if commit_result.returncode != 0:
        raise RuntimeError((commit_result.stderr or commit_result.stdout or "git commit failed").strip())

    push_branch = str(branch or "").strip() or _git_current_branch(repo_root)
    push_result = _git_run(repo_root, ["push", str(remote), push_branch])
    if push_result.returncode != 0:
        raise RuntimeError((push_result.stderr or push_result.stdout or "git push failed").strip())

    head_result = _git_run(repo_root, ["rev-parse", "HEAD"])
    commit_sha = str(head_result.stdout or "").strip() if head_result.returncode == 0 else ""
    return {
        "status": "ok",
        "remote": str(remote),
        "branch": str(push_branch),
        "commit_message": normalized_message,
        "commit_sha": commit_sha,
        "committed_paths": staged_paths,
        "preexisting_change_count": int(len(before)),
    }


def _run_ui_daily_workflow(args: argparse.Namespace, *, raw_argv: List[str]) -> int:
    game_out = _resolve_path_arg(str(getattr(args, "out", "") or ""), default=(_DATA_DIR / "daily"))
    default_pitcher_out, default_hitter_out = _default_ui_profile_out_dirs(game_out)
    pitcher_out = _resolve_path_arg(
        str(getattr(args, "workflow_out_pitcher", "") or ""),
        default=default_pitcher_out,
    )
    hitter_out = _resolve_path_arg(
        str(getattr(args, "workflow_out_hitter", "") or ""),
        default=default_hitter_out,
    )
    _ensure_dir(game_out)

    token = str(args.date).replace("-", "_")
    prior_date = str(getattr(args, "reconcile_date", "") or "").strip() or _date_plus_days(str(args.date), -1)
    prior_token = str(prior_date).replace("-", "_")
    prior_season = _season_from_date_str(prior_date, int(args.season))

    settlement_out_arg = str(getattr(args, "prior_card_settlement_out", "") or "").strip()
    settlement_path = _resolve_path_arg(
        settlement_out_arg,
        default=(game_out / "settlements" / f"daily_summary_{prior_token}_locked_policy_settlement.json"),
    )
    season_batch_dir_arg = str(getattr(args, "season_batch_dir", "") or "").strip()
    season_batch_dir = _resolve_path_arg(
        season_batch_dir_arg,
        default=(_DATA_DIR / "eval" / "batches" / f"season_{int(args.season)}_ui_daily_live"),
    )
    season_output_dir_arg = str(getattr(args, "season_output_dir", "") or "").strip()
    season_output_dir = _resolve_path_arg(
        season_output_dir_arg,
        default=(_DATA_DIR / "eval" / "seasons" / str(int(args.season))),
    )
    prior_report_path = season_batch_dir / f"sim_vs_actual_{prior_date}.json"
    ops_report_arg = str(getattr(args, "ops_report_out", "") or "").strip()
    ops_report_path = _resolve_path_arg(
        ops_report_arg,
        default=(game_out / "ops" / f"daily_ops_{token}.json"),
    )

    report: Dict[str, Any] = {
        "tool": "tools/daily_update.py",
        "workflow": "ui-daily",
        "generated_at": datetime.now().isoformat(),
        "date": str(args.date),
        "season": int(args.season),
        "seed": {
            "value": int(getattr(args, "seed", 1337) or 1337),
            "source": str(getattr(args, "seed_source", "default_fixed") or "default_fixed"),
            "explicit_cli": bool(getattr(args, "seed_explicit", False)),
        },
        "prior_day": {
            "date": str(prior_date),
            "season": int(prior_season),
            "eval_report_path": _relative_path_str(prior_report_path),
            "top_props_artifact_path": _relative_path_str(game_out / "top_props" / f"daily_top_props_{prior_token}.json"),
        },
        "current_day": {
            "date": str(args.date),
            "season": int(args.season),
            "out_game": _relative_path_str(game_out),
            "out_pitcher": _relative_path_str(pitcher_out),
            "out_hitter": _relative_path_str(hitter_out),
            "summary_path": _relative_path_str(game_out / f"daily_summary_{token}.json"),
            "profile_bundle_path": _relative_path_str(game_out / f"daily_summary_{token}_profile_bundle.json"),
            "locked_policy_path": _relative_path_str(game_out / f"daily_summary_{token}_locked_policy.json"),
            "top_props_artifact_path": _relative_path_str(game_out / "top_props" / f"daily_top_props_{token}.json"),
            "season_frontend_dir": _relative_path_str(game_out / "season_frontend"),
            "season_manifest_artifact_path": _relative_path_str(game_out / "season_frontend" / f"season_manifest_{int(args.season)}_{token}.json"),
            "season_day_artifact_path": _relative_path_str(game_out / "season_frontend" / f"season_day_{int(args.season)}_{token}_{str(getattr(args, 'season_betting_profile', 'retuned') or 'retuned').strip().lower()}.json"),
            "season_betting_day_artifact_path": _relative_path_str(game_out / "season_frontend" / f"season_betting_day_{int(args.season)}_{token}_{str(getattr(args, 'season_betting_profile', 'retuned') or 'retuned').strip().lower()}.json"),
            "season_official_betting_day_artifact_path": _relative_path_str(game_out / "season_frontend" / f"season_official_betting_day_{int(args.season)}_{token}_{str(getattr(args, 'season_betting_profile', 'retuned') or 'retuned').strip().lower()}.json"),
        },
        "stages": {},
        "warnings": [],
        "errors": [],
    }
    git_push_enabled = str(getattr(args, "git_push", "off") or "off") == "on"
    git_push_stage: Dict[str, Any] = {
        "requested": bool(git_push_enabled),
        "remote": str(getattr(args, "git_push_remote", "origin") or "origin"),
        "branch": str(getattr(args, "git_push_branch", "") or ""),
    }
    preexisting_changes: Optional[set[str]] = None
    if git_push_enabled:
        try:
            preexisting_changes = _git_current_change_set(_ROOT)
            git_push_stage["preexisting_change_count"] = int(len(preexisting_changes))
        except Exception as exc:
            git_push_stage["requested"] = False
            git_push_stage["status"] = "warning"
            git_push_stage["error"] = f"{type(exc).__name__}: {exc}"
            report["warnings"].append(f"git push disabled: {type(exc).__name__}: {exc}")
            git_push_enabled = False
    report["git_push"] = git_push_stage

    def _prior_day_settlement_diagnostics(settled_card: Dict[str, Any], date_str: str) -> Dict[str, Any]:
        unresolved_rows = [
            row
            for row in (settled_card.get("all_unresolved_recommendations") or [])
            if isinstance(row, dict)
        ]
        unresolved_game_pks = sorted(
            {
                int(row.get("game_pk") or 0)
                for row in unresolved_rows
                if int(row.get("game_pk") or 0) > 0
            }
        )
        reason_counts: Dict[str, int] = {}
        final_game_pks: List[int] = []
        non_final_game_pks: List[int] = []
        unknown_game_pks: List[int] = []

        for row in unresolved_rows:
            reason = str(row.get("reason") or "unknown")
            reason_counts[reason] = int(reason_counts.get(reason, 0) + 1)

        for game_pk in unresolved_game_pks:
            try:
                feed = _load_feed(str(date_str), int(game_pk))
            except Exception:
                unknown_game_pks.append(int(game_pk))
                continue
            if _feed_is_final(feed):
                final_game_pks.append(int(game_pk))
            else:
                non_final_game_pks.append(int(game_pk))

        all_games_final = bool(unresolved_rows) and not non_final_game_pks and not unknown_game_pks
        return {
            "unresolved_reason_counts": reason_counts,
            "unresolved_game_pks": unresolved_game_pks,
            "final_game_pks": final_game_pks,
            "non_final_game_pks": non_final_game_pks,
            "unknown_game_pks": unknown_game_pks,
            "all_unresolved_games_final": bool(all_games_final),
        }

    refresh_stage: Dict[str, Any]
    if str(getattr(args, "refresh_prior_feed_live", "on") or "on") == "on":
        print(f"[ui-daily] Refreshing prior-day StatsAPI feed/live cache for {prior_date}...")
        try:
            client = StatsApiClient.with_default_cache(ttl_seconds=int(args.cache_ttl_hours * 3600))
            refresh_stage = _refresh_feed_live_cache_for_date(
                client=client,
                date_str=str(prior_date),
                season=int(prior_season),
            )
            if int(refresh_stage.get("error_count") or 0) > 0:
                report["warnings"].append(
                    f"prior-day feed/live refresh had {int(refresh_stage.get('error_count') or 0)} fetch error(s)"
                )
        except Exception as exc:
            refresh_stage = {
                "status": "error",
                "date": str(prior_date),
                "season": int(prior_season),
                "error": f"{type(exc).__name__}: {exc}",
            }
            report["errors"].append(f"prior-day feed/live refresh failed: {type(exc).__name__}: {exc}")
    else:
        refresh_stage = {
            "status": "skipped",
            "date": str(prior_date),
            "season": int(prior_season),
            "reason": "refresh_prior_feed_live=off",
        }
    report["stages"]["prior_day_feed_live_refresh"] = refresh_stage

    settlement_stage: Dict[str, Any]
    prior_card_path = game_out / f"daily_summary_{prior_token}_locked_policy.json"
    if str(getattr(args, "settle_prior_card", "on") or "on") == "on":
        if prior_card_path.exists() and prior_card_path.is_file():
            print(f"[ui-daily] Settling prior-day locked card for {prior_date}...")
            try:
                settled_card = _settle_card(prior_card_path)
                _ensure_dir(settlement_path.parent)
                _write_json(settlement_path, settled_card)
                settlement_stage = {
                    "status": "ok",
                    "date": str(prior_date),
                    "card_path": _relative_path_str(prior_card_path),
                    "settlement_path": _relative_path_str(settlement_path),
                    "selected_counts": dict(settled_card.get("selected_counts") or {}),
                    "playable_selected_counts": dict(settled_card.get("playable_selected_counts") or {}),
                    "all_selected_counts": dict(settled_card.get("all_selected_counts") or {}),
                    "results": dict(settled_card.get("results") or {}),
                    "playable_results": dict(settled_card.get("playable_results") or {}),
                    "all_results": dict(settled_card.get("all_results") or {}),
                    "settled_n": int(settled_card.get("settled_n") or 0),
                    "playable_settled_n": int(settled_card.get("playable_settled_n") or 0),
                    "all_settled_n": int(settled_card.get("all_settled_n") or 0),
                    "unresolved_n": int(settled_card.get("unresolved_n") or 0),
                    "playable_unresolved_n": int(settled_card.get("playable_unresolved_n") or 0),
                    "all_unresolved_n": int(settled_card.get("all_unresolved_n") or 0),
                }
                if int(settlement_stage.get("all_unresolved_n") or 0) > 0:
                    diagnostics = _prior_day_settlement_diagnostics(settled_card, str(prior_date))
                    settlement_stage.update(diagnostics)
                    unresolved_count = int(settlement_stage.get("all_unresolved_n") or 0)
                    if bool(diagnostics.get("all_unresolved_games_final")):
                        settlement_stage["status"] = "error"
                        report["errors"].append(
                            f"prior-day card settlement left {unresolved_count} unresolved recommendation(s) even though all affected games are final"
                        )
                    else:
                        report["warnings"].append(
                            f"prior-day card settlement left {unresolved_count} unresolved recommendation(s)"
                        )
            except Exception as exc:
                settlement_stage = {
                    "status": "error",
                    "date": str(prior_date),
                    "card_path": _relative_path_str(prior_card_path),
                    "settlement_path": _relative_path_str(settlement_path),
                    "error": f"{type(exc).__name__}: {exc}",
                }
                report["errors"].append(f"prior-day card settlement failed: {type(exc).__name__}: {exc}")
        else:
            settlement_stage = {
                "status": "skipped",
                "date": str(prior_date),
                "card_path": _relative_path_str(prior_card_path),
                "reason": "prior-day locked-policy card not found",
            }
    else:
        settlement_stage = {
            "status": "skipped",
            "date": str(prior_date),
            "card_path": _relative_path_str(prior_card_path),
            "reason": "settle_prior_card=off",
        }
    report["stages"]["prior_day_card_settlement"] = settlement_stage

    live_lens_stage = _prior_day_live_lens_stage(args, str(prior_date))
    report["stages"]["prior_day_live_lens"] = live_lens_stage
    live_lens_readout = live_lens_stage.get("readout") if isinstance(live_lens_stage.get("readout"), dict) else {}
    report["prior_day"]["live_lens"] = {
        "status": str(live_lens_stage.get("status") or "skipped"),
        "source": live_lens_stage.get("source"),
        "sync_out_path": live_lens_stage.get("synced_report_path") or live_lens_stage.get("sync_out_path"),
        "headline": live_lens_readout.get("headline"),
    }
    if live_lens_readout.get("headline"):
        print(f"[ui-daily] Live-lens readout for {prior_date}: {live_lens_readout.get('headline')}")
        for line in (live_lens_readout.get("learnings") or []):
            print(f"[ui-daily]   learning: {line}")
        for line in (live_lens_readout.get("performance") or []):
            print(f"[ui-daily]   performance: {line}")
    if str(live_lens_stage.get("status") or "") == "warning":
        report["warnings"].append(
            f"prior-day live-lens sync/readout warning: {str(live_lens_stage.get('error') or live_lens_stage.get('reason') or 'unknown')}"
        )

    prior_eval_stage: Dict[str, Any]
    publish_stage: Dict[str, Any]
    if str(getattr(args, "refresh_season_manifests", "on") or "on") == "on":
        refresh_games_scheduled = int((refresh_stage.get("regular_season_games_scheduled") or 0)) if isinstance(refresh_stage, dict) else 0
        if str(refresh_stage.get("status") or "") == "skipped":
            try:
                client = StatsApiClient.with_default_cache(ttl_seconds=int(args.cache_ttl_hours * 3600))
                refresh_games_scheduled = int(
                    sum(
                        1
                        for game in (fetch_schedule_for_date(client, str(prior_date)) or [])
                        if str((game or {}).get("gameType") or "").strip().upper() == "R"
                    )
                )
            except Exception:
                refresh_games_scheduled = 0

        if refresh_games_scheduled <= 0:
            prior_eval_stage = {
                "status": "skipped",
                "date": str(prior_date),
                "report_path": _relative_path_str(prior_report_path),
                "reason": "no scheduled prior-day regular-season games",
            }
            publish_stage = {
                "status": "skipped",
                "season": int(args.season),
                "batch_dir": _relative_path_str(season_batch_dir),
                "reason": "no prior-day report to publish",
            }
        else:
            print(f"[ui-daily] Reconciling prior-day season report for {prior_date}...")
            _ensure_dir(season_batch_dir)
            lineups_last_known_path = game_out / "lineups_last_known_by_team.json"
            eval_cmd = _build_prior_eval_command(
                args=args,
                prior_date=str(prior_date),
                prior_season=int(prior_season),
                out_path=prior_report_path,
                daily_snapshots_root=(game_out / "snapshots"),
                lineups_last_known_path=(lineups_last_known_path if lineups_last_known_path.exists() else None),
            )
            prior_eval_stage = {
                "status": "ok",
                "date": str(prior_date),
                "batch_dir": _relative_path_str(season_batch_dir),
                "report_path": _relative_path_str(prior_report_path),
                "command": [str(part) for part in eval_cmd],
            }
            try:
                eval_rc = subprocess.run(eval_cmd, check=False).returncode
                prior_eval_stage["exit_code"] = int(eval_rc)
                prior_eval_stage["report_exists"] = bool(prior_report_path.exists())
                if eval_rc != 0 or not prior_report_path.exists():
                    prior_eval_stage["status"] = "error"
                    report["errors"].append(
                        f"prior-day eval report refresh failed for {prior_date}"
                    )
                    publish_stage = {
                        "status": "skipped",
                        "season": int(args.season),
                        "batch_dir": _relative_path_str(season_batch_dir),
                        "reason": "prior-day eval report refresh failed",
                    }
                else:
                    print(f"[ui-daily] Publishing rolling season manifests for {args.season}...")
                    publish_stage = {
                        "status": "ok",
                        "season": int(args.season),
                        "batch_dir": _relative_path_str(season_batch_dir),
                    }
                    try:
                        publish_details = _publish_live_season_manifests(
                            season=int(args.season),
                            batch_dir=season_batch_dir,
                            betting_profile=str(getattr(args, "season_betting_profile", "retuned") or "retuned"),
                            season_dir=season_output_dir,
                        )
                        publish_stage.update(publish_details)
                        if int(publish_stage.get("season_betting_exit_code") or 0) != 0:
                            publish_stage["status"] = "error"
                            report["errors"].append(
                                f"season betting-card manifest publish failed with exit {int(publish_stage.get('season_betting_exit_code') or 0)}"
                            )
                    except Exception as exc:
                        publish_stage["status"] = "error"
                        publish_stage["error"] = f"{type(exc).__name__}: {exc}"
                        report["errors"].append(f"season manifest publish failed: {type(exc).__name__}: {exc}")
            except Exception as exc:
                prior_eval_stage["status"] = "error"
                prior_eval_stage["error"] = f"{type(exc).__name__}: {exc}"
                report["errors"].append(f"prior-day eval report refresh failed: {type(exc).__name__}: {exc}")
                publish_stage = {
                    "status": "skipped",
                    "season": int(args.season),
                    "batch_dir": _relative_path_str(season_batch_dir),
                    "reason": "prior-day eval report refresh failed",
                }
    else:
        prior_eval_stage = {
            "status": "skipped",
            "date": str(prior_date),
            "report_path": _relative_path_str(prior_report_path),
            "reason": "refresh_season_manifests=off",
        }
        publish_stage = {
            "status": "skipped",
            "season": int(args.season),
            "batch_dir": _relative_path_str(season_batch_dir),
            "reason": "refresh_season_manifests=off",
        }
    report["stages"]["prior_day_eval_report"] = prior_eval_stage
    report["stages"]["season_publish"] = publish_stage

    prior_top_props_stage: Dict[str, Any]
    prior_top_props_artifact_path = game_out / "top_props" / f"daily_top_props_{prior_token}.json"
    if int(prior_season) != int(args.season):
        prior_top_props_stage = {
            "status": "skipped",
            "date": str(prior_date),
            "artifact_path": _relative_path_str(prior_top_props_artifact_path),
            "reason": "prior-day season differs from requested season",
        }
    elif str(refresh_stage.get("status") or "") == "error":
        prior_top_props_stage = {
            "status": "skipped",
            "date": str(prior_date),
            "artifact_path": _relative_path_str(prior_top_props_artifact_path),
            "reason": "prior-day feed/live refresh failed",
        }
    else:
        print(f"[ui-daily] Building prior-day top-props artifact for {prior_date}...")
        try:
            prior_top_props_result = write_daily_top_props_artifact(
                str(prior_date),
                out_path=prior_top_props_artifact_path,
            )
            prior_top_props_stage = {
                "status": "ok",
                "date": str(prior_date),
                "artifact_path": _relative_path_str(prior_top_props_result.get("path")),
                "group_summaries": dict(prior_top_props_result.get("groupSummaries") or {}),
            }
        except Exception as exc:
            prior_top_props_stage = {
                "status": "error",
                "date": str(prior_date),
                "artifact_path": _relative_path_str(prior_top_props_artifact_path),
                "error": f"{type(exc).__name__}: {exc}",
            }
            report["errors"].append(f"prior-day top-props artifact build failed: {type(exc).__name__}: {exc}")
    report["stages"]["prior_day_top_props_artifact"] = prior_top_props_stage

    odds_stage: Dict[str, Any]
    if str(getattr(args, "refresh_current_oddsapi", "on") or "on") == "on":
        print(f"[ui-daily] Fetching current-day OddsAPI markets for {args.date}...")
        try:
            hitter_markets = [
                part.strip()
                for part in str(getattr(args, "current_oddsapi_hitter_markets", "") or "").split(",")
                if part.strip()
            ]
            odds_stage = fetch_and_write_live_odds_for_date(
                str(args.date),
                overwrite=(str(getattr(args, "current_oddsapi_overwrite", "on") or "on") == "on"),
                regions=str(getattr(args, "current_oddsapi_regions", "us") or "us"),
                bookmakers=(
                    str(getattr(args, "current_oddsapi_bookmakers", "") or "").strip() or None
                ),
                hitter_markets=(hitter_markets or None),
            )
            game_counts = dict(((odds_stage.get("counts") or {}).get("game_lines") or {}))
            pitcher_counts = dict(((odds_stage.get("counts") or {}).get("pitcher_props") or {}))
            hitter_counts = dict(((odds_stage.get("counts") or {}).get("hitter_props") or {}))
            odds_warnings: List[str] = []
            if int(game_counts.get("games") or 0) <= 0:
                odds_warnings.append("current-day OddsAPI ingest captured no game lines")
            elif int(game_counts.get("h2h_games") or 0) > 0 and int(game_counts.get("totals_games") or 0) <= 0:
                odds_warnings.append("current-day OddsAPI game lines currently expose moneylines without totals")
            if int(pitcher_counts.get("players") or 0) <= 0:
                odds_warnings.append("current-day OddsAPI ingest captured no pitcher props")
            if int(hitter_counts.get("players") or 0) <= 0:
                odds_warnings.append("current-day OddsAPI ingest captured no hitter props")
            if odds_warnings and str(odds_stage.get("status") or "") == "ok":
                odds_stage["status"] = "warning"
            if odds_warnings:
                odds_stage["warnings"] = list(odds_warnings)
                report["warnings"].extend(list(odds_warnings))
        except Exception as exc:
            odds_stage = {
                "status": "warning",
                "date": str(args.date),
                "error": f"{type(exc).__name__}: {exc}",
            }
            report["warnings"].append(f"current-day OddsAPI ingest failed: {type(exc).__name__}: {exc}")
    else:
        odds_stage = {
            "status": "skipped",
            "date": str(args.date),
            "reason": "refresh_current_oddsapi=off",
        }
    report["stages"]["current_day_oddsapi"] = odds_stage

    print(f"[ui-daily] Building current-day multi-profile outputs for {args.date}...")
    passthrough_args = _strip_cli_args(
        list(raw_argv),
        flags_with_values=(
            "--workflow",
            "--date",
            "--season",
            "--out",
            "--workflow-out-pitcher",
            "--workflow-out-hitter",
            "--reconcile-date",
            "--refresh-prior-feed-live",
            "--settle-prior-card",
            "--prior-card-settlement-out",
            "--ops-report-out",
            "--sync-live-lens",
            "--live-lens-base-url",
            "--live-lens-cron-token",
            "--live-lens-timeout-seconds",
            "--live-lens-sync-out",
            "--refresh-current-oddsapi",
            "--current-oddsapi-overwrite",
            "--current-oddsapi-regions",
            "--current-oddsapi-bookmakers",
            "--current-oddsapi-hitter-markets",
            "--git-push",
            "--git-push-remote",
            "--git-push-branch",
            "--git-commit-message",
        ),
        flags_no_values=(),
    )
    if not _argv_has_flag(list(raw_argv), "--seed"):
        passthrough_args.extend([
            "--seed",
            str(int(getattr(args, "seed", 1337) or 1337)),
            "--seed-source",
            str(getattr(args, "seed_source", "default_fixed") or "default_fixed"),
        ])
    multi_profile_py = (_ROOT / "tools" / "daily_update_multi_profile.py").resolve()
    cmd = [
        sys.executable,
        str(multi_profile_py),
        "--date",
        str(args.date),
        "--season",
        str(int(args.season)),
        "--python-exe",
        str(Path(sys.executable).resolve()),
        "--out-game",
        str(game_out),
        "--out-pitcher",
        str(pitcher_out),
        "--out-hitter",
        str(hitter_out),
    ]
    cmd.extend(passthrough_args)
    current_stage: Dict[str, Any] = {
        "status": "ok",
        "command": [str(part) for part in cmd],
        "seed": dict(report.get("seed") or {}),
        "summary_path": _relative_path_str(game_out / f"daily_summary_{token}.json"),
        "profile_bundle_path": _relative_path_str(game_out / f"daily_summary_{token}_profile_bundle.json"),
        "locked_policy_path": _relative_path_str(game_out / f"daily_summary_{token}_locked_policy.json"),
    }
    if int(getattr(args, "sims", 0) or 0) < int(_OFFICIAL_CARD_MIN_PUBLISH_SIMS):
        current_stage["official_card_publish_warning"] = (
            f"current-day run is using only {int(getattr(args, 'sims', 0) or 0)} sims; official locked-card publish requires at least {int(_OFFICIAL_CARD_MIN_PUBLISH_SIMS)} sims"
        )
        report["warnings"].append(str(current_stage["official_card_publish_warning"]))
    try:
        rc = subprocess.run(cmd, check=False).returncode
        current_stage["exit_code"] = int(rc)
        summary_path = game_out / f"daily_summary_{token}.json"
        bundle_path = game_out / f"daily_summary_{token}_profile_bundle.json"
        locked_path = game_out / f"daily_summary_{token}_locked_policy.json"
        current_stage["summary_exists"] = bool(summary_path.exists())
        current_stage["profile_bundle_exists"] = bool(bundle_path.exists())
        current_stage["locked_policy_exists"] = bool(locked_path.exists())
        if rc != 0:
            current_stage["status"] = "error"
            report["errors"].append(f"current-day multi-profile build failed with exit {rc}")
        else:
            missing = [
                name
                for name, path in (
                    ("daily_summary", summary_path),
                    ("profile_bundle", bundle_path),
                )
                if not path.exists()
            ]
            if missing:
                current_stage["status"] = "error"
                current_stage["missing_outputs"] = list(missing)
                report["errors"].append(
                    "current-day multi-profile build completed without expected output(s): " + ", ".join(missing)
                )
            elif not locked_path.exists():
                current_stage["status"] = "warning"
                report["warnings"].append("current-day locked-policy card was not written")
            else:
                try:
                    locked_card = json.loads(locked_path.read_text(encoding="utf-8")) or {}
                except Exception as exc:
                    current_stage["status"] = "warning" if current_stage.get("status") == "ok" else current_stage.get("status")
                    current_stage["locked_policy_audit_error"] = f"{type(exc).__name__}: {exc}"
                    report["warnings"].append(f"current-day locked-policy audit unreadable: {type(exc).__name__}: {exc}")
                else:
                    audit_track = locked_card.get("audit_track") if isinstance(locked_card, dict) else None
                    explanation_diagnostics = locked_card.get("explanation_diagnostics") if isinstance(locked_card, dict) else None
                    if isinstance(audit_track, dict):
                        current_stage["official_card_audit_track"] = audit_track
                    if isinstance(explanation_diagnostics, dict):
                        current_stage["official_card_explanation_diagnostics"] = explanation_diagnostics
                        sparse_support_n = int(explanation_diagnostics.get("sparse_support_n") or 0)
                        selected_rows_n = int(explanation_diagnostics.get("selected_rows_n") or 0)
                        if sparse_support_n > 0:
                            report["warnings"].append(
                                f"official locked card has {sparse_support_n} sparse-support selected recommendation(s) out of {selected_rows_n}"
                            )
                    selected_policy = (audit_track or {}).get("selected_support_policy") if isinstance(audit_track, dict) else None
                    if isinstance(selected_policy, dict):
                        removed_n = int(selected_policy.get("removed_sparse_support_n") or 0)
                        replacement_n = int(selected_policy.get("replacement_added_n") or 0)
                        shortfall_n = int(selected_policy.get("selection_shortfall_n") or 0)
                        if removed_n > 0:
                            report["warnings"].append(
                                f"official locked card removed {removed_n} sparse-support selected recommendation(s) before publish and added {replacement_n} replacement(s)"
                            )
                        if shortfall_n > 0:
                            report["warnings"].append(
                                f"official locked card still has {shortfall_n} unfilled slot(s) because no support-qualified replacement was available"
                            )
                    playable_policy = (audit_track or {}).get("playable_support_policy") if isinstance(audit_track, dict) else None
                    if isinstance(playable_policy, dict):
                        removed_n = int(playable_policy.get("removed_sparse_support_n") or 0)
                        if removed_n > 0:
                            report["warnings"].append(
                                f"official locked card removed {removed_n} sparse-support playable candidate(s)"
                            )
    except Exception as exc:
        current_stage["status"] = "error"
        current_stage["error"] = f"{type(exc).__name__}: {exc}"
        report["errors"].append(f"current-day multi-profile build failed: {type(exc).__name__}: {exc}")
    report["stages"]["current_day_multi_profile"] = current_stage

    top_props_stage: Dict[str, Any]
    top_props_artifact_path = game_out / "top_props" / f"daily_top_props_{token}.json"
    if str(current_stage.get("status") or "") == "error":
        top_props_stage = {
            "status": "skipped",
            "date": str(args.date),
            "artifact_path": _relative_path_str(top_props_artifact_path),
            "reason": "current-day multi-profile build failed",
        }
    else:
        print(f"[ui-daily] Building current-day top-props artifact for {args.date}...")
        try:
            top_props_result = write_daily_top_props_artifact(
                str(args.date),
                out_path=top_props_artifact_path,
            )
            top_props_stage = {
                "status": "ok",
                "date": str(args.date),
                "artifact_path": _relative_path_str(top_props_result.get("path")),
                "group_summaries": dict(top_props_result.get("groupSummaries") or {}),
            }
        except Exception as exc:
            top_props_stage = {
                "status": "error",
                "date": str(args.date),
                "artifact_path": _relative_path_str(top_props_artifact_path),
                "error": f"{type(exc).__name__}: {exc}",
            }
            report["errors"].append(f"current-day top-props artifact build failed: {type(exc).__name__}: {exc}")
    report["stages"]["current_day_top_props_artifact"] = top_props_stage

    ladders_stage: Dict[str, Any]
    ladders_artifact_path = game_out / "ladders" / f"daily_ladders_{token}.json"
    if str(current_stage.get("status") or "") == "error":
        ladders_stage = {
            "status": "skipped",
            "date": str(args.date),
            "artifact_path": _relative_path_str(ladders_artifact_path),
            "reason": "current-day multi-profile build failed",
        }
    else:
        print(f"[ui-daily] Building current-day ladders artifact for {args.date}...")
        try:
            ladders_result = write_daily_ladders_artifact(
                str(args.date),
                out_path=ladders_artifact_path,
            )
            ladders_stage = {
                "status": "ok",
                "date": str(args.date),
                "artifact_path": _relative_path_str(ladders_result.get("path")),
                "group_summaries": dict(ladders_result.get("groupSummaries") or {}),
            }
        except Exception as exc:
            ladders_stage = {
                "status": "error",
                "date": str(args.date),
                "artifact_path": _relative_path_str(ladders_artifact_path),
                "error": f"{type(exc).__name__}: {exc}",
            }
            report["errors"].append(f"current-day ladders artifact build failed: {type(exc).__name__}: {exc}")
    report["stages"]["current_day_ladders_artifact"] = ladders_stage

    season_frontend_stage: Dict[str, Any]
    season_frontend_dir = game_out / "season_frontend"
    if str(current_stage.get("status") or "") == "error":
        season_frontend_stage = {
            "status": "skipped",
            "date": str(args.date),
            "dir": _relative_path_str(season_frontend_dir),
            "reason": "current-day multi-profile build failed",
        }
    else:
        print(f"[ui-daily] Building current-day season frontend artifacts for {args.date}...")
        try:
            season_frontend_result = write_current_day_season_frontend_artifacts(
                int(args.season),
                str(args.date),
                betting_profile=str(getattr(args, "season_betting_profile", "retuned") or "retuned"),
                out_dir=season_frontend_dir,
            )
            season_frontend_stage = {
                "status": "ok",
                "date": str(args.date),
                "dir": _relative_path_str(season_frontend_result.get("dir")),
                "profile": season_frontend_result.get("profile"),
                "artifacts": {
                    str(name): {
                        "path": _relative_path_str((info or {}).get("path")),
                        "found": bool((info or {}).get("found")),
                        "error": (info or {}).get("error"),
                    }
                    for name, info in dict(season_frontend_result.get("artifacts") or {}).items()
                },
            }
            soft_artifact_warnings = []
            odds_stage = dict((report.get("stages") or {}).get("current_day_oddsapi") or {})
            odds_counts = dict(odds_stage.get("counts") or {})
            game_line_counts = dict(odds_counts.get("game_lines") or {})
            no_current_day_game_lines = int(game_line_counts.get("games") or 0) <= 0
            for name, info in dict(season_frontend_stage.get("artifacts") or {}).items():
                error_code = str(info.get("error") or "")
                if name == "season_official_betting_day" and error_code == "official_betting_card_day_missing":
                    soft_artifact_warnings.append(
                        "current-day official betting card day artifact has no selected rows yet"
                    )
                    info["error"] = None
                    info["found"] = True
                elif error_code == "season_betting_day_missing" and no_current_day_game_lines:
                    if name == "season_official_betting_day":
                        soft_artifact_warnings.append(
                            "current-day official betting card day artifact was skipped because no game lines were available"
                        )
                    else:
                        soft_artifact_warnings.append(
                            "current-day season betting day artifact was skipped because no game lines were available"
                        )
                    info["error"] = None
                    info["found"] = True
            artifact_errors = [
                f"{name}: {info.get('error')}"
                for name, info in dict(season_frontend_stage.get("artifacts") or {}).items()
                if info.get("error")
            ]
            if artifact_errors:
                season_frontend_stage["status"] = "error"
                report["errors"].append(
                    "current-day season frontend artifact build failed: " + "; ".join(artifact_errors)
                )
            elif soft_artifact_warnings:
                season_frontend_stage["status"] = "partial"
                report["warnings"].extend(soft_artifact_warnings)
        except Exception as exc:
            season_frontend_stage = {
                "status": "error",
                "date": str(args.date),
                "dir": _relative_path_str(season_frontend_dir),
                "error": f"{type(exc).__name__}: {exc}",
            }
            report["errors"].append(f"current-day season frontend artifact build failed: {type(exc).__name__}: {exc}")
    report["stages"]["current_day_season_frontend_artifacts"] = season_frontend_stage

    current_inputs = _current_day_inputs_stage(game_out=game_out, date_str=str(args.date))
    report["stages"]["current_day_roster_snapshot"] = current_inputs["roster_snapshot"]
    report["stages"]["current_day_batting_lineups"] = current_inputs["batting_lineups"]
    report["stages"]["current_day_probable_pitchers"] = current_inputs["probable_pitchers"]
    lineup_stage = current_inputs["batting_lineups"]
    if int(lineup_stage.get("adjusted_teams") or 0) > 0:
        report["warnings"].append(
            f"current-day lineup validation adjusted projected lineups for {int(lineup_stage.get('adjusted_teams') or 0)} team(s)"
        )
    if int(lineup_stage.get("partial_teams") or 0) > 0:
        report["warnings"].append(
            f"current-day lineup validation remained partial for {int(lineup_stage.get('partial_teams') or 0)} team(s)"
        )
    for stage_name in (
        "current_day_roster_snapshot",
        "current_day_batting_lineups",
        "current_day_probable_pitchers",
    ):
        stage_status = str(((report.get("stages") or {}).get(stage_name) or {}).get("status") or "")
        if stage_status == "missing":
            report["errors"].append(f"{stage_name} artifact missing after current-day build")

    if report["errors"]:
        status = "error"
    elif report["warnings"]:
        status = "partial"
    else:
        status = "ok"
    report["status"] = status

    _ensure_dir(ops_report_path.parent)
    _write_json(ops_report_path, report)
    print(f"[ui-daily] Wrote ops report: {ops_report_path}")

    if git_push_enabled and str(report.get("status") or "") != "error":
        print(f"[ui-daily] Committing and pushing workflow outputs for {args.date}...")
        try:
            git_push_result = _maybe_git_push_daily_update(
                repo_root=_ROOT,
                date_str=str(args.date),
                workflow="ui-daily",
                preexisting_changes=preexisting_changes,
                enabled=True,
                remote=str(getattr(args, "git_push_remote", "origin") or "origin"),
                branch=str(getattr(args, "git_push_branch", "") or ""),
                commit_message=str(getattr(args, "git_commit_message", "Daily update {date}") or "Daily update {date}"),
            )
            report["git_push"].update(git_push_result)
            print(
                f"[ui-daily] Git push status: {str(git_push_result.get('status') or 'unknown')}"
                + (f" ({git_push_result.get('commit_sha')})" if git_push_result.get("commit_sha") else "")
            )
        except Exception as exc:
            report["git_push"].update({
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
            })
            report["errors"].append(f"git push failed: {type(exc).__name__}: {exc}")
            report["status"] = "error"
            _write_json(ops_report_path, report)
            return 1

    if str(report.get("status") or "") == "error":
        return int((report.get("stages") or {}).get("current_day_multi_profile", {}).get("exit_code") or 1)
    return 0


def _apply_umpire_shrink(umpire, shrink: float) -> None:
    """Shrink umpire.called_strike_mult toward 1.0.

    shrink=1.0 keeps the raw value.
    shrink=0.0 forces neutral (1.0).
    """

    if umpire is None:
        return

    try:
        s = float(shrink)
    except Exception:
        s = 1.0

    if s >= 0.999:
        return
    if s <= 0.0:
        umpire.called_strike_mult = 1.0
        return

    try:
        old = float(getattr(umpire, "called_strike_mult", 1.0) or 1.0)
    except Exception:
        old = 1.0

    umpire.called_strike_mult = float(1.0 + s * (old - 1.0))


def _maybe_prefetch_statcast_x64(args: argparse.Namespace, snapshot_dir: Path) -> bool:
    """Optionally pre-populate cached Statcast pitch splits via the x64 helper.

    This is useful on Windows ARM64 where `pybaseball` may not install, but it can
    be installed in a side-by-side x64 venv. The simulator reads cache-only.
    """
    mode = str(getattr(args, "statcast_x64_prefetch", "off") or "off").lower()
    if mode == "off":
        return False

    report_path = snapshot_dir / "statcast_fetch_report.json"
    ttl_hours = int(getattr(args, "statcast_cache_ttl_hours", 24 * 14) or (24 * 14))

    # In auto mode, skip if we already fetched recently.
    if mode == "auto" and report_path.exists():
        try:
            age_sec = max(0.0, (datetime.now().timestamp() - report_path.stat().st_mtime))
            if age_sec < float(ttl_hours * 3600):
                print(f"Statcast x64 prefetch: skip (fresh report: {report_path})")
                return False
        except Exception:
            pass

    # Determine x64 python.
    x64_py = str(getattr(args, "statcast_x64_python", "") or "").strip()
    if not x64_py:
        default = _ROOT / ".venv_x64" / "Scripts" / "python.exe"
        x64_py = str(default)

    if not Path(x64_py).exists():
        msg = f"Statcast x64 prefetch: missing x64 python at {x64_py}"
        if mode == "force":
            raise RuntimeError(msg)
        print(msg)
        return False

    tool = _ROOT / "tools" / "statcast" / "fetch_pitcher_pitch_splits_x64.py"
    cmd = [
        x64_py,
        str(tool),
        "--date",
        str(args.date),
        "--season",
        str(int(args.season)),
        "--out-report",
        str(report_path),
    ]

    print("Statcast x64 prefetch: running helper...")
    try:
        r = subprocess.run(cmd, check=False)
        if r.returncode != 0:
            if mode == "force":
                raise RuntimeError(f"Statcast x64 helper failed with exit code {r.returncode}")
            print(f"Statcast x64 prefetch: helper failed (exit {r.returncode}); continuing")
            return False
        return True
    except Exception:
        if mode == "force":
            raise
        print("Statcast x64 prefetch: error; continuing")
        return False


def _maybe_prefetch_umpire_factors_x64(args: argparse.Namespace, snapshot_dir: Path) -> bool:
    """Optionally build Statcast-based umpire called-strike multipliers via the x64 helper.

    The helper writes the local map at MLB-BettingV2/data/umpire/umpire_factors.json,
    which the simulator reads at runtime.
    """
    mode = str(getattr(args, "umpire_x64_prefetch", "off") or "off").lower()
    if mode == "off":
        return False

    report_path = snapshot_dir / "umpire_statcast_report.json"
    ttl_hours = int(getattr(args, "umpire_x64_ttl_hours", 24 * 14) or (24 * 14))

    # In auto mode, skip if we already fetched recently.
    if mode == "auto" and report_path.exists():
        try:
            age_sec = max(0.0, (datetime.now().timestamp() - report_path.stat().st_mtime))
            if age_sec < float(ttl_hours * 3600):
                print(f"Umpire Statcast x64 prefetch: skip (fresh report: {report_path})")
                return False
        except Exception:
            pass

    # Determine x64 python.
    x64_py = str(getattr(args, "umpire_x64_python", "") or "").strip()
    if not x64_py:
        default = _ROOT / ".venv_x64" / "Scripts" / "python.exe"
        x64_py = str(default)

    if not Path(x64_py).exists():
        msg = f"Umpire Statcast x64 prefetch: missing x64 python at {x64_py}"
        if mode == "force":
            raise RuntimeError(msg)
        print(msg)
        return False

    tool = _ROOT / "tools" / "statcast" / "fetch_umpire_factors_x64.py"
    cmd = [
        x64_py,
        str(tool),
        "--date",
        str(args.date),
        "--days-back",
        str(int(getattr(args, "umpire_statcast_days_back", 21) or 21)),
        "--min-pitches",
        str(int(getattr(args, "umpire_statcast_min_pitches", 1500) or 1500)),
        "--out-report",
        str(report_path),
    ]

    print("Umpire Statcast x64 prefetch: running helper...")
    try:
        r = subprocess.run(cmd, check=False)
        if r.returncode != 0:
            if mode == "force":
                raise RuntimeError(f"Umpire Statcast x64 helper failed with exit code {r.returncode}")
            print(f"Umpire Statcast x64 prefetch: helper failed (exit {r.returncode}); continuing")
            return False
        return True
    except Exception as e:
        if mode == "force":
            raise
        print(f"Umpire Statcast x64 prefetch: error ({type(e).__name__}); continuing")
        return False


def _sim_many(
    away_roster,
    home_roster,
    sims: int,
    seed: int,
    workers: int = 1,
    weather=None,
    park=None,
    umpire=None,
    hitter_hr_top_n: int = 0,
    hitter_props_top_n: int = 24,
    hitter_hr_prob_calibration: Optional[Dict[str, Any]] = None,
    hitter_props_prob_calibration: Optional[Dict[str, Any]] = None,
    pitcher_prop_ids: Optional[List[int]] = None,
    cfg_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rng_seed = seed

    away_abbr = str(getattr(getattr(away_roster, "team", None), "abbreviation", ""))
    home_abbr = str(getattr(getattr(home_roster, "team", None), "abbreviation", ""))

    try:
        batter_meta = _build_batter_meta(away_roster, home_roster)
    except Exception:
        batter_meta = {}
    try:
        pitcher_meta = _build_pitcher_meta(away_roster, home_roster)
    except Exception:
        pitcher_meta = {}

    hr_top_n = max(0, int(hitter_hr_top_n or 0))
    props_top_n_raw = int(hitter_props_top_n or 0)
    props_top_n = hr_top_n if props_top_n_raw < 0 else max(0, props_top_n_raw)
    max_top_n = max(hr_top_n, props_top_n)
    want_hitter = bool(batter_meta)
    sum_stats: Dict[int, Dict[str, float]] = {}
    ge_counts: Dict[str, Dict[int, int]] = {}
    prop_ids = [int(x) for x in (pitcher_prop_ids or []) if int(x or 0) > 0]
    prop_acc: Dict[int, Dict[str, Any]] = {}
    hitter_prop_acc: Dict[int, Dict[str, Any]] = {}
    pitcher_box_acc: Dict[int, Dict[str, Any]] = {}
    for pid in prop_ids:
        acc: Dict[str, Any] = {}
        for dist_key, _row_key, mean_key in _PITCHER_PROP_DIST_SPECS:
            acc[str(dist_key)] = {}
            acc[str(mean_key)] = 0.0
        prop_acc[int(pid)] = acc
    if want_hitter and batter_meta:
        for pid in batter_meta.keys():
            acc = {}
            for dist_key, _row_key, mean_key in _HITTER_PROP_DIST_SPECS:
                acc[str(dist_key)] = {}
                acc[str(mean_key)] = 0.0
            hitter_prop_acc[int(pid)] = acc

    def _stat(pid: int, key: str) -> float:
        return float((sum_stats.get(int(pid)) or {}).get(key) or 0.0)

    def _inc_sum(pid: int, key: str, v: float) -> None:
        row = sum_stats.setdefault(int(pid), {})
        row[key] = float(row.get(key, 0.0)) + float(v)

    def _inc_ge(prop_key: str, pid: int) -> None:
        m = ge_counts.setdefault(str(prop_key), {})
        m[int(pid)] = int(m.get(int(pid), 0) + 1)

    def _inc_ge_thresholds(prop_base: str, pid: int, value: int, max_threshold: int) -> None:
        ivalue = int(value)
        for threshold in range(1, int(max_threshold) + 1):
            if ivalue < threshold:
                break
            _inc_ge(f"{prop_base}_{threshold}plus", pid)

    def seg_score(r, innings: int) -> Dict[str, int]:
        a = sum((r.away_inning_runs or [])[:innings])
        h = sum((r.home_inning_runs or [])[:innings])
        return {"away": int(a), "home": int(h)}

    def init_seg():
        return {
            "home_wins": 0,
            "away_wins": 0,
            "ties": 0,
            "away_runs_sum": 0.0,
            "home_runs_sum": 0.0,
            "totals": {},
            "margins": {},
            "samples": [],
        }

    seg_full = init_seg()
    seg_f1 = init_seg()
    seg_f5 = init_seg()
    seg_f3 = init_seg()

    total_sims = int(max(1, sims))
    workers = int(max(1, workers or 1))

    def _merge_seg(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
        dst["home_wins"] += int(src.get("home_wins") or 0)
        dst["away_wins"] += int(src.get("away_wins") or 0)
        dst["ties"] += int(src.get("ties") or 0)
        dst["away_runs_sum"] = float(dst.get("away_runs_sum", 0.0)) + float(src.get("away_runs_sum", 0.0) or 0.0)
        dst["home_runs_sum"] = float(dst.get("home_runs_sum", 0.0)) + float(src.get("home_runs_sum", 0.0) or 0.0)
        for k, v in (src.get("totals") or {}).items():
            kk = int(k)
            dst["totals"][kk] = dst["totals"].get(kk, 0) + int(v)
        for k, v in (src.get("margins") or {}).items():
            kk = int(k)
            dst["margins"][kk] = dst["margins"].get(kk, 0) + int(v)
        for s in (src.get("samples") or []):
            if len(dst["samples"]) >= 50:
                break
            dst["samples"].append(s)

    def _merge_sum_stats(src: Dict[int, Dict[str, float]]) -> None:
        for pid, row in (src or {}).items():
            dst_row = sum_stats.setdefault(int(pid), {})
            for key, value in (row or {}).items():
                dst_row[str(key)] = float(dst_row.get(str(key), 0.0)) + float(value or 0.0)

    def _merge_ge_counts(src: Dict[str, Dict[int, int]]) -> None:
        for prop_key, m in (src or {}).items():
            dst_m = ge_counts.setdefault(str(prop_key), {})
            for pid, c in (m or {}).items():
                dst_m[int(pid)] = int(dst_m.get(int(pid), 0) + int(c))

    def _merge_prop_acc(src: Dict[int, Dict[str, Any]]) -> None:
        for pid, row in (src or {}).items():
            dst_row = prop_acc.setdefault(int(pid), {})
            for dist_key, _row_key, mean_key in _PITCHER_PROP_DIST_SPECS:
                dst_row.setdefault(str(dist_key), {})
                dst_row.setdefault(str(mean_key), 0.0)
            for dist_key, _row_key, _mean_key in _PITCHER_PROP_DIST_SPECS:
                src_dist = (row or {}).get(str(dist_key)) or {}
                dst_dist = dst_row.setdefault(str(dist_key), {})
                for bucket, count in src_dist.items():
                    bucket_i = int(bucket)
                    dst_dist[bucket_i] = int(dst_dist.get(bucket_i, 0) + int(count))
            for _dist_key, _row_key, mean_key in _PITCHER_PROP_DIST_SPECS:
                dst_row[str(mean_key)] = float(dst_row.get(str(mean_key), 0.0)) + float((row or {}).get(str(mean_key), 0.0) or 0.0)

    def _merge_hitter_prop_acc(src: Dict[int, Dict[str, Any]]) -> None:
        for pid, row in (src or {}).items():
            dst_row = hitter_prop_acc.setdefault(int(pid), {})
            for dist_key, _row_key, mean_key in _HITTER_PROP_DIST_SPECS:
                dst_row.setdefault(str(dist_key), {})
                dst_row.setdefault(str(mean_key), 0.0)
            for dist_key, _row_key, _mean_key in _HITTER_PROP_DIST_SPECS:
                src_dist = (row or {}).get(str(dist_key)) or {}
                dst_dist = dst_row.setdefault(str(dist_key), {})
                for bucket, count in src_dist.items():
                    bucket_i = int(bucket)
                    dst_dist[bucket_i] = int(dst_dist.get(bucket_i, 0) + int(count))
            for _dist_key, _row_key, mean_key in _HITTER_PROP_DIST_SPECS:
                dst_row[str(mean_key)] = float(dst_row.get(str(mean_key), 0.0)) + float((row or {}).get(str(mean_key), 0.0) or 0.0)

    def _merge_pitcher_box_acc(src: Dict[int, Dict[str, Any]]) -> None:
        for pid, row in (src or {}).items():
            dst_row = pitcher_box_acc.setdefault(int(pid), _new_pitcher_box_acc_row())
            for key in _PITCHER_BOX_KEYS:
                dst_row[key] = float(dst_row.get(key, 0.0)) + float((row or {}).get(key, 0.0) or 0.0)
            dst_row["appearances"] = int(dst_row.get("appearances", 0) or 0) + int((row or {}).get("appearances", 0) or 0)

    batter_ids = [int(pid) for pid in batter_meta.keys()] if batter_meta else []
    cfg_kwargs = dict(cfg_kwargs or {})
    if workers > 1 and total_sims > 1:
        ctx = multiprocessing.get_context("spawn")
        chunk_count = min(int(workers), int(total_sims))
        chunk_size = int(math.ceil(float(total_sims) / float(chunk_count)))
        chunks = []
        start = 0
        while start < total_sims:
            n = min(chunk_size, total_sims - start)
            chunks.append((int(start), int(n)))
            start += n

        with ProcessPoolExecutor(
            max_workers=int(chunk_count),
            mp_context=ctx,
            initializer=_simw_init,
            initargs=(
                away_roster,
                home_roster,
                int(rng_seed),
                weather,
                park,
                umpire,
                bool(want_hitter),
                batter_ids,
                prop_ids,
                cfg_kwargs,
            ),
        ) as ex:
            futures = [ex.submit(_simw_chunk, st, n) for st, n in chunks]
            for fut in futures:
                res = fut.result()
                _merge_seg(seg_full, res.get("seg_full") or {})
                _merge_seg(seg_f1, res.get("seg_f1") or {})
                _merge_seg(seg_f5, res.get("seg_f5") or {})
                _merge_seg(seg_f3, res.get("seg_f3") or {})
                _merge_sum_stats(res.get("sum_stats") or {})
                _merge_ge_counts(res.get("ge_counts") or {})
                _merge_prop_acc(res.get("prop_acc") or {})
                _merge_hitter_prop_acc(res.get("hitter_prop_acc") or {})
                _merge_pitcher_box_acc(res.get("pitcher_box_acc") or {})
    else:
        for i in range(total_sims):
            cfg = GameConfig(rng_seed=rng_seed + i, weather=weather, park=park, umpire=umpire, **cfg_kwargs)
            r = simulate_game(away_roster, home_roster, cfg)

            ps = r.pitcher_stats or {}
            if ps:
                for pid_raw, row in ps.items():
                    try:
                        pid = int(pid_raw)
                    except Exception:
                        continue
                    if pid <= 0 or not isinstance(row, dict):
                        continue
                    _accumulate_pitcher_box_row(pitcher_box_acc, int(pid), row)

            if prop_ids:
                for pid in prop_ids:
                    row = ps.get(int(pid)) or {}
                    acc = prop_acc[int(pid)]
                    for dist_key, row_key, mean_key in _PITCHER_PROP_DIST_SPECS:
                        try:
                            value = int(round(float(row.get(str(row_key)) or 0.0)))
                        except Exception:
                            value = 0
                        dist = acc.setdefault(str(dist_key), {})
                        dist[int(value)] = int(dist.get(int(value), 0) + 1)
                        acc[str(mean_key)] = float(acc.get(str(mean_key), 0.0) or 0.0) + float(value)

            if want_hitter and batter_meta:
                bs = r.batter_stats or {}
                for pid in batter_meta.keys():
                    row = bs.get(int(pid)) or {}
                    try:
                        pa = int(row.get("PA") or 0)
                        ab = int(row.get("AB") or 0)
                        h = int(row.get("H") or 0)
                        d2 = int(row.get("2B") or 0)
                        d3 = int(row.get("3B") or 0)
                        hr = int(row.get("HR") or 0)
                        rr = int(row.get("R") or 0)
                        rbi = int(row.get("RBI") or 0)
                        bb = int(row.get("BB") or 0)
                        so = int(row.get("SO") or 0)
                        hbp = int(row.get("HBP") or 0)
                        sb = int(row.get("SB") or 0)
                    except Exception:
                        continue
                    tb = int(h + d2 + 2 * d3 + 3 * hr)
                    hitter_stat_values = {
                        "H": h,
                        "HR": hr,
                        "TB": tb,
                        "R": rr,
                        "RBI": rbi,
                        "2B": d2,
                        "3B": d3,
                        "SB": sb,
                    }

                    _inc_sum(pid, "PA", pa)
                    _inc_sum(pid, "AB", ab)
                    _inc_sum(pid, "H", h)
                    _inc_sum(pid, "2B", d2)
                    _inc_sum(pid, "3B", d3)
                    _inc_sum(pid, "HR", hr)
                    _inc_sum(pid, "R", rr)
                    _inc_sum(pid, "RBI", rbi)
                    _inc_sum(pid, "BB", bb)
                    _inc_sum(pid, "SO", so)
                    _inc_sum(pid, "HBP", hbp)
                    _inc_sum(pid, "SB", sb)
                    _inc_sum(pid, "TB", tb)

                    acc = hitter_prop_acc.setdefault(int(pid), {})
                    for dist_key, row_key, mean_key in _HITTER_PROP_DIST_SPECS:
                        value = int(hitter_stat_values.get(str(row_key), 0))
                        dist = acc.setdefault(str(dist_key), {})
                        dist[int(value)] = int(dist.get(int(value), 0) + 1)
                        acc[str(mean_key)] = float(acc.get(str(mean_key), 0.0) or 0.0) + float(value)

                    _inc_ge_thresholds("hits", pid, h, 3)
                    if d2 >= 1:
                        _inc_ge("doubles_1plus", pid)
                    if d3 >= 1:
                        _inc_ge("triples_1plus", pid)
                    if hr >= 1:
                        _inc_ge("hr_1plus", pid)
                    _inc_ge_thresholds("runs", pid, rr, 3)
                    _inc_ge_thresholds("rbi", pid, rbi, 4)
                    _inc_ge_thresholds("total_bases", pid, tb, 5)
                    if sb >= 1:
                        _inc_ge("sb_1plus", pid)

            full = {"away": int(r.away_score), "home": int(r.home_score)}
            f1 = seg_score(r, 1)
            f5 = seg_score(r, 5)
            f3 = seg_score(r, 3)

            for seg, score in ((seg_full, full), (seg_f1, f1), (seg_f5, f5), (seg_f3, f3)):
                if len(seg["samples"]) < 50:
                    seg["samples"].append(score)
                seg["away_runs_sum"] = float(seg.get("away_runs_sum", 0.0)) + float(score["away"])
                seg["home_runs_sum"] = float(seg.get("home_runs_sum", 0.0)) + float(score["home"])
                tot = int(score["away"] + score["home"])
                seg["totals"][tot] = seg["totals"].get(tot, 0) + 1
                margin = int(score["home"] - score["away"])
                seg["margins"][margin] = seg["margins"].get(margin, 0) + 1
                if score["home"] > score["away"]:
                    seg["home_wins"] += 1
                elif score["away"] > score["home"]:
                    seg["away_wins"] += 1
                else:
                    seg["ties"] += 1

    denom = float(max(1, sims))

    def finalize(seg: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "home_win_prob": seg["home_wins"] / denom,
            "away_win_prob": seg["away_wins"] / denom,
            "tie_prob": seg["ties"] / denom,
            "away_runs_mean": float(seg.get("away_runs_sum", 0.0)) / denom,
            "home_runs_mean": float(seg.get("home_runs_sum", 0.0)) / denom,
            "total_runs_dist": seg["totals"],
            "run_margin_dist": seg["margins"],
            "samples": seg["samples"],
        }

    out: Dict[str, Any] = {
        "sims": sims,
        "segments": {
            "full": finalize(seg_full),
            "first1": finalize(seg_f1),
            "first5": finalize(seg_f5),
            "first3": finalize(seg_f3),
        },
    }

    out["aggregate_boxscore"] = _build_aggregate_boxscore(
        sims=int(sims),
        full_segment=out["segments"]["full"],
        batter_meta=batter_meta,
        sum_stats=sum_stats,
        pitcher_meta=pitcher_meta,
        pitcher_box_acc=pitcher_box_acc,
    )

    if prop_acc:
        out["pitcher_props"] = {
            str(int(pid)): {
                **{
                    f"{dist_key}_dist": {str(int(k)): int(v) for k, v in (acc.get(str(dist_key)) or {}).items()}
                    for dist_key, _row_key, _mean_key in _PITCHER_PROP_DIST_SPECS
                },
                **{
                    str(mean_key): float(acc.get(str(mean_key), 0.0)) / float(max(1, sims))
                    for _dist_key, _row_key, mean_key in _PITCHER_PROP_DIST_SPECS
                },
            }
            for pid, acc in prop_acc.items()
        }

    if want_hitter and batter_meta and sims > 0:
        denom_sims = float(max(1, int(sims)))
        eligible_batter_ids = [
            int(pid) for pid, meta in (batter_meta or {}).items() if bool((meta or {}).get("is_lineup_batter"))
        ]
        if not eligible_batter_ids:
            eligible_batter_ids = [
                int(pid) for pid, meta in (batter_meta or {}).items() if isinstance((meta or {}).get("order"), int)
            ]
        if not eligible_batter_ids:
            eligible_batter_ids = [int(pid) for pid in batter_meta.keys()]

        def _p(prop_key: str, pid: int) -> float:
            return float((ge_counts.get(str(prop_key)) or {}).get(int(pid), 0)) / denom_sims

        def _row(pid: int, prop_key: str, p_field: str, stat_key: str, mean_field: str) -> Dict[str, Any]:
            meta = batter_meta.get(int(pid)) or {}
            p0 = _p(prop_key, int(pid))
            p_cal = apply_prop_prob_calibration(float(p0), hitter_props_prob_calibration, prop_key=str(prop_key))
            if str(prop_key) == "hr_1plus":
                p_cal = apply_prop_prob_calibration(float(p0), hitter_hr_prob_calibration, prop_key="hr_1plus")

            return {
                "batter_id": int(pid),
                "name": str(meta.get("name") or ""),
                "team": str(meta.get("team") or ""),
                str(p_field): float(p0),
                str(p_field) + "_cal": float(p_cal),
                str(mean_field): float(_stat(pid, str(stat_key))) / denom_sims,
                "pa_mean": float(_stat(pid, "PA")) / denom_sims,
                "ab_mean": float(_stat(pid, "AB")) / denom_sims,
                "lineup_order": meta.get("order"),
                "is_lineup_batter": bool(meta.get("is_lineup_batter")),
            }

        # HR top-N
        if int(hr_top_n) > 0:
            rows = []
            for pid in eligible_batter_ids:
                rows.append(
                    {
                        "batter_id": int(pid),
                        "name": str((batter_meta.get(int(pid)) or {}).get("name") or ""),
                        "team": str((batter_meta.get(int(pid)) or {}).get("team") or ""),
                        "p_hr_1plus": float(_p("hr_1plus", int(pid))),
                        "p_hr_1plus_cal": float(
                            apply_prop_prob_calibration(
                                float(_p("hr_1plus", int(pid))), hitter_hr_prob_calibration, prop_key="hr_1plus"
                            )
                        ),
                        "hr_mean": float(_stat(pid, "HR")) / denom_sims,
                        "pa_mean": float(_stat(pid, "PA")) / denom_sims,
                        "ab_mean": float(_stat(pid, "AB")) / denom_sims,
                        "lineup_order": (batter_meta.get(int(pid)) or {}).get("order"),
                        "is_lineup_batter": bool((batter_meta.get(int(pid)) or {}).get("is_lineup_batter")),
                    }
                )
            rows.sort(key=lambda r: float(r.get("p_hr_1plus") or 0.0), reverse=True)
            out["hitter_hr_likelihood_topn"] = {"n": int(hr_top_n), "overall": rows[: int(hr_top_n)]}

        # Other hitter props top-N
        if int(props_top_n) > 0:
            mapping = {
                "hits_1plus": ("p_h_1plus", "H", "h_mean"),
                "hits_2plus": ("p_h_2plus", "H", "h_mean"),
                "hits_3plus": ("p_h_3plus", "H", "h_mean"),
                "doubles_1plus": ("p_2b_1plus", "2B", "2b_mean"),
                "triples_1plus": ("p_3b_1plus", "3B", "3b_mean"),
                "runs_1plus": ("p_r_1plus", "R", "r_mean"),
                "runs_2plus": ("p_r_2plus", "R", "r_mean"),
                "runs_3plus": ("p_r_3plus", "R", "r_mean"),
                "rbi_1plus": ("p_rbi_1plus", "RBI", "rbi_mean"),
                "rbi_2plus": ("p_rbi_2plus", "RBI", "rbi_mean"),
                "rbi_3plus": ("p_rbi_3plus", "RBI", "rbi_mean"),
                "rbi_4plus": ("p_rbi_4plus", "RBI", "rbi_mean"),
                "total_bases_1plus": ("p_tb_1plus", "TB", "tb_mean"),
                "total_bases_2plus": ("p_tb_2plus", "TB", "tb_mean"),
                "total_bases_3plus": ("p_tb_3plus", "TB", "tb_mean"),
                "total_bases_4plus": ("p_tb_4plus", "TB", "tb_mean"),
                "total_bases_5plus": ("p_tb_5plus", "TB", "tb_mean"),
                "sb_1plus": ("p_sb_1plus", "SB", "sb_mean"),
            }
            props_out: Dict[str, Any] = {"n": int(props_top_n)}
            for prop_key, (p_field, stat_key, mean_field) in mapping.items():
                rows = [_row(pid, prop_key, p_field, stat_key, mean_field) for pid in eligible_batter_ids]
                rows.sort(key=lambda r: float(r.get(p_field) or 0.0), reverse=True)
                props_out[str(prop_key)] = rows[: int(props_top_n)]
            out["hitter_props_likelihood_topn"] = props_out

    if hitter_prop_acc and batter_meta and sims > 0:
        out["hitter_props"] = {
            str(int(pid)): {
                "batter_id": int(pid),
                "name": str((batter_meta.get(int(pid)) or {}).get("name") or ""),
                "team": str((batter_meta.get(int(pid)) or {}).get("team") or ""),
                "lineup_order": (batter_meta.get(int(pid)) or {}).get("order"),
                "is_lineup_batter": bool((batter_meta.get(int(pid)) or {}).get("is_lineup_batter")),
                "pa_mean": float(_stat(int(pid), "PA")) / denom,
                "ab_mean": float(_stat(int(pid), "AB")) / denom,
                **{
                    f"{dist_key}_dist": {str(int(k)): int(v) for k, v in (acc.get(str(dist_key)) or {}).items()}
                    for dist_key, _row_key, _mean_key in _HITTER_PROP_DIST_SPECS
                },
                **{
                    str(mean_key): float(acc.get(str(mean_key), 0.0)) / float(max(1, sims))
                    for _dist_key, _row_key, mean_key in _HITTER_PROP_DIST_SPECS
                },
            }
            for pid, acc in hitter_prop_acc.items()
        }

    return out


def _load_json_cfg(path_str: str) -> Optional[Dict[str, Any]]:
    s = str(path_str or "").strip()
    if not s or s.lower() in ("off", "false", "0", "none", "null"):
        return None
    p = Path(s)
    if not p.is_absolute():
        p = _ROOT / p
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_jsonish(val: str) -> Optional[Dict[str, Any]]:
    s = str(val or "").strip()
    if not s or s.lower() in ("off", "false", "0", "none", "null"):
        return None
    try:
        if s.startswith("{"):
            obj = json.loads(s)
        else:
            p = Path(s)
            if not p.is_absolute():
                p = _ROOT / p
            if not p.exists():
                return None
            obj = json.loads(p.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _coerce_pitch_model_scalar(field: str, raw_val: str) -> Any:
    """Parse and coerce a PitchModelConfig scalar field override.

    Accepts values as JSON when possible (e.g. 0.15, true, "name").
    Falls back to raw string. Only scalar PitchModelConfig fields are allowed.
    """

    allowed_fields = set(PitchModelConfig.__dataclass_fields__.keys())
    if field not in allowed_fields:
        raise ValueError(f"Unknown PitchModelConfig field: {field}")

    # Only allow scalar typed fields via this path.
    # PitchModelConfig uses postponed annotations, so resolve via get_type_hints.
    ftype = get_type_hints(PitchModelConfig).get(field, PitchModelConfig.__dataclass_fields__[field].type)
    if ftype not in (float, int, str, bool):
        raise ValueError(f"Field is not a supported scalar type for --pm-set: {field}")

    s = str(raw_val).strip()
    if s == "":
        raise ValueError(f"Empty value for --pm-set {field}=")

    parsed: Any
    try:
        # Handle common Python-ish literals.
        if s in ("True", "False", "None"):
            s = {"True": "true", "False": "false", "None": "null"}[s]
        parsed = json.loads(s)
    except Exception:
        parsed = s

    if isinstance(parsed, (dict, list)):
        raise ValueError(f"Non-scalar value not supported for --pm-set: {field}")

    if ftype is float:
        return float(parsed)
    if ftype is int:
        return int(parsed)
    if ftype is bool:
        if isinstance(parsed, bool):
            return bool(parsed)
        if isinstance(parsed, str):
            v = parsed.strip().lower()
            if v in ("true", "1", "yes", "y", "on"):
                return True
            if v in ("false", "0", "no", "n", "off"):
                return False
        return bool(parsed)
    return str(parsed)


def _parse_json_scalar(raw_val: str) -> Any:
    s = str(raw_val).strip()
    if s == "":
        raise ValueError("empty value")
    try:
        if s in ("True", "False", "None"):
            s = {"True": "true", "False": "false", "None": "null"}[s]
        v = json.loads(s)
    except Exception:
        v = s
    if isinstance(v, (dict, list)):
        raise ValueError("non-scalar value")
    return v


def _set_nested_scalar(cfg: Dict[str, Any], dotted_key: str, value: Any) -> None:
    """Set a nested dict value using a dotted key path.

    Examples:
      - mode=affine_logit
      - default.mode=tail_shrink
      - props.hr_1plus.a=0.95
    """

    parts = [p for p in str(dotted_key or "").split(".") if str(p).strip()]
    if not parts:
        raise ValueError("empty key")

    cur: Dict[str, Any] = cfg
    for p in parts[:-1]:
        nxt = cur.get(p)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[p] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _scale_affine_logit_params_toward_identity(a: float, b: float, scale: float) -> Tuple[float, float]:
    """Scale affine-logit calibration toward identity (a=1,b=0).

    scale=1.0 keeps params as-is.
    scale=0.0 turns calibration off (identity).
    """

    s = float(scale)
    if not math.isfinite(s):
        raise ValueError(f"non-finite scale: {scale}")
    # Allow slight extrapolation, but clamp to avoid truly wild params.
    s = float(max(-1.0, min(2.0, s)))
    aa = float(a)
    bb = float(b)
    return (1.0 + (aa - 1.0) * s, bb * s)


def _apply_prop_calibration_scale(cfg: Optional[Dict[str, Any]], prop_key: str, scale: float) -> Optional[Dict[str, Any]]:
    """Apply a scale override for a single prop block in a per-prop wrapper calibration."""

    if not isinstance(cfg, dict) or not cfg:
        return cfg

    props = cfg.get("props")
    if not isinstance(props, dict):
        raise ValueError("calibration cfg is missing 'props' dict; cannot apply per-prop scaling")

    key = str(prop_key)
    if key == "default":
        blk = cfg.get("default")
        if not isinstance(blk, dict) or not blk:
            raise ValueError("calibration cfg missing 'default' block")
        if str(blk.get("mode") or "affine_logit").strip().lower() not in ("", "affine_logit", "logit_affine"):
            return cfg
        a0 = float(blk.get("a") or 1.0)
        b0 = float(blk.get("b") or 0.0)
        a1, b1 = _scale_affine_logit_params_toward_identity(a0, b0, float(scale))
        blk2 = dict(blk)
        blk2["a"] = float(a1)
        blk2["b"] = float(b1)
        out = dict(cfg)
        out["default"] = blk2
        return out

    if key == "all":
        out = dict(cfg)
        props2 = dict(props)
        out["props"] = props2
        for pk, blk in list(props2.items()):
            if not isinstance(blk, dict) or not blk:
                continue
            if str(blk.get("mode") or "affine_logit").strip().lower() not in ("", "affine_logit", "logit_affine"):
                continue
            a0 = float(blk.get("a") or 1.0)
            b0 = float(blk.get("b") or 0.0)
            a1, b1 = _scale_affine_logit_params_toward_identity(a0, b0, float(scale))
            blk2 = dict(blk)
            blk2["a"] = float(a1)
            blk2["b"] = float(b1)
            props2[pk] = blk2
        return out

    blk = props.get(key)
    if not isinstance(blk, dict) or not blk:
        raise ValueError(f"unknown prop key in calibration cfg: {key}")
    if str(blk.get("mode") or "affine_logit").strip().lower() not in ("", "affine_logit", "logit_affine"):
        return cfg
    a0 = float(blk.get("a") or 1.0)
    b0 = float(blk.get("b") or 0.0)
    a1, b1 = _scale_affine_logit_params_toward_identity(a0, b0, float(scale))
    blk2 = dict(blk)
    blk2["a"] = float(a1)
    blk2["b"] = float(b1)
    out = dict(cfg)
    props2 = dict(props)
    props2[key] = blk2
    out["props"] = props2
    return out


def _as_boxscore(game_result) -> Dict[str, Any]:
    # Minimal serializable "boxscore" view.
    return {
        "away_score": int(game_result.away_score),
        "home_score": int(game_result.home_score),
        "innings_played": int(game_result.innings_played),
        "away_inning_runs": [int(x) for x in (game_result.away_inning_runs or [])],
        "home_inning_runs": [int(x) for x in (game_result.home_inning_runs or [])],
        "batter_stats": game_result.batter_stats,
        "pitcher_stats": game_result.pitcher_stats,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="V2 daily updater: core sim rebuild or UI daily ops workflow")
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    ap.add_argument("--season", type=int, default=datetime.now().year)
    ap.add_argument(
        "--workflow",
        choices=["core", "ui-daily"],
        default="core",
        help=(
            "core = single-profile snapshot/sim rebuild used by internal callers; "
            "ui-daily = refresh yesterday's actual feed/PBP cache, settle yesterday's locked card, "
            "refresh today's canonical OddsAPI market snapshot, then build today's multi-profile UI artifacts."
        ),
    )
    ap.add_argument(
        "--spring-mode",
        action="store_true",
        help="Spring training mode: default stats season to prior year and enable roster-type fallbacks for sparse/empty active rosters.",
    )
    ap.add_argument(
        "--stats-season",
        type=int,
        default=0,
        help="Season to use for player season stats (default: --season, or --season-1 when --spring-mode).",
    )
    ap.add_argument("--sims", type=int, default=1000)
    ap.add_argument(
        "--bvp-hr",
        choices=["on", "off"],
        default="on",
        help="If on, apply shrunk batter-vs-starter matchup multipliers from local Statcast raw pitch files.",
    )
    ap.add_argument("--bvp-days-back", type=int, default=365, help="How many days of history to consider for BvP lookup.")
    ap.add_argument("--bvp-min-pa", type=int, default=10, help="Minimum BvP PA required to apply a multiplier.")
    ap.add_argument("--bvp-shrink-pa", type=float, default=50.0, help="Shrinkage PA constant (higher = more shrink toward 1.0).")
    ap.add_argument("--bvp-clamp-lo", type=float, default=0.80, help="Lower clamp for BvP HR multiplier.")
    ap.add_argument("--bvp-clamp-hi", type=float, default=1.25, help="Upper clamp for BvP HR multiplier.")
    ap.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker processes for distribution sims (set to 1 to disable multiprocessing).",
    )
    ap.add_argument(
        "--use-roster-artifacts",
        choices=["on", "off"],
        default="on",
        help="If on, reuse serialized roster artifacts from data/daily/snapshots/<date>/roster_objs/ when present.",
    )
    ap.add_argument(
        "--write-roster-artifacts",
        choices=["on", "off"],
        default="on",
        help="If on, write serialized roster artifacts to data/daily/snapshots/<date>/roster_objs/.",
    )
    ap.add_argument(
        "--roster-events-baseline",
        choices=["off", "on"],
        default="off",
        help="If on, include full baseline player lists in roster_events.json for roster types with no previous snapshot.",
    )
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--seed-source", default="", help=argparse.SUPPRESS)
    ap.add_argument("--out", default=str(_DATA_DIR / "daily"))
    ap.add_argument(
        "--lineups-last-known",
        default="",
        help=(
            "Optional shared path to lineups_last_known_by_team.json. "
            "Defaults to <out>/lineups_last_known_by_team.json when unset."
        ),
    )
    ap.add_argument(
        "--max-games",
        type=int,
        default=0,
        help="If >0, limit processing to the first N games on the schedule (useful for smoke runs).",
    )
    ap.add_argument("--cache-ttl-hours", type=int, default=6)
    ap.add_argument(
        "--statcast-starter-splits",
        choices=["off", "starter"],
        default="starter",
        help="If enabled, enrich probable starters with Statcast-derived pitch mix + pitch-type whiff/in-play multipliers.",
    )
    ap.add_argument("--statcast-cache-ttl-hours", type=int, default=24 * 14)
    ap.add_argument(
        "--statcast-x64-prefetch",
        choices=["off", "auto", "force"],
        default="off",
        help="Optionally run the x64 pybaseball helper to populate cached Statcast splits before simming.",
    )
    ap.add_argument(
        "--statcast-x64-python",
        default="",
        help="Override path to x64 python.exe (defaults to .venv_x64/Scripts/python.exe)",
    )
    ap.add_argument(
        "--pbp",
        choices=["off", "pa", "pitch"],
        default="off",
        help="If enabled, persist play-by-play for ONE representative sim (not for all sims).",
    )
    ap.add_argument(
        "--pbp-max-events",
        type=int,
        default=20000,
        help="Maximum number of PBP events to store (only applies when --pbp != off).",
    )
    ap.add_argument(
        "--workflow-out-pitcher",
        default="",
        help=(
            "Optional pitcher-props output root for --workflow ui-daily. "
            "Default: data/daily_pitcher_props for the canonical UI path, or a sibling <out>_pitcher_props for custom --out roots."
        ),
    )
    ap.add_argument(
        "--workflow-out-hitter",
        default="",
        help=(
            "Optional hitter-props output root for --workflow ui-daily. "
            "Default: data/daily_hitter_props for the canonical UI path, or a sibling <out>_hitter_props for custom --out roots."
        ),
    )
    ap.add_argument(
        "--reconcile-date",
        default="",
        help="Optional prior-day override for --workflow ui-daily (defaults to --date minus one day).",
    )
    ap.add_argument(
        "--refresh-prior-feed-live",
        choices=["on", "off"],
        default="on",
        help="If on, refresh cached StatsAPI feed/live raw for the reconcile date before settlement (includes actual play-by-play).",
    )
    ap.add_argument(
        "--settle-prior-card",
        choices=["on", "off"],
        default="on",
        help="If on, write an exact-settlement artifact for the prior-day locked-policy card during --workflow ui-daily.",
    )
    ap.add_argument(
        "--prior-card-settlement-out",
        default="",
        help="Optional exact-settlement JSON path for the prior-day locked card in --workflow ui-daily.",
    )
    ap.add_argument(
        "--ops-report-out",
        default="",
        help="Optional workflow report JSON path for --workflow ui-daily.",
    )
    ap.add_argument(
        "--sync-live-lens",
        choices=["on", "off"],
        default="on",
        help="If on, fetch the prior-day live-lens report summary from Render during --workflow ui-daily and write a compact readout into the ops report.",
    )
    ap.add_argument(
        "--live-lens-base-url",
        default="",
        help="Optional Render base URL override for the prior-day live-lens sync (defaults to MLB_BETTING_BASE_URL, BASE_URL, RENDER_URL, or RENDER_EXTERNAL_URL).",
    )
    ap.add_argument(
        "--live-lens-cron-token",
        default="",
        help="Optional cron token override for the prior-day live-lens sync (defaults to MLB_BETTING_CRON_TOKEN or CRON_TOKEN).",
    )
    ap.add_argument(
        "--live-lens-timeout-seconds",
        type=int,
        default=45,
        help="HTTP timeout used when syncing the prior-day live-lens report summary during --workflow ui-daily.",
    )
    ap.add_argument(
        "--live-lens-sync-out",
        default="",
        help="Optional local JSON snapshot path for the synced prior-day live-lens report payload during --workflow ui-daily.",
    )
    ap.add_argument(
        "--refresh-current-oddsapi",
        choices=["on", "off"],
        default="on",
        help="If on, refresh the canonical current-day OddsAPI market snapshot before building --workflow ui-daily outputs.",
    )
    ap.add_argument(
        "--current-oddsapi-overwrite",
        choices=["on", "off"],
        default="on",
        help="If on, overwrite canonical current-day OddsAPI files when rerunning --workflow ui-daily.",
    )
    ap.add_argument(
        "--current-oddsapi-regions",
        default="us",
        help="Regions string passed to the current-day OddsAPI fetch during --workflow ui-daily.",
    )
    ap.add_argument(
        "--current-oddsapi-bookmakers",
        default="",
        help="Optional comma-separated bookmaker keys for the current-day OddsAPI fetch during --workflow ui-daily.",
    )
    ap.add_argument(
        "--current-oddsapi-hitter-markets",
        default="",
        help="Optional comma-separated hitter market keys for the current-day OddsAPI fetch during --workflow ui-daily.",
    )
    ap.add_argument(
        "--git-push",
        choices=["on", "off"],
        default="off",
        help="If on, auto-commit and push new repository changes produced by --workflow ui-daily after a successful run.",
    )
    ap.add_argument(
        "--git-push-remote",
        default="origin",
        help="Remote name used when --git-push on (default: origin).",
    )
    ap.add_argument(
        "--git-push-branch",
        default="",
        help="Optional branch used when --git-push on (default: current git branch).",
    )
    ap.add_argument(
        "--git-commit-message",
        default="Daily update {date}",
        help="Commit message template used when --git-push on. Supports {date} and {workflow} placeholders.",
    )
    ap.add_argument(
        "--prior-eval-sims",
        type=int,
        default=0,
        help="Optional sims-per-game override for the prior-day eval report refresh in --workflow ui-daily (default: reuse --sims).",
    )
    ap.add_argument(
        "--prior-eval-prop-lines-source",
        choices=["auto", "oddsapi", "last_known", "bovada", "off"],
        default="auto",
        help="Prop-lines source for the prior-day eval report used to refresh rolling season manifests.",
    )
    ap.add_argument(
        "--prior-reconcile-mode",
        choices=["artifact", "resim"],
        default="artifact",
        help="How --workflow ui-daily refreshes the prior-day sim_vs_actual report: artifact reuses saved daily sims, resim reruns the historical eval job.",
    )
    ap.add_argument(
        "--refresh-season-manifests",
        choices=["on", "off"],
        default="on",
        help="If on, rebuild a rolling prior-day sim_vs_actual report and refresh season eval/betting manifests during --workflow ui-daily.",
    )
    ap.add_argument(
        "--season-batch-dir",
        default="",
        help="Optional rolling batch dir for prior-day season reports in --workflow ui-daily.",
    )
    ap.add_argument(
        "--season-output-dir",
        default="",
        help="Optional season manifest output directory for --workflow ui-daily.",
    )
    ap.add_argument(
        "--season-betting-profile",
        choices=["baseline", "retuned"],
        default="retuned",
        help="Season betting-manifest profile name written by --workflow ui-daily.",
    )
    ap.add_argument(
        "--umpire-x64-prefetch",
        choices=["off", "auto", "force"],
        default="off",
        help="Optionally run the x64 helper to build Statcast-based umpire called-strike multipliers before simming.",
    )
    ap.add_argument(
        "--umpire-x64-python",
        default="",
        help="Override path to x64 python.exe for umpire helper (defaults to .venv_x64/Scripts/python.exe)",
    )
    ap.add_argument("--umpire-x64-ttl-hours", type=int, default=24 * 14)
    ap.add_argument("--umpire-statcast-days-back", type=int, default=21)
    ap.add_argument("--umpire-statcast-min-pitches", type=int, default=1500)
    ap.add_argument(
        "--umpire-shrink",
        type=float,
        default=0.75,
        help="Shrink fetched umpire called_strike_mult toward 1.0. 1.0=no shrink, 0.75=default, 0.0=neutral.",
    )
    ap.add_argument(
        "--hitter-hr-topn",
        type=int,
        default=0,
        help="If >0, include top-N hitter HR likelihood rows in each sim_*.json output.",
    )
    ap.add_argument(
        "--hitter-props-topn",
        type=int,
        default=24,
        help=(
            "Top-N size for broader hitter props (hits/runs/RBI/SB/etc) in each sim_*.json output. "
            "Default 24. -1=use --hitter-hr-topn (back-compat), 0=disable."
        ),
    )
    ap.add_argument(
        "--hitter-hr-prob-calibration",
        default="data/tuning/hitter_hr_calibration/default.json",
        help="Calibration JSON for hitter HR likelihood probabilities (use 'off' to disable)",
    )
    ap.add_argument(
        "--hitter-props-prob-calibration",
        default="data/tuning/hitter_props_calibration/default.json",
        help="Calibration JSON for hitter props likelihood probabilities (use 'off' to disable)",
    )
    ap.add_argument(
        "--hitter-hr-calib-set",
        action="append",
        default=[],
        help=(
            "Override hitter HR calibration config key(s) as key=value (repeatable). "
            "Supports dotted keys for nested dicts. Value is parsed as JSON when possible."
        ),
    )
    ap.add_argument(
        "--hitter-props-calib-set",
        action="append",
        default=[],
        help=(
            "Override hitter props calibration config key(s) as key=value (repeatable). "
            "Supports dotted keys for nested dicts. Value is parsed as JSON when possible."
        ),
    )
    ap.add_argument(
        "--hitter-props-calib-scale",
        action="append",
        default=[],
        help=(
            "Scale affine_logit calibration toward identity for a prop as prop=scale (repeatable). "
            "scale=1 keeps as-is; scale=0 disables calibration for that prop. "
            "Special keys: all=<s>, default=<s>."
        ),
    )
    ap.add_argument(
        "--pitch-model-overrides",
        default="",
        help="JSON dict or path to JSON file to override sim_engine.pitch_model.PitchModelConfig fields (tuning hook)",
    )

    # Convenience pitch-model knobs (merged into --pitch-model-overrides).
    # These are aimed at quick regular-season tuning without writing JSON files.
    ap.add_argument(
        "--pm-k-logit-mult",
        type=float,
        default=None,
        help="Override PitchModelConfig.k_logit_mult (optional; merged into pitch_model_overrides)",
    )
    ap.add_argument(
        "--pm-k-logit-bias",
        type=float,
        default=None,
        help="Override PitchModelConfig.k_logit_bias (optional; merged into pitch_model_overrides)",
    )
    ap.add_argument(
        "--pm-hr-rate-mult",
        type=float,
        default=None,
        help="Override PitchModelConfig.hr_rate_mult (optional; merged into pitch_model_overrides)",
    )
    ap.add_argument(
        "--pm-inplay-hit-rate-mult",
        type=float,
        default=None,
        help="Override PitchModelConfig.inplay_hit_rate_mult (optional; merged into pitch_model_overrides)",
    )
    ap.add_argument(
        "--pm-xb-share-mult",
        type=float,
        default=None,
        help="Override PitchModelConfig.xb_share_mult (optional; merged into pitch_model_overrides)",
    )
    ap.add_argument(
        "--pm-run-env-sigma",
        type=float,
        default=None,
        help="Override PitchModelConfig.run_env_sigma (optional; 0 disables latent run environment)",
    )
    ap.add_argument(
        "--pm-batter-pt-alpha",
        type=float,
        default=None,
        help="Override PitchModelConfig.batter_pt_alpha (optional; 0 disables batter-vs-pitch-type effects)",
    )

    # Additional convenience pitch-model knobs.
    ap.add_argument(
        "--pm-hr-on-bip-factor",
        type=float,
        default=None,
        help="Override PitchModelConfig.hr_on_ball_in_play_factor (optional)",
    )
    ap.add_argument(
        "--pm-bbtype-sample-scale",
        type=float,
        default=None,
        help="Override PitchModelConfig.bbtype_sample_scale (optional; higher=more shrink-to-prior)",
    )
    ap.add_argument(
        "--pm-bbtype-prior-weight",
        type=float,
        default=None,
        help="Override PitchModelConfig.bbtype_prior_weight (optional)",
    )
    ap.add_argument(
        "--pm-bbtype-batter-weight",
        type=float,
        default=None,
        help="Override PitchModelConfig.bbtype_batter_weight (optional)",
    )
    ap.add_argument(
        "--pm-bbtype-pitcher-weight",
        type=float,
        default=None,
        help="Override PitchModelConfig.bbtype_pitcher_weight (optional)",
    )
    ap.add_argument(
        "--pm-run-env-clamp-min",
        type=float,
        default=None,
        help="Override PitchModelConfig.run_env_clamp_min (optional)",
    )
    ap.add_argument(
        "--pm-run-env-clamp-max",
        type=float,
        default=None,
        help="Override PitchModelConfig.run_env_clamp_max (optional)",
    )
    ap.add_argument(
        "--pm-run-env-hr-weight",
        type=float,
        default=None,
        help="Override PitchModelConfig.run_env_hr_weight (optional)",
    )
    ap.add_argument(
        "--pm-run-env-inplay-hit-weight",
        type=float,
        default=None,
        help="Override PitchModelConfig.run_env_inplay_hit_weight (optional)",
    )
    ap.add_argument(
        "--pm-run-env-xb-share-weight",
        type=float,
        default=None,
        help="Override PitchModelConfig.run_env_xb_share_weight (optional)",
    )
    ap.add_argument(
        "--pm-set",
        action="append",
        default=[],
        help=(
            "Extra PitchModelConfig scalar override(s) as field=value. Repeatable. "
            "Value is parsed as JSON when possible (e.g. 0.15, true, \"name\")."
        ),
    )

    ap.add_argument(
        "--pitcher-distribution-overrides",
        default="",
        help="JSON dict or path to JSON file to override sim_engine.pitcher_distributions.PitcherDistributionConfig fields (tuning hook)",
    )
    ap.add_argument(
        "--manager-pitching",
        choices=["off", "legacy", "v2"],
        default="v2",
        help="Pitching change / bullpen management model",
    )
    ap.add_argument(
        "--manager-pitching-overrides",
        default="data/tuning/manager_pitching_overrides/default.json",
        help=(
            "JSON dict or path to JSON file to override manager pitching behavior. "
            "Use --manager-pitching-overrides '' to disable."
        ),
    )
    ap.add_argument(
        "--pitcher-rate-sampling",
        choices=["on", "off"],
        default="on",
        help="Toggle per-game pitcher day-rate sampling (uncertainty)",
    )
    ap.add_argument(
        "--bip-baserunning",
        choices=["on", "off"],
        default="on",
        help="Toggle batted-ball-informed baserunning (DP/SF/advancement)",
    )
    ap.add_argument(
        "--bip-dp-rate",
        type=float,
        default=None,
        help="Override DP rate on in-play ground-ball outs. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-sf-rate-flypop",
        type=float,
        default=None,
        help="Override sac-fly rate for fly/pop outs with runner on 3B. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-sf-rate-line",
        type=float,
        default=None,
        help="Override sac-fly rate for line-drive outs with runner on 3B. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-1b-p2-scores-mult",
        type=float,
        default=None,
        help="Override probability scale for runner on 2B scoring on a 1B. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-2b-p1-scores-mult",
        type=float,
        default=None,
        help="Override probability scale for runner on 1B scoring on a 2B. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-1b-p1-to-3b-rate",
        type=float,
        default=None,
        help="Override probability runner on 1B advances to 3B on a 1B when not forced. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-ground-rbi-out-rate",
        type=float,
        default=None,
        help="Override probability of a ground-ball RBI out with runner on 3B and less than 2 outs. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-out-2b-to-3b-rate",
        type=float,
        default=None,
        help="Override probability runner on 2B advances to 3B on a productive out. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-out-1b-to-2b-rate",
        type=float,
        default=None,
        help="Override probability runner on 1B advances to 2B on a productive out. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-misc-advance-pitch-rate",
        type=float,
        default=None,
        help="Override probability of a WP/PB/balk-style runner advance on a non-in-play pitch. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-roe-rate",
        type=float,
        default=None,
        help="Override probability an in-play out becomes reach-on-error. If omitted, uses GameConfig default.",
    )
    ap.add_argument(
        "--bip-fc-rate",
        type=float,
        default=None,
        help="Override probability of a fielder's-choice style out on a ground ball with runner on 1B. If omitted, uses GameConfig default.",
    )
    raw_argv = list(sys.argv[1:])
    args = ap.parse_args()
    effective_seed, seed_source, seed_explicit = _resolve_effective_seed(args, raw_argv)
    args.seed = int(effective_seed)
    args.seed_source = str(seed_source)
    args.seed_explicit = bool(seed_explicit)

    if str(getattr(args, "workflow", "core") or "core") == "ui-daily":
        return _run_ui_daily_workflow(args, raw_argv=raw_argv)

    spring_mode = bool(getattr(args, "spring_mode", False))
    stats_season = int(getattr(args, "stats_season", 0) or 0)
    if stats_season <= 0:
        stats_season = int(args.season) - 1 if spring_mode else int(args.season)
    args.stats_season = int(stats_season)

    hitter_hr_prob_calibration = _load_json_cfg(str(getattr(args, "hitter_hr_prob_calibration", "") or ""))
    hitter_props_prob_calibration = _load_json_cfg(str(getattr(args, "hitter_props_prob_calibration", "") or ""))

    # Apply optional CLI overrides for hitter calibration configs.
    # This is a convenience layer to avoid editing JSON files during tuning.
    hr_sets = list(getattr(args, "hitter_hr_calib_set", []) or [])
    if hr_sets:
        hitter_hr_prob_calibration = dict(hitter_hr_prob_calibration or {})
        for item in hr_sets:
            s = str(item or "").strip()
            if not s:
                continue
            if "=" not in s:
                raise SystemExit(f"Invalid --hitter-hr-calib-set (expected key=value): {s}")
            k, v = s.split("=", 1)
            k = str(k).strip()
            if not k:
                raise SystemExit(f"Invalid --hitter-hr-calib-set (empty key): {s}")
            try:
                _set_nested_scalar(hitter_hr_prob_calibration, k, _parse_json_scalar(v))
            except Exception as e:
                raise SystemExit(f"Invalid --hitter-hr-calib-set {k}={v}: {e}")

    props_sets = list(getattr(args, "hitter_props_calib_set", []) or [])
    if props_sets:
        hitter_props_prob_calibration = dict(hitter_props_prob_calibration or {})
        for item in props_sets:
            s = str(item or "").strip()
            if not s:
                continue
            if "=" not in s:
                raise SystemExit(f"Invalid --hitter-props-calib-set (expected key=value): {s}")
            k, v = s.split("=", 1)
            k = str(k).strip()
            if not k:
                raise SystemExit(f"Invalid --hitter-props-calib-set (empty key): {s}")
            try:
                _set_nested_scalar(hitter_props_prob_calibration, k, _parse_json_scalar(v))
            except Exception as e:
                raise SystemExit(f"Invalid --hitter-props-calib-set {k}={v}: {e}")

    props_scales = list(getattr(args, "hitter_props_calib_scale", []) or [])
    if props_scales:
        hitter_props_prob_calibration = dict(hitter_props_prob_calibration or {})
        for item in props_scales:
            s = str(item or "").strip()
            if not s:
                continue
            if "=" not in s:
                raise SystemExit(f"Invalid --hitter-props-calib-scale (expected prop=scale): {s}")
            k, v = s.split("=", 1)
            k = str(k).strip()
            if not k:
                raise SystemExit(f"Invalid --hitter-props-calib-scale (empty prop): {s}")
            try:
                sc = float(str(v).strip())
            except Exception as e:
                raise SystemExit(f"Invalid --hitter-props-calib-scale {k}={v}: {e}")
            try:
                hitter_props_prob_calibration = _apply_prop_calibration_scale(hitter_props_prob_calibration, prop_key=k, scale=sc)
            except Exception as e:
                raise SystemExit(f"Invalid --hitter-props-calib-scale {k}={v}: {e}")

    pitch_model_overrides = _load_jsonish(str(getattr(args, "pitch_model_overrides", "") or ""))
    pitcher_distribution_overrides = _load_jsonish(str(getattr(args, "pitcher_distribution_overrides", "") or ""))
    manager_pitching_overrides = _load_jsonish(str(getattr(args, "manager_pitching_overrides", "") or ""))
    probable_pitcher_overrides = _load_json_if_exists(_TRACKED_DATA_DIR / "manager" / "probable_pitcher_overrides.json")

    # Merge convenience pitch-model knobs into overrides (CLI flags win).
    pitch_model_overrides = dict(pitch_model_overrides or {})
    if getattr(args, "pm_k_logit_mult", None) is not None:
        pitch_model_overrides["k_logit_mult"] = float(getattr(args, "pm_k_logit_mult"))
    if getattr(args, "pm_k_logit_bias", None) is not None:
        pitch_model_overrides["k_logit_bias"] = float(getattr(args, "pm_k_logit_bias"))
    if getattr(args, "pm_hr_rate_mult", None) is not None:
        pitch_model_overrides["hr_rate_mult"] = float(getattr(args, "pm_hr_rate_mult"))
    if getattr(args, "pm_inplay_hit_rate_mult", None) is not None:
        pitch_model_overrides["inplay_hit_rate_mult"] = float(getattr(args, "pm_inplay_hit_rate_mult"))
    if getattr(args, "pm_xb_share_mult", None) is not None:
        pitch_model_overrides["xb_share_mult"] = float(getattr(args, "pm_xb_share_mult"))
    if getattr(args, "pm_run_env_sigma", None) is not None:
        pitch_model_overrides["run_env_sigma"] = float(getattr(args, "pm_run_env_sigma"))
    if getattr(args, "pm_batter_pt_alpha", None) is not None:
        pitch_model_overrides["batter_pt_alpha"] = float(getattr(args, "pm_batter_pt_alpha"))
    if getattr(args, "pm_hr_on_bip_factor", None) is not None:
        pitch_model_overrides["hr_on_ball_in_play_factor"] = float(getattr(args, "pm_hr_on_bip_factor"))
    if getattr(args, "pm_bbtype_sample_scale", None) is not None:
        pitch_model_overrides["bbtype_sample_scale"] = float(getattr(args, "pm_bbtype_sample_scale"))
    if getattr(args, "pm_bbtype_prior_weight", None) is not None:
        pitch_model_overrides["bbtype_prior_weight"] = float(getattr(args, "pm_bbtype_prior_weight"))
    if getattr(args, "pm_bbtype_batter_weight", None) is not None:
        pitch_model_overrides["bbtype_batter_weight"] = float(getattr(args, "pm_bbtype_batter_weight"))
    if getattr(args, "pm_bbtype_pitcher_weight", None) is not None:
        pitch_model_overrides["bbtype_pitcher_weight"] = float(getattr(args, "pm_bbtype_pitcher_weight"))
    if getattr(args, "pm_run_env_clamp_min", None) is not None:
        pitch_model_overrides["run_env_clamp_min"] = float(getattr(args, "pm_run_env_clamp_min"))
    if getattr(args, "pm_run_env_clamp_max", None) is not None:
        pitch_model_overrides["run_env_clamp_max"] = float(getattr(args, "pm_run_env_clamp_max"))
    if getattr(args, "pm_run_env_hr_weight", None) is not None:
        pitch_model_overrides["run_env_hr_weight"] = float(getattr(args, "pm_run_env_hr_weight"))
    if getattr(args, "pm_run_env_inplay_hit_weight", None) is not None:
        pitch_model_overrides["run_env_inplay_hit_weight"] = float(getattr(args, "pm_run_env_inplay_hit_weight"))
    if getattr(args, "pm_run_env_xb_share_weight", None) is not None:
        pitch_model_overrides["run_env_xb_share_weight"] = float(getattr(args, "pm_run_env_xb_share_weight"))

    pm_set_items = list(getattr(args, "pm_set", []) or [])
    if pm_set_items:
        for item in pm_set_items:
            s = str(item or "").strip()
            if not s:
                continue
            if "=" not in s:
                raise SystemExit(f"Invalid --pm-set (expected field=value): {s}")
            k, v = s.split("=", 1)
            k = str(k).strip()
            if not k:
                raise SystemExit(f"Invalid --pm-set (empty field): {s}")
            try:
                pitch_model_overrides[str(k)] = _coerce_pitch_model_scalar(str(k), str(v))
            except Exception as e:
                raise SystemExit(f"Invalid --pm-set {k}={v}: {e}")

    cfg_defaults = GameConfig()
    cfg_kwargs: Dict[str, Any] = {
        "bip_baserunning": (str(getattr(args, "bip_baserunning", "on")) == "on"),
        "bip_dp_rate": float(args.bip_dp_rate) if args.bip_dp_rate is not None else float(cfg_defaults.bip_dp_rate),
        "bip_sf_rate_flypop": float(args.bip_sf_rate_flypop) if args.bip_sf_rate_flypop is not None else float(cfg_defaults.bip_sf_rate_flypop),
        "bip_sf_rate_line": float(args.bip_sf_rate_line) if args.bip_sf_rate_line is not None else float(cfg_defaults.bip_sf_rate_line),
        "bip_1b_p2_scores_mult": float(args.bip_1b_p2_scores_mult) if args.bip_1b_p2_scores_mult is not None else float(cfg_defaults.bip_1b_p2_scores_mult),
        "bip_2b_p1_scores_mult": float(args.bip_2b_p1_scores_mult) if args.bip_2b_p1_scores_mult is not None else float(cfg_defaults.bip_2b_p1_scores_mult),
        "bip_1b_p1_to_3b_rate": float(args.bip_1b_p1_to_3b_rate) if args.bip_1b_p1_to_3b_rate is not None else float(cfg_defaults.bip_1b_p1_to_3b_rate),
        "bip_ground_rbi_out_rate": float(args.bip_ground_rbi_out_rate) if args.bip_ground_rbi_out_rate is not None else float(cfg_defaults.bip_ground_rbi_out_rate),
        "bip_out_2b_to_3b_rate": float(args.bip_out_2b_to_3b_rate) if args.bip_out_2b_to_3b_rate is not None else float(cfg_defaults.bip_out_2b_to_3b_rate),
        "bip_out_1b_to_2b_rate": float(args.bip_out_1b_to_2b_rate) if args.bip_out_1b_to_2b_rate is not None else float(cfg_defaults.bip_out_1b_to_2b_rate),
        "bip_misc_advance_pitch_rate": float(args.bip_misc_advance_pitch_rate) if args.bip_misc_advance_pitch_rate is not None else float(cfg_defaults.bip_misc_advance_pitch_rate),
        "bip_roe_rate": float(args.bip_roe_rate) if args.bip_roe_rate is not None else float(cfg_defaults.bip_roe_rate),
        "bip_fc_rate": float(args.bip_fc_rate) if args.bip_fc_rate is not None else float(cfg_defaults.bip_fc_rate),
        "pitcher_rate_sampling": (str(getattr(args, "pitcher_rate_sampling", "on")) == "on"),
        "manager_pitching": str(getattr(args, "manager_pitching", "v2") or "v2"),
        "manager_pitching_overrides": (manager_pitching_overrides or {}),
        "pitch_model_overrides": (pitch_model_overrides or {}),
        "pitcher_distribution_overrides": (pitcher_distribution_overrides or {}),
    }

    out_root = Path(args.out)
    _ensure_dir(out_root)
    snapshot_dir = out_root / "snapshots" / args.date
    sim_dir = out_root / "sims" / args.date
    _ensure_dir(snapshot_dir)
    _ensure_dir(sim_dir)
    roster_obj_dir = snapshot_dir / "roster_objs"
    _ensure_dir(roster_obj_dir)
    _sync_oddsapi_market_snapshots(str(args.date), snapshot_dir)

    # Persist run metadata for debugging/repro.
    _write_json(
        snapshot_dir / "meta.json",
        {
            "date": str(args.date),
            "season": int(args.season),
            "stats_season": int(args.stats_season),
            "spring_mode": bool(spring_mode),
            "seed": int(getattr(args, "seed", 1337) or 1337),
            "seed_source": str(getattr(args, "seed_source", "default_fixed") or "default_fixed"),
            "seed_explicit_cli": bool(getattr(args, "seed_explicit", False)),
            "cfg_kwargs": cfg_kwargs,
            "hitter_hr_prob_calibration": hitter_hr_prob_calibration,
            "hitter_props_prob_calibration": hitter_props_prob_calibration,
            "generated_at": datetime.now().isoformat(),
        },
    )

    client = StatsApiClient.with_default_cache(ttl_seconds=int(args.cache_ttl_hours * 3600))

    statcast_cache = None
    statcast_ttl_seconds = None
    if args.statcast_starter_splits != "off":
        statcast_ttl_seconds = int(args.statcast_cache_ttl_hours * 3600)
        statcast_cache = default_statcast_cache(ttl_seconds=statcast_ttl_seconds)

    bvp_hr_on = True if str(args.bvp_hr) == "on" else False
    try:
        daily_date = datetime.fromisoformat(str(args.date)).date()
    except Exception:
        daily_date = datetime.strptime(str(args.date), "%Y-%m-%d").date()
    bvp_days_back = max(0, int(args.bvp_days_back))
    bvp_start_date = daily_date - timedelta(days=bvp_days_back)
    bvp_min_pa = max(1, int(args.bvp_min_pa))
    bvp_shrink_pa = float(args.bvp_shrink_pa)
    bvp_clamp_lo = float(args.bvp_clamp_lo)
    bvp_clamp_hi = float(args.bvp_clamp_hi)
    bvp_cache = default_bvp_cache() if bvp_hr_on else None

    games = fetch_schedule_for_date(client, args.date)
    if not games:
        print(f"No games found for {args.date}")
        return 2

    if int(getattr(args, "max_games", 0) or 0) > 0:
        games = list(games)[: int(args.max_games)]

    # Save raw schedule snapshot
    _write_json(snapshot_dir / "schedule_raw.json", games)

    # Scaffold artifacts: snapshot raw per-team rosters + normalized injuries (best-effort).
    # Prefer snapshotting the entire league so artifacts exist even for off-days.
    injuries_by_team_id: Dict[int, List[int]] = {}
    teams_by_id: Dict[int, Dict[str, Any]] = {}
    roster_artifacts: Dict[str, Any] = {
        "date": str(args.date),
        "season": int(args.season),
        "spring_mode": bool(spring_mode),
        "teams": {},
        "errors": [],
    }
    try:
        try:
            league_teams = fetch_mlb_teams(client, season=int(args.season))
        except Exception:
            league_teams = []

        if league_teams:
            for t in league_teams:
                if not isinstance(t, dict):
                    continue
                try:
                    tid = int(t.get("id") or 0)
                except Exception:
                    tid = 0
                if tid <= 0:
                    continue
                teams_by_id[tid] = {
                    "id": tid,
                    "name": t.get("name"),
                    "abbreviation": _abbr(t),
                }
        else:
            # Fallback: only teams on today's schedule.
            for g in games or []:
                for side in ("away", "home"):
                    t = (((g.get("teams") or {}).get(side) or {}).get("team") or {})
                    try:
                        tid = int(t.get("id") or 0)
                    except Exception:
                        tid = 0
                    if tid <= 0:
                        continue
                    teams_by_id[tid] = {
                        "id": tid,
                        "name": t.get("name"),
                        "abbreviation": _abbr(t),
                    }

        roster_types: List[str] = ["active", "40Man"]
        if spring_mode:
            roster_types.append("nonRosterInvitees")

        def _status_is_injured(status_obj: Any) -> bool:
            if not isinstance(status_obj, dict) or not status_obj:
                return False
            code = str(status_obj.get("code") or "").strip().upper()
            desc = str(status_obj.get("description") or "").strip().lower()
            if code.startswith("IL") or code.startswith("DL"):
                return True
            # StatsAPI often uses D10/D15/D60/etc for disabled list.
            if len(code) >= 2 and code.startswith("D") and any(ch.isdigit() for ch in code[1:]):
                return True
            if "injured list" in desc or "disabled list" in desc:
                return True
            return False

        injuries_artifacts: Dict[str, Any] = {
            "date": str(args.date),
            "season": int(args.season),
            "spring_mode": bool(spring_mode),
            "teams": {},
            "players": [],
        }

        for tid, tinfo in sorted(teams_by_id.items(), key=lambda kv: kv[0]):
            team_obj: Dict[str, Any] = {"team": tinfo, "rosters": {}}
            injured_ids: set[int] = set()
            injured_players: List[Dict[str, Any]] = []
            for rt in roster_types:
                try:
                    entries = fetch_team_roster(client, int(tid), roster_type=str(rt), date_str=str(args.date))
                    team_obj["rosters"][rt] = entries
                    for e in entries or []:
                        if not isinstance(e, dict):
                            continue
                        person = (e.get("person") or {})
                        try:
                            pid = int(person.get("id") or 0)
                        except Exception:
                            pid = 0
                        if pid <= 0:
                            continue
                        status = e.get("status") or {}
                        if _status_is_injured(status):
                            injured_ids.add(int(pid))
                            injured_players.append(
                                {
                                    "player_id": int(pid),
                                    "full_name": person.get("fullName"),
                                    "status": status,
                                    "position": e.get("position"),
                                    "roster_type_source": str(rt),
                                }
                            )
                except Exception as e:
                    team_obj["rosters"][rt] = []
                    roster_artifacts["errors"].append(
                        {
                            "team_id": int(tid),
                            "roster_type": str(rt),
                            "error": f"{type(e).__name__}: {e}",
                        }
                    )

            # Persist a per-team roster history file for longitudinal tracking.
            try:
                update_team_roster_registry(
                    team_id=int(tid),
                    team_abbr=str(tinfo.get("abbreviation") or ""),
                    date_str=str(args.date),
                    rosters_by_type=team_obj.get("rosters") or {},
                )
            except Exception:
                pass
            roster_artifacts["teams"][str(tid)] = team_obj

            injuries_by_team_id[int(tid)] = sorted(injured_ids)
            injuries_artifacts["teams"][str(tid)] = {
                "team": tinfo,
                "injured_ids": sorted(injured_ids),
                "injured": injured_players,
            }
            for row in injured_players:
                injuries_artifacts["players"].append({"team_id": int(tid), **row})

        _write_json(snapshot_dir / "team_rosters_raw.json", roster_artifacts)
        _write_json(snapshot_dir / "injuries_raw.json", injuries_artifacts)

        # Emit a single daily roster-events artifact derived from the per-team registries.
        try:
            events = build_roster_events_for_date(date_str=str(args.date), include_baseline=(args.roster_events_baseline == "on"))
            _write_json(snapshot_dir / "roster_events.json", events)
        except Exception:
            pass
    except Exception:
        pass

    # Optional x64 prefetch (cache population) step.
    if args.statcast_starter_splits != "off" and args.statcast_x64_prefetch != "off":
        _maybe_prefetch_statcast_x64(args, snapshot_dir)

    # Optional x64 prefetch (umpire factor population) step.
    if args.umpire_x64_prefetch != "off":
        _maybe_prefetch_umpire_factors_x64(args, snapshot_dir)

    # Pre-compute bullpen availability multipliers from recent workload (best-effort).
    # We only consult raw feed/live files (fast, no extra API calls).
    pitcher_availability_by_team: Dict[int, Dict[int, float]] = {}
    try:
        today = datetime.strptime(str(args.date), "%Y-%m-%d").date()
        weights = {1: 1.0, 2: 0.6, 3: 0.4}
        pitches_by_team_day: Dict[int, Dict[int, Dict[int, int]]] = {}
        pitched_days_by_team_pitcher: Dict[int, Dict[int, set[int]]] = {}

        for days_ago in (1, 2, 3):
            d = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            sched = fetch_schedule_for_date(client, d)
            for g in sched or []:
                try:
                    game_pk = int(g.get("gamePk") or 0)
                except Exception:
                    game_pk = 0
                if game_pk <= 0:
                    continue
                away = (g.get("teams") or {}).get("away") or {}
                home = (g.get("teams") or {}).get("home") or {}
                away_team = away.get("team") or {}
                home_team = home.get("team") or {}
                try:
                    away_id = int(away_team.get("id") or 0)
                    home_id = int(home_team.get("id") or 0)
                except Exception:
                    continue
                if away_id <= 0 or home_id <= 0:
                    continue

                feed = load_feed_live_from_raw(int(args.season), d, game_pk)
                if not isinstance(feed, dict) or not feed:
                    continue

                for tid in (away_id, home_id):
                    pitches = extract_team_pitcher_pitches_thrown(feed, tid)
                    if not pitches:
                        continue
                    td = pitches_by_team_day.setdefault(tid, {})
                    day_map = td.setdefault(days_ago, {})
                    pitched_days = pitched_days_by_team_pitcher.setdefault(tid, {})
                    for pid, pth in pitches.items():
                        day_map[pid] = int(day_map.get(pid, 0) + int(pth or 0))
                        if int(pth or 0) > 0:
                            pitched_days.setdefault(pid, set()).add(int(days_ago))

        for tid, by_day in pitches_by_team_day.items():
            weighted: Dict[int, float] = {}
            for days_ago, pitch_map in (by_day or {}).items():
                w = float(weights.get(int(days_ago), 0.0))
                if w <= 0:
                    continue
                for pid, pth in (pitch_map or {}).items():
                    weighted[int(pid)] = float(weighted.get(int(pid), 0.0)) + w * float(pth or 0.0)

            avail_map: Dict[int, float] = {}
            pitched_days = pitched_days_by_team_pitcher.get(tid, {})
            for pid, wp in weighted.items():
                # 120 weighted pitches ~= fully gassed; clamp to a conservative floor.
                avail = max(0.35, 1.0 - (float(wp) / 120.0))
                days = pitched_days.get(int(pid), set())
                if 1 in days and 2 in days and 3 in days:
                    avail *= 0.75
                elif 1 in days and 2 in days:
                    avail *= 0.85
                avail_map[int(pid)] = float(max(0.25, min(1.0, avail)))

            pitcher_availability_by_team[int(tid)] = avail_map
    except Exception:
        pitcher_availability_by_team = {}

    outputs: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    # Lineups: persist confirmed lineups + projected (last-known) fallback.
    last_known_path = (
        _resolve_path_arg(
            str(args.lineups_last_known),
            default=(out_root / "lineups_last_known_by_team.json"),
        )
        if str(getattr(args, "lineups_last_known", "") or "").strip()
        else (out_root / "lineups_last_known_by_team.json")
    )
    last_known_by_team: Dict[str, Any] = {}
    try:
        if last_known_path.exists():
            last_known_by_team = json.loads(last_known_path.read_text(encoding="utf-8"))
            if not isinstance(last_known_by_team, dict):
                last_known_by_team = {}
    except Exception:
        last_known_by_team = {}

    def _normalize_lineup_ids(ids: Any) -> List[int]:
        if not ids:
            return []
        out: List[int] = []
        seen = set()
        for x in ids or []:
            try:
                pid = int(x)
            except Exception:
                continue
            if pid <= 0 or pid in seen:
                continue
            seen.add(pid)
            out.append(pid)
        return out

    def _extract_hitter_ids_from_roster_entries(entries: Any, *, excluded_ids: Optional[set[int]] = None) -> List[int]:
        out: List[int] = []
        seen = set()
        blocked = set(int(x) for x in (excluded_ids or set()) if int(x) > 0)
        for entry in entries or []:
            if not isinstance(entry, dict):
                continue
            try:
                pid = int(entry.get("player_id") or ((entry.get("person") or {}).get("id") or 0))
            except Exception:
                pid = 0
            if pid <= 0 or pid in seen or pid in blocked:
                continue
            pos_obj = entry.get("position") or {}
            pos = str(entry.get("primary_pos_abbr") or entry.get("pos") or pos_obj.get("abbreviation") or "").strip().upper()
            pos_type = str(pos_obj.get("type") or "").strip().lower()
            if pos == "P" or pos_type == "pitcher":
                continue
            seen.add(pid)
            out.append(pid)
        return out

    def _projected_lineup_roster_keys(team_id: int) -> List[str]:
        team_block = ((roster_artifacts.get("teams") or {}).get(str(int(team_id))) or {}) if isinstance(roster_artifacts, dict) else {}
        rosters = (team_block.get("rosters") or {}) if isinstance(team_block, dict) else {}
        excluded_ids = set(int(x) for x in (injuries_by_team_id.get(int(team_id), []) or []) if int(x) > 0)

        keys: List[str] = []
        active_pool = _extract_hitter_ids_from_roster_entries(rosters.get("active") or [], excluded_ids=excluded_ids)
        if active_pool:
            keys.append("active")

        fallback_pool = _extract_hitter_ids_from_roster_entries(rosters.get("40Man") or [], excluded_ids=excluded_ids)
        if fallback_pool:
            keys.append("40Man")

        nri_pool = _extract_hitter_ids_from_roster_entries(rosters.get("nonRosterInvitees") or [], excluded_ids=excluded_ids)
        if nri_pool:
            keys.append("nonRosterInvitees")

        return keys

    def _tighten_projected_lineup(team_id: int, projected_ids: Any) -> Dict[str, Any]:
        raw_ids = _normalize_lineup_ids(projected_ids)
        if not raw_ids:
            return {
                "status": "none",
                "raw_ids": [],
                "ids": [],
                "missing_ids": [],
                "backfilled_ids": [],
                "pool_source": "none",
                "pool_size": 0,
            }

        team_block = ((roster_artifacts.get("teams") or {}).get(str(int(team_id))) or {}) if isinstance(roster_artifacts, dict) else {}
        rosters = (team_block.get("rosters") or {}) if isinstance(team_block, dict) else {}
        excluded_ids = set(int(x) for x in (injuries_by_team_id.get(int(team_id), []) or []) if int(x) > 0)
        active_pool = _extract_hitter_ids_from_roster_entries(rosters.get("active") or [], excluded_ids=excluded_ids)
        fallback_40_pool = _extract_hitter_ids_from_roster_entries(rosters.get("40Man") or [], excluded_ids=excluded_ids)
        nri_pool = _extract_hitter_ids_from_roster_entries(rosters.get("nonRosterInvitees") or [], excluded_ids=excluded_ids)

        usable_pool: List[int] = []
        seen_pool_ids: set[int] = set()
        pool_labels: List[str] = []
        for roster_key, pool in (("active", active_pool), ("40Man", fallback_40_pool), ("nonRosterInvitees", nri_pool)):
            if not pool:
                continue
            pool_labels.append(str(roster_key))
            for pid in pool:
                pid_i = int(pid)
                if pid_i <= 0 or pid_i in seen_pool_ids:
                    continue
                seen_pool_ids.add(pid_i)
                usable_pool.append(pid_i)
        pool_source = "+".join(pool_labels) if pool_labels else "none"
        usable_set = set(int(pid) for pid in usable_pool)

        kept_ids = [int(pid) for pid in raw_ids if int(pid) in usable_set]
        missing_ids = [int(pid) for pid in raw_ids if int(pid) not in usable_set]
        backfilled_ids: List[int] = []
        if len(kept_ids) < 9:
            for pid in usable_pool:
                pid_i = int(pid)
                if pid_i in kept_ids:
                    continue
                kept_ids.append(pid_i)
                backfilled_ids.append(pid_i)
                if len(kept_ids) >= 9:
                    break

        status = "ok"
        if missing_ids or backfilled_ids:
            status = "adjusted" if len(kept_ids) >= 9 else "partial"
        elif len(kept_ids) < 9:
            status = "partial"

        return {
            "status": status,
            "raw_ids": [int(pid) for pid in raw_ids],
            "ids": [int(pid) for pid in kept_ids[:9]],
            "missing_ids": [int(pid) for pid in missing_ids],
            "backfilled_ids": [int(pid) for pid in backfilled_ids],
            "pool_source": str(pool_source),
            "pool_size": int(len(usable_pool)),
        }

    def _preferred_roster_entries_for_builder(
        team_id: int,
        *,
        probable_pitcher_id: Any,
        lineup_ids: Any,
    ) -> Optional[List[Dict[str, Any]]]:
        team_block = ((roster_artifacts.get("teams") or {}).get(str(int(team_id))) or {}) if isinstance(roster_artifacts, dict) else {}
        rosters = (team_block.get("rosters") or {}) if isinstance(team_block, dict) else {}
        active_entries = list(rosters.get("active") or []) if isinstance(rosters, dict) else []
        if not active_entries and not rosters:
            return None

        required_ids: set[int] = set(int(pid) for pid in (_normalize_lineup_ids(lineup_ids) or []) if int(pid) > 0)
        try:
            probable_id = int(probable_pitcher_id or 0)
        except Exception:
            probable_id = 0
        if probable_id > 0:
            required_ids.add(int(probable_id))

        merged: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()

        def _append_entries(entries: Any, *, required_only: bool = False, pitcher_only: bool = False, hitter_only: bool = False, max_add: int = 0) -> int:
            added = 0
            for entry in entries or []:
                if not isinstance(entry, dict):
                    continue
                pid = _roster_entry_player_id(entry)
                if pid <= 0 or pid in seen_ids:
                    continue
                if required_only and pid not in required_ids:
                    continue
                pos_obj = entry.get("position") or {}
                pos = str(entry.get("primary_pos_abbr") or entry.get("pos") or pos_obj.get("abbreviation") or "").strip().upper()
                pos_type = str(pos_obj.get("type") or "").strip().lower() if isinstance(pos_obj, dict) else ""
                is_pitcher = pos == "P" or pos_type == "pitcher"
                if pitcher_only and not is_pitcher:
                    continue
                if hitter_only and is_pitcher:
                    continue
                seen_ids.add(pid)
                merged.append(entry)
                added += 1
                if max_add > 0 and added >= int(max_add):
                    break
            return added

        _append_entries(active_entries)

        supplemental_keys = [key for key in ("40Man", "nonRosterInvitees") if isinstance(rosters.get(key), list)]
        for roster_key in supplemental_keys:
            _append_entries(rosters.get(roster_key) or [], required_only=True)

        pitcher_count = 0
        hitter_count = 0
        for entry in merged:
            pos_obj = entry.get("position") or {}
            pos = str(entry.get("primary_pos_abbr") or entry.get("pos") or pos_obj.get("abbreviation") or "").strip().upper()
            pos_type = str(pos_obj.get("type") or "").strip().lower() if isinstance(pos_obj, dict) else ""
            if pos == "P" or pos_type == "pitcher":
                pitcher_count += 1
            else:
                hitter_count += 1

        for roster_key in supplemental_keys:
            if pitcher_count < 8:
                pitcher_count += _append_entries(rosters.get(roster_key) or [], pitcher_only=True, max_add=(8 - pitcher_count))
            if hitter_count < 11:
                hitter_count += _append_entries(rosters.get(roster_key) or [], hitter_only=True, max_add=(11 - hitter_count))

        return merged or active_entries or None

    def _normalize_player_name(name: Any) -> str:
        text = unicodedata.normalize("NFKD", str(name or ""))
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.replace("’", "'").replace("`", "'")
        text = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"[^a-z0-9]+", " ", text.casefold())
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _roster_entry_player_id(entry: Any) -> int:
        if not isinstance(entry, dict):
            return 0
        try:
            return int(entry.get("player_id") or ((entry.get("person") or {}).get("id") or 0))
        except Exception:
            return 0

    def _roster_entry_full_name(entry: Any) -> str:
        if not isinstance(entry, dict):
            return ""
        return str(entry.get("full_name") or entry.get("name") or ((entry.get("person") or {}).get("fullName") or "")).strip()

    roster_player_ids_by_team: Dict[int, set[int]] = {}
    roster_team_ids_by_player: Dict[int, set[int]] = {}
    for team_key, team_block in ((roster_artifacts.get("teams") or {}).items() if isinstance(roster_artifacts, dict) else []):
        try:
            team_id_i = int(team_key)
        except Exception:
            team_id_i = 0
        if team_id_i <= 0 or not isinstance(team_block, dict):
            continue
        rosters = team_block.get("rosters") or {}
        if not isinstance(rosters, dict):
            continue
        team_player_ids = roster_player_ids_by_team.setdefault(team_id_i, set())
        for roster_key in ("active", "40Man", "nonRosterInvitees"):
            entries = rosters.get(roster_key) or []
            if not isinstance(entries, list):
                continue
            for entry in entries:
                pid = _roster_entry_player_id(entry)
                if pid <= 0:
                    continue
                team_player_ids.add(pid)
                roster_team_ids_by_player.setdefault(pid, set()).add(team_id_i)

    registry_team_ids_by_player: Dict[int, set[int]] = {}
    registry_dir = _TRACKED_DATA_DIR / "roster_registry"
    if registry_dir.exists():
        for registry_path in sorted(registry_dir.glob("team_*.json")):
            try:
                registry_doc = json.loads(registry_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(registry_doc, dict):
                continue
            try:
                registry_team_id = int(registry_doc.get("team_id") or 0)
            except Exception:
                registry_team_id = 0
            if registry_team_id <= 0:
                continue
            snapshots = registry_doc.get("snapshots") or {}
            if not isinstance(snapshots, dict):
                continue
            today_snapshot = snapshots.get(str(args.date)) or {}
            if not isinstance(today_snapshot, dict):
                continue
            for roster_key in ("active", "40Man", "nonRosterInvitees"):
                roster_snapshot = today_snapshot.get(roster_key) or {}
                if not isinstance(roster_snapshot, dict):
                    continue
                players = roster_snapshot.get("players") or []
                if not isinstance(players, list):
                    continue
                for player in players:
                    pid = _roster_entry_player_id(player)
                    if pid <= 0:
                        try:
                            pid = int((player or {}).get("player_id") or 0)
                        except Exception:
                            pid = 0
                    if pid <= 0:
                        continue
                    registry_team_ids_by_player.setdefault(pid, set()).add(registry_team_id)

    def _validate_probable_pitcher_candidate(team_id: Any, player_id: Any, *, opponent_team_id: Any = None) -> Dict[str, Any]:
        try:
            tid = int(team_id or 0)
        except Exception:
            tid = 0
        try:
            pid = int(player_id or 0)
        except Exception:
            pid = 0
        try:
            opp_tid = int(opponent_team_id or 0)
        except Exception:
            opp_tid = 0

        if tid <= 0 or pid <= 0:
            return {
                "status": "none",
                "accepted": False,
                "team_id": (tid if tid > 0 else None),
                "player_id": (pid if pid > 0 else None),
                "opponent_team_id": (opp_tid if opp_tid > 0 else None),
                "roster_team_ids": [],
                "on_expected_team": False,
                "on_opponent_team": False,
            }

        roster_team_ids = sorted(int(x) for x in (roster_team_ids_by_player.get(pid) or set()) if int(x) > 0)
        registry_team_ids = sorted(int(x) for x in (registry_team_ids_by_player.get(pid) or set()) if int(x) > 0)
        on_expected_team = int(tid) in roster_team_ids
        on_opponent_team = int(opp_tid) in roster_team_ids if opp_tid > 0 else False
        on_expected_registry_team = int(tid) in registry_team_ids
        on_opponent_registry_team = int(opp_tid) in registry_team_ids if opp_tid > 0 else False

        if len(registry_team_ids) > 1:
            status = "conflicting_registry_team_membership"
            accepted = False
        elif registry_team_ids and not on_expected_registry_team:
            status = "wrong_team"
            accepted = False
        elif len(roster_team_ids) > 1:
            status = "conflicting_roster_team_membership"
            accepted = False
        elif on_expected_team:
            status = "ok"
            accepted = True
        elif roster_team_ids:
            status = "wrong_team"
            accepted = False
        else:
            status = "unverified"
            accepted = True

        return {
            "status": str(status),
            "accepted": bool(accepted),
            "team_id": int(tid),
            "player_id": int(pid),
            "opponent_team_id": (int(opp_tid) if opp_tid > 0 else None),
            "roster_team_ids": roster_team_ids,
            "registry_team_ids": registry_team_ids,
            "on_expected_team": bool(on_expected_team),
            "on_opponent_team": bool(on_opponent_team),
            "on_expected_registry_team": bool(on_expected_registry_team),
            "on_opponent_registry_team": bool(on_opponent_registry_team),
        }

    def _lookup_probable_pitcher_override(game_pk: Any, team_id: Any) -> Dict[str, Any]:
        try:
            game_pk_i = int(game_pk or 0)
        except Exception:
            game_pk_i = 0
        try:
            team_id_i = int(team_id or 0)
        except Exception:
            team_id_i = 0
        if game_pk_i <= 0 or team_id_i <= 0:
            return {}
        date_block = probable_pitcher_overrides.get(str(args.date)) or {}
        if not isinstance(date_block, dict):
            return {}
        game_block = date_block.get(str(game_pk_i)) or {}
        if not isinstance(game_block, dict):
            return {}
        override = game_block.get(str(team_id_i)) or {}
        return dict(override) if isinstance(override, dict) else {}

    def _suppressed_probable_candidate_ids(probable_override: Any, *candidate_ids: Any) -> List[int]:
        if not isinstance(probable_override, dict) or "player_id" not in probable_override:
            return []
        override_player_id = _safe_int(probable_override.get("player_id"))
        if override_player_id is not None:
            return []
        blocked: set[int] = set()
        for candidate_id in candidate_ids:
            try:
                player_id = int(candidate_id or 0)
            except Exception:
                player_id = 0
            if player_id > 0:
                blocked.add(player_id)
        return sorted(blocked)

    def _select_probable_pitcher_for_team(
        team_id: Any,
        *,
        probable_override: Optional[Dict[str, Any]] = None,
        opponent_team_id: Any,
        feed_id: Any,
        feed_name: Any,
        schedule_id: Any,
        schedule_name: Any,
    ) -> Tuple[Optional[int], str, float, Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        probable_override = dict(probable_override or {})
        try:
            feed_id_i = int(feed_id or 0)
        except Exception:
            feed_id_i = 0
        try:
            schedule_id_i = int(schedule_id or 0)
        except Exception:
            schedule_id_i = 0

        override_has_player_id = isinstance(probable_override, dict) and ("player_id" in probable_override)
        override_player_id = _safe_int((probable_override.get("player_id") if isinstance(probable_override, dict) else None))
        override_name = str((probable_override.get("name") if isinstance(probable_override, dict) else "") or "")
        override_reason = str((probable_override.get("reason") if isinstance(probable_override, dict) else "") or "")

        if override_has_player_id and (override_player_id or 0) <= 0:
            return (
                None,
                "manual_probable_override",
                1.0,
                {
                    "status": "manual_override_suppressed",
                    "selected_id": None,
                    "selected_name": override_name,
                    "selected_source": "manual_probable_override",
                    "override_reason": override_reason,
                    "roster_team_ids": [],
                    "registry_team_ids": [],
                    "on_expected_team": False,
                    "on_opponent_team": False,
                    "on_expected_registry_team": False,
                    "on_opponent_registry_team": False,
                    "rejected_candidates": [],
                },
            )

        if (override_player_id or 0) > 0:
            candidates.append(
                {
                    "id": int(override_player_id or 0),
                    "source": "manual_probable_override",
                    "confidence": 1.0,
                    "name": override_name,
                    "override_reason": override_reason,
                }
            )

        if feed_id_i > 0:
            candidates.append(
                {
                    "id": int(feed_id_i),
                    "source": "feed_gameData_probablePitchers",
                    "confidence": 0.75,
                    "name": str(feed_name or ""),
                }
            )
        if schedule_id_i > 0:
            candidates.append(
                {
                    "id": int(schedule_id_i),
                    "source": "schedule_probablePitcher",
                    "confidence": 0.80,
                    "name": str(schedule_name or ""),
                }
            )

        rejected_candidates: List[Dict[str, Any]] = []
        for candidate in candidates:
            validation = _validate_probable_pitcher_candidate(
                team_id,
                candidate.get("id"),
                opponent_team_id=opponent_team_id,
            )
            if bool(validation.get("accepted")):
                return (
                    int(candidate.get("id") or 0),
                    str(candidate.get("source") or "none"),
                    float(candidate.get("confidence") or 0.0),
                    {
                        "status": str(validation.get("status") or "none"),
                        "selected_id": int(candidate.get("id") or 0),
                        "selected_name": str(candidate.get("name") or ""),
                        "selected_source": str(candidate.get("source") or "none"),
                        "override_reason": str(candidate.get("override_reason") or ""),
                        "roster_team_ids": [int(x) for x in (validation.get("roster_team_ids") or [])],
                        "registry_team_ids": [int(x) for x in (validation.get("registry_team_ids") or [])],
                        "on_expected_team": bool(validation.get("on_expected_team")),
                        "on_opponent_team": bool(validation.get("on_opponent_team")),
                        "on_expected_registry_team": bool(validation.get("on_expected_registry_team")),
                        "on_opponent_registry_team": bool(validation.get("on_opponent_registry_team")),
                        "rejected_candidates": rejected_candidates,
                    },
                )
            rejected_candidates.append(
                {
                    "id": int(candidate.get("id") or 0),
                    "name": str(candidate.get("name") or ""),
                    "source": str(candidate.get("source") or "none"),
                    "status": str(validation.get("status") or "none"),
                    "override_reason": str(candidate.get("override_reason") or ""),
                    "roster_team_ids": [int(x) for x in (validation.get("roster_team_ids") or [])],
                    "registry_team_ids": [int(x) for x in (validation.get("registry_team_ids") or [])],
                    "on_expected_team": bool(validation.get("on_expected_team")),
                    "on_opponent_team": bool(validation.get("on_opponent_team")),
                    "on_expected_registry_team": bool(validation.get("on_expected_registry_team")),
                    "on_opponent_registry_team": bool(validation.get("on_opponent_registry_team")),
                }
            )

        return (
            None,
            "none",
            0.0,
            {
                "status": ("rejected" if rejected_candidates else "none"),
                "selected_id": None,
                "selected_name": "",
                "selected_source": "none",
                "override_reason": "",
                "roster_team_ids": [],
                "registry_team_ids": [],
                "on_expected_team": False,
                "on_opponent_team": False,
                "on_expected_registry_team": False,
                "on_opponent_registry_team": False,
                "rejected_candidates": rejected_candidates,
            },
        )

    def _artifact_lineup_ids(value: Any) -> List[int]:
        out: List[int] = []
        for item in (value or []):
            pid = _safe_int(item)
            if pid is not None and int(pid) > 0:
                out.append(int(pid))
        return out[:9]

    def _roster_artifact_matches_inputs(
        meta: Any,
        *,
        away_probable_pitcher_id: Any,
        home_probable_pitcher_id: Any,
        away_lineup_ids: Any,
        home_lineup_ids: Any,
    ) -> Tuple[bool, str]:
        if not isinstance(meta, dict):
            return False, "missing_meta"

        builder = meta.get("roster_builder") if isinstance(meta.get("roster_builder"), dict) else {}
        artifact_date = str(meta.get("date") or builder.get("as_of_date") or "").strip()
        if artifact_date != str(args.date):
            return False, "date_mismatch"
        if _safe_int(meta.get("stats_season")) != int(args.stats_season):
            return False, "stats_season_mismatch"
        if bool(meta.get("spring_mode")) != bool(spring_mode):
            return False, "spring_mode_mismatch"

        current_away_probable = _safe_int(away_probable_pitcher_id)
        current_home_probable = _safe_int(home_probable_pitcher_id)
        artifact_away_probable = _safe_int(builder.get("away_probable_pitcher_id"))
        artifact_home_probable = _safe_int(builder.get("home_probable_pitcher_id"))
        if artifact_away_probable != current_away_probable:
            return False, "away_probable_mismatch"
        if artifact_home_probable != current_home_probable:
            return False, "home_probable_mismatch"

        artifact_away_lineup = _artifact_lineup_ids(builder.get("away_lineup_ids"))
        artifact_home_lineup = _artifact_lineup_ids(builder.get("home_lineup_ids"))
        current_away_lineup = _artifact_lineup_ids(away_lineup_ids)
        current_home_lineup = _artifact_lineup_ids(home_lineup_ids)
        if artifact_away_lineup != current_away_lineup:
            return False, "away_lineup_mismatch"
        if artifact_home_lineup != current_home_lineup:
            return False, "home_lineup_mismatch"

        return True, "ok"

    projected_name_index_by_team: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}

    def _build_team_hitter_name_index(team_id: int) -> Dict[str, List[Dict[str, Any]]]:
        tid = int(team_id or 0)
        cached = projected_name_index_by_team.get(tid)
        if isinstance(cached, dict):
            return cached

        team_block = ((roster_artifacts.get("teams") or {}).get(str(tid)) or {}) if isinstance(roster_artifacts, dict) else {}
        rosters = (team_block.get("rosters") or {}) if isinstance(team_block, dict) else {}
        blocked_ids = {int(x) for x in (injuries_by_team_id.get(tid, []) or []) if int(x) > 0}
        name_index: Dict[str, List[Dict[str, Any]]] = {}
        seen_ids: set[int] = set()
        for priority, roster_key in enumerate(_projected_lineup_roster_keys(tid)):
            entries = rosters.get(roster_key) or []
            for entry in entries:
                pid = _roster_entry_player_id(entry)
                if pid <= 0 or pid in seen_ids or pid in blocked_ids:
                    continue
                pos_obj = entry.get("position") or {} if isinstance(entry, dict) else {}
                pos = str(entry.get("primary_pos_abbr") or entry.get("pos") or pos_obj.get("abbreviation") or "").strip().upper() if isinstance(entry, dict) else ""
                pos_type = str(pos_obj.get("type") or "").strip().lower() if isinstance(pos_obj, dict) else ""
                if pos == "P" or pos_type == "pitcher":
                    continue
                full_name = _roster_entry_full_name(entry)
                norm_name = _normalize_player_name(full_name)
                if not norm_name:
                    continue
                seen_ids.add(pid)
                name_index.setdefault(norm_name, []).append(
                    {
                        "id": int(pid),
                        "name": full_name,
                        "priority": int(priority),
                        "roster_key": str(roster_key),
                    }
                )

        projected_name_index_by_team[tid] = name_index
        return name_index

    def _match_projected_names_to_lineup(team_id: int, names: Any, source_label: str) -> Dict[str, Any]:
        ordered_names = [str(name or "").strip() for name in (names or []) if str(name or "").strip()]
        if not ordered_names:
            return {
                "status": "none",
                "raw_ids": [],
                "ids": [],
                "missing_ids": [],
                "backfilled_ids": [],
                "pool_source": "none",
                "pool_size": 0,
                "raw_names": [],
                "matched_names": [],
                "missing_name_labels": [],
                "mapping_source": str(source_label),
            }

        name_index = _build_team_hitter_name_index(int(team_id))
        matched_ids: List[int] = []
        matched_names: List[str] = []
        missing_name_labels: List[str] = []
        used_ids: set[int] = set()
        for raw_name in ordered_names[:9]:
            norm_name = _normalize_player_name(raw_name)
            candidates = name_index.get(norm_name) or []
            chosen = next((entry for entry in candidates if int(entry.get("id") or 0) not in used_ids), None)
            if not chosen:
                missing_name_labels.append(str(raw_name))
                continue
            pid = int(chosen.get("id") or 0)
            if pid <= 0:
                missing_name_labels.append(str(raw_name))
                continue
            used_ids.add(pid)
            matched_ids.append(pid)
            matched_names.append(str(chosen.get("name") or raw_name))

        validation = _tighten_projected_lineup(int(team_id), matched_ids)
        validation["raw_names"] = ordered_names[:9]
        validation["matched_names"] = matched_names
        validation["missing_name_labels"] = missing_name_labels
        validation["mapping_source"] = str(source_label)
        return validation

    probable_throw_side_by_pitcher: Dict[int, str] = {}

    def _pitcher_throw_side(person_id: Any) -> str:
        try:
            pid = int(person_id or 0)
        except Exception:
            pid = 0
        if pid <= 0:
            return ""
        cached = probable_throw_side_by_pitcher.get(pid)
        if isinstance(cached, str):
            return cached
        hand = ""
        try:
            person = fetch_person(client, pid)
            raw = str(((person.get("pitchHand") or {}).get("code") or (person.get("throwSide") or {}).get("code") or "")).strip().upper()
            if raw.startswith("L"):
                hand = "L"
            elif raw.startswith("R"):
                hand = "R"
            elif raw.startswith("S"):
                hand = "S"
        except Exception:
            hand = ""
        probable_throw_side_by_pitcher[pid] = hand
        return hand

    rotowire_projected_by_team: Dict[int, Dict[str, Any]] = {}
    try:
        scheduled_team_ids: set[int] = set()
        for game in games or []:
            for side in ("away", "home"):
                team_obj = (((game.get("teams") or {}).get(side) or {}).get("team") or {})
                try:
                    tid = int(team_obj.get("id") or 0)
                except Exception:
                    tid = 0
                if tid <= 0:
                    continue
                scheduled_team_ids.add(tid)
        for tid in sorted(scheduled_team_ids):
            team_info = teams_by_id.get(int(tid)) or {}
            team_abbr = str(team_info.get("abbreviation") or "").strip().upper()
            if not team_abbr:
                continue
            parsed = fetch_rotowire_batting_orders_for_team(client, team_abbr)
            if isinstance(parsed, dict) and parsed:
                rotowire_projected_by_team[int(tid)] = parsed
    except Exception:
        rotowire_projected_by_team = {}

    def _select_rotowire_projected_lineup(team_id: int, opposing_probable_pitcher_id: Any) -> Dict[str, Any]:
        team_page = rotowire_projected_by_team.get(int(team_id)) or {}
        if not isinstance(team_page, dict) or not team_page:
            return {}

        today_lineup = [str(x or "").strip() for x in (team_page.get("today_lineup") or []) if str(x or "").strip()]
        default_vs_rhp = [str(x or "").strip() for x in (team_page.get("default_vs_rhp") or []) if str(x or "").strip()]
        default_vs_lhp = [str(x or "").strip() for x in (team_page.get("default_vs_lhp") or []) if str(x or "").strip()]
        opposing_hand = _pitcher_throw_side(opposing_probable_pitcher_id)

        options: List[Tuple[str, List[str], float]] = []
        if len(today_lineup) >= 9:
            options.append(("projected_rotowire_today_lineup", today_lineup[:9], 0.72))
        if opposing_hand == "L":
            if len(default_vs_lhp) >= 9:
                options.append(("projected_rotowire_default_vs_lhp", default_vs_lhp[:9], 0.66))
            if len(default_vs_rhp) >= 9:
                options.append(("projected_rotowire_default_vs_rhp", default_vs_rhp[:9], 0.56))
        elif opposing_hand == "R":
            if len(default_vs_rhp) >= 9:
                options.append(("projected_rotowire_default_vs_rhp", default_vs_rhp[:9], 0.66))
            if len(default_vs_lhp) >= 9:
                options.append(("projected_rotowire_default_vs_lhp", default_vs_lhp[:9], 0.56))
        else:
            if len(default_vs_rhp) >= 9:
                options.append(("projected_rotowire_default_vs_rhp", default_vs_rhp[:9], 0.58))
            if len(default_vs_lhp) >= 9:
                options.append(("projected_rotowire_default_vs_lhp", default_vs_lhp[:9], 0.58))

        status_rank = {"ok": 2, "adjusted": 1, "partial": 0, "none": -1}
        best_choice: Dict[str, Any] = {}
        best_key: Tuple[float, int, int, float] | None = None
        for source_label, candidate_names, base_confidence in options:
            validation = _match_projected_names_to_lineup(int(team_id), candidate_names, source_label)
            ids = [int(pid) for pid in (validation.get("ids") or []) if int(pid) > 0][:9]
            if not ids:
                continue
            confidence = float(base_confidence)
            missing_count = len(validation.get("missing_name_labels") or [])
            status = str(validation.get("status") or "")
            if status == "adjusted":
                confidence = min(confidence, 0.52)
            elif status == "partial":
                confidence = min(confidence, 0.35)
            if missing_count > 0:
                confidence = max(0.2, confidence - 0.04 * float(missing_count))
            candidate = {
                "source": str(source_label),
                "confidence": float(confidence),
                "ids": ids,
                "validation": validation,
                "opposing_pitcher_hand": str(opposing_hand),
            }
            candidate_key = (
                float(confidence),
                int(status_rank.get(status, -1)),
                -int(missing_count),
                float(base_confidence),
            )
            if best_key is None or candidate_key > best_key:
                best_key = candidate_key
                best_choice = candidate
        return best_choice

    def _is_mlb_team(team_obj: Dict[str, Any]) -> bool:
        try:
            sport = (team_obj or {}).get("sport") or {}
            return int(sport.get("id") or 0) == 1
        except Exception:
            return False

    lineup_games: List[Dict[str, Any]] = []
    probable_games: List[Dict[str, Any]] = []
    official_starting_lineups_by_game: Dict[int, Dict[str, Any]] = {}
    try:
        official_starting_lineups_by_game = fetch_official_starting_lineups_for_date(client, str(args.date))
    except Exception:
        official_starting_lineups_by_game = {}
    for idx, g in enumerate(games):
        game_pk = g.get("gamePk")
        game_type = g.get("gameType")
        double_header = g.get("doubleHeader")
        game_number = g.get("gameNumber")
        series_game_number = g.get("seriesGameNumber")
        status = (g.get("status") or {})
        status_obj = {
            "abstract": status.get("abstractGameState"),
            "detailed": status.get("detailedState"),
        }

        away = (g.get("teams") or {}).get("away") or {}
        home = (g.get("teams") or {}).get("home") or {}
        away_team = away.get("team") or {}
        home_team = home.get("team") or {}

        # Spring training schedules can include college/minor/non-MLB opponents.
        # Skip those games to avoid unsupported roster building.
        if not (_is_mlb_team(away_team) and _is_mlb_team(home_team)):
            away_abbr = _abbr(away_team)
            home_abbr = _abbr(home_team)
            gn = f"_g{int(game_number)}" if isinstance(game_number, (int, float)) else ""
            pk_label = f" gamePk={game_pk}" if game_pk else ""
            print(f"[{idx+1}/{len(games)}] Skipping non-MLB game:{pk_label} {away_abbr} @ {home_abbr}")

            try:
                if game_pk:
                    sim_path = sim_dir / f"sim_{idx}_{away_abbr}_at_{home_abbr}_pk{game_pk}{gn}.json"
                    if sim_path.exists():
                        sim_path.unlink()
                    roster_path = snapshot_dir / f"roster_{idx}_{away_abbr}_at_{home_abbr}_pk{game_pk}{gn}.json"
                    if roster_path.exists():
                        roster_path.unlink()
            except Exception:
                pass

            failures.append(
                {
                    "idx": int(idx),
                    "game_pk": int(game_pk) if game_pk else None,
                    "stage": "skip_non_mlb",
                    "error": "non-MLB opponent in schedule",
                    "away": {"abbr": away_abbr, "team_id": int(away_team.get("id") or 0)},
                    "home": {"abbr": home_abbr, "team_id": int(home_team.get("id") or 0)},
                }
            )
            continue
        try:
            away_id = int(away_team.get("id") or 0)
            home_id = int(home_team.get("id") or 0)
        except Exception:
            failures.append(
                {
                    "idx": int(idx),
                    "game_pk": game_pk,
                    "stage": "schedule",
                    "error": "Missing team id",
                }
            )
            continue

        if away_id <= 0 or home_id <= 0:
            failures.append(
                {
                    "idx": int(idx),
                    "game_pk": game_pk,
                    "stage": "schedule",
                    "error": "Invalid team id",
                    "away": away_team,
                    "home": home_team,
                }
            )
            continue

        away_prob_schedule = (away.get("probablePitcher") or {})
        home_prob_schedule = (home.get("probablePitcher") or {})
        away_prob_schedule_id = away_prob_schedule.get("id")
        home_prob_schedule_id = home_prob_schedule.get("id")
        away_prob_schedule_name = away_prob_schedule.get("fullName") or away_prob_schedule.get("name") or ""
        home_prob_schedule_name = home_prob_schedule.get("fullName") or home_prob_schedule.get("name") or ""

        away_prob_feed_id = None
        home_prob_feed_id = None
        away_prob_feed_name = ""
        home_prob_feed_name = ""

        away_prob_id = None
        home_prob_id = None
        away_prob_source = "none"
        home_prob_source = "none"
        away_prob_confidence = 0.0
        home_prob_confidence = 0.0

        away_lineup_ids: List[int] = []
        home_lineup_ids: List[int] = []
        away_rotowire_projection: Dict[str, Any] = {}
        home_rotowire_projection: Dict[str, Any] = {}
        away_projected_ids: List[int] = []
        home_projected_ids: List[int] = []
        away_projected_validation: Dict[str, Any] = {"status": "none", "raw_ids": [], "ids": []}
        home_projected_validation: Dict[str, Any] = {"status": "none", "raw_ids": [], "ids": []}
        away_lineup_source = "none"
        home_lineup_source = "none"
        away_lineup_confidence = 0.0
        home_lineup_confidence = 0.0
        feed = None
        if game_pk:
            try:
                feed = fetch_game_feed_live(client, int(game_pk))
                away_lineup_ids = _normalize_lineup_ids(parse_confirmed_lineup_ids(feed, "away"))
                home_lineup_ids = _normalize_lineup_ids(parse_confirmed_lineup_ids(feed, "home"))

                prob = (feed.get("gameData") or {}).get("probablePitchers") or {}
                ap = prob.get("away") or {}
                hp = prob.get("home") or {}
                away_prob_feed_id = ap.get("id")
                home_prob_feed_id = hp.get("id")
                away_prob_feed_name = ap.get("fullName") or ap.get("name") or ""
                home_prob_feed_name = hp.get("fullName") or hp.get("name") or ""
            except Exception:
                away_lineup_ids = []
                home_lineup_ids = []

        official_lineup_block = {}
        if game_pk:
            official_lineup_block = dict(official_starting_lineups_by_game.get(int(game_pk)) or {})
        away_official_lineup_ids = _normalize_lineup_ids((official_lineup_block.get("away_ids") or []))
        home_official_lineup_ids = _normalize_lineup_ids((official_lineup_block.get("home_ids") or []))
        if len(away_lineup_ids) < 9 and len(away_official_lineup_ids) >= 9:
            away_lineup_ids = away_official_lineup_ids[:9]
        if len(home_lineup_ids) < 9 and len(home_official_lineup_ids) >= 9:
            home_lineup_ids = home_official_lineup_ids[:9]

        away_prob_override = _lookup_probable_pitcher_override(game_pk, away_id)
        home_prob_override = _lookup_probable_pitcher_override(game_pk, home_id)

        away_prob_id, away_prob_source, away_prob_confidence, away_prob_validation = _select_probable_pitcher_for_team(
            away_id,
            probable_override=away_prob_override,
            opponent_team_id=home_id,
            feed_id=away_prob_feed_id,
            feed_name=away_prob_feed_name,
            schedule_id=away_prob_schedule_id,
            schedule_name=away_prob_schedule_name,
        )
        home_prob_id, home_prob_source, home_prob_confidence, home_prob_validation = _select_probable_pitcher_for_team(
            home_id,
            probable_override=home_prob_override,
            opponent_team_id=away_id,
            feed_id=home_prob_feed_id,
            feed_name=home_prob_feed_name,
            schedule_id=home_prob_schedule_id,
            schedule_name=home_prob_schedule_name,
        )

        # Projected lineups: last-known confirmed lineup for the team.
        try:
            if not away_lineup_ids:
                away_rotowire_projection = _select_rotowire_projected_lineup(int(away_id), home_prob_id)
                if away_rotowire_projection:
                    away_projected_validation = dict(away_rotowire_projection.get("validation") or {})
                    away_projected_ids = [int(pid) for pid in (away_rotowire_projection.get("ids") or [])]
                else:
                    lk = last_known_by_team.get(str(int(away_id))) or {}
                    away_projected_validation = _tighten_projected_lineup(int(away_id), lk.get("ids"))
                    away_projected_ids = [int(pid) for pid in (away_projected_validation.get("ids") or [])]
            if not home_lineup_ids:
                home_rotowire_projection = _select_rotowire_projected_lineup(int(home_id), away_prob_id)
                if home_rotowire_projection:
                    home_projected_validation = dict(home_rotowire_projection.get("validation") or {})
                    home_projected_ids = [int(pid) for pid in (home_rotowire_projection.get("ids") or [])]
                else:
                    lk = last_known_by_team.get(str(int(home_id))) or {}
                    home_projected_validation = _tighten_projected_lineup(int(home_id), lk.get("ids"))
                    home_projected_ids = [int(pid) for pid in (home_projected_validation.get("ids") or [])]
        except Exception:
            away_projected_ids = []
            home_projected_ids = []
            away_rotowire_projection = {}
            home_rotowire_projection = {}
            away_projected_validation = {"status": "none", "raw_ids": [], "ids": []}
            home_projected_validation = {"status": "none", "raw_ids": [], "ids": []}

        if len(away_lineup_ids) >= 9:
            away_lineup_source = (
                "confirmed_feed_live"
                if len(_normalize_lineup_ids(parse_confirmed_lineup_ids(feed, "away") if isinstance(feed, dict) else [])) >= 9
                else "confirmed_official_starting_lineups"
            )
            away_lineup_confidence = 1.0
            last_known_by_team[str(int(away_id))] = {"date": str(args.date), "ids": away_lineup_ids[:9], "source": away_lineup_source}
        elif away_rotowire_projection:
            away_lineup_source = str(away_rotowire_projection.get("source") or "projected_rotowire")
            away_lineup_confidence = float(away_rotowire_projection.get("confidence") or 0.0)
            away_projected_ids = away_projected_ids[:9]
        elif (away_projected_validation.get("raw_ids") or away_projected_ids):
            away_lineup_source = "projected_last_known"
            away_lineup_confidence = 0.6 if str(away_projected_validation.get("status") or "") == "ok" else (0.35 if len(away_projected_ids) >= 9 else 0.15)
            away_projected_ids = away_projected_ids[:9]

        if len(home_lineup_ids) >= 9:
            home_lineup_source = (
                "confirmed_feed_live"
                if len(_normalize_lineup_ids(parse_confirmed_lineup_ids(feed, "home") if isinstance(feed, dict) else [])) >= 9
                else "confirmed_official_starting_lineups"
            )
            home_lineup_confidence = 1.0
            last_known_by_team[str(int(home_id))] = {"date": str(args.date), "ids": home_lineup_ids[:9], "source": home_lineup_source}
        elif home_rotowire_projection:
            home_lineup_source = str(home_rotowire_projection.get("source") or "projected_rotowire")
            home_lineup_confidence = float(home_rotowire_projection.get("confidence") or 0.0)
            home_projected_ids = home_projected_ids[:9]
        elif (home_projected_validation.get("raw_ids") or home_projected_ids):
            home_lineup_source = "projected_last_known"
            home_lineup_confidence = 0.6 if str(home_projected_validation.get("status") or "") == "ok" else (0.35 if len(home_projected_ids) >= 9 else 0.15)
            home_projected_ids = home_projected_ids[:9]

        lineup_games.append(
            {
                "idx": int(idx),
                "game_pk": int(game_pk) if game_pk else None,
                "away": {"team_id": int(away_id), "abbr": _abbr(away_team)},
                "home": {"team_id": int(home_id), "abbr": _abbr(home_team)},
                "away_confirmed_ids": away_lineup_ids[:9] if len(away_lineup_ids) >= 9 else [],
                "home_confirmed_ids": home_lineup_ids[:9] if len(home_lineup_ids) >= 9 else [],
                "away_projected_ids": [int(pid) for pid in away_projected_ids[:9]],
                "home_projected_ids": [int(pid) for pid in home_projected_ids[:9]],
                "away_projected_validation": dict(away_projected_validation or {}),
                "home_projected_validation": dict(home_projected_validation or {}),
                "away_source": away_lineup_source,
                "home_source": home_lineup_source,
                "away_confidence": float(away_lineup_confidence),
                "home_confidence": float(home_lineup_confidence),
            }
        )

        probable_games.append(
            {
                "idx": int(idx),
                "game_pk": int(game_pk) if game_pk else None,
                "away": {"team_id": int(away_id), "abbr": _abbr(away_team)},
                "home": {"team_id": int(home_id), "abbr": _abbr(home_team)},
                "away_probable_id": (int(away_prob_id) if away_prob_id else None),
                "home_probable_id": (int(home_prob_id) if home_prob_id else None),
                "away_source": str(away_prob_source),
                "home_source": str(home_prob_source),
                "away_confidence": float(away_prob_confidence),
                "home_confidence": float(home_prob_confidence),
                "away_validation": dict(away_prob_validation or {}),
                "home_validation": dict(home_prob_validation or {}),
                "raw": {
                    "away_schedule_id": (int(away_prob_schedule_id) if away_prob_schedule_id else None),
                    "home_schedule_id": (int(home_prob_schedule_id) if home_prob_schedule_id else None),
                    "away_schedule_name": str(away_prob_schedule_name or ""),
                    "home_schedule_name": str(home_prob_schedule_name or ""),
                    "away_feed_id": (int(away_prob_feed_id) if away_prob_feed_id else None),
                    "home_feed_id": (int(home_prob_feed_id) if home_prob_feed_id else None),
                    "away_feed_name": str(away_prob_feed_name or ""),
                    "home_feed_name": str(home_prob_feed_name or ""),
                },
            }
        )

        t_away = build_team(away_id, away_team.get("name") or "Away", _abbr(away_team))
        t_home = build_team(home_id, home_team.get("name") or "Home", _abbr(home_team))

        dh_label = ""
        if double_header and str(double_header).strip() not in ("", "N", "n", "0"):
            dh_label = f" DH={double_header} G={game_number if game_number is not None else '-'}"
        pk_label = f" gamePk={game_pk}" if game_pk else ""
        print(f"[{idx+1}/{len(games)}] Preparing rosters:{pk_label}{dh_label} {t_away.abbreviation} @ {t_home.abbreviation}")

        gn = f"_g{int(game_number)}" if isinstance(game_number, (int, float)) else ""
        roster_obj_path = None
        if game_pk:
            roster_obj_path = roster_obj_dir / f"roster_obj_{idx}_{t_away.abbreviation}_at_{t_home.abbreviation}_pk{game_pk}{gn}.json"

        away_excluded_starter_ids = _suppressed_probable_candidate_ids(
            away_prob_override,
            away_prob_feed_id,
            away_prob_schedule_id,
        )
        home_excluded_starter_ids = _suppressed_probable_candidate_ids(
            home_prob_override,
            home_prob_feed_id,
            home_prob_schedule_id,
        )
        has_probable_override = bool(away_prob_override) or bool(home_prob_override)

        used_roster_artifact = False
        if (
            str(getattr(args, "use_roster_artifacts", "off")) == "on"
            and roster_obj_path is not None
            and roster_obj_path.exists()
            and not has_probable_override
        ):
            try:
                rr = read_game_roster_artifact(roster_obj_path)
                expected_away_lineup_ids = away_lineup_ids if away_lineup_ids else away_projected_ids
                expected_home_lineup_ids = home_lineup_ids if home_lineup_ids else home_projected_ids
                artifact_ok, artifact_reason = _roster_artifact_matches_inputs(
                    rr.get("meta"),
                    away_probable_pitcher_id=away_prob_id,
                    home_probable_pitcher_id=home_prob_id,
                    away_lineup_ids=expected_away_lineup_ids,
                    home_lineup_ids=expected_home_lineup_ids,
                )
                if artifact_ok:
                    away_roster = rr["away"]
                    home_roster = rr["home"]
                    used_roster_artifact = True
                    print(f"[{idx+1}/{len(games)}] Loaded roster artifact: {roster_obj_path.name}")
                else:
                    print(f"[{idx+1}/{len(games)}] Ignoring roster artifact ({artifact_reason}): {roster_obj_path.name}")
            except KeyboardInterrupt:
                raise
            except Exception:
                used_roster_artifact = False
        try:
            if not used_roster_artifact:
                # Reuse the already-snapshotted raw team rosters when available.
                away_roster_entries = None
                home_roster_entries = None
                try:
                    away_roster_entries = _preferred_roster_entries_for_builder(
                        int(away_id),
                        probable_pitcher_id=away_prob_id,
                        lineup_ids=(away_lineup_ids or away_projected_ids),
                    )
                    home_roster_entries = _preferred_roster_entries_for_builder(
                        int(home_id),
                        probable_pitcher_id=home_prob_id,
                        lineup_ids=(home_lineup_ids or home_projected_ids),
                    )
                except Exception:
                    away_roster_entries = None
                    home_roster_entries = None

                away_roster = build_team_roster(
                    client,
                    t_away,
                    int(args.stats_season),
                    as_of_date=str(args.date),
                    probable_pitcher_id=int(away_prob_id) if away_prob_id else None,
                    excluded_starter_ids=away_excluded_starter_ids,
                    statcast_cache=statcast_cache,
                    statcast_ttl_seconds=statcast_ttl_seconds,
                    confirmed_lineup_ids=away_lineup_ids,
                    projected_lineup_ids=away_projected_ids,
                    pitcher_availability=pitcher_availability_by_team.get(int(away_id), {}),
                    roster_type="active",
                    fallback_roster_types=(["40Man", "nonRosterInvitees"] if spring_mode else ["40Man"]),
                    injured_player_ids=injuries_by_team_id.get(int(away_id)),
                    roster_entries=away_roster_entries,
                    fast_mode=bool(spring_mode),
                )
                home_roster = build_team_roster(
                    client,
                    t_home,
                    int(args.stats_season),
                    as_of_date=str(args.date),
                    probable_pitcher_id=int(home_prob_id) if home_prob_id else None,
                    excluded_starter_ids=home_excluded_starter_ids,
                    statcast_cache=statcast_cache,
                    statcast_ttl_seconds=statcast_ttl_seconds,
                    confirmed_lineup_ids=home_lineup_ids,
                    projected_lineup_ids=home_projected_ids,
                    pitcher_availability=pitcher_availability_by_team.get(int(home_id), {}),
                    roster_type="active",
                    fallback_roster_types=(["40Man", "nonRosterInvitees"] if spring_mode else ["40Man"]),
                    injured_player_ids=injuries_by_team_id.get(int(home_id)),
                    roster_entries=home_roster_entries,
                    fast_mode=bool(spring_mode),
                )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            failures.append(
                {
                    "idx": int(idx),
                    "game_pk": game_pk,
                    "stage": "build_roster",
                    "error": f"{type(e).__name__}: {e}",
                    "away": {"team_id": int(away_id), "abbr": t_away.abbreviation},
                    "home": {"team_id": int(home_id), "abbr": t_home.abbreviation},
                }
            )
            print(f"[{idx+1}/{len(games)}] ERROR building rosters: {type(e).__name__}")
            continue

        if bvp_hr_on:
            try:
                away_pitcher_id = int(getattr(getattr(getattr(home_roster, "lineup", None), "pitcher", None), "player", None).mlbam_id or 0)
            except Exception:
                away_pitcher_id = 0
            try:
                home_pitcher_id = int(getattr(getattr(getattr(away_roster, "lineup", None), "pitcher", None), "player", None).mlbam_id or 0)
            except Exception:
                home_pitcher_id = 0

            if away_pitcher_id > 0:
                try:
                    apply_starter_bvp_hr_multipliers(
                        batting_roster=away_roster,
                        pitcher_id=away_pitcher_id,
                        season=int(args.season),
                        start_date=bvp_start_date,
                        end_date=daily_date,
                        cache=bvp_cache,
                        min_pa=bvp_min_pa,
                        shrink_pa=bvp_shrink_pa,
                        clamp_lo=bvp_clamp_lo,
                        clamp_hi=bvp_clamp_hi,
                    )
                except Exception:
                    pass

            if home_pitcher_id > 0:
                try:
                    apply_starter_bvp_hr_multipliers(
                        batting_roster=home_roster,
                        pitcher_id=home_pitcher_id,
                        season=int(args.season),
                        start_date=bvp_start_date,
                        end_date=daily_date,
                        cache=bvp_cache,
                        min_pa=bvp_min_pa,
                        shrink_pa=bvp_shrink_pa,
                        clamp_lo=bvp_clamp_lo,
                        clamp_hi=bvp_clamp_hi,
                    )
                except Exception:
                    pass

        if (
            str(getattr(args, "write_roster_artifacts", "off")) == "on"
            and roster_obj_path is not None
            and not used_roster_artifact
        ):
            try:
                write_game_roster_artifact(
                    roster_obj_path,
                    away_roster=away_roster,
                    home_roster=home_roster,
                    meta={
                        "date": str(args.date),
                        "stats_season": int(args.stats_season),
                        "spring_mode": bool(spring_mode),
                        "game_pk": int(game_pk) if game_pk else None,
                        "away_abbr": str(t_away.abbreviation),
                        "home_abbr": str(t_home.abbreviation),
                        "statcast_starter_splits": str(getattr(args, "statcast_starter_splits", "")),
                        "roster_builder": {
                            "as_of_date": str(args.date),
                            "roster_type": "active",
                            "fallback_roster_types": (["40Man", "nonRosterInvitees"] if spring_mode else None),
                            "exclude_injured": True,
                            "enable_batter_vs_pitch_type": True,
                            "enable_batter_platoon": True,
                            "enable_pitcher_platoon": True,
                            "batter_platoon_alpha": 0.55,
                            "pitcher_platoon_alpha": 0.55,
                            "away_probable_pitcher_id": (int(away_prob_id) if away_prob_id else None),
                            "home_probable_pitcher_id": (int(home_prob_id) if home_prob_id else None),
                            "away_lineup_ids": [int(pid) for pid in ((away_lineup_ids or away_projected_ids) or [])[:9]],
                            "home_lineup_ids": [int(pid) for pid in ((home_lineup_ids or home_projected_ids) or [])[:9]],
                            "away_lineup_source": str(away_lineup_source or "none"),
                            "home_lineup_source": str(home_lineup_source or "none"),
                        },
                    },
                )
            except KeyboardInterrupt:
                raise
            except Exception:
                pass

        def _batter_feat(b):
            return {
                "id": b.player.mlbam_id,
                "name": b.player.full_name,
                "pos": b.player.primary_position,
                "bat": b.player.bat_side.value,
                "throw": b.player.throw_side.value,
                "k_rate": b.k_rate,
                "bb_rate": b.bb_rate,
                "hbp_rate": b.hbp_rate,
                "hr_rate": b.hr_rate,
                "inplay_hit_rate": b.inplay_hit_rate,
                "xb_hit_share": b.xb_hit_share,
                "vs_pitch_type": {k.value: float(v) for k, v in (b.vs_pitch_type or {}).items()},
                "platoon_mult_vs_lhp": {str(k): float(v) for k, v in (getattr(b, "platoon_mult_vs_lhp", {}) or {}).items() if isinstance(v, (int, float))},
                "platoon_mult_vs_rhp": {str(k): float(v) for k, v in (getattr(b, "platoon_mult_vs_rhp", {}) or {}).items() if isinstance(v, (int, float))},
                "statcast_quality_mult": {str(k): float(v) for k, v in (getattr(b, "statcast_quality_mult", {}) or {}).items() if isinstance(v, (int, float))},
                "vs_pitcher_history": {
                    str(pid): {
                        str(key): float(value)
                        for key, value in (history or {}).items()
                        if isinstance(value, (int, float))
                    }
                    for pid, history in (getattr(b, "vs_pitcher_history", {}) or {}).items()
                    if isinstance(history, dict)
                },
            }

        def _pitcher_feat(p):
            return {
                "id": p.player.mlbam_id,
                "name": p.player.full_name,
                "throw": p.player.throw_side.value,
                "role": p.role,
                "leverage_skill": p.leverage_skill,
                "stamina_pitches": p.stamina_pitches,
                "availability_mult": float(getattr(p, "availability_mult", 1.0) or 1.0),
                "platoon_mult_vs_lhb": {str(k): float(v) for k, v in (getattr(p, "platoon_mult_vs_lhb", {}) or {}).items() if isinstance(v, (int, float))},
                "platoon_mult_vs_rhb": {str(k): float(v) for k, v in (getattr(p, "platoon_mult_vs_rhb", {}) or {}).items() if isinstance(v, (int, float))},
                "statcast_quality_mult": {str(k): float(v) for k, v in (getattr(p, "statcast_quality_mult", {}) or {}).items() if isinstance(v, (int, float))},
                "arsenal_source": getattr(p, "arsenal_source", "default"),
                "arsenal_sample_size": int(getattr(p, "arsenal_sample_size", 0) or 0),
                "statcast_splits_found": bool(getattr(p, "statcast_splits_n_pitches", 0) or 0),
                "statcast_splits_source": str(getattr(p, "statcast_splits_source", "") or ""),
                "statcast_splits_n_pitches": int(getattr(p, "statcast_splits_n_pitches", 0) or 0),
                "statcast_splits_start_date": str(getattr(p, "statcast_splits_start_date", "") or ""),
                "statcast_splits_end_date": str(getattr(p, "statcast_splits_end_date", "") or ""),
                "k_rate": p.k_rate,
                "bb_rate": p.bb_rate,
                "hbp_rate": p.hbp_rate,
                "hr_rate": p.hr_rate,
                "inplay_hit_rate": p.inplay_hit_rate,
                "arsenal": {k.value: float(v) for k, v in (p.arsenal or {}).items()},
                "pitch_type_whiff_mult": {k.value: float(v) for k, v in (getattr(p, "pitch_type_whiff_mult", {}) or {}).items()},
                "pitch_type_inplay_mult": {k.value: float(v) for k, v in (getattr(p, "pitch_type_inplay_mult", {}) or {}).items()},
            }

        # Persist a feature snapshot (exact sim inputs)
        pitch_model = PitchModelConfig()
        roster_snap = {
            "pitch_model": {
                "name": pitch_model.name,
            },
            "mode": {
                "spring_mode": bool(spring_mode),
                "season": int(args.season),
                "stats_season": int(args.stats_season),
            },
            "statcast": {
                "starter_splits": args.statcast_starter_splits,
                "cache_ttl_hours": int(args.statcast_cache_ttl_hours),
                "enabled": bool(statcast_cache is not None),
                "x64_prefetch": str(args.statcast_x64_prefetch),
            },
            "umpire_factors": {
                "x64_prefetch": str(args.umpire_x64_prefetch),
                "x64_ttl_hours": int(args.umpire_x64_ttl_hours),
                "statcast_days_back": int(args.umpire_statcast_days_back),
                "statcast_min_pitches": int(args.umpire_statcast_min_pitches),
                "shrink": float(args.umpire_shrink),
            },
            "pbp": {
                "mode": str(args.pbp),
                "max_events": int(args.pbp_max_events),
            },
            "away": {
                "team": asdict(away_roster.team),
                "manager": asdict(away_roster.manager),
                "confirmed_lineup_ids": [int(x) for x in (away_lineup_ids or [])],
                "projected_lineup_ids": [int(x) for x in (away_projected_ids or [])],
                "lineup_source": str(away_lineup_source),
                "lineup_confidence": float(away_lineup_confidence),
                "probable_pitcher": {
                    "id": (int(away_prob_id) if away_prob_id else None),
                    "source": str(away_prob_source),
                    "confidence": float(away_prob_confidence),
                    "raw_schedule_id": (int(away_prob_schedule_id) if away_prob_schedule_id else None),
                    "raw_feed_id": (int(away_prob_feed_id) if away_prob_feed_id else None),
                },
                "starter": {
                    "id": away_roster.lineup.pitcher.player.mlbam_id,
                    "name": away_roster.lineup.pitcher.player.full_name,
                    "role": away_roster.lineup.pitcher.role,
                    "selection_source": str(getattr(away_roster.lineup.pitcher, "starter_selection_source", "") or ""),
                    "requested_id": getattr(away_roster.lineup.pitcher, "starter_requested_id", None),
                },
                "bullpen": [
                    {"id": p.player.mlbam_id, "name": p.player.full_name, "role": p.role, "lev": p.leverage_skill}
                    for p in away_roster.lineup.bullpen
                ],
                "lineup": [_batter_feat(b) for b in away_roster.lineup.batters],
                "bench": [_batter_feat(b) for b in (away_roster.lineup.bench or [])],
                "starter_profile": _pitcher_feat(away_roster.lineup.pitcher),
                "bullpen_profiles": [_pitcher_feat(p) for p in (away_roster.lineup.bullpen or [])],
            },
            "home": {
                "team": asdict(home_roster.team),
                "manager": asdict(home_roster.manager),
                "confirmed_lineup_ids": [int(x) for x in (home_lineup_ids or [])],
                "projected_lineup_ids": [int(x) for x in (home_projected_ids or [])],
                "lineup_source": str(home_lineup_source),
                "lineup_confidence": float(home_lineup_confidence),
                "probable_pitcher": {
                    "id": (int(home_prob_id) if home_prob_id else None),
                    "source": str(home_prob_source),
                    "confidence": float(home_prob_confidence),
                    "raw_schedule_id": (int(home_prob_schedule_id) if home_prob_schedule_id else None),
                    "raw_feed_id": (int(home_prob_feed_id) if home_prob_feed_id else None),
                },
                "starter": {
                    "id": home_roster.lineup.pitcher.player.mlbam_id,
                    "name": home_roster.lineup.pitcher.player.full_name,
                    "role": home_roster.lineup.pitcher.role,
                    "selection_source": str(getattr(home_roster.lineup.pitcher, "starter_selection_source", "") or ""),
                    "requested_id": getattr(home_roster.lineup.pitcher, "starter_requested_id", None),
                },
                "bullpen": [
                    {"id": p.player.mlbam_id, "name": p.player.full_name, "role": p.role, "lev": p.leverage_skill}
                    for p in home_roster.lineup.bullpen
                ],
                "lineup": [_batter_feat(b) for b in home_roster.lineup.batters],
                "bench": [_batter_feat(b) for b in (home_roster.lineup.bench or [])],
                "starter_profile": _pitcher_feat(home_roster.lineup.pitcher),
                "bullpen_profiles": [_pitcher_feat(p) for p in (home_roster.lineup.bullpen or [])],
            },
        }
        try:
            weather, park, umpire = fetch_game_context(client, int(game_pk)) if game_pk else (None, None, None)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            failures.append(
                {
                    "idx": int(idx),
                    "game_pk": game_pk,
                    "stage": "context",
                    "error": f"{type(e).__name__}: {e}",
                }
            )
            weather, park, umpire = None, None, None

        # Apply the same umpire shrink used in eval tuning so daily sims stay consistent.
        _apply_umpire_shrink(umpire, float(getattr(args, "umpire_shrink", 1.0)))
        gn = f"_g{int(game_number)}" if isinstance(game_number, (int, float)) else ""
        roster_path = snapshot_dir / f"roster_{idx}_{t_away.abbreviation}_at_{t_home.abbreviation}_pk{game_pk}{gn}.json"

        weather_obj = None
        if weather is not None:
            wm = weather.multipliers()
            weather_obj = {
                "source": weather.source,
                "condition": weather.condition,
                "temperature_f": weather.temperature_f,
                "wind_speed_mph": weather.wind_speed_mph,
                "wind_direction": weather.wind_direction,
                "wind_raw": weather.wind_raw,
                "is_dome": weather.is_dome,
                "multipliers": {
                    "hr_mult": wm.hr_mult,
                    "inplay_hit_mult": wm.inplay_hit_mult,
                    "xb_share_mult": wm.xb_share_mult,
                },
            }

        park_obj = None
        if park is not None:
            pm = park.multipliers()
            park_obj = {
                "source": park.source,
                "venue_id": park.venue_id,
                "venue_name": park.venue_name,
                "roof_type": park.roof_type,
                "roof_status": park.roof_status,
                "left_line": park.left_line,
                "center": park.center,
                "right_line": park.right_line,
                "multipliers": {
                    "hr_mult": pm.hr_mult,
                    "inplay_hit_mult": pm.inplay_hit_mult,
                    "xb_share_mult": pm.xb_share_mult,
                },
            }

        umpire_obj = None
        if umpire is not None:
            um = umpire.multipliers()
            umpire_obj = {
                "source": umpire.source,
                "home_plate_umpire_id": umpire.home_plate_umpire_id,
                "home_plate_umpire_name": umpire.home_plate_umpire_name,
                "called_strike_mult": umpire.called_strike_mult,
                "multipliers": {
                    "called_strike_mult": um.called_strike_mult,
                },
            }

        if weather_obj is not None:
            roster_snap["weather"] = weather_obj
        if park_obj is not None:
            roster_snap["park"] = park_obj
        if umpire_obj is not None:
            roster_snap["umpire"] = umpire_obj
        _write_json(roster_path, roster_snap)

        print(
            f"[{idx+1}/{len(games)}] Simulating ({args.sims}, workers={int(getattr(args, 'workers', 1) or 1)}): {t_away.abbreviation} @ {t_home.abbreviation}"
        )

        try:
            sim_out = _sim_many(
                away_roster,
                home_roster,
                sims=args.sims,
                seed=args.seed + idx * 100000,
                workers=int(getattr(args, "workers", 1) or 1),
                weather=weather,
                park=park,
                umpire=umpire,
                hitter_hr_top_n=int(getattr(args, "hitter_hr_topn", 0) or 0),
                hitter_props_top_n=int(getattr(args, "hitter_props_topn", 0) or 0),
                hitter_hr_prob_calibration=hitter_hr_prob_calibration,
                hitter_props_prob_calibration=hitter_props_prob_calibration,
                pitcher_prop_ids=[
                    int(away_roster.lineup.pitcher.player.mlbam_id),
                    int(home_roster.lineup.pitcher.player.mlbam_id),
                ],
                cfg_kwargs=cfg_kwargs,
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            failures.append(
                {
                    "idx": int(idx),
                    "game_pk": game_pk,
                    "stage": "simulate",
                    "error": f"{type(e).__name__}: {e}",
                    "away": {"abbr": t_away.abbreviation, "team_id": int(t_away.team_id)},
                    "home": {"abbr": t_home.abbreviation, "team_id": int(t_home.team_id)},
                }
            )
            print(f"[{idx+1}/{len(games)}] ERROR simulating: {type(e).__name__}")
            continue

        # Representative single-game boxscore (and optional PBP) separate from distribution sims.
        # Even when --pbp off, we still run 1 representative game so the web UI can render a boxscore.
        pbp_mode = str(args.pbp).lower().strip() if getattr(args, "pbp", None) is not None else "off"
        want_pbp = pbp_mode != "off"
        pbp_cfg = GameConfig(
            rng_seed=(args.seed + idx * 100000 + 999),
            weather=weather,
            park=park,
            umpire=umpire,
            **cfg_kwargs,
            pbp=(pbp_mode if want_pbp else "off"),
            pbp_max_events=(int(args.pbp_max_events) if want_pbp else 0),
        )
        r1 = simulate_game(away_roster, home_roster, pbp_cfg)
        pbp_obj = {
            "pbp_mode": str(getattr(r1, "pbp_mode", "off")),
            "pbp_truncated": bool(getattr(r1, "pbp_truncated", False)),
            "pbp": (getattr(r1, "pbp", []) or []) if want_pbp else [],
            "boxscore": _as_boxscore(r1),
        }

        record = {
            "date": args.date,
            "season": args.season,
            "stats_season": int(args.stats_season),
            "spring_mode": bool(spring_mode),
            "game_pk": game_pk,
            "schedule": {
                "game_type": game_type,
                "double_header": double_header,
                "game_number": game_number,
                "series_game_number": series_game_number,
                "status": status_obj,
            },
            "away": asdict(t_away),
            "home": asdict(t_home),
            "probable": {
                "away_id": (int(away_prob_id) if away_prob_id else None),
                "home_id": (int(home_prob_id) if home_prob_id else None),
                "away_source": str(away_prob_source),
                "home_source": str(home_prob_source),
                "away_confidence": float(away_prob_confidence),
                "home_confidence": float(home_prob_confidence),
                "raw": {
                    "away_schedule_id": (int(away_prob_schedule_id) if away_prob_schedule_id else None),
                    "home_schedule_id": (int(home_prob_schedule_id) if home_prob_schedule_id else None),
                    "away_feed_id": (int(away_prob_feed_id) if away_prob_feed_id else None),
                    "home_feed_id": (int(home_prob_feed_id) if home_prob_feed_id else None),
                },
            },
            "starters": {
                "away": int(away_roster.lineup.pitcher.player.mlbam_id),
                "home": int(home_roster.lineup.pitcher.player.mlbam_id),
            },
            "starter_names": {
                "away": str(away_roster.lineup.pitcher.player.full_name),
                "home": str(home_roster.lineup.pitcher.player.full_name),
            },
            "weather": weather_obj,
            "park": park_obj,
            "umpire": umpire_obj,
            "sim": sim_out,
            "pbp": pbp_obj,
            "meta": {
                "spring_mode": bool(spring_mode),
                "stats_season": int(args.stats_season),
                "away_lineup_source": str(away_lineup_source),
                "home_lineup_source": str(home_lineup_source),
                "hitter_hr_topn": int(getattr(args, "hitter_hr_topn", 0) or 0),
                "hitter_props_topn": int(getattr(args, "hitter_props_topn", 0) or 0),
                "hitter_props_topn_effective": (
                    int(getattr(args, "hitter_hr_topn", 0) or 0)
                    if int(getattr(args, "hitter_props_topn", 0) or 0) < 0
                    else int(getattr(args, "hitter_props_topn", 0) or 0)
                ),
                "hitter_hr_prob_calibration": str(getattr(args, "hitter_hr_prob_calibration", "") or ""),
                "hitter_props_prob_calibration": str(getattr(args, "hitter_props_prob_calibration", "") or ""),
            },
        }
        outputs.append(record)

        sim_path = sim_dir / f"sim_{idx}_{t_away.abbreviation}_at_{t_home.abbreviation}_pk{game_pk}{gn}.json"
        _write_json(sim_path, record)

    # Summary index
    def _summary_segment(segment: Any) -> Dict[str, Any]:
        seg = segment if isinstance(segment, dict) else {}
        return {
            "home_win_prob": seg.get("home_win_prob"),
            "away_win_prob": seg.get("away_win_prob"),
            "tie_prob": seg.get("tie_prob"),
            "away_runs_mean": seg.get("away_runs_mean"),
            "home_runs_mean": seg.get("home_runs_mean"),
            "total_runs_dist": dict(seg.get("total_runs_dist") or {}),
            "run_margin_dist": dict(seg.get("run_margin_dist") or {}),
        }

    summary = {
        "date": args.date,
        "season": args.season,
        "games": len(outputs),
        "failures": failures,
        "failures_n": int(len(failures)),
        "generated_at": datetime.now().isoformat(),
        "outputs": [
            {
                "game_pk": o.get("game_pk"),
                "double_header": (o.get("schedule") or {}).get("double_header"),
                "game_number": (o.get("schedule") or {}).get("game_number"),
                "away": o["away"]["abbreviation"],
                "home": o["home"]["abbreviation"],
                "starter_names": o.get("starter_names"),
                "full": _summary_segment((o.get("sim") or {}).get("segments", {}).get("full")),
                "first1": _summary_segment((o.get("sim") or {}).get("segments", {}).get("first1")),
                "first5": _summary_segment((o.get("sim") or {}).get("segments", {}).get("first5")),
                "first3": _summary_segment((o.get("sim") or {}).get("segments", {}).get("first3")),
                "hitter_hr_likelihood_topn": (o.get("sim") or {}).get("hitter_hr_likelihood_topn"),
                "hitter_props_likelihood_topn": (o.get("sim") or {}).get("hitter_props_likelihood_topn"),
                "pitcher_props": (o.get("sim") or {}).get("pitcher_props"),
            }
            for o in outputs
        ],
    }
    summary_path = out_root / f"daily_summary_{args.date.replace('-', '_')}.json"
    _write_json(summary_path, summary)
    try:
        top_props_result = write_daily_top_props_artifact(
            str(args.date),
            out_path=(out_root / "top_props" / f"daily_top_props_{args.date.replace('-', '_')}.json"),
        )
        print(f"Wrote top-props artifact: {top_props_result.get('path')}")
    except Exception as exc:
        print(f"Warning: failed to write top-props artifact for {args.date}: {type(exc).__name__}: {exc}")
    try:
        ladders_result = write_daily_ladders_artifact(
            str(args.date),
            out_path=(out_root / "ladders" / f"daily_ladders_{args.date.replace('-', '_')}.json"),
        )
        print(f"Wrote ladders artifact: {ladders_result.get('path')}")
    except Exception as exc:
        print(f"Warning: failed to write ladders artifact for {args.date}: {type(exc).__name__}: {exc}")
    try:
        season_frontend_result = write_current_day_season_frontend_artifacts(
            int(args.season),
            str(args.date),
            betting_profile="retuned",
            out_dir=(out_root / "season_frontend"),
        )
        print(f"Wrote season frontend artifacts: {season_frontend_result.get('dir')}")
    except Exception as exc:
        print(f"Warning: failed to write season frontend artifacts for {args.date}: {type(exc).__name__}: {exc}")

    # Persist lineup artifacts (best-effort).
    try:
        lineup_summary = {
            "games": int(len(lineup_games)),
            "projected_teams": 0,
            "adjusted_teams": 0,
            "partial_teams": 0,
            "fallback_pool_teams": 0,
        }
        for row in lineup_games:
            for side in ("away", "home"):
                validation = dict((row.get(f"{side}_projected_validation") or {}))
                raw_ids = [int(pid) for pid in (validation.get("raw_ids") or []) if int(pid) > 0]
                if raw_ids:
                    lineup_summary["projected_teams"] = int(lineup_summary.get("projected_teams") or 0) + 1
                status = str(validation.get("status") or "")
                if status == "adjusted":
                    lineup_summary["adjusted_teams"] = int(lineup_summary.get("adjusted_teams") or 0) + 1
                elif status == "partial":
                    lineup_summary["partial_teams"] = int(lineup_summary.get("partial_teams") or 0) + 1
                if str(validation.get("pool_source") or "") == "fallback":
                    lineup_summary["fallback_pool_teams"] = int(lineup_summary.get("fallback_pool_teams") or 0) + 1
        _write_json(
            snapshot_dir / "lineups.json",
            {
                "date": str(args.date),
                "season": int(args.season),
                "spring_mode": bool(spring_mode),
                "generated_at": datetime.now().isoformat(),
                "summary": lineup_summary,
                "games": lineup_games,
            },
        )
    except Exception:
        pass

    # Persist probable starters artifact (best-effort).
    try:
        _write_json(
            snapshot_dir / "probables.json",
            {
                "date": str(args.date),
                "season": int(args.season),
                "spring_mode": bool(spring_mode),
                "generated_at": datetime.now().isoformat(),
                "games": probable_games,
            },
        )
    except Exception:
        pass
    try:
        last_known_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = last_known_path.with_suffix(last_known_path.suffix + ".tmp")
        tmp.write_text(json.dumps(last_known_by_team, indent=2), encoding="utf-8")
        tmp.replace(last_known_path)
    except Exception:
        pass
    print(f"Wrote: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
