from __future__ import annotations

import argparse
import copy
import json
import math
import multiprocessing
import multiprocessing.spawn
import random
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# On Windows, ensure ProcessPoolExecutor workers use this interpreter (venv).
if sys.platform.startswith("win"):
    try:
        multiprocessing.spawn.set_executable(sys.executable)
    except Exception:
        pass


# Ensure the project root (MLB-BettingV2/) is importable.
_ROOT = Path(__file__).resolve().parents[2]
_TRACKED_DATA_DIR = (_ROOT / "data").resolve()
_DATA_ROOT_ENV = str(__import__("os").environ.get("MLB_BETTING_DATA_ROOT") or "").strip()
_DATA_DIR = (Path(_DATA_ROOT_ENV).resolve() if _DATA_ROOT_ENV else _TRACKED_DATA_DIR)
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import (
    StatsApiClient,
    fetch_game_context,
    fetch_schedule_for_date,
    load_feed_live_from_raw,
    fetch_game_feed_live,
    parse_confirmed_lineup_ids,
    extract_team_pitcher_pitches_thrown,
)
from sim_engine.data.statcast_bvp import apply_starter_bvp_hr_multipliers, default_bvp_cache
from sim_engine.data.build_roster import build_team, build_team_roster
from sim_engine.data.roster_artifact import read_game_roster_artifact, write_game_roster_artifact
from sim_engine.forward_tuning import (
    FORWARD_BVP_MATCHUP_MODE,
    FORWARD_BVP_MIN_PA,
    FORWARD_MANAGER_PITCHING_OVERRIDES_PATH,
    FORWARD_PITCH_MODEL_OVERRIDES_PATH,
    should_use_forward_tuning,
)
from sim_engine.models import GameConfig
from sim_engine.market_pitcher_props import (
    load_pitcher_prop_lines,
    normalize_pitcher_name,
    no_vig_over_prob,
)
from sim_engine.pitch_model import PitchModelConfig
from sim_engine.simulate import simulate_game
from sim_engine.prob_calibration import apply_prob_calibration, apply_prop_prob_calibration


# prop_key, probability field, actual stat key, mean field, threshold
_HITTER_PROP_SPECS: List[Tuple[str, str, str, str, int]] = [
    ("hits_1plus", "p_h_1plus", "H", "h_mean", 1),
    ("hits_2plus", "p_h_2plus", "H", "h_mean", 2),
    ("hits_3plus", "p_h_3plus", "H", "h_mean", 3),
    ("doubles_1plus", "p_2b_1plus", "2B", "2b_mean", 1),
    ("triples_1plus", "p_3b_1plus", "3B", "3b_mean", 1),
    ("runs_1plus", "p_r_1plus", "R", "r_mean", 1),
    ("runs_2plus", "p_r_2plus", "R", "r_mean", 2),
    ("runs_3plus", "p_r_3plus", "R", "r_mean", 3),
    ("rbi_1plus", "p_rbi_1plus", "RBI", "rbi_mean", 1),
    ("rbi_2plus", "p_rbi_2plus", "RBI", "rbi_mean", 2),
    ("rbi_3plus", "p_rbi_3plus", "RBI", "rbi_mean", 3),
    ("rbi_4plus", "p_rbi_4plus", "RBI", "rbi_mean", 4),
    ("total_bases_1plus", "p_tb_1plus", "TB", "tb_mean", 1),
    ("total_bases_2plus", "p_tb_2plus", "TB", "tb_mean", 2),
    ("total_bases_3plus", "p_tb_3plus", "TB", "tb_mean", 3),
    ("total_bases_4plus", "p_tb_4plus", "TB", "tb_mean", 4),
    ("total_bases_5plus", "p_tb_5plus", "TB", "tb_mean", 5),
    ("sb_1plus", "p_sb_1plus", "SB", "sb_mean", 1),
]


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _argv_has_flag(argv: List[str], flag: str) -> bool:
    needle = str(flag or "").strip()
    if not needle:
        return False
    for raw in argv:
        arg = str(raw or "")
        if arg == needle or arg.startswith(f"{needle}="):
            return True
    return False


def _neutralize_pitch_type_hr(roster: Any) -> None:
    try:
        lineup = getattr(roster, "lineup", None)
        batters = getattr(lineup, "batters", None) or []
        for batter in batters:
            try:
                batter.vs_pitch_type_hr = {}
            except Exception:
                pass
        pitcher = getattr(lineup, "pitcher", None)
        if pitcher is not None:
            try:
                pitcher.pitch_type_hr_mult = {}
            except Exception:
                pass
    except Exception:
        pass


def _apply_forward_tuning_defaults(args: argparse.Namespace, raw_argv: List[str]) -> None:
    if not should_use_forward_tuning(str(getattr(args, "date", "") or "")):
        return
    if not _argv_has_flag(list(raw_argv), "--pitch-model-overrides"):
        args.pitch_model_overrides = str(FORWARD_PITCH_MODEL_OVERRIDES_PATH)
    if not _argv_has_flag(list(raw_argv), "--manager-pitching-overrides"):
        args.manager_pitching_overrides = str(FORWARD_MANAGER_PITCHING_OVERRIDES_PATH)
    if not _argv_has_flag(list(raw_argv), "--bvp-hr"):
        args.bvp_hr = str(FORWARD_BVP_MATCHUP_MODE)
    if not _argv_has_flag(list(raw_argv), "--bvp-min-pa"):
        args.bvp_min_pa = int(FORWARD_BVP_MIN_PA)


def _abbr(team_obj: dict) -> str:
    return (team_obj.get("abbreviation") or team_obj.get("teamName") or team_obj.get("name") or "UNK")


def _get_box_pitchers(feed: Dict[str, Any], side: str) -> List[int]:
    live = (feed.get("liveData") or {})
    box = (live.get("boxscore") or {})
    teams = (box.get("teams") or {})
    t = (teams.get(side) or {})
    pitchers = t.get("pitchers") or []
    out: List[int] = []
    if isinstance(pitchers, list):
        for x in pitchers:
            try:
                out.append(int(x))
            except Exception:
                continue
    return out


def _get_box_starting_pitcher_id(feed: Dict[str, Any], side: str) -> Optional[int]:
    """Best-effort starter identification from boxscore.

    Rationale:
    - boxscore.teams[side].pitchers ordering is not guaranteed to put the starter first.
    - players[*].stats.pitching.gamesStarted is a more reliable indicator.
    """
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

    # Fallback: first listed pitcher id (may be wrong)
    ps = _get_box_pitchers(feed, side)
    return int(ps[0]) if ps else None


def _starter_name_from_feed(feed: Dict[str, Any], side: str, starter_id: Optional[int]) -> str:
    try:
        pid = int(starter_id or 0)
    except Exception:
        pid = 0
    if pid <= 0:
        return ""
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        players = t.get("players") or {}
        pobj = players.get(f"ID{pid}") or {}
        person = pobj.get("person") or {}
        return str(person.get("fullName") or "")
    except Exception:
        return ""


def _actual_linescore(feed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    live = (feed.get("liveData") or {})
    ls = (live.get("linescore") or {})
    teams = (ls.get("teams") or {})
    away = (teams.get("away") or {})
    home = (teams.get("home") or {})

    def _safe_int(x) -> Optional[int]:
        try:
            if x is None:
                return None
            return int(x)
        except Exception:
            try:
                return int(float(str(x)))
            except Exception:
                return None

    away_r = _safe_int(away.get("runs"))
    home_r = _safe_int(home.get("runs"))
    if away_r is None or home_r is None:
        return None

    innings = ls.get("innings") or []
    inn_runs: List[Tuple[int, int]] = []
    if isinstance(innings, list):
        for inn in innings:
            if not isinstance(inn, dict):
                continue
            a = _safe_int((inn.get("away") or {}).get("runs"))
            h = _safe_int((inn.get("home") or {}).get("runs"))
            if a is None or h is None:
                continue
            inn_runs.append((int(a), int(h)))

    def seg(n: int) -> Dict[str, int]:
        a = sum(x[0] for x in inn_runs[:n]) if inn_runs else 0
        h = sum(x[1] for x in inn_runs[:n]) if inn_runs else 0
        return {"away": int(a), "home": int(h)}

    return {
        "full": {"away": int(away_r), "home": int(home_r)},
        "first5": seg(5),
        "first3": seg(3),
        "innings": [{"away": a, "home": h} for a, h in inn_runs],
    }


def _actual_team_batting(feed: Dict[str, Any], side: str) -> Optional[Dict[str, Any]]:
    """Extract team-level batting totals from StatsAPI feed/live boxscore."""
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        ts = t.get("teamStats") or {}
        batting = ts.get("batting") or {}

        def _i(k: str) -> Optional[int]:
            try:
                v = batting.get(k)
                if v is None:
                    return None
                return int(float(v))
            except Exception:
                return None

        hits = _i("hits")
        hr = _i("homeRuns")
        so = _i("strikeOuts")
        bb = _i("baseOnBalls")
        if hits is None and hr is None and so is None and bb is None:
            return None
        return {"H": hits, "HR": hr, "SO": so, "BB": bb}
    except Exception:
        return None


def _actual_batter_box_batting(feed: Dict[str, Any], side: str) -> Dict[int, Dict[str, int]]:
    """Extract per-batter batting totals from StatsAPI feed/live boxscore.

    Returns a map batter_id -> batting stat dict.
    Keys are a best-effort subset: R, H, 2B, 3B, HR, AB, RBI, SB.
    """

    def _i(x) -> Optional[int]:
        try:
            if x is None:
                return None
            return int(x)
        except Exception:
            try:
                return int(float(str(x)))
            except Exception:
                return None

    out: Dict[int, Dict[str, int]] = {}
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        players = t.get("players") or {}
        batters = t.get("batters") or []
        if not isinstance(batters, list) or not isinstance(players, dict):
            return {}
        for pid in batters:
            try:
                pid_i = int(pid)
            except Exception:
                continue
            pobj = players.get(f"ID{pid_i}") or {}
            batting = ((pobj.get("stats") or {}).get("batting") or {})

            r = _i(batting.get("runs"))
            h = _i(batting.get("hits"))
            d2 = _i(batting.get("doubles"))
            d3 = _i(batting.get("triples"))
            hr = _i(batting.get("homeRuns"))
            ab = _i(batting.get("atBats"))
            rbi = _i(batting.get("rbi"))
            sb = _i(batting.get("stolenBases"))

            if r is None and h is None and d2 is None and d3 is None and hr is None and ab is None and rbi is None and sb is None:
                continue
            out[int(pid_i)] = {
                "R": int(r or 0),
                "H": int(h or 0),
                "2B": int(d2 or 0),
                "3B": int(d3 or 0),
                "HR": int(hr or 0),
                "AB": int(ab or 0),
                "RBI": int(rbi or 0),
                "SB": int(sb or 0),
            }
        return out
    except Exception:
        return {}


def _pitcher_availability_for_date(client: StatsApiClient, season: int, date_str: str) -> Dict[int, Dict[int, float]]:
    """Compute bullpen availability multipliers from last 3 days workload.

    Mirrors the logic in tools/daily_update.py:
    - fetch each prior day's schedule (cached)
    - load raw feed/live from disk for those games
    - use per-pitcher pitchesThrown to derive availability
    """
    try:
        today = datetime.strptime(str(date_str), "%Y-%m-%d").date()
    except Exception:
        return {}

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

            feed = load_feed_live_from_raw(int(season), d, game_pk)
            if not isinstance(feed, dict) or not feed:
                continue

            for tid in (away_id, home_id):
                pitches = extract_team_pitcher_pitches_thrown(feed, tid)
                if not pitches:
                    continue
                td = pitches_by_team_day.setdefault(tid, {})
                day_map = td.setdefault(days_ago, {})
                pitched_days = pitched_days_by_team_pitcher.setdefault(tid, {})
                for pid, pth in (pitches or {}).items():
                    day_map[pid] = int(day_map.get(pid, 0) + int(pth or 0))
                    if int(pth or 0) > 0:
                        pitched_days.setdefault(pid, set()).add(int(days_ago))

    pitcher_availability_by_team: Dict[int, Dict[int, float]] = {}
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
            avail = max(0.35, 1.0 - (float(wp) / 120.0))
            days = pitched_days.get(int(pid), set())
            if 1 in days and 2 in days and 3 in days:
                avail *= 0.75
            elif 1 in days and 2 in days:
                avail *= 0.85
            avail_map[int(pid)] = float(max(0.25, min(1.0, avail)))

        pitcher_availability_by_team[int(tid)] = avail_map

    return pitcher_availability_by_team


def _sim_many(
    away_roster,
    home_roster,
    sims: int,
    seed: int,
    weather=None,
    park=None,
    umpire=None,
    weather_hr_weight: float = 1.0,
    weather_inplay_hit_weight: float = 1.0,
    weather_xb_share_weight: float = 1.0,
    park_hr_weight: float = 1.0,
    park_inplay_hit_weight: float = 1.0,
    park_xb_share_weight: float = 1.0,
    pitch_model_overrides: Optional[Dict[str, Any]] = None,
    bip_baserunning: bool = True,
    bip_dp_rate: float = 0.06,
    bip_sf_rate_flypop: float = 0.48,
    bip_sf_rate_line: float = 0.36,
    bip_1b_p2_scores_mult: float = 1.15,
    bip_2b_p1_scores_mult: float = 1.15,
    bip_1b_p1_to_3b_rate: float = 0.24,
    bip_ground_rbi_out_rate: float = 0.18,
    bip_out_2b_to_3b_rate: float = 0.24,
    bip_out_1b_to_2b_rate: float = 0.14,
    bip_misc_advance_pitch_rate: float = 0.004,
    bip_roe_rate: float = 0.012,
    bip_fc_rate: float = 0.04,
    bip_fc_runner_on_3b_score_rate: float = 0.0,
    pitcher_rate_sampling: bool = True,
    pitcher_distribution_overrides: Optional[Dict[str, Any]] = None,
    manager_pitching: str = "legacy",
    manager_pitching_overrides: Optional[Dict[str, Any]] = None,
    pitcher_prop_ids: Optional[List[int]] = None,
    hitter_hr_top_n: int = 0,
    hitter_props_top_n: int = -1,
) -> Dict[str, Any]:
    def init_seg():
        return {"home_wins": 0, "away_wins": 0, "ties": 0, "totals": {}, "margins": {}}

    seg_full = init_seg()
    seg_f5 = init_seg()
    seg_f3 = init_seg()

    prop_ids = [int(x) for x in (pitcher_prop_ids or []) if int(x) > 0]
    prop_acc: Dict[int, Dict[str, Any]] = {}
    for pid in prop_ids:
        prop_acc[int(pid)] = {
            "so": {},
            "outs": {},
            "pitches": {},
            "so_sum": 0.0,
            "outs_sum": 0.0,
            "pitches_sum": 0.0,
        }

    # Hitter/team stats (team-level aggregates; avoids per-player explosion)
    def _team_batter_ids(roster) -> set[int]:
        ids: set[int] = set()
        try:
            for b in (roster.lineup.batters or []):
                ids.add(int(b.player.mlbam_id))
            for b in (roster.lineup.bench or []):
                ids.add(int(b.player.mlbam_id))
        except Exception:
            pass
        return ids

    away_batter_ids = _team_batter_ids(away_roster)
    home_batter_ids = _team_batter_ids(home_roster)

    team_bat = {
        "away": {"H": {}, "HR": {}, "SO": {}, "BB": {}, "H_sum": 0.0, "HR_sum": 0.0, "SO_sum": 0.0, "BB_sum": 0.0},
        "home": {"H": {}, "HR": {}, "SO": {}, "BB": {}, "H_sum": 0.0, "HR_sum": 0.0, "SO_sum": 0.0, "BB_sum": 0.0},
    }

    # Optional: per-batter likelihoods (kept small by outputting top-N only).
    hr_top_n = max(0, int(hitter_hr_top_n or 0))
    props_top_n_raw = int(hitter_props_top_n)
    props_top_n = hr_top_n if props_top_n_raw < 0 else max(0, props_top_n_raw)
    max_top_n = max(hr_top_n, props_top_n)

    def _lineup_batters(roster) -> List[Any]:
        try:
            return list(roster.lineup.batters or [])
        except Exception:
            return []

    def _batter_name_map(roster) -> Dict[int, str]:
        out: Dict[int, str] = {}
        for b in _lineup_batters(roster):
            try:
                out[int(b.player.mlbam_id)] = str(b.player.full_name)
            except Exception:
                continue
        return out

    away_lineup_ids = [int(b.player.mlbam_id) for b in _lineup_batters(away_roster) if int(getattr(b.player, "mlbam_id", 0) or 0) > 0]
    home_lineup_ids = [int(b.player.mlbam_id) for b in _lineup_batters(home_roster) if int(getattr(b.player, "mlbam_id", 0) or 0) > 0]
    away_name = _batter_name_map(away_roster)
    home_name = _batter_name_map(home_roster)


    hr_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    hr_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    ab_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}

    # Broader hitter props.
    h_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    h_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    h_ge2: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    h_ge3: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    d2_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    d2_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    d3_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    d3_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    r_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    r_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    r_ge2: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    r_ge3: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    rbi_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    rbi_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    rbi_ge2: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    rbi_ge3: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    rbi_ge4: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    tb_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    tb_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    tb_ge2: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    tb_ge3: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    tb_ge4: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    tb_ge5: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    sb_sum: Dict[str, Dict[int, float]] = {"away": {}, "home": {}}
    sb_ge1: Dict[str, Dict[int, int]] = {"away": {}, "home": {}}
    for pid in away_lineup_ids:
        hr_sum["away"][pid] = 0.0
        hr_ge1["away"][pid] = 0
        ab_sum["away"][pid] = 0.0
        h_sum["away"][pid] = 0.0
        h_ge1["away"][pid] = 0
        h_ge2["away"][pid] = 0
        h_ge3["away"][pid] = 0
        d2_sum["away"][pid] = 0.0
        d2_ge1["away"][pid] = 0
        d3_sum["away"][pid] = 0.0
        d3_ge1["away"][pid] = 0
        r_sum["away"][pid] = 0.0
        r_ge1["away"][pid] = 0
        r_ge2["away"][pid] = 0
        r_ge3["away"][pid] = 0
        rbi_sum["away"][pid] = 0.0
        rbi_ge1["away"][pid] = 0
        rbi_ge2["away"][pid] = 0
        rbi_ge3["away"][pid] = 0
        rbi_ge4["away"][pid] = 0
        tb_sum["away"][pid] = 0.0
        tb_ge1["away"][pid] = 0
        tb_ge2["away"][pid] = 0
        tb_ge3["away"][pid] = 0
        tb_ge4["away"][pid] = 0
        tb_ge5["away"][pid] = 0
        sb_sum["away"][pid] = 0.0
        sb_ge1["away"][pid] = 0
    for pid in home_lineup_ids:
        hr_sum["home"][pid] = 0.0
        hr_ge1["home"][pid] = 0
        ab_sum["home"][pid] = 0.0
        h_sum["home"][pid] = 0.0
        h_ge1["home"][pid] = 0
        h_ge2["home"][pid] = 0
        h_ge3["home"][pid] = 0
        d2_sum["home"][pid] = 0.0
        d2_ge1["home"][pid] = 0
        d3_sum["home"][pid] = 0.0
        d3_ge1["home"][pid] = 0
        r_sum["home"][pid] = 0.0
        r_ge1["home"][pid] = 0
        r_ge2["home"][pid] = 0
        r_ge3["home"][pid] = 0
        rbi_sum["home"][pid] = 0.0
        rbi_ge1["home"][pid] = 0
        rbi_ge2["home"][pid] = 0
        rbi_ge3["home"][pid] = 0
        rbi_ge4["home"][pid] = 0
        tb_sum["home"][pid] = 0.0
        tb_ge1["home"][pid] = 0
        tb_ge2["home"][pid] = 0
        tb_ge3["home"][pid] = 0
        tb_ge4["home"][pid] = 0
        tb_ge5["home"][pid] = 0
        sb_sum["home"][pid] = 0.0
        sb_ge1["home"][pid] = 0

    def _sum_team_stat(batter_stats: Dict[int, Dict[str, int]], ids: set[int], key: str) -> int:
        tot = 0
        for pid in ids:
            row = batter_stats.get(int(pid)) or {}
            try:
                tot += int(row.get(key) or 0)
            except Exception:
                continue
        return int(tot)

    def seg_score(r, innings: int) -> Dict[str, int]:
        a = sum((r.away_inning_runs or [])[:innings])
        h = sum((r.home_inning_runs or [])[:innings])
        return {"away": int(a), "home": int(h)}

    for i in range(max(1, sims)):
        cfg = GameConfig(
            rng_seed=int(seed) + i,
            weather=weather,
            park=park,
            weather_hr_weight=float(weather_hr_weight),
            weather_inplay_hit_weight=float(weather_inplay_hit_weight),
            weather_xb_share_weight=float(weather_xb_share_weight),
            park_hr_weight=float(park_hr_weight),
            park_inplay_hit_weight=float(park_inplay_hit_weight),
            park_xb_share_weight=float(park_xb_share_weight),
            umpire=umpire,
            pitch_model_overrides=(pitch_model_overrides or {}),
            bip_baserunning=bool(bip_baserunning),
            bip_dp_rate=float(bip_dp_rate),
            bip_sf_rate_flypop=float(bip_sf_rate_flypop),
            bip_sf_rate_line=float(bip_sf_rate_line),
            bip_1b_p2_scores_mult=float(bip_1b_p2_scores_mult),
            bip_2b_p1_scores_mult=float(bip_2b_p1_scores_mult),
            bip_1b_p1_to_3b_rate=float(bip_1b_p1_to_3b_rate),
            bip_ground_rbi_out_rate=float(bip_ground_rbi_out_rate),
            bip_out_2b_to_3b_rate=float(bip_out_2b_to_3b_rate),
            bip_out_1b_to_2b_rate=float(bip_out_1b_to_2b_rate),
            bip_misc_advance_pitch_rate=float(bip_misc_advance_pitch_rate),
            bip_roe_rate=float(bip_roe_rate),
            bip_fc_rate=float(bip_fc_rate),
            bip_fc_runner_on_3b_score_rate=float(bip_fc_runner_on_3b_score_rate),
            pitcher_rate_sampling=bool(pitcher_rate_sampling),
            pitcher_distribution_overrides=(pitcher_distribution_overrides or {}),
            manager_pitching=str(manager_pitching or "legacy"),
            manager_pitching_overrides=(manager_pitching_overrides or {}),
        )
        r = simulate_game(away_roster, home_roster, cfg)

        full = {"away": int(r.away_score), "home": int(r.home_score)}
        f5 = seg_score(r, 5)
        f3 = seg_score(r, 3)

        for seg, score in ((seg_full, full), (seg_f5, f5), (seg_f3, f3)):
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

        # Pitcher props (starter IDs passed in)
        if prop_ids:
            ps = r.pitcher_stats or {}
            for pid in prop_ids:
                row = ps.get(int(pid)) or {}
                try:
                    so = int(round(float(row.get("SO") or 0.0)))
                except Exception:
                    so = 0
                try:
                    outs = int(round(float(row.get("OUTS") or 0.0)))
                except Exception:
                    outs = 0
                try:
                    pitches = int(round(float(row.get("P") or 0.0)))
                except Exception:
                    pitches = 0
                acc = prop_acc[int(pid)]
                acc["so"][so] = int(acc["so"].get(so, 0) + 1)
                acc["outs"][outs] = int(acc["outs"].get(outs, 0) + 1)
                acc["pitches"][pitches] = int(acc["pitches"].get(pitches, 0) + 1)
                acc["so_sum"] = float(acc["so_sum"]) + float(so)
                acc["outs_sum"] = float(acc["outs_sum"]) + float(outs)
                acc["pitches_sum"] = float(acc["pitches_sum"]) + float(pitches)

        # Team batting stats
        bs = r.batter_stats or {}
        for side, ids in (("away", away_batter_ids), ("home", home_batter_ids)):
            h = _sum_team_stat(bs, ids, "H")
            hr = _sum_team_stat(bs, ids, "HR")
            so = _sum_team_stat(bs, ids, "SO")
            bb = _sum_team_stat(bs, ids, "BB")
            acc = team_bat[side]
            for k, v in (("H", h), ("HR", hr), ("SO", so), ("BB", bb)):
                acc[k][int(v)] = int(acc[k].get(int(v), 0) + 1)
            acc["H_sum"] = float(acc["H_sum"]) + float(h)
            acc["HR_sum"] = float(acc["HR_sum"]) + float(hr)
            acc["SO_sum"] = float(acc["SO_sum"]) + float(so)
            acc["BB_sum"] = float(acc["BB_sum"]) + float(bb)

        # Optional per-batter likelihoods (lineup only)
        if max_top_n > 0:
            for side, lineup_ids in (("away", away_lineup_ids), ("home", home_lineup_ids)):
                for pid in lineup_ids:
                    row = bs.get(int(pid)) or {}
                    try:
                        hr_i = int(row.get("HR") or 0)
                    except Exception:
                        hr_i = 0
                    try:
                        ab_i = int(row.get("AB") or 0)
                    except Exception:
                        ab_i = 0

                    try:
                        h_i = int(row.get("H") or 0)
                    except Exception:
                        h_i = 0
                    try:
                        d2_i = int(row.get("2B") or 0)
                    except Exception:
                        d2_i = 0
                    try:
                        d3_i = int(row.get("3B") or 0)
                    except Exception:
                        d3_i = 0
                    try:
                        r_i = int(row.get("R") or 0)
                    except Exception:
                        r_i = 0
                    try:
                        rbi_i = int(row.get("RBI") or 0)
                    except Exception:
                        rbi_i = 0
                    try:
                        sb_i = int(row.get("SB") or 0)
                    except Exception:
                        sb_i = 0
                    tb_i = int(h_i + d2_i + 2 * d3_i + 3 * hr_i)

                    hr_sum[side][int(pid)] = float(hr_sum[side].get(int(pid), 0.0)) + float(hr_i)
                    ab_sum[side][int(pid)] = float(ab_sum[side].get(int(pid), 0.0)) + float(ab_i)
                    if hr_i > 0:
                        hr_ge1[side][int(pid)] = int(hr_ge1[side].get(int(pid), 0) + 1)

                    h_sum[side][int(pid)] = float(h_sum[side].get(int(pid), 0.0)) + float(h_i)
                    if h_i > 0:
                        h_ge1[side][int(pid)] = int(h_ge1[side].get(int(pid), 0) + 1)
                    if h_i >= 2:
                        h_ge2[side][int(pid)] = int(h_ge2[side].get(int(pid), 0) + 1)
                    if h_i >= 3:
                        h_ge3[side][int(pid)] = int(h_ge3[side].get(int(pid), 0) + 1)

                    d2_sum[side][int(pid)] = float(d2_sum[side].get(int(pid), 0.0)) + float(d2_i)
                    if d2_i > 0:
                        d2_ge1[side][int(pid)] = int(d2_ge1[side].get(int(pid), 0) + 1)

                    d3_sum[side][int(pid)] = float(d3_sum[side].get(int(pid), 0.0)) + float(d3_i)
                    if d3_i > 0:
                        d3_ge1[side][int(pid)] = int(d3_ge1[side].get(int(pid), 0) + 1)

                    r_sum[side][int(pid)] = float(r_sum[side].get(int(pid), 0.0)) + float(r_i)
                    if r_i > 0:
                        r_ge1[side][int(pid)] = int(r_ge1[side].get(int(pid), 0) + 1)
                    if r_i >= 2:
                        r_ge2[side][int(pid)] = int(r_ge2[side].get(int(pid), 0) + 1)
                    if r_i >= 3:
                        r_ge3[side][int(pid)] = int(r_ge3[side].get(int(pid), 0) + 1)

                    rbi_sum[side][int(pid)] = float(rbi_sum[side].get(int(pid), 0.0)) + float(rbi_i)
                    if rbi_i > 0:
                        rbi_ge1[side][int(pid)] = int(rbi_ge1[side].get(int(pid), 0) + 1)
                    if rbi_i >= 2:
                        rbi_ge2[side][int(pid)] = int(rbi_ge2[side].get(int(pid), 0) + 1)
                    if rbi_i >= 3:
                        rbi_ge3[side][int(pid)] = int(rbi_ge3[side].get(int(pid), 0) + 1)
                    if rbi_i >= 4:
                        rbi_ge4[side][int(pid)] = int(rbi_ge4[side].get(int(pid), 0) + 1)

                    tb_sum[side][int(pid)] = float(tb_sum[side].get(int(pid), 0.0)) + float(tb_i)
                    if tb_i > 0:
                        tb_ge1[side][int(pid)] = int(tb_ge1[side].get(int(pid), 0) + 1)
                    if tb_i >= 2:
                        tb_ge2[side][int(pid)] = int(tb_ge2[side].get(int(pid), 0) + 1)
                    if tb_i >= 3:
                        tb_ge3[side][int(pid)] = int(tb_ge3[side].get(int(pid), 0) + 1)
                    if tb_i >= 4:
                        tb_ge4[side][int(pid)] = int(tb_ge4[side].get(int(pid), 0) + 1)
                    if tb_i >= 5:
                        tb_ge5[side][int(pid)] = int(tb_ge5[side].get(int(pid), 0) + 1)

                    sb_sum[side][int(pid)] = float(sb_sum[side].get(int(pid), 0.0)) + float(sb_i)
                    if sb_i > 0:
                        sb_ge1[side][int(pid)] = int(sb_ge1[side].get(int(pid), 0) + 1)

    denom = float(max(1, sims))

    def finalize(seg: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "home_win_prob": seg["home_wins"] / denom,
            "away_win_prob": seg["away_wins"] / denom,
            "tie_prob": seg["ties"] / denom,
            "total_runs_dist": seg["totals"],
            "run_margin_dist": seg["margins"],
        }

    out: Dict[str, Any] = {
        "sims": int(sims),
        "segments": {
            "full": finalize(seg_full),
            "first5": finalize(seg_f5),
            "first3": finalize(seg_f3),
        },
        "team_batting": {
            side: {
                "H_dist": {str(int(k)): int(v) for k, v in (acc.get("H") or {}).items()},
                "HR_dist": {str(int(k)): int(v) for k, v in (acc.get("HR") or {}).items()},
                "SO_dist": {str(int(k)): int(v) for k, v in (acc.get("SO") or {}).items()},
                "BB_dist": {str(int(k)): int(v) for k, v in (acc.get("BB") or {}).items()},
                "H_mean": float(acc.get("H_sum", 0.0)) / float(max(1, sims)),
                "HR_mean": float(acc.get("HR_sum", 0.0)) / float(max(1, sims)),
                "SO_mean": float(acc.get("SO_sum", 0.0)) / float(max(1, sims)),
                "BB_mean": float(acc.get("BB_sum", 0.0)) / float(max(1, sims)),
            }
            for side, acc in team_bat.items()
        },
        "pitcher_props": {
            str(int(pid)): {
                "so_dist": {str(int(k)): int(v) for k, v in (acc.get("so") or {}).items()},
                "outs_dist": {str(int(k)): int(v) for k, v in (acc.get("outs") or {}).items()},
                "pitches_dist": {str(int(k)): int(v) for k, v in (acc.get("pitches") or {}).items()},
                "so_mean": float(acc.get("so_sum", 0.0)) / float(max(1, sims)),
                "outs_mean": float(acc.get("outs_sum", 0.0)) / float(max(1, sims)),
                "pitches_mean": float(acc.get("pitches_sum", 0.0)) / float(max(1, sims)),
            }
            for pid, acc in prop_acc.items()
        },
    }

    if max_top_n > 0:
        def _finalize_side_all(side: str, name_map: Dict[int, str]) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            for pid, s_hr in (hr_sum.get(side) or {}).items():
                n_ge1 = int((hr_ge1.get(side) or {}).get(int(pid), 0) or 0)
                s_ab = float((ab_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)
                rows.append(
                    {
                        "batter_id": int(pid),
                        "name": str(name_map.get(int(pid), "")),
                        "p_hr_1plus": float(n_ge1) / float(denom),
                        "hr_mean": float(s_hr) / float(denom),
                        "ab_mean": float(s_ab) / float(denom),
                        "p_h_1plus": float(int((h_ge1.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_h_2plus": float(int((h_ge2.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_h_3plus": float(int((h_ge3.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "h_mean": float(float((h_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)) / float(denom),
                        "p_2b_1plus": float(int((d2_ge1.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "2b_mean": float(float((d2_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)) / float(denom),
                        "p_3b_1plus": float(int((d3_ge1.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "3b_mean": float(float((d3_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)) / float(denom),
                        "p_r_1plus": float(int((r_ge1.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_r_2plus": float(int((r_ge2.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_r_3plus": float(int((r_ge3.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "r_mean": float(float((r_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)) / float(denom),
                        "p_rbi_1plus": float(int((rbi_ge1.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_rbi_2plus": float(int((rbi_ge2.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_rbi_3plus": float(int((rbi_ge3.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_rbi_4plus": float(int((rbi_ge4.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "rbi_mean": float(float((rbi_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)) / float(denom),
                        "p_tb_1plus": float(int((tb_ge1.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_tb_2plus": float(int((tb_ge2.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_tb_3plus": float(int((tb_ge3.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_tb_4plus": float(int((tb_ge4.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "p_tb_5plus": float(int((tb_ge5.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "tb_mean": float(float((tb_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)) / float(denom),
                        "p_sb_1plus": float(int((sb_ge1.get(side) or {}).get(int(pid), 0) or 0)) / float(denom),
                        "sb_mean": float(float((sb_sum.get(side) or {}).get(int(pid), 0.0) or 0.0)) / float(denom),
                    }
                )
            return rows

        away_all = _finalize_side_all("away", away_name)
        home_all = _finalize_side_all("home", home_name)
        all_rows = away_all + home_all

        out["hitter_hr_likelihood_all"] = {
            "top_n": int(hr_top_n),
            "n": int(len(all_rows)),
            "away": list(away_all),
            "home": list(home_all),
            "overall": list(all_rows),
        }

        def _top(rows: List[Dict[str, Any]], metric: str, tiebreak: str, n: int) -> List[Dict[str, Any]]:
            rs = list(rows)
            rs.sort(
                key=lambda x: (
                    float(x.get(metric) or 0.0),
                    float(x.get(tiebreak) or 0.0),
                    float(x.get("ab_mean") or 0.0),
                ),
                reverse=True,
            )
            return rs[: max(0, int(n))]

        if hr_top_n > 0:
            out["hitter_hr_likelihood"] = {
                "top_n": int(hr_top_n),
                "away": _top(away_all, "p_hr_1plus", "hr_mean", n=hr_top_n),
                "home": _top(home_all, "p_hr_1plus", "hr_mean", n=hr_top_n),
                "overall": _top(all_rows, "p_hr_1plus", "hr_mean", n=hr_top_n),
            }

        if props_top_n > 0:
            props_out: Dict[str, Any] = {"top_n": int(props_top_n)}
            for prop_key, p_field, _actual_key, mean_field, _threshold in _HITTER_PROP_SPECS:
                props_out[prop_key] = _top(all_rows, p_field, mean_field, n=props_top_n)
            out["hitter_props_likelihood"] = props_out

    return out


def _mean_from_dist(dist: Dict[str, Any]) -> Optional[float]:
    # dist keys are ints (serialized) -> counts
    if not isinstance(dist, dict) or not dist:
        return None
    s = 0.0
    n = 0.0
    for k, v in dist.items():
        try:
            kk = int(k)
            vv = float(v)
        except Exception:
            continue
        if vv <= 0:
            continue
        s += float(kk) * vv
        n += vv
    if n <= 0:
        return None
    return s / n


def _rmse(errors: List[float]) -> Optional[float]:
    if not errors:
        return None
    return float((sum(float(e) * float(e) for e in errors) / float(len(errors))) ** 0.5)


def _logloss(p: float, y: float, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    try:
        yy = float(y)
    except Exception:
        yy = 0.0
    yy = float(min(1.0, max(0.0, yy)))
    return float(-(yy * math.log(pp) + (1.0 - yy) * math.log(1.0 - pp)))


def _brier(p: float, y: int) -> float:
    return float((float(p) - float(y)) ** 2)


def _prob_from_dist(dist: Dict[str, Any], value: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    try:
        c = float(dist.get(str(int(value)), dist.get(int(value), 0)) or 0.0)
    except Exception:
        c = 0.0
    return float(max(0.0, min(1.0, c / float(denom))))


def _prob_margin_le(dist: Dict[str, Any], threshold: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    tot = 0.0
    for k, v in (dist or {}).items():
        try:
            kk = int(k)
            vv = float(v)
        except Exception:
            continue
        if kk <= int(threshold):
            tot += vv
    return float(max(0.0, min(1.0, tot / float(denom))))


def _prob_margin_ge(dist: Dict[str, Any], threshold: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    tot = 0.0
    for k, v in (dist or {}).items():
        try:
            kk = int(k)
            vv = float(v)
        except Exception:
            continue
        if kk >= int(threshold):
            tot += vv
    return float(max(0.0, min(1.0, tot / float(denom))))


def _prob_over_line_from_dist(dist: Dict[str, Any], line: float) -> Optional[float]:
    if not isinstance(dist, dict) or not dist:
        return None
    try:
        ln = float(line)
    except Exception:
        return None
    n = 0.0
    over = 0.0
    for k, v in dist.items():
        try:
            kk = int(k)
            vv = float(v)
        except Exception:
            continue
        if vv <= 0:
            continue
        n += vv
        if float(kk) > ln:
            over += vv
    if n <= 0:
        return None
    return float(max(0.0, min(1.0, over / n)))


 


def _parse_actual_starter_pitching(feed: Dict[str, Any], side: str, starter_id: Optional[int]) -> Optional[Dict[str, Any]]:
    try:
        pid = int(starter_id or 0)
    except Exception:
        pid = 0
    if pid <= 0:
        return None
    try:
        box = (feed.get("liveData") or {}).get("boxscore") or {}
        teams = box.get("teams") or {}
        t = teams.get(str(side)) or {}
        players = t.get("players") or {}
        pobj = players.get(f"ID{pid}") or {}
        pitching = ((pobj.get("stats") or {}).get("pitching") or {})
        so = pitching.get("strikeOuts")
        outs = pitching.get("outs")
        if so is None or outs is None:
            return None
        pitches = pitching.get("pitchesThrown")
        if pitches is None:
            pitches = pitching.get("numberOfPitches")
        pitches_i: Optional[int]
        try:
            pitches_i = int(float(pitches)) if pitches is not None else None
        except Exception:
            pitches_i = None

        return {
            "pitcher_id": int(pid),
            "so": int(float(so)),
            "outs": int(float(outs)),
            "pitches": int(pitches_i) if pitches_i is not None else None,
        }
    except Exception:
        return None


def _simulate_one_game_task(task: Dict[str, Any]) -> Dict[str, Any]:
    away_roster = task["away_roster"]
    home_roster = task["home_roster"]
    sims_per_game = int(task["sims_per_game"])
    seed = int(task["seed"])
    weather = task.get("weather")
    park = task.get("park")
    umpire = task.get("umpire")
    # NOTE: umpire factors are already baked into the UmpireFactors object.
    # When running ablations, main() will neutralize called_strike_mult as needed.
    pitch_model_overrides = task.get("pitch_model_overrides")
    weather_hr_weight = float(task.get("weather_hr_weight", 1.0))
    weather_inplay_hit_weight = float(task.get("weather_inplay_hit_weight", 1.0))
    weather_xb_share_weight = float(task.get("weather_xb_share_weight", 1.0))
    park_hr_weight = float(task.get("park_hr_weight", 1.0))
    park_inplay_hit_weight = float(task.get("park_inplay_hit_weight", 1.0))
    park_xb_share_weight = float(task.get("park_xb_share_weight", 1.0))
    bip_baserunning = task.get("bip_baserunning", True)
    cfg_defaults = GameConfig()
    bip_dp_rate = task.get("bip_dp_rate", float(getattr(cfg_defaults, "bip_dp_rate", 0.0)))
    bip_sf_rate_flypop = task.get("bip_sf_rate_flypop", float(getattr(cfg_defaults, "bip_sf_rate_flypop", 0.48)))
    bip_sf_rate_line = task.get("bip_sf_rate_line", float(getattr(cfg_defaults, "bip_sf_rate_line", 0.36)))
    bip_1b_p2_scores_mult = task.get(
        "bip_1b_p2_scores_mult", float(getattr(cfg_defaults, "bip_1b_p2_scores_mult", 1.0))
    )
    bip_2b_p1_scores_mult = task.get(
        "bip_2b_p1_scores_mult", float(getattr(cfg_defaults, "bip_2b_p1_scores_mult", 1.0))
    )
    bip_1b_p1_to_3b_rate = task.get(
        "bip_1b_p1_to_3b_rate", float(getattr(cfg_defaults, "bip_1b_p1_to_3b_rate", 0.24))
    )
    bip_ground_rbi_out_rate = task.get(
        "bip_ground_rbi_out_rate", float(getattr(cfg_defaults, "bip_ground_rbi_out_rate", 0.18))
    )
    bip_out_2b_to_3b_rate = task.get(
        "bip_out_2b_to_3b_rate", float(getattr(cfg_defaults, "bip_out_2b_to_3b_rate", 0.24))
    )
    bip_out_1b_to_2b_rate = task.get(
        "bip_out_1b_to_2b_rate", float(getattr(cfg_defaults, "bip_out_1b_to_2b_rate", 0.14))
    )
    bip_misc_advance_pitch_rate = task.get(
        "bip_misc_advance_pitch_rate", float(getattr(cfg_defaults, "bip_misc_advance_pitch_rate", 0.004))
    )
    bip_roe_rate = task.get(
        "bip_roe_rate", float(getattr(cfg_defaults, "bip_roe_rate", 0.012))
    )
    bip_fc_rate = task.get(
        "bip_fc_rate", float(getattr(cfg_defaults, "bip_fc_rate", 0.04))
    )
    pitcher_rate_sampling = task.get("pitcher_rate_sampling", True)
    pitcher_distribution_overrides = task.get("pitcher_distribution_overrides")
    manager_pitching = task.get("manager_pitching", "legacy")
    manager_pitching_overrides = task.get("manager_pitching_overrides")
    hitter_hr_top_n = int(task.get("hitter_hr_top_n") or 0)
    hitter_props_top_n = int(task.get("hitter_props_top_n") if task.get("hitter_props_top_n") is not None else -1)
    sims = _sim_many(
        away_roster,
        home_roster,
        sims=sims_per_game,
        seed=seed,
        weather=weather,
        park=park,
        umpire=umpire,
        weather_hr_weight=float(weather_hr_weight),
        weather_inplay_hit_weight=float(weather_inplay_hit_weight),
        weather_xb_share_weight=float(weather_xb_share_weight),
        park_hr_weight=float(park_hr_weight),
        park_inplay_hit_weight=float(park_inplay_hit_weight),
        park_xb_share_weight=float(park_xb_share_weight),
        pitch_model_overrides=pitch_model_overrides,
        bip_baserunning=bool(bip_baserunning),
        bip_dp_rate=float(bip_dp_rate),
        bip_sf_rate_flypop=float(bip_sf_rate_flypop),
        bip_sf_rate_line=float(bip_sf_rate_line),
        bip_1b_p2_scores_mult=float(bip_1b_p2_scores_mult),
        bip_2b_p1_scores_mult=float(bip_2b_p1_scores_mult),
        bip_1b_p1_to_3b_rate=float(bip_1b_p1_to_3b_rate),
        bip_ground_rbi_out_rate=float(bip_ground_rbi_out_rate),
        bip_out_2b_to_3b_rate=float(bip_out_2b_to_3b_rate),
        bip_out_1b_to_2b_rate=float(bip_out_1b_to_2b_rate),
        bip_misc_advance_pitch_rate=float(bip_misc_advance_pitch_rate),
        bip_roe_rate=float(bip_roe_rate),
        bip_fc_rate=float(bip_fc_rate),
        pitcher_rate_sampling=bool(pitcher_rate_sampling),
        pitcher_distribution_overrides=(pitcher_distribution_overrides or {}),
        manager_pitching=str(manager_pitching or "legacy"),
        manager_pitching_overrides=(manager_pitching_overrides or {}),
        pitcher_prop_ids=(task.get("pitcher_prop_ids") or []),
        hitter_hr_top_n=int(hitter_hr_top_n),
        hitter_props_top_n=int(hitter_props_top_n),
    )
    return {"task": task, "sims": sims}


def _apply_stamina_mode(roster, mode: str) -> None:
    """Override pitcher stamina_pitches on a built roster for A/B ablations.

    mode:
      - season: keep season-derived stamina (default behavior)
      - season_bullpen65: keep season-derived starter stamina, set bullpen to 65
      - prior: use role-based priors only (SP~92, RP~25, LR~45)
      - legacy92: historical baseline where everyone was 92
    """
    m = str(mode or "season").strip().lower()
    if m not in ("season", "season_bullpen65", "prior", "legacy92"):
        m = "season"
    if m == "season":
        return

    try:
        pitchers = [roster.lineup.pitcher] + list(roster.lineup.bullpen or [])
    except Exception:
        pitchers = []

    for p in pitchers:
        try:
            if m == "season_bullpen65":
                # Keep starter as season-derived; pin bullpen to a high-stamina
                # setting that matches the v2 reliever effective-stamina clamp.
                if p is roster.lineup.pitcher:
                    continue
                p.stamina_pitches = 65
                continue

            if m == "legacy92":
                p.stamina_pitches = 92
                continue

            if m == "prior":
                role = str(getattr(p, "role", "") or "").upper()
                if p is roster.lineup.pitcher or role == "SP":
                    p.stamina_pitches = 92
                elif role == "LR":
                    p.stamina_pitches = 45
                else:
                    p.stamina_pitches = 25
        except Exception:
            continue


def _apply_umpire_mode(umpire, mode: str) -> None:
    """Ablation hook for umpire called-strike multiplier.

    mode:
      - factors: keep fetched umpire.called_strike_mult (default)
      - neutral: force called_strike_mult=1.0 (no umpire effect)
    """
    if umpire is None:
        return
    m = str(mode or "factors").strip().lower()
    if m not in ("factors", "neutral"):
        m = "factors"
    if m == "factors":
        return
    try:
        umpire.called_strike_mult = 1.0
        src = str(getattr(umpire, "source", "") or "")
        setattr(umpire, "source", (src + "|neutralized") if src else "neutralized")
    except Exception:
        return


def _apply_umpire_shrink(umpire, shrink: float) -> None:
    """Shrink called_strike_mult toward 1.0.

    new = 1 + a*(old-1), with a clamped to [0, 1].
    """
    if umpire is None:
        return
    try:
        a = float(shrink)
    except Exception:
        return
    a = max(0.0, min(1.0, a))
    try:
        old = float(getattr(umpire, "called_strike_mult", 1.0) or 1.0)
    except Exception:
        old = 1.0
    new = 1.0 + a * (old - 1.0)
    try:
        umpire.called_strike_mult = float(new)
        if a < 0.999:
            src = str(getattr(umpire, "source", "") or "")
            tag = f"shrink{a:.3g}"
            setattr(umpire, "source", (src + "|" + tag) if src else tag)
    except Exception:
        return


def _load_jsonish(val: str) -> Optional[Dict[str, Any]]:
    s = str(val or "").strip()
    if not s:
        return None
    try:
        if s.startswith("{"):
            obj = json.loads(s.lstrip("\ufeff"))
        else:
            txt = Path(s).read_text(encoding="utf-8-sig")
            obj = json.loads(txt.lstrip("\ufeff"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _implied_prob_from_american(odds: Any) -> Optional[float]:
    s = str(odds or "").strip()
    if not s:
        return None
    try:
        if s.startswith("+"):
            s = s[1:]
        v = float(s)
    except Exception:
        return None
    if v > 0:
        return float(100.0 / (v + 100.0))
    if v < 0:
        return float(abs(v) / (abs(v) + 100.0))
    return None


def _no_vig_two_way(p1: Optional[float], p2: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if p1 is None or p2 is None:
        return None, None
    z = float(p1) + float(p2)
    if z <= 0.0:
        return None, None
    return float(p1 / z), float(p2 / z)


def _load_game_lines_for_date(date_str: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    token = str(date_str).replace("-", "_")
    p = _ROOT / "data" / "market" / "oddsapi" / f"oddsapi_game_lines_{token}.json"
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8-sig").lstrip("\ufeff"))
    except Exception:
        return {}
    games = obj.get("games") or []
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not isinstance(games, list):
        return out
    for g in games:
        if not isinstance(g, dict):
            continue
        away = str(g.get("away_team") or "").strip()
        home = str(g.get("home_team") or "").strip()
        if not away or not home:
            continue
        out[(away, home)] = g
    return out


def _preferred_hitter_hr_likelihood(sim_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(sim_payload, dict):
        return {}
    full_rows = sim_payload.get("hitter_hr_likelihood_all")
    if isinstance(full_rows, dict) and isinstance(full_rows.get("overall"), list) and full_rows.get("overall"):
        return full_rows
    top_rows = sim_payload.get("hitter_hr_likelihood")
    if isinstance(top_rows, dict) and isinstance(top_rows.get("overall"), list) and top_rows.get("overall"):
        return top_rows
    return top_rows if isinstance(top_rows, dict) else {}


def _market_context_from_game_line(game_line: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(game_line, dict):
        return {}
    markets = game_line.get("markets") or {}
    h2h = markets.get("h2h") or {}
    totals = markets.get("totals") or {}
    total_line_raw = totals.get("line")
    try:
        total_line = float(total_line_raw) if total_line_raw is not None else None
    except Exception:
        total_line = None

    home_imp = _implied_prob_from_american(h2h.get("home_odds"))
    away_imp = _implied_prob_from_american(h2h.get("away_odds"))
    home_nv, away_nv = _no_vig_two_way(home_imp, away_imp)
    favorite_side = None
    favorite_prob = None
    if home_nv is not None and away_nv is not None:
        if float(home_nv) >= float(away_nv):
            favorite_side = "home"
            favorite_prob = float(home_nv)
        else:
            favorite_side = "away"
            favorite_prob = float(away_nv)

    return {
        "away_team": str(game_line.get("away_team") or ""),
        "home_team": str(game_line.get("home_team") or ""),
        "total_line": total_line,
        "home_odds": h2h.get("home_odds"),
        "away_odds": h2h.get("away_odds"),
        "favorite_side": favorite_side,
        "favorite_prob": favorite_prob,
    }


def _match_market_override_rule(rule: Dict[str, Any], market_context: Dict[str, Any]) -> bool:
    when = rule.get("when") or {}
    if not isinstance(when, dict):
        return False

    total_line = market_context.get("total_line")
    favorite_prob = market_context.get("favorite_prob")
    favorite_side = str(market_context.get("favorite_side") or "").strip().lower()

    numeric_checks = [
        ("total_line_gte", total_line, lambda a, b: a >= b),
        ("total_line_gt", total_line, lambda a, b: a > b),
        ("total_line_lte", total_line, lambda a, b: a <= b),
        ("total_line_lt", total_line, lambda a, b: a < b),
        ("favorite_prob_gte", favorite_prob, lambda a, b: a >= b),
        ("favorite_prob_gt", favorite_prob, lambda a, b: a > b),
        ("favorite_prob_lte", favorite_prob, lambda a, b: a <= b),
        ("favorite_prob_lt", favorite_prob, lambda a, b: a < b),
    ]
    for key, current, cmp_fn in numeric_checks:
        if key not in when:
            continue
        if current is None:
            return False
        try:
            expected = float(when.get(key))
        except Exception:
            return False
        if not cmp_fn(float(current), expected):
            return False

    if "favorite_side" in when:
        expected_side = str(when.get("favorite_side") or "").strip().lower()
        if not expected_side or favorite_side != expected_side:
            return False

    return True


def _apply_market_game_config_overrides(
    base_cfg: Dict[str, Any],
    selector: Optional[Dict[str, Any]],
    market_context: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    effective = copy.deepcopy(base_cfg)
    meta: Dict[str, Any] = {
        "market_context": dict(market_context or {}),
        "matched_rules": [],
        "applied_overrides": {},
    }
    if not isinstance(selector, dict):
        return effective, meta

    rules = selector.get("rules") or []
    if not isinstance(rules, list):
        return effective, meta

    allowed_keys = set(getattr(GameConfig, "__dataclass_fields__", {}).keys())
    blocked_keys = {"weather", "park", "umpire", "rng_seed"}

    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict) or not _match_market_override_rule(rule, market_context):
            continue
        overrides = rule.get("overrides") or {}
        if not isinstance(overrides, dict):
            continue
        rule_name = str(rule.get("name") or f"rule_{idx + 1}")
        applied_for_rule: Dict[str, Any] = {}
        for key, value in overrides.items():
            if key in blocked_keys or key not in allowed_keys:
                continue
            if isinstance(effective.get(key), dict) and isinstance(value, dict):
                effective[key] = {**dict(effective.get(key) or {}), **dict(value)}
            else:
                effective[key] = value
            applied_for_rule[str(key)] = effective.get(key)
        if applied_for_rule:
            meta["matched_rules"].append(rule_name)
            meta["applied_overrides"][rule_name] = applied_for_rule

    return effective, meta


def _load_daily_injuries(snapshot_dir: Path) -> Dict[int, List[int]]:
    """Load injuries_raw.json produced by tools/daily_update.py.

    Returns team_id -> injured_id_list.
    """
    p = snapshot_dir / "injuries_raw.json"
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8-sig").lstrip("\ufeff"))
    except Exception:
        return {}

    teams = obj.get("teams") or {}
    out: Dict[int, List[int]] = {}
    if isinstance(teams, dict):
        for k, v in teams.items():
            try:
                tid = int(k)
            except Exception:
                try:
                    tid = int((v.get("team") or {}).get("id") or 0)
                except Exception:
                    tid = 0
            if tid <= 0 or not isinstance(v, dict):
                continue
            ids = v.get("injured_ids") or []
            if not isinstance(ids, list):
                continue
            out[int(tid)] = [int(x) for x in ids if isinstance(x, (int, float, str)) and str(x).strip().isdigit()]
    return out


def _load_daily_lineups(snapshot_dir: Path) -> Dict[int, Dict[str, Any]]:
    """Load lineups.json produced by tools/daily_update.py.

    Returns game_pk -> dict with keys like away_confirmed_ids/home_confirmed_ids and projected variants.
    """
    p = snapshot_dir / "lineups.json"
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8-sig").lstrip("\ufeff"))
    except Exception:
        return {}

    games = obj.get("games") or []
    out: Dict[int, Dict[str, Any]] = {}
    if isinstance(games, list):
        for g in games:
            if not isinstance(g, dict):
                continue
            try:
                pk = int(g.get("game_pk") or 0)
            except Exception:
                pk = 0
            if pk <= 0:
                continue

            def _ids(key: str) -> List[int]:
                xs = g.get(key) or []
                if not isinstance(xs, list):
                    return []
                out_ids: List[int] = []
                for x in xs:
                    try:
                        out_ids.append(int(x))
                    except Exception:
                        continue
                return out_ids

            out[int(pk)] = {
                "away_confirmed_ids": _ids("away_confirmed_ids"),
                "home_confirmed_ids": _ids("home_confirmed_ids"),
                "away_projected_ids": _ids("away_projected_ids"),
                "home_projected_ids": _ids("home_projected_ids"),
                "away_source": str(g.get("away_source") or ""),
                "home_source": str(g.get("home_source") or ""),
                "away_confidence": float(g.get("away_confidence") or 0.0),
                "home_confidence": float(g.get("home_confidence") or 0.0),
            }
    return out


def _load_daily_probables(snapshot_dir: Path) -> Dict[int, Dict[str, Any]]:
    """Load probables.json produced by tools/daily_update.py.

    Returns game_pk -> dict with away/home probable ids and provenance fields.
    """
    p = snapshot_dir / "probables.json"
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8-sig").lstrip("\ufeff"))
    except Exception:
        return {}

    games = obj.get("games") or []
    out: Dict[int, Dict[str, Any]] = {}
    if isinstance(games, list):
        for g in games:
            if not isinstance(g, dict):
                continue
            try:
                pk = int(g.get("game_pk") or 0)
            except Exception:
                pk = 0
            if pk <= 0:
                continue

            def _iid(v) -> Optional[int]:
                try:
                    if v is None:
                        return None
                    iv = int(v)
                    return int(iv) if iv > 0 else None
                except Exception:
                    return None

            out[int(pk)] = {
                "away_probable_id": _iid(g.get("away_probable_id")),
                "home_probable_id": _iid(g.get("home_probable_id")),
                "away_source": str(g.get("away_source") or ""),
                "home_source": str(g.get("home_source") or ""),
                "away_confidence": float(g.get("away_confidence") or 0.0),
                "home_confidence": float(g.get("home_confidence") or 0.0),
            }
    return out


def _load_last_known_lineups(path_str: str) -> Dict[int, List[int]]:
    """Load data/daily/lineups_last_known_by_team.json (team_id -> ids)."""
    p = Path(path_str)
    if not p.is_absolute():
        p = (_ROOT / p).resolve()
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8-sig").lstrip("\ufeff"))
    except Exception:
        return {}
    out: Dict[int, List[int]] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            try:
                tid = int(k)
            except Exception:
                tid = 0
            if tid <= 0 or not isinstance(v, dict):
                continue
            ids = v.get("ids") or []
            if not isinstance(ids, list):
                continue
            out_ids: List[int] = []
            for x in ids:
                try:
                    out_ids.append(int(x))
                except Exception:
                    continue
            if out_ids:
                out[int(tid)] = out_ids
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Simulate a day of games and evaluate vs actual results (historical)")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", type=int, default=0)
    ap.add_argument(
        "--spring-mode",
        choices=["on", "off"],
        default="off",
        help="If on, use spring-training-friendly roster fallbacks and default stats season to season-1.",
    )
    ap.add_argument(
        "--stats-season",
        type=int,
        default=0,
        help="Season year to use for StatsAPI stat lookups (0=auto). In --spring-mode on, auto=season-1.",
    )
    ap.add_argument(
        "--use-daily-snapshots",
        choices=["on", "off"],
        default="on",
        help="If on, load injuries/lineups artifacts from data/daily/snapshots/<date>/ when present.",
    )
    ap.add_argument(
        "--daily-snapshots-root",
        default="data/daily/snapshots",
        help="Root folder containing per-date snapshot folders (default: data/daily/snapshots).",
    )
    ap.add_argument(
        "--use-roster-artifacts",
        choices=["on", "off"],
        default="on",
        help="If on, reuse serialized roster artifacts from data/daily/snapshots/<date>/roster_objs/ when present and compatible.",
    )
    ap.add_argument(
        "--write-roster-artifacts",
        choices=["on", "off"],
        default="off",
        help="If on, write serialized roster artifacts to data/daily/snapshots/<date>/roster_objs/ for reuse in later eval/tuning runs.",
    )
    ap.add_argument(
        "--lineups-last-known",
        default="",
        help="Optional path to lineups_last_known_by_team.json for projected-lineup fallback.",
    )
    ap.add_argument("--sims-per-game", type=int, default=500)
    ap.add_argument(
        "--bvp-hr",
        choices=["on", "off"],
        default="off",
        help="If on, apply shrunk batter-vs-starter matchup multipliers from local Statcast raw pitch files for HR, K, BB, and contact quality.",
    )
    ap.add_argument("--bvp-days-back", type=int, default=365, help="How many days of history to consider for BvP lookup.")
    ap.add_argument("--bvp-min-pa", type=int, default=10, help="Minimum BvP PA required to apply a multiplier.")
    ap.add_argument("--bvp-shrink-pa", type=float, default=50.0, help="Shrinkage PA constant (higher = more shrink toward 1.0).")
    ap.add_argument("--bvp-clamp-lo", type=float, default=0.80, help="Lower clamp for BvP HR multiplier.")
    ap.add_argument("--bvp-clamp-hi", type=float, default=1.25, help="Upper clamp for BvP HR multiplier.")
    ap.add_argument(
        "--hitter-hr-topn",
        type=int,
        default=0,
        help="If >0, include top-N lineup batters by HR likelihood (Monte Carlo p(HR>=1), mean HR, mean AB).",
    )
    ap.add_argument(
        "--hitter-props-topn",
        type=int,
        default=24,
        help=(
            "Top-N size for broader hitter props (hits/runs/RBI/SB/etc). "
            "Default 24. -1=use --hitter-hr-topn (back-compat), 0=disable."
        ),
    )
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--jobs", type=int, default=1, help="Parallel workers (games in parallel). 1=off")
    ap.add_argument("--use-raw", choices=["on", "off"], default="on")
    ap.add_argument("--write-missing-raw", choices=["on", "off"], default="off")
    ap.add_argument(
        "--prop-lines-source",
        choices=["auto", "oddsapi", "last_known", "bovada", "off"],
        default="auto",
        help="Source of pitcher prop lines from original repo (used for O/U-at-line scoring)",
    )
    ap.add_argument(
        "--market-push-policy",
        choices=["loss", "half", "skip"],
        default="skip",
        help=(
            "How to score exact-at-line outcomes (pushes) for pitcher O/U at market lines. "
            "loss=treat push as not-over (y=0), half=soft label y=0.5, skip=exclude push rows from scoring."
        ),
    )
    ap.add_argument(
        "--original-repo-root",
        default="",
        help="Optional path to original MLB-Betting repo root (defaults to sibling folder)",
    )
    ap.add_argument("--out", default="")
    ap.add_argument("--cache-ttl-hours", type=int, default=24)
    ap.add_argument(
        "--umpire-mode",
        choices=["factors", "neutral"],
        default="factors",
        help="Ablation hook for umpire called-strike multiplier (factors=use local map if present, neutral=force 1.0)",
    )
    ap.add_argument(
        "--umpire-shrink",
        type=float,
        default=0.75,
        help="Shrink umpire called_strike_mult toward 1.0. 1.0=no shrink, 0.5=half effect, 0.0=neutral.",
    )
    ap.add_argument(
        "--pitch-model-overrides",
        default="",
        help='JSON dict or path to JSON file to override sim_engine.pitch_model.PitchModelConfig fields (tuning hook)',
    )
    ap.add_argument(
        "--market-game-config-overrides",
        default="",
        help=(
            "Optional JSON dict or path to JSON file with per-game config override rules keyed off market total line "
            "and no-vig favorite probability. Each rule must contain {when, overrides}."
        ),
    )
    ap.add_argument(
        "--bip-baserunning",
        choices=["on", "off"],
        default="on",
        help="Toggle batted-ball-informed baserunning (DP/SF/advancement)",
    )
    ap.add_argument(
        "--batter-vs-pitch-type",
        choices=["on", "off"],
        default="on",
        help="Toggle Statcast-derived batter vs pitch-type multipliers (ablation hook)",
    )
    ap.add_argument(
        "--pitch-type-hr",
        choices=["on", "off"],
        default="on",
        help="Toggle HR-specific pitch-type multipliers while leaving other pitch-type effects intact.",
    )
    ap.add_argument(
        "--batter-platoon",
        choices=["on", "off"],
        default="on",
        help="Toggle batter platoon split multipliers vs pitcher handedness (StatsAPI splits)",
    )
    ap.add_argument(
        "--pitcher-platoon",
        choices=["on", "off"],
        default="on",
        help="Toggle pitcher platoon split multipliers vs batter handedness (StatsAPI splits)",
    )
    ap.add_argument(
        "--batter-platoon-alpha",
        type=float,
        default=0.55,
        help="Shrink batter platoon multipliers toward 1.0. 1.0=full effect, 0.5=half effect, 0.0=neutral.",
    )
    ap.add_argument(
        "--pitcher-platoon-alpha",
        type=float,
        default=0.55,
        help="Shrink pitcher platoon multipliers toward 1.0. 1.0=full effect, 0.5=half effect, 0.0=neutral.",
    )
    ap.add_argument(
        "--batter-recency-games",
        type=int,
        default=14,
        help="Recent-games window for batter recency blend when building rosters.",
    )
    ap.add_argument(
        "--batter-recency-weight",
        type=float,
        default=0.15,
        help="Weight for batter recency blend (0.0=off, 1.0=all recent).",
    )
    ap.add_argument(
        "--pitcher-recency-games",
        type=int,
        default=6,
        help="Recent-games window for pitcher recency blend when building rosters.",
    )
    ap.add_argument(
        "--pitcher-recency-weight",
        type=float,
        default=0.15,
        help="Weight for pitcher recency blend (0.0=off, 1.0=all recent).",
    )
    ap.add_argument(
        "--weather-hr-weight",
        type=float,
        default=1.0,
        help="Exponent weight for weather HR multiplier (mult^weight). 1.0=baseline.",
    )
    ap.add_argument(
        "--weather-inplay-hit-weight",
        type=float,
        default=1.0,
        help="Exponent weight for weather in-play hit multiplier (mult^weight). 1.0=baseline.",
    )
    ap.add_argument(
        "--weather-xb-share-weight",
        type=float,
        default=1.0,
        help="Exponent weight for weather XB share multiplier (mult^weight). 1.0=baseline.",
    )
    ap.add_argument(
        "--park-hr-weight",
        type=float,
        default=1.0,
        help="Exponent weight for park HR multiplier (mult^weight). 1.0=baseline.",
    )
    ap.add_argument(
        "--park-inplay-hit-weight",
        type=float,
        default=1.0,
        help="Exponent weight for park in-play hit multiplier (mult^weight). 1.0=baseline.",
    )
    ap.add_argument(
        "--park-xb-share-weight",
        type=float,
        default=1.0,
        help="Exponent weight for park XB share multiplier (mult^weight). 1.0=baseline.",
    )
    ap.add_argument(
        "--bip-dp-rate",
        type=float,
        default=None,
        help="Override DP rate on in-play ground-ball outs (only used when --bip-baserunning on). If omitted, uses GameConfig default (currently 0.06).",
    )
    ap.add_argument(
        "--bip-sf-rate-flypop",
        type=float,
        default=None,
        help="Override sac-fly rate for fly/pop outs with runner on 3rd (only used when --bip-baserunning on). If omitted, uses GameConfig default (currently 0.48).",
    )
    ap.add_argument(
        "--bip-sf-rate-line",
        type=float,
        default=None,
        help="Override sac-fly rate for line-drive outs with runner on 3rd (only used when --bip-baserunning on). If omitted, uses GameConfig default (currently 0.36).",
    )
    ap.add_argument(
        "--bip-1b-p2-scores-mult",
        type=float,
        default=None,
        help=(
            "Scale probability runner on 2B scores on 1B (only used when --bip-baserunning on). "
            "If omitted, uses GameConfig default (currently 1.15)."
        ),
    )
    ap.add_argument(
        "--bip-2b-p1-scores-mult",
        type=float,
        default=None,
        help=(
            "Scale probability runner on 1B scores on 2B (only used when --bip-baserunning on). "
            "If omitted, uses GameConfig default (currently 1.15)."
        ),
    )
    ap.add_argument(
        "--bip-1b-p1-to-3b-rate",
        type=float,
        default=None,
        help="Override probability runner on 1B advances to 3B on a 1B when not forced. If omitted, uses GameConfig default (currently 0.24).",
    )
    ap.add_argument(
        "--bip-ground-rbi-out-rate",
        type=float,
        default=None,
        help="Override probability of a ground-ball RBI out with runner on 3B and less than 2 outs. If omitted, uses GameConfig default (currently 0.18).",
    )
    ap.add_argument(
        "--bip-out-2b-to-3b-rate",
        type=float,
        default=None,
        help="Override probability runner on 2B advances to 3B on a productive out. If omitted, uses GameConfig default (currently 0.24).",
    )
    ap.add_argument(
        "--bip-out-1b-to-2b-rate",
        type=float,
        default=None,
        help="Override probability runner on 1B advances to 2B on a productive out. If omitted, uses GameConfig default (currently 0.14).",
    )
    ap.add_argument(
        "--bip-misc-advance-pitch-rate",
        type=float,
        default=None,
        help="Override probability of a WP/PB/balk-style runner advance on a non-in-play pitch. If omitted, uses GameConfig default (currently 0.004).",
    )
    ap.add_argument(
        "--bip-roe-rate",
        type=float,
        default=None,
        help="Override probability an in-play out becomes reach-on-error. If omitted, uses GameConfig default (currently 0.012).",
    )
    ap.add_argument(
        "--bip-fc-rate",
        type=float,
        default=None,
        help="Override probability of a fielder's-choice style out on a ground ball with a runner on 1B. If omitted, uses GameConfig default (currently 0.04).",
    )
    ap.add_argument(
        "--bip-fc-runner-on-3b-score-rate",
        type=float,
        default=None,
        help="Override probability a runner on 3B scores on a fielder's-choice ground ball. If omitted, uses GameConfig default (currently 0.0).",
    )
    ap.add_argument(
        "--pitcher-rate-sampling",
        choices=["on", "off"],
        default="on",
        help="Toggle per-game pitcher day-rate sampling (uncertainty)",
    )
    ap.add_argument(
        "--stamina-mode",
        choices=["season", "season_bullpen65", "prior", "legacy92"],
        default="season",
        help="Ablation hook for pitcher stamina_pitches (season=derived, season_bullpen65=derived starters + bullpen=65, prior=role priors, legacy92=all 92)",
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
            "JSON dict or path to JSON file to override sim_engine.models.GameConfig manager pitching behavior. "
            "Use --manager-pitching-overrides '' to disable."
        ),
    )
    ap.add_argument(
        "--pitcher-distribution-overrides",
        default="",
        help="JSON dict or path to JSON file to override sim_engine.pitcher_distributions.PitcherDistributionConfig fields",
    )
    ap.add_argument(
        "--so-prob-calibration",
        default="data/tuning/so_calibration/default.json",
        help="JSON dict or path to JSON file; applies SO-only p_over calibration at market lines. Format: {\"mode\":\"affine_logit\",\"a\":1.0,\"b\":0.0}",
    )
    ap.add_argument(
        "--outs-prob-calibration",
        default="data/tuning/outs_calibration/default.json",
        help="JSON dict or path to JSON file; applies OUTS-only p_over calibration at market lines (same schema as --so-prob-calibration).",
    )
    ap.add_argument(
        "--hitter-hr-prob-calibration",
        default="data/tuning/hitter_hr_calibration/default.json",
        help="JSON dict or path to JSON file; applies calibration to hitter HR top-N probabilities (supports per-prop wrapper schema).",
    )
    ap.add_argument(
        "--hitter-props-prob-calibration",
        default="data/tuning/hitter_props_calibration/default.json",
        help="JSON dict or path to JSON file; applies calibration to hitter prop top-N probabilities (supports per-prop wrapper schema).",
    )
    raw_argv = list(sys.argv[1:])
    args = ap.parse_args(raw_argv)
    _apply_forward_tuning_defaults(args, raw_argv)

    season = int(args.season) if int(args.season or 0) > 0 else int(args.date.split("-")[0])
    spring_mode = True if str(args.spring_mode) == "on" else False
    stats_season = int(args.stats_season) if int(args.stats_season or 0) > 0 else (int(season) - 1 if spring_mode else int(season))

    snap_root = Path(str(args.daily_snapshots_root))
    if not snap_root.is_absolute():
        snap_root = (_ROOT / snap_root).resolve()
    snap_dir = snap_root / str(args.date)
    roster_obj_dir = snap_dir / "roster_objs"

    injured_by_team: Dict[int, List[int]] = {}
    lineups_by_game_pk: Dict[int, Dict[str, Any]] = {}
    probables_by_game_pk: Dict[int, Dict[str, Any]] = {}
    if str(args.use_daily_snapshots) == "on" and snap_dir.exists():
        injured_by_team = _load_daily_injuries(snap_dir)
        lineups_by_game_pk = _load_daily_lineups(snap_dir)
        probables_by_game_pk = _load_daily_probables(snap_dir)

    last_known_lineups_by_team: Dict[int, List[int]] = {}
    if str(args.lineups_last_known).strip():
        last_known_lineups_by_team = _load_last_known_lineups(str(args.lineups_last_known))

    bvp_hr_on = True if str(args.bvp_hr) == "on" else False
    try:
        eval_date = datetime.fromisoformat(str(args.date)).date()
    except Exception:
        eval_date = datetime.strptime(str(args.date), "%Y-%m-%d").date()
    days_back = max(0, int(args.bvp_days_back))
    bvp_start_date = eval_date - timedelta(days=days_back)
    bvp_min_pa = max(1, int(args.bvp_min_pa))
    bvp_shrink_pa = float(args.bvp_shrink_pa)
    bvp_clamp_lo = float(args.bvp_clamp_lo)
    bvp_clamp_hi = float(args.bvp_clamp_hi)
    bvp_cache = default_bvp_cache()

    client = StatsApiClient.with_default_cache(ttl_seconds=int(args.cache_ttl_hours * 3600))
    games = fetch_schedule_for_date(client, str(args.date))

    pitcher_avail = _pitcher_availability_for_date(client=client, season=season, date_str=str(args.date))

    # Load real market lines (from original repo) once per day.
    market_lines: Dict[str, Dict[str, Dict[str, Any]]] = {}
    market_meta: Dict[str, Any] = {"source": None, "path": None, "pitchers": 0}
    if str(args.prop_lines_source) != "off":
        orig_root = Path(args.original_repo_root) if str(args.original_repo_root).strip() else None
        market_lines, market_meta = load_pitcher_prop_lines(
            str(args.date),
            original_repo_root=orig_root,
            prefer=str(args.prop_lines_source),
        )

    tasks: List[Dict[str, Any]] = []
    skipped = 0

    pitch_model_overrides = _load_jsonish(str(args.pitch_model_overrides))
    market_game_config_overrides = _load_jsonish(str(args.market_game_config_overrides))
    pitcher_distribution_overrides = _load_jsonish(str(args.pitcher_distribution_overrides))
    manager_pitching_overrides = _load_jsonish(str(args.manager_pitching_overrides))
    so_prob_calibration = _load_jsonish(str(args.so_prob_calibration))
    outs_prob_calibration = _load_jsonish(str(args.outs_prob_calibration))
    hitter_hr_prob_calibration = _load_jsonish(str(args.hitter_hr_prob_calibration))
    hitter_props_prob_calibration = _load_jsonish(str(args.hitter_props_prob_calibration))
    pitcher_rate_sampling = True if str(args.pitcher_rate_sampling) == "on" else False
    bip_baserunning = True if str(args.bip_baserunning) == "on" else False
    manager_pitching = str(args.manager_pitching or "v2")
    market_game_lines = _load_game_lines_for_date(str(args.date)) if market_game_config_overrides else {}

    cfg_defaults = GameConfig()
    bip_dp_rate = float(args.bip_dp_rate) if args.bip_dp_rate is not None else float(cfg_defaults.bip_dp_rate)
    bip_sf_rate_flypop = float(args.bip_sf_rate_flypop) if args.bip_sf_rate_flypop is not None else float(cfg_defaults.bip_sf_rate_flypop)
    bip_sf_rate_line = float(args.bip_sf_rate_line) if args.bip_sf_rate_line is not None else float(cfg_defaults.bip_sf_rate_line)
    bip_1b_p2_scores_mult = (
        float(args.bip_1b_p2_scores_mult)
        if args.bip_1b_p2_scores_mult is not None
        else float(getattr(cfg_defaults, "bip_1b_p2_scores_mult", 1.0))
    )
    bip_2b_p1_scores_mult = (
        float(args.bip_2b_p1_scores_mult)
        if args.bip_2b_p1_scores_mult is not None
        else float(getattr(cfg_defaults, "bip_2b_p1_scores_mult", 1.0))
    )
    bip_1b_p1_to_3b_rate = (
        float(args.bip_1b_p1_to_3b_rate)
        if args.bip_1b_p1_to_3b_rate is not None
        else float(getattr(cfg_defaults, "bip_1b_p1_to_3b_rate", 0.24))
    )
    bip_ground_rbi_out_rate = (
        float(args.bip_ground_rbi_out_rate)
        if args.bip_ground_rbi_out_rate is not None
        else float(getattr(cfg_defaults, "bip_ground_rbi_out_rate", 0.18))
    )
    bip_out_2b_to_3b_rate = (
        float(args.bip_out_2b_to_3b_rate)
        if args.bip_out_2b_to_3b_rate is not None
        else float(getattr(cfg_defaults, "bip_out_2b_to_3b_rate", 0.24))
    )
    bip_out_1b_to_2b_rate = (
        float(args.bip_out_1b_to_2b_rate)
        if args.bip_out_1b_to_2b_rate is not None
        else float(getattr(cfg_defaults, "bip_out_1b_to_2b_rate", 0.14))
    )
    bip_misc_advance_pitch_rate = (
        float(args.bip_misc_advance_pitch_rate)
        if args.bip_misc_advance_pitch_rate is not None
        else float(getattr(cfg_defaults, "bip_misc_advance_pitch_rate", 0.004))
    )
    bip_roe_rate = (
        float(args.bip_roe_rate)
        if args.bip_roe_rate is not None
        else float(getattr(cfg_defaults, "bip_roe_rate", 0.012))
    )
    bip_fc_rate = (
        float(args.bip_fc_rate)
        if args.bip_fc_rate is not None
        else float(getattr(cfg_defaults, "bip_fc_rate", 0.04))
    )
    bip_fc_runner_on_3b_score_rate = (
        float(args.bip_fc_runner_on_3b_score_rate)
        if args.bip_fc_runner_on_3b_score_rate is not None
        else float(getattr(cfg_defaults, "bip_fc_runner_on_3b_score_rate", 0.0))
    )

    base_game_cfg: Dict[str, Any] = {
        "pitch_model_overrides": (pitch_model_overrides or {}),
        "bip_baserunning": bool(bip_baserunning),
        "bip_dp_rate": float(bip_dp_rate),
        "bip_sf_rate_flypop": float(bip_sf_rate_flypop),
        "bip_sf_rate_line": float(bip_sf_rate_line),
        "bip_1b_p2_scores_mult": float(bip_1b_p2_scores_mult),
        "bip_2b_p1_scores_mult": float(bip_2b_p1_scores_mult),
        "bip_1b_p1_to_3b_rate": float(bip_1b_p1_to_3b_rate),
        "bip_ground_rbi_out_rate": float(bip_ground_rbi_out_rate),
        "bip_out_2b_to_3b_rate": float(bip_out_2b_to_3b_rate),
        "bip_out_1b_to_2b_rate": float(bip_out_1b_to_2b_rate),
        "bip_misc_advance_pitch_rate": float(bip_misc_advance_pitch_rate),
        "bip_roe_rate": float(bip_roe_rate),
        "bip_fc_rate": float(bip_fc_rate),
        "bip_fc_runner_on_3b_score_rate": float(bip_fc_runner_on_3b_score_rate),
        "pitcher_rate_sampling": bool(pitcher_rate_sampling),
        "pitcher_distribution_overrides": (pitcher_distribution_overrides or {}),
        "manager_pitching": str(manager_pitching),
        "manager_pitching_overrides": (manager_pitching_overrides or {}),
    }

    for g in games:
        game_pk = g.get("gamePk")
        if not game_pk:
            continue
        try:
            game_pk_i = int(game_pk)
        except Exception:
            continue

        game_number = g.get("gameNumber")

        away = ((g.get("teams") or {}).get("away") or {})
        home = ((g.get("teams") or {}).get("home") or {})
        away_team = away.get("team") or {}
        home_team = home.get("team") or {}

        try:
            away_id = int(away_team.get("id"))
            home_id = int(home_team.get("id"))
        except Exception:
            continue

        # Load feed/live (prefer raw)
        feed: Optional[Dict[str, Any]] = None
        if args.use_raw == "on":
            feed = load_feed_live_from_raw(season, str(args.date), game_pk_i)

        if not isinstance(feed, dict) or not feed:
            feed = fetch_game_feed_live(client, game_pk_i)
            if args.write_missing_raw == "on" and isinstance(feed, dict) and feed:
                # best-effort persist
                raw_root = _DATA_DIR / "raw" / "statsapi" / "feed_live"
                out_dir = raw_root / str(int(season)) / str(args.date)
                _ensure_dir(out_dir)
                import gzip

                tmp = out_dir / f"{game_pk_i}.json.gz.tmp"
                final = out_dir / f"{game_pk_i}.json.gz"
                with gzip.open(tmp, "wt", encoding="utf-8") as f:
                    json.dump(feed, f)
                tmp.replace(final)

        if not isinstance(feed, dict) or not feed:
            continue

        actual = _actual_linescore(feed)
        if actual is None:
            skipped += 1
            continue

        actual_team_batting = {
            "away": _actual_team_batting(feed, "away"),
            "home": _actual_team_batting(feed, "home"),
        }
        actual_batter_box = {
            "away": _actual_batter_box_batting(feed, "away"),
            "home": _actual_batter_box_batting(feed, "home"),
        }

        # Context (weather/park/umpire). Use cached API fetch for the same gamePk.
        weather, park, umpire = fetch_game_context(client, int(game_pk_i))
        _apply_umpire_mode(umpire, str(args.umpire_mode))
        if str(args.umpire_mode) == "factors":
            _apply_umpire_shrink(umpire, float(args.umpire_shrink))

        # Confirmed lineups + actual starters from boxscore
        away_lineup_ids = parse_confirmed_lineup_ids(feed, "away")
        home_lineup_ids = parse_confirmed_lineup_ids(feed, "home")
        away_projected_ids: List[int] = []
        home_projected_ids: List[int] = []
        away_lineup_source = "confirmed_feed_live" if away_lineup_ids else ""
        home_lineup_source = "confirmed_feed_live" if home_lineup_ids else ""
        away_lineup_confidence = 1.0 if away_lineup_ids else 0.0
        home_lineup_confidence = 1.0 if home_lineup_ids else 0.0

        snap = lineups_by_game_pk.get(int(game_pk_i))
        if isinstance(snap, dict) and snap:
            snap_away_conf = snap.get("away_confirmed_ids") or []
            snap_home_conf = snap.get("home_confirmed_ids") or []
            if isinstance(snap_away_conf, list) and snap_away_conf:
                away_lineup_ids = [int(x) for x in snap_away_conf]
            if isinstance(snap_home_conf, list) and snap_home_conf:
                home_lineup_ids = [int(x) for x in snap_home_conf]
            snap_away_proj = snap.get("away_projected_ids") or []
            snap_home_proj = snap.get("home_projected_ids") or []
            if isinstance(snap_away_proj, list):
                away_projected_ids = [int(x) for x in snap_away_proj]
            if isinstance(snap_home_proj, list):
                home_projected_ids = [int(x) for x in snap_home_proj]

            away_lineup_source = str(snap.get("away_source") or away_lineup_source)
            home_lineup_source = str(snap.get("home_source") or home_lineup_source)
            try:
                away_lineup_confidence = float(snap.get("away_confidence") if snap.get("away_confidence") is not None else away_lineup_confidence)
            except Exception:
                pass
            try:
                home_lineup_confidence = float(snap.get("home_confidence") if snap.get("home_confidence") is not None else home_lineup_confidence)
            except Exception:
                pass

        if not away_lineup_ids and not away_projected_ids:
            away_projected_ids = last_known_lineups_by_team.get(int(away_id), [])
            if away_projected_ids and not away_lineup_source:
                away_lineup_source = "projected_last_known"
                away_lineup_confidence = 0.4
        if not home_lineup_ids and not home_projected_ids:
            home_projected_ids = last_known_lineups_by_team.get(int(home_id), [])
            if home_projected_ids and not home_lineup_source:
                home_lineup_source = "projected_last_known"
                home_lineup_confidence = 0.4

        away_starter = _get_box_starting_pitcher_id(feed, "away")
        home_starter = _get_box_starting_pitcher_id(feed, "home")

        probable_away_id = None
        probable_home_id = None
        probable_away_source = ""
        probable_home_source = ""
        probable_away_conf = 0.0
        probable_home_conf = 0.0
        psnap = probables_by_game_pk.get(int(game_pk_i))
        if isinstance(psnap, dict) and psnap:
            try:
                probable_away_id = int(psnap.get("away_probable_id") or 0) or None
            except Exception:
                probable_away_id = None
            try:
                probable_home_id = int(psnap.get("home_probable_id") or 0) or None
            except Exception:
                probable_home_id = None
            probable_away_source = str(psnap.get("away_source") or "")
            probable_home_source = str(psnap.get("home_source") or "")
            try:
                probable_away_conf = float(psnap.get("away_confidence") or 0.0)
            except Exception:
                probable_away_conf = 0.0
            try:
                probable_home_conf = float(psnap.get("home_confidence") or 0.0)
            except Exception:
                probable_home_conf = 0.0

        # Fallback: if starter detection fails, use probable starter from snapshots.
        away_pitcher_for_roster = away_starter if away_starter else (probable_away_id if probable_away_id else None)
        home_pitcher_for_roster = home_starter if home_starter else (probable_home_id if probable_home_id else None)

        away_starter_name = _starter_name_from_feed(feed, "away", away_pitcher_for_roster)
        home_starter_name = _starter_name_from_feed(feed, "home", home_pitcher_for_roster)

        t_away = build_team(away_id, away_team.get("name") or "Away", _abbr(away_team))
        t_home = build_team(home_id, home_team.get("name") or "Home", _abbr(home_team))

        fallback_roster_types = ["40Man", "nonRosterInvitees"] if spring_mode else None

        used_roster_artifact = False
        roster_artifact_path: Optional[Path] = None
        roster_artifact_paths: List[Path] = []
        if str(getattr(args, "use_roster_artifacts", "off")) == "on" and roster_obj_dir.exists():
            pat = f"roster_obj_*_{t_away.abbreviation}_at_{t_home.abbreviation}_pk{game_pk_i}*.json"
            matches = sorted(roster_obj_dir.glob(pat))
            if not matches:
                matches = sorted(roster_obj_dir.glob(f"*pk{game_pk_i}*.json"))
            if matches:
                roster_artifact_paths = list(matches)

        def _meta_compatible(meta: Any) -> bool:
            if not isinstance(meta, dict):
                return False
            try:
                if int(meta.get("stats_season") or 0) != int(stats_season):
                    return False
            except Exception:
                return False
            rb = meta.get("roster_builder")
            if not isinstance(rb, dict):
                return False

            expected = {
                "as_of_date": str(args.date),
                "roster_type": "active",
                "fallback_roster_types": (fallback_roster_types if fallback_roster_types else None),
                "exclude_injured": True,
                "enable_batter_vs_pitch_type": (str(args.batter_vs_pitch_type) == "on"),
                "enable_batter_platoon": (str(args.batter_platoon) == "on"),
                "enable_pitcher_platoon": (str(args.pitcher_platoon) == "on"),
                "batter_platoon_alpha": float(args.batter_platoon_alpha),
                "pitcher_platoon_alpha": float(args.pitcher_platoon_alpha),
                "batter_recency_games": int(args.batter_recency_games),
                "batter_recency_weight": float(args.batter_recency_weight),
                "pitcher_recency_games": int(args.pitcher_recency_games),
                "pitcher_recency_weight": float(args.pitcher_recency_weight),
                "away_probable_pitcher_id": (int(away_pitcher_for_roster) if away_pitcher_for_roster else None),
                "home_probable_pitcher_id": (int(home_pitcher_for_roster) if home_pitcher_for_roster else None),
            }

            for k, exp in expected.items():
                if k not in rb:
                    return False
                got = rb.get(k)
                if isinstance(exp, float):
                    try:
                        if abs(float(got) - float(exp)) > 1e-9:
                            return False
                    except Exception:
                        return False
                else:
                    if got != exp:
                        return False
            return True

        if roster_artifact_paths:
            for p in roster_artifact_paths:
                try:
                    rr = read_game_roster_artifact(p)
                    meta = rr.get("meta")
                    if _meta_compatible(meta):
                        away_roster = rr["away"]
                        home_roster = rr["home"]
                        used_roster_artifact = True
                        roster_artifact_path = p
                        print(f"Loaded roster artifact: {p.name}")
                        break
                except KeyboardInterrupt:
                    raise
                except Exception:
                    continue

        if not used_roster_artifact:
            away_roster = build_team_roster(
                client,
                t_away,
                stats_season,
                as_of_date=str(args.date),
                probable_pitcher_id=away_pitcher_for_roster,
                confirmed_lineup_ids=away_lineup_ids,
                projected_lineup_ids=away_projected_ids,
                injured_player_ids=injured_by_team.get(int(away_id), []),
                roster_type="active",
                fallback_roster_types=fallback_roster_types,
                pitcher_availability=pitcher_avail.get(int(away_id), {}),
                enable_batter_vs_pitch_type=(str(args.batter_vs_pitch_type) == "on"),
                enable_batter_platoon=(str(args.batter_platoon) == "on"),
                enable_pitcher_platoon=(str(args.pitcher_platoon) == "on"),
                batter_platoon_alpha=float(args.batter_platoon_alpha),
                pitcher_platoon_alpha=float(args.pitcher_platoon_alpha),
                batter_recency_games=int(args.batter_recency_games),
                batter_recency_weight=float(args.batter_recency_weight),
                pitcher_recency_games=int(args.pitcher_recency_games),
                pitcher_recency_weight=float(args.pitcher_recency_weight),
            )
            home_roster = build_team_roster(
                client,
                t_home,
                stats_season,
                as_of_date=str(args.date),
                probable_pitcher_id=home_pitcher_for_roster,
                confirmed_lineup_ids=home_lineup_ids,
                projected_lineup_ids=home_projected_ids,
                injured_player_ids=injured_by_team.get(int(home_id), []),
                roster_type="active",
                fallback_roster_types=fallback_roster_types,
                pitcher_availability=pitcher_avail.get(int(home_id), {}),
                enable_batter_vs_pitch_type=(str(args.batter_vs_pitch_type) == "on"),
                enable_batter_platoon=(str(args.batter_platoon) == "on"),
                enable_pitcher_platoon=(str(args.pitcher_platoon) == "on"),
                batter_platoon_alpha=float(args.batter_platoon_alpha),
                pitcher_platoon_alpha=float(args.pitcher_platoon_alpha),
                batter_recency_games=int(args.batter_recency_games),
                batter_recency_weight=float(args.batter_recency_weight),
                pitcher_recency_games=int(args.pitcher_recency_games),
                pitcher_recency_weight=float(args.pitcher_recency_weight),
            )

            if str(getattr(args, "write_roster_artifacts", "off")) == "on":
                try:
                    roster_obj_dir.mkdir(parents=True, exist_ok=True)
                    gn = f"_g{int(game_number)}" if isinstance(game_number, (int, float)) else ""
                    out_path = roster_obj_dir / f"roster_obj_eval_{t_away.abbreviation}_at_{t_home.abbreviation}_pk{game_pk_i}{gn}.json"
                    write_game_roster_artifact(
                        out_path,
                        away_roster=away_roster,
                        home_roster=home_roster,
                        meta={
                            "date": str(args.date),
                            "stats_season": int(stats_season),
                            "spring_mode": bool(spring_mode),
                            "game_pk": int(game_pk_i),
                            "away_abbr": str(t_away.abbreviation),
                            "home_abbr": str(t_home.abbreviation),
                            "roster_builder": {
                                "as_of_date": str(args.date),
                                "roster_type": "active",
                                "fallback_roster_types": (fallback_roster_types if fallback_roster_types else None),
                                "exclude_injured": True,
                                "enable_batter_vs_pitch_type": (str(args.batter_vs_pitch_type) == "on"),
                                "enable_batter_platoon": (str(args.batter_platoon) == "on"),
                                "enable_pitcher_platoon": (str(args.pitcher_platoon) == "on"),
                                "batter_platoon_alpha": float(args.batter_platoon_alpha),
                                "pitcher_platoon_alpha": float(args.pitcher_platoon_alpha),
                                "batter_recency_games": int(args.batter_recency_games),
                                "batter_recency_weight": float(args.batter_recency_weight),
                                "pitcher_recency_games": int(args.pitcher_recency_games),
                                "pitcher_recency_weight": float(args.pitcher_recency_weight),
                                "away_probable_pitcher_id": (int(away_pitcher_for_roster) if away_pitcher_for_roster else None),
                                "home_probable_pitcher_id": (int(home_pitcher_for_roster) if home_pitcher_for_roster else None),
                            },
                        },
                    )
                except KeyboardInterrupt:
                    raise
                except Exception:
                    pass

        # Optional ablation: override pitcher stamina after roster construction.
        _apply_stamina_mode(away_roster, str(args.stamina_mode))
        _apply_stamina_mode(home_roster, str(args.stamina_mode))

        if str(getattr(args, "pitch_type_hr", "on")) != "on":
            _neutralize_pitch_type_hr(away_roster)
            _neutralize_pitch_type_hr(home_roster)

        # Optional: apply batter-vs-starter HR multipliers derived from local Statcast raw pitch files.
        if bvp_hr_on:
            try:
                away_pid = int(getattr(getattr(getattr(home_roster, "lineup", None), "pitcher", None), "player", None).mlbam_id or 0)
            except Exception:
                away_pid = 0
            try:
                home_pid = int(getattr(getattr(getattr(away_roster, "lineup", None), "pitcher", None), "player", None).mlbam_id or 0)
            except Exception:
                home_pid = 0

            if away_pid > 0:
                try:
                    apply_starter_bvp_hr_multipliers(
                        batting_roster=away_roster,
                        pitcher_id=away_pid,
                        season=season,
                        start_date=bvp_start_date,
                        end_date=eval_date,
                        cache=bvp_cache,
                        min_pa=bvp_min_pa,
                        shrink_pa=bvp_shrink_pa,
                        clamp_lo=bvp_clamp_lo,
                        clamp_hi=bvp_clamp_hi,
                    )
                except Exception:
                    pass

            if home_pid > 0:
                try:
                    apply_starter_bvp_hr_multipliers(
                        batting_roster=home_roster,
                        pitcher_id=home_pid,
                        season=season,
                        start_date=bvp_start_date,
                        end_date=eval_date,
                        cache=bvp_cache,
                        min_pa=bvp_min_pa,
                        shrink_pa=bvp_shrink_pa,
                        clamp_lo=bvp_clamp_lo,
                        clamp_hi=bvp_clamp_hi,
                    )
                except Exception:
                    pass

        market_game = market_game_lines.get((str(t_away.name), str(t_home.name))) if market_game_lines else None
        market_context = _market_context_from_game_line(market_game)
        effective_game_cfg, market_override_meta = _apply_market_game_config_overrides(
            base_cfg=base_game_cfg,
            selector=market_game_config_overrides,
            market_context=market_context,
        )

        tasks.append(
            {
                "game_pk": int(game_pk_i),
                "away": {"id": int(away_id), "abbr": t_away.abbreviation, "name": t_away.name},
                "home": {"id": int(home_id), "abbr": t_home.abbreviation, "name": t_home.name},
                "starters": {"away": away_starter, "home": home_starter},
                "roster_starters": {
                    "away": int(getattr(getattr(away_roster, "lineup", None), "pitcher").player.mlbam_id) if away_roster else None,
                    "home": int(getattr(getattr(home_roster, "lineup", None), "pitcher").player.mlbam_id) if home_roster else None,
                    "away_source": str(getattr(getattr(getattr(away_roster, "lineup", None), "pitcher", None), "starter_selection_source", "") or ""),
                    "home_source": str(getattr(getattr(getattr(home_roster, "lineup", None), "pitcher", None), "starter_selection_source", "") or ""),
                    "away_requested_id": getattr(getattr(getattr(away_roster, "lineup", None), "pitcher", None), "starter_requested_id", None),
                    "home_requested_id": getattr(getattr(getattr(home_roster, "lineup", None), "pitcher", None), "starter_requested_id", None),
                },
                "probable_starters": {
                    "away": probable_away_id,
                    "home": probable_home_id,
                    "away_source": probable_away_source,
                    "home_source": probable_home_source,
                    "away_confidence": float(probable_away_conf),
                    "home_confidence": float(probable_home_conf),
                },
                "starter_names": {"away": away_starter_name, "home": home_starter_name},
                "confirmed_lineup_ids": {"away": away_lineup_ids, "home": home_lineup_ids},
                "projected_lineup_ids": {"away": away_projected_ids, "home": home_projected_ids},
                "lineup_source": {"away": away_lineup_source, "home": home_lineup_source},
                "lineup_confidence": {"away": away_lineup_confidence, "home": home_lineup_confidence},
                "actual": actual,
                "actual_team_batting": actual_team_batting,
                "actual_batter_box": actual_batter_box,
                "pitch_model_overrides": effective_game_cfg.get("pitch_model_overrides") or {},
                "bip_baserunning": bool(effective_game_cfg.get("bip_baserunning", bip_baserunning)),
                "bip_dp_rate": float(effective_game_cfg.get("bip_dp_rate", bip_dp_rate)),
                "bip_sf_rate_flypop": float(effective_game_cfg.get("bip_sf_rate_flypop", bip_sf_rate_flypop)),
                "bip_sf_rate_line": float(effective_game_cfg.get("bip_sf_rate_line", bip_sf_rate_line)),
                "bip_1b_p2_scores_mult": float(effective_game_cfg.get("bip_1b_p2_scores_mult", bip_1b_p2_scores_mult)),
                "bip_2b_p1_scores_mult": float(effective_game_cfg.get("bip_2b_p1_scores_mult", bip_2b_p1_scores_mult)),
                "bip_1b_p1_to_3b_rate": float(effective_game_cfg.get("bip_1b_p1_to_3b_rate", bip_1b_p1_to_3b_rate)),
                "bip_ground_rbi_out_rate": float(effective_game_cfg.get("bip_ground_rbi_out_rate", bip_ground_rbi_out_rate)),
                "bip_out_2b_to_3b_rate": float(effective_game_cfg.get("bip_out_2b_to_3b_rate", bip_out_2b_to_3b_rate)),
                "bip_out_1b_to_2b_rate": float(effective_game_cfg.get("bip_out_1b_to_2b_rate", bip_out_1b_to_2b_rate)),
                "bip_misc_advance_pitch_rate": float(effective_game_cfg.get("bip_misc_advance_pitch_rate", bip_misc_advance_pitch_rate)),
                "bip_roe_rate": float(effective_game_cfg.get("bip_roe_rate", bip_roe_rate)),
                "bip_fc_rate": float(effective_game_cfg.get("bip_fc_rate", bip_fc_rate)),
                "bip_fc_runner_on_3b_score_rate": float(effective_game_cfg.get("bip_fc_runner_on_3b_score_rate", bip_fc_runner_on_3b_score_rate)),
                "pitcher_rate_sampling": bool(effective_game_cfg.get("pitcher_rate_sampling", pitcher_rate_sampling)),
                "stamina_mode": str(args.stamina_mode),
                "umpire_mode": str(args.umpire_mode),
                "umpire_shrink": float(args.umpire_shrink),
                "pitcher_distribution_overrides": effective_game_cfg.get("pitcher_distribution_overrides") or {},
                "manager_pitching": str(effective_game_cfg.get("manager_pitching", manager_pitching)),
                "manager_pitching_overrides": effective_game_cfg.get("manager_pitching_overrides") or {},
                "market_game_context": market_context,
                "market_game_config_overrides": market_override_meta,
                "actual_starters": {
                    "away": _parse_actual_starter_pitching(feed, "away", away_starter),
                    "home": _parse_actual_starter_pitching(feed, "home", home_starter),
                },
                "weather": weather,
                "park": park,
                "umpire": umpire,
                "weather_hr_weight": float(args.weather_hr_weight),
                "weather_inplay_hit_weight": float(args.weather_inplay_hit_weight),
                "weather_xb_share_weight": float(args.weather_xb_share_weight),
                "park_hr_weight": float(args.park_hr_weight),
                "park_inplay_hit_weight": float(args.park_inplay_hit_weight),
                "park_xb_share_weight": float(args.park_xb_share_weight),
                "away_roster": away_roster,
                "home_roster": home_roster,
                "sims_per_game": int(args.sims_per_game),
                "hitter_hr_top_n": int(args.hitter_hr_topn),
                "hitter_props_top_n": int(args.hitter_props_topn),
                "seed": int(args.seed) + int(game_pk_i) % 100000,
                "pitcher_prop_ids": [int(x) for x in (away_starter, home_starter) if x],
            }
        )

    # Run sims (optionally parallel per game)
    jobs = max(1, int(args.jobs or 1))
    sim_outputs: List[Dict[str, Any]] = []
    if jobs == 1:
        for t in tasks:
            sim_outputs.append(_simulate_one_game_task(t))
    else:
        # Note: on Windows this uses spawn; keep tasks limited to per-game granularity.
        with ProcessPoolExecutor(max_workers=jobs) as ex:
            futs = [ex.submit(_simulate_one_game_task, t) for t in tasks]
            for f in as_completed(futs):
                sim_outputs.append(f.result())

    # Build per-game evaluation results
    results: List[Dict[str, Any]] = []
    by_pk = {int(o["task"]["game_pk"]): o for o in sim_outputs}
    for t in tasks:
        game_pk_i = int(t["game_pk"])
        o = by_pk.get(game_pk_i)
        if not o:
            continue
        sims = o["sims"]
        actual = t["actual"]
        pitcher_props = sims.get("pitcher_props") or {}
        team_batting = sims.get("team_batting") or {}
        hitter_hr_likelihood = _preferred_hitter_hr_likelihood(sims)
        hitter_props_likelihood = sims.get("hitter_props_likelihood")

        actual_team_batting = t.get("actual_team_batting") or {}
        actual_batter_box = t.get("actual_batter_box") or {}

        # Attach market lines to each starter, if available.
        market_for_game: Dict[str, Any] = {"away": None, "home": None}
        for side in ("away", "home"):
            raw_name = ((t.get("starter_names") or {}).get(side) or "")
            nk = normalize_pitcher_name(str(raw_name))
            if nk and nk in market_lines:
                market_for_game[side] = {"name_key": nk, **(market_lines.get(nk) or {})}

        per_seg: Dict[str, Any] = {}
        for seg_name in ("full", "first5", "first3"):
            seg = (sims.get("segments") or {}).get(seg_name) or {}

            p_home = float(seg.get("home_win_prob") or 0.0)
            if not (0.0 <= p_home <= 1.0):
                p_home = float(min(1.0, max(0.0, p_home)))

            act = actual.get(seg_name) or {}
            a_away = int(act.get("away") or 0)
            a_home = int(act.get("home") or 0)

            y = 1 if a_home > a_away else 0

            mean_total = _mean_from_dist(seg.get("total_runs_dist") or {})
            mean_margin = _mean_from_dist(seg.get("run_margin_dist") or {})

            extra_metrics: Dict[str, Any] = {}
            if seg_name == "full":
                sims_n = int(sims.get("sims") or 0)
                a_tot = int(a_away + a_home)
                dist_tot = seg.get("total_runs_dist") or {}
                p_exact_total = _prob_from_dist(dist_tot, a_tot, denom=sims_n)
                p_exact_total = float(max(1.0 / float(max(1, sims_n * 1000)), p_exact_total))
                extra_metrics["nll_exact_total"] = float(-math.log(p_exact_total))

                # ATS runline: choose favorite by ML prob; evaluate favorite -1.5 cover.
                dist_margin = seg.get("run_margin_dist") or {}
                fav_is_home = bool(p_home >= 0.5)
                if fav_is_home:
                    p_cover = _prob_margin_ge(dist_margin, threshold=2, denom=sims_n)
                    y_cover = 1 if (a_home - a_away) >= 2 else 0
                else:
                    p_cover = _prob_margin_le(dist_margin, threshold=-2, denom=sims_n)
                    y_cover = 1 if (a_home - a_away) <= -2 else 0
                extra_metrics["fav_is_home"] = bool(fav_is_home)
                extra_metrics["p_fav_cover_minus_1_5"] = float(p_cover)
                extra_metrics["brier_fav_cover_minus_1_5"] = _brier(float(p_cover), int(y_cover))
                extra_metrics["logloss_fav_cover_minus_1_5"] = _logloss(float(p_cover), int(y_cover))
                extra_metrics["fav_cover_minus_1_5_actual"] = int(y_cover)

            per_seg[seg_name] = {
                "home_win_prob": p_home,
                "away_win_prob": float(seg.get("away_win_prob") or 0.0),
                "tie_prob": float(seg.get("tie_prob") or 0.0),
                "mean_total_runs": mean_total,
                "mean_run_margin_home_minus_away": mean_margin,
                "actual": {"away": a_away, "home": a_home},
                "metrics": {
                    "brier_home_win": _brier(p_home, y),
                    "logloss_home_win": _logloss(p_home, y),
                    "abs_err_total_runs": None if mean_total is None else float(abs(float(mean_total) - float(a_away + a_home))),
                    "outs_prob_calibration": (outs_prob_calibration or {}),
                    "abs_err_run_margin": None if mean_margin is None else float(abs(float(mean_margin) - float(a_home - a_away))),
                    **extra_metrics,
                },
            }

        row: Dict[str, Any] = {
                "game_pk": int(game_pk_i),
                "away": t["away"],
                "home": t["home"],
                "starters": t["starters"],
                "starter_names": t.get("starter_names") or {},
            "probable_starters": t.get("probable_starters") or {},
                "roster_starters": t.get("roster_starters") or {},
                "confirmed_lineup_ids": t["confirmed_lineup_ids"],
                "projected_lineup_ids": t.get("projected_lineup_ids") or {},
                "lineup_source": t.get("lineup_source") or {},
                "lineup_confidence": t.get("lineup_confidence") or {},
                "market_game_context": t.get("market_game_context") or {},
                "market_game_config_overrides": t.get("market_game_config_overrides") or {},
                "segments": per_seg,
                "team_batting": {
                    "away": {
                        "actual": actual_team_batting.get("away"),
                        "pred": team_batting.get("away"),
                    },
                    "home": {
                        "actual": actual_team_batting.get("home"),
                        "pred": team_batting.get("home"),
                    },
                },
                "pitcher_props": {
                    "away": {
                        "starter_id": t["starters"].get("away"),
                        "actual": (t.get("actual_starters") or {}).get("away"),
                        "pred": pitcher_props.get(str(t["starters"].get("away"))) if t["starters"].get("away") else None,
                        "market": market_for_game.get("away"),
                    },
                    "home": {
                        "starter_id": t["starters"].get("home"),
                        "actual": (t.get("actual_starters") or {}).get("home"),
                        "pred": pitcher_props.get(str(t["starters"].get("home"))) if t["starters"].get("home") else None,
                        "market": market_for_game.get("home"),
                    },
                },
            }

        if isinstance(hitter_hr_likelihood, dict) and hitter_hr_likelihood:
            # Include calibrated fields for downstream consumption (ranking unchanged for monotonic modes).
            try:
                overall0 = hitter_hr_likelihood.get("overall") or []
                if isinstance(overall0, list):
                    for rr in overall0:
                        if not isinstance(rr, dict):
                            continue
                        try:
                            p0 = float(rr.get("p_hr_1plus") or 0.0)
                        except Exception:
                            p0 = 0.0
                        rr["p_hr_1plus_cal"] = float(
                            apply_prop_prob_calibration(float(p0), hitter_hr_prob_calibration, prop_key="hr_1plus")
                        )
            except Exception:
                pass

            row["hitter_hr_likelihood"] = hitter_hr_likelihood

            scored: List[Dict[str, Any]] = []
            briers: List[float] = []
            loglosses: List[float] = []
            ps: List[float] = []
            ys: List[int] = []

            away_box = actual_batter_box.get("away") if isinstance(actual_batter_box, dict) else None
            home_box = actual_batter_box.get("home") if isinstance(actual_batter_box, dict) else None
            away_box = away_box if isinstance(away_box, dict) else {}
            home_box = home_box if isinstance(home_box, dict) else {}

            overall = hitter_hr_likelihood.get("overall") or []
            if isinstance(overall, list):
                for r in overall:
                    if not isinstance(r, dict):
                        continue
                    try:
                        pid = int(r.get("batter_id") or 0)
                    except Exception:
                        pid = 0
                    if pid <= 0:
                        continue
                    try:
                        p = float(r.get("p_hr_1plus") or 0.0)
                    except Exception:
                        p = 0.0
                    p_cal = apply_prop_prob_calibration(float(p), hitter_hr_prob_calibration, prop_key="hr_1plus")
                    hr_act = 0
                    if pid in away_box:
                        hr_act = int((away_box.get(pid) or {}).get("HR") or 0)
                    elif pid in home_box:
                        hr_act = int((home_box.get(pid) or {}).get("HR") or 0)
                    y = 1 if int(hr_act) >= 1 else 0
                    scored.append(
                        {
                            "batter_id": int(pid),
                            "name": str(r.get("name") or ""),
                            "p_hr_1plus": float(p),
                            "p_hr_1plus_cal": float(p_cal),
                            "actual_hr": int(hr_act),
                            "y_hr_1plus": int(y),
                            "hr_mean": r.get("hr_mean"),
                            "ab_mean": r.get("ab_mean"),
                        }
                    )
                    briers.append(_brier(float(p_cal), int(y)))
                    loglosses.append(_logloss(float(p_cal), int(y)))
                    ps.append(float(p_cal))
                    ys.append(int(y))

            row["hitter_hr_backtest"] = {
                "n": int(len(scored)),
                "brier": (sum(briers) / len(briers)) if briers else None,
                "logloss": (sum(loglosses) / len(loglosses)) if loglosses else None,
                "avg_p": (sum(ps) / len(ps)) if ps else None,
                "emp_rate": (sum(float(y) for y in ys) / float(len(ys))) if ys else None,
                "scored_overall": scored,
            }

        if isinstance(hitter_props_likelihood, dict) and hitter_props_likelihood:
            # Include calibrated fields for downstream consumption (ranking unchanged for monotonic modes).
            try:
                for prop_key, p_key, _actual_key, _mean_field, _threshold in _HITTER_PROP_SPECS:
                    rows0 = hitter_props_likelihood.get(prop_key) or []
                    if not isinstance(rows0, list):
                        continue
                    for rr in rows0:
                        if not isinstance(rr, dict):
                            continue
                        try:
                            p0 = float(rr.get(p_key) or 0.0)
                        except Exception:
                            p0 = 0.0
                        rr[p_key + "_cal"] = float(
                            apply_prop_prob_calibration(float(p0), hitter_props_prob_calibration, prop_key=prop_key)
                        )
            except Exception:
                pass

            row["hitter_props_likelihood"] = hitter_props_likelihood

            away_box = actual_batter_box.get("away") if isinstance(actual_batter_box, dict) else None
            home_box = actual_batter_box.get("home") if isinstance(actual_batter_box, dict) else None
            away_box = away_box if isinstance(away_box, dict) else {}
            home_box = home_box if isinstance(home_box, dict) else {}

            def _actual_stat(pid: int, key: str) -> int:
                if pid in away_box:
                    try:
                        if str(key) == "TB":
                            row = away_box.get(pid) or {}
                            hits = int(row.get("H") or 0)
                            doubles = int(row.get("2B") or 0)
                            triples = int(row.get("3B") or 0)
                            home_runs = int(row.get("HR") or 0)
                            return int(hits + doubles + 2 * triples + 3 * home_runs)
                        return int((away_box.get(pid) or {}).get(key) or 0)
                    except Exception:
                        return 0
                if pid in home_box:
                    try:
                        if str(key) == "TB":
                            row = home_box.get(pid) or {}
                            hits = int(row.get("H") or 0)
                            doubles = int(row.get("2B") or 0)
                            triples = int(row.get("3B") or 0)
                            home_runs = int(row.get("HR") or 0)
                            return int(hits + doubles + 2 * triples + 3 * home_runs)
                        return int((home_box.get(pid) or {}).get(key) or 0)
                    except Exception:
                        return 0
                return 0

            def _score_list(rows: Any, p_key: str, y_fn, prop_key: str, actual_key: str) -> Dict[str, Any]:
                scored: List[Dict[str, Any]] = []
                briers: List[float] = []
                loglosses: List[float] = []
                ps: List[float] = []
                ys: List[int] = []
                if isinstance(rows, list):
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        try:
                            pid = int(r.get("batter_id") or 0)
                        except Exception:
                            pid = 0
                        if pid <= 0:
                            continue
                        try:
                            p = float(r.get(p_key) or 0.0)
                        except Exception:
                            p = 0.0
                        actual_val = int(_actual_stat(int(pid), str(actual_key)))
                        y = 1 if bool(y_fn(int(pid))) else 0
                        p_cal = apply_prop_prob_calibration(float(p), hitter_props_prob_calibration, prop_key=str(prop_key))
                        scored.append(
                            {
                                "batter_id": int(pid),
                                "name": str(r.get("name") or ""),
                                "p": float(p),
                                "p_cal": float(p_cal),
                                "actual": int(actual_val),
                                "y": int(y),
                            }
                        )
                        briers.append(_brier(float(p_cal), int(y)))
                        loglosses.append(_logloss(float(p_cal), int(y)))
                        ps.append(float(p_cal))
                        ys.append(int(y))

                return {
                    "n": int(len(scored)),
                    "brier": (sum(briers) / len(briers)) if briers else None,
                    "logloss": (sum(loglosses) / len(loglosses)) if loglosses else None,
                    "avg_p": (sum(ps) / len(ps)) if ps else None,
                    "emp_rate": (sum(float(y) for y in ys) / float(len(ys))) if ys else None,
                    "scored": scored,
                }

            hitter_props_backtest: Dict[str, Any] = {}
            for prop_key, p_key, actual_key, _mean_field, threshold in _HITTER_PROP_SPECS:
                hitter_props_backtest[prop_key] = _score_list(
                    hitter_props_likelihood.get(prop_key) or [],
                    p_key,
                    lambda pid, actual_key=actual_key, threshold=threshold: _actual_stat(pid, actual_key) >= threshold,
                    prop_key,
                    actual_key,
                )
            row["hitter_props_backtest"] = hitter_props_backtest

        results.append(row)

    # Aggregate metrics
    def agg(seg: str) -> Dict[str, Any]:
        briers: List[float] = []
        mae_total: List[float] = []
        mae_margin: List[float] = []
        for g in results:
            s = (g.get("segments") or {}).get(seg) or {}
            m = (s.get("metrics") or {})
            b = m.get("brier_home_win")
            if isinstance(b, (int, float)):
                briers.append(float(b))
            at = m.get("abs_err_total_runs")
            if isinstance(at, (int, float)):
                mae_total.append(float(at))
            am = m.get("abs_err_run_margin")
            if isinstance(am, (int, float)):
                mae_margin.append(float(am))
        return {
            "games": int(len(results)),
            "brier_home_win": (sum(briers) / len(briers)) if briers else None,
            "mae_total_runs": (sum(mae_total) / len(mae_total)) if mae_total else None,
            "mae_run_margin": (sum(mae_margin) / len(mae_margin)) if mae_margin else None,
        }

    # Compute effective pitch-model config for reproducible reporting.
    # This should match the override filtering behavior in the simulator.
    try:
        allowed_pm = set(getattr(PitchModelConfig, "__dataclass_fields__", {}).keys())
        pmo = pitch_model_overrides or {}
        pmo_safe = {k: v for k, v in pmo.items() if k in allowed_pm}
        pitch_cfg = PitchModelConfig(**pmo_safe) if pmo_safe else PitchModelConfig()
    except Exception:
        pitch_cfg = PitchModelConfig()

    report = {
        "meta": {
            "date": str(args.date),
            "season": int(season),
            "sims_per_game": int(args.sims_per_game),
            "hitter_hr_top_n": int(args.hitter_hr_topn),
            "hitter_props_top_n": int(args.hitter_props_topn),
            "seed": int(args.seed),
            "generated_at": datetime.now().isoformat(),
            "use_raw": str(args.use_raw),
            "prop_lines_source": str(args.prop_lines_source),
            "market_push_policy": str(args.market_push_policy),
            "jobs": int(jobs),
            "skipped_games": int(skipped),
            "pitch_model_config_name": str(getattr(pitch_cfg, "name", "")),
            "pitch_model_batter_pt_alpha": float(getattr(pitch_cfg, "batter_pt_alpha", 0.5)),
            "pitch_model_batter_pt_scale": float(getattr(pitch_cfg, "batter_pt_scale", 1.0)),
            "pitch_model_overrides": (pitch_model_overrides or {}),
            "market_game_config_overrides": (market_game_config_overrides or {}),
            "pitcher_distribution_overrides": (pitcher_distribution_overrides or {}),
            "bip_baserunning": bool(bip_baserunning),
            "batter_vs_pitch_type": (str(args.batter_vs_pitch_type) == "on"),
            "pitch_type_hr": (str(args.pitch_type_hr) == "on"),
            "batter_platoon": (str(args.batter_platoon) == "on"),
            "pitcher_platoon": (str(args.pitcher_platoon) == "on"),
            "batter_platoon_alpha": float(args.batter_platoon_alpha),
            "pitcher_platoon_alpha": float(args.pitcher_platoon_alpha),
            "batter_recency_games": int(args.batter_recency_games),
            "batter_recency_weight": float(args.batter_recency_weight),
            "pitcher_recency_games": int(args.pitcher_recency_games),
            "pitcher_recency_weight": float(args.pitcher_recency_weight),
            "weather_hr_weight": float(args.weather_hr_weight),
            "weather_inplay_hit_weight": float(args.weather_inplay_hit_weight),
            "weather_xb_share_weight": float(args.weather_xb_share_weight),
            "park_hr_weight": float(args.park_hr_weight),
            "park_inplay_hit_weight": float(args.park_inplay_hit_weight),
            "park_xb_share_weight": float(args.park_xb_share_weight),
            "bip_dp_rate": float(bip_dp_rate),
            "bip_sf_rate_flypop": float(bip_sf_rate_flypop),
            "bip_sf_rate_line": float(bip_sf_rate_line),
            "bip_1b_p2_scores_mult": float(bip_1b_p2_scores_mult),
            "bip_2b_p1_scores_mult": float(bip_2b_p1_scores_mult),
            "bip_1b_p1_to_3b_rate": float(bip_1b_p1_to_3b_rate),
            "bip_ground_rbi_out_rate": float(bip_ground_rbi_out_rate),
            "bip_out_2b_to_3b_rate": float(bip_out_2b_to_3b_rate),
            "bip_out_1b_to_2b_rate": float(bip_out_1b_to_2b_rate),
            "bip_misc_advance_pitch_rate": float(bip_misc_advance_pitch_rate),
            "bip_roe_rate": float(bip_roe_rate),
            "bip_fc_rate": float(bip_fc_rate),
            "bip_fc_runner_on_3b_score_rate": float(bip_fc_runner_on_3b_score_rate),
            "pitcher_rate_sampling": bool(pitcher_rate_sampling),
            "stamina_mode": str(args.stamina_mode),
            "umpire_mode": str(args.umpire_mode),
            "umpire_shrink": float(args.umpire_shrink),
            "manager_pitching": str(manager_pitching),
            "manager_pitching_overrides": (manager_pitching_overrides or {}),
            "so_prob_calibration": (so_prob_calibration or {}),
            "outs_prob_calibration": (outs_prob_calibration or {}),
            "hitter_hr_prob_calibration": (hitter_hr_prob_calibration or {}),
            "hitter_props_prob_calibration": (hitter_props_prob_calibration or {}),
        },
        "assessment": {},
        "aggregate": {
            "full": agg("full"),
            "first5": agg("first5"),
            "first3": agg("first3"),
        },
        "games": results,
    }

    # Day-level assessment
    sims_n = int(report["meta"]["sims_per_game"])
    totals_abs: List[float] = []
    totals_err: List[float] = []
    totals_nll: List[float] = []
    ml_brier: List[float] = []
    ml_logloss: List[float] = []
    ml_acc: List[float] = []
    ats_brier: List[float] = []
    ats_logloss: List[float] = []
    ats_acc: List[float] = []
    so_abs: List[float] = []
    so_err: List[float] = []
    outs_abs: List[float] = []
    outs_err: List[float] = []
    pitches_abs: List[float] = []
    pitches_err: List[float] = []

    # Hitter HR likelihood scoring (top-N only)
    hr_brier: List[float] = []
    hr_logloss: List[float] = []
    hr_ps: List[float] = []
    hr_ys: List[int] = []

    hitter_prop_rollup: Dict[str, Dict[str, List[float]]] = {
        prop_key: {"brier": [], "logloss": [], "p": [], "y": []}
        for prop_key, _p_key, _actual_key, _mean_field, _threshold in _HITTER_PROP_SPECS
    }

    # Pitcher props at market lines (O/U scoring)
    so_line_brier: List[float] = []
    so_line_logloss: List[float] = []
    so_line_acc: List[float] = []
    outs_line_brier: List[float] = []
    outs_line_logloss: List[float] = []
    outs_line_acc: List[float] = []
    so_line_edge: List[float] = []
    outs_line_edge: List[float] = []

    so_line_pushes = 0
    outs_line_pushes = 0

    for g in results:
        full = (g.get("segments") or {}).get("full") or {}
        m = (full.get("metrics") or {})
        if isinstance(m.get("abs_err_total_runs"), (int, float)):
            totals_abs.append(float(m["abs_err_total_runs"]))
        if isinstance(full.get("mean_total_runs"), (int, float)):
            act = (full.get("actual") or {})
            a_tot = float(int(act.get("away") or 0) + int(act.get("home") or 0))
            totals_err.append(float(full["mean_total_runs"]) - a_tot)
        if isinstance(m.get("nll_exact_total"), (int, float)):
            totals_nll.append(float(m["nll_exact_total"]))

        if isinstance(m.get("brier_home_win"), (int, float)):
            ml_brier.append(float(m["brier_home_win"]))
        if isinstance(m.get("logloss_home_win"), (int, float)):
            ml_logloss.append(float(m["logloss_home_win"]))
        try:
            p_home = float(full.get("home_win_prob") or 0.0)
            act = (full.get("actual") or {})
            y = 1 if int(act.get("home") or 0) > int(act.get("away") or 0) else 0
            ml_acc.append(1.0 if ((p_home >= 0.5) == (y == 1)) else 0.0)
        except Exception:
            pass

        if isinstance(m.get("brier_fav_cover_minus_1_5"), (int, float)):
            ats_brier.append(float(m["brier_fav_cover_minus_1_5"]))
        if isinstance(m.get("logloss_fav_cover_minus_1_5"), (int, float)):
            ats_logloss.append(float(m["logloss_fav_cover_minus_1_5"]))
        try:
            p_cover = float(m.get("p_fav_cover_minus_1_5") or 0.0)
            y_cover = int(m.get("fav_cover_minus_1_5_actual") or 0)
            ats_acc.append(1.0 if ((p_cover >= 0.5) == (y_cover == 1)) else 0.0)
        except Exception:
            pass

        # Pitcher props: starters
        for side in ("away", "home"):
            pp = ((g.get("pitcher_props") or {}).get(side) or {})
            actp = pp.get("actual") or {}
            pred = pp.get("pred") or {}
            market = pp.get("market") or {}
            if not actp or not pred:
                continue
            try:
                a_so = int(actp.get("so"))
                a_outs = int(actp.get("outs"))
            except Exception:
                continue
            if isinstance(pred.get("so_mean"), (int, float)):
                e = float(pred["so_mean"]) - float(a_so)
                so_abs.append(abs(e))
                so_err.append(e)
            if isinstance(pred.get("outs_mean"), (int, float)):
                e = float(pred["outs_mean"]) - float(a_outs)
                outs_abs.append(abs(e))
                outs_err.append(e)

            # Pitches thrown (best-effort; actual may be missing for some feeds)
            try:
                a_pitches = actp.get("pitches")
                a_pitches_i = int(a_pitches) if a_pitches is not None else None
            except Exception:
                a_pitches_i = None
            if a_pitches_i is not None and isinstance(pred.get("pitches_mean"), (int, float)):
                e = float(pred["pitches_mean"]) - float(a_pitches_i)
                pitches_abs.append(abs(e))
                pitches_err.append(e)

            # Real lines O/U scoring (if lines exist)
            try:
                so_dist = pred.get("so_dist") or {}
                outs_dist = pred.get("outs_dist") or {}
            except Exception:
                so_dist = {}
                outs_dist = {}

            mk_so = (market.get("strikeouts") or {}) if isinstance(market, dict) else {}
            mk_outs = (market.get("outs") or {}) if isinstance(market, dict) else {}

            so_line = mk_so.get("line")
            if so_line is not None:
                p_over = _prob_over_line_from_dist(so_dist, float(so_line))
                if p_over is not None:
                    p_over = apply_prob_calibration(float(p_over), so_prob_calibration)
                    is_push = abs(float(a_so) - float(so_line)) < 1e-9
                    if is_push:
                        so_line_pushes += 1
                        if str(args.market_push_policy) == "skip":
                            pass
                        else:
                            y_over = 0.5 if str(args.market_push_policy) == "half" else 0.0
                            so_line_brier.append(_brier(float(p_over), float(y_over)))
                            so_line_logloss.append(_logloss(float(p_over), float(y_over)))
                    else:
                        y_over = 1.0 if float(a_so) > float(so_line) else 0.0
                        so_line_brier.append(_brier(float(p_over), float(y_over)))
                        so_line_logloss.append(_logloss(float(p_over), float(y_over)))
                        so_line_acc.append(1.0 if ((float(p_over) >= 0.5) == (float(y_over) >= 0.5)) else 0.0)
                    p_imp = no_vig_over_prob(mk_so.get("over_odds"), mk_so.get("under_odds"))
                    if p_imp is not None:
                        so_line_edge.append(float(p_over) - float(p_imp))

            outs_line = mk_outs.get("line")
            if outs_line is not None:
                p_over = _prob_over_line_from_dist(outs_dist, float(outs_line))
                if p_over is not None:
                    p_over = apply_prob_calibration(float(p_over), outs_prob_calibration)
                    is_push = abs(float(a_outs) - float(outs_line)) < 1e-9
                    if is_push:
                        outs_line_pushes += 1
                        if str(args.market_push_policy) == "skip":
                            pass
                        else:
                            y_over = 0.5 if str(args.market_push_policy) == "half" else 0.0
                            outs_line_brier.append(_brier(float(p_over), float(y_over)))
                            outs_line_logloss.append(_logloss(float(p_over), float(y_over)))
                    else:
                        y_over = 1.0 if float(a_outs) > float(outs_line) else 0.0
                        outs_line_brier.append(_brier(float(p_over), float(y_over)))
                        outs_line_logloss.append(_logloss(float(p_over), float(y_over)))
                        outs_line_acc.append(1.0 if ((float(p_over) >= 0.5) == (float(y_over) >= 0.5)) else 0.0)
                    p_imp = no_vig_over_prob(mk_outs.get("over_odds"), mk_outs.get("under_odds"))
                    if p_imp is not None:
                        outs_line_edge.append(float(p_over) - float(p_imp))

        # Hitter HR likelihood scoring (top-N overall list)
        hb = g.get("hitter_hr_backtest") or {}
        if isinstance(hb, dict):
            try:
                b = hb.get("brier")
                ll = hb.get("logloss")
                n = int(hb.get("n") or 0)
            except Exception:
                b = None
                ll = None
                n = 0
            if isinstance(b, (int, float)) and n > 0:
                # If per-game average was computed, re-aggregate via underlying rows if present.
                scored = hb.get("scored_overall") or []
                if isinstance(scored, list) and scored:
                    for r in scored:
                        if not isinstance(r, dict):
                            continue
                        try:
                            p = float(r.get("p_hr_1plus_cal") or r.get("p_hr_1plus") or 0.0)
                            y = int(r.get("y_hr_1plus") or 0)
                        except Exception:
                            continue
                        hr_brier.append(_brier(float(p), int(y)))
                        hr_logloss.append(_logloss(float(p), int(y)))
                        hr_ps.append(float(p))
                        hr_ys.append(int(y))

        # Hitter props scoring
        hp = g.get("hitter_props_backtest") or {}
        if isinstance(hp, dict) and hp:
            for k in list(hitter_prop_rollup.keys()):
                sub = hp.get(k) or {}
                if not isinstance(sub, dict):
                    continue
                scored = sub.get("scored") or []
                if not isinstance(scored, list) or not scored:
                    continue
                for r in scored:
                    if not isinstance(r, dict):
                        continue
                    try:
                        p = float(r.get("p_cal") or r.get("p") or 0.0)
                        y = int(r.get("y") or 0)
                    except Exception:
                        continue
                    hitter_prop_rollup[k]["brier"].append(_brier(float(p), int(y)))
                    hitter_prop_rollup[k]["logloss"].append(_logloss(float(p), int(y)))
                    hitter_prop_rollup[k]["p"].append(float(p))
                    hitter_prop_rollup[k]["y"].append(int(y))

    report["assessment"] = {
        "full_game": {
            "totals": {
                "games": int(len(results)),
                "sims_per_game": int(sims_n),
                "mae": (sum(totals_abs) / len(totals_abs)) if totals_abs else None,
                "rmse": _rmse(totals_err),
                "avg_nll_exact_total": (sum(totals_nll) / len(totals_nll)) if totals_nll else None,
            },
            "moneyline": {
                "games": int(len(results)),
                "brier": (sum(ml_brier) / len(ml_brier)) if ml_brier else None,
                "logloss": (sum(ml_logloss) / len(ml_logloss)) if ml_logloss else None,
                "accuracy": (sum(ml_acc) / len(ml_acc)) if ml_acc else None,
            },
            "ats_runline_fav_minus_1_5": {
                "games": int(len(results)),
                "brier": (sum(ats_brier) / len(ats_brier)) if ats_brier else None,
                "logloss": (sum(ats_logloss) / len(ats_logloss)) if ats_logloss else None,
                "accuracy": (sum(ats_acc) / len(ats_acc)) if ats_acc else None,
            },
            "pitcher_props_starters": {
                "starters": int(len(so_abs)),
                "so_mae": (sum(so_abs) / len(so_abs)) if so_abs else None,
                "so_rmse": _rmse(so_err),
                "outs_mae": (sum(outs_abs) / len(outs_abs)) if outs_abs else None,
                "outs_rmse": _rmse(outs_err),
                "pitches_n": int(len(pitches_abs)),
                "pitches_mae": (sum(pitches_abs) / len(pitches_abs)) if pitches_abs else None,
                "pitches_rmse": _rmse(pitches_err),
                "pitches_bias": (sum(pitches_err) / len(pitches_err)) if pitches_err else None,
            },
            "pitcher_props_at_market_lines": {
                "lines_meta": market_meta,
                "push_policy": str(args.market_push_policy),
                "strikeouts": {
                    "n": int(len(so_line_brier)),
                    "n_accuracy": int(len(so_line_acc)),
                    "n_edge": int(len(so_line_edge)),
                    "pushes": int(so_line_pushes),
                    "brier": (sum(so_line_brier) / len(so_line_brier)) if so_line_brier else None,
                    "logloss": (sum(so_line_logloss) / len(so_line_logloss)) if so_line_logloss else None,
                    "accuracy": (sum(so_line_acc) / len(so_line_acc)) if so_line_acc else None,
                    "avg_edge_vs_no_vig": (sum(so_line_edge) / len(so_line_edge)) if so_line_edge else None,
                },
                "outs": {
                    "n": int(len(outs_line_brier)),
                    "n_accuracy": int(len(outs_line_acc)),
                    "n_edge": int(len(outs_line_edge)),
                    "pushes": int(outs_line_pushes),
                    "brier": (sum(outs_line_brier) / len(outs_line_brier)) if outs_line_brier else None,
                    "logloss": (sum(outs_line_logloss) / len(outs_line_logloss)) if outs_line_logloss else None,
                    "accuracy": (sum(outs_line_acc) / len(outs_line_acc)) if outs_line_acc else None,
                    "avg_edge_vs_no_vig": (sum(outs_line_edge) / len(outs_line_edge)) if outs_line_edge else None,
                },
            },
            "hitter_hr_likelihood_topn": {
                "top_n": int(args.hitter_hr_topn),
                "n": int(len(hr_brier)),
                "brier": (sum(hr_brier) / len(hr_brier)) if hr_brier else None,
                "logloss": (sum(hr_logloss) / len(hr_logloss)) if hr_logloss else None,
                "avg_p": (sum(hr_ps) / len(hr_ps)) if hr_ps else None,
                "emp_rate": (sum(float(y) for y in hr_ys) / float(len(hr_ys))) if hr_ys else None,
            },
            "hitter_props_likelihood_topn": {
                "top_n": (int(args.hitter_hr_topn) if int(args.hitter_props_topn) < 0 else int(args.hitter_props_topn)),
                **{
                    k: {
                        "n": int(len(v.get("brier") or [])),
                        "brier": (sum(v.get("brier") or []) / len(v.get("brier") or [])) if (v.get("brier") or []) else None,
                        "logloss": (sum(v.get("logloss") or []) / len(v.get("logloss") or [])) if (v.get("logloss") or []) else None,
                        "avg_p": (sum(v.get("p") or []) / len(v.get("p") or [])) if (v.get("p") or []) else None,
                        "emp_rate": (sum(float(y) for y in (v.get("y") or [])) / float(len(v.get("y") or []))) if (v.get("y") or []) else None,
                    }
                    for k, v in hitter_prop_rollup.items()
                },
            },
        }
    }

    out_path = Path(args.out) if str(args.out).strip() else (_ROOT / "data" / "eval" / f"sim_vs_actual_{str(args.date)}.json")
    _ensure_dir(out_path.parent)
    _write_json(out_path, report)

    print(f"Wrote: {out_path}")
    print("Aggregate (full):", report["aggregate"]["full"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
