from __future__ import annotations

import math
import bisect
import random
from dataclasses import replace
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    BaseState,
    BattedBallType,
    GameConfig,
    GameResult,
    InningHalfState,
    PitchCall,
    PitchResult,
    PitchType,
    TeamRoster,
)
from .state import GameState, PlateAppearanceState
from .stats import StatsTracker
from .pitch_model import PitchModelConfig, simulate_pitch
from .pitcher_distributions import PitcherDistributionConfig, sample_pitcher_day_rates


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        return float(max(float(lo), min(float(hi), float(x))))
    except Exception:
        return float(lo)


def _lognormal_mean1(rng: random.Random, sigma: float) -> float:
    """Sample a mean-1 lognormal multiplier.

    If Z~N(0,1), then exp(-0.5*sigma^2 + sigma*Z) has E[.] = 1.
    """
    sigma = float(sigma or 0.0)
    if sigma <= 0.0:
        return 1.0
    z = float(rng.gauss(0.0, 1.0))
    return float(math.exp(-0.5 * sigma * sigma + sigma * z))


def _pow_mult(mult: float, weight: float) -> float:
    """Apply a weight in log space: mult^weight, stable for mult>0."""
    try:
        w = float(weight)
        if w == 0.0:
            return 1.0
        m = float(mult)
        if m <= 0.0:
            return 1.0
        if w == 1.0:
            return m
        return float(math.exp(w * math.log(m)))
    except Exception:
        return float(mult)


def _weighted_choice(rng: random.Random, weights: Dict[PitchType, float], default: PitchType) -> PitchType:
    if not weights:
        return default
    total = sum(max(0.0, float(w)) for w in weights.values())
    if total <= 0:
        return default
    r = rng.random() * total
    acc = 0.0
    for k, w in weights.items():
        acc += max(0.0, float(w))
        if r <= acc:
            return k
    return default


def _build_weight_cdf(weights: Dict[PitchType, float]) -> Tuple[List[PitchType], List[float], float]:
    """Build cumulative weights for fast repeated sampling."""
    if not weights:
        return ([], [], 0.0)
    types: List[PitchType] = []
    cdf: List[float] = []
    acc = 0.0
    for k, w in weights.items():
        try:
            ww = float(w)
        except Exception:
            continue
        if ww <= 0.0:
            continue
        acc += ww
        types.append(k)
        cdf.append(acc)
    return (types, cdf, float(acc))


def _sample_weight_cdf(rng: random.Random, types: List[PitchType], cdf: List[float], total: float, default: PitchType) -> PitchType:
    if total <= 0.0 or not types:
        return default
    r = rng.random() * float(total)
    idx = bisect.bisect_left(cdf, r)
    if idx <= 0:
        return types[0]
    if idx >= len(types):
        return types[-1]
    return types[idx]


def _eff_bat_side(bat_side, pit_throw_side) -> str:
    try:
        b = str(getattr(bat_side, "value", bat_side) or "R").upper()
        p = str(getattr(pit_throw_side, "value", pit_throw_side) or "R").upper()
        if b == "S":
            return "L" if p == "R" else "R"
        if b in ("L", "R"):
            return b
        return "R"
    except Exception:
        return "R"


def _hand(pit_throw_side) -> str:
    try:
        p = str(getattr(pit_throw_side, "value", pit_throw_side) or "R").upper()
        return "L" if p == "L" else "R"
    except Exception:
        return "R"


def _mult_from_map(mult_map: Dict[str, float] | None, key: str) -> float:
    try:
        if not mult_map:
            return 1.0
        v = mult_map.get(key)
        if not isinstance(v, (int, float)):
            return 1.0
        return _clamp01(max(0.4, min(1.6, float(v))))
    except Exception:
        return 1.0


def _clamp_rate(x: float, lo: float, hi: float) -> float:
    try:
        return float(max(lo, min(hi, float(x))))
    except Exception:
        return float(max(lo, min(hi, 0.0)))


def _batted_ball_type(rng: random.Random) -> BattedBallType:
    x = rng.random()
    if x < 0.44:
        return BattedBallType.GROUND
    if x < 0.69:
        return BattedBallType.FLY
    if x < 0.89:
        return BattedBallType.LINE
    return BattedBallType.POP


def _bases_to_tuple(bases: BaseState) -> Tuple[bool, bool, bool]:
    if bases == BaseState.EMPTY:
        return False, False, False
    if bases == BaseState.FIRST:
        return True, False, False
    if bases == BaseState.SECOND:
        return False, True, False
    if bases == BaseState.THIRD:
        return False, False, True
    if bases == BaseState.FIRST_SECOND:
        return True, True, False
    if bases == BaseState.FIRST_THIRD:
        return True, False, True
    if bases == BaseState.SECOND_THIRD:
        return False, True, True
    return True, True, True


def _tuple_to_bases(on1: bool, on2: bool, on3: bool) -> BaseState:
    if on1 and on2 and on3:
        return BaseState.LOADED
    if on1 and on2:
        return BaseState.FIRST_SECOND
    if on1 and on3:
        return BaseState.FIRST_THIRD
    if on2 and on3:
        return BaseState.SECOND_THIRD
    if on1:
        return BaseState.FIRST
    if on2:
        return BaseState.SECOND
    if on3:
        return BaseState.THIRD
    return BaseState.EMPTY


def _set_half_bases_from_runners(half: InningHalfState, on1_id: int, on2_id: int, on3_id: int) -> None:
    on1 = int(on1_id or 0)
    on2 = int(on2_id or 0)
    on3 = int(on3_id or 0)
    half.runner_on_1b = on1
    half.runner_on_2b = on2
    half.runner_on_3b = on3
    half.bases = _tuple_to_bases(bool(on1), bool(on2), bool(on3))


def _walk_bases_with_runners(on1_id: int, on2_id: int, on3_id: int, batter_id: int) -> Tuple[int, int, int, int, List[int]]:
    """Walk/HBP advancement with runner ids.

    Returns (new_on1, new_on2, new_on3, runs_scored, scoring_runner_ids).
    """
    b = int(batter_id or 0)
    on1 = int(on1_id or 0)
    on2 = int(on2_id or 0)
    on3 = int(on3_id or 0)
    scoring: List[int] = []

    if on1 == 0 and on2 == 0 and on3 == 0:
        return b, 0, 0, 0, scoring

    # forced advances
    if on1 and on2 and on3:
        # runner from 3B scores
        scoring.append(int(on3))
        return b, int(on1), int(on2), 1, scoring

    if on1 and on2 and not on3:
        return b, int(on1), int(on2), 0, scoring
    if on1 and not on2 and on3:
        return b, int(on1), int(on3), 0, scoring
    if not on1 and on2 and on3:
        return b, int(on2), int(on3), 0, scoring
    if on1 and not on2 and not on3:
        return b, int(on1), 0, 0, scoring
    if not on1 and on2 and not on3:
        return b, int(on2), 0, 0, scoring
    if not on1 and not on2 and on3:
        return b, 0, int(on3), 0, scoring

    return b, int(on2), int(on3), 0, scoring


def _advance_runners_one_base_with_runners(on1_id: int, on2_id: int, on3_id: int) -> Tuple[int, int, int, int, List[int]]:
    on1 = int(on1_id or 0)
    on2 = int(on2_id or 0)
    on3 = int(on3_id or 0)
    scoring: List[int] = []

    if on3:
        scoring.append(int(on3))
    return 0, int(on1), int(on2), int(len(scoring)), scoring


def _single_first_to_third_rate(batted_ball_type: Optional[BattedBallType], base_rate: float) -> float:
    p = float(base_rate or 0.0)
    if batted_ball_type == BattedBallType.GROUND:
        p *= 0.60
    elif batted_ball_type == BattedBallType.LINE:
        p *= 1.45
    elif batted_ball_type == BattedBallType.POP:
        p *= 0.35
    return _clamp01(p)


def _productive_out_advance_rate(batted_ball_type: Optional[BattedBallType], base_rate: float) -> float:
    p = float(base_rate or 0.0)
    if batted_ball_type == BattedBallType.LINE:
        p *= 0.75
    elif batted_ball_type in (BattedBallType.FLY, BattedBallType.POP):
        p *= 1.25
    return _clamp01(p)


def _roe_reach_rate(batted_ball_type: Optional[BattedBallType], base_rate: float) -> float:
    p = float(base_rate or 0.0)
    if batted_ball_type == BattedBallType.GROUND:
        p *= 1.40
    elif batted_ball_type == BattedBallType.LINE:
        p *= 0.85
    else:
        p *= 0.45
    return _clamp01(p)


def _pick_misc_pitch_advance_event(rng: random.Random, pitch_call: PitchCall) -> str:
    x = rng.random()
    if pitch_call == PitchCall.BALL:
        if x < 0.72:
            return "WP"
        if x < 0.90:
            return "PB"
        return "BALK"
    if x < 0.55:
        return "WP"
    if x < 0.90:
        return "PB"
    return "BALK"


RUNNER_SRC_HIT_REACH = "hit_reach"
RUNNER_SRC_BB_HBP = "bb_hbp"
RUNNER_SRC_NON_HIT_REACH = "non_hit_reach"


def _sync_runner_reach_sources(source_by_id: Dict[int, str], half: Optional[InningHalfState]) -> None:
    if half is None:
        source_by_id.clear()
        return
    keep = {
        int(runner_id)
        for runner_id in (half.runner_on_1b, half.runner_on_2b, half.runner_on_3b)
        if int(runner_id or 0) > 0
    }
    for runner_id in list(source_by_id.keys()):
        if int(runner_id) not in keep:
            source_by_id.pop(runner_id, None)


def _hr_context_mult(
    pitch_cfg: PitchModelConfig,
    inning: int,
    half: InningHalfState,
    pitcher_id: int,
    starter_pitcher_id: int,
    runner_source_by_id: Dict[int, str],
) -> float:
    runner_ids = [
        int(half.runner_on_1b or 0),
        int(half.runner_on_2b or 0),
        int(half.runner_on_3b or 0),
    ]
    has_runners = any(runner_id > 0 for runner_id in runner_ids)
    has_bb_hbp_runner = any(
        runner_source_by_id.get(runner_id) == RUNNER_SRC_BB_HBP
        for runner_id in runner_ids
        if runner_id > 0
    )
    has_non_hit_reach_runner = any(
        runner_source_by_id.get(runner_id) == RUNNER_SRC_NON_HIT_REACH
        for runner_id in runner_ids
        if runner_id > 0
    )
    is_starter = int(starter_pitcher_id or 0) > 0 and int(pitcher_id or 0) == int(starter_pitcher_id or 0)
    is_reliever = int(starter_pitcher_id or 0) > 0 and int(pitcher_id or 0) != int(starter_pitcher_id or 0)

    mult = 1.0
    if has_bb_hbp_runner:
        mult *= float(getattr(pitch_cfg, "hr_bb_hbp_runner_mult", 1.0) or 1.0)
        if is_starter:
            mult *= float(getattr(pitch_cfg, "hr_starter_bb_hbp_runner_mult", 1.0) or 1.0)
    if has_non_hit_reach_runner:
        mult *= float(getattr(pitch_cfg, "hr_non_hit_reach_runner_mult", 1.0) or 1.0)
    if has_runners and is_reliever:
        mult *= float(getattr(pitch_cfg, "hr_reliever_runner_on_mult", 1.0) or 1.0)
    if has_runners and is_reliever and int(inning) >= 7:
        mult *= float(getattr(pitch_cfg, "hr_late_reliever_runner_on_mult", 1.0) or 1.0)
    return _clamp(float(mult), 0.5, 2.0)


def _advance_bases_simple_with_runners(bases: BaseState, on1_id: int, on2_id: int, on3_id: int, batter_id: int, hit: str) -> Tuple[BaseState, int, int, int, int, List[int]]:
    """Deterministic forced-advance baserunning with runner ids.

    Returns (new_bases, new_on1, new_on2, new_on3, runs_scored, scoring_runner_ids).
    """
    on1 = int(on1_id or 0)
    on2 = int(on2_id or 0)
    on3 = int(on3_id or 0)
    b = int(batter_id or 0)
    scoring: List[int] = []

    if hit == "OUT":
        return bases, on1, on2, on3, 0, scoring

    if hit == "HR":
        for rid in (on1, on2, on3, b):
            if int(rid) > 0:
                scoring.append(int(rid))
        return BaseState.EMPTY, 0, 0, 0, int(len(scoring)), scoring

    if hit == "3B":
        for rid in (on1, on2, on3):
            if int(rid) > 0:
                scoring.append(int(rid))
        return BaseState.THIRD, 0, 0, b, int(len(scoring)), scoring

    if hit == "2B":
        # 3B/2B score
        if on3:
            scoring.append(int(on3))
            on3 = 0
        if on2:
            scoring.append(int(on2))
            on2 = 0

        # 1B -> 3B
        if on1:
            on3 = int(on1)
            on1 = 0

        on2 = b
        nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
        return nb, int(on1), int(on2), int(on3), int(len(scoring)), scoring

    # 1B
    if on3:
        scoring.append(int(on3))
        on3 = 0

    # 2B -> 3B, 1B -> 2B
    if on2:
        on3 = int(on2)
        on2 = 0
    if on1:
        on2 = int(on1)
        on1 = 0
    on1 = b
    nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
    return nb, int(on1), int(on2), int(on3), int(len(scoring)), scoring


def _advance_bases_hit_with_runners(
    rng: random.Random,
    bases: BaseState,
    on1_id: int,
    on2_id: int,
    on3_id: int,
    batter_id: int,
    hit: str,
    batted_ball_type: Optional[BattedBallType],
    p2_scores_on_1b_mult: float = 1.0,
    p1_scores_on_2b_mult: float = 1.0,
    p1_to_3b_on_1b_rate: float = 0.24,
) -> Tuple[BaseState, int, int, int, int, List[int]]:
    """Batted-ball-informed baserunning with runner ids.

    Returns (new_bases, new_on1, new_on2, new_on3, runs_scored, scoring_runner_ids).
    """
    on1 = int(on1_id or 0)
    on2 = int(on2_id or 0)
    on3 = int(on3_id or 0)
    b = int(batter_id or 0)
    scoring: List[int] = []
    bb = batted_ball_type

    if hit == "HR":
        for rid in (on1, on2, on3, b):
            if int(rid) > 0:
                scoring.append(int(rid))
        return BaseState.EMPTY, 0, 0, 0, int(len(scoring)), scoring

    if hit == "3B":
        for rid in (on1, on2, on3):
            if int(rid) > 0:
                scoring.append(int(rid))
        return BaseState.THIRD, 0, 0, b, int(len(scoring)), scoring

    if hit == "2B":
        if on3:
            scoring.append(int(on3))
            on3 = 0
        if on2:
            scoring.append(int(on2))
            on2 = 0

        p_1st_scores = 0.55
        if bb == BattedBallType.GROUND:
            p_1st_scores = 0.42
        elif bb == BattedBallType.LINE:
            p_1st_scores = 0.60
        p_1st_scores = _clamp01(float(p_1st_scores) * float(p1_scores_on_2b_mult or 1.0))
        if on1:
            if rng.random() < p_1st_scores:
                scoring.append(int(on1))
                on1 = 0
                on3 = 0
            else:
                on3 = int(on1)
                on1 = 0

        on2 = b
        nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
        return nb, int(on1), int(on2), int(on3), int(len(scoring)), scoring

    # 1B
    if on3:
        scoring.append(int(on3))
        on3 = 0

    p_2nd_scores = 0.28
    if bb == BattedBallType.GROUND:
        p_2nd_scores = 0.18
    elif bb == BattedBallType.LINE:
        p_2nd_scores = 0.34
    elif bb == BattedBallType.FLY:
        p_2nd_scores = 0.30
    p_2nd_scores = _clamp01(float(p_2nd_scores) * float(p2_scores_on_1b_mult or 1.0))

    if on2:
        if rng.random() < p_2nd_scores:
            scoring.append(int(on2))
            on2 = 0
        else:
            on3 = int(on2)
            on2 = 0

    if on1:
        p_1st_to_3rd = _single_first_to_third_rate(bb, p1_to_3b_on_1b_rate)
        if not on3 and rng.random() < p_1st_to_3rd:
            on3 = int(on1)
            on1 = 0
        else:
            on2 = int(on1)
            on1 = 0

    on1 = b
    nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
    return nb, int(on1), int(on2), int(on3), int(len(scoring)), scoring


def _advance_bases_hit(
    rng: random.Random,
    bases: BaseState,
    hit: str,
    batted_ball_type: Optional[BattedBallType],
) -> Tuple[BaseState, int]:
    """Light baserunning model for hits.

    Returns (new_bases, runs_scored).
    """
    on1, on2, on3 = _bases_to_tuple(bases)
    runs = 0
    bb = batted_ball_type

    if hit == "HR":
        runs = int(on1) + int(on2) + int(on3) + 1
        return BaseState.EMPTY, runs

    if hit == "3B":
        runs = int(on1) + int(on2) + int(on3)
        return BaseState.THIRD, runs

    if hit == "2B":
        # Runners from 2B/3B score; runner from 1B scores sometimes.
        if on3:
            runs += 1
            on3 = False
        if on2:
            runs += 1
            on2 = False

        p_1st_scores = 0.55
        if bb == BattedBallType.GROUND:
            p_1st_scores = 0.42
        elif bb == BattedBallType.LINE:
            p_1st_scores = 0.60
        if on1:
            if rng.random() < p_1st_scores:
                runs += 1
                on1 = False
                on3 = False
            else:
                on1 = False
                on3 = True

        # Batter to 2B
        on2 = True
        return _tuple_to_bases(on1, on2, on3), runs

    # 1B
    # Runner from 3B scores; runner from 2B scores sometimes.
    if on3:
        runs += 1
        on3 = False

    p_2nd_scores = 0.28
    if bb == BattedBallType.GROUND:
        p_2nd_scores = 0.18
    elif bb == BattedBallType.LINE:
        p_2nd_scores = 0.34
    elif bb == BattedBallType.FLY:
        p_2nd_scores = 0.30

    if on2:
        if rng.random() < p_2nd_scores:
            runs += 1
            on2 = False
        else:
            on2 = False
            on3 = True

    if on1:
        on1 = False
        on2 = True

    on1 = True
    return _tuple_to_bases(on1, on2, on3), runs


def _advance_bases_simple(bases: BaseState, hit: str) -> Tuple[BaseState, int]:
    """Deterministic forced-advance baserunning used for A/B baselines."""
    if hit == "OUT":
        return bases, 0
    if hit == "HR":
        runners = {
            BaseState.EMPTY: 0,
            BaseState.FIRST: 1,
            BaseState.SECOND: 1,
            BaseState.THIRD: 1,
            BaseState.FIRST_SECOND: 2,
            BaseState.FIRST_THIRD: 2,
            BaseState.SECOND_THIRD: 2,
            BaseState.LOADED: 3,
        }[bases]
        return BaseState.EMPTY, runners + 1

    if hit == "3B":
        runners = {
            BaseState.EMPTY: 0,
            BaseState.FIRST: 1,
            BaseState.SECOND: 1,
            BaseState.THIRD: 1,
            BaseState.FIRST_SECOND: 2,
            BaseState.FIRST_THIRD: 2,
            BaseState.SECOND_THIRD: 2,
            BaseState.LOADED: 3,
        }[bases]
        return BaseState.THIRD, runners

    if hit == "2B":
        runs = 0
        if bases in (BaseState.THIRD, BaseState.FIRST_THIRD, BaseState.SECOND_THIRD, BaseState.LOADED):
            runs += 1
        if bases in (BaseState.SECOND, BaseState.FIRST_SECOND, BaseState.SECOND_THIRD, BaseState.LOADED):
            runs += 1
        if bases in (BaseState.FIRST, BaseState.FIRST_SECOND, BaseState.FIRST_THIRD, BaseState.LOADED):
            return BaseState.THIRD, runs
        return BaseState.SECOND, runs

    # 1B
    runs = 0
    if bases in (BaseState.THIRD, BaseState.FIRST_THIRD, BaseState.SECOND_THIRD, BaseState.LOADED):
        runs += 1

    if bases == BaseState.EMPTY:
        return BaseState.FIRST, runs
    if bases == BaseState.FIRST:
        return BaseState.FIRST_SECOND, runs
    if bases == BaseState.SECOND:
        return BaseState.FIRST_THIRD, runs
    if bases == BaseState.THIRD:
        return BaseState.FIRST, runs
    if bases == BaseState.FIRST_SECOND:
        return BaseState.LOADED, runs
    if bases == BaseState.FIRST_THIRD:
        return BaseState.FIRST_SECOND, runs
    if bases == BaseState.SECOND_THIRD:
        return BaseState.FIRST_THIRD, runs
    return BaseState.LOADED, runs


def _resolve_in_play_out(
    rng: random.Random,
    bases: BaseState,
    outs: int,
    batted_ball_type: Optional[BattedBallType],
    dp_rate: float,
    sf_rate_flypop: float,
    sf_rate_line: float,
) -> Tuple[BaseState, int, int, str]:
    """Resolve an in-play out with crude DP/SF logic.

    Returns (new_bases, runs_scored, outs_added, subtype).
    subtype is one of: OUT, DP, SF.
    """
    on1, on2, on3 = _bases_to_tuple(bases)
    bb = batted_ball_type

    # Double play: ground ball with runner on 1st and <2 outs.
    if bb == BattedBallType.GROUND and outs <= 1 and on1:
        p_dp = _clamp01(float(dp_rate))
        if rng.random() < p_dp:
            on1 = False
            # Keep other runners in place (conservative).
            return _tuple_to_bases(on1, on2, on3), 0, 2, "DP"

    # Sac fly: fly/pop/line with runner on 3rd and <2 outs.
    if bb in (BattedBallType.FLY, BattedBallType.POP, BattedBallType.LINE) and outs <= 1 and on3:
        p_sf = _clamp01(float(sf_rate_flypop)) if bb in (BattedBallType.FLY, BattedBallType.POP) else _clamp01(float(sf_rate_line))
        if rng.random() < p_sf:
            on3 = False
            return _tuple_to_bases(on1, on2, on3), 1, 1, "SF"

    return bases, 0, 1, "OUT"


def _resolve_in_play_out_with_runners(
    rng: random.Random,
    bases: BaseState,
    outs: int,
    on1_id: int,
    on2_id: int,
    on3_id: int,
    batter_id: int,
    batted_ball_type: Optional[BattedBallType],
    dp_rate: float,
    sf_rate_flypop: float,
    sf_rate_line: float,
    ground_rbi_out_rate: float,
    out_2b_to_3b_rate: float,
    out_1b_to_2b_rate: float,
    roe_rate: float,
    fc_rate: float,
    fc_runner_on_3b_score_rate: float,
    p1_to_3b_on_1b_rate: float,
) -> Tuple[BaseState, int, int, int, int, List[int], int, str]:
    """Runner-id-aware resolution for non-hit in-play balls.

    Returns (new_bases, new_on1, new_on2, new_on3, runs_scored, scoring_runner_ids, outs_added, subtype).
    """
    on1 = int(on1_id or 0)
    on2 = int(on2_id or 0)
    on3 = int(on3_id or 0)
    b = int(batter_id or 0)
    scoring: List[int] = []
    bb = batted_ball_type

    if bb == BattedBallType.GROUND and outs <= 1 and on1:
        p_dp = _clamp01(float(dp_rate))
        if rng.random() < p_dp:
            on1 = 0
            nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
            return nb, int(on1), int(on2), int(on3), 0, scoring, 2, "DP"

        if rng.random() < _clamp01(float(fc_rate or 0.0)):
            if on3 and rng.random() < _clamp01(float(fc_runner_on_3b_score_rate or 0.0)):
                scoring.append(int(on3))
                on3 = int(on2) if on2 else 0
                on2 = 0
                on1 = b
                nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
                return nb, int(on1), int(on2), int(on3), 1, scoring, 1, "FC"
            if on2 and on3:
                on3 = int(on2)
                on2 = int(on1)
                on1 = b
            else:
                if on2 and not on3:
                    on3 = int(on2)
                    on2 = 0
                on1 = b
            nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
            return nb, int(on1), int(on2), int(on3), 0, scoring, 1, "FC"

    if bb in (BattedBallType.FLY, BattedBallType.POP, BattedBallType.LINE) and outs <= 1 and on3:
        p_sf = _clamp01(float(sf_rate_flypop)) if bb in (BattedBallType.FLY, BattedBallType.POP) else _clamp01(float(sf_rate_line))
        if rng.random() < p_sf:
            scoring.append(int(on3))
            on3 = 0
            nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
            return nb, int(on1), int(on2), int(on3), 1, scoring, 1, "SF"

    if rng.random() < _roe_reach_rate(bb, roe_rate):
        nb, on1, on2, on3, runs, scoring = _advance_bases_hit_with_runners(
            rng,
            bases,
            on1,
            on2,
            on3,
            b,
            "1B",
            bb,
            p1_to_3b_on_1b_rate=p1_to_3b_on_1b_rate,
        )
        return nb, int(on1), int(on2), int(on3), int(runs), scoring, 0, "ROE"

    scored_on_out = False
    if bb == BattedBallType.GROUND and outs <= 1 and on3 and rng.random() < _clamp01(float(ground_rbi_out_rate or 0.0)):
        scoring.append(int(on3))
        on3 = 0
        scored_on_out = True

    advanced = False
    if outs <= 1:
        p_2b_to_3b = _productive_out_advance_rate(bb, out_2b_to_3b_rate)
        p_1b_to_2b = _productive_out_advance_rate(bb, out_1b_to_2b_rate)
        if on2 and not on3 and rng.random() < p_2b_to_3b:
            on3 = int(on2)
            on2 = 0
            advanced = True
        if on1 and not on2 and rng.random() < p_1b_to_2b:
            on2 = int(on1)
            on1 = 0
            advanced = True

    nb = _tuple_to_bases(bool(on1), bool(on2), bool(on3))
    if scored_on_out:
        return nb, int(on1), int(on2), int(on3), int(len(scoring)), scoring, 1, "RBI_OUT"
    if advanced:
        return nb, int(on1), int(on2), int(on3), 0, scoring, 1, "ADV_OUT"
    return nb, int(on1), int(on2), int(on3), 0, scoring, 1, "OUT"


def _advance_bases(bases: BaseState, hit: str, outs: int) -> Tuple[BaseState, int, int]:
    """Backward-compatible wrapper (kept for safety).

    Prefer using `_advance_bases_hit` / `_resolve_in_play_out` directly.
    """
    if hit == "OUT":
        return bases, 0, 1
    if hit in ("1B", "2B", "3B", "HR"):
        new_bases, runs = _advance_bases_simple(bases, hit)
        return new_bases, runs, 0
    return bases, 0, 1


def _walk_bases(bases: BaseState) -> Tuple[BaseState, int]:
    """Returns (new_bases, runs_scored)."""
    if bases == BaseState.EMPTY:
        return BaseState.FIRST, 0
    if bases == BaseState.FIRST:
        return BaseState.FIRST_SECOND, 0
    if bases == BaseState.SECOND:
        return BaseState.FIRST_SECOND, 0
    if bases == BaseState.THIRD:
        return BaseState.FIRST_THIRD, 0
    if bases == BaseState.FIRST_SECOND:
        return BaseState.LOADED, 0
    if bases == BaseState.FIRST_THIRD:
        return BaseState.LOADED, 0
    if bases == BaseState.SECOND_THIRD:
        return BaseState.LOADED, 0
    # loaded
    return BaseState.LOADED, 1


def _combined_rate(a: float, b: float) -> float:
    # Combine two rate estimates without exploding extremes.
    return _clamp01(0.5 * a + 0.5 * b)


def _simulate_pitch(
    rng: random.Random,
    pitch_cfg: PitchModelConfig,
    batter,
    pitcher,
    pitcher_day_rates: Optional[Dict[str, float]],
    count: Tuple[int, int],
    weather_hr_mult: float,
    weather_inplay_hit_mult: float,
    weather_xb_share_mult: float,
    park_hr_mult: float,
    park_inplay_hit_mult: float,
    park_xb_share_mult: float,
    umpire_called_strike_mult: float,
) -> PitchResult:
    def _eff_bat_side(bat_side, pit_throw_side):
        try:
            b = str(getattr(bat_side, "value", bat_side) or "R").upper()
            p = str(getattr(pit_throw_side, "value", pit_throw_side) or "R").upper()
            if b == "S":
                # Switch hitter bats opposite pitcher.
                return "L" if p == "R" else "R"
            if b in ("L", "R"):
                return b
            return "R"
        except Exception:
            return "R"

    def _hand(pit_throw_side):
        try:
            p = str(getattr(pit_throw_side, "value", pit_throw_side) or "R").upper()
            return "L" if p == "L" else "R"
        except Exception:
            return "R"

    def _m(d: Dict[str, float] | None, k: str) -> float:
        try:
            if not d:
                return 1.0
            v = d.get(k)
            if not isinstance(v, (int, float)):
                return 1.0
            return _clamp01(max(0.4, min(1.6, float(v))))
        except Exception:
            return 1.0

    def _clamp_rate(x: float, lo: float, hi: float) -> float:
        try:
            return float(max(lo, min(hi, float(x))))
        except Exception:
            return float(max(lo, min(hi, 0.0)))

    # Platoon adjustments (best-effort, defaults to neutral).
    pit_hand = _hand(getattr(pitcher.player, "throw_side", getattr(pitcher, "throw_side", "R")))
    eff_bat = _eff_bat_side(getattr(batter.player, "bat_side", getattr(batter, "bat_side", "R")), getattr(pitcher.player, "throw_side", getattr(pitcher, "throw_side", "R")))

    b_mults = getattr(batter, "platoon_mult_vs_lhp", {}) if pit_hand == "L" else getattr(batter, "platoon_mult_vs_rhp", {})
    p_mults = getattr(pitcher, "platoon_mult_vs_lhb", {}) if eff_bat == "L" else getattr(pitcher, "platoon_mult_vs_rhb", {})

    batter_k = _clamp_rate(float(batter.k_rate) * _m(b_mults, "k"), 0.05, 0.55)
    batter_bb = _clamp_rate(float(batter.bb_rate) * _m(b_mults, "bb"), 0.01, 0.22)
    batter_hr = _clamp_rate(float(batter.hr_rate) * _m(b_mults, "hr"), 0.002, 0.12)
    batter_inplay = _clamp_rate(float(batter.inplay_hit_rate) * _m(b_mults, "inplay"), 0.10, 0.45)

    # Optional head-to-head batter-vs-pitcher HR adjustment.
    try:
        pid = int(getattr(getattr(pitcher, "player", None), "mlbam_id", 0) or 0)
        if pid > 0:
            mm = getattr(batter, "vs_pitcher_hr_mult", None)
            if isinstance(mm, dict):
                mult = mm.get(pid)
                if isinstance(mult, (int, float)):
                    batter_hr = _clamp_rate(float(batter_hr) * float(mult), 0.002, 0.12)
    except Exception:
        pass

    pr = pitcher_day_rates or {}
    base_pitcher_k = float(pr.get("k_rate", pitcher.k_rate))
    base_pitcher_bb = float(pr.get("bb_rate", pitcher.bb_rate))
    base_pitcher_hr = float(pr.get("hr_rate", pitcher.hr_rate))
    base_pitcher_inplay = float(pr.get("inplay_hit_rate", pitcher.inplay_hit_rate))

    pitcher_k = _clamp_rate(base_pitcher_k * _m(p_mults, "k"), 0.05, 0.60)
    pitcher_bb = _clamp_rate(base_pitcher_bb * _m(p_mults, "bb"), 0.01, 0.25)
    pitcher_hr = _clamp_rate(base_pitcher_hr * _m(p_mults, "hr"), 0.002, 0.14)
    pitcher_inplay = _clamp_rate(base_pitcher_inplay * _m(p_mults, "inplay"), 0.10, 0.45)

    pitch_type = _weighted_choice(rng, pitcher.arsenal, PitchType.FF)
    raw_pt_mult = float((batter.vs_pitch_type or {}).get(pitch_type, 1.0))
    try:
        raw_pt_mult = float(max(0.4, min(1.6, raw_pt_mult)))
    except Exception:
        raw_pt_mult = 1.0

    try:
        _scale_raw = getattr(pitch_cfg, "batter_pt_scale", 1.0)
        pt_scale = 1.0 if _scale_raw is None else float(_scale_raw)
    except Exception:
        pt_scale = 1.0
    pt_scale = float(max(0.0, min(2.0, pt_scale)))
    raw_pt_mult = float(1.0 + pt_scale * (raw_pt_mult - 1.0))
    raw_pt_mult = float(max(0.4, min(1.6, raw_pt_mult)))

    try:
        _alpha_raw = getattr(pitch_cfg, "batter_pt_alpha", 0.5)
        alpha = 0.5 if _alpha_raw is None else float(_alpha_raw)
    except Exception:
        alpha = 0.5
    alpha = float(max(0.0, min(1.0, alpha)))
    pt_mult = float(1.0 + alpha * (raw_pt_mult - 1.0))
    p_whiff_mult = float((pitcher.pitch_type_whiff_mult or {}).get(pitch_type, 1.0))
    p_inplay_mult = float((pitcher.pitch_type_inplay_mult or {}).get(pitch_type, 1.0))
    return simulate_pitch(
        rng=rng,
        cfg=pitch_cfg,
        pitch_type=pitch_type,
        pitcher_whiff_mult=p_whiff_mult,
        pitcher_inplay_mult=p_inplay_mult,
        weather_hr_mult=weather_hr_mult,
        weather_inplay_hit_mult=weather_inplay_hit_mult,
        weather_xb_share_mult=weather_xb_share_mult,
        park_hr_mult=park_hr_mult,
        park_inplay_hit_mult=park_inplay_hit_mult,
        park_xb_share_mult=park_xb_share_mult,
        umpire_called_strike_mult=umpire_called_strike_mult,
        count=count,
        batter_k_rate=batter_k,
        batter_bb_rate=batter_bb,
        batter_hbp_rate=batter.hbp_rate,
        batter_hr_rate=batter_hr,
        batter_inplay_hit_rate=batter_inplay,
        batter_xb_hit_share=batter.xb_hit_share,
        batter_triple_share_of_xb=float(getattr(batter, "triple_share_of_xb", 0.12) or 0.12),
        batter_bb_gb_rate=float(getattr(batter, "bb_gb_rate", 0.44)),
        batter_bb_fb_rate=float(getattr(batter, "bb_fb_rate", 0.25)),
        batter_bb_ld_rate=float(getattr(batter, "bb_ld_rate", 0.20)),
        batter_bb_pu_rate=float(getattr(batter, "bb_pu_rate", 0.11)),
        batter_bb_inplay_n=int(getattr(batter, "bb_inplay_n", 0) or 0),
        batter_pt_mult=pt_mult,
        pitcher_k_rate=pitcher_k,
        pitcher_bb_rate=pitcher_bb,
        pitcher_hbp_rate=float(pr.get("hbp_rate", pitcher.hbp_rate)),
        pitcher_hr_rate=pitcher_hr,
        pitcher_inplay_hit_rate=pitcher_inplay,
        pitcher_bb_gb_rate=float(getattr(pitcher, "bb_gb_rate", 0.44)),
        pitcher_bb_fb_rate=float(getattr(pitcher, "bb_fb_rate", 0.25)),
        pitcher_bb_ld_rate=float(getattr(pitcher, "bb_ld_rate", 0.20)),
        pitcher_bb_pu_rate=float(getattr(pitcher, "bb_pu_rate", 0.11)),
        pitcher_bb_inplay_n=int(getattr(pitcher, "bb_inplay_n", 0) or 0),
    )


def _leverage_index(inning: int, top: bool, outs: int, bases: BaseState, score_diff_from_fielding: int) -> float:
    """0..1 crude leverage proxy.

    - Later innings increase leverage
    - Close games increase leverage
    - Runners on increase leverage
    - 2 outs slightly increases leverage
    """
    late = min(1.0, max(0.0, (inning - 6) / 3.0))
    close = 1.0 - min(1.0, abs(score_diff_from_fielding) / 6.0)
    runners = {
        BaseState.EMPTY: 0.0,
        BaseState.FIRST: 0.25,
        BaseState.SECOND: 0.3,
        BaseState.THIRD: 0.35,
        BaseState.FIRST_SECOND: 0.45,
        BaseState.FIRST_THIRD: 0.5,
        BaseState.SECOND_THIRD: 0.55,
        BaseState.LOADED: 0.7,
    }[bases]
    outs_boost = 0.08 if outs == 2 else 0.0
    return _clamp01(0.45 * late + 0.35 * close + 0.2 * runners + outs_boost)


def _score_diff_from_fielding(state: GameState) -> int:
    # Score diff from fielding team's POV.
    # Positive => fielding team leading.
    if state.top:
        # home fielding in top
        return int(state.home_score - state.away_score)
    # away fielding in bottom
    return int(state.away_score - state.home_score)


def _select_reliever_legacy(roster: TeamRoster, lev: float, inning: int, score_diff_fielding: int) -> int:
    """Choose a bullpen arm by role and leverage.

    - High leverage late & close: closer
    - Medium leverage: setup
    - Low leverage/blowouts/early: long or middle
    """
    bullpen = roster.lineup.bullpen or []
    if not bullpen:
        return roster.lineup.pitcher.player.mlbam_id

    def by_role(role: str):
        return [p for p in bullpen if (p.role or "").upper() == role]

    closers = by_role("CL")
    setups = by_role("SU")
    longs = by_role("LR")
    middles = [p for p in bullpen if p not in closers + setups + longs]

    close_game = abs(score_diff_fielding) <= roster.manager.closer_leverage_max_run_diff
    late_inning = inning >= 8

    def _avail(p) -> float:
        try:
            return _clamp01(float(getattr(p, "availability_mult", 1.0) or 1.0))
        except Exception:
            return 1.0

    if lev >= 0.72 and close_game and late_inning and closers:
        return max(closers, key=lambda p: (_avail(p), _avail(p) * p.leverage_skill, p.leverage_skill)).player.mlbam_id
    if lev >= 0.58 and close_game and inning >= 7 and (setups or closers):
        pool = setups if setups else closers
        return max(pool, key=lambda p: (_avail(p), _avail(p) * p.leverage_skill, p.leverage_skill)).player.mlbam_id
    if lev <= 0.35 and (not close_game) and longs:
        return max(longs, key=lambda p: (_avail(p), _avail(p) * p.stamina_pitches, p.stamina_pitches)).player.mlbam_id
    pool = middles or setups or closers or bullpen
    return max(pool, key=lambda p: (_avail(p), _avail(p) * p.leverage_skill, p.leverage_skill, _avail(p) * p.stamina_pitches, p.stamina_pitches)).player.mlbam_id


def _select_pitcher_legacy(roster: TeamRoster, state: GameState) -> int:
    """Manager hook: decide whether to keep current pitcher or make a change."""
    team_id = roster.team.team_id
    current = state.current_pitcher_by_team.get(team_id)

    starter = roster.lineup.pitcher.player.mlbam_id
    if current is None:
        current = starter

    pc = state.pitcher_pitch_count.get(current, 0)
    half = state.half
    outs = half.outs if half else 0
    bases = half.bases if half else BaseState.EMPTY

    fielding_diff = _score_diff_from_fielding(state)

    lev = _leverage_index(state.inning, state.top, outs, bases, fielding_diff)

    # Starter pull logic
    if current == starter:
        # "F5 leash": keep starter through manager.starter_min_innings unless pitch count is extreme
        # or the game is a blowout.
        in_leash_window = state.inning <= max(1, int(roster.manager.starter_min_innings))
        blowout = abs(fielding_diff) >= int(roster.manager.starter_blowup_run_diff)
        if in_leash_window and (not blowout) and pc < (roster.manager.pull_starter_pitch_count + 15):
            state.current_pitcher_by_team[team_id] = current
            return current

        if pc < roster.manager.pull_starter_pitch_count:
            state.current_pitcher_by_team[team_id] = current
            return current
        # Pull starter once over hook.
        rel = _select_reliever_legacy(roster, lev, state.inning, fielding_diff)
        state.current_pitcher_by_team[team_id] = rel
        return rel

    # Reliever: keep unless very low stamina (pitch count > stamina+15)
    prof = _pitcher_profile(roster, current)
    if pc > (prof.stamina_pitches + 15):
        rel = _select_reliever_legacy(roster, lev, state.inning, fielding_diff)
        state.current_pitcher_by_team[team_id] = rel
        return rel

    state.current_pitcher_by_team[team_id] = current
    return current


def _select_reliever_v2(
    roster: TeamRoster,
    lev: float,
    inning: int,
    score_diff_fielding: int,
    overrides: Optional[dict] = None,
) -> int:
    """V2 bullpen choice.

    - Honors manager.use_closer_in_9th_only
    - Prefers available arms
    - Uses leverage + close-game context
    """
    bullpen = roster.lineup.bullpen or []
    if not bullpen:
        return roster.lineup.pitcher.player.mlbam_id

    def by_role(role: str):
        return [p for p in bullpen if (p.role or "").upper() == role]

    closers = by_role("CL")
    setups = by_role("SU")
    longs = by_role("LR")
    middles = [p for p in bullpen if p not in closers + setups + longs]

    close_game = abs(int(score_diff_fielding)) <= int(roster.manager.closer_leverage_max_run_diff)
    late_inning = int(inning) >= 8
    ninth_or_later = int(inning) >= 9

    ov = overrides if isinstance(overrides, dict) else {}

    def _ov_f(key: str, default: float) -> float:
        v = ov.get(key)
        try:
            if v is None:
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    # Optional reliever selection shrink: shrink reliever leverage_skill toward 0.5.
    # 1.0 => no change; 0.0 => all arms treated equal by leverage_skill.
    reliever_leverage_skill_shrink = _clamp01(_ov_f("reliever_leverage_skill_shrink", 1.0))

    def _lev_skill(p) -> float:
        try:
            base = _clamp01(float(getattr(p, "leverage_skill", 0.5) or 0.5))
        except Exception:
            base = 0.5
        return 0.5 + float(reliever_leverage_skill_shrink) * (base - 0.5)

    def _avail(p) -> float:
        try:
            return _clamp01(float(getattr(p, "availability_mult", 1.0) or 1.0))
        except Exception:
            return 1.0

    def _usable(p) -> bool:
        return _avail(p) >= 0.25

    closers = [p for p in closers if _usable(p)]
    setups = [p for p in setups if _usable(p)]
    longs = [p for p in longs if _usable(p)]
    middles = [p for p in middles if _usable(p)]

    allow_closer = True
    if bool(getattr(roster.manager, "use_closer_in_9th_only", True)):
        allow_closer = ninth_or_later

    if allow_closer and lev >= 0.70 and close_game and late_inning and closers:
        return max(closers, key=lambda p: (_avail(p) * _lev_skill(p), _avail(p), _lev_skill(p))).player.mlbam_id
    if lev >= 0.58 and close_game and int(inning) >= 7 and (setups or (allow_closer and closers)):
        pool = setups if setups else closers
        return max(pool, key=lambda p: (_avail(p) * _lev_skill(p), _avail(p), _lev_skill(p))).player.mlbam_id
    if lev <= 0.35 and (not close_game) and longs:
        return max(longs, key=lambda p: (_avail(p) * p.stamina_pitches, _avail(p), p.stamina_pitches)).player.mlbam_id

    pool = middles or setups or (closers if allow_closer else []) or bullpen
    return max(
        pool,
        key=lambda p: (
            _avail(p) * _lev_skill(p),
            _avail(p),
            _lev_skill(p),
            _avail(p) * p.stamina_pitches,
            p.stamina_pitches,
        ),
    ).player.mlbam_id


def _sigmoid(x: float) -> float:
    # stable logistic-ish curve for probabilities
    try:
        if x >= 8:
            return 0.9997
        if x <= -8:
            return 0.0003
        import math

        return 1.0 / (1.0 + math.exp(-float(x)))
    except Exception:
        return 0.5


def _select_pitcher_v2(roster: TeamRoster, state: GameState, rng: random.Random) -> int:
    """V2 manager hook with probabilistic pull decisions.

    Goal: better calibration of starter outs by allowing realistic variance
    around the hook, and reacting to fatigue / TTO / leverage.
    """
    team_id = roster.team.team_id
    current = state.current_pitcher_by_team.get(team_id)
    starter = roster.lineup.pitcher.player.mlbam_id
    if current is None:
        current = starter

    cfg = getattr(state, "config", None)
    overrides = getattr(cfg, "manager_pitching_overrides", {}) if cfg is not None else {}
    if not isinstance(overrides, dict):
        overrides = {}

    def _ov_f(key: str, default: float) -> float:
        v = overrides.get(key)
        try:
            if v is None:
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    def _ov_i(key: str, default: int) -> int:
        v = overrides.get(key)
        try:
            if v is None:
                return int(default)
            return int(v)
        except Exception:
            return int(default)

    # Tuning knobs (defaults are the current promoted behavior)
    hook_jitter_pitches = max(0, _ov_i("hook_jitter_pitches", 0))
    starter_hook_add_pitches = _ov_i("starter_hook_add_pitches", 0)
    starter_hook_spread = max(1.0, _ov_f("starter_hook_spread", 6.0))
    starter_pull_bias = _clamp01(_ov_f("starter_pull_bias", 0.15))
    starter_hard_cap_buffer = max(0, _ov_i("starter_hard_cap_buffer", 28))
    starter_third_time_scale = max(0.0, _ov_f("starter_third_time_scale", 1.0))
    # Starter leash-break controls (only relevant while inning <= starter_min_innings).
    # Defaults preserve the existing behavior (i.e., "always keep" within leash unless blowout).
    starter_leash_pc_buffer = max(0, _ov_i("starter_leash_pc_buffer", 20))
    starter_leash_lev_max = _clamp01(_ov_f("starter_leash_lev_max", 1.0))
    starter_leash_runner_max = _clamp01(_ov_f("starter_leash_runner_max", 1.0))
    starter_leash_tto_max = max(0.0, _ov_f("starter_leash_tto_max", 99.0))
    # Promoted default: rare large negative hook shift to prevent pathological
    # overconfidence in starter outs-at-line.
    starter_short_start_prob = _clamp01(_ov_f("starter_short_start_prob", 0.06))
    starter_short_start_hook_delta = _ov_i("starter_short_start_hook_delta", -32)
    reliever_hook_spread = max(1.0, _ov_f("reliever_hook_spread", 5.5))
    reliever_pull_bias = _clamp01(_ov_f("reliever_pull_bias", 0.20))
    reliever_hard_cap_buffer = max(0, _ov_i("reliever_hard_cap_buffer", 18))

    pc = int(state.pitcher_pitch_count.get(int(current), 0) or 0)
    bf = int(state.pitcher_batters_faced.get(int(current), 0) or 0)
    half = state.half
    outs = int(half.outs) if half else 0
    bases = half.bases if half else BaseState.EMPTY
    fielding_diff = _score_diff_from_fielding(state)
    lev = _leverage_index(state.inning, state.top, outs, bases, fielding_diff)

    def _runner_pressure(b: BaseState) -> float:
        return {
            BaseState.EMPTY: 0.0,
            BaseState.FIRST: 0.25,
            BaseState.SECOND: 0.35,
            BaseState.THIRD: 0.40,
            BaseState.FIRST_SECOND: 0.55,
            BaseState.FIRST_THIRD: 0.60,
            BaseState.SECOND_THIRD: 0.68,
            BaseState.LOADED: 0.80,
        }[b]

    def _avail_pitcher(p) -> float:
        try:
            return _clamp01(float(getattr(p, "availability_mult", 1.0) or 1.0))
        except Exception:
            return 1.0

    if int(current) == int(starter):
        starter_prof = roster.lineup.pitcher
        # Effective hook blends manager tendency + pitcher stamina + availability.
        base_hook = int(roster.manager.pull_starter_pitch_count)
        stamina_hook = int(getattr(starter_prof, "stamina_pitches", base_hook) or base_hook)
        avail = _avail_pitcher(starter_prof)
        eff_hook = int(min(base_hook, stamina_hook) - round((1.0 - avail) * 10.0))

        if starter_hook_add_pitches:
            eff_hook = int(eff_hook + int(starter_hook_add_pitches))

        # Per-game hook adjustment to inject realistic variance.
        # This can include (a) small uniform jitter, and (b) rare large negative shifts
        # to represent "short starts" (quick hooks due to ineffectiveness/injury).
        if hook_jitter_pitches > 0 or starter_short_start_prob > 0:
            j = state.manager_hook_jitter.get(int(starter))
            if j is None:
                j = int(rng.randint(-int(hook_jitter_pitches), int(hook_jitter_pitches))) if hook_jitter_pitches > 0 else 0
                if starter_short_start_prob > 0 and rng.random() < float(starter_short_start_prob):
                    j = int(j + int(starter_short_start_hook_delta))
                state.manager_hook_jitter[int(starter)] = int(j)
            eff_hook = int(eff_hook + int(j))

        eff_hook = max(45, min(120, eff_hook))

        in_leash_window = int(state.inning) <= max(1, int(roster.manager.starter_min_innings))
        blowout = abs(int(fielding_diff)) >= int(roster.manager.starter_blowup_run_diff)
        runner_pressure = _runner_pressure(bases)
        third_time = bf >= 18
        tto = float(bf) / 9.0 if bf > 0 else 0.0

        # Keep starter early unless extreme. Allow a "leash break" in high-pressure spots
        # (high leverage, runners, or 3rd time through) via tuning overrides.
        if (
            in_leash_window
            and (not blowout)
            and pc < (eff_hook + int(starter_leash_pc_buffer))
            and float(lev) < float(starter_leash_lev_max)
            and float(runner_pressure) < float(starter_leash_runner_max)
            and float(tto) < float(starter_leash_tto_max)
        ):
            state.current_pitcher_by_team[team_id] = int(current)
            return int(current)

        # Build pull probability around hook.
        # Centered at eff_hook, sharper when late/high leverage.
        x = (float(pc) - float(eff_hook)) / float(starter_hook_spread)
        x += 0.9 * float(lev)
        x += 0.6 * float(runner_pressure)
        if third_time:
            x += float(roster.manager.pull_starter_third_time_penalty) * 8.0 * float(starter_third_time_scale)
        if blowout:
            x -= 0.8  # leave him in during blowouts

        p_pull = _clamp01(_sigmoid(x) - float(starter_pull_bias))

        # Hard cap: always pull if very deep.
        if pc >= eff_hook + int(starter_hard_cap_buffer):
            p_pull = 1.0

        # Mid-inning: be more conservative unless there's pressure.
        if outs > 0 and lev < 0.65 and runner_pressure < 0.55 and pc < eff_hook + 18:
            p_pull = 0.0

        if rng.random() < p_pull:
            rel = _select_reliever_v2(roster, lev, state.inning, fielding_diff, overrides)
            state.current_pitcher_by_team[team_id] = int(rel)
            return int(rel)

        state.current_pitcher_by_team[team_id] = int(current)
        return int(current)

    # Reliever management
    prof = _pitcher_profile(roster, int(current))
    stamina = int(getattr(prof, "stamina_pitches", 25) or 25)
    avail = _avail_pitcher(prof)
    eff_stamina = int(max(12, min(65, stamina - round((1.0 - avail) * 8.0))))

    x = (float(pc) - float(eff_stamina)) / float(reliever_hook_spread)
    x += 0.8 * float(lev)
    x += 0.6 * float(_runner_pressure(bases))
    # If low leverage and blowout-ish, keep the arm in.
    if abs(int(fielding_diff)) >= 5 and lev < 0.40:
        x -= 0.9

    p_pull = _clamp01(_sigmoid(x) - float(reliever_pull_bias))
    if pc >= eff_stamina + int(reliever_hard_cap_buffer):
        p_pull = 1.0

    if rng.random() < p_pull:
        rel = _select_reliever_v2(roster, lev, state.inning, fielding_diff, overrides)
        state.current_pitcher_by_team[team_id] = int(rel)
        return int(rel)

    state.current_pitcher_by_team[team_id] = int(current)
    return int(current)


def _adjust_pitcher_day_rates_v2(
    roster: TeamRoster,
    state: GameState,
    pitcher_id: int,
    pitcher_prof,
    day_rates: Optional[Dict[str, float]],
) -> Optional[Dict[str, float]]:
    """Apply fatigue/TTO pressure to per-game sampled pitcher day rates.

    Keeps changes local to this call (does not mutate state.pitcher_day_rates).
    """
    try:
        pc = int(state.pitcher_pitch_count.get(int(pitcher_id), 0) or 0)
        bf = int(state.pitcher_batters_faced.get(int(pitcher_id), 0) or 0)
    except Exception:
        pc, bf = 0, 0

    is_starter = int(roster.lineup.pitcher.player.mlbam_id) == int(pitcher_id)

    cfg = getattr(state, "config", None)
    overrides = getattr(cfg, "manager_pitching_overrides", {}) if cfg is not None else {}
    if not isinstance(overrides, dict):
        overrides = {}

    def _ov_f(key: str, default: float) -> float:
        v = overrides.get(key)
        try:
            if v is None:
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    # Optional reliever quality shrink: blend reliever day rates back toward
    # neutral priors to prevent overconfident bullpen strength when usage rises.
    # Off by default.
    reliever_rate_shrink = _clamp01(_ov_f("reliever_rate_shrink", 0.0))
    stamina = int(getattr(pitcher_prof, "stamina_pitches", 90 if is_starter else 25) or (90 if is_starter else 25))
    stamina = max(10, stamina)
    fatigue = max(0.0, float(pc - stamina) / float(max(1, stamina)))

    # Optional bullpen tax: as the team uses more bullpen pitches in-game,
    # apply a small additional fatigue-like penalty to reliever effectiveness.
    # This is off by default to preserve current behavior.
    bullpen_tax_scale = max(0.0, _ov_f("bullpen_tax_scale", 0.0))
    bullpen_tax_pitches = max(1.0, _ov_f("bullpen_tax_pitches", 140.0))
    bullpen_tax = 0.0
    if (not is_starter) and bullpen_tax_scale > 0:
        try:
            bullpen_ids = [int(p.player.mlbam_id) for p in (roster.lineup.bullpen or [])]
            bullpen_pitches = float(sum(int(state.pitcher_pitch_count.get(pid, 0) or 0) for pid in bullpen_ids))
            bullpen_tax = float(bullpen_tax_scale) * min(1.0, bullpen_pitches / float(bullpen_tax_pitches))
        except Exception:
            bullpen_tax = 0.0

    fatigue = min(2.0, float(fatigue) + float(bullpen_tax))
    third_time = (bf >= 18) if is_starter else False
    tto_pen = float(getattr(roster.manager, "pull_starter_third_time_penalty", 0.0) or 0.0)

    # Convert fatigue/TTO into small rate multipliers.
    # These are intentionally conservative.
    k_mult = 1.0
    bb_mult = 1.0
    hr_mult = 1.0
    ip_mult = 1.0

    if third_time and tto_pen > 0:
        k_mult *= max(0.85, 1.0 - 0.9 * tto_pen)
        bb_mult *= min(1.25, 1.0 + 1.1 * tto_pen)
        hr_mult *= min(1.30, 1.0 + 1.0 * tto_pen)
        ip_mult *= min(1.20, 1.0 + 0.8 * tto_pen)

    if fatigue > 0:
        k_mult *= max(0.80, 1.0 - 0.10 * fatigue)
        bb_mult *= min(1.30, 1.0 + 0.14 * fatigue)
        hr_mult *= min(1.35, 1.0 + 0.16 * fatigue)
        ip_mult *= min(1.25, 1.0 + 0.10 * fatigue)

    if (k_mult, bb_mult, hr_mult, ip_mult) == (1.0, 1.0, 1.0, 1.0) and not ((not is_starter) and reliever_rate_shrink > 0):
        return day_rates

    pr = dict(day_rates or {})

    def _base(key: str, fallback: float) -> float:
        try:
            v = pr.get(key)
            if isinstance(v, (int, float)):
                return float(v)
        except Exception:
            pass
        return float(fallback)

    pr["k_rate"] = _clamp01(_base("k_rate", float(pitcher_prof.k_rate)) * k_mult)
    pr["bb_rate"] = _clamp01(_base("bb_rate", float(pitcher_prof.bb_rate)) * bb_mult)
    pr["hr_rate"] = _clamp01(_base("hr_rate", float(pitcher_prof.hr_rate)) * hr_mult)
    pr["inplay_hit_rate"] = _clamp01(_base("inplay_hit_rate", float(pitcher_prof.inplay_hit_rate)) * ip_mult)
    # hbp_rate left unchanged

    if (not is_starter) and reliever_rate_shrink > 0:
        prior_k = _clamp01(_ov_f("reliever_prior_k_rate", 0.24))
        prior_bb = _clamp01(_ov_f("reliever_prior_bb_rate", 0.08))
        prior_hr = _clamp01(_ov_f("reliever_prior_hr_rate", 0.03))
        prior_inplay = _clamp01(_ov_f("reliever_prior_inplay_hit_rate", 0.27))
        w = 1.0 - float(reliever_rate_shrink)
        try:
            pr["k_rate"] = _clamp01(float(pr.get("k_rate", prior_k)) * w + prior_k * (1.0 - w))
            pr["bb_rate"] = _clamp01(float(pr.get("bb_rate", prior_bb)) * w + prior_bb * (1.0 - w))
            pr["hr_rate"] = _clamp01(float(pr.get("hr_rate", prior_hr)) * w + prior_hr * (1.0 - w))
            pr["inplay_hit_rate"] = _clamp01(float(pr.get("inplay_hit_rate", prior_inplay)) * w + prior_inplay * (1.0 - w))
        except Exception:
            pass
    return pr


def _pitcher_profile(roster: TeamRoster, pitcher_id: int):
    pid = int(pitcher_id or 0)
    try:
        cache = getattr(roster, "_pitcher_by_id", None)
        if not isinstance(cache, dict):
            cache = {}
            try:
                sp = roster.lineup.pitcher
                cache[int(sp.player.mlbam_id)] = sp
            except Exception:
                pass
            try:
                for p in (roster.lineup.bullpen or []):
                    try:
                        cache[int(p.player.mlbam_id)] = p
                    except Exception:
                        continue
            except Exception:
                pass
            setattr(roster, "_pitcher_by_id", cache)
        prof = cache.get(pid)
        if prof is not None:
            return prof
    except Exception:
        pass

    if roster.lineup.pitcher.player.mlbam_id == pid:
        return roster.lineup.pitcher
    for p in roster.lineup.bullpen:
        if p.player.mlbam_id == pid:
            return p
    return roster.lineup.pitcher


def _batter_profile(roster: TeamRoster, batter_index: int):
    return roster.lineup.batters[batter_index % len(roster.lineup.batters)]


def _batter_profile_by_id(roster: TeamRoster, batter_id: int):
    bid = int(batter_id or 0)
    if bid <= 0:
        return None
    try:
        cache = getattr(roster, "_batter_by_id", None)
        if not isinstance(cache, dict):
            cache = {}
            for b in (roster.lineup.batters or []):
                try:
                    cache[int(b.player.mlbam_id)] = b
                except Exception:
                    continue
            for b in (roster.lineup.bench or []):
                try:
                    cache[int(b.player.mlbam_id)] = b
                except Exception:
                    continue
            setattr(roster, "_batter_by_id", cache)
        prof = cache.get(bid)
        if prof is not None:
            return prof
    except Exception:
        return None
    return None


def simulate_game(away: TeamRoster, home: TeamRoster, config: Optional[GameConfig] = None) -> GameResult:
    cfg = config or GameConfig()
    rng = random.Random(cfg.rng_seed)
    st = StatsTracker()
    state = GameState(away=away, home=home, config=cfg)

    bip_baserunning = bool(getattr(cfg, "bip_baserunning", True))
    bip_1b_p2_scores_mult = _clamp(float(getattr(cfg, "bip_1b_p2_scores_mult", 1.0) or 1.0), 0.5, 1.5)
    bip_2b_p1_scores_mult = _clamp(float(getattr(cfg, "bip_2b_p1_scores_mult", 1.0) or 1.0), 0.5, 1.5)
    bip_1b_p1_to_3b_rate = _clamp(float(getattr(cfg, "bip_1b_p1_to_3b_rate", 0.24) or 0.24), 0.0, 0.8)
    bip_ground_rbi_out_rate = _clamp(float(getattr(cfg, "bip_ground_rbi_out_rate", 0.18) or 0.18), 0.0, 0.8)
    bip_out_2b_to_3b_rate = _clamp(float(getattr(cfg, "bip_out_2b_to_3b_rate", 0.24) or 0.24), 0.0, 0.8)
    bip_out_1b_to_2b_rate = _clamp(float(getattr(cfg, "bip_out_1b_to_2b_rate", 0.14) or 0.14), 0.0, 0.8)
    bip_misc_advance_pitch_rate = _clamp(float(getattr(cfg, "bip_misc_advance_pitch_rate", 0.004) or 0.004), 0.0, 0.05)
    bip_roe_rate = _clamp(float(getattr(cfg, "bip_roe_rate", 0.012) or 0.012), 0.0, 0.1)
    bip_fc_rate = _clamp(float(getattr(cfg, "bip_fc_rate", 0.04) or 0.04), 0.0, 0.2)
    bip_fc_runner_on_3b_score_rate = _clamp(float(getattr(cfg, "bip_fc_runner_on_3b_score_rate", 0.0) or 0.0), 0.0, 1.0)

    # Optional: sample per-game pitcher rates (starter + bullpen) once per game.
    # This injects uncertainty into K/BB/HR/in-play hit rates while keeping the
    # inning-by-inning pitch model structure intact.
    if bool(getattr(cfg, "pitcher_rate_sampling", True)):
        pdo = getattr(cfg, "pitcher_distribution_overrides", None)
        if not isinstance(pdo, dict):
            pdo = {}
        try:
            allowed = set(getattr(PitcherDistributionConfig, "__dataclass_fields__", {}).keys())
            pdo_safe = {k: v for k, v in pdo.items() if k in allowed}
            dist_cfg = PitcherDistributionConfig(**pdo_safe)
        except Exception:
            dist_cfg = PitcherDistributionConfig()

        for roster in (away, home):
            pitchers = [roster.lineup.pitcher] + list(roster.lineup.bullpen or [])
            for p in pitchers:
                pid = int(getattr(p.player, "mlbam_id", 0) or 0)
                if pid <= 0:
                    continue
                if pid in state.pitcher_day_rates:
                    continue
                state.pitcher_day_rates[pid] = sample_pitcher_day_rates(rng, p, dist_cfg).as_dict()

    # Pitch model config (tunable via GameConfig.pitch_model_overrides)
    pmo = getattr(cfg, "pitch_model_overrides", None)
    if not isinstance(pmo, dict):
        pmo = {}
    try:
        allowed = set(getattr(PitchModelConfig, "__dataclass_fields__", {}).keys())
        pmo_safe = {k: v for k, v in pmo.items() if k in allowed}
        pitch_cfg = PitchModelConfig(**pmo_safe)
    except Exception:
        pitch_cfg = PitchModelConfig()

    # Optional: per-game run environment latent factor.
    # Fold into the same hooks as weather/park so it stays overrideable and low-risk.
    run_env_sigma = float(getattr(pitch_cfg, "run_env_sigma", 0.0) or 0.0)
    run_env_clamp_min = float(getattr(pitch_cfg, "run_env_clamp_min", 0.75) or 0.75)
    run_env_clamp_max = float(getattr(pitch_cfg, "run_env_clamp_max", 1.33) or 1.33)
    run_env_hr_weight = float(getattr(pitch_cfg, "run_env_hr_weight", 1.0) or 1.0)
    run_env_inplay_hit_weight = float(getattr(pitch_cfg, "run_env_inplay_hit_weight", 1.0) or 1.0)
    run_env_xb_share_weight = float(getattr(pitch_cfg, "run_env_xb_share_weight", 1.0) or 1.0)

    run_env_base = _clamp(_lognormal_mean1(rng, run_env_sigma), run_env_clamp_min, run_env_clamp_max)
    run_env_hr_mult = _pow_mult(run_env_base, run_env_hr_weight)
    run_env_inplay_hit_mult = _pow_mult(run_env_base, run_env_inplay_hit_weight)
    run_env_xb_share_mult = _pow_mult(run_env_base, run_env_xb_share_weight)

    pbp_mode = str(getattr(cfg, "pbp", "off") or "off").lower()
    if pbp_mode not in ("off", "pa", "pitch"):
        pbp_mode = "off"
    pbp_max = int(getattr(cfg, "pbp_max_events", 0) or 0)
    pbp: List[Dict[str, Any]] = []
    pbp_truncated = False

    def _log(ev: Dict[str, Any]) -> None:
        nonlocal pbp_truncated
        if pbp_mode == "off":
            return
        if pbp_max > 0 and len(pbp) >= pbp_max:
            pbp_truncated = True
            return
        pbp.append(ev)

    wm = (cfg.weather.multipliers() if getattr(cfg, "weather", None) is not None else None)
    pm = (cfg.park.multipliers() if getattr(cfg, "park", None) is not None else None)
    um = (cfg.umpire.multipliers() if getattr(cfg, "umpire", None) is not None else None)

    weather_hr_mult = float(getattr(wm, "hr_mult", 1.0) if wm else 1.0)
    weather_inplay_hit_mult = float(getattr(wm, "inplay_hit_mult", 1.0) if wm else 1.0)
    weather_xb_share_mult = float(getattr(wm, "xb_share_mult", 1.0) if wm else 1.0)

    # Weather sensitivity weights.
    w_hr_w = float(getattr(cfg, "weather_hr_weight", 1.0) or 1.0)
    w_ip_w = float(getattr(cfg, "weather_inplay_hit_weight", 1.0) or 1.0)
    w_xb_w = float(getattr(cfg, "weather_xb_share_weight", 1.0) or 1.0)
    weather_hr_mult = _clamp(_pow_mult(weather_hr_mult, w_hr_w), 0.75, 1.35)
    weather_inplay_hit_mult = _clamp(_pow_mult(weather_inplay_hit_mult, w_ip_w), 0.90, 1.10)
    weather_xb_share_mult = _clamp(_pow_mult(weather_xb_share_mult, w_xb_w), 0.92, 1.08)

    # Apply run environment latent multiplier via the weather hooks.
    weather_hr_mult *= float(run_env_hr_mult)
    weather_inplay_hit_mult *= float(run_env_inplay_hit_mult)
    weather_xb_share_mult *= float(run_env_xb_share_mult)

    park_hr_mult = float(getattr(pm, "hr_mult", 1.0) if pm else 1.0)
    park_inplay_hit_mult = float(getattr(pm, "inplay_hit_mult", 1.0) if pm else 1.0)
    park_xb_share_mult = float(getattr(pm, "xb_share_mult", 1.0) if pm else 1.0)

    # Park sensitivity weights.
    p_hr_w = float(getattr(cfg, "park_hr_weight", 1.0) or 1.0)
    p_ip_w = float(getattr(cfg, "park_inplay_hit_weight", 1.0) or 1.0)
    p_xb_w = float(getattr(cfg, "park_xb_share_weight", 1.0) or 1.0)
    park_hr_mult = _clamp(_pow_mult(park_hr_mult, p_hr_w), 0.85, 1.15)
    park_inplay_hit_mult = _clamp(_pow_mult(park_inplay_hit_mult, p_ip_w), 0.90, 1.10)
    park_xb_share_mult = _clamp(_pow_mult(park_xb_share_mult, p_xb_w), 0.92, 1.08)

    umpire_called_strike_mult = float(getattr(um, "called_strike_mult", 1.0) if um else 1.0)

    away_inning_runs: List[int] = []
    home_inning_runs: List[int] = []

    # Per-game cache: pitcher_id -> (pitch_types, cumulative_weights, total)
    pitch_cdf_cache: Dict[int, Tuple[List[PitchType], List[float], float]] = {}

    def start_half_inning():
        batting = state.batting_roster().team
        fielding = state.fielding_roster().team
        try:
            next_idx = int(state.next_batter_index_by_team.get(int(batting.team_id), 0) or 0)
        except Exception:
            next_idx = 0
        state.runner_reach_source_by_id.clear()
        state.half = InningHalfState(
            batting_team=batting,
            fielding_team=fielding,
            outs=0,
            bases=BaseState.EMPTY,
            runner_on_1b=0,
            runner_on_2b=0,
            runner_on_3b=0,
            runs_scored=0,
            next_batter_index=next_idx,
        )
        if pbp_mode == "pitch":
            _log(
                {
                    "type": "HALF_START",
                    "inning": int(state.inning),
                    "half": "top" if state.top else "bottom",
                    "batting_team_id": int(batting.team_id),
                    "fielding_team_id": int(fielding.team_id),
                    "score": {"away": int(state.away_score), "home": int(state.home_score)},
                }
            )

    def end_half_inning():
        # Persist batting order state for the team that just hit.
        try:
            if state.half is not None:
                tid = int(state.half.batting_team.team_id)
                state.next_batter_index_by_team[tid] = int(state.half.next_batter_index)
        except Exception:
            pass

        # apply half runs to score
        inning_idx = state.inning - 1
        # Ensure arrays are long enough
        while len(away_inning_runs) <= inning_idx:
            away_inning_runs.append(0)
        while len(home_inning_runs) <= inning_idx:
            home_inning_runs.append(0)

        if state.top:
            state.away_score += state.half.runs_scored
            away_inning_runs[inning_idx] += state.half.runs_scored
        else:
            state.home_score += state.half.runs_scored
            home_inning_runs[inning_idx] += state.half.runs_scored

        if pbp_mode == "pitch":
            _log(
                {
                    "type": "HALF_END",
                    "inning": int(state.inning),
                    "half": "top" if state.top else "bottom",
                    "runs": int(state.half.runs_scored),
                    "score": {"away": int(state.away_score), "home": int(state.home_score)},
                }
            )
        # flip
        finished_top = state.top
        state.top = not state.top
        if finished_top is False:
            # just finished bottom half
            state.inning += 1
        state.half = None
        state.pa = None

        return finished_top

    def record_run(batter_id: int, pitcher_id: int, runs: int):
        if runs <= 0:
            return
        # credit batter RBI heuristically (not perfect base-running attribution)
        br = st.batter_row(batter_id)
        br["RBI"] += runs
        pr = st.pitcher_row(pitcher_id)
        pr["R"] += float(runs)

    def record_runner_runs(runner_ids: List[int]) -> None:
        for rid in runner_ids:
            try:
                rr = int(rid or 0)
            except Exception:
                rr = 0
            if rr <= 0:
                continue
            st.batter_row(rr)["R"] += 1

    def charge_pitcher_runs(pitcher_id: int, runs: int) -> None:
        if runs <= 0:
            return
        st.pitcher_row(pitcher_id)["R"] += float(runs)

    innings_target = cfg.innings
    extra_innings_cap = max(0, int(getattr(cfg, "extra_innings", 0) or 0))
    max_innings = cfg.innings + extra_innings_cap
    allow_tied_final = bool(getattr(cfg, "allow_ties_after_max_innings", False))

    def game_over_after_half(finished_top: bool) -> bool:
        # After top half: if home is leading in regulation or extras, skip bottom.
        if finished_top:
            if state.inning >= innings_target and state.home_score > state.away_score:
                return True
            return False
        # After bottom half: if inning >= 9 and not tied, game over.
        if state.inning > innings_target:
            return state.home_score != state.away_score
        if state.inning == innings_target + 1:
            # we already incremented inning after finishing bottom
            return state.home_score != state.away_score
        # After completing bottom of inning N (meaning state.inning already advanced)
        if state.inning >= innings_target + 1 and state.home_score != state.away_score:
            return True
        return False

    while True:
        if state.half is None:
            start_half_inning()

        batting_roster = state.batting_roster()
        fielding_roster = state.fielding_roster()
        half = state.half

        batter_prof = _batter_profile(batting_roster, half.next_batter_index)
        prev_pitcher_id = state.current_pitcher_by_team.get(fielding_roster.team.team_id)

        mp = str(getattr(cfg, "manager_pitching", "legacy") or "legacy").lower()
        if mp not in ("off", "legacy", "v2"):
            mp = "legacy"

        if mp == "off":
            pitcher_id = int(fielding_roster.lineup.pitcher.player.mlbam_id)
            state.current_pitcher_by_team[fielding_roster.team.team_id] = int(pitcher_id)
        elif mp == "v2":
            pitcher_id = int(_select_pitcher_v2(fielding_roster, state, rng))
        else:
            pitcher_id = int(_select_pitcher_legacy(fielding_roster, state))

        pitcher_prof = _pitcher_profile(fielding_roster, pitcher_id)

        # Pre-compute (expensive) fatigue/TTO adjustments once per PA (not per pitch).
        pitcher_day_rates_eff = (
            _adjust_pitcher_day_rates_v2(fielding_roster, state, pitcher_id, pitcher_prof, state.pitcher_day_rates.get(pitcher_id))
            if mp == "v2"
            else state.pitcher_day_rates.get(pitcher_id)
        )

        if pbp_mode != "off" and prev_pitcher_id is not None and int(prev_pitcher_id) != int(pitcher_id):
            _log(
                {
                    "type": "PITCHING_CHANGE",
                    "inning": int(state.inning),
                    "half": "top" if state.top else "bottom",
                    "fielding_team_id": int(fielding_roster.team.team_id),
                    "from_pitcher_id": int(prev_pitcher_id),
                    "to_pitcher_id": int(pitcher_id),
                    "score": {"away": int(state.away_score), "home": int(state.home_score)},
                }
            )

        state.pa = PlateAppearanceState(
            batter_id=batter_prof.player.mlbam_id,
            pitcher_id=pitcher_id,
            pitch_count=0,
            pitches=([] if pbp_mode == "pitch" else None),
        )
        balls, strikes = 0, 0

        outs_before_pa = int(half.outs)
        bases_before_pa = str(half.bases.value)
        score_before_pa = {"away": int(state.away_score), "home": int(state.home_score)}

        # Simple stolen base attempt model (only 2B steals, no third/double steals).
        # Happens before the PA, so it should not count as a PA for the current batter.
        try:
            if int(half.outs) <= 1 and int(half.runner_on_1b) > 0 and int(half.runner_on_2b) == 0:
                rid = int(half.runner_on_1b)
                rprof = _batter_profile_by_id(batting_roster, rid)
                if rprof is not None:
                    ar = float(getattr(rprof, "sb_attempt_rate", 0.0) or 0.0)
                    sr = float(getattr(rprof, "sb_success_rate", 0.72) or 0.72)
                    ar = float(max(0.0, min(0.40, ar)))
                    sr = float(max(0.40, min(0.95, sr)))
                    if ar > 0.0 and rng.random() < ar:
                        if rng.random() < sr:
                            # SB
                            st.batter_row(rid)["SB"] += 1
                            _set_half_bases_from_runners(half, 0, rid, int(half.runner_on_3b))
                            _sync_runner_reach_sources(state.runner_reach_source_by_id, half)
                            if pbp_mode in ("pa", "pitch"):
                                _log(
                                    {
                                        "type": "SB",
                                        "inning": int(state.inning),
                                        "half": "top" if state.top else "bottom",
                                        "batting_team_id": int(batting_roster.team.team_id),
                                        "runner_id": int(rid),
                                        "to": "2B",
                                        "outs": int(half.outs),
                                        "bases": str(half.bases.value),
                                        "score": {"away": int(state.away_score), "home": int(state.home_score)},
                                    }
                                )
                        else:
                            # CS
                            st.batter_row(rid)["CS"] += 1
                            _set_half_bases_from_runners(half, 0, int(half.runner_on_2b), int(half.runner_on_3b))
                            _sync_runner_reach_sources(state.runner_reach_source_by_id, half)
                            half.outs += 1
                            if pbp_mode in ("pa", "pitch"):
                                _log(
                                    {
                                        "type": "CS",
                                        "inning": int(state.inning),
                                        "half": "top" if state.top else "bottom",
                                        "batting_team_id": int(batting_roster.team.team_id),
                                        "runner_id": int(rid),
                                        "outs": int(half.outs),
                                        "bases": str(half.bases.value),
                                        "score": {"away": int(state.away_score), "home": int(state.home_score)},
                                    }
                                )

                            if half.outs >= 3:
                                finished_top = end_half_inning()
                                if game_over_after_half(finished_top):
                                    break
                                if allow_tied_final and state.inning > max_innings:
                                    break
                                if state.inning > innings_target and state.home_score != state.away_score:
                                    break
                                continue
        except Exception:
            pass

        batter_id = int(batter_prof.player.mlbam_id)
        # Mark PA
        br = st.batter_row(batter_id)
        pr = st.pitcher_row(pitcher_id)
        br["PA"] += 1
        pr["BF"] += 1.0
        state.pitcher_batters_faced[pitcher_id] = state.pitcher_batters_faced.get(pitcher_id, 0) + 1
        starter_pitcher_id = int(getattr(getattr(fielding_roster.lineup.pitcher, "player", None), "mlbam_id", 0) or 0)

        pa_ended = False
        pa_result: Optional[str] = None

        # Pitch-type sampling CDF (cached per pitcher for this game)
        cdf_entry = pitch_cdf_cache.get(int(pitcher_id))
        if cdf_entry is None:
            cdf_entry = _build_weight_cdf(getattr(pitcher_prof, "arsenal", {}) or {})
            pitch_cdf_cache[int(pitcher_id)] = cdf_entry
        pitch_types, pitch_cdf, pitch_total = cdf_entry

        # Precompute matchup rates once per PA (platoon + per-game day rates)
        pit_hand = _hand(getattr(pitcher_prof.player, "throw_side", getattr(pitcher_prof, "throw_side", "R")))
        eff_bat = _eff_bat_side(
            getattr(batter_prof.player, "bat_side", getattr(batter_prof, "bat_side", "R")),
            getattr(pitcher_prof.player, "throw_side", getattr(pitcher_prof, "throw_side", "R")),
        )
        b_mults = getattr(batter_prof, "platoon_mult_vs_lhp", {}) if pit_hand == "L" else getattr(batter_prof, "platoon_mult_vs_rhp", {})
        p_mults = getattr(pitcher_prof, "platoon_mult_vs_lhb", {}) if eff_bat == "L" else getattr(pitcher_prof, "platoon_mult_vs_rhb", {})

        batter_k = _clamp_rate(float(batter_prof.k_rate) * _mult_from_map(b_mults, "k"), 0.05, 0.55)
        batter_bb = _clamp_rate(float(batter_prof.bb_rate) * _mult_from_map(b_mults, "bb"), 0.01, 0.22)
        batter_hr = _clamp_rate(float(batter_prof.hr_rate) * _mult_from_map(b_mults, "hr"), 0.002, 0.12)
        batter_inplay = _clamp_rate(float(batter_prof.inplay_hit_rate) * _mult_from_map(b_mults, "inplay"), 0.10, 0.45)

        try:
            mm = getattr(batter_prof, "vs_pitcher_hr_mult", None)
            if isinstance(mm, dict):
                mult = mm.get(int(pitcher_id))
                if isinstance(mult, (int, float)):
                    batter_hr = _clamp_rate(float(batter_hr) * float(mult), 0.002, 0.12)
        except Exception:
            pass

        prates = pitcher_day_rates_eff or {}
        base_pitcher_k = float(prates.get("k_rate", pitcher_prof.k_rate))
        base_pitcher_bb = float(prates.get("bb_rate", pitcher_prof.bb_rate))
        base_pitcher_hr = float(prates.get("hr_rate", pitcher_prof.hr_rate))
        base_pitcher_inplay = float(prates.get("inplay_hit_rate", pitcher_prof.inplay_hit_rate))

        pitcher_k = _clamp_rate(base_pitcher_k * _mult_from_map(p_mults, "k"), 0.05, 0.60)
        pitcher_bb = _clamp_rate(base_pitcher_bb * _mult_from_map(p_mults, "bb"), 0.01, 0.25)
        pitcher_hr = _clamp_rate(base_pitcher_hr * _mult_from_map(p_mults, "hr"), 0.002, 0.14)
        pitcher_inplay = _clamp_rate(base_pitcher_inplay * _mult_from_map(p_mults, "inplay"), 0.10, 0.45)
        pitcher_hbp = float(prates.get("hbp_rate", pitcher_prof.hbp_rate))

        batter_hbp = float(getattr(batter_prof, "hbp_rate", 0.008) or 0.008)
        batter_xb_share = float(getattr(batter_prof, "xb_hit_share", 0.28) or 0.28)
        batter_triple_share = float(getattr(batter_prof, "triple_share_of_xb", 0.12) or 0.12)

        batter_bb_gb = float(getattr(batter_prof, "bb_gb_rate", 0.44))
        batter_bb_fb = float(getattr(batter_prof, "bb_fb_rate", 0.25))
        batter_bb_ld = float(getattr(batter_prof, "bb_ld_rate", 0.20))
        batter_bb_pu = float(getattr(batter_prof, "bb_pu_rate", 0.11))
        batter_bb_n = int(getattr(batter_prof, "bb_inplay_n", 0) or 0)

        pitcher_bb_gb = float(getattr(pitcher_prof, "bb_gb_rate", 0.44))
        pitcher_bb_fb = float(getattr(pitcher_prof, "bb_fb_rate", 0.25))
        pitcher_bb_ld = float(getattr(pitcher_prof, "bb_ld_rate", 0.20))
        pitcher_bb_pu = float(getattr(pitcher_prof, "bb_pu_rate", 0.11))
        pitcher_bb_n = int(getattr(pitcher_prof, "bb_inplay_n", 0) or 0)

        vs_pt = getattr(batter_prof, "vs_pitch_type", None) or {}
        try:
            _alpha_raw = getattr(pitch_cfg, "batter_pt_alpha", 0.5)
            alpha = 0.5 if _alpha_raw is None else float(_alpha_raw)
        except Exception:
            alpha = 0.5
        alpha = float(max(0.0, min(1.0, alpha)))

        try:
            _scale_raw = getattr(pitch_cfg, "batter_pt_scale", 1.0)
            pt_scale = 1.0 if _scale_raw is None else float(_scale_raw)
        except Exception:
            pt_scale = 1.0
        pt_scale = float(max(0.0, min(2.0, pt_scale)))
        p_whiff_map = getattr(pitcher_prof, "pitch_type_whiff_mult", None) or {}
        p_inplay_map = getattr(pitcher_prof, "pitch_type_inplay_mult", None) or {}

        for _ in range(cfg.max_pitches_per_pa):
            pitch_num = int(state.pa.pitch_count) + 1
            count_before = (int(balls), int(strikes))
            outs_before_pitch = int(half.outs)
            bases_before_pitch = str(half.bases.value)
            pitch_type = _sample_weight_cdf(rng, pitch_types, pitch_cdf, pitch_total, PitchType.FF)
            raw_pt_mult = float(vs_pt.get(pitch_type, 1.0))
            try:
                raw_pt_mult = float(max(0.4, min(1.6, raw_pt_mult)))
            except Exception:
                raw_pt_mult = 1.0

            # Optional: scale raw pitch-type multipliers around 1.0 before applying alpha shrink.
            raw_pt_mult = float(1.0 + pt_scale * (raw_pt_mult - 1.0))
            raw_pt_mult = float(max(0.4, min(1.6, raw_pt_mult)))
            batter_pt_mult = float(1.0 + alpha * (raw_pt_mult - 1.0))
            pitch_weather_hr_mult = float(weather_hr_mult) * _hr_context_mult(
                pitch_cfg,
                int(state.inning),
                half,
                int(pitcher_id),
                int(starter_pitcher_id),
                state.runner_reach_source_by_id,
            )
            has_runners_on = bool(int(half.runner_on_1b or 0) or int(half.runner_on_2b or 0) or int(half.runner_on_3b or 0))
            is_reliever = int(starter_pitcher_id or 0) > 0 and int(pitcher_id or 0) != int(starter_pitcher_id or 0)
            pitch = simulate_pitch(
                rng=rng,
                cfg=pitch_cfg,
                pitch_type=pitch_type,
                pitcher_whiff_mult=float(p_whiff_map.get(pitch_type, 1.0)),
                pitcher_inplay_mult=float(p_inplay_map.get(pitch_type, 1.0)),
                weather_hr_mult=pitch_weather_hr_mult,
                weather_inplay_hit_mult=weather_inplay_hit_mult,
                weather_xb_share_mult=weather_xb_share_mult,
                park_hr_mult=park_hr_mult,
                park_inplay_hit_mult=park_inplay_hit_mult,
                park_xb_share_mult=park_xb_share_mult,
                umpire_called_strike_mult=umpire_called_strike_mult,
                count=(balls, strikes),
                has_runners_on=has_runners_on,
                is_reliever=is_reliever,
                batter_k_rate=batter_k,
                batter_bb_rate=batter_bb,
                batter_hbp_rate=batter_hbp,
                batter_hr_rate=batter_hr,
                batter_inplay_hit_rate=batter_inplay,
                batter_xb_hit_share=batter_xb_share,
                batter_pt_mult=batter_pt_mult,
                batter_triple_share_of_xb=batter_triple_share,
                pitcher_k_rate=pitcher_k,
                pitcher_bb_rate=pitcher_bb,
                pitcher_hbp_rate=pitcher_hbp,
                pitcher_hr_rate=pitcher_hr,
                pitcher_inplay_hit_rate=pitcher_inplay,
                batter_bb_gb_rate=batter_bb_gb,
                batter_bb_fb_rate=batter_bb_fb,
                batter_bb_ld_rate=batter_bb_ld,
                batter_bb_pu_rate=batter_bb_pu,
                batter_bb_inplay_n=batter_bb_n,
                pitcher_bb_gb_rate=pitcher_bb_gb,
                pitcher_bb_fb_rate=pitcher_bb_fb,
                pitcher_bb_ld_rate=pitcher_bb_ld,
                pitcher_bb_pu_rate=pitcher_bb_pu,
                pitcher_bb_inplay_n=pitcher_bb_n,
            )

            state.pa.pitch_count += 1
            if state.pa.pitches is not None:
                state.pa.pitches.append(pitch)

            # pitch count
            state.pitcher_pitch_count[pitcher_id] = state.pitcher_pitch_count.get(pitcher_id, 0) + 1
            pr["P"] += 1.0

            if pbp_mode == "pitch":
                _log(
                    {
                        "type": "PITCH",
                        "inning": int(state.inning),
                        "half": "top" if state.top else "bottom",
                        "pitch_num": int(pitch_num),
                        "outs": int(outs_before_pitch),
                        "bases": bases_before_pitch,
                        "count": {"balls": int(count_before[0]), "strikes": int(count_before[1])},
                        "batting_team_id": int(batting_roster.team.team_id),
                        "fielding_team_id": int(fielding_roster.team.team_id),
                        "batter_id": int(batter_prof.player.mlbam_id),
                        "pitcher_id": int(pitcher_id),
                        "pitch_type": str(pitch.pitch_type.value),
                        "call": str(pitch.call.value),
                        "in_play": bool(pitch.in_play),
                        "in_play_result": pitch.in_play_result,
                        "batted_ball_type": (pitch.batted_ball_type.value if pitch.batted_ball_type else None),
                        "score": {"away": int(state.away_score), "home": int(state.home_score)},
                    }
                )

            if (
                bip_baserunning
                and pitch.call in (PitchCall.BALL, PitchCall.CALLED_STRIKE, PitchCall.SWINGING_STRIKE, PitchCall.FOUL)
                and (int(half.runner_on_1b) > 0 or int(half.runner_on_2b) > 0 or int(half.runner_on_3b) > 0)
            ):
                misc_rate = float(bip_misc_advance_pitch_rate)
                if pitch.call == PitchCall.FOUL:
                    misc_rate *= 0.80
                elif pitch.call in (PitchCall.CALLED_STRIKE, PitchCall.SWINGING_STRIKE):
                    misc_rate *= 0.90
                if int(half.runner_on_3b) > 0:
                    misc_rate *= 1.20
                if rng.random() < _clamp01(misc_rate):
                    event_name = _pick_misc_pitch_advance_event(rng, pitch.call)
                    bases_before_misc = str(half.bases.value)
                    on1, on2, on3, misc_runs, misc_scorers = _advance_runners_one_base_with_runners(
                        half.runner_on_1b,
                        half.runner_on_2b,
                        half.runner_on_3b,
                    )
                    _set_half_bases_from_runners(half, on1, on2, on3)
                    _sync_runner_reach_sources(state.runner_reach_source_by_id, half)
                    half.runs_scored += misc_runs
                    charge_pitcher_runs(pitcher_id, misc_runs)
                    if misc_scorers:
                        record_runner_runs(misc_scorers)
                    if pbp_mode in ("pa", "pitch"):
                        _log(
                            {
                                "type": "RUNNER_ADVANCE",
                                "subtype": str(event_name),
                                "inning": int(state.inning),
                                "half": "top" if state.top else "bottom",
                                "batting_team_id": int(batting_roster.team.team_id),
                                "fielding_team_id": int(fielding_roster.team.team_id),
                                "pitch_num": int(pitch_num),
                                "pitch_call": str(pitch.call.value),
                                "bases_before": bases_before_misc,
                                "bases_after": str(half.bases.value),
                                "runs": int(misc_runs),
                                "score": {"away": int(state.away_score), "home": int(state.home_score)},
                            }
                        )

            if pitch.call == PitchCall.HIT_BY_PITCH:
                br["HBP"] += 1
                pr["HBP"] += 1.0
                state.runner_reach_source_by_id[int(batter_id)] = RUNNER_SRC_BB_HBP
                on1, on2, on3, runs, scorers = _walk_bases_with_runners(half.runner_on_1b, half.runner_on_2b, half.runner_on_3b, batter_id)
                _set_half_bases_from_runners(half, on1, on2, on3)
                _sync_runner_reach_sources(state.runner_reach_source_by_id, half)
                half.runs_scored += runs
                record_run(batter_id, pitcher_id, runs)
                if scorers:
                    record_runner_runs(scorers)
                pa_result = "HBP"
                pa_ended = True
                break

            if pitch.call == PitchCall.BALL:
                balls += 1
                if balls >= 4:
                    br["BB"] += 1
                    pr["BB"] += 1.0
                    state.runner_reach_source_by_id[int(batter_id)] = RUNNER_SRC_BB_HBP
                    on1, on2, on3, runs, scorers = _walk_bases_with_runners(half.runner_on_1b, half.runner_on_2b, half.runner_on_3b, batter_id)
                    _set_half_bases_from_runners(half, on1, on2, on3)
                    _sync_runner_reach_sources(state.runner_reach_source_by_id, half)
                    half.runs_scored += runs
                    record_run(batter_id, pitcher_id, runs)
                    if scorers:
                        record_runner_runs(scorers)
                    pa_result = "BB"
                    pa_ended = True
                    break
                continue

            if pitch.call in (PitchCall.CALLED_STRIKE, PitchCall.SWINGING_STRIKE):
                strikes += 1
                if strikes >= 3:
                    br["AB"] += 1
                    br["SO"] += 1
                    pr["SO"] += 1.0
                    half.outs += 1
                    pr["OUTS"] += 1.0
                    pa_result = "SO"
                    pa_ended = True
                    break
                continue

            if pitch.call == PitchCall.FOUL:
                if strikes < 2:
                    strikes += 1
                continue

            if pitch.call == PitchCall.IN_PLAY:
                res = pitch.in_play_result or "OUT"
                br["AB"] += 1

                bb = pitch.batted_ball_type or _batted_ball_type(rng)

                if res == "OUT":
                    if not bip_baserunning:
                        half.outs += 1
                        pr["OUTS"] += 1.0
                        pa_result = "OUT"
                    else:
                        new_bases, on1, on2, on3, runs, scorers, outs_added, subtype = _resolve_in_play_out_with_runners(
                            rng,
                            half.bases,
                            half.outs,
                            half.runner_on_1b,
                            half.runner_on_2b,
                            half.runner_on_3b,
                            batter_id,
                            bb,
                            float(getattr(cfg, "bip_dp_rate", 0.06)),
                            float(getattr(cfg, "bip_sf_rate_flypop", 0.48)),
                            float(getattr(cfg, "bip_sf_rate_line", 0.36)),
                            bip_ground_rbi_out_rate,
                            bip_out_2b_to_3b_rate,
                            bip_out_1b_to_2b_rate,
                            bip_roe_rate,
                            bip_fc_rate,
                            bip_fc_runner_on_3b_score_rate,
                            bip_1b_p1_to_3b_rate,
                        )
                        if subtype in ("ROE", "FC"):
                            state.runner_reach_source_by_id[int(batter_id)] = RUNNER_SRC_NON_HIT_REACH
                        _set_half_bases_from_runners(half, on1, on2, on3)
                        _sync_runner_reach_sources(state.runner_reach_source_by_id, half)
                        half.runs_scored += runs
                        half.outs += outs_added
                        if outs_added:
                            pr["OUTS"] += float(outs_added)
                        if subtype == "ROE":
                            charge_pitcher_runs(pitcher_id, runs)
                        else:
                            record_run(batter_id, pitcher_id, runs)
                        if scorers:
                            record_runner_runs(scorers)
                        pa_result = subtype
                else:
                    br["H"] += 1
                    pr["H"] += 1.0
                    if res in ("1B", "2B", "3B", "HR"):
                        br[res] += 1
                    if res == "HR":
                        pr["HR"] += 1.0

                    if not bip_baserunning:
                        new_bases, on1, on2, on3, runs, scorers = _advance_bases_simple_with_runners(
                            half.bases,
                            half.runner_on_1b,
                            half.runner_on_2b,
                            half.runner_on_3b,
                            batter_id,
                            res,
                        )
                    else:
                        new_bases, on1, on2, on3, runs, scorers = _advance_bases_hit_with_runners(
                            rng,
                            half.bases,
                            half.runner_on_1b,
                            half.runner_on_2b,
                            half.runner_on_3b,
                            batter_id,
                            res,
                            bb,
                            p2_scores_on_1b_mult=bip_1b_p2_scores_mult,
                            p1_scores_on_2b_mult=bip_2b_p1_scores_mult,
                            p1_to_3b_on_1b_rate=bip_1b_p1_to_3b_rate,
                        )

                    if res in ("1B", "2B", "3B"):
                        state.runner_reach_source_by_id[int(batter_id)] = RUNNER_SRC_HIT_REACH
                    _set_half_bases_from_runners(half, on1, on2, on3)
                    _sync_runner_reach_sources(state.runner_reach_source_by_id, half)
                    half.runs_scored += runs
                    record_run(batter_id, pitcher_id, runs)
                    if scorers:
                        record_runner_runs(scorers)
                    pa_result = res

                pa_ended = True
                break

        if not pa_ended:
            # Safety valve: treat as ball in play out.
            br["AB"] += 1
            half.outs += 1
            pr["OUTS"] += 1.0
            pa_result = "OUT"

        # PA-level event (always for pbp_mode pa/pitch)
        if pbp_mode in ("pa", "pitch"):
            _log(
                {
                    "type": "PA",
                    "inning": int(state.inning),
                    "half": "top" if state.top else "bottom",
                    "batting_team_id": int(batting_roster.team.team_id),
                    "fielding_team_id": int(fielding_roster.team.team_id),
                    "batter_id": int(batter_id),
                    "pitcher_id": int(pitcher_id),
                    "result": str(pa_result or ""),
                    "pitches": int(state.pa.pitch_count),
                    "outs_before": int(outs_before_pa),
                    "outs_after": int(half.outs),
                    "bases_before": bases_before_pa,
                    "bases_after": str(half.bases.value),
                    "runs_in_half": int(half.runs_scored),
                    "score_before": score_before_pa,
                    "score_after": {"away": int(state.away_score), "home": int(state.home_score)},
                }
            )

        # advance lineup regardless
        half.next_batter_index = (half.next_batter_index + 1) % len(batting_roster.lineup.batters)

        if half.outs >= 3:
            finished_top = end_half_inning()

            if game_over_after_half(finished_top):
                break
            if allow_tied_final and state.inning > max_innings:
                break
            # If we've completed regulation innings and are not tied, stop.
            if state.inning > innings_target and state.home_score != state.away_score:
                break

        # continue until break

    innings_played = max(innings_target, len(away_inning_runs), len(home_inning_runs))
    if allow_tied_final:
        innings_played = min(max_innings, innings_played)
    return GameResult(
        home_team=home.team,
        away_team=away.team,
        home_score=state.home_score,
        away_score=state.away_score,
        innings_played=innings_played,
        batter_stats=st.batter,
        pitcher_stats=st.pitcher,
        away_inning_runs=away_inning_runs,
        home_inning_runs=home_inning_runs,
        pbp_mode=pbp_mode,
        pbp_truncated=bool(pbp_truncated),
        pbp=pbp,
    )
