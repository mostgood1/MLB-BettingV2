from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from .models import BattedBallType, PitchCall, PitchResult, PitchType


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _logit01(p: float, eps: float = 1e-6) -> float:
    p = max(eps, min(1.0 - eps, float(p)))
    return math.log(p / (1.0 - p))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-float(x)))


def _cfg_float(cfg: PitchModelConfig, name: str, default: float) -> float:
    v = getattr(cfg, name, default)
    return float(v) if isinstance(v, (int, float)) else float(default)


@dataclass(frozen=True)
class PitchModelConfig:
    """Config for the pitch model.

    This is a baseline model. As we add real features (arsenal quality, swing
    decisions, zone models, Statcast-derived whiff/contact by pitch type), we
    should version this config and persist it in daily feature snapshots.
    """

    name: str = "pitch_model_v1_tuned_20260308_hrcluster_sigma008_hrw135"

    # Baseline call mix; these are used as priors then adjusted by count and rates.
    base_ball: float = 0.34
    base_called_strike: float = 0.18
    base_swinging_strike: float = 0.12
    base_foul: float = 0.12
    base_in_play: float = 0.23
    base_hbp: float = 0.01

    # Situational adjustments (tunable)
    three_ball_take_bias: float = 0.08
    three_ball_take_bias_runner_on_bonus: float = 0.0
    three_ball_take_bias_reliever_bonus: float = 0.0
    three_ball_take_bias_reliever_runner_on_bonus: float = 0.0
    two_strike_whiff_boost: float = 0.040865875554627173
    two_strike_foul_boost: float = 0.07198880502282089
    two_strike_inplay_penalty: float = 0.056359400456758316
    # Extra extension probability at 2 strikes.
    # If >0, a fraction of two-strike pitches become a foul that does NOT change
    # the count (self-loop). This increases pitches/PA while (in expectation)
    # preserving outcome rates, since it only adds delay.
    two_strike_extra_foul_prob: float = 0.04
    bb_ball_bias_mult: float = 1.05

    # Optional calibration of strikeout target (k_tgt) via logit scaling.
    # - k_logit_mult < 1.0 shrinks extremes toward 0.5 (less confident)
    # - k_logit_mult > 1.0 amplifies extremes (more confident)
    # - k_logit_bias shifts overall K propensity up/down in logit space
    k_logit_mult: float = 0.95
    k_logit_bias: float = 0.0

    # Mapping from PA-level HR target to per-ball-in-play HR probability.
    # Keep conservative; tuning this affects run environment materially.
    hr_on_ball_in_play_factor: float = 0.62

    # Matchup-aware batted-ball type modeling.
    # Uses batter/pitcher Statcast gb/fb/ld/pu rates (when available) and shrinks to a
    # global prior based on sample size.
    bbtype_prior_weight: float = 1.0
    bbtype_batter_weight: float = 1.0
    bbtype_pitcher_weight: float = 1.0
    bbtype_sample_scale: float = 200.0

    # Conditional mapping weights (renormalized per-PA to preserve overall means).
    hr_bb_w_ground: float = 0.06
    hr_bb_w_fly: float = 1.70
    hr_bb_w_line: float = 0.75
    hr_bb_w_pop: float = 0.02

    xb_bb_w_ground: float = 0.75
    xb_bb_w_fly: float = 1.10
    xb_bb_w_line: float = 1.28
    xb_bb_w_pop: float = 0.45

    # Deterministic run-environment multipliers (neutral by default).
    # These apply before weather/park and can be used to calibrate mean totals/margins
    # without injecting extra variance.
    hr_rate_mult: float = 1.03
    inplay_hit_rate_mult: float = 1.03
    xb_share_mult: float = 0.94

    # Optional: per-game run environment multiplier (latent).
    # If run_env_sigma > 0, simulate_game() will sample a mean-1 lognormal multiplier
    # and fold it into the weather multipliers (HR / in-play hit / XB share).
    #
    # Baseline uses this as an HR-only clustering lane to improve totals tails
    # without broadly lifting in-play hit or XB environments.
    run_env_sigma: float = 0.08
    run_env_clamp_min: float = 0.75
    run_env_clamp_max: float = 1.33
    run_env_hr_weight: float = 1.35
    run_env_inplay_hit_weight: float = 0.0
    run_env_xb_share_weight: float = 0.0

    # Optional contextual HR interaction hooks used for targeted diagnostics.
    hr_bb_hbp_runner_mult: float = 1.0
    hr_starter_bb_hbp_runner_mult: float = 1.0
    hr_non_hit_reach_runner_mult: float = 1.0
    hr_reliever_runner_on_mult: float = 1.0
    hr_late_reliever_runner_on_mult: float = 1.0

    # Pitch-type whiff/contact biases (multipliers)
    pitch_whiff_mult: Dict[PitchType, float] = None  # type: ignore[assignment]
    pitch_inplay_mult: Dict[PitchType, float] = None  # type: ignore[assignment]

    # Optional: shrink batter vs pitch-type multipliers toward neutral.
    # alpha=0.5 => balanced default (half effect)
    # alpha=1.0 => full effect
    # alpha=0.0 => disable effect (equivalent to pt_mult=1.0)
    batter_pt_alpha: float = 0.5

    # Optional: scale raw batter vs pitch-type multipliers around 1.0 before applying
    # batter_pt_alpha shrink.
    #   1.0 => neutral (no extra scaling beyond the raw multipliers)
    #   0.0 => neutralize raw multipliers (becomes 1.0 after scaling)
    #   >1.0 => amplify deviations from 1.0 (kept clamped in-engine)
    batter_pt_scale: float = 0.75

    def __post_init__(self):
        object.__setattr__(
            self,
            "pitch_whiff_mult",
            self.pitch_whiff_mult
            or {
                PitchType.FF: 1.00,
                PitchType.SI: 0.95,
                PitchType.FC: 1.00,
                PitchType.SL: 1.12,
                PitchType.CU: 1.10,
                PitchType.CH: 1.08,
                PitchType.FS: 1.12,
                PitchType.KC: 1.08,
                PitchType.KN: 0.85,
                PitchType.OTHER: 1.00,
            },
        )
        object.__setattr__(
            self,
            "pitch_inplay_mult",
            self.pitch_inplay_mult
            or {
                PitchType.FF: 1.00,
                PitchType.SI: 1.05,
                PitchType.FC: 0.98,
                PitchType.SL: 0.92,
                PitchType.CU: 0.92,
                PitchType.CH: 0.95,
                PitchType.FS: 0.92,
                PitchType.KC: 0.93,
                PitchType.KN: 1.08,
                PitchType.OTHER: 1.00,
            },
        )


def _combined(a: float, b: float) -> float:
    return clamp01(0.5 * float(a) + 0.5 * float(b))


_BB_PRIOR = {
    BattedBallType.GROUND: 0.44,
    BattedBallType.FLY: 0.25,
    BattedBallType.LINE: 0.20,
    BattedBallType.POP: 0.11,
}


def _norm4(gb: float, fb: float, ld: float, pu: float) -> Tuple[float, float, float, float]:
    gb = max(0.0, float(gb))
    fb = max(0.0, float(fb))
    ld = max(0.0, float(ld))
    pu = max(0.0, float(pu))
    s = gb + fb + ld + pu
    if s <= 1e-12:
        return (0.44, 0.25, 0.20, 0.11)
    return (gb / s, fb / s, ld / s, pu / s)


def _bb_dist_matchup(
    cfg: PitchModelConfig,
    batter_gb: float,
    batter_fb: float,
    batter_ld: float,
    batter_pu: float,
    batter_inplay_n: int,
    pitcher_gb: float,
    pitcher_fb: float,
    pitcher_ld: float,
    pitcher_pu: float,
    pitcher_inplay_n: int,
) -> Dict[BattedBallType, float]:
    sample_scale = _cfg_float(cfg, "bbtype_sample_scale", 200.0)
    wb = min(1.0, max(0.0, float(batter_inplay_n) / sample_scale)) if sample_scale > 1e-9 else 1.0
    wp = min(1.0, max(0.0, float(pitcher_inplay_n) / sample_scale)) if sample_scale > 1e-9 else 1.0

    prior_w = _cfg_float(cfg, "bbtype_prior_weight", 1.0)
    batter_w = _cfg_float(cfg, "bbtype_batter_weight", 1.0) * wb
    pitcher_w = _cfg_float(cfg, "bbtype_pitcher_weight", 1.0) * wp

    bgb, bfb, bld, bpu = _norm4(batter_gb, batter_fb, batter_ld, batter_pu)
    pgb, pfb, pld, ppu = _norm4(pitcher_gb, pitcher_fb, pitcher_ld, pitcher_pu)

    wsum = prior_w + batter_w + pitcher_w
    if wsum <= 1e-12:
        return dict(_BB_PRIOR)

    gb = prior_w * _BB_PRIOR[BattedBallType.GROUND] + batter_w * bgb + pitcher_w * pgb
    fb = prior_w * _BB_PRIOR[BattedBallType.FLY] + batter_w * bfb + pitcher_w * pfb
    ld = prior_w * _BB_PRIOR[BattedBallType.LINE] + batter_w * bld + pitcher_w * pld
    pu = prior_w * _BB_PRIOR[BattedBallType.POP] + batter_w * bpu + pitcher_w * ppu
    gb, fb, ld, pu = _norm4(gb, fb, ld, pu)

    return {
        BattedBallType.GROUND: gb,
        BattedBallType.FLY: fb,
        BattedBallType.LINE: ld,
        BattedBallType.POP: pu,
    }


def _sample_bb_type(rng: random.Random, dist: Dict[BattedBallType, float]) -> BattedBallType:
    gb = float(dist.get(BattedBallType.GROUND, _BB_PRIOR[BattedBallType.GROUND]))
    fb = float(dist.get(BattedBallType.FLY, _BB_PRIOR[BattedBallType.FLY]))
    ld = float(dist.get(BattedBallType.LINE, _BB_PRIOR[BattedBallType.LINE]))
    pu = float(dist.get(BattedBallType.POP, _BB_PRIOR[BattedBallType.POP]))
    gb, fb, ld, pu = _norm4(gb, fb, ld, pu)
    x = rng.random()
    if x < gb:
        return BattedBallType.GROUND
    x -= gb
    if x < fb:
        return BattedBallType.FLY
    x -= fb
    if x < ld:
        return BattedBallType.LINE
    return BattedBallType.POP


def _pick_hit_type(rng: random.Random, xb_share: float, triple_share_of_xb: float) -> str:
    xb_share = clamp01(xb_share)
    if rng.random() < xb_share:
        ts = clamp01(triple_share_of_xb)
        return "3B" if rng.random() < ts else "2B"
    return "1B"


def simulate_pitch(
    rng: random.Random,
    cfg: PitchModelConfig,
    pitch_type: PitchType,
    pitcher_whiff_mult: float,
    pitcher_inplay_mult: float,
    weather_hr_mult: float,
    weather_inplay_hit_mult: float,
    weather_xb_share_mult: float,
    park_hr_mult: float,
    park_inplay_hit_mult: float,
    park_xb_share_mult: float,
    umpire_called_strike_mult: float,
    count: Tuple[int, int],
    batter_k_rate: float,
    batter_bb_rate: float,
    batter_hbp_rate: float,
    batter_hr_rate: float,
    batter_inplay_hit_rate: float,
    batter_xb_hit_share: float,
    batter_pt_mult: float,
    batter_pt_hr_mult: float,
    batter_triple_share_of_xb: float,
    pitcher_k_rate: float,
    pitcher_bb_rate: float,
    pitcher_hbp_rate: float,
    pitcher_hr_rate: float,
    pitcher_inplay_hit_rate: float,
    pitcher_pt_hr_mult: float = 1.0,
    batter_bb_gb_rate: float = 0.44,
    batter_bb_fb_rate: float = 0.25,
    batter_bb_ld_rate: float = 0.20,
    batter_bb_pu_rate: float = 0.11,
    batter_bb_inplay_n: int = 0,
    pitcher_bb_gb_rate: float = 0.44,
    pitcher_bb_fb_rate: float = 0.25,
    pitcher_bb_ld_rate: float = 0.20,
    pitcher_bb_pu_rate: float = 0.11,
    pitcher_bb_inplay_n: int = 0,
    has_runners_on: bool = False,
    is_reliever: bool = False,
) -> PitchResult:
    """Structured pitch model.

    Outputs one of:
    - BALL / CALLED_STRIKE / SWINGING_STRIKE / FOUL / IN_PLAY / HIT_BY_PITCH
    - If IN_PLAY, includes batted_ball_type and in_play_result (OUT/1B/2B/3B/HR)

    Count effects:
    - 3 balls: more takes -> more balls
    - 2 strikes: more fouls and whiffs, fewer in-play balls
    """
    balls, strikes = count

    # Minimal pitch-count calibration hook: at 2 strikes, optionally insert extra
    # foul pitches that leave the count unchanged (self-loop).
    try:
        ext = float(getattr(cfg, "two_strike_extra_foul_prob", 0.0) or 0.0)
    except Exception:
        ext = 0.0
    if strikes == 2 and ext > 0.0:
        if rng.random() < min(0.90, max(0.0, ext)):
            return PitchResult(pitch_type=pitch_type, call=PitchCall.FOUL, is_strike=True, is_ball=False, in_play=False)

    # Combine rates (PA-level priors)
    k_tgt = _combined(batter_k_rate, pitcher_k_rate)
    bb_tgt = _combined(batter_bb_rate, pitcher_bb_rate)
    hbp_tgt = _combined(batter_hbp_rate, pitcher_hbp_rate)
    hr_tgt = _combined(float(batter_hr_rate) * float(batter_pt_hr_mult), float(pitcher_hr_rate) * float(pitcher_pt_hr_mult))
    inplay_hit = _combined(batter_inplay_hit_rate, pitcher_inplay_hit_rate)

    # Optional K-target calibration (applied before translating k_tgt into call mix).
    k_mult = _cfg_float(cfg, "k_logit_mult", 1.0)
    k_bias = _cfg_float(cfg, "k_logit_bias", 0.0)
    if (k_mult != 1.0) or (k_bias != 0.0):
        k_tgt = clamp01(_sigmoid(_logit01(k_tgt) * k_mult + k_bias))

    # Deterministic run environment tuning.
    hr_tgt = clamp01(hr_tgt * float(getattr(cfg, "hr_rate_mult", 1.0) or 1.0))
    inplay_hit = clamp01(inplay_hit * float(getattr(cfg, "inplay_hit_rate_mult", 1.0) or 1.0))

    # Weather adjustments (kept intentionally conservative).
    hr_tgt = clamp01(hr_tgt * float(weather_hr_mult) * float(park_hr_mult))
    inplay_hit = clamp01(inplay_hit * float(weather_inplay_hit_mult) * float(park_inplay_hit_mult))
    xb_share_adj = clamp01(
        float(batter_xb_hit_share)
        * float(getattr(cfg, "xb_share_mult", 1.0) or 1.0)
        * float(weather_xb_share_mult)
        * float(park_xb_share_mult)
    )

    # Priors
    p_hbp = clamp01(cfg.base_hbp * (hbp_tgt / 0.008))

    # Plate discipline / control proxy: BB target increases BALL probability
    ball_bias = (bb_tgt - 0.08) * float(cfg.bb_ball_bias_mult)
    take_bias = 0.0
    if balls == 3:
        take_bias = float(cfg.three_ball_take_bias)
        if has_runners_on:
            take_bias += _cfg_float(cfg, "three_ball_take_bias_runner_on_bonus", 0.0)
        if is_reliever:
            take_bias += _cfg_float(cfg, "three_ball_take_bias_reliever_bonus", 0.0)
        if has_runners_on and is_reliever:
            take_bias += _cfg_float(cfg, "three_ball_take_bias_reliever_runner_on_bonus", 0.0)

    # Two-strike: more whiff/foul, less in-play
    two_strike = 1.0 if strikes == 2 else 0.0
    whiff_boost = float(cfg.two_strike_whiff_boost) * two_strike
    foul_boost = float(cfg.two_strike_foul_boost) * two_strike
    inplay_penalty = float(cfg.two_strike_inplay_penalty) * two_strike

    # Pitch-type effects (global priors * pitcher-specific multipliers)
    whiff_mult = float(cfg.pitch_whiff_mult.get(pitch_type, 1.0)) * float(pitcher_whiff_mult)
    inplay_mult = float(cfg.pitch_inplay_mult.get(pitch_type, 1.0)) * float(pitcher_inplay_mult)

    # Batter pitch-type multiplier (hook populated by features)
    pt_mult = float(batter_pt_mult)

    p_ball = clamp01(cfg.base_ball + ball_bias + take_bias)
    p_called = clamp01(cfg.base_called_strike * (1.0 - 0.5 * k_tgt))

    # Umpire zone effect: slightly shift BALL vs CALLED_STRIKE.
    ump = max(0.92, min(1.08, float(umpire_called_strike_mult)))
    p_called = clamp01(p_called * ump)
    p_ball = clamp01(p_ball * (1.0 / ump))
    p_whiff = clamp01((cfg.base_swinging_strike + whiff_boost) * (0.7 + 1.2 * k_tgt) * whiff_mult / max(0.75, pt_mult))
    p_foul = clamp01((cfg.base_foul + foul_boost) * (0.9 + 0.4 * (1.0 - k_tgt)))
    p_inplay = clamp01((cfg.base_in_play - inplay_penalty) * (0.85 + 0.5 * (1.0 - k_tgt)) * inplay_mult * pt_mult)

    # Normalize after HBP
    rest = 1.0 - p_hbp
    s = p_ball + p_called + p_whiff + p_foul + p_inplay
    if s <= 0:
        p_ball, p_called, p_whiff, p_foul, p_inplay = 0.40, 0.18, 0.12, 0.12, 0.18
        s = p_ball + p_called + p_whiff + p_foul + p_inplay
    scale = rest / s
    p_ball *= scale
    p_called *= scale
    p_whiff *= scale
    p_foul *= scale
    p_inplay *= scale

    x = rng.random()
    if x < p_hbp:
        return PitchResult(pitch_type=pitch_type, call=PitchCall.HIT_BY_PITCH, is_strike=False, is_ball=False, in_play=False)
    x -= p_hbp
    if x < p_ball:
        return PitchResult(pitch_type=pitch_type, call=PitchCall.BALL, is_strike=False, is_ball=True, in_play=False)
    x -= p_ball
    if x < p_called:
        return PitchResult(pitch_type=pitch_type, call=PitchCall.CALLED_STRIKE, is_strike=True, is_ball=False, in_play=False)
    x -= p_called
    if x < p_whiff:
        return PitchResult(pitch_type=pitch_type, call=PitchCall.SWINGING_STRIKE, is_strike=True, is_ball=False, in_play=False)
    x -= p_whiff
    if x < p_foul:
        return PitchResult(pitch_type=pitch_type, call=PitchCall.FOUL, is_strike=True, is_ball=False, in_play=False)

    dist = _bb_dist_matchup(
        cfg,
        batter_bb_gb_rate,
        batter_bb_fb_rate,
        batter_bb_ld_rate,
        batter_bb_pu_rate,
        int(batter_bb_inplay_n or 0),
        pitcher_bb_gb_rate,
        pitcher_bb_fb_rate,
        pitcher_bb_ld_rate,
        pitcher_bb_pu_rate,
        int(pitcher_bb_inplay_n or 0),
    )
    bb = _sample_bb_type(rng, dist)

    # HR and hit-in-play mapping.
    # Keep HR partly independent of in-play hit rate.
    hr_on_ball_in_play = clamp01(hr_tgt * float(cfg.hr_on_ball_in_play_factor))
    hr_w = {
        BattedBallType.GROUND: _cfg_float(cfg, "hr_bb_w_ground", 0.06),
        BattedBallType.FLY: _cfg_float(cfg, "hr_bb_w_fly", 1.70),
        BattedBallType.LINE: _cfg_float(cfg, "hr_bb_w_line", 0.75),
        BattedBallType.POP: _cfg_float(cfg, "hr_bb_w_pop", 0.02),
    }
    mean_hr_w = 0.0
    for t, p in dist.items():
        mean_hr_w += float(p) * float(hr_w.get(t, 1.0))
    mean_hr_w = max(1e-9, mean_hr_w)
    p_hr = clamp01(hr_on_ball_in_play * float(hr_w.get(bb, 1.0)) / mean_hr_w)
    if rng.random() < p_hr:
        return PitchResult(
            pitch_type=pitch_type,
            call=PitchCall.IN_PLAY,
            is_strike=True,
            is_ball=False,
            in_play=True,
            batted_ball_type=bb,
            in_play_result="HR",
        )

    if rng.random() < inplay_hit:
        xb_w = {
            BattedBallType.GROUND: _cfg_float(cfg, "xb_bb_w_ground", 0.75),
            BattedBallType.FLY: _cfg_float(cfg, "xb_bb_w_fly", 1.10),
            BattedBallType.LINE: _cfg_float(cfg, "xb_bb_w_line", 1.28),
            BattedBallType.POP: _cfg_float(cfg, "xb_bb_w_pop", 0.45),
        }
        mean_xb_w = 0.0
        for t, p in dist.items():
            mean_xb_w += float(p) * float(xb_w.get(t, 1.0))
        mean_xb_w = max(1e-9, mean_xb_w)
        xb_share_type = clamp01(xb_share_adj * float(xb_w.get(bb, 1.0)) / mean_xb_w)
        hit = _pick_hit_type(rng, xb_share_type, float(batter_triple_share_of_xb))
        return PitchResult(
            pitch_type=pitch_type,
            call=PitchCall.IN_PLAY,
            is_strike=True,
            is_ball=False,
            in_play=True,
            batted_ball_type=bb,
            in_play_result=hit,
        )

    return PitchResult(
        pitch_type=pitch_type,
        call=PitchCall.IN_PLAY,
        is_strike=True,
        is_ball=False,
        in_play=True,
        batted_ball_type=bb,
        in_play_result="OUT",
    )
