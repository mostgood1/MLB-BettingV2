from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from .models import BatterProfile, PitchType, PitcherProfile


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


@dataclass
class RecencyConfig:
    games: int = 14
    weight: float = 0.25  # how much to blend recency into baseline


def blend_rate(baseline: float, recent: Optional[float], weight: float) -> float:
    if recent is None:
        return clamp01(baseline)
    w = clamp01(weight)
    return clamp01((1.0 - w) * baseline + w * recent)


def apply_recency_to_batter(b: BatterProfile, recent: Dict[str, float], cfg: RecencyConfig) -> BatterProfile:
    # expected keys: k_rate, bb_rate, hr_rate, inplay_hit_rate
    b.k_rate = blend_rate(b.k_rate, recent.get("k_rate"), cfg.weight)
    b.bb_rate = blend_rate(b.bb_rate, recent.get("bb_rate"), cfg.weight)
    b.hr_rate = blend_rate(b.hr_rate, recent.get("hr_rate"), cfg.weight)
    b.inplay_hit_rate = blend_rate(b.inplay_hit_rate, recent.get("inplay_hit_rate"), cfg.weight)
    return b


def apply_recency_to_pitcher(p: PitcherProfile, recent: Dict[str, float], cfg: RecencyConfig) -> PitcherProfile:
    p.k_rate = blend_rate(p.k_rate, recent.get("k_rate"), cfg.weight)
    p.bb_rate = blend_rate(p.bb_rate, recent.get("bb_rate"), cfg.weight)
    p.hr_rate = blend_rate(p.hr_rate, recent.get("hr_rate"), cfg.weight)
    p.inplay_hit_rate = blend_rate(p.inplay_hit_rate, recent.get("inplay_hit_rate"), cfg.weight)
    return p


def batter_pitch_type_multiplier(b: BatterProfile, pitch_type: PitchType) -> float:
    # Hook: daily updater can populate b.vs_pitch_type with real split-derived multipliers.
    return float(b.vs_pitch_type.get(pitch_type, 1.0))
