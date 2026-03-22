from __future__ import annotations

import random
from dataclasses import dataclass

from .models import PitcherProfile


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
    except Exception:
        v = 0.0
    return float(max(lo, min(hi, v)))


def _beta_sample(rng: random.Random, mean: float, n_eff: float) -> float:
    """Sample from a Beta distribution with a given mean and effective sample size.

    Uses alpha=mean*n_eff + 1 and beta=(1-mean)*n_eff + 1.
    """
    m = _clamp(mean, 1e-6, 1.0 - 1e-6)
    n = max(1.0, float(n_eff))
    a = m * n + 1.0
    b = (1.0 - m) * n + 1.0
    return float(rng.betavariate(a, b))


@dataclass(frozen=True)
class PitcherDistributionConfig:
    """Controls how much uncertainty to inject into per-game pitcher rates.

    n_eff is sized from season-to-date sample sizes, but deliberately shrunk
    to represent "today" uncertainty rather than pure sampling error.
    """

    bf_scale: float = 0.15
    bf_min_n: float = 30.0
    bf_max_n: float = 250.0

    bip_scale: float = 0.25
    bip_min_n: float = 25.0
    bip_max_n: float = 250.0


@dataclass(frozen=True)
class PitcherDayRates:
    k_rate: float
    bb_rate: float
    hbp_rate: float
    hr_rate: float
    inplay_hit_rate: float

    def as_dict(self) -> dict[str, float]:
        return {
            "k_rate": float(self.k_rate),
            "bb_rate": float(self.bb_rate),
            "hbp_rate": float(self.hbp_rate),
            "hr_rate": float(self.hr_rate),
            "inplay_hit_rate": float(self.inplay_hit_rate),
        }


def sample_pitcher_day_rates(
    rng: random.Random,
    pitcher: PitcherProfile,
    cfg: PitcherDistributionConfig | None = None,
) -> PitcherDayRates:
    c = cfg or PitcherDistributionConfig()

    bf = float(getattr(pitcher, "batters_faced", 0.0) or 0.0)
    bip = float(getattr(pitcher, "balls_in_play", 0.0) or 0.0)

    n_bf = _clamp(c.bf_scale * bf if bf > 0 else c.bf_min_n, c.bf_min_n, c.bf_max_n)
    n_bip = _clamp(c.bip_scale * bip if bip > 0 else c.bip_min_n, c.bip_min_n, c.bip_max_n)

    # Keep rates in sane ranges; simulate.py has additional clamps after platoon multipliers.
    k = _clamp(_beta_sample(rng, float(pitcher.k_rate), n_bf), 0.05, 0.60)
    bb = _clamp(_beta_sample(rng, float(pitcher.bb_rate), n_bf), 0.01, 0.25)
    hbp = _clamp(_beta_sample(rng, float(pitcher.hbp_rate), n_bf), 0.002, 0.03)
    hr = _clamp(_beta_sample(rng, float(pitcher.hr_rate), n_bf), 0.002, 0.14)

    # Conditional on ball in play.
    inplay_hit = _clamp(_beta_sample(rng, float(pitcher.inplay_hit_rate), n_bip), 0.10, 0.45)

    return PitcherDayRates(k_rate=k, bb_rate=bb, hbp_rate=hbp, hr_rate=hr, inplay_hit_rate=inplay_hit)
