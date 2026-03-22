from __future__ import annotations

import copy
import random
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from .models import BaseState, GameConfig, TeamRoster
from .simulate import simulate_game


@dataclass
class LiveSituation:
    inning: int
    top: bool
    outs: int
    bases: BaseState
    away_score: int
    home_score: int


@dataclass
class LiveMcResult:
    home_win_prob: float
    away_win_prob: float
    avg_total_runs: float
    total_runs_dist: Dict[int, int]


def estimate_live(
    away: TeamRoster,
    home: TeamRoster,
    situation: LiveSituation,
    sims: int = 300,
    seed: Optional[int] = None,
) -> LiveMcResult:
    """Monte Carlo estimate for winner/total from a given game situation.

    Baseline implementation (wired for daily updater):
    - We simulate full games but seed and then adjust final scores by forcing
      the current score as the starting offset.

    Next iteration (when you’re ready): convert simulate_game to accept a
    starting GameState so the remainder-of-game sim is exact.
    """
    rng = random.Random(seed)
    home_wins = 0
    away_wins = 0
    total_sum = 0.0
    dist: Dict[int, int] = {}

    # Current score offset
    cur_total = situation.away_score + situation.home_score

    for i in range(max(1, int(sims))):
        cfg = GameConfig(rng_seed=rng.randint(1, 2**31 - 1))
        res = simulate_game(away, home, cfg)

        # Offset totals to current score; crude but keeps the live totals coherent.
        away_final = situation.away_score + max(0, res.away_score)
        home_final = situation.home_score + max(0, res.home_score)
        total = int(away_final + home_final)
        total_sum += float(total)
        dist[total] = dist.get(total, 0) + 1

        if home_final > away_final:
            home_wins += 1
        elif away_final > home_final:
            away_wins += 1
        else:
            # tie: count half/half
            home_wins += 0.5
            away_wins += 0.5

    denom = float(max(1, sims))
    return LiveMcResult(
        home_win_prob=float(home_wins) / denom,
        away_win_prob=float(away_wins) / denom,
        avg_total_runs=total_sum / denom,
        total_runs_dist=dist,
    )
