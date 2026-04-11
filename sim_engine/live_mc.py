from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict, Optional

from .models import BaseState, GameConfig, InningHalfState, TeamRoster
from .state import GameState
from .simulate import simulate_game


@dataclass
class LiveSituation:
    inning: int
    top: bool
    outs: int
    bases: BaseState
    away_score: int
    home_score: int
    runner_on_1b: int = 0
    runner_on_2b: int = 0
    runner_on_3b: int = 0
    away_next_batter_index: int = 0
    home_next_batter_index: int = 0
    away_pitcher_id: Optional[int] = None
    home_pitcher_id: Optional[int] = None
    pitcher_pitch_count: Dict[int, int] = field(default_factory=dict)
    pitcher_batters_faced: Dict[int, int] = field(default_factory=dict)


@dataclass
class LiveMcResult:
    home_win_prob: float
    away_win_prob: float
    avg_total_runs: float
    avg_away_runs: float
    avg_home_runs: float
    total_runs_dist: Dict[int, int]


def _clamp_batter_index(roster: TeamRoster, raw_index: int) -> int:
    lineup = list(getattr(getattr(roster, "lineup", None), "batters", []) or [])
    if not lineup:
        return 0
    try:
        return int(max(0, raw_index)) % len(lineup)
    except Exception:
        return 0


def _default_pitcher_id(roster: TeamRoster) -> int:
    try:
        return int(getattr(getattr(roster.lineup.pitcher, "player", None), "mlbam_id", 0) or 0)
    except Exception:
        return 0


def _build_initial_state(
    away: TeamRoster,
    home: TeamRoster,
    situation: LiveSituation,
    cfg: GameConfig,
) -> GameState:
    top = bool(situation.top)
    batting_roster = away if top else home
    fielding_roster = home if top else away
    next_batter_index_by_team = {
        int(away.team.team_id): _clamp_batter_index(away, int(situation.away_next_batter_index or 0)),
        int(home.team.team_id): _clamp_batter_index(home, int(situation.home_next_batter_index or 0)),
    }
    state = GameState(
        away=away,
        home=home,
        config=cfg,
        inning=max(1, int(situation.inning or 1)),
        top=top,
        away_score=max(0, int(situation.away_score or 0)),
        home_score=max(0, int(situation.home_score or 0)),
        pitcher_pitch_count={
            int(pid): max(0, int(value or 0))
            for pid, value in (situation.pitcher_pitch_count or {}).items()
            if int(pid or 0) > 0
        },
        pitcher_batters_faced={
            int(pid): max(0, int(value or 0))
            for pid, value in (situation.pitcher_batters_faced or {}).items()
            if int(pid or 0) > 0
        },
        next_batter_index_by_team=next_batter_index_by_team,
    )

    away_pitcher_id = int(situation.away_pitcher_id or 0) or _default_pitcher_id(away)
    home_pitcher_id = int(situation.home_pitcher_id or 0) or _default_pitcher_id(home)
    if away_pitcher_id > 0:
        state.current_pitcher_by_team[int(away.team.team_id)] = away_pitcher_id
    if home_pitcher_id > 0:
        state.current_pitcher_by_team[int(home.team.team_id)] = home_pitcher_id

    batting_team_id = int(batting_roster.team.team_id)
    state.half = InningHalfState(
        batting_team=batting_roster.team,
        fielding_team=fielding_roster.team,
        outs=max(0, min(2, int(situation.outs or 0))),
        bases=situation.bases if isinstance(situation.bases, BaseState) else BaseState.EMPTY,
        runner_on_1b=max(0, int(situation.runner_on_1b or 0)),
        runner_on_2b=max(0, int(situation.runner_on_2b or 0)),
        runner_on_3b=max(0, int(situation.runner_on_3b or 0)),
        runs_scored=0,
        next_batter_index=int(next_batter_index_by_team.get(batting_team_id, 0) or 0),
    )
    return state


def estimate_live(
    away: TeamRoster,
    home: TeamRoster,
    situation: LiveSituation,
    sims: int = 300,
    seed: Optional[int] = None,
) -> LiveMcResult:
    """Monte Carlo estimate for winner/total from the actual current game state."""
    rng = random.Random(seed)
    home_wins = 0
    away_wins = 0
    total_sum = 0.0
    away_sum = 0.0
    home_sum = 0.0
    dist: Dict[int, int] = {}

    for i in range(max(1, int(sims))):
        cfg = GameConfig(rng_seed=rng.randint(1, 2**31 - 1))
        state = _build_initial_state(away, home, situation, cfg)
        res = simulate_game(away, home, cfg, initial_state=state)
        away_final = max(0, int(res.away_score or 0))
        home_final = max(0, int(res.home_score or 0))
        total = int(away_final + home_final)
        total_sum += float(total)
        away_sum += float(away_final)
        home_sum += float(home_final)
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
        avg_away_runs=away_sum / denom,
        avg_home_runs=home_sum / denom,
        total_runs_dist=dist,
    )
