from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .models import Count, GameConfig, InningHalfState, PitchResult, TeamRoster


@dataclass
class PlateAppearanceState:
    batter_id: int
    pitcher_id: int
    count: Count = (0, 0)
    # For performance, we don't always store full per-pitch objects.
    # - pitch_count tracks pitches seen in the PA (used for summaries).
    # - pitches is only populated when pitch-level PBP is enabled.
    pitch_count: int = 0
    pitches: Optional[List[PitchResult]] = None


@dataclass
class GameState:
    away: TeamRoster
    home: TeamRoster
    config: GameConfig

    inning: int = 1
    top: bool = True
    away_score: int = 0
    home_score: int = 0

    pitcher_pitch_count: Dict[int, int] = field(default_factory=dict)
    pitcher_batters_faced: Dict[int, int] = field(default_factory=dict)

    # Track who is currently pitching for each team_id
    current_pitcher_by_team: Dict[int, int] = field(default_factory=dict)

    # Persist batting order across innings.
    # Key: team_id, Value: next batter index into roster.lineup.batters
    next_batter_index_by_team: Dict[int, int] = field(default_factory=dict)

    # Per-game sampled pitcher rates (uncertainty injection).
    # Keys are pitcher mlbam_id.
    pitcher_day_rates: Dict[int, Dict[str, float]] = field(default_factory=dict)

    # Per-game manager hook jitter (used by manager_pitching=v2).
    # Keys are pitcher mlbam_id.
    manager_hook_jitter: Dict[int, int] = field(default_factory=dict)

    # Current on-base reach source by runner id.
    runner_reach_source_by_id: Dict[int, str] = field(default_factory=dict)

    half: Optional[InningHalfState] = None
    pa: Optional[PlateAppearanceState] = None

    def batting_roster(self) -> TeamRoster:
        return self.away if self.top else self.home

    def fielding_roster(self) -> TeamRoster:
        return self.home if self.top else self.away

    def fielding_team_id(self) -> int:
        return self.fielding_roster().team.team_id
