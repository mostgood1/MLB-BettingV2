"""Pitch-level MLB game simulation engine (V2).

This package is designed to simulate a game at the pitch level, aggregating
to plate appearances, innings, and a full game, while tracking full boxscore
and prop-relevant stats.

The first pass is a functional baseline with clear extension points:
- Replace/augment rate models with Statcast / pitch-arsenal features
- Add manager decision models (bullpen, pinch-hitting, etc.)
- Add live win-prob/total-prob Monte Carlo wrappers
"""

from .models import (
    BaseState,
    BattedBallType,
    GameConfig,
    GameResult,
    Handedness,
    Lineup,
    ManagerProfile,
    PitchCall,
    PitchResult,
    PitchType,
    Player,
    BatterProfile,
    PitcherProfile,
    Team,
    TeamRoster,
)
from .state import GameState
from .simulate import simulate_game

__all__ = [
    "BaseState",
    "BattedBallType",
    "GameConfig",
    "GameResult",
    "Handedness",
    "Lineup",
    "ManagerProfile",
    "PitchCall",
    "PitchResult",
    "PitchType",
    "Player",
    "BatterProfile",
    "PitcherProfile",
    "Team",
    "TeamRoster",
    "GameState",
    "simulate_game",
]
