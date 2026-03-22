from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


def _batter_template() -> Dict[str, int]:
    return {
        "PA": 0,
        "AB": 0,
        "H": 0,
        "1B": 0,
        "2B": 0,
        "3B": 0,
        "HR": 0,
        "R": 0,
        "RBI": 0,
        "SB": 0,
        "CS": 0,
        "BB": 0,
        "SO": 0,
        "HBP": 0,
    }


def _pitcher_template() -> Dict[str, float]:
    return {
        "BF": 0.0,
        "P": 0.0,   # pitches
        "OUTS": 0.0,
        "H": 0.0,
        "R": 0.0,
        "BB": 0.0,
        "SO": 0.0,
        "HR": 0.0,
        "HBP": 0.0,
    }


@dataclass
class StatsTracker:
    batter: Dict[int, Dict[str, int]] = field(default_factory=dict)
    pitcher: Dict[int, Dict[str, float]] = field(default_factory=dict)

    def batter_row(self, mlbam_id: int) -> Dict[str, int]:
        if mlbam_id not in self.batter:
            self.batter[mlbam_id] = _batter_template()
        return self.batter[mlbam_id]

    def pitcher_row(self, mlbam_id: int) -> Dict[str, float]:
        if mlbam_id not in self.pitcher:
            self.pitcher[mlbam_id] = _pitcher_template()
        return self.pitcher[mlbam_id]
