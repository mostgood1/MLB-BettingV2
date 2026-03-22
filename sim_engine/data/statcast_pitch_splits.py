from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from .disk_cache import DiskCache
from ..models import PitchType


@dataclass(frozen=True)
class PitcherPitchSplits:
    pitcher_id: int
    season: int
    n_pitches: int
    pitch_mix: Dict[PitchType, float]
    whiff_mult: Dict[PitchType, float]
    inplay_mult: Dict[PitchType, float]
    source: str = "statcast_cache"
    start_date: str = ""
    end_date: str = ""


_SC_TO_CANON: Dict[str, PitchType] = {
    "FF": PitchType.FF,
    "FA": PitchType.FF,
    "FT": PitchType.SI,
    "SI": PitchType.SI,
    "FC": PitchType.FC,
    "SL": PitchType.SL,
    "CU": PitchType.CU,
    "KC": PitchType.KC,
    "CS": PitchType.CU,
    "CH": PitchType.CH,
    "FS": PitchType.FS,
    "FO": PitchType.CH,  # forkball-ish; treat as CH bucket
    "KN": PitchType.KN,
}


def _canon_pitch_type(code: str) -> PitchType:
    code = (code or "").strip().upper()
    return _SC_TO_CANON.get(code, PitchType.OTHER)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def default_statcast_cache(ttl_seconds: int = 7 * 24 * 3600, cache_dir: str | None = None) -> DiskCache:
    root = Path(cache_dir) if cache_dir else (Path(__file__).resolve().parents[2] / "data" / "cache" / "statcast")
    return DiskCache(root_dir=root, default_ttl_seconds=ttl_seconds)


def fetch_pitcher_pitch_splits(
    cache: DiskCache,
    pitcher_id: int,
    season: int,
    ttl_seconds: Optional[int] = None,
) -> Optional[PitcherPitchSplits]:
    """Load cached Statcast-derived pitch splits.

    This function is intentionally *cache-only* so the simulator can run in
    Windows ARM64 environments without `pybaseball` (which pulls `cryptography`).

    Populate the cache using the x64 fetch tool:
    - tools/statcast/fetch_pitcher_pitch_splits_x64.py
    """
    if pitcher_id <= 0:
        return None

    cache_key = {"pitcher_id": int(pitcher_id), "season": int(season)}
    hit = cache.get("pitcher_pitch_splits", cache_key, ttl_seconds=ttl_seconds)
    if isinstance(hit, dict) and hit.get("n_pitches"):
        try:
            return PitcherPitchSplits(
                pitcher_id=int(hit["pitcher_id"]),
                season=int(hit["season"]),
                n_pitches=int(hit["n_pitches"]),
                pitch_mix={PitchType(k): float(v) for k, v in (hit.get("pitch_mix") or {}).items()},
                whiff_mult={PitchType(k): float(v) for k, v in (hit.get("whiff_mult") or {}).items()},
                inplay_mult={PitchType(k): float(v) for k, v in (hit.get("inplay_mult") or {}).items()},
                source=str(hit.get("source") or "statcast_cache"),
                start_date=str(hit.get("start_date") or ""),
                end_date=str(hit.get("end_date") or ""),
            )
        except Exception:
            pass
    return None


def normalize_pitch_mix(mix: Dict[PitchType, float], min_share: float = 0.02) -> Dict[PitchType, float]:
    """Drop tiny categories and re-normalize to sum to ~1.0."""
    filtered = {k: float(v) for k, v in (mix or {}).items() if float(v) >= float(min_share)}
    s = sum(max(0.0, float(v)) for v in filtered.values())
    if s <= 0:
        return {}
    return {k: float(v) / s for k, v in filtered.items()}
