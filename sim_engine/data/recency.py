from __future__ import annotations

from typing import Dict, List, Optional

from .statsapi import StatsApiClient, fetch_person_gamelog


def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _rate(num: float, denom: float) -> Optional[float]:
    if denom <= 0:
        return None
    return num / denom


def batter_recent_rates(client: StatsApiClient, person_id: int, season: int, games: int = 14) -> Dict[str, float]:
    splits = fetch_person_gamelog(client, person_id, season, group="hitting")
    if not splits:
        return {}
    tail = splits[-games:]
    pa = 0.0
    so = 0.0
    bb = 0.0
    hr = 0.0
    h = 0.0
    for s in tail:
        st = s.get("stat", {}) or {}
        pa += _safe_float(st.get("plateAppearances"))
        so += _safe_float(st.get("strikeOuts"))
        bb += _safe_float(st.get("baseOnBalls"))
        hr += _safe_float(st.get("homeRuns"))
        h += _safe_float(st.get("hits"))

    # In-play hits approximation
    inplay = max(pa - so - bb, 0.0)
    inplay_hits = max(h - hr, 0.0)

    out: Dict[str, float] = {}
    r = _rate(so, pa)
    if r is not None:
        out["k_rate"] = r
    r = _rate(bb, pa)
    if r is not None:
        out["bb_rate"] = r
    r = _rate(hr, pa)
    if r is not None:
        out["hr_rate"] = r
    r = _rate(inplay_hits, max(inplay, 1.0))
    if r is not None:
        out["inplay_hit_rate"] = r
    return out


def pitcher_recent_rates(client: StatsApiClient, person_id: int, season: int, games: int = 6) -> Dict[str, float]:
    # Pitchers have fewer appearances; default fewer games.
    splits = fetch_person_gamelog(client, person_id, season, group="pitching")
    if not splits:
        return {}
    tail = splits[-games:]
    bf = 0.0
    so = 0.0
    bb = 0.0
    hr = 0.0
    h = 0.0
    for s in tail:
        st = s.get("stat", {}) or {}
        bf += _safe_float(st.get("battersFaced"))
        so += _safe_float(st.get("strikeOuts"))
        bb += _safe_float(st.get("baseOnBalls"))
        hr += _safe_float(st.get("homeRuns"))
        h += _safe_float(st.get("hits"))

    inplay = max(bf - so - bb, 0.0)
    out: Dict[str, float] = {}
    r = _rate(so, bf)
    if r is not None:
        out["k_rate"] = r
    r = _rate(bb, bf)
    if r is not None:
        out["bb_rate"] = r
    r = _rate(hr, bf)
    if r is not None:
        out["hr_rate"] = r
    r = _rate(h, max(inplay, 1.0))
    if r is not None:
        out["inplay_hit_rate"] = r
    return out
