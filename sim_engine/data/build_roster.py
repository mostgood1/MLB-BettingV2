from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..models import (
    BatterProfile,
    Handedness,
    Lineup,
    ManagerProfile,
    PitcherProfile,
    PitchType,
    Player,
    Team,
    TeamRoster,
)
from .statsapi import (
    StatsApiClient,
    fetch_active_roster,
    fetch_team_roster,
    fetch_person,
    fetch_person_season_hitting,
    fetch_person_season_pitching,
    fetch_person_home_away_splits,
    fetch_person_stat_splits,
)
from .recency import batter_recent_rates, pitcher_recent_rates
from ..features import RecencyConfig, apply_recency_to_batter, apply_recency_to_pitcher
from .disk_cache import DiskCache
from .statsapi import fetch_person_pitch_arsenal
from .statcast_pitch_splits import fetch_pitcher_pitch_splits


_DEFAULT_ARSENAL = {
    PitchType.FF: 0.45,
    PitchType.SL: 0.22,
    PitchType.CH: 0.12,
    PitchType.SI: 0.13,
    PitchType.CU: 0.08,
}


_MANAGER_TENDENCIES_CACHE: Dict[str, Any] | None = None

_STATCAST_QUALITY_CACHE_BY_SEASON: Dict[int, Dict[str, Any]] = {}

_STATCAST_FEATURES_CACHE_BY_SEASON: Dict[int, Dict[str, Any]] = {}


def _default_profile_cache_root() -> Path:
    # build_roster.py lives at MLB-BettingV2/sim_engine/data/build_roster.py
    # parents[2] => MLB-BettingV2/
    root = Path(__file__).resolve().parents[2]
    return root / "data" / "cache" / "roster_profiles"


def _get_profile_cache(client: StatsApiClient, override: Optional[DiskCache]) -> Optional[DiskCache]:
    if override is not None:
        return override
    try:
        c = getattr(client, "cache", None)
        if isinstance(c, DiskCache):
            return c
    except Exception:
        pass
    try:
        return DiskCache(root_dir=_default_profile_cache_root(), default_ttl_seconds=24 * 3600)
    except Exception:
        return None


def _cache_ttl_seconds(client: StatsApiClient, override: Optional[int]) -> int:
    if override is not None:
        try:
            return int(override)
        except Exception:
            return 0
    try:
        v = getattr(client, "cache_ttl_seconds", None)
        if isinstance(v, int):
            return int(v)
    except Exception:
        pass
    return 24 * 3600


def _log_ratio(obs: Any, base: Any) -> float:
    try:
        obs_f = float(obs)
        base_f = float(base)
        if obs_f <= 0.0 or base_f <= 0.0:
            return 0.0
        return float(__import__("math").log(obs_f / base_f))
    except Exception:
        return 0.0


def _combine_power_log(parts: List[Tuple[float, float]], lo: float, hi: float) -> float:
    try:
        score = 0.0
        for weight, value in parts:
            score += float(weight) * float(value)
        mult = float(__import__("math").exp(score))
        return float(max(lo, min(hi, mult)))
    except Exception:
        return 1.0


def _pitch_type_hr_mult_from_summary(summary: Dict[str, Any], base: Dict[str, Any]) -> float:
    return float(
        _combine_power_log(
            [
                (0.45, _log_ratio(summary.get("barrel_rate"), base.get("barrel_rate"))),
                (0.30, _log_ratio(summary.get("hr_per_bip"), base.get("hr_per_bip"))),
                (0.15, _log_ratio(summary.get("pulled_air_rate"), base.get("pulled_air_rate"))),
                (0.10, _log_ratio(summary.get("sweet_spot_rate"), base.get("sweet_spot_rate"))),
            ],
            0.80,
            1.25,
        )
    )


def _profile_cache_key(
    *,
    pid: int,
    season: int,
    kind: str,
    enable_batter_vs_pitch_type: bool,
) -> Dict[str, Any]:
    return {
        "pid": int(pid),
        "season": int(season),
        "kind": str(kind),
        "enable_batter_vs_pitch_type": bool(enable_batter_vs_pitch_type),
    }


def _bprof_from_cached(player: Player, row: Dict[str, Any]) -> BatterProfile:
    prof = BatterProfile(player=player)
    for k in (
        "k_rate",
        "bb_rate",
        "hbp_rate",
        "hr_rate",
        "inplay_hit_rate",
        "xb_hit_share",
        "triple_share_of_xb",
        "sb_attempt_rate",
        "sb_success_rate",
        "bb_gb_rate",
        "bb_fb_rate",
        "bb_ld_rate",
        "bb_pu_rate",
    ):
        if k in row:
            try:
                setattr(prof, k, float(row.get(k)))
            except Exception:
                pass
    try:
        prof.bb_inplay_n = int(row.get("bb_inplay_n") or 0)
    except Exception:
        pass
    try:
        vpt = row.get("vs_pitch_type") or {}
        if isinstance(vpt, dict):
            prof.vs_pitch_type = {PitchType(str(k)): float(v) for k, v in vpt.items() if isinstance(v, (int, float))}
    except Exception:
        pass
    try:
        vpt_hr = row.get("vs_pitch_type_hr") or {}
        if isinstance(vpt_hr, dict):
            prof.vs_pitch_type_hr = {PitchType(str(k)): float(v) for k, v in vpt_hr.items() if isinstance(v, (int, float))}
    except Exception:
        pass
    try:
        sqm = row.get("statcast_quality_mult") or {}
        if isinstance(sqm, dict):
            prof.statcast_quality_mult = {str(k): float(v) for k, v in sqm.items() if isinstance(v, (int, float))}
    except Exception:
        pass
    try:
        hrm = row.get("pitch_type_hr_mult") or {}
        if isinstance(hrm, dict):
            prof.pitch_type_hr_mult = {PitchType(str(k)): float(v) for k, v in hrm.items() if isinstance(v, (int, float))}
    except Exception:
        pass
    return prof


def _pprof_from_cached(player: Player, row: Dict[str, Any]) -> PitcherProfile:
    prof = PitcherProfile(player=player)
    for k in (
        "k_rate",
        "bb_rate",
        "hbp_rate",
        "hr_rate",
        "inplay_hit_rate",
        "batters_faced",
        "balls_in_play",
        "stamina_pitches",
        "leverage_skill",
        "bb_gb_rate",
        "bb_fb_rate",
        "bb_ld_rate",
        "bb_pu_rate",
    ):
        if k in row:
            try:
                setattr(prof, k, float(row.get(k)))
            except Exception:
                pass
    for k in ("role",):
        if k in row:
            try:
                setattr(prof, k, str(row.get(k) or ""))
            except Exception:
                pass
    try:
        prof.bb_inplay_n = int(row.get("bb_inplay_n") or 0)
    except Exception:
        pass
    try:
        sqm = row.get("statcast_quality_mult") or {}
        if isinstance(sqm, dict):
            prof.statcast_quality_mult = {str(k): float(v) for k, v in sqm.items() if isinstance(v, (int, float))}
    except Exception:
        pass
    # Keep arsenal a sane default; starter arsenal enrichment happens later.
    prof.arsenal = dict(_DEFAULT_ARSENAL)
    return prof


def _load_manager_tendencies_anykey() -> Dict[str, Dict[str, Any]]:
    global _MANAGER_TENDENCIES_CACHE
    if _MANAGER_TENDENCIES_CACHE is not None:
        return _MANAGER_TENDENCIES_CACHE
    try:
        root = Path(__file__).resolve().parents[2]
        p = root / "data" / "manager" / "manager_tendencies.json"
        if not p.exists():
            _MANAGER_TENDENCIES_CACHE = {}
            return {}
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            _MANAGER_TENDENCIES_CACHE = {}
            return {}
        out: Dict[str, Dict[str, Any]] = {}
        for k, v in raw.items():
            if isinstance(v, dict):
                out[str(k).strip()] = v
        _MANAGER_TENDENCIES_CACHE = out
        return out
    except Exception:
        _MANAGER_TENDENCIES_CACHE = {}
        return {}


def _hand(code: str) -> Handedness:
    code = (code or "R").upper()
    if code.startswith("L"):
        return Handedness.L
    if code.startswith("S"):
        return Handedness.S
    return Handedness.R


def _safe_int(x, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _shrink_to_prior(obs: float, prior: float, n: float, n0: float) -> float:
    try:
        n = float(max(0.0, n))
        n0 = float(max(0.0, n0))
        if n + n0 <= 0:
            return float(prior)
        w = n / (n + n0)
        return float(w * float(obs) + (1.0 - w) * float(prior))
    except Exception:
        return float(prior)


def _derive_stamina_pitches_from_season_stats(
    pstat: Dict[str, Any],
    *,
    force_starter: bool = False,
) -> int:
    """Estimate per-appearance pitch stamina from season workload.

    Uses season-level pitchesThrown divided by GS (starter) or G (reliever) with shrink-to-prior.
    If workload stats are missing, returns a conservative role-based prior.
    """
    # StatsAPI tends to return these keys for season pitching; tolerate variants.
    total_pitches = _safe_float(pstat.get("pitchesThrown"), 0.0)
    if total_pitches <= 0:
        total_pitches = _safe_float(pstat.get("numberOfPitches"), 0.0)

    gs = _safe_float(pstat.get("gamesStarted"), 0.0)
    g = _safe_float(pstat.get("gamesPitched"), 0.0)
    if g <= 0:
        g = _safe_float(pstat.get("gamesPlayed"), 0.0)

    # Priors roughly match existing engine assumptions.
    sp_prior = 92.0
    rp_prior = 25.0

    if force_starter:
        # If we don't have a pitch count sample yet (early season), keep a SP-like prior.
        if total_pitches > 0 and gs >= 1:
            obs = total_pitches / max(1.0, gs)
            est = _shrink_to_prior(obs, sp_prior, n=gs, n0=10.0)
        else:
            est = sp_prior
        return int(max(70, min(115, round(est))))

    # Heuristic role classification: starter-like if they start often.
    starter_like = (gs >= 3.0) and (g <= 0 or (gs / max(1.0, g)) >= 0.45)

    if starter_like:
        if total_pitches > 0 and gs >= 1:
            obs = total_pitches / max(1.0, gs)
            est = _shrink_to_prior(obs, sp_prior, n=gs, n0=10.0)
        else:
            est = sp_prior
        return int(max(70, min(115, round(est))))

    # Reliever-like: estimate by pitches per game pitched, shrunk to RP prior.
    if total_pitches > 0 and g >= 1:
        obs = total_pitches / max(1.0, g)
        est = _shrink_to_prior(obs, rp_prior, n=g, n0=20.0)
    else:
        est = rp_prior
    return int(max(12, min(65, round(est))))


def _pitching_role_workload(pstat: Dict[str, Any]) -> Dict[str, float]:
    gs = _safe_float(pstat.get("gamesStarted", pstat.get("gs")), 0.0)
    g = _safe_float(pstat.get("gamesPitched", pstat.get("g")), 0.0)
    if g <= 0.0:
        g = _safe_float(pstat.get("gamesPlayed"), 0.0)
    return {
        "saves": _safe_float(pstat.get("saves"), 0.0),
        "gs": gs,
        "g": g,
        "gf": _safe_float(pstat.get("gamesFinished", pstat.get("gf")), 0.0),
        "bf": _safe_float(pstat.get("battersFaced", pstat.get("bf")), 0.0),
    }


def _has_pitching_workload(pstat: Dict[str, Any]) -> bool:
    role_workload = _pitching_role_workload(pstat)
    bf = float(role_workload.get("bf", 0.0) or 0.0)
    gs = float(role_workload.get("gs", 0.0) or 0.0)
    g = float(role_workload.get("g", 0.0) or 0.0)
    return bool(bf > 0.0 or gs > 0.0 or g > 0.0)


def _is_sparse_pitching_workload(pstat: Dict[str, Any]) -> bool:
    role_workload = _pitching_role_workload(pstat)
    bf = float(role_workload.get("bf", 0.0) or 0.0)
    gs = float(role_workload.get("gs", 0.0) or 0.0)
    g = float(role_workload.get("g", 0.0) or 0.0)
    gf = float(role_workload.get("gf", 0.0) or 0.0)
    saves = float(role_workload.get("saves", 0.0) or 0.0)
    return bool(bf < 80.0 and g < 4.0 and gs < 3.0 and gf < 3.0 and saves < 2.0)


def _merge_pitching_workload_stats(current_pstat: Dict[str, Any], prior_pstat: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(prior_pstat or {})
    merged.update(dict(current_pstat or {}))
    additive_keys = (
        "gamesPitched",
        "gamesPlayed",
        "gamesStarted",
        "gamesFinished",
        "saves",
        "battersFaced",
        "pitchesThrown",
        "numberOfPitches",
    )
    for key in additive_keys:
        cur = _safe_float((current_pstat or {}).get(key), 0.0)
        prev = _safe_float((prior_pstat or {}).get(key), 0.0)
        total = cur + prev
        if total > 0.0:
            merged[key] = total
    return merged


def _resolve_pitching_workload_stats(
    client: StatsApiClient,
    person_id: int,
    season: int,
    current_pstat: Dict[str, Any],
    *,
    lookback_seasons: int = 2,
) -> Dict[str, Any]:
    if _has_pitching_workload(current_pstat) and not _is_sparse_pitching_workload(current_pstat):
        return current_pstat
    max_lookback = max(0, int(lookback_seasons))
    for offset in range(1, max_lookback + 1):
        prior_season = int(season) - offset
        if prior_season <= 0:
            break
        try:
            prior_pstat = fetch_person_season_pitching(client, person_id, prior_season)
        except Exception:
            prior_pstat = {}
        if _has_pitching_workload(prior_pstat):
            if _has_pitching_workload(current_pstat):
                return _merge_pitching_workload_stats(current_pstat, prior_pstat)
            return prior_pstat
    return current_pstat


def _rate(num: float, denom: float, default: float) -> float:
    try:
        if denom <= 0:
            return default
        return float(num) / float(denom)
    except Exception:
        return default


def _mult(split_rate: float, base_rate: float, lo: float = 0.65, hi: float = 1.35) -> float:
    try:
        if base_rate <= 0:
            return 1.0
        m = float(split_rate) / float(base_rate)
        return float(max(lo, min(hi, m)))
    except Exception:
        return 1.0


def _clamp_rate(x: float, lo: float, hi: float) -> float:
    try:
        return float(max(lo, min(hi, float(x))))
    except Exception:
        return float(max(lo, min(hi, 0.0)))


def _apply_statcast_bb_type_rates(prof: Any, entry: Dict[str, Any]) -> bool:
    """Apply Statcast-derived batted-ball-type rates to a profile.

    Reads from entry["overall"]: gb_rate/fb_rate/ld_rate/pu_rate and inplay.
    Normalizes to sum to 1 when valid.
    """
    try:
        overall = entry.get("overall") or {}
        if not isinstance(overall, dict) or not overall:
            return False

        def _f(k: str) -> Optional[float]:
            v = overall.get(k)
            return float(v) if isinstance(v, (int, float)) else None

        gb = _f("gb_rate")
        fb = _f("fb_rate")
        ld = _f("ld_rate")
        pu = _f("pu_rate")

        rates = [gb, fb, ld, pu]
        if any(r is None for r in rates):
            return False
        gb_f, fb_f, ld_f, pu_f = [float(r) for r in rates]  # type: ignore[misc]
        gb_f = float(max(0.0, min(1.0, gb_f)))
        fb_f = float(max(0.0, min(1.0, fb_f)))
        ld_f = float(max(0.0, min(1.0, ld_f)))
        pu_f = float(max(0.0, min(1.0, pu_f)))
        s = gb_f + fb_f + ld_f + pu_f
        if s <= 1e-9:
            return False

        gb_f /= s
        fb_f /= s
        ld_f /= s
        pu_f /= s

        prof.bb_gb_rate = gb_f
        prof.bb_fb_rate = fb_f
        prof.bb_ld_rate = ld_f
        prof.bb_pu_rate = pu_f

        ip = overall.get("inplay")
        if isinstance(ip, (int, float)):
            try:
                prof.bb_inplay_n = int(ip)
            except Exception:
                pass
        return True
    except Exception:
        return False


def _load_statcast_quality_map_anykey(season: int) -> Dict[str, Any]:
    global _STATCAST_QUALITY_CACHE_BY_SEASON
    season_i = int(season)
    if season_i in _STATCAST_QUALITY_CACHE_BY_SEASON:
        return _STATCAST_QUALITY_CACHE_BY_SEASON[season_i]
    try:
        root = Path(__file__).resolve().parents[2]
        base = root / "data" / "statcast" / "quality"
        # Preferred stable path
        p = base / f"player_quality_{season_i}.json"
        if not p.exists():
            # Fallback: latest
            p2 = base / "player_quality_latest.json"
            p = p2 if p2.exists() else p
        if not p.exists():
            _STATCAST_QUALITY_CACHE_BY_SEASON[season_i] = {}
            return _STATCAST_QUALITY_CACHE_BY_SEASON[season_i]
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            _STATCAST_QUALITY_CACHE_BY_SEASON[season_i] = {}
            return _STATCAST_QUALITY_CACHE_BY_SEASON[season_i]
        _STATCAST_QUALITY_CACHE_BY_SEASON[season_i] = raw
        return _STATCAST_QUALITY_CACHE_BY_SEASON[season_i]
    except Exception:
        _STATCAST_QUALITY_CACHE_BY_SEASON[season_i] = {}
        return _STATCAST_QUALITY_CACHE_BY_SEASON[season_i]


def _load_statcast_features_anykey(season: int) -> Dict[str, Any]:
    global _STATCAST_FEATURES_CACHE_BY_SEASON
    season_i = int(season)
    if season_i in _STATCAST_FEATURES_CACHE_BY_SEASON:
        return _STATCAST_FEATURES_CACHE_BY_SEASON[season_i]
    try:
        root = Path(__file__).resolve().parents[2]
        base = root / "data" / "statcast" / "features"
        p = base / f"player_features_{season_i}.json"
        if not p.exists():
            p2 = base / "player_features_latest.json"
            p = p2 if p2.exists() else p
        if not p.exists():
            _STATCAST_FEATURES_CACHE_BY_SEASON[season_i] = {}
            return _STATCAST_FEATURES_CACHE_BY_SEASON[season_i]
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            _STATCAST_FEATURES_CACHE_BY_SEASON[season_i] = {}
            return _STATCAST_FEATURES_CACHE_BY_SEASON[season_i]
        _STATCAST_FEATURES_CACHE_BY_SEASON[season_i] = raw
        return _STATCAST_FEATURES_CACHE_BY_SEASON[season_i]
    except Exception:
        _STATCAST_FEATURES_CACHE_BY_SEASON[season_i] = {}
        return _STATCAST_FEATURES_CACHE_BY_SEASON[season_i]


def _apply_statcast_features_to_pitcher(prof: PitcherProfile, season: int) -> bool:
    if prof.player.mlbam_id <= 0:
        return False
    m = _load_statcast_features_anykey(season)
    if not isinstance(m, dict) or not m:
        return False
    pitchers = m.get("pitchers") or {}
    entry = pitchers.get(str(prof.player.mlbam_id)) if isinstance(pitchers, dict) else None
    if not isinstance(entry, dict):
        return False

    applied = False

    # Batted-ball type tendencies
    if _apply_statcast_bb_type_rates(prof, entry):
        applied = True

    # Overall multipliers (preferred over older quality-map).
    mult = entry.get("mult_overall") or {}
    if isinstance(mult, dict) and mult:
        def _m(key: str) -> float:
            v = mult.get(key)
            return float(v) if isinstance(v, (int, float)) else 1.0

        k_m = max(0.75, min(1.25, _m("k")))
        bb_m = max(0.75, min(1.25, _m("bb")))
        hr_m = max(0.75, min(1.35, _m("hr")))
        ip_m = max(0.85, min(1.20, _m("inplay")))

        prof.statcast_quality_mult = {"k": k_m, "bb": bb_m, "hr": hr_m, "inplay": ip_m}
        prof.k_rate = _clamp_rate(float(prof.k_rate) * k_m, 0.05, 0.60)
        prof.bb_rate = _clamp_rate(float(prof.bb_rate) * bb_m, 0.01, 0.25)
        prof.hr_rate = _clamp_rate(float(prof.hr_rate) * hr_m, 0.002, 0.14)
        prof.inplay_hit_rate = _clamp_rate(float(prof.inplay_hit_rate) * ip_m, 0.10, 0.45)
        applied = True

    # Pitch mix + pitch-type multipliers
    mix = entry.get("pitch_mix") or {}
    if isinstance(mix, dict) and mix:
        try:
            # Map string keys to PitchType enums.
            mm: Dict[PitchType, float] = {}
            for k, v in mix.items():
                if not isinstance(v, (int, float)):
                    continue
                try:
                    pt = PitchType(str(k))
                except Exception:
                    continue
                mm[pt] = float(v)
            if mm:
                prof.arsenal = mm
                prof.arsenal_source = "statcast_features"
                try:
                    prof.arsenal_sample_size = int((entry.get("overall") or {}).get("pitches") or 0)
                except Exception:
                    prof.arsenal_sample_size = 0
                applied = True
        except Exception:
            pass

    pt_map = entry.get("pitch_type") or {}
    league_power = ((((m.get("league") or {}).get("overall") or {}).get("pitcher")) or {}) if isinstance(m, dict) else {}
    if isinstance(pt_map, dict) and pt_map:
        whiff_mult: Dict[PitchType, float] = {}
        inplay_mult: Dict[PitchType, float] = {}
        hr_mult: Dict[PitchType, float] = {}
        for k, v in pt_map.items():
            if not isinstance(v, dict):
                continue
            try:
                pt = PitchType(str(k))
            except Exception:
                continue
            wm = v.get("whiff_mult")
            im = v.get("inplay_mult")
            if isinstance(wm, (int, float)):
                whiff_mult[pt] = float(max(0.70, min(1.40, float(wm))))
            if isinstance(im, (int, float)):
                inplay_mult[pt] = float(max(0.70, min(1.40, float(im))))
            summary = v.get("summary") or {}
            if isinstance(summary, dict) and isinstance(league_power, dict):
                hr_mult[pt] = _pitch_type_hr_mult_from_summary(summary, league_power)
        if whiff_mult:
            prof.pitch_type_whiff_mult = whiff_mult
            applied = True
        if inplay_mult:
            prof.pitch_type_inplay_mult = inplay_mult
            applied = True
        if hr_mult:
            prof.pitch_type_hr_mult = hr_mult
            applied = True

    return applied


def _apply_statcast_features_to_batter(prof: BatterProfile, season: int) -> bool:
    if prof.player.mlbam_id <= 0:
        return False
    m = _load_statcast_features_anykey(season)
    if not isinstance(m, dict) or not m:
        return False
    batters = m.get("batters") or {}
    entry = batters.get(str(prof.player.mlbam_id)) if isinstance(batters, dict) else None
    if not isinstance(entry, dict):
        return False

    applied = False

    # Batted-ball type tendencies
    if _apply_statcast_bb_type_rates(prof, entry):
        applied = True

    # Extra-base tendencies (best-effort): adjust xb_hit_share and 2B/3B split.
    try:
        overall = entry.get("overall") or {}
        if isinstance(overall, dict) and overall:
            n_ip = overall.get("inplay")
            try:
                n_ip_f = float(n_ip) if isinstance(n_ip, (int, float)) else float(int(n_ip))
            except Exception:
                n_ip_f = 0.0

            xb_share_obs = overall.get("xb_hit_share")
            if isinstance(xb_share_obs, (int, float)) and n_ip_f > 0:
                xb_share_new = _shrink_to_prior(float(xb_share_obs), float(prof.xb_hit_share), n=n_ip_f, n0=400.0)
                prof.xb_hit_share = float(max(0.05, min(0.45, xb_share_new)))
                applied = True

            tri_obs = overall.get("triple_share_xb")
            if isinstance(tri_obs, (int, float)) and n_ip_f > 0:
                tri_new = _shrink_to_prior(float(tri_obs), float(getattr(prof, "triple_share_of_xb", 0.12) or 0.12), n=n_ip_f, n0=800.0)
                try:
                    prof.triple_share_of_xb = float(max(0.02, min(0.25, tri_new)))
                except Exception:
                    pass
                applied = True
    except Exception:
        pass

    mult = entry.get("mult_overall") or {}
    if isinstance(mult, dict) and mult:
        def _m(key: str) -> float:
            v = mult.get(key)
            return float(v) if isinstance(v, (int, float)) else 1.0

        k_m = max(0.75, min(1.25, _m("k")))
        bb_m = max(0.75, min(1.25, _m("bb")))
        hr_m = max(0.75, min(1.35, _m("hr")))
        ip_m = max(0.85, min(1.20, _m("inplay")))

        prof.statcast_quality_mult = {"k": k_m, "bb": bb_m, "hr": hr_m, "inplay": ip_m}
        prof.k_rate = _clamp_rate(float(prof.k_rate) * k_m, 0.05, 0.55)
        prof.bb_rate = _clamp_rate(float(prof.bb_rate) * bb_m, 0.01, 0.22)
        prof.hr_rate = _clamp_rate(float(prof.hr_rate) * hr_m, 0.002, 0.12)
        prof.inplay_hit_rate = _clamp_rate(float(prof.inplay_hit_rate) * ip_m, 0.10, 0.45)
        applied = True

    vs_pt = entry.get("vs_pitch_type") or {}
    pt_map = entry.get("pitch_type") or {}
    league_power = ((((m.get("league") or {}).get("overall") or {}).get("pitcher")) or {}) if isinstance(m, dict) else {}
    if isinstance(vs_pt, dict) and vs_pt:
        out: Dict[PitchType, float] = {}
        for k, v in vs_pt.items():
            if not isinstance(v, (int, float)):
                continue
            try:
                pt = PitchType(str(k))
            except Exception:
                continue
            out[pt] = float(max(0.80, min(1.25, float(v))))
        if out:
            prof.vs_pitch_type = out
            applied = True

    if isinstance(pt_map, dict) and pt_map:
        out_hr: Dict[PitchType, float] = {}
        for k, v in pt_map.items():
            if not isinstance(v, dict):
                continue
            try:
                pt = PitchType(str(k))
            except Exception:
                continue
            summary = v.get("summary") or {}
            if not isinstance(summary, dict) or not isinstance(league_power, dict):
                continue
            out_hr[pt] = _pitch_type_hr_mult_from_summary(summary, league_power)
        if out_hr:
            prof.vs_pitch_type_hr = out_hr
            applied = True

    return applied


def _apply_statcast_quality_to_pitcher(prof: PitcherProfile, season: int) -> None:
    if prof.player.mlbam_id <= 0:
        return
    m = _load_statcast_quality_map_anykey(season)
    if not isinstance(m, dict) or not m:
        return
    pitchers = m.get("pitchers") or {}
    entry = None
    if isinstance(pitchers, dict):
        entry = pitchers.get(str(prof.player.mlbam_id))
    if not isinstance(entry, dict):
        return
    mult = entry.get("mult") or {}
    if not isinstance(mult, dict) or not mult:
        return

    def _m(key: str) -> float:
        v = mult.get(key)
        return float(v) if isinstance(v, (int, float)) else 1.0

    k_m = max(0.75, min(1.25, _m("k")))
    bb_m = max(0.75, min(1.25, _m("bb")))
    hr_m = max(0.75, min(1.35, _m("hr")))
    ip_m = max(0.85, min(1.20, _m("inplay")))

    prof.statcast_quality_mult = {"k": k_m, "bb": bb_m, "hr": hr_m, "inplay": ip_m}
    prof.k_rate = _clamp_rate(float(prof.k_rate) * k_m, 0.05, 0.60)
    prof.bb_rate = _clamp_rate(float(prof.bb_rate) * bb_m, 0.01, 0.25)
    prof.hr_rate = _clamp_rate(float(prof.hr_rate) * hr_m, 0.002, 0.14)
    prof.inplay_hit_rate = _clamp_rate(float(prof.inplay_hit_rate) * ip_m, 0.10, 0.45)


def _apply_statcast_quality_to_batter(prof: BatterProfile, season: int) -> None:
    if prof.player.mlbam_id <= 0:
        return
    m = _load_statcast_quality_map_anykey(season)
    if not isinstance(m, dict) or not m:
        return
    batters = m.get("batters") or {}
    entry = None
    if isinstance(batters, dict):
        entry = batters.get(str(prof.player.mlbam_id))
    if not isinstance(entry, dict):
        return
    mult = entry.get("mult") or {}
    if not isinstance(mult, dict) or not mult:
        return

    def _m(key: str) -> float:
        v = mult.get(key)
        return float(v) if isinstance(v, (int, float)) else 1.0

    k_m = max(0.75, min(1.25, _m("k")))
    bb_m = max(0.75, min(1.25, _m("bb")))
    hr_m = max(0.75, min(1.35, _m("hr")))
    ip_m = max(0.85, min(1.20, _m("inplay")))

    prof.statcast_quality_mult = {"k": k_m, "bb": bb_m, "hr": hr_m, "inplay": ip_m}
    prof.k_rate = _clamp_rate(float(prof.k_rate) * k_m, 0.05, 0.55)
    prof.bb_rate = _clamp_rate(float(prof.bb_rate) * bb_m, 0.01, 0.22)
    prof.hr_rate = _clamp_rate(float(prof.hr_rate) * hr_m, 0.002, 0.12)
    prof.inplay_hit_rate = _clamp_rate(float(prof.inplay_hit_rate) * ip_m, 0.10, 0.45)


def _apply_platoon_splits_to_pitcher(client: StatsApiClient, prof: PitcherProfile, season: int) -> None:
    if prof.player.mlbam_id <= 0:
        return
    try:
        splits = fetch_person_stat_splits(client, prof.player.mlbam_id, season, group="pitching", sit_codes="vl,vr")
        if not isinstance(splits, dict) or not splits:
            return
        for code in ("vl", "vr"):
            s = splits.get(code) or {}
            if not isinstance(s, dict) or not s:
                continue
            bf_s = float(s.get("battersFaced") or 0.0)
            if bf_s <= 0:
                continue
            so_s = float(s.get("strikeOuts") or 0.0)
            bb_s = float(s.get("baseOnBalls") or 0.0)
            hbp_s = float(s.get("hitBatsmen") or 0.0)
            hr_s = float(s.get("homeRuns") or 0.0)
            hits_s = float(s.get("hits") or 0.0)
            inplay_s = max(bf_s - so_s - bb_s - hbp_s, 1.0)

            k_rate_s = _rate(so_s, bf_s, prof.k_rate)
            bb_rate_s = _rate(bb_s, bf_s, prof.bb_rate)
            hr_rate_s = _rate(hr_s, bf_s, prof.hr_rate)
            inplay_hit_s = _rate(hits_s, inplay_s, prof.inplay_hit_rate)

            mults = {
                "k": _mult(k_rate_s, prof.k_rate),
                "bb": _mult(bb_rate_s, prof.bb_rate),
                "hr": _mult(hr_rate_s, prof.hr_rate, lo=0.6, hi=1.5),
                "inplay": _mult(inplay_hit_s, prof.inplay_hit_rate),
            }
            if code == "vl":
                prof.platoon_mult_vs_lhb = mults
            else:
                prof.platoon_mult_vs_rhb = mults
    except Exception:
        return


def _apply_platoon_splits_to_batter(client: StatsApiClient, prof: BatterProfile, season: int) -> None:
    if prof.player.mlbam_id <= 0:
        return
    try:
        splits = fetch_person_stat_splits(client, prof.player.mlbam_id, season, group="hitting", sit_codes="vl,vr")
        if not isinstance(splits, dict) or not splits:
            return
        for code in ("vl", "vr"):
            s = splits.get(code) or {}
            if not isinstance(s, dict) or not s:
                continue
            pa_s = float(s.get("plateAppearances") or 0.0)
            if pa_s <= 0:
                continue
            so_s = float(s.get("strikeOuts") or 0.0)
            bb_s = float(s.get("baseOnBalls") or 0.0)
            hbp_s = float(s.get("hitByPitch") or 0.0)
            hr_s = float(s.get("homeRuns") or 0.0)
            hits_s = float(s.get("hits") or 0.0)
            inplay_s = max(pa_s - so_s - bb_s - hbp_s, 1.0)
            inplay_hits_s = max(hits_s - hr_s, 0.0)

            k_rate_s = _rate(so_s, pa_s, prof.k_rate)
            bb_rate_s = _rate(bb_s, pa_s, prof.bb_rate)
            hr_rate_s = _rate(hr_s, pa_s, prof.hr_rate)
            inplay_hit_s = _rate(inplay_hits_s, inplay_s, prof.inplay_hit_rate)

            mults = {
                "k": _mult(k_rate_s, prof.k_rate),
                "bb": _mult(bb_rate_s, prof.bb_rate),
                "hr": _mult(hr_rate_s, prof.hr_rate, lo=0.6, hi=1.5),
                "inplay": _mult(inplay_hit_s, prof.inplay_hit_rate),
            }
            if code == "vl":
                prof.platoon_mult_vs_lhp = mults
            else:
                prof.platoon_mult_vs_rhp = mults
    except Exception:
        return


def _apply_home_away_splits_to_pitcher(client: StatsApiClient, prof: PitcherProfile, season: int) -> None:
    if prof.player.mlbam_id <= 0:
        return
    try:
        splits = fetch_person_home_away_splits(client, prof.player.mlbam_id, season, group="pitching")
        if not isinstance(splits, dict) or not splits:
            return
        for code in ("home", "away"):
            s = splits.get(code) or {}
            if not isinstance(s, dict) or not s:
                continue
            bf_s = float(s.get("battersFaced") or 0.0)
            if bf_s <= 0:
                continue
            so_s = float(s.get("strikeOuts") or 0.0)
            bb_s = float(s.get("baseOnBalls") or 0.0)
            hbp_s = float(s.get("hitBatsmen") or 0.0)
            hr_s = float(s.get("homeRuns") or 0.0)
            hits_s = float(s.get("hits") or 0.0)
            inplay_s = max(bf_s - so_s - bb_s - hbp_s, 1.0)

            k_rate_s = _rate(so_s, bf_s, prof.k_rate)
            bb_rate_s = _rate(bb_s, bf_s, prof.bb_rate)
            hr_rate_s = _rate(hr_s, bf_s, prof.hr_rate)
            inplay_hit_s = _rate(hits_s, inplay_s, prof.inplay_hit_rate)

            mults = {
                "k": _mult(k_rate_s, prof.k_rate),
                "bb": _mult(bb_rate_s, prof.bb_rate),
                "hr": _mult(hr_rate_s, prof.hr_rate, lo=0.6, hi=1.5),
                "inplay": _mult(inplay_hit_s, prof.inplay_hit_rate),
            }
            if code == "home":
                prof.venue_mult_home = mults
            else:
                prof.venue_mult_away = mults
    except Exception:
        return


def _apply_home_away_splits_to_batter(client: StatsApiClient, prof: BatterProfile, season: int) -> None:
    if prof.player.mlbam_id <= 0:
        return
    try:
        splits = fetch_person_home_away_splits(client, prof.player.mlbam_id, season, group="hitting")
        if not isinstance(splits, dict) or not splits:
            return
        for code in ("home", "away"):
            s = splits.get(code) or {}
            if not isinstance(s, dict) or not s:
                continue
            pa_s = float(s.get("plateAppearances") or 0.0)
            if pa_s <= 0:
                continue
            so_s = float(s.get("strikeOuts") or 0.0)
            bb_s = float(s.get("baseOnBalls") or 0.0)
            hbp_s = float(s.get("hitByPitch") or 0.0)
            hr_s = float(s.get("homeRuns") or 0.0)
            hits_s = float(s.get("hits") or 0.0)
            inplay_s = max(pa_s - so_s - bb_s - hbp_s, 1.0)
            inplay_hits_s = max(hits_s - hr_s, 0.0)

            k_rate_s = _rate(so_s, pa_s, prof.k_rate)
            bb_rate_s = _rate(bb_s, pa_s, prof.bb_rate)
            hr_rate_s = _rate(hr_s, pa_s, prof.hr_rate)
            inplay_hit_s = _rate(inplay_hits_s, inplay_s, prof.inplay_hit_rate)

            mults = {
                "k": _mult(k_rate_s, prof.k_rate),
                "bb": _mult(bb_rate_s, prof.bb_rate),
                "hr": _mult(hr_rate_s, prof.hr_rate, lo=0.6, hi=1.5),
                "inplay": _mult(inplay_hit_s, prof.inplay_hit_rate),
            }
            if code == "home":
                prof.venue_mult_home = mults
            else:
                prof.venue_mult_away = mults
    except Exception:
        return


def build_team(team_id: int, name: str, abbr: str) -> Team:
    return Team(team_id=team_id, name=name, abbreviation=abbr)


def build_team_roster(
    client: StatsApiClient,
    team: Team,
    season: int,
    as_of_date: Optional[str] = None,
    probable_pitcher_id: Optional[int] = None,
    excluded_starter_ids: Optional[List[int]] = None,
    statcast_cache: Optional[DiskCache] = None,
    statcast_ttl_seconds: Optional[int] = None,
    confirmed_lineup_ids: Optional[List[int]] = None,
    projected_lineup_ids: Optional[List[int]] = None,
    pitcher_availability: Optional[Dict[int, float]] = None,
    enable_batter_vs_pitch_type: bool = True,
    enable_batter_platoon: bool = True,
    enable_pitcher_platoon: bool = True,
    batter_platoon_alpha: float = 0.55,
    pitcher_platoon_alpha: float = 0.55,
    batter_home_away_alpha: float = 0.35,
    pitcher_home_away_alpha: float = 0.35,
    roster_type: str = "active",
    batter_recency_games: int = 14,
    batter_recency_weight: float = 0.15,
    pitcher_recency_games: int = 6,
    pitcher_recency_weight: float = 0.15,
    fallback_roster_types: Optional[List[str]] = None,
    injured_player_ids: Optional[List[int]] = None,
    exclude_injured: bool = True,
    roster_entries: Optional[List[Dict[str, Any]]] = None,
    fast_mode: bool = False,
    fast_bullpen_pitchers: int = 8,
    profile_cache: Optional[DiskCache] = None,
    profile_ttl_seconds: Optional[int] = None,
    use_profile_cache: bool = True,
) -> TeamRoster:
    roster: List[Dict[str, Any]] = []
    excluded_starter_set: set[int] = set()
    for value in excluded_starter_ids or []:
        try:
            player_id = int(value or 0)
        except Exception:
            player_id = 0
        if player_id > 0:
            excluded_starter_set.add(player_id)

    batter_rec_cfg = RecencyConfig(games=int(batter_recency_games), weight=float(batter_recency_weight))
    pitcher_rec_cfg = RecencyConfig(games=int(pitcher_recency_games), weight=float(pitcher_recency_weight))
    if roster_entries is not None:
        roster = list(roster_entries)
    else:
        try:
            roster = fetch_team_roster(client, team.team_id, roster_type=str(roster_type or "active"), date_str=as_of_date)
        except Exception:
            roster = []

    if (not roster) and fallback_roster_types:
        for rt in fallback_roster_types:
            try:
                roster = fetch_team_roster(client, team.team_id, roster_type=str(rt), date_str=as_of_date)
            except Exception:
                roster = []
            if roster:
                break

    # Spring training can have huge rosters. In fast_mode we only fully enrich
    # likely-used players to keep daily runs practical.
    wanted_lineup_ids: List[int] = []
    try:
        if confirmed_lineup_ids and len(confirmed_lineup_ids) >= 9:
            wanted_lineup_ids = [int(x) for x in (confirmed_lineup_ids or []) if int(x or 0) > 0][:9]
        elif projected_lineup_ids and len(projected_lineup_ids) >= 9:
            wanted_lineup_ids = [int(x) for x in (projected_lineup_ids or []) if int(x or 0) > 0][:9]
    except Exception:
        wanted_lineup_ids = []

    wanted_hitters: set[int] = set(wanted_lineup_ids)
    wanted_pitchers: set[int] = set()
    if probable_pitcher_id and int(probable_pitcher_id) not in excluded_starter_set:
        try:
            wanted_pitchers.add(int(probable_pitcher_id))
        except Exception:
            pass

    # Build a small bullpen candidate list from roster entries without extra API calls.
    if bool(fast_mode):
        roster_hitter_ids: List[int] = []
        present_lineup_hitter_ids: set[int] = set()
        for e in roster or []:
            if not isinstance(e, dict):
                continue
            person = (e.get("person") or {})
            pid = _safe_int(person.get("id"), 0)
            if pid <= 0:
                continue
            pos = (e.get("position") or {}) if isinstance(e.get("position"), dict) else {}
            abbr = str(pos.get("abbreviation") or "").strip().upper()
            if abbr == "P":
                continue
            roster_hitter_ids.append(int(pid))
            if pid in wanted_hitters:
                present_lineup_hitter_ids.add(int(pid))

        if wanted_hitters:
            # Projected/confirmed lineup ids can be stale relative to today's active roster.
            # Keep the matched hitters, then backfill with active-roster bats so we don't pad
            # an otherwise healthy team with synthetic placeholder hitters.
            wanted_hitters = set(present_lineup_hitter_ids)
            if len(wanted_hitters) < 9:
                for pid in roster_hitter_ids:
                    wanted_hitters.add(int(pid))
                    if len(wanted_hitters) >= 11:
                        break

        try:
            max_bp = int(fast_bullpen_pitchers or 0)
        except Exception:
            max_bp = 0
        max_bp = max(0, min(12, max_bp))

        if max_bp > 0:
            # Oversample a few extra arms so later starter-like filtering can still
            # leave us with a realistic bullpen in fast_mode.
            bullpen_pitcher_target = int(min(15, max_bp + 3))
            for e in roster or []:
                if not isinstance(e, dict):
                    continue
                person = (e.get("person") or {})
                pid = _safe_int(person.get("id"), 0)
                if pid <= 0:
                    continue
                if probable_pitcher_id and int(pid) == int(probable_pitcher_id):
                    continue
                pos = (e.get("position") or {}) if isinstance(e.get("position"), dict) else {}
                abbr = str(pos.get("abbreviation") or "").strip().upper()
                if abbr != "P":
                    continue
                wanted_pitchers.add(int(pid))
                if len(wanted_pitchers) >= (1 + bullpen_pitcher_target) and probable_pitcher_id:
                    break
                if len(wanted_pitchers) >= bullpen_pitcher_target and not probable_pitcher_id:
                    break
        # If we don't have a lineup, at least pick a few hitters so we can build a plausible 9.
        if not wanted_hitters:
            for pid in roster_hitter_ids:
                wanted_hitters.add(int(pid))
                if len(wanted_hitters) >= 11:
                    break

    injured_set = set()
    if injured_player_ids:
        for x in injured_player_ids:
            try:
                xi = int(x)
            except Exception:
                continue
            if xi > 0:
                injured_set.add(xi)

    def _status_is_injured(status_obj: Any) -> bool:
        if not isinstance(status_obj, dict) or not status_obj:
            return False
        code = str(status_obj.get("code") or "").strip().upper()
        desc = str(status_obj.get("description") or "").strip().lower()
        if code.startswith("IL") or code.startswith("DL"):
            return True
        # StatsAPI often uses D10/D15/D60/etc for disabled list.
        if len(code) >= 2 and code.startswith("D") and any(ch.isdigit() for ch in code[1:]):
            return True
        if "injured list" in desc or "disabled list" in desc:
            return True
        # Keep this conservative: treat "Inactive" as non-injury unless explicitly IL/DL.
        return False

    hitters: List[BatterProfile] = []
    pitchers: List[PitcherProfile] = []

    # Keep small pitcher stat features for bullpen role heuristics.
    pitcher_role_feats: Dict[int, Dict[str, float]] = {}

    prof_cache = _get_profile_cache(client, profile_cache)
    prof_ttl = _cache_ttl_seconds(client, profile_ttl_seconds)

    for entry in roster:
        person = (entry.get("person") or {})
        pid = _safe_int(person.get("id"), 0)
        if pid <= 0:
            continue

        if bool(fast_mode):
            # Skip expensive enrichment for non-essential players.
            pos = (entry.get("position") or {}) if isinstance(entry.get("position"), dict) else {}
            abbr = str(pos.get("abbreviation") or "").strip().upper()
            if abbr == "P":
                if pid not in wanted_pitchers:
                    continue
            else:
                if pid not in wanted_hitters:
                    continue

        if bool(exclude_injured):
            try:
                if pid in injured_set:
                    continue
            except Exception:
                pass
            try:
                if _status_is_injured(entry.get("status")):
                    continue
            except Exception:
                pass
        info = fetch_person(client, pid)
        prim_pos = ((info.get("primaryPosition") or {}).get("abbreviation") or "").upper()
        bat_side = _hand(((info.get("batSide") or {}).get("code") or "R"))
        throw_side = _hand(((info.get("pitchHand") or {}).get("code") or "R"))
        full_name = info.get("fullName") or person.get("fullName") or str(pid)
        player = Player(
            mlbam_id=pid,
            full_name=full_name,
            primary_position=prim_pos or "?",
            bat_side=bat_side,
            throw_side=throw_side,
        )

        cached = None
        if bool(use_profile_cache) and prof_cache is not None:
            try:
                kind = "P" if prim_pos == "P" else "B"
                cached = prof_cache.get(
                    "player_base_profile",
                    _profile_cache_key(pid=pid, season=season, kind=kind, enable_batter_vs_pitch_type=bool(enable_batter_vs_pitch_type)),
                    ttl_seconds=prof_ttl,
                )
            except Exception:
                cached = None

        if prim_pos == "P":
            if isinstance(cached, dict) and cached.get("kind") == "P" and isinstance(cached.get("profile"), dict):
                prof = _pprof_from_cached(player, cached.get("profile") or {})
                pitchers.append(prof)
                try:
                    rf = cached.get("role_feats") or {}
                    if isinstance(rf, dict):
                        workload_pstat = _resolve_pitching_workload_stats(client, pid, season, rf)
                        role_feats = _pitching_role_workload(workload_pstat)
                        pitcher_role_feats[pid] = {
                            **role_feats,
                            "k_rate": float(getattr(prof, "k_rate", 0.0) or 0.0),
                            "bb_rate": float(getattr(prof, "bb_rate", 0.0) or 0.0),
                        }
                        derived_stamina = _derive_stamina_pitches_from_season_stats(
                            workload_pstat,
                            force_starter=bool(probable_pitcher_id and int(pid) == int(probable_pitcher_id)),
                        )
                        try:
                            prof.stamina_pitches = int(max(float(getattr(prof, "stamina_pitches", 0) or 0.0), float(derived_stamina)))
                        except Exception:
                            pass
                except Exception:
                    pass
            else:
                pstat = fetch_person_season_pitching(client, pid, season)
                workload_pstat = _resolve_pitching_workload_stats(client, pid, season, pstat)
                bf = float(pstat.get("battersFaced") or 0.0)
                so = float(pstat.get("strikeOuts") or 0.0)
                bb = float(pstat.get("baseOnBalls") or 0.0)
                hbp = float(pstat.get("hitBatsmen") or 0.0)
                hr = float(pstat.get("homeRuns") or 0.0)
                role_bf = float(workload_pstat.get("battersFaced") or 0.0)
                saves = float(workload_pstat.get("saves") or 0.0)
                games_pitched = float(workload_pstat.get("gamesPitched") or workload_pstat.get("gamesPlayed") or 0.0)
                games_started = float(workload_pstat.get("gamesStarted") or 0.0)
                games_finished = float(workload_pstat.get("gamesFinished") or 0.0)
                inplay = max(bf - so - bb - hbp, 1.0)
                hits = float(pstat.get("hits") or 0.0)

                stamina_pitches = _derive_stamina_pitches_from_season_stats(
                    workload_pstat,
                    force_starter=bool(probable_pitcher_id and int(pid) == int(probable_pitcher_id)),
                )

                prof = PitcherProfile(
                    player=player,
                    k_rate=_rate(so, bf, 0.24),
                    bb_rate=_rate(bb, bf, 0.08),
                    hbp_rate=_rate(hbp, bf, 0.008),
                    hr_rate=_rate(hr, bf, 0.03),
                    inplay_hit_rate=_rate(hits, inplay, 0.27),
                    batters_faced=float(bf),
                    balls_in_play=float(inplay),
                    arsenal=dict(_DEFAULT_ARSENAL),
                    stamina_pitches=int(stamina_pitches),
                    role="RP",
                    leverage_skill=0.5,
                )
                pitchers.append(prof)
                role_feats = _pitching_role_workload(workload_pstat)

                # Apply Statcast-derived features (preferred) or fallback quality multipliers.
                applied_features = False
                try:
                    applied_features = _apply_statcast_features_to_pitcher(prof, season)
                except Exception:
                    applied_features = False
                if not applied_features:
                    try:
                        _apply_statcast_quality_to_pitcher(prof, season)
                    except Exception:
                        pass

                pitcher_role_feats[pid] = {
                    **role_feats,
                    "k_rate": prof.k_rate,
                    "bb_rate": prof.bb_rate,
                }

                # Cache base pitcher profile excluding volatile recency/platoon/availability.
                if bool(use_profile_cache) and prof_cache is not None:
                    try:
                        prof_cache.set(
                            "player_base_profile",
                            _profile_cache_key(pid=pid, season=season, kind="P", enable_batter_vs_pitch_type=bool(enable_batter_vs_pitch_type)),
                            {
                                "kind": "P",
                                "profile": {
                                    "k_rate": float(prof.k_rate),
                                    "bb_rate": float(prof.bb_rate),
                                    "hbp_rate": float(prof.hbp_rate),
                                    "hr_rate": float(prof.hr_rate),
                                    "inplay_hit_rate": float(prof.inplay_hit_rate),
                                    "batters_faced": float(prof.batters_faced),
                                    "balls_in_play": float(prof.balls_in_play),
                                    "stamina_pitches": float(getattr(prof, "stamina_pitches", 0) or 0),
                                    "role": str(getattr(prof, "role", "RP") or "RP"),
                                    "leverage_skill": float(getattr(prof, "leverage_skill", 0.5) or 0.5),
                                    "statcast_quality_mult": dict(getattr(prof, "statcast_quality_mult", {}) or {}),
                                    "pitch_type_hr_mult": {
                                        str(k.value if isinstance(k, PitchType) else str(k)): float(v)
                                        for k, v in (getattr(prof, "pitch_type_hr_mult", {}) or {}).items()
                                        if isinstance(v, (int, float))
                                    },
                                    "bb_gb_rate": float(getattr(prof, "bb_gb_rate", 0.44) or 0.44),
                                    "bb_fb_rate": float(getattr(prof, "bb_fb_rate", 0.25) or 0.25),
                                    "bb_ld_rate": float(getattr(prof, "bb_ld_rate", 0.20) or 0.20),
                                    "bb_pu_rate": float(getattr(prof, "bb_pu_rate", 0.11) or 0.11),
                                    "bb_inplay_n": int(getattr(prof, "bb_inplay_n", 0) or 0),
                                },
                                    "role_feats": dict(role_feats),
                            },
                        )
                    except Exception:
                        pass

            # Recency hook: last few appearances (apply even on cache hits)
            try:
                recent = pitcher_recent_rates(client, pid, season, games=int(pitcher_rec_cfg.games))
                apply_recency_to_pitcher(prof, recent, pitcher_rec_cfg)
            except Exception:
                pass
        else:
            if isinstance(cached, dict) and cached.get("kind") == "B" and isinstance(cached.get("profile"), dict):
                prof = _bprof_from_cached(player, cached.get("profile") or {})
                hitters.append(prof)
            else:
                hstat = fetch_person_season_hitting(client, pid, season)
                pa = float(hstat.get("plateAppearances") or 0.0)
                so = float(hstat.get("strikeOuts") or 0.0)
                bb = float(hstat.get("baseOnBalls") or 0.0)
                hbp = float(hstat.get("hitByPitch") or 0.0)
                hr = float(hstat.get("homeRuns") or 0.0)
                hits = float(hstat.get("hits") or 0.0)
                doubles = float(hstat.get("doubles") or 0.0)
                triples = float(hstat.get("triples") or 0.0)
                sb = float(hstat.get("stolenBases") or 0.0)
                cs = float(hstat.get("caughtStealing") or 0.0)
                inplay = max(pa - so - bb - hbp, 1.0)
                inplay_hits = max(hits - hr, 0.0)

                xb_hits = max(doubles + triples, 0.0)
                xb_share = xb_hits / hits if hits > 0 else 0.22

                denom_xb = max(doubles + triples, 0.0)
                triple_share = (triples / denom_xb) if denom_xb > 0 else 0.12
                triple_share = float(max(0.02, min(0.25, triple_share)))

                # Very rough "steal opportunity" proxy: times reaching 1B.
                singles = max(hits - doubles - triples - hr, 0.0)
                steal_opp = max(singles + bb + hbp, 1.0)
                sb_att = max(sb + cs, 0.0)
                sb_attempt_rate = float(max(0.0, min(0.35, sb_att / steal_opp)))
                sb_success_rate = float(max(0.40, min(0.95, (sb / sb_att) if sb_att > 0 else 0.72)))

                prof = BatterProfile(
                    player=player,
                    k_rate=_rate(so, pa, 0.22),
                    bb_rate=_rate(bb, pa, 0.08),
                    hbp_rate=_rate(hbp, pa, 0.008),
                    hr_rate=_rate(hr, pa, 0.03),
                    inplay_hit_rate=_rate(inplay_hits, inplay, 0.28),
                    xb_hit_share=float(max(0.05, min(0.45, xb_share))),
                    triple_share_of_xb=float(triple_share),
                    sb_attempt_rate=float(sb_attempt_rate),
                    sb_success_rate=float(sb_success_rate),
                )
                hitters.append(prof)

                # Apply Statcast-derived features (preferred) or fallback quality multipliers.
                applied_features = False
                try:
                    applied_features = _apply_statcast_features_to_batter(prof, season)
                except Exception:
                    applied_features = False
                if not applied_features:
                    try:
                        _apply_statcast_quality_to_batter(prof, season)
                    except Exception:
                        pass

            # Cache base batter profile excluding volatile recency/platoon.
            if bool(use_profile_cache) and prof_cache is not None:
                try:
                    vs_pt = {}
                    try:
                        vs_pt = {str(k.value if isinstance(k, PitchType) else str(k)): float(v) for k, v in (prof.vs_pitch_type or {}).items() if isinstance(v, (int, float))}
                    except Exception:
                        vs_pt = {}
                    try:
                        vs_pt_hr = {str(k.value if isinstance(k, PitchType) else str(k)): float(v) for k, v in (getattr(prof, "vs_pitch_type_hr", {}) or {}).items() if isinstance(v, (int, float))}
                    except Exception:
                        vs_pt_hr = {}

                    prof_cache.set(
                        "player_base_profile",
                        _profile_cache_key(pid=pid, season=season, kind="B", enable_batter_vs_pitch_type=bool(enable_batter_vs_pitch_type)),
                        {
                            "kind": "B",
                            "profile": {
                                "k_rate": float(prof.k_rate),
                                "bb_rate": float(prof.bb_rate),
                                "hbp_rate": float(prof.hbp_rate),
                                "hr_rate": float(prof.hr_rate),
                                "inplay_hit_rate": float(prof.inplay_hit_rate),
                                "xb_hit_share": float(prof.xb_hit_share),
                                "triple_share_of_xb": float(prof.triple_share_of_xb),
                                "sb_attempt_rate": float(prof.sb_attempt_rate),
                                "sb_success_rate": float(prof.sb_success_rate),
                                "vs_pitch_type": vs_pt,
                                "vs_pitch_type_hr": vs_pt_hr,
                                "statcast_quality_mult": dict(getattr(prof, "statcast_quality_mult", {}) or {}),
                                "bb_gb_rate": float(getattr(prof, "bb_gb_rate", 0.44) or 0.44),
                                "bb_fb_rate": float(getattr(prof, "bb_fb_rate", 0.25) or 0.25),
                                "bb_ld_rate": float(getattr(prof, "bb_ld_rate", 0.20) or 0.20),
                                "bb_pu_rate": float(getattr(prof, "bb_pu_rate", 0.11) or 0.11),
                                "bb_inplay_n": int(getattr(prof, "bb_inplay_n", 0) or 0),
                            },
                        },
                    )
                except Exception:
                    pass

            # Optional ablation: remove per-pitch-type batter multipliers.
            if not bool(enable_batter_vs_pitch_type):
                try:
                    prof.vs_pitch_type = {}
                except Exception:
                    pass
                try:
                    prof.vs_pitch_type_hr = {}
                except Exception:
                    pass

            # Recency hook: last ~2 weeks
            try:
                recent = batter_recent_rates(client, pid, season, games=int(batter_rec_cfg.games))
                apply_recency_to_batter(prof, recent, batter_rec_cfg)
            except Exception:
                pass

    # Choose starter
    starter: Optional[PitcherProfile] = None
    starter_selection_source = ""
    if probable_pitcher_id and int(probable_pitcher_id) not in excluded_starter_set:
        for p in pitchers:
            if p.player.mlbam_id == probable_pitcher_id:
                starter = p
                break
    eligible_pitchers = [p for p in pitchers if int(getattr(getattr(p, "player", None), "mlbam_id", 0) or 0) not in excluded_starter_set]
    if starter is None and eligible_pitchers:
        # Heuristic fallback when we don't have a probable starter:
        # prefer starter-like pitchers (gamesStarted) and higher stamina,
        # while down-weighting clear late-inning relievers (saves/gamesFinished).
        def _starter_score(p: PitcherProfile) -> float:
            pid = int(p.player.mlbam_id or 0)
            f = pitcher_role_feats.get(pid, {})
            gs = float(f.get("gs", 0.0) or 0.0)
            bf = float(f.get("bf", 0.0) or 0.0)
            saves = float(f.get("saves", 0.0) or 0.0)
            gf = float(f.get("gf", 0.0) or 0.0)
            stamina = float(getattr(p, "stamina_pitches", 0) or 0)
            return 1000.0 * gs + 1.0 * stamina + 0.05 * bf - 50.0 * saves - 10.0 * gf

        starter = max(eligible_pitchers, key=_starter_score)
    elif starter is None and pitchers:
        starter = max(pitchers, key=lambda p: float(getattr(p, "stamina_pitches", 0) or 0.0))
    if starter is None:
        # fallback synthetic pitcher
        starter = PitcherProfile(
            player=Player(mlbam_id=0, full_name=f"{team.abbreviation} Pitcher", primary_position="P", bat_side=Handedness.R, throw_side=Handedness.R),
            arsenal=dict(_DEFAULT_ARSENAL),
            role="SP",
        )

    # Record starter selection provenance for debugging/replay.
    try:
        if probable_pitcher_id and starter and int(starter.player.mlbam_id or 0) == int(probable_pitcher_id):
            starter_selection_source = "probable"
        elif probable_pitcher_id and pitchers:
            starter_selection_source = "probable_not_found"
        elif pitchers:
            starter_selection_source = "heuristic"
        else:
            starter_selection_source = "synthetic"

        setattr(starter, "starter_selection_source", str(starter_selection_source))
        setattr(starter, "starter_requested_id", int(probable_pitcher_id or 0) if probable_pitcher_id else None)
    except Exception:
        pass

    # Optional: enrich starter arsenal + pitch-type multipliers from Statcast.
    if statcast_cache is not None and starter.player.mlbam_id > 0:
        try:
            mix, total_pitches = fetch_person_pitch_arsenal(client, starter.player.mlbam_id, season)
            if mix:
                starter.arsenal = mix
                starter.arsenal_source = "statsapi_pitchArsenal"
                starter.arsenal_sample_size = int(total_pitches or 0)

            # Cache-only Statcast per-pitch outcome multipliers (populated via x64 tool).
            splits = fetch_pitcher_pitch_splits(
                cache=statcast_cache,
                pitcher_id=starter.player.mlbam_id,
                season=season,
                ttl_seconds=statcast_ttl_seconds,
            )
            if splits is not None:
                starter.statcast_splits_source = str(splits.source or "")
                starter.statcast_splits_n_pitches = int(splits.n_pitches or 0)
                starter.statcast_splits_start_date = str(splits.start_date or "")
                starter.statcast_splits_end_date = str(splits.end_date or "")
                if splits.whiff_mult:
                    starter.pitch_type_whiff_mult = dict(splits.whiff_mult)
                if splits.inplay_mult:
                    starter.pitch_type_inplay_mult = dict(splits.inplay_mult)
        except Exception:
            pass

    bullpen_all = [p for p in pitchers if p.player.mlbam_id != starter.player.mlbam_id]
    if starter.player.mlbam_id:
        starter.role = "SP"
    try:
        # Safety floor: even when season data is sparse, starters should not look like 1-inning arms.
        starter.stamina_pitches = int(max(70, int(getattr(starter, "stamina_pitches", 92) or 92)))
    except Exception:
        pass

    # Apply recent-usage availability multipliers (best-effort).
    try:
        pa = pitcher_availability or {}
        if isinstance(pa, dict) and pa:
            try:
                if starter.player.mlbam_id in pa and isinstance(pa.get(starter.player.mlbam_id), (int, float)):
                    starter.availability_mult = float(pa.get(starter.player.mlbam_id) or 1.0)
            except Exception:
                pass
            for p in bullpen_all:
                try:
                    v = pa.get(p.player.mlbam_id)
                    if isinstance(v, (int, float)):
                        p.availability_mult = float(v)
                except Exception:
                    continue
    except Exception:
        pass

    def _role_snapshot(p: PitcherProfile) -> Dict[str, float]:
        pid = int(getattr(getattr(p, "player", None), "mlbam_id", 0) or 0)
        f = pitcher_role_feats.get(pid, {})
        gs = float(f.get("gs", 0.0) or 0.0)
        g = float(f.get("g", 0.0) or 0.0)
        gf = float(f.get("gf", 0.0) or 0.0)
        saves = float(f.get("saves", 0.0) or 0.0)
        bf = float(f.get("bf", 0.0) or 0.0)
        stamina = float(getattr(p, "stamina_pitches", 0) or 0.0)
        if g <= 0.0:
            g = max(gs, gf, 0.0)
        gs_share = (gs / max(1.0, g)) if g > 0.0 else (1.0 if gs > 0.0 else 0.0)
        return {
            "gs": gs,
            "g": g,
            "gs_share": gs_share,
            "gf": gf,
            "saves": saves,
            "bf": bf,
            "stamina": stamina,
        }

    def _avail_mult(p: PitcherProfile) -> float:
        try:
            return float(max(0.0, min(1.0, float(getattr(p, "availability_mult", 1.0) or 1.0))))
        except Exception:
            return 1.0

    def _is_clear_rotation_arm(p: PitcherProfile) -> bool:
        snap = _role_snapshot(p)
        gs = float(snap.get("gs", 0.0) or 0.0)
        gs_share = float(snap.get("gs_share", 0.0) or 0.0)
        stamina = float(snap.get("stamina", 0.0) or 0.0)
        saves = float(snap.get("saves", 0.0) or 0.0)
        gf = float(snap.get("gf", 0.0) or 0.0)
        if gs >= 8.0:
            return True
        if gs >= 5.0 and gs_share >= 0.40:
            return True
        if gs >= 2.0 and stamina >= 55.0 and saves <= 1.0 and gf <= 6.0:
            return True
        if gs >= 1.0 and stamina >= 65.0 and saves <= 1.0 and gf <= 4.0:
            return True
        if stamina >= 75.0 and gs_share >= 0.10 and saves <= 0.0 and gf <= 3.0:
            return True
        if gs >= 3.0 and stamina >= 60.0 and saves <= 1.0 and gf <= 6.0:
            return True
        return False

    def _is_swingman_candidate(p: PitcherProfile) -> bool:
        if _is_clear_rotation_arm(p):
            return False
        snap = _role_snapshot(p)
        gs = float(snap.get("gs", 0.0) or 0.0)
        stamina = float(snap.get("stamina", 0.0) or 0.0)
        gs_share = float(snap.get("gs_share", 0.0) or 0.0)
        gf = float(snap.get("gf", 0.0) or 0.0)
        saves = float(snap.get("saves", 0.0) or 0.0)
        if gs >= 2.0 and stamina < 65.0:
            return True
        if gs >= 1.0 and stamina >= 35.0 and stamina < 55.0:
            return True
        if stamina >= 45.0 and stamina < 65.0 and gs_share >= 0.20 and saves <= 2.0 and gf <= 8.0:
            return True
        return False

    def _long_relief_score(p: PitcherProfile) -> float:
        snap = _role_snapshot(p)
        return (
            1.5 * float(snap.get("gs", 0.0) or 0.0)
            + 0.05 * float(snap.get("stamina", 0.0) or 0.0)
            + 0.015 * float(snap.get("bf", 0.0) or 0.0)
            + 0.75 * _avail_mult(p)
            - 0.25 * float(snap.get("saves", 0.0) or 0.0)
            - 0.08 * float(snap.get("gf", 0.0) or 0.0)
        )

    # Exclude clear rotation arms from the bullpen entirely, and keep at most
    # one swingman/long-relief arm from the starter-like fringe.
    dedicated_bullpen: List[PitcherProfile] = []
    swingman_candidates: List[PitcherProfile] = []
    excluded_rotation_arms: List[PitcherProfile] = []
    for p in bullpen_all:
        if _is_clear_rotation_arm(p):
            excluded_rotation_arms.append(p)
        elif _is_swingman_candidate(p):
            swingman_candidates.append(p)
        else:
            dedicated_bullpen.append(p)

    bullpen: List[PitcherProfile] = list(dedicated_bullpen)
    if swingman_candidates:
        bullpen.append(max(swingman_candidates, key=_long_relief_score))

    min_bullpen_arms = 6
    if len(bullpen) < min_bullpen_arms:
        selected_ids = {int(getattr(getattr(p, "player", None), "mlbam_id", 0) or 0) for p in bullpen}
        fallback_pool = [
            p
            for p in swingman_candidates
            if int(getattr(getattr(p, "player", None), "mlbam_id", 0) or 0) not in selected_ids
        ]
        fallback_pool = sorted(
            fallback_pool,
            key=lambda p: (
                _avail_mult(p),
                _long_relief_score(p),
            ),
            reverse=True,
        )
        for p in fallback_pool:
            bullpen.append(p)
            if len(bullpen) >= min_bullpen_arms:
                break

    # Bullpen roles: simple heuristics from saves/gamesFinished.
    def _score_closer(pid: int) -> float:
        f = pitcher_role_feats.get(pid, {})
        return 3.0 * f.get("saves", 0.0) + 1.0 * f.get("gf", 0.0) + 0.5 * f.get("k_rate", 0.0) - 0.2 * f.get("bb_rate", 0.0)

    bullpen_ids = [p.player.mlbam_id for p in bullpen]
    closer_id: Optional[int] = None
    if bullpen_ids:
        closer_id = max(bullpen_ids, key=_score_closer)

    setup_candidates = [pid for pid in bullpen_ids if pid != closer_id]
    setup_ids: List[int] = []
    if setup_candidates:
        # pick top 2 for setup
        setup_ids = sorted(setup_candidates, key=_score_closer, reverse=True)[:2]

    long_relief_id: Optional[int] = None
    long_relief_pool = [
        p
        for p in bullpen
        if int(p.player.mlbam_id or 0) not in set([pid for pid in ([closer_id] + setup_ids) if pid])
        and _is_swingman_candidate(p)
    ]
    if long_relief_pool:
        long_relief_id = max(long_relief_pool, key=_long_relief_score).player.mlbam_id

    for p in bullpen:
        pid = p.player.mlbam_id
        if closer_id and pid == closer_id:
            p.role = "CL"
            p.leverage_skill = 0.85
        elif pid in setup_ids:
            p.role = "SU"
            p.leverage_skill = 0.7
        elif long_relief_id and pid == long_relief_id:
            p.role = "LR"
            p.leverage_skill = 0.35
        else:
            p.role = "MR"
            p.leverage_skill = 0.5

    # Lineup: best-effort reorder hitters to match confirmed batting order.
    # If not confirmed, allow a "projected" batting order (e.g., last-known).
    lineup_ids = confirmed_lineup_ids or projected_lineup_ids
    if lineup_ids:
        try:
            ordered_ids: List[int] = []
            seen = set()
            for x in lineup_ids:
                try:
                    pid = int(x)
                except Exception:
                    continue
                if pid <= 0 or pid in seen:
                    continue
                seen.add(pid)
                ordered_ids.append(pid)

            if ordered_ids:
                by_id = {b.player.mlbam_id: b for b in hitters}
                ordered = [by_id[pid] for pid in ordered_ids if pid in by_id]
                if ordered:
                    hitters = ordered + [b for b in hitters if b.player.mlbam_id not in set(ordered_ids)]
        except Exception:
            pass

    # Fallback: first 9 hitters (or pad with synthetic)
    if len(hitters) < 9:
        for i in range(9 - len(hitters)):
            hitters.append(
                BatterProfile(
                    player=Player(
                        mlbam_id=-(i + 1),
                        full_name=f"{team.abbreviation} Batter {i+1}",
                        primary_position="OF",
                        bat_side=Handedness.R,
                        throw_side=Handedness.R,
                    )
                )
            )
    lineup_batters = hitters[:9]
    bench = hitters[9:]

    # In fast_mode, keep the bench small to bound downstream API calls (platoon splits).
    if bool(fast_mode) and len(bench) > 3:
        bench = bench[:3]

    # Ensure we always have a reasonable bullpen so the simulator can make pitching changes.
    if len(bullpen) < 6:
        need = 6 - len(bullpen)
        for i in range(need):
            bullpen.append(
                PitcherProfile(
                    player=Player(
                        mlbam_id=-(100 + i + 1),
                        full_name=f"{team.abbreviation} Reliever {i+1}",
                        primary_position="P",
                        bat_side=Handedness.R,
                        throw_side=Handedness.R,
                    ),
                    arsenal=dict(_DEFAULT_ARSENAL),
                    role="MR",
                    stamina_pitches=25,
                    leverage_skill=0.45,
                )
            )

    def _clamp01(x: float) -> float:
        try:
            return float(max(0.0, min(1.0, float(x))))
        except Exception:
            return 1.0

    def _shrink_mult_dict(d: Dict[str, float] | None, alpha: float) -> Dict[str, float]:
        if not d:
            return {}
        a = _clamp01(alpha)
        if a <= 0.0:
            return {}
        if a >= 1.0:
            return dict(d)
        out: Dict[str, float] = {}
        for k, v in d.items():
            if not isinstance(v, (int, float)):
                continue
            m = float(v)
            # Safety clamp consistent with simulate.py.
            m = float(max(0.4, min(1.6, m)))
            out[str(k)] = float(1.0 + a * (m - 1.0))
        return out

    # Apply platoon split multipliers (StatsAPI) to relevant players only.
    # This keeps API usage bounded while still affecting all batters and arms we may use.
    try:
        if bool(enable_batter_platoon):
            for b in (lineup_batters or []) + (bench or []):
                _apply_platoon_splits_to_batter(client, b, season)
                if float(batter_platoon_alpha) != 1.0:
                    b.platoon_mult_vs_lhp = _shrink_mult_dict(getattr(b, "platoon_mult_vs_lhp", None), float(batter_platoon_alpha))
                    b.platoon_mult_vs_rhp = _shrink_mult_dict(getattr(b, "platoon_mult_vs_rhp", None), float(batter_platoon_alpha))
                _apply_home_away_splits_to_batter(client, b, season)
                if float(batter_home_away_alpha) != 1.0:
                    b.venue_mult_home = _shrink_mult_dict(getattr(b, "venue_mult_home", None), float(batter_home_away_alpha))
                    b.venue_mult_away = _shrink_mult_dict(getattr(b, "venue_mult_away", None), float(batter_home_away_alpha))
        if bool(enable_pitcher_platoon):
            _apply_platoon_splits_to_pitcher(client, starter, season)
            if float(pitcher_platoon_alpha) != 1.0:
                starter.platoon_mult_vs_lhb = _shrink_mult_dict(getattr(starter, "platoon_mult_vs_lhb", None), float(pitcher_platoon_alpha))
                starter.platoon_mult_vs_rhb = _shrink_mult_dict(getattr(starter, "platoon_mult_vs_rhb", None), float(pitcher_platoon_alpha))
            _apply_home_away_splits_to_pitcher(client, starter, season)
            if float(pitcher_home_away_alpha) != 1.0:
                starter.venue_mult_home = _shrink_mult_dict(getattr(starter, "venue_mult_home", None), float(pitcher_home_away_alpha))
                starter.venue_mult_away = _shrink_mult_dict(getattr(starter, "venue_mult_away", None), float(pitcher_home_away_alpha))
            for p in bullpen or []:
                _apply_platoon_splits_to_pitcher(client, p, season)
                if float(pitcher_platoon_alpha) != 1.0:
                    p.platoon_mult_vs_lhb = _shrink_mult_dict(getattr(p, "platoon_mult_vs_lhb", None), float(pitcher_platoon_alpha))
                    p.platoon_mult_vs_rhb = _shrink_mult_dict(getattr(p, "platoon_mult_vs_rhb", None), float(pitcher_platoon_alpha))
                _apply_home_away_splits_to_pitcher(client, p, season)
                if float(pitcher_home_away_alpha) != 1.0:
                    p.venue_mult_home = _shrink_mult_dict(getattr(p, "venue_mult_home", None), float(pitcher_home_away_alpha))
                    p.venue_mult_away = _shrink_mult_dict(getattr(p, "venue_mult_away", None), float(pitcher_home_away_alpha))
    except Exception:
        pass

    manager = ManagerProfile()
    # Apply optional per-team manager overrides derived from historical feeds.
    try:
        m = _load_manager_tendencies_anykey()
        entry = m.get(str(team.team_id)) or m.get(str(team.abbreviation))
        if isinstance(entry, dict):
            overrides = entry.get("recommended_manager_overrides") if "recommended_manager_overrides" in entry else entry
            if isinstance(overrides, dict):
                for fld in (
                    "pull_starter_pitch_count",
                    "starter_min_innings",
                    "starter_blowup_run_diff",
                    "closer_leverage_max_run_diff",
                    "use_closer_in_9th_only",
                    "pinch_hit_aggressiveness",
                ):
                    if fld in overrides and getattr(manager, fld, None) is not None:
                        try:
                            setattr(manager, fld, overrides[fld])
                        except Exception:
                            pass
    except Exception:
        pass

    lineup = Lineup(batters=lineup_batters, pitcher=starter, bench=bench, bullpen=bullpen)
    return TeamRoster(team=team, manager=manager, lineup=lineup)
