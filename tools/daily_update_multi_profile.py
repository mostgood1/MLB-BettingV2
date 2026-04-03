from __future__ import annotations

import argparse
from collections import Counter
from functools import lru_cache
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parents[1]

if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.market_pitcher_props import (
    american_implied_prob,
    market_side_probabilities,
    no_vig_over_prob,
    normalize_pitcher_name,
)
from sim_engine.prob_calibration import apply_prob_calibration
from sim_engine.data.statcast_bvp import (
    default_bvp_cache,
    hr_multiplier_from_bvp,
    pitcher_vs_batters_counts,
    rate_multiplier_from_bvp,
)
from sim_engine.data.statsapi import StatsApiClient, fetch_person_gamelog


HITTER_MARKET_ORDER: Tuple[str, ...] = (
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
)

DEFAULT_HITTER_EDGE_MIN_BY_MARKET: Dict[str, float] = {
    "hitter_runs": 0.10,
}

DEFAULT_HITTER_MODEL_PROB_MIN_BY_MARKET: Dict[str, float] = {
    "hitter_home_runs": 0.25,
}

PITCHER_MARKET_SPECS: Dict[str, Dict[str, str]] = {
    "outs": {
        "market_key": "outs",
        "dist_key": "outs_dist",
        "mean_key": "outs_mean",
    },
    "strikeouts": {
        "market_key": "strikeouts",
        "dist_key": "so_dist",
        "mean_key": "so_mean",
    },
}

PITCHER_MARKET_ALIASES: Dict[str, str] = {
    "k": "strikeouts",
    "ks": "strikeouts",
    "so": "strikeouts",
}

DEFAULT_LOCK_POLICY: Dict[str, Any] = {
    "totals_side": "best_edge_side",
    "totals_diff_min": 0.0,
    "totals_edge_min": 0.01,
    "ml_side": "best_edge_side",
    "ml_edge_min": 0.01,
    "hitter_edge_min": 0.0,
    "hitter_edge_min_by_market": dict(DEFAULT_HITTER_EDGE_MIN_BY_MARKET),
    "hitter_model_prob_min_by_market": dict(DEFAULT_HITTER_MODEL_PROB_MIN_BY_MARKET),
    "hitter_max_favorite_odds": -200,
    "hitter_hr_under_0_5_max_favorite_odds": -200,
    "pitcher_market": "best",
    "pitcher_side": "best_edge_side",
    "pitcher_edge_min": 0.01,
    "pitcher_max_favorite_odds": -200,
}

DEFAULT_STANDARD_STAKE_U = 1.0
DEFAULT_HITTER_STAKE_U = 0.25

DEFAULT_OFFICIAL_HITTER_SUBCAPS: Dict[str, int] = {
    "hitter_home_runs": 2,
    "hitter_hits": 4,
    "hitter_total_bases": 4,
    "hitter_runs": 1,
    "hitter_rbis": 0,
}

DEFAULT_OFFICIAL_CAP_PROFILE = "totals2_p3_tbheavy11_r1"
DEFAULT_OFFICIAL_CAPS: Dict[str, int] = {
    "totals": 2,
    "ml": 1,
    "pitcher_props": 3,
    "hitter_props": sum(DEFAULT_OFFICIAL_HITTER_SUBCAPS.values()),
}

KNOWN_OFFICIAL_CAP_PROFILES: Dict[str, Dict[str, Dict[str, int]]] = {
    "totals2_p3_tbheavy11_r1": {
        "caps": {
            "totals": 2,
            "ml": 1,
            "pitcher_props": 3,
            "hitter_props": 11,
        },
        "hitter_subcaps": {
            "hitter_home_runs": 2,
            "hitter_hits": 4,
            "hitter_total_bases": 4,
            "hitter_runs": 1,
            "hitter_rbis": 0,
        },
    },
    "tight_p3_tbheavy12_rbi0": {
        "caps": {
            "totals": 1,
            "ml": 1,
            "pitcher_props": 3,
            "hitter_props": 12,
        },
        "hitter_subcaps": {
            "hitter_home_runs": 2,
            "hitter_hits": 4,
            "hitter_total_bases": 4,
            "hitter_runs": 2,
            "hitter_rbis": 0,
        },
    },
    "nototals_p3_tbheavy10_r0": {
        "caps": {
            "totals": 0,
            "ml": 1,
            "pitcher_props": 3,
            "hitter_props": 10,
        },
        "hitter_subcaps": {
            "hitter_home_runs": 2,
            "hitter_hits": 4,
            "hitter_total_bases": 4,
            "hitter_runs": 0,
            "hitter_rbis": 0,
        },
    },
}

HITTER_MARKET_SPECS: Dict[str, Dict[str, Any]] = {
    "batter_home_runs": {
        "market": "hitter_home_runs",
        "label": "Hitter HRs",
        "prob_base": "hr",
        "mean_key": "hr_mean",
        "primary_lines": (0.5,),
    },
    "batter_hits": {
        "market": "hitter_hits",
        "label": "Hitter Hits",
        "prob_base": "hits",
        "mean_key": "h_mean",
        "primary_lines": (0.5,),
    },
    "batter_total_bases": {
        "market": "hitter_total_bases",
        "label": "Hitter Total Bases",
        "prob_base": "total_bases",
        "mean_key": "tb_mean",
        "primary_lines": (1.5,),
    },
    "batter_runs_scored": {
        "market": "hitter_runs",
        "label": "Hitter Runs",
        "prob_base": "runs",
        "mean_key": "r_mean",
        "primary_lines": (0.5,),
    },
    "batter_rbis": {
        "market": "hitter_rbis",
        "label": "Hitter RBIs",
        "prob_base": "rbi",
        "mean_key": "rbi_mean",
        "primary_lines": (0.5,),
    },
}

HITTER_PREDICTION_FIELDS: Dict[str, Tuple[str, str]] = {
    "hits_1plus": ("p_h_1plus_cal", "p_h_1plus"),
    "hits_2plus": ("p_h_2plus_cal", "p_h_2plus"),
    "hits_3plus": ("p_h_3plus_cal", "p_h_3plus"),
    "runs_1plus": ("p_r_1plus_cal", "p_r_1plus"),
    "runs_2plus": ("p_r_2plus_cal", "p_r_2plus"),
    "runs_3plus": ("p_r_3plus_cal", "p_r_3plus"),
    "rbi_1plus": ("p_rbi_1plus_cal", "p_rbi_1plus"),
    "rbi_2plus": ("p_rbi_2plus_cal", "p_rbi_2plus"),
    "rbi_3plus": ("p_rbi_3plus_cal", "p_rbi_3plus"),
    "rbi_4plus": ("p_rbi_4plus_cal", "p_rbi_4plus"),
    "total_bases_1plus": ("p_tb_1plus_cal", "p_tb_1plus"),
    "total_bases_2plus": ("p_tb_2plus_cal", "p_tb_2plus"),
    "total_bases_3plus": ("p_tb_3plus_cal", "p_tb_3plus"),
    "total_bases_4plus": ("p_tb_4plus_cal", "p_tb_4plus"),
    "total_bases_5plus": ("p_tb_5plus_cal", "p_tb_5plus"),
}


def _resolve_path(s: str) -> Path:
    p = Path(str(s))
    if not p.is_absolute():
        p = _ROOT / p
    return p


def _is_off(s: str) -> bool:
    v = str(s or "").strip().lower()
    return v in ("", "off", "none", "null", "0", "false")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(_ROOT.resolve()))
    except Exception:
        return str(path)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _market_entries_n(path: Path, *, root_key: str) -> int:
    if not path.exists() or not path.is_file():
        return 0
    try:
        doc = _read_json(path)
    except Exception:
        return 0
    if not isinstance(doc, dict):
        return 0
    meta_counts = (doc.get("meta") or {}).get("counts") or {}
    if root_key == "pitcher_props":
        try:
            return int(meta_counts.get("players") or 0)
        except Exception:
            return 0
    if root_key == "hitter_props":
        try:
            return int(meta_counts.get("players") or 0)
        except Exception:
            return 0
    payload = doc.get(root_key)
    return len(payload) if isinstance(payload, (list, dict)) else 0


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _load_json_cfg(path_str: str) -> Optional[Dict[str, Any]]:
    if _is_off(path_str):
        return None
    path = _resolve_path(path_str)
    if not path.exists():
        return None
    try:
        obj = _read_json(path)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _normalize_cap(value: Any) -> Optional[int]:
    try:
        ivalue = int(value)
    except Exception:
        return None
    return None if ivalue < 0 else ivalue


def _normalize_edge_min(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _normalized_official_caps(caps: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[int]]:
    src = dict(caps or {})
    return {
        market: _normalize_cap(src.get(market, DEFAULT_OFFICIAL_CAPS[market]))
        for market in DEFAULT_OFFICIAL_CAPS
    }


def _normalized_hitter_subcaps(subcaps: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[int]]:
    src = dict(subcaps or {})
    return {
        market: _normalize_cap(src.get(market, DEFAULT_OFFICIAL_HITTER_SUBCAPS[market]))
        for market in HITTER_MARKET_ORDER
    }


def _has_hitter_subcaps(subcaps: Dict[str, Optional[int]]) -> bool:
    return any(subcaps.get(market) is not None for market in HITTER_MARKET_ORDER)


def _hitter_edge_min_overrides(policy: Optional[Dict[str, Any]]) -> Dict[str, float]:
    if not isinstance(policy, dict):
        return dict(DEFAULT_HITTER_EDGE_MIN_BY_MARKET)
    default_edge = _normalize_edge_min(policy.get("hitter_edge_min"))
    raw_overrides = policy.get("hitter_edge_min_by_market") or {}
    if not isinstance(raw_overrides, dict):
        return {}
    out: Dict[str, float] = {}
    for market_name in HITTER_MARKET_ORDER:
        value = _normalize_edge_min(raw_overrides.get(market_name))
        if value is None:
            continue
        if default_edge is not None and abs(float(value) - float(default_edge)) <= 1e-12:
            continue
        out[str(market_name)] = float(value)
    return out


def _hitter_edge_min_for_market(policy: Optional[Dict[str, Any]], market_name: str) -> float:
    default_edge = _normalize_edge_min((policy or {}).get("hitter_edge_min"))
    default_value = float(default_edge) if default_edge is not None else 0.0
    if not isinstance(policy, dict):
        return default_value
    raw_overrides = policy.get("hitter_edge_min_by_market") or {}
    if not isinstance(raw_overrides, dict):
        return default_value
    override_value = _normalize_edge_min(raw_overrides.get(str(market_name)))
    return float(override_value) if override_value is not None else default_value


def _hitter_model_prob_min_for_market(policy: Optional[Dict[str, Any]], market_name: str) -> float:
    if not isinstance(policy, dict):
        return float(DEFAULT_HITTER_MODEL_PROB_MIN_BY_MARKET.get(str(market_name), 0.0))
    raw_overrides = policy.get("hitter_model_prob_min_by_market") or {}
    if not isinstance(raw_overrides, dict):
        return float(DEFAULT_HITTER_MODEL_PROB_MIN_BY_MARKET.get(str(market_name), 0.0))
    override_value = _normalize_edge_min(raw_overrides.get(str(market_name)))
    if override_value is not None:
        return float(override_value)
    return float(DEFAULT_HITTER_MODEL_PROB_MIN_BY_MARKET.get(str(market_name), 0.0))


def _policy_with_overrides(
    base_policy: Optional[Dict[str, Any]] = None,
    *,
    scalar_updates: Optional[Dict[str, Any]] = None,
    hitter_edge_updates: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(base_policy or DEFAULT_LOCK_POLICY)
    for key, value in (scalar_updates or {}).items():
        if value is None:
            continue
        out[str(key)] = value
    merged_hitter_edge = dict((out.get("hitter_edge_min_by_market") or {}))
    for market_name, value in (hitter_edge_updates or {}).items():
        edge_value = _normalize_edge_min(value)
        if edge_value is None:
            continue
        merged_hitter_edge[str(market_name)] = float(edge_value)
    out["hitter_edge_min_by_market"] = merged_hitter_edge
    return out


def _cap_text(value: Optional[int]) -> str:
    return "uncapped" if value is None else str(int(value))


def _selection_allowed(selected: Any, requested: Any) -> bool:
    selection = str(selected or "").strip().lower()
    policy_side = str(requested or "").strip().lower()
    if policy_side in {"", "best", "auto", "either", "both", "best_edge_side"}:
        return True
    return selection == policy_side


def _selected_side_prob_from_over_prob(over_prob: Any, selection: Any) -> Optional[float]:
    try:
        prob = float(over_prob)
    except Exception:
        return None
    choice = str(selection or "").strip().lower()
    if choice == "under":
        return float(1.0 - prob)
    return float(prob)


def _selected_side_prob_from_home_prob(home_prob: Any, selection: Any) -> Optional[float]:
    try:
        prob = float(home_prob)
    except Exception:
        return None
    choice = str(selection or "").strip().lower()
    if choice == "away":
        return float(1.0 - prob)
    return float(prob)


def _mean_support_for_selection(mean_value: Any, line_value: Any, selection: Any) -> Optional[float]:
    try:
        mean_float = float(mean_value)
        line_float = float(line_value)
    except Exception:
        return None
    choice = str(selection or "").strip().lower()
    gap = float(mean_float - line_float)
    if choice == "under":
        return float(-gap)
    if choice == "over":
        return float(gap)
    return None


def _passes_mean_alignment(mean_value: Any, line_value: Any, selection: Any, min_gap: Any) -> bool:
    support = _mean_support_for_selection(mean_value, line_value, selection)
    if support is None:
        return True
    try:
        threshold = float(min_gap)
    except Exception:
        threshold = 0.0
    return float(support) >= float(threshold)


def _select_moneyline_side(
    home_prob: Any,
    home_odds: Any,
    away_odds: Any,
    edge_min: Any,
    requested_side: Any,
) -> Optional[Dict[str, Any]]:
    try:
        model_home = float(home_prob)
    except Exception:
        return None
    home_market_prob, away_market_prob = _no_vig_two_way(home_odds, away_odds)
    if home_market_prob is None or away_market_prob is None:
        return None
    edge_floor = float(edge_min or 0.0)
    candidates = [
        {
            "selection": "home",
            "edge": float(model_home - home_market_prob),
            "selected_side_model_prob": float(model_home),
            "selected_side_market_prob": float(home_market_prob),
            "market_no_vig_prob": float(home_market_prob),
            "odds": home_odds,
        },
        {
            "selection": "away",
            "edge": float((1.0 - model_home) - away_market_prob),
            "selected_side_model_prob": float(1.0 - model_home),
            "selected_side_market_prob": float(away_market_prob),
            "market_no_vig_prob": float(home_market_prob),
            "odds": away_odds,
        },
    ]
    allowed = [row for row in candidates if _selection_allowed(row.get("selection"), requested_side)]
    if not allowed:
        return None
    best = max(allowed, key=lambda row: (float(row.get("edge") or 0.0), float(row.get("selected_side_model_prob") or 0.0)))
    return best if float(best.get("edge") or 0.0) >= edge_floor else None


def _format_reason_number(value: Any) -> str:
    try:
        num = float(value)
    except Exception:
        return "-"
    if abs(num - round(num)) <= 1e-9:
        return str(int(round(num)))
    return f"{num:.1f}"


def _format_reason_percent(value: Any) -> str:
    try:
        num = float(value)
    except Exception:
        return "-"
    return f"{num * 100.0:.1f}%"


def _format_reason_ratio(value: Any) -> str:
    try:
        num = float(value)
    except Exception:
        return "-"
    return f"{num:.2f}x"


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


def _season_from_date_str(value: Any) -> Optional[int]:
    token = str(value or "").strip()
    if len(token) < 4:
        return None
    return _safe_int(token[:4])


def _normalized_hitter_history_prop(prop: Any) -> str:
    raw = str(prop or "").strip().lower()
    mapping = {
        "batter_home_runs": "home_runs",
        "batter_hits": "hits",
        "batter_total_bases": "total_bases",
        "batter_runs_scored": "runs",
        "batter_rbis": "rbis",
    }
    return str(mapping.get(raw, raw))


@lru_cache(maxsize=1)
def _statsapi_reason_client_cached() -> StatsApiClient:
    client = StatsApiClient.with_default_cache(ttl_seconds=24 * 3600)
    client.timeout_sec = 4.0
    client.max_retries = 0
    return client


@lru_cache(maxsize=8192)
def _fetch_person_gamelog_cached(person_id: int, season: int, group: str) -> Tuple[Dict[str, Any], ...]:
    try:
        rows = fetch_person_gamelog(_statsapi_reason_client_cached(), int(person_id), int(season), str(group)) or []
    except Exception:
        rows = []
    return tuple(row for row in rows if isinstance(row, dict))


@lru_cache(maxsize=1)
def _statcast_bvp_reason_cache():
    return default_bvp_cache(ttl_seconds=30 * 24 * 3600)


@lru_cache(maxsize=2048)
def _pitcher_bvp_counts_cached(pitcher_id: int, season: int) -> Dict[int, Dict[str, int]]:
    try:
        rows = pitcher_vs_batters_counts(
            season=int(season),
            pitcher_id=int(pitcher_id),
            start_date=datetime(int(season), 1, 1).date(),
            end_date=datetime(int(season), 12, 31).date(),
            cache=_statcast_bvp_reason_cache(),
        )
    except Exception:
        rows = {}
    out: Dict[int, Dict[str, int]] = {}
    for batter_id, counts in (rows or {}).items():
        try:
            bid = int(batter_id)
        except Exception:
            continue
        out[bid] = {
            "pa": int(getattr(counts, "pa", 0) or 0),
            "hits": int(getattr(counts, "hits", 0) or 0),
            "hr": int(getattr(counts, "hr", 0) or 0),
            "so": int(getattr(counts, "so", 0) or 0),
            "bb": int(getattr(counts, "bb", 0) or 0),
            "hbp": int(getattr(counts, "hbp", 0) or 0),
            "inplay_pa": int(getattr(counts, "inplay_pa", 0) or 0),
            "inplay_hits": int(getattr(counts, "inplay_hits", 0) or 0),
        }
    return out


def _derived_hitter_bvp_history(
    batter_profile: Dict[str, Any],
    pitcher_profile: Dict[str, Any],
    season: Optional[int],
) -> Optional[Dict[str, float]]:
    batter_id = _safe_int((batter_profile or {}).get("id"))
    pitcher_id = _safe_int((pitcher_profile or {}).get("id"))
    season_i = _safe_int(season)
    if batter_id is None or pitcher_id is None or season_i is None:
        return None
    merged: Dict[str, int] = {
        "pa": 0,
        "hits": 0,
        "hr": 0,
        "so": 0,
        "bb": 0,
        "hbp": 0,
        "inplay_pa": 0,
        "inplay_hits": 0,
    }
    for season_part in range(max(2015, int(season_i) - 1), int(season_i) + 1):
        counts = (_pitcher_bvp_counts_cached(int(pitcher_id), int(season_part)) or {}).get(int(batter_id)) or {}
        for key in list(merged.keys()):
            merged[key] += int(counts.get(key) or 0)
    if int(merged.get("pa") or 0) <= 0:
        return None

    pa = int(merged.get("pa") or 0)
    inplay_pa = int(merged.get("inplay_pa") or 0)
    history = {
        "pa": float(pa),
        "hits": float(merged.get("hits") or 0),
        "hr": float(merged.get("hr") or 0),
        "so": float(merged.get("so") or 0),
        "bb": float(merged.get("bb") or 0),
        "hbp": float(merged.get("hbp") or 0),
        "inplay_pa": float(inplay_pa),
        "inplay_hits": float(merged.get("inplay_hits") or 0),
        "hr_mult": float(
            hr_multiplier_from_bvp(
                batter_hr_rate=float((batter_profile or {}).get("hr_rate") or 0.03),
                pa=pa,
                hr=int(merged.get("hr") or 0),
            )
        ),
        "k_mult": float(
            rate_multiplier_from_bvp(
                base_rate=float((batter_profile or {}).get("k_rate") or 0.22),
                opportunities=pa,
                successes=int(merged.get("so") or 0),
            )
        ),
        "bb_mult": float(
            rate_multiplier_from_bvp(
                base_rate=float((batter_profile or {}).get("bb_rate") or 0.08),
                opportunities=pa,
                successes=int(merged.get("bb") or 0),
            )
        ),
        "inplay_mult": float(
            rate_multiplier_from_bvp(
                base_rate=float((batter_profile or {}).get("inplay_hit_rate") or 0.28),
                opportunities=inplay_pa,
                successes=int(merged.get("inplay_hits") or 0),
            )
        ) if inplay_pa > 0 else 1.0,
    }
    return history


def _pitching_outs_from_stat(stat: Dict[str, Any]) -> Optional[float]:
    if not isinstance(stat, dict):
        return None
    outs_value = _safe_int(stat.get("outs"))
    if outs_value is not None:
        return float(outs_value)
    innings = str(stat.get("inningsPitched") or "").strip()
    if not innings:
        return None
    whole, _, frac = innings.partition(".")
    frac_outs = {"0": 0, "1": 1, "2": 2}.get(frac)
    if frac_outs is None:
        return None
    whole_outs = _safe_int(whole)
    if whole_outs is None:
        return None
    return float((int(whole_outs) * 3) + int(frac_outs))


def _history_metric_value(group: str, prop: str, stat: Dict[str, Any]) -> Optional[float]:
    if not isinstance(stat, dict):
        return None
    prop_key = str(prop or "").strip().lower()
    if str(group) == "pitching":
        if prop_key == "outs":
            return _pitching_outs_from_stat(stat)
        mapping = {
            "strikeouts": "strikeOuts",
            "earned_runs": "earnedRuns",
            "walks": "baseOnBalls",
            "hits": "hits",
            "batters_faced": "battersFaced",
            "pitches": "numberOfPitches",
        }
    else:
        prop_key = _normalized_hitter_history_prop(prop_key)
        mapping = {
            "hits": "hits",
            "home_runs": "homeRuns",
            "runs": "runs",
            "rbis": "rbi",
            "rbi": "rbi",
            "total_bases": "totalBases",
        }
    stat_key = mapping.get(prop_key)
    if not stat_key:
        return None
    try:
        return float(stat.get(stat_key))
    except Exception:
        return None


def _average_metric_from_logs(group: str, prop: str, rows: Sequence[Dict[str, Any]]) -> Optional[float]:
    values: List[float] = []
    for row in rows:
        stat = row.get("stat") if isinstance(row, dict) else None
        value = _history_metric_value(group, prop, stat if isinstance(stat, dict) else {})
        if value is None:
            continue
        values.append(float(value))
    if not values:
        return None
    return float(sum(values) / len(values))


def _recent_season_logs(person_id: int, season: int, group: str, *, seasons_back: int = 1) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start_season = max(2000, int(season) - max(0, int(seasons_back)))
    for season_i in range(start_season, int(season) + 1):
        out.extend(_fetch_person_gamelog_cached(int(person_id), int(season_i), str(group)))
    return list(out)


def _opponent_logs_recent_seasons(
    person_id: int,
    season: int,
    group: str,
    opponent_team_id: int,
    *,
    seasons_back: int = 1,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in _recent_season_logs(int(person_id), int(season), str(group), seasons_back=seasons_back):
        opponent = row.get("opponent") if isinstance(row, dict) else None
        if not isinstance(opponent, dict):
            continue
        if _safe_int(opponent.get("id")) == int(opponent_team_id):
            out.append(row)
    return out


def _prop_unit_label(prop: str) -> str:
    prop_key = _normalized_hitter_history_prop(prop)
    labels = {
        "strikeouts": "strikeouts",
        "outs": "outs",
        "earned_runs": "earned runs",
        "hits": "hits",
        "home_runs": "home runs",
        "runs": "runs",
        "rbis": "RBIs",
        "rbi": "RBIs",
        "total_bases": "total bases",
    }
    return str(labels.get(prop_key) or str(prop_key or "").replace("_", " "))


def _hitter_line_history_clause(
    prop: str,
    values: Sequence[float],
    *,
    selection: Optional[str] = None,
    line_value: Optional[float] = None,
    subject_name: Optional[str] = None,
) -> Optional[str]:
    sample = [float(value) for value in values]
    if not sample:
        return None
    choice = str(selection or "").strip().lower()
    line = float(line_value) if line_value is not None else None
    subject = str(subject_name or "he").strip()
    lower_subject = subject.lower()

    if choice in {"over", "under"} and line is not None:
        if choice == "over":
            count = sum(1 for value in sample if float(value) > line)
        else:
            count = sum(1 for value in sample if float(value) < line)
        total = len(sample)
        prop_key = _normalized_hitter_history_prop(prop)

        if prop_key == "home_runs" and line <= 0.5:
            if choice == "over":
                return f"{subject} has homered in {int(count)} of {int(total)} games"
            return f"{subject} has been held without a homer in {int(count)} of {int(total)} games"
        if prop_key == "hits" and line <= 1.5:
            if line <= 0.5:
                if choice == "over":
                    return f"{subject} has recorded a hit in {int(count)} of {int(total)} games"
                return f"{subject} has been held hitless in {int(count)} of {int(total)} games"
            if choice == "over":
                return f"{subject} has recorded multiple hits in {int(count)} of {int(total)} games"
            return f"{subject} has been held to one hit or fewer in {int(count)} of {int(total)} games"
        if prop_key in {"rbi", "rbis"} and line <= 0.5:
            if choice == "over":
                if lower_subject == "he":
                    return f"he has driven in a run in {int(count)} of {int(total)} games"
                return f"{subject} has driven in a run in {int(count)} of {int(total)} games"
            if lower_subject == "he":
                return f"he has been held without an RBI in {int(count)} of {int(total)} games"
            return f"{subject} has been held without an RBI in {int(count)} of {int(total)} games"
        if prop_key == "runs" and line <= 0.5:
            if choice == "over":
                if lower_subject == "he":
                    return f"he has scored in {int(count)} of {int(total)} games"
                return f"{subject} has scored in {int(count)} of {int(total)} games"
            if lower_subject == "he":
                return f"he has been held scoreless in {int(count)} of {int(total)} games"
            return f"{subject} has been held scoreless in {int(count)} of {int(total)} games"
        if prop_key == "total_bases" and line <= 1.5:
            if line <= 0.5:
                if choice == "over":
                    return f"{subject} has recorded at least one total base in {int(count)} of {int(total)} games"
                return f"{subject} has been held without a total base in {int(count)} of {int(total)} games"
            if choice == "over":
                return f"{subject} has cleared 1.5 total bases in {int(count)} of {int(total)} games"
            return f"{subject} has been held to one total base or fewer in {int(count)} of {int(total)} games"
        if choice == "over":
            return f"{subject} has cleared {_format_reason_number(line)} {_prop_unit_label(prop)} in {int(count)} of {int(total)} games"
        return f"{subject} has stayed under {_format_reason_number(line)} {_prop_unit_label(prop)} in {int(count)} of {int(total)} games"

    prop_key = _normalized_hitter_history_prop(prop)
    if prop_key == "home_runs":
        total = int(round(sum(sample)))
        if total <= 0:
            return f"{subject} has not homered"
        return f"{subject} has homered {int(total)} times"
    avg_value = float(sum(sample) / len(sample))
    return f"{subject} has averaged {_format_reason_number(avg_value)} {_prop_unit_label(prop)}"


def _history_supports_selection(
    values: Sequence[float],
    *,
    selection: Optional[str] = None,
    line_value: Optional[float] = None,
) -> bool:
    choice = str(selection or "").strip().lower()
    if choice not in {"over", "under"} or line_value is None:
        return True
    sample = [float(value) for value in values]
    if not sample:
        return False
    line = float(line_value)
    if choice == "over":
        hits = sum(1 for value in sample if float(value) > line)
    else:
        hits = sum(1 for value in sample if float(value) < line)
    return float(hits) > (float(len(sample)) / 2.0)


def _pitcher_recent_form_reason(
    pitcher_profile: Dict[str, Any],
    season: int,
    prop: str,
    *,
    selection: Optional[str] = None,
    line_value: Optional[float] = None,
    subject_name: Optional[str] = None,
) -> Optional[str]:
    pitcher_id = _safe_int((pitcher_profile or {}).get("id"))
    if pitcher_id is None or int(pitcher_id) <= 0:
        return None
    logs = _recent_season_logs(int(pitcher_id), int(season), "pitching", seasons_back=1)[-5:]
    values = [
        float(value)
        for value in (
            _history_metric_value("pitching", str(prop), (row.get("stat") or {}))
            for row in logs
        )
        if value is not None
    ]
    min_samples = 3 if str(selection or "").strip().lower() in {"over", "under"} and line_value is not None else 3
    if len(values) < min_samples:
        return None
    if not _history_supports_selection(values, selection=selection, line_value=line_value):
        return None
    avg_value = float(sum(values) / len(values))
    label = _prop_unit_label(str(prop))
    subject = str(subject_name or "He").strip()
    if str(prop) == "earned_runs":
        if subject.lower() == "he":
            return f"Across his last {int(len(values))} starts, he has allowed about {_format_reason_number(avg_value)} {label} per outing."
        return f"Across his last {int(len(values))} starts, {subject} has allowed about {_format_reason_number(avg_value)} {label} per outing."
    if subject.lower() == "he":
        return f"Across his last {int(len(values))} starts, he has averaged {_format_reason_number(avg_value)} {label}."
    return f"Across his last {int(len(values))} starts, {subject} has averaged {_format_reason_number(avg_value)} {label}."


def _pitcher_opponent_team_reason(
    pitcher_profile: Dict[str, Any],
    opponent_team_id: Optional[int],
    opponent_label: str,
    season: int,
    prop: str,
    *,
    selection: Optional[str] = None,
    line_value: Optional[float] = None,
    subject_name: Optional[str] = None,
) -> Optional[str]:
    pitcher_id = _safe_int((pitcher_profile or {}).get("id"))
    opponent_id = _safe_int(opponent_team_id)
    if pitcher_id is None or int(pitcher_id) <= 0 or opponent_id is None or int(opponent_id) <= 0:
        return None
    logs = _opponent_logs_recent_seasons(int(pitcher_id), int(season), "pitching", int(opponent_id), seasons_back=1)
    values = [
        float(value)
        for value in (
            _history_metric_value("pitching", str(prop), (row.get("stat") or {}))
            for row in logs
        )
        if value is not None
    ]
    min_samples = 2 if str(selection or "").strip().lower() in {"over", "under"} and line_value is not None else 2
    if len(values) < min_samples:
        return None
    if not _history_supports_selection(values, selection=selection, line_value=line_value):
        return None
    avg_value = float(sum(values) / len(values))
    subject = str(subject_name or "He").strip()
    opponent = str(opponent_label or "this opponent").strip()
    if str(prop) == "earned_runs":
        if subject.lower() == "he":
            return f"This season against {opponent}, he has allowed about {_format_reason_number(avg_value)} earned runs per outing across {int(len(values))} starts."
        return f"This season against {opponent}, {subject} has allowed about {_format_reason_number(avg_value)} earned runs per outing across {int(len(values))} starts."
    label = _prop_unit_label(str(prop))
    if subject.lower() == "he":
        return f"This season against {opponent}, he has averaged {_format_reason_number(avg_value)} {label} across {int(len(values))} starts."
    return f"This season against {opponent}, {subject} has averaged {_format_reason_number(avg_value)} {label} across {int(len(values))} starts."


def _hitter_recent_form_reason(
    batter_profile: Dict[str, Any],
    season: int,
    prop: str,
    *,
    selection: Optional[str] = None,
    line_value: Optional[float] = None,
    subject_name: Optional[str] = None,
) -> Optional[str]:
    batter_id = _safe_int((batter_profile or {}).get("id"))
    if batter_id is None or int(batter_id) <= 0:
        return None
    logs = _recent_season_logs(int(batter_id), int(season), "hitting", seasons_back=1)[-10:]
    values = [
        float(value)
        for value in (
            _history_metric_value("hitting", str(prop), (row.get("stat") or {}))
            for row in logs
        )
        if value is not None
    ]
    min_samples = 3 if str(selection or "").strip().lower() in {"over", "under"} and line_value is not None else 5
    if len(values) < min_samples:
        return None
    if not _history_supports_selection(values, selection=selection, line_value=line_value):
        return None
    clause = _hitter_line_history_clause(
        str(prop),
        values,
        selection=selection,
        line_value=line_value,
        subject_name=str(subject_name or "he"),
    )
    if not clause:
        return None
    return f"Over his last {int(len(values))} games, {clause}."


def _hitter_opponent_team_reason(
    batter_profile: Dict[str, Any],
    opponent_team_id: Optional[int],
    opponent_label: str,
    season: int,
    prop: str,
    *,
    selection: Optional[str] = None,
    line_value: Optional[float] = None,
    subject_name: Optional[str] = None,
) -> Optional[str]:
    batter_id = _safe_int((batter_profile or {}).get("id"))
    opponent_id = _safe_int(opponent_team_id)
    if batter_id is None or int(batter_id) <= 0 or opponent_id is None or int(opponent_id) <= 0:
        return None
    logs = _opponent_logs_recent_seasons(int(batter_id), int(season), "hitting", int(opponent_id), seasons_back=1)
    values = [
        float(value)
        for value in (
            _history_metric_value("hitting", str(prop), (row.get("stat") or {}))
            for row in logs
        )
        if value is not None
    ]
    min_samples = 2 if str(selection or "").strip().lower() in {"over", "under"} and line_value is not None else 3
    if len(values) < min_samples:
        return None
    if not _history_supports_selection(values, selection=selection, line_value=line_value):
        return None
    opponent = str(opponent_label or "this opponent").strip()
    clause = _hitter_line_history_clause(
        str(prop),
        values,
        selection=selection,
        line_value=line_value,
        subject_name=str(subject_name or "he"),
    )
    if not clause:
        return None
    return f"Against {opponent}, {clause}."


def _append_unique_reason(reasons: List[str], value: Optional[str]) -> None:
    text = str(value or "").strip()
    if not text or text in reasons:
        return
    reasons.append(text)


_RECOMMENDATION_BASEBALL_REASON_LIMIT = 5
_RECOMMENDATION_REASON_SENTENCE_LIMIT = 6
_EXPLANATION_SUPPORT_MIN_REASONS = 2
_LOW_SIM_REASON_SAMPLE_MIN = 25
_DEFAULT_LOCKED_POLICY_MIN_SIMS = 250


def _selection_choice(value: Any) -> str:
    return str(value or "").strip().lower()


def _argv_flag_value(argv: Sequence[str], flag: str) -> Optional[str]:
    values = list(argv or [])
    for index, item in enumerate(values):
        if str(item) != str(flag):
            continue
        next_index = index + 1
        if next_index >= len(values):
            return None
        return str(values[next_index])
    return None


def _sim_sample_size_from_sim_obj(sim_obj: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(sim_obj, dict):
        return None
    sim_payload = sim_obj.get("sim") if isinstance(sim_obj.get("sim"), dict) else None
    return _safe_int((sim_payload or {}).get("sims"))


def _sim_sample_size_from_row(row: Dict[str, Any]) -> Optional[int]:
    return _safe_int((row or {}).get("sim_sample_size"))


def _is_low_sim_reason_sample(sim_sample_size: Optional[int]) -> bool:
    return sim_sample_size is not None and int(sim_sample_size) < int(_LOW_SIM_REASON_SAMPLE_MIN)


def _selected_side_reason_sentence(row: Dict[str, Any], *, selection: str) -> Optional[str]:
    selected_model_prob = row.get("selected_side_model_prob")
    selected_market_prob = row.get("selected_side_market_prob")
    if selected_model_prob is None or selected_market_prob is None:
        return None
    sim_sample_size = _sim_sample_size_from_row(row)
    if _is_low_sim_reason_sample(sim_sample_size):
        sims_label = int(sim_sample_size) if sim_sample_size is not None else 0
        return (
            f"This snapshot only used {sims_label} sim{'s' if sims_label != 1 else ''}, so the model-side frequency is too coarse to quote; "
            f"the market is pricing the {selection or 'selected'} side closer to {_format_reason_percent(selected_market_prob)}."
        )
    return (
        f"The model lands on the {selection or 'selected'} side in {_format_reason_percent(selected_model_prob)} of sims, "
        f"while the market is pricing it closer to {_format_reason_percent(selected_market_prob)}."
    )


def _safe_profile_mult(profile: Optional[Dict[str, Any]], key: str) -> Optional[float]:
    if not isinstance(profile, dict):
        return None
    try:
        value = profile.get(key)
        return float(value) if value is not None else None
    except Exception:
        return None


def _weighted_pitch_metric(profile: Dict[str, Any], metric_key: str) -> Optional[float]:
    arsenal = profile.get("arsenal") if isinstance(profile, dict) else None
    metric_map = profile.get(metric_key) if isinstance(profile, dict) else None
    if not isinstance(arsenal, dict) or not isinstance(metric_map, dict):
        return None
    weighted = 0.0
    denom = 0.0
    for raw_pitch, raw_share in arsenal.items():
        try:
            pitch = str(raw_pitch).strip().upper()
            share = float(raw_share)
            metric = float(metric_map.get(pitch, 1.0))
        except Exception:
            continue
        if not pitch or share <= 0.0:
            continue
        weighted += float(share) * float(metric)
        denom += float(share)
    if denom <= 0.0:
        return None
    return float(weighted / denom)


def _trim_reason_list(reasons: Sequence[str]) -> List[str]:
    limit = max(1, int(_RECOMMENDATION_BASEBALL_REASON_LIMIT))
    cleaned = [str(reason or "").strip() for reason in reasons if str(reason or "").strip()]
    return cleaned[:limit]


def _recommendation_subject_label(row: Dict[str, Any]) -> str:
    for key in ("player_name", "pitcher_name"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    away = str(row.get("away_abbr") or row.get("away") or "").strip()
    home = str(row.get("home_abbr") or row.get("home") or "").strip()
    if away and home:
        return f"{away} @ {home}"
    return str(row.get("market_label") or row.get("market") or "pick").strip() or "pick"


def _recommendation_market_label(row: Dict[str, Any]) -> str:
    market = str(row.get("market") or "").strip().lower()
    if market == "pitcher_props":
        prop = str(row.get("prop") or "").strip().replace("_", " ")
        return f"pitcher_props:{prop}" if prop else "pitcher_props"
    if market in {"hitter_home_runs", "hitter_hits", "hitter_total_bases", "hitter_runs", "hitter_rbis"}:
        return market
    return market or "unknown"


def _explanation_diagnostic(
    row: Dict[str, Any],
    reasons: Sequence[str],
    baseball_reasons: Sequence[str],
) -> Dict[str, Any]:
    baseball_reason_list = [str(reason or "").strip() for reason in baseball_reasons if str(reason or "").strip()]
    total_reasons = [str(reason or "").strip() for reason in reasons if str(reason or "").strip()]
    baseball_reason_count = int(len(baseball_reason_list))
    if baseball_reason_count >= 3:
        status = "strong"
    elif baseball_reason_count >= int(_EXPLANATION_SUPPORT_MIN_REASONS):
        status = "supported"
    elif baseball_reason_count == 1:
        status = "thin"
    else:
        status = "none"
    return {
        "status": status,
        "flag_sparse_support": baseball_reason_count < int(_EXPLANATION_SUPPORT_MIN_REASONS),
        "support_min_reasons": int(_EXPLANATION_SUPPORT_MIN_REASONS),
        "baseball_reasons_n": baseball_reason_count,
        "reason_sentences_n": int(len(total_reasons)),
        "market": _recommendation_market_label(row),
        "subject": _recommendation_subject_label(row),
        "supporting_reasons": baseball_reason_list,
    }


def _collect_card_explanation_diagnostics(markets: Dict[str, Any]) -> Dict[str, Any]:
    status_counts: Dict[str, int] = {"strong": 0, "supported": 0, "thin": 0, "none": 0}
    market_rows: Dict[str, List[Dict[str, Any]]] = {}

    for market_name, market_payload in (markets or {}).items():
        if not isinstance(market_payload, dict):
            continue
        rows = market_payload.get("recommendations")
        if not isinstance(rows, list):
            continue
        collected: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            diagnostic = row.get("explanation_diagnostic")
            if not isinstance(diagnostic, dict):
                continue
            status = str(diagnostic.get("status") or "").strip().lower()
            if status in status_counts:
                status_counts[status] += 1
            collected.append(row)
        market_rows[str(market_name)] = collected

    selected_rows_n = int(sum(len(rows) for rows in market_rows.values()))
    sparse_examples: List[Dict[str, Any]] = []
    markets_summary: Dict[str, Any] = {}
    sparse_total = 0

    for market_name, rows in market_rows.items():
        sparse_rows = []
        for row in rows:
            diagnostic = row.get("explanation_diagnostic") or {}
            if bool(diagnostic.get("flag_sparse_support")):
                sparse_rows.append(row)
        sparse_total += int(len(sparse_rows))
        markets_summary[market_name] = {
            "selected_n": int(len(rows)),
            "sparse_support_n": int(len(sparse_rows)),
            "status_counts": {
                key: int(
                    sum(
                        1
                        for row in rows
                        if str(((row.get("explanation_diagnostic") or {}).get("status") or "")).strip().lower() == key
                    )
                )
                for key in status_counts.keys()
            },
            "examples": [
                {
                    "subject": str(((row.get("explanation_diagnostic") or {}).get("subject") or _recommendation_subject_label(row))),
                    "selection": str(row.get("selection") or ""),
                    "market": str(((row.get("explanation_diagnostic") or {}).get("market") or _recommendation_market_label(row))),
                    "baseball_reasons_n": int(((row.get("explanation_diagnostic") or {}).get("baseball_reasons_n") or 0)),
                    "reason_summary": str(row.get("reason_summary") or ""),
                }
                for row in sparse_rows[:3]
            ],
        }
        for row in sparse_rows:
            if len(sparse_examples) >= 10:
                break
            sparse_examples.append(
                {
                    "subject": str(((row.get("explanation_diagnostic") or {}).get("subject") or _recommendation_subject_label(row))),
                    "selection": str(row.get("selection") or ""),
                    "market": str(((row.get("explanation_diagnostic") or {}).get("market") or _recommendation_market_label(row))),
                    "baseball_reasons_n": int(((row.get("explanation_diagnostic") or {}).get("baseball_reasons_n") or 0)),
                    "reason_summary": str(row.get("reason_summary") or ""),
                }
            )

    return {
        "selected_rows_n": int(selected_rows_n),
        "sparse_support_n": int(sparse_total),
        "sparse_support_rate": (float(sparse_total) / float(selected_rows_n)) if selected_rows_n > 0 else 0.0,
        "support_min_reasons": int(_EXPLANATION_SUPPORT_MIN_REASONS),
        "status_counts": {key: int(value) for key, value in status_counts.items()},
        "markets": markets_summary,
        "sparse_support_examples": sparse_examples,
    }


def _row_explanation_diagnostic(row: Dict[str, Any]) -> Dict[str, Any]:
    diagnostic = row.get("explanation_diagnostic") if isinstance(row, dict) else None
    if isinstance(diagnostic, dict):
        return diagnostic
    baseball_reasons = _trim_reason_list((row or {}).get("baseball_reasons") or [])
    reasons = _build_recommendation_reasons({**(row or {}), "baseball_reasons": baseball_reasons}) if isinstance(row, dict) else []
    return _explanation_diagnostic((row or {}), reasons, baseball_reasons)


def _filter_playable_candidates_by_support(
    rows: Sequence[Dict[str, Any]],
    *,
    market_name: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        diagnostic = _row_explanation_diagnostic(row)
        if bool(diagnostic.get("flag_sparse_support")):
            removed.append(row)
        else:
            kept.append(row)
    audit = {
        "market": str(market_name),
        "evaluated_n": int(len([row for row in rows if isinstance(row, dict)])),
        "kept_n": int(len(kept)),
        "removed_sparse_support_n": int(len(removed)),
        "removed_examples": [
            {
                "subject": str((_row_explanation_diagnostic(row).get("subject") or _recommendation_subject_label(row))),
                "selection": str(row.get("selection") or ""),
                "market": str((_row_explanation_diagnostic(row).get("market") or _recommendation_market_label(row))),
                "baseball_reasons_n": int((_row_explanation_diagnostic(row).get("baseball_reasons_n") or 0)),
                "reason_summary": str(row.get("reason_summary") or ""),
            }
            for row in removed[:5]
        ],
    }
    return kept, audit


def _filter_candidates_by_support(
    rows: Sequence[Dict[str, Any]],
    *,
    market_name: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        diagnostic = _row_explanation_diagnostic(row)
        if bool(diagnostic.get("flag_sparse_support")):
            removed.append(row)
        else:
            kept.append(row)
    audit = {
        "market": str(market_name),
        "evaluated_n": int(len([row for row in rows if isinstance(row, dict)])),
        "kept_n": int(len(kept)),
        "removed_sparse_support_n": int(len(removed)),
        "removed_examples": [
            {
                "subject": str((_row_explanation_diagnostic(row).get("subject") or _recommendation_subject_label(row))),
                "selection": str(row.get("selection") or ""),
                "market": str((_row_explanation_diagnostic(row).get("market") or _recommendation_market_label(row))),
                "baseball_reasons_n": int((_row_explanation_diagnostic(row).get("baseball_reasons_n") or 0)),
                "reason_summary": str(row.get("reason_summary") or ""),
            }
            for row in removed[:5]
        ],
    }
    return kept, audit


def _audit_selected_support_policy(
    *,
    market_name: str,
    baseline_selected: Sequence[Dict[str, Any]],
    final_selected: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    baseline_ids = Counter(_candidate_row_id(row) for row in baseline_selected if isinstance(row, dict))
    final_ids = Counter(_candidate_row_id(row) for row in final_selected if isinstance(row, dict))
    displaced: List[Dict[str, Any]] = []
    replaced_n = 0

    for row in final_selected:
        if not isinstance(row, dict):
            continue
        row_id = _candidate_row_id(row)
        if baseline_ids.get(row_id, 0) > 0:
            baseline_ids[row_id] -= 1
        else:
            replaced_n += 1

    for row in baseline_selected:
        if not isinstance(row, dict):
            continue
        row_id = _candidate_row_id(row)
        if final_ids.get(row_id, 0) > 0:
            final_ids[row_id] -= 1
            continue
        diagnostic = _row_explanation_diagnostic(row)
        if bool(diagnostic.get("flag_sparse_support")):
            displaced.append(row)

    return {
        "market": str(market_name),
        "support_min_reasons": int(_EXPLANATION_SUPPORT_MIN_REASONS),
        "baseline_selected_n": int(len([row for row in baseline_selected if isinstance(row, dict)])),
        "final_selected_n": int(len([row for row in final_selected if isinstance(row, dict)])),
        "removed_sparse_support_n": int(len(displaced)),
        "replacement_added_n": int(replaced_n),
        "selection_shortfall_n": int(
            max(0, len([row for row in baseline_selected if isinstance(row, dict)]) - len([row for row in final_selected if isinstance(row, dict)]))
        ),
        "removed_examples": [
            {
                "subject": str((_row_explanation_diagnostic(row).get("subject") or _recommendation_subject_label(row))),
                "selection": str(row.get("selection") or ""),
                "market": str((_row_explanation_diagnostic(row).get("market") or _recommendation_market_label(row))),
                "baseball_reasons_n": int((_row_explanation_diagnostic(row).get("baseball_reasons_n") or 0)),
                "reason_summary": str(row.get("reason_summary") or ""),
            }
            for row in displaced[:5]
        ],
    }


_PITCH_TYPE_REASON_LABELS = {
    "FF": "four-seam fastball",
    "SI": "sinker",
    "FC": "cutter",
    "SL": "slider",
    "CH": "changeup",
    "CU": "curveball",
    "KC": "knuckle-curve",
    "SV": "sweeper",
    "FS": "splitter",
    "FO": "forkball",
    "CS": "slow curve",
    "KN": "knuckleball",
    "OTHER": "secondary mix",
}


def _pitch_type_reason_label(raw_pitch: Any) -> str:
    code = str(raw_pitch or "").strip().upper()
    return str(_PITCH_TYPE_REASON_LABELS.get(code) or code or "secondary mix")


def _join_reason_labels(labels: Sequence[str]) -> str:
    cleaned = [str(label or "").strip() for label in labels if str(label or "").strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return ", ".join(cleaned[:-1]) + f", and {cleaned[-1]}"


def _pitch_mix_reason(
    profile: Dict[str, Any],
    *,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
) -> Optional[str]:
    prop_key = str(prop or "").strip().lower()
    choice = _selection_choice(selection)
    arsenal = profile.get("arsenal")
    if not isinstance(arsenal, dict) or not arsenal:
        return None
    parts: List[Tuple[str, float]] = []
    for raw_pitch, raw_share in arsenal.items():
        try:
            pitch = str(raw_pitch).strip().upper()
            share = float(raw_share)
        except Exception:
            continue
        if not pitch or share <= 0.0:
            continue
        parts.append((pitch, share))
    if not parts:
        return None
    parts.sort(key=lambda item: item[1], reverse=True)
    top = parts[:3]
    pitch_names = [_pitch_type_reason_label(pitch) for pitch, _ in top]
    lead_share = float(top[0][1]) if top else 0.0
    whiff_score = _weighted_pitch_metric(profile, "pitch_type_whiff_mult")
    inplay_score = _weighted_pitch_metric(profile, "pitch_type_inplay_mult")

    if choice in {"over", "under"}:
        if prop_key == "strikeouts":
            if choice == "over" and whiff_score is not None and whiff_score >= 1.03:
                return f"His primary mix of { _join_reason_labels(pitch_names) } is still generating more swing-and-miss than baseline."
            if choice == "under" and whiff_score is not None and whiff_score <= 0.97:
                return f"His main mix of { _join_reason_labels(pitch_names) } is grading lighter on swing-and-miss than his baseline."
            return None
        if prop_key == "outs":
            if choice == "over" and inplay_score is not None and inplay_score <= 0.97:
                return f"The contact profile on his { _join_reason_labels(pitch_names) } points to a slightly cleaner path to quick outs."
            if choice == "over" and whiff_score is not None and whiff_score >= 1.03:
                return f"His mix of { _join_reason_labels(pitch_names) } is still missing enough bats to help him work deeper into the outing."
            if choice == "under" and inplay_score is not None and inplay_score >= 1.03:
                return f"The contact profile on his { _join_reason_labels(pitch_names) } is allowing a bit more quality contact than usual, which can shorten the outing."
            if choice == "under" and whiff_score is not None and whiff_score <= 0.97:
                return f"His main mix of { _join_reason_labels(pitch_names) } is not carrying the usual bat-missing support for a long outing."
            return None
        if prop_key == "earned_runs":
            if choice == "over" and inplay_score is not None and inplay_score >= 1.03:
                return f"His pitch mix of { _join_reason_labels(pitch_names) } is giving hitters a slightly friendlier contact look than baseline."
            if choice == "under" and inplay_score is not None and inplay_score <= 0.97:
                return f"His pitch mix of { _join_reason_labels(pitch_names) } is still suppressing contact quality a bit better than baseline."
            return None

    if lead_share >= 0.45:
        return f"He leans heavily on his {pitch_names[0]}, with { _join_reason_labels(pitch_names[1:]) } working as the main support." if len(pitch_names) > 1 else f"He leans heavily on his {pitch_names[0]}."
    return f"He mixes { _join_reason_labels(pitch_names) } often enough that hitters have to cover multiple looks."


def _opponent_lineup_reason(
    pitcher_profile: Dict[str, Any],
    opponent_lineup: List[Dict[str, Any]],
    *,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
) -> Optional[str]:
    if not isinstance(opponent_lineup, list) or not opponent_lineup:
        return None

    bats = Counter()
    for row in opponent_lineup:
        bat = str((row or {}).get("bat") or "").strip().upper()
        if bat in ("L", "R", "S"):
            bats[bat] += 1
    total = sum(bats.values())
    if total <= 0:
        return None

    opp_bits = []
    if bats.get("L"):
        opp_bits.append(f"{int(bats['L'])}L")
    if bats.get("R"):
        opp_bits.append(f"{int(bats['R'])}R")
    if bats.get("S"):
        opp_bits.append(f"{int(bats['S'])}S")
    opp_label = "/".join(opp_bits) if opp_bits else "-"

    platoon_lhb = pitcher_profile.get("platoon_mult_vs_lhb") if isinstance(pitcher_profile, dict) else None
    platoon_rhb = pitcher_profile.get("platoon_mult_vs_rhb") if isinstance(pitcher_profile, dict) else None

    def _platoon_value(key: str, *, bat: str) -> Optional[float]:
        source = platoon_lhb if bat == "L" else platoon_rhb
        if not isinstance(source, dict):
            return None
        value = source.get(key)
        return float(value) if isinstance(value, (int, float)) else None

    def _avg_platoon(key: str) -> Optional[float]:
        weighted = 0.0
        denom = 0
        for bat_key, count in bats.items():
            if bat_key not in ("L", "R"):
                continue
            val = _platoon_value(key, bat=bat_key)
            if val is None:
                continue
            weighted += float(val) * int(count)
            denom += int(count)
        if denom <= 0:
            return None
        return float(weighted / float(denom))

    k_mult = _avg_platoon("k")
    hr_mult = _avg_platoon("hr")

    platoon_bits: List[str] = []
    if k_mult is not None:
        platoon_bits.append(f"K {_format_reason_ratio(k_mult)}")
    if hr_mult is not None:
        platoon_bits.append(f"HR {_format_reason_ratio(hr_mult)}")

    arsenal = pitcher_profile.get("arsenal") if isinstance(pitcher_profile, dict) else None
    pitch_shares: List[Tuple[str, float]] = []
    if isinstance(arsenal, dict):
        for raw_pitch, raw_share in arsenal.items():
            try:
                pitch = str(raw_pitch).strip().upper()
                share = float(raw_share)
            except Exception:
                continue
            if pitch and share > 0.0:
                pitch_shares.append((pitch, share))
    mix_avg = None
    if pitch_shares:
        weighted_sum = 0.0
        count = 0
        for batter in opponent_lineup:
            vs_pitch_type = (batter or {}).get("vs_pitch_type")
            if not isinstance(vs_pitch_type, dict) or not vs_pitch_type:
                continue
            batter_mult = 0.0
            for pitch, share in pitch_shares:
                try:
                    mult = float(vs_pitch_type.get(pitch, 1.0))
                except Exception:
                    mult = 1.0
                batter_mult += float(share) * float(mult)
            weighted_sum += batter_mult
            count += 1
        if count > 0:
            mix_avg = float(weighted_sum / float(count))

    choice = _selection_choice(selection)
    prop_key = str(prop or "").strip().lower()

    handedness_bits: List[str] = []
    if bats.get("L"):
        handedness_bits.append(f"{int(bats['L'])} left-handed")
    if bats.get("R"):
        handedness_bits.append(f"{int(bats['R'])} right-handed")
    if bats.get("S"):
        handedness_bits.append(f"{int(bats['S'])} switch-hitting")
    handedness_label = _join_reason_labels(handedness_bits)

    supportive_bits: List[str] = []
    if prop_key == "strikeouts":
        if choice == "over":
            if mix_avg is not None and mix_avg <= 0.97:
                supportive_bits.append("this projected lineup grades a bit below average against his mix")
            if k_mult is not None and k_mult >= 1.03:
                supportive_bits.append("the handedness split adds some strikeout lift")
        elif choice == "under":
            if mix_avg is not None and mix_avg >= 1.03:
                supportive_bits.append("this projected lineup grades better than average against his mix")
            if k_mult is not None and k_mult <= 0.97:
                supportive_bits.append("the handedness split trims some strikeout upside")
    elif prop_key == "earned_runs":
        if choice == "over":
            if mix_avg is not None and mix_avg >= 1.03:
                supportive_bits.append("this projected lineup looks a little stronger than average against his mix")
            if hr_mult is not None and hr_mult >= 1.03:
                supportive_bits.append("the power risk comes in a little hotter than baseline")
        elif choice == "under":
            if mix_avg is not None and mix_avg <= 0.97:
                supportive_bits.append("this projected lineup grades a little below average against his mix")
            if hr_mult is not None and hr_mult <= 0.97:
                supportive_bits.append("the power risk also comes in lighter than baseline")
    elif prop_key == "outs":
        if choice == "over":
            if mix_avg is not None and mix_avg <= 0.97:
                supportive_bits.append("this projected lineup grades a little below average against his mix")
            if hr_mult is not None and hr_mult <= 0.97:
                supportive_bits.append("the damage profile is lighter than average")
        elif choice == "under":
            if mix_avg is not None and mix_avg >= 1.03:
                supportive_bits.append("this projected lineup grades better than average against his mix")
            if hr_mult is not None and hr_mult >= 1.03:
                supportive_bits.append("the damage profile is a bit hotter than average")

    if not supportive_bits:
        return None

    lead = f"The projected lineup is mostly {handedness_label}" if handedness_label else "The projected lineup"
    first = supportive_bits[0]
    rest = supportive_bits[1:]
    sentence = f"{lead}, and {first}"
    if rest:
        sentence += ", while " + ", while ".join(rest)
    return sentence + "."


def _pitcher_statcast_quality_reason(
    pitcher_profile: Dict[str, Any],
    *,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
) -> Optional[str]:
    quality = pitcher_profile.get("statcast_quality_mult") if isinstance(pitcher_profile, dict) else None
    if not isinstance(quality, dict):
        return None
    prop_key = str(prop or "").strip().lower()
    choice = _selection_choice(selection)
    k_mult = _safe_profile_mult(quality, "k")
    bb_mult = _safe_profile_mult(quality, "bb")
    hr_mult = _safe_profile_mult(quality, "hr")
    inplay_mult = _safe_profile_mult(quality, "inplay")

    if prop_key == "strikeouts":
        if choice == "over" and k_mult is not None and k_mult >= 1.03:
            return "His underlying bat-missing quality is still grading above baseline, which supports the strikeout ceiling."
        if choice == "under" and k_mult is not None and k_mult <= 0.97:
            return "His underlying bat-missing quality is grading a bit lighter than baseline, which supports the lower strikeout path."
        return None
    if prop_key == "earned_runs":
        if choice == "over" and ((hr_mult is not None and hr_mult >= 1.03) or (inplay_mult is not None and inplay_mult >= 1.03)):
            return "The underlying contact-quality profile is allowing a little more damage than baseline, which raises the run-risk case."
        if choice == "under" and ((hr_mult is not None and hr_mult <= 0.97) or (inplay_mult is not None and inplay_mult <= 0.97)):
            return "The underlying contact-quality profile is keeping damage a bit lighter than baseline, which supports the run suppression case."
        return None
    if prop_key == "outs":
        if choice == "over" and ((bb_mult is not None and bb_mult <= 0.97) or (inplay_mult is not None and inplay_mult <= 0.97)):
            return "His underlying profile is still limiting free passes and noisy contact enough to help the workload case."
        if choice == "under" and ((bb_mult is not None and bb_mult >= 1.03) or (inplay_mult is not None and inplay_mult >= 1.03)):
            return "His underlying profile is carrying a bit more traffic than baseline, which can shorten the outing."
        return None
    return None


def _pitcher_workload_reason(
    pitcher_profile: Dict[str, Any],
    *,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
) -> Optional[str]:
    prop_key = str(prop or "").strip().lower()
    if prop_key not in {"strikeouts", "outs"}:
        return None
    choice = _selection_choice(selection)
    stamina = _safe_int((pitcher_profile or {}).get("stamina_pitches"))
    availability = None
    try:
        raw_availability = (pitcher_profile or {}).get("availability_mult")
        availability = float(raw_availability) if raw_availability is not None else None
    except Exception:
        availability = None

    if choice == "over":
        if stamina is not None and int(stamina) >= 90:
            return f"His starter leash still looks solid at roughly {int(stamina)} pitches, which keeps the volume path available."
        if availability is not None and availability >= 1.03:
            return "The availability and usage profile still point to a full starter workload."
    elif choice == "under":
        if stamina is not None and int(stamina) <= 82:
            return f"The expected leash is closer to {int(stamina)} pitches than a deep-workload profile, which supports the shorter outing path."
        if availability is not None and availability <= 0.95:
            return "The availability signal is a bit lighter than a true full-workload starter profile."
    return None


def _pitcher_bvp_reason(
    pitcher_profile: Dict[str, Any],
    opponent_lineup: List[Dict[str, Any]],
) -> Optional[str]:
    if not isinstance(pitcher_profile, dict) or not isinstance(opponent_lineup, list) or not opponent_lineup:
        return None
    try:
        pitcher_id = int(pitcher_profile.get("id") or 0)
    except Exception:
        pitcher_id = 0
    if pitcher_id <= 0:
        return None

    total_pa = 0.0
    hitter_matches = 0
    weighted_k = 0.0
    weighted_hr = 0.0
    weighted_inplay = 0.0

    for batter in opponent_lineup:
        if not isinstance(batter, dict):
            continue
        history_map = batter.get("vs_pitcher_history")
        if not isinstance(history_map, dict):
            continue
        history = history_map.get(str(pitcher_id)) if str(pitcher_id) in history_map else history_map.get(pitcher_id)
        if not isinstance(history, dict):
            continue
        try:
            pa = float(history.get("pa") or 0.0)
        except Exception:
            pa = 0.0
        if pa <= 0.0:
            continue
        hitter_matches += 1
        total_pa += float(pa)
        try:
            weighted_k += float(pa) * float(history.get("k_mult") or 1.0)
        except Exception:
            weighted_k += float(pa)
        try:
            weighted_hr += float(pa) * float(history.get("hr_mult") or 1.0)
        except Exception:
            weighted_hr += float(pa)
        try:
            weighted_inplay += float(pa) * float(history.get("inplay_mult") or 1.0)
        except Exception:
            weighted_inplay += float(pa)

    if total_pa < 12.0 or hitter_matches < 2:
        return None

    avg_k = float(weighted_k / total_pa) if total_pa > 0.0 else 1.0
    avg_hr = float(weighted_hr / total_pa) if total_pa > 0.0 else 1.0
    avg_inplay = float(weighted_inplay / total_pa) if total_pa > 0.0 else 1.0

    bits: List[str] = []
    if avg_k >= 1.05:
        bits.append("their prior looks against him have come with a little more swing-and-miss than baseline")
    elif avg_k <= 0.95:
        bits.append("their prior looks against him have produced a little less swing-and-miss than baseline")
    if avg_inplay <= 0.95:
        bits.append("the contact they have made has turned into fewer hits than expected")
    elif avg_inplay >= 1.05:
        bits.append("the contact they have made has turned into hits a bit more often than expected")
    if avg_hr <= 0.94:
        bits.append("the damage profile has also come in lighter than a neutral matchup")
    elif avg_hr >= 1.06:
        bits.append("the damage profile has also come in a little hotter than a neutral matchup")

    if not bits:
        return None

    lead = bits[0]
    rest = bits[1:]
    sentence = f"There is some real lineup-level history here ({int(round(total_pa))} plate appearances across {int(hitter_matches)} hitters), and {lead}"
    if rest:
        sentence += ", while " + ", while ".join(rest)
    return sentence + "."


def _hitter_pitch_mix_reason(
    batter_profile: Dict[str, Any],
    pitcher_profile: Dict[str, Any],
    *,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
) -> Optional[str]:
    vs_pitch_type = batter_profile.get("vs_pitch_type") if isinstance(batter_profile, dict) else None
    arsenal = pitcher_profile.get("arsenal") if isinstance(pitcher_profile, dict) else None
    if not isinstance(vs_pitch_type, dict) or not isinstance(arsenal, dict):
        return None

    pitch_rows: List[Tuple[str, float, float]] = []
    weighted = 0.0
    share_total = 0.0
    for raw_pitch, raw_share in arsenal.items():
        try:
            pitch = str(raw_pitch).strip().upper()
            share = float(raw_share)
            mult = float(vs_pitch_type.get(pitch, 1.0))
        except Exception:
            continue
        if not pitch or share <= 0.0:
            continue
        pitch_rows.append((pitch, share, mult))
        weighted += float(share) * float(mult)
        share_total += float(share)
    if not pitch_rows or share_total <= 0.0:
        return None

    mix_score = float(weighted / share_total)
    pitch_rows.sort(key=lambda item: item[1], reverse=True)
    strong = [
        _pitch_type_reason_label(pitch)
        for pitch, share, mult in pitch_rows
        if share >= 0.12 and mult >= 1.05
    ][:2]
    weak = [
        _pitch_type_reason_label(pitch)
        for pitch, share, mult in pitch_rows
        if share >= 0.12 and mult <= 0.95
    ][:2]

    choice = _selection_choice(selection)
    prop_key = _normalized_hitter_history_prop(prop)
    if mix_score >= 1.04:
        if choice in {"", "over"}:
            if strong:
                return f"His profile lines up well with this starter's { _join_reason_labels(strong) }, so the overall pitch mix looks favorable for hard contact."
            return "His profile matches this starter's mix well enough to give the at-bat quality a small boost."
        return None
    if mix_score <= 0.96:
        if choice == "under" and prop_key in {"hits", "total_bases", "runs", "rbis", "rbi", "home_runs"}:
            if weak:
                return f"The tougher part of this matchup is the starter's { _join_reason_labels(weak) }, which pulls the pitch-mix look below his usual baseline."
            return "The starter's pitch mix grades a little less favorable than this hitter's usual baseline."
        return None
    return None


def _hitter_platoon_reason(
    batter_profile: Dict[str, Any],
    pitcher_profile: Dict[str, Any],
    *,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
) -> Optional[str]:
    if not isinstance(batter_profile, dict) or not isinstance(pitcher_profile, dict):
        return None
    throw_hand = str(pitcher_profile.get("throw") or pitcher_profile.get("handedness") or "").strip().upper()
    if throw_hand not in {"L", "R"}:
        return None
    platoon_key = "platoon_mult_vs_lhp" if throw_hand == "L" else "platoon_mult_vs_rhp"
    platoon = batter_profile.get(platoon_key)
    if not isinstance(platoon, dict):
        return None

    inplay = platoon.get("inplay")
    hr_mult = platoon.get("hr")
    k_mult = platoon.get("k")
    try:
        inplay_v = float(inplay) if inplay is not None else None
    except Exception:
        inplay_v = None
    try:
        hr_v = float(hr_mult) if hr_mult is not None else None
    except Exception:
        hr_v = None
    try:
        k_v = float(k_mult) if k_mult is not None else None
    except Exception:
        k_v = None

    choice = _selection_choice(selection)
    prop_key = _normalized_hitter_history_prop(prop)

    if (inplay_v is not None and inplay_v >= 1.05) or (hr_v is not None and hr_v >= 1.05):
        if choice in {"", "over"}:
            return f"The handedness matchup leans his way here, with his expected damage against {throw_hand}-handed pitching grading above baseline."
        return None
    if (inplay_v is not None and inplay_v <= 0.95) or (k_v is not None and k_v >= 1.05):
        if choice == "under" and prop_key in {"hits", "total_bases", "runs", "rbis", "rbi", "home_runs"}:
            return f"The handedness matchup is a little tougher than usual, so this spot comes with more swing-and-miss risk against {throw_hand}-handed pitching."
        return None
    return None


def _hitter_statcast_quality_reason(
    batter_profile: Dict[str, Any],
    *,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
) -> Optional[str]:
    quality = batter_profile.get("statcast_quality_mult") if isinstance(batter_profile, dict) else None
    if not isinstance(quality, dict):
        return None
    prop_key = _normalized_hitter_history_prop(prop)
    choice = _selection_choice(selection)
    k_mult = _safe_profile_mult(quality, "k")
    hr_mult = _safe_profile_mult(quality, "hr")
    inplay_mult = _safe_profile_mult(quality, "inplay")

    if prop_key == "home_runs":
        if choice == "over" and hr_mult is not None and hr_mult >= 1.03:
            return "His underlying batted-ball quality is still running strong enough to keep the home-run path live."
        if choice == "under" and hr_mult is not None and hr_mult <= 0.97:
            return "His underlying damage quality is a bit lighter than baseline, which supports the lower home-run path."
        return None

    if prop_key in {"hits", "total_bases", "runs", "rbis", "rbi"}:
        if choice == "over":
            if inplay_mult is not None and inplay_mult >= 1.03:
                return "His underlying contact quality is grading above baseline, which supports the production side of the prop."
            if k_mult is not None and k_mult <= 0.97:
                return "His underlying strikeout risk is running below baseline, which helps the ball-in-play volume case."
            if hr_mult is not None and hr_mult >= 1.03 and prop_key in {"total_bases", "runs", "rbis", "rbi"}:
                return "His underlying damage quality is strong enough to support the extra-base production path."
        elif choice == "under":
            if inplay_mult is not None and inplay_mult <= 0.97:
                return "His underlying contact quality is coming in a bit lighter than baseline, which supports the under path."
            if k_mult is not None and k_mult >= 1.03:
                return "His underlying strikeout pressure is elevated enough to support the lower-volume outcome."
            if hr_mult is not None and hr_mult <= 0.97 and prop_key in {"total_bases", "runs", "rbis", "rbi"}:
                return "His underlying damage quality is lighter than baseline, which trims the extra-base ceiling."
    return None


def _hitter_bvp_reason(
    batter_profile: Dict[str, Any],
    pitcher_profile: Dict[str, Any],
    *,
    season: Optional[int] = None,
    prop: Optional[str] = None,
    selection: Optional[str] = None,
    line_value: Optional[float] = None,
) -> Optional[str]:
    if not isinstance(batter_profile, dict) or not isinstance(pitcher_profile, dict):
        return None
    try:
        pitcher_id = int(pitcher_profile.get("id") or 0)
    except Exception:
        pitcher_id = 0
    if pitcher_id <= 0:
        return None
    history_map = batter_profile.get("vs_pitcher_history")
    history = None
    if isinstance(history_map, dict):
        history = history_map.get(str(pitcher_id)) if str(pitcher_id) in history_map else history_map.get(pitcher_id)
    if not isinstance(history, dict):
        history = _derived_hitter_bvp_history(batter_profile, pitcher_profile, season)
    if not isinstance(history, dict):
        return None

    try:
        pa = int(round(float(history.get("pa") or 0)))
    except Exception:
        pa = 0
    if pa < 3:
        return None

    prop_key = _normalized_hitter_history_prop(prop)
    side = str(selection or "").strip().lower()
    hits = _safe_int(history.get("hits")) or 0
    homers = _safe_int(history.get("hr")) or 0
    supportive_bits: List[str] = []
    caution_bits: List[str] = []
    try:
        inplay_mult = float(history.get("inplay_mult") or 1.0)
        if inplay_mult >= 1.06:
            supportive_bits.append("he has turned balls in play against this starter into hits a little more often than his usual rate")
        elif inplay_mult <= 0.94:
            caution_bits.append("he has not converted many balls in play into hits against this starter")
    except Exception:
        pass
    try:
        hr_mult = float(history.get("hr_mult") or 1.0)
        if hr_mult >= 1.08:
            supportive_bits.append("the head-to-head sample has shown a bit more damage than baseline")
        elif hr_mult <= 0.94:
            caution_bits.append("the head-to-head damage has been lighter than baseline")
    except Exception:
        pass
    try:
        k_mult = float(history.get("k_mult") or 1.0)
        if k_mult <= 0.94:
            supportive_bits.append("he has also managed the strikeout risk well in prior meetings")
        elif k_mult >= 1.08:
            caution_bits.append("the prior meetings have come with elevated strikeout pressure")
    except Exception:
        pass

    preferred_bits: List[str] = []
    fallback_bits: List[str] = []
    if side == "under":
        preferred_bits = caution_bits
        fallback_bits = supportive_bits
    else:
        preferred_bits = supportive_bits
        fallback_bits = caution_bits

    if prop_key == "home_runs":
        if side == "over":
            homer_text = "no homers" if homers <= 0 else ("1 homer" if homers == 1 else f"{int(homers)} homers")
            if supportive_bits:
                return f"Against this starter, he has {homer_text} in {pa} prior plate appearances, and {supportive_bits[0]}."
            return f"Against this starter, he has {homer_text} in {pa} prior plate appearances."
        if side == "under" and caution_bits:
            return f"Against this starter, he has {homers} homers in {pa} prior plate appearances, and {caution_bits[0]}."
        return f"Against this starter, he has {homers} homers in {pa} prior plate appearances."
    elif prop_key in {"hits", "total_bases", "runs", "rbis", "rbi"}:
        if side == "over" and not preferred_bits:
            return None
        hit_label = "hit" if int(hits) == 1 else "hits"
        lead = f"Against this starter, he has {hits} {hit_label}"
        if homers > 0:
            lead += f", including {homers} homer{'s' if homers != 1 else ''}"
        lead += f" in {pa} prior plate appearances"
        if preferred_bits:
            return f"{lead}, and {preferred_bits[0]}."
        return lead + "."

    if preferred_bits:
        return f"Against this starter, he has seen {pa} prior plate appearances, and {preferred_bits[0]}."
    if fallback_bits and pa >= 5:
        return f"Against this starter, he has seen {pa} prior plate appearances, though {fallback_bits[0]}."
    if pa >= 8:
        return f"Against this starter, he has seen {pa} prior plate appearances, even if the prior meetings have been fairly neutral overall."
    if line_value is not None and pa >= 3:
        return f"Against this starter, he has seen {pa} prior plate appearances."
    return None


def _lookup_hitter_matchup_context(
    sim_obj: Dict[str, Any],
    rec: Dict[str, Any],
    roster_snapshot: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not isinstance(roster_snapshot, dict):
        return {}
    team = str(rec.get("team") or "").strip().upper()
    if not team:
        return {}

    away_abbr = str(((sim_obj.get("away") or {}).get("abbreviation") or "")).strip().upper()
    home_abbr = str(((sim_obj.get("home") or {}).get("abbreviation") or "")).strip().upper()
    if team == away_abbr:
        side = "away"
        opp_side = "home"
    elif team == home_abbr:
        side = "home"
        opp_side = "away"
    else:
        return {}

    side_doc = roster_snapshot.get(side)
    opp_doc = roster_snapshot.get(opp_side)
    if not isinstance(side_doc, dict) or not isinstance(opp_doc, dict):
        return {}

    lineup = side_doc.get("lineup") if isinstance(side_doc.get("lineup"), list) else []
    target_name = normalize_pitcher_name(str(rec.get("name") or ""))
    target_order = rec.get("lineup_order")
    batter_profile = None
    for row in lineup:
        if not isinstance(row, dict):
            continue
        if target_name and normalize_pitcher_name(str(row.get("name") or "")) == target_name:
            batter_profile = row
            break
        try:
            if batter_profile is None and target_order is not None and int(row.get("lineup_order") or 0) == int(target_order):
                batter_profile = row
        except Exception:
            pass
    pitcher_profile = opp_doc.get("starter_profile") if isinstance(opp_doc.get("starter_profile"), dict) else None
    if not isinstance(batter_profile, dict) or not isinstance(pitcher_profile, dict):
        return {}
    return {
        "batter_profile": batter_profile,
        "pitcher_profile": pitcher_profile,
        "opponent": str((sim_obj.get(opp_side) or {}).get("abbreviation") or "").strip(),
        "opponent_team_id": _safe_int((((opp_doc.get("team") or {}) if isinstance(opp_doc, dict) else {}).get("team_id"))),
    }


def _reason_paragraph(reasons: Sequence[str], *, max_sentences: int = _RECOMMENDATION_REASON_SENTENCE_LIMIT) -> str:
    cleaned = [str(item or "").strip() for item in reasons if str(item or "").strip()]
    if not cleaned:
        return ""
    limited = cleaned[: max(1, int(max_sentences))]
    return " ".join(limited)


def _build_recommendation_reasons(row: Dict[str, Any]) -> List[str]:
    market = str(row.get("market") or "").strip().lower()
    selection = str(row.get("selection") or "").strip().lower()
    reasons: List[str] = []

    selected_side_reason = _selected_side_reason_sentence(row, selection=selection)
    if selected_side_reason:
        reasons.append(selected_side_reason)

    baseball_reasons = row.get("baseball_reasons")
    if isinstance(baseball_reasons, list):
        for item in baseball_reasons:
            text = str(item or "").strip()
            if text:
                reasons.append(text)

    if market == "totals":
        if row.get("model_mean_total") is not None and row.get("market_line") is not None:
            reasons.append(
                f"The game is projecting around {_format_reason_number(row.get('model_mean_total'))} runs against a line of {_format_reason_number(row.get('market_line'))}."
            )
    elif market == "ml":
        team_label = str(row.get("home_abbr") or row.get("home") or "Home") if selection == "home" else str(row.get("away_abbr") or row.get("away") or "Away")
        if row.get("selected_side_model_prob") is not None and not _is_low_sim_reason_sample(_sim_sample_size_from_row(row)):
            reasons.append(f"{team_label} wins this matchup in about {_format_reason_percent(row.get('selected_side_model_prob'))} of model runs.")
    elif market == "pitcher_props":
        prop_label = str(row.get("prop") or "prop").replace("_", " ")
        mean_key = str(PITCHER_MARKET_SPECS.get(str(row.get("prop") or ""), {}).get("mean_key") or "")
        if mean_key and row.get(mean_key) is not None and row.get("market_line") is not None:
            reasons.append(
                f"The model baseline sits around {_format_reason_number(row.get(mean_key))} {prop_label} against a line of {_format_reason_number(row.get('market_line'))}."
            )
        opponent = row.get("away_abbr") if str(row.get("team_side") or "") == "home" else row.get("home_abbr")
        if opponent:
            reasons.append(f"If he stays on his normal starter path, the matchup against {opponent} gives him a fair shot to reach full workload volume.")
    else:
        prop_label = str(row.get("prop") or "prop").replace("_", " ")
        mean_key = str(HITTER_MARKET_SPECS.get(str(row.get("prop_market_key") or ""), {}).get("mean_key") or "")
        if mean_key and row.get(mean_key) is not None and row.get("market_line") is not None:
            reasons.append(
                f"The model baseline comes in around {_format_reason_number(row.get(mean_key))} {prop_label} against a line of {_format_reason_number(row.get('market_line'))}."
            )
        lineup_order = row.get("lineup_order")
        pa_mean = row.get("pa_mean")
        if isinstance(lineup_order, int) and pa_mean is not None:
            reasons.append(
                f"He is projected to hit in the {int(lineup_order)} spot, which points to about {_format_reason_number(pa_mean)} plate appearances."
            )
        elif pa_mean is not None:
            reasons.append(f"The playing-time outlook points to about {_format_reason_number(pa_mean)} plate appearances, which keeps the volume case in play.")

    return reasons


def _annotate_recommendation(row: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(row)
    baseball_reasons = _trim_reason_list(item.get("baseball_reasons") or [])
    item["baseball_reasons"] = list(baseball_reasons)
    reasons = _build_recommendation_reasons(item)
    item["explanation_diagnostic"] = _explanation_diagnostic(item, reasons, baseball_reasons)
    if reasons:
        paragraph = _reason_paragraph(reasons)
        item["reasons"] = ([paragraph] if paragraph else reasons)
        item["reason_summary"] = paragraph or reasons[0]
    return item


def _official_cap_profile_name(
    caps: Dict[str, Optional[int]],
    hitter_subcaps: Optional[Dict[str, Any]] = None,
) -> str:
    normalized_caps = _normalized_official_caps(caps)
    normalized_hitter_subcaps = _normalized_hitter_subcaps(hitter_subcaps)
    for profile_name, profile_spec in KNOWN_OFFICIAL_CAP_PROFILES.items():
        if normalized_caps != _normalized_official_caps(profile_spec.get("caps")):
            continue
        if normalized_hitter_subcaps != _normalized_hitter_subcaps(profile_spec.get("hitter_subcaps")):
            continue
        return str(profile_name)
    return "custom"


def _half_line_to_threshold(line: Any) -> Optional[int]:
    try:
        line_value = float(line)
    except Exception:
        return None
    threshold = int(round(line_value + 0.5))
    if threshold < 1:
        return None
    expected_line = float(threshold) - 0.5
    if abs(line_value - expected_line) > 1e-9:
        return None
    return int(threshold)


def _hitter_prob_key(prop_base: str, threshold: int) -> str:
    return f"{str(prop_base)}_{int(threshold)}plus"


def _locked_policy_selected_counts(card: Optional[Dict[str, Any]]) -> Optional[Dict[str, int]]:
    if not isinstance(card, dict):
        return None
    markets = card.get("markets") or {}
    market_groups = card.get("market_groups") or {}
    counts: Dict[str, int] = {
        market: int((markets.get(market) or {}).get("selected_n", 0))
        for market in ("totals", "ml", "pitcher_props")
    }
    counts["hitter_props"] = int((market_groups.get("hitter_props") or {}).get("selected_n", 0))
    for market in HITTER_MARKET_ORDER:
        counts[market] = int((markets.get(market) or {}).get("selected_n", 0))
    return counts


def _mean_from_dist(dist: Dict[str, Any]) -> Optional[float]:
    total = 0
    weighted = 0.0
    for raw_bucket, raw_count in (dist or {}).items():
        try:
            bucket = float(raw_bucket)
            count = int(raw_count)
        except Exception:
            continue
        total += count
        weighted += float(bucket) * float(count)
    if total <= 0:
        return None
    return float(weighted / float(total))


def _prob_over_line_from_dist(dist: Dict[str, Any], line: float) -> Optional[float]:
    total = 0
    over = 0
    for raw_bucket, raw_count in (dist or {}).items():
        try:
            bucket = float(raw_bucket)
            count = int(raw_count)
        except Exception:
            continue
        total += count
        if float(bucket) > float(line):
            over += count
    if total <= 0:
        return None
    return float(over / float(total))


def _no_vig_two_way(home_odds: Any, away_odds: Any) -> Tuple[Optional[float], Optional[float]]:
    home_prob = american_implied_prob(home_odds)
    away_prob = american_implied_prob(away_odds)
    if home_prob is None or away_prob is None:
        return None, None
    denom = float(home_prob + away_prob)
    if denom <= 0.0:
        return None, None
    return float(home_prob / denom), float(away_prob / denom)


def _iter_sim_records(sim_dir: Path) -> List[Dict[str, Any]]:
    if not sim_dir.exists():
        return []
    out: List[Dict[str, Any]] = []
    for path in sorted(sim_dir.glob("sim_*.json")):
        try:
            obj = _read_json(path)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _base_game_row(sim_obj: Dict[str, Any], market_game: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    schedule = sim_obj.get("schedule") or {}
    return {
        "date": str(sim_obj.get("date") or ""),
        "game_pk": sim_obj.get("game_pk"),
        "away": (sim_obj.get("away") or {}).get("name"),
        "home": (sim_obj.get("home") or {}).get("name"),
        "away_abbr": (sim_obj.get("away") or {}).get("abbreviation"),
        "home_abbr": (sim_obj.get("home") or {}).get("abbreviation"),
        "double_header": schedule.get("double_header"),
        "game_number": schedule.get("game_number"),
        "event_id": (market_game or {}).get("event_id"),
        "commence_time": (market_game or {}).get("commence_time"),
    }


def _collect_game_recommendations(sim_dir: Path, game_lines_path: Path, policy: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {"totals": [], "ml": []}
    if not game_lines_path.exists():
        return out

    snapshots_dir = _ROOT / "data" / "daily" / "snapshots"
    roster_cache: Dict[Tuple[int, int], Optional[Dict[str, Any]]] = {}

    def _roster_for(sim_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        date_str = str(sim_obj.get("date") or "").strip()
        if not date_str:
            return None
        day_dir = snapshots_dir / date_str
        if not day_dir.exists():
            return None
        game_pk = _safe_int(sim_obj.get("game_pk"))
        if game_pk is None or int(game_pk) <= 0:
            return None
        game_number = _safe_int(((sim_obj.get("schedule") or {}).get("game_number") or 1)) or 1
        cache_key = (int(game_pk), int(game_number))
        if cache_key in roster_cache:
            return roster_cache[cache_key]
        doc = None
        matches = sorted(day_dir.glob(f"roster_*_pk{int(game_pk)}_g{int(game_number)}.json"))
        if not matches:
            matches = sorted(day_dir.glob(f"roster_*_pk{int(game_pk)}_g*.json"))
        if matches:
            try:
                raw = _read_json(matches[0])
                doc = raw if isinstance(raw, dict) else None
            except Exception:
                doc = None
        roster_cache[cache_key] = doc
        return doc

    games = (_read_json(game_lines_path).get("games") or [])
    line_lookup = {
        (g.get("away_team"), g.get("home_team")): g
        for g in games
        if isinstance(g, dict) and g.get("away_team") and g.get("home_team")
    }

    for sim_obj in _iter_sim_records(sim_dir):
        away_name = (sim_obj.get("away") or {}).get("name")
        home_name = (sim_obj.get("home") or {}).get("name")
        market_game = line_lookup.get((away_name, home_name))
        if not market_game:
            continue

        base = _base_game_row(sim_obj, market_game)
        roster_snapshot = _roster_for(sim_obj)
        season_value = _season_from_date_str(base.get("date")) or _safe_int(sim_obj.get("season")) or datetime.now().year
        full = ((sim_obj.get("sim") or {}).get("segments") or {}).get("full") or {}

        totals_market = ((market_game.get("markets") or {}).get("totals") or {})
        total_line = totals_market.get("line")
        mean_total = _mean_from_dist(full.get("total_runs_dist") or {})
        p_over_total = None
        if total_line is not None:
            p_over_total = _prob_over_line_from_dist(full.get("total_runs_dist") or {}, float(total_line))
        if total_line is not None and mean_total is not None and p_over_total is not None:
            side_pick = _select_market_side(
                float(p_over_total),
                totals_market.get("over_odds"),
                totals_market.get("under_odds"),
                float(policy.get("totals_edge_min") or 0.0),
            )
            if side_pick is not None and _selection_allowed(side_pick.get("selection"), policy.get("totals_side")):
                selection = str(side_pick.get("selection") or "")
                if _passes_mean_alignment(mean_total, total_line, selection, policy.get("totals_diff_min")):
                    totals_reasons: List[str] = []
                    if isinstance(roster_snapshot, dict):
                        for pitcher_side, opponent_side in (("home", "away"), ("away", "home")):
                            side_doc = roster_snapshot.get(pitcher_side) if isinstance(roster_snapshot.get(pitcher_side), dict) else {}
                            opp_doc = roster_snapshot.get(opponent_side) if isinstance(roster_snapshot.get(opponent_side), dict) else {}
                            pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else {}
                            opponent_lineup = opp_doc.get("lineup") if isinstance(opp_doc.get("lineup"), list) else []
                            opponent_team = sim_obj.get(opponent_side) if isinstance(sim_obj.get(opponent_side), dict) else {}
                            opponent_id = _safe_int(opponent_team.get("id"))
                            opponent_label = str(opponent_team.get("abbreviation") or opponent_team.get("name") or "opponent").strip()
                            subject_name = str(pitcher_profile.get("name") or "").strip() or None
                            _append_unique_reason(
                                totals_reasons,
                                _pitcher_opponent_team_reason(
                                    pitcher_profile,
                                    opponent_id,
                                    opponent_label,
                                    int(season_value),
                                    "earned_runs",
                                    selection=selection,
                                    subject_name=subject_name,
                                ),
                            )
                            _append_unique_reason(totals_reasons, _pitcher_bvp_reason(pitcher_profile, opponent_lineup))
                            _append_unique_reason(
                                totals_reasons,
                                _pitcher_recent_form_reason(
                                    pitcher_profile,
                                    int(season_value),
                                    "earned_runs",
                                    selection=selection,
                                    subject_name=subject_name,
                                ),
                            )
                            _append_unique_reason(
                                totals_reasons,
                                _pitcher_statcast_quality_reason(
                                    pitcher_profile,
                                    prop="earned_runs",
                                    selection=selection,
                                ),
                            )
                            _append_unique_reason(
                                totals_reasons,
                                _pitch_mix_reason(
                                    pitcher_profile,
                                    prop="earned_runs",
                                    selection=selection,
                                ),
                            )
                            _append_unique_reason(
                                totals_reasons,
                                _opponent_lineup_reason(
                                    pitcher_profile,
                                    opponent_lineup,
                                    prop="earned_runs",
                                    selection=selection,
                                ),
                            )
                    out["totals"].append(
                        _annotate_recommendation(
                            {
                                **base,
                                "market": "totals",
                                "selection": selection,
                                "edge": float(side_pick["edge"]),
                                "market_line": float(total_line),
                                "model_mean_total": float(mean_total),
                                "model_prob_over": float(p_over_total),
                                "market_prob_over": side_pick.get("market_prob_over"),
                                "market_prob_under": side_pick.get("market_prob_under"),
                                "market_prob_mode": side_pick.get("market_prob_mode"),
                                "market_no_vig_prob_over": side_pick.get("market_no_vig_prob_over"),
                                "selected_side_market_prob": side_pick.get("selected_side_market_prob"),
                                "selected_side_model_prob": _selected_side_prob_from_over_prob(p_over_total, selection),
                                "mean_support": _mean_support_for_selection(mean_total, total_line, selection),
                                "odds": side_pick.get("odds"),
                                "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                                "market_no_vig_prob": no_vig_over_prob(
                                    totals_market.get("over_odds"), totals_market.get("under_odds")
                                ),
                                "sim_sample_size": _sim_sample_size_from_sim_obj(sim_obj),
                                "baseball_reasons": _trim_reason_list(totals_reasons),
                            }
                        )
                    )

        h2h_market = ((market_game.get("markets") or {}).get("h2h") or {})
        home_prob = float(full.get("home_win_prob") or 0.0)
        away_prob = float(full.get("away_win_prob") or 0.0)
        denom = float(home_prob + away_prob)
        if denom > 0.0:
            home_prob /= denom
            side_pick = _select_moneyline_side(
                home_prob,
                h2h_market.get("home_odds"),
                h2h_market.get("away_odds"),
                float(policy["ml_edge_min"]),
                policy.get("ml_side"),
            )
            if side_pick is not None:
                ml_reasons: List[str] = []
                if isinstance(roster_snapshot, dict):
                    selected_side = str(side_pick.get("selection") or "home")
                    opponent_side = "away" if selected_side == "home" else "home"
                    side_doc = roster_snapshot.get(selected_side) if isinstance(roster_snapshot.get(selected_side), dict) else {}
                    opp_doc = roster_snapshot.get(opponent_side) if isinstance(roster_snapshot.get(opponent_side), dict) else {}
                    pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else {}
                    opponent_lineup = opp_doc.get("lineup") if isinstance(opp_doc.get("lineup"), list) else []
                    opponent_team = sim_obj.get(opponent_side) if isinstance(sim_obj.get(opponent_side), dict) else {}
                    opponent_id = _safe_int(opponent_team.get("id"))
                    opponent_label = str(opponent_team.get("abbreviation") or opponent_team.get("name") or "opponent").strip()
                    subject_name = str(pitcher_profile.get("name") or "").strip() or None
                    _append_unique_reason(
                        ml_reasons,
                        _pitcher_opponent_team_reason(
                            pitcher_profile,
                            opponent_id,
                            opponent_label,
                            int(season_value),
                            "earned_runs",
                            selection="under",
                            subject_name=subject_name,
                        ),
                    )
                    _append_unique_reason(ml_reasons, _pitcher_bvp_reason(pitcher_profile, opponent_lineup))
                    _append_unique_reason(
                        ml_reasons,
                        _pitcher_recent_form_reason(
                            pitcher_profile,
                            int(season_value),
                            "earned_runs",
                            selection="under",
                            subject_name=subject_name,
                        ),
                    )
                    _append_unique_reason(
                        ml_reasons,
                        _pitcher_statcast_quality_reason(
                            pitcher_profile,
                            prop="earned_runs",
                            selection="under",
                        ),
                    )
                    _append_unique_reason(
                        ml_reasons,
                        _pitch_mix_reason(
                            pitcher_profile,
                            prop="earned_runs",
                            selection="under",
                        ),
                    )
                    _append_unique_reason(
                        ml_reasons,
                        _opponent_lineup_reason(
                            pitcher_profile,
                            opponent_lineup,
                            prop="earned_runs",
                            selection="under",
                        ),
                    )
                out["ml"].append(
                    _annotate_recommendation(
                        {
                            **base,
                            "market": "ml",
                            "selection": str(side_pick.get("selection") or "home"),
                            "edge": float(side_pick["edge"]),
                            "model_prob": float(home_prob),
                            "selected_side_model_prob": side_pick.get("selected_side_model_prob"),
                            "selected_side_market_prob": side_pick.get("selected_side_market_prob"),
                            "market_no_vig_prob": side_pick.get("market_no_vig_prob"),
                            "odds": side_pick.get("odds"),
                            "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                            "sim_sample_size": _sim_sample_size_from_sim_obj(sim_obj),
                            "baseball_reasons": _trim_reason_list(ml_reasons),
                        }
                    )
                )

    return out


def _extract_hitter_predictions(sim_obj: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    pred: Dict[str, Dict[str, Any]] = {}

    def _rec_for(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        name = str(row.get("name") or "").strip()
        if not name:
            return None
        key = normalize_pitcher_name(name)
        if not key:
            return None
        rec = pred.setdefault(
            key,
            {
                "name": name,
                "team": str(row.get("team") or ""),
            },
        )
        if not rec.get("team") and row.get("team"):
            rec["team"] = str(row.get("team") or "")
        lineup_flag = row.get("is_lineup_batter")
        if isinstance(lineup_flag, bool):
            rec["is_lineup_batter"] = bool(lineup_flag)
        lineup_order = row.get("lineup_order")
        if isinstance(lineup_order, int) and rec.get("lineup_order") is None:
            rec["lineup_order"] = int(lineup_order)
        for key in ("pa_mean", "ab_mean"):
            value = row.get(key)
            if isinstance(value, (int, float)):
                prev = rec.get(key)
                if not isinstance(prev, (int, float)) or float(value) > float(prev):
                    rec[key] = float(value)
        return rec

    props_topn = ((sim_obj.get("sim") or {}).get("hitter_props_likelihood_topn") or {})
    for prop_key, (cal_key, raw_key) in HITTER_PREDICTION_FIELDS.items():
        for row in (props_topn.get(prop_key) or []):
            if not isinstance(row, dict):
                continue
            rec = _rec_for(row)
            if rec is None:
                continue
            value = row.get(cal_key, row.get(raw_key))
            if isinstance(value, (int, float)):
                rec[prop_key] = float(value)

    hr_topn = (((sim_obj.get("sim") or {}).get("hitter_hr_likelihood_topn") or {}).get("overall") or [])
    for row in hr_topn:
        if not isinstance(row, dict):
            continue
        rec = _rec_for(row)
        if rec is None:
            continue
        value = row.get("p_hr_1plus_cal", row.get("p_hr_1plus"))
        if isinstance(value, (int, float)):
            rec["hr_1plus"] = float(value)

    return pred


def _is_hitter_prediction_eligible(rec: Dict[str, Any]) -> bool:
    lineup_flag = rec.get("is_lineup_batter")
    if isinstance(lineup_flag, bool) and not lineup_flag:
        return False
    pa_mean = rec.get("pa_mean")
    if isinstance(pa_mean, (int, float)):
        if float(pa_mean) <= 0.0:
            return False
        if isinstance(lineup_flag, bool):
            return True
    ab_mean = rec.get("ab_mean")
    if isinstance(ab_mean, (int, float)):
        if float(ab_mean) <= 0.0:
            return False
        if isinstance(lineup_flag, bool):
            return True
    if isinstance(rec.get("lineup_order"), int):
        return True
    if isinstance(pa_mean, (int, float)) or isinstance(ab_mean, (int, float)):
        return False
    return True


def _get_hitter_prob(market_key: str, line: float, rec: Dict[str, Any]) -> Optional[float]:
    spec = HITTER_MARKET_SPECS.get(str(market_key))
    if not isinstance(spec, dict):
        return None
    threshold = _half_line_to_threshold(line)
    if threshold is None:
        return None
    prop_base = str(spec.get("prob_base") or "").strip()
    if not prop_base:
        return None
    if prop_base == "hr" and int(threshold) != 1:
        return None
    return rec.get(_hitter_prob_key(prop_base, int(threshold)))


def _line_matches(value: Any, target: float, tol: float = 1e-9) -> bool:
    try:
        return abs(float(value) - float(target)) <= float(tol)
    except Exception:
        return False


def _select_hitter_props_market(market_key: str, props_market: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(props_market, dict):
        return {}

    spec = HITTER_MARKET_SPECS.get(str(market_key)) or {}
    preferred_lines = tuple(spec.get("primary_lines") or ())
    if not preferred_lines:
        return props_market

    lanes_raw = props_market.get("lanes") or []
    lanes: List[Dict[str, Any]] = []
    for lane in lanes_raw:
        if not isinstance(lane, dict):
            continue
        line = lane.get("line")
        if line is None:
            continue
        try:
            line_value = float(line)
        except Exception:
            continue
        lanes.append(
            {
                "line": line_value,
                "over_odds": lane.get("over_odds"),
                "under_odds": lane.get("under_odds"),
                "_src": lane.get("_src"),
            }
        )

    if not lanes:
        return props_market

    for preferred_line in preferred_lines:
        for require_two_way in (True, False):
            for lane in lanes:
                if not _line_matches(lane.get("line"), preferred_line):
                    continue
                if require_two_way and (lane.get("over_odds") is None or lane.get("under_odds") is None):
                    continue
                return {
                    "line": lane.get("line"),
                    "over_odds": lane.get("over_odds"),
                    "under_odds": lane.get("under_odds"),
                    "_src": lane.get("_src") or props_market.get("_src"),
                    "lanes": lanes,
                    "alternates": [alt for alt in lanes if not _line_matches(alt.get("line"), lane.get("line"))],
                }

    return props_market


def _select_market_side(
    model_prob_over: float,
    over_odds: Any,
    under_odds: Any,
    edge_min: float,
) -> Optional[Dict[str, Any]]:
    side_probs = market_side_probabilities(over_odds, under_odds)
    if not side_probs:
        return None

    candidates: List[Dict[str, Any]] = []
    market_prob_over = side_probs.get("over")
    if isinstance(market_prob_over, (int, float)) and over_odds is not None:
        edge_over = float(model_prob_over) - float(market_prob_over)
        if edge_over >= float(edge_min):
            candidates.append(
                {
                    "selection": "over",
                    "edge": float(edge_over),
                    "odds": over_odds,
                    "selected_side_market_prob": float(market_prob_over),
                }
            )

    market_prob_under = side_probs.get("under")
    if isinstance(market_prob_under, (int, float)) and under_odds is not None:
        edge_under = float(1.0 - float(model_prob_over)) - float(market_prob_under)
        if edge_under >= float(edge_min):
            candidates.append(
                {
                    "selection": "under",
                    "edge": float(edge_under),
                    "odds": under_odds,
                    "selected_side_market_prob": float(market_prob_under),
                }
            )

    if not candidates:
        return None

    best = max(candidates, key=lambda row: (float(row["edge"]), 1 if row["selection"] == "over" else 0))
    best["market_prob_mode"] = str(side_probs.get("mode") or "unknown")
    best["market_prob_over"] = (
        float(side_probs["over"]) if isinstance(side_probs.get("over"), (int, float)) else None
    )
    best["market_prob_under"] = (
        float(side_probs["under"]) if isinstance(side_probs.get("under"), (int, float)) else None
    )
    best["market_no_vig_prob_over"] = (
        float(side_probs["over"])
        if str(side_probs.get("mode") or "") == "no_vig_two_way" and isinstance(side_probs.get("over"), (int, float))
        else None
    )
    return best


def _normalize_american_odds(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(round(float(value)))
    except Exception:
        return None


def _favorite_price_exceeds_limit(odds: Any, limit: Any) -> bool:
    odds_value = _normalize_american_odds(odds)
    limit_value = _normalize_american_odds(limit)
    if odds_value is None or limit_value is None:
        return False
    if odds_value >= 0 or limit_value >= 0:
        return False
    return odds_value < limit_value


def _hitter_price_allowed(
    policy: Optional[Dict[str, Any]],
    *,
    market_name: str,
    selection: str,
    market_line: float,
    odds: Any,
) -> bool:
    if _favorite_price_exceeds_limit(odds, (policy or {}).get("hitter_max_favorite_odds")):
        return False
    if (
        str(market_name) == "hitter_home_runs"
        and str(selection) == "under"
        and _line_matches(market_line, 0.5)
        and _favorite_price_exceeds_limit(odds, (policy or {}).get("hitter_hr_under_0_5_max_favorite_odds"))
    ):
        return False
    return True


def _pitcher_price_allowed(policy: Optional[Dict[str, Any]], *, odds: Any) -> bool:
    return not _favorite_price_exceeds_limit(odds, (policy or {}).get("pitcher_max_favorite_odds"))


def _normalized_pitcher_market(value: Any) -> str:
    raw = str(value or "").strip().lower()
    normalized = PITCHER_MARKET_ALIASES.get(raw, raw)
    if normalized in PITCHER_MARKET_SPECS:
        return normalized
    if normalized in {"", "best", "all", "mixed", "any", "best_available"}:
        return "best"
    return "best"


def _iter_pitcher_market_names(policy: Optional[Dict[str, Any]]) -> List[str]:
    configured = _normalized_pitcher_market((policy or {}).get("pitcher_market"))
    if configured == "best":
        return list(PITCHER_MARKET_SPECS.keys())
    return [configured]


def _collect_hitter_recommendations(
    sim_dir: Path,
    hitter_lines_path: Path,
    policy: Dict[str, Any],
    snapshots_dir: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    if not hitter_lines_path.exists():
        return []

    hitter_odds_raw = (_read_json(hitter_lines_path).get("hitter_props") or {})
    hitter_odds = {normalize_pitcher_name(str(name)): markets for name, markets in hitter_odds_raw.items()}
    rows: List[Dict[str, Any]] = []
    roster_cache: Dict[Tuple[int, int], Optional[Dict[str, Any]]] = {}

    def _roster_for(sim_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if snapshots_dir is None or not snapshots_dir.exists():
            return None
        try:
            game_pk = int(sim_obj.get("game_pk") or 0)
        except Exception:
            return None
        try:
            game_number = int(((sim_obj.get("schedule") or {}).get("game_number") or 1))
        except Exception:
            game_number = 1
        cache_key = (game_pk, int(game_number or 1))
        if cache_key in roster_cache:
            return roster_cache[cache_key]
        doc = None
        pattern = f"roster_*_pk{game_pk}_g{int(game_number or 1)}.json"
        matches = sorted(snapshots_dir.glob(pattern))
        if not matches:
            matches = sorted(snapshots_dir.glob(f"roster_*_pk{game_pk}_g*.json"))
        if matches:
            try:
                raw = _read_json(matches[0])
                doc = raw if isinstance(raw, dict) else None
            except Exception:
                doc = None
        roster_cache[cache_key] = doc
        return doc

    for sim_obj in _iter_sim_records(sim_dir):
        pred = _extract_hitter_predictions(sim_obj)
        if not pred:
            continue
        base = _base_game_row(sim_obj)
        roster_snapshot = _roster_for(sim_obj)
        season_value = _season_from_date_str(sim_obj.get("date")) or _safe_int(sim_obj.get("season")) or datetime.now().year

        for player_key, rec in pred.items():
            if not _is_hitter_prediction_eligible(rec):
                continue
            markets = hitter_odds.get(player_key)
            if not isinstance(markets, dict):
                continue
            matchup_ctx = _lookup_hitter_matchup_context(sim_obj, rec, roster_snapshot)
            baseball_reasons: List[str] = []
            batter_profile = matchup_ctx.get("batter_profile") if isinstance(matchup_ctx.get("batter_profile"), dict) else None
            pitcher_profile = matchup_ctx.get("pitcher_profile") if isinstance(matchup_ctx.get("pitcher_profile"), dict) else None
            opponent_label = str(matchup_ctx.get("opponent") or "").strip()
            opponent_side = "home" if str(rec.get("team") or "").strip().upper() == str((sim_obj.get("away") or {}).get("abbreviation") or "").strip().upper() else "away"
            opponent_team = sim_obj.get(opponent_side) if isinstance(sim_obj.get(opponent_side), dict) else {}
            opponent_team_id = _safe_int(matchup_ctx.get("opponent_team_id")) or _safe_int(opponent_team.get("id"))
            for market_key, market_spec in HITTER_MARKET_SPECS.items():
                props_market = _select_hitter_props_market(market_key, markets.get(market_key) or {})
                line = props_market.get("line")
                if line is None:
                    continue
                line_value = float(line)
                p_over = _get_hitter_prob(market_key, line_value, rec)
                if p_over is None:
                    continue
                side_pick = _select_market_side(
                    float(p_over),
                    props_market.get("over_odds"),
                    props_market.get("under_odds"),
                    _hitter_edge_min_for_market(policy, str(market_spec["market"])),
                )
                if side_pick is None:
                    continue
                selected_model_prob = _selected_side_prob_from_over_prob(p_over, side_pick["selection"])
                if selected_model_prob < _hitter_model_prob_min_for_market(policy, str(market_spec["market"])):
                    continue
                if not _hitter_price_allowed(
                    policy,
                    market_name=str(market_spec["market"]),
                    selection=str(side_pick.get("selection") or ""),
                    market_line=float(line_value),
                    odds=side_pick.get("odds"),
                ):
                    continue
                mean_value = rec.get(str(market_spec.get("mean_key") or ""))
                if not _passes_mean_alignment(mean_value, line_value, side_pick["selection"], 0.0):
                    continue
                reason_items: List[str] = list(baseball_reasons)
                if isinstance(batter_profile, dict) and isinstance(pitcher_profile, dict):
                    _append_unique_reason(
                        reason_items,
                        _hitter_bvp_reason(
                            batter_profile,
                            pitcher_profile,
                            season=int(season_value),
                            prop=str(market_key),
                            selection=str(side_pick.get("selection") or ""),
                            line_value=float(line_value),
                        ),
                    )
                if isinstance(batter_profile, dict):
                    _append_unique_reason(
                        reason_items,
                        _hitter_opponent_team_reason(
                            batter_profile,
                            opponent_team_id,
                            opponent_label,
                            int(season_value),
                            str(market_key),
                            selection=str(side_pick.get("selection") or ""),
                            line_value=float(line_value),
                        ),
                    )
                    _append_unique_reason(
                        reason_items,
                        _hitter_recent_form_reason(
                            batter_profile,
                            int(season_value),
                            str(market_key),
                            selection=str(side_pick.get("selection") or ""),
                            line_value=float(line_value),
                        ),
                    )
                if isinstance(batter_profile, dict) and isinstance(pitcher_profile, dict):
                    _append_unique_reason(
                        reason_items,
                        _hitter_pitch_mix_reason(
                            batter_profile,
                            pitcher_profile,
                            prop=str(market_key),
                            selection=str(side_pick.get("selection") or ""),
                        ),
                    )
                    _append_unique_reason(
                        reason_items,
                        _hitter_platoon_reason(
                            batter_profile,
                            pitcher_profile,
                            prop=str(market_key),
                            selection=str(side_pick.get("selection") or ""),
                        ),
                    )
                    _append_unique_reason(
                        reason_items,
                        _hitter_statcast_quality_reason(
                            batter_profile,
                            prop=str(market_key),
                            selection=str(side_pick.get("selection") or ""),
                        ),
                    )
                rows.append(
                    _annotate_recommendation(
                        {
                            **base,
                            "market": str(market_spec["market"]),
                            "market_label": str(market_spec["label"]),
                            "market_group": "hitter_props",
                            "player_name": rec.get("name"),
                            "team": rec.get("team"),
                            "prop": market_key,
                            "prop_market_key": market_key,
                            "selection": side_pick["selection"],
                            "edge": float(side_pick["edge"]),
                            "market_line": float(line_value),
                            "model_prob_over": float(p_over),
                            "market_prob_over": side_pick["market_prob_over"],
                            "market_prob_under": side_pick["market_prob_under"],
                            "market_prob_mode": side_pick["market_prob_mode"],
                            "market_no_vig_prob_over": side_pick["market_no_vig_prob_over"],
                            "selected_side_market_prob": float(side_pick["selected_side_market_prob"]),
                            "selected_side_model_prob": float(selected_model_prob),
                            "mean_support": _mean_support_for_selection(
                                mean_value,
                                line_value,
                                side_pick["selection"],
                            ),
                            str(market_spec.get("mean_key") or ""): mean_value,
                            "pa_mean": rec.get("pa_mean"),
                            "ab_mean": rec.get("ab_mean"),
                            "lineup_order": rec.get("lineup_order"),
                            "market_alternates": list(props_market.get("alternates") or []),
                            "odds": side_pick["odds"],
                            "stake_u": float(DEFAULT_HITTER_STAKE_U),
                            "sim_sample_size": _sim_sample_size_from_sim_obj(sim_obj),
                            "baseball_reasons": _trim_reason_list(reason_items),
                        }
                    )
                )

    return rows


def _collect_pitcher_recommendations(
    sim_dir: Path,
    pitcher_lines_path: Path,
    policy: Dict[str, Any],
    so_prob_calibration: Optional[Dict[str, Any]],
    outs_prob_calibration: Optional[Dict[str, Any]],
    snapshots_dir: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    if not pitcher_lines_path.exists():
        return []

    pitcher_odds_raw = (_read_json(pitcher_lines_path).get("pitcher_props") or {})
    pitcher_odds = {normalize_pitcher_name(str(name)): markets for name, markets in pitcher_odds_raw.items()}
    rows: List[Dict[str, Any]] = []

    roster_cache: Dict[Tuple[int, int], Optional[Dict[str, Any]]] = {}

    def _roster_for(sim_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if snapshots_dir is None or not snapshots_dir.exists():
            return None
        try:
            game_pk = int(sim_obj.get("game_pk") or 0)
        except Exception:
            return None
        game_number = None
        try:
            game_number = int(((sim_obj.get("schedule") or {}).get("game_number") or 1))
        except Exception:
            game_number = 1
        cache_key = (game_pk, int(game_number or 1))
        if cache_key in roster_cache:
            return roster_cache[cache_key]

        doc = None
        pattern = f"roster_*_pk{game_pk}_g{int(game_number or 1)}.json"
        matches = sorted(snapshots_dir.glob(pattern))
        if not matches:
            matches = sorted(snapshots_dir.glob(f"roster_*_pk{game_pk}_g*.json"))
        if matches:
            try:
                raw = _read_json(matches[0])
                doc = raw if isinstance(raw, dict) else None
            except Exception:
                doc = None
        roster_cache[cache_key] = doc
        return doc

    for sim_obj in _iter_sim_records(sim_dir):
        base = _base_game_row(sim_obj)
        roster_snapshot = _roster_for(sim_obj)
        season_value = _season_from_date_str(sim_obj.get("date")) or _safe_int(sim_obj.get("season")) or datetime.now().year
        starter_names = sim_obj.get("starter_names") or {}
        starters = sim_obj.get("starters") or {}
        sim_pitcher_props = ((sim_obj.get("sim") or {}).get("pitcher_props") or {})

        for side in ("away", "home"):
            starter_name = str(starter_names.get(side) or "").strip()
            starter_id = starters.get(side)
            if not starter_name or starter_id is None:
                continue
            pred = sim_pitcher_props.get(str(int(starter_id)))
            if not isinstance(pred, dict):
                continue
            markets = pitcher_odds.get(normalize_pitcher_name(starter_name))
            if not isinstance(markets, dict):
                continue
            for market_name in _iter_pitcher_market_names(policy):
                market_spec = PITCHER_MARKET_SPECS.get(market_name) or {}
                market_key = str(market_spec.get("market_key") or "")
                props_market = markets.get(market_key) or {}
                line = props_market.get("line")
                if line is None:
                    continue
                line_value = float(line)
                dist_key = str(market_spec.get("dist_key") or "")
                p_raw = _prob_over_line_from_dist(pred.get(dist_key) or {}, line_value)
                if p_raw is None:
                    continue
                calibration = so_prob_calibration if market_name == "strikeouts" else outs_prob_calibration
                p_over = apply_prob_calibration(float(p_raw), calibration)
                side_pick = _select_market_side(
                    float(p_over),
                    props_market.get("over_odds"),
                    props_market.get("under_odds"),
                    float(policy["pitcher_edge_min"]),
                )
                if side_pick is None or not _selection_allowed(side_pick.get("selection"), policy.get("pitcher_side")):
                    continue
                if not _pitcher_price_allowed(policy, odds=side_pick.get("odds")):
                    continue
                mean_key = str(market_spec.get("mean_key") or "")
                if not _passes_mean_alignment(pred.get(mean_key), line_value, side_pick.get("selection"), 0.0):
                    continue

                baseball_reasons: List[str] = []
                if isinstance(roster_snapshot, dict):
                    side_doc = (roster_snapshot.get(side) or {}) if isinstance(roster_snapshot.get(side), dict) else {}
                    opp_side = "home" if side == "away" else "away"
                    opp_doc = (roster_snapshot.get(opp_side) or {}) if isinstance(roster_snapshot.get(opp_side), dict) else {}
                    pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else {}
                    if pitcher_profile and int(pitcher_profile.get("id") or 0) == int(starter_id):
                        opponent_lineup = opp_doc.get("lineup") if isinstance(opp_doc.get("lineup"), list) else []
                        bvp_reason = _pitcher_bvp_reason(pitcher_profile, opponent_lineup)
                        opponent_team = sim_obj.get(opp_side) if isinstance(sim_obj.get(opp_side), dict) else {}
                        opponent_id = _safe_int(opponent_team.get("id"))
                        opponent_label = str(opponent_team.get("abbreviation") or opponent_team.get("name") or "opponent").strip()
                        _append_unique_reason(baseball_reasons, bvp_reason)
                        _append_unique_reason(
                            baseball_reasons,
                            _pitcher_opponent_team_reason(
                                pitcher_profile,
                                opponent_id,
                                opponent_label,
                                int(season_value),
                                str(market_name),
                                selection=str(side_pick.get("selection") or ""),
                                line_value=float(line_value),
                            ),
                        )
                        _append_unique_reason(
                            baseball_reasons,
                            _pitcher_recent_form_reason(
                                pitcher_profile,
                                int(season_value),
                                str(market_name),
                                selection=str(side_pick.get("selection") or ""),
                                line_value=float(line_value),
                            ),
                        )
                        _append_unique_reason(
                            baseball_reasons,
                            _pitcher_statcast_quality_reason(
                                pitcher_profile,
                                prop=str(market_name),
                                selection=str(side_pick.get("selection") or ""),
                            ),
                        )
                        _append_unique_reason(
                            baseball_reasons,
                            _pitch_mix_reason(
                                pitcher_profile,
                                prop=str(market_name),
                                selection=str(side_pick.get("selection") or ""),
                            ),
                        )
                        _append_unique_reason(
                            baseball_reasons,
                            _opponent_lineup_reason(
                                pitcher_profile,
                                opponent_lineup,
                                prop=str(market_name),
                                selection=str(side_pick.get("selection") or ""),
                            ),
                        )
                        _append_unique_reason(
                            baseball_reasons,
                            _pitcher_workload_reason(
                                pitcher_profile,
                                prop=str(market_name),
                                selection=str(side_pick.get("selection") or ""),
                            ),
                        )

                rows.append(
                    _annotate_recommendation(
                        {
                            **base,
                            "market": "pitcher_props",
                            "pitcher_id": int(starter_id),
                            "pitcher_name": starter_name,
                            "team": (sim_obj.get(side) or {}).get("abbreviation"),
                            "team_side": side,
                            "prop": str(market_name),
                            "selection": str(side_pick.get("selection") or ""),
                            "edge": float(side_pick["edge"]),
                            "market_line": float(line_value),
                            "model_prob_over": float(p_over),
                            "market_prob_over": side_pick.get("market_prob_over"),
                            "market_prob_under": side_pick.get("market_prob_under"),
                            "market_prob_mode": side_pick.get("market_prob_mode"),
                            "market_no_vig_prob_over": side_pick.get("market_no_vig_prob_over"),
                            "selected_side_market_prob": side_pick.get("selected_side_market_prob"),
                            "selected_side_model_prob": _selected_side_prob_from_over_prob(p_over, side_pick.get("selection")),
                            "mean_support": _mean_support_for_selection(pred.get(mean_key), line_value, side_pick.get("selection")),
                            mean_key: pred.get(mean_key),
                            "market_alternates": list(props_market.get("alternates") or []),
                            "odds": side_pick.get("odds"),
                            "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                            "sim_sample_size": _sim_sample_size_from_sim_obj(sim_obj),
                            "baseball_reasons": _trim_reason_list(baseball_reasons),
                        }
                    )
                )

    return rows


def _row_model_prob(row: Dict[str, Any]) -> float:
    return float(row.get("selected_side_model_prob") or row.get("model_prob") or row.get("model_prob_over") or 0.0)


def _row_market_prob(row: Dict[str, Any]) -> float:
    return float(row.get("selected_side_market_prob") or row.get("market_prob") or row.get("market_prob_over") or 0.0)


def _row_rank_key(row: Dict[str, Any]) -> Tuple[float, float, float, float]:
    model_prob = _row_model_prob(row)
    market_prob = _row_market_prob(row)
    edge = float(row.get("edge") or 0.0)
    mean_support = float(row.get("mean_support") or row.get("model_mean_total") or 0.0)
    if str(row.get("market") or "") == "hitter_home_runs":
        return (
            model_prob,
            market_prob,
            edge,
            mean_support,
        )
    return (
        model_prob,
        edge,
        mean_support,
        market_prob,
    )


def _candidate_row_id(row: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        str(row.get("date") or ""),
        row.get("game_pk"),
        str(row.get("market") or ""),
        str(row.get("player_name") or ""),
        str(row.get("pitcher_name") or ""),
        str(row.get("team") or ""),
        str(row.get("team_side") or ""),
        str(row.get("prop") or ""),
        str(row.get("selection") or ""),
        row.get("market_line"),
        row.get("odds"),
    )


def _player_prop_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    raw_name = str(row.get("player_name") or row.get("pitcher_name") or "").strip()
    normalized_name = normalize_pitcher_name(raw_name) if raw_name else ""
    if not normalized_name:
        return ("row",) + _candidate_row_id(row)
    return (
        str(row.get("date") or ""),
        row.get("game_pk"),
        normalized_name,
        str(row.get("team") or ""),
        str(row.get("team_side") or ""),
    )


def _selected_player_keys(rows: List[Dict[str, Any]]) -> set[Tuple[Any, ...]]:
    return {_player_prop_key(row) for row in rows}


def _rank_and_cap(rows: List[Dict[str, Any]], cap: Optional[int]) -> List[Dict[str, Any]]:
    ranked = sorted(rows, key=_row_rank_key, reverse=True)
    if cap is not None and int(cap) >= 0:
        ranked = ranked[: int(cap)]
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(ranked, start=1):
        item = dict(row)
        item["rank"] = int(idx)
        out.append(item)
    return out


def _rank_and_cap_unique_players(
    rows: List[Dict[str, Any]],
    cap: Optional[int],
    *,
    blocked_player_keys: Optional[set[Tuple[Any, ...]]] = None,
) -> List[Dict[str, Any]]:
    cap_limit = (int(cap) if cap is not None and int(cap) >= 0 else None)
    ranked = sorted(rows, key=_row_rank_key, reverse=True)
    selected: List[Dict[str, Any]] = []
    used_player_keys = set(blocked_player_keys or set())

    for row in ranked:
        if cap_limit is not None and len(selected) >= cap_limit:
            break
        player_key = _player_prop_key(row)
        if player_key in used_player_keys:
            continue
        used_player_keys.add(player_key)
        selected.append(row)

    return _rank_and_cap(selected, None)


def _subtract_selected_rows(rows: List[Dict[str, Any]], selected_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected_row_counts = Counter(_candidate_row_id(row) for row in selected_rows)
    remaining: List[Dict[str, Any]] = []
    for row in rows:
        row_id = _candidate_row_id(row)
        if selected_row_counts.get(row_id, 0) > 0:
            selected_row_counts[row_id] -= 1
            continue
        remaining.append(row)
    return _rank_and_cap(remaining, None)


def _select_hitter_recommendations(
    hitter_rows: List[Dict[str, Any]],
    shared_cap: Optional[int],
    hitter_subcaps: Dict[str, Optional[int]],
    *,
    blocked_player_keys: Optional[set[Tuple[Any, ...]]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]], str]:
    selected_by_market: Dict[str, List[Dict[str, Any]]] = {market_name: [] for market_name in HITTER_MARKET_ORDER}
    blocked_keys = set(blocked_player_keys or set())
    if not _has_hitter_subcaps(hitter_subcaps):
        selected_rows = _rank_and_cap_unique_players(
            hitter_rows,
            shared_cap,
            blocked_player_keys=blocked_keys,
        )
        for row in selected_rows:
            market_name = str(row.get("market") or "")
            selected_by_market.setdefault(market_name, []).append(row)
        return selected_rows, selected_by_market, "shared_cap"

    shared_cap_limit = (int(shared_cap) if shared_cap is not None and int(shared_cap) >= 0 else None)
    selected_market_counts: Counter[str] = Counter()
    selected_rows_raw: List[Dict[str, Any]] = []
    used_player_keys = set(blocked_keys)

    for row in sorted(hitter_rows, key=_row_rank_key, reverse=True):
        if shared_cap_limit is not None and len(selected_rows_raw) >= shared_cap_limit:
            break
        market_name = str(row.get("market") or "")
        market_cap = hitter_subcaps.get(market_name)
        if market_cap is not None and int(market_cap) >= 0 and selected_market_counts[market_name] >= int(market_cap):
            continue
        player_key = _player_prop_key(row)
        if player_key in used_player_keys:
            continue
        used_player_keys.add(player_key)
        selected_market_counts[market_name] += 1
        selected_rows_raw.append(row)

    for market_name in HITTER_MARKET_ORDER:
        market_selected = [row for row in selected_rows_raw if str(row.get("market") or "") == market_name]
        selected_by_market[market_name] = _rank_and_cap(market_selected, None)

    selected_rows = _rank_and_cap(selected_rows_raw, None)
    return selected_rows, selected_by_market, "submarket_caps"


def _build_locked_policy_card(
    *,
    date: str,
    season: int,
    out_game: Path,
    out_pitcher: Path,
    out_hitter: Path,
    best_selection_path: Path,
    best_selection: Optional[Dict[str, Any]],
    profile_info: Dict[str, Any],
    so_prob_calibration_path: Optional[Path],
    so_prob_calibration: Optional[Dict[str, Any]],
    outs_prob_calibration_path: Optional[Path],
    outs_prob_calibration: Optional[Dict[str, Any]],
    policy_overrides: Optional[Dict[str, Any]],
    market_caps: Dict[str, Optional[int]],
    hitter_subcaps: Optional[Dict[str, Optional[int]]],
) -> Dict[str, Any]:
    token = str(date).replace("-", "_")
    policy = _policy_with_overrides(policy_overrides)
    caps = _normalized_official_caps(market_caps)
    normalized_hitter_subcaps = _normalized_hitter_subcaps(hitter_subcaps)
    use_hitter_subcaps = _has_hitter_subcaps(normalized_hitter_subcaps)
    cap_profile = _official_cap_profile_name(caps, normalized_hitter_subcaps)

    game_sim_dir = out_game / "sims" / str(date)
    pitcher_sim_dir = out_pitcher / "sims" / str(date)
    hitter_sim_dir = out_hitter / "sims" / str(date)

    game_lines_path = _ROOT / "data" / "market" / "oddsapi" / f"oddsapi_game_lines_{token}.json"
    pitcher_lines_path = _ROOT / "data" / "market" / "oddsapi" / f"oddsapi_pitcher_props_{token}.json"
    hitter_lines_path = _ROOT / "data" / "market" / "oddsapi" / f"oddsapi_hitter_props_{token}.json"
    skipped_roles = {
        role_name
        for role_name, info in (profile_info or {}).items()
        if isinstance(info, dict) and bool(info.get("skipped"))
    }

    warnings: List[str] = []
    for label, path in (
        ("game sims", game_sim_dir),
        ("pitcher sims", pitcher_sim_dir),
        ("hitter sims", hitter_sim_dir),
        ("game lines", game_lines_path),
        ("pitcher lines", pitcher_lines_path),
        ("hitter lines", hitter_lines_path),
    ):
        if label == "pitcher sims" and "pitcher_props_recos" in skipped_roles:
            continue
        if label == "hitter sims" and "hitter_props_recos" in skipped_roles:
            continue
        if not path.exists():
            warnings.append(f"Missing {label}: {_rel(path)}")

    for label, path, root_key in (
        ("game lines", game_lines_path, "games"),
        ("pitcher lines", pitcher_lines_path, "pitcher_props"),
        ("hitter lines", hitter_lines_path, "hitter_props"),
    ):
        if not path.exists():
            continue
        try:
            doc = _read_json(path)
        except Exception as exc:
            warnings.append(f"Unreadable {label}: {_rel(path)} ({type(exc).__name__}: {exc})")
            continue
        payload = doc.get(root_key) if isinstance(doc, dict) else None
        entries_n = len(payload) if isinstance(payload, (list, dict)) else 0
        if entries_n <= 0:
            warnings.append(f"No {label} entries found in {_rel(path)}")

    game_rows = _collect_game_recommendations(game_sim_dir, game_lines_path, policy)
    pitcher_rows = _collect_pitcher_recommendations(
        pitcher_sim_dir,
        pitcher_lines_path,
        policy,
        so_prob_calibration,
        outs_prob_calibration,
        _ROOT / "data" / "daily" / "snapshots" / str(date),
    )
    hitter_rows = _collect_hitter_recommendations(
        hitter_sim_dir,
        hitter_lines_path,
        policy,
        _ROOT / "data" / "daily" / "snapshots" / str(date),
    )

    raw_rows: Dict[str, List[Dict[str, Any]]] = {
        "totals": list(game_rows.get("totals") or []),
        "ml": list(game_rows.get("ml") or []),
    }

    markets: Dict[str, Any] = {}
    selected_support_policy_markets: Dict[str, Any] = {}
    for market_name, rows in raw_rows.items():
        baseline_selected = _rank_and_cap(rows, caps.get(market_name))
        supported_rows, _ = _filter_candidates_by_support(rows, market_name=str(market_name))
        selected = _rank_and_cap(supported_rows, caps.get(market_name))
        selected_support_policy_markets[str(market_name)] = _audit_selected_support_policy(
            market_name=str(market_name),
            baseline_selected=baseline_selected,
            final_selected=selected,
        )
        markets[market_name] = {
            "raw_candidates_n": int(len(rows)),
            "selected_n": int(len(selected)),
            "cap": (int(caps[market_name]) if market_name in caps else None),
            "stake_u": float(DEFAULT_STANDARD_STAKE_U),
            "recommendations": selected,
        }

    baseline_selected_pitcher_rows = _rank_and_cap_unique_players(pitcher_rows, caps.get("pitcher_props"))
    supported_pitcher_rows, _ = _filter_candidates_by_support(pitcher_rows, market_name="pitcher_props")
    selected_pitcher_rows = _rank_and_cap_unique_players(supported_pitcher_rows, caps.get("pitcher_props"))
    selected_support_policy_markets["pitcher_props"] = _audit_selected_support_policy(
        market_name="pitcher_props",
        baseline_selected=baseline_selected_pitcher_rows,
        final_selected=selected_pitcher_rows,
    )
    extra_pitcher_rows, pitcher_playable_audit = _filter_playable_candidates_by_support(
        _subtract_selected_rows(supported_pitcher_rows, selected_pitcher_rows),
        market_name="pitcher_props",
    )
    markets["pitcher_props"] = {
        "raw_candidates_n": int(len(pitcher_rows)),
        "selected_n": int(len(selected_pitcher_rows)),
        "other_playable_candidates_n": int(len(extra_pitcher_rows)),
        "cap": (int(caps["pitcher_props"]) if caps.get("pitcher_props") is not None else None),
        "stake_u": float(DEFAULT_STANDARD_STAKE_U),
        "one_prop_per_player": True,
        "recommendations": selected_pitcher_rows,
        "other_playable_candidates": extra_pitcher_rows,
        "playable_support_removed_n": int(pitcher_playable_audit.get("removed_sparse_support_n") or 0),
    }

    hitter_raw_by_market: Dict[str, List[Dict[str, Any]]] = {market_name: [] for market_name in HITTER_MARKET_ORDER}
    for row in hitter_rows:
        market_name = str(row.get("market") or "")
        hitter_raw_by_market.setdefault(market_name, []).append(row)

    baseline_selected_hitter_rows, baseline_selected_hitter_by_market, _ = _select_hitter_recommendations(
        hitter_rows,
        caps.get("hitter_props"),
        normalized_hitter_subcaps,
        blocked_player_keys=_selected_player_keys(selected_pitcher_rows),
    )
    supported_hitter_rows, _ = _filter_candidates_by_support(hitter_rows, market_name="hitter_props")
    selected_hitter_rows, selected_hitter_by_market, hitter_selection_mode = _select_hitter_recommendations(
        supported_hitter_rows,
        caps.get("hitter_props"),
        normalized_hitter_subcaps,
        blocked_player_keys=_selected_player_keys(selected_pitcher_rows),
    )
    selected_support_policy_markets["hitter_props"] = _audit_selected_support_policy(
        market_name="hitter_props",
        baseline_selected=baseline_selected_hitter_rows,
        final_selected=selected_hitter_rows,
    )

    for market_name in HITTER_MARKET_ORDER:
        rows = list(hitter_raw_by_market.get(market_name) or [])
        selected = list(selected_hitter_by_market.get(market_name) or [])
        baseline_selected = list(baseline_selected_hitter_by_market.get(market_name) or [])
        selected_support_policy_markets[str(market_name)] = _audit_selected_support_policy(
            market_name=str(market_name),
            baseline_selected=baseline_selected,
            final_selected=selected,
        )
        extra, playable_audit = _filter_playable_candidates_by_support(
            _subtract_selected_rows(list(row for row in supported_hitter_rows if str(row.get("market") or "") == market_name), selected),
            market_name=str(market_name),
        )
        market_cap = normalized_hitter_subcaps.get(market_name) if hitter_selection_mode == "submarket_caps" else None
        markets[market_name] = {
            "raw_candidates_n": int(len(rows)),
            "selected_n": int(len(selected)),
            "other_playable_candidates_n": int(len(extra)),
            "cap": (int(market_cap) if market_cap is not None else None),
            "cap_mode": ("submarket" if hitter_selection_mode == "submarket_caps" else "shared_group"),
            "shared_cap_bucket": "hitter_props",
            "stake_u": float(DEFAULT_HITTER_STAKE_U),
            "one_prop_per_player": True,
            "recommendations": selected,
            "other_playable_candidates": extra,
            "playable_support_removed_n": int(playable_audit.get("removed_sparse_support_n") or 0),
        }

    playable_support_policy = {
        "support_min_reasons": int(_EXPLANATION_SUPPORT_MIN_REASONS),
        "removed_sparse_support_n": int(
            (pitcher_playable_audit.get("removed_sparse_support_n") or 0)
            + sum(int((markets.get(market_name, {}) or {}).get("playable_support_removed_n") or 0) for market_name in HITTER_MARKET_ORDER)
        ),
        "markets": {
            "pitcher_props": pitcher_playable_audit,
            **{
                str(market_name): {
                    "market": str(market_name),
                    "evaluated_n": int(
                        len(_subtract_selected_rows(list(hitter_raw_by_market.get(market_name) or []), list(selected_hitter_by_market.get(market_name) or [])))
                    ),
                    "kept_n": int(len((markets.get(market_name, {}) or {}).get("other_playable_candidates") or [])),
                    "removed_sparse_support_n": int((markets.get(market_name, {}) or {}).get("playable_support_removed_n") or 0),
                    "removed_examples": [],
                }
                for market_name in HITTER_MARKET_ORDER
            },
        },
    }
    for market_name in HITTER_MARKET_ORDER:
        _, playable_audit = _filter_playable_candidates_by_support(
            _subtract_selected_rows(list(hitter_raw_by_market.get(market_name) or []), list(selected_hitter_by_market.get(market_name) or [])),
            market_name=str(market_name),
        )
        playable_support_policy["markets"][str(market_name)] = playable_audit

    if int(playable_support_policy.get("removed_sparse_support_n") or 0) > 0:
        warnings.append(
            f"Removed {int(playable_support_policy.get('removed_sparse_support_n') or 0)} sparse-support playable candidate(s) from the official card output"
        )

    selected_support_summary_markets = ("totals", "ml", "pitcher_props", "hitter_props")
    selected_support_policy = {
        "support_min_reasons": int(_EXPLANATION_SUPPORT_MIN_REASONS),
        "removed_sparse_support_n": int(
            sum(int((selected_support_policy_markets.get(market_name, {}) or {}).get("removed_sparse_support_n") or 0) for market_name in selected_support_summary_markets)
        ),
        "replacement_added_n": int(
            sum(int((selected_support_policy_markets.get(market_name, {}) or {}).get("replacement_added_n") or 0) for market_name in selected_support_summary_markets)
        ),
        "selection_shortfall_n": int(
            sum(int((selected_support_policy_markets.get(market_name, {}) or {}).get("selection_shortfall_n") or 0) for market_name in selected_support_summary_markets)
        ),
        "markets": selected_support_policy_markets,
    }
    if int(selected_support_policy.get("removed_sparse_support_n") or 0) > 0:
        warnings.append(
            "Removed "
            f"{int(selected_support_policy.get('removed_sparse_support_n') or 0)} sparse-support official recommendation(s) before final publish"
        )
    if int(selected_support_policy.get("selection_shortfall_n") or 0) > 0:
        warnings.append(
            "Official card could not fully replace "
            f"{int(selected_support_policy.get('selection_shortfall_n') or 0)} sparse-support recommendation slot(s) with support-qualified alternatives"
        )

    market_groups = {
        "hitter_props": {
            "raw_candidates_n": int(len(hitter_rows)),
            "selected_n": int(len(selected_hitter_rows)),
            "other_playable_candidates_n": int(sum(len(markets.get(market_name, {}).get("other_playable_candidates") or []) for market_name in HITTER_MARKET_ORDER)),
            "cap": (int(caps["hitter_props"]) if caps.get("hitter_props") is not None else None),
            "selection_mode": hitter_selection_mode,
            "one_prop_per_player": True,
            "stake_u": float(DEFAULT_HITTER_STAKE_U),
            "submarkets": list(HITTER_MARKET_ORDER),
            "submarket_caps": {
                market_name: (int(value) if value is not None else None)
                for market_name, value in normalized_hitter_subcaps.items()
            },
            "selected_counts": {
                market_name: int(len(selected_hitter_by_market.get(market_name) or []))
                for market_name in HITTER_MARKET_ORDER
            },
        }
    }

    hitter_policy: Dict[str, Any] = {
        "side": "best_edge_side",
        "no_vig_edge_min": float(policy["hitter_edge_min"]),
        "max_favorite_odds": _normalize_american_odds(policy.get("hitter_max_favorite_odds")),
        "home_run_under_0_5_max_favorite_odds": _normalize_american_odds(
            policy.get("hitter_hr_under_0_5_max_favorite_odds")
        ),
        "selection_mode": hitter_selection_mode,
        "one_prop_per_player": True,
        "shared_cap_bucket": "hitter_props",
        "aggregate_cap": (int(caps["hitter_props"]) if caps.get("hitter_props") is not None else None),
        "submarkets": list(HITTER_MARKET_ORDER),
    }
    hitter_edge_overrides = _hitter_edge_min_overrides(policy)
    if hitter_edge_overrides:
        hitter_policy["no_vig_edge_min_by_submarket"] = dict(hitter_edge_overrides)
    if use_hitter_subcaps:
        hitter_policy["submarket_caps"] = {
            market_name: (int(value) if value is not None else None)
            for market_name, value in normalized_hitter_subcaps.items()
        }

    cap_note = (
        "Current live defaults run totals at 2/day, keep ml at 1 and pitcher props at 3, and add one runs slot to the HR/Hits/TB hitter mix while ranking sides from the sim first."
        if cap_profile == DEFAULT_OFFICIAL_CAP_PROFILE
        else "This card uses a custom cap overlay."
    )
    hitter_cap_note = (
        (
            "Hitter submarkets are separated in output and capped independently at "
            f"HR {_cap_text(normalized_hitter_subcaps.get('hitter_home_runs'))} / "
            f"Hits {_cap_text(normalized_hitter_subcaps.get('hitter_hits'))} / "
            f"Total Bases {_cap_text(normalized_hitter_subcaps.get('hitter_total_bases'))} / "
            f"Runs {_cap_text(normalized_hitter_subcaps.get('hitter_runs'))} / "
            f"RBIs {_cap_text(normalized_hitter_subcaps.get('hitter_rbis'))}, "
            f"with a {_cap_text(caps.get('hitter_props'))}-pick aggregate hitter ceiling."
        )
        if hitter_selection_mode == "submarket_caps"
        else "Hitter submarkets are separated in output but still share the combined hitter_props cap."
    )

    explanation_diagnostics = _collect_card_explanation_diagnostics(markets)

    return {
        "date": str(date),
        "season": int(season),
        "generated_at": datetime.now().isoformat(),
        "tool": "tools/daily_update_multi_profile.py",
        "selection_source": _rel(best_selection_path),
        "best_selection": best_selection,
        "policy": {
            "totals": {
                "side": str(policy.get("totals_side") or "best_edge_side"),
                "calibrated_no_vig_edge_min": float(policy.get("totals_edge_min") or 0.0),
                "mean_support_min": float(policy["totals_diff_min"]),
            },
            "ml": {"side": str(policy.get("ml_side") or "best_edge_side"), "no_vig_edge_min": float(policy["ml_edge_min"])},
            "hitter_props": hitter_policy,
            "pitcher_props": {
                "market": str(_normalized_pitcher_market(policy.get("pitcher_market"))),
                "eligible_markets": list(_iter_pitcher_market_names(policy)),
                "side": str(policy["pitcher_side"]),
                "one_prop_per_player": True,
                "calibrated_no_vig_edge_min": float(policy["pitcher_edge_min"]),
                "max_favorite_odds": _normalize_american_odds(policy.get("pitcher_max_favorite_odds")),
            },
        },
        "cap_profile": cap_profile,
        "caps": dict(caps),
        "hitter_subcaps": {
            market_name: (int(value) if value is not None else None)
            for market_name, value in normalized_hitter_subcaps.items()
        },
        "staking": {
            "totals": float(DEFAULT_STANDARD_STAKE_U),
            "ml": float(DEFAULT_STANDARD_STAKE_U),
            "pitcher_props": float(DEFAULT_STANDARD_STAKE_U),
            "hitter_props": float(DEFAULT_HITTER_STAKE_U),
        },
        "notes": [
            "Base profiles come from the best-by-bet-type selection artifact.",
            "The official caps are a post-selection risk overlay that combines validation-44 market caps with a refreshed hitter-market subcap backfill.",
            cap_note,
            hitter_cap_note,
            "Official player props are limited to one selected lane per player; additional qualified lanes remain available as playable candidates.",
            "Official sides are now picked from the sim distribution first, with market edge used as a secondary ranking input.",
            "Totals and player props must keep their projected mean on the same side of the betting line before they can be promoted.",
            "Pitcher props rank the best qualified outs/strikeouts lanes into the shared pitcher bucket.",
            "Prop price guardrails drop overly juiced favorites before official and other playable candidates are ranked.",
            "Totals, moneyline, and pitcher props are graded at 1.0u; hitter props are graded at 0.25u.",
        ],
        "profiles": profile_info,
        "inputs": {
            "game_lines": _rel(game_lines_path),
            "pitcher_lines": _rel(pitcher_lines_path),
            "hitter_lines": _rel(hitter_lines_path),
            "so_prob_calibration": (_rel(so_prob_calibration_path) if so_prob_calibration_path is not None else None),
            "outs_prob_calibration": (_rel(outs_prob_calibration_path) if outs_prob_calibration_path is not None else None),
        },
        "warnings": warnings,
        "explanation_diagnostics": explanation_diagnostics,
        "audit_track": {
            "official_card_explanation_support": explanation_diagnostics,
            "selected_support_policy": selected_support_policy,
            "playable_support_policy": playable_support_policy,
        },
        "market_groups": market_groups,
        "markets": markets,
        "combined": {
            "raw_candidates_n": int(sum(v["raw_candidates_n"] for v in markets.values())),
            "selected_n": int(sum(v["selected_n"] for v in markets.values())),
        },
    }


def _run_profile(
    *,
    profile_name: str,
    py_exe: Path,
    daily_update_py: Path,
    date: str,
    season: int,
    passthrough_args: List[str],
    out_dir: Path,
    extra_args: List[str],
    lineups_last_known_path: Optional[Path] = None,
) -> Tuple[int, List[str]]:
    cmd: List[str] = [
        str(py_exe),
        str(daily_update_py),
        "--date",
        str(date),
        "--season",
        str(int(season)),
    ]
    cmd.extend(list(passthrough_args))
    cmd.extend(["--out", str(out_dir)])
    if lineups_last_known_path is not None:
        cmd.extend(["--lineups-last-known", str(lineups_last_known_path)])
    cmd.extend(list(extra_args))

    print(f"[multi-profile] Running profile '{profile_name}' -> {_rel(out_dir)}")
    rc = subprocess.run(cmd, check=False).returncode
    return int(rc), cmd


def _sync_profile_snapshot_dir(source_dir: Path, target_dir: Path) -> None:
    if not source_dir.exists() or not source_dir.is_dir():
        return
    target_dir.mkdir(parents=True, exist_ok=True)
    for source_path in source_dir.rglob("*"):
        rel_path = source_path.relative_to(source_dir)
        target_path = target_dir / rel_path
        if source_path.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
            continue
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Run tools/daily_update.py three times for specialized recommendation profiles: "
            "game ROI, pitcher props, hitter props."
        )
    )
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    ap.add_argument("--season", type=int, default=datetime.now().year)
    ap.add_argument(
        "--python-exe",
        default=str(_ROOT / ".venv_x64" / "Scripts" / "python.exe"),
        help="Python executable for launching tools/daily_update.py",
    )

    # Output roots for each profile.
    ap.add_argument("--out-game", default="data/daily")
    ap.add_argument("--out-pitcher", default="data/daily_pitcher_props")
    ap.add_argument("--out-hitter", default="data/daily_hitter_props")

    # Profile default knobs.
    ap.add_argument(
        "--game-pitch-model-overrides",
        default="data/tuning/pitch_model_overrides/_tmp_hr_bbhbp1p04_starterbbhbp1p04.json",
        help="Pitch-model override for the game-ROI profile (set to 'off' to disable)",
    )
    ap.add_argument(
        "--pitcher-pitch-model-overrides",
        default="off",
        help="Pitch-model override for the pitcher-props profile (default: promoted baseline / off)",
    )
    ap.add_argument(
        "--hitter-pitch-model-overrides",
        default="off",
        help="Pitch-model override for the hitter-props profile (default: promoted baseline / off)",
    )
    ap.add_argument(
        "--hitter-bip-roe-rate",
        type=float,
        default=0.015,
        help="bip_roe_rate used by hitter-props profile",
    )
    ap.add_argument(
        "--hitter-bip-fc-rate",
        type=float,
        default=0.05,
        help="bip_fc_rate used by hitter-props profile",
    )

    ap.add_argument(
        "--manifest-out",
        default="",
        help=(
            "Optional explicit bundle-manifest path. "
            "Default: <out-game>/daily_summary_<date>_profile_bundle.json"
        ),
    )
    ap.add_argument(
        "--locked-policy-out",
        default="",
        help=(
            "Optional explicit locked-policy card path. "
            "Default: <out-game>/daily_summary_<date>_locked_policy.json"
        ),
    )
    ap.add_argument(
        "--official-totals-cap",
        type=int,
        default=DEFAULT_OFFICIAL_CAPS["totals"],
        help="Daily max totals bets for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-totals-diff-min",
        type=float,
        default=float(DEFAULT_LOCK_POLICY["totals_diff_min"]),
        help="Minimum mean support gap that must agree with the selected totals side.",
    )
    ap.add_argument(
        "--official-ml-cap",
        type=int,
        default=DEFAULT_OFFICIAL_CAPS["ml"],
        help="Daily max moneyline bets for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-pitcher-cap",
        type=int,
        default=DEFAULT_OFFICIAL_CAPS["pitcher_props"],
        help="Daily max pitcher props bets for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-hitter-cap",
        type=int,
        default=DEFAULT_OFFICIAL_CAPS["hitter_props"],
        help="Aggregate daily max hitter props for the official locked-policy card after submarket caps are applied (negative = uncapped).",
    )
    ap.add_argument(
        "--official-hitter-hr-cap",
        type=int,
        default=DEFAULT_OFFICIAL_HITTER_SUBCAPS["hitter_home_runs"],
        help="Daily max hitter HR props for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-hitter-hits-cap",
        type=int,
        default=DEFAULT_OFFICIAL_HITTER_SUBCAPS["hitter_hits"],
        help="Daily max hitter hits props for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-hitter-tb-cap",
        type=int,
        default=DEFAULT_OFFICIAL_HITTER_SUBCAPS["hitter_total_bases"],
        help="Daily max hitter total-bases props for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-hitter-runs-cap",
        type=int,
        default=DEFAULT_OFFICIAL_HITTER_SUBCAPS["hitter_runs"],
        help="Daily max hitter runs props for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-hitter-rbis-cap",
        type=int,
        default=DEFAULT_OFFICIAL_HITTER_SUBCAPS["hitter_rbis"],
        help="Daily max hitter RBI props for the official locked-policy card (negative = uncapped).",
    )
    ap.add_argument(
        "--official-hitter-edge-min",
        type=float,
        default=float(DEFAULT_LOCK_POLICY["hitter_edge_min"]),
        help="Base minimum no-vig edge for official hitter props.",
    )
    ap.add_argument(
        "--official-hitter-max-favorite-odds",
        type=int,
        default=int(DEFAULT_LOCK_POLICY["hitter_max_favorite_odds"]),
        help="Maximum allowed favorite price for official hitter props; more negative prices are discarded.",
    )
    ap.add_argument(
        "--official-hitter-hr-under-0-5-max-favorite-odds",
        type=int,
        default=int(DEFAULT_LOCK_POLICY["hitter_hr_under_0_5_max_favorite_odds"]),
        help="Maximum allowed favorite price for official hitter HR under 0.5 props; more negative prices are discarded.",
    )
    ap.add_argument(
        "--official-hitter-runs-edge-min",
        type=float,
        default=float(_hitter_edge_min_for_market(DEFAULT_LOCK_POLICY, "hitter_runs")),
        help="Override minimum no-vig edge for official hitter runs picks.",
    )
    ap.add_argument(
        "--official-hitter-rbis-edge-min",
        type=float,
        default=float(_hitter_edge_min_for_market(DEFAULT_LOCK_POLICY, "hitter_rbis")),
        help="Override minimum no-vig edge for official hitter RBI picks.",
    )
    ap.add_argument(
        "--official-hitter-hr-topn",
        type=int,
        default=24,
        help="Force hitter HR top-N output high enough to build the official hitter card.",
    )
    ap.add_argument(
        "--official-pitcher-max-favorite-odds",
        type=int,
        default=int(DEFAULT_LOCK_POLICY["pitcher_max_favorite_odds"]),
        help="Maximum allowed favorite price for official pitcher props; more negative prices are discarded.",
    )
    ap.add_argument(
        "--official-hitter-props-topn",
        type=int,
        default=24,
        help="Force hitter props top-N output high enough to build the official hitter card.",
    )
    ap.add_argument(
        "--outs-prob-calibration",
        default="data/tuning/outs_calibration/default.json",
        help="Calibration JSON for official pitcher outs recommendations (use 'off' to disable).",
    )
    ap.add_argument(
        "--so-prob-calibration",
        default="data/tuning/so_calibration/default.json",
        help="Calibration JSON for official pitcher strikeout recommendations (use 'off' to disable).",
    )
    ap.add_argument(
        "--locked-policy-min-sims",
        type=int,
        default=int(_DEFAULT_LOCKED_POLICY_MIN_SIMS),
        help="Minimum simulation count required before writing the official locked-policy card; lower-sim runs skip final card publish.",
    )

    # Parse known args and pass all unknown args through to each daily_update run.
    args, passthrough = ap.parse_known_args()

    py_exe = _resolve_path(str(args.python_exe))
    daily_update_py = _ROOT / "tools" / "daily_update.py"
    if not py_exe.exists():
        raise SystemExit(f"Python executable not found: {py_exe}")
    if not daily_update_py.exists():
        raise SystemExit(f"Missing daily_update tool: {daily_update_py}")

    out_game = _resolve_path(str(args.out_game))
    out_pitcher = _resolve_path(str(args.out_pitcher))
    out_hitter = _resolve_path(str(args.out_hitter))
    out_game.mkdir(parents=True, exist_ok=True)
    out_pitcher.mkdir(parents=True, exist_ok=True)
    out_hitter.mkdir(parents=True, exist_ok=True)

    token = str(args.date).replace("-", "_")
    pitcher_lines_path = _ROOT / "data" / "market" / "oddsapi" / f"oddsapi_pitcher_props_{token}.json"
    hitter_lines_path = _ROOT / "data" / "market" / "oddsapi" / f"oddsapi_hitter_props_{token}.json"
    pitcher_market_entries = _market_entries_n(pitcher_lines_path, root_key="pitcher_props")
    hitter_market_entries = _market_entries_n(hitter_lines_path, root_key="hitter_props")

    game_extra: List[str] = []
    if not _is_off(str(args.game_pitch_model_overrides)):
        game_extra.extend(["--pitch-model-overrides", str(args.game_pitch_model_overrides)])

    pitcher_extra: List[str] = []
    if not _is_off(str(args.pitcher_pitch_model_overrides)):
        pitcher_extra.extend(["--pitch-model-overrides", str(args.pitcher_pitch_model_overrides)])

    hitter_extra: List[str] = [
        "--bip-roe-rate",
        str(float(args.hitter_bip_roe_rate)),
        "--bip-fc-rate",
        str(float(args.hitter_bip_fc_rate)),
        "--hitter-hr-topn",
        str(int(args.official_hitter_hr_topn)),
        "--hitter-props-topn",
        str(int(args.official_hitter_props_topn)),
    ]
    if not _is_off(str(args.hitter_pitch_model_overrides)):
        hitter_extra.extend(["--pitch-model-overrides", str(args.hitter_pitch_model_overrides)])

    profiles: List[Tuple[str, str, Path, List[str]]] = [
        ("game_roi", "game_recos", out_game, game_extra),
        ("pitcher_props", "pitcher_props_recos", out_pitcher, pitcher_extra),
        ("hitter_props", "hitter_props_recos", out_hitter, hitter_extra),
    ]
    profile_skip_reasons: Dict[str, str] = {}
    if pitcher_market_entries <= 0:
        profile_skip_reasons["pitcher_props"] = f"no pitcher prop market entries in {_rel(pitcher_lines_path)}"
    if hitter_market_entries <= 0:
        profile_skip_reasons["hitter_props"] = f"no hitter prop market entries in {_rel(hitter_lines_path)}"

    failures: List[Dict[str, Any]] = []
    profile_info: Dict[str, Any] = {}
    shared_lineups_last_known_path = out_game / "lineups_last_known_by_team.json"

    for profile_name, role_name, out_dir, extra in profiles:
        summary_path = out_dir / f"daily_summary_{token}.json"
        sim_dir = out_dir / "sims" / str(args.date)
        snapshot_dir = out_dir / "snapshots" / str(args.date)
        if role_name != "game_recos":
            source_snapshot_dir = out_game / "snapshots" / str(args.date)
            _sync_profile_snapshot_dir(source_snapshot_dir, snapshot_dir)
        skip_reason = profile_skip_reasons.get(profile_name)
        if skip_reason:
            print(f"[multi-profile] Skipping profile '{profile_name}' -> {skip_reason}")
            profile_info[role_name] = {
                "profile": profile_name,
                "out_dir": _rel(out_dir),
                "summary_path": _rel(summary_path),
                "sim_dir": _rel(sim_dir),
                "snapshot_dir": _rel(snapshot_dir),
                "extra_args": list(extra),
                "exit_code": 0,
                "skipped": True,
                "skip_reason": str(skip_reason),
            }
            continue
        rc, cmd = _run_profile(
            profile_name=profile_name,
            py_exe=py_exe,
            daily_update_py=daily_update_py,
            date=str(args.date),
            season=int(args.season),
            passthrough_args=list(passthrough),
            out_dir=out_dir,
            extra_args=extra,
            lineups_last_known_path=shared_lineups_last_known_path,
        )
        profile_info[role_name] = {
            "profile": profile_name,
            "out_dir": _rel(out_dir),
            "summary_path": _rel(summary_path),
            "sim_dir": _rel(sim_dir),
            "snapshot_dir": _rel(snapshot_dir),
            "extra_args": list(extra),
            "exit_code": int(rc),
            "skipped": False,
        }
        if rc != 0:
            failures.append(
                {
                    "role": role_name,
                    "profile": profile_name,
                    "exit_code": int(rc),
                    "command": cmd,
                }
            )

    best_selection_path = _ROOT / "_tmp_best_set_selection_holdout13.json"
    best_selection: Optional[Dict[str, Any]] = None
    try:
        if best_selection_path.exists():
            best_selection = json.loads(best_selection_path.read_text(encoding="utf-8"))
    except Exception:
        best_selection = None

    so_prob_calibration = _load_json_cfg(str(args.so_prob_calibration))
    so_prob_calibration_path = None if _is_off(str(args.so_prob_calibration)) else _resolve_path(str(args.so_prob_calibration))
    outs_prob_calibration = _load_json_cfg(str(args.outs_prob_calibration))
    outs_prob_calibration_path = None if _is_off(str(args.outs_prob_calibration)) else _resolve_path(str(args.outs_prob_calibration))
    official_caps = _normalized_official_caps(
        {
            "totals": args.official_totals_cap,
            "ml": args.official_ml_cap,
            "pitcher_props": args.official_pitcher_cap,
            "hitter_props": args.official_hitter_cap,
        }
    )
    official_hitter_subcaps = _normalized_hitter_subcaps(
        {
            "hitter_home_runs": args.official_hitter_hr_cap,
            "hitter_hits": args.official_hitter_hits_cap,
            "hitter_total_bases": args.official_hitter_tb_cap,
            "hitter_runs": args.official_hitter_runs_cap,
            "hitter_rbis": args.official_hitter_rbis_cap,
        }
    )
    official_policy_overrides = _policy_with_overrides(
        DEFAULT_LOCK_POLICY,
        scalar_updates={
            "totals_diff_min": args.official_totals_diff_min,
            "hitter_edge_min": args.official_hitter_edge_min,
            "hitter_max_favorite_odds": args.official_hitter_max_favorite_odds,
            "hitter_hr_under_0_5_max_favorite_odds": args.official_hitter_hr_under_0_5_max_favorite_odds,
            "pitcher_max_favorite_odds": args.official_pitcher_max_favorite_odds,
        },
        hitter_edge_updates={
            "hitter_runs": args.official_hitter_runs_edge_min,
            "hitter_rbis": args.official_hitter_rbis_edge_min,
        },
    )

    if str(args.locked_policy_out).strip():
        locked_policy_path = _resolve_path(str(args.locked_policy_out))
    else:
        locked_policy_path = out_game / f"daily_summary_{token}_locked_policy.json"
    locked_policy_error: Optional[str] = None
    locked_policy_card: Optional[Dict[str, Any]] = None
    current_run_sims = _safe_int(_argv_flag_value(list(passthrough), "--sims"))
    try:
        if current_run_sims is not None and int(current_run_sims) < int(args.locked_policy_min_sims):
            locked_policy_error = (
                f"Skipped locked-policy card publish because this run only used {int(current_run_sims)} sims "
                f"and the minimum is {int(args.locked_policy_min_sims)}"
            )
        else:
            locked_policy_card = _build_locked_policy_card(
                date=str(args.date),
                season=int(args.season),
                out_game=out_game,
                out_pitcher=out_pitcher,
                out_hitter=out_hitter,
                best_selection_path=best_selection_path,
                best_selection=best_selection,
                profile_info=profile_info,
                so_prob_calibration_path=so_prob_calibration_path,
                so_prob_calibration=so_prob_calibration,
                outs_prob_calibration_path=outs_prob_calibration_path,
                outs_prob_calibration=outs_prob_calibration,
                policy_overrides=official_policy_overrides,
                market_caps=official_caps,
                hitter_subcaps=official_hitter_subcaps,
            )
            _write_json(locked_policy_path, locked_policy_card)
    except Exception as e:
        locked_policy_error = f"{type(e).__name__}: {e}"

    if str(args.manifest_out).strip():
        manifest_path = _resolve_path(str(args.manifest_out))
    else:
        manifest_path = out_game / f"daily_summary_{token}_profile_bundle.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    manifest = {
        "date": str(args.date),
        "season": int(args.season),
        "generated_at": datetime.now().isoformat(),
        "tool": "tools/daily_update_multi_profile.py",
        "daily_update_tool": _rel(daily_update_py),
        "python_exe": _rel(py_exe),
        "passthrough_args": list(passthrough),
        "profiles": profile_info,
        "selection_source": _rel(best_selection_path),
        "best_selection": best_selection,
        "official_locked_policy": {
            "card_path": (_rel(locked_policy_path) if locked_policy_card is not None else None),
            "cap_profile": (
                str(locked_policy_card.get("cap_profile") or "custom")
                if locked_policy_card is not None
                else _official_cap_profile_name(official_caps, official_hitter_subcaps)
            ),
            "caps": (dict(locked_policy_card.get("caps") or {}) if locked_policy_card is not None else dict(official_caps)),
            "hitter_subcaps": (
                dict(locked_policy_card.get("hitter_subcaps") or {})
                if locked_policy_card is not None
                else {
                    market_name: (int(value) if value is not None else None)
                    for market_name, value in official_hitter_subcaps.items()
                }
            ),
            "staking": ((locked_policy_card.get("staking") or {}) if locked_policy_card is not None else None),
            "selected_counts": (_locked_policy_selected_counts(locked_policy_card) if locked_policy_card is not None else None),
            "warnings": (locked_policy_card.get("warnings") if locked_policy_card is not None else []),
            "explanation_diagnostics": ((locked_policy_card.get("explanation_diagnostics") or {}) if locked_policy_card is not None else None),
            "audit_track": ((locked_policy_card.get("audit_track") or {}) if locked_policy_card is not None else None),
            "error": locked_policy_error,
        },
        "failures": failures,
        "failures_n": int(len(failures)),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"[multi-profile] Wrote bundle manifest: {_rel(manifest_path)}")
    if locked_policy_card is not None:
        print(f"[multi-profile] Wrote locked-policy card: {_rel(locked_policy_path)}")
    elif locked_policy_error:
        print(f"[multi-profile] Locked-policy card error: {locked_policy_error}")
    print("[multi-profile] Recommendation source mapping:")
    print(f"  games -> {profile_info['game_recos']['summary_path']}")
    print(f"  pitcher props -> {profile_info['pitcher_props_recos']['summary_path']}")
    print(f"  hitter props -> {profile_info['hitter_props_recos']['summary_path']}")

    if failures:
        print(f"[multi-profile] {len(failures)} profile run(s) failed.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
