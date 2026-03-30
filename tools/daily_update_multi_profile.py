from __future__ import annotations

import argparse
from collections import Counter
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    "totals_side": "over",
    "totals_diff_min": 1.0,
    "ml_side": "home",
    "ml_edge_min": 0.01,
    "hitter_edge_min": 0.0,
    "hitter_edge_min_by_market": dict(DEFAULT_HITTER_EDGE_MIN_BY_MARKET),
    "hitter_max_favorite_odds": -200,
    "hitter_hr_under_0_5_max_favorite_odds": -200,
    "pitcher_market": "best",
    "pitcher_side": "over",
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
        "primary_lines": (0.5,),
    },
    "batter_hits": {
        "market": "hitter_hits",
        "label": "Hitter Hits",
        "prob_base": "hits",
        "primary_lines": (0.5,),
    },
    "batter_total_bases": {
        "market": "hitter_total_bases",
        "label": "Hitter Total Bases",
        "prob_base": "total_bases",
        "primary_lines": (1.5,),
    },
    "batter_runs_scored": {
        "market": "hitter_runs",
        "label": "Hitter Runs",
        "prob_base": "runs",
        "primary_lines": (0.5,),
    },
    "batter_rbis": {
        "market": "hitter_rbis",
        "label": "Hitter RBIs",
        "prob_base": "rbi",
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
        full = ((sim_obj.get("sim") or {}).get("segments") or {}).get("full") or {}

        totals_market = ((market_game.get("markets") or {}).get("totals") or {})
        total_line = totals_market.get("line")
        mean_total = _mean_from_dist(full.get("total_runs_dist") or {})
        if total_line is not None and mean_total is not None:
            edge = float(mean_total) - float(total_line)
            if edge >= float(policy["totals_diff_min"]):
                out["totals"].append(
                    {
                        **base,
                        "market": "totals",
                        "selection": str(policy["totals_side"]),
                        "edge": float(edge),
                        "market_line": float(total_line),
                        "model_mean_total": float(mean_total),
                        "odds": totals_market.get("over_odds"),
                        "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                        "market_no_vig_prob": no_vig_over_prob(
                            totals_market.get("over_odds"), totals_market.get("under_odds")
                        ),
                    }
                )

        h2h_market = ((market_game.get("markets") or {}).get("h2h") or {})
        home_prob = float(full.get("home_win_prob") or 0.0)
        away_prob = float(full.get("away_win_prob") or 0.0)
        denom = float(home_prob + away_prob)
        if denom > 0.0:
            home_prob /= denom
            home_nv, _ = _no_vig_two_way(h2h_market.get("home_odds"), h2h_market.get("away_odds"))
            if home_nv is not None:
                edge = float(home_prob) - float(home_nv)
                if edge >= float(policy["ml_edge_min"]):
                    out["ml"].append(
                        {
                            **base,
                            "market": "ml",
                            "selection": str(policy["ml_side"]),
                            "edge": float(edge),
                            "model_prob": float(home_prob),
                            "market_no_vig_prob": float(home_nv),
                            "odds": h2h_market.get("home_odds"),
                            "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                        }
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


def _collect_hitter_recommendations(sim_dir: Path, hitter_lines_path: Path, policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not hitter_lines_path.exists():
        return []

    hitter_odds_raw = (_read_json(hitter_lines_path).get("hitter_props") or {})
    hitter_odds = {normalize_pitcher_name(str(name)): markets for name, markets in hitter_odds_raw.items()}
    rows: List[Dict[str, Any]] = []

    for sim_obj in _iter_sim_records(sim_dir):
        pred = _extract_hitter_predictions(sim_obj)
        if not pred:
            continue
        base = _base_game_row(sim_obj)

        for player_key, rec in pred.items():
            if not _is_hitter_prediction_eligible(rec):
                continue
            markets = hitter_odds.get(player_key)
            if not isinstance(markets, dict):
                continue
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
                if not _hitter_price_allowed(
                    policy,
                    market_name=str(market_spec["market"]),
                    selection=str(side_pick.get("selection") or ""),
                    market_line=float(line_value),
                    odds=side_pick.get("odds"),
                ):
                    continue
                rows.append(
                    {
                        **base,
                        "market": str(market_spec["market"]),
                        "market_label": str(market_spec["label"]),
                        "market_group": "hitter_props",
                        "player_name": rec.get("name"),
                        "team": rec.get("team"),
                        "prop": market_key,
                        "selection": side_pick["selection"],
                        "edge": float(side_pick["edge"]),
                        "market_line": float(line_value),
                        "model_prob_over": float(p_over),
                        "market_prob_over": side_pick["market_prob_over"],
                        "market_prob_under": side_pick["market_prob_under"],
                        "market_prob_mode": side_pick["market_prob_mode"],
                        "market_no_vig_prob_over": side_pick["market_no_vig_prob_over"],
                        "selected_side_market_prob": float(side_pick["selected_side_market_prob"]),
                        "market_alternates": list(props_market.get("alternates") or []),
                        "odds": side_pick["odds"],
                        "stake_u": float(DEFAULT_HITTER_STAKE_U),
                    }
                )

    return rows


def _collect_pitcher_recommendations(
    sim_dir: Path,
    pitcher_lines_path: Path,
    policy: Dict[str, Any],
    so_prob_calibration: Optional[Dict[str, Any]],
    outs_prob_calibration: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not pitcher_lines_path.exists():
        return []

    pitcher_odds_raw = (_read_json(pitcher_lines_path).get("pitcher_props") or {})
    pitcher_odds = {normalize_pitcher_name(str(name)): markets for name, markets in pitcher_odds_raw.items()}
    rows: List[Dict[str, Any]] = []

    for sim_obj in _iter_sim_records(sim_dir):
        base = _base_game_row(sim_obj)
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
                if side_pick is None or str(side_pick.get("selection") or "") != str(policy["pitcher_side"]):
                    continue
                if not _pitcher_price_allowed(policy, odds=side_pick.get("odds")):
                    continue
                mean_key = str(market_spec.get("mean_key") or "")
                rows.append(
                    {
                        **base,
                        "market": "pitcher_props",
                        "pitcher_name": starter_name,
                        "team": (sim_obj.get(side) or {}).get("abbreviation"),
                        "team_side": side,
                        "prop": str(market_name),
                        "selection": str(policy["pitcher_side"]),
                        "edge": float(side_pick["edge"]),
                        "market_line": float(line_value),
                        "model_prob_over": float(p_over),
                        "market_prob_over": side_pick.get("market_prob_over"),
                        "market_prob_under": side_pick.get("market_prob_under"),
                        "market_prob_mode": side_pick.get("market_prob_mode"),
                        "market_no_vig_prob_over": side_pick.get("market_no_vig_prob_over"),
                        "selected_side_market_prob": side_pick.get("selected_side_market_prob"),
                        mean_key: pred.get(mean_key),
                        "market_alternates": list(props_market.get("alternates") or []),
                        "odds": side_pick.get("odds"),
                        "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                    }
                )

    return rows


def _row_rank_key(row: Dict[str, Any]) -> Tuple[float, float]:
    return (
        float(row.get("edge") or 0.0),
        float(row.get("model_prob") or row.get("model_prob_over") or row.get("model_mean_total") or 0.0),
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
    )
    hitter_rows = _collect_hitter_recommendations(hitter_sim_dir, hitter_lines_path, policy)

    raw_rows: Dict[str, List[Dict[str, Any]]] = {
        "totals": list(game_rows.get("totals") or []),
        "ml": list(game_rows.get("ml") or []),
    }

    markets: Dict[str, Any] = {}
    for market_name, rows in raw_rows.items():
        selected = _rank_and_cap(rows, caps.get(market_name))
        markets[market_name] = {
            "raw_candidates_n": int(len(rows)),
            "selected_n": int(len(selected)),
            "cap": (int(caps[market_name]) if market_name in caps else None),
            "stake_u": float(DEFAULT_STANDARD_STAKE_U),
            "recommendations": selected,
        }

    selected_pitcher_rows = _rank_and_cap_unique_players(pitcher_rows, caps.get("pitcher_props"))
    extra_pitcher_rows = _subtract_selected_rows(pitcher_rows, selected_pitcher_rows)
    markets["pitcher_props"] = {
        "raw_candidates_n": int(len(pitcher_rows)),
        "selected_n": int(len(selected_pitcher_rows)),
        "other_playable_candidates_n": int(len(extra_pitcher_rows)),
        "cap": (int(caps["pitcher_props"]) if caps.get("pitcher_props") is not None else None),
        "stake_u": float(DEFAULT_STANDARD_STAKE_U),
        "one_prop_per_player": True,
        "recommendations": selected_pitcher_rows,
        "other_playable_candidates": extra_pitcher_rows,
    }

    hitter_raw_by_market: Dict[str, List[Dict[str, Any]]] = {market_name: [] for market_name in HITTER_MARKET_ORDER}
    for row in hitter_rows:
        market_name = str(row.get("market") or "")
        hitter_raw_by_market.setdefault(market_name, []).append(row)

    selected_hitter_rows, selected_hitter_by_market, hitter_selection_mode = _select_hitter_recommendations(
        hitter_rows,
        caps.get("hitter_props"),
        normalized_hitter_subcaps,
        blocked_player_keys=_selected_player_keys(selected_pitcher_rows),
    )

    for market_name in HITTER_MARKET_ORDER:
        rows = list(hitter_raw_by_market.get(market_name) or [])
        selected = list(selected_hitter_by_market.get(market_name) or [])
        extra = _subtract_selected_rows(rows, selected)
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
        }

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
        "Current live defaults run totals at 2/day with a 1.0 mean-minus-line gate, keep ml at 1 and pitcher props at 3, and add one high-edge runs slot to the HR/Hits/TB hitter mix."
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

    return {
        "date": str(date),
        "season": int(season),
        "generated_at": datetime.now().isoformat(),
        "tool": "tools/daily_update_multi_profile.py",
        "selection_source": _rel(best_selection_path),
        "best_selection": best_selection,
        "policy": {
            "totals": {"side": "over", "mean_minus_line_min": float(policy["totals_diff_min"])},
            "ml": {"side": "home", "no_vig_edge_min": float(policy["ml_edge_min"])},
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
            "Pitcher props rank the best qualified outs/strikeouts over lanes into the shared pitcher bucket.",
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
        help="Minimum mean-minus-line edge for official totals picks.",
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
    try:
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
