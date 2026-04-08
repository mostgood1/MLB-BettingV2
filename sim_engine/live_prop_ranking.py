from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional, Sequence


DEFAULT_LIVE_PROP_FEATURES: tuple[str, ...] = (
    "live_edge",
    "selected_implied_prob",
    "market_line",
    "pregame_gap_selected",
    "progress_fraction",
    "score_diff_team",
    "inning",
    "outs",
    "state_available",
)


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return float(number)


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _sigmoid(value: float) -> float:
    try:
        return float(1.0 / (1.0 + math.exp(-float(value))))
    except OverflowError:
        return 0.0 if float(value) < 0.0 else 1.0


def _clip_prob(value: float, eps: float = 1e-9) -> float:
    return float(min(1.0 - eps, max(eps, float(value))))


def _bounded_probability(value: float, floor: float, ceiling: float) -> float:
    lower = min(max(float(floor), 1e-6), 0.499999)
    upper = max(min(float(ceiling), 0.999999), 0.500001)
    if upper < lower:
        lower, upper = upper, lower
    return float(min(upper, max(lower, float(value))))


def american_odds_implied_prob(odds: Any) -> Optional[float]:
    value = _safe_int(odds)
    if value is None or value == 0:
        return None
    if value > 0:
        return float(100.0 / (float(value) + 100.0))
    return float(abs(float(value)) / (abs(float(value)) + 100.0))


def selected_side_model_prob(row: Mapping[str, Any]) -> Optional[float]:
    selection = str(row.get("selection") or "").strip().lower()
    model_prob_over = _safe_float(row.get("model_prob_over"))
    if model_prob_over is None:
        return None
    if selection == "over":
        return float(model_prob_over)
    if selection == "under":
        return float(1.0 - float(model_prob_over))
    return None


def selected_pregame_gap(row: Mapping[str, Any]) -> float:
    selection = str(row.get("selection") or "").strip().lower()
    model_mean = _safe_float(row.get("model_mean"))
    market_line = _safe_float(row.get("market_line") if row.get("market_line") is not None else row.get("marketLine"))
    if model_mean is None or market_line is None:
        return 0.0
    if selection == "under":
        return float(market_line - model_mean)
    return float(model_mean - market_line)


def build_live_prop_feature_map(row: Mapping[str, Any]) -> Dict[str, float]:
    selection = str(row.get("selection") or "").strip().lower()
    market_line = _safe_float(row.get("market_line") if row.get("market_line") is not None else row.get("marketLine")) or 0.0
    live_edge = _safe_float(row.get("live_edge") if row.get("live_edge") is not None else row.get("liveEdge")) or 0.0
    implied_prob = american_odds_implied_prob(row.get("odds")) or 0.0

    progress_fraction = _safe_float(row.get("progress_fraction"))
    inning = _safe_float(row.get("inning"))
    outs = _safe_float(row.get("outs"))
    team_side = str(row.get("team_side") if row.get("team_side") is not None else row.get("teamSide") or "").strip().lower()

    score_away = _safe_float(row.get("score_away"))
    score_home = _safe_float(row.get("score_home"))
    if (score_away is None or score_home is None) and isinstance(row.get("gameState"), Mapping):
        game_state = row.get("gameState") or {}
        progress_fraction = progress_fraction if progress_fraction is not None else _safe_float(game_state.get("progressFraction"))
        inning = inning if inning is not None else _safe_float(game_state.get("inning"))
        outs = outs if outs is not None else _safe_float(game_state.get("outs"))
        score = game_state.get("score") if isinstance(game_state.get("score"), Mapping) else {}
        score_away = score_away if score_away is not None else _safe_float(score.get("away"))
        score_home = score_home if score_home is not None else _safe_float(score.get("home"))

    state_available = 1.0 if progress_fraction is not None or inning is not None or outs is not None else 0.0
    progress_fraction = float(progress_fraction or 0.0)
    inning = float(inning or 0.0)
    outs = float(outs or 0.0)

    score_diff_team = 0.0
    if score_away is not None and score_home is not None:
        if team_side == "away":
            score_diff_team = float(score_away - score_home)
        elif team_side == "home":
            score_diff_team = float(score_home - score_away)

    return {
        "live_edge": float(live_edge),
        "selected_implied_prob": float(implied_prob),
        "market_line": float(market_line),
        "pregame_gap_selected": float(selected_pregame_gap(row)),
        "progress_fraction": float(progress_fraction),
        "score_diff_team": float(score_diff_team),
        "inning": float(inning),
        "outs": float(outs),
        "selection_is_under": 1.0 if selection == "under" else 0.0,
        "state_available": float(state_available),
    }


def resolve_live_prop_ranking_cfg(cfg: Optional[Dict[str, Any]], prop_key: str) -> Optional[Dict[str, Any]]:
    if not isinstance(cfg, dict) or not cfg:
        return None
    if str(cfg.get("enabled", "true")).lower() in {"0", "false", "off", "no"}:
        return None
    props = cfg.get("props")
    if isinstance(props, dict):
        sub = props.get(str(prop_key))
        if isinstance(sub, dict) and str(sub.get("enabled", "true")).lower() not in {"0", "false", "off", "no"}:
            return sub
        default = cfg.get("default")
        if isinstance(default, dict) and str(default.get("enabled", "true")).lower() not in {"0", "false", "off", "no"}:
            return default
    if str(cfg.get("mode") or "").strip().lower() == "logistic_linear":
        return cfg
    return None


def _resolve_side_prior(row: Mapping[str, Any], cfg: Optional[Mapping[str, Any]]) -> tuple[Optional[float], float]:
    if not isinstance(cfg, Mapping):
        return None, 0.0
    selection = str(row.get("selection") or "").strip().lower()
    if selection not in {"over", "under"}:
        return None, 0.0
    side_priors = cfg.get("side_priors") if isinstance(cfg.get("side_priors"), Mapping) else {}
    side_cfg = side_priors.get(selection) if isinstance(side_priors.get(selection), Mapping) else {}
    probability = _safe_float(side_cfg.get("prob"))
    samples = max(0.0, float(_safe_float(side_cfg.get("n")) or 0.0))
    if probability is None:
        return None, 0.0
    blend_k = max(1.0, float(_safe_float(cfg.get("prior_blend_k")) or 25.0))
    blend_cap = min(0.95, max(0.0, float(_safe_float(cfg.get("prior_blend_cap")) or 0.75)))
    weight = min(blend_cap, samples / (samples + blend_k))
    return _clip_prob(probability), float(weight)


def predict_live_prop_win_probability_from_features(feature_map: Mapping[str, Any], cfg: Optional[Mapping[str, Any]]) -> Optional[float]:
    if not isinstance(cfg, Mapping):
        return None
    if str(cfg.get("enabled", "true")).lower() in {"0", "false", "off", "no"}:
        return None
    if str(cfg.get("mode") or "").strip().lower() != "logistic_linear":
        return None
    probability_floor = float(_safe_float(cfg.get("probability_floor")) or 0.03)
    probability_ceiling = float(_safe_float(cfg.get("probability_ceiling")) or 0.97)

    intercept = _safe_float(cfg.get("intercept"))
    weights = cfg.get("weights") if isinstance(cfg.get("weights"), Mapping) else {}
    centers = cfg.get("centers") if isinstance(cfg.get("centers"), Mapping) else {}
    scales = cfg.get("scales") if isinstance(cfg.get("scales"), Mapping) else {}
    feature_names = cfg.get("feature_names") if isinstance(cfg.get("feature_names"), Sequence) else DEFAULT_LIVE_PROP_FEATURES
    model_probability: Optional[float] = None

    if intercept is not None and isinstance(weights, Mapping):
        score = float(intercept)
        for feature_name in feature_names:
            name = str(feature_name)
            raw = _safe_float(feature_map.get(name))
            if raw is None:
                raw = 0.0
            center = _safe_float(centers.get(name)) or 0.0
            scale = _safe_float(scales.get(name)) or 1.0
            if abs(float(scale)) < 1e-9:
                scale = 1.0
            weight = _safe_float(weights.get(name)) or 0.0
            score += float(weight) * ((float(raw) - float(center)) / float(scale))
        model_probability = float(_sigmoid(score))

    side_prior_probability, side_prior_weight = _resolve_side_prior(feature_map, cfg)
    if model_probability is None:
        if side_prior_probability is None:
            return None
        return _bounded_probability(side_prior_probability, probability_floor, probability_ceiling)
    if side_prior_probability is None or side_prior_weight <= 0.0:
        return _bounded_probability(_clip_prob(model_probability), probability_floor, probability_ceiling)
    blended = (side_prior_weight * side_prior_probability) + ((1.0 - side_prior_weight) * model_probability)
    return _bounded_probability(_clip_prob(blended), probability_floor, probability_ceiling)


def predict_live_prop_win_probability(row: Mapping[str, Any], cfg: Optional[Dict[str, Any]], *, prop_key: Optional[str] = None) -> Optional[float]:
    key = str(prop_key or row.get("prop") or "").strip().lower()
    resolved = resolve_live_prop_ranking_cfg(cfg, key)
    if not isinstance(resolved, Mapping):
        return None
    return predict_live_prop_win_probability_from_features(build_live_prop_feature_map(row), resolved)