from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import load_feed_live_from_raw
from sim_engine.market_pitcher_props import load_pitcher_prop_lines, no_vig_over_prob, normalize_pitcher_name
from sim_engine.prob_calibration import apply_prob_calibration, apply_prop_prob_calibration
from tools.eval.eval_sim_day_vs_actual import (
    _HITTER_PROP_SPECS,
    _actual_batter_box_batting,
    _actual_linescore,
    _brier,
    _logloss,
    _mean_from_dist,
    _parse_actual_starter_pitching,
    _prob_from_dist,
    _prob_margin_ge,
    _prob_margin_le,
    _prob_over_line_from_dist,
    _rmse,
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _load_jsonish(value: str) -> Dict[str, Any]:
    text = str(value or "").strip()
    if not text or text.lower() == "off":
        return {}
    candidate = Path(text)
    if not candidate.is_absolute():
        candidate = (_ROOT / candidate).resolve()
    if candidate.exists() and candidate.is_file():
        try:
            obj = _read_json(candidate)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _team_row(team_obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team_id": _safe_int(team_obj.get("team_id")),
        "name": str(team_obj.get("name") or ""),
        "abbr": str(team_obj.get("abbreviation") or team_obj.get("abbr") or ""),
    }


def _actual_linescore_with_first1(feed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    base = _actual_linescore(feed)
    if not isinstance(base, dict):
        return None
    innings = base.get("innings") or []
    if isinstance(innings, list) and innings:
        first = innings[0] if isinstance(innings[0], dict) else {}
        base["first1"] = {
            "away": int(_safe_int(first.get("away")) or 0),
            "home": int(_safe_int(first.get("home")) or 0),
        }
    else:
        base["first1"] = {"away": 0, "home": 0}
    return base


def _actual_stat(actual_batter_box: Dict[str, Dict[int, Dict[str, int]]], batter_id: int, key: str) -> int:
    for side in ("away", "home"):
        side_box = actual_batter_box.get(side) or {}
        row = side_box.get(int(batter_id)) or {}
        if not row:
            continue
        if str(key) == "TB":
            try:
                hits = int(row.get("H") or 0)
                doubles = int(row.get("2B") or 0)
                triples = int(row.get("3B") or 0)
                home_runs = int(row.get("HR") or 0)
                return int(hits + doubles + 2 * triples + 3 * home_runs)
            except Exception:
                return 0
        try:
            return int(row.get(key) or 0)
        except Exception:
            return 0
    return 0


def _score_hitter_props(
    hitter_topn: Dict[str, Any],
    actual_batter_box: Dict[str, Dict[int, Dict[str, int]]],
    hitter_props_prob_calibration: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, List[float]]], int]:
    backtest: Dict[str, Any] = {}
    rollup: Dict[str, Dict[str, List[float]]] = {
        prop_key: {"brier": [], "logloss": [], "p": [], "y": []}
        for prop_key, _prob_key, _actual_key, _mean_field, _threshold in _HITTER_PROP_SPECS
    }
    top_n = int(_safe_int(hitter_topn.get("n")) or 0)
    for prop_key, prob_key, actual_key, _mean_field, threshold in _HITTER_PROP_SPECS:
        rows = hitter_topn.get(prop_key) or []
        scored: List[Dict[str, Any]] = []
        if not isinstance(rows, list):
            backtest[prop_key] = {"n": 0, "brier": None, "logloss": None, "avg_p": None, "emp_rate": None, "scored": []}
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            batter_id = _safe_int(row.get("batter_id"))
            if not batter_id or batter_id <= 0:
                continue
            p_raw = _safe_float(row.get(prob_key)) or 0.0
            p_value = _safe_float(row.get(f"{prob_key}_cal"))
            if p_value is None:
                p_value = float(apply_prop_prob_calibration(float(p_raw), hitter_props_prob_calibration, prop_key=prop_key))
            actual_value = int(_actual_stat(actual_batter_box, int(batter_id), actual_key))
            y_value = 1 if int(actual_value) >= int(threshold) else 0
            scored.append(
                {
                    "batter_id": int(batter_id),
                    "name": str(row.get("name") or ""),
                    "p": float(p_raw),
                    "p_cal": float(p_value),
                    "actual": int(actual_value),
                    "y": int(y_value),
                }
            )
            rollup[prop_key]["brier"].append(_brier(float(p_value), int(y_value)))
            rollup[prop_key]["logloss"].append(_logloss(float(p_value), int(y_value)))
            rollup[prop_key]["p"].append(float(p_value))
            rollup[prop_key]["y"].append(int(y_value))
        values = rollup[prop_key]
        backtest[prop_key] = {
            "n": int(len(scored)),
            "brier": (sum(values["brier"]) / len(values["brier"])) if values["brier"] else None,
            "logloss": (sum(values["logloss"]) / len(values["logloss"])) if values["logloss"] else None,
            "avg_p": (sum(values["p"]) / len(values["p"])) if values["p"] else None,
            "emp_rate": (sum(float(y) for y in values["y"]) / float(len(values["y"]))) if values["y"] else None,
            "scored": scored,
        }
    return backtest, rollup, top_n


def _score_hitter_hr(
    hitter_hr_topn: Dict[str, Any],
    actual_batter_box: Dict[str, Dict[int, Dict[str, int]]],
    hitter_hr_prob_calibration: Dict[str, Any],
) -> Tuple[Dict[str, Any], int]:
    overall = hitter_hr_topn.get("overall") if isinstance(hitter_hr_topn, dict) else []
    scored: List[Dict[str, Any]] = []
    brier: List[float] = []
    logloss: List[float] = []
    probs: List[float] = []
    ys: List[int] = []
    if isinstance(overall, list):
        for row in overall:
            if not isinstance(row, dict):
                continue
            batter_id = _safe_int(row.get("batter_id"))
            if not batter_id or batter_id <= 0:
                continue
            p_raw = _safe_float(row.get("p_hr_1plus")) or 0.0
            p_value = _safe_float(row.get("p_hr_1plus_cal"))
            if p_value is None:
                p_value = float(apply_prop_prob_calibration(float(p_raw), hitter_hr_prob_calibration, prop_key="hr_1plus"))
            actual_hr = int(_actual_stat(actual_batter_box, int(batter_id), "HR"))
            y_value = 1 if int(actual_hr) >= 1 else 0
            scored.append(
                {
                    "batter_id": int(batter_id),
                    "name": str(row.get("name") or ""),
                    "p_hr_1plus": float(p_raw),
                    "p_hr_1plus_cal": float(p_value),
                    "actual_hr": int(actual_hr),
                    "y_hr_1plus": int(y_value),
                }
            )
            brier.append(_brier(float(p_value), int(y_value)))
            logloss.append(_logloss(float(p_value), int(y_value)))
            probs.append(float(p_value))
            ys.append(int(y_value))
    return {
        "n": int(len(scored)),
        "brier": (sum(brier) / len(brier)) if brier else None,
        "logloss": (sum(logloss) / len(logloss)) if logloss else None,
        "avg_p": (sum(probs) / len(probs)) if probs else None,
        "emp_rate": (sum(float(y) for y in ys) / float(len(ys))) if ys else None,
        "scored_overall": scored,
    }, int(len(overall) if isinstance(overall, list) else 0)


def _preferred_hitter_hr_likelihood(sim_payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(sim_payload, dict):
        return {}
    full_rows = sim_payload.get("hitter_hr_likelihood_all")
    if isinstance(full_rows, dict) and isinstance(full_rows.get("overall"), list) and full_rows.get("overall"):
        return full_rows
    top_rows = sim_payload.get("hitter_hr_likelihood_topn")
    return top_rows if isinstance(top_rows, dict) else {}


def _build_game_row(
    *,
    sim_obj: Dict[str, Any],
    feed: Dict[str, Any],
    market_lines: Dict[str, Dict[str, Dict[str, Any]]],
    so_prob_calibration: Dict[str, Any],
    outs_prob_calibration: Dict[str, Any],
    hitter_hr_prob_calibration: Dict[str, Any],
    hitter_props_prob_calibration: Dict[str, Any],
    market_push_policy: str,
) -> Optional[Dict[str, Any]]:
    actual = _actual_linescore_with_first1(feed)
    if not isinstance(actual, dict):
        return None
    sim_payload = (sim_obj.get("sim") or {}) if isinstance(sim_obj.get("sim"), dict) else {}
    segments = (sim_payload.get("segments") or {}) if isinstance(sim_payload.get("segments"), dict) else {}
    sims_n = int(_safe_int(sim_payload.get("sims")) or 0)
    if sims_n <= 0:
        return None

    per_seg: Dict[str, Any] = {}
    for seg_name in ("full", "first1", "first5", "first3"):
        seg = segments.get(seg_name) or {}
        if not isinstance(seg, dict) or not seg:
            continue
        p_home = float(seg.get("home_win_prob") or 0.0)
        p_home = float(min(1.0, max(0.0, p_home)))
        act = actual.get(seg_name) or {}
        a_away = int(act.get("away") or 0)
        a_home = int(act.get("home") or 0)
        y_value = 1 if a_home > a_away else 0
        mean_total = _mean_from_dist(seg.get("total_runs_dist") or {})
        mean_margin = _mean_from_dist(seg.get("run_margin_dist") or {})
        extra_metrics: Dict[str, Any] = {}
        if seg_name == "full":
            actual_total = int(a_away + a_home)
            p_exact_total = _prob_from_dist(seg.get("total_runs_dist") or {}, actual_total, denom=sims_n)
            p_exact_total = float(max(1.0 / float(max(1, sims_n * 1000)), p_exact_total))
            extra_metrics["nll_exact_total"] = float(-math.log(p_exact_total))
            dist_margin = seg.get("run_margin_dist") or {}
            fav_is_home = bool(p_home >= 0.5)
            if fav_is_home:
                p_cover = _prob_margin_ge(dist_margin, threshold=2, denom=sims_n)
                y_cover = 1 if (a_home - a_away) >= 2 else 0
            else:
                p_cover = _prob_margin_le(dist_margin, threshold=-2, denom=sims_n)
                y_cover = 1 if (a_home - a_away) <= -2 else 0
            extra_metrics["fav_is_home"] = bool(fav_is_home)
            extra_metrics["p_fav_cover_minus_1_5"] = float(p_cover)
            extra_metrics["brier_fav_cover_minus_1_5"] = _brier(float(p_cover), int(y_cover))
            extra_metrics["logloss_fav_cover_minus_1_5"] = _logloss(float(p_cover), int(y_cover))
            extra_metrics["fav_cover_minus_1_5_actual"] = int(y_cover)
        per_seg[seg_name] = {
            "home_win_prob": p_home,
            "away_win_prob": float(seg.get("away_win_prob") or 0.0),
            "tie_prob": float(seg.get("tie_prob") or 0.0),
            "mean_total_runs": mean_total,
            "mean_run_margin_home_minus_away": mean_margin,
            "actual": {"away": a_away, "home": a_home},
            "metrics": {
                "brier_home_win": _brier(p_home, y_value),
                "logloss_home_win": _logloss(p_home, y_value),
                "abs_err_total_runs": None if mean_total is None else float(abs(float(mean_total) - float(a_away + a_home))),
                "abs_err_run_margin": None if mean_margin is None else float(abs(float(mean_margin) - float(a_home - a_away))),
                **extra_metrics,
            },
        }

    actual_batter_box = {
        "away": _actual_batter_box_batting(feed, "away"),
        "home": _actual_batter_box_batting(feed, "home"),
    }
    actual_starters = {
        "away": _parse_actual_starter_pitching(feed, "away", _safe_int((sim_obj.get("starters") or {}).get("away"))),
        "home": _parse_actual_starter_pitching(feed, "home", _safe_int((sim_obj.get("starters") or {}).get("home"))),
    }

    market_for_game: Dict[str, Any] = {"away": None, "home": None}
    for side in ("away", "home"):
        raw_name = str(((sim_obj.get("starter_names") or {}).get(side) or ""))
        name_key = normalize_pitcher_name(raw_name)
        if name_key and name_key in market_lines:
            market_for_game[side] = {"name_key": name_key, **(market_lines.get(name_key) or {})}

    pitcher_preds = (sim_payload.get("pitcher_props") or {}) if isinstance(sim_payload.get("pitcher_props"), dict) else {}
    hitter_topn = sim_payload.get("hitter_props_likelihood_topn") or {}
    hitter_hr_topn = _preferred_hitter_hr_likelihood(sim_payload)
    hitter_props_backtest, hitter_prop_rollup, hitter_top_n = _score_hitter_props(
        hitter_topn if isinstance(hitter_topn, dict) else {},
        actual_batter_box,
        hitter_props_prob_calibration,
    )
    hitter_hr_backtest, hitter_hr_top_n = _score_hitter_hr(
        hitter_hr_topn if isinstance(hitter_hr_topn, dict) else {},
        actual_batter_box,
        hitter_hr_prob_calibration,
    )

    row = {
        "game_pk": int(_safe_int(sim_obj.get("game_pk")) or 0),
        "game_date": str(sim_obj.get("date") or ""),
        "status_abstract": str((((sim_obj.get("schedule") or {}).get("status") or {}).get("abstract") or "")),
        "status_detailed": str((((sim_obj.get("schedule") or {}).get("status") or {}).get("detailed") or "")),
        "away": _team_row(sim_obj.get("away") or {}),
        "home": _team_row(sim_obj.get("home") or {}),
        "starters": dict(sim_obj.get("starters") or {}),
        "starter_names": dict(sim_obj.get("starter_names") or {}),
        "segments": per_seg,
        "pitcher_props": {
            "away": {
                "starter_id": _safe_int((sim_obj.get("starters") or {}).get("away")),
                "actual": actual_starters.get("away"),
                "pred": pitcher_preds.get(str(_safe_int((sim_obj.get("starters") or {}).get("away")) or "")) if _safe_int((sim_obj.get("starters") or {}).get("away")) else None,
                "market": market_for_game.get("away"),
            },
            "home": {
                "starter_id": _safe_int((sim_obj.get("starters") or {}).get("home")),
                "actual": actual_starters.get("home"),
                "pred": pitcher_preds.get(str(_safe_int((sim_obj.get("starters") or {}).get("home")) or "")) if _safe_int((sim_obj.get("starters") or {}).get("home")) else None,
                "market": market_for_game.get("home"),
            },
        },
        "hitter_props_likelihood": hitter_topn if isinstance(hitter_topn, dict) else {},
        "hitter_hr_likelihood": hitter_hr_topn if isinstance(hitter_hr_topn, dict) else {},
        "hitter_props_backtest": hitter_props_backtest,
        "hitter_hr_backtest": hitter_hr_backtest,
        "_hitter_rollup": hitter_prop_rollup,
        "_hitter_props_top_n": int(hitter_top_n),
        "_hitter_hr_top_n": int(hitter_hr_top_n),
        "_market_push_policy": str(market_push_policy),
        "_so_prob_calibration": dict(so_prob_calibration or {}),
        "_outs_prob_calibration": dict(outs_prob_calibration or {}),
    }
    return row


def _aggregate_segments(results: List[Dict[str, Any]], seg_name: str) -> Dict[str, Any]:
    briers: List[float] = []
    mae_total: List[float] = []
    mae_margin: List[float] = []
    for game in results:
        seg = ((game.get("segments") or {}).get(seg_name) or {})
        metrics = seg.get("metrics") or {}
        brier = _safe_float(metrics.get("brier_home_win"))
        if brier is not None:
            briers.append(float(brier))
        abs_total = _safe_float(metrics.get("abs_err_total_runs"))
        if abs_total is not None:
            mae_total.append(float(abs_total))
        abs_margin = _safe_float(metrics.get("abs_err_run_margin"))
        if abs_margin is not None:
            mae_margin.append(float(abs_margin))
    return {
        "games": int(len(results)),
        "brier_home_win": (sum(briers) / len(briers)) if briers else None,
        "mae_total_runs": (sum(mae_total) / len(mae_total)) if mae_total else None,
        "mae_run_margin": (sum(mae_margin) / len(mae_margin)) if mae_margin else None,
    }


def _build_assessment(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    totals_abs: List[float] = []
    totals_err: List[float] = []
    totals_nll: List[float] = []
    ml_brier: List[float] = []
    ml_logloss: List[float] = []
    ml_acc: List[float] = []
    ats_brier: List[float] = []
    ats_logloss: List[float] = []
    ats_acc: List[float] = []
    so_abs: List[float] = []
    so_err: List[float] = []
    outs_abs: List[float] = []
    outs_err: List[float] = []
    pitches_abs: List[float] = []
    pitches_err: List[float] = []
    so_line_brier: List[float] = []
    so_line_logloss: List[float] = []
    so_line_acc: List[float] = []
    outs_line_brier: List[float] = []
    outs_line_logloss: List[float] = []
    outs_line_acc: List[float] = []
    so_line_edge: List[float] = []
    outs_line_edge: List[float] = []
    so_line_pushes = 0
    outs_line_pushes = 0
    hr_brier: List[float] = []
    hr_logloss: List[float] = []
    hr_ps: List[float] = []
    hr_ys: List[int] = []
    hitter_prop_rollup: Dict[str, Dict[str, List[float]]] = {
        prop_key: {"brier": [], "logloss": [], "p": [], "y": []}
        for prop_key, _prob_key, _actual_key, _mean_field, _threshold in _HITTER_PROP_SPECS
    }

    market_push_policy = "skip"
    so_prob_calibration: Dict[str, Any] = {}
    outs_prob_calibration: Dict[str, Any] = {}
    hitter_props_top_n = 0
    hitter_hr_top_n = 0

    for game in results:
        full = ((game.get("segments") or {}).get("full") or {})
        metrics = full.get("metrics") or {}
        market_push_policy = str(game.get("_market_push_policy") or market_push_policy)
        if not so_prob_calibration:
            so_prob_calibration = dict(game.get("_so_prob_calibration") or {})
        if not outs_prob_calibration:
            outs_prob_calibration = dict(game.get("_outs_prob_calibration") or {})
        hitter_props_top_n = max(hitter_props_top_n, int(_safe_int(game.get("_hitter_props_top_n")) or 0))
        hitter_hr_top_n = max(hitter_hr_top_n, int(_safe_int(game.get("_hitter_hr_top_n")) or 0))

        abs_total = _safe_float(metrics.get("abs_err_total_runs"))
        if abs_total is not None:
            totals_abs.append(float(abs_total))
        mean_total = _safe_float(full.get("mean_total_runs"))
        actual_full = full.get("actual") or {}
        if mean_total is not None:
            actual_total = float(int(actual_full.get("away") or 0) + int(actual_full.get("home") or 0))
            totals_err.append(float(mean_total) - actual_total)
        nll_total = _safe_float(metrics.get("nll_exact_total"))
        if nll_total is not None:
            totals_nll.append(float(nll_total))

        ml_b = _safe_float(metrics.get("brier_home_win"))
        if ml_b is not None:
            ml_brier.append(float(ml_b))
        ml_ll = _safe_float(metrics.get("logloss_home_win"))
        if ml_ll is not None:
            ml_logloss.append(float(ml_ll))
        try:
            p_home = float(full.get("home_win_prob") or 0.0)
            y_value = 1 if int(actual_full.get("home") or 0) > int(actual_full.get("away") or 0) else 0
            ml_acc.append(1.0 if ((p_home >= 0.5) == (y_value == 1)) else 0.0)
        except Exception:
            pass

        ats_b = _safe_float(metrics.get("brier_fav_cover_minus_1_5"))
        if ats_b is not None:
            ats_brier.append(float(ats_b))
        ats_ll = _safe_float(metrics.get("logloss_fav_cover_minus_1_5"))
        if ats_ll is not None:
            ats_logloss.append(float(ats_ll))
        try:
            p_cover = float(metrics.get("p_fav_cover_minus_1_5") or 0.0)
            y_cover = int(metrics.get("fav_cover_minus_1_5_actual") or 0)
            ats_acc.append(1.0 if ((p_cover >= 0.5) == (y_cover == 1)) else 0.0)
        except Exception:
            pass

        for side in ("away", "home"):
            side_row = ((game.get("pitcher_props") or {}).get(side) or {})
            actual = side_row.get("actual") or {}
            pred = side_row.get("pred") or {}
            market = side_row.get("market") or {}
            if not isinstance(actual, dict) or not isinstance(pred, dict):
                continue
            actual_so = _safe_int(actual.get("so"))
            actual_outs = _safe_int(actual.get("outs"))
            if actual_so is None or actual_outs is None:
                continue
            so_mean = _safe_float(pred.get("so_mean"))
            if so_mean is not None:
                err = float(so_mean) - float(actual_so)
                so_abs.append(abs(err))
                so_err.append(err)
            outs_mean = _safe_float(pred.get("outs_mean"))
            if outs_mean is not None:
                err = float(outs_mean) - float(actual_outs)
                outs_abs.append(abs(err))
                outs_err.append(err)
            actual_pitches = _safe_int(actual.get("pitches"))
            pitches_mean = _safe_float(pred.get("pitches_mean"))
            if actual_pitches is not None and pitches_mean is not None:
                err = float(pitches_mean) - float(actual_pitches)
                pitches_abs.append(abs(err))
                pitches_err.append(err)

            so_market = (market.get("strikeouts") or {}) if isinstance(market, dict) else {}
            outs_market = (market.get("outs") or {}) if isinstance(market, dict) else {}
            so_line = _safe_float(so_market.get("line"))
            if so_line is not None:
                p_over = _prob_over_line_from_dist(pred.get("so_dist") or {}, float(so_line))
                if p_over is not None:
                    p_over = float(apply_prob_calibration(float(p_over), so_prob_calibration))
                    is_push = abs(float(actual_so) - float(so_line)) < 1e-9
                    if is_push:
                        so_line_pushes += 1
                        if market_push_policy != "skip":
                            y_value = 0.5 if market_push_policy == "half" else 0.0
                            so_line_brier.append(_brier(float(p_over), float(y_value)))
                            so_line_logloss.append(_logloss(float(p_over), float(y_value)))
                    else:
                        y_value = 1.0 if float(actual_so) > float(so_line) else 0.0
                        so_line_brier.append(_brier(float(p_over), float(y_value)))
                        so_line_logloss.append(_logloss(float(p_over), float(y_value)))
                        so_line_acc.append(1.0 if ((float(p_over) >= 0.5) == (float(y_value) >= 0.5)) else 0.0)
                    implied = no_vig_over_prob(so_market.get("over_odds"), so_market.get("under_odds"))
                    if implied is not None:
                        so_line_edge.append(float(p_over) - float(implied))

            outs_line = _safe_float(outs_market.get("line"))
            if outs_line is not None:
                p_over = _prob_over_line_from_dist(pred.get("outs_dist") or {}, float(outs_line))
                if p_over is not None:
                    p_over = float(apply_prob_calibration(float(p_over), outs_prob_calibration))
                    is_push = abs(float(actual_outs) - float(outs_line)) < 1e-9
                    if is_push:
                        outs_line_pushes += 1
                        if market_push_policy != "skip":
                            y_value = 0.5 if market_push_policy == "half" else 0.0
                            outs_line_brier.append(_brier(float(p_over), float(y_value)))
                            outs_line_logloss.append(_logloss(float(p_over), float(y_value)))
                    else:
                        y_value = 1.0 if float(actual_outs) > float(outs_line) else 0.0
                        outs_line_brier.append(_brier(float(p_over), float(y_value)))
                        outs_line_logloss.append(_logloss(float(p_over), float(y_value)))
                        outs_line_acc.append(1.0 if ((float(p_over) >= 0.5) == (float(y_value) >= 0.5)) else 0.0)
                    implied = no_vig_over_prob(outs_market.get("over_odds"), outs_market.get("under_odds"))
                    if implied is not None:
                        outs_line_edge.append(float(p_over) - float(implied))

        hitter_hr_backtest = game.get("hitter_hr_backtest") or {}
        for scored in (hitter_hr_backtest.get("scored_overall") or []):
            if not isinstance(scored, dict):
                continue
            p_value = _safe_float(scored.get("p_hr_1plus_cal"))
            y_value = _safe_int(scored.get("y_hr_1plus"))
            if p_value is None or y_value is None:
                continue
            hr_brier.append(_brier(float(p_value), int(y_value)))
            hr_logloss.append(_logloss(float(p_value), int(y_value)))
            hr_ps.append(float(p_value))
            hr_ys.append(int(y_value))

        raw_rollup = game.get("_hitter_rollup") or {}
        if isinstance(raw_rollup, dict):
            for prop_key, values in raw_rollup.items():
                if prop_key not in hitter_prop_rollup or not isinstance(values, dict):
                    continue
                for metric_key in ("brier", "logloss", "p", "y"):
                    metric_values = values.get(metric_key) or []
                    if isinstance(metric_values, list):
                        hitter_prop_rollup[prop_key][metric_key].extend(metric_values)

    return {
        "full_game": {
            "totals": {
                "games": int(len(results)),
                "mae": (sum(totals_abs) / len(totals_abs)) if totals_abs else None,
                "rmse": _rmse(totals_err),
                "avg_nll_exact_total": (sum(totals_nll) / len(totals_nll)) if totals_nll else None,
            },
            "moneyline": {
                "games": int(len(results)),
                "brier": (sum(ml_brier) / len(ml_brier)) if ml_brier else None,
                "logloss": (sum(ml_logloss) / len(ml_logloss)) if ml_logloss else None,
                "accuracy": (sum(ml_acc) / len(ml_acc)) if ml_acc else None,
            },
            "ats_runline_fav_minus_1_5": {
                "games": int(len(results)),
                "brier": (sum(ats_brier) / len(ats_brier)) if ats_brier else None,
                "logloss": (sum(ats_logloss) / len(ats_logloss)) if ats_logloss else None,
                "accuracy": (sum(ats_acc) / len(ats_acc)) if ats_acc else None,
            },
            "pitcher_props_starters": {
                "starters": int(len(so_abs)),
                "so_mae": (sum(so_abs) / len(so_abs)) if so_abs else None,
                "so_rmse": _rmse(so_err),
                "outs_mae": (sum(outs_abs) / len(outs_abs)) if outs_abs else None,
                "outs_rmse": _rmse(outs_err),
                "pitches_n": int(len(pitches_abs)),
                "pitches_mae": (sum(pitches_abs) / len(pitches_abs)) if pitches_abs else None,
                "pitches_rmse": _rmse(pitches_err),
                "pitches_bias": (sum(pitches_err) / len(pitches_err)) if pitches_err else None,
            },
            "pitcher_props_at_market_lines": {
                "lines_meta": {},
                "push_policy": str(market_push_policy),
                "strikeouts": {
                    "n": int(len(so_line_brier)),
                    "n_accuracy": int(len(so_line_acc)),
                    "n_edge": int(len(so_line_edge)),
                    "pushes": int(so_line_pushes),
                    "brier": (sum(so_line_brier) / len(so_line_brier)) if so_line_brier else None,
                    "logloss": (sum(so_line_logloss) / len(so_line_logloss)) if so_line_logloss else None,
                    "accuracy": (sum(so_line_acc) / len(so_line_acc)) if so_line_acc else None,
                    "avg_edge_vs_no_vig": (sum(so_line_edge) / len(so_line_edge)) if so_line_edge else None,
                },
                "outs": {
                    "n": int(len(outs_line_brier)),
                    "n_accuracy": int(len(outs_line_acc)),
                    "n_edge": int(len(outs_line_edge)),
                    "pushes": int(outs_line_pushes),
                    "brier": (sum(outs_line_brier) / len(outs_line_brier)) if outs_line_brier else None,
                    "logloss": (sum(outs_line_logloss) / len(outs_line_logloss)) if outs_line_logloss else None,
                    "accuracy": (sum(outs_line_acc) / len(outs_line_acc)) if outs_line_acc else None,
                    "avg_edge_vs_no_vig": (sum(outs_line_edge) / len(outs_line_edge)) if outs_line_edge else None,
                },
            },
            "hitter_hr_likelihood_topn": {
                "top_n": int(hitter_hr_top_n),
                "n": int(len(hr_brier)),
                "brier": (sum(hr_brier) / len(hr_brier)) if hr_brier else None,
                "logloss": (sum(hr_logloss) / len(hr_logloss)) if hr_logloss else None,
                "avg_p": (sum(hr_ps) / len(hr_ps)) if hr_ps else None,
                "emp_rate": (sum(float(y) for y in hr_ys) / float(len(hr_ys))) if hr_ys else None,
            },
            "hitter_props_likelihood_topn": {
                "top_n": int(hitter_props_top_n),
                **{
                    prop_key: {
                        "n": int(len(values.get("brier") or [])),
                        "brier": (sum(values.get("brier") or []) / len(values.get("brier") or [])) if (values.get("brier") or []) else None,
                        "logloss": (sum(values.get("logloss") or []) / len(values.get("logloss") or [])) if (values.get("logloss") or []) else None,
                        "avg_p": (sum(values.get("p") or []) / len(values.get("p") or [])) if (values.get("p") or []) else None,
                        "emp_rate": (sum(float(y) for y in (values.get("y") or [])) / float(len(values.get("y") or []))) if (values.get("y") or []) else None,
                    }
                    for prop_key, values in hitter_prop_rollup.items()
                },
            },
        }
    }


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Reconcile saved daily sim artifacts against actual results without rerunning sims")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", type=int, default=0)
    ap.add_argument("--sim-dir", default="", help="Optional directory containing saved sim_*.json artifacts")
    ap.add_argument("--out", default="", help="Output sim_vs_actual JSON path")
    ap.add_argument(
        "--prop-lines-source",
        choices=["auto", "oddsapi", "last_known", "bovada", "off"],
        default="auto",
        help="Source used for pitcher prop market-line scoring",
    )
    ap.add_argument(
        "--market-push-policy",
        choices=["loss", "half", "skip"],
        default="skip",
        help="How to score exact-at-line outcomes for pitcher O/U markets",
    )
    ap.add_argument(
        "--so-prob-calibration",
        default="data/tuning/so_calibration/default.json",
        help="JSON dict or path to JSON file for strikeout market calibration",
    )
    ap.add_argument(
        "--outs-prob-calibration",
        default="data/tuning/outs_calibration/default.json",
        help="JSON dict or path to JSON file for outs market calibration",
    )
    ap.add_argument(
        "--hitter-hr-prob-calibration",
        default="data/tuning/hitter_hr_calibration/default.json",
        help="JSON dict or path to JSON file for hitter HR calibration",
    )
    ap.add_argument(
        "--hitter-props-prob-calibration",
        default="data/tuning/hitter_props_calibration/default.json",
        help="JSON dict or path to JSON file for hitter props calibration",
    )
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    season = int(args.season) if int(args.season or 0) > 0 else int(str(args.date).split("-")[0])
    sim_dir = Path(str(args.sim_dir)).resolve() if str(args.sim_dir).strip() else (_ROOT / "data" / "daily" / "sims" / str(args.date)).resolve()
    if not sim_dir.exists() or not sim_dir.is_dir():
        raise SystemExit(f"Sim dir not found: {sim_dir}")

    sim_paths = sorted(path for path in sim_dir.glob("sim_*.json") if path.is_file())
    if not sim_paths:
        raise SystemExit(f"No sim_*.json artifacts found under: {sim_dir}")

    so_prob_calibration = _load_jsonish(str(args.so_prob_calibration))
    outs_prob_calibration = _load_jsonish(str(args.outs_prob_calibration))
    hitter_hr_prob_calibration = _load_jsonish(str(args.hitter_hr_prob_calibration))
    hitter_props_prob_calibration = _load_jsonish(str(args.hitter_props_prob_calibration))

    if str(args.prop_lines_source) == "off":
        market_lines = {}
        market_meta = {"source": "off", "path": None, "pitchers": 0}
    else:
        market_lines, market_meta = load_pitcher_prop_lines(str(args.date), prefer=str(args.prop_lines_source))

    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    skipped_games = 0
    sims_per_game = 0

    for sim_path in sim_paths:
        try:
            sim_obj = _read_json(sim_path)
        except Exception as exc:
            failures.append({"path": str(sim_path), "error": f"read_failed: {type(exc).__name__}: {exc}"})
            continue
        if not isinstance(sim_obj, dict):
            failures.append({"path": str(sim_path), "error": "invalid_json_root"})
            continue
        game_pk = _safe_int(sim_obj.get("game_pk"))
        if not game_pk or game_pk <= 0:
            failures.append({"path": str(sim_path), "error": "missing_game_pk"})
            continue
        try:
            feed = load_feed_live_from_raw(int(season), str(args.date), int(game_pk))
        except Exception as exc:
            failures.append({"game_pk": int(game_pk), "path": str(sim_path), "error": f"feed_live_load_failed: {type(exc).__name__}: {exc}"})
            continue
        row = _build_game_row(
            sim_obj=sim_obj,
            feed=feed,
            market_lines=market_lines,
            so_prob_calibration=so_prob_calibration,
            outs_prob_calibration=outs_prob_calibration,
            hitter_hr_prob_calibration=hitter_hr_prob_calibration,
            hitter_props_prob_calibration=hitter_props_prob_calibration,
            market_push_policy=str(args.market_push_policy),
        )
        if row is None:
            skipped_games += 1
            failures.append({"game_pk": int(game_pk), "path": str(sim_path), "error": "reconcile_skipped"})
            continue
        if sims_per_game <= 0:
            sims_per_game = int(_safe_int(((sim_obj.get("sim") or {}).get("sims"))) or 0)
        results.append(row)

    if not results:
        raise SystemExit("No prior-day sim artifacts could be reconciled")

    report = {
        "meta": {
            "date": str(args.date),
            "season": int(season),
            "sims_per_game": int(sims_per_game),
            "jobs": 0,
            "use_raw": True,
            "skipped_games": int(skipped_games),
            "generated_at": datetime.now().isoformat(),
            "tool": "tools/eval/reconcile_daily_sim_artifacts.py",
            "source_sim_dir": str(sim_dir),
            "prop_lines_source": str(args.prop_lines_source),
            "market_push_policy": str(args.market_push_policy),
            "so_prob_calibration": so_prob_calibration,
            "outs_prob_calibration": outs_prob_calibration,
            "hitter_hr_prob_calibration": hitter_hr_prob_calibration,
            "hitter_props_prob_calibration": hitter_props_prob_calibration,
        },
        "assessment": {},
        "aggregate": {
            "full": _aggregate_segments(results, "full"),
            "first1": _aggregate_segments(results, "first1"),
            "first5": _aggregate_segments(results, "first5"),
            "first3": _aggregate_segments(results, "first3"),
        },
        "games": results,
        "failures": failures,
        "failures_n": int(len(failures)),
    }
    report["assessment"] = _build_assessment(results)
    ((report.get("assessment") or {}).get("full_game") or {}).get("pitcher_props_at_market_lines", {}).update({"lines_meta": market_meta})

    out_path = Path(str(args.out)).resolve() if str(args.out).strip() else (_ROOT / "data" / "eval" / f"sim_vs_actual_{str(args.date)}.json").resolve()
    _write_json(out_path, report)

    print(f"Wrote: {out_path}")
    print("Aggregate (full):", report["aggregate"]["full"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())