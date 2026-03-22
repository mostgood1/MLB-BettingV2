from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sim_engine.prob_calibration import apply_prop_prob_calibration


_ROOT = Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_json_dict(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        obj = _read_json(path)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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


def _relative_path_str(path: Optional[Path]) -> Optional[str]:
    if not path:
        return None
    try:
        return str(path.resolve().relative_to(_ROOT)).replace("\\", "/")
    except Exception:
        return str(path.resolve()).replace("\\", "/")


def _wavg(sum_wx: float, sum_w: float) -> Optional[float]:
    if sum_w <= 0:
        return None
    return float(sum_wx / sum_w)


def _acc_wavg(acc: Dict[str, Tuple[float, float]], key: str, value: float, weight: float) -> None:
    sx, sw = acc.get(key, (0.0, 0.0))
    acc[key] = (sx + (float(value) * float(weight)), sw + float(weight))


def _brier(p: float, y: int) -> float:
    diff = float(p) - float(y)
    return float(diff * diff)


def _logloss(p: float, y: int, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    yy = int(y)
    return float(-math.log(pp if yy else (1.0 - pp)))


def _finalize_wavg(acc: Dict[str, Tuple[float, float]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, (sx, sw) in acc.items():
        out[key] = _wavg(sx, sw)
        out[f"{key}_weight"] = float(sw)
    return out


def _month_label(month_key: str) -> str:
    try:
        return datetime.strptime(month_key, "%Y-%m").strftime("%b %Y")
    except ValueError:
        return month_key


def _resolve_path(path_str: str) -> Optional[Path]:
    text = str(path_str or "").strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


def _find_argv_value(argv: Any, flag: str) -> Optional[str]:
    if not isinstance(argv, list):
        return None
    for idx, value in enumerate(argv[:-1]):
        if str(value).strip() != str(flag):
            continue
        nxt = str(argv[idx + 1] or "").strip()
        return nxt or None
    return None


def _count_nonempty_lines(path: Optional[Path]) -> Optional[int]:
    if not path or not path.exists() or not path.is_file():
        return None
    try:
        return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if str(line).strip())
    except Exception:
        return None


def _batch_progress(batch_dir: Path, reports_count: int, summary_exists: bool, *, season: int, game_types: str) -> Dict[str, Any]:
    lock_path = batch_dir / ".run_batch_eval_days.lock.json"
    lock_obj = _read_json_dict(lock_path)
    date_file = _resolve_path(_find_argv_value(lock_obj.get("argv"), "--date-file") or "")
    expected_reports = _count_nonempty_lines(date_file)
    if expected_reports is None:
        normalized_types = [part.strip().upper() for part in str(game_types or "").split(",") if part.strip()]
        phase_label = "regular" if normalized_types == ["R"] else "custom"
        fallback_date_file = _ROOT / "data" / "eval" / "date_sets" / f"season_{int(season)}_{phase_label}.txt"
        fallback_expected = _count_nonempty_lines(fallback_date_file)
        if fallback_expected is not None:
            expected_reports = int(fallback_expected)
            if date_file is None:
                date_file = fallback_date_file
    in_progress = bool(lock_obj) and lock_path.exists()
    partial = bool(in_progress or not summary_exists)
    if expected_reports is not None and reports_count < expected_reports:
        partial = True
    completion_ratio = None
    if expected_reports and expected_reports > 0:
        completion_ratio = float(reports_count) / float(expected_reports)
    return {
        "partial": bool(partial),
        "in_progress": bool(in_progress),
        "completed_reports": int(reports_count),
        "expected_reports": expected_reports,
        "completion_ratio": completion_ratio,
        "lock_path": _relative_path_str(lock_path if lock_path.exists() else None),
        "date_file": _relative_path_str(date_file),
    }


def _cards_artifacts_available(date_str: str) -> bool:
    slug = str(date_str or "").strip().replace("-", "_")
    if not slug:
        return False
    data_dir = _ROOT / "data"
    direct_candidates = [
        data_dir / "_tmp_live_subcap_random_day" / f"daily_summary_{slug}_profile_bundle.json",
        data_dir / "_tmp_live_subcap_smoke" / f"daily_summary_{slug}_profile_bundle.json",
        data_dir / "daily" / f"daily_summary_{slug}_profile_bundle.json",
        data_dir / "_tmp_live_subcap_random_day" / f"daily_summary_{slug}_locked_policy.json",
        data_dir / "_tmp_live_subcap_smoke" / f"daily_summary_{slug}_locked_policy.json",
        data_dir / f"daily_summary_{slug}_locked_policy.json",
        data_dir / "daily" / f"daily_summary_{slug}.json",
        data_dir / "daily" / "sims" / str(date_str),
        data_dir / "daily" / "snapshots" / str(date_str),
    ]
    return any(path.exists() for path in direct_candidates)


def _season_betting_manifest_candidates(season: int) -> Dict[str, List[Path]]:
    season_dir = _ROOT / "data" / "eval" / "seasons" / str(int(season))
    return {
        "baseline": [season_dir / "season_betting_cards_manifest.json"],
        "retuned": [season_dir / "season_betting_cards_retuned_manifest.json"],
    }


def _betting_counts_with_defaults(counts: Any) -> Dict[str, int]:
    out: Dict[str, int] = {
        key: 0
        for key in (
            "totals",
            "ml",
            "pitcher_props",
            "hitter_props",
            "hitter_home_runs",
            "hitter_hits",
            "hitter_total_bases",
            "hitter_runs",
            "hitter_rbis",
            "combined",
        )
    }
    if isinstance(counts, dict):
        for key in out:
            out[key] = int(counts.get(key) or 0)
    return out


def _playable_counts_from_card(card_obj: Dict[str, Any]) -> Dict[str, int]:
    counts = _betting_counts_with_defaults({})
    markets = (card_obj.get("markets") or {}) if isinstance(card_obj, dict) else {}
    if not isinstance(markets, dict):
        return counts

    hitter_market_names = {"hitter_home_runs", "hitter_hits", "hitter_total_bases", "hitter_runs", "hitter_rbis"}
    for market_name, market_info in markets.items():
        if not isinstance(market_info, dict):
            continue
        playable_n = int(len(market_info.get("other_playable_candidates") or []))
        key = str(market_name or "")
        if key not in counts:
            continue
        counts[key] += playable_n
        if key in hitter_market_names:
            counts["hitter_props"] += playable_n
        counts["combined"] += playable_n
    return counts


def _season_betting_coverage(season: int) -> Dict[str, Any]:
    manifests: Dict[str, Dict[str, Any]] = {}
    manifest_paths: Dict[str, str] = {}
    for profile_name, candidates in _season_betting_manifest_candidates(int(season)).items():
        for path in candidates:
            if not path.exists() or not path.is_file():
                continue
            manifest = _read_json_dict(path)
            if not manifest:
                continue
            manifests[str(profile_name)] = manifest
            manifest_paths[str(profile_name)] = _relative_path_str(path) or str(path)
            break

    default_profile = "retuned" if "retuned" in manifests else "baseline" if "baseline" in manifests else None
    by_date: Dict[str, Dict[str, Any]] = {}
    days_by_profile: Dict[str, int] = {}
    settled_days_by_profile: Dict[str, int] = {}
    unresolved_recommendations_by_profile: Dict[str, int] = {}
    cards_with_bets_by_profile: Dict[str, int] = {}
    days_with_playable_by_profile: Dict[str, int] = {}
    official_recommendations_by_profile: Dict[str, int] = {}
    playable_recommendations_by_profile: Dict[str, int] = {}

    for profile_name in ("baseline", "retuned"):
        manifest = manifests.get(profile_name)
        if not isinstance(manifest, dict):
            continue
        days = [row for row in (manifest.get("days") or []) if isinstance(row, dict)]
        days_by_profile[profile_name] = 0
        settled_days_by_profile[profile_name] = 0
        unresolved_recommendations_by_profile[profile_name] = int(((manifest.get("summary") or {}).get("unresolved_recommendations") or 0))
        cards_with_bets_by_profile[profile_name] = int((((manifest.get("summary") or {}).get("daily") or {}).get("cards_with_bets") or 0))
        days_with_playable_by_profile[profile_name] = 0
        official_recommendations_by_profile[profile_name] = 0
        playable_recommendations_by_profile[profile_name] = 0

        for row in days:
            date_str = str(row.get("date") or "").strip()
            if not date_str:
                continue
            available = bool(str(row.get("card_path") or "").strip())
            card_obj: Dict[str, Any] = {}
            card_path = _resolve_path(str(row.get("card_path") or ""))
            if card_path and card_path.exists() and card_path.is_file():
                card_obj = _read_json_dict(card_path)
            official_counts = _betting_counts_with_defaults(row.get("selected_counts") or {})
            playable_counts = _playable_counts_from_card(card_obj)
            if available:
                days_by_profile[profile_name] += 1
            if available and int(row.get("unresolved_n") or 0) <= 0:
                settled_days_by_profile[profile_name] += 1
            if playable_counts.get("combined", 0) > 0:
                days_with_playable_by_profile[profile_name] += 1
            official_recommendations_by_profile[profile_name] += int(official_counts.get("combined") or 0)
            playable_recommendations_by_profile[profile_name] += int(playable_counts.get("combined") or 0)

            entry = by_date.setdefault(
                date_str,
                {
                    "available": False,
                    "available_profiles": [],
                    "default_profile": default_profile,
                    "profiles": {},
                },
            )
            entry["profiles"][profile_name] = {
                "available": bool(available),
                "card_path": row.get("card_path"),
                "report_path": row.get("report_path"),
                "cap_profile": row.get("cap_profile"),
                "selected_counts": official_counts,
                "playable_counts": playable_counts,
                "settled_n": int(row.get("settled_n") or 0),
                "unresolved_n": int(row.get("unresolved_n") or 0),
                "profit_u": row.get("profit_u"),
                "roi": row.get("roi"),
            }
            if available:
                entry["available"] = True
                if profile_name not in entry["available_profiles"]:
                    entry["available_profiles"].append(profile_name)

    for entry in by_date.values():
        ordered = [profile for profile in ("baseline", "retuned") if profile in entry.get("available_profiles", [])]
        entry["available_profiles"] = ordered
        if entry.get("default_profile") not in ordered:
            entry["default_profile"] = ordered[0] if ordered else default_profile

    return {
        "days": by_date,
        "overview": {
            "available_profiles": [profile for profile in ("baseline", "retuned") if profile in manifests],
            "manifest_paths": manifest_paths,
            "default_profile": default_profile,
            "days_any": int(sum(1 for entry in by_date.values() if entry.get("available"))),
            "days_by_profile": days_by_profile,
            "settled_days_by_profile": settled_days_by_profile,
            "unresolved_recommendations_by_profile": unresolved_recommendations_by_profile,
            "cards_with_bets_by_profile": cards_with_bets_by_profile,
            "days_with_playable_by_profile": days_with_playable_by_profile,
            "official_recommendations_by_profile": official_recommendations_by_profile,
            "playable_recommendations_by_profile": playable_recommendations_by_profile,
        },
    }


def _empty_metric_groups() -> Dict[str, Dict[str, Tuple[float, float]]]:
    return {
        "segments_full": {},
        "segments_first5": {},
        "segments_first3": {},
        "totals": {},
        "moneyline": {},
        "runline": {},
        "pitcher_starters": {},
        "market_so": {},
        "market_outs": {},
        "hitter_hr": {},
        "hitter_props": {},
    }


def _report_hr_calibration_cfg(report_obj: Dict[str, Any]) -> Dict[str, Any]:
    for source in (report_obj.get("meta"), report_obj):
        if not isinstance(source, dict):
            continue
        cfg = source.get("hitter_hr_prob_calibration")
        if isinstance(cfg, dict) and cfg:
            return cfg
    return {}


def _collect_actual_counts(scored_rows: Any, target: Dict[int, Dict[str, int]], field: str) -> None:
    if not isinstance(scored_rows, list):
        return
    for row in scored_rows:
        if not isinstance(row, dict):
            continue
        batter_id = _safe_int(row.get("batter_id"))
        actual = _safe_int(row.get("actual"))
        if not batter_id or actual is None:
            continue
        target.setdefault(int(batter_id), {})[str(field)] = int(actual)


def _infer_actual_hr_count(actuals: Dict[str, int]) -> Optional[int]:
    hits = _safe_int(actuals.get("hits"))
    doubles = _safe_int(actuals.get("doubles"))
    triples = _safe_int(actuals.get("triples"))
    total_bases = _safe_int(actuals.get("total_bases"))
    if hits is None or doubles is None or triples is None or total_bases is None:
        return None
    numerator = int(total_bases) - int(hits) - int(doubles) - (2 * int(triples))
    if numerator < 0 or (numerator % 3) != 0:
        return None
    return int(numerator // 3)


def _fallback_hitter_hr_metrics(report_obj: Dict[str, Any]) -> Dict[str, Any]:
    games = report_obj.get("games") or []
    if not isinstance(games, list) or not games:
        return {}

    hr_calibration = _report_hr_calibration_cfg(report_obj)
    briers: List[float] = []
    loglosses: List[float] = []
    ps: List[float] = []
    ys: List[int] = []
    top_n_out = 0

    for game in games:
        if not isinstance(game, dict):
            continue
        hitter_props_likelihood = game.get("hitter_props_likelihood") or {}
        hitter_props_backtest = game.get("hitter_props_backtest") or {}
        if not isinstance(hitter_props_likelihood, dict) or not isinstance(hitter_props_backtest, dict):
            continue

        per_game_top_n = int(_safe_int(hitter_props_likelihood.get("top_n")) or 0)
        if per_game_top_n > 0:
            top_n_out = max(top_n_out, per_game_top_n)

        candidates: Dict[int, Dict[str, Any]] = {}
        for prop_key, rows in hitter_props_likelihood.items():
            if str(prop_key) == "top_n" or not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                batter_id = _safe_int(row.get("batter_id"))
                p_hr = _safe_float(row.get("p_hr_1plus"))
                if not batter_id or p_hr is None:
                    continue
                existing = candidates.get(int(batter_id))
                if existing is None or float(p_hr) > float(existing.get("p_hr_1plus") or 0.0):
                    candidates[int(batter_id)] = {
                        "batter_id": int(batter_id),
                        "name": str(row.get("name") or ""),
                        "p_hr_1plus": float(p_hr),
                    }

        actual_counts: Dict[int, Dict[str, int]] = {}
        for prop_key, field in (
            ("hits_1plus", "hits"),
            ("doubles_1plus", "doubles"),
            ("triples_1plus", "triples"),
            ("total_bases_1plus", "total_bases"),
        ):
            block = hitter_props_backtest.get(prop_key) or {}
            if isinstance(block, dict):
                _collect_actual_counts(block.get("scored"), actual_counts, field)

        selected = sorted(candidates.values(), key=lambda row: float(row.get("p_hr_1plus") or 0.0), reverse=True)
        if per_game_top_n > 0:
            selected = selected[:per_game_top_n]
        elif selected:
            top_n_out = max(top_n_out, len(selected))

        for row in selected:
            batter_id = int(row.get("batter_id") or 0)
            hr_count = _infer_actual_hr_count(actual_counts.get(batter_id) or {})
            if hr_count is None:
                continue
            p_raw = float(row.get("p_hr_1plus") or 0.0)
            p_cal = float(apply_prop_prob_calibration(p_raw, hr_calibration, prop_key="hr_1plus"))
            y = 1 if int(hr_count) >= 1 else 0
            briers.append(_brier(p_cal, y))
            loglosses.append(_logloss(p_cal, y))
            ps.append(float(p_cal))
            ys.append(int(y))

    if not ps:
        return {}

    n_rows = len(ps)
    return {
        "top_n": int(top_n_out),
        "n": int(n_rows),
        "brier": float(sum(briers) / n_rows),
        "logloss": float(sum(loglosses) / n_rows),
        "avg_p": float(sum(ps) / n_rows),
        "emp_rate": float(sum(float(y) for y in ys) / n_rows),
    }


def _accumulate_report_metrics(
    *,
    target: Dict[str, Dict[str, Tuple[float, float]]],
    report_obj: Dict[str, Any],
) -> Dict[str, int]:
    assessment = ((report_obj.get("assessment") or {}).get("full_game") or {})
    aggregate = report_obj.get("aggregate") or {}
    games_arr = report_obj.get("games") or []

    agg_full = aggregate.get("full") or {}
    agg_first5 = aggregate.get("first5") or {}
    agg_first3 = aggregate.get("first3") or {}
    totals = assessment.get("totals") or {}
    moneyline = assessment.get("moneyline") or {}
    runline = assessment.get("ats_runline_fav_minus_1_5") or {}
    starter_props = assessment.get("pitcher_props_starters") or {}
    market_props = assessment.get("pitcher_props_at_market_lines") or {}
    so_market = market_props.get("strikeouts") or {}
    outs_market = market_props.get("outs") or {}
    hitter_hr = assessment.get("hitter_hr_likelihood_topn") or {}
    hitter_props = assessment.get("hitter_props_likelihood_topn") or {}

    games_weight = int(
        _safe_int(totals.get("games"))
        or _safe_int(agg_full.get("games"))
        or (len(games_arr) if isinstance(games_arr, list) else 0)
    )
    starter_weight = int(_safe_int(starter_props.get("starters")) or 0)
    so_weight = int(_safe_int(so_market.get("n")) or 0)
    outs_weight = int(_safe_int(outs_market.get("n")) or 0)
    hr_weight = int(_safe_int(hitter_hr.get("n")) or 0)
    if hr_weight <= 0:
        hitter_hr = _fallback_hitter_hr_metrics(report_obj)
        hr_weight = int(_safe_int(hitter_hr.get("n")) or 0)

    if games_weight > 0:
        for out_key, source_key in (
            ("brier_home_win", "brier_home_win"),
            ("mae_total_runs", "mae_total_runs"),
            ("mae_run_margin", "mae_run_margin"),
        ):
            value = _safe_float(agg_full.get(source_key))
            if value is not None:
                _acc_wavg(target["segments_full"], out_key, value, games_weight)
        for out_key, source_key in (
            ("brier_home_win", "brier_home_win"),
            ("mae_total_runs", "mae_total_runs"),
            ("mae_run_margin", "mae_run_margin"),
        ):
            value = _safe_float(agg_first5.get(source_key))
            if value is not None:
                _acc_wavg(target["segments_first5"], out_key, value, games_weight)
        for out_key, source_key in (
            ("brier_home_win", "brier_home_win"),
            ("mae_total_runs", "mae_total_runs"),
            ("mae_run_margin", "mae_run_margin"),
        ):
            value = _safe_float(agg_first3.get(source_key))
            if value is not None:
                _acc_wavg(target["segments_first3"], out_key, value, games_weight)

        for out_key, source_key in (
            ("mae", "mae"),
            ("rmse", "rmse"),
            ("avg_nll_exact_total", "avg_nll_exact_total"),
        ):
            value = _safe_float(totals.get(source_key))
            if value is not None:
                _acc_wavg(target["totals"], out_key, value, games_weight)

        for out_key, source_key in (
            ("brier", "brier"),
            ("logloss", "logloss"),
            ("accuracy", "accuracy"),
        ):
            value = _safe_float(moneyline.get(source_key))
            if value is not None:
                _acc_wavg(target["moneyline"], out_key, value, games_weight)

        for out_key, source_key in (
            ("brier", "brier"),
            ("logloss", "logloss"),
            ("accuracy", "accuracy"),
        ):
            value = _safe_float(runline.get(source_key))
            if value is not None:
                _acc_wavg(target["runline"], out_key, value, games_weight)

    if starter_weight > 0:
        for out_key, source_key in (
            ("so_mae", "so_mae"),
            ("so_rmse", "so_rmse"),
            ("outs_mae", "outs_mae"),
            ("outs_rmse", "outs_rmse"),
        ):
            value = _safe_float(starter_props.get(source_key))
            if value is not None:
                _acc_wavg(target["pitcher_starters"], out_key, value, starter_weight)

    if so_weight > 0:
        for out_key, source_key in (
            ("brier", "brier"),
            ("logloss", "logloss"),
            ("accuracy", "accuracy"),
            ("avg_edge_vs_no_vig", "avg_edge_vs_no_vig"),
        ):
            value = _safe_float(so_market.get(source_key))
            if value is not None:
                _acc_wavg(target["market_so"], out_key, value, so_weight)

    if outs_weight > 0:
        for out_key, source_key in (
            ("brier", "brier"),
            ("logloss", "logloss"),
            ("accuracy", "accuracy"),
            ("avg_edge_vs_no_vig", "avg_edge_vs_no_vig"),
        ):
            value = _safe_float(outs_market.get(source_key))
            if value is not None:
                _acc_wavg(target["market_outs"], out_key, value, outs_weight)

    if hr_weight > 0:
        for out_key, source_key in (
            ("hr_brier", "brier"),
            ("hr_logloss", "logloss"),
            ("hr_avg_p", "avg_p"),
            ("hr_emp_rate", "emp_rate"),
        ):
            value = _safe_float(hitter_hr.get(source_key))
            if value is not None:
                _acc_wavg(target["hitter_hr"], out_key, value, hr_weight)

    if isinstance(hitter_props, dict) and hitter_props:
        for prop_key, block in hitter_props.items():
            if not isinstance(block, dict):
                continue
            prop_weight = int(_safe_int(block.get("n")) or 0)
            if prop_weight <= 0:
                continue
            for metric_key in ("brier", "logloss", "avg_p", "emp_rate"):
                value = _safe_float(block.get(metric_key))
                if value is None:
                    continue
                _acc_wavg(target["hitter_props"], f"{str(prop_key)}_{metric_key}", value, prop_weight)

    return {
        "games": games_weight,
        "starters": starter_weight,
        "market_so": so_weight,
        "market_outs": outs_weight,
    }


def _build_recap(target: Dict[str, Dict[str, Tuple[float, float]]], summary_obj: Dict[str, Any]) -> Dict[str, Any]:
    hitter_hr = _finalize_wavg(target["hitter_hr"])
    hitter_props = _finalize_wavg(target["hitter_props"])
    return {
        "segments": {
            "full": _finalize_wavg(target["segments_full"]),
            "first5": _finalize_wavg(target["segments_first5"]),
            "first3": _finalize_wavg(target["segments_first3"]),
        },
        "full_game": {
            "totals": _finalize_wavg(target["totals"]),
            "moneyline": _finalize_wavg(target["moneyline"]),
            "runline_fav_minus_1_5": _finalize_wavg(target["runline"]),
            "pitcher_props_starters": _finalize_wavg(target["pitcher_starters"]),
            "pitcher_props_at_market_lines": {
                "strikeouts": _finalize_wavg(target["market_so"]),
                "outs": _finalize_wavg(target["market_outs"]),
            },
        },
        "starter_sources": (summary_obj.get("starter_sources") or {}),
        "hitter_hr_likelihood_topn": hitter_hr if hitter_hr else (summary_obj.get("hitter_hr_likelihood_topn_weighted") or {}),
        "hitter_props_likelihood_topn": hitter_props if hitter_props else (summary_obj.get("hitter_props_likelihood_topn_weighted") or {}),
    }


def _leader_entry(days: List[Dict[str, Any]], *, section: str, metric: str, reverse: bool = False) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    for day in days:
        block = (day.get(section) or {}) if isinstance(day, dict) else {}
        value = _safe_float(block.get(metric))
        if value is None:
            continue
        candidates.append((value, day))
    if not candidates:
        return None
    value, day = sorted(candidates, key=lambda item: item[0], reverse=bool(reverse))[0]
    return {
        "date": day.get("date"),
        "value": value,
        "games": _safe_int(day.get("games")),
    }


def _render_recap_markdown(manifest: Dict[str, Any]) -> str:
    meta = manifest.get("meta") or {}
    overview = manifest.get("overview") or {}
    betting_overview = overview.get("betting_cards") or {}
    progress = meta.get("progress") or {}
    recap = manifest.get("recap") or {}
    full_game = recap.get("full_game") or {}
    segments = recap.get("segments") or {}
    totals = full_game.get("totals") or {}
    moneyline = full_game.get("moneyline") or {}
    runline = full_game.get("runline_fav_minus_1_5") or {}
    starters = full_game.get("pitcher_props_starters") or {}
    market_props = full_game.get("pitcher_props_at_market_lines") or {}
    hitter_hr = recap.get("hitter_hr_likelihood_topn") or {}
    leaders = manifest.get("leaders") or {}
    months = manifest.get("months") or []

    lines: List[str] = []
    lines.append(f"# MLB Season Eval {meta.get('season')}")
    lines.append("")
    lines.append(f"- Generated: {meta.get('generated_at')}")
    lines.append(f"- Batch: {meta.get('batch_dir')}")
    if meta.get("partial"):
        lines.append(
            f"- Status: partial ({progress.get('completed_reports')} of {progress.get('expected_reports') or '?'} reports published)"
        )
    lines.append(
        f"- Days: {overview.get('days')} | Games: {overview.get('total_games')} | "
        f"Season card days: {betting_overview.get('days_any', 'n/a')} | "
        f"Legacy cards-page days: {overview.get('legacy_cards_page_days', overview.get('cards_available_days'))}"
    )
    lines.append("")
    lines.append("## Overall recap")
    lines.append("")
    lines.append(f"- Full-game moneyline: brier {moneyline.get('brier', 'n/a')}, logloss {moneyline.get('logloss', 'n/a')}, accuracy {moneyline.get('accuracy', 'n/a')}")
    lines.append(f"- Totals: mae {totals.get('mae', 'n/a')}, rmse {totals.get('rmse', 'n/a')}, avg exact-total nll {totals.get('avg_nll_exact_total', 'n/a')}")
    lines.append(f"- Runline (-1.5 fav): brier {runline.get('brier', 'n/a')}, accuracy {runline.get('accuracy', 'n/a')}")
    lines.append(f"- Starter props: SO mae {starters.get('so_mae', 'n/a')}, Outs mae {starters.get('outs_mae', 'n/a')}")
    so_market = (market_props.get('strikeouts') or {})
    outs_market = (market_props.get('outs') or {})
    if so_market:
        lines.append(f"- Pitcher market strikeouts: brier {so_market.get('brier', 'n/a')}, accuracy {so_market.get('accuracy', 'n/a')}, avg edge vs no-vig {so_market.get('avg_edge_vs_no_vig', 'n/a')}")
    if outs_market:
        lines.append(f"- Pitcher market outs: brier {outs_market.get('brier', 'n/a')}, accuracy {outs_market.get('accuracy', 'n/a')}, avg edge vs no-vig {outs_market.get('avg_edge_vs_no_vig', 'n/a')}")
    if hitter_hr:
        lines.append(f"- Hitter HR 1+: brier {hitter_hr.get('hr_brier', 'n/a')}, logloss {hitter_hr.get('hr_logloss', 'n/a')}, avg p {hitter_hr.get('hr_avg_p', 'n/a')}, emp rate {hitter_hr.get('hr_emp_rate', 'n/a')}")
    lines.append(f"- Segment error: full total mae {((segments.get('full') or {}).get('mae_total_runs'))}, first5 total mae {((segments.get('first5') or {}).get('mae_total_runs'))}, first3 total mae {((segments.get('first3') or {}).get('mae_total_runs'))}")
    lines.append("")
    lines.append("## Day leaders")
    lines.append("")
    best_ml = leaders.get("best_moneyline_brier_day") or {}
    worst_total = leaders.get("worst_total_mae_day") or {}
    if best_ml:
        lines.append(f"- Best moneyline brier day: {best_ml.get('date')} ({best_ml.get('value')})")
    if worst_total:
        lines.append(f"- Worst totals mae day: {worst_total.get('date')} ({worst_total.get('value')})")
    lines.append("")
    lines.append("## By month")
    lines.append("")
    lines.append("| Month | Days | Games | ML Brier | Total MAE | SO MAE | Outs MAE |")
    lines.append("|---|---|---|---|---|---|---|")
    for month in months:
        month_full_game = (month.get("full_game") or {})
        month_moneyline = (month_full_game.get("moneyline") or {})
        month_totals = (month_full_game.get("totals") or {})
        month_starters = (month_full_game.get("pitcher_props_starters") or {})
        lines.append(
            "| {label} | {days} | {games} | {ml_brier} | {total_mae} | {so_mae} | {outs_mae} |".format(
                label=month.get("label") or month.get("month") or "",
                days=month.get("days") or 0,
                games=month.get("games") or 0,
                ml_brier=month_moneyline.get("brier") or "",
                total_mae=month_totals.get("mae") or "",
                so_mae=month_starters.get("so_mae") or "",
                outs_mae=month_starters.get("outs_mae") or "",
            )
        )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def build_manifest(*, season: int, batch_dir: Path, title: str = "", game_types: str = "R") -> Dict[str, Any]:
    if not batch_dir.exists() or not batch_dir.is_dir():
        raise SystemExit(f"Batch dir not found: {batch_dir}")

    reports = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    if not reports:
        raise SystemExit(f"No sim_vs_actual_*.json reports found under: {batch_dir}")

    summary_path = batch_dir / "summary.json"
    summary_obj = _read_json_dict(summary_path)

    batch_meta_path = batch_dir / "batch_meta.json"
    batch_meta_obj = _read_json_dict(batch_meta_path)
    progress = _batch_progress(
        batch_dir,
        len(reports),
        summary_path.exists() and summary_path.is_file(),
        season=int(season),
        game_types=str(game_types),
    )

    overall_acc = _empty_metric_groups()
    months_raw: Dict[str, Dict[str, Any]] = {}
    days: List[Dict[str, Any]] = []
    total_games = 0
    cards_available_days = 0
    betting_coverage = _season_betting_coverage(int(season))
    betting_days = betting_coverage.get("days") or {}
    betting_overview = betting_coverage.get("overview") or {}

    for report_path in reports:
        report_obj = _read_json(report_path)
        if not isinstance(report_obj, dict):
            continue
        meta = report_obj.get("meta") or {}
        date_str = str(meta.get("date") or report_path.stem.replace("sim_vs_actual_", "")).strip()
        month_key = date_str[:7]

        weights = _accumulate_report_metrics(target=overall_acc, report_obj=report_obj)
        total_games += int(weights.get("games") or 0)

        month_bucket = months_raw.setdefault(
            month_key,
            {
                "month": month_key,
                "label": _month_label(month_key),
                "days": 0,
                "games": 0,
                "acc": _empty_metric_groups(),
            },
        )
        month_bucket["days"] = int(month_bucket.get("days") or 0) + 1
        month_bucket["games"] = int(month_bucket.get("games") or 0) + int(weights.get("games") or 0)
        _accumulate_report_metrics(target=month_bucket["acc"], report_obj=report_obj)

        assessment = ((report_obj.get("assessment") or {}).get("full_game") or {})
        aggregate = report_obj.get("aggregate") or {}
        totals = assessment.get("totals") or {}
        moneyline = assessment.get("moneyline") or {}
        runline = assessment.get("ats_runline_fav_minus_1_5") or {}
        starter_props = assessment.get("pitcher_props_starters") or {}
        market_props = assessment.get("pitcher_props_at_market_lines") or {}
        legacy_cards_available = _cards_artifacts_available(date_str)
        if legacy_cards_available:
            cards_available_days += 1
        day_betting = betting_days.get(date_str) if isinstance(betting_days, dict) else None

        days.append(
            {
                "date": date_str,
                "month": month_key,
                "games": int(weights.get("games") or 0),
                "cards_available": bool(legacy_cards_available),
                "cards_url": f"/?date={date_str}" if legacy_cards_available else None,
                "legacy_cards_available": bool(legacy_cards_available),
                "legacy_cards_url": f"/?date={date_str}" if legacy_cards_available else None,
                "betting_cards": day_betting
                if isinstance(day_betting, dict)
                else {
                    "available": False,
                    "available_profiles": [],
                    "default_profile": betting_overview.get("default_profile"),
                    "profiles": {},
                },
                "report_path": _relative_path_str(report_path),
                "aggregate": {
                    "full": aggregate.get("full") or {},
                    "first5": aggregate.get("first5") or {},
                    "first3": aggregate.get("first3") or {},
                },
                "full_game": {
                    "totals": totals,
                    "moneyline": moneyline,
                    "runline_fav_minus_1_5": runline,
                    "pitcher_props_starters": starter_props,
                    "pitcher_props_at_market_lines": {
                        "lines_meta": (market_props.get("lines_meta") or {}),
                        "strikeouts": (market_props.get("strikeouts") or {}),
                        "outs": (market_props.get("outs") or {}),
                    },
                },
            }
        )

    days.sort(key=lambda row: str(row.get("date") or ""))

    months: List[Dict[str, Any]] = []
    for month_key in sorted(months_raw):
        bucket = months_raw[month_key]
        months.append(
            {
                "month": month_key,
                "label": bucket.get("label") or _month_label(month_key),
                "days": int(bucket.get("days") or 0),
                "games": int(bucket.get("games") or 0),
                "segments": {
                    "full": _finalize_wavg(bucket["acc"]["segments_full"]),
                    "first5": _finalize_wavg(bucket["acc"]["segments_first5"]),
                    "first3": _finalize_wavg(bucket["acc"]["segments_first3"]),
                },
                "full_game": {
                    "totals": _finalize_wavg(bucket["acc"]["totals"]),
                    "moneyline": _finalize_wavg(bucket["acc"]["moneyline"]),
                    "runline_fav_minus_1_5": _finalize_wavg(bucket["acc"]["runline"]),
                    "pitcher_props_starters": _finalize_wavg(bucket["acc"]["pitcher_starters"]),
                    "pitcher_props_at_market_lines": {
                        "strikeouts": _finalize_wavg(bucket["acc"]["market_so"]),
                        "outs": _finalize_wavg(bucket["acc"]["market_outs"]),
                    },
                },
                "hitter_hr_likelihood_topn": _finalize_wavg(bucket["acc"]["hitter_hr"]),
                "hitter_props_likelihood_topn": _finalize_wavg(bucket["acc"]["hitter_props"]),
            }
        )

    recap = _build_recap(overall_acc, summary_obj)
    first_date = days[0]["date"] if days else None
    last_date = days[-1]["date"] if days else None
    leaders = {
        "best_moneyline_brier_day": _leader_entry(
            [
                {
                    "date": day.get("date"),
                    "games": day.get("games"),
                    "metrics": ((day.get("full_game") or {}).get("moneyline") or {}),
                }
                for day in days
            ],
            section="metrics",
            metric="brier",
            reverse=False,
        ),
        "worst_total_mae_day": _leader_entry(
            [
                {
                    "date": day.get("date"),
                    "games": day.get("games"),
                    "metrics": ((day.get("full_game") or {}).get("totals") or {}),
                }
                for day in days
            ],
            section="metrics",
            metric="mae",
            reverse=True,
        ),
    }

    manifest = {
        "meta": {
            "season": int(season),
            "title": str(title or f"MLB {int(season)} Season Eval").strip(),
            "generated_at": datetime.now().isoformat(),
            "batch_dir": _relative_path_str(batch_dir),
            "game_types": [part.strip().upper() for part in str(game_types or "").split(",") if part.strip()],
            "status": "partial" if progress.get("partial") else "complete",
            "partial": bool(progress.get("partial")),
            "progress": progress,
            "sources": {
                "batch_meta": _relative_path_str(batch_meta_path if batch_meta_path.exists() else None),
                "summary": _relative_path_str(summary_path if summary_path.exists() else None),
            },
            "batch_meta": {
                "sims_per_game": _safe_int(batch_meta_obj.get("sims_per_game")),
                "jobs": _safe_int(batch_meta_obj.get("jobs")),
                "prop_lines_source": batch_meta_obj.get("prop_lines_source"),
                "use_raw": batch_meta_obj.get("use_raw"),
            },
        },
        "overview": {
            "days": int(len(days)),
            "reports": int(len(reports)),
            "completed_reports": int(progress.get("completed_reports") or len(reports)),
            "expected_reports": _safe_int(progress.get("expected_reports")),
            "completion_ratio": _safe_float(progress.get("completion_ratio")),
            "total_games": int(total_games),
            "cards_available_days": int(cards_available_days),
            "legacy_cards_page_days": int(cards_available_days),
            "betting_cards": betting_overview,
            "first_date": first_date,
            "last_date": last_date,
        },
        "recap": recap,
        "leaders": leaders,
        "months": months,
        "days": days,
    }

    return manifest


def write_manifest_artifacts(
    manifest: Dict[str, Any],
    *,
    season: int,
    out: str = "",
    recap_md: str = "",
) -> Tuple[Path, Path]:
    out_path = Path(str(out).strip()) if str(out).strip() else (_ROOT / "data" / "eval" / "seasons" / str(int(season)) / "season_eval_manifest.json")
    if not out_path.is_absolute():
        out_path = (_ROOT / out_path).resolve()
    _write_json(out_path, manifest)

    recap_md_path = Path(str(recap_md).strip()) if str(recap_md).strip() else out_path.with_name("season_eval_recap.md")
    if not recap_md_path.is_absolute():
        recap_md_path = (_ROOT / recap_md_path).resolve()
    _write_text(recap_md_path, _render_recap_markdown(manifest))
    return out_path, recap_md_path


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a frontend season manifest from a batch eval folder")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--batch-dir", required=True, help="Path to data/eval/batches/<batch>")
    ap.add_argument("--out", default="", help="Output JSON path")
    ap.add_argument("--recap-md", default="", help="Optional markdown recap output path")
    ap.add_argument("--title", default="", help="Optional title shown in the frontend manifest")
    ap.add_argument("--game-types", default="R", help="Comma-separated schedule game types represented by this manifest")
    args = ap.parse_args()

    batch_dir = Path(str(args.batch_dir))
    if not batch_dir.is_absolute():
        batch_dir = (_ROOT / batch_dir).resolve()

    manifest = build_manifest(
        season=int(args.season),
        batch_dir=batch_dir,
        title=str(args.title),
        game_types=str(args.game_types),
    )

    out_path, recap_md_path = write_manifest_artifacts(
        manifest,
        season=int(args.season),
        out=str(args.out),
        recap_md=str(args.recap_md),
    )
    print(f"Wrote manifest: {out_path}")
    print(f"Wrote recap: {recap_md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())