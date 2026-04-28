from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from tools.tune.export_live_pitcher_projection_dataset import _collect_projection_rows, _maybe_sync_render_history


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return float(number)


def _gaussian_solve(matrix: List[List[float]], vector: List[float]) -> Optional[List[float]]:
    n = len(vector)
    aug = [list(matrix[i]) + [float(vector[i])] for i in range(n)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(aug[row][col]))
        if abs(aug[pivot][col]) < 1e-12:
            return None
        if pivot != col:
            aug[col], aug[pivot] = aug[pivot], aug[col]
        pivot_val = aug[col][col]
        for j in range(col, n + 1):
            aug[col][j] /= pivot_val
        for row in range(n):
            if row == col:
                continue
            factor = aug[row][col]
            if factor == 0.0:
                continue
            for j in range(col, n + 1):
                aug[row][j] -= factor * aug[col][j]
    return [float(aug[i][n]) for i in range(n)]


def _feature_value(row: Dict[str, Any], name: str) -> float:
    if name == "game_state_parsed_flag":
        return 1.0 if bool(row.get("game_state_parsed")) else 0.0
    return float(_safe_float(row.get(name)) or 0.0)


def _standardize_rows(rows: Sequence[Dict[str, Any]], feature_names: Sequence[str]) -> Tuple[List[List[float]], Dict[str, float], Dict[str, float]]:
    centers: Dict[str, float] = {}
    scales: Dict[str, float] = {}
    matrix: List[List[float]] = []
    for name in feature_names:
        values = [_feature_value(row, name) for row in rows]
        mean = float(sum(values) / len(values)) if values else 0.0
        variance = float(sum((value - mean) ** 2 for value in values) / len(values)) if values else 0.0
        std = float(math.sqrt(max(variance, 1e-12)))
        centers[str(name)] = mean
        scales[str(name)] = std if std > 1e-6 else 1.0
    for row in rows:
        vector: List[float] = []
        for name in feature_names:
            raw = _feature_value(row, name)
            vector.append((raw - centers[str(name)]) / scales[str(name)])
        matrix.append(vector)
    return matrix, centers, scales


def _fit_ridge_error_model(
    rows: Sequence[Dict[str, Any]],
    feature_names: Sequence[str],
    *,
    l2: float = 1.0,
) -> Dict[str, Any]:
    matrix, centers, scales = _standardize_rows(rows, feature_names)
    targets = [float((_safe_float(row.get("final_actual")) or 0.0) - (_safe_float(row.get("live_projection")) or 0.0)) for row in rows]
    n_features = len(feature_names)
    gram = [[0.0 for _ in range(n_features + 1)] for _ in range(n_features + 1)]
    rhs = [0.0 for _ in range(n_features + 1)]
    for vector, target in zip(matrix, targets):
        design = [1.0] + list(vector)
        for i in range(n_features + 1):
            rhs[i] += design[i] * target
            for j in range(n_features + 1):
                gram[i][j] += design[i] * design[j]
    for idx in range(1, n_features + 1):
        gram[idx][idx] += float(l2)
    solution = _gaussian_solve(gram, rhs)
    if solution is None:
        raise RuntimeError("unable to solve ridge system for live pitcher outs correction")
    intercept = float(solution[0])
    weights = [float(value) for value in solution[1:]]
    return {
        "intercept": intercept,
        "weights": {str(name): float(weight) for name, weight in zip(feature_names, weights)},
        "feature_centers": centers,
        "feature_scales": scales,
    }


def _predict_error(row: Dict[str, Any], model: Dict[str, Any], feature_names: Sequence[str]) -> float:
    intercept = float(model.get("intercept") or 0.0)
    centers = dict(model.get("feature_centers") or {})
    scales = dict(model.get("feature_scales") or {})
    weights = dict(model.get("weights") or {})
    total = intercept
    for name in feature_names:
        raw = _feature_value(row, name)
        center = float(centers.get(str(name)) or 0.0)
        scale = float(scales.get(str(name)) or 1.0) or 1.0
        total += ((raw - center) / scale) * float(weights.get(str(name)) or 0.0)
    return float(total)


def _mae(pairs: Sequence[Tuple[float, float]]) -> Optional[float]:
    if not pairs:
        return None
    return float(sum(abs(actual - pred) for actual, pred in pairs) / len(pairs))


def _rmse(pairs: Sequence[Tuple[float, float]]) -> Optional[float]:
    if not pairs:
        return None
    return float(math.sqrt(sum((actual - pred) ** 2 for actual, pred in pairs) / len(pairs)))


def _evaluate(rows: Sequence[Dict[str, Any]], model: Dict[str, Any], feature_names: Sequence[str]) -> Dict[str, Optional[float]]:
    baseline_pairs: List[Tuple[float, float]] = []
    corrected_pairs: List[Tuple[float, float]] = []
    for row in rows:
        actual = _safe_float(row.get("final_actual"))
        baseline = _safe_float(row.get("live_projection"))
        if actual is None or baseline is None:
            continue
        corrected = baseline + _predict_error(row, model, feature_names)
        baseline_pairs.append((actual, baseline))
        corrected_pairs.append((actual, corrected))
    return {
        "baseline_mae": round(_mae(baseline_pairs), 3) if baseline_pairs else None,
        "baseline_rmse": round(_rmse(baseline_pairs), 3) if baseline_pairs else None,
        "corrected_mae": round(_mae(corrected_pairs), 3) if corrected_pairs else None,
        "corrected_rmse": round(_rmse(corrected_pairs), 3) if corrected_pairs else None,
    }


def _leave_one_out(rows: Sequence[Dict[str, Any]], feature_names: Sequence[str], *, l2: float) -> Dict[str, Optional[float]]:
    if len(rows) < 3:
        return {"baseline_mae": None, "baseline_rmse": None, "corrected_mae": None, "corrected_rmse": None}
    baseline_pairs: List[Tuple[float, float]] = []
    corrected_pairs: List[Tuple[float, float]] = []
    for index, held_out in enumerate(rows):
        train_rows = [row for idx, row in enumerate(rows) if idx != index]
        model = _fit_ridge_error_model(train_rows, feature_names, l2=l2)
        actual = _safe_float(held_out.get("final_actual"))
        baseline = _safe_float(held_out.get("live_projection"))
        if actual is None or baseline is None:
            continue
        corrected = baseline + _predict_error(held_out, model, feature_names)
        baseline_pairs.append((actual, baseline))
        corrected_pairs.append((actual, corrected))
    return {
        "baseline_mae": round(_mae(baseline_pairs), 3) if baseline_pairs else None,
        "baseline_rmse": round(_rmse(baseline_pairs), 3) if baseline_pairs else None,
        "corrected_mae": round(_mae(corrected_pairs), 3) if corrected_pairs else None,
        "corrected_rmse": round(_rmse(corrected_pairs), 3) if corrected_pairs else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fit a simple Render-backed live pitcher outs correction layer.")
    parser.add_argument("--live-lens-dir", default="data/live_lens")
    parser.add_argument("--source", choices=("render-sync", "local", "both"), default="render-sync")
    parser.add_argument("--min-date", default="")
    parser.add_argument("--max-date", default="")
    parser.add_argument("--exclude-today", action="store_true")
    parser.add_argument("--sync-render", choices=("on", "off"), default="on")
    parser.add_argument("--render-base-url", default="")
    parser.add_argument("--render-cron-token", default="")
    parser.add_argument("--render-timeout-seconds", type=int, default=45)
    parser.add_argument("--l2", type=float, default=1.0)
    parser.add_argument("--out", default="data/eval/live_pitcher_outs_correction.json")
    args = parser.parse_args()

    live_lens_dir = Path(str(args.live_lens_dir)).resolve()
    max_date = str(args.max_date or "").strip()
    if bool(args.exclude_today) and not max_date:
        max_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()

    sync_summary = _maybe_sync_render_history(
        live_lens_dir,
        source=str(args.source or "render-sync"),
        sync_render=str(args.sync_render or "on") == "on",
        min_date=str(args.min_date or ""),
        max_date=max_date,
        render_base_url=str(args.render_base_url or ""),
        render_cron_token=str(args.render_cron_token or ""),
        render_timeout_seconds=int(args.render_timeout_seconds or 45),
        require_archive=True,
    )

    rows = _collect_projection_rows(
        live_lens_dir,
        {"outs"},
        source=str(args.source or "render-sync"),
        include_pregame=False,
    )
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        date_text = str(row.get("date") or "").strip()
        if str(args.min_date or "").strip() and date_text < str(args.min_date or "").strip():
            continue
        if max_date and date_text > max_date:
            continue
        filtered.append(row)
    rows = filtered
    if len(rows) < 5:
        raise SystemExit(f"Need at least 5 settled outs rows to fit a correction layer; found {len(rows)}")

    feature_names = [
        "line_gap",
        "model_gap",
        "actual_so_far",
        "inning",
        "game_outs",
        "progress_fraction",
        "game_state_parsed_flag",
    ]
    model = _fit_ridge_error_model(rows, feature_names, l2=float(args.l2))
    artifact = {
        "ok": True,
        "model_type": "ridge_linear_error_correction",
        "prop": "outs",
        "source": str(args.source or "render-sync"),
        "rows": len(rows),
        "date_window": {
            "min_date": min(str(row.get("date") or "") for row in rows),
            "max_date": max(str(row.get("date") or "") for row in rows),
        },
        "feature_names": feature_names,
        "l2": float(args.l2),
        "model": model,
        "training_metrics": _evaluate(rows, model, feature_names),
        "leave_one_out_metrics": _leave_one_out(rows, feature_names, l2=float(args.l2)),
        "sample_rows": [
            {
                "date": row.get("date"),
                "owner": row.get("owner"),
                "live_projection": row.get("live_projection"),
                "final_actual": row.get("final_actual"),
                "projection_error": row.get("projection_error"),
                "game_state_parsed": row.get("game_state_parsed"),
            }
            for row in rows[:5]
        ],
    }
    if isinstance(sync_summary, dict):
        artifact["renderSync"] = {
            "startDate": sync_summary.get("startDate"),
            "endDate": sync_summary.get("endDate"),
            "archiveOkCount": sync_summary.get("archiveOkCount"),
            "errorCount": sync_summary.get("errorCount"),
        }

    out_path = Path(str(args.out)).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(artifact, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())