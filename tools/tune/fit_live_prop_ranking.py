from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.live_prop_ranking import DEFAULT_LIVE_PROP_FEATURES, build_live_prop_feature_map


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), separators=(",", ":")))
            handle.write("\n")
    tmp.replace(path)


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return float(number)


def _clip_prob(p: float, eps: float = 1e-12) -> float:
    return float(min(1.0 - eps, max(eps, float(p))))


def _sigmoid(x: float) -> float:
    try:
        return float(1.0 / (1.0 + math.exp(-float(x))))
    except OverflowError:
        return 0.0 if float(x) < 0.0 else 1.0


def _logloss(ps: Sequence[float], ys: Sequence[int], ws: Optional[Sequence[float]] = None) -> float:
    weights = list(ws or [1.0] * len(ps))
    total = 0.0
    denom = 0.0
    for p, y, w in zip(ps, ys, weights):
        if not math.isfinite(float(w)) or float(w) <= 0.0:
            continue
        pp = _clip_prob(float(p))
        yy = 1 if int(y) == 1 else 0
        total += float(w) * (-(yy * math.log(pp) + (1 - yy) * math.log(1.0 - pp)))
        denom += float(w)
    return float(total / max(1e-12, denom))


def _brier(ps: Sequence[float], ys: Sequence[int], ws: Optional[Sequence[float]] = None) -> float:
    weights = list(ws or [1.0] * len(ps))
    total = 0.0
    denom = 0.0
    for p, y, w in zip(ps, ys, weights):
        if not math.isfinite(float(w)) or float(w) <= 0.0:
            continue
        yy = 1.0 if int(y) == 1 else 0.0
        total += float(w) * ((float(p) - yy) ** 2)
        denom += float(w)
    return float(total / max(1e-12, denom))


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


def _result_label(selection: Any, line: Any, actual: Any) -> str:
    line_value = _safe_float(line)
    actual_value = _safe_float(actual)
    if line_value is None or actual_value is None:
        return "pending"
    if abs(float(actual_value) - float(line_value)) < 1e-9:
        return "push"
    selection_text = str(selection or "").strip().lower()
    if selection_text == "under":
        return "win" if float(actual_value) < float(line_value) else "loss"
    return "win" if float(actual_value) > float(line_value) else "loss"


def _load_first_observations(observation_path: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not observation_path.exists() or not observation_path.is_file():
        return out
    try:
        lines = observation_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return out
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").strip()
        if key and key not in out:
            out[key] = row
    return out


def _iter_registry_rows(live_lens_dir: Path) -> Iterable[Dict[str, Any]]:
    registry_dir = live_lens_dir / "prop_registry"
    if not registry_dir.exists():
        return
    for registry_path in sorted(registry_dir.glob("live_prop_registry_*.json")):
        try:
            doc = _read_json(registry_path)
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
        if not isinstance(entries, dict):
            continue
        suffix = registry_path.stem.replace("live_prop_registry_", "")
        observations = _load_first_observations(registry_dir / f"live_prop_observations_{suffix}.jsonl")
        date_str = str(doc.get("date") or suffix.replace("_", "-")).strip()
        for key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
            last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
            result = _result_label(entry.get("selection"), entry.get("marketLine"), last_snapshot.get("actual"))
            if result not in {"win", "loss"}:
                continue
            observation = observations.get(str(key)) if isinstance(observations.get(str(key)), dict) else {}
            game_state = observation.get("gameState") if isinstance(observation.get("gameState"), dict) else {}
            score = game_state.get("score") if isinstance(game_state.get("score"), dict) else {}
            row: Dict[str, Any] = {
                "date": date_str,
                "key": key,
                "game_pk": entry.get("gamePk"),
                "gamePk": entry.get("gamePk"),
                "market": entry.get("market"),
                "prop": entry.get("prop"),
                "selection": entry.get("selection"),
                "market_line": entry.get("marketLine"),
                "odds": first_snapshot.get("odds"),
                "live_edge": first_snapshot.get("liveEdge"),
                "live_projection": first_snapshot.get("liveProjection"),
                "model_mean": first_snapshot.get("modelMean"),
                "actual": last_snapshot.get("actual"),
                "owner": entry.get("owner"),
                "first_seen_at": entry.get("firstSeenAt"),
                "last_seen_at": entry.get("lastSeenAt"),
                "seen_count": entry.get("seenCount"),
                "team_side": observation.get("teamSide"),
                "progress_fraction": game_state.get("progressFraction"),
                "inning": game_state.get("inning"),
                "outs": game_state.get("outs"),
                "score_away": score.get("away"),
                "score_home": score.get("home"),
                "label": 1 if result == "win" else 0,
            }
            row.update(build_live_prop_feature_map(row))
            yield row


def _split_rows_by_dates(rows: Sequence[Dict[str, Any]], val_last_dates: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if int(val_last_dates) <= 0:
        return list(rows), []
    dates = sorted({str(row.get("date") or "") for row in rows if str(row.get("date") or "")})
    if len(dates) <= int(val_last_dates):
        return list(rows), []
    val_dates = set(dates[-int(val_last_dates):])
    train = [row for row in rows if str(row.get("date") or "") not in val_dates]
    val = [row for row in rows if str(row.get("date") or "") in val_dates]
    return train, val


def _standardize_feature_rows(rows: Sequence[Dict[str, Any]], feature_names: Sequence[str]) -> Tuple[List[List[float]], Dict[str, float], Dict[str, float]]:
    centers: Dict[str, float] = {}
    scales: Dict[str, float] = {}
    matrix: List[List[float]] = []
    for name in feature_names:
        values = [float(_safe_float(row.get(name)) or 0.0) for row in rows]
        if values:
            mean = float(sum(values) / len(values))
            variance = float(sum((value - mean) ** 2 for value in values) / len(values))
            std = float(math.sqrt(max(variance, 1e-12)))
        else:
            mean = 0.0
            std = 1.0
        centers[str(name)] = mean
        scales[str(name)] = std if std > 1e-6 else 1.0
    for row in rows:
        vector: List[float] = []
        for name in feature_names:
            raw = float(_safe_float(row.get(name)) or 0.0)
            vector.append((raw - centers[str(name)]) / scales[str(name)])
        matrix.append(vector)
    return matrix, centers, scales


def _predict_matrix(matrix: Sequence[Sequence[float]], intercept: float, weights: Sequence[float]) -> List[float]:
    out: List[float] = []
    for row in matrix:
        score = float(intercept)
        for value, weight in zip(row, weights):
            score += float(value) * float(weight)
        out.append(float(_sigmoid(score)))
    return out


def _build_side_priors(rows: Sequence[Dict[str, Any]], *, alpha: float = 5.0, beta: float = 5.0) -> Dict[str, Dict[str, float]]:
    side_counts: Dict[str, List[int]] = {}
    for row in rows:
        selection = str(row.get("selection") or "").strip().lower()
        if selection not in {"over", "under"}:
            continue
        wins, total = side_counts.get(selection, [0, 0])
        total += 1
        if int(row.get("label") or 0) == 1:
            wins += 1
        side_counts[selection] = [wins, total]
    out: Dict[str, Dict[str, float]] = {}
    for selection, counts in side_counts.items():
        wins, total = counts
        probability = float((float(wins) + float(alpha)) / (float(total) + float(alpha) + float(beta)))
        out[selection] = {
            "wins": int(wins),
            "n": int(total),
            "prob": probability,
        }
    return out


def fit_logistic_linear(rows: Sequence[Dict[str, Any]], labels: Sequence[int], feature_names: Sequence[str], *, weights: Optional[Sequence[float]] = None, max_iters: int = 60, l2: float = 1e-2) -> Tuple[Dict[str, float], float, Dict[str, float], Dict[str, float], Dict[str, Any]]:
    matrix, centers, scales = _standardize_feature_rows(rows, feature_names)
    ys = [1 if int(y) == 1 else 0 for y in labels]
    ws = [float(w) for w in (weights or [1.0] * len(matrix))]
    dim = len(feature_names)
    beta = [0.0] * (dim + 1)

    def nll(current: Sequence[float]) -> float:
        total = 0.0
        for x_row, y, w in zip(matrix, ys, ws):
            if w <= 0.0:
                continue
            score = float(current[0])
            for value, weight in zip(x_row, current[1:]):
                score += float(value) * float(weight)
            p = _clip_prob(_sigmoid(score))
            total += float(w) * (-(y * math.log(p) + (1 - y) * math.log(1.0 - p)))
        total += 0.5 * float(l2) * sum(weight * weight for weight in current[1:])
        return float(total)

    before = nll(beta)
    iters = 0
    for iteration in range(int(max_iters)):
        grad = [0.0] * (dim + 1)
        hess = [[0.0] * (dim + 1) for _ in range(dim + 1)]
        for x_row, y, w in zip(matrix, ys, ws):
            if w <= 0.0:
                continue
            score = float(beta[0])
            for value, weight in zip(x_row, beta[1:]):
                score += float(value) * float(weight)
            p = _clip_prob(_sigmoid(score))
            err = float(p - y)
            row_full = [1.0] + list(x_row)
            for i in range(dim + 1):
                grad[i] += float(w) * err * float(row_full[i])
            scale = float(w) * float(p) * (1.0 - float(p))
            for i in range(dim + 1):
                for j in range(dim + 1):
                    hess[i][j] += scale * float(row_full[i]) * float(row_full[j])
        for index in range(1, dim + 1):
            grad[index] += float(l2) * float(beta[index])
            hess[index][index] += float(l2)
        step = _gaussian_solve(hess, [-value for value in grad])
        if not step:
            break
        base_nll = nll(beta)
        step_scale = 1.0
        improved = False
        while step_scale >= 1e-4:
            trial = [float(value) + step_scale * float(delta) for value, delta in zip(beta, step)]
            trial_nll = nll(trial)
            if trial_nll <= base_nll + 1e-10:
                beta = trial
                improved = True
                break
            step_scale *= 0.5
        if not improved:
            break
        iters = iteration + 1
        if max(abs(step_scale * float(delta)) for delta in step) < 1e-6:
            break

    weights_out = {str(name): float(beta[idx + 1]) for idx, name in enumerate(feature_names)}
    after = nll(beta)
    diag = {
        "n": len(rows),
        "iters": int(iters),
        "nll_before": float(before),
        "nll_after": float(after),
        "feature_names": [str(name) for name in feature_names],
        "l2": float(l2),
    }
    return weights_out, float(beta[0]), centers, scales, diag


def _build_cfg_block(rows: Sequence[Dict[str, Any]], feature_names: Sequence[str], *, val_rows: Optional[Sequence[Dict[str, Any]]] = None, l2: float, max_iters: int, l2_grid: Sequence[float]) -> Dict[str, Any]:
    labels = [int(row.get("label") or 0) for row in rows]
    weights = [1.0] * len(rows)
    best_block: Optional[Dict[str, Any]] = None
    best_logloss = float("inf")
    candidate_grid = [float(value) for value in l2_grid] if l2_grid else [float(l2)]
    if not candidate_grid:
        candidate_grid = [float(l2)]
    for l2_value in candidate_grid:
        weights_out, intercept, centers, scales, diag = fit_logistic_linear(rows, labels, feature_names, weights=weights, max_iters=int(max_iters), l2=float(l2_value))
        matrix, _, _ = _standardize_feature_rows(rows, feature_names)
        train_ps = _predict_matrix(matrix, intercept, [weights_out[str(name)] for name in feature_names])
        side_priors = _build_side_priors(rows)
        block = {
            "enabled": True,
            "mode": "logistic_linear",
            "intercept": float(intercept),
            "feature_names": [str(name) for name in feature_names],
            "weights": weights_out,
            "centers": centers,
            "scales": scales,
            "side_priors": side_priors,
            "prior_blend_k": 25.0,
            "prior_blend_cap": 0.75,
            "probability_floor": 0.03,
            "probability_ceiling": 0.97,
            "diag": {
                **diag,
                "train_logloss": float(_logloss(train_ps, labels)),
                "train_brier": float(_brier(train_ps, labels)),
                "side_priors": side_priors,
            },
        }
        if val_rows:
            val_matrix: List[List[float]] = []
            for row in val_rows:
                vector: List[float] = []
                for name in feature_names:
                    raw = float(_safe_float(row.get(name)) or 0.0)
                    center = float(centers.get(str(name)) or 0.0)
                    scale = float(scales.get(str(name)) or 1.0)
                    vector.append((raw - center) / (scale if abs(scale) >= 1e-9 else 1.0))
                val_matrix.append(vector)
            val_labels = [int(row.get("label") or 0) for row in val_rows]
            val_ps = _predict_matrix(val_matrix, intercept, [weights_out[str(name)] for name in feature_names])
            block["diag"]["val_logloss"] = float(_logloss(val_ps, val_labels))
            block["diag"]["val_brier"] = float(_brier(val_ps, val_labels))
            score = float(block["diag"]["val_logloss"])
        else:
            score = float(block["diag"]["train_logloss"])
        block["diag"]["selected_l2"] = float(l2_value)
        if score < best_logloss:
            best_logloss = score
            best_block = block
    return best_block or {
        "enabled": False,
        "mode": "logistic_linear",
        "intercept": 0.0,
        "feature_names": [str(name) for name in feature_names],
        "weights": {str(name): 0.0 for name in feature_names},
        "centers": {str(name): 0.0 for name in feature_names},
        "scales": {str(name): 1.0 for name in feature_names},
        "side_priors": _build_side_priors(rows),
        "prior_blend_k": 25.0,
        "prior_blend_cap": 0.75,
        "probability_floor": 0.03,
        "probability_ceiling": 0.97,
        "diag": {"n": len(rows)},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fit live prop ranking/calibration models from historical live-lens registry data")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Path to the live_lens directory")
    parser.add_argument("--out-config", default="data/tuning/live_prop_ranking/default.json", help="Output JSON config path")
    parser.add_argument("--out-dataset", default="", help="Optional JSONL path for extracted training rows")
    parser.add_argument("--min-n", type=int, default=50, help="Minimum settled rows required for a per-prop model")
    parser.add_argument("--val-last-dates", type=int, default=2, help="Hold out the last N settled dates for diagnostics")
    parser.add_argument("--l2", type=float, default=0.1, help="Default L2 regularization strength")
    parser.add_argument("--l2-grid", default="0.01,0.03,0.1,0.3,1.0,3.0", help="Comma-separated L2 values to try")
    parser.add_argument("--max-iters", type=int, default=60)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    live_lens_dir = Path(args.live_lens_dir)
    if not live_lens_dir.is_absolute():
        live_lens_dir = (root / live_lens_dir).resolve()
    if not live_lens_dir.exists():
        raise SystemExit(f"Live lens dir not found: {live_lens_dir}")

    rows = list(_iter_registry_rows(live_lens_dir))
    if not rows:
        raise SystemExit("No settled live-lens registry rows found")
    rows.sort(key=lambda row: (str(row.get("date") or ""), str(row.get("first_seen_at") or ""), str(row.get("key") or "")))

    feature_names = list(DEFAULT_LIVE_PROP_FEATURES)
    train_rows, val_rows = _split_rows_by_dates(rows, int(args.val_last_dates))
    if not train_rows:
        train_rows = list(rows)
        val_rows = []

    l2_grid: List[float] = []
    for token in str(args.l2_grid).split(","):
        text = token.strip()
        if not text:
            continue
        try:
            l2_grid.append(float(text))
        except Exception:
            continue
    if not l2_grid:
        l2_grid = [float(args.l2)]

    config: Dict[str, Any] = {
        "enabled": True,
        "default": _build_cfg_block(train_rows, feature_names, val_rows=val_rows, l2=float(args.l2), max_iters=int(args.max_iters), l2_grid=l2_grid),
        "props": {},
        "diag": {
            "n_total": len(rows),
            "n_train": len(train_rows),
            "n_val": len(val_rows),
            "feature_names": feature_names,
            "val_last_dates": int(args.val_last_dates),
            "prop_counts": dict(Counter(str(row.get("prop") or "") for row in rows)),
            "date_counts": dict(Counter(str(row.get("date") or "") for row in rows)),
        },
    }

    prop_keys = sorted({str(row.get("prop") or "").strip().lower() for row in rows if str(row.get("prop") or "").strip()})
    for prop_key in prop_keys:
        prop_train = [row for row in train_rows if str(row.get("prop") or "").strip().lower() == prop_key]
        prop_val = [row for row in val_rows if str(row.get("prop") or "").strip().lower() == prop_key]
        if len(prop_train) < int(args.min_n):
            config["props"][prop_key] = {
                "enabled": False,
                "mode": "logistic_linear",
                "intercept": 0.0,
                "feature_names": feature_names,
                "weights": {name: 0.0 for name in feature_names},
                "centers": {name: 0.0 for name in feature_names},
                "scales": {name: 1.0 for name in feature_names},
                "diag": {"n": len(prop_train)},
            }
            continue
        config["props"][prop_key] = _build_cfg_block(prop_train, feature_names, val_rows=prop_val, l2=float(args.l2), max_iters=int(args.max_iters), l2_grid=l2_grid)

    out_config = Path(args.out_config)
    if not out_config.is_absolute():
        out_config = (root / out_config).resolve()
    _write_json(out_config, config)

    if str(args.out_dataset).strip():
        out_dataset = Path(str(args.out_dataset))
        if not out_dataset.is_absolute():
            out_dataset = (root / out_dataset).resolve()
        _write_jsonl(out_dataset, rows)
        print(f"Wrote dataset: {out_dataset}")

    print(f"Rows: {len(rows)} (train={len(train_rows)}, val={len(val_rows)})")
    print(f"Wrote config: {out_config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())