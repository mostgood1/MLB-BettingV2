from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _clip_prob(p: float, eps: float = 1e-12) -> float:
    return float(min(1.0 - eps, max(eps, float(p))))


def _logit(p: float) -> float:
    pp = _clip_prob(p)
    return float(math.log(pp) - math.log(1.0 - pp))


def _sigmoid(x: float) -> float:
    try:
        return float(1.0 / (1.0 + math.exp(-float(x))))
    except OverflowError:
        return 0.0 if float(x) < 0 else 1.0


def _nll_affine_logit(
    a: float,
    b: float,
    xs: List[float],
    ys: List[int],
    ws: Optional[List[float]] = None,
    *,
    l2_a: float = 0.0,
    l2_b: float = 0.0,
    prior_a: float = 1.0,
    prior_b: float = 0.0,
) -> float:
    nll = 0.0
    if ws is None:
        ws = [1.0] * len(xs)
    for x, y, w in zip(xs, ys, ws):
        ww = float(w)
        if not math.isfinite(ww) or ww <= 0:
            continue
        p = _clip_prob(_sigmoid(a * x + b))
        nll += ww * (-(y * math.log(p) + (1 - y) * math.log(1.0 - p)))
    if l2_a > 0:
        da = (a - float(prior_a))
        nll += 0.5 * float(l2_a) * (da * da)
    if l2_b > 0:
        db = (b - float(prior_b))
        nll += 0.5 * float(l2_b) * (db * db)
    return float(nll)


def fit_affine_logit(
    ps: List[float],
    ys: List[int],
    ws: Optional[List[float]] = None,
    *,
    max_iters: int = 50,
    l2_a: float = 1e-3,
    l2_b: float = 0.0,
    prior_a: float = 1.0,
    prior_b: float = 0.0,
    min_step: float = 1e-4,
) -> Tuple[float, float, Dict[str, Any]]:
    """Fit p' = sigmoid(a*logit(p) + b) by Newton/IRLS.

    Returns (a, b, diagnostics).
    """
    xs = [_logit(float(p)) for p in ps]
    ys_i = [1 if int(y) == 1 else 0 for y in ys]
    if ws is None:
        ws_i = [1.0] * len(xs)
    else:
        ws_i = [float(w) if (w is not None and math.isfinite(float(w)) and float(w) > 0) else 0.0 for w in ws]
        if len(ws_i) != len(xs):
            ws_i = [1.0] * len(xs)

    # Initialize close to identity
    a = 1.0
    b = 0.0

    diag: Dict[str, Any] = {"n": len(xs), "iters": 0, "nll_before": None, "nll_after": None}
    diag["sum_w"] = float(sum(ws_i))
    diag["prior_a"] = float(prior_a)
    diag["prior_b"] = float(prior_b)
    diag["l2_a"] = float(l2_a)
    diag["l2_b"] = float(l2_b)
    diag["nll_before"] = _nll_affine_logit(
        a,
        b,
        xs,
        ys_i,
        ws_i,
        l2_a=l2_a,
        l2_b=l2_b,
        prior_a=prior_a,
        prior_b=prior_b,
    )

    for it in range(int(max_iters)):
        # Gradient + Hessian
        g_a = 0.0
        g_b = 0.0
        h_aa = 0.0
        h_ab = 0.0
        h_bb = 0.0

        for x, y, w in zip(xs, ys_i, ws_i):
            ww = float(w)
            if ww <= 0.0:
                continue
            p = _clip_prob(_sigmoid(a * x + b))
            w = p * (1.0 - p)
            # gradient of NLL
            err = (p - float(y))
            g_a += ww * (err * x)
            g_b += ww * err
            # Hessian of NLL
            h_aa += ww * (w * x * x)
            h_ab += ww * (w * x)
            h_bb += ww * w

        # L2 priors
        if l2_a > 0:
            g_a += float(l2_a) * (a - float(prior_a))
            h_aa += float(l2_a)
        if l2_b > 0:
            g_b += float(l2_b) * (b - float(prior_b))
            h_bb += float(l2_b)

        # Solve 2x2: H * delta = -g
        det = (h_aa * h_bb - h_ab * h_ab)
        if not math.isfinite(det) or abs(det) < 1e-12:
            break

        delta_a = (-g_a * h_bb - (-g_b) * h_ab) / det
        delta_b = (h_aa * (-g_b) - h_ab * (-g_a)) / det

        if not (math.isfinite(delta_a) and math.isfinite(delta_b)):
            break

        # Line search
        step = 1.0
        nll0 = _nll_affine_logit(a, b, xs, ys_i, ws_i, l2_a=l2_a, l2_b=l2_b, prior_a=prior_a, prior_b=prior_b)
        improved = False
        while step >= float(min_step):
            a_try = a + step * delta_a
            b_try = b + step * delta_b
            nll1 = _nll_affine_logit(a_try, b_try, xs, ys_i, ws_i, l2_a=l2_a, l2_b=l2_b, prior_a=prior_a, prior_b=prior_b)
            if nll1 <= nll0 + 1e-10:
                a, b = a_try, b_try
                improved = True
                break
            step *= 0.5

        if not improved:
            break

        diag["iters"] = it + 1
        if abs(step * delta_a) < 1e-6 and abs(step * delta_b) < 1e-6:
            break

    diag["nll_after"] = _nll_affine_logit(a, b, xs, ys_i, ws_i, l2_a=l2_a, l2_b=l2_b, prior_a=prior_a, prior_b=prior_b)
    return float(a), float(b), diag


def _logloss(ps: List[float], ys: List[int], ws: Optional[List[float]] = None) -> float:
    n = 0.0
    s = 0.0
    if ws is None:
        ws = [1.0] * len(ps)
    for p, y, w in zip(ps, ys, ws):
        ww = float(w)
        if not math.isfinite(ww) or ww <= 0:
            continue
        pp = _clip_prob(float(p))
        yy = 1 if int(y) == 1 else 0
        s += ww * (-(yy * math.log(pp) + (1 - yy) * math.log(1.0 - pp)))
        n += ww
    return float(s / max(1e-12, n))


def _brier(ps: List[float], ys: List[int], ws: Optional[List[float]] = None) -> float:
    n = 0.0
    s = 0.0
    if ws is None:
        ws = [1.0] * len(ps)
    for p, y, w in zip(ps, ys, ws):
        ww = float(w)
        if not math.isfinite(ww) or ww <= 0:
            continue
        pp = float(p)
        yy = 1.0 if int(y) == 1 else 0.0
        s += ww * ((pp - yy) * (pp - yy))
        n += ww
    return float(s / max(1e-12, n))


def _apply_affine_logit_to_ps(ps: List[float], *, a: float, b: float) -> List[float]:
    out: List[float] = []
    for p in ps:
        x = _logit(float(p))
        out.append(_clip_prob(_sigmoid(float(a) * x + float(b))))
    return out


def _pick_best_l2(
    train_ps: List[float],
    train_ys: List[int],
    train_ws: Optional[List[float]],
    val_ps: List[float],
    val_ys: List[int],
    val_ws: Optional[List[float]],
    *,
    l2_grid: List[float],
    l2_b_mult: float,
    max_iters: int,
) -> Tuple[float, float, Dict[str, Any]]:
    best: Optional[Tuple[float, float, float, float, Dict[str, Any]]] = None

    for l2 in l2_grid:
        l2 = float(l2)
        a, b, diag = fit_affine_logit(
            train_ps,
            train_ys,
            train_ws,
            max_iters=int(max_iters),
            l2_a=l2,
            l2_b=float(l2) * float(l2_b_mult),
            prior_a=1.0,
            prior_b=0.0,
        )
        p_val = _apply_affine_logit_to_ps(val_ps, a=a, b=b)
        ll = _logloss(p_val, val_ys, val_ws)
        br = _brier(p_val, val_ys, val_ws)
        if best is None or ll < best[2]:
            diag2 = dict(diag)
            diag2["val_logloss"] = float(ll)
            diag2["val_brier"] = float(br)
            diag2["selected_l2"] = float(l2)
            diag2["selected_l2_b"] = float(l2) * float(l2_b_mult)
            best = (float(a), float(b), float(ll), float(br), diag2)

    if best is None:
        a, b, diag = fit_affine_logit(train_ps, train_ys, max_iters=int(max_iters), l2_a=0.0, l2_b=0.0)
        return float(a), float(b), diag
    return float(best[0]), float(best[1]), best[4]


def _rank_weights(n: int, *, alpha: float) -> List[float]:
    a = float(alpha)
    if a == 0.0:
        return [1.0] * int(n)
    out: List[float] = []
    for i in range(int(n)):
        rank = float(i + 1)
        out.append(float(1.0 / (rank**a)))
    return out


def _extract_hr_pairs(report: Dict[str, Any], *, rank_weight_alpha: float = 0.0) -> Tuple[List[float], List[int], List[float]]:
    ps: List[float] = []
    ys: List[int] = []
    ws: List[float] = []
    games = report.get("games") or []
    if not isinstance(games, list):
        return ps, ys, ws

    for g in games:
        if not isinstance(g, dict):
            continue
        hb = g.get("hitter_hr_backtest") or {}
        if not isinstance(hb, dict):
            continue
        scored = hb.get("scored_overall") or []
        if not isinstance(scored, list):
            continue
        w_by_rank = _rank_weights(len(scored), alpha=float(rank_weight_alpha))
        for idx, r in enumerate(scored):
            if not isinstance(r, dict):
                continue
            try:
                p = float(r.get("p_hr_1plus") or 0.0)
                y = int(r.get("y_hr_1plus") or 0)
            except Exception:
                continue
            ps.append(float(p))
            ys.append(1 if y == 1 else 0)
            ws.append(float(w_by_rank[idx]) if idx < len(w_by_rank) else 1.0)

    return ps, ys, ws


def _extract_prop_pairs(
    report: Dict[str, Any],
    prop_key: str,
    *,
    rank_weight_alpha: float = 0.0,
) -> Tuple[List[float], List[int], List[float]]:
    ps: List[float] = []
    ys: List[int] = []
    ws: List[float] = []
    games = report.get("games") or []
    if not isinstance(games, list):
        return ps, ys, ws

    for g in games:
        if not isinstance(g, dict):
            continue
        hp = g.get("hitter_props_backtest") or {}
        if not isinstance(hp, dict):
            continue
        sub = hp.get(str(prop_key)) or {}
        if not isinstance(sub, dict):
            continue
        scored = sub.get("scored") or []
        if not isinstance(scored, list):
            continue
        w_by_rank = _rank_weights(len(scored), alpha=float(rank_weight_alpha))
        for idx, r in enumerate(scored):
            if not isinstance(r, dict):
                continue
            try:
                p = float(r.get("p") or 0.0)
                y = int(r.get("y") or 0)
            except Exception:
                continue
            ps.append(float(p))
            ys.append(1 if y == 1 else 0)
            ws.append(float(w_by_rank[idx]) if idx < len(w_by_rank) else 1.0)

    return ps, ys, ws


def _iter_reports_from_batch(batch_dir: Path) -> Iterable[Path]:
    for p in sorted(batch_dir.glob("sim_vs_actual_*.json")):
        if p.name == "summary.json":
            continue
        yield p


def main() -> int:
    ap = argparse.ArgumentParser(description="Fit affine-logit probability calibration for hitter HR and hitter props from a batch folder")
    ap.add_argument(
        "--batch-dir",
        required=True,
        help="Path to data/eval/batches/<batch>/ containing sim_vs_actual_*.json reports",
    )
    ap.add_argument(
        "--val-batch-dir",
        default="",
        help="Optional holdout batch dir for selecting L2 via validation logloss",
    )
    ap.add_argument(
        "--out-hr",
        default="data/tuning/hitter_hr_calibration/default.json",
        help="Output path for hitter HR calibration JSON",
    )
    ap.add_argument(
        "--out-props",
        default="data/tuning/hitter_props_calibration/default.json",
        help="Output path for hitter props calibration JSON",
    )
    ap.add_argument("--min-n", type=int, default=500, help="Minimum observations required to fit a prop")
    ap.add_argument("--max-iters", type=int, default=50)
    ap.add_argument("--l2", type=float, default=1e-3, help="L2 strength on (a-1) (and b if --l2-b-mult>0)")
    ap.add_argument(
        "--l2-b-mult",
        type=float,
        default=1.0,
        help="Relative L2 strength for b (i.e., l2_b = l2*l2_b_mult). Set 0 to not regularize b.",
    )
    ap.add_argument(
        "--l2-grid",
        default="0,1e-6,1e-5,1e-4,1e-3,1e-2,1e-1,3e-1,1,3,10,30,100,300,1000",
        help="Comma-separated list of L2 values to try when --val-batch-dir is set (use large values for large n)",
    )
    ap.add_argument(
        "--rank-weight-alpha",
        type=float,
        default=0.0,
        help="Optional rank-weighting by within-game rank (1-indexed). Weight = 1/(rank^alpha). 0 disables.",
    )
    ap.add_argument(
        "--rank-weight-overrides",
        default="",
        help="Optional per-prop rank-weight alpha overrides, e.g. 'runs_1plus=0.75,sb_1plus=1.0'.",
    )
    args = ap.parse_args()

    rank_alpha_default = float(args.rank_weight_alpha)
    rank_alpha_overrides: Dict[str, float] = {}
    if str(args.rank_weight_overrides).strip():
        for tok in str(args.rank_weight_overrides).split(","):
            t = tok.strip()
            if not t or "=" not in t:
                continue
            k, v = t.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                continue
            try:
                rank_alpha_overrides[k] = float(v)
            except Exception:
                continue

    v2_root = Path(__file__).resolve().parents[2]
    batch_dir = Path(args.batch_dir)
    if not batch_dir.is_absolute():
        batch_dir = (v2_root / batch_dir).resolve()
    if not batch_dir.exists():
        raise SystemExit(f"Batch dir not found: {batch_dir}")

    val_batch_dir: Optional[Path] = None
    if str(args.val_batch_dir).strip():
        vb = Path(str(args.val_batch_dir))
        if not vb.is_absolute():
            vb = (v2_root / vb).resolve()
        if not vb.exists():
            raise SystemExit(f"Val batch dir not found: {vb}")
        val_batch_dir = vb

    # Aggregate pairs across all reports
    hr_ps: List[float] = []
    hr_ys: List[int] = []
    hr_ws: List[float] = []

    prop_keys = [
        "hits_1plus",
        "hits_2plus",
        "hits_3plus",
        "doubles_1plus",
        "triples_1plus",
        "runs_1plus",
        "runs_2plus",
        "runs_3plus",
        "rbi_1plus",
        "rbi_2plus",
        "rbi_3plus",
        "rbi_4plus",
        "total_bases_1plus",
        "total_bases_2plus",
        "total_bases_3plus",
        "total_bases_4plus",
        "total_bases_5plus",
        "sb_1plus",
    ]
    props_pairs: Dict[str, Tuple[List[float], List[int]]] = {k: ([], []) for k in prop_keys}
    props_ws: Dict[str, List[float]] = {k: [] for k in prop_keys}

    n_reports = 0
    for rp in _iter_reports_from_batch(batch_dir):
        try:
            report = _read_json(rp)
        except Exception:
            continue
        if not isinstance(report, dict):
            continue
        n_reports += 1

        p1, y1, w1 = _extract_hr_pairs(report, rank_weight_alpha=rank_alpha_default)
        hr_ps.extend(p1)
        hr_ys.extend(y1)
        hr_ws.extend(w1)

        for k in prop_keys:
            ps_k, ys_k = props_pairs[k]
            alpha_k = float(rank_alpha_overrides.get(k, rank_alpha_default))
            p2, y2, w2 = _extract_prop_pairs(report, k, rank_weight_alpha=alpha_k)
            ps_k.extend(p2)
            ys_k.extend(y2)
            props_ws[k].extend(w2)

    # Optional validation pairs (for L2 selection)
    hr_ps_val: List[float] = []
    hr_ys_val: List[int] = []
    hr_ws_val: List[float] = []
    props_pairs_val: Dict[str, Tuple[List[float], List[int]]] = {k: ([], []) for k in prop_keys}
    props_ws_val: Dict[str, List[float]] = {k: [] for k in prop_keys}
    if val_batch_dir is not None:
        for rp in _iter_reports_from_batch(val_batch_dir):
            try:
                report = _read_json(rp)
            except Exception:
                continue
            if not isinstance(report, dict):
                continue

            p1, y1, w1 = _extract_hr_pairs(report, rank_weight_alpha=rank_alpha_default)
            hr_ps_val.extend(p1)
            hr_ys_val.extend(y1)
            hr_ws_val.extend(w1)
            for k in prop_keys:
                ps_k, ys_k = props_pairs_val[k]
                alpha_k = float(rank_alpha_overrides.get(k, rank_alpha_default))
                p2, y2, w2 = _extract_prop_pairs(report, k, rank_weight_alpha=alpha_k)
                ps_k.extend(p2)
                ys_k.extend(y2)
                props_ws_val[k].extend(w2)

    print(f"Reports: {n_reports}")
    print(f"HR pairs: {len(hr_ps)}")
    if val_batch_dir is not None:
        print(f"Val batch: {val_batch_dir}")
        print(f"Val HR pairs: {len(hr_ps_val)}")

    l2_grid: List[float] = []
    try:
        for tok in str(args.l2_grid).split(","):
            t = tok.strip()
            if not t:
                continue
            l2_grid.append(float(t))
    except Exception:
        l2_grid = [float(args.l2)]
    if not l2_grid:
        l2_grid = [float(args.l2)]

    # Fit HR
    hr_cfg: Dict[str, Any]
    if len(hr_ps) >= int(args.min_n):
        if val_batch_dir is not None and len(hr_ps_val) >= int(args.min_n):
            a, b, diag = _pick_best_l2(
                hr_ps,
                hr_ys,
                hr_ws,
                hr_ps_val,
                hr_ys_val,
                hr_ws_val,
                l2_grid=l2_grid,
                l2_b_mult=float(args.l2_b_mult),
                max_iters=int(args.max_iters),
            )
        else:
            a, b, diag = fit_affine_logit(
                hr_ps,
                hr_ys,
                hr_ws,
                max_iters=int(args.max_iters),
                l2_a=float(args.l2),
                l2_b=float(args.l2) * float(args.l2_b_mult),
                prior_a=1.0,
                prior_b=0.0,
            )
        a = float(max(0.05, min(5.0, a)))
        b = float(max(-5.0, min(5.0, b)))
        hr_cfg = {
            "enabled": True,
            "mode": "affine_logit",
            "a": a,
            "b": b,
            "diag": {**diag, "rank_weight_alpha": float(rank_alpha_default)},
        }
    else:
        hr_cfg = {"enabled": False, "mode": "affine_logit", "a": 1.0, "b": 0.0, "diag": {"n": len(hr_ps)}}

    # Fit props
    props_out: Dict[str, Any] = {
        "enabled": True,
        "default": {"enabled": True, "mode": "affine_logit", "a": 1.0, "b": 0.0},
        "props": {},
    }

    for k in prop_keys:
        ps_k, ys_k = props_pairs[k]
        ws_k = props_ws.get(k) or []
        n = len(ps_k)
        if n < int(args.min_n):
            props_out["props"][k] = {"enabled": False, "mode": "affine_logit", "a": 1.0, "b": 0.0, "diag": {"n": n}}
            continue
        if val_batch_dir is not None:
            ps_v, ys_v = props_pairs_val.get(k) or ([], [])
            ws_v = props_ws_val.get(k) or []
        else:
            ps_v, ys_v = ([], [])
            ws_v = []

        alpha_k = float(rank_alpha_overrides.get(k, rank_alpha_default))

        if val_batch_dir is not None and len(ps_v) >= int(args.min_n):
            a, b, diag = _pick_best_l2(
                ps_k,
                ys_k,
                ws_k,
                ps_v,
                ys_v,
                ws_v,
                l2_grid=l2_grid,
                l2_b_mult=float(args.l2_b_mult),
                max_iters=int(args.max_iters),
            )
        else:
            a, b, diag = fit_affine_logit(
                ps_k,
                ys_k,
                ws_k,
                max_iters=int(args.max_iters),
                l2_a=float(args.l2),
                l2_b=float(args.l2) * float(args.l2_b_mult),
                prior_a=1.0,
                prior_b=0.0,
            )
        a = float(max(0.05, min(5.0, a)))
        b = float(max(-5.0, min(5.0, b)))
        props_out["props"][k] = {
            "enabled": True,
            "mode": "affine_logit",
            "a": a,
            "b": b,
            "diag": {**diag, "rank_weight_alpha": float(alpha_k)},
        }

    out_hr = Path(args.out_hr)
    out_props = Path(args.out_props)
    if not out_hr.is_absolute():
        out_hr = (v2_root / out_hr).resolve()
    if not out_props.is_absolute():
        out_props = (v2_root / out_props).resolve()

    _write_json(out_hr, hr_cfg)
    _write_json(out_props, props_out)

    print(f"Wrote HR calibration: {out_hr}")
    print(f"Wrote hitter props calibration: {out_props}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
