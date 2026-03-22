from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _iter_reports(batch_dir: Path) -> List[Path]:
    if not batch_dir.exists() or not batch_dir.is_dir():
        return []
    return sorted([p for p in batch_dir.glob("sim_vs_actual_*.json") if p.is_file()])


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _prob_over_line_from_dist(dist: Dict[str, Any], line: float) -> Optional[float]:
    if not isinstance(dist, dict) or not dist:
        return None
    try:
        ln = float(line)
    except Exception:
        return None
    n = 0.0
    over = 0.0
    for k, v in dist.items():
        try:
            kk = int(k)
            vv = float(v)
        except Exception:
            continue
        if vv <= 0:
            continue
        n += vv
        if float(kk) > ln:
            over += vv
    if n <= 0:
        return None
    return float(max(0.0, min(1.0, over / n)))


def _sigmoid(x: float) -> float:
    try:
        return float(1.0 / (1.0 + math.exp(-float(x))))
    except OverflowError:
        return 0.0 if float(x) < 0 else 1.0


def _logit(p: float, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    return float(math.log(pp) - math.log(1.0 - pp))


def _calibrate_affine_logit(p: float, a: float, b: float, eps: float = 1e-12) -> float:
    z = _logit(float(p), eps=eps)
    return float(min(1.0 - eps, max(eps, _sigmoid(float(a) * z + float(b)))))


def _logloss(p: float, y: int, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    yy = 1.0 if int(y) == 1 else 0.0
    return float(-(yy * math.log(pp) + (1.0 - yy) * math.log(1.0 - pp)))


def _collect_examples(batch_dirs: List[Path]) -> Tuple[List[float], List[int], Dict[str, Any]]:
    ps: List[float] = []
    ys: List[int] = []
    meta: Dict[str, Any] = {
        "batch_dirs": [str(p) for p in batch_dirs],
        "reports": 0,
        "examples": 0,
        "skipped": 0,
    }

    for bd in batch_dirs:
        for rp in _iter_reports(bd):
            meta["reports"] += 1
            try:
                report = _read_json(rp)
            except Exception:
                meta["skipped"] += 1
                continue
            games = report.get("games") or []
            if not isinstance(games, list):
                continue
            for g in games:
                props = (g.get("pitcher_props") or {}) if isinstance(g, dict) else {}
                for side in ("away", "home"):
                    pp = (props.get(side) or {}) if isinstance(props, dict) else {}
                    actp = pp.get("actual") or {}
                    pred = pp.get("pred") or {}
                    market = pp.get("market") or {}
                    if not isinstance(actp, dict) or not isinstance(pred, dict) or not isinstance(market, dict):
                        continue

                    mk_outs = market.get("outs") or {}
                    if not isinstance(mk_outs, dict):
                        continue
                    outs_line = mk_outs.get("line")
                    if outs_line is None:
                        continue
                    try:
                        a_outs = int(actp.get("outs"))
                        line = float(outs_line)
                    except Exception:
                        continue

                    outs_dist = pred.get("outs_dist") or {}
                    p_over = _prob_over_line_from_dist(outs_dist, line)
                    if p_over is None:
                        continue
                    y_over = 1 if float(a_outs) > float(line) else 0
                    ps.append(float(p_over))
                    ys.append(int(y_over))
                    meta["examples"] += 1

    return ps, ys, meta


def _fit_affine_logit(ps: List[float], ys: List[int], steps: int = 800, lr: float = 0.2) -> Dict[str, Any]:
    if not ps or not ys or len(ps) != len(ys):
        raise ValueError("No examples")

    zs = [_logit(p) for p in ps]
    a = 1.0
    b = 0.0

    def loss(a_: float, b_: float) -> float:
        tot = 0.0
        for z, y in zip(zs, ys):
            p2 = _sigmoid(a_ * z + b_)
            tot += _logloss(p2, y)
        return tot / float(len(ys))

    base_loss = loss(a, b)

    for t in range(int(steps)):
        ga = 0.0
        gb = 0.0
        for z, y in zip(zs, ys):
            p2 = _sigmoid(a * z + b)
            err = float(p2) - (1.0 if int(y) == 1 else 0.0)
            ga += err * float(z)
            gb += err
        ga /= float(len(ys))
        gb /= float(len(ys))

        step_lr = float(lr) / float((1.0 + t) ** 0.5)
        a -= step_lr * ga
        b -= step_lr * gb

        a = float(max(0.05, min(5.0, a)))
        b = float(max(-5.0, min(5.0, b)))

    final_loss = loss(a, b)

    cal_ps = [_calibrate_affine_logit(p, a=a, b=b) for p in ps]
    base_ll = sum(_logloss(p, y) for p, y in zip(ps, ys)) / float(len(ys))
    cal_ll = sum(_logloss(p, y) for p, y in zip(cal_ps, ys)) / float(len(ys))

    return {
        "mode": "affine_logit",
        "a": float(a),
        "b": float(b),
        "diagnostics": {
            "n": int(len(ys)),
            "base_loss_identity": float(base_loss),
            "final_loss": float(final_loss),
            "base_logloss": float(base_ll),
            "cal_logloss": float(cal_ll),
        },
    }


def _fit_shrink_to_half(ps: List[float], ys: List[int], reg: float = 0.0) -> Dict[str, Any]:
    if not ps or not ys or len(ps) != len(ys):
        raise ValueError("No examples")

    def loss(alpha: float) -> float:
        a = float(max(0.0, min(1.0, alpha)))
        tot = 0.0
        for p, y in zip(ps, ys):
            p2 = float((1.0 - a) * float(p) + a * 0.5)
            tot += _logloss(p2, y)
        tot /= float(len(ys))
        if float(reg) > 0:
            tot += float(reg) * (a * a)
        return float(tot)

    best_a = 0.0
    best = loss(best_a)
    for i in range(0, 201):
        a = float(i) / 200.0
        v = loss(a)
        if v < best:
            best = v
            best_a = a

    cal_ps = [float((1.0 - best_a) * float(p) + best_a * 0.5) for p in ps]
    base_ll = sum(_logloss(p, y) for p, y in zip(ps, ys)) / float(len(ys))
    cal_ll = sum(_logloss(p, y) for p, y in zip(cal_ps, ys)) / float(len(ys))

    return {
        "mode": "shrink_0p5",
        "alpha": float(best_a),
        "diagnostics": {
            "n": int(len(ys)),
            "reg": float(reg),
            "base_logloss": float(base_ll),
            "cal_logloss": float(cal_ll),
            "objective": float(best),
        },
    }


def _fit_tail_shrink(ps: List[float], ys: List[int], reg: float = 0.0) -> Dict[str, Any]:
    if not ps or not ys or len(ps) != len(ys):
        raise ValueError("No examples")

    def apply(p: float, p0: float, alpha_max: float) -> float:
        p0 = float(max(1e-6, min(0.499, p0)))
        alpha_max = float(max(0.0, min(1.0, alpha_max)))
        pp = float(p)
        if p0 <= pp <= (1.0 - p0):
            return pp
        if pp < p0:
            t = float((p0 - pp) / p0)
        else:
            t = float((pp - (1.0 - p0)) / p0)
        alpha = float(max(0.0, min(1.0, alpha_max * t)))
        return float((1.0 - alpha) * pp + alpha * 0.5)

    p0_grid = [0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25]
    alpha_grid = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]

    best: Optional[float] = None
    best_params: Optional[Tuple[float, float]] = None

    for p0 in p0_grid:
        for amax in alpha_grid:
            tot = 0.0
            for p, y in zip(ps, ys):
                p2 = apply(p, p0=p0, alpha_max=amax)
                tot += _logloss(p2, y)
            tot /= float(len(ys))
            if float(reg) > 0:
                tot += float(reg) * (amax * amax)
            if best is None or tot < best:
                best = float(tot)
                best_params = (float(p0), float(amax))

    if best_params is None:
        return {
            "mode": "tail_shrink",
            "p0": 0.15,
            "alpha_max": 0.0,
            "diagnostics": {
                "n": int(len(ys)),
                "reg": float(reg),
                "base_logloss": None,
                "cal_logloss": None,
                "objective": None,
                "grid": {"p0": p0_grid, "alpha_max": alpha_grid},
            },
        }

    bp0, bamax = best_params
    cal_ps = [apply(p, p0=bp0, alpha_max=bamax) for p in ps]
    base_ll = sum(_logloss(p, y) for p, y in zip(ps, ys)) / float(len(ys))
    cal_ll = sum(_logloss(p, y) for p, y in zip(cal_ps, ys)) / float(len(ys))

    return {
        "mode": "tail_shrink",
        "p0": float(bp0),
        "alpha_max": float(bamax),
        "diagnostics": {
            "n": int(len(ys)),
            "reg": float(reg),
            "base_logloss": float(base_ll),
            "cal_logloss": float(cal_ll),
            "objective": float(best if best is not None else cal_ll),
            "grid": {"p0": p0_grid, "alpha_max": alpha_grid},
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Fit OUTS p_over calibration from existing batch eval reports")
    ap.add_argument("--batch-dir", default="", help="Batch folder (data/eval/batches/<name>)")
    ap.add_argument("--batch-dirs", default="", help="Comma-separated list of batch folders")
    ap.add_argument("--out", default="", help="Output JSON path (default: <batch_dir>/outs_prob_calibration.json)")
    ap.add_argument("--min-n", type=int, default=500, help="Minimum examples required to fit")
    ap.add_argument(
        "--mode",
        choices=["affine_logit", "shrink_0p5", "tail_shrink"],
        default="affine_logit",
        help="Calibration family to fit",
    )
    ap.add_argument(
        "--reg",
        type=float,
        default=0.0,
        help="Regularization strength (shrink_0p5/tail_shrink; penalizes alpha^2 / alpha_max^2)",
    )
    ap.add_argument("--steps", type=int, default=800)
    ap.add_argument("--lr", type=float, default=0.2)
    args = ap.parse_args()

    batch_dirs: List[Path] = []
    if str(args.batch_dir).strip():
        batch_dirs.append(Path(str(args.batch_dir)).resolve())
    if str(args.batch_dirs).strip():
        for s in str(args.batch_dirs).split(","):
            ss = s.strip()
            if ss:
                batch_dirs.append(Path(ss).resolve())

    batch_dirs = [p for p in batch_dirs if p.exists() and p.is_dir()]
    if not batch_dirs:
        print("No valid --batch-dir/--batch-dirs provided")
        return 2

    ps, ys, meta = _collect_examples(batch_dirs)
    if len(ys) < int(args.min_n):
        print(f"Not enough examples: n={len(ys)} < min_n={int(args.min_n)}")
        print("Meta:", json.dumps(meta, indent=2))
        return 3

    if str(args.mode) == "shrink_0p5":
        fit = _fit_shrink_to_half(ps, ys, reg=float(args.reg))
    elif str(args.mode) == "tail_shrink":
        fit = _fit_tail_shrink(ps, ys, reg=float(args.reg))
    else:
        fit = _fit_affine_logit(ps, ys, steps=int(args.steps), lr=float(args.lr))

    out: Dict[str, Any] = {
        "enabled": True,
        "mode": str(fit.get("mode") or str(args.mode)),
        "fitted_at": datetime.now().isoformat(),
        "data": meta,
        "diagnostics": fit.get("diagnostics") or {},
    }
    if out["mode"] == "shrink_0p5":
        out["alpha"] = float(fit.get("alpha") or 0.0)
    elif out["mode"] == "tail_shrink":
        out["p0"] = float(fit.get("p0") or 0.15)
        out["alpha_max"] = float(fit.get("alpha_max") or 0.0)
    else:
        out["a"] = float(fit["a"])
        out["b"] = float(fit["b"])

    if str(args.out).strip():
        out_path = Path(str(args.out)).resolve()
    else:
        out_path = batch_dirs[0] / "outs_prob_calibration.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"Wrote: {out_path}")
    if out["mode"] == "shrink_0p5":
        print("Calibration:", {"alpha": out.get("alpha"), "mode": out["mode"]})
    elif out["mode"] == "tail_shrink":
        print("Calibration:", {"p0": out.get("p0"), "alpha_max": out.get("alpha_max"), "mode": out["mode"]})
    else:
        print("Calibration:", {"a": out.get("a"), "b": out.get("b"), "mode": out["mode"]})
    print("Diagnostics:", json.dumps(out.get("diagnostics") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
