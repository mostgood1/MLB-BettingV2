from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _load_module_from_path(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _prob_over_line_from_dist(dist: Dict[str, Any], line: float) -> Optional[float]:
    if not isinstance(dist, dict) or not dist:
        return None
    try:
        ln = float(line)
    except Exception:
        return None

    total = 0.0
    over = 0.0
    for k, v in dist.items():
        try:
            outs = int(k)
            w = float(v)
        except Exception:
            continue
        if not math.isfinite(w) or w <= 0:
            continue
        total += w
        if float(outs) > ln:
            over += w

    if total <= 0:
        return None
    return float(max(0.0, min(1.0, over / total)))


def _logloss(p: float, y: int, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    yy = 1.0 if int(y) == 1 else 0.0
    return float(-(yy * math.log(pp) + (1.0 - yy) * math.log(1.0 - pp)))


def _brier(p: float, y: int) -> float:
    return float((float(p) - float(y)) ** 2)


def _no_vig_over_prob_from_american(over_odds: Any, under_odds: Any) -> Optional[float]:
    def imp(od: Any) -> Optional[float]:
        try:
            o = float(od)
        except Exception:
            return None
        if o > 0:
            return 100.0 / (o + 100.0)
        if o < 0:
            return (-o) / ((-o) + 100.0)
        return None

    po = imp(over_odds)
    pu = imp(under_odds)
    if po is None or pu is None:
        return None
    if (po + pu) <= 0:
        return None
    return float(po / (po + pu))


@dataclass(frozen=True)
class _Obs:
    p_raw: float
    y_over: int
    p_imp: Optional[float]


def _iter_outs_observations(batch_dir: Path) -> Iterable[_Obs]:
    reports = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    for rp in reports:
        try:
            report = _read_json(rp)
        except Exception:
            continue

        games = report.get("games") or []
        if not isinstance(games, list):
            continue

        for g in games:
            props = (g.get("pitcher_props") or {}) if isinstance(g, dict) else {}
            if not isinstance(props, dict):
                continue

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

                line = mk_outs.get("line")
                if line is None:
                    continue

                try:
                    a_outs = int(actp.get("outs"))
                    ln = float(line)
                except Exception:
                    continue

                outs_dist = pred.get("outs_dist") or {}
                p_raw = _prob_over_line_from_dist(outs_dist, ln)
                if p_raw is None:
                    continue

                y_over = 1 if float(a_outs) > float(ln) else 0

                p_imp = _no_vig_over_prob_from_american(mk_outs.get("over_odds"), mk_outs.get("under_odds"))

                yield _Obs(p_raw=float(p_raw), y_over=int(y_over), p_imp=(float(p_imp) if p_imp is not None else None))


def _outs_metrics_for_alpha(observations: List[_Obs], *, alpha: float) -> Dict[str, Any]:
    a = float(max(0.0, min(1.0, float(alpha))))

    briers: List[float] = []
    lls: List[float] = []
    accs: List[float] = []
    edges: List[float] = []

    for ob in observations:
        p = float((1.0 - a) * float(ob.p_raw) + a * 0.5)
        y = int(ob.y_over)

        briers.append(_brier(p, y))
        lls.append(_logloss(p, y))
        accs.append(1.0 if ((p >= 0.5) == (y == 1)) else 0.0)
        if ob.p_imp is not None:
            edges.append(float(p) - float(ob.p_imp))

    n = len(briers)
    return {
        "n": int(n),
        "outs_brier": (sum(briers) / n) if n else None,
        "outs_logloss": (sum(lls) / n) if n else None,
        "outs_accuracy": (sum(accs) / n) if n else None,
        "outs_avg_edge_vs_no_vig": (sum(edges) / len(edges)) if edges else None,
    }


def _patch_outs_metrics(summary: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    s = copy.deepcopy(summary)
    block = s.get("pitcher_props_at_market_lines_weighted")
    if not isinstance(block, dict):
        block = {}
        s["pitcher_props_at_market_lines_weighted"] = block

    for k in ("outs_brier", "outs_logloss", "outs_accuracy", "outs_avg_edge_vs_no_vig"):
        if k in metrics:
            block[k] = metrics[k]

    n = metrics.get("n")
    if isinstance(n, int):
        for k in (
            "outs_brier_weight",
            "outs_logloss_weight",
            "outs_accuracy_weight",
            "outs_avg_edge_vs_no_vig_weight",
        ):
            block[k] = float(n)

    return s


def main() -> int:
    ap = argparse.ArgumentParser(description="Sweep shrink-to-0.5 alpha for outs market-line calibration (rescore-only)")
    ap.add_argument("--candidate-batch-dir", required=True, help="Batch dir containing sim_vs_actual_*.json with outs_dist")
    ap.add_argument("--baseline-batch-dir", required=True, help="Baseline batch dir containing summary.json")
    ap.add_argument(
        "--objective",
        default="data/tuning/objectives/all_metrics_v3_tuned_best20260210b_random50.json",
        help="Path to objective JSON",
    )
    ap.add_argument("--alphas", default="0.50,0.60,0.70,0.80,0.85,0.90,0.95,1.00")
    args = ap.parse_args()

    candidate_batch_dir = Path(str(args.candidate_batch_dir)).resolve()
    baseline_batch_dir = Path(str(args.baseline_batch_dir)).resolve()
    objective_path = Path(str(args.objective)).resolve()

    if not candidate_batch_dir.exists() or not candidate_batch_dir.is_dir():
        raise SystemExit(f"Invalid candidate batch dir: {candidate_batch_dir}")
    if not baseline_batch_dir.exists() or not baseline_batch_dir.is_dir():
        raise SystemExit(f"Invalid baseline batch dir: {baseline_batch_dir}")
    if not objective_path.exists() or not objective_path.is_file():
        raise SystemExit(f"Invalid objective path: {objective_path}")

    baseline_summary_path = baseline_batch_dir / "summary.json"
    if not baseline_summary_path.exists():
        raise SystemExit(f"Missing baseline summary.json: {baseline_summary_path}")

    baseline_summary = _read_json(baseline_summary_path)
    objective = _read_json(objective_path)

    compare_mod = _load_module_from_path(
        "compare_objective_summaries", Path(__file__).resolve().parent / "compare_objective_summaries.py"
    )
    compare_fn = getattr(compare_mod, "compare", None)
    if compare_fn is None:
        raise SystemExit("Could not load compare() from compare_objective_summaries.py")

    observations = list(_iter_outs_observations(candidate_batch_dir))
    if not observations:
        raise SystemExit(f"No outs observations found under: {candidate_batch_dir}")

    alphas: List[float] = []
    for part in str(args.alphas).split(","):
        s = part.strip()
        if not s:
            continue
        alphas.append(float(s))

    rows: List[Tuple[float, float, Dict[str, Any]]] = []
    for a in alphas:
        metrics = _outs_metrics_for_alpha(observations, alpha=a)

        cand_summary = _patch_outs_metrics(baseline_summary, metrics)
        cand_summary["batch_dir"] = str(candidate_batch_dir)

        cmp = compare_fn(objective=objective, baseline=baseline_summary, candidate=cand_summary)
        score = float(cmp.get("score")) if cmp.get("score") is not None else float("nan")
        rows.append((float(a), score, metrics))

    rows_sorted = sorted(rows, key=lambda t: (t[1], t[0]))

    print(f"candidate_batch_dir: {candidate_batch_dir}")
    print(f"baseline_batch_dir:  {baseline_batch_dir}")
    print(f"objective:           {objective.get('name')}")
    print(f"n_outs:              {len(observations)}")
    print("")
    print("alpha\tscore_vs_base\touts_brier\touts_logloss\touts_edge_abs")
    for a, score, m in rows_sorted:
        edge = m.get("outs_avg_edge_vs_no_vig")
        edge_abs = abs(edge) if isinstance(edge, (int, float)) else None
        print(
            f"{a:.3f}\t{score:.12f}\t{float(m['outs_brier']):.6f}\t{float(m['outs_logloss']):.6f}\t{edge_abs:.6f}"
        )

    best_a, best_score, _ = rows_sorted[0]
    print("")
    print(f"best_alpha: {best_a:.6g}")
    print(f"best_score: {best_score:.12f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
