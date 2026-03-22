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


def _sigmoid(x: float) -> float:
    # numerically stable-ish sigmoid
    if x >= 0:
        z = math.exp(-x)
        return float(1.0 / (1.0 + z))
    z = math.exp(x)
    return float(z / (1.0 + z))


def _logit(p: float, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    return float(math.log(pp / (1.0 - pp)))


def _affine_logit_cal(p: float, *, a: float, b: float) -> float:
    return float(_sigmoid(float(a) * _logit(p) + float(b)))


def _logloss(p: float, y: int, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    yy = 1.0 if int(y) == 1 else 0.0
    return float(-(yy * math.log(pp) + (1.0 - yy) * math.log(1.0 - pp)))


def _brier(p: float, y: int) -> float:
    return float((float(p) - float(y)) ** 2)


@dataclass(frozen=True)
class _Obs:
    p_raw: float
    y: int


def _iter_hr_observations(batch_dir: Path) -> Iterable[_Obs]:
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
            if not isinstance(g, dict):
                continue
            bt = g.get("hitter_hr_backtest") or {}
            if not isinstance(bt, dict):
                continue
            scored = bt.get("scored_overall") or []
            if not isinstance(scored, list):
                continue

            for row in scored:
                if not isinstance(row, dict):
                    continue
                p = row.get("p_hr_1plus")
                y = row.get("y_hr_1plus")
                try:
                    pf = float(p)
                    yi = int(y)
                except Exception:
                    continue
                if not math.isfinite(pf):
                    continue
                yield _Obs(p_raw=pf, y=yi)


def _iter_prop_observations(batch_dir: Path) -> Iterable[Tuple[str, _Obs]]:
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
            if not isinstance(g, dict):
                continue
            bt = g.get("hitter_props_backtest") or {}
            if not isinstance(bt, dict):
                continue

            for prop, block in bt.items():
                if not isinstance(block, dict):
                    continue
                scored = block.get("scored") or []
                if not isinstance(scored, list):
                    continue

                for row in scored:
                    if not isinstance(row, dict):
                        continue
                    p = row.get("p")
                    y = row.get("y")
                    try:
                        pf = float(p)
                        yi = int(y)
                    except Exception:
                        continue
                    if not math.isfinite(pf):
                        continue
                    yield str(prop), _Obs(p_raw=pf, y=yi)


def _load_hr_ab(path: Path) -> Tuple[float, float]:
    cfg = _read_json(path)
    if not isinstance(cfg, dict):
        return 1.0, 0.0
    if cfg.get("enabled") is False:
        return 1.0, 0.0
    mode = str(cfg.get("mode") or "").strip().lower()
    if mode and mode != "affine_logit":
        raise SystemExit(f"Unsupported HR calibration mode: {mode}")
    try:
        return float(cfg.get("a") or 1.0), float(cfg.get("b") or 0.0)
    except Exception:
        return 1.0, 0.0


def _load_props_ab(path: Path) -> Tuple[Tuple[float, float], Dict[str, Tuple[float, float]]]:
    cfg = _read_json(path)
    if not isinstance(cfg, dict):
        return (1.0, 0.0), {}

    default_cfg = cfg.get("default") if isinstance(cfg.get("default"), dict) else {}
    if isinstance(default_cfg, dict) and default_cfg.get("enabled") is False:
        default_ab = (1.0, 0.0)
    else:
        mode = str((default_cfg or {}).get("mode") or "").strip().lower()
        if mode and mode != "affine_logit":
            raise SystemExit(f"Unsupported props default calibration mode: {mode}")
        try:
            default_ab = (float((default_cfg or {}).get("a") or 1.0), float((default_cfg or {}).get("b") or 0.0))
        except Exception:
            default_ab = (1.0, 0.0)

    out: Dict[str, Tuple[float, float]] = {}
    props = cfg.get("props")
    if isinstance(props, dict):
        for prop, pcfg in props.items():
            if not isinstance(pcfg, dict):
                continue
            if pcfg.get("enabled") is False:
                continue
            mode = str(pcfg.get("mode") or "").strip().lower()
            if mode and mode != "affine_logit":
                raise SystemExit(f"Unsupported props calibration mode for {prop}: {mode}")
            try:
                out[str(prop)] = (float(pcfg.get("a") or 1.0), float(pcfg.get("b") or 0.0))
            except Exception:
                continue

    return default_ab, out


def _shrink_ab(a0: float, b0: float, *, shrink: float) -> Tuple[float, float]:
    s = float(max(0.0, min(1.0, float(shrink))))
    return (1.0 + s * (float(a0) - 1.0), s * float(b0))


def _metrics_from_obs(obs: List[_Obs], *, a: float, b: float) -> Dict[str, Any]:
    briers: List[float] = []
    lls: List[float] = []
    ps: List[float] = []
    ys: List[int] = []

    for ob in obs:
        pcal = _affine_logit_cal(ob.p_raw, a=a, b=b)
        y = int(ob.y)
        briers.append(_brier(pcal, y))
        lls.append(_logloss(pcal, y))
        ps.append(float(pcal))
        ys.append(y)

    n = int(len(obs))
    return {
        "n": n,
        "brier": (sum(briers) / n) if n else None,
        "logloss": (sum(lls) / n) if n else None,
        "avg_p": (sum(ps) / n) if n else None,
        "emp_rate": (sum(float(y) for y in ys) / n) if n else None,
    }


def _patch_hitter_metrics(
    baseline_summary: Dict[str, Any],
    *,
    hr_metrics: Dict[str, Any],
    prop_metrics: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    s = copy.deepcopy(baseline_summary)

    hr_block = s.get("hitter_hr_likelihood_topn_weighted")
    if not isinstance(hr_block, dict):
        hr_block = {}
        s["hitter_hr_likelihood_topn_weighted"] = hr_block

    n_hr = hr_metrics.get("n")
    for k in ("brier", "logloss", "avg_p", "emp_rate"):
        v = hr_metrics.get(k)
        hr_block[f"hr_{k}"] = v
        if isinstance(n_hr, int):
            hr_block[f"hr_{k}_weight"] = float(n_hr)

    hp_block = s.get("hitter_props_likelihood_topn_weighted")
    if not isinstance(hp_block, dict):
        hp_block = {}
        s["hitter_props_likelihood_topn_weighted"] = hp_block

    for prop, m in prop_metrics.items():
        n = m.get("n")
        for k in ("brier", "logloss", "avg_p", "emp_rate"):
            hp_block[f"{prop}_{k}"] = m.get(k)
            if isinstance(n, int):
                hp_block[f"{prop}_{k}_weight"] = float(n)

    return s


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Sweep a single knob: shrink all hitter affine-logit calibrations toward identity (a=1,b=0), rescore-only"
        )
    )
    ap.add_argument("--candidate-batch-dir", required=True, help="Batch dir containing sim_vs_actual_*.json")
    ap.add_argument(
        "--objective",
        default="data/tuning/objectives/hitter_props_topn_v1_baseline_20260218_random50_s250.json",
        help="Path to objective JSON",
    )
    ap.add_argument(
        "--hr-cal",
        default="data/tuning/hitter_hr_calibration/default.json",
        help="Path to hitter HR calibration JSON",
    )
    ap.add_argument(
        "--props-cal",
        default="data/tuning/hitter_props_calibration/default.json",
        help="Path to hitter props calibration JSON",
    )
    ap.add_argument("--shrinks", default="0.00,0.25,0.50,0.75,1.00")
    args = ap.parse_args()

    candidate_batch_dir = Path(str(args.candidate_batch_dir)).resolve()
    objective_path = Path(str(args.objective)).resolve()
    hr_cal_path = Path(str(args.hr_cal)).resolve()
    props_cal_path = Path(str(args.props_cal)).resolve()

    if not candidate_batch_dir.exists() or not candidate_batch_dir.is_dir():
        raise SystemExit(f"Invalid candidate batch dir: {candidate_batch_dir}")
    if not objective_path.exists() or not objective_path.is_file():
        raise SystemExit(f"Invalid objective path: {objective_path}")
    if not hr_cal_path.exists() or not hr_cal_path.is_file():
        raise SystemExit(f"Invalid hr-cal path: {hr_cal_path}")
    if not props_cal_path.exists() or not props_cal_path.is_file():
        raise SystemExit(f"Invalid props-cal path: {props_cal_path}")

    objective = _read_json(objective_path)
    baseline_summary_rel = objective.get("baseline_summary")
    if not isinstance(baseline_summary_rel, str) or not baseline_summary_rel.strip():
        raise SystemExit("Objective JSON missing baseline_summary")

    # Objective paths are workspace/repo-relative.
    repo_root = Path(__file__).resolve().parents[2]
    baseline_summary_path = (repo_root / baseline_summary_rel).resolve()
    if not baseline_summary_path.exists():
        # Fallback: treat as CWD-relative.
        baseline_summary_path = Path(baseline_summary_rel).resolve()

    if not baseline_summary_path.exists() or not baseline_summary_path.is_file():
        raise SystemExit(f"Missing baseline summary: {baseline_summary_path}")

    baseline_summary = _read_json(baseline_summary_path)

    compare_mod = _load_module_from_path(
        "compare_objective_summaries", Path(__file__).resolve().parent / "compare_objective_summaries.py"
    )
    compare_fn = getattr(compare_mod, "compare", None)
    if compare_fn is None:
        raise SystemExit("Could not load compare() from compare_objective_summaries.py")

    hr_a0, hr_b0 = _load_hr_ab(hr_cal_path)
    props_default_ab, props_ab = _load_props_ab(props_cal_path)

    hr_obs = list(_iter_hr_observations(candidate_batch_dir))
    prop_obs: Dict[str, List[_Obs]] = {}
    for prop, ob in _iter_prop_observations(candidate_batch_dir):
        prop_obs.setdefault(prop, []).append(ob)

    if not hr_obs and not prop_obs:
        raise SystemExit(f"No hitter observations found under: {candidate_batch_dir}")

    shrinks: List[float] = []
    for part in str(args.shrinks).split(","):
        s = part.strip()
        if not s:
            continue
        shrinks.append(float(s))
    if not shrinks:
        raise SystemExit("No shrinks specified")

    rows: List[Tuple[float, float, Dict[str, Any], Dict[str, Dict[str, Any]]]] = []

    for s in shrinks:
        ha, hb = _shrink_ab(hr_a0, hr_b0, shrink=s)
        hr_metrics = _metrics_from_obs(hr_obs, a=ha, b=hb) if hr_obs else {"n": 0, "brier": None, "logloss": None}

        prop_metrics: Dict[str, Dict[str, Any]] = {}
        for prop, obs_list in prop_obs.items():
            a0, b0 = props_ab.get(prop, props_default_ab)
            pa, pb = _shrink_ab(a0, b0, shrink=s)
            prop_metrics[prop] = _metrics_from_obs(obs_list, a=pa, b=pb)

        cand_summary = _patch_hitter_metrics(baseline_summary, hr_metrics=hr_metrics, prop_metrics=prop_metrics)
        cand_summary["batch_dir"] = str(candidate_batch_dir)

        cmp = compare_fn(objective=objective, baseline=baseline_summary, candidate=cand_summary)
        score = float(cmp.get("score")) if cmp.get("score") is not None else float("nan")
        rows.append((float(s), score, hr_metrics, prop_metrics))

    rows_sorted = sorted(rows, key=lambda t: (t[1], t[0]))

    print(f"candidate_batch_dir: {candidate_batch_dir}")
    print(f"objective:           {objective.get('name')}")
    print(f"baseline_summary:    {baseline_summary_path}")
    print(f"n_hr:                {len(hr_obs)}")
    print(f"n_props_total:       {sum(len(v) for v in prop_obs.values())}")
    print("")
    print("shrink\tscore_vs_base\thr_brier\thr_logloss\truns_ll\thits1_ll")

    def _pm(prop: str, k: str, pms: Dict[str, Dict[str, Any]]) -> Optional[float]:
        v = (pms.get(prop) or {}).get(k)
        return float(v) if isinstance(v, (int, float)) else None

    for s, score, hm, pm in rows_sorted:
        hr_brier = float(hm.get("brier")) if isinstance(hm.get("brier"), (int, float)) else float("nan")
        hr_ll = float(hm.get("logloss")) if isinstance(hm.get("logloss"), (int, float)) else float("nan")
        runs_ll = _pm("runs_1plus", "logloss", pm)
        hits1_ll = _pm("hits_1plus", "logloss", pm)
        print(
            f"{s:.3f}\t{score:.12f}\t{hr_brier:.6f}\t{hr_ll:.6f}\t{(runs_ll if runs_ll is not None else float('nan')):.6f}\t{(hits1_ll if hits1_ll is not None else float('nan')):.6f}"
        )

    best_s, best_score, _, _ = rows_sorted[0]
    print("")
    print(f"best_shrink: {best_s:.6g}")
    print(f"best_score:  {best_score:.12f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
