from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _read_text_dates(path: Path) -> List[str]:
    out: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().lstrip("\ufeff")
        if s and not s.startswith("#"):
            out.append(s)
    return out


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _run(cmd: List[str], cwd: Path) -> int:
    p = subprocess.run(cmd, cwd=str(cwd))
    return int(p.returncode)


def _parse_grid_vals(raw: str) -> List[int]:
    vals: List[int] = []
    for part in str(raw).split(","):
        s = part.strip()
        if not s:
            continue
        v = int(float(s))
        vals.append(int(v))
    if not vals:
        raise ValueError("Empty --grid-vals")
    return vals


def _read_overrides(path_or_json: str) -> Dict[str, Any]:
    s = (path_or_json or "").strip()
    if not s:
        return {}

    p = Path(s)
    if p.exists():
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    # Try inline JSON dict
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _score_one(
    *,
    scorer: Path,
    objective: Path,
    candidate_summary: Path,
    baseline_summary: Path,
    out_path: Path,
) -> Optional[float]:
    rc = _run(
        [
            sys.executable,
            str(scorer),
            "--candidate-summary",
            str(candidate_summary),
            "--objective",
            str(objective),
            "--baseline-summary",
            str(baseline_summary),
            "--out",
            str(out_path),
        ],
        cwd=_ROOT,
    )
    if rc != 0 or not out_path.exists():
        return None
    try:
        obj = json.loads(out_path.read_text(encoding="utf-8"))
        v = obj.get("score")
        if isinstance(v, (int, float)) and math.isfinite(float(v)):
            return float(v)
    except Exception:
        return None
    return None


def _summarize_batch(*, summarizer: Path, batch_dir: Path, out_path: Path) -> bool:
    rc = _run([sys.executable, str(summarizer), "--batch-dir", str(batch_dir), "--out", str(out_path)], cwd=_ROOT)
    return rc == 0 and out_path.exists()


def _run_batch(
    *,
    batch_runner: Path,
    date_file: Path,
    out_dir: Path,
    sims_per_game: int,
    jobs: int,
    use_raw: str,
    prop_lines_source: str,
    manager_pitching: str,
    manager_pitching_overrides: str,
) -> int:
    cmd = [
        sys.executable,
        str(batch_runner),
        "--date-file",
        str(date_file),
        "--sims-per-game",
        str(int(sims_per_game)),
        "--jobs",
        str(int(jobs)),
        "--use-raw",
        str(use_raw),
        "--prop-lines-source",
        str(prop_lines_source),
        "--manager-pitching",
        str(manager_pitching),
        "--manager-pitching-overrides",
        str(manager_pitching_overrides or ""),
        "--batch-out",
        str(out_dir),
    ]
    return _run(cmd, cwd=_ROOT)


def _extract_pitch_mae(batch_dir: Path) -> Tuple[Optional[float], Optional[float], int]:
    """Return (mae, bias, n) for starter pitches in a batch.

    - Uses per-day report field: assessment.full_game.pitcher_props_starters.pitches_mae
    - Weights by pitches_n (number of starter slots with actual pitch counts).
    """
    reports = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    if not reports:
        return (None, None, 0)

    sum_w = 0.0
    sum_mae = 0.0
    sum_bias = 0.0

    for rp in reports:
        try:
            obj = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            continue
        pp = (((obj.get("assessment") or {}).get("full_game") or {}).get("pitcher_props_starters") or {})
        mae = pp.get("pitches_mae")
        bias = pp.get("pitches_bias")
        n = pp.get("pitches_n")
        try:
            w = float(n) if n is not None else 0.0
        except Exception:
            w = 0.0
        if w <= 0.0:
            continue
        if not isinstance(mae, (int, float)):
            continue
        if isinstance(bias, (int, float)):
            sum_bias += float(bias) * w
        sum_mae += float(mae) * w
        sum_w += w

    if sum_w <= 0.0:
        return (None, None, 0)
    mae_out = float(sum_mae / sum_w)
    bias_out = float(sum_bias / sum_w) if sum_bias != 0.0 else 0.0
    return (mae_out, bias_out, int(round(sum_w)))


@dataclass
class CandidateResult:
    val: int
    tune_pitch_mae: Optional[float]
    tune_pitch_bias: Optional[float]
    hold_pitch_mae: Optional[float]
    hold_pitch_bias: Optional[float]
    allmetrics_tune: Optional[float]
    hitterprops_tune: Optional[float]
    allmetrics_hold: Optional[float]
    hitterprops_hold: Optional[float]
    passed_guardrail: bool


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Sweep manager_pitching_overrides['starter_hook_add_pitches'] and evaluate starter pitch-count realism.\n\n"
            "This lever shifts the starter pull hook in manager_pitching='v2' without changing pitch outcome rates."
        )
    )
    ap.add_argument(
        "--tune-date-file",
        default="data/eval/date_sets/random_feed_live_2025_regseason_50days_min10_seed2026.txt",
    )
    ap.add_argument(
        "--holdout-date-file",
        default="data/eval/date_sets/holdout_disjoint_13days_from_pushPolicy20_half_s10_excluding_tune_random50.txt",
    )
    ap.add_argument("--tune-n-days", type=int, default=0)
    ap.add_argument("--holdout-n-days", type=int, default=0)
    ap.add_argument("--sims-per-game", type=int, default=200)
    ap.add_argument("--jobs", type=int, default=6)
    ap.add_argument("--use-raw", choices=["on", "off"], default="on")
    ap.add_argument("--prop-lines-source", choices=["auto", "oddsapi", "last_known", "bovada", "off"], default="last_known")
    ap.add_argument("--manager-pitching", choices=["off", "legacy", "v2"], default="v2")
    ap.add_argument(
        "--base-manager-overrides",
        default="data/tuning/manager_pitching_overrides/default.json",
        help="Base manager overrides JSON/path; candidate overrides add starter_hook_add_pitches on top.",
    )
    ap.add_argument(
        "--objective-allmetrics",
        default="data/tuning/objectives/all_metrics_v3_tuned_best20260210b_random50.json",
    )
    ap.add_argument(
        "--objective-hitterprops",
        default="data/tuning/objectives/hitter_props_topn_v1_baseline_20260218_random50_s250.json",
    )
    ap.add_argument(
        "--grid-vals",
        default="0,2,4,6,8",
        help="Comma-separated integer values (pitches) to add to starter hook.",
    )
    ap.add_argument("--holdout-tol", type=float, default=0.002)
    ap.add_argument(
        "--holdout-pitch-mae-tol",
        type=float,
        default=0.25,
        help="Allowable regression (absolute) in holdout pitch MAE vs holdout-baseline.",
    )
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_mgr_starter_hook_add_pitches",
        help="Root folder for this sweep's outputs.",
    )
    args = ap.parse_args()

    tune_date_file = Path(args.tune_date_file)
    if not tune_date_file.is_absolute():
        tune_date_file = (_ROOT / tune_date_file).resolve()
    holdout_date_file = Path(args.holdout_date_file)
    if not holdout_date_file.is_absolute():
        holdout_date_file = (_ROOT / holdout_date_file).resolve()

    if not tune_date_file.exists():
        print(f"Missing tune-date-file: {tune_date_file}")
        return 2
    if not holdout_date_file.exists():
        print(f"Missing holdout-date-file: {holdout_date_file}")
        return 2

    tune_dates = _read_text_dates(tune_date_file)
    holdout_dates = _read_text_dates(holdout_date_file)
    if int(args.tune_n_days) > 0:
        tune_dates = tune_dates[: int(args.tune_n_days)]
    if int(args.holdout_n_days) > 0:
        holdout_dates = holdout_dates[: int(args.holdout_n_days)]

    grid_vals = _parse_grid_vals(str(args.grid_vals))

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = (_ROOT / Path(str(args.batch_root)) / stamp).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    (out_root / "tune_dates.txt").write_text("\n".join(tune_dates) + "\n", encoding="utf-8")
    (out_root / "holdout_dates.txt").write_text("\n".join(holdout_dates) + "\n", encoding="utf-8")

    # materialize shortened date files into the sweep folder for reproducibility
    tune_dates_path = out_root / "_tune_dates_used.txt"
    holdout_dates_path = out_root / "_holdout_dates_used.txt"
    tune_dates_path.write_text("\n".join(tune_dates) + "\n", encoding="utf-8")
    holdout_dates_path.write_text("\n".join(holdout_dates) + "\n", encoding="utf-8")

    batch_runner = (_ROOT / "tools" / "eval" / "run_batch_eval_days.py").resolve()
    summarizer = (_ROOT / "tools" / "eval" / "summarize_batch_eval.py").resolve()
    scorer = (_ROOT / "tools" / "tune" / "score_batch_summary.py").resolve()

    obj_all = (_ROOT / Path(str(args.objective_allmetrics))).resolve()
    obj_props = (_ROOT / Path(str(args.objective_hitterprops))).resolve()
    if not obj_all.exists():
        print(f"Missing objective-allmetrics: {obj_all}")
        return 2
    if not obj_props.exists():
        print(f"Missing objective-hitterprops: {obj_props}")
        return 2

    base_overrides = _read_overrides(str(args.base_manager_overrides))
    try:
        baseline_value = int(float(base_overrides.get("starter_hook_add_pitches", 0) or 0))
    except Exception:
        baseline_value = 0
    baseline_value = max(0, int(baseline_value))
    if "starter_hook_add_pitches" in base_overrides:
        base_overrides["starter_hook_add_pitches"] = int(baseline_value)

    # Baseline (base overrides)
    baseline_dir = out_root / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    baseline_overrides_path = baseline_dir / "manager_pitching_overrides.json"
    _write_json(baseline_overrides_path, dict(base_overrides))

    baseline_tune_batch = baseline_dir / "tune_batch"
    baseline_hold_batch = baseline_dir / "holdout_batch"

    rc = _run_batch(
        batch_runner=batch_runner,
        date_file=tune_dates_path,
        out_dir=baseline_tune_batch,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=str(args.use_raw),
        prop_lines_source=str(args.prop_lines_source),
        manager_pitching=str(args.manager_pitching),
        manager_pitching_overrides=str(baseline_overrides_path),
    )
    if rc != 0:
        print(f"Baseline tune batch failed rc={rc}")
        return int(rc)

    rc = _run_batch(
        batch_runner=batch_runner,
        date_file=holdout_dates_path,
        out_dir=baseline_hold_batch,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=str(args.use_raw),
        prop_lines_source=str(args.prop_lines_source),
        manager_pitching=str(args.manager_pitching),
        manager_pitching_overrides=str(baseline_overrides_path),
    )
    if rc != 0:
        print(f"Baseline holdout batch failed rc={rc}")
        return int(rc)

    baseline_tune_summary = baseline_dir / "tune_summary.json"
    baseline_hold_summary = baseline_dir / "holdout_summary.json"
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_batch, out_path=baseline_tune_summary):
        print("Baseline summarize tune failed")
        return 2
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_hold_batch, out_path=baseline_hold_summary):
        print("Baseline summarize holdout failed")
        return 2

    base_tune_pitch_mae, base_tune_pitch_bias, _ = _extract_pitch_mae(baseline_tune_batch)
    base_hold_pitch_mae, base_hold_pitch_bias, _ = _extract_pitch_mae(baseline_hold_batch)

    baseline_all_tune = 1.0
    baseline_props_tune = 1.0
    baseline_all_hold = 1.0
    baseline_props_hold = 1.0

    results: List[CandidateResult] = []

    for v in grid_vals:
        if int(v) == int(baseline_value):
            continue

        cand_dir = out_root / f"cand_{int(v):03d}"
        cand_dir.mkdir(parents=True, exist_ok=True)

        cand_overrides = dict(base_overrides)
        cand_overrides["starter_hook_add_pitches"] = int(v)
        cand_overrides_path = cand_dir / "manager_pitching_overrides.json"
        _write_json(cand_overrides_path, cand_overrides)

        cand_tune_batch = cand_dir / "tune_batch"
        cand_hold_batch = cand_dir / "holdout_batch"

        rc = _run_batch(
            batch_runner=batch_runner,
            date_file=tune_dates_path,
            out_dir=cand_tune_batch,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=str(args.use_raw),
            prop_lines_source=str(args.prop_lines_source),
            manager_pitching=str(args.manager_pitching),
            manager_pitching_overrides=str(cand_overrides_path),
        )
        if rc != 0:
            print(f"Candidate {v} tune batch failed rc={rc}")
            continue

        rc = _run_batch(
            batch_runner=batch_runner,
            date_file=holdout_dates_path,
            out_dir=cand_hold_batch,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=str(args.use_raw),
            prop_lines_source=str(args.prop_lines_source),
            manager_pitching=str(args.manager_pitching),
            manager_pitching_overrides=str(cand_overrides_path),
        )
        if rc != 0:
            print(f"Candidate {v} holdout batch failed rc={rc}")
            continue

        cand_tune_summary = cand_dir / "tune_summary.json"
        cand_hold_summary = cand_dir / "holdout_summary.json"
        if not _summarize_batch(summarizer=summarizer, batch_dir=cand_tune_batch, out_path=cand_tune_summary):
            print(f"Candidate {v} summarize tune failed")
            continue
        if not _summarize_batch(summarizer=summarizer, batch_dir=cand_hold_batch, out_path=cand_hold_summary):
            print(f"Candidate {v} summarize holdout failed")
            continue

        tune_pitch_mae, tune_pitch_bias, _ = _extract_pitch_mae(cand_tune_batch)
        hold_pitch_mae, hold_pitch_bias, _ = _extract_pitch_mae(cand_hold_batch)

        all_tune = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=cand_tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "score_allmetrics_tune.json",
        )
        props_tune = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=cand_tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "score_hitterprops_tune.json",
        )
        all_hold = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=cand_hold_summary,
            baseline_summary=baseline_hold_summary,
            out_path=cand_dir / "score_allmetrics_holdout.json",
        )
        props_hold = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=cand_hold_summary,
            baseline_summary=baseline_hold_summary,
            out_path=cand_dir / "score_hitterprops_holdout.json",
        )

        # Guardrail
        ok = True
        if all_hold is None or props_hold is None:
            ok = False
        else:
            floor = 1.0 - float(args.holdout_tol)
            ok = ok and float(all_hold) >= float(floor)
            ok = ok and float(props_hold) >= float(floor)

        if base_hold_pitch_mae is not None and hold_pitch_mae is not None:
            ok = ok and float(hold_pitch_mae) <= float(base_hold_pitch_mae) + float(args.holdout_pitch_mae_tol)

        results.append(
            CandidateResult(
                val=int(v),
                tune_pitch_mae=tune_pitch_mae,
                tune_pitch_bias=tune_pitch_bias,
                hold_pitch_mae=hold_pitch_mae,
                hold_pitch_bias=hold_pitch_bias,
                allmetrics_tune=all_tune,
                hitterprops_tune=props_tune,
                allmetrics_hold=all_hold,
                hitterprops_hold=props_hold,
                passed_guardrail=bool(ok),
            )
        )

    best: Optional[CandidateResult] = None
    for r in results:
        if not r.passed_guardrail:
            continue
        if best is None:
            best = r
            continue
        # prefer lower holdout pitch MAE; break ties with allmetrics_hold
        if r.hold_pitch_mae is not None and best.hold_pitch_mae is not None:
            if float(r.hold_pitch_mae) < float(best.hold_pitch_mae) - 1e-9:
                best = r
                continue
        if r.allmetrics_hold is not None and best.allmetrics_hold is not None:
            if float(r.allmetrics_hold) > float(best.allmetrics_hold) + 1e-9:
                best = r
                continue

    sweep_summary = {
        "meta": {
            "grid_vals": grid_vals,
            "baseline_value": baseline_value,
            "sims_per_game": int(args.sims_per_game),
            "jobs": int(args.jobs),
            "tune_date_file": str(tune_date_file),
            "holdout_date_file": str(holdout_date_file),
            "tune_days": len(tune_dates),
            "holdout_days": len(holdout_dates),
            "holdout_tol": float(args.holdout_tol),
            "holdout_pitch_mae_tol": float(args.holdout_pitch_mae_tol),
            "base_manager_overrides": str(args.base_manager_overrides),
            "manager_pitching": str(args.manager_pitching),
            "generated_at": datetime.now().isoformat(),
        },
        "baseline": {
            "starter_hook_add_pitches": baseline_value,
            "tune_pitch_mae": base_tune_pitch_mae,
            "tune_pitch_bias": base_tune_pitch_bias,
            "holdout_pitch_mae": base_hold_pitch_mae,
            "holdout_pitch_bias": base_hold_pitch_bias,
            "allmetrics_tune": baseline_all_tune,
            "hitterprops_tune": baseline_props_tune,
            "allmetrics_hold": baseline_all_hold,
            "hitterprops_hold": baseline_props_hold,
        },
        "candidates": [r.__dict__ for r in results],
        "best": (best.__dict__ if best is not None else None),
    }

    _write_json(out_root / "sweep_summary.json", sweep_summary)

    if best is None:
        print("No candidate passed guardrails.")
        return 0

    print("Best candidate:")
    print(json.dumps(best.__dict__, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
