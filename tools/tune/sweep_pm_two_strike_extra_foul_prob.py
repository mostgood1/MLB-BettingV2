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

from sim_engine.pitch_model import PitchModelConfig


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


def _parse_grid_vals(raw: str) -> List[float]:
    vals: List[float] = []
    for part in str(raw).split(","):
        s = part.strip()
        if not s:
            continue
        v = float(s)
        if not math.isfinite(v) or v < 0.0:
            continue
        vals.append(float(v))
    if not vals:
        raise ValueError("Empty --grid-vals")
    return vals


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
    pitch_model_overrides: Optional[str],
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
        "--pitch-model-overrides",
        str(pitch_model_overrides or ""),
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
    val: float
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
    baseline_default = float(getattr(PitchModelConfig(), "two_strike_extra_foul_prob", 0.0) or 0.0)

    ap = argparse.ArgumentParser(
        description=(
            "Sweep PitchModelConfig.two_strike_extra_foul_prob and evaluate starter pitch-count realism.\n\n"
            "This knob inserts extra two-strike foul pitches (count unchanged) to increase pitches/PA "
            "with minimal expected impact on outcome rates.\n\n"
            "Guardrails: can optionally enforce the existing all-metrics + hitter-props holdout ratios."
        )
    )
    ap.add_argument(
        "--tune-date-file",
        default="data/eval/date_sets/random_feed_live_2025_regseason_50days_min10_seed2026.txt",
        help="Main tuning date set (regseason batch).",
    )
    ap.add_argument(
        "--holdout-date-file",
        default="data/eval/date_sets/holdout_disjoint_13days_from_pushPolicy20_half_s10_excluding_tune_random50.txt",
        help="Holdout/guardrail date set.",
    )
    ap.add_argument("--tune-n-days", type=int, default=0, help="If >0, only use first N tune dates")
    ap.add_argument("--holdout-n-days", type=int, default=0, help="If >0, only use first N holdout dates")
    ap.add_argument("--sims-per-game", type=int, default=200)
    ap.add_argument("--jobs", type=int, default=6)
    ap.add_argument("--use-raw", choices=["on", "off"], default="on")
    ap.add_argument(
        "--prop-lines-source",
        choices=["auto", "oddsapi", "last_known", "bovada", "off"],
        default="last_known",
    )
    ap.add_argument(
        "--objective-allmetrics",
        default="data/tuning/objectives/all_metrics_v3_tuned_best20260210b_random50.json",
        help="Objective file for team totals + core metrics (guardrail).",
    )
    ap.add_argument(
        "--objective-hitterprops",
        default="data/tuning/objectives/hitter_props_topn_v1_baseline_20260218_random50_s250.json",
        help="Objective file for hitter props (guardrail).",
    )
    ap.add_argument(
        "--grid-vals",
        default="0.03,0.06,0.09,0.12,0.15",
        help=(
            "Comma-separated grid values to try for two_strike_extra_foul_prob. "
            f"Baseline default is {baseline_default:.4f}; that value is auto-skipped if present in the grid."
        ),
    )
    ap.add_argument(
        "--holdout-tol",
        type=float,
        default=0.002,
        help="Allowable regression on holdout vs holdout-baseline for the ratio-scored guardrail objectives.",
    )
    ap.add_argument(
        "--holdout-pitch-mae-tol",
        type=float,
        default=0.25,
        help="Allowable regression (absolute) in holdout pitch MAE vs holdout-baseline.",
    )
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_pm_two_strike_extra_foul_prob",
        help="Root folder for this sweep's outputs.",
    )
    ap.add_argument(
        "--reuse-baseline-from",
        default="",
        help=(
            "Optional path to a prior sweep output folder (the timestamp folder) to reuse its baseline. "
            "If set, expects <path>/baseline/{tune_batch,holdout_batch,tune_summary.json,holdout_summary.json}. "
            "Useful for strict follow-ups where you want to compare multiple candidates against the same baseline "
            "without recomputing it each time."
        ),
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
    if not tune_dates:
        print(f"No dates in: {tune_date_file}")
        return 2
    if not holdout_dates:
        print(f"No dates in: {holdout_date_file}")
        return 2

    if int(args.tune_n_days) > 0:
        tune_dates = tune_dates[: int(args.tune_n_days)]
    if int(args.holdout_n_days) > 0:
        holdout_dates = holdout_dates[: int(args.holdout_n_days)]

    grid_vals = _parse_grid_vals(str(args.grid_vals))

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = (_ROOT / Path(str(args.batch_root)) / stamp).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    tune_dates_path = out_root / "tune_dates.txt"
    holdout_dates_path = out_root / "holdout_dates.txt"
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

    baseline_dir = out_root / "baseline"
    reuse_root_raw = str(args.reuse_baseline_from or "").strip()
    if reuse_root_raw:
        reuse_root = Path(reuse_root_raw)
        if not reuse_root.is_absolute():
            reuse_root = (_ROOT / reuse_root).resolve()
        reuse_baseline_dir = (reuse_root / "baseline").resolve()
        baseline_tune_batch = reuse_baseline_dir / "tune_batch"
        baseline_holdout_batch = reuse_baseline_dir / "holdout_batch"
        baseline_tune_summary = reuse_baseline_dir / "tune_summary.json"
        baseline_holdout_summary = reuse_baseline_dir / "holdout_summary.json"
        missing = [
            p
            for p in [
                baseline_tune_batch,
                baseline_holdout_batch,
                baseline_tune_summary,
                baseline_holdout_summary,
            ]
            if not p.exists()
        ]
        if missing:
            print("--reuse-baseline-from is missing required paths:")
            for p in missing:
                print(f"  - {p}")
            return 2
    else:
        baseline_dir.mkdir(parents=True, exist_ok=True)

        baseline_tune_batch = baseline_dir / "tune_batch"
        baseline_holdout_batch = baseline_dir / "holdout_batch"

        for bdir, dates_path in [(baseline_tune_batch, tune_dates_path), (baseline_holdout_batch, holdout_dates_path)]:
            rc = _run_batch(
                batch_runner=batch_runner,
                date_file=dates_path,
                out_dir=bdir,
                sims_per_game=int(args.sims_per_game),
                jobs=int(args.jobs),
                use_raw=str(args.use_raw),
                prop_lines_source=str(args.prop_lines_source),
                pitch_model_overrides=None,
            )
            if rc != 0:
                print(f"Baseline batch failed (exit={rc}): {bdir}")
                return rc

        baseline_tune_summary = baseline_dir / "tune_summary.json"
        baseline_holdout_summary = baseline_dir / "holdout_summary.json"
        if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_batch, out_path=baseline_tune_summary):
            print("Baseline tune summarize failed")
            return 2
        if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_holdout_batch, out_path=baseline_holdout_summary):
            print("Baseline holdout summarize failed")
            return 2

    base_tune_pitch_mae, base_tune_pitch_bias, _ = _extract_pitch_mae(baseline_tune_batch)
    base_hold_pitch_mae, base_hold_pitch_bias, _ = _extract_pitch_mae(baseline_holdout_batch)

    base_all_tune = _score_one(
        scorer=scorer,
        objective=obj_all,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "score_allmetrics_tune.json",
    )
    base_props_tune = _score_one(
        scorer=scorer,
        objective=obj_props,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "score_hitterprops_tune.json",
    )
    base_all_hold = _score_one(
        scorer=scorer,
        objective=obj_all,
        candidate_summary=baseline_holdout_summary,
        baseline_summary=baseline_holdout_summary,
        out_path=baseline_dir / "score_allmetrics_holdout.json",
    )
    base_props_hold = _score_one(
        scorer=scorer,
        objective=obj_props,
        candidate_summary=baseline_holdout_summary,
        baseline_summary=baseline_holdout_summary,
        out_path=baseline_dir / "score_hitterprops_holdout.json",
    )

    results: List[CandidateResult] = []

    for v in grid_vals:
        if abs(float(v) - float(baseline_default)) < 1e-12:
            continue

        cand_dir = out_root / f"cand_{v:.4f}".replace(".", "p")
        cand_dir.mkdir(parents=True, exist_ok=True)

        overrides = json.dumps({"two_strike_extra_foul_prob": float(v)})

        cand_tune_batch = cand_dir / "tune_batch"
        cand_hold_batch = cand_dir / "holdout_batch"

        for bdir, dates_path in [(cand_tune_batch, tune_dates_path), (cand_hold_batch, holdout_dates_path)]:
            rc = _run_batch(
                batch_runner=batch_runner,
                date_file=dates_path,
                out_dir=bdir,
                sims_per_game=int(args.sims_per_game),
                jobs=int(args.jobs),
                use_raw=str(args.use_raw),
                prop_lines_source=str(args.prop_lines_source),
                pitch_model_overrides=overrides,
            )
            if rc != 0:
                print(f"Candidate {v} batch failed (exit={rc}): {bdir}")
                return rc

        cand_tune_summary = cand_dir / "tune_summary.json"
        cand_hold_summary = cand_dir / "holdout_summary.json"
        if not _summarize_batch(summarizer=summarizer, batch_dir=cand_tune_batch, out_path=cand_tune_summary):
            print(f"Candidate {v} tune summarize failed")
            return 2
        if not _summarize_batch(summarizer=summarizer, batch_dir=cand_hold_batch, out_path=cand_hold_summary):
            print(f"Candidate {v} holdout summarize failed")
            return 2

        tune_pitch_mae, tune_pitch_bias, _ = _extract_pitch_mae(cand_tune_batch)
        hold_pitch_mae, hold_pitch_bias, _ = _extract_pitch_mae(cand_hold_batch)

        s_all_tune = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=cand_tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "score_allmetrics_tune.json",
        )
        s_props_tune = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=cand_tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "score_hitterprops_tune.json",
        )
        s_all_hold = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=cand_hold_summary,
            baseline_summary=baseline_holdout_summary,
            out_path=cand_dir / "score_allmetrics_holdout.json",
        )
        s_props_hold = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=cand_hold_summary,
            baseline_summary=baseline_holdout_summary,
            out_path=cand_dir / "score_hitterprops_holdout.json",
        )

        # Guardrails
        passed = True
        ht = float(args.holdout_tol)
        if s_all_hold is not None and s_all_hold < (1.0 - ht):
            passed = False
        if s_props_hold is not None and s_props_hold < (1.0 - ht):
            passed = False

        if base_hold_pitch_mae is not None and hold_pitch_mae is not None:
            if float(hold_pitch_mae) > float(base_hold_pitch_mae) + float(args.holdout_pitch_mae_tol):
                passed = False

        results.append(
            CandidateResult(
                val=float(v),
                tune_pitch_mae=tune_pitch_mae,
                tune_pitch_bias=tune_pitch_bias,
                hold_pitch_mae=hold_pitch_mae,
                hold_pitch_bias=hold_pitch_bias,
                allmetrics_tune=s_all_tune,
                hitterprops_tune=s_props_tune,
                allmetrics_hold=s_all_hold,
                hitterprops_hold=s_props_hold,
                passed_guardrail=bool(passed),
            )
        )

    # Rank: best holdout pitch MAE among guardrail passers; fallback to tune MAE.
    def _key(r: CandidateResult) -> Tuple[float, float]:
        hp = r.hold_pitch_mae if isinstance(r.hold_pitch_mae, (int, float)) else 1e9
        tp = r.tune_pitch_mae if isinstance(r.tune_pitch_mae, (int, float)) else 1e9
        return (float(hp), float(tp))

    best = None
    for r in sorted([x for x in results if x.passed_guardrail], key=_key):
        best = r
        break

    out_obj: Dict[str, Any] = {
        "meta": {
            "baseline_default": float(baseline_default),
            "grid_vals": [float(x) for x in grid_vals],
            "sims_per_game": int(args.sims_per_game),
            "jobs": int(args.jobs),
            "tune_date_file": str(tune_date_file),
            "holdout_date_file": str(holdout_date_file),
            "tune_days": int(len(tune_dates)),
            "holdout_days": int(len(holdout_dates)),
            "holdout_tol": float(args.holdout_tol),
            "holdout_pitch_mae_tol": float(args.holdout_pitch_mae_tol),
            "reuse_baseline_from": (reuse_root_raw if reuse_root_raw else None),
            "generated_at": datetime.now().isoformat(),
        },
        "baseline": {
            "tune_pitch_mae": base_tune_pitch_mae,
            "tune_pitch_bias": base_tune_pitch_bias,
            "holdout_pitch_mae": base_hold_pitch_mae,
            "holdout_pitch_bias": base_hold_pitch_bias,
            "allmetrics_tune": base_all_tune,
            "hitterprops_tune": base_props_tune,
            "allmetrics_hold": base_all_hold,
            "hitterprops_hold": base_props_hold,
        },
        "candidates": [r.__dict__ for r in results],
        "best": (best.__dict__ if best else None),
    }

    _write_json(out_root / "sweep_summary.json", out_obj)

    if best is not None:
        print(
            "Best candidate: "
            f"two_strike_extra_foul_prob={best.val:.4f} "
            f"(hold_pitch_mae={best.hold_pitch_mae}, tune_pitch_mae={best.tune_pitch_mae})"
        )
    else:
        print("No candidate passed guardrails")

    print(f"Wrote: {out_root / 'sweep_summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
