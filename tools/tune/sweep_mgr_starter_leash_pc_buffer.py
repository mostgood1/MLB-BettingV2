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
        vals.append(max(0, int(v)))
    if not vals:
        raise ValueError("Empty --grid-vals")
    vals = sorted(set(int(v) for v in vals))
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


def _is_ok(score: Optional[float], floor: float) -> bool:
    return isinstance(score, (int, float)) and math.isfinite(float(score)) and float(score) >= float(floor)


def _coerce_int(v: Any, default: int) -> int:
    try:
        out = int(float(v))
    except Exception:
        return int(default)
    return int(max(0, out))


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Sweep manager_pitching_overrides['starter_leash_pc_buffer'] and evaluate with objective scoring + holdout guardrails.\n\n"
            "starter_leash_pc_buffer is the pitch-count buffer added to eff_hook during the early-inning leash window.\n"
            "Smaller values allow earlier leash breaks (more earlier pulls); larger values force longer leash." 
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
        help="Base manager overrides JSON/path; candidates set starter_leash_pc_buffer on top.",
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
        default="0,5,10,15,20,25,30",
        help=(
            "Comma-separated ints. Baseline is read from --base-manager-overrides "
            "(or code default=20 if absent)."
        ),
    )
    ap.add_argument("--holdout-tol", type=float, default=0.002)
    ap.add_argument(
        "--holdout-pitch-mae-tol",
        type=float,
        default=0.25,
        help="Allowable regression (absolute) in holdout starter pitch MAE vs holdout-baseline.",
    )
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_mgr_starter_leash_pc_buffer_quick",
        help="Batch root; run is written under batch-root/YYYYMMDD_HHMMSS/",
    )
    ap.add_argument(
        "--base-overrides",
        default="",
        help="Optional JSON/path merged into manager overrides before setting the lever (handy for local experiments).",
    )

    args = ap.parse_args()

    grid_vals = _parse_grid_vals(args.grid_vals)
    floor = 1.0 - float(args.holdout_tol)

    tune_date_file = (_ROOT / args.tune_date_file).resolve()
    holdout_date_file = (_ROOT / args.holdout_date_file).resolve()
    if not tune_date_file.exists():
        raise FileNotFoundError(str(tune_date_file))
    if not holdout_date_file.exists():
        raise FileNotFoundError(str(holdout_date_file))

    tune_dates = _read_text_dates(tune_date_file)
    hold_dates = _read_text_dates(holdout_date_file)
    if int(args.tune_n_days) > 0:
        tune_dates = tune_dates[: int(args.tune_n_days)]
    if int(args.holdout_n_days) > 0:
        hold_dates = hold_dates[: int(args.holdout_n_days)]
    if not tune_dates or not hold_dates:
        raise ValueError("Empty tune/holdout dates")

    run_root = (_ROOT / args.batch_root / datetime.now().strftime("%Y%m%d_%H%M%S")).resolve()
    run_root.mkdir(parents=True, exist_ok=True)

    tune_file = run_root / "tune_dates.txt"
    hold_file = run_root / "holdout_dates.txt"
    tune_file.write_text("\n".join(tune_dates) + "\n", encoding="utf-8")
    hold_file.write_text("\n".join(hold_dates) + "\n", encoding="utf-8")

    batch_runner = (_ROOT / "tools" / "eval" / "run_batch_eval_days.py").resolve()
    summarizer = (_ROOT / "tools" / "eval" / "summarize_batch_eval.py").resolve()
    scorer = (_ROOT / "tools" / "tune" / "score_batch_summary.py").resolve()
    obj_all = (_ROOT / args.objective_allmetrics).resolve()
    obj_props = (_ROOT / args.objective_hitterprops).resolve()

    for p in [batch_runner, summarizer, scorer, obj_all, obj_props]:
        if not p.exists():
            raise FileNotFoundError(str(p))

    base_manager_overrides = _read_overrides(args.base_manager_overrides)
    extra_base_overrides = _read_overrides(args.base_overrides)

    merged_base_overrides: Dict[str, Any] = {}
    merged_base_overrides.update(base_manager_overrides)
    merged_base_overrides.update(extra_base_overrides)

    baseline_value = _coerce_int(merged_base_overrides.get("starter_leash_pc_buffer", 20), 20)

    def _make_overrides(v: int) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(merged_base_overrides)
        if int(v) != int(baseline_value):
            merged["starter_leash_pc_buffer"] = int(max(0, int(v)))
        return merged

    def _tag(v: int) -> str:
        return f"b{int(v):03d}"

    # Baseline
    baseline_dir = run_root / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)
    baseline_overrides = _make_overrides(baseline_value)
    baseline_overrides_path = baseline_dir / "manager_overrides.json"
    _write_json(baseline_overrides_path, baseline_overrides)

    baseline_tune_batch = baseline_dir / "tune_batch"
    baseline_hold_batch = baseline_dir / "holdout_batch"
    rc = _run_batch(
        batch_runner=batch_runner,
        date_file=tune_file,
        out_dir=baseline_tune_batch,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=args.use_raw,
        prop_lines_source=args.prop_lines_source,
        manager_pitching=args.manager_pitching,
        manager_pitching_overrides=str(baseline_overrides_path),
    )
    if rc != 0:
        return rc
    rc = _run_batch(
        batch_runner=batch_runner,
        date_file=hold_file,
        out_dir=baseline_hold_batch,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=args.use_raw,
        prop_lines_source=args.prop_lines_source,
        manager_pitching=args.manager_pitching,
        manager_pitching_overrides=str(baseline_overrides_path),
    )
    if rc != 0:
        return rc

    baseline_tune_summary = baseline_dir / "tune_summary.json"
    baseline_hold_summary = baseline_dir / "holdout_summary.json"
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_batch, out_path=baseline_tune_summary):
        return 2
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_hold_batch, out_path=baseline_hold_summary):
        return 2

    baseline_all_tune = _score_one(
        scorer=scorer,
        objective=obj_all,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "allmetrics_tune_score.json",
    )
    baseline_props_tune = _score_one(
        scorer=scorer,
        objective=obj_props,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "hitterprops_tune_score.json",
    )
    baseline_all_hold = _score_one(
        scorer=scorer,
        objective=obj_all,
        candidate_summary=baseline_hold_summary,
        baseline_summary=baseline_hold_summary,
        out_path=baseline_dir / "allmetrics_holdout_score.json",
    )
    baseline_props_hold = _score_one(
        scorer=scorer,
        objective=obj_props,
        candidate_summary=baseline_hold_summary,
        baseline_summary=baseline_hold_summary,
        out_path=baseline_dir / "hitterprops_holdout_score.json",
    )

    baseline_combined_tune = (baseline_all_tune or 1.0) + (baseline_props_tune or 1.0)
    (baseline_tune_pitch_mae, baseline_tune_pitch_bias, _) = _extract_pitch_mae(baseline_tune_batch)
    (baseline_hold_pitch_mae, baseline_hold_pitch_bias, _) = _extract_pitch_mae(baseline_hold_batch)

    results: List[CandidateResult] = []

    for v in grid_vals:
        if int(v) == int(baseline_value):
            continue
        cand_dir = run_root / f"cand_{_tag(v)}"
        cand_dir.mkdir(parents=True, exist_ok=True)
        ov = _make_overrides(int(v))
        ov_path = cand_dir / "manager_overrides.json"
        _write_json(ov_path, ov)

        tune_batch = cand_dir / "tune_batch"
        hold_batch = cand_dir / "holdout_batch"
        rc = _run_batch(
            batch_runner=batch_runner,
            date_file=tune_file,
            out_dir=tune_batch,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=args.use_raw,
            prop_lines_source=args.prop_lines_source,
            manager_pitching=args.manager_pitching,
            manager_pitching_overrides=str(ov_path),
        )
        if rc != 0:
            return rc
        rc = _run_batch(
            batch_runner=batch_runner,
            date_file=hold_file,
            out_dir=hold_batch,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=args.use_raw,
            prop_lines_source=args.prop_lines_source,
            manager_pitching=args.manager_pitching,
            manager_pitching_overrides=str(ov_path),
        )
        if rc != 0:
            return rc

        tune_summary = cand_dir / "tune_summary.json"
        hold_summary = cand_dir / "holdout_summary.json"
        if not _summarize_batch(summarizer=summarizer, batch_dir=tune_batch, out_path=tune_summary):
            return 2
        if not _summarize_batch(summarizer=summarizer, batch_dir=hold_batch, out_path=hold_summary):
            return 2

        all_tune = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "allmetrics_tune_score.json",
        )
        props_tune = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "hitterprops_tune_score.json",
        )
        all_hold = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=hold_summary,
            baseline_summary=baseline_hold_summary,
            out_path=cand_dir / "allmetrics_holdout_score.json",
        )
        props_hold = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=hold_summary,
            baseline_summary=baseline_hold_summary,
            out_path=cand_dir / "hitterprops_holdout_score.json",
        )

        (t_mae, t_bias, _) = _extract_pitch_mae(tune_batch)
        (h_mae, h_bias, _) = _extract_pitch_mae(hold_batch)
        pitch_guard_ok = True
        if isinstance(baseline_hold_pitch_mae, (int, float)) and isinstance(h_mae, (int, float)):
            pitch_guard_ok = float(h_mae) <= float(baseline_hold_pitch_mae) + float(args.holdout_pitch_mae_tol)

        holdout_ok = _is_ok(all_hold, floor) and _is_ok(props_hold, floor) and bool(pitch_guard_ok)
        results.append(
            CandidateResult(
                val=int(v),
                tune_pitch_mae=t_mae,
                tune_pitch_bias=t_bias,
                hold_pitch_mae=h_mae,
                hold_pitch_bias=h_bias,
                allmetrics_tune=all_tune,
                hitterprops_tune=props_tune,
                allmetrics_hold=all_hold,
                hitterprops_hold=props_hold,
                passed_guardrail=bool(holdout_ok),
            )
        )

    leaderboard: List[Dict[str, Any]] = []
    best: Optional[Dict[str, Any]] = None
    best_combined = -1e9

    for r in results:
        combined_tune = (r.allmetrics_tune or float("nan")) + (r.hitterprops_tune or float("nan"))
        combined_hold = (r.allmetrics_hold or float("nan")) + (r.hitterprops_hold or float("nan"))
        row = {
            "starter_leash_pc_buffer": int(r.val),
            "tune_pitch_mae": r.tune_pitch_mae,
            "tune_pitch_bias": r.tune_pitch_bias,
            "hold_pitch_mae": r.hold_pitch_mae,
            "hold_pitch_bias": r.hold_pitch_bias,
            "allmetrics_tune": r.allmetrics_tune,
            "hitterprops_tune": r.hitterprops_tune,
            "combined_tune": combined_tune,
            "allmetrics_holdout": r.allmetrics_hold,
            "hitterprops_holdout": r.hitterprops_hold,
            "combined_holdout": combined_hold,
            "holdout_ok": bool(r.passed_guardrail),
        }
        leaderboard.append(row)
        if r.passed_guardrail and math.isfinite(float(combined_tune)) and float(combined_tune) > best_combined:
            best_combined = float(combined_tune)
            best = row

    leaderboard.sort(key=lambda d: float(d.get("combined_tune") or -1e18), reverse=True)
    _write_json(run_root / "leaderboard.json", leaderboard)

    promote_ok = False
    if best is not None and float(best.get("combined_tune") or -1e9) > float(baseline_combined_tune):
        promote_ok = True

    summary = {
        "lever": "starter_leash_pc_buffer",
        "baseline_value": int(baseline_value),
        "baseline_combined_tune": baseline_combined_tune,
        "baseline_tune_pitch_mae": baseline_tune_pitch_mae,
        "baseline_tune_pitch_bias": baseline_tune_pitch_bias,
        "baseline_hold_pitch_mae": baseline_hold_pitch_mae,
        "baseline_hold_pitch_bias": baseline_hold_pitch_bias,
        "baseline_allmetrics_holdout": baseline_all_hold,
        "baseline_hitterprops_holdout": baseline_props_hold,
        "floor": floor,
        "holdout_pitch_mae_tol": float(args.holdout_pitch_mae_tol),
        "best": best,
        "promote_ok": bool(promote_ok),
    }
    _write_json(run_root / "best.json", summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
