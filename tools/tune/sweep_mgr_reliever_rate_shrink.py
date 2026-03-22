from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


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


def _parse_grid_vals(raw: str) -> List[float]:
    vals: List[float] = []
    for part in str(raw).split(","):
        s = part.strip()
        if not s:
            continue
        v = float(s)
        if not math.isfinite(v):
            continue
        v = max(0.0, min(1.0, float(v)))
        vals.append(float(v))
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
    rc = _run(
        [sys.executable, str(summarizer), "--batch-dir", str(batch_dir), "--out", str(out_path)],
        cwd=_ROOT,
    )
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


def _tag(v: float) -> str:
    # shrink*1000 (e.g., 0.20 -> 0200)
    return f"{int(round(float(v) * 1000.0)):04d}"


def _is_ok(score: Optional[float], floor: float) -> bool:
    return isinstance(score, (int, float)) and math.isfinite(float(score)) and float(score) >= float(floor)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Sweep manager_pitching_overrides['reliever_rate_shrink'] and rank candidates using tune objectives with a holdout guardrail.\n\n"
            "reliever_rate_shrink blends reliever day rates toward neutral priors to reduce overconfidence in bullpen strength when usage rises."
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
        help="Base manager overrides JSON/path; candidates set reliever_rate_shrink on top.",
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
        default="0.00,0.10,0.20,0.30,0.40",
        help="Comma-separated reliever_rate_shrink values in [0,1]. Baseline value from base overrides is auto-skipped if present.",
    )
    ap.add_argument("--holdout-tol", type=float, default=0.002)
    ap.add_argument("--w-allmetrics", type=float, default=1.0)
    ap.add_argument("--w-hitterprops", type=float, default=1.0)
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_mgr_reliever_rate_shrink",
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
    hold_dates = _read_text_dates(holdout_date_file)

    if args.tune_n_days and args.tune_n_days > 0:
        tune_dates = tune_dates[: int(args.tune_n_days)]
    if args.holdout_n_days and args.holdout_n_days > 0:
        hold_dates = hold_dates[: int(args.holdout_n_days)]

    if not tune_dates:
        print("No tune dates")
        return 2
    if not hold_dates:
        print("No holdout dates")
        return 2

    grid = _parse_grid_vals(args.grid_vals)

    batch_root = Path(args.batch_root)
    if not batch_root.is_absolute():
        batch_root = (_ROOT / batch_root).resolve()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = (batch_root / ts).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    runner = (_ROOT / "tools" / "eval" / "run_batch_eval_days.py").resolve()
    summarizer = (_ROOT / "tools" / "eval" / "summarize_batch_eval.py").resolve()
    scorer = (_ROOT / "tools" / "tune" / "score_batch_summary.py").resolve()

    objective_all = Path(args.objective_allmetrics)
    if not objective_all.is_absolute():
        objective_all = (_ROOT / objective_all).resolve()
    objective_props = Path(args.objective_hitterprops)
    if not objective_props.is_absolute():
        objective_props = (_ROOT / objective_props).resolve()

    base_overrides = _read_overrides(args.base_manager_overrides)
    baseline_val = base_overrides.get("reliever_rate_shrink")
    try:
        baseline_val_f = max(0.0, min(1.0, float(baseline_val))) if baseline_val is not None else 0.0
    except Exception:
        baseline_val_f = 0.0

    # Write per-run date subsets for reproducibility.
    tune_dates_path = out_root / "tune_dates.txt"
    hold_dates_path = out_root / "holdout_dates.txt"
    tune_dates_path.write_text("\n".join(tune_dates) + "\n", encoding="utf-8")
    hold_dates_path.write_text("\n".join(hold_dates) + "\n", encoding="utf-8")

    meta = {
        "lever": "reliever_rate_shrink",
        "baseline_value": baseline_val_f,
        "grid": grid,
        "tune_n_days": len(tune_dates),
        "holdout_n_days": len(hold_dates),
        "sims_per_game": int(args.sims_per_game),
        "jobs": int(args.jobs),
        "use_raw": args.use_raw,
        "prop_lines_source": args.prop_lines_source,
        "manager_pitching": args.manager_pitching,
        "holdout_tol": float(args.holdout_tol),
        "w_allmetrics": float(args.w_allmetrics),
        "w_hitterprops": float(args.w_hitterprops),
        "objective_allmetrics": str(objective_all),
        "objective_hitterprops": str(objective_props),
        "timestamp": ts,
        "out_root": str(out_root),
    }
    _write_json(out_root / "meta.json", meta)

    # Baseline
    baseline_dir = out_root / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    baseline_overrides_path = baseline_dir / "manager_overrides.json"
    _write_json(baseline_overrides_path, base_overrides)

    baseline_tune_dir = baseline_dir / "tune_batch"
    baseline_hold_dir = baseline_dir / "holdout_batch"

    # Run baseline tune/holdout
    if _run_batch(
        batch_runner=runner,
        date_file=tune_dates_path,
        out_dir=baseline_tune_dir,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=args.use_raw,
        prop_lines_source=args.prop_lines_source,
        manager_pitching=args.manager_pitching,
        manager_pitching_overrides=str(baseline_overrides_path),
    ) != 0:
        print("Baseline tune batch failed")
        return 1

    if _run_batch(
        batch_runner=runner,
        date_file=hold_dates_path,
        out_dir=baseline_hold_dir,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=args.use_raw,
        prop_lines_source=args.prop_lines_source,
        manager_pitching=args.manager_pitching,
        manager_pitching_overrides=str(baseline_overrides_path),
    ) != 0:
        print("Baseline holdout batch failed")
        return 1

    baseline_tune_summary = baseline_dir / "tune_summary.json"
    baseline_hold_summary = baseline_dir / "holdout_summary.json"
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_dir, out_path=baseline_tune_summary):
        print("Baseline tune summarize failed")
        return 1
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_hold_dir, out_path=baseline_hold_summary):
        print("Baseline holdout summarize failed")
        return 1

    baseline_all_tune = _score_one(
        scorer=scorer,
        objective=objective_all,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "baseline_allmetrics_tune_score.json",
    )
    baseline_props_tune = _score_one(
        scorer=scorer,
        objective=objective_props,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "baseline_hitterprops_tune_score.json",
    )
    baseline_all_hold = _score_one(
        scorer=scorer,
        objective=objective_all,
        candidate_summary=baseline_hold_summary,
        baseline_summary=baseline_hold_summary,
        out_path=baseline_dir / "baseline_allmetrics_holdout_score.json",
    )
    baseline_props_hold = _score_one(
        scorer=scorer,
        objective=objective_props,
        candidate_summary=baseline_hold_summary,
        baseline_summary=baseline_hold_summary,
        out_path=baseline_dir / "baseline_hitterprops_holdout_score.json",
    )

    floor = 1.0 - float(args.holdout_tol)

    rows: List[Dict[str, Any]] = []

    for v in grid:
        # Skip redundant candidate if it matches baseline.
        if abs(float(v) - float(baseline_val_f)) < 1e-12:
            continue

        cand_dir = out_root / f"cand_{_tag(v)}"
        cand_dir.mkdir(parents=True, exist_ok=True)

        overrides = dict(base_overrides)
        overrides["reliever_rate_shrink"] = float(v)
        overrides_path = cand_dir / "manager_overrides.json"
        _write_json(overrides_path, overrides)

        tune_dir = cand_dir / "tune_batch"
        hold_dir = cand_dir / "holdout_batch"

        rc1 = _run_batch(
            batch_runner=runner,
            date_file=tune_dates_path,
            out_dir=tune_dir,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=args.use_raw,
            prop_lines_source=args.prop_lines_source,
            manager_pitching=args.manager_pitching,
            manager_pitching_overrides=str(overrides_path),
        )
        rc2 = _run_batch(
            batch_runner=runner,
            date_file=hold_dates_path,
            out_dir=hold_dir,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=args.use_raw,
            prop_lines_source=args.prop_lines_source,
            manager_pitching=args.manager_pitching,
            manager_pitching_overrides=str(overrides_path),
        )
        if rc1 != 0 or rc2 != 0:
            rows.append(
                {
                    "reliever_rate_shrink": float(v),
                    "failed": True,
                }
            )
            continue

        tune_summary = cand_dir / "tune_summary.json"
        hold_summary = cand_dir / "holdout_summary.json"
        if not _summarize_batch(summarizer=summarizer, batch_dir=tune_dir, out_path=tune_summary):
            rows.append({"reliever_rate_shrink": float(v), "failed": True})
            continue
        if not _summarize_batch(summarizer=summarizer, batch_dir=hold_dir, out_path=hold_summary):
            rows.append({"reliever_rate_shrink": float(v), "failed": True})
            continue

        all_tune = _score_one(
            scorer=scorer,
            objective=objective_all,
            candidate_summary=tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "allmetrics_tune_score.json",
        )
        props_tune = _score_one(
            scorer=scorer,
            objective=objective_props,
            candidate_summary=tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "hitterprops_tune_score.json",
        )
        all_hold = _score_one(
            scorer=scorer,
            objective=objective_all,
            candidate_summary=hold_summary,
            baseline_summary=baseline_hold_summary,
            out_path=cand_dir / "allmetrics_holdout_score.json",
        )
        props_hold = _score_one(
            scorer=scorer,
            objective=objective_props,
            candidate_summary=hold_summary,
            baseline_summary=baseline_hold_summary,
            out_path=cand_dir / "hitterprops_holdout_score.json",
        )

        ok = _is_ok(all_hold, floor) and _is_ok(props_hold, floor)

        combined_tune = 0.0
        if isinstance(all_tune, (int, float)):
            combined_tune += float(args.w_allmetrics) * float(all_tune)
        if isinstance(props_tune, (int, float)):
            combined_tune += float(args.w_hitterprops) * float(props_tune)

        combined_hold = 0.0
        if isinstance(all_hold, (int, float)):
            combined_hold += float(args.w_allmetrics) * float(all_hold)
        if isinstance(props_hold, (int, float)):
            combined_hold += float(args.w_hitterprops) * float(props_hold)

        rows.append(
            {
                "reliever_rate_shrink": float(v),
                "allmetrics_tune": all_tune,
                "hitterprops_tune": props_tune,
                "combined_tune": combined_tune,
                "allmetrics_holdout": all_hold,
                "hitterprops_holdout": props_hold,
                "combined_holdout": combined_hold,
                "holdout_ok": bool(ok),
                "failed": False,
            }
        )

    # Leaderboard: only non-failed candidates, sorted by (holdout_ok desc, combined_tune desc)
    rows_ok = [r for r in rows if not r.get("failed")]
    rows_ok.sort(key=lambda r: (bool(r.get("holdout_ok")), float(r.get("combined_tune") or -1e9)), reverse=True)

    _write_json(out_root / "leaderboard.json", rows_ok)

    best: Optional[Dict[str, Any]] = None
    for r in rows_ok:
        if r.get("holdout_ok"):
            best = r
            break

    # Default baseline tune combined score is w_all+w_props (ratio 1+1).
    baseline_combined = float(args.w_allmetrics) * 1.0 + float(args.w_hitterprops) * 1.0

    best_out = {
        "lever": "reliever_rate_shrink",
        "baseline_value": baseline_val_f,
        "baseline_combined_tune": baseline_combined,
        "baseline_allmetrics_tune": baseline_all_tune,
        "baseline_hitterprops_tune": baseline_props_tune,
        "baseline_allmetrics_holdout": baseline_all_hold,
        "baseline_hitterprops_holdout": baseline_props_hold,
        "floor": floor,
        "best": best,
        "promote_ok": bool(best and float(best.get("combined_tune") or 0.0) > float(baseline_combined) and bool(best.get("holdout_ok"))),
    }
    _write_json(out_root / "best.json", best_out)

    # Convenience: if no holdout-ok candidate, still record top by combined_tune.
    if best is None and rows_ok:
        best_out["best"] = rows_ok[0]
        best_out["promote_ok"] = False
        _write_json(out_root / "best.json", best_out)

    print(json.dumps(best_out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
