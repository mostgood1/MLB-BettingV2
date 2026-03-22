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


def _parse_grid_vals(raw: str) -> List[int]:
    vals: List[int] = []
    for part in str(raw).split(","):
        s = part.strip()
        if not s:
            continue
        v = int(float(s))
        if v < 0:
            v = 0
        vals.append(int(v))
    if not vals:
        raise ValueError("Empty --grid-vals")
    # Stable unique
    out: List[int] = []
    seen = set()
    for v in vals:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


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


def _tag(v: int) -> str:
    return f"{int(v):03d}"


def _is_ok(score: Optional[float], floor: float) -> bool:
    return isinstance(score, (int, float)) and math.isfinite(float(score)) and float(score) >= float(floor)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Sweep manager_pitching_overrides['reliever_hard_cap_buffer'] and rank candidates using tune objectives with holdout floors.\n\n"
            "reliever_hard_cap_buffer forces a pull when pitch_count >= eff_stamina + buffer (smaller buffer => earlier forced pulls)."
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
        help="Base manager overrides JSON/path; candidates set reliever_hard_cap_buffer on top.",
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
        default="12,15,18,21,24",
        help="Comma-separated integer buffers (>=0). Baseline is 18.",
    )
    ap.add_argument("--holdout-tol", type=float, default=0.002)
    ap.add_argument("--w-allmetrics", type=float, default=1.0)
    ap.add_argument("--w-hitterprops", type=float, default=1.0)
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_mgr_reliever_hard_cap_buffer_quick",
        help="Root folder for this sweep's outputs.",
    )
    ap.add_argument(
        "--base-overrides",
        default="",
        help="Optional JSON/path merged into manager overrides before setting the lever.",
    )
    args = ap.parse_args()

    floor = 1.0 - float(args.holdout_tol)
    grid_vals = _parse_grid_vals(args.grid_vals)

    tune_date_file = Path(args.tune_date_file)
    if not tune_date_file.is_absolute():
        tune_date_file = (_ROOT / tune_date_file).resolve()
    holdout_date_file = Path(args.holdout_date_file)
    if not holdout_date_file.is_absolute():
        holdout_date_file = (_ROOT / holdout_date_file).resolve()
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

    baseline_value_default = 18
    baseline_overrides: Dict[str, Any] = {}
    baseline_overrides.update(base_manager_overrides)
    baseline_overrides.update(extra_base_overrides)

    try:
        baseline_value = int(float(baseline_overrides.get("reliever_hard_cap_buffer", baseline_value_default)))
    except Exception:
        baseline_value = baseline_value_default
    baseline_value = max(0, int(baseline_value))
    if "reliever_hard_cap_buffer" in baseline_overrides:
        baseline_overrides["reliever_hard_cap_buffer"] = int(baseline_value)

    def _make_overrides(v: int) -> Dict[str, Any]:
        merged: Dict[str, Any] = dict(baseline_overrides)
        if int(v) != int(baseline_value):
            merged["reliever_hard_cap_buffer"] = int(v)
        return merged

    # Baseline
    baseline_dir = run_root / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)
    baseline_overrides_path = baseline_dir / "manager_overrides.json"
    _write_json(baseline_overrides_path, _make_overrides(baseline_value))

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

    baseline_combined_tune = (float(args.w_allmetrics) * (baseline_all_tune or 1.0)) + (
        float(args.w_hitterprops) * (baseline_props_tune or 1.0)
    )

    leaderboard: List[Dict[str, Any]] = []
    best: Optional[Dict[str, Any]] = None
    best_score = -1e18

    for v in grid_vals:
        if int(v) == int(baseline_value):
            continue

        cand_dir = run_root / f"cand_{_tag(v)}"
        cand_dir.mkdir(parents=True, exist_ok=True)
        ov_path = cand_dir / "manager_overrides.json"
        _write_json(ov_path, _make_overrides(v))

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

        combined_tune = (float(args.w_allmetrics) * (all_tune or float("nan"))) + (
            float(args.w_hitterprops) * (props_tune or float("nan"))
        )
        combined_hold = (all_hold or float("nan")) + (props_hold or float("nan"))
        holdout_ok = _is_ok(all_hold, floor) and _is_ok(props_hold, floor)

        row = {
            "reliever_hard_cap_buffer": int(v),
            "allmetrics_tune": all_tune,
            "hitterprops_tune": props_tune,
            "combined_tune": combined_tune,
            "allmetrics_holdout": all_hold,
            "hitterprops_holdout": props_hold,
            "combined_holdout": combined_hold,
            "holdout_ok": bool(holdout_ok),
        }
        leaderboard.append(row)
        if holdout_ok and math.isfinite(float(combined_tune)) and float(combined_tune) > best_score:
            best_score = float(combined_tune)
            best = row

    leaderboard.sort(key=lambda d: float(d.get("combined_tune") or -1e18), reverse=True)
    _write_json(run_root / "leaderboard.json", leaderboard)

    promote_ok = False
    if best is not None and float(best.get("combined_tune") or -1e9) > float(baseline_combined_tune):
        promote_ok = True

    summary = {
        "lever": "reliever_hard_cap_buffer",
        "baseline_value": baseline_value,
        "baseline_combined_tune": baseline_combined_tune,
        "baseline_allmetrics_holdout": baseline_all_hold,
        "baseline_hitterprops_holdout": baseline_props_hold,
        "floor": floor,
        "best": best,
        "promote_ok": bool(promote_ok),
    }
    _write_json(run_root / "best.json", summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
