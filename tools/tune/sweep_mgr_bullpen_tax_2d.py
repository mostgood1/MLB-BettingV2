from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
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


def _parse_float_list(raw: str, *, nonneg: bool = False, positive: bool = False) -> List[float]:
    vals: List[float] = []
    for part in str(raw).split(","):
        s = part.strip()
        if not s:
            continue
        v = float(s)
        if not math.isfinite(v):
            continue
        if nonneg and v < 0.0:
            continue
        if positive and v <= 0.0:
            continue
        vals.append(float(v))
    if not vals:
        raise ValueError("Empty grid")
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


def _tag(scale: float, pitches: float) -> str:
    # scale*1000 + pitches*10, e.g., 0.20/140 -> s0200_p1400
    s = f"s{int(round(float(scale) * 1000.0)):04d}"
    p = f"p{int(round(float(pitches) * 10.0)):04d}"
    return f"{s}_{p}"


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "2D sweep over manager_pitching_overrides['bullpen_tax_scale'] and ['bullpen_tax_pitches'] "
            "and rank candidates using tune objectives with a holdout guardrail."
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
    ap.add_argument(
        "--prop-lines-source",
        choices=["auto", "oddsapi", "last_known", "bovada", "off"],
        default="last_known",
    )
    ap.add_argument("--manager-pitching", choices=["off", "legacy", "v2"], default="v2")
    ap.add_argument(
        "--base-manager-overrides",
        default="data/tuning/manager_pitching_overrides/default.json",
        help="Base manager overrides JSON/path; candidates set bullpen_tax_scale/bullpen_tax_pitches on top.",
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
        "--grid-scales",
        default="0.00,0.20,0.40,0.60",
        help="Comma-separated bullpen_tax_scale values (>=0).",
    )
    ap.add_argument(
        "--grid-pitches",
        default="100,120,140,160,180",
        help="Comma-separated bullpen_tax_pitches values (>0).",
    )
    ap.add_argument("--holdout-tol", type=float, default=0.002)
    ap.add_argument("--w-allmetrics", type=float, default=1.0)
    ap.add_argument("--w-hitterprops", type=float, default=1.0)
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_mgr_bullpen_tax_2d",
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

    if not tune_dates:
        print(f"No dates in: {tune_date_file}")
        return 2
    if not holdout_dates:
        print(f"No dates in: {holdout_date_file}")
        return 2

    scales = _parse_float_list(str(args.grid_scales), nonneg=True)
    pitches_list = _parse_float_list(str(args.grid_pitches), positive=True)

    base_overrides = _read_overrides(str(args.base_manager_overrides))
    baseline_scale = float(base_overrides.get("bullpen_tax_scale", 0.0) or 0.0)
    baseline_pitches = float(base_overrides.get("bullpen_tax_pitches", 0.0) or 0.0)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = (_ROOT / Path(str(args.batch_root)) / stamp).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

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

    meta = {
        "sims_per_game": int(args.sims_per_game),
        "jobs": int(args.jobs),
        "use_raw": str(args.use_raw),
        "prop_lines_source": str(args.prop_lines_source),
        "manager_pitching": str(args.manager_pitching),
        "base_manager_overrides": str(args.base_manager_overrides),
        "baseline_bullpen_tax_scale": float(baseline_scale),
        "baseline_bullpen_tax_pitches": float(baseline_pitches),
        "grid_scales": list(scales),
        "grid_pitches": list(pitches_list),
        "tune_dates": str(tune_dates_path),
        "holdout_dates": str(holdout_dates_path),
    }
    _write_json(out_root / "meta.json", meta)

    def _write_dates_file(dates: List[str], path: Path) -> None:
        path.write_text("\n".join(dates) + "\n", encoding="utf-8")

    tune_date_list_path = out_root / "_tune_dates_list.txt"
    holdout_date_list_path = out_root / "_holdout_dates_list.txt"
    _write_dates_file(tune_dates, tune_date_list_path)
    _write_dates_file(holdout_dates, holdout_date_list_path)

    leaderboard: List[Dict[str, Any]] = []

    # Baseline run
    baseline_dir = out_root / "baseline"
    baseline_tune_batch = baseline_dir / "tune_batch"
    baseline_holdout_batch = baseline_dir / "holdout_batch"
    baseline_tune_summary = baseline_dir / "tune_summary.json"
    baseline_holdout_summary = baseline_dir / "holdout_summary.json"

    rc = _run_batch(
        batch_runner=batch_runner,
        date_file=tune_date_list_path,
        out_dir=baseline_tune_batch,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=str(args.use_raw),
        prop_lines_source=str(args.prop_lines_source),
        manager_pitching=str(args.manager_pitching),
        manager_pitching_overrides=str(args.base_manager_overrides),
    )
    if rc != 0:
        print(f"Baseline tune run failed rc={rc}")
        return int(rc)
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_batch, out_path=baseline_tune_summary):
        print("Baseline tune summarize failed")
        return 2

    rc = _run_batch(
        batch_runner=batch_runner,
        date_file=holdout_date_list_path,
        out_dir=baseline_holdout_batch,
        sims_per_game=int(args.sims_per_game),
        jobs=int(args.jobs),
        use_raw=str(args.use_raw),
        prop_lines_source=str(args.prop_lines_source),
        manager_pitching=str(args.manager_pitching),
        manager_pitching_overrides=str(args.base_manager_overrides),
    )
    if rc != 0:
        print(f"Baseline holdout run failed rc={rc}")
        return int(rc)
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_holdout_batch, out_path=baseline_holdout_summary):
        print("Baseline holdout summarize failed")
        return 2

    baseline_entry: Dict[str, Any] = {
        **meta,
        "id": "baseline",
        "bullpen_tax_scale": baseline_scale,
        "bullpen_tax_pitches": baseline_pitches,
        "status": "ok",
        "tune": {"score_allmetrics": 1.0, "score_hitterprops": 1.0, "combined": float(args.w_allmetrics) + float(args.w_hitterprops)},
        "holdout": {"score_allmetrics": 1.0, "score_hitterprops": 1.0, "ok": True},
    }
    leaderboard.append(baseline_entry)

    best: Optional[Dict[str, Any]] = None

    def _iter_grid() -> List[Tuple[float, float]]:
        pairs: List[Tuple[float, float]] = []
        for s in scales:
            for p in pitches_list:
                pairs.append((float(s), float(p)))
        return pairs

    for scale, pitches in _iter_grid():
        # skip exact baseline (since baseline already run)
        if abs(float(scale) - float(baseline_scale)) < 1e-12 and abs(float(pitches) - float(baseline_pitches)) < 1e-9:
            continue

        tag = _tag(scale, pitches)
        cid = f"cand_{tag}"
        cand_dir = out_root / cid
        cand_tune_batch = cand_dir / "tune_batch"
        cand_holdout_batch = cand_dir / "holdout_batch"
        cand_tune_summary = cand_dir / "tune_summary.json"
        cand_holdout_summary = cand_dir / "holdout_summary.json"

        cand_overrides = dict(base_overrides)
        cand_overrides["bullpen_tax_scale"] = float(scale)
        cand_overrides["bullpen_tax_pitches"] = float(pitches)
        overrides_path = cand_dir / "manager_pitching_overrides.json"
        _write_json(overrides_path, cand_overrides)

        entry: Dict[str, Any] = {
            **meta,
            "id": cid,
            "bullpen_tax_scale": float(scale),
            "bullpen_tax_pitches": float(pitches),
            "manager_pitching_overrides": cand_overrides,
            "status": "ok",
        }

        rc = _run_batch(
            batch_runner=batch_runner,
            date_file=tune_date_list_path,
            out_dir=cand_tune_batch,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=str(args.use_raw),
            prop_lines_source=str(args.prop_lines_source),
            manager_pitching=str(args.manager_pitching),
            manager_pitching_overrides=str(overrides_path),
        )
        if rc != 0:
            entry["status"] = f"tune_failed_rc_{rc}"
            leaderboard.append(entry)
            continue
        if not _summarize_batch(summarizer=summarizer, batch_dir=cand_tune_batch, out_path=cand_tune_summary):
            entry["status"] = "tune_summarize_failed"
            leaderboard.append(entry)
            continue

        tune_score_all = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=cand_tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "tune_score_allmetrics.json",
        )
        tune_score_props = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=cand_tune_summary,
            baseline_summary=baseline_tune_summary,
            out_path=cand_dir / "tune_score_hitterprops.json",
        )
        if tune_score_all is None or tune_score_props is None:
            entry["status"] = "tune_score_failed"
            leaderboard.append(entry)
            continue

        entry["tune"] = {
            "score_allmetrics": float(tune_score_all),
            "score_hitterprops": float(tune_score_props),
            "combined": float(args.w_allmetrics) * float(tune_score_all) + float(args.w_hitterprops) * float(tune_score_props),
            "batch_dir": str(cand_tune_batch),
            "summary": str(cand_tune_summary),
        }

        rc = _run_batch(
            batch_runner=batch_runner,
            date_file=holdout_date_list_path,
            out_dir=cand_holdout_batch,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=str(args.use_raw),
            prop_lines_source=str(args.prop_lines_source),
            manager_pitching=str(args.manager_pitching),
            manager_pitching_overrides=str(overrides_path),
        )
        if rc != 0:
            entry["status"] = f"holdout_failed_rc_{rc}"
            leaderboard.append(entry)
            continue
        if not _summarize_batch(summarizer=summarizer, batch_dir=cand_holdout_batch, out_path=cand_holdout_summary):
            entry["status"] = "holdout_summarize_failed"
            leaderboard.append(entry)
            continue

        hold_score_all = _score_one(
            scorer=scorer,
            objective=obj_all,
            candidate_summary=cand_holdout_summary,
            baseline_summary=baseline_holdout_summary,
            out_path=cand_dir / "holdout_score_allmetrics.json",
        )
        hold_score_props = _score_one(
            scorer=scorer,
            objective=obj_props,
            candidate_summary=cand_holdout_summary,
            baseline_summary=baseline_holdout_summary,
            out_path=cand_dir / "holdout_score_hitterprops.json",
        )
        if hold_score_all is None or hold_score_props is None:
            entry["status"] = "holdout_score_failed"
            leaderboard.append(entry)
            continue

        ok = (float(hold_score_all) >= 1.0 - float(args.holdout_tol)) and (float(hold_score_props) >= 1.0 - float(args.holdout_tol))
        entry["holdout"] = {
            "score_allmetrics": float(hold_score_all),
            "score_hitterprops": float(hold_score_props),
            "ok": bool(ok),
            "batch_dir": str(cand_holdout_batch),
            "summary": str(cand_holdout_summary),
        }

        leaderboard.append(entry)

        if ok:
            if best is None or float(entry["tune"]["combined"]) > float(best["tune"]["combined"]):
                best = entry

    _write_json(out_root / "leaderboard.json", leaderboard)
    if best is not None:
        _write_json(out_root / "best.json", best)
        print(
            f"Best (guardrail-ok): {best['id']} bullpen_tax_scale={best['bullpen_tax_scale']} "
            f"bullpen_tax_pitches={best['bullpen_tax_pitches']} combined={best['tune']['combined']}"
        )
    else:
        print("No guardrail-ok candidate found")
    print(f"Sweep root: {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
