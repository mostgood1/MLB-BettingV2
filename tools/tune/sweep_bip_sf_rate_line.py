from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional


_ROOT = Path(__file__).resolve().parents[2]


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
        vals.append(v)
    if not vals:
        raise ValueError("Empty --grid-vals")
    return vals


def _fmt_id(prefix: str, v: float) -> str:
    s = f"{float(v):.4f}".replace(".", "p")
    return f"{prefix}_{s}"


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
    bip_sf_rate_line: Optional[float],
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
        "--batch-out",
        str(out_dir),
    ]

    if bip_sf_rate_line is not None:
        cmd += ["--bip-sf-rate-line", str(float(bip_sf_rate_line))]

    return _run(cmd, cwd=_ROOT)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Sweep bip_sf_rate_line and rank candidates using both objectives with a holdout guardrail.\n\n"
            "Notes: bip_sf_rate_line is used only when bip_baserunning is enabled."
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
        help="Objective file for team totals + core metrics.",
    )
    ap.add_argument(
        "--objective-hitterprops",
        default="data/tuning/objectives/hitter_props_topn_v1_baseline_20260218_random50_s250.json",
        help="Objective file for hitter props (top-N likelihood metrics).",
    )
    ap.add_argument(
        "--grid-vals",
        default="0.16,0.20,0.24,0.28,0.32,0.36,0.40",
        help="Comma-separated grid values to try for bip_sf_rate_line.",
    )
    ap.add_argument(
        "--holdout-tol",
        type=float,
        default=0.002,
        help="Allowable regression on holdout vs holdout-baseline (ratio score). 0.0=strict.",
    )
    ap.add_argument("--w-allmetrics", type=float, default=1.0)
    ap.add_argument("--w-hitterprops", type=float, default=1.0)
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_bip_sf_rate_line",
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

    tol = float(args.holdout_tol)
    w_all = float(args.w_allmetrics)
    w_props = float(args.w_hitterprops)

    leaderboard: List[dict] = []

    # Baselines (separate for tune vs holdout)
    baseline_dir = out_root / "baseline"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    baseline_tune_batch = baseline_dir / "tune_batch"
    baseline_holdout_batch = baseline_dir / "holdout_batch"

    for bdir, dates_path in [
        (baseline_tune_batch, tune_dates_path),
        (baseline_holdout_batch, holdout_dates_path),
    ]:
        rc = _run_batch(
            batch_runner=batch_runner,
            date_file=dates_path,
            out_dir=bdir,
            sims_per_game=int(args.sims_per_game),
            jobs=int(args.jobs),
            use_raw=str(args.use_raw),
            prop_lines_source=str(args.prop_lines_source),
            bip_sf_rate_line=None,
        )
        if rc != 0:
            print(f"Baseline batch failed rc={rc}: {bdir}")
            return rc

    baseline_tune_summary = baseline_tune_batch / "summary.json"
    baseline_holdout_summary = baseline_holdout_batch / "summary.json"
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_batch, out_path=baseline_tune_summary):
        print(f"Baseline tune summarize failed: {baseline_tune_batch}")
        return 3
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_holdout_batch, out_path=baseline_holdout_summary):
        print(f"Baseline holdout summarize failed: {baseline_holdout_batch}")
        return 3

    baseline_row = {
        "id": "baseline",
        "bip_sf_rate_line": None,
        "sims_per_game": int(args.sims_per_game),
        "jobs": int(args.jobs),
        "use_raw": str(args.use_raw),
        "prop_lines_source": str(args.prop_lines_source),
        "tune_dates": str(tune_dates_path),
        "holdout_dates": str(holdout_dates_path),
        "status": "ok",
        "tune": {
            "score_allmetrics": 1.0,
            "score_hitterprops": 1.0,
            "combined": float(w_all + w_props),
            "batch_dir": str(baseline_tune_batch),
            "summary": str(baseline_tune_summary),
        },
        "holdout": {
            "score_allmetrics": 1.0,
            "score_hitterprops": 1.0,
            "ok": True,
            "batch_dir": str(baseline_holdout_batch),
            "summary": str(baseline_holdout_summary),
        },
    }
    leaderboard.append(baseline_row)

    best_row: Optional[dict] = baseline_row

    for v in grid_vals:
        cand_id = _fmt_id("sfline", v)
        cand_dir = out_root / cand_id
        cand_dir.mkdir(parents=True, exist_ok=True)

        tune_batch = cand_dir / "tune_batch"
        holdout_batch = cand_dir / "holdout_batch"

        for bdir, dates_path in [(tune_batch, tune_dates_path), (holdout_batch, holdout_dates_path)]:
            rc = _run_batch(
                batch_runner=batch_runner,
                date_file=dates_path,
                out_dir=bdir,
                sims_per_game=int(args.sims_per_game),
                jobs=int(args.jobs),
                use_raw=str(args.use_raw),
                prop_lines_source=str(args.prop_lines_source),
                bip_sf_rate_line=float(v),
            )
            if rc != 0:
                print(f"Candidate {cand_id} batch failed rc={rc}: {bdir}")
                leaderboard.append({"id": cand_id, "bip_sf_rate_line": float(v), "status": f"batch_failed_rc_{rc}"})
                break
        else:
            tune_summary = tune_batch / "summary.json"
            holdout_summary = holdout_batch / "summary.json"
            if not _summarize_batch(summarizer=summarizer, batch_dir=tune_batch, out_path=tune_summary):
                leaderboard.append({"id": cand_id, "bip_sf_rate_line": float(v), "status": "summarize_failed_tune"})
                continue
            if not _summarize_batch(summarizer=summarizer, batch_dir=holdout_batch, out_path=holdout_summary):
                leaderboard.append({"id": cand_id, "bip_sf_rate_line": float(v), "status": "summarize_failed_holdout"})
                continue

            tune_all = _score_one(
                scorer=scorer,
                objective=obj_all,
                candidate_summary=tune_summary,
                baseline_summary=baseline_tune_summary,
                out_path=cand_dir / "tune_score_allmetrics.json",
            )
            tune_props = _score_one(
                scorer=scorer,
                objective=obj_props,
                candidate_summary=tune_summary,
                baseline_summary=baseline_tune_summary,
                out_path=cand_dir / "tune_score_hitterprops.json",
            )
            hold_all = _score_one(
                scorer=scorer,
                objective=obj_all,
                candidate_summary=holdout_summary,
                baseline_summary=baseline_holdout_summary,
                out_path=cand_dir / "holdout_score_allmetrics.json",
            )
            hold_props = _score_one(
                scorer=scorer,
                objective=obj_props,
                candidate_summary=holdout_summary,
                baseline_summary=baseline_holdout_summary,
                out_path=cand_dir / "holdout_score_hitterprops.json",
            )

            if tune_all is None or tune_props is None or hold_all is None or hold_props is None:
                leaderboard.append({"id": cand_id, "bip_sf_rate_line": float(v), "status": "score_failed"})
                continue

            combined = float(w_all * float(tune_all) + w_props * float(tune_props))
            hold_ok = (float(hold_all) >= 1.0 - tol) and (float(hold_props) >= 1.0 - tol)

            row = {
                "id": cand_id,
                "bip_sf_rate_line": float(v),
                "sims_per_game": int(args.sims_per_game),
                "jobs": int(args.jobs),
                "use_raw": str(args.use_raw),
                "prop_lines_source": str(args.prop_lines_source),
                "tune_dates": str(tune_dates_path),
                "holdout_dates": str(holdout_dates_path),
                "status": "ok",
                "tune": {
                    "score_allmetrics": float(tune_all),
                    "score_hitterprops": float(tune_props),
                    "combined": float(combined),
                    "batch_dir": str(tune_batch),
                    "summary": str(tune_summary),
                },
                "holdout": {
                    "score_allmetrics": float(hold_all),
                    "score_hitterprops": float(hold_props),
                    "ok": bool(hold_ok),
                    "batch_dir": str(holdout_batch),
                    "summary": str(holdout_summary),
                },
            }
            leaderboard.append(row)

            if hold_ok and float(row["tune"]["combined"]) > float(best_row["tune"]["combined"]):
                best_row = row

    _write_json(out_root / "leaderboard.json", leaderboard)
    _write_json(out_root / "best.json", best_row)

    print(f"Best (guardrail-ok): {best_row['id']} combined={best_row['tune']['combined']}")
    print(f"Sweep root: {out_root}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
