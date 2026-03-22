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


def _parse_grid_vals(raw: str) -> List[float]:
    vals: List[float] = []
    for part in str(raw).split(","):
        s = part.strip()
        if not s:
            continue
        v = float(s)
        if not math.isfinite(v):
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
    p2_scores_on_1b_mult: float,
    p1_scores_on_2b_mult: float,
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
        "--bip-1b-p2-scores-mult",
        str(float(p2_scores_on_1b_mult)),
        "--bip-2b-p1-scores-mult",
        str(float(p1_scores_on_2b_mult)),
        "--batch-out",
        str(out_dir),
    ]
    return _run(cmd, cwd=_ROOT)


def main() -> int:
    # Import here to keep this tuning tool lightweight on module import.
    from sim_engine.models import GameConfig

    ap = argparse.ArgumentParser(
        description=(
            "Sweep baserunning scoring multipliers and rank candidates using both objectives "
            "with a holdout guardrail."
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
        default="0.90,1.00,1.10",
        help="Comma-separated grid values to try for each multiplier.",
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
        default="data/eval/batches/tuning_baserunning_scoring_mults",
        help="Root folder for this sweep's outputs.",
    )
    args = ap.parse_args()

    cfg_defaults = GameConfig()
    baseline_p2_scores_on_1b_mult = float(getattr(cfg_defaults, "bip_1b_p2_scores_mult", 1.0))
    baseline_p1_scores_on_2b_mult = float(getattr(cfg_defaults, "bip_2b_p1_scores_mult", 1.0))

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
            p2_scores_on_1b_mult=float(baseline_p2_scores_on_1b_mult),
            p1_scores_on_2b_mult=float(baseline_p1_scores_on_2b_mult),
        )
        if rc != 0:
            print(f"Baseline batch failed: {bdir} (rc={rc})")
            return 1

    baseline_tune_summary = baseline_tune_batch / "summary.json"
    baseline_holdout_summary = baseline_holdout_batch / "summary.json"
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_batch, out_path=baseline_tune_summary):
        print("Baseline tune summarize failed")
        return 1
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_holdout_batch, out_path=baseline_holdout_summary):
        print("Baseline holdout summarize failed")
        return 1

    # Record baseline self-scores for reference
    _score_one(
        scorer=scorer,
        objective=obj_all,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "tune_score_allmetrics.json",
    )
    _score_one(
        scorer=scorer,
        objective=obj_props,
        candidate_summary=baseline_tune_summary,
        baseline_summary=baseline_tune_summary,
        out_path=baseline_dir / "tune_score_hitterprops.json",
    )
    _score_one(
        scorer=scorer,
        objective=obj_all,
        candidate_summary=baseline_holdout_summary,
        baseline_summary=baseline_holdout_summary,
        out_path=baseline_dir / "holdout_score_allmetrics.json",
    )
    _score_one(
        scorer=scorer,
        objective=obj_props,
        candidate_summary=baseline_holdout_summary,
        baseline_summary=baseline_holdout_summary,
        out_path=baseline_dir / "holdout_score_hitterprops.json",
    )

    leaderboard: List[Dict[str, Any]] = []

    def cand_key(p2: float, p1: float) -> str:
        return f"p2on1b_{p2:.3f}__p1on2b_{p1:.3f}".replace(".", "p")

    # Add an explicit baseline row so we always have a guardrail-ok fallback.
    baseline_row = {
        "id": "baseline",
        "bip_1b_p2_scores_mult": float(baseline_p2_scores_on_1b_mult),
        "bip_2b_p1_scores_mult": float(baseline_p1_scores_on_2b_mult),
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
            "combined": float(args.w_allmetrics) * 1.0 + float(args.w_hitterprops) * 1.0,
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
    _write_json(out_root / "leaderboard.json", leaderboard)

    for p2 in grid_vals:
        for p1 in grid_vals:
            if float(p2) == float(baseline_p2_scores_on_1b_mult) and float(p1) == float(baseline_p1_scores_on_2b_mult):
                continue
            cand_id = cand_key(float(p2), float(p1))
            cand_dir = out_root / cand_id
            cand_dir.mkdir(parents=True, exist_ok=True)

            meta = {
                "id": cand_id,
                "bip_1b_p2_scores_mult": float(p2),
                "bip_2b_p1_scores_mult": float(p1),
                "sims_per_game": int(args.sims_per_game),
                "jobs": int(args.jobs),
                "use_raw": str(args.use_raw),
                "prop_lines_source": str(args.prop_lines_source),
                "tune_dates": str(tune_dates_path),
                "holdout_dates": str(holdout_dates_path),
            }
            _write_json(cand_dir / "config.json", meta)

            tune_batch = cand_dir / "tune_batch"
            holdout_batch = cand_dir / "holdout_batch"

            rc_tune = _run_batch(
                batch_runner=batch_runner,
                date_file=tune_dates_path,
                out_dir=tune_batch,
                sims_per_game=int(args.sims_per_game),
                jobs=int(args.jobs),
                use_raw=str(args.use_raw),
                prop_lines_source=str(args.prop_lines_source),
                p2_scores_on_1b_mult=float(p2),
                p1_scores_on_2b_mult=float(p1),
            )
            if rc_tune != 0:
                leaderboard.append({"id": cand_id, "status": "tune_batch_failed", "returncode": rc_tune, **meta})
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            rc_hold = _run_batch(
                batch_runner=batch_runner,
                date_file=holdout_dates_path,
                out_dir=holdout_batch,
                sims_per_game=int(args.sims_per_game),
                jobs=int(args.jobs),
                use_raw=str(args.use_raw),
                prop_lines_source=str(args.prop_lines_source),
                p2_scores_on_1b_mult=float(p2),
                p1_scores_on_2b_mult=float(p1),
            )
            if rc_hold != 0:
                leaderboard.append({"id": cand_id, "status": "holdout_batch_failed", "returncode": rc_hold, **meta})
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            tune_summary = tune_batch / "summary.json"
            holdout_summary = holdout_batch / "summary.json"
            if not _summarize_batch(summarizer=summarizer, batch_dir=tune_batch, out_path=tune_summary):
                leaderboard.append({"id": cand_id, "status": "tune_summarize_failed", **meta})
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue
            if not _summarize_batch(summarizer=summarizer, batch_dir=holdout_batch, out_path=holdout_summary):
                leaderboard.append({"id": cand_id, "status": "holdout_summarize_failed", **meta})
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            tune_score_all = _score_one(
                scorer=scorer,
                objective=obj_all,
                candidate_summary=tune_summary,
                baseline_summary=baseline_tune_summary,
                out_path=cand_dir / "tune_score_allmetrics.json",
            )
            tune_score_props = _score_one(
                scorer=scorer,
                objective=obj_props,
                candidate_summary=tune_summary,
                baseline_summary=baseline_tune_summary,
                out_path=cand_dir / "tune_score_hitterprops.json",
            )
            hold_score_all = _score_one(
                scorer=scorer,
                objective=obj_all,
                candidate_summary=holdout_summary,
                baseline_summary=baseline_holdout_summary,
                out_path=cand_dir / "holdout_score_allmetrics.json",
            )
            hold_score_props = _score_one(
                scorer=scorer,
                objective=obj_props,
                candidate_summary=holdout_summary,
                baseline_summary=baseline_holdout_summary,
                out_path=cand_dir / "holdout_score_hitterprops.json",
            )

            if tune_score_all is None or tune_score_props is None or hold_score_all is None or hold_score_props is None:
                leaderboard.append({"id": cand_id, "status": "score_failed", **meta})
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            # Score is a ratio vs baseline where >1.0 is better.
            # Guardrail: do not regress on holdout by more than `holdout_tol`.
            holdout_ok = (hold_score_all >= 1.0 - float(args.holdout_tol)) and (
                hold_score_props >= 1.0 - float(args.holdout_tol)
            )

            combined = float(args.w_allmetrics) * float(tune_score_all) + float(args.w_hitterprops) * float(tune_score_props)
            row = {
                **meta,
                "status": "ok",
                "tune": {
                    "score_allmetrics": tune_score_all,
                    "score_hitterprops": tune_score_props,
                    "combined": combined,
                    "batch_dir": str(tune_batch),
                    "summary": str(tune_summary),
                },
                "holdout": {
                    "score_allmetrics": hold_score_all,
                    "score_hitterprops": hold_score_props,
                    "ok": bool(holdout_ok),
                    "batch_dir": str(holdout_batch),
                    "summary": str(holdout_summary),
                },
            }
            leaderboard.append(row)
            _write_json(out_root / "leaderboard.json", leaderboard)

            ok_rows = [
                x
                for x in leaderboard
                if isinstance(x, dict)
                and x.get("status") == "ok"
                and isinstance((x.get("holdout") or {}).get("ok"), bool)
                and bool((x.get("holdout") or {}).get("ok"))
                and isinstance(((x.get("tune") or {}).get("combined")), (int, float))
            ]
            if ok_rows:
                best = max(ok_rows, key=lambda x: float((x.get("tune") or {}).get("combined")))
                _write_json(out_root / "best.json", best)

    _write_json(out_root / "leaderboard.json", leaderboard)
    ok_rows = [
        x
        for x in leaderboard
        if isinstance(x, dict)
        and x.get("status") == "ok"
        and bool((x.get("holdout") or {}).get("ok"))
        and isinstance(((x.get("tune") or {}).get("combined")), (int, float))
    ]

    if ok_rows:
        best = max(ok_rows, key=lambda x: float((x.get("tune") or {}).get("combined")))
        _write_json(out_root / "best.json", best)
        print(f"Best (guardrail-ok): {best.get('id')} combined={best.get('tune', {}).get('combined')}")
    else:
        print("No guardrail-ok candidates")

    print(f"Sweep root: {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
