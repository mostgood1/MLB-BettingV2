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

from sim_engine.pitcher_distributions import PitcherDistributionConfig


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


def _parse_float_list(raw: str, *, positive: bool = False) -> List[float]:
    vals: List[float] = []
    for part in str(raw).split(","):
        s = part.strip()
        if not s:
            continue
        v = float(s)
        if not math.isfinite(v):
            continue
        if positive and v <= 0.0:
            continue
        vals.append(float(v))
    if not vals:
        raise ValueError("Empty grid")
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
    pitcher_rate_sampling: str,
    pitcher_distribution_overrides: str,
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
        "--pitcher-rate-sampling",
        str(pitcher_rate_sampling),
        "--pitcher-distribution-overrides",
        str(pitcher_distribution_overrides or ""),
        "--batch-out",
        str(out_dir),
    ]
    return _run(cmd, cwd=_ROOT)


def _fmt_tag(prefix: str, v: float) -> str:
    return f"{prefix}_{str(f'{float(v):.4f}').replace('.', 'p')}"


def main() -> int:
    baseline_cfg = PitcherDistributionConfig()
    baseline_bf_scale = float(baseline_cfg.bf_scale)
    baseline_bip_scale = float(baseline_cfg.bip_scale)

    ap = argparse.ArgumentParser(
        description=(
            "Small 2D sweep over pitcher_distribution_overrides bf_scale and bip_scale "
            "with tune objectives plus holdout guardrails."
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
    ap.add_argument(
        "--pitcher-rate-sampling",
        choices=["on", "off"],
        default="on",
        help="Must stay on for this sweep to matter.",
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
        "--grid-bf-scales",
        default="0.20,0.25,0.30",
        help=(
            "Comma-separated bf_scale values. "
            f"Baseline default is {baseline_bf_scale:.4f}."
        ),
    )
    ap.add_argument(
        "--grid-bip-scales",
        default="0.20,0.25,0.30",
        help=(
            "Comma-separated bip_scale values. "
            f"Baseline default is {baseline_bip_scale:.4f}."
        ),
    )
    ap.add_argument("--holdout-tol", type=float, default=0.002)
    ap.add_argument("--w-allmetrics", type=float, default=1.0)
    ap.add_argument("--w-hitterprops", type=float, default=1.0)
    ap.add_argument(
        "--batch-root",
        default="data/eval/batches/tuning_pitcher_distribution_scales_2d",
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

    bf_scales = _parse_float_list(str(args.grid_bf_scales), positive=True)
    bip_scales = _parse_float_list(str(args.grid_bip_scales), positive=True)

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
            pitcher_rate_sampling=str(args.pitcher_rate_sampling),
            pitcher_distribution_overrides="",
        )
        if rc != 0:
            print(f"Baseline batch failed rc={rc}: {bdir}")
            return rc

    baseline_tune_summary = baseline_tune_batch / "summary.json"
    baseline_holdout_summary = baseline_holdout_batch / "summary.json"
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_tune_batch, out_path=baseline_tune_summary):
        print("Baseline tune summarize failed")
        return 3
    if not _summarize_batch(summarizer=summarizer, batch_dir=baseline_holdout_batch, out_path=baseline_holdout_summary):
        print("Baseline holdout summarize failed")
        return 3

    baseline_row: Dict[str, Any] = {
        "id": "baseline",
        "bf_scale": None,
        "bip_scale": None,
        "baseline_defaults": {
            "bf_scale": baseline_bf_scale,
            "bip_scale": baseline_bip_scale,
        },
        "sims_per_game": int(args.sims_per_game),
        "jobs": int(args.jobs),
        "use_raw": str(args.use_raw),
        "prop_lines_source": str(args.prop_lines_source),
        "pitcher_rate_sampling": str(args.pitcher_rate_sampling),
        "tune_dates": str(tune_dates_path),
        "holdout_dates": str(holdout_dates_path),
        "status": "ok",
        "tune": {
            "score_allmetrics": 1.0,
            "score_hitterprops": 1.0,
            "combined": float(args.w_allmetrics) + float(args.w_hitterprops),
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

    leaderboard: List[Dict[str, Any]] = [baseline_row]
    best_row = baseline_row

    for bf_scale in bf_scales:
        for bip_scale in bip_scales:
            if abs(float(bf_scale) - baseline_bf_scale) < 1e-12 and abs(float(bip_scale) - baseline_bip_scale) < 1e-12:
                continue

            tag = f"{_fmt_tag('bf', bf_scale)}_{_fmt_tag('bip', bip_scale)}"
            cand_dir = out_root / tag
            cand_dir.mkdir(parents=True, exist_ok=True)

            overrides = {
                "bf_scale": float(bf_scale),
                "bip_scale": float(bip_scale),
            }
            overrides_str = json.dumps(overrides)

            cand_tune_batch = cand_dir / "tune_batch"
            cand_holdout_batch = cand_dir / "holdout_batch"

            rc = _run_batch(
                batch_runner=batch_runner,
                date_file=tune_dates_path,
                out_dir=cand_tune_batch,
                sims_per_game=int(args.sims_per_game),
                jobs=int(args.jobs),
                use_raw=str(args.use_raw),
                prop_lines_source=str(args.prop_lines_source),
                pitcher_rate_sampling=str(args.pitcher_rate_sampling),
                pitcher_distribution_overrides=overrides_str,
            )
            if rc != 0:
                row = {
                    "id": tag,
                    "bf_scale": float(bf_scale),
                    "bip_scale": float(bip_scale),
                    "pitcher_distribution_overrides": overrides,
                    "status": f"tune_failed_rc_{rc}",
                }
                leaderboard.append(row)
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            rc = _run_batch(
                batch_runner=batch_runner,
                date_file=holdout_dates_path,
                out_dir=cand_holdout_batch,
                sims_per_game=int(args.sims_per_game),
                jobs=int(args.jobs),
                use_raw=str(args.use_raw),
                prop_lines_source=str(args.prop_lines_source),
                pitcher_rate_sampling=str(args.pitcher_rate_sampling),
                pitcher_distribution_overrides=overrides_str,
            )
            if rc != 0:
                row = {
                    "id": tag,
                    "bf_scale": float(bf_scale),
                    "bip_scale": float(bip_scale),
                    "pitcher_distribution_overrides": overrides,
                    "status": f"holdout_failed_rc_{rc}",
                }
                leaderboard.append(row)
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            cand_tune_summary = cand_tune_batch / "summary.json"
            cand_holdout_summary = cand_holdout_batch / "summary.json"
            if not _summarize_batch(summarizer=summarizer, batch_dir=cand_tune_batch, out_path=cand_tune_summary):
                row = {
                    "id": tag,
                    "bf_scale": float(bf_scale),
                    "bip_scale": float(bip_scale),
                    "pitcher_distribution_overrides": overrides,
                    "status": "summarize_tune_failed",
                }
                leaderboard.append(row)
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue
            if not _summarize_batch(summarizer=summarizer, batch_dir=cand_holdout_batch, out_path=cand_holdout_summary):
                row = {
                    "id": tag,
                    "bf_scale": float(bf_scale),
                    "bip_scale": float(bip_scale),
                    "pitcher_distribution_overrides": overrides,
                    "status": "summarize_holdout_failed",
                }
                leaderboard.append(row)
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            tune_all = _score_one(
                scorer=scorer,
                objective=obj_all,
                candidate_summary=cand_tune_summary,
                baseline_summary=baseline_tune_summary,
                out_path=cand_dir / "tune_score_allmetrics.json",
            )
            tune_props = _score_one(
                scorer=scorer,
                objective=obj_props,
                candidate_summary=cand_tune_summary,
                baseline_summary=baseline_tune_summary,
                out_path=cand_dir / "tune_score_hitterprops.json",
            )
            hold_all = _score_one(
                scorer=scorer,
                objective=obj_all,
                candidate_summary=cand_holdout_summary,
                baseline_summary=baseline_holdout_summary,
                out_path=cand_dir / "holdout_score_allmetrics.json",
            )
            hold_props = _score_one(
                scorer=scorer,
                objective=obj_props,
                candidate_summary=cand_holdout_summary,
                baseline_summary=baseline_holdout_summary,
                out_path=cand_dir / "holdout_score_hitterprops.json",
            )

            if any(x is None for x in (tune_all, tune_props, hold_all, hold_props)):
                row = {
                    "id": tag,
                    "bf_scale": float(bf_scale),
                    "bip_scale": float(bip_scale),
                    "pitcher_distribution_overrides": overrides,
                    "status": "score_failed",
                }
                leaderboard.append(row)
                _write_json(out_root / "leaderboard.json", leaderboard)
                continue

            combined = float(args.w_allmetrics) * float(tune_all) + float(args.w_hitterprops) * float(tune_props)
            ok = (float(hold_all) >= 1.0 - float(args.holdout_tol)) and (
                float(hold_props) >= 1.0 - float(args.holdout_tol)
            )

            row = {
                "id": tag,
                "bf_scale": float(bf_scale),
                "bip_scale": float(bip_scale),
                "pitcher_distribution_overrides": overrides,
                "sims_per_game": int(args.sims_per_game),
                "jobs": int(args.jobs),
                "use_raw": str(args.use_raw),
                "prop_lines_source": str(args.prop_lines_source),
                "pitcher_rate_sampling": str(args.pitcher_rate_sampling),
                "tune_dates": str(tune_dates_path),
                "holdout_dates": str(holdout_dates_path),
                "status": "ok",
                "tune": {
                    "score_allmetrics": float(tune_all),
                    "score_hitterprops": float(tune_props),
                    "combined": float(combined),
                    "batch_dir": str(cand_tune_batch),
                    "summary": str(cand_tune_summary),
                },
                "holdout": {
                    "score_allmetrics": float(hold_all),
                    "score_hitterprops": float(hold_props),
                    "ok": bool(ok),
                    "batch_dir": str(cand_holdout_batch),
                    "summary": str(cand_holdout_summary),
                },
            }
            leaderboard.append(row)
            _write_json(out_root / "leaderboard.json", leaderboard)

            if bool(ok) and float(combined) > float(best_row.get("tune", {}).get("combined", float("-inf"))):
                best_row = row
                _write_json(out_root / "best.json", best_row)

    _write_json(out_root / "leaderboard.json", leaderboard)
    _write_json(out_root / "best.json", best_row)
    print(f"Wrote: {out_root}")
    print(
        f"Best: {best_row.get('id')} bf_scale={best_row.get('bf_scale')} "
        f"bip_scale={best_row.get('bip_scale')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
