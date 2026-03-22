from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


_ROOT = Path(__file__).resolve().parents[2]


def _read_text_dates(path: Path) -> List[str]:
    out: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
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


def _rand_range(rng: random.Random, lo: float, hi: float) -> float:
    return float(lo + (hi - lo) * rng.random())


def _make_candidate(rng: random.Random) -> Dict[str, Any]:
    # Conservative ranges; we can widen once we have stability.
    return {
        "hr_on_ball_in_play_factor": _rand_range(rng, 0.45, 0.65),
        "two_strike_whiff_boost": _rand_range(rng, 0.040, 0.075),
        "two_strike_foul_boost": _rand_range(rng, 0.040, 0.085),
        "two_strike_inplay_penalty": _rand_range(rng, 0.040, 0.085),
        "three_ball_take_bias": _rand_range(rng, 0.030, 0.090),
        "bb_ball_bias_mult": _rand_range(rng, 0.70, 1.20),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Random-search pitch model overrides by running short batch evals")
    ap.add_argument("--date-file", required=True, help="Date set file (YYYY-MM-DD per line)")
    ap.add_argument("--n-days", type=int, default=10, help="How many dates to use from the file")
    ap.add_argument("--sims-per-game", type=int, default=150)
    ap.add_argument("--jobs", type=int, default=6)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--n-candidates", type=int, default=12)
    ap.add_argument("--objective", default="data/tuning/objectives/all_metrics_v1.json")
    ap.add_argument(
        "--objective-hitterprops",
        default="",
        help=(
            "Optional second objective (e.g., hitter props) to score each candidate. "
            "If provided, candidates are ranked by a weighted combined score."
        ),
    )
    ap.add_argument("--w-allmetrics", type=float, default=1.0, help="Weight for the primary objective")
    ap.add_argument("--w-hitterprops", type=float, default=1.0, help="Weight for --objective-hitterprops")
    ap.add_argument("--batch-root", default="data/eval/batches/tuning_pitch_model")
    ap.add_argument("--prop-lines-source", default="auto")
    ap.add_argument("--use-raw", default="on")
    args = ap.parse_args()

    date_file = Path(args.date_file)
    if not date_file.is_absolute():
        date_file = (Path.cwd() / date_file).resolve()
    if not date_file.exists():
        print(f"Missing date-file: {date_file}")
        return 2

    dates = _read_text_dates(date_file)
    if not dates:
        print(f"No dates in file: {date_file}")
        return 2

    dates = dates[: max(1, int(args.n_days))]

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = _ROOT / Path(str(args.batch_root)) / stamp
    out_root.mkdir(parents=True, exist_ok=True)

    # Write a shortened date-file to keep runs comparable.
    short_dates_path = out_root / "dates.txt"
    short_dates_path.write_text("\n".join(dates) + "\n", encoding="utf-8")

    py = sys.executable
    batch_runner = _ROOT / "tools" / "eval" / "run_batch_eval_days.py"
    summarizer = _ROOT / "tools" / "eval" / "summarize_batch_eval.py"
    scorer = _ROOT / "tools" / "tune" / "score_batch_summary.py"

    rng = random.Random(int(args.seed))

    leaderboard: List[Dict[str, Any]] = []

    # Baseline run on the same dates/sims for apples-to-apples scoring.
    baseline_dir = out_root / "baseline"
    baseline_batch = baseline_dir / "batch"
    baseline_dir.mkdir(parents=True, exist_ok=True)
    baseline_summary = baseline_batch / "summary.json"

    rc0 = _run(
        [
            py,
            str(batch_runner),
            "--date-file",
            str(short_dates_path),
            "--sims-per-game",
            str(int(args.sims_per_game)),
            "--jobs",
            str(int(args.jobs)),
            "--use-raw",
            str(args.use_raw),
            "--prop-lines-source",
            str(args.prop_lines_source),
            "--pitch-model-overrides",
            "",
            "--batch-out",
            str(baseline_batch),
        ],
        cwd=_ROOT,
    )
    if rc0 != 0:
        print("Baseline batch failed; aborting")
        return 1

    rc0b = _run([py, str(summarizer), "--batch-dir", str(baseline_batch), "--out", str(baseline_summary)], cwd=_ROOT)
    if rc0b != 0 or not baseline_summary.exists():
        print("Baseline summarize failed; aborting")
        return 1

    # Baseline score should be 1.0 by construction; write it for reference.
    obj_all = (_ROOT / Path(str(args.objective))).resolve()
    _run(
        [
            py,
            str(scorer),
            "--candidate-summary",
            str(baseline_summary),
            "--objective",
            str(obj_all),
            "--baseline-summary",
            str(baseline_summary),
            "--out",
            str(baseline_dir / "score.json"),
        ],
        cwd=_ROOT,
    )

    obj_props_raw = str(args.objective_hitterprops or "").strip()
    obj_props: Optional[Path] = None
    if obj_props_raw:
        obj_props = (_ROOT / Path(obj_props_raw)).resolve()
        if obj_props.exists():
            _run(
                [
                    py,
                    str(scorer),
                    "--candidate-summary",
                    str(baseline_summary),
                    "--objective",
                    str(obj_props),
                    "--baseline-summary",
                    str(baseline_summary),
                    "--out",
                    str(baseline_dir / "score_hitterprops.json"),
                ],
                cwd=_ROOT,
            )
        else:
            print(f"Missing objective-hitterprops: {obj_props}")
            return 2

    for i in range(int(args.n_candidates)):
        cand = _make_candidate(rng)
        cand_id = f"cand_{i:03d}"
        cand_dir = out_root / cand_id
        cand_dir.mkdir(parents=True, exist_ok=True)

        overrides_path = cand_dir / "overrides.json"
        _write_json(overrides_path, cand)

        batch_out = cand_dir / "batch"

        cmd = [
            py,
            str(batch_runner),
            "--date-file",
            str(short_dates_path),
            "--sims-per-game",
            str(int(args.sims_per_game)),
            "--jobs",
            str(int(args.jobs)),
            "--use-raw",
            str(args.use_raw),
            "--prop-lines-source",
            str(args.prop_lines_source),
            "--pitch-model-overrides",
            str(overrides_path),
            "--batch-out",
            str(batch_out),
        ]

        rc = _run(cmd, cwd=_ROOT)
        if rc != 0:
            leaderboard.append({"id": cand_id, "overrides": cand, "status": "batch_failed", "returncode": rc})
            continue

        # Summarize
        summary_path = batch_out / "summary.json"
        rc2 = _run([py, str(summarizer), "--batch-dir", str(batch_out), "--out", str(summary_path)], cwd=_ROOT)
        if rc2 != 0 or not summary_path.exists():
            leaderboard.append({"id": cand_id, "overrides": cand, "status": "summarize_failed", "returncode": rc2})
            continue

        # Score
        score_out = cand_dir / "score.json"
        rc3 = _run(
            [
                py,
                str(scorer),
                "--candidate-summary",
                str(summary_path),
                "--objective",
                str(obj_all),
                "--baseline-summary",
                str(baseline_summary),
                "--out",
                str(score_out),
            ],
            cwd=_ROOT,
        )
        if rc3 != 0 or not score_out.exists():
            leaderboard.append({"id": cand_id, "overrides": cand, "status": "score_failed", "returncode": rc3})
            continue

        score_obj = json.loads(score_out.read_text(encoding="utf-8"))
        score_all = score_obj.get("score")
        score_props: Optional[float] = None

        if obj_props is not None:
            score_props_out = cand_dir / "score_hitterprops.json"
            rc3b = _run(
                [
                    py,
                    str(scorer),
                    "--candidate-summary",
                    str(summary_path),
                    "--objective",
                    str(obj_props),
                    "--baseline-summary",
                    str(baseline_summary),
                    "--out",
                    str(score_props_out),
                ],
                cwd=_ROOT,
            )
            if rc3b == 0 and score_props_out.exists():
                score_props_obj = json.loads(score_props_out.read_text(encoding="utf-8"))
                v = score_props_obj.get("score")
                if isinstance(v, (int, float)):
                    score_props = float(v)

        # Primary ranking score
        score: Any = score_all
        if obj_props is not None and isinstance(score_all, (int, float)) and isinstance(score_props, (int, float)):
            score = float(args.w_allmetrics) * float(score_all) + float(args.w_hitterprops) * float(score_props)

        leaderboard.append(
            {
                "id": cand_id,
                "overrides": cand,
                "status": "ok",
                "score": score,
                "score_allmetrics": score_all,
                "score_hitterprops": score_props,
                "w_allmetrics": float(args.w_allmetrics),
                "w_hitterprops": float(args.w_hitterprops),
                "batch_dir": str(batch_out),
                "summary": str(summary_path),
                "score_json": str(score_out),
            }
        )

        # Keep a running best file
        ok = [x for x in leaderboard if x.get("status") == "ok" and isinstance(x.get("score"), (int, float))]
        ok_sorted = sorted(ok, key=lambda x: float(x.get("score")), reverse=True)
        _write_json(out_root / "leaderboard.json", leaderboard)
        if ok_sorted:
            _write_json(out_root / "best.json", ok_sorted[0])

    # Final write
    _write_json(out_root / "leaderboard.json", leaderboard)
    ok = [x for x in leaderboard if x.get("status") == "ok" and isinstance(x.get("score"), (int, float))]
    ok_sorted = sorted(ok, key=lambda x: float(x.get("score")), reverse=True)
    if ok_sorted:
        _write_json(out_root / "best.json", ok_sorted[0])
        print(f"Best: {ok_sorted[0]['id']} score={ok_sorted[0]['score']}")
        if obj_props is not None:
            print(
                "Scores: allmetrics="
                + str(ok_sorted[0].get("score_allmetrics"))
                + " hitterprops="
                + str(ok_sorted[0].get("score_hitterprops"))
            )
        print(f"Overrides: {ok_sorted[0]['overrides']}")
        print(f"Batch: {ok_sorted[0]['batch_dir']}")
    else:
        print("No successful candidates")

    print(f"Tuning run root: {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
