from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


_ROOT = Path(__file__).resolve().parents[2]


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _latest_run_dir(tuning_root: Path) -> Optional[Path]:
    dirs = [p for p in tuning_root.iterdir() if p.is_dir()]
    if not dirs:
        return None
    return max(dirs, key=lambda p: p.stat().st_mtime)


def _safe_load_leaderboard(path: Path) -> Optional[List[Dict[str, Any]]]:
    try:
        obj = _read_json(path)
    except Exception:
        return None
    if not isinstance(obj, list):
        return None
    return obj


def _run(cmd: List[str], cwd: Path) -> int:
    p = subprocess.run(cmd, cwd=str(cwd))
    return int(p.returncode)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Watch a tuning run folder until complete, then promote best overrides, run quick confirm, "
            "and optionally run a full confirmation batch."
        )
    )
    ap.add_argument("--tuning-root", required=True, help="Folder containing timestamped tuning runs")
    ap.add_argument("--n-candidates", type=int, required=True, help="Expected number of candidates")
    ap.add_argument("--objective", required=True, help="Objective JSON")
    ap.add_argument("--date-file-50", required=True, help="Random50 date file")
    ap.add_argument("--poll-seconds", type=int, default=120, help="Polling interval")
    args = ap.parse_args()

    tuning_root = (_ROOT / Path(args.tuning_root)).resolve()
    objective = (_ROOT / Path(args.objective)).resolve()
    date_file_50 = (_ROOT / Path(args.date_file_50)).resolve()

    py = sys.executable
    batch_runner = _ROOT / "tools" / "eval" / "run_batch_eval_days.py"
    summarizer = _ROOT / "tools" / "eval" / "summarize_batch_eval.py"
    scorer = _ROOT / "tools" / "tune" / "score_batch_summary.py"

    if not tuning_root.exists():
        print(f"Missing tuning root: {tuning_root}")
        return 2

    print(f"{_ts()} watching tuning root: {tuning_root}")

    run_dir: Optional[Path] = None
    while True:
        run_dir = _latest_run_dir(tuning_root)
        if run_dir is None:
            print(f"{_ts()} no run dirs yet")
            time.sleep(max(5, int(args.poll_seconds)))
            continue

        leaderboard_path = run_dir / "leaderboard.json"
        best_path = run_dir / "best.json"

        if not leaderboard_path.exists():
            cand_dirs = len([p for p in run_dir.iterdir() if p.is_dir() and p.name.startswith("cand_")])
            print(f"{_ts()} run={run_dir.name} leaderboard missing; cand_dirs={cand_dirs}")
            time.sleep(max(5, int(args.poll_seconds)))
            continue

        lb = _safe_load_leaderboard(leaderboard_path)
        if lb is None:
            print(f"{_ts()} run={run_dir.name} leaderboard parse failed; retrying")
            time.sleep(10)
            continue

        print(f"{_ts()} run={run_dir.name} candidates={len(lb)}/{int(args.n_candidates)}")
        if len(lb) >= int(args.n_candidates) and best_path.exists():
            break

        time.sleep(max(5, int(args.poll_seconds)))

    assert run_dir is not None
    best_obj = _read_json(run_dir / "best.json")
    if not isinstance(best_obj, dict) or not isinstance(best_obj.get("overrides"), dict):
        print(f"Bad best.json: {run_dir / 'best.json'}")
        return 2

    stamp = run_dir.name
    out_overrides = _ROOT / "data" / "tuning" / "pitch_model" / f"allmetrics_v3_best_{stamp}.json"
    _write_json(out_overrides, best_obj["overrides"])
    print(f"{_ts()} promoted best overrides -> {out_overrides}")
    print(f"{_ts()} best.score (same-days baseline) = {best_obj.get('score')}")

    # Quick confirm on first 6 dates from the run's dates.txt
    dates_path = run_dir / "dates.txt"
    if not dates_path.exists():
        print(f"Missing dates.txt: {dates_path}")
        return 2
    dates = [ln.strip().lstrip("\ufeff") for ln in dates_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    dates6 = dates[:6]
    dates6_path = run_dir / "dates6.txt"
    dates6_path.write_text("\n".join(dates6) + "\n", encoding="utf-8")

    base6 = run_dir / "confirm6_baseline"
    cand6 = run_dir / "confirm6_candidate"

    print(f"{_ts()} running 6-day baseline (500 sims)")
    rc = _run(
        [
            py,
            str(batch_runner),
            "--date-file",
            str(dates6_path),
            "--sims-per-game",
            "500",
            "--jobs",
            "6",
            "--use-raw",
            "on",
            "--prop-lines-source",
            "auto",
            "--pitch-model-overrides",
            "",
            "--batch-out",
            str(base6),
        ],
        cwd=_ROOT,
    )
    if rc != 0:
        print(f"Baseline6 batch failed rc={rc}")
        return 1

    print(f"{_ts()} running 6-day candidate (500 sims)")
    rc = _run(
        [
            py,
            str(batch_runner),
            "--date-file",
            str(dates6_path),
            "--sims-per-game",
            "500",
            "--jobs",
            "6",
            "--use-raw",
            "on",
            "--prop-lines-source",
            "auto",
            "--pitch-model-overrides",
            str(out_overrides),
            "--batch-out",
            str(cand6),
        ],
        cwd=_ROOT,
    )
    if rc != 0:
        print(f"Candidate6 batch failed rc={rc}")
        return 1

    base6_sum = base6 / "summary.json"
    cand6_sum = cand6 / "summary.json"

    _run([py, str(summarizer), "--batch-dir", str(base6), "--out", str(base6_sum)], cwd=_ROOT)
    _run([py, str(summarizer), "--batch-dir", str(cand6), "--out", str(cand6_sum)], cwd=_ROOT)

    print(f"{_ts()} apples-to-apples 6-day score (candidate vs baseline6):")
    _run(
        [
            py,
            str(scorer),
            "--objective",
            str(objective),
            "--candidate-summary",
            str(cand6_sum),
            "--baseline-summary",
            str(base6_sum),
        ],
        cwd=_ROOT,
    )

    # Full confirm only if the tuning best beat baseline on the same tuning days
    try:
        best_score = float(best_obj.get("score"))
    except Exception:
        best_score = 999.0

    if best_score >= 1.0:
        print(f"{_ts()} not launching full50; best.score >= 1.0 on tuning days")
        return 0

    full_out = _ROOT / "data" / "eval" / "batches" / f"random50_regseason_sims500_seed2026_overrides_allmetrics_v3_best_{stamp}"
    print(f"{_ts()} launching full random50 confirm (500 sims) -> {full_out}")

    rc = _run(
        [
            py,
            str(batch_runner),
            "--date-file",
            str(date_file_50),
            "--sims-per-game",
            "500",
            "--jobs",
            "6",
            "--use-raw",
            "on",
            "--prop-lines-source",
            "auto",
            "--pitch-model-overrides",
            str(out_overrides),
            "--batch-out",
            str(full_out),
        ],
        cwd=_ROOT,
    )
    if rc != 0:
        print(f"Full50 batch failed rc={rc}")
        return 1

    full_sum = full_out / "summary.json"
    _run([py, str(summarizer), "--batch-dir", str(full_out), "--out", str(full_sum)], cwd=_ROOT)

    print(f"{_ts()} full50 score vs tuned baseline objective:")
    _run(
        [
            py,
            str(scorer),
            "--objective",
            str(objective),
            "--candidate-summary",
            str(full_sum),
        ],
        cwd=_ROOT,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
