from __future__ import annotations

import argparse
import calendar
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple


_ROOT = Path(__file__).resolve().parents[2]


def _ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _month_range(start_date: str, end_date: str) -> List[str]:
    s = _ymd(start_date)
    e = _ymd(end_date)
    out: List[str] = []
    y, m = s.year, s.month
    while (y, m) <= (e.year, e.month):
        out.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return out


def _statcast_months_present(statcast_root: Path) -> List[str]:
    if not statcast_root.exists():
        return []
    return sorted([p.name for p in statcast_root.iterdir() if p.is_dir() and len(p.name) == 7])


def _statcast_file_count(statcast_root: Path) -> int:
    if not statcast_root.exists():
        return 0
    return len(list(statcast_root.rglob("*.csv.gz")))


@dataclass
class Step:
    name: str
    cmd: List[str]
    required: bool = True


def _run_step(step: Step) -> int:
    print("\n===", step.name, "===")
    print(" ", " ".join(step.cmd))
    try:
        r = subprocess.run(step.cmd, cwd=str(_ROOT))
        return int(r.returncode)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        return 127


def _python_exe_x64() -> Optional[Path]:
    # Convention in this repo: MLB-BettingV2/.venv_x64/Scripts/python.exe
    cand = _ROOT / ".venv_x64" / "Scripts" / "python.exe"
    return cand if cand.exists() else None


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Bootstrap prior-season artifacts (raw coverage + derived JSONs) so early-season sims can run fast and reproducibly. "
            "Designed for 2026 preseason by building 2025 priors, but works for any season."
        )
    )
    ap.add_argument("--season", type=int, required=True, help="Prior season to build artifacts from (e.g. 2025 for 2026 preseason)")
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)

    ap.add_argument("--backfill-statsapi", choices=["on", "off"], default="on")
    ap.add_argument("--backfill-statcast", choices=["on", "off"], default="off", help="Requires .venv_x64 with pybaseball")

    ap.add_argument("--build-manager", choices=["on", "off"], default="on")
    ap.add_argument("--build-park", choices=["on", "off"], default="on")
    ap.add_argument("--build-quality", choices=["on", "off"], default="on")

    ap.add_argument("--cache-ttl-hours", type=int, default=24)
    ap.add_argument("--sleep-ms", type=int, default=25)
    ap.add_argument("--overwrite", choices=["on", "off"], default="off")
    args = ap.parse_args()

    season = int(args.season)

    # Quick inventory before we run heavy steps
    statcast_root = _ROOT / "data" / "raw" / "statcast" / "pitches" / str(season)
    expected_months = _month_range(args.start_date, args.end_date)
    present_months = _statcast_months_present(statcast_root)
    missing_months = [m for m in expected_months if m not in present_months]

    print("\n=== Inventory ===")
    print(f"Root: {_ROOT}")
    print(f"Season: {season} window: {args.start_date}..{args.end_date}")
    print(f"Statcast months present: {len(present_months)}")
    if present_months:
        print("  ", ", ".join(present_months))
    print(f"Statcast files: {_statcast_file_count(statcast_root)}")
    if missing_months:
        print(f"WARNING: missing Statcast months in window: {', '.join(missing_months)}")
        print("  Park factors and full-season quality will be incomplete until these are backfilled.")

    py = sys.executable
    x64 = _python_exe_x64()

    steps: List[Step] = []

    # 1) Backfill StatsAPI feed/live
    if args.backfill_statsapi == "on":
        steps.append(
            Step(
                name="Backfill StatsAPI feed/live raw",
                cmd=[
                    py,
                    "tools/datasets/backfill_statsapi_feed_live.py",
                    "--season",
                    str(season),
                    "--start-date",
                    args.start_date,
                    "--end-date",
                    args.end_date,
                    "--overwrite",
                    args.overwrite,
                    "--sleep-ms",
                    str(int(args.sleep_ms)),
                    "--cache-ttl-hours",
                    str(int(args.cache_ttl_hours)),
                ],
                required=False,  # network can fail; we still want derived steps to run best-effort
            )
        )

    # 2) Coverage report (always)
    steps.append(
        Step(
            name="Report feed/live coverage",
            cmd=[
                py,
                "tools/datasets/report_feed_live_coverage.py",
                "--season",
                str(season),
                "--start-date",
                args.start_date,
                "--end-date",
                args.end_date,
                "--cache-ttl-hours",
                str(int(args.cache_ttl_hours)),
            ],
            required=False,
        )
    )

    # 3) Optional Statcast backfill (x64)
    if args.backfill_statcast == "on":
        if x64 is None:
            print("\nWARNING: --backfill-statcast=on but .venv_x64/Scripts/python.exe not found; skipping.")
        else:
            steps.append(
                Step(
                    name="Backfill Statcast pitches raw (x64)",
                    cmd=[
                        str(x64),
                        "tools/datasets/backfill_statcast_pitches_x64.py",
                        "--season",
                        str(season),
                        "--start-date",
                        args.start_date,
                        "--end-date",
                        args.end_date,
                        "--overwrite",
                        args.overwrite,
                        "--chunk",
                        "month",
                    ],
                    required=False,
                )
            )

    # 4) Derived: manager tendencies
    if args.build_manager == "on":
        steps.append(
            Step(
                name="Build manager tendencies",
                cmd=[
                    py,
                    "tools/datasets/build_manager_tendencies_from_feed_live.py",
                    "--season",
                    str(season),
                    "--start-date",
                    args.start_date,
                    "--end-date",
                    args.end_date,
                    "--merge",
                    "on",
                ],
                required=False,
            )
        )

    # 5) Derived: park factors
    if args.build_park == "on":
        steps.append(
            Step(
                name="Build park factors",
                cmd=[
                    py,
                    "tools/datasets/build_park_factors_from_raw.py",
                    "--season",
                    str(season),
                    "--start-date",
                    args.start_date,
                    "--end-date",
                    args.end_date,
                    "--merge",
                    "on",
                ],
                required=False,
            )
        )

    # 6) Derived: Statcast player quality
    if args.build_quality == "on":
        steps.append(
            Step(
                name="Build Statcast player quality",
                cmd=[
                    py,
                    "tools/datasets/build_statcast_player_quality.py",
                    "--season",
                    str(season),
                    "--start-date",
                    args.start_date,
                    "--end-date",
                    args.end_date,
                    "--write-latest",
                ],
                required=False,
            )
        )

    failures: List[Tuple[str, int]] = []
    for step in steps:
        rc = _run_step(step)
        if rc != 0:
            failures.append((step.name, rc))
            if step.required:
                break

    print("\n=== Outputs (expected) ===")
    print(f"Manager tendencies: {_ROOT / 'data' / 'manager' / 'manager_tendencies.json'}")
    print(f"Park factors:       {_ROOT / 'data' / 'park' / 'park_factors.json'}")
    print(f"Umpire factors:     {_ROOT / 'data' / 'umpire' / 'umpire_factors.json'}")
    print(f"Statcast quality:   {_ROOT / 'data' / 'statcast' / 'quality' / f'player_quality_{season}.json'}")

    if failures:
        print("\nWARNING: some steps failed (best-effort run):")
        for name, rc in failures:
            print(f"  - {name}: exit_code={rc}")
        return 2

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
