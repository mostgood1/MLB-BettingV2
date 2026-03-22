from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[1]


def _read_dates(path: Path) -> list[str]:
    txt = path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    for line in txt:
        s = (line or "").strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Batch runner for tools/daily_update.py over a list of dates")
    ap.add_argument("--date-set", required=True, help="Path to a newline-delimited date set file (YYYY-MM-DD)")
    ap.add_argument("--season", type=int, default=0, help="Season year for --season (default: inferred per date)")
    ap.add_argument("--spring-mode", action="store_true", help="Pass through --spring-mode")
    ap.add_argument("--sims", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--pbp", choices=["off", "pa", "pitch"], default="off")
    ap.add_argument("--pbp-max-events", type=int, default=0)
    ap.add_argument("--use-roster-artifacts", choices=["on", "off"], default="on")
    ap.add_argument("--write-roster-artifacts", choices=["on", "off"], default="on")
    ap.add_argument("--max-games", type=int, default=0, help="If >0, limit games per date")
    ap.add_argument("--stop-on-error", action="store_true")
    args = ap.parse_args()

    dates_path = Path(args.date_set)
    if not dates_path.is_file():
        raise SystemExit(f"date-set file not found: {dates_path}")

    dates = _read_dates(dates_path)
    if not dates:
        print(f"No dates found in {dates_path}")
        return 2

    py = _ROOT / ".venv_x64" / "Scripts" / "python.exe"
    if not py.exists():
        raise SystemExit(f"Missing python at {py} (expected .venv_x64)")

    daily_update = _ROOT / "tools" / "daily_update.py"

    failures: list[dict] = []

    for i, d in enumerate(dates):
        try:
            year = int(d.split("-")[0])
        except Exception:
            print(f"[{i+1}/{len(dates)}] Skipping invalid date: {d}")
            failures.append({"date": d, "stage": "parse", "error": "invalid date"})
            if args.stop_on_error:
                break
            continue

        season = int(args.season or year)

        cmd: list[str] = [
            str(py),
            "-u",
            str(daily_update),
            "--date",
            str(d),
            "--season",
            str(season),
            "--sims",
            str(int(args.sims)),
            "--workers",
            str(int(args.workers)),
            "--pbp",
            str(args.pbp),
            "--use-roster-artifacts",
            str(args.use_roster_artifacts),
            "--write-roster-artifacts",
            str(args.write_roster_artifacts),
        ]

        if args.spring_mode:
            cmd.append("--spring-mode")

        if int(args.pbp_max_events or 0) > 0:
            cmd.extend(["--pbp-max-events", str(int(args.pbp_max_events))])

        if int(args.max_games or 0) > 0:
            cmd.extend(["--max-games", str(int(args.max_games))])

        start = datetime.now()
        print(f"[{i+1}/{len(dates)}] Running daily_update for {d} (season={season})...")
        r = subprocess.run(cmd, cwd=str(_ROOT), check=False)
        dur = datetime.now() - start
        if r.returncode != 0:
            print(f"[{i+1}/{len(dates)}] ERROR: exit={r.returncode} date={d} dur={dur}")
            failures.append({"date": d, "stage": "daily_update", "exit": int(r.returncode)})
            if args.stop_on_error:
                break
        else:
            print(f"[{i+1}/{len(dates)}] OK: date={d} dur={dur}")

    if failures:
        print(f"Done with {len(failures)} failures")
        return 1

    print("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
