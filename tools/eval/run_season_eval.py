from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Sequence


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import StatsApiClient, fetch_schedule_for_date


def _resolve_path(value: str, *, default: Path) -> Path:
    raw = str(value or "").strip()
    path = Path(raw) if raw else default
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


def _daterange(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _parse_iso_date(value: str) -> date:
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()


def _write_dates(path: Path, dates: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(str(d) for d in dates) + "\n", encoding="utf-8")


def _build_schedule_date_set(*, start_date: date, end_date: date, game_types: set[str]) -> List[str]:
    client = StatsApiClient.with_default_cache(ttl_seconds=3600)
    out: List[str] = []
    for current in _daterange(start_date, end_date):
        date_str = current.isoformat()
        try:
            games = fetch_schedule_for_date(client, date_str) or []
        except Exception:
            games = []
        if not games:
            continue
        if game_types:
            games = [
                game
                for game in games
                if str((game or {}).get("gameType") or "").strip().upper() in game_types
            ]
        if games:
            out.append(date_str)
    return out


def _run(cmd: List[str]) -> int:
    return int(subprocess.run(cmd, cwd=str(_ROOT), check=False).returncode)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate, run, and publish a full-season eval batch")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--start-date", default="", help="Default: <season>-03-01")
    ap.add_argument("--end-date", default="", help="Default: <season>-11-30")
    ap.add_argument("--game-types", default="R", help="Comma-separated StatsAPI game types to include (default: R)")
    ap.add_argument("--date-file-out", default="", help="Where to write the generated date set")
    ap.add_argument("--batch-out", default="", help="Batch output folder")
    ap.add_argument("--manifest-out", default="", help="Season manifest output JSON")
    ap.add_argument("--recap-md", default="", help="Season recap markdown output")
    ap.add_argument("--generate-only", action="store_true", help="Only generate the date set, do not run eval")
    ap.add_argument("--skip-batch", action="store_true", help="Skip the eval batch run and publish from an existing batch")
    ap.add_argument("--skip-manifest", action="store_true", help="Skip manifest generation after the batch run")
    ap.add_argument("--title", default="", help="Optional frontend title for the manifest")
    ap.add_argument("--sims-per-game", type=int, default=500)
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--use-raw", choices=["on", "off"], default="on")
    ap.add_argument("--prop-lines-source", choices=["auto", "oddsapi", "last_known", "bovada", "off"], default="last_known")
    ap.add_argument("--spring-mode", choices=["on", "off"], default="off")
    ap.add_argument("--stats-season", type=int, default=0)
    ap.add_argument("--use-daily-snapshots", choices=["on", "off"], default="on")
    ap.add_argument("--daily-snapshots-root", default="data/daily/snapshots")
    ap.add_argument("--use-roster-artifacts", choices=["on", "off"], default="on")
    ap.add_argument("--write-roster-artifacts", choices=["on", "off"], default="off")
    ap.add_argument("--lineups-last-known", default="")
    ap.add_argument("--hitter-hr-topn", type=int, default=24)
    ap.add_argument("--hitter-props-topn", type=int, default=24)
    ap.add_argument("--retries", type=int, default=1)
    ap.add_argument("--child-stdout", choices=["inherit", "quiet"], default="inherit")
    ap.add_argument("--progress", choices=["on", "off"], default="on")
    ap.add_argument(
        "--extra-batch-arg",
        action="append",
        default=[],
        help="Repeat to forward additional raw args to run_batch_eval_days.py, for example --extra-batch-arg=--umpire-shrink --extra-batch-arg=1.0",
    )
    args = ap.parse_args()

    season = int(args.season)
    phase_label = "regular" if str(args.game_types).strip().upper() == "R" else "custom"
    start_date = _parse_iso_date(str(args.start_date).strip() or f"{season}-03-01")
    end_date = _parse_iso_date(str(args.end_date).strip() or f"{season}-11-30")
    if end_date < start_date:
        raise SystemExit("end-date must be on or after start-date")

    game_types = {part.strip().upper() for part in str(args.game_types or "").split(",") if part.strip()}
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    date_file_out = _resolve_path(
        str(args.date_file_out),
        default=_ROOT / "data" / "eval" / "date_sets" / f"season_{season}_{phase_label}.txt",
    )
    batch_out = _resolve_path(
        str(args.batch_out),
        default=_ROOT / "data" / "eval" / "batches" / f"season_{season}_{phase_label}_sims{int(args.sims_per_game)}_{stamp}",
    )
    manifest_out = _resolve_path(
        str(args.manifest_out),
        default=_ROOT / "data" / "eval" / "seasons" / str(season) / "season_eval_manifest.json",
    )
    recap_md = _resolve_path(
        str(args.recap_md),
        default=manifest_out.with_name("season_eval_recap.md"),
    )

    dates = _build_schedule_date_set(start_date=start_date, end_date=end_date, game_types=game_types)
    if not dates:
        raise SystemExit("No schedule dates matched the requested season window and game types")
    _write_dates(date_file_out, dates)
    print(f"Wrote date set: {date_file_out} ({len(dates)} dates)")

    if args.generate_only:
        return 0

    if not args.skip_batch:
        batch_tool = _ROOT / "tools" / "eval" / "run_batch_eval_days.py"
        batch_cmd = [
            sys.executable,
            str(batch_tool),
            "--date-file",
            str(date_file_out),
            "--batch-out",
            str(batch_out),
            "--sims-per-game",
            str(int(args.sims_per_game)),
            "--jobs",
            str(int(args.jobs)),
            "--use-raw",
            str(args.use_raw),
            "--prop-lines-source",
            str(args.prop_lines_source),
            "--spring-mode",
            str(args.spring_mode),
            "--stats-season",
            str(int(args.stats_season)),
            "--use-daily-snapshots",
            str(args.use_daily_snapshots),
            "--daily-snapshots-root",
            str(args.daily_snapshots_root),
            "--use-roster-artifacts",
            str(args.use_roster_artifacts),
            "--write-roster-artifacts",
            str(args.write_roster_artifacts),
            "--hitter-hr-topn",
            str(int(args.hitter_hr_topn)),
            "--hitter-props-topn",
            str(int(args.hitter_props_topn)),
            "--retries",
            str(int(args.retries)),
            "--child-stdout",
            str(args.child_stdout),
            "--progress",
            str(args.progress),
        ]
        if str(args.lineups_last_known).strip():
            batch_cmd.extend(["--lineups-last-known", str(args.lineups_last_known)])
        if args.extra_batch_arg:
            batch_cmd.extend([str(part) for part in args.extra_batch_arg if str(part).strip()])

        batch_rc = _run(batch_cmd)
        if batch_rc != 0:
            print(f"Warning: batch run exited with {batch_rc}; attempting to summarize partial outputs if any exist")
    else:
        batch_rc = 0

    if not batch_out.exists() or not batch_out.is_dir():
        raise SystemExit(f"Batch output folder not found: {batch_out}")
    report_files = sorted(batch_out.glob("sim_vs_actual_*.json"))
    if not report_files:
        raise SystemExit(f"No per-day reports found in batch output: {batch_out}")

    summary_tool = _ROOT / "tools" / "eval" / "summarize_batch_eval.py"
    summary_rc = _run([
        sys.executable,
        str(summary_tool),
        "--batch-dir",
        str(batch_out),
        "--out",
        str(batch_out / "summary.json"),
    ])
    if summary_rc != 0:
        raise SystemExit(f"summarize_batch_eval.py failed with exit {summary_rc}")

    if args.skip_manifest:
        return 0 if batch_rc == 0 else 1

    manifest_tool = _ROOT / "tools" / "eval" / "build_season_eval_manifest.py"
    manifest_cmd = [
        sys.executable,
        str(manifest_tool),
        "--season",
        str(season),
        "--batch-dir",
        str(batch_out),
        "--out",
        str(manifest_out),
        "--recap-md",
        str(recap_md),
        "--game-types",
        str(args.game_types),
    ]
    if str(args.title).strip():
        manifest_cmd.extend(["--title", str(args.title)])
    manifest_rc = _run(manifest_cmd)
    if manifest_rc != 0:
        raise SystemExit(f"build_season_eval_manifest.py failed with exit {manifest_rc}")

    print(f"Season batch ready: {batch_out}")
    print(f"Season manifest: {manifest_out}")
    print(f"Season recap: {recap_md}")
    return 0 if batch_rc == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())