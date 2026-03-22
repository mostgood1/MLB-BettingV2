from __future__ import annotations

import atexit
import argparse
import gzip
import json
import os
import sys
import ctypes
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple


def _parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid)
        if handle == 0:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _acquire_lock(root: Path, season: int) -> Optional[Path]:
    lock_dir = root / "data" / "raw" / ".locks"
    _ensure_dir(lock_dir)
    lock_path = lock_dir / f"backfill_statcast_pitches_{int(season)}.lock"

    def _try_create() -> Optional[int]:
        try:
            return os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return None

    fd = _try_create()
    if fd is None:
        try:
            existing_text = lock_path.read_text(encoding="utf-8")
            existing_obj = json.loads(existing_text)
            existing_pid = int(existing_obj.get("pid") or 0)
        except Exception:
            existing_text = "(unable to read lock contents)"
            existing_pid = 0

        if existing_pid and not _pid_is_running(existing_pid):
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                pass
            fd = _try_create()

        if fd is None:
            print(f"Lock exists: {lock_path}")
            print(existing_text)
            print("If this is stale, delete the lock file and re-run.")
            return None

    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(
            {
                "pid": os.getpid(),
                "argv": sys.argv,
                "started_utc": datetime.utcnow().isoformat() + "Z",
            },
            f,
        )

    def _release() -> None:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass

    atexit.register(_release)
    return lock_path


def _month_window(d: datetime) -> Tuple[datetime, datetime]:
    start = datetime(d.year, d.month, 1)
    if d.month == 12:
        end = datetime(d.year + 1, 1, 1) - timedelta(days=1)
    else:
        end = datetime(d.year, d.month + 1, 1) - timedelta(days=1)
    return start, end


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill Statcast pitch events to data/raw/statcast/pitches/ (x64 only)")
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--overwrite", choices=["on", "off"], default="off")
    ap.add_argument("--chunk", choices=["month", "range"], default="month", help="Fetch in monthly partitions or one range")
    args = ap.parse_args()

    try:
        from pybaseball import statcast  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "pybaseball import failed. Run this with the Windows x64 venv (.venv_x64) where pybaseball is installed."
        ) from e

    start = _parse_ymd(args.start_date)
    end = _parse_ymd(args.end_date)
    if end < start:
        raise SystemExit("end-date must be >= start-date")

    # Resolve project root (MLB-BettingV2/) based on this file's location.
    root = Path(__file__).resolve().parents[2]

    if _acquire_lock(root, int(args.season)) is None:
        return 3

    out_root = Path(args.out) if args.out else (root / "data" / "raw" / "statcast" / "pitches" / str(int(args.season)))
    _ensure_dir(out_root)

    def write_window(ws: datetime, we: datetime) -> None:
        ws_s = ws.strftime("%Y-%m-%d")
        we_s = we.strftime("%Y-%m-%d")
        ym = ws.strftime("%Y-%m")
        part_dir = out_root / ym
        _ensure_dir(part_dir)
        out_path = part_dir / f"statcast_{ws_s}_{we_s}.csv.gz"
        if out_path.exists() and args.overwrite == "off":
            print(f"Skip (exists): {out_path}")
            return

        print(f"Fetching Statcast: {ws_s}..{we_s}")
        df = statcast(ws_s, we_s)
        if df is None or len(df) == 0:
            print("No rows")
            return

        tmp = out_path.with_suffix(out_path.suffix + ".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
            df.to_csv(f, index=False)
        tmp.replace(out_path)
        print(f"Wrote: {out_path} rows={len(df)}")

    if args.chunk == "range":
        write_window(start, end)
        return 0

    # month partition
    cur = datetime(start.year, start.month, 1)
    while cur <= end:
        ws, we = _month_window(cur)
        if ws < start:
            ws = start
        if we > end:
            we = end
        write_window(ws, we)
        # next month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
