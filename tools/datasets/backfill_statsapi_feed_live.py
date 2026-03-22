from __future__ import annotations

import atexit
import argparse
import gzip
import json
import os
import sys
import time
import ctypes
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import StatsApiClient


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
    lock_path = lock_dir / f"backfill_statsapi_feed_live_{int(season)}.lock"

    def _try_create() -> Optional[int]:
        try:
            return os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return None

    fd = _try_create()
    if fd is None:
        # If the lock looks stale (PID no longer alive), clear it and retry once.
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


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _daterange(start: datetime, end: datetime) -> List[str]:
    out: List[str] = []
    d = start
    while d <= end:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out


def _write_gz_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(obj, f)
    tmp.replace(path)


def _fetch_schedule(client: StatsApiClient, date_str: str) -> List[Dict[str, Any]]:
    data = client.get(
        "/schedule",
        params={
            "sportId": 1,
            "date": date_str,
            "hydrate": "team",
        },
    )
    games: List[Dict[str, Any]] = []
    for d in data.get("dates", []) or []:
        for g in d.get("games", []) or []:
            games.append(g)
    return games


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill StatsAPI live feed payloads to data/raw/statsapi/feed_live/")
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--out", default=str(_ROOT / "data" / "raw" / "statsapi" / "feed_live"))
    ap.add_argument("--sleep-ms", type=int, default=50)
    ap.add_argument("--overwrite", choices=["on", "off"], default="off")
    ap.add_argument("--cache-ttl-hours", type=int, default=24)
    args = ap.parse_args()

    if _acquire_lock(_ROOT, int(args.season)) is None:
        return 3

    client = StatsApiClient.with_default_cache(ttl_seconds=int(args.cache_ttl_hours * 3600))

    start = _parse_ymd(args.start_date)
    end = _parse_ymd(args.end_date)
    dates = _daterange(start, end)

    out_root = Path(args.out) / str(int(args.season))
    _ensure_dir(out_root)

    total_games = 0
    wrote = 0
    skipped = 0
    errors = 0

    for di, d in enumerate(dates, start=1):
        games = _fetch_schedule(client, d)
        if not games:
            continue

        day_dir = out_root / d
        _ensure_dir(day_dir)

        for g in games:
            pk = g.get("gamePk")
            if not pk:
                continue
            try:
                pk_i = int(pk)
            except Exception:
                continue

            total_games += 1
            out_path = day_dir / f"{pk_i}.json.gz"
            if out_path.exists() and args.overwrite == "off":
                skipped += 1
                continue

            url = f"https://statsapi.mlb.com/api/v1.1/game/{pk_i}/feed/live"
            try:
                payload = client.get(url)
                _write_gz_json(out_path, payload)
                wrote += 1
            except Exception as e:
                errors += 1
                print(f"ERROR gamePk={pk_i} date={d}: {e}")

            if args.sleep_ms > 0:
                time.sleep(float(args.sleep_ms) / 1000.0)

        if di % 7 == 0:
            print(f"Progress: {di}/{len(dates)} days, wrote={wrote} skipped={skipped} errors={errors}")

    print(f"Done. days={len(dates)} games={total_games} wrote={wrote} skipped={skipped} errors={errors}")
    print(f"Out: {out_root}")
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
