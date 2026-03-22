from __future__ import annotations

import argparse
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


# Ensure project root importable (MLB-BettingV2/)
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_date(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _list_candidate_days(raw_root: Path, start: str, end: str, min_games: int) -> List[Tuple[str, int]]:
    start_d = _parse_date(start)
    end_d = _parse_date(end)

    out: List[Tuple[str, int]] = []
    if not raw_root.exists():
        return out

    for p in sorted(raw_root.iterdir()):
        if not p.is_dir():
            continue
        ds = p.name
        if not _DATE_RE.match(ds):
            continue
        try:
            d = _parse_date(ds)
        except Exception:
            continue
        if d < start_d or d > end_d:
            continue

        # Each file corresponds to one gamePk (stored as *.json.gz).
        n_games = 0
        try:
            for f in p.iterdir():
                if not f.is_file():
                    continue
                nm = f.name.lower()
                if nm.endswith(".json.gz") or nm.endswith(".json"):
                    n_games += 1
        except Exception:
            continue

        if n_games >= int(min_games):
            out.append((ds, int(n_games)))

    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Select fully random MLB game-days based on available StatsAPI feed_live raw folders"
    )
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--start-date", default="2025-03-01")
    ap.add_argument("--end-date", default="2025-11-30")
    ap.add_argument("--min-games", type=int, default=10, help="Minimum games on a day to qualify")
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--out-date-file", default="", help="Write dates (YYYY-MM-DD) one per line")
    args = ap.parse_args()

    year = int(args.year)
    raw_root = _ROOT / "data" / "raw" / "statsapi" / "feed_live" / str(year)

    candidates = _list_candidate_days(
        raw_root=raw_root,
        start=str(args.start_date),
        end=str(args.end_date),
        min_games=int(args.min_games),
    )

    if not candidates:
        print(f"No qualifying days found under: {raw_root}")
        return 2

    if int(args.n) > len(candidates):
        print(f"Requested n={int(args.n)} but only {len(candidates)} qualifying days exist; sampling all.")
        n = len(candidates)
    else:
        n = int(args.n)

    rng = random.Random(int(args.seed))
    picked = rng.sample(candidates, k=n)
    picked_sorted = sorted(picked, key=lambda x: x[0])

    print(
        f"Qualifying days: {len(candidates)} (min_games={int(args.min_games)}; range={args.start_date}..{args.end_date})"
    )
    print(f"Picked: {n} (seed={int(args.seed)})")

    for ds, ng in picked_sorted:
        print(f"{ds}  games={ng}")

    out_path = str(args.out_date_file or "").strip()
    if out_path:
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n".join([d for d, _ in picked_sorted]) + "\n", encoding="utf-8")
        print(f"Wrote: {p}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
