from __future__ import annotations

import argparse
import glob
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class DayCoverage:
    date: str
    starting_pitchers: int
    unmatched_starting_pitchers: int
    matched_starting_pitchers: int
    props_pitchers: int
    so_lines: int
    outs_lines: int
    both_lines: int
    matching_report_path: str
    last_known_lines_path: str


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _token_to_date(token: str) -> str:
    return token.replace("_", "-")


def _count_lines(last_known: Dict[str, Any]) -> Tuple[int, int, int]:
    pitchers = (last_known.get("pitchers") or {}) if isinstance(last_known, dict) else {}
    if not isinstance(pitchers, dict) or not pitchers:
        return 0, 0, 0

    so = 0
    outs = 0
    both = 0
    for v in pitchers.values():
        if not isinstance(v, dict):
            continue
        so_obj = v.get("strikeouts")
        outs_obj = v.get("outs")
        has_so = isinstance(so_obj, dict) and (so_obj.get("line") is not None)
        has_outs = isinstance(outs_obj, dict) and (outs_obj.get("line") is not None)
        if has_so:
            so += 1
        if has_outs:
            outs += 1
        if has_so and has_outs:
            both += 1
    return int(so), int(outs), int(both)


def scan_days(original_repo_root: Path, *, year: int) -> List[DayCoverage]:
    daily_bovada = original_repo_root / "data" / "daily_bovada"
    reports = sorted(glob.glob(str(daily_bovada / f"matching_report_{year}_*.json")))

    out: List[DayCoverage] = []
    for rp in reports:
        m = re.search(r"matching_report_(\d{4}_\d{2}_\d{2})\.json$", rp)
        if not m:
            continue
        token = m.group(1)
        date = _token_to_date(token)

        report_path = Path(rp)
        report = _load_json(report_path)
        if not report:
            continue

        sp = int(report.get("starting_pitchers_count") or 0)
        um = len(report.get("unmatched_starting_pitchers") or [])
        pp = int(report.get("props_pitchers_count") or 0)
        matched = int(sp - um)

        last_known_path = daily_bovada / f"pitcher_last_known_lines_{token}.json"
        last_known = _load_json(last_known_path) or {}
        so_lines, outs_lines, both_lines = _count_lines(last_known)

        out.append(
            DayCoverage(
                date=str(date),
                starting_pitchers=int(sp),
                unmatched_starting_pitchers=int(um),
                matched_starting_pitchers=int(matched),
                props_pitchers=int(pp),
                so_lines=int(so_lines),
                outs_lines=int(outs_lines),
                both_lines=int(both_lines),
                matching_report_path=str(report_path),
                last_known_lines_path=str(last_known_path),
            )
        )

    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Select 2025 days with strong pitcher prop line coverage")
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--original-repo-root", default="", help="Path to MLB-Betting repo (defaults to sibling)")
    ap.add_argument("--months", default="", help="Comma list like 09,10 (optional)")
    ap.add_argument("--min-matched-starters", type=int, default=20)
    ap.add_argument("--max-unmatched", type=int, default=2)
    ap.add_argument("--min-so-lines", type=int, default=20)
    ap.add_argument("--min-outs-lines", type=int, default=15)
    ap.add_argument("--random", type=int, default=0, help="Randomly sample N dates from filtered set")
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--out-date-file", default="", help="Optional output text file with one date per line")
    args = ap.parse_args()

    v2_root = Path(__file__).resolve().parents[2]
    orig = Path(args.original_repo_root) if str(args.original_repo_root).strip() else (v2_root.parent / "MLB-Betting")

    months: Optional[set[str]] = None
    if str(args.months).strip():
        months = set(x.strip() for x in str(args.months).split(",") if x.strip())

    days = scan_days(orig, year=int(args.year))
    if months is not None:
        days = [d for d in days if len(d.date) >= 7 and d.date[5:7] in months]

    days = [
        d
        for d in days
        if d.matched_starting_pitchers >= int(args.min_matched_starters)
        and d.unmatched_starting_pitchers <= int(args.max_unmatched)
        and d.so_lines >= int(args.min_so_lines)
        and d.outs_lines >= int(args.min_outs_lines)
    ]

    # Sort best-first
    days.sort(key=lambda d: (d.matched_starting_pitchers, d.outs_lines, d.so_lines, -d.unmatched_starting_pitchers, d.props_pitchers), reverse=True)

    if int(args.random or 0) > 0:
        rng = random.Random(int(args.seed))
        n = min(int(args.random), len(days))
        days = rng.sample(days, n)
        days.sort(key=lambda d: d.date)

    for d in days:
        print(
            f"{d.date} matched {d.matched_starting_pitchers}/{d.starting_pitchers} "
            f"(unmatched {d.unmatched_starting_pitchers}) | "
            f"lines: SO {d.so_lines}, outs {d.outs_lines}, both {d.both_lines} | props_pitchers {d.props_pitchers}"
        )

    if str(args.out_date_file).strip():
        out_path = Path(str(args.out_date_file))
        if not out_path.is_absolute():
            out_path = (Path.cwd() / out_path).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join([d.date for d in days]) + "\n", encoding="utf-8")
        print(f"Wrote date file: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
