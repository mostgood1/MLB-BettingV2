from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import StatsApiClient


def _parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _daterange(start: datetime, end: datetime) -> List[str]:
    out: List[str] = []
    d = start
    while d <= end:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out


def _fetch_schedule_game_pks(client: StatsApiClient, date_str: str) -> List[Dict[str, Any]]:
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


def _game_meta(g: Dict[str, Any]) -> Dict[str, Any]:
    status = (g.get("status") or {})
    teams = (g.get("teams") or {})
    away = (teams.get("away") or {}).get("team") or {}
    home = (teams.get("home") or {}).get("team") or {}

    return {
        "game_pk": g.get("gamePk"),
        "game_type": g.get("gameType"),
        "double_header": g.get("doubleHeader"),
        "game_number": g.get("gameNumber"),
        "series_game_number": g.get("seriesGameNumber"),
        "status": {
            "abstract": status.get("abstractGameState"),
            "detailed": status.get("detailedState"),
        },
        "away": {"id": away.get("id"), "name": away.get("name"), "abbreviation": away.get("abbreviation")},
        "home": {"id": home.get("id"), "name": home.get("name"), "abbreviation": home.get("abbreviation")},
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Report coverage of data/raw/statsapi/feed_live/<season>/<date>/<gamePk>.json.gz by comparing against StatsAPI schedule"
        )
    )
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument(
        "--feed-live-root",
        default=str(_ROOT / "data" / "raw" / "statsapi" / "feed_live"),
        help="Root of raw feed/live gz payloads",
    )
    ap.add_argument(
        "--out",
        default="",
        help="Optional output path for JSON report (defaults to data/raw/statsapi/feed_live_coverage_<season>_<start>_<end>.json)",
    )
    ap.add_argument("--cache-ttl-hours", type=int, default=24)
    args = ap.parse_args()

    start = _parse_ymd(args.start_date)
    end = _parse_ymd(args.end_date)
    if end < start:
        raise SystemExit("end-date must be >= start-date")

    client = StatsApiClient.with_default_cache(ttl_seconds=int(args.cache_ttl_hours * 3600))

    feed_root = Path(args.feed_live_root) / str(int(args.season))
    dates = _daterange(start, end)

    missing_by_date: Dict[str, List[Dict[str, Any]]] = {}
    present_games = 0
    expected_games = 0

    for date_str in dates:
        games = _fetch_schedule_game_pks(client, date_str)
        if not games:
            continue

        expected: List[Tuple[int, Dict[str, Any]]] = []
        for g in games:
            try:
                pk = int(g.get("gamePk"))
            except Exception:
                continue
            expected.append((pk, g))

        if not expected:
            continue

        expected_games += len(expected)
        day_dir = feed_root / date_str

        missing: List[Dict[str, Any]] = []
        for pk, g in expected:
            path = day_dir / f"{pk}.json.gz"
            if path.exists():
                present_games += 1
            else:
                missing.append(_game_meta(g))

        if missing:
            missing_by_date[date_str] = missing

    report = {
        "season": int(args.season),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "expected_games": int(expected_games),
        "present_games": int(present_games),
        "missing_games": int(expected_games - present_games),
        "missing_dates": int(len(missing_by_date)),
        "missing_by_date": missing_by_date,
        "generated_at": datetime.now().isoformat(),
        "feed_live_root": str(feed_root),
        "notes": [
            "Compares StatsAPI schedule gamePks to raw feed/live files.",
            "Doubleheaders are handled naturally (distinct gamePk/gameNumber).",
        ],
    }

    if args.out:
        out_path = Path(args.out)
    else:
        out_path = _ROOT / "data" / "raw" / f"statsapi_feed_live_coverage_{int(args.season)}_{args.start_date}_{args.end_date}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(
        f"Coverage: expected_games={expected_games} present_games={present_games} missing_games={expected_games - present_games} missing_dates={len(missing_by_date)}"
    )
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
