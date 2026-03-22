from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


# Ensure the project root (MLB-BettingV2/) is importable.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import (
    StatsApiClient,
    fetch_game_feed_live,
    fetch_schedule_for_date,
    parse_confirmed_lineup_ids,
)
from sim_engine.data.build_roster import build_team, build_team_roster


def main() -> int:
    ap = argparse.ArgumentParser(description="Report platoon/statcast feature coverage on built rosters")
    ap.add_argument("--date", required=True)
    ap.add_argument("--season", type=int, default=0)
    ap.add_argument("--max-games", type=int, default=0, help="0=all")
    ap.add_argument("--cache-ttl-hours", type=int, default=24)
    args = ap.parse_args()

    season = int(args.season) if int(args.season or 0) > 0 else int(str(args.date).split("-")[0])
    client = StatsApiClient.with_default_cache(ttl_seconds=int(args.cache_ttl_hours * 3600))

    games = fetch_schedule_for_date(client, str(args.date))
    if int(args.max_games) > 0:
        games = games[: int(args.max_games)]

    agg: Dict[str, int] = {
        "games": 0,
        "batters": 0,
        "batters_platoon": 0,
        "batters_vs_pitch_type": 0,
        "batters_statcast": 0,
        "pitchers": 0,
        "pitchers_platoon": 0,
        "pitchers_statcast": 0,
    }

    for g in games:
        game_pk = g.get("gamePk")
        if game_pk is None:
            continue

        try:
            feed = fetch_game_feed_live(client, int(game_pk))
        except Exception:
            continue

        agg["games"] += 1

        teams = g.get("teams") or {}
        away_team = (teams.get("away") or {}).get("team") or {}
        home_team = (teams.get("home") or {}).get("team") or {}

        away_id = int(away_team.get("id"))
        home_id = int(home_team.get("id"))

        away_lineup_ids = parse_confirmed_lineup_ids(feed, "away")
        home_lineup_ids = parse_confirmed_lineup_ids(feed, "home")

        t_away = build_team(away_id, away_team.get("name") or "Away", away_team.get("abbreviation") or "AW")
        t_home = build_team(home_id, home_team.get("name") or "Home", home_team.get("abbreviation") or "HM")

        away_roster = build_team_roster(client, t_away, season, as_of_date=str(args.date), confirmed_lineup_ids=away_lineup_ids)
        home_roster = build_team_roster(client, t_home, season, as_of_date=str(args.date), confirmed_lineup_ids=home_lineup_ids)

        for roster in (away_roster, home_roster):
            for b in roster.lineup.batters:
                agg["batters"] += 1
                agg["batters_platoon"] += 1 if (b.platoon_mult_vs_lhp or b.platoon_mult_vs_rhp) else 0
                agg["batters_vs_pitch_type"] += 1 if (b.vs_pitch_type or {}) else 0
                agg["batters_statcast"] += 1 if (b.statcast_quality_mult or {}) else 0

            ps = [roster.lineup.pitcher] + list(roster.lineup.bullpen or [])
            for p in ps:
                agg["pitchers"] += 1
                agg["pitchers_platoon"] += 1 if (p.platoon_mult_vs_lhb or p.platoon_mult_vs_rhb) else 0
                agg["pitchers_statcast"] += 1 if (p.statcast_quality_mult or {}) else 0

    def frac(a: int, b: int) -> float:
        return float(a) / float(b) if b else 0.0

    out = {
        "date": str(args.date),
        "season": season,
        "counts": agg,
        "rates": {
            "batter_platoon": frac(agg["batters_platoon"], agg["batters"]),
            "batter_vs_pitch_type": frac(agg["batters_vs_pitch_type"], agg["batters"]),
            "batter_statcast": frac(agg["batters_statcast"], agg["batters"]),
            "pitcher_platoon": frac(agg["pitchers_platoon"], agg["pitchers"]),
            "pitcher_statcast": frac(agg["pitchers_statcast"], agg["pitchers"]),
        },
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
