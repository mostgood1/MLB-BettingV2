from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# Ensure the project root (MLB-BettingV2/) is importable.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import StatsApiClient, fetch_schedule_for_date, fetch_game_feed_live, parse_confirmed_lineup_ids


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _feature_paths(season: int) -> Tuple[Path, Optional[Path]]:
    feats = _ROOT / "data" / "statcast" / "features" / f"player_features_{int(season)}.json"
    qual = _ROOT / "data" / "statcast" / "quality" / f"player_quality_{int(season)}.json"
    return feats, (qual if qual.exists() else None)


def _iter_lineup_ids(feed: Dict[str, Any]) -> Iterable[Tuple[str, int]]:
    # returns (side, player_id)
    for side in ("away", "home"):
        ids = parse_confirmed_lineup_ids(feed, side)
        for pid in ids:
            try:
                pid_i = int(pid)
            except Exception:
                continue
            if pid_i > 0:
                yield side, pid_i


def _starter_id_from_probable(game_obj: Dict[str, Any], side: str) -> Optional[int]:
    try:
        t = ((game_obj.get("teams") or {}).get(side) or {})
        prob = (t.get("probablePitcher") or {})
        pid = prob.get("id")
        return int(pid) if pid is not None else None
    except Exception:
        return None


def _has_nonempty(d: Any, key: str) -> bool:
    if not isinstance(d, dict):
        return False
    v = d.get(key)
    return isinstance(v, dict) and len(v) > 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Report coverage of local Statcast player feature artifacts for confirmed lineups")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--max-games", type=int, default=0, help="0 = all games on schedule")
    args = ap.parse_args()

    season = int(args.season)
    date_str = str(args.date)

    feats_path, qual_path = _feature_paths(season)
    if not feats_path.exists():
        print(f"Missing required Statcast features file: {feats_path}")
        print("Run: tools/datasets/build_statcast_player_feature_set.py --season <season> --start-date <...> --end-date <...>")
        return 2

    feats = _load_json(feats_path)
    batters = (feats.get("batters") or {}) if isinstance(feats, dict) else {}
    pitchers = (feats.get("pitchers") or {}) if isinstance(feats, dict) else {}

    quality = None
    if qual_path is not None:
        try:
            quality = _load_json(qual_path)
        except Exception:
            quality = None

    client = StatsApiClient()
    games = fetch_schedule_for_date(client, date_str)
    if int(args.max_games) > 0:
        games = games[: int(args.max_games)]

    agg = {
        "games": 0,
        "lineup_batters": 0,
        "batters_in_features": 0,
        "batters_with_mult_overall": 0,
        "batters_with_vs_pitch_type": 0,
        "probable_pitchers": 0,
        "probable_pitchers_in_features": 0,
        "probable_pitchers_with_mult_overall": 0,
        "probable_pitchers_with_pitch_mix": 0,
        "quality_map_present": bool(isinstance(quality, dict) and quality),
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

        for _side, pid in _iter_lineup_ids(feed):
            agg["lineup_batters"] += 1
            entry = batters.get(str(pid)) if isinstance(batters, dict) else None
            if isinstance(entry, dict):
                agg["batters_in_features"] += 1
                if _has_nonempty(entry, "mult_overall"):
                    agg["batters_with_mult_overall"] += 1
                if _has_nonempty(entry, "vs_pitch_type"):
                    agg["batters_with_vs_pitch_type"] += 1

        for side in ("away", "home"):
            spid = _starter_id_from_probable(g, side)
            if spid is None or spid <= 0:
                continue
            agg["probable_pitchers"] += 1
            pentry = pitchers.get(str(spid)) if isinstance(pitchers, dict) else None
            if isinstance(pentry, dict):
                agg["probable_pitchers_in_features"] += 1
                if _has_nonempty(pentry, "mult_overall"):
                    agg["probable_pitchers_with_mult_overall"] += 1
                if _has_nonempty(pentry, "pitch_mix"):
                    agg["probable_pitchers_with_pitch_mix"] += 1

    def frac(a: int, b: int) -> float:
        return float(a) / float(b) if b else 0.0

    out = {
        "date": date_str,
        "season": season,
        "inputs": {
            "features_path": str(feats_path),
            "quality_path": str(qual_path) if qual_path is not None else "",
            "max_games": int(args.max_games),
        },
        "counts": agg,
        "rates": {
            "batters_in_features": frac(agg["batters_in_features"], agg["lineup_batters"]),
            "batters_with_mult_overall": frac(agg["batters_with_mult_overall"], agg["lineup_batters"]),
            "batters_with_vs_pitch_type": frac(agg["batters_with_vs_pitch_type"], agg["lineup_batters"]),
            "probable_pitchers_in_features": frac(agg["probable_pitchers_in_features"], agg["probable_pitchers"]),
            "probable_pitchers_with_mult_overall": frac(agg["probable_pitchers_with_mult_overall"], agg["probable_pitchers"]),
            "probable_pitchers_with_pitch_mix": frac(agg["probable_pitchers_with_pitch_mix"], agg["probable_pitchers"]),
        },
    }

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
