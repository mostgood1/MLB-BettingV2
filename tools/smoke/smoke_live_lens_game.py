from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import StatsApiClient, fetch_schedule_for_date
from tools.web import flask_frontend as ff


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _pick_game(date_str: str, game_pk: Optional[int], game_index: int) -> Optional[Dict[str, Any]]:
    if game_pk is None:
        client = StatsApiClient.with_default_cache(ttl_seconds=24 * 3600)
        games = fetch_schedule_for_date(client, date_str) or []
        if not games:
            return None
        idx = min(max(int(game_index), 0), len(games) - 1)
        return games[idx]

    client = StatsApiClient.with_default_cache(ttl_seconds=24 * 3600)
    games = fetch_schedule_for_date(client, date_str) or []
    for game in games:
        if int(_safe_int(game.get("gamePk")) or 0) == int(game_pk):
            return game
    return {"gamePk": int(game_pk)}


def _status_summary(payload: Dict[str, Any]) -> str:
    status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
    abstract = str(status.get("abstractGameState") or status.get("abstract") or "").strip()
    detailed = str(status.get("detailedState") or status.get("detailed") or "").strip()
    return detailed or abstract or "unknown"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    ap.add_argument("--game-pk", type=int, default=None)
    ap.add_argument("--game-index", type=int, default=0)
    ap.add_argument(
        "--raw-feed-path",
        default="",
        help="Optional path to a captured raw feed JSON to use instead of fetching live feed data.",
    )
    ap.add_argument(
        "--allow-nonlive",
        choices=["off", "on"],
        default="off",
        help="Allow smoke to pass on a non-live game state.",
    )
    ap.add_argument(
        "--require-live-source",
        choices=["off", "on"],
        default="on",
        help="Require the live game lane to use source=live_mc.",
    )
    args = ap.parse_args()

    game = _pick_game(str(args.date), args.game_pk, int(args.game_index))
    if not isinstance(game, dict):
        print(f"No scheduled game found for {args.date}")
        return 2

    target_game_pk = _safe_int(game.get("gamePk"))
    if target_game_pk is None or int(target_game_pk) <= 0:
        print("Missing valid gamePk")
        return 2

    feed_override = None
    if str(args.raw_feed_path or "").strip():
        feed_path = Path(str(args.raw_feed_path)).resolve()
        if not feed_path.exists():
            print(f"Raw feed file not found: {feed_path}")
            return 2
        feed_override = json.loads(feed_path.read_text(encoding="utf-8"))

    original_loader = ff._load_live_lens_feed
    if isinstance(feed_override, dict):
        def _patched_loader(game_pk: int, d: str) -> Optional[Dict[str, Any]]:
            if int(game_pk) == int(target_game_pk) and str(d) == str(args.date):
                return dict(feed_override)
            return original_loader(game_pk, d)

        ff._load_live_lens_feed = _patched_loader

    try:
        payload = ff._build_game_sim_payload(int(target_game_pk), str(args.date))
        feed = ff._load_live_lens_feed(int(target_game_pk), str(args.date))
        snapshot = ff._load_live_lens_snapshot(int(target_game_pk), str(args.date), feed=feed)
    finally:
        ff._load_live_lens_feed = original_loader

    found = bool(payload.get("found"))
    status_text = _status_summary(snapshot if isinstance(snapshot, dict) else payload)
    live_props = payload.get("livePropRows") if isinstance(payload.get("livePropRows"), list) else []
    game_lens = payload.get("gameLens") if isinstance(payload.get("gameLens"), list) else []
    live_lane = next((row for row in game_lens if isinstance(row, dict) and str(row.get("key") or "") == "live"), None)

    print(f"gamePk={int(target_game_pk)} date={args.date} found={found} status={status_text}")
    if isinstance(live_lane, dict):
        projection = live_lane.get("projection") if isinstance(live_lane.get("projection"), dict) else {}
        print(
            "live_lane="
            f"source={live_lane.get('source')} "
            f"homeWinProb={live_lane.get('modelHomeWinProb')} "
            f"home={projection.get('home')} away={projection.get('away')} total={projection.get('total')}"
        )
    else:
        print("live_lane=missing")

    print(f"live_prop_rows={len(live_props)}")
    for row in live_props[:5]:
        if not isinstance(row, dict):
            continue
        owner = str(row.get("pitcher_name") or row.get("player_name") or "").strip()
        print(
            "prop="
            f"{owner} {row.get('prop')} {row.get('selection')} "
            f"line={row.get('market_line')} liveProjection={row.get('live_projection')} source={row.get('source')}"
        )

    if not found:
        return 3

    status_token = status_text.strip().lower()
    is_live = status_token in {"in progress", "manager challenge"} or status_token.startswith(("top ", "bottom ", "mid ", "end ")) or status_token == "live"
    if not is_live and str(args.allow_nonlive) != "on":
        print("Smoke target is not currently live; rerun with --allow-nonlive on or provide --raw-feed-path for a captured live state.")
        return 4

    if str(args.require_live_source) == "on" and is_live:
        if not isinstance(live_lane, dict):
            print("Missing live lane in game lens payload")
            return 5
        if str(live_lane.get("source") or "") != "live_mc":
            print(f"Expected live lane source=live_mc, saw {live_lane.get('source')}")
            return 6

    return 0


if __name__ == "__main__":
    raise SystemExit(main())