from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.env import load_dotenv_if_present
from tools.oddsapi.backfill_oddsapi_historical_2025 import (
    API_BASE,
    DEFAULT_HITTER_MARKETS,
    PITCHER_MARKET_KEY_MAP,
    SPORT,
    _as_events_list,
    _event_matches_slate_date,
    _extract_game_lines,
    _extract_player_props,
    _finalize_prop_market_map,
    _http_get,
    _merge_prop_market_rows,
)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    _ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _unwrap_live_odds_payload(obj: Any) -> Optional[Dict[str, Any]]:
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        for row in obj:
            if isinstance(row, dict):
                return row
    return None


def _fetch_live_events_for_date(api_key: str, date_str: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/sports/{SPORT}/events"
    raw, _ = _http_get(url, {"apiKey": api_key})
    events = _as_events_list(raw)
    return [event for event in events if _event_matches_slate_date(event, date_str)]


def _fetch_live_event_odds(
    api_key: str,
    event_id: str,
    *,
    markets_csv: str,
    regions: str,
    bookmakers: Optional[str],
) -> Optional[Dict[str, Any]]:
    url = f"{API_BASE}/sports/{SPORT}/events/{event_id}/odds"
    params: Dict[str, Any] = {
        "apiKey": api_key,
        "regions": str(regions or "us"),
        "oddsFormat": "american",
        "markets": str(markets_csv or "").strip(),
    }
    if bookmakers:
        params["bookmakers"] = str(bookmakers)
    try:
        raw, _ = _http_get(url, params)
    except requests.HTTPError:
        return None
    return _unwrap_live_odds_payload(raw)


def _best_bookmaker_game_lines(payload: Dict[str, Any], *, home_team: str, away_team: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    best_lines: Optional[Dict[str, Any]] = None
    best_key: Optional[str] = None
    best_score = -1
    for bookmaker in (payload.get("bookmakers") or []):
        if not isinstance(bookmaker, dict):
            continue
        lines = _extract_game_lines(
            bookmaker.get("markets"),
            home_team=str(home_team or ""),
            away_team=str(away_team or ""),
        )
        score = int(bool(lines.get("h2h"))) + int(bool(lines.get("totals"))) + int(bool(lines.get("spreads")))
        if score <= best_score:
            continue
        if score <= 0:
            continue
        best_lines = lines
        best_key = str(bookmaker.get("key") or bookmaker.get("title") or "")
        best_score = score
    return best_lines, best_key


def _prop_market_counts(props_by_name: Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Any]:
    players = 0
    markets: Dict[str, int] = {}
    for market_rows in (props_by_name or {}).values():
        if not isinstance(market_rows, dict):
            continue
        player_has_market = False
        for market_name, row in market_rows.items():
            if not isinstance(row, dict):
                continue
            if row.get("line") is None:
                continue
            player_has_market = True
            markets[str(market_name)] = int(markets.get(str(market_name), 0) + 1)
        if player_has_market:
            players += 1
    return {
        "players": int(players),
        "markets": {key: int(value) for key, value in sorted(markets.items())},
    }


def fetch_live_game_lines_for_date(
    api_key: str,
    date_str: str,
    *,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    live_events = list(events or _fetch_live_events_for_date(api_key, date_str))
    games: List[Dict[str, Any]] = []
    for event in live_events:
        event_id = str(event.get("id") or "").strip()
        if not event_id:
            continue
        payload = _fetch_live_event_odds(
            api_key,
            event_id,
            markets_csv="h2h,spreads,totals",
            regions=regions,
            bookmakers=bookmakers,
        )
        if not isinstance(payload, dict):
            continue
        home_team = str(event.get("home_team") or payload.get("home_team") or "")
        away_team = str(event.get("away_team") or payload.get("away_team") or "")
        best_lines, bookmaker_key = _best_bookmaker_game_lines(payload, home_team=home_team, away_team=away_team)
        if not isinstance(best_lines, dict):
            continue
        games.append(
            {
                "event_id": event_id,
                "commence_time": event.get("commence_time") or payload.get("commence_time"),
                "home_team": home_team,
                "away_team": away_team,
                "bookmaker": bookmaker_key,
                "markets": best_lines,
            }
        )

    counts = {
        "events_matched": int(len(live_events)),
        "games": int(len(games)),
        "h2h_games": int(sum(1 for row in games if isinstance((row.get("markets") or {}).get("h2h"), dict))),
        "totals_games": int(sum(1 for row in games if isinstance((row.get("markets") or {}).get("totals"), dict))),
        "spreads_games": int(sum(1 for row in games if isinstance((row.get("markets") or {}).get("spreads"), dict))),
    }
    return {
        "date": str(date_str),
        "mode": "live",
        "retrieved_at": datetime.utcnow().isoformat(),
        "games": games,
        "meta": {
            "markets": ["h2h", "spreads", "totals"],
            "regions": str(regions or "us"),
            "bookmakers": (str(bookmakers).split(",") if bookmakers else None),
            "counts": counts,
        },
    }


def fetch_live_pitcher_props_for_date(
    api_key: str,
    date_str: str,
    *,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    live_events = list(events or _fetch_live_events_for_date(api_key, date_str))
    desired_markets = list(PITCHER_MARKET_KEY_MAP.keys())
    pitcher_props: Dict[str, Dict[str, Dict[str, Any]]] = {}
    market_warnings: List[str] = []

    for event in live_events:
        event_id = str(event.get("id") or "").strip()
        if not event_id:
            continue
        try:
            payload = _fetch_live_event_odds(
                api_key,
                event_id,
                markets_csv=",".join(desired_markets),
                regions=regions,
                bookmakers=bookmakers,
            )
        except requests.HTTPError as exc:
            fallback_markets = [market for market in desired_markets if market != "pitcher_earned_runs"]
            if not fallback_markets:
                raise
            market_warnings.append(f"pitcher_earned_runs unavailable for event {event_id}; fetched legacy pitcher markets only")
            payload = _fetch_live_event_odds(
                api_key,
                event_id,
                markets_csv=",".join(fallback_markets),
                regions=regions,
                bookmakers=bookmakers,
            )
        if not isinstance(payload, dict):
            continue
        for bookmaker in (payload.get("bookmakers") or []):
            if not isinstance(bookmaker, dict):
                continue
            extracted = _extract_player_props(bookmaker.get("markets"), key_map=PITCHER_MARKET_KEY_MAP)
            for name, market_rows in extracted.items():
                dst = pitcher_props.setdefault(name, {})
                for market_name, row in market_rows.items():
                    dst[market_name] = _merge_prop_market_rows(dst.get(market_name, {}), row)

    finalized = _finalize_prop_market_map(pitcher_props)
    counts = _prop_market_counts(finalized)
    counts["events_matched"] = int(len(live_events))
    return {
        "date": str(date_str),
        "mode": "live",
        "retrieved_at": datetime.utcnow().isoformat(),
        "pitcher_props": finalized,
        "meta": {
            "markets": desired_markets,
            "regions": str(regions or "us"),
            "bookmakers": (str(bookmakers).split(",") if bookmakers else None),
            "counts": counts,
            "warnings": market_warnings,
        },
    }


def fetch_live_hitter_props_for_date(
    api_key: str,
    date_str: str,
    *,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    markets: Optional[List[str]] = None,
    events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    live_events = list(events or _fetch_live_events_for_date(api_key, date_str))
    desired_markets = [str(m).strip().lower() for m in (markets or []) if str(m).strip()]
    if not desired_markets:
        desired_markets = [str(m).strip().lower() for m in DEFAULT_HITTER_MARKETS]
    key_map = {market_name: market_name for market_name in desired_markets}
    markets_csv = ",".join(desired_markets)
    hitter_props: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for event in live_events:
        event_id = str(event.get("id") or "").strip()
        if not event_id:
            continue
        payload = _fetch_live_event_odds(
            api_key,
            event_id,
            markets_csv=markets_csv,
            regions=regions,
            bookmakers=bookmakers,
        )
        if not isinstance(payload, dict):
            continue
        for bookmaker in (payload.get("bookmakers") or []):
            if not isinstance(bookmaker, dict):
                continue
            extracted = _extract_player_props(bookmaker.get("markets"), key_map=key_map)
            for name, market_rows in extracted.items():
                dst = hitter_props.setdefault(name, {})
                for market_name, row in market_rows.items():
                    dst[market_name] = _merge_prop_market_rows(dst.get(market_name, {}), row)

    finalized = _finalize_prop_market_map(hitter_props)
    counts = _prop_market_counts(finalized)
    counts["events_matched"] = int(len(live_events))
    return {
        "date": str(date_str),
        "mode": "live",
        "retrieved_at": datetime.utcnow().isoformat(),
        "hitter_props": finalized,
        "meta": {
            "markets": desired_markets,
            "regions": str(regions or "us"),
            "bookmakers": (str(bookmakers).split(",") if bookmakers else None),
            "counts": counts,
        },
    }


def fetch_and_write_live_odds_for_date(
    date_str: str,
    *,
    out_dir: Optional[Path] = None,
    overwrite: bool = True,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    hitter_markets: Optional[List[str]] = None,
) -> Dict[str, Any]:
    load_dotenv_if_present(_ROOT / ".env")
    api_key = os.environ.get("ODDS_API_KEY") or os.environ.get("ODDSAPI_KEY")
    if not api_key:
        raise RuntimeError("ODDS_API_KEY not set")

    target_dir = Path(out_dir) if out_dir else (_ROOT / "data" / "market" / "oddsapi")
    _ensure_dir(target_dir)
    token = str(date_str).replace("-", "_")
    game_lines_path = target_dir / f"oddsapi_game_lines_{token}.json"
    pitcher_props_path = target_dir / f"oddsapi_pitcher_props_{token}.json"
    hitter_props_path = target_dir / f"oddsapi_hitter_props_{token}.json"

    if not overwrite and game_lines_path.exists() and pitcher_props_path.exists() and hitter_props_path.exists():
        return {
            "status": "skipped",
            "date": str(date_str),
            "out_dir": str(target_dir),
            "game_lines_path": str(game_lines_path),
            "pitcher_props_path": str(pitcher_props_path),
            "hitter_props_path": str(hitter_props_path),
            "reason": "overwrite=off and market files already exist",
        }

    live_events = _fetch_live_events_for_date(api_key, date_str)
    game_lines_doc = fetch_live_game_lines_for_date(
        api_key,
        date_str,
        regions=regions,
        bookmakers=bookmakers,
        events=live_events,
    )
    pitcher_props_doc = fetch_live_pitcher_props_for_date(
        api_key,
        date_str,
        regions=regions,
        bookmakers=bookmakers,
        events=live_events,
    )
    hitter_props_doc = fetch_live_hitter_props_for_date(
        api_key,
        date_str,
        regions=regions,
        bookmakers=bookmakers,
        markets=hitter_markets,
        events=live_events,
    )

    _write_json(game_lines_path, game_lines_doc)
    _write_json(pitcher_props_path, pitcher_props_doc)
    _write_json(hitter_props_path, hitter_props_doc)

    return {
        "status": "ok",
        "date": str(date_str),
        "mode": "live",
        "out_dir": str(target_dir),
        "game_lines_path": str(game_lines_path),
        "pitcher_props_path": str(pitcher_props_path),
        "hitter_props_path": str(hitter_props_path),
        "counts": {
            "game_lines": dict(((game_lines_doc.get("meta") or {}).get("counts") or {})),
            "pitcher_props": dict(((pitcher_props_doc.get("meta") or {}).get("counts") or {})),
            "hitter_props": dict(((hitter_props_doc.get("meta") or {}).get("counts") or {})),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch current-day live OddsAPI markets into canonical MLB-BettingV2 market files.")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--regions", default="us")
    ap.add_argument("--bookmakers", default=None)
    ap.add_argument(
        "--hitter-markets",
        default=None,
        help="Comma-separated hitter market keys. Defaults to the built-in live set.",
    )
    ap.add_argument("--out-dir", default=str(_ROOT / "data" / "market" / "oddsapi"))
    ap.add_argument("--overwrite", choices=["on", "off"], default="on")
    args = ap.parse_args()

    hitter_markets = None
    if args.hitter_markets:
        hitter_markets = [part.strip() for part in str(args.hitter_markets).split(",") if part.strip()]

    try:
        result = fetch_and_write_live_odds_for_date(
            str(args.date),
            out_dir=Path(str(args.out_dir)),
            overwrite=(str(args.overwrite) == "on"),
            regions=str(args.regions or "us"),
            bookmakers=(str(args.bookmakers).strip() if args.bookmakers else None),
            hitter_markets=hitter_markets,
        )
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}")
        print(traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())