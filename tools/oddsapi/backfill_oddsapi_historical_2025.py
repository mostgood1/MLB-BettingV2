from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

# Ensure the project root (MLB-BettingV2/) is importable.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.env import load_dotenv_if_present

API_BASE = "https://api.the-odds-api.com/v4"
SPORT = "baseball_mlb"
SCHEDULE_TZ = os.environ.get("SCHEDULE_TZ", "America/New_York")


DEFAULT_HITTER_MARKETS: List[str] = [
    # These keys are supported by OddsAPI for MLB player props, but availability varies by event/book.
    # We keep a small default set and rely on --probe-markets / snapshots for coverage.
    "batter_hits",
    "batter_total_bases",
    "batter_home_runs",
    "batter_rbis",
    "batter_runs_scored",
    "batter_strikeouts",
]


PITCHER_MARKET_KEY_MAP: Dict[str, str] = {
    "pitcher_strikeouts": "strikeouts",
    "pitcher_outs": "outs",
    "pitcher_earned_runs": "earned_runs",
}


PLAYER_PROP_PRIMARY_LINE_PREFERENCES: Dict[str, Tuple[float, ...]] = {
    "batter_home_runs": (0.5,),
    "batter_hits": (0.5,),
    "batter_rbis": (0.5,),
    "batter_runs_scored": (0.5,),
    "batter_total_bases": (1.5,),
    "earned_runs": (1.5, 2.5, 0.5, 3.5),
}


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    _ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _http_get(url: str, params: Dict[str, Any], timeout: int = 30) -> Tuple[Any, Dict[str, str]]:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json(), {k.lower(): v for k, v in r.headers.items()}


def _as_events_list(obj: Any) -> List[Dict[str, Any]]:
    """Normalize OddsAPI /historical/.../events responses to a list of event dicts."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for k in ("data", "events"):
            v = obj.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        # Some variants might wrap deeper; be conservative.
        return []
    return []


def _unwrap_odds_payload(obj: Any) -> Any:
        """Unwrap historical odds envelopes.

        OddsAPI historical endpoints often return:
            {timestamp, previous_timestamp, next_timestamp, data: {...event odds...}}
        where `data.bookmakers` contains the actual odds.
        """
        if isinstance(obj, dict):
                data = obj.get("data")
                if isinstance(data, (dict, list)):
                        return data
        return obj


def _parse_iso(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _format_iso_z(dt: datetime) -> str:
    dtu = dt
    if dtu.tzinfo is None:
        dtu = dtu.replace(tzinfo=timezone.utc)
    dtu = dtu.astimezone(timezone.utc).replace(microsecond=0)
    return dtu.strftime("%Y-%m-%dT%H:%M:%SZ")


def _event_matches_slate_date(event: Dict[str, Any], date_str: str) -> bool:
    dt = _parse_iso(str(event.get("commence_time") or ""))
    if dt is None:
        return False
    try:
        return dt.astimezone(ZoneInfo(SCHEDULE_TZ)).strftime("%Y-%m-%d") == str(date_str)
    except Exception:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d") == str(date_str)


def _normalize_snapshot_iso(date_str: str, t: str) -> Optional[str]:
    """Return a strict ISO-8601 UTC timestamp with seconds: YYYY-MM-DDTHH:MM:SSZ."""
    s = str(t or "").strip()
    if not s:
        return None
    if "T" in s:
        iso = s if s.endswith("Z") else (s + "Z")
    else:
        iso = f"{date_str}T{s}Z"
    dt = _parse_iso(iso)
    if dt is not None:
        return _format_iso_z(dt)

    # Fallback for common case: missing seconds (OddsAPI rejects T19:00Z with 422)
    if iso.endswith("Z"):
        base = iso[:-1]
        if base.count(":") == 1:
            return base + ":00Z"
    return iso


def _american_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if s.upper() == "EVEN":
            return "+100"
        return s
    try:
        v = int(float(x))
    except Exception:
        return None
    if v == 0:
        return None
    return f"{v:+d}" if v > 0 else str(v)


def _prop_lane_row(*, line: float, src: str = "oddsapi") -> Dict[str, Any]:
    return {
        "line": float(line),
        "over_odds": None,
        "under_odds": None,
        "_src": str(src),
    }


def _merge_prop_lane_row(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(dst or {})
    if out.get("line") is None and src.get("line") is not None:
        out["line"] = src.get("line")
    if out.get("over_odds") is None and src.get("over_odds") is not None:
        out["over_odds"] = src.get("over_odds")
    if out.get("under_odds") is None and src.get("under_odds") is not None:
        out["under_odds"] = src.get("under_odds")
    if not out.get("_src") and src.get("_src"):
        out["_src"] = src.get("_src")
    return out


def _line_matches(value: Any, target: float, tol: float = 1e-9) -> bool:
    try:
        return abs(float(value) - float(target)) <= float(tol)
    except Exception:
        return False


def _primary_lane_sort_key(row: Dict[str, Any]) -> Tuple[float, float, float, float]:
    from sim_engine.market_pitcher_props import american_implied_prob

    p_over = american_implied_prob(row.get("over_odds"))
    p_under = american_implied_prob(row.get("under_odds"))
    line_value = float(row.get("line") or 0.0)

    if p_over is not None and p_under is not None:
        return (
            0.0,
            abs(float(p_over) - float(p_under)),
            abs(float(p_over + p_under) - 1.0),
            abs(line_value),
        )

    implied = p_over if p_over is not None else p_under
    if implied is not None:
        return (
            1.0,
            abs(float(implied) - 0.5),
            0.0,
            abs(line_value),
        )

    return (2.0, 1e9, 1e9, abs(line_value))


def _select_primary_prop_lane(lanes: List[Dict[str, Any]], market_name: Optional[str]) -> Dict[str, Any]:
    preferred_lines = PLAYER_PROP_PRIMARY_LINE_PREFERENCES.get(str(market_name or ""), ())
    for preferred_line in preferred_lines:
        for require_two_way in (True, False):
            for lane in lanes:
                if not _line_matches(lane.get("line"), preferred_line):
                    continue
                if require_two_way and (lane.get("over_odds") is None or lane.get("under_odds") is None):
                    continue
                return lane
    return min(lanes, key=_primary_lane_sort_key) if lanes else {}


def _finalize_prop_market(row: Dict[str, Any], market_name: Optional[str] = None) -> Dict[str, Any]:
    lanes_map = (row or {}).get("_lanes") or {}
    lanes: List[Dict[str, Any]] = []
    for lane in lanes_map.values():
        if not isinstance(lane, dict):
            continue
        if lane.get("line") is None:
            continue
        lanes.append(
            {
                "line": float(lane.get("line")),
                "over_odds": lane.get("over_odds"),
                "under_odds": lane.get("under_odds"),
                "_src": str(lane.get("_src") or "oddsapi"),
            }
        )

    lanes.sort(key=lambda item: float(item.get("line") or 0.0))
    primary = _select_primary_prop_lane(lanes, market_name)
    primary_line = primary.get("line") if isinstance(primary, dict) else None

    alternates = [lane for lane in lanes if lane.get("line") != primary_line]
    return {
        "line": primary.get("line") if isinstance(primary, dict) else None,
        "over_odds": primary.get("over_odds") if isinstance(primary, dict) else None,
        "under_odds": primary.get("under_odds") if isinstance(primary, dict) else None,
        "_src": str((primary or {}).get("_src") or (row or {}).get("_src") or "oddsapi"),
        "lanes": lanes,
        "alternates": alternates,
    }


def _merge_prop_market_rows(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    merged_lanes: Dict[str, Dict[str, Any]] = {}
    for container in (dst or {}, src or {}):
        lanes_map = container.get("_lanes") if isinstance(container, dict) else None
        if isinstance(lanes_map, dict):
            for line_key, lane in lanes_map.items():
                if not isinstance(lane, dict):
                    continue
                merged_lanes[str(line_key)] = _merge_prop_lane_row(merged_lanes.get(str(line_key), {}), lane)
            continue

        line_value = container.get("line") if isinstance(container, dict) else None
        if line_value is None:
            continue
        line_key = f"{float(line_value):.3f}"
        merged_lanes[line_key] = _merge_prop_lane_row(
            merged_lanes.get(line_key, {}),
            {
                "line": float(line_value),
                "over_odds": container.get("over_odds") if isinstance(container, dict) else None,
                "under_odds": container.get("under_odds") if isinstance(container, dict) else None,
                "_src": container.get("_src") if isinstance(container, dict) else "oddsapi",
            },
        )

    return {"_src": str((dst or {}).get("_src") or (src or {}).get("_src") or "oddsapi"), "_lanes": merged_lanes}


def _finalize_prop_market_map(markets: Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for name, market_rows in (markets or {}).items():
        if not isinstance(market_rows, dict):
            continue
        out[name] = {}
        for market_name, row in market_rows.items():
            if not isinstance(row, dict):
                continue
            out[name][market_name] = _finalize_prop_market(row, market_name)
    return out


def _as_market_list(markets: Any) -> List[Dict[str, Any]]:
    """Normalize OddsAPI `markets` container to a list of dicts.

    Some endpoints return `markets` as a list, others as a dict keyed by market.
    """
    if markets is None:
        return []
    if isinstance(markets, list):
        return [m for m in markets if isinstance(m, dict)]
    if isinstance(markets, dict):
        return [m for m in markets.values() if isinstance(m, dict)]
    return []


def _extract_player_props(
    markets: Any,
    *,
    key_map: Dict[str, str],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Extract player props (any markets in key_map) into name -> market -> line/odds.

    Output schema:
      player_props[name_lower][internal_market] = {line, over_odds, under_odds, _src}
    """
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for m in _as_market_list(markets):
        key = (m.get("key") or "").lower().strip()
        if key not in key_map:
            continue
        internal = str(key_map[key])
        outcomes = m.get("outcomes")
        if not isinstance(outcomes, list):
            continue
        for oc in outcomes:
            if not isinstance(oc, dict):
                continue
            # OddsAPI uses description/participant for player, name is Over/Under
            name = (oc.get("description") or oc.get("participant") or "").strip()
            if not name:
                continue
            line = oc.get("point")
            if line is None:
                continue
            side = (oc.get("name") or "").strip().lower()  # over/under
            price = oc.get("price")
            nk = name.lower().strip()
            row = out.setdefault(nk, {}).setdefault(
                internal,
                {"_src": "oddsapi", "_lanes": {}},
            )
            try:
                line_value = float(line)
            except Exception:
                continue
            line_key = f"{line_value:.3f}"
            lane = row.setdefault("_lanes", {}).setdefault(line_key, _prop_lane_row(line=line_value))
            if side.startswith("over") and lane.get("over_odds") is None:
                lane["over_odds"] = _american_str(price)
            elif side.startswith("under") and lane.get("under_odds") is None:
                lane["under_odds"] = _american_str(price)
    finalized: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for name, markets_map in out.items():
        finalized[name] = {}
        for market_name, row in markets_map.items():
            finalized[name][market_name] = _finalize_prop_market(row, market_name)
    return finalized


def _extract_game_lines(
    markets: List[Dict[str, Any]],
    *,
    home_team: str,
    away_team: str,
) -> Dict[str, Any]:
    """Extract core game markets (h2h/spreads/totals) from bookmaker markets."""
    segment_market_map = {
        "h2h": ("full", "h2h"),
        "spreads": ("full", "spreads"),
        "totals": ("full", "totals"),
        "h2h_1st_1_innings": ("first1", "h2h"),
        "spreads_1st_1_innings": ("first1", "spreads"),
        "totals_1st_1_innings": ("first1", "totals"),
        "h2h_1st_3_innings": ("first3", "h2h"),
        "spreads_1st_3_innings": ("first3", "spreads"),
        "totals_1st_3_innings": ("first3", "totals"),
        "h2h_1st_5_innings": ("first5", "h2h"),
        "spreads_1st_5_innings": ("first5", "spreads"),
        "totals_1st_5_innings": ("first5", "totals"),
        "h2h_1st_7_innings": ("first7", "h2h"),
        "spreads_1st_7_innings": ("first7", "spreads"),
        "totals_1st_7_innings": ("first7", "totals"),
    }
    out: Dict[str, Any] = {
        "h2h": None,
        "spreads": None,
        "totals": None,
        "segments": {
            "full": {"h2h": None, "spreads": None, "totals": None},
            "first1": {"h2h": None, "spreads": None, "totals": None},
            "first3": {"h2h": None, "spreads": None, "totals": None},
            "first5": {"h2h": None, "spreads": None, "totals": None},
            "first7": {"h2h": None, "spreads": None, "totals": None},
        },
    }

    home = str(home_team or "").strip().lower()
    away = str(away_team or "").strip().lower()

    for m in _as_market_list(markets):
        key = (m.get("key") or "").lower().strip()
        segment_spec = segment_market_map.get(key)
        if segment_spec is None:
            continue
        segment_key, market_key = segment_spec
        outcomes = m.get("outcomes") or []
        if market_key == "h2h":
            row = {"home_odds": None, "away_odds": None}
            for oc in outcomes:
                nm = str(oc.get("name") or "").strip().lower()
                price = oc.get("price")
                if not nm:
                    continue
                if nm == home and row["home_odds"] is None:
                    row["home_odds"] = _american_str(price)
                elif nm == away and row["away_odds"] is None:
                    row["away_odds"] = _american_str(price)
            if row["home_odds"] is not None or row["away_odds"] is not None:
                out["segments"][segment_key]["h2h"] = row
                if segment_key == "full":
                    out["h2h"] = row

        elif market_key == "totals":
            row = {"line": None, "over_odds": None, "under_odds": None}
            for oc in outcomes:
                side = str(oc.get("name") or "").strip().lower()  # Over/Under
                price = oc.get("price")
                point = oc.get("point")
                if row["line"] is None and point is not None:
                    try:
                        row["line"] = float(point)
                    except Exception:
                        row["line"] = None
                if side.startswith("over") and row["over_odds"] is None:
                    row["over_odds"] = _american_str(price)
                elif side.startswith("under") and row["under_odds"] is None:
                    row["under_odds"] = _american_str(price)
            if row["line"] is not None:
                out["segments"][segment_key]["totals"] = row
                if segment_key == "full":
                    out["totals"] = row

        elif market_key == "spreads":
            row = {
                "home_line": None,
                "home_odds": None,
                "away_line": None,
                "away_odds": None,
            }
            for oc in outcomes:
                nm = str(oc.get("name") or "").strip().lower()
                price = oc.get("price")
                point = oc.get("point")
                if not nm:
                    continue
                if nm == home:
                    if row["home_line"] is None and point is not None:
                        try:
                            row["home_line"] = float(point)
                        except Exception:
                            row["home_line"] = None
                    if row["home_odds"] is None:
                        row["home_odds"] = _american_str(price)
                elif nm == away:
                    if row["away_line"] is None and point is not None:
                        try:
                            row["away_line"] = float(point)
                        except Exception:
                            row["away_line"] = None
                    if row["away_odds"] is None:
                        row["away_odds"] = _american_str(price)
            if row["home_line"] is not None or row["away_line"] is not None:
                out["segments"][segment_key]["spreads"] = row
                if segment_key == "full":
                    out["spreads"] = row

    if not any(
        any(bucket.get(market) is not None for market in ("h2h", "spreads", "totals"))
        for key, bucket in (out.get("segments") or {}).items()
        if key != "full" and isinstance(bucket, dict)
    ):
        out.pop("segments", None)

    return out


def fetch_historical_pitcher_props_for_date(
    api_key: str,
    date_str: str,
    *,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    hist_snapshots: Optional[List[str]] = None,
    probe_markets: bool = False,
) -> Dict[str, Any]:
    """Fetch completed+same-day pitcher props using historical endpoints.

    Uses:
      - /historical/sports/{sport}/events?date=end_of_day
      - /historical/sports/{sport}/events/{eventId}/odds?date=snapshot

    This avoids relying on the live /events set.
    """
    end_of_day_iso = f"{date_str}T23:59:59Z"

    # snapshots
    snapshots: List[str] = []
    if hist_snapshots:
        for t in hist_snapshots:
            iso_ts = _normalize_snapshot_iso(date_str, str(t))
            if iso_ts:
                snapshots.append(iso_ts)
    if not snapshots:
        snapshots = [end_of_day_iso]

    ev_url = f"{API_BASE}/historical/sports/{SPORT}/events"
    events_raw, _ = _http_get(ev_url, {"apiKey": api_key, "date": end_of_day_iso})
    events = _as_events_list(events_raw)

    desired_markets = ["pitcher_strikeouts", "pitcher_outs"]
    markets_csv = ",".join(desired_markets)

    pitcher_props: Dict[str, Dict[str, Dict[str, Any]]] = {}

    probed = 0
    with_markets = 0

    for ev in events or []:
        if not _event_matches_slate_date(ev, date_str):
            continue
        ev_id = ev.get("id")
        if not ev_id:
            continue

        if probe_markets:
            probed += 1
            try:
                mk_url = f"{API_BASE}/sports/{SPORT}/events/{ev_id}/markets"
                mks, _ = _http_get(mk_url, {"apiKey": api_key})
                keys = set((m.get("key") or "").lower() for m in (mks or []))
                if not any(k in keys for k in desired_markets):
                    continue
                with_markets += 1
            except Exception:
                # permissive
                with_markets += 1

        odds_url = f"{API_BASE}/historical/sports/{SPORT}/events/{ev_id}/odds"
        for snap in snapshots:
            params = {
                "apiKey": api_key,
                "regions": regions,
                "oddsFormat": "american",
                "markets": markets_csv,
                "date": snap,
            }
            if bookmakers:
                params["bookmakers"] = bookmakers
            try:
                odds_raw, _ = _http_get(odds_url, params)
            except requests.HTTPError:
                continue
            odds = _unwrap_odds_payload(odds_raw)
            if not isinstance(odds, dict):
                continue
            for bk in (odds.get("bookmakers") or []):
                pp = _extract_player_props(
                    bk.get("markets"),
                    key_map=PITCHER_MARKET_KEY_MAP,
                )
                for name, mk in pp.items():
                    dst = pitcher_props.setdefault(name, {})
                    for k, v in mk.items():
                        dst[k] = _merge_prop_market_rows(dst.get(k, {}), v)

    return {
        "date": date_str,
        "retrieved_at": datetime.utcnow().isoformat(),
        "pitcher_props": _finalize_prop_market_map(pitcher_props),
        "meta": {
            "markets": desired_markets,
            "regions": regions,
            "bookmakers": bookmakers.split(",") if bookmakers else None,
            "hist_snapshots": snapshots,
            "probe_markets": bool(probe_markets),
            "probe_stats": {"probed_events": probed, "events_with_markets": with_markets} if probe_markets else None,
        },
    }


def fetch_historical_hitter_props_for_date(
    api_key: str,
    date_str: str,
    *,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    hist_snapshots: Optional[List[str]] = None,
    markets: Optional[List[str]] = None,
    probe_markets: bool = False,
) -> Dict[str, Any]:
    end_of_day_iso = f"{date_str}T23:59:59Z"

    desired_markets = [m.strip() for m in (markets or []) if str(m).strip()]
    if not desired_markets:
        desired_markets = list(DEFAULT_HITTER_MARKETS)
    markets_csv = ",".join(desired_markets)

    snapshots: List[str] = []
    if hist_snapshots:
        for t in hist_snapshots:
            iso_ts = _normalize_snapshot_iso(date_str, str(t))
            if iso_ts:
                snapshots.append(iso_ts)
    if not snapshots:
        snapshots = [end_of_day_iso]

    ev_url = f"{API_BASE}/historical/sports/{SPORT}/events"
    events_raw, _ = _http_get(ev_url, {"apiKey": api_key, "date": end_of_day_iso})
    events = _as_events_list(events_raw)

    hitter_props: Dict[str, Dict[str, Dict[str, Any]]] = {}

    probed = 0
    with_markets = 0

    for ev in events or []:
        if not _event_matches_slate_date(ev, date_str):
            continue
        ev_id = ev.get("id")
        if not ev_id:
            continue

        if probe_markets:
            probed += 1
            try:
                mk_url = f"{API_BASE}/sports/{SPORT}/events/{ev_id}/markets"
                mks, _ = _http_get(mk_url, {"apiKey": api_key})
                keys = set((m.get("key") or "").lower() for m in (mks or []))
                if not any(k in keys for k in desired_markets):
                    continue
                with_markets += 1
            except Exception:
                with_markets += 1

        odds_url = f"{API_BASE}/historical/sports/{SPORT}/events/{ev_id}/odds"
        for snap in snapshots:
            params = {
                "apiKey": api_key,
                "regions": regions,
                "oddsFormat": "american",
                "markets": markets_csv,
                "date": snap,
            }
            if bookmakers:
                params["bookmakers"] = bookmakers
            try:
                odds_raw, _ = _http_get(odds_url, params)
            except requests.HTTPError:
                continue
            odds = _unwrap_odds_payload(odds_raw)
            if not isinstance(odds, dict):
                continue
            for bk in (odds.get("bookmakers") or []):
                pp = _extract_player_props(
                    bk.get("markets"),
                    key_map={m.lower(): m.lower() for m in desired_markets},
                )
                for name, mk in pp.items():
                    dst = hitter_props.setdefault(name, {})
                    for k, v in mk.items():
                        dst[k] = _merge_prop_market_rows(dst.get(k, {}), v)

    return {
        "date": date_str,
        "retrieved_at": datetime.utcnow().isoformat(),
        "hitter_props": _finalize_prop_market_map(hitter_props),
        "meta": {
            "markets": desired_markets,
            "regions": regions,
            "bookmakers": bookmakers.split(",") if bookmakers else None,
            "hist_snapshots": snapshots,
            "probe_markets": bool(probe_markets),
            "probe_stats": {"probed_events": probed, "events_with_markets": with_markets} if probe_markets else None,
        },
    }


def fetch_historical_all_for_date(
    api_key: str,
    date_str: str,
    *,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    hist_snapshots: Optional[List[str]] = None,
    hitter_markets: Optional[List[str]] = None,
    snapshot_offset_minutes: int = 60,
    probe_markets: bool = False,
) -> Dict[str, Any]:
    """Fetch game lines + pitcher props + hitter props together.

    This is the efficient path for full-season backfills: one historical odds request per event per snapshot.
    """
    end_of_day_iso = f"{date_str}T23:59:59Z"

    desired_hitter = [m.strip() for m in (hitter_markets or []) if str(m).strip()]
    if not desired_hitter:
        desired_hitter = list(DEFAULT_HITTER_MARKETS)

    hitter_key_map = {m.lower(): m.lower() for m in desired_hitter}

    base_snapshots: List[str] = []
    if hist_snapshots:
        for t in hist_snapshots:
            iso_ts = _normalize_snapshot_iso(date_str, str(t))
            if iso_ts:
                base_snapshots.append(iso_ts)
    if not base_snapshots:
        base_snapshots = [end_of_day_iso]

    # Fetch event list once
    ev_url = f"{API_BASE}/historical/sports/{SPORT}/events"
    events_raw, _ = _http_get(ev_url, {"apiKey": api_key, "date": end_of_day_iso})
    events = _as_events_list(events_raw)

    desired_markets: List[str] = ["h2h", "spreads", "totals"] + list(PITCHER_MARKET_KEY_MAP.keys()) + [m.lower() for m in desired_hitter]
    markets_csv = ",".join(desired_markets)

    pitcher_props: Dict[str, Dict[str, Dict[str, Any]]] = {}
    hitter_props: Dict[str, Dict[str, Dict[str, Any]]] = {}
    games: List[Dict[str, Any]] = []

    probed = 0
    with_markets = 0

    for ev in events or []:
        if not _event_matches_slate_date(ev, date_str):
            continue
        ev_id = ev.get("id")
        if not ev_id:
            continue

        home_team = ev.get("home_team")
        away_team = ev.get("away_team")
        commence_time = ev.get("commence_time")
        commence_dt = _parse_iso(str(commence_time) if commence_time else "")

        snapshots: List[str] = list(base_snapshots)
        if commence_dt is not None:
            snap_dt = commence_dt - timedelta(minutes=int(snapshot_offset_minutes))
            snapshots = [_format_iso_z(snap_dt)] + snapshots

        if probe_markets:
            probed += 1
            try:
                mk_url = f"{API_BASE}/sports/{SPORT}/events/{ev_id}/markets"
                mks, _ = _http_get(mk_url, {"apiKey": api_key})
                keys = set((m.get("key") or "").lower() for m in (mks or []))
                if not any(k in keys for k in desired_markets):
                    continue
                with_markets += 1
            except Exception:
                with_markets += 1

        odds_url = f"{API_BASE}/historical/sports/{SPORT}/events/{ev_id}/odds"

        chosen_payload: Optional[Dict[str, Any]] = None
        chosen_snap: Optional[str] = None
        chosen_bookmaker: Optional[str] = None
        chosen_game_lines: Optional[Dict[str, Any]] = None

        # Merge player props across all snapshots in order, but keep the first usable game-line snapshot.
        for snap in snapshots:
            params = {
                "apiKey": api_key,
                "regions": regions,
                "oddsFormat": "american",
                "markets": markets_csv,
                "date": snap,
            }
            if bookmakers:
                params["bookmakers"] = bookmakers
            try:
                odds_raw, _ = _http_get(odds_url, params)
            except requests.HTTPError:
                continue

            payload = _unwrap_odds_payload(odds_raw)
            if not isinstance(payload, dict):
                continue
            bks = payload.get("bookmakers") or []
            if not isinstance(bks, list) or not bks:
                continue

            if chosen_payload is None:
                chosen_payload = payload

            # Merge props from all books (first-seen wins), but choose a single book for game lines.
            for bk in bks:
                if not isinstance(bk, dict):
                    continue
                mk = bk.get("markets")

                # Player props
                pp_pitch = _extract_player_props(mk, key_map=PITCHER_MARKET_KEY_MAP)
                for name, mk2 in pp_pitch.items():
                    dst = pitcher_props.setdefault(name, {})
                    for k, v in mk2.items():
                        dst[k] = _merge_prop_market_rows(dst.get(k, {}), v)

                pp_hit = _extract_player_props(mk, key_map=hitter_key_map)
                for name, mk2 in pp_hit.items():
                    dst = hitter_props.setdefault(name, {})
                    for k, v in mk2.items():
                        dst[k] = _merge_prop_market_rows(dst.get(k, {}), v)

                # Game lines (pick first book that has something)
                if chosen_game_lines is None:
                    gl = _extract_game_lines(mk, home_team=str(home_team or ""), away_team=str(away_team or ""))
                    if gl.get("h2h") or gl.get("totals") or gl.get("spreads"):
                        chosen_game_lines = gl
                        chosen_bookmaker = str(bk.get("key") or bk.get("title") or "")
                        chosen_snap = snap

        if chosen_payload and chosen_game_lines:
            games.append(
                {
                    "event_id": ev_id,
                    "commence_time": commence_time,
                    "home_team": home_team,
                    "away_team": away_team,
                    "snapshot": chosen_snap,
                    "bookmaker": chosen_bookmaker,
                    "markets": chosen_game_lines,
                }
            )

    return {
        "date": date_str,
        "retrieved_at": datetime.utcnow().isoformat(),
        "pitcher_props": _finalize_prop_market_map(pitcher_props),
        "hitter_props": _finalize_prop_market_map(hitter_props),
        "games": games,
        "meta": {
            "markets": desired_markets,
            "regions": regions,
            "bookmakers": bookmakers.split(",") if bookmakers else None,
            "hist_snapshots": base_snapshots,
            "snapshot_offset_minutes": int(snapshot_offset_minutes),
            "probe_markets": bool(probe_markets),
            "probe_stats": {"probed_events": probed, "events_with_markets": with_markets} if probe_markets else None,
        },
    }


def fetch_historical_game_lines_for_date(
    api_key: str,
    date_str: str,
    *,
    regions: str = "us",
    bookmakers: Optional[str] = None,
    hist_snapshots: Optional[List[str]] = None,
    snapshot_offset_minutes: int = 60,
    probe_markets: bool = False,
) -> Dict[str, Any]:
    """Fetch historical core game markets for all events on a date.

    Uses historical events list and then per-event historical odds so we can align snapshots
    near (commence_time - offset).
    """
    end_of_day_iso = f"{date_str}T23:59:59Z"

    base_snapshots: List[str] = []
    if hist_snapshots:
        for t in hist_snapshots:
            iso_ts = _normalize_snapshot_iso(date_str, str(t))
            if iso_ts:
                base_snapshots.append(iso_ts)
    if not base_snapshots:
        base_snapshots = [end_of_day_iso]

    ev_url = f"{API_BASE}/historical/sports/{SPORT}/events"
    events_raw, _ = _http_get(ev_url, {"apiKey": api_key, "date": end_of_day_iso})
    events = _as_events_list(events_raw)

    desired_markets = ["h2h", "spreads", "totals"]
    markets_csv = ",".join(desired_markets)

    games: List[Dict[str, Any]] = []

    probed = 0
    with_markets = 0

    for ev in events or []:
        if not _event_matches_slate_date(ev, date_str):
            continue
        ev_id = ev.get("id")
        if not ev_id:
            continue

        home_team = ev.get("home_team")
        away_team = ev.get("away_team")
        commence_time = ev.get("commence_time")
        commence_dt = _parse_iso(str(commence_time) if commence_time else "")

        # Prefer a snapshot near start time if we have commence_time.
        snapshots: List[str] = list(base_snapshots)
        if commence_dt is not None:
            snap_dt = commence_dt - timedelta(minutes=int(snapshot_offset_minutes))
            snapshots = [_format_iso_z(snap_dt)] + snapshots

        if probe_markets:
            probed += 1
            try:
                mk_url = f"{API_BASE}/sports/{SPORT}/events/{ev_id}/markets"
                mks, _ = _http_get(mk_url, {"apiKey": api_key})
                keys = set((m.get("key") or "").lower() for m in (mks or []))
                if not any(k in keys for k in desired_markets):
                    continue
                with_markets += 1
            except Exception:
                with_markets += 1

        odds_url = f"{API_BASE}/historical/sports/{SPORT}/events/{ev_id}/odds"
        chosen: Optional[Dict[str, Any]] = None
        chosen_snap: Optional[str] = None
        best: Optional[Dict[str, Any]] = None
        best_bk: Optional[str] = None

        for snap in snapshots:
            params = {
                "apiKey": api_key,
                "regions": regions,
                "oddsFormat": "american",
                "markets": markets_csv,
                "date": snap,
            }
            if bookmakers:
                params["bookmakers"] = bookmakers
            try:
                odds_raw, _ = _http_get(odds_url, params)
            except requests.HTTPError:
                continue
            odds = _unwrap_odds_payload(odds_raw)
            if not isinstance(odds, dict):
                continue
            for bk in (odds.get("bookmakers") or []):
                lines = _extract_game_lines(
                    bk.get("markets"),
                    home_team=str(home_team or ""),
                    away_team=str(away_team or ""),
                )
                if lines.get("h2h") or lines.get("totals") or lines.get("spreads"):
                    chosen = odds
                    chosen_snap = snap
                    best = lines
                    best_bk = str(bk.get("key") or bk.get("title") or "")
                    break
            if best:
                break

        if not best:
            continue

        games.append(
            {
                "event_id": ev_id,
                "commence_time": commence_time,
                "home_team": home_team,
                "away_team": away_team,
                "snapshot": chosen_snap,
                "bookmaker": best_bk,
                "markets": best,
            }
        )

    return {
        "date": date_str,
        "retrieved_at": datetime.utcnow().isoformat(),
        "games": games,
        "meta": {
            "markets": desired_markets,
            "regions": regions,
            "bookmakers": bookmakers.split(",") if bookmakers else None,
            "hist_snapshots": base_snapshots,
            "snapshot_offset_minutes": int(snapshot_offset_minutes),
            "probe_markets": bool(probe_markets),
            "probe_stats": {"probed_events": probed, "events_with_markets": with_markets} if probe_markets else None,
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill 2025 OddsAPI historical lines into MLB-BettingV2 (pitcher props, hitter props, game lines)")
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--kind",
        choices=["pitcher_props", "hitter_props", "game_lines", "all"],
        default="pitcher_props",
        help="Which markets to fetch and persist",
    )
    ap.add_argument("--regions", default="us")
    ap.add_argument("--bookmakers", default=None)
    ap.add_argument("--hist-snapshots", default="15:00,19:00,23:59:59", help="Comma-separated UTC times (HH:MM[:SS])")
    ap.add_argument(
        "--hitter-markets",
        default=None,
        help="Comma-separated OddsAPI hitter prop market keys (default: small built-in set)",
    )
    ap.add_argument(
        "--snapshot-offset-minutes",
        type=int,
        default=60,
        help="For game lines, also try snapshot at (commence_time - offset)",
    )
    ap.add_argument("--probe-markets", action="store_true")
    ap.add_argument("--sleep-ms", type=int, default=250)
    ap.add_argument("--overwrite", choices=["on", "off"], default="off")
    args = ap.parse_args()

    load_dotenv_if_present(_ROOT / ".env")
    api_key = os.environ.get("ODDS_API_KEY") or os.environ.get("ODDSAPI_KEY")
    if not api_key:
        print("ERROR: ODDS_API_KEY not set (use MLB-BettingV2/.env or environment)")
        return 2

    start = datetime.fromisoformat(args.start_date).date()
    end = datetime.fromisoformat(args.end_date).date()
    snaps = [s.strip() for s in str(args.hist_snapshots).split(",") if s.strip()]
    hitter_markets = None
    if args.hitter_markets:
        hitter_markets = [s.strip() for s in str(args.hitter_markets).split(",") if s.strip()]

    out_dir = _ROOT / "data" / "market" / "oddsapi"
    _ensure_dir(out_dir)

    d = start
    n_ok = 0
    n_skip = 0
    n_err = 0

    while d <= end:
        date_str = d.isoformat()
        token = date_str.replace("-", "_")
        try:
            kinds = [str(args.kind)]
            if str(args.kind) == "all":
                # Efficient combined fetch path; write three files from one pull.
                out_pitch = out_dir / f"oddsapi_pitcher_props_{token}.json"
                out_hit = out_dir / f"oddsapi_hitter_props_{token}.json"
                out_games = out_dir / f"oddsapi_game_lines_{token}.json"

                if (
                    str(args.overwrite) == "off"
                    and out_pitch.exists()
                    and out_hit.exists()
                    and out_games.exists()
                ):
                    n_skip += 1
                    d = d + timedelta(days=1)
                    continue

                doc = fetch_historical_all_for_date(
                    api_key,
                    date_str,
                    regions=str(args.regions),
                    bookmakers=str(args.bookmakers) if args.bookmakers else None,
                    hist_snapshots=snaps,
                    hitter_markets=hitter_markets,
                    snapshot_offset_minutes=int(args.snapshot_offset_minutes),
                    probe_markets=bool(args.probe_markets),
                )
                _write_json(out_pitch, {"date": doc["date"], "retrieved_at": doc["retrieved_at"], "pitcher_props": doc.get("pitcher_props") or {}, "meta": doc.get("meta")})
                _write_json(out_hit, {"date": doc["date"], "retrieved_at": doc["retrieved_at"], "hitter_props": doc.get("hitter_props") or {}, "meta": doc.get("meta")})
                _write_json(out_games, {"date": doc["date"], "retrieved_at": doc["retrieved_at"], "games": doc.get("games") or [], "meta": doc.get("meta")})

                print(
                    f"OK {date_str} all pitcher={len(doc.get('pitcher_props') or {})} "
                    f"hitter={len(doc.get('hitter_props') or {})} games={len(doc.get('games') or [])}"
                )
                n_ok += 1
                wrote_any = True
                time.sleep(max(0.0, float(args.sleep_ms)) / 1000.0)
                d = d + timedelta(days=1)
                continue

            wrote_any = False
            for k in kinds:
                if k == "pitcher_props":
                    out_path = out_dir / f"oddsapi_pitcher_props_{token}.json"
                    if out_path.exists() and str(args.overwrite) == "off":
                        n_skip += 1
                        continue
                    doc = fetch_historical_pitcher_props_for_date(
                        api_key,
                        date_str,
                        regions=str(args.regions),
                        bookmakers=str(args.bookmakers) if args.bookmakers else None,
                        hist_snapshots=snaps,
                        probe_markets=bool(args.probe_markets),
                    )
                    _write_json(out_path, doc)
                    wrote_any = True
                    print(f"OK {date_str} pitcher_props pitchers={len(doc.get('pitcher_props') or {})} -> {out_path}")

                elif k == "hitter_props":
                    out_path = out_dir / f"oddsapi_hitter_props_{token}.json"
                    if out_path.exists() and str(args.overwrite) == "off":
                        n_skip += 1
                        continue
                    doc = fetch_historical_hitter_props_for_date(
                        api_key,
                        date_str,
                        regions=str(args.regions),
                        bookmakers=str(args.bookmakers) if args.bookmakers else None,
                        hist_snapshots=snaps,
                        markets=hitter_markets,
                        probe_markets=bool(args.probe_markets),
                    )
                    _write_json(out_path, doc)
                    wrote_any = True
                    print(f"OK {date_str} hitter_props players={len(doc.get('hitter_props') or {})} -> {out_path}")

                elif k == "game_lines":
                    out_path = out_dir / f"oddsapi_game_lines_{token}.json"
                    if out_path.exists() and str(args.overwrite) == "off":
                        n_skip += 1
                        continue
                    doc = fetch_historical_game_lines_for_date(
                        api_key,
                        date_str,
                        regions=str(args.regions),
                        bookmakers=str(args.bookmakers) if args.bookmakers else None,
                        hist_snapshots=snaps,
                        snapshot_offset_minutes=int(args.snapshot_offset_minutes),
                        probe_markets=bool(args.probe_markets),
                    )
                    _write_json(out_path, doc)
                    wrote_any = True
                    print(f"OK {date_str} game_lines games={len(doc.get('games') or [])} -> {out_path}")

            if wrote_any:
                n_ok += 1
        except Exception as e:
            n_err += 1
            print(f"ERR {date_str}: {e}")
            print(traceback.format_exc())
        time.sleep(max(0.0, float(args.sleep_ms)) / 1000.0)
        d = d + timedelta(days=1)

    print(json.dumps({"ok": True, "written": n_ok, "skipped": n_skip, "errors": n_err}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
