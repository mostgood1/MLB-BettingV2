from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import gzip
import html
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from .disk_cache import DiskCache
from ..models import PitchType, ParkFactors, UmpireFactors, WeatherFactors


@dataclass
class StatsApiClient:
    base_url: str = "https://statsapi.mlb.com/api/v1"
    timeout_sec: float = 8.0
    max_retries: int = 2
    retry_backoff_sec: float = 0.6
    cache: DiskCache | None = None
    cache_ttl_seconds: int = 6 * 3600
    # If True, requests will respect HTTP(S)_PROXY env vars. Default False to avoid
    # accidental routing through local/corporate proxies (e.g., localhost ports).
    trust_env: bool = False

    @staticmethod
    def with_default_cache(cache_dir: str | None = None, ttl_seconds: int = 6 * 3600) -> "StatsApiClient":
        # statsapi.py lives at MLB-BettingV2/sim_engine/data/statsapi.py
        # parents[2] => MLB-BettingV2/
        root = Path(cache_dir) if cache_dir else (Path(__file__).resolve().parents[2] / "data" / "cache" / "statsapi")
        return StatsApiClient(cache=DiskCache(root_dir=root, default_ttl_seconds=ttl_seconds), cache_ttl_seconds=ttl_seconds)

    def _effective_trust_env(self) -> bool:
        env = os.environ.get("STATSAPI_TRUST_ENV")
        if env is None:
            return bool(self.trust_env)
        return str(env).strip().lower() in ("1", "true", "yes", "y", "on")

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = path if str(path).lower().startswith("http") else f"{self.base_url}{path}"
        p = params or {}
        if self.cache is not None:
            hit = self.cache.get("get", {"url": url, "params": p}, ttl_seconds=self.cache_ttl_seconds)
            if isinstance(hit, dict) and hit:
                return hit

        # Use a Session so we can reliably control proxy/env behavior.
        s = requests.Session()
        s.trust_env = self._effective_trust_env()

        max_attempts = max(1, 1 + int(self.max_retries or 0))
        last_exc: Exception | None = None
        for attempt in range(max_attempts):
            try:
                # Use an explicit (connect, read) tuple for more predictable behavior.
                r = s.get(url, params=p, timeout=(self.timeout_sec, self.timeout_sec))
                r.raise_for_status()
                data = r.json()
                if self.cache is not None and isinstance(data, dict):
                    self.cache.set("get", {"url": url, "params": p}, data)
                return data
            except requests.exceptions.RequestException as e:
                last_exc = e
                # Retry transient network-ish failures and server errors.
                status = getattr(getattr(e, "response", None), "status_code", None)
                retryable_status = isinstance(status, int) and 500 <= int(status) < 600
                retryable = isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)) or retryable_status
                if (attempt + 1) >= max_attempts or not retryable:
                    break
                base = float(self.retry_backoff_sec or 0.0)
                # small jitter to avoid thundering herd
                delay = base * (2.0**attempt) * (1.0 + 0.25 * random.random())
                time.sleep(delay)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("StatsApiClient.get failed without exception")


def fetch_game_weather(client: StatsApiClient, game_pk: int) -> WeatherFactors:
    """Fetch best-effort weather context for a given gamePk.

    Uses the MLB StatsAPI live feed (v1.1). For completed games this is typically
    present; for future games it may be missing.
    """
    weather, _, _ = fetch_game_context(client, game_pk)
    return weather


def fetch_game_context(client: StatsApiClient, game_pk: int) -> tuple[WeatherFactors, ParkFactors, UmpireFactors]:
    """Fetch (weather, park, umpire) from the live feed for a given gamePk."""
    try:
        game_pk_i = int(game_pk)
    except Exception:
        return WeatherFactors(source="statsapi_live_feed"), ParkFactors(source="statsapi_live_feed"), UmpireFactors(source="statsapi_live_feed")

    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk_i}/feed/live"
    data = client.get(url)
    game_data = (data.get("gameData") or {})
    live_data = (data.get("liveData") or {})

    weather = _parse_weather_from_game_data(game_data)
    park = _parse_park_from_game_data(game_data)
    ump = _parse_umpire_from_feed(game_data, live_data)
    return weather, park, ump


def fetch_game_feed_live(client: StatsApiClient, game_pk: int) -> Dict[str, Any]:
    """Fetch the StatsAPI v1.1 live feed payload for a given gamePk."""
    try:
        game_pk_i = int(game_pk)
    except Exception:
        return {}
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk_i}/feed/live"
    try:
        data = client.get(url)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_feed_live_from_raw(season: int, date_str: str, game_pk: int, raw_root: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Load a previously saved feed/live payload from disk.

    Expected path:
      MLB-BettingV2/data/raw/statsapi/feed_live/<season>/<YYYY-MM-DD>/<gamePk>.json.gz
    """
    try:
        game_pk_i = int(game_pk)
    except Exception:
        return None

    try:
        root = raw_root
        if root is None:
            data_root_env = str(os.environ.get("MLB_BETTING_DATA_ROOT") or "").strip()
            if data_root_env:
                root = Path(data_root_env).resolve() / "raw" / "statsapi" / "feed_live"
            else:
                root = Path(__file__).resolve().parents[2] / "data" / "raw" / "statsapi" / "feed_live"
        p = Path(root) / str(int(season)) / str(date_str) / f"{game_pk_i}.json.gz"
        if not p.exists():
            return None
        with gzip.open(p, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def parse_confirmed_lineup_ids(feed_live: Dict[str, Any], side: str) -> List[int]:
    """Best-effort parse of a confirmed batting order (first 9) from the live feed.

    side: "home" or "away"
    Returns: list of MLBAM player IDs in batting order. Empty if unknown/unavailable.
    """
    side = str(side or "").strip().lower()
    if side not in ("home", "away"):
        return []

    live_data = (feed_live.get("liveData") or {})
    box = (live_data.get("boxscore") or {})
    teams = (box.get("teams") or {})
    t = (teams.get(side) or {})

    def _uniq(ids: List[int]) -> List[int]:
        seen = set()
        out: List[int] = []
        for pid in ids:
            if pid in seen:
                continue
            if pid <= 0:
                continue
            seen.add(pid)
            out.append(pid)
        return out

    ids: List[int] = []

    # Preferred: explicit battingOrder list.
    order = t.get("battingOrder")
    if isinstance(order, list):
        for x in order:
            try:
                ids.append(int(x))
            except Exception:
                continue
        ids = _uniq(ids)
        if len(ids) >= 9:
            return ids[:9]

    # Fallback: players dict with numeric battingOrder fields.
    players = (t.get("players") or {})
    if not isinstance(players, dict) or not players:
        return []

    entries: List[tuple[int, int]] = []  # (batting_order, pid)
    for k, v in players.items():
        if not isinstance(v, dict):
            continue
        bo = v.get("battingOrder")
        if bo is None:
            continue
        try:
            bo_i = int(str(bo).strip())
        except Exception:
            continue
        pid = None
        try:
            ks = str(k)
            if ks.startswith("ID"):
                pid = int(ks[2:])
        except Exception:
            pid = None
        if pid is None:
            try:
                pid = int(((v.get("person") or {}).get("id")) or 0)
            except Exception:
                pid = 0
        if not pid or pid <= 0:
            continue
        entries.append((bo_i, int(pid)))

    if not entries:
        return []

    # Prefer the canonical starter slots (100, 200, ... 900) if present.
    starters = sorted([e for e in entries if e[0] % 100 == 0], key=lambda x: x[0])
    others = sorted([e for e in entries if e[0] % 100 != 0], key=lambda x: x[0])
    merged = starters + others
    ids2 = _uniq([pid for _, pid in merged])
    return ids2[:9]


def fetch_confirmed_lineup_ids(client: StatsApiClient, game_pk: int, side: str) -> List[int]:
    feed = fetch_game_feed_live(client, game_pk)
    return parse_confirmed_lineup_ids(feed, side)


def _parse_starting_lineups_block_lineup_ids(block_html: str, side: str) -> List[int]:
    side_token = str(side or "").strip().lower()
    if side_token not in ("away", "home"):
        return []
    pattern = rf'<ol class="starting-lineups__team[^\"]*starting-lineups__team--{side_token}[^\"]*">(.*?)</ol>'
    seen_variants: set[tuple[int, ...]] = set()
    for match in re.finditer(pattern, str(block_html or ""), flags=re.IGNORECASE | re.DOTALL):
        section_html = str(match.group(1) or "")
        if "starting-lineups__player--TBD" in section_html:
            continue
        ids: List[int] = []
        seen_ids: set[int] = set()
        for href_match in re.finditer(r'href="/player/[^\"]*-(\d+)"', section_html, flags=re.IGNORECASE):
            try:
                pid = int(href_match.group(1))
            except Exception:
                continue
            if pid <= 0 or pid in seen_ids:
                continue
            seen_ids.add(pid)
            ids.append(pid)
        if len(ids) >= 9:
            return ids[:9]
        key = tuple(ids)
        if key and key not in seen_variants:
            seen_variants.add(key)
    return []


def parse_official_starting_lineups_page(html_text: str) -> Dict[int, Dict[str, Any]]:
    text = html.unescape(str(html_text or ""))
    out: Dict[int, Dict[str, Any]] = {}
    if not text:
        return out

    starts = list(re.finditer(r'<div class="starting-lineups__matchup\b[^>]*data-gamePk=?"?(\d+)"?[^>]*>', text, flags=re.IGNORECASE))
    if not starts:
        return out

    for idx, match in enumerate(starts):
        try:
            game_pk = int(match.group(1))
        except Exception:
            continue
        start_pos = int(match.start())
        end_pos = int(starts[idx + 1].start()) if idx + 1 < len(starts) else len(text)
        block = text[start_pos:end_pos]
        tri_codes = re.findall(r'data-tri-code="([A-Z]{2,3})"', block, flags=re.IGNORECASE)
        away_ids = _parse_starting_lineups_block_lineup_ids(block, "away")
        home_ids = _parse_starting_lineups_block_lineup_ids(block, "home")
        out[int(game_pk)] = {
            "game_pk": int(game_pk),
            "away_ids": [int(pid) for pid in away_ids[:9]],
            "home_ids": [int(pid) for pid in home_ids[:9]],
            "away_team": str(tri_codes[0]).upper() if len(tri_codes) >= 1 else "",
            "home_team": str(tri_codes[1]).upper() if len(tri_codes) >= 2 else "",
            "official": bool(len(away_ids) >= 9 and len(home_ids) >= 9),
        }
    return out


def fetch_official_starting_lineups_for_date(client: StatsApiClient, date_str: str) -> Dict[int, Dict[str, Any]]:
    text = str(date_str or "").strip()
    if not text:
        return {}
    url = f"https://www.mlb.com/starting-lineups/{text}"
    cache_key = {"url": url, "date": text}
    if client.cache is not None:
        hit = client.cache.get("mlb_starting_lineups_html", cache_key, ttl_seconds=int(min(client.cache_ttl_seconds, 900)))
        if isinstance(hit, dict) and isinstance(hit.get("html"), str):
            return parse_official_starting_lineups_page(str(hit.get("html") or ""))

    session = requests.Session()
    session.trust_env = client._effective_trust_env()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml",
    }
    response = session.get(url, headers=headers, timeout=(client.timeout_sec, client.timeout_sec))
    response.raise_for_status()
    html_text = str(response.text or "")
    if client.cache is not None and html_text:
        client.cache.set("mlb_starting_lineups_html", cache_key, {"html": html_text})
    return parse_official_starting_lineups_page(html_text)


def _parse_rotowire_batting_order_block(html_text: str, label: str) -> List[str]:
    pattern = rf'>{re.escape(str(label or "").strip())}</div>\s*<ol class="list is-rankings pad-5-10">(.*?)</ol>'
    match = re.search(pattern, str(html_text or ""), flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    block = str(match.group(1) or "")
    out: List[str] = []
    seen: set[str] = set()
    for raw_name in re.findall(r'<a [^>]*>(.*?)</a>', block, flags=re.IGNORECASE | re.DOTALL):
        clean = re.sub(r"<[^>]+>", "", html.unescape(str(raw_name or "")))
        clean = re.sub(r"\s+", " ", clean).strip()
        if not clean:
            continue
        key = clean.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out


def parse_rotowire_batting_orders_page(html_text: str) -> Dict[str, Any]:
    text = html.unescape(str(html_text or ""))
    if not text:
        return {}

    team_name = ""
    title_match = re.search(r"<title>\s*\d{4}\s+(.*?)\s+Batting Orders\s*\|\s*RotoWire\s*</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if title_match:
        team_name = re.sub(r"\s+", " ", str(title_match.group(1) or "")).strip()

    today_lineup = _parse_rotowire_batting_order_block(text, "Today's Lineup")
    default_vs_rhp = _parse_rotowire_batting_order_block(text, "Default vs. RHP")
    default_vs_lhp = _parse_rotowire_batting_order_block(text, "Default vs. LHP")

    return {
        "team_name": team_name,
        "today_lineup": today_lineup[:9],
        "default_vs_rhp": default_vs_rhp[:9],
        "default_vs_lhp": default_vs_lhp[:9],
        "has_today_lineup": bool(len(today_lineup) >= 9),
    }


def fetch_rotowire_batting_orders_for_team(client: StatsApiClient, team_abbr: str) -> Dict[str, Any]:
    abbr = str(team_abbr or "").strip().upper()
    if not abbr:
        return {}

    url = f"https://www.rotowire.com/baseball/batting-orders.php?team={abbr}"
    cache_key = {"url": url, "team": abbr}
    if client.cache is not None:
        hit = client.cache.get("rotowire_batting_orders_html", cache_key, ttl_seconds=int(min(client.cache_ttl_seconds, 3600)))
        if isinstance(hit, dict) and isinstance(hit.get("html"), str):
            parsed = parse_rotowire_batting_orders_page(str(hit.get("html") or ""))
            if parsed:
                parsed["team_abbr"] = abbr
            return parsed

    session = requests.Session()
    session.trust_env = client._effective_trust_env()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml",
    }
    response = session.get(url, headers=headers, timeout=(client.timeout_sec, client.timeout_sec))
    response.raise_for_status()
    html_text = str(response.text or "")
    if client.cache is not None and html_text:
        client.cache.set("rotowire_batting_orders_html", cache_key, {"html": html_text})
    parsed = parse_rotowire_batting_orders_page(html_text)
    if parsed:
        parsed["team_abbr"] = abbr
    return parsed


def extract_team_pitcher_pitches_thrown(feed_live: Dict[str, Any], team_id: int) -> Dict[int, int]:
    """Extract per-pitcher pitches thrown for a given team from a feed/live payload."""
    try:
        tid = int(team_id)
    except Exception:
        return {}

    game_data = (feed_live.get("gameData") or {})
    teams_gd = (game_data.get("teams") or {})
    home_id = (teams_gd.get("home") or {}).get("id")
    away_id = (teams_gd.get("away") or {}).get("id")
    side = None
    try:
        if home_id is not None and int(home_id) == tid:
            side = "home"
        elif away_id is not None and int(away_id) == tid:
            side = "away"
    except Exception:
        side = None
    if side is None:
        return {}

    live_data = (feed_live.get("liveData") or {})
    box = (live_data.get("boxscore") or {})
    teams = (box.get("teams") or {})
    t = (teams.get(side) or {})
    pitchers = t.get("pitchers") or []
    players = t.get("players") or {}
    if not isinstance(pitchers, list) or not isinstance(players, dict):
        return {}

    out: Dict[int, int] = {}

    def _safe_int(x) -> Optional[int]:
        try:
            if x is None or str(x).strip() == "":
                return None
            return int(float(x))
        except Exception:
            return None

    for pid_any in pitchers:
        try:
            pid = int(pid_any)
        except Exception:
            continue
        pobj = players.get(f"ID{pid}") or {}
        if not isinstance(pobj, dict):
            continue
        pst = ((pobj.get("stats") or {}).get("pitching") or {})
        if not isinstance(pst, dict):
            pst = {}

        pitches = _safe_int(pst.get("pitchesThrown"))
        if pitches is None:
            pitches = _safe_int(pst.get("numberOfPitches"))

        if pitches is None:
            # crude fallback from outs if pitchesThrown is absent
            outs = _safe_int(pst.get("outs"))
            if outs is not None and outs > 0:
                # ~15 pitches per inning
                pitches = int(round((outs / 3.0) * 15.0))

        if pitches is None:
            continue
        out[pid] = max(0, int(pitches))

    return out


def _parse_weather_from_game_data(game_data: Dict[str, Any]) -> WeatherFactors:
    w = (game_data.get("weather") or {})

    condition = str(w.get("condition") or w.get("weather") or "").strip()
    temp = w.get("temp")
    if temp is None:
        temp = w.get("temperature")
    temperature_f = None
    try:
        if temp is not None and str(temp).strip() != "":
            temperature_f = float(temp)
    except Exception:
        temperature_f = None

    wind_raw = str(w.get("wind") or "").strip()
    wind_speed_mph = None
    if wind_raw:
        m = re.search(r"(\d+(?:\.\d+)?)\s*mph", wind_raw, flags=re.IGNORECASE)
        if m:
            try:
                wind_speed_mph = float(m.group(1))
            except Exception:
                wind_speed_mph = None

    wind_direction = "unknown"
    wr = wind_raw.lower()
    if "out" in wr:
        wind_direction = "out"
    elif "in" in wr:
        wind_direction = "in"
    elif "l to r" in wr or "left to right" in wr or "r to l" in wr or "right to left" in wr:
        wind_direction = "cross"

    # Dome / roof status (best effort)
    is_dome = None
    try:
        venue = (game_data.get("venue") or {})
        fi = (venue.get("fieldInfo") or {})
        roof_type = str(fi.get("roofType") or "").lower()
        roof_status = str(fi.get("roofStatus") or "").lower()
        if "dome" in roof_type:
            is_dome = True
        if roof_status in ("closed", "open"):
            is_dome = (roof_status == "closed")
    except Exception:
        is_dome = None

    return WeatherFactors(
        source="statsapi_live_feed",
        condition=condition,
        temperature_f=temperature_f,
        wind_speed_mph=wind_speed_mph,
        wind_direction=wind_direction,
        wind_raw=wind_raw,
        is_dome=is_dome,
    )


def _parse_park_from_game_data(game_data: Dict[str, Any]) -> ParkFactors:
    venue = (game_data.get("venue") or {})
    fi = (venue.get("fieldInfo") or {})
    park = ParkFactors(
        source="statsapi_live_feed",
        venue_id=int(venue.get("id")) if venue.get("id") is not None else None,
        venue_name=str(venue.get("name") or ""),
        roof_type=str(fi.get("roofType") or ""),
        roof_status=str(fi.get("roofStatus") or ""),
    )

    def _num(x):
        try:
            if x is None or str(x).strip() == "":
                return None
            return float(x)
        except Exception:
            return None

    # Use the three most commonly present for simple geometry.
    park.left_line = _num(fi.get("leftLine"))
    park.center = _num(fi.get("center"))
    park.right_line = _num(fi.get("rightLine"))

    # Apply optional Statcast-derived park factor overrides if present.
    try:
        root = Path(__file__).resolve().parents[2]
        p = root / "data" / "park" / "park_factors.json"
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                key_id = str(park.venue_id) if park.venue_id is not None else ""
                key_name = str(park.venue_name or "").strip()
                entry = None
                if key_id and key_id in raw and isinstance(raw.get(key_id), dict):
                    entry = raw.get(key_id)
                elif key_name and key_name in raw and isinstance(raw.get(key_name), dict):
                    entry = raw.get(key_name)
                if isinstance(entry, dict):
                    for k_src, k_dst in (
                        ("hr_mult", "hr_mult_override"),
                        ("inplay_hit_mult", "inplay_hit_mult_override"),
                        ("xb_share_mult", "xb_share_mult_override"),
                    ):
                        v = entry.get(k_src)
                        if isinstance(v, (int, float)):
                            setattr(park, k_dst, float(v))
                    park.source = "statsapi_live_feed+local_map"
    except Exception:
        pass
    return park


def _load_local_umpire_factor_map() -> Dict[int, Dict[str, Any]]:
    """Load local umpire factor overrides.

    File format: data/umpire/umpire_factors.json
    {
      "521251": {"called_strike_mult": 1.02}
    }

    Returns: {umpire_id: {..}} and name-keyed entries are handled separately.
    """
    try:
        root = Path(__file__).resolve().parents[2]
        p = root / "data" / "umpire" / "umpire_factors.json"
        if not p.exists():
            return {}
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        out: Dict[int, Dict[str, Any]] = {}
        for k, v in raw.items():
            try:
                kid = int(k)
            except Exception:
                continue
            if isinstance(v, dict):
                out[kid] = v
        return out
    except Exception:
        return {}


_UMPIRE_FACTOR_MAP_CACHE: Dict[str, Any] | None = None


def _load_local_umpire_factor_map_anykey() -> Dict[str, Dict[str, Any]]:
    """Load local umpire factor overrides keyed by either ID (string) or name."""
    try:
        # cache per-process; file is small
        global _UMPIRE_FACTOR_MAP_CACHE
        if _UMPIRE_FACTOR_MAP_CACHE is not None:
            return _UMPIRE_FACTOR_MAP_CACHE

        root = Path(__file__).resolve().parents[2]
        p = root / "data" / "umpire" / "umpire_factors.json"
        if not p.exists():
            _UMPIRE_FACTOR_MAP_CACHE = {}
            return {}
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            _UMPIRE_FACTOR_MAP_CACHE = {}
            return {}

        out: Dict[str, Dict[str, Any]] = {}
        for k, v in raw.items():
            if not isinstance(v, dict):
                continue
            ks = str(k).strip()
            if not ks:
                continue
            out[ks] = v
        _UMPIRE_FACTOR_MAP_CACHE = out
        return out
    except Exception:
        _UMPIRE_FACTOR_MAP_CACHE = {}
        return {}


def _parse_umpire_from_feed(game_data: Dict[str, Any], live_data: Dict[str, Any]) -> UmpireFactors:
    """Parse home-plate umpire from the live feed.

    StatsAPI commonly exposes officials under liveData.boxscore.officials.
    """
    ump = UmpireFactors(source="statsapi_live_feed")

    # Preferred location: liveData.boxscore.officials
    officials = ((live_data.get("boxscore") or {}).get("officials") or [])
    # Fallback (rare): gameData.officials
    if not officials:
        officials = (game_data.get("officials") or [])

    hp = None
    for o in officials:
        ot = str(o.get("officialType") or "").strip().lower()
        if ot in ("home plate", "home_plate", "hp"):
            hp = o
            break

    if not hp:
        return ump

    off = (hp.get("official") or {})
    try:
        if off.get("id") is not None:
            ump.home_plate_umpire_id = int(off.get("id"))
    except Exception:
        pass
    ump.home_plate_umpire_name = str(off.get("fullName") or "")

    # Apply optional local overrides if present.
    factor_map = _load_local_umpire_factor_map_anykey()
    key_id = str(ump.home_plate_umpire_id) if ump.home_plate_umpire_id is not None else ""
    key_name = str(ump.home_plate_umpire_name or "").strip()
    m = None
    if key_id and key_id in factor_map:
        m = factor_map.get(key_id)
    elif key_name and key_name in factor_map:
        m = factor_map.get(key_name)

    if isinstance(m, dict):
        csm = m.get("called_strike_mult")
        try:
            if csm is not None:
                ump.called_strike_mult = float(csm)
                ump.source = "statsapi_live_feed+local_map"
        except Exception:
            pass

    return ump


def fetch_schedule_for_date(client: StatsApiClient, date_str: str) -> List[Dict[str, Any]]:
    data = client.get(
        "/schedule",
        params={
            "sportId": 1,
            "date": date_str,
            "hydrate": "probablePitcher,team",
        },
    )
    games: List[Dict[str, Any]] = []
    for d in data.get("dates", []) or []:
        for g in d.get("games", []) or []:
            games.append(g)
    return games


def fetch_schedule_date_buckets(client: StatsApiClient, season: int, game_type: str | None = None) -> List[Dict[str, Any]]:
    """Fetch raw schedule date buckets for an MLB season.

    This is useful for deriving season calendar bounds and previous/next game
    dates without loading per-game detail endpoints.
    """
    params: Dict[str, Any] = {
        "sportId": 1,
        "season": int(season),
    }
    if game_type:
        params["gameType"] = str(game_type)

    data = client.get("/schedule", params=params)
    buckets: List[Dict[str, Any]] = []
    for bucket in data.get("dates", []) or []:
        if isinstance(bucket, dict):
            buckets.append(bucket)
    return buckets


def fetch_mlb_teams(client: StatsApiClient, season: int | None = None) -> List[Dict[str, Any]]:
    """Fetch MLB teams (sportId=1).

    Returns the raw `teams` list from the StatsAPI.
    """
    params: Dict[str, Any] = {"sportId": 1}
    if season is not None:
        params["season"] = int(season)
    data = client.get("/teams", params=params)
    return data.get("teams", []) or []


def fetch_team_roster(client: StatsApiClient, team_id: int, roster_type: str = "active", date_str: str | None = None) -> List[Dict[str, Any]]:
    """Fetch a team's roster from StatsAPI.

    roster_type examples: "active", "40Man", "nonRosterInvitees".
    The StatsAPI may support additional values; this helper intentionally stays
    thin and returns the raw `roster` list.
    """
    params: Dict[str, Any] = {"rosterType": str(roster_type)}
    if date_str:
        params["date"] = str(date_str)
    data = client.get(
        f"/teams/{int(team_id)}/roster",
        params=params,
    )
    return data.get("roster", []) or []


def fetch_active_roster(client: StatsApiClient, team_id: int, date_str: str | None = None) -> List[Dict[str, Any]]:
    return fetch_team_roster(client, team_id=team_id, roster_type="active", date_str=date_str)


def fetch_person(client: StatsApiClient, person_id: int) -> Dict[str, Any]:
    data = client.get(f"/people/{person_id}")
    people = data.get("people", []) or []
    return people[0] if people else {}


def fetch_person_season_hitting(client: StatsApiClient, person_id: int, season: int) -> Dict[str, Any]:
    data = client.get(
        f"/people/{person_id}/stats",
        params={"stats": "season", "group": "hitting", "season": season},
    )
    for grp in data.get("stats", []) or []:
        for split in grp.get("splits", []) or []:
            return split.get("stat", {}) or {}
    return {}


def fetch_person_season_pitching(client: StatsApiClient, person_id: int, season: int) -> Dict[str, Any]:
    data = client.get(
        f"/people/{person_id}/stats",
        params={"stats": "season", "group": "pitching", "season": season},
    )
    for grp in data.get("stats", []) or []:
        for split in grp.get("splits", []) or []:
            return split.get("stat", {}) or {}
    return {}


def fetch_person_gamelog(client: StatsApiClient, person_id: int, season: int, group: str) -> List[Dict[str, Any]]:
    """Fetch a player's season game log splits.

    group: "hitting" or "pitching"
    """
    data = client.get(
        f"/people/{person_id}/stats",
        params={"stats": "gameLog", "group": group, "season": season},
    )
    for grp in data.get("stats", []) or []:
        splits = grp.get("splits", []) or []
        return splits
    return []


def fetch_person_stat_splits(client: StatsApiClient, person_id: int, season: int, group: str, sit_codes: str = "vl,vr") -> Dict[str, Dict[str, Any]]:
    """Fetch season stat splits for a player.

    group: "hitting" or "pitching"
    sit_codes: comma-separated situational codes. For platoon splits, use "vl,vr".

    Returns mapping: {code: stat_dict}
    """
    out: Dict[str, Dict[str, Any]] = {}
    try:
        data = client.get(
            f"/people/{int(person_id)}/stats",
            params={"stats": "statSplits", "group": group, "season": int(season), "sitCodes": str(sit_codes)},
        )
        stats = data.get("stats", []) or []
        for grp in stats:
            for split in (grp.get("splits", []) or []):
                s = (split.get("split") or {}) if isinstance(split, dict) else {}
                code = str(s.get("code") or "").strip().lower()
                if not code:
                    desc = str(s.get("description") or "").strip().lower()
                    if "left" in desc:
                        code = "vl"
                    elif "right" in desc:
                        code = "vr"
                stat = (split.get("stat") or {}) if isinstance(split, dict) else {}
                if code and isinstance(stat, dict):
                    out[code] = stat
    except Exception:
        out = {}

    # Fallback attempt: some environments use different sit codes.
    if not out:
        try:
            data = client.get(
                f"/people/{int(person_id)}/stats",
                params={"stats": "statSplits", "group": group, "season": int(season), "sitCodes": "vsLeft,vsRight"},
            )
            stats = data.get("stats", []) or []
            for grp in stats:
                for split in (grp.get("splits", []) or []):
                    s = (split.get("split") or {}) if isinstance(split, dict) else {}
                    code = str(s.get("code") or "").strip().lower()
                    desc = str(s.get("description") or "").strip().lower()
                    if "left" in desc:
                        code = "vl"
                    elif "right" in desc:
                        code = "vr"
                    stat = (split.get("stat") or {}) if isinstance(split, dict) else {}
                    if code and isinstance(stat, dict):
                        out[code] = stat
        except Exception:
            return out

    return out


def fetch_person_pitch_arsenal(client: StatsApiClient, person_id: int, season: int) -> tuple[Dict[PitchType, float], int]:
    """Fetch pitch mix (usage) for a pitcher from StatsAPI.

    Returns (mix, total_pitches). Mix is keyed by the engine's canonical PitchType.
    """
    data = client.get(
        f"/people/{person_id}/stats",
        params={"stats": "pitchArsenal", "group": "pitching", "season": season},
    )

    stats = data.get("stats", []) or []
    if not stats:
        return {}, 0
    splits = (stats[0].get("splits") or [])
    if not splits:
        return {}, 0

    def canon(code: str) -> PitchType:
        code = (code or "").strip().upper()
        try:
            return PitchType(code)
        except Exception:
            # Map a couple known alternates
            if code in ("FT",):
                return PitchType.SI
            if code in ("FA",):
                return PitchType.FF
            return PitchType.OTHER

    mix: Dict[PitchType, float] = {}
    total_pitches = 0
    for s in splits:
        stat = (s.get("stat") or {})
        t = (stat.get("type") or {})
        code = (t.get("code") or "")
        pct = stat.get("percentage")
        tot = stat.get("totalPitches")
        if isinstance(tot, (int, float)):
            total_pitches = max(total_pitches, int(tot))
        try:
            pct_f = float(pct)
        except Exception:
            continue
        pt = canon(code)
        mix[pt] = mix.get(pt, 0.0) + max(0.0, pct_f)

    # Normalize in case percentages don't sum perfectly.
    s = sum(mix.values())
    if s > 0:
        mix = {k: float(v) / s for k, v in mix.items()}
    return mix, int(total_pitches)
