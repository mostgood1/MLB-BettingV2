from __future__ import annotations

import csv
import gzip
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from .disk_cache import DiskCache


_FILE_DAILY_INDEX_MEMO: Dict[str, Dict[str, Dict[int, Dict[int, "BvPCounts"]]]] = {}


@dataclass(frozen=True)
class BvPCounts:
    pa: int
    hr: int
    hits: int = 0
    so: int = 0
    bb: int = 0
    hbp: int = 0
    inplay_pa: int = 0
    inplay_hits: int = 0


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_bvp_cache(ttl_seconds: int = 30 * 24 * 3600, cache_dir: str | None = None) -> DiskCache:
    root = Path(cache_dir) if cache_dir else (_root() / "data" / "cache" / "statcast" / "bvp")
    return DiskCache(root_dir=root, default_ttl_seconds=ttl_seconds)


def _parse_date(x: str) -> date:
    return date.fromisoformat(str(x).strip()[:10])


def _iter_statcast_pitch_files(season: int, raw_root: Optional[Path] = None) -> Iterable[Path]:
    root = raw_root if raw_root is not None else (_root() / "data" / "raw" / "statcast" / "pitches" / str(int(season)))
    if not root.exists():
        return []
    return sorted(root.glob("**/statcast_*.csv.gz"))


def _file_window(path: Path) -> Optional[Tuple[date, date]]:
    # Expect: statcast_YYYY-MM-DD_YYYY-MM-DD.csv.gz
    name = path.name
    try:
        stem = name
        if stem.endswith(".csv.gz"):
            stem = stem[: -len(".csv.gz")]
        if not stem.startswith("statcast_"):
            return None
        parts = stem.split("_")
        if len(parts) < 3:
            return None
        start = _parse_date(parts[1])
        end = _parse_date(parts[2])
        return start, end
    except Exception:
        return None


def _overlaps(a0: date, a1: date, b0: date, b1: date) -> bool:
    return not (a1 < b0 or b1 < a0)


def _query_seasons_for_window(season: int, start_date: date, end_date: date, raw_root: Optional[Path] = None) -> Tuple[int, ...]:
    if raw_root is not None:
        return (int(season),)
    lo = min(start_date, end_date)
    hi = max(start_date, end_date)
    seasons = {int(season)}
    for year in range(int(lo.year), int(hi.year) + 1):
        seasons.add(int(year))
    return tuple(sorted(seasons))


def _available_statcast_seasons(raw_root: Optional[Path] = None) -> Tuple[int, ...]:
    root = raw_root if raw_root is not None else (_root() / "data" / "raw" / "statcast" / "pitches")
    if not root.exists() or not root.is_dir():
        return ()
    seasons = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            seasons.append(int(child.name))
        except Exception:
            continue
    return tuple(sorted(set(seasons)))


def _career_start_date(end_date: date, raw_root: Optional[Path] = None) -> date:
    seasons = _available_statcast_seasons(raw_root=raw_root)
    if seasons:
        return date(int(min(seasons)), 1, 1)
    return date(int(end_date.year), 1, 1)


def _metrics_from_counts(
    *,
    counts: BvPCounts,
    batter: Any,
    shrink_pa: float,
    clamp_lo: float,
    clamp_hi: float,
) -> Dict[str, float]:
    hr_mult = hr_multiplier_from_bvp(
        batter_hr_rate=float(getattr(batter, "hr_rate", 0.03) or 0.03),
        pa=int(counts.pa),
        hr=int(counts.hr),
        shrink_pa=float(shrink_pa),
        clamp_lo=float(clamp_lo),
        clamp_hi=float(clamp_hi),
    )
    k_mult = rate_multiplier_from_bvp(
        base_rate=float(getattr(batter, "k_rate", 0.22) or 0.22),
        opportunities=int(counts.pa),
        successes=int(counts.so),
        shrink_pa=float(shrink_pa),
        clamp_lo=float(clamp_lo),
        clamp_hi=float(clamp_hi),
    )
    bb_mult = rate_multiplier_from_bvp(
        base_rate=float(getattr(batter, "bb_rate", 0.08) or 0.08),
        opportunities=int(counts.pa),
        successes=int(counts.bb),
        shrink_pa=float(shrink_pa),
        clamp_lo=float(clamp_lo),
        clamp_hi=float(clamp_hi),
    )
    inplay_mult = rate_multiplier_from_bvp(
        base_rate=float(getattr(batter, "inplay_hit_rate", 0.28) or 0.28),
        opportunities=int(counts.inplay_pa),
        successes=int(counts.inplay_hits),
        shrink_pa=float(shrink_pa),
        clamp_lo=float(clamp_lo),
        clamp_hi=float(clamp_hi),
    )
    return {
        "pa": float(counts.pa),
        "hits": float(counts.hits),
        "hr": float(counts.hr),
        "so": float(counts.so),
        "bb": float(counts.bb),
        "hbp": float(counts.hbp),
        "inplay_pa": float(counts.inplay_pa),
        "inplay_hits": float(counts.inplay_hits),
        "hr_mult": float(hr_mult),
        "k_mult": float(k_mult),
        "bb_mult": float(bb_mult),
        "inplay_mult": float(inplay_mult),
    }


def _counts_from_cache_doc(doc: Any) -> Optional[Dict[int, BvPCounts]]:
    if not isinstance(doc, dict) or "by_batter" not in doc:
        return None
    out: Dict[int, BvPCounts] = {}
    by_b = doc.get("by_batter") or {}
    if not isinstance(by_b, dict):
        return out
    for k, v in by_b.items():
        try:
            bid = int(k)
        except Exception:
            continue
        if not isinstance(v, dict):
            continue
        try:
            pa = int(v.get("pa") or 0)
            hr = int(v.get("hr") or 0)
            hits = int(v.get("hits") or 0)
            so = int(v.get("so") or 0)
            bb = int(v.get("bb") or 0)
            hbp = int(v.get("hbp") or 0)
            inplay_pa = int(v.get("inplay_pa") or 0)
            inplay_hits = int(v.get("inplay_hits") or 0)
        except Exception:
            continue
        if pa > 0:
            out[bid] = BvPCounts(
                pa=pa,
                hr=hr,
                hits=hits,
                so=so,
                bb=bb,
                hbp=hbp,
                inplay_pa=inplay_pa,
                inplay_hits=inplay_hits,
            )
    return out


def _counts_to_cache_payload(result: Dict[int, BvPCounts]) -> Dict[str, Dict[str, int]]:
    return {
        str(bid): {
            "pa": c.pa,
            "hr": c.hr,
            "hits": c.hits,
            "so": c.so,
            "bb": c.bb,
            "hbp": c.hbp,
            "inplay_pa": c.inplay_pa,
            "inplay_hits": c.inplay_hits,
        }
        for bid, c in result.items()
    }


def _counts_to_cache_doc(result: Dict[int, BvPCounts], *, seasons: Tuple[int, ...]) -> Dict[str, Any]:
    return {
        "seasons": [int(x) for x in seasons],
        "by_batter": _counts_to_cache_payload(result),
    }


def _merge_bvp_counts(dst: Dict[int, BvPCounts], src: Dict[int, BvPCounts]) -> Dict[int, BvPCounts]:
    out = dict(dst)
    for bid, counts in src.items():
        current = out.get(int(bid))
        if current is None:
            out[int(bid)] = counts
            continue
        out[int(bid)] = BvPCounts(
            pa=int(current.pa) + int(counts.pa),
            hr=int(current.hr) + int(counts.hr),
            hits=int(current.hits) + int(counts.hits),
            so=int(current.so) + int(counts.so),
            bb=int(current.bb) + int(counts.bb),
            hbp=int(current.hbp) + int(counts.hbp),
            inplay_pa=int(current.inplay_pa) + int(counts.inplay_pa),
            inplay_hits=int(current.inplay_hits) + int(counts.inplay_hits),
        )
    return out


def _file_cache_parts(path: Path) -> Dict[str, Any]:
    try:
        st = path.stat()
        return {
            "path": str(path.resolve()),
            "size": int(st.st_size),
            "mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))),
        }
    except Exception:
        return {"path": str(path.resolve())}


def _counts_map_from_payload(payload: Any) -> Dict[int, BvPCounts]:
    return _counts_from_cache_doc({"by_batter": payload}) or {}


def _daily_file_index_from_cache_doc(doc: Any) -> Optional[Dict[str, Dict[int, Dict[int, BvPCounts]]]]:
    if not isinstance(doc, dict):
        return None
    by_date = doc.get("by_date") or {}
    if not isinstance(by_date, dict):
        return None
    out: Dict[str, Dict[int, Dict[int, BvPCounts]]] = {}
    for day_key, day_payload in by_date.items():
        if not isinstance(day_payload, dict):
            continue
        pitcher_map: Dict[int, Dict[int, BvPCounts]] = {}
        for pid_key, batter_payload in day_payload.items():
            try:
                pid = int(pid_key)
            except Exception:
                continue
            counts = _counts_map_from_payload(batter_payload)
            if counts:
                pitcher_map[int(pid)] = counts
        if pitcher_map:
            out[str(day_key)] = pitcher_map
    return out


def _daily_file_index_to_cache_doc(index: Dict[str, Dict[int, Dict[int, BvPCounts]]]) -> Dict[str, Any]:
    return {
        "by_date": {
            str(day_key): {
                str(pid): _counts_to_cache_payload(counts)
                for pid, counts in pitcher_map.items()
                if counts
            }
            for day_key, pitcher_map in index.items()
            if pitcher_map
        }
    }


def _scan_statcast_file_daily_index(path: Path) -> Dict[str, Dict[int, Dict[int, BvPCounts]]]:
    daily: Dict[str, Dict[int, Dict[int, Dict[str, int]]]] = {}

    hit_events = {"single", "double", "triple", "home_run"}
    walk_events = {"walk", "intent_walk"}
    hbp_events = {"hit_by_pitch"}
    inplay_hit_events = {"single", "double", "triple"}

    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            gd = row.get("game_date")
            if not gd:
                continue
            try:
                day_key = _parse_date(gd).isoformat()
            except Exception:
                continue

            ev = (row.get("events") or "").strip().lower()
            if not ev:
                continue

            try:
                pid = int(row.get("pitcher") or 0)
                bid = int(row.get("batter") or 0)
            except Exception:
                continue
            if pid <= 0 or bid <= 0:
                continue

            pitcher_map = daily.setdefault(day_key, {})
            batter_map = pitcher_map.setdefault(int(pid), {})
            counts = batter_map.get(int(bid))
            if counts is None:
                counts = {
                    "pa": 0,
                    "hr": 0,
                    "hits": 0,
                    "so": 0,
                    "bb": 0,
                    "hbp": 0,
                    "inplay_pa": 0,
                    "inplay_hits": 0,
                }
                batter_map[int(bid)] = counts

            counts["pa"] += 1
            if ev in hit_events:
                counts["hits"] += 1
            if ev == "home_run":
                counts["hr"] += 1
            elif ev in walk_events:
                counts["bb"] += 1
            elif ev in hbp_events:
                counts["hbp"] += 1
            elif ev.startswith("strikeout"):
                counts["so"] += 1
            else:
                counts["inplay_pa"] += 1
                if ev in inplay_hit_events:
                    counts["inplay_hits"] += 1

    out: Dict[str, Dict[int, Dict[int, BvPCounts]]] = {}
    for day_key, pitcher_map in daily.items():
        out_pitchers: Dict[int, Dict[int, BvPCounts]] = {}
        for pid, batter_map in pitcher_map.items():
            out_batters: Dict[int, BvPCounts] = {}
            for bid, counts in batter_map.items():
                pa = int(counts.get("pa") or 0)
                if pa <= 0:
                    continue
                out_batters[int(bid)] = BvPCounts(
                    pa=pa,
                    hr=int(counts.get("hr") or 0),
                    hits=int(counts.get("hits") or 0),
                    so=int(counts.get("so") or 0),
                    bb=int(counts.get("bb") or 0),
                    hbp=int(counts.get("hbp") or 0),
                    inplay_pa=int(counts.get("inplay_pa") or 0),
                    inplay_hits=int(counts.get("inplay_hits") or 0),
                )
            if out_batters:
                out_pitchers[int(pid)] = out_batters
        if out_pitchers:
            out[str(day_key)] = out_pitchers
    return out


def _statcast_file_daily_index(
    path: Path,
    *,
    cache: Optional[DiskCache] = None,
    ttl_seconds: int = 30 * 24 * 3600,
) -> Dict[str, Dict[int, Dict[int, BvPCounts]]]:
    parts = _file_cache_parts(path)
    memo_key = str(parts)
    cached_memo = _FILE_DAILY_INDEX_MEMO.get(memo_key)
    if cached_memo is not None:
        return cached_memo

    if cache is not None:
        hit = cache.get("statcast_bvp_file_daily", parts, ttl_seconds=ttl_seconds)
        cached_doc = _daily_file_index_from_cache_doc(hit)
        if cached_doc is not None:
            _FILE_DAILY_INDEX_MEMO[memo_key] = cached_doc
            return cached_doc

    fresh = _scan_statcast_file_daily_index(path)
    _FILE_DAILY_INDEX_MEMO[memo_key] = fresh
    if cache is not None:
        cache.set("statcast_bvp_file_daily", parts, _daily_file_index_to_cache_doc(fresh))
    return fresh


def pitcher_vs_batters_counts(
    *,
    season: int,
    pitcher_id: int,
    start_date: date,
    end_date: date,
    raw_root: Optional[Path] = None,
    cache: Optional[DiskCache] = None,
    ttl_seconds: int = 30 * 24 * 3600,
) -> Dict[int, BvPCounts]:
    """Return per-batter (PA, HR) for a given pitcher from Statcast raw pitch files.

    Uses the Statcast pitch-level CSVs where `events` is populated on the terminal pitch
    of a plate appearance. PA count is approximated by counting rows with non-empty
    `events` for the pitcher/batter matchup.
    """

    pid = int(pitcher_id)
    if pid <= 0:
        return {}

    seasons = _query_seasons_for_window(int(season), start_date, end_date, raw_root=raw_root)

    parts = {
        "season": int(season),
        "seasons": ",".join(str(int(x)) for x in seasons),
        "pitcher_id": int(pid),
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
    }
    if cache is not None:
        hit = cache.get("statcast_bvp_pitcher", parts, ttl_seconds=ttl_seconds)
        cached = _counts_from_cache_doc(hit)
        if cached is not None:
            return cached

    result: Dict[int, BvPCounts] = {}

    for query_season in seasons:
        for path in _iter_statcast_pitch_files(int(query_season), raw_root=raw_root):
            w = _file_window(path)
            if w is None:
                continue
            f0, f1 = w
            if not _overlaps(f0, f1, start_date, end_date):
                continue

            day_index = _statcast_file_daily_index(path, cache=cache, ttl_seconds=ttl_seconds)
            for day_key, pitcher_map in day_index.items():
                try:
                    d = _parse_date(day_key)
                except Exception:
                    continue
                if d < start_date or d > end_date:
                    continue
                pitcher_counts = pitcher_map.get(pid)
                if pitcher_counts:
                    result = _merge_bvp_counts(result, pitcher_counts)

    if cache is not None:
        cache.set("statcast_bvp_pitcher", parts, _counts_to_cache_doc(result, seasons=seasons))

    return result


def rate_multiplier_from_bvp(
    *,
    base_rate: float,
    opportunities: int,
    successes: int,
    shrink_pa: float = 50.0,
    clamp_lo: float = 0.80,
    clamp_hi: float = 1.25,
) -> float:
    try:
        base = float(base_rate)
        if base <= 1e-9:
            return 1.0
        opp_i = int(opportunities)
        succ_i = int(successes)
        if opp_i <= 0:
            return 1.0
        emp = float(succ_i) / float(opp_i)
        raw = emp / base
        w = float(opp_i) / float(opp_i + max(1e-9, float(shrink_pa)))
        mult = 1.0 + w * (raw - 1.0)
        if not math.isfinite(mult):
            return 1.0
        return float(max(clamp_lo, min(clamp_hi, mult)))
    except Exception:
        return 1.0


def hr_multiplier_from_bvp(
    *,
    batter_hr_rate: float,
    pa: int,
    hr: int,
    shrink_pa: float = 50.0,
    clamp_lo: float = 0.80,
    clamp_hi: float = 1.25,
) -> float:
    return rate_multiplier_from_bvp(
        base_rate=batter_hr_rate,
        opportunities=pa,
        successes=hr,
        shrink_pa=shrink_pa,
        clamp_lo=clamp_lo,
        clamp_hi=clamp_hi,
    )


def apply_starter_bvp_hr_multipliers(
    *,
    batting_roster: Any,
    pitcher_id: int,
    season: int,
    start_date: date,
    end_date: date,
    cache: Optional[DiskCache] = None,
    min_pa: int = 10,
    shrink_pa: float = 50.0,
    clamp_lo: float = 0.80,
    clamp_hi: float = 1.25,
) -> int:
    pid = int(pitcher_id or 0)
    if pid <= 0:
        return 0

    lineup = getattr(getattr(batting_roster, "lineup", None), "batters", None) or []
    if not lineup:
        return 0

    window_by_batter = pitcher_vs_batters_counts(
        season=int(season),
        pitcher_id=pid,
        start_date=start_date,
        end_date=end_date,
        cache=cache,
    )
    career_start = _career_start_date(end_date)
    career_by_batter: Dict[int, BvPCounts] = {}
    if career_start < start_date:
        career_by_batter = pitcher_vs_batters_counts(
            season=int(season),
            pitcher_id=pid,
            start_date=career_start,
            end_date=end_date,
            cache=cache,
        )
    else:
        career_by_batter = dict(window_by_batter)

    if not window_by_batter and not career_by_batter:
        return 0

    applied = 0
    min_pa_i = max(1, int(min_pa))
    for batter in lineup:
        try:
            bid = int(getattr(getattr(batter, "player", None), "mlbam_id", 0) or 0)
        except Exception:
            bid = 0
        if bid <= 0:
            continue

        window_counts = window_by_batter.get(bid)
        career_counts = career_by_batter.get(bid)
        if window_counts is None and career_counts is None:
            continue
        window_metrics = (
            _metrics_from_counts(
                counts=window_counts,
                batter=batter,
                shrink_pa=float(shrink_pa),
                clamp_lo=float(clamp_lo),
                clamp_hi=float(clamp_hi),
            )
            if window_counts is not None and int(window_counts.pa) > 0
            else None
        )
        career_metrics = (
            _metrics_from_counts(
                counts=career_counts,
                batter=batter,
                shrink_pa=float(shrink_pa),
                clamp_lo=float(clamp_lo),
                clamp_hi=float(clamp_hi),
            )
            if career_counts is not None and int(career_counts.pa) > 0
            else None
        )
        effective_metrics = None
        if window_counts is not None and int(window_counts.pa) >= min_pa_i:
            effective_metrics = window_metrics
        elif career_counts is not None and int(career_counts.pa) >= min_pa_i:
            effective_metrics = career_metrics
        try:
            history = dict(effective_metrics or window_metrics or career_metrics or {})
            if window_metrics is not None:
                for key, value in window_metrics.items():
                    history[f"window_{key}"] = float(value)
            if career_metrics is not None:
                for key, value in career_metrics.items():
                    history[f"career_{key}"] = float(value)
            if history:
                batter.vs_pitcher_history[int(pid)] = history
            if effective_metrics is not None:
                batter.vs_pitcher_hr_mult[int(pid)] = float(effective_metrics.get("hr_mult") or 1.0)
                batter.vs_pitcher_k_mult[int(pid)] = float(effective_metrics.get("k_mult") or 1.0)
                batter.vs_pitcher_bb_mult[int(pid)] = float(effective_metrics.get("bb_mult") or 1.0)
                batter.vs_pitcher_inplay_mult[int(pid)] = float(effective_metrics.get("inplay_mult") or 1.0)
                applied += 1
        except Exception:
            pass

    return applied
