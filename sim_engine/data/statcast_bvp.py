from __future__ import annotations

import csv
import gzip
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from .disk_cache import DiskCache


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

    parts = {
        "season": int(season),
        "pitcher_id": int(pid),
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
    }
    if cache is not None:
        hit = cache.get("statcast_bvp_pitcher", parts, ttl_seconds=ttl_seconds)
        if isinstance(hit, dict) and hit.get("by_batter"):
            out: Dict[int, BvPCounts] = {}
            by_b = hit.get("by_batter") or {}
            if isinstance(by_b, dict):
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
            if out:
                return out

    out_pa: Dict[int, int] = {}
    out_hr: Dict[int, int] = {}
    out_hits: Dict[int, int] = {}
    out_so: Dict[int, int] = {}
    out_bb: Dict[int, int] = {}
    out_hbp: Dict[int, int] = {}
    out_inplay_pa: Dict[int, int] = {}
    out_inplay_hits: Dict[int, int] = {}

    hit_events = {"single", "double", "triple", "home_run"}
    walk_events = {"walk", "intent_walk"}
    hbp_events = {"hit_by_pitch"}
    inplay_hit_events = {"single", "double", "triple"}

    for path in _iter_statcast_pitch_files(season, raw_root=raw_root):
        w = _file_window(path)
        if w is None:
            continue
        f0, f1 = w
        if not _overlaps(f0, f1, start_date, end_date):
            continue

        with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    if int(row.get("pitcher") or 0) != pid:
                        continue
                except Exception:
                    continue

                gd = row.get("game_date")
                if gd:
                    try:
                        d = _parse_date(gd)
                        if d < start_date or d > end_date:
                            continue
                    except Exception:
                        pass

                ev = (row.get("events") or "").strip().lower()
                if not ev:
                    continue

                try:
                    bid = int(row.get("batter") or 0)
                except Exception:
                    continue
                if bid <= 0:
                    continue

                out_pa[bid] = out_pa.get(bid, 0) + 1
                if ev in hit_events:
                    out_hits[bid] = out_hits.get(bid, 0) + 1
                if ev == "home_run":
                    out_hr[bid] = out_hr.get(bid, 0) + 1
                elif ev in walk_events:
                    out_bb[bid] = out_bb.get(bid, 0) + 1
                elif ev in hbp_events:
                    out_hbp[bid] = out_hbp.get(bid, 0) + 1
                elif ev.startswith("strikeout"):
                    out_so[bid] = out_so.get(bid, 0) + 1
                else:
                    out_inplay_pa[bid] = out_inplay_pa.get(bid, 0) + 1
                    if ev in inplay_hit_events:
                        out_inplay_hits[bid] = out_inplay_hits.get(bid, 0) + 1

    result: Dict[int, BvPCounts] = {
        bid: BvPCounts(
            pa=pa,
            hr=int(out_hr.get(bid, 0)),
            hits=int(out_hits.get(bid, 0)),
            so=int(out_so.get(bid, 0)),
            bb=int(out_bb.get(bid, 0)),
            hbp=int(out_hbp.get(bid, 0)),
            inplay_pa=int(out_inplay_pa.get(bid, 0)),
            inplay_hits=int(out_inplay_hits.get(bid, 0)),
        )
        for bid, pa in out_pa.items()
        if pa > 0
    }

    if cache is not None:
        cache.set(
            "statcast_bvp_pitcher",
            parts,
            {
                "by_batter": {
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
                },
            },
        )

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

    by_batter = pitcher_vs_batters_counts(
        season=int(season),
        pitcher_id=pid,
        start_date=start_date,
        end_date=end_date,
        cache=cache,
    )
    if not by_batter:
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

        counts = by_batter.get(bid)
        if counts is None:
            continue

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
        try:
            batter.vs_pitcher_history[int(pid)] = {
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
            if int(counts.pa) >= min_pa_i:
                batter.vs_pitcher_hr_mult[int(pid)] = float(hr_mult)
                batter.vs_pitcher_k_mult[int(pid)] = float(k_mult)
                batter.vs_pitcher_bb_mult[int(pid)] = float(bb_mult)
                batter.vs_pitcher_inplay_mult[int(pid)] = float(inplay_mult)
                applied += 1
        except Exception:
            pass

    return applied
