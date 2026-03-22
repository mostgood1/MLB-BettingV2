from __future__ import annotations

import argparse
import csv
import gzip
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _load_gamepk_to_venue(feed_root: Path, season: int, start_date: str, end_date: str) -> Dict[int, Dict[str, Any]]:
    """Read raw feed/live gz files and build {gamePk: {venue_id, venue_name}}."""
    start = _parse_ymd(start_date)
    end = _parse_ymd(end_date)

    out: Dict[int, Dict[str, Any]] = {}
    cur = start
    while cur <= end:
        day = cur.strftime("%Y-%m-%d")
        day_dir = feed_root / str(int(season)) / day
        if day_dir.exists():
            for p in day_dir.glob("*.json.gz"):
                try:
                    game_pk = int(p.name.split(".")[0])
                except Exception:
                    continue
                try:
                    with gzip.open(p, "rt", encoding="utf-8") as f:
                        payload = json.load(f)
                    gd = payload.get("gameData") or {}
                    venue = gd.get("venue") or {}
                    vid = venue.get("id")
                    vname = venue.get("name")
                    if vid is None:
                        continue
                    out[game_pk] = {"venue_id": int(vid), "venue_name": str(vname or "")}
                except Exception:
                    continue
        cur = cur + timedelta(days=1)

    return out


def _iter_statcast_rows(statcast_root: Path) -> Iterable[Tuple[Path, Dict[str, str]]]:
    for p in sorted(statcast_root.rglob("*.csv.gz")):
        with gzip.open(p, "rt", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield p, row


def main() -> int:
    ap = argparse.ArgumentParser(description="Build Statcast-derived park multipliers and write data/park/park_factors.json")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument(
        "--feed-live-root",
        default=str(Path(__file__).resolve().parents[2] / "data" / "raw" / "statsapi" / "feed_live"),
    )
    ap.add_argument(
        "--statcast-root",
        default="",
        help="Root containing Statcast csv.gz partitions (defaults to data/raw/statcast/pitches/<season>/)",
    )
    ap.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[2] / "data" / "park" / "park_factors.json"),
    )
    ap.add_argument("--min-pa", type=int, default=4000)
    ap.add_argument("--merge", choices=["on", "off"], default="on")
    args = ap.parse_args()

    start = _parse_ymd(args.start_date)
    end = _parse_ymd(args.end_date)

    root = Path(__file__).resolve().parents[2]
    feed_root = Path(args.feed_live_root)
    statcast_root = Path(args.statcast_root) if args.statcast_root else (root / "data" / "raw" / "statcast" / "pitches" / str(int(args.season)))

    if not statcast_root.exists():
        raise SystemExit(f"Missing statcast_root: {statcast_root} (run the x64 backfill first)")

    gamepk_to_venue = _load_gamepk_to_venue(feed_root, int(args.season), args.start_date, args.end_date)
    if not gamepk_to_venue:
        print("Warning: no gamePk->venue mapping found in raw feed/live for this window")

    # Aggregates per venue
    agg: Dict[int, Dict[str, float]] = {}
    venue_name_by_id: Dict[int, str] = {}
    league = {"pa": 0.0, "hr": 0.0, "so": 0.0, "bb": 0.0, "hbp": 0.0, "inplay_opp": 0.0, "inplay_hits": 0.0, "xb_hits": 0.0}

    def bump(vid: int, key: str, inc: float = 1.0) -> None:
        d = agg.get(vid)
        if d is None:
            d = {"pa": 0.0, "hr": 0.0, "so": 0.0, "bb": 0.0, "hbp": 0.0, "inplay_opp": 0.0, "inplay_hits": 0.0, "xb_hits": 0.0}
            agg[vid] = d
        d[key] = d.get(key, 0.0) + float(inc)

    def in_window(s: str) -> bool:
        try:
            d = _parse_ymd(s)
        except Exception:
            return False
        return start <= d <= end

    # Events that terminate a PA in Statcast
    ev_bb = {"walk", "intent_walk"}
    ev_so = {"strikeout"}
    ev_hbp = {"hit_by_pitch"}

    ev_hit = {"single", "double", "triple", "home_run"}
    ev_xb = {"double", "triple"}

    rows = 0
    used = 0
    for _, row in _iter_statcast_rows(statcast_root):
        rows += 1
        gd = (row.get("game_date") or "").strip()
        if not gd or not in_window(gd):
            continue
        game_pk_s = (row.get("game_pk") or "").strip()
        events = (row.get("events") or "").strip()
        if not game_pk_s or not events:
            continue
        try:
            game_pk = int(float(game_pk_s))
        except Exception:
            continue

        venue = gamepk_to_venue.get(game_pk)
        if not venue:
            continue
        vid = int(venue.get("venue_id"))
        if vid not in venue_name_by_id:
            venue_name_by_id[vid] = str(venue.get("venue_name") or "")

        used += 1
        bump(vid, "pa", 1.0)
        league["pa"] += 1.0

        if events in ev_bb:
            bump(vid, "bb", 1.0)
            league["bb"] += 1.0
            continue
        if events in ev_hbp:
            bump(vid, "hbp", 1.0)
            league["hbp"] += 1.0
            continue
        if events in ev_so:
            bump(vid, "so", 1.0)
            league["so"] += 1.0
            continue

        # Ball in play
        if events == "home_run":
            bump(vid, "hr", 1.0)
            league["hr"] += 1.0
            continue

        bump(vid, "inplay_opp", 1.0)
        league["inplay_opp"] += 1.0

        if events in ev_hit:
            # (single/double/triple)
            bump(vid, "inplay_hits", 1.0)
            league["inplay_hits"] += 1.0
            if events in ev_xb:
                bump(vid, "xb_hits", 1.0)
                league["xb_hits"] += 1.0

    def rate(num: float, denom: float, default: float = 0.0) -> float:
        if denom <= 0:
            return default
        return float(num) / float(denom)

    league_hr_rate = rate(league["hr"], league["pa"], 0.0)
    league_inplay_hit_rate = rate(league["inplay_hits"], league["inplay_opp"], 0.0)
    league_xb_share = rate(league["xb_hits"], league["inplay_hits"], 0.0)

    out_path = Path(args.out)
    _ensure_dir(out_path.parent)

    existing: Dict[str, Any] = {}
    if args.merge == "on" and out_path.exists():
        try:
            raw = json.loads(out_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                existing = raw
        except Exception:
            existing = {}

    computed: Dict[str, Any] = {}
    written = 0
    for vid, d in agg.items():
        pa = int(d.get("pa", 0.0))
        if pa < int(args.min_pa):
            continue

        hr_rate = rate(d.get("hr", 0.0), d.get("pa", 0.0), 0.0)
        inp_rate = rate(d.get("inplay_hits", 0.0), d.get("inplay_opp", 0.0), 0.0)
        xb_share = rate(d.get("xb_hits", 0.0), d.get("inplay_hits", 0.0), league_xb_share)

        hr_mult = _clamp(hr_rate / league_hr_rate, 0.85, 1.15) if league_hr_rate > 1e-12 else 1.0
        inplay_mult = _clamp(inp_rate / league_inplay_hit_rate, 0.90, 1.10) if league_inplay_hit_rate > 1e-12 else 1.0
        xb_mult = _clamp(xb_share / league_xb_share, 0.92, 1.08) if league_xb_share > 1e-12 else 1.0

        payload = {
            "venue_id": int(vid),
            "venue_name": str(venue_name_by_id.get(int(vid), "") or ""),
            "start_date": args.start_date,
            "end_date": args.end_date,
            "pa": int(pa),
            "league": {
                "hr_rate": float(league_hr_rate),
                "inplay_hit_rate": float(league_inplay_hit_rate),
                "xb_share": float(league_xb_share),
            },
            "venue": {
                "hr_rate": float(hr_rate),
                "inplay_hit_rate": float(inp_rate),
                "xb_share": float(xb_share),
            },
            "multipliers": {
                "hr_mult": float(hr_mult),
                "inplay_hit_mult": float(inplay_mult),
                "xb_share_mult": float(xb_mult),
            },
            "generated_at": datetime.now().isoformat(),
            "source": "statcast_raw+statsapi_feed_live_raw",
        }

        # Write with both id and name keys for robustness
        computed[str(vid)] = payload["multipliers"]
        if payload.get("venue_name"):
            computed[str(payload["venue_name"])] = payload["multipliers"]

        written += 1

    merged = dict(existing)
    for k, v in computed.items():
        merged[k] = v

    out_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")

    print(f"Processed statcast rows={rows} used_pa_rows={used} venues={len(agg)} wrote={written}")
    print(f"Out: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
