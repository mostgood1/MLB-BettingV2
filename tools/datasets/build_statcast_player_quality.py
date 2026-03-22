from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

# Ensure the project root (MLB-BettingV2/) is used for relative paths when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _safe_int(x: Any) -> int:
    try:
        if x is None:
            return 0
        s = str(x).strip()
        if not s:
            return 0
        return int(float(s))
    except Exception:
        return 0


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s or s.upper() in ("NA", "NAN", "NONE"):
            return None
        return float(s)
    except Exception:
        return None


def _safe_date(x: Any) -> Optional[date]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if len(s) >= 10:
            s = s[:10]
        return date.fromisoformat(s)
    except Exception:
        return None


SWING_DESCS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "foul_bunt",
    "missed_bunt",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}

WHIFF_DESCS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "missed_bunt",
}

INPLAY_DESCS = {
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}


@dataclass
class Acc:
    pitches: int = 0
    swings: int = 0
    whiffs: int = 0
    bip: int = 0
    bip_ev: int = 0
    hardhit: int = 0
    barrels: int = 0
    hr: int = 0
    xba_sum: float = 0.0
    xba_n: int = 0


def _iter_statcast_files(raw_root: Path, season: int) -> Iterable[Path]:
    base = raw_root / str(int(season))
    if not base.exists():
        return
    for month_dir in sorted([p for p in base.iterdir() if p.is_dir()], key=lambda p: p.name):
        for f in sorted(month_dir.glob("*.csv.gz")):
            yield f


def _read_rows(path: Path) -> Iterable[Dict[str, str]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if isinstance(row, dict):
                yield row


def _rate(num: int, denom: int) -> Optional[float]:
    if denom <= 0:
        return None
    return float(num) / float(denom)


def _mean(sum_x: float, n: int) -> Optional[float]:
    if n <= 0:
        return None
    return float(sum_x) / float(n)


def _mult_from_ratio(r: Optional[float], baseline: Optional[float], lo: float, hi: float, power: float = 0.6) -> float:
    if r is None or baseline is None or baseline <= 0:
        return 1.0
    x = max(1e-9, float(r) / float(baseline))
    return _clamp(x**power, lo, hi)


def _log_ratio(r: Optional[float], baseline: Optional[float]) -> float:
    if r is None or baseline is None or baseline <= 0 or r <= 0:
        return 0.0
    return math.log(float(r) / float(baseline))


def _combine_power_log(log_terms: Tuple[Tuple[float, float], ...], lo: float, hi: float) -> float:
    # log_terms: ((weight, log_ratio), ...)
    s = 0.0
    wsum = 0.0
    for w, lr in log_terms:
        if w <= 0:
            continue
        s += float(w) * float(lr)
        wsum += float(w)
    if wsum <= 0:
        return 1.0
    # soft power by shrinking logs
    m = math.exp(0.6 * (s / wsum))
    return _clamp(m, lo, hi)


def build_quality(
    raw_root: Path,
    season: int,
    start_date: date,
    end_date: date,
    min_pitches_pitcher: int,
    min_pitches_batter: int,
    min_bip_ev: int,
) -> Dict[str, Any]:
    pitchers: Dict[int, Acc] = {}
    batters: Dict[int, Acc] = {}
    league_p = Acc()
    league_b = Acc()

    for f in _iter_statcast_files(raw_root, season):
        try:
            for row in _read_rows(f):
                gd = _safe_date(row.get("game_date"))
                if gd is None or gd < start_date or gd > end_date:
                    continue

                pid = _safe_int(row.get("pitcher"))
                bid = _safe_int(row.get("batter"))
                if pid <= 0 and bid <= 0:
                    continue

                desc = str(row.get("description") or "").strip().lower()
                typ = str(row.get("type") or "").strip().upper()
                events = str(row.get("events") or "").strip().lower()

                swing = desc in SWING_DESCS
                whiff = desc in WHIFF_DESCS
                bip = (typ == "X") or (desc in INPLAY_DESCS)

                launch_speed = _safe_float(row.get("launch_speed"))
                barrel_v = row.get("barrel")
                barrel = False
                if barrel_v is not None:
                    try:
                        barrel = int(float(str(barrel_v).strip() or "0")) == 1
                    except Exception:
                        barrel = False

                # xBA (estimated BA using speed/angle)
                xba = _safe_float(row.get("estimated_ba_using_speedangle"))

                # Pitcher acc
                if pid > 0:
                    a = pitchers.setdefault(pid, Acc())
                    a.pitches += 1
                    league_p.pitches += 1
                    if swing:
                        a.swings += 1
                        league_p.swings += 1
                    if whiff:
                        a.whiffs += 1
                        league_p.whiffs += 1
                    if bip:
                        a.bip += 1
                        league_p.bip += 1
                        if isinstance(launch_speed, (int, float)):
                            a.bip_ev += 1
                            league_p.bip_ev += 1
                            if float(launch_speed) >= 95.0:
                                a.hardhit += 1
                                league_p.hardhit += 1
                            if barrel:
                                a.barrels += 1
                                league_p.barrels += 1
                        if xba is not None:
                            a.xba_sum += float(xba)
                            a.xba_n += 1
                            league_p.xba_sum += float(xba)
                            league_p.xba_n += 1
                        if events == "home_run":
                            a.hr += 1
                            league_p.hr += 1

                # Batter acc
                if bid > 0:
                    b = batters.setdefault(bid, Acc())
                    b.pitches += 1
                    league_b.pitches += 1
                    if swing:
                        b.swings += 1
                        league_b.swings += 1
                    if whiff:
                        b.whiffs += 1
                        league_b.whiffs += 1
                    if bip:
                        b.bip += 1
                        league_b.bip += 1
                        if isinstance(launch_speed, (int, float)):
                            b.bip_ev += 1
                            league_b.bip_ev += 1
                            if float(launch_speed) >= 95.0:
                                b.hardhit += 1
                                league_b.hardhit += 1
                            if barrel:
                                b.barrels += 1
                                league_b.barrels += 1
                        if xba is not None:
                            b.xba_sum += float(xba)
                            b.xba_n += 1
                            league_b.xba_sum += float(xba)
                            league_b.xba_n += 1
                        if events == "home_run":
                            b.hr += 1
                            league_b.hr += 1
        except Exception:
            # skip unreadable files
            continue

    # League baselines
    league_p_whiff = _rate(league_p.whiffs, league_p.swings)
    league_p_barrel = _rate(league_p.barrels, league_p.bip_ev)
    league_p_hardhit = _rate(league_p.hardhit, league_p.bip_ev)
    league_p_hr_bip = _rate(league_p.hr, league_p.bip)
    league_p_xba = _mean(league_p.xba_sum, league_p.xba_n)

    league_b_whiff = _rate(league_b.whiffs, league_b.swings)
    league_b_barrel = _rate(league_b.barrels, league_b.bip_ev)
    league_b_hardhit = _rate(league_b.hardhit, league_b.bip_ev)
    league_b_hr_bip = _rate(league_b.hr, league_b.bip)
    league_b_xba = _mean(league_b.xba_sum, league_b.xba_n)

    def pitcher_entry(pid: int, a: Acc) -> Optional[Dict[str, Any]]:
        if a.pitches < min_pitches_pitcher:
            return None
        whiff = _rate(a.whiffs, a.swings)
        barrel = _rate(a.barrels, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        hardhit = _rate(a.hardhit, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        hr_bip = _rate(a.hr, a.bip) if a.bip >= max(1, min_bip_ev) else None
        xba = _mean(a.xba_sum, a.xba_n) if a.xba_n >= min_bip_ev else None

        k_mult = _mult_from_ratio(whiff, league_p_whiff, 0.85, 1.15)
        inplay_mult = _mult_from_ratio(xba, league_p_xba, 0.90, 1.10)
        hr_mult = _combine_power_log(
            (
                (0.55, _log_ratio(barrel, league_p_barrel)),
                (0.25, _log_ratio(hardhit, league_p_hardhit)),
                (0.20, _log_ratio(hr_bip, league_p_hr_bip)),
            ),
            0.85,
            1.25,
        )

        mult = {"k": float(k_mult), "bb": 1.0, "hr": float(hr_mult), "inplay": float(inplay_mult)}
        return {
            "id": int(pid),
            "pitches": int(a.pitches),
            "swings": int(a.swings),
            "whiff_rate": whiff,
            "bip": int(a.bip),
            "bip_ev": int(a.bip_ev),
            "hardhit_rate": hardhit,
            "barrel_rate": barrel,
            "hr_per_bip": hr_bip,
            "xba": xba,
            "mult": mult,
        }

    def batter_entry(bid: int, a: Acc) -> Optional[Dict[str, Any]]:
        if a.pitches < min_pitches_batter:
            return None
        whiff = _rate(a.whiffs, a.swings)
        barrel = _rate(a.barrels, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        hardhit = _rate(a.hardhit, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        hr_bip = _rate(a.hr, a.bip) if a.bip >= max(1, min_bip_ev) else None
        xba = _mean(a.xba_sum, a.xba_n) if a.xba_n >= min_bip_ev else None

        # batter whiff increases K (inverse: more whiff => higher K)
        k_mult = _mult_from_ratio(whiff, league_b_whiff, 0.85, 1.15)
        inplay_mult = _mult_from_ratio(xba, league_b_xba, 0.90, 1.10)
        hr_mult = _combine_power_log(
            (
                (0.55, _log_ratio(barrel, league_b_barrel)),
                (0.25, _log_ratio(hardhit, league_b_hardhit)),
                (0.20, _log_ratio(hr_bip, league_b_hr_bip)),
            ),
            0.85,
            1.25,
        )

        mult = {"k": float(k_mult), "bb": 1.0, "hr": float(hr_mult), "inplay": float(inplay_mult)}
        return {
            "id": int(bid),
            "pitches": int(a.pitches),
            "swings": int(a.swings),
            "whiff_rate": whiff,
            "bip": int(a.bip),
            "bip_ev": int(a.bip_ev),
            "hardhit_rate": hardhit,
            "barrel_rate": barrel,
            "hr_per_bip": hr_bip,
            "xba": xba,
            "mult": mult,
        }

    out_pitchers: Dict[str, Any] = {}
    for pid, a in pitchers.items():
        ent = pitcher_entry(pid, a)
        if ent is not None:
            out_pitchers[str(pid)] = ent

    out_batters: Dict[str, Any] = {}
    for bid, a in batters.items():
        ent = batter_entry(bid, a)
        if ent is not None:
            out_batters[str(bid)] = ent

    return {
        "meta": {
            "season": int(season),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "min_pitches_pitcher": int(min_pitches_pitcher),
            "min_pitches_batter": int(min_pitches_batter),
            "min_bip_ev": int(min_bip_ev),
            "league_pitcher": {
                "whiff_rate": league_p_whiff,
                "barrel_rate": league_p_barrel,
                "hardhit_rate": league_p_hardhit,
                "hr_per_bip": league_p_hr_bip,
                "xba": league_p_xba,
            },
            "league_batter": {
                "whiff_rate": league_b_whiff,
                "barrel_rate": league_b_barrel,
                "hardhit_rate": league_b_hardhit,
                "hr_per_bip": league_b_hr_bip,
                "xba": league_b_xba,
            },
        },
        "pitchers": out_pitchers,
        "batters": out_batters,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build Statcast-derived player quality multipliers")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--raw-root", default=str(_ROOT / "data" / "raw" / "statcast" / "pitches"))
    ap.add_argument("--out", default="")
    ap.add_argument("--write-latest", action="store_true")
    ap.add_argument("--min-pitches-pitcher", type=int, default=350)
    ap.add_argument("--min-pitches-batter", type=int, default=350)
    ap.add_argument("--min-bip-ev", type=int, default=25)
    args = ap.parse_args()

    start_d = date.fromisoformat(str(args.start_date))
    end_d = date.fromisoformat(str(args.end_date))

    raw_root = Path(args.raw_root)
    out_dir = _ROOT / "data" / "statcast" / "quality"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = Path(args.out) if str(args.out).strip() else (out_dir / f"player_quality_{int(args.season)}_{start_d.isoformat()}_{end_d.isoformat()}.json")

    quality = build_quality(
        raw_root=raw_root,
        season=int(args.season),
        start_date=start_d,
        end_date=end_d,
        min_pitches_pitcher=int(args.min_pitches_pitcher),
        min_pitches_batter=int(args.min_pitches_batter),
        min_bip_ev=int(args.min_bip_ev),
    )

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(quality, indent=2), encoding="utf-8")
    tmp.replace(out_path)
    print(f"Wrote: {out_path}")

    # Stable path per-season
    stable = out_dir / f"player_quality_{int(args.season)}.json"
    tmp2 = stable.with_suffix(stable.suffix + ".tmp")
    tmp2.write_text(json.dumps(quality, indent=2), encoding="utf-8")
    tmp2.replace(stable)
    print(f"Wrote: {stable}")

    if args.write_latest:
        latest = out_dir / "player_quality_latest.json"
        tmp3 = latest.with_suffix(latest.suffix + ".tmp")
        tmp3.write_text(json.dumps(quality, indent=2), encoding="utf-8")
        tmp3.replace(latest)
        print(f"Wrote: {latest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
