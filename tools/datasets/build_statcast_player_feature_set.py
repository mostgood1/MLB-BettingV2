from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


# Ensure the project root (MLB-BettingV2/) is importable and used for relative paths.
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


# Statcast descriptions
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

BALL_DESCS = {
    "ball",
    "blocked_ball",
    "pitchout",
    "intent_ball",
}

CALLED_STRIKE_DESCS = {
    "called_strike",
}

FOUL_DESCS = {
    "foul",
    "foul_tip",
    "foul_bunt",
}


def _is_zone(zone: Optional[float]) -> Optional[bool]:
    if zone is None:
        return None
    try:
        z = int(zone)
    except Exception:
        return None
    # Statcast zone 1-9 = in zone; 11-14 = out of zone; 10 is sometimes "unknown"
    if 1 <= z <= 9:
        return True
    if 11 <= z <= 14:
        return False
    return None


def _bb_bucket(launch_angle: Optional[float]) -> Optional[str]:
    if launch_angle is None:
        return None
    la = float(launch_angle)
    if la < 10.0:
        return "gb"
    if la < 25.0:
        return "ld"
    if la < 50.0:
        return "fb"
    return "pu"


_SC_TO_CANON = {
    "FF": "FF",
    "FA": "FF",
    "FT": "SI",
    "SI": "SI",
    "FC": "FC",
    "SL": "SL",
    "CU": "CU",
    "KC": "KC",
    "CS": "CU",
    "CH": "CH",
    "FS": "FS",
    "FO": "CH",
    "KN": "KN",
}


def _canon_pitch_type(code: str) -> str:
    c = (code or "").strip().upper()
    return _SC_TO_CANON.get(c, "OTHER")


def _spray_angle_deg(hc_x: float, hc_y: float) -> Optional[float]:
    """Compute a rough spray angle (degrees) from Statcast hit coordinates.

    This is best-effort. Negative angles trend toward 3B/left-field; positive toward
    1B/right-field.
    """
    try:
        x = float(hc_x) - 125.42
        y = 198.27 - float(hc_y)
        if not math.isfinite(x) or not math.isfinite(y):
            return None
        if abs(x) < 1e-9 and abs(y) < 1e-9:
            return None
        return float(math.degrees(math.atan2(x, y)))
    except Exception:
        return None


def _is_pulled(stand: str, angle_deg: float, pull_cut: float = 15.0) -> bool:
    s = (stand or "").strip().upper()
    try:
        a = float(angle_deg)
    except Exception:
        return False
    # For RHB, pulled balls are typically to LF (negative angles).
    if s == "R":
        return a <= -abs(pull_cut)
    # For LHB, pulled balls are typically to RF (positive angles).
    if s == "L":
        return a >= abs(pull_cut)
    # Switch/unknown: treat strong-side angles as "pulled" either way.
    return abs(a) >= abs(pull_cut)


@dataclass
class Acc:
    pitches: int = 0
    swings: int = 0
    whiffs: int = 0
    fouls: int = 0
    balls: int = 0
    called_strikes: int = 0
    csw: int = 0

    zone_pitches: int = 0
    zone_swings: int = 0
    chase_swings: int = 0

    inplay: int = 0
    bip_ev: int = 0
    hardhit: int = 0
    barrels: int = 0
    hr: int = 0

    singles: int = 0
    doubles: int = 0
    triples: int = 0

    ev_sum: float = 0.0
    ev_n: int = 0
    ev_max: float = 0.0

    la_sum: float = 0.0
    la_n: int = 0

    sweet_spot: int = 0  # LA in [8, 32] on BIP with EV
    pull_air: int = 0  # pulled LD/FB on BIP with EV and valid hit coords

    xba_sum: float = 0.0
    xba_n: int = 0

    xwoba_sum: float = 0.0
    xwoba_n: int = 0

    gb: int = 0
    fb: int = 0
    ld: int = 0
    pu: int = 0

    # Pitch-quality continuous metrics (pitcher-side, by pitch type)
    velo_sum: float = 0.0
    velo_n: int = 0
    spin_sum: float = 0.0
    spin_n: int = 0
    pfx_x_sum: float = 0.0
    pfx_x_n: int = 0
    pfx_z_sum: float = 0.0
    pfx_z_n: int = 0
    ext_sum: float = 0.0
    ext_n: int = 0


def _rate(num: int, denom: int) -> Optional[float]:
    if denom <= 0:
        return None
    return float(num) / float(denom)


def _mean(sum_x: float, n: int) -> Optional[float]:
    if n <= 0:
        return None
    return float(sum_x) / float(n)


def _mult_ratio(r: Optional[float], base: Optional[float], lo: float, hi: float, power: float = 0.60) -> float:
    if r is None or base is None or base <= 0:
        return 1.0
    x = max(1e-9, float(r) / float(base))
    return _clamp(x**power, lo, hi)


def _log_ratio(r: Optional[float], base: Optional[float]) -> float:
    if r is None or base is None or base <= 0 or r <= 0:
        return 0.0
    return math.log(float(r) / float(base))


def _combine_power_log(log_terms: Tuple[Tuple[float, float], ...], lo: float, hi: float) -> float:
    s = 0.0
    wsum = 0.0
    for w, lr in log_terms:
        if w <= 0:
            continue
        s += float(w) * float(lr)
        wsum += float(w)
    if wsum <= 0:
        return 1.0
    m = math.exp(0.60 * (s / wsum))
    return _clamp(m, lo, hi)


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


def build_feature_set(
    raw_root: Path,
    season: int,
    start_date: date,
    end_date: date,
    min_pitches_pitcher: int,
    min_pitches_batter: int,
    min_pitches_pitch_type: int,
    min_bip_ev: int,
) -> Dict[str, Any]:
    # Overall
    pitchers: Dict[int, Acc] = {}
    batters: Dict[int, Acc] = {}

    # By pitch type
    pitchers_pt: Dict[Tuple[int, str], Acc] = {}
    batters_pt: Dict[Tuple[int, str], Acc] = {}

    league_overall_p = Acc()
    league_overall_b = Acc()
    league_pt = {pt: Acc() for pt in ["FF", "SI", "FC", "SL", "CU", "CH", "FS", "KC", "KN", "OTHER"]}

    rows = 0
    used = 0

    for f in _iter_statcast_files(raw_root, season):
        try:
            for row in _read_rows(f):
                rows += 1
                gd = _safe_date(row.get("game_date"))
                if gd is None or gd < start_date or gd > end_date:
                    continue

                pid = _safe_int(row.get("pitcher"))
                bid = _safe_int(row.get("batter"))
                if pid <= 0 and bid <= 0:
                    continue

                used += 1

                pt = _canon_pitch_type(str(row.get("pitch_type") or ""))

                desc = str(row.get("description") or "").strip().lower()
                typ = str(row.get("type") or "").strip().upper()
                events = str(row.get("events") or "").strip().lower()

                swing = desc in SWING_DESCS
                whiff = desc in WHIFF_DESCS
                foul = desc in FOUL_DESCS
                inplay = (typ == "X") or (desc in INPLAY_DESCS)
                ball = desc in BALL_DESCS
                called = desc in CALLED_STRIKE_DESCS
                csw = called or whiff

                zone = _safe_float(row.get("zone"))
                in_zone = _is_zone(zone)

                launch_speed = _safe_float(row.get("launch_speed"))
                launch_angle = _safe_float(row.get("launch_angle"))

                stand = str(row.get("stand") or "").strip().upper()
                hc_x = _safe_float(row.get("hc_x"))
                hc_y = _safe_float(row.get("hc_y"))

                barrel_v = row.get("barrel")
                barrel = False
                if barrel_v is not None:
                    try:
                        barrel = int(float(str(barrel_v).strip() or "0")) == 1
                    except Exception:
                        barrel = False

                xba = _safe_float(row.get("estimated_ba_using_speedangle"))
                xwoba = _safe_float(row.get("estimated_woba_using_speedangle"))

                # Pitch-quality
                release_speed = _safe_float(row.get("release_speed"))
                spin = _safe_float(row.get("release_spin_rate"))
                pfx_x = _safe_float(row.get("pfx_x"))
                pfx_z = _safe_float(row.get("pfx_z"))
                ext = _safe_float(row.get("release_extension"))

                # League pitch-type baselines from pitcher perspective (one set)
                lpt = league_pt.get(pt)
                if lpt is None:
                    lpt = league_pt["OTHER"]

                def bump(a: Acc) -> None:
                    a.pitches += 1
                    if swing:
                        a.swings += 1
                    if whiff:
                        a.whiffs += 1
                    if foul:
                        a.fouls += 1
                    if ball:
                        a.balls += 1
                    if called:
                        a.called_strikes += 1
                    if csw:
                        a.csw += 1

                    if in_zone is True:
                        a.zone_pitches += 1
                        if swing:
                            a.zone_swings += 1
                    elif in_zone is False:
                        if swing:
                            a.chase_swings += 1

                    if inplay:
                        a.inplay += 1
                        if isinstance(launch_speed, (int, float)):
                            a.bip_ev += 1
                            a.ev_sum += float(launch_speed)
                            a.ev_n += 1
                            if float(launch_speed) > float(a.ev_max or 0.0):
                                a.ev_max = float(launch_speed)
                            if float(launch_speed) >= 95.0:
                                a.hardhit += 1
                            if barrel:
                                a.barrels += 1
                        if isinstance(launch_angle, (int, float)):
                            a.la_sum += float(launch_angle)
                            a.la_n += 1
                            bb = _bb_bucket(launch_angle)
                            if bb == "gb":
                                a.gb += 1
                            elif bb == "ld":
                                a.ld += 1
                            elif bb == "fb":
                                a.fb += 1
                            elif bb == "pu":
                                a.pu += 1

                            # Contact-quality HR helpers (best-effort).
                            if isinstance(launch_speed, (int, float)):
                                la_f = float(launch_angle)
                                if 8.0 <= la_f <= 32.0:
                                    a.sweet_spot += 1

                                if bb in ("ld", "fb") and isinstance(hc_x, (int, float)) and isinstance(hc_y, (int, float)):
                                    ang = _spray_angle_deg(float(hc_x), float(hc_y))
                                    if ang is not None and _is_pulled(stand, ang, pull_cut=15.0):
                                        a.pull_air += 1
                        if xba is not None:
                            a.xba_sum += float(xba)
                            a.xba_n += 1
                        if xwoba is not None:
                            a.xwoba_sum += float(xwoba)
                            a.xwoba_n += 1
                        if events == "home_run":
                            a.hr += 1
                        elif events == "single":
                            a.singles += 1
                        elif events == "double":
                            a.doubles += 1
                        elif events == "triple":
                            a.triples += 1

                    if isinstance(release_speed, (int, float)):
                        a.velo_sum += float(release_speed)
                        a.velo_n += 1
                    if isinstance(spin, (int, float)):
                        a.spin_sum += float(spin)
                        a.spin_n += 1
                    if isinstance(pfx_x, (int, float)):
                        a.pfx_x_sum += float(pfx_x)
                        a.pfx_x_n += 1
                    if isinstance(pfx_z, (int, float)):
                        a.pfx_z_sum += float(pfx_z)
                        a.pfx_z_n += 1
                    if isinstance(ext, (int, float)):
                        a.ext_sum += float(ext)
                        a.ext_n += 1

                # Pitcher
                if pid > 0:
                    a = pitchers.setdefault(pid, Acc())
                    bump(a)
                    bump(league_overall_p)
                    bump(lpt)
                    ap = pitchers_pt.setdefault((pid, pt), Acc())
                    bump(ap)

                # Batter
                if bid > 0:
                    b = batters.setdefault(bid, Acc())
                    bump(b)
                    bump(league_overall_b)
                    bp = batters_pt.setdefault((bid, pt), Acc())
                    bump(bp)
        except Exception:
            continue

    # League baselines (overall)
    league_whiff = _rate(league_overall_p.whiffs, league_overall_p.swings)
    league_inplay = _rate(league_overall_p.inplay, league_overall_p.pitches)
    league_xba = _mean(league_overall_p.xba_sum, league_overall_p.xba_n)
    league_barrel = _rate(league_overall_p.barrels, league_overall_p.bip_ev)
    league_hardhit = _rate(league_overall_p.hardhit, league_overall_p.bip_ev)
    league_hr_bip = _rate(league_overall_p.hr, league_overall_p.inplay)
    league_sweet = _rate(league_overall_p.sweet_spot, league_overall_p.bip_ev)
    league_pull_air = _rate(league_overall_p.pull_air, league_overall_p.bip_ev)
    league_ev = _mean(league_overall_p.ev_sum, league_overall_p.ev_n)

    # League baselines by pitch type
    league_pt_baseline: Dict[str, Dict[str, Any]] = {}
    for pt, a in league_pt.items():
        wh = _rate(a.whiffs, a.swings)
        inp = _rate(a.inplay, a.pitches)
        # Contact rate (for batter pt mult)
        contact = None
        if a.swings > 0:
            contact = float(max(0, a.swings - a.whiffs)) / float(a.swings)
        league_pt_baseline[pt] = {
            "pitches": int(a.pitches),
            "swings": int(a.swings),
            "whiff_rate": wh,
            "inplay_rate": inp,
            "contact_rate": contact,
        }

    def summarize(a: Acc) -> Dict[str, Any]:
        wh = _rate(a.whiffs, a.swings)
        contact = None
        if a.swings > 0:
            contact = float(max(0, a.swings - a.whiffs)) / float(a.swings)
        inp = _rate(a.inplay, a.pitches)
        zone_rate = _rate(a.zone_pitches, a.pitches)
        chase = _rate(a.chase_swings, max(1, a.swings))
        csw_r = _rate(a.csw, a.pitches)

        hard = _rate(a.hardhit, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        barrel = _rate(a.barrels, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        hr_bip = _rate(a.hr, a.inplay) if a.inplay >= max(1, min_bip_ev) else None

        inplay_hits = int(a.singles + a.doubles + a.triples)
        inplay_hit_rate = (float(inplay_hits) / float(a.inplay)) if a.inplay >= max(1, min_bip_ev) else None
        hits_no_hr = float(max(0, inplay_hits))
        xb = float(max(0, int(a.doubles + a.triples)))
        xb_hit_share = (xb / hits_no_hr) if hits_no_hr > 0 else None
        triple_share_xb = (float(a.triples) / xb) if xb > 0 else None
        xba = _mean(a.xba_sum, a.xba_n) if a.xba_n >= min_bip_ev else None
        xwoba = _mean(a.xwoba_sum, a.xwoba_n) if a.xwoba_n >= min_bip_ev else None

        ev = _mean(a.ev_sum, a.ev_n) if a.ev_n >= min_bip_ev else None
        ev_max = float(a.ev_max) if a.ev_n >= min_bip_ev and float(a.ev_max or 0.0) > 0 else None
        la = _mean(a.la_sum, a.la_n) if a.la_n >= min_bip_ev else None

        sweet = _rate(a.sweet_spot, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        pull_air = _rate(a.pull_air, a.bip_ev) if a.bip_ev >= min_bip_ev else None

        bip = int(a.inplay)
        gb_r = float(a.gb) / float(bip) if bip > 0 else None
        fb_r = float(a.fb) / float(bip) if bip > 0 else None
        ld_r = float(a.ld) / float(bip) if bip > 0 else None
        pu_r = float(a.pu) / float(bip) if bip > 0 else None

        velo = _mean(a.velo_sum, a.velo_n) if a.velo_n >= min_pitches_pitch_type else None
        spin = _mean(a.spin_sum, a.spin_n) if a.spin_n >= min_pitches_pitch_type else None
        pfx_x = _mean(a.pfx_x_sum, a.pfx_x_n) if a.pfx_x_n >= min_pitches_pitch_type else None
        pfx_z = _mean(a.pfx_z_sum, a.pfx_z_n) if a.pfx_z_n >= min_pitches_pitch_type else None
        ext = _mean(a.ext_sum, a.ext_n) if a.ext_n >= min_pitches_pitch_type else None

        return {
            "pitches": int(a.pitches),
            "swings": int(a.swings),
            "whiff_rate": wh,
            "contact_rate": contact,
            "inplay_rate": inp,
            "csw_rate": csw_r,
            "zone_rate": zone_rate,
            "chase_swing_rate": chase,
            "inplay": int(a.inplay),
            "bip_ev": int(a.bip_ev),
            "ev_mean": ev,
            "ev_max": ev_max,
            "la_mean": la,
            "sweet_spot_rate": sweet,
            "pulled_air_rate": pull_air,
            "hardhit_rate": hard,
            "barrel_rate": barrel,
            "hr_per_bip": hr_bip,
            "inplay_hit_rate": inplay_hit_rate,
            "xb_hit_share": xb_hit_share,
            "triple_share_xb": triple_share_xb,
            "xba": xba,
            "xwoba": xwoba,
            "gb_rate": gb_r,
            "fb_rate": fb_r,
            "ld_rate": ld_r,
            "pu_rate": pu_r,
            "pitch_quality": {
                "velo_mean": velo,
                "spin_mean": spin,
                "pfx_x_mean": pfx_x,
                "pfx_z_mean": pfx_z,
                "extension_mean": ext,
            },
        }

    def overall_mult(a: Acc) -> Dict[str, float]:
        wh = _rate(a.whiffs, a.swings)
        xba = _mean(a.xba_sum, a.xba_n) if a.xba_n >= min_bip_ev else None
        barrel = _rate(a.barrels, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        hard = _rate(a.hardhit, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        hr_bip = _rate(a.hr, a.inplay) if a.inplay >= max(1, min_bip_ev) else None
        sweet = _rate(a.sweet_spot, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        pull_air = _rate(a.pull_air, a.bip_ev) if a.bip_ev >= min_bip_ev else None
        ev = _mean(a.ev_sum, a.ev_n) if a.ev_n >= min_bip_ev else None

        k_m = _mult_ratio(wh, league_whiff, 0.85, 1.15)
        inplay_m = _mult_ratio(xba, league_xba, 0.90, 1.10)
        hr_m = _combine_power_log(
            (
                (0.40, _log_ratio(barrel, league_barrel)),
                (0.18, _log_ratio(hard, league_hardhit)),
                (0.14, _log_ratio(hr_bip, league_hr_bip)),
                (0.10, _log_ratio(pull_air, league_pull_air)),
                (0.10, _log_ratio(sweet, league_sweet)),
                (0.08, _log_ratio(ev, league_ev)),
            ),
            0.85,
            1.25,
        )
        # bb left neutral for now (we can add zone/chase discipline later without destabilizing)
        return {"k": float(k_m), "bb": 1.0, "hr": float(hr_m), "inplay": float(inplay_m)}

    # Pitchers output
    out_pitchers: Dict[str, Any] = {}
    for pid, a in pitchers.items():
        if a.pitches < min_pitches_pitcher:
            continue

        # pitch mix
        mix_counts: Dict[str, int] = {}
        pt_payload: Dict[str, Any] = {}
        for (ppid, pt), acc in pitchers_pt.items():
            if ppid != pid:
                continue
            if acc.pitches < min_pitches_pitch_type:
                continue
            mix_counts[pt] = mix_counts.get(pt, 0) + int(acc.pitches)

        total = float(sum(mix_counts.values()))
        pitch_mix: Dict[str, float] = {}
        if total > 0:
            for pt, c in mix_counts.items():
                pitch_mix[pt] = float(c) / total

        for (ppid, pt), acc in pitchers_pt.items():
            if ppid != pid:
                continue
            if acc.pitches < min_pitches_pitch_type:
                continue

            base = league_pt_baseline.get(pt) or {}
            base_wh = base.get("whiff_rate")
            base_inp = base.get("inplay_rate")

            wh = _rate(acc.whiffs, acc.swings)
            inp = _rate(acc.inplay, acc.pitches)

            wh_m = _mult_ratio(wh, base_wh, 0.80, 1.25)
            inp_m = _mult_ratio(inp, base_inp, 0.80, 1.25)

            pt_payload[pt] = {
                "summary": summarize(acc),
                "whiff_mult": float(wh_m),
                "inplay_mult": float(inp_m),
            }

        out_pitchers[str(pid)] = {
            "id": int(pid),
            "overall": summarize(a),
            "mult_overall": overall_mult(a),
            "pitch_mix": pitch_mix,
            "pitch_type": pt_payload,
        }

    # Batters output
    out_batters: Dict[str, Any] = {}
    for bid, a in batters.items():
        if a.pitches < min_pitches_batter:
            continue

        vs_pt: Dict[str, float] = {}
        pt_payload: Dict[str, Any] = {}

        for (bbid, pt), acc in batters_pt.items():
            if bbid != bid:
                continue
            if acc.pitches < min_pitches_pitch_type:
                continue

            base = league_pt_baseline.get(pt) or {}
            base_contact = base.get("contact_rate")

            contact = None
            if acc.swings > 0:
                contact = float(max(0, acc.swings - acc.whiffs)) / float(acc.swings)

            # pt_mult > 1 => fewer whiffs + more balls in play in the pitch model.
            vs_pt[pt] = float(_mult_ratio(contact, base_contact, 0.85, 1.15, power=0.65))
            pt_payload[pt] = {
                "summary": summarize(acc),
            }

        out_batters[str(bid)] = {
            "id": int(bid),
            "overall": summarize(a),
            "mult_overall": overall_mult(a),
            "vs_pitch_type": vs_pt,
            "pitch_type": pt_payload,
        }

    return {
        "meta": {
            "season": int(season),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "min_pitches_pitcher": int(min_pitches_pitcher),
            "min_pitches_batter": int(min_pitches_batter),
            "min_pitches_pitch_type": int(min_pitches_pitch_type),
            "min_bip_ev": int(min_bip_ev),
            "rows": int(rows),
            "used_rows": int(used),
            "generated_at": datetime.now().isoformat(),
            "source": "statcast_raw",
        },
        "league": {
            "overall": {
                "pitcher": {
                    "whiff_rate": league_whiff,
                    "inplay_rate": league_inplay,
                    "xba": league_xba,
                    "barrel_rate": league_barrel,
                    "hardhit_rate": league_hardhit,
                    "hr_per_bip": league_hr_bip,
                    "ev_mean": league_ev,
                    "sweet_spot_rate": league_sweet,
                    "pulled_air_rate": league_pull_air,
                },
                "batter": {
                    "whiff_rate": _rate(league_overall_b.whiffs, league_overall_b.swings),
                },
            },
            "by_pitch_type": league_pt_baseline,
        },
        "pitchers": out_pitchers,
        "batters": out_batters,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build an expansive Statcast-derived player feature set (season window)")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--raw-root", default=str(_ROOT / "data" / "raw" / "statcast" / "pitches"))
    ap.add_argument("--out", default="")
    ap.add_argument("--write-latest", action="store_true")

    ap.add_argument("--min-pitches-pitcher", type=int, default=450)
    ap.add_argument("--min-pitches-batter", type=int, default=450)
    ap.add_argument("--min-pitches-pitch-type", type=int, default=60)
    ap.add_argument("--min-bip-ev", type=int, default=25)
    args = ap.parse_args()

    season = int(args.season)
    start_d = date.fromisoformat(str(args.start_date))
    end_d = date.fromisoformat(str(args.end_date))

    raw_root = Path(args.raw_root)
    out_dir = _ROOT / "data" / "statcast" / "features"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = Path(args.out) if str(args.out).strip() else (out_dir / f"player_features_{season}_{start_d.isoformat()}_{end_d.isoformat()}.json")

    feats = build_feature_set(
        raw_root=raw_root,
        season=season,
        start_date=start_d,
        end_date=end_d,
        min_pitches_pitcher=int(args.min_pitches_pitcher),
        min_pitches_batter=int(args.min_pitches_batter),
        min_pitches_pitch_type=int(args.min_pitches_pitch_type),
        min_bip_ev=int(args.min_bip_ev),
    )

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(feats, indent=2), encoding="utf-8")
    tmp.replace(out_path)
    print(f"Wrote: {out_path}")

    stable = out_dir / f"player_features_{season}.json"
    tmp2 = stable.with_suffix(stable.suffix + ".tmp")
    tmp2.write_text(json.dumps(feats, indent=2), encoding="utf-8")
    tmp2.replace(stable)
    print(f"Wrote: {stable}")

    if args.write_latest:
        latest = out_dir / "player_features_latest.json"
        tmp3 = latest.with_suffix(latest.suffix + ".tmp")
        tmp3.write_text(json.dumps(feats, indent=2), encoding="utf-8")
        tmp3.replace(latest)
        print(f"Wrote: {latest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
