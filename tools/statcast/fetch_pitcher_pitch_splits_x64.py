from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.disk_cache import DiskCache
from sim_engine.data.statsapi import StatsApiClient, fetch_schedule_for_date
from sim_engine.models import PitchType


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _canon(code: str) -> PitchType:
    code = (code or "").strip().upper()
    try:
        return PitchType(code)
    except Exception:
        if code in ("FT",):
            return PitchType.SI
        if code in ("FA",):
            return PitchType.FF
        return PitchType.OTHER


def _compute_splits_pybaseball(pitcher_id: int, season: int) -> Optional[dict]:
    """Compute pitch-mix + whiff/in-play multipliers from pybaseball statcast_pitcher.

    This file is intended to be run under a Windows x64 Python where `pybaseball`
    (and thus `cryptography`) can install via prebuilt wheels.
    """
    try:
        from pybaseball import statcast_pitcher  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "pybaseball import failed. Ensure you are running this tool with a Windows x64 Python/venv and have installed pybaseball."
        ) from e

    start_date = f"{season}-03-01"
    end_date = f"{season}-11-30"

    df = statcast_pitcher(start_date, end_date, int(pitcher_id))
    if df is None or len(df) < 200:
        return None

    if "pitch_type" not in df.columns or "description" not in df.columns:
        return None

    whiff_desc = {
        "swinging_strike",
        "swinging_strike_blocked",
        "missed_bunt",
    }
    inplay_desc = {
        "hit_into_play",
        "hit_into_play_no_out",
        "hit_into_play_score",
    }

    counts: Dict[PitchType, int] = {}
    whiffs: Dict[PitchType, int] = {}
    inplay: Dict[PitchType, int] = {}
    overall_whiffs = 0
    overall_inplay = 0

    for _, row in df[["pitch_type", "description"]].iterrows():
        pt = _canon(str(row["pitch_type"]))
        desc = str(row["description"]) if row["description"] is not None else ""
        counts[pt] = counts.get(pt, 0) + 1
        if desc in whiff_desc:
            whiffs[pt] = whiffs.get(pt, 0) + 1
            overall_whiffs += 1
        if desc in inplay_desc:
            inplay[pt] = inplay.get(pt, 0) + 1
            overall_inplay += 1

    total = sum(counts.values())
    if total <= 0:
        return None

    pitch_mix = {k: v / float(total) for k, v in counts.items()}

    overall_whiff_rate = overall_whiffs / float(total)
    overall_inplay_rate = overall_inplay / float(total)

    whiff_mult: Dict[PitchType, float] = {}
    inplay_mult: Dict[PitchType, float] = {}

    for pt, n in counts.items():
        if n <= 0:
            continue
        pt_whiff = float(whiffs.get(pt, 0)) / float(n)
        pt_inp = float(inplay.get(pt, 0)) / float(n)
        if overall_whiff_rate > 1e-9:
            whiff_mult[pt] = _clamp(pt_whiff / overall_whiff_rate, 0.65, 1.55)
        if overall_inplay_rate > 1e-9:
            inplay_mult[pt] = _clamp(pt_inp / overall_inplay_rate, 0.65, 1.55)

    return {
        "pitcher_id": int(pitcher_id),
        "season": int(season),
        "n_pitches": int(total),
        "pitch_mix": {k.value: float(v) for k, v in pitch_mix.items()},
        "whiff_mult": {k.value: float(v) for k, v in whiff_mult.items()},
        "inplay_mult": {k.value: float(v) for k, v in inplay_mult.items()},
        "source": "pybaseball_statcast_pitcher",
        "start_date": start_date,
        "end_date": end_date,
        "generated_at": datetime.now().isoformat(),
        "platform": sys.platform,
        "python": sys.version,
    }


def _default_cache_dir() -> Path:
    return _ROOT / "data" / "cache" / "statcast"


def main() -> int:
    ap = argparse.ArgumentParser(description="Populate V2 cached Statcast pitch splits (run under x64 Python)")
    ap.add_argument("--date", help="If set, pulls probable starters for this date via StatsAPI and fetches splits for them")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--pitcher-ids", help="Comma-separated MLBAM pitcher ids (overrides --date)")
    ap.add_argument("--cache-dir", default=str(_default_cache_dir()))
    ap.add_argument("--ttl-hours", type=int, default=24 * 14)
    ap.add_argument("--out-report", default="")
    args = ap.parse_args()

    cache = DiskCache(root_dir=Path(args.cache_dir), default_ttl_seconds=int(args.ttl_hours * 3600))

    pitcher_ids: List[int] = []
    if args.pitcher_ids:
        for part in args.pitcher_ids.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                pitcher_ids.append(int(part))
            except Exception:
                continue
    elif args.date:
        client = StatsApiClient.with_default_cache(ttl_seconds=24 * 3600)
        games = fetch_schedule_for_date(client, args.date)
        for g in games:
            away = (g.get("teams") or {}).get("away") or {}
            home = (g.get("teams") or {}).get("home") or {}
            apid = (away.get("probablePitcher") or {}).get("id")
            hpid = (home.get("probablePitcher") or {}).get("id")
            if apid:
                pitcher_ids.append(int(apid))
            if hpid:
                pitcher_ids.append(int(hpid))
    else:
        raise SystemExit("Provide either --pitcher-ids or --date")

    pitcher_ids = sorted({pid for pid in pitcher_ids if pid > 0})
    if not pitcher_ids:
        print("No pitcher ids found")
        return 2

    report = {
        "season": int(args.season),
        "count": len(pitcher_ids),
        "cache_dir": str(Path(args.cache_dir)),
        "generated_at": datetime.now().isoformat(),
        "pitchers": [],
    }

    ok = 0
    for i, pid in enumerate(pitcher_ids, start=1):
        print(f"[{i}/{len(pitcher_ids)}] pitcher_id={pid}")
        try:
            payload = _compute_splits_pybaseball(pid, args.season)
            if payload is None:
                report["pitchers"].append({"pitcher_id": pid, "status": "skipped_small_or_missing"})
                continue

            cache.set("pitcher_pitch_splits", {"pitcher_id": int(pid), "season": int(args.season)}, payload)
            report["pitchers"].append({"pitcher_id": pid, "status": "ok", "n_pitches": int(payload.get("n_pitches") or 0)})
            ok += 1
        except Exception as e:
            report["pitchers"].append({"pitcher_id": pid, "status": "error", "error": str(e)})

    print(f"Wrote cache for {ok}/{len(pitcher_ids)} pitchers")

    if args.out_report:
        outp = Path(args.out_report)
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Report: {outp}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
