from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import StatsApiClient, fetch_game_context


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date) -> List[date]:
    out: List[date] = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def _fetch_schedule_range(client: StatsApiClient, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    data = client.get(
        "/schedule",
        params={
            "sportId": 1,
            "startDate": start_date,
            "endDate": end_date,
            "hydrate": "team",
        },
    )
    games: List[Dict[str, Any]] = []
    for d in data.get("dates", []) or []:
        for g in d.get("games", []) or []:
            games.append(g)
    return games


def _compute_called_strike_multipliers_from_statcast(
    start_date: str,
    end_date: str,
    *,
    pk_to_ump_name: Dict[int, str],
    chunk_days: int = 3,
    sleep_seconds: float = 0.5,
    max_consecutive_failures: int = 8,
) -> Tuple[float, Dict[str, Dict[str, float]]]:
    """Returns (league_called_strike_rate, per_ump_name stats).

    Robust to intermittent Statcast download failures by chunking requests.

    per_ump_name stats: { ump_name: {"called": x, "ball": y, "rate": r} }
    """
    try:
        from pybaseball import statcast  # type: ignore
        from pybaseball import cache as pyb_cache  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "pybaseball import failed. Run this with the Windows x64 venv (.venv_x64) where pybaseball is installed."
        ) from e

    # Strongly recommended by pybaseball; makes long runs recoverable.
    try:
        pyb_cache.enable()
    except Exception:
        pass

    start = _parse_ymd(start_date)
    end = _parse_ymd(end_date)
    chunk_days = max(1, int(chunk_days))

    called_desc = {"called_strike"}
    ball_desc = {"ball", "blocked_ball", "pitchout"}

    per: Dict[str, Dict[str, float]] = {}
    called_total = 0.0
    ball_total = 0.0
    consecutive_failures = 0

    d = start
    while d <= end:
        d2 = min(end, d + timedelta(days=chunk_days - 1))
        s1 = d.strftime("%Y-%m-%d")
        s2 = d2.strftime("%Y-%m-%d")
        try:
            df = statcast(s1, s2)
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            print(f"WARN: statcast fetch failed for {s1}..{s2}: {type(e).__name__}: {e}")
            if consecutive_failures >= int(max_consecutive_failures):
                raise
            if sleep_seconds > 0:
                import time

                time.sleep(float(sleep_seconds) * float(consecutive_failures))
            d = d2 + timedelta(days=1)
            continue

        if df is None or len(df) == 0:
            d = d2 + timedelta(days=1)
            continue
        if "description" not in df.columns:
            d = d2 + timedelta(days=1)
            continue
        if "game_pk" not in df.columns:
            d = d2 + timedelta(days=1)
            continue

        filt = df[df["description"].isin(called_desc.union(ball_desc))]
        if len(filt) == 0:
            d = d2 + timedelta(days=1)
            continue

        # Attribute Statcast pitch outcomes to home-plate umpire via game_pk -> StatsAPI live-feed mapping.
        # Note: pybaseball's 'umpire' column is often empty; game_pk is reliable.
        try:
            grp = (
                filt[["game_pk", "description"]]
                .groupby(["game_pk", "description"], dropna=True)
                .size()
                .unstack(fill_value=0)
            )
        except Exception:
            d = d2 + timedelta(days=1)
            continue

        for game_pk, row in grp.iterrows():
            try:
                pk_int = int(game_pk)
            except Exception:
                continue
            ump_name = pk_to_ump_name.get(pk_int)
            if not ump_name:
                continue

            called_n = float(row.get("called_strike", 0.0))
            ball_n = 0.0
            for b in ball_desc:
                if b in row.index:
                    ball_n += float(row.get(b, 0.0))
            if called_n + ball_n <= 0:
                continue

            dct = per.get(ump_name)
            if dct is None:
                dct = {"called": 0.0, "ball": 0.0}
                per[ump_name] = dct
            dct["called"] += called_n
            dct["ball"] += ball_n
            called_total += called_n
            ball_total += ball_n

        d = d2 + timedelta(days=1)

    denom = float(called_total + ball_total)
    league_rate = (float(called_total) / denom) if denom > 0 else 0.0
    for name, dct in per.items():
        n = float(dct.get("called", 0.0) + dct.get("ball", 0.0))
        dct["rate"] = (float(dct.get("called", 0.0)) / n) if n > 0 else 0.0

    return float(league_rate), per


def main() -> int:
    ap = argparse.ArgumentParser(description="Build per-umpire called-strike multipliers from Statcast (run under x64 Python)")
    ap.add_argument("--start-date", default="", help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end-date", default="", help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--date", default="", help="Convenience: end date (YYYY-MM-DD); used with --days-back")
    ap.add_argument("--days-back", type=int, default=21, help="If --date is set, window is [date-days_back+1, date]")
    ap.add_argument("--min-pitches", type=int, default=1500, help="Min (ball+called_strike) samples per umpire")
    ap.add_argument("--chunk-days", type=int, default=3, help="Statcast fetch chunk size in days (smaller is more robust)")
    ap.add_argument("--sleep-seconds", type=float, default=0.5, help="Backoff base sleep between failed Statcast chunks")
    ap.add_argument("--max-consecutive-failures", type=int, default=8, help="Abort after this many consecutive Statcast chunk failures")
    ap.add_argument("--out", default=str(_ROOT / "data" / "umpire" / "umpire_factors.json"))
    ap.add_argument("--merge", choices=["on", "off"], default="on")
    ap.add_argument("--out-report", default="")
    args = ap.parse_args()

    if args.start_date and args.end_date:
        start = _parse_ymd(args.start_date)
        end = _parse_ymd(args.end_date)
    else:
        end_s = args.date or datetime.now().strftime("%Y-%m-%d")
        end = _parse_ymd(end_s)
        start = end - timedelta(days=max(1, int(args.days_back)) - 1)

    start_s = start.strftime("%Y-%m-%d")
    end_s = end.strftime("%Y-%m-%d")

    client = StatsApiClient.with_default_cache(ttl_seconds=24 * 3600)
    games = _fetch_schedule_range(client, start_s, end_s)

    # Build mapping from live feeds
    id_to_name: Dict[int, str] = {}
    pk_to_ump_name: Dict[int, str] = {}
    pk_to_ump_id: Dict[int, int] = {}
    game_pks: List[int] = []
    for g in games:
        pk = g.get("gamePk")
        if not pk:
            continue
        try:
            game_pks.append(int(pk))
        except Exception:
            continue

    for pk in sorted(set(game_pks)):
        try:
            _, _, ump = fetch_game_context(client, int(pk))
            if ump.home_plate_umpire_id is None:
                continue
            uid = int(ump.home_plate_umpire_id)
            nm = str(ump.home_plate_umpire_name or "").strip()
            if uid > 0 and nm:
                id_to_name[uid] = nm
                pk_to_ump_name[int(pk)] = nm
                pk_to_ump_id[int(pk)] = uid
        except Exception:
            continue

    if not id_to_name:
        print(f"No home-plate umpire IDs found in feeds for {start_s}..{end_s}")

    print(f"Statcast window: {start_s}..{end_s}")
    print(f"Umpires discovered from feeds: {len(id_to_name)}")

    league_rate, per_ump = _compute_called_strike_multipliers_from_statcast(
        start_s,
        end_s,
        pk_to_ump_name=pk_to_ump_name,
        chunk_days=int(args.chunk_days),
        sleep_seconds=float(args.sleep_seconds),
        max_consecutive_failures=int(args.max_consecutive_failures),
    )
    if league_rate <= 1e-9:
        print("No Statcast called-strike/ball rows found")
        return 2

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    existing: Dict[str, Any] = {}
    if args.merge == "on" and out_path.exists():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
            if not isinstance(existing, dict):
                existing = {}
        except Exception:
            existing = {}

    computed: Dict[str, Any] = {}
    written = 0
    skipped_small = 0

    for uid, name in sorted(id_to_name.items()):
        stats = per_ump.get(name)
        if not stats:
            continue
        called = float(stats.get("called", 0.0))
        ball = float(stats.get("ball", 0.0))
        n = int(called + ball)
        if n < int(args.min_pitches):
            skipped_small += 1
            continue
        ump_rate = float(stats.get("rate", 0.0))
        mult = _clamp((ump_rate / league_rate) if league_rate > 1e-9 else 1.0, 0.92, 1.08)

        payload = {
            "called_strike_mult": float(mult),
            "n_called_strike": int(called),
            "n_ball": int(ball),
            "n_total": int(n),
            "league_called_strike_rate": float(league_rate),
            "ump_called_strike_rate": float(ump_rate),
            "start_date": start_s,
            "end_date": end_s,
            "generated_at": datetime.now().isoformat(),
            "source": "pybaseball_statcast+statsapi_feed_map",
        }

        # Write with both id and name keys for robustness.
        computed[str(uid)] = payload
        computed[name] = payload
        written += 1

    merged = dict(existing)
    for k, v in computed.items():
        merged[k] = v

    out_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")

    print(f"League called-strike rate: {league_rate:.4f}")
    print(f"Wrote/updated {written} umpires (min_pitches={int(args.min_pitches)}); skipped_small={skipped_small}")
    print(f"Out: {out_path}")

    report = {
        "start_date": start_s,
        "end_date": end_s,
        "min_pitches": int(args.min_pitches),
        "league_called_strike_rate": float(league_rate),
        "umpires_discovered": len(id_to_name),
        "umpires_written": int(written),
        "umpires_skipped_small": int(skipped_small),
        "out": str(out_path),
        "generated_at": datetime.now().isoformat(),
        "python": sys.version,
        "platform": sys.platform,
    }

    if args.out_report:
        rp = Path(args.out_report)
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Report: {rp}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
