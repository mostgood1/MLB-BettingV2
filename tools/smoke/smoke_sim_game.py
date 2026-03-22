from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import (
    StatsApiClient,
    extract_team_pitcher_pitches_thrown,
    fetch_game_context,
    fetch_game_feed_live,
    fetch_schedule_for_date,
    load_feed_live_from_raw,
    parse_confirmed_lineup_ids,
)
from sim_engine.data.build_roster import build_team, build_team_roster
from sim_engine.data.statcast_pitch_splits import default_statcast_cache
from sim_engine.models import GameConfig
from sim_engine.simulate import simulate_game
from sim_engine.pitch_model import PitchModelConfig


def _abbr(team_obj: dict) -> str:
    return (team_obj.get("abbreviation") or team_obj.get("teamName") or team_obj.get("name") or "UNK")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    ap.add_argument("--season", type=int, default=datetime.now().year)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--game-index", type=int, default=0)
    ap.add_argument("--statcast-starter-splits", choices=["off", "starter"], default="starter")
    ap.add_argument("--statcast-cache-ttl-hours", type=int, default=24 * 14)
    ap.add_argument(
        "--statcast-x64-prefetch",
        choices=["off", "auto", "force"],
        default="off",
        help="Optionally run the x64 pybaseball helper to populate cached Statcast splits before simming.",
    )
    ap.add_argument(
        "--statcast-x64-python",
        default="",
        help="Override path to x64 python.exe (defaults to .venv_x64/Scripts/python.exe)",
    )
    args = ap.parse_args()

    client = StatsApiClient.with_default_cache(ttl_seconds=24 * 3600)

    # Optional x64 prefetch (cache population) step.
    if args.statcast_starter_splits != "off" and args.statcast_x64_prefetch != "off":
        snapshot_dir = _ROOT / "data" / "daily" / "snapshots" / str(args.date)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        report_path = snapshot_dir / "statcast_fetch_report.json"
        ttl_hours = int(args.statcast_cache_ttl_hours or (24 * 14))

        should_run_helper = args.statcast_x64_prefetch == "force"
        if args.statcast_x64_prefetch == "auto":
            should_run_helper = True

        if args.statcast_x64_prefetch == "auto" and report_path.exists():
            try:
                age_sec = max(0.0, (datetime.now().timestamp() - report_path.stat().st_mtime))
                if age_sec < float(ttl_hours * 3600):
                    print(f"Statcast x64 prefetch: skip (fresh report: {report_path})")
                    should_run_helper = False
            except Exception:
                pass

        # If auto didn't skip (or force), run helper.
        if should_run_helper:
            x64_py = (args.statcast_x64_python or "").strip()
            if not x64_py:
                x64_py = str(_ROOT / ".venv_x64" / "Scripts" / "python.exe")
            if not Path(x64_py).exists():
                msg = f"Statcast x64 prefetch: missing x64 python at {x64_py}"
                if args.statcast_x64_prefetch == "force":
                    raise RuntimeError(msg)
                else:
                    print(msg)
            else:
                tool = _ROOT / "tools" / "statcast" / "fetch_pitcher_pitch_splits_x64.py"
                cmd = [
                    x64_py,
                    str(tool),
                    "--date",
                    str(args.date),
                    "--season",
                    str(int(args.season)),
                    "--out-report",
                    str(report_path),
                ]
                print("Statcast x64 prefetch: running helper...")
                r = subprocess.run(cmd, check=False)
                if r.returncode != 0:
                    if args.statcast_x64_prefetch == "force":
                        raise RuntimeError(f"Statcast x64 helper failed with exit code {r.returncode}")
                    print(f"Statcast x64 prefetch: helper failed (exit {r.returncode}); continuing")

    statcast_cache = None
    statcast_ttl_seconds = None
    if args.statcast_starter_splits != "off":
        statcast_ttl_seconds = int(args.statcast_cache_ttl_hours * 3600)
        statcast_cache = default_statcast_cache(ttl_seconds=statcast_ttl_seconds)

    games = fetch_schedule_for_date(client, args.date)
    if not games:
        print(f"No games found for {args.date}")
        return 2
    g = games[min(max(args.game_index, 0), len(games) - 1)]

    dh = g.get("doubleHeader")
    gn = g.get("gameNumber")
    if dh and str(dh).strip() not in ("", "N", "n", "0"):
        print(f"Schedule: doubleHeader={dh} gameNumber={gn}")

    game_pk = g.get("gamePk")
    away_lineup_ids = []
    home_lineup_ids = []
    if game_pk:
        try:
            feed = fetch_game_feed_live(client, int(game_pk))
            away_lineup_ids = parse_confirmed_lineup_ids(feed, "away")
            home_lineup_ids = parse_confirmed_lineup_ids(feed, "home")
            if away_lineup_ids:
                print(f"Away confirmed lineup: {away_lineup_ids}")
            if home_lineup_ids:
                print(f"Home confirmed lineup: {home_lineup_ids}")
        except Exception:
            pass
    weather, park, umpire = fetch_game_context(client, int(game_pk)) if game_pk else (None, None, None)
    if weather is not None:
        wm = weather.multipliers()
        print(
            "Weather: "
            f"temp_f={weather.temperature_f if weather.temperature_f is not None else '-'} "
            f"wind={weather.wind_raw or '-'} "
            f"cond={weather.condition or '-'} "
            f"dome={weather.is_dome if weather.is_dome is not None else '-'} "
            f"mult(hr={wm.hr_mult:.3f} inplay={wm.inplay_hit_mult:.3f} xb={wm.xb_share_mult:.3f})"
        )
    if park is not None:
        pm = park.multipliers()
        dims = f"{park.left_line if park.left_line is not None else '-'}|{park.center if park.center is not None else '-'}|{park.right_line if park.right_line is not None else '-'}"
        print(
            "Park: "
            f"venue={park.venue_name or '-'} "
            f"roof={park.roof_type or '-'}:{park.roof_status or '-'} "
            f"dims(L|C|R)={dims} "
            f"mult(hr={pm.hr_mult:.3f} inplay={pm.inplay_hit_mult:.3f} xb={pm.xb_share_mult:.3f})"
        )
    if umpire is not None:
        um = umpire.multipliers()
        print(
            "Umpire: "
            f"hp={umpire.home_plate_umpire_name or '-'} "
            f"id={umpire.home_plate_umpire_id if umpire.home_plate_umpire_id is not None else '-'} "
            f"mult(called_strike={um.called_strike_mult:.3f})"
        )

    away = (g.get("teams") or {}).get("away") or {}
    home = (g.get("teams") or {}).get("home") or {}
    away_team = away.get("team") or {}
    home_team = home.get("team") or {}
    away_id = int(away_team.get("id"))
    home_id = int(home_team.get("id"))

    away_prob = ((away.get("probablePitcher") or {}).get("id"))
    home_prob = ((home.get("probablePitcher") or {}).get("id"))

    t_away = build_team(away_id, away_team.get("name") or "Away", _abbr(away_team))
    t_home = build_team(home_id, home_team.get("name") or "Home", _abbr(home_team))

    # Bullpen availability (best-effort) from recent raw feed/live.
    pitcher_availability_by_team = {away_id: {}, home_id: {}}
    try:
        today = datetime.strptime(str(args.date), "%Y-%m-%d").date()
        weights = {1: 1.0, 2: 0.6, 3: 0.4}
        pitches_by_team_day = {away_id: {}, home_id: {}}
        pitched_days_by_team_pitcher = {away_id: {}, home_id: {}}

        for days_ago in (1, 2, 3):
            d = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            sched = fetch_schedule_for_date(client, d)
            for sg in sched or []:
                try:
                    pk = int(sg.get("gamePk") or 0)
                except Exception:
                    pk = 0
                if pk <= 0:
                    continue
                sa = (sg.get("teams") or {}).get("away") or {}
                sh = (sg.get("teams") or {}).get("home") or {}
                ta = (sa.get("team") or {})
                th = (sh.get("team") or {})
                try:
                    aid = int(ta.get("id") or 0)
                    hid = int(th.get("id") or 0)
                except Exception:
                    continue
                if aid not in (away_id, home_id) and hid not in (away_id, home_id):
                    continue

                feed_prev = load_feed_live_from_raw(int(args.season), d, pk)
                if not isinstance(feed_prev, dict) or not feed_prev:
                    continue

                for tid in (aid, hid):
                    if tid not in (away_id, home_id):
                        continue
                    pitches = extract_team_pitcher_pitches_thrown(feed_prev, tid)
                    if not pitches:
                        continue
                    day_map = pitches_by_team_day[tid].setdefault(days_ago, {})
                    pitched_days = pitched_days_by_team_pitcher[tid]
                    for pid, pth in pitches.items():
                        day_map[pid] = int(day_map.get(pid, 0) + int(pth or 0))
                        if int(pth or 0) > 0:
                            pitched_days.setdefault(pid, set()).add(int(days_ago))

        for tid in (away_id, home_id):
            weighted = {}
            for days_ago, pitch_map in (pitches_by_team_day.get(tid) or {}).items():
                w = float(weights.get(int(days_ago), 0.0))
                if w <= 0:
                    continue
                for pid, pth in (pitch_map or {}).items():
                    weighted[int(pid)] = float(weighted.get(int(pid), 0.0)) + w * float(pth or 0.0)

            avail_map = {}
            pitched_days = pitched_days_by_team_pitcher.get(tid, {})
            for pid, wp in weighted.items():
                avail = max(0.35, 1.0 - (float(wp) / 120.0))
                days = pitched_days.get(int(pid), set())
                if 1 in days and 2 in days and 3 in days:
                    avail *= 0.75
                elif 1 in days and 2 in days:
                    avail *= 0.85
                avail_map[int(pid)] = float(max(0.25, min(1.0, avail)))
            pitcher_availability_by_team[int(tid)] = avail_map
    except Exception:
        pass

    print(f"Building rosters for {t_away.abbreviation} @ {t_home.abbreviation} ({args.date})")
    away_roster = build_team_roster(
        client,
        t_away,
        args.season,
        probable_pitcher_id=int(away_prob) if away_prob else None,
        statcast_cache=statcast_cache,
        statcast_ttl_seconds=statcast_ttl_seconds,
        confirmed_lineup_ids=away_lineup_ids,
        pitcher_availability=pitcher_availability_by_team.get(int(away_id), {}),
    )
    home_roster = build_team_roster(
        client,
        t_home,
        args.season,
        probable_pitcher_id=int(home_prob) if home_prob else None,
        statcast_cache=statcast_cache,
        statcast_ttl_seconds=statcast_ttl_seconds,
        confirmed_lineup_ids=home_lineup_ids,
        pitcher_availability=pitcher_availability_by_team.get(int(home_id), {}),
    )

    cfg = GameConfig(rng_seed=args.seed, weather=weather, park=park, umpire=umpire)
    pm = PitchModelConfig()
    print(f"Pitch model: {pm.name}")
    asp = away_roster.lineup.pitcher
    hsp = home_roster.lineup.pitcher
    print(f"Away starter arsenal source: {asp.arsenal_source} (n={asp.arsenal_sample_size})")
    print(f"Home starter arsenal source: {hsp.arsenal_source} (n={hsp.arsenal_sample_size})")
    print(
        f"Away starter Statcast splits: "
        f"{'yes' if (asp.statcast_splits_n_pitches or 0) > 0 else 'no'} "
        f"(src={asp.statcast_splits_source or '-'} n={int(asp.statcast_splits_n_pitches or 0)})"
    )
    print(
        f"Home starter Statcast splits: "
        f"{'yes' if (hsp.statcast_splits_n_pitches or 0) > 0 else 'no'} "
        f"(src={hsp.statcast_splits_source or '-'} n={int(hsp.statcast_splits_n_pitches or 0)})"
    )
    result = simulate_game(away_roster, home_roster, cfg)
    print(f"Final: {result.away_team.abbreviation} {result.away_score} - {result.home_team.abbreviation} {result.home_score} ({result.innings_played} inn)")

    # Show a couple prop-relevant lines
    away_sp = away_roster.lineup.pitcher.player.mlbam_id
    home_sp = home_roster.lineup.pitcher.player.mlbam_id
    aps = result.pitcher_stats.get(away_sp, {})
    hps = result.pitcher_stats.get(home_sp, {})
    print(f"Away SP BF={int(aps.get('BF',0))} P={int(aps.get('P',0))} SO={int(aps.get('SO',0))} BB={int(aps.get('BB',0))} H={int(aps.get('H',0))} R={int(aps.get('R',0))}")
    print(f"Home SP BF={int(hps.get('BF',0))} P={int(hps.get('P',0))} SO={int(hps.get('SO',0))} BB={int(hps.get('BB',0))} H={int(hps.get('H',0))} R={int(hps.get('R',0))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
