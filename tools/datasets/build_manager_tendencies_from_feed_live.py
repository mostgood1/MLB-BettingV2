from __future__ import annotations

import argparse
import gzip
import json
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple


# Canonical MLB team IDs (StatsAPI) for filtering.
# Note: StatsAPI schedule with sportId=1 can include spring/exhibition games
# against non-MLB opponents; raw feed/live boxscore team objects may not include
# sport/league metadata to disambiguate.
MLB_TEAM_IDS: set[int] = {
    108,  # LAA
    109,  # ARI
    110,  # BAL
    111,  # BOS
    112,  # CHC
    113,  # CIN
    114,  # CLE
    115,  # COL
    116,  # DET
    117,  # HOU
    118,  # KC
    119,  # LAD
    120,  # WSH
    121,  # NYM
    133,  # OAK/ATH
    134,  # PIT
    135,  # SD
    136,  # SEA
    137,  # SF
    138,  # STL
    139,  # TB
    140,  # TEX
    141,  # TOR
    142,  # MIN
    143,  # PHI
    144,  # ATL
    145,  # CWS
    146,  # MIA
    147,  # NYY
    158,  # MIL
}


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _daterange(start: datetime, end: datetime) -> List[str]:
    out: List[str] = []
    d = start
    while d <= end:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or str(x).strip() == "":
            return default
        return float(x)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _recommend_manager_profile(agg: Dict[str, Any]) -> Dict[str, Any]:
    games = max(1.0, float(agg.get("games") or 0.0))
    avg_sp_pitches = float(agg.get("sp_pitches_sum") or 0.0) / games
    avg_sp_ip = float(agg.get("sp_ip_sum") or 0.0) / games
    avg_pinch = float(agg.get("pinch_hitters_sum") or 0.0) / games

    pull_pc = int(round(_clamp(avg_sp_pitches, 75.0, 115.0)))
    starter_min_inn = 5 if avg_sp_ip >= 5.2 else 4

    # Map 0..2 pinch hitters per game => ~0.05..0.30
    pinch_aggr = _clamp(avg_pinch / 6.0, 0.05, 0.35)

    return {
        "pull_starter_pitch_count": pull_pc,
        "starter_min_innings": int(starter_min_inn),
        "pinch_hit_aggressiveness": float(pinch_aggr),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build per-team manager tendency map from raw StatsAPI feed/live payloads")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument(
        "--feed-live-root",
        default=str(Path(__file__).resolve().parents[2] / "data" / "raw" / "statsapi" / "feed_live"),
    )
    ap.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[2] / "data" / "manager" / "manager_tendencies.json"),
    )
    ap.add_argument("--merge", choices=["on", "off"], default="on")
    args = ap.parse_args()

    start = _parse_ymd(args.start_date)
    end = _parse_ymd(args.end_date)
    dates = _daterange(start, end)

    root = Path(args.feed_live_root) / str(int(args.season))
    out_path = Path(args.out)
    _ensure_dir(out_path.parent)

    agg_by_team: Dict[int, Dict[str, Any]] = {}

    def take_team(team_obj: Dict[str, Any], is_home: bool, date_str: str, game_pk: int) -> None:
        team = (team_obj.get("team") or {})
        team_id = team.get("id")
        if team_id is None:
            return
        try:
            tid = int(team_id)
        except Exception:
            return

        if tid not in MLB_TEAM_IDS:
            return

        pitchers = team_obj.get("pitchers") or []
        players = team_obj.get("players") or {}
        batting_order = team_obj.get("battingOrder") or []
        starters = set(int(x) for x in batting_order if isinstance(x, int) or str(x).isdigit())

        # Count only players who recorded a PA.
        batters_used: set[int] = set()
        for _, pl in (players or {}).items():
            bst = ((pl.get("stats") or {}).get("batting") or {})
            try:
                pa = int(bst.get("plateAppearances") or 0)
            except Exception:
                pa = 0
            if pa > 0:
                try:
                    pid = int((pl.get("person") or {}).get("id") or 0)
                except Exception:
                    pid = 0
                if pid > 0:
                    batters_used.add(pid)

        pinch = len([x for x in batters_used if x not in starters])

        sp_pitches = 0.0
        sp_ip = 0.0
        bullpen_used = 0
        closer_save_opp = 0

        if pitchers:
            try:
                sp_id = int(pitchers[0])
                sp_pl = players.get(f"ID{sp_id}") or {}
                pst = ((sp_pl.get("stats") or {}).get("pitching") or {})
                sp_pitches = _safe_float(pst.get("pitchesThrown"), 0.0)
                sp_ip = _safe_float(pst.get("inningsPitched"), 0.0)
            except Exception:
                sp_pitches = 0.0
                sp_ip = 0.0

            bullpen_used = max(0, len(pitchers) - 1)

            # closer usage proxy: any pitcher with a save opportunity
            for pid in pitchers[1:]:
                pl = players.get(f"ID{int(pid)}") or {}
                pst = ((pl.get("stats") or {}).get("pitching") or {})
                so = pst.get("saveOpportunities")
                try:
                    if so is not None and int(so) > 0:
                        closer_save_opp = 1
                        break
                except Exception:
                    continue

        a = agg_by_team.get(tid)
        if a is None:
            a = {
                "team_id": tid,
                "team_name": str(team.get("name") or ""),
                "games": 0,
                "sp_pitches_sum": 0.0,
                "sp_ip_sum": 0.0,
                "bullpen_pitchers_sum": 0.0,
                "pinch_hitters_sum": 0.0,
                "closer_save_opp_games": 0,
                "sample": [],
            }
            agg_by_team[tid] = a

        a["games"] += 1
        a["sp_pitches_sum"] += float(sp_pitches)
        a["sp_ip_sum"] += float(sp_ip)
        a["bullpen_pitchers_sum"] += float(bullpen_used)
        a["pinch_hitters_sum"] += float(pinch)
        a["closer_save_opp_games"] += int(closer_save_opp)

        if len(a["sample"]) < 3:
            a["sample"].append(
                {
                    "date": date_str,
                    "game_pk": int(game_pk),
                    "is_home": bool(is_home),
                    "sp_pitches": float(sp_pitches),
                    "sp_ip": float(sp_ip),
                    "bullpen_pitchers": int(bullpen_used),
                    "pinch_hitters": int(pinch),
                    "closer_save_opp": int(closer_save_opp),
                }
            )

    for date_str in dates:
        day_dir = root / date_str
        if not day_dir.exists():
            continue

        for gz_path in sorted(day_dir.glob("*.json.gz")):
            try:
                game_pk = int(gz_path.name.split(".")[0])
            except Exception:
                continue

            try:
                with gzip.open(gz_path, "rt", encoding="utf-8") as f:
                    payload = json.load(f)
                box = ((payload.get("liveData") or {}).get("boxscore") or {})
                teams = (box.get("teams") or {})
                home = teams.get("home") or {}
                away = teams.get("away") or {}
                take_team(home, True, date_str, game_pk)
                take_team(away, False, date_str, game_pk)
            except Exception:
                continue

    out: Dict[str, Any] = {}
    if args.merge == "on" and out_path.exists():
        try:
            raw = json.loads(out_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                out = dict(raw)
        except Exception:
            out = {}

    for tid, a in agg_by_team.items():
        games = max(1.0, float(a.get("games") or 0.0))
        derived = {
            "avg_sp_pitches": float(a.get("sp_pitches_sum") or 0.0) / games,
            "avg_sp_ip": float(a.get("sp_ip_sum") or 0.0) / games,
            "avg_bullpen_pitchers": float(a.get("bullpen_pitchers_sum") or 0.0) / games,
            "avg_pinch_hitters": float(a.get("pinch_hitters_sum") or 0.0) / games,
            "closer_save_opp_rate": float(a.get("closer_save_opp_games") or 0.0) / games,
        }
        rec = _recommend_manager_profile(a)

        payload = {
            "team_id": int(tid),
            "team_name": str(a.get("team_name") or ""),
            "season": int(args.season),
            "start_date": str(args.start_date),
            "end_date": str(args.end_date),
            "games": int(a.get("games") or 0),
            "derived": derived,
            "recommended_manager_overrides": rec,
            "sample": a.get("sample") or [],
            "generated_at": datetime.now().isoformat(),
            "source": "statsapi_feed_live_raw",
        }
        out[str(tid)] = payload

    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote: {out_path} teams={len(agg_by_team)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
