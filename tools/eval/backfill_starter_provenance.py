from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure the project root (MLB-BettingV2/) is importable.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.statsapi import StatsApiClient, fetch_schedule_for_date
from sim_engine.data.build_roster import build_team, build_team_roster


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, obj: Any) -> None:
    payload = json.dumps(obj, indent=2)
    # On Windows (especially with OneDrive), `os.replace` can transiently fail with
    # WinError 32 if either the target or temp file is briefly locked. Use retries
    # with a unique temp filename to avoid collisions.
    last_err: Optional[BaseException] = None
    for attempt in range(6):
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
        tmp.write_text(payload, encoding="utf-8")
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            time.sleep(0.25 * (attempt + 1))
    if last_err is not None:
        raise last_err


def _iter_day_reports(batch_dir: Path) -> List[Path]:
    return sorted(batch_dir.glob("sim_vs_actual_*.json"))


def _schedule_maps(client: StatsApiClient, date_str: str) -> Tuple[Dict[int, str], Dict[int, Dict[str, Optional[int]]]]:
    """Return (game_pk->gameType, game_pk->{away_probable_id, home_probable_id})."""
    game_type: Dict[int, str] = {}
    probables: Dict[int, Dict[str, Optional[int]]] = {}
    try:
        games = fetch_schedule_for_date(client, date_str)
    except Exception:
        games = []

    for g in games or []:
        try:
            game_pk = int(g.get("gamePk") or 0)
        except Exception:
            continue
        if game_pk <= 0:
            continue

        gt = str(g.get("gameType") or "").strip()
        if gt:
            game_type[game_pk] = gt

        away = ((g.get("teams") or {}).get("away") or {})
        home = ((g.get("teams") or {}).get("home") or {})
        away_prob = away.get("probablePitcher") or {}
        home_prob = home.get("probablePitcher") or {}

        def _pid(x: Any) -> Optional[int]:
            try:
                v = int((x or {}).get("id") or 0)
            except Exception:
                v = 0
            return int(v) if v > 0 else None

        probables[game_pk] = {
            "away": _pid(away_prob),
            "home": _pid(home_prob),
        }

    return game_type, probables


def _as_int(x: Any) -> Optional[int]:
    try:
        v = int(x)
        return v if v > 0 else None
    except Exception:
        return None


def _backfill_day_report(
    *,
    path: Path,
    client: StatsApiClient,
    cache_season: Optional[int] = None,
) -> Dict[str, int]:
    obj = _read_json(path)
    if not isinstance(obj, dict):
        return {"files": 0, "games_updated": 0, "games_skipped": 0}

    meta = obj.get("meta") or {}
    date_str = str(meta.get("date") or "").strip()
    if not date_str:
        # fallback: parse from filename
        stem = path.stem
        if stem.startswith("sim_vs_actual_"):
            date_str = stem.replace("sim_vs_actual_", "")

    season = _as_int(meta.get("season")) or _as_int(date_str.split("-")[0]) or 0
    if season <= 0:
        return {"files": 0, "games_updated": 0, "games_skipped": 0}

    game_type, probables = _schedule_maps(client, date_str)

    games = obj.get("games") or []
    if not isinstance(games, list) or not games:
        return {"files": 0, "games_updated": 0, "games_skipped": 0}

    updated = 0
    skipped = 0

    for g in games:
        if not isinstance(g, dict):
            continue
        rs_existing = g.get("roster_starters")
        if not isinstance(rs_existing, dict):
            rs_existing = {}

        # Backfill if roster starters are missing entirely OR if starter provenance fields are missing.
        # Many older reports already have away/home starter ids but have blank *_source, which causes
        # batch summaries to show 'missing' starter_sources.
        away_src_existing = str(rs_existing.get("away_source") or "").strip()
        home_src_existing = str(rs_existing.get("home_source") or "").strip()
        away_id_existing = _as_int(rs_existing.get("away"))
        home_id_existing = _as_int(rs_existing.get("home"))

        needs_backfill = False
        if not rs_existing:
            needs_backfill = True
        if not away_src_existing or not home_src_existing:
            needs_backfill = True
        if away_id_existing is None or home_id_existing is None:
            needs_backfill = True

        if not needs_backfill:
            skipped += 1
            continue

        game_pk = _as_int(g.get("game_pk")) or 0

        away_team = g.get("away") or {}
        home_team = g.get("home") or {}
        away_id = _as_int(away_team.get("id")) or 0
        home_id = _as_int(home_team.get("id")) or 0
        if away_id <= 0 or home_id <= 0:
            skipped += 1
            continue

        starters = g.get("starters") or {}
        away_box = _as_int(starters.get("away"))
        home_box = _as_int(starters.get("home"))

        probs = probables.get(int(game_pk)) or {}
        away_prob = _as_int(probs.get("away"))
        home_prob = _as_int(probs.get("home"))

        # Mirror eval_sim_day_vs_actual: prefer boxscore starter; otherwise probable from schedule.
        away_pitcher_for_roster = away_box if away_box else away_prob
        home_pitcher_for_roster = home_box if home_box else home_prob

        # Spring-mode inference: use schedule gameType.
        gt = str(game_type.get(int(game_pk), ""))
        spring_mode = bool(gt.upper() == "S")
        stats_season = (season - 1) if spring_mode else season

        fallback_roster_types = ["40Man", "nonRosterInvitees"] if spring_mode else None

        t_away = build_team(away_id, str(away_team.get("name") or "Away"), str(away_team.get("abbr") or away_team.get("name") or "AWY"))
        t_home = build_team(home_id, str(home_team.get("name") or "Home"), str(home_team.get("abbr") or home_team.get("name") or "HME"))

        try:
            away_roster = build_team_roster(
                client,
                t_away,
                int(stats_season),
                as_of_date=str(date_str),
                probable_pitcher_id=away_pitcher_for_roster,
                roster_type="active",
                fallback_roster_types=fallback_roster_types,
            )
        except Exception:
            away_roster = None
        try:
            home_roster = build_team_roster(
                client,
                t_home,
                int(stats_season),
                as_of_date=str(date_str),
                probable_pitcher_id=home_pitcher_for_roster,
                roster_type="active",
                fallback_roster_types=fallback_roster_types,
            )
        except Exception:
            home_roster = None

        def _starter_info(roster: Any) -> Tuple[Optional[int], str, Optional[int]]:
            try:
                p = getattr(getattr(roster, "lineup", None), "pitcher", None)
                pid = int(getattr(getattr(p, "player", None), "mlbam_id", 0) or 0)
                pid_out = pid if pid > 0 else None
            except Exception:
                pid_out = None
            try:
                src = str(getattr(p, "starter_selection_source", "") or "")
            except Exception:
                src = ""
            try:
                req = getattr(p, "starter_requested_id", None)
            except Exception:
                req = None
            return pid_out, src, req

        away_pid, away_src, away_req = _starter_info(away_roster)
        home_pid, home_src, home_req = _starter_info(home_roster)

        # Preserve existing starter ids if already present and non-null; otherwise use backfilled.
        away_final = away_id_existing if away_id_existing is not None else away_pid
        home_final = home_id_existing if home_id_existing is not None else home_pid

        away_src_final = away_src_existing if away_src_existing else away_src
        home_src_final = home_src_existing if home_src_existing else home_src

        g["roster_starters"] = {
            "away": away_final,
            "home": home_final,
            "away_source": away_src_final,
            "home_source": home_src_final,
            "away_requested_id": rs_existing.get("away_requested_id", away_req),
            "home_requested_id": rs_existing.get("home_requested_id", home_req),
        }

        # Also backfill probable_starters (from schedule) if missing.
        if not isinstance(g.get("probable_starters"), dict):
            g["probable_starters"] = {
                "away": away_prob,
                "home": home_prob,
                "away_source": "schedule" if away_prob else "",
                "home_source": "schedule" if home_prob else "",
                "away_confidence": 1.0 if away_prob else 0.0,
                "home_confidence": 1.0 if home_prob else 0.0,
            }

        updated += 1

    if updated > 0:
        _write_json_atomic(path, obj)

    return {"files": 1, "games_updated": updated, "games_skipped": skipped}


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill roster/probable starter provenance into existing sim_vs_actual_*.json day reports.")
    ap.add_argument("--batch-dir", required=True, help="Batch dir containing sim_vs_actual_*.json")
    ap.add_argument("--cache-ttl-hours", type=float, default=24.0, help="StatsAPI cache TTL (hours)")
    args = ap.parse_args()

    batch_dir = Path(str(args.batch_dir))
    if not batch_dir.is_absolute():
        batch_dir = (_ROOT / batch_dir).resolve()
    if not batch_dir.exists():
        raise FileNotFoundError(f"Batch dir not found: {batch_dir}")

    reports = _iter_day_reports(batch_dir)
    if not reports:
        print(f"No sim_vs_actual_*.json in {batch_dir}")
        return 0

    client = StatsApiClient.with_default_cache(ttl_seconds=int(float(args.cache_ttl_hours) * 3600))

    tot_files = 0
    tot_updated = 0
    tot_skipped = 0

    for p in reports:
        r = _backfill_day_report(path=p, client=client)
        tot_files += int(r.get("files") or 0)
        tot_updated += int(r.get("games_updated") or 0)
        tot_skipped += int(r.get("games_skipped") or 0)
        if int(r.get("games_updated") or 0) > 0:
            print(f"Updated {p.name}: +{r['games_updated']} games")

    print(f"Done. Reports: {tot_files}, games updated: {tot_updated}, games skipped: {tot_skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
