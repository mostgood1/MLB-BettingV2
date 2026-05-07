from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


_SCHEMA_VERSION = 1


def _root_dir() -> Path:
    # roster_registry.py lives at MLB-BettingV2/sim_engine/data/roster_registry.py
    # parents[2] => MLB-BettingV2/
    return Path(__file__).resolve().parents[2]


def default_registry_dir() -> Path:
    return _root_dir() / "data" / "roster_registry"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        return


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _status_obj(entry: Dict[str, Any]) -> Dict[str, Any]:
    s = entry.get("status") or {}
    if not isinstance(s, dict):
        return {}
    out: Dict[str, Any] = {}
    for k in ("code", "description"):
        if k in s:
            out[k] = s.get(k)
    return out


def _position_abbr(entry: Dict[str, Any]) -> str:
    pos = entry.get("position") or {}
    if isinstance(pos, dict):
        abbr = pos.get("abbreviation") or ""
        return str(abbr).strip().upper()
    return ""


def _person_obj(entry: Dict[str, Any]) -> Tuple[int, str]:
    person = entry.get("person") or {}
    if not isinstance(person, dict):
        return 0, ""
    pid = _safe_int(person.get("id"), 0)
    name = str(person.get("fullName") or "").strip()
    return pid, name


def _normalize_players(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        pid, name = _person_obj(e)
        if pid <= 0 or pid in seen:
            continue
        seen.add(pid)
        out.append(
            {
                "player_id": int(pid),
                "full_name": name,
                "position": _position_abbr(e),
                "status": _status_obj(e),
            }
        )
    return out


def _playerset(players: List[Dict[str, Any]]) -> set[int]:
    s: set[int] = set()
    for p in players or []:
        try:
            pid = int(p.get("player_id") or 0)
        except Exception:
            pid = 0
        if pid > 0:
            s.add(pid)
    return s


def _latest_date_for_roster_type(team_doc: Dict[str, Any], roster_type: str) -> Optional[str]:
    try:
        snapshots = team_doc.get("snapshots") or {}
        if not isinstance(snapshots, dict) or not snapshots:
            return None
        dates: List[str] = []
        for d, by_rt in snapshots.items():
            if not isinstance(by_rt, dict):
                continue
            if str(roster_type) in by_rt:
                dates.append(str(d))
        return max(dates) if dates else None
    except Exception:
        return None


def _prev_date_for_roster_type(snapshots: Dict[str, Any], date_str: str, roster_type: str) -> Optional[str]:
    try:
        if not isinstance(snapshots, dict) or not snapshots:
            return None
        dates: List[str] = []
        for d, by_rt in snapshots.items():
            if not isinstance(by_rt, dict):
                continue
            if str(roster_type) not in by_rt:
                continue
            ds = str(d)
            if ds < str(date_str):
                dates.append(ds)
        return max(dates) if dates else None
    except Exception:
        return None


def build_latest_registry_team_by_player(
    *,
    as_of_date: Optional[str] = None,
    registry_dir: Optional[Path] = None,
    roster_types: Tuple[str, ...] = ("active", "40Man", "nonRosterInvitees"),
) -> Dict[int, Dict[str, Any]]:
    """Return each player's latest known team from registry snapshots before `as_of_date`."""

    registry_dir = registry_dir or default_registry_dir()
    cutoff = str(as_of_date or "").strip()
    latest_by_player: Dict[int, Dict[str, Any]] = {}

    try:
        paths = sorted(Path(registry_dir).glob("team_*.json"))
    except Exception:
        paths = []

    for path in paths:
        doc = _load_json(path)
        if not doc:
            continue
        team_id = _safe_int(doc.get("team_id"), 0)
        if team_id <= 0:
            continue
        team_abbr = str(doc.get("team_abbr") or "").strip().upper()
        snapshots = doc.get("snapshots") or {}
        if not isinstance(snapshots, dict):
            continue

        for snap_date in sorted(snapshots.keys()):
            snap_date_s = str(snap_date)
            if cutoff and snap_date_s >= cutoff:
                continue
            by_rt = snapshots.get(snap_date_s) or {}
            if not isinstance(by_rt, dict):
                continue

            for roster_type in roster_types:
                roster_snapshot = by_rt.get(str(roster_type)) or {}
                if not isinstance(roster_snapshot, dict):
                    continue
                players = roster_snapshot.get("players") or []
                if not isinstance(players, list):
                    continue

                for player in players:
                    if not isinstance(player, dict):
                        continue
                    pid = _safe_int(player.get("player_id"), 0)
                    if pid <= 0:
                        continue
                    prev = latest_by_player.get(pid) or {}
                    prev_date = str(prev.get("date") or "")
                    if prev_date and prev_date > snap_date_s:
                        continue
                    latest_by_player[pid] = {
                        "team_id": int(team_id),
                        "team_abbr": team_abbr,
                        "date": snap_date_s,
                    }

    return latest_by_player


def sanitize_rosters_by_latest_registry_team(
    *,
    team_id: int,
    team_abbr: str,
    rosters_by_type: Dict[str, List[Dict[str, Any]]],
    latest_team_by_player: Dict[int, Dict[str, Any]],
    min_conflicts: int = 4,
    min_conflict_ratio: float = 0.15,
    activation_roster_type: str = "active",
    sanitize_roster_types: Tuple[str, ...] = ("active", "40Man", "nonRosterInvitees"),
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    """Filter clearly contaminated team rosters using prior registry ownership.

    The filter is activated only when the fetched roster has several players whose latest
    known team is a different club, which protects normal same-day moves while guarding
    against source payload corruption.
    """

    team_id = int(team_id)
    team_abbr = str(team_abbr or "").strip().upper()
    sanitized: Dict[str, List[Dict[str, Any]]] = {
        str(rt): list(entries or []) for rt, entries in (rosters_by_type or {}).items()
    }

    report: Dict[str, Any] = {
        "team_id": int(team_id),
        "team_abbr": team_abbr,
        "applied": False,
        "activation_roster_type": str(activation_roster_type),
        "conflict_count": 0,
        "activation_player_count": 0,
        "conflict_ratio": 0.0,
        "removed_players": [],
    }

    activation_entries = sanitized.get(str(activation_roster_type)) or []
    conflicting_ids: set[int] = set()
    removed_players: List[Dict[str, Any]] = []

    for entry in activation_entries:
        if not isinstance(entry, dict):
            continue
        pid, full_name = _person_obj(entry)
        if pid <= 0:
            continue
        latest = latest_team_by_player.get(pid) or {}
        latest_team_id = _safe_int(latest.get("team_id"), 0)
        if latest_team_id <= 0 or latest_team_id == team_id:
            continue
        conflicting_ids.add(pid)
        removed_players.append(
            {
                "player_id": int(pid),
                "full_name": full_name,
                "latest_team_id": int(latest_team_id),
                "latest_team_abbr": str(latest.get("team_abbr") or ""),
                "latest_date": str(latest.get("date") or ""),
            }
        )

    activation_player_count = len({_person_obj(entry)[0] for entry in activation_entries if isinstance(entry, dict) and _person_obj(entry)[0] > 0})
    conflict_count = len(conflicting_ids)
    conflict_ratio = (float(conflict_count) / float(activation_player_count)) if activation_player_count > 0 else 0.0
    report["conflict_count"] = int(conflict_count)
    report["activation_player_count"] = int(activation_player_count)
    report["conflict_ratio"] = float(conflict_ratio)

    if conflict_count < int(min_conflicts) or conflict_ratio < float(min_conflict_ratio):
        return sanitized, report

    report["applied"] = True
    report["removed_players"] = sorted(removed_players, key=lambda item: (str(item.get("full_name") or ""), int(item.get("player_id") or 0)))

    for roster_type in sanitize_roster_types:
        entries = sanitized.get(str(roster_type))
        if not isinstance(entries, list):
            continue
        sanitized[str(roster_type)] = [
            entry
            for entry in entries
            if _person_obj(entry)[0] <= 0 or _person_obj(entry)[0] not in conflicting_ids
        ]

    return sanitized, report


def build_roster_events_for_date(
    *,
    date_str: str,
    registry_dir: Optional[Path] = None,
    primary_move_roster_type: str = "40Man",
    include_baseline: bool = False,
) -> Dict[str, Any]:
    """Build a daily roster event summary from per-team registry files.

    Output is designed to be written as `data/daily/snapshots/<date>/roster_events.json`.
    """

    date_str = str(date_str)
    registry_dir = registry_dir or default_registry_dir()

    events: Dict[str, Any] = {
        "schema_version": 1,
        "date": date_str,
        "generated_at": datetime.now().isoformat(),
        "include_baseline": bool(include_baseline),
        "teams": {},
        "moves_between_teams": [],
    }

    team_docs: List[Dict[str, Any]] = []
    try:
        for path in sorted(Path(registry_dir).glob("team_*.json")):
            doc = _load_json(path)
            if doc:
                team_docs.append(doc)
    except Exception:
        team_docs = []

    # Per-team add/remove events with player details.
    for doc in team_docs:
        try:
            team_id = _safe_int(doc.get("team_id"), 0)
        except Exception:
            team_id = 0
        if team_id <= 0:
            continue

        team_abbr = str(doc.get("team_abbr") or "").strip().upper()
        snapshots = doc.get("snapshots") or {}
        if not isinstance(snapshots, dict):
            continue
        today = snapshots.get(date_str) or {}
        if not isinstance(today, dict) or not today:
            continue

        team_out: Dict[str, Any] = {
            "team_id": int(team_id),
            "team_abbr": team_abbr,
            "roster_types": {},
        }

        for rt, snap in today.items():
            if not isinstance(snap, dict):
                continue
            players_today = snap.get("players") or []
            added_ids = snap.get("added") or []
            removed_ids = snap.get("removed") or []

            try:
                added_set = {int(x) for x in added_ids or [] if int(x or 0) > 0}
            except Exception:
                added_set = set()
            try:
                removed_set = {int(x) for x in removed_ids or [] if int(x or 0) > 0}
            except Exception:
                removed_set = set()

            prev_date = _prev_date_for_roster_type(snapshots, date_str, str(rt))
            prev_players: List[Dict[str, Any]] = []
            prev_players = []
            if prev_date:
                try:
                    prev_players = (((snapshots.get(prev_date) or {}).get(str(rt)) or {}).get("players") or [])
                except Exception:
                    prev_players = []

            def _pid(p: Any) -> int:
                try:
                    return int((p or {}).get("player_id") or 0)
                except Exception:
                    return 0

            # Always emit counts so day-1 isn't empty, but keep lists small unless we have a baseline.
            rt_out: Dict[str, Any] = {
                "prev_date": prev_date,
                "n_players": int(len(players_today or [])),
                "n_added": int(len(added_set)),
                "n_removed": int(len(removed_set)),
            }

            # Emit detailed deltas only when we can diff against a previous snapshot.
            if prev_date and (added_set or removed_set):
                added_players = [p for p in (players_today or []) if _pid(p) in added_set]
                removed_players = [p for p in (prev_players or []) if _pid(p) in removed_set]
                rt_out.update(
                    {
                        "added": added_players,
                        "removed": removed_players,
                        "added_ids": sorted(list(added_set)),
                        "removed_ids": sorted(list(removed_set)),
                    }
                )
            elif (not prev_date) and bool(include_baseline):
                rt_out.update({"baseline": True, "players": players_today})

            team_out["roster_types"][str(rt)] = rt_out

        if team_out["roster_types"]:
            events["teams"][str(team_id)] = team_out

    # Cross-team moves (very rough heuristic): same-day removed on one team and added on another.
    # Only makes sense when we have diff output (prev snapshots).
    primary_rt = str(primary_move_roster_type)
    removed_map: Dict[int, Dict[str, Any]] = {}
    added_map: Dict[int, Dict[str, Any]] = {}

    for team_id_s, team_out in (events.get("teams") or {}).items():
        try:
            team_id = int(team_id_s)
        except Exception:
            continue
        rt_block = (team_out.get("roster_types") or {}).get(primary_rt) or {}
        if not isinstance(rt_block, dict) or not rt_block:
            continue
        for p in rt_block.get("removed") or []:
            try:
                pid = int((p or {}).get("player_id") or 0)
            except Exception:
                pid = 0
            if pid > 0:
                removed_map[pid] = {"from_team_id": team_id, "player": p}
        for p in rt_block.get("added") or []:
            try:
                pid = int((p or {}).get("player_id") or 0)
            except Exception:
                pid = 0
            if pid > 0:
                added_map[pid] = {"to_team_id": team_id, "player": p}

    moves: List[Dict[str, Any]] = []
    for pid, rinfo in removed_map.items():
        ainfo = added_map.get(pid)
        if not ainfo:
            continue
        from_team_id = int(rinfo.get("from_team_id") or 0)
        to_team_id = int(ainfo.get("to_team_id") or 0)
        if from_team_id <= 0 or to_team_id <= 0 or from_team_id == to_team_id:
            continue
        player = ainfo.get("player") or rinfo.get("player") or {}
        moves.append(
            {
                "player_id": int(pid),
                "full_name": (player or {}).get("full_name"),
                "from_team_id": from_team_id,
                "to_team_id": to_team_id,
                "roster_type": primary_rt,
            }
        )

    events["moves_between_teams"] = sorted(moves, key=lambda x: (x.get("from_team_id", 0), x.get("to_team_id", 0), x.get("player_id", 0)))
    return events


def update_team_roster_registry(
    *,
    team_id: int,
    team_abbr: str,
    date_str: str,
    rosters_by_type: Dict[str, List[Dict[str, Any]]],
    registry_dir: Optional[Path] = None,
) -> Path:
    """Persist a per-team roster registry file with daily snapshots.

    This is intended as an append-only history so you can infer promotions/demotions,
    IL moves, and trades by diffing snapshots over time.
    """

    team_id = int(team_id)
    team_abbr = str(team_abbr or "").strip().upper()
    date_str = str(date_str)
    registry_dir = registry_dir or default_registry_dir()

    path = Path(registry_dir) / f"team_{team_id}.json"
    doc = _load_json(path)

    if not doc:
        doc = {
            "schema_version": _SCHEMA_VERSION,
            "team_id": int(team_id),
            "team_abbr": str(team_abbr),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "snapshots": {},
        }

    snapshots = doc.get("snapshots")
    if not isinstance(snapshots, dict):
        snapshots = {}
        doc["snapshots"] = snapshots

    by_rt = snapshots.get(date_str)
    if not isinstance(by_rt, dict):
        by_rt = {}
        snapshots[date_str] = by_rt

    for rt, entries in (rosters_by_type or {}).items():
        rt_s = str(rt)
        players = _normalize_players(entries or [])

        # Diff vs last snapshot for this roster type.
        prev_date = _latest_date_for_roster_type(doc, rt_s)
        prev_players: List[Dict[str, Any]] = []
        if prev_date and prev_date != date_str:
            try:
                prev_players = ((snapshots.get(prev_date) or {}).get(rt_s) or {}).get("players") or []
            except Exception:
                prev_players = []

        cur_set = _playerset(players)
        prev_set = _playerset(prev_players)
        added = sorted(list(cur_set - prev_set))
        removed = sorted(list(prev_set - cur_set))

        by_rt[rt_s] = {
            "players": players,
            "added": added,
            "removed": removed,
        }

    doc["team_abbr"] = str(team_abbr or doc.get("team_abbr") or "")
    doc["updated_at"] = datetime.now().isoformat()
    _write_json(path, doc)
    return path
