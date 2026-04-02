from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from sim_engine.models import (
    BatterProfile,
    Handedness,
    Lineup,
    ManagerProfile,
    PitcherProfile,
    PitchType,
    Player,
    Team,
    TeamRoster,
)


_SCHEMA_VERSION = 2


def _hand(x: Any, default: Handedness = Handedness.R) -> Handedness:
    try:
        if isinstance(x, Handedness):
            return x
        s = str(x or "").strip().upper()
        return Handedness(s)
    except Exception:
        return default


def _pitch_type(x: Any) -> PitchType:
    try:
        if isinstance(x, PitchType):
            return x
        s = str(x or "").strip().upper()
        return PitchType(s)
    except Exception:
        return PitchType.OTHER


def _ser_pitchtype_map(m: Optional[Dict[Any, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, v in (m or {}).items():
        try:
            kk = _pitch_type(k).value
        except Exception:
            kk = str(k)
        try:
            out[str(kk)] = float(v)
        except Exception:
            continue
    return out


def _de_pitchtype_map(m: Optional[Dict[str, Any]]) -> Dict[PitchType, float]:
    out: Dict[PitchType, float] = {}
    for k, v in (m or {}).items():
        pt = _pitch_type(k)
        try:
            out[pt] = float(v)
        except Exception:
            continue
    return out


def _ser_intkey_map(m: Optional[Dict[Any, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, v in (m or {}).items():
        try:
            kk = str(int(k))
            out[kk] = float(v)
        except Exception:
            continue
    return out


def _de_intkey_map(m: Optional[Dict[str, Any]]) -> Dict[int, float]:
    out: Dict[int, float] = {}
    for k, v in (m or {}).items():
        try:
            kk = int(k)
            out[kk] = float(v)
        except Exception:
            continue
    return out


def _ser_intkey_nested_map(m: Optional[Dict[Any, Any]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for k, inner in (m or {}).items():
        try:
            kk = str(int(k))
        except Exception:
            continue
        if not isinstance(inner, dict):
            continue
        inner_out: Dict[str, float] = {}
        for inner_key, inner_value in inner.items():
            try:
                inner_out[str(inner_key)] = float(inner_value)
            except Exception:
                continue
        if inner_out:
            out[kk] = inner_out
    return out


def _de_intkey_nested_map(m: Optional[Dict[str, Any]]) -> Dict[int, Dict[str, float]]:
    out: Dict[int, Dict[str, float]] = {}
    for k, inner in (m or {}).items():
        try:
            kk = int(k)
        except Exception:
            continue
        if not isinstance(inner, dict):
            continue
        inner_out: Dict[str, float] = {}
        for inner_key, inner_value in inner.items():
            try:
                inner_out[str(inner_key)] = float(inner_value)
            except Exception:
                continue
        if inner_out:
            out[kk] = inner_out
    return out


def roster_to_dict(roster: TeamRoster) -> Dict[str, Any]:
    """Serialize a TeamRoster to a JSON-friendly dict."""

    def ser_player(p: Player) -> Dict[str, Any]:
        return {
            "mlbam_id": int(p.mlbam_id),
            "full_name": str(p.full_name),
            "primary_position": str(p.primary_position),
            "bat_side": _hand(p.bat_side).value,
            "throw_side": _hand(p.throw_side).value,
        }

    def ser_batter(b: BatterProfile) -> Dict[str, Any]:
        return {
            "player": ser_player(b.player),
            "k_rate": float(b.k_rate),
            "bb_rate": float(b.bb_rate),
            "hbp_rate": float(b.hbp_rate),
            "hr_rate": float(b.hr_rate),
            "inplay_hit_rate": float(b.inplay_hit_rate),
            "xb_hit_share": float(b.xb_hit_share),
            "triple_share_of_xb": float(b.triple_share_of_xb),
            "sb_attempt_rate": float(b.sb_attempt_rate),
            "sb_success_rate": float(b.sb_success_rate),
            "vs_pitch_type": _ser_pitchtype_map(b.vs_pitch_type),
            "platoon_mult_vs_lhp": {str(k): float(v) for k, v in (b.platoon_mult_vs_lhp or {}).items()},
            "platoon_mult_vs_rhp": {str(k): float(v) for k, v in (b.platoon_mult_vs_rhp or {}).items()},
            "venue_mult_home": {str(k): float(v) for k, v in (b.venue_mult_home or {}).items()},
            "venue_mult_away": {str(k): float(v) for k, v in (b.venue_mult_away or {}).items()},
            "statcast_quality_mult": {str(k): float(v) for k, v in (b.statcast_quality_mult or {}).items()},
            "vs_pitcher_hr_mult": _ser_intkey_map(b.vs_pitcher_hr_mult),
            "vs_pitcher_k_mult": _ser_intkey_map(b.vs_pitcher_k_mult),
            "vs_pitcher_bb_mult": _ser_intkey_map(b.vs_pitcher_bb_mult),
            "vs_pitcher_inplay_mult": _ser_intkey_map(b.vs_pitcher_inplay_mult),
            "vs_pitcher_history": _ser_intkey_nested_map(b.vs_pitcher_history),
            "bb_gb_rate": float(b.bb_gb_rate),
            "bb_fb_rate": float(b.bb_fb_rate),
            "bb_ld_rate": float(b.bb_ld_rate),
            "bb_pu_rate": float(b.bb_pu_rate),
            "bb_inplay_n": int(b.bb_inplay_n),
        }

    def ser_pitcher(p: PitcherProfile) -> Dict[str, Any]:
        return {
            "player": ser_player(p.player),
            "k_rate": float(p.k_rate),
            "bb_rate": float(p.bb_rate),
            "hbp_rate": float(p.hbp_rate),
            "hr_rate": float(p.hr_rate),
            "inplay_hit_rate": float(p.inplay_hit_rate),
            "batters_faced": float(p.batters_faced),
            "balls_in_play": float(p.balls_in_play),
            "arsenal": _ser_pitchtype_map(p.arsenal),
            "pitch_type_whiff_mult": _ser_pitchtype_map(p.pitch_type_whiff_mult),
            "pitch_type_inplay_mult": _ser_pitchtype_map(p.pitch_type_inplay_mult),
            "statcast_splits_source": str(p.statcast_splits_source),
            "statcast_splits_n_pitches": int(p.statcast_splits_n_pitches),
            "statcast_splits_start_date": str(p.statcast_splits_start_date),
            "statcast_splits_end_date": str(p.statcast_splits_end_date),
            "arsenal_source": str(p.arsenal_source),
            "arsenal_sample_size": int(p.arsenal_sample_size),
            "stamina_pitches": int(p.stamina_pitches),
            "role": str(p.role),
            "availability_mult": float(p.availability_mult),
            "platoon_mult_vs_lhb": {str(k): float(v) for k, v in (p.platoon_mult_vs_lhb or {}).items()},
            "platoon_mult_vs_rhb": {str(k): float(v) for k, v in (p.platoon_mult_vs_rhb or {}).items()},
            "venue_mult_home": {str(k): float(v) for k, v in (p.venue_mult_home or {}).items()},
            "venue_mult_away": {str(k): float(v) for k, v in (p.venue_mult_away or {}).items()},
            "statcast_quality_mult": {str(k): float(v) for k, v in (p.statcast_quality_mult or {}).items()},
            "bb_gb_rate": float(p.bb_gb_rate),
            "bb_fb_rate": float(p.bb_fb_rate),
            "bb_ld_rate": float(p.bb_ld_rate),
            "bb_pu_rate": float(p.bb_pu_rate),
            "bb_inplay_n": int(p.bb_inplay_n),
            "leverage_skill": float(p.leverage_skill),
        }

    team = roster.team
    lineup = roster.lineup

    return {
        "schema_version": _SCHEMA_VERSION,
        "team": {"team_id": int(team.team_id), "name": str(team.name), "abbreviation": str(team.abbreviation)},
        "manager": asdict(roster.manager),
        "lineup": {
            "batters": [ser_batter(b) for b in (lineup.batters or [])],
            "pitcher": ser_pitcher(lineup.pitcher),
            "bench": [ser_batter(b) for b in (lineup.bench or [])],
            "bullpen": [ser_pitcher(p) for p in (lineup.bullpen or [])],
        },
    }


def roster_from_dict(d: Dict[str, Any]) -> TeamRoster:
    """Deserialize a TeamRoster from a dict produced by roster_to_dict."""

    def de_player(p: Dict[str, Any]) -> Player:
        return Player(
            mlbam_id=int(p.get("mlbam_id") or 0),
            full_name=str(p.get("full_name") or ""),
            primary_position=str(p.get("primary_position") or ""),
            bat_side=_hand(p.get("bat_side"), Handedness.R),
            throw_side=_hand(p.get("throw_side"), Handedness.R),
        )

    def de_batter(b: Dict[str, Any]) -> BatterProfile:
        player = de_player(b.get("player") or {})
        prof = BatterProfile(player=player)
        for k in (
            "k_rate",
            "bb_rate",
            "hbp_rate",
            "hr_rate",
            "inplay_hit_rate",
            "xb_hit_share",
            "triple_share_of_xb",
            "sb_attempt_rate",
            "sb_success_rate",
            "bb_gb_rate",
            "bb_fb_rate",
            "bb_ld_rate",
            "bb_pu_rate",
        ):
            if k in b:
                try:
                    setattr(prof, k, float(b.get(k)))
                except Exception:
                    pass
        try:
            prof.bb_inplay_n = int(b.get("bb_inplay_n") or 0)
        except Exception:
            pass

        prof.vs_pitch_type = _de_pitchtype_map(b.get("vs_pitch_type") or {})
        prof.platoon_mult_vs_lhp = {str(k): float(v) for k, v in (b.get("platoon_mult_vs_lhp") or {}).items()}
        prof.platoon_mult_vs_rhp = {str(k): float(v) for k, v in (b.get("platoon_mult_vs_rhp") or {}).items()}
        prof.venue_mult_home = {str(k): float(v) for k, v in (b.get("venue_mult_home") or {}).items()}
        prof.venue_mult_away = {str(k): float(v) for k, v in (b.get("venue_mult_away") or {}).items()}
        prof.statcast_quality_mult = {str(k): float(v) for k, v in (b.get("statcast_quality_mult") or {}).items()}
        prof.vs_pitcher_hr_mult = _de_intkey_map(b.get("vs_pitcher_hr_mult") or {})
        prof.vs_pitcher_k_mult = _de_intkey_map(b.get("vs_pitcher_k_mult") or {})
        prof.vs_pitcher_bb_mult = _de_intkey_map(b.get("vs_pitcher_bb_mult") or {})
        prof.vs_pitcher_inplay_mult = _de_intkey_map(b.get("vs_pitcher_inplay_mult") or {})
        prof.vs_pitcher_history = _de_intkey_nested_map(b.get("vs_pitcher_history") or {})
        return prof

    def de_pitcher(p: Dict[str, Any]) -> PitcherProfile:
        player = de_player(p.get("player") or {})
        prof = PitcherProfile(player=player)
        for k in (
            "k_rate",
            "bb_rate",
            "hbp_rate",
            "hr_rate",
            "inplay_hit_rate",
            "batters_faced",
            "balls_in_play",
            "availability_mult",
            "bb_gb_rate",
            "bb_fb_rate",
            "bb_ld_rate",
            "bb_pu_rate",
            "leverage_skill",
        ):
            if k in p:
                try:
                    setattr(prof, k, float(p.get(k)))
                except Exception:
                    pass
        for k in ("statcast_splits_source", "statcast_splits_start_date", "statcast_splits_end_date", "arsenal_source", "role"):
            if k in p:
                try:
                    setattr(prof, k, str(p.get(k) or ""))
                except Exception:
                    pass
        for k in ("statcast_splits_n_pitches", "arsenal_sample_size", "stamina_pitches", "bb_inplay_n"):
            if k in p:
                try:
                    setattr(prof, k, int(p.get(k) or 0))
                except Exception:
                    pass

        prof.arsenal = _de_pitchtype_map(p.get("arsenal") or {})
        prof.pitch_type_whiff_mult = _de_pitchtype_map(p.get("pitch_type_whiff_mult") or {})
        prof.pitch_type_inplay_mult = _de_pitchtype_map(p.get("pitch_type_inplay_mult") or {})
        prof.platoon_mult_vs_lhb = {str(k): float(v) for k, v in (p.get("platoon_mult_vs_lhb") or {}).items()}
        prof.platoon_mult_vs_rhb = {str(k): float(v) for k, v in (p.get("platoon_mult_vs_rhb") or {}).items()}
        prof.venue_mult_home = {str(k): float(v) for k, v in (p.get("venue_mult_home") or {}).items()}
        prof.venue_mult_away = {str(k): float(v) for k, v in (p.get("venue_mult_away") or {}).items()}
        prof.statcast_quality_mult = {str(k): float(v) for k, v in (p.get("statcast_quality_mult") or {}).items()}
        return prof

    schema_v = int(d.get("schema_version") or 0)
    if schema_v != _SCHEMA_VERSION:
        raise ValueError(f"Unsupported roster artifact schema_version={schema_v}")

    t = d.get("team") or {}
    team = Team(team_id=int(t.get("team_id") or 0), name=str(t.get("name") or ""), abbreviation=str(t.get("abbreviation") or ""))

    mgr = d.get("manager") or {}
    manager = ManagerProfile(**{k: mgr[k] for k in mgr.keys()})

    lu = d.get("lineup") or {}
    batters = [de_batter(x) for x in (lu.get("batters") or [])]
    pitcher = de_pitcher(lu.get("pitcher") or {})
    bench = [de_batter(x) for x in (lu.get("bench") or [])]
    bullpen = [de_pitcher(x) for x in (lu.get("bullpen") or [])]

    lineup = Lineup(batters=batters, pitcher=pitcher, bench=bench, bullpen=bullpen)
    return TeamRoster(team=team, manager=manager, lineup=lineup)


def write_game_roster_artifact(
    path: Path,
    *,
    away_roster: TeamRoster,
    home_roster: TeamRoster,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {
        "schema_version": _SCHEMA_VERSION,
        "meta": meta or {},
        "away": roster_to_dict(away_roster),
        "home": roster_to_dict(home_roster),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_game_roster_artifact(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if int(obj.get("schema_version") or 0) != _SCHEMA_VERSION:
        raise ValueError(f"Unsupported game roster artifact schema_version={obj.get('schema_version')}")
    meta = obj.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    away = roster_from_dict(obj.get("away") or {})
    home = roster_from_dict(obj.get("home") or {})
    return {"away": away, "home": home, "meta": meta}
