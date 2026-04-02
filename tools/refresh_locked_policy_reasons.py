from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.daily_update_multi_profile import (
    _ROOT,
    _annotate_recommendation,
    _append_unique_reason,
    _hitter_bvp_reason,
    _hitter_opponent_team_reason,
    _hitter_pitch_mix_reason,
    _hitter_platoon_reason,
    _hitter_recent_form_reason,
    _iter_sim_records,
    _lookup_hitter_matchup_context,
    _pitch_mix_reason,
    _pitcher_bvp_reason,
    _pitcher_opponent_team_reason,
    _pitcher_recent_form_reason,
    _read_json,
    _rel,
    _safe_int,
    _season_from_date_str,
    _write_json,
    _opponent_lineup_reason,
)


def _load_roster_snapshot(date_str: str, game_pk: Any, game_number: Any) -> Optional[Dict[str, Any]]:
    snapshots_dir = _ROOT / "data" / "daily" / "snapshots" / str(date_str)
    if not snapshots_dir.exists():
        return None
    try:
        game_pk_int = int(game_pk or 0)
    except Exception:
        return None
    if game_pk_int <= 0:
        return None
    try:
        game_number_int = int(game_number or 1)
    except Exception:
        game_number_int = 1
    matches = sorted(snapshots_dir.glob(f"roster_*_pk{game_pk_int}_g{game_number_int}.json"))
    if not matches:
        matches = sorted(snapshots_dir.glob(f"roster_*_pk{game_pk_int}_g*.json"))
    if not matches:
        return None
    raw = _read_json(matches[0])
    return raw if isinstance(raw, dict) else None


def _sim_lookup(sim_dir: Path) -> Dict[Tuple[str, int, int], Dict[str, Any]]:
    lookup: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for sim_obj in _iter_sim_records(sim_dir):
        date_str = str(sim_obj.get("date") or "").strip()
        try:
            game_pk = int(sim_obj.get("game_pk") or 0)
        except Exception:
            continue
        try:
            game_number = int(((sim_obj.get("schedule") or {}).get("game_number") or 1))
        except Exception:
            game_number = 1
        if not date_str or game_pk <= 0:
            continue
        lookup[(date_str, game_pk, game_number)] = sim_obj
    return lookup


def _find_sim_obj(row: Dict[str, Any], lookup: Dict[Tuple[str, int, int], Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    date_str = str(row.get("date") or "").strip()
    game_pk = _safe_int(row.get("game_pk"))
    game_number = _safe_int(row.get("game_number")) or 1
    if not date_str or game_pk is None or int(game_pk) <= 0:
        return None
    sim_obj = lookup.get((date_str, int(game_pk), int(game_number)))
    if sim_obj is not None:
        return sim_obj
    for (sim_date, sim_game_pk, _), candidate in lookup.items():
        if sim_date == date_str and sim_game_pk == int(game_pk):
            return candidate
    return None


def _refresh_game_baseball_reasons(row: Dict[str, Any], sim_obj: Dict[str, Any], roster_snapshot: Optional[Dict[str, Any]]) -> List[str]:
    reasons: List[str] = []
    if not isinstance(roster_snapshot, dict):
        return reasons
    season_value = _season_from_date_str(row.get("date")) or _safe_int(sim_obj.get("season")) or 0
    market = str(row.get("market") or "")

    if market == "totals":
        for pitcher_side, opponent_side in (("home", "away"), ("away", "home")):
            side_doc = roster_snapshot.get(pitcher_side) if isinstance(roster_snapshot.get(pitcher_side), dict) else {}
            opp_doc = roster_snapshot.get(opponent_side) if isinstance(roster_snapshot.get(opponent_side), dict) else {}
            pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else {}
            opponent_lineup = opp_doc.get("lineup") if isinstance(opp_doc.get("lineup"), list) else []
            opponent_team = sim_obj.get(opponent_side) if isinstance(sim_obj.get(opponent_side), dict) else {}
            opponent_id = _safe_int(opponent_team.get("id"))
            opponent_label = str(opponent_team.get("abbreviation") or opponent_team.get("name") or "opponent").strip()
            subject_name = str(pitcher_profile.get("name") or "").strip() or None
            _append_unique_reason(
                reasons,
                _pitcher_opponent_team_reason(
                    pitcher_profile,
                    opponent_id,
                    opponent_label,
                    int(season_value),
                    "earned_runs",
                    subject_name=subject_name,
                ),
            )
            _append_unique_reason(reasons, _pitcher_bvp_reason(pitcher_profile, opponent_lineup))
            if len(reasons) >= 2:
                break
        if not reasons:
            for pitcher_side in ("home", "away"):
                side_doc = roster_snapshot.get(pitcher_side) if isinstance(roster_snapshot.get(pitcher_side), dict) else {}
                pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else {}
                subject_name = str(pitcher_profile.get("name") or "").strip() or None
                _append_unique_reason(
                    reasons,
                    _pitcher_recent_form_reason(
                        pitcher_profile,
                        int(season_value),
                        "earned_runs",
                        subject_name=subject_name,
                    ),
                )
                if len(reasons) >= 2:
                    break
        return reasons[:3]

    if market == "ml":
        selected_side = str(row.get("selection") or "home")
        opponent_side = "away" if selected_side == "home" else "home"
        side_doc = roster_snapshot.get(selected_side) if isinstance(roster_snapshot.get(selected_side), dict) else {}
        opp_doc = roster_snapshot.get(opponent_side) if isinstance(roster_snapshot.get(opponent_side), dict) else {}
        pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else {}
        opponent_lineup = opp_doc.get("lineup") if isinstance(opp_doc.get("lineup"), list) else []
        opponent_team = sim_obj.get(opponent_side) if isinstance(sim_obj.get(opponent_side), dict) else {}
        opponent_id = _safe_int(opponent_team.get("id"))
        opponent_label = str(opponent_team.get("abbreviation") or opponent_team.get("name") or "opponent").strip()
        subject_name = str(pitcher_profile.get("name") or "").strip() or None
        _append_unique_reason(
            reasons,
            _pitcher_opponent_team_reason(
                pitcher_profile,
                opponent_id,
                opponent_label,
                int(season_value),
                "earned_runs",
                subject_name=subject_name,
            ),
        )
        _append_unique_reason(reasons, _pitcher_bvp_reason(pitcher_profile, opponent_lineup))
        if not reasons:
            _append_unique_reason(
                reasons,
                _pitcher_recent_form_reason(
                    pitcher_profile,
                    int(season_value),
                    "earned_runs",
                    subject_name=subject_name,
                ),
            )
        return reasons[:3]

    return reasons


def _refresh_pitcher_baseball_reasons(row: Dict[str, Any], sim_obj: Dict[str, Any], roster_snapshot: Optional[Dict[str, Any]]) -> List[str]:
    reasons: List[str] = []
    if not isinstance(roster_snapshot, dict):
        return reasons
    side = str(row.get("team_side") or "").strip().lower()
    if side not in {"home", "away"}:
        return reasons
    season_value = _season_from_date_str(row.get("date")) or _safe_int(sim_obj.get("season")) or 0
    side_doc = roster_snapshot.get(side) if isinstance(roster_snapshot.get(side), dict) else {}
    opp_side = "home" if side == "away" else "away"
    opp_doc = roster_snapshot.get(opp_side) if isinstance(roster_snapshot.get(opp_side), dict) else {}
    pitcher_profile = side_doc.get("starter_profile") if isinstance(side_doc.get("starter_profile"), dict) else {}
    try:
        row_pitcher_id = int(row.get("pitcher_id") or 0)
    except Exception:
        row_pitcher_id = 0
    try:
        profile_pitcher_id = int(pitcher_profile.get("id") or 0)
    except Exception:
        profile_pitcher_id = 0
    if row_pitcher_id > 0 and profile_pitcher_id > 0 and row_pitcher_id != profile_pitcher_id:
        return reasons
    opponent_lineup = opp_doc.get("lineup") if isinstance(opp_doc.get("lineup"), list) else []
    opponent_team = sim_obj.get(opp_side) if isinstance(sim_obj.get(opp_side), dict) else {}
    opponent_id = _safe_int(opponent_team.get("id"))
    opponent_label = str(opponent_team.get("abbreviation") or opponent_team.get("name") or "opponent").strip()
    _append_unique_reason(reasons, _pitcher_bvp_reason(pitcher_profile, opponent_lineup))
    _append_unique_reason(
        reasons,
        _pitcher_opponent_team_reason(
            pitcher_profile,
            opponent_id,
            opponent_label,
            int(season_value),
            str(row.get("prop") or ""),
        ),
    )
    _append_unique_reason(
        reasons,
        _pitcher_recent_form_reason(
            pitcher_profile,
            int(season_value),
            str(row.get("prop") or ""),
        ),
    )
    _append_unique_reason(reasons, _pitch_mix_reason(pitcher_profile))
    if len(reasons) < 2:
        _append_unique_reason(reasons, _opponent_lineup_reason(pitcher_profile, opponent_lineup))
    return reasons[:3]


def _refresh_hitter_baseball_reasons(row: Dict[str, Any], sim_obj: Dict[str, Any], roster_snapshot: Optional[Dict[str, Any]]) -> List[str]:
    reasons: List[str] = []
    if not isinstance(roster_snapshot, dict):
        return reasons
    season_value = _season_from_date_str(row.get("date")) or _safe_int(sim_obj.get("season")) or 0
    rec = {
        "name": row.get("player_name"),
        "team": row.get("team"),
        "lineup_order": row.get("lineup_order"),
    }
    matchup_ctx = _lookup_hitter_matchup_context(sim_obj, rec, roster_snapshot)
    batter_profile = matchup_ctx.get("batter_profile") if isinstance(matchup_ctx.get("batter_profile"), dict) else None
    pitcher_profile = matchup_ctx.get("pitcher_profile") if isinstance(matchup_ctx.get("pitcher_profile"), dict) else None
    opponent_label = str(matchup_ctx.get("opponent") or "").strip()
    opponent_side = "home" if str(row.get("team") or "").strip().upper() == str((sim_obj.get("away") or {}).get("abbreviation") or "").strip().upper() else "away"
    opponent_team = sim_obj.get(opponent_side) if isinstance(sim_obj.get(opponent_side), dict) else {}
    opponent_team_id = _safe_int(matchup_ctx.get("opponent_team_id")) or _safe_int(opponent_team.get("id"))

    if isinstance(batter_profile, dict) and isinstance(pitcher_profile, dict):
        _append_unique_reason(
            reasons,
            _hitter_bvp_reason(
                batter_profile,
                pitcher_profile,
                season=int(season_value),
                prop=str(row.get("prop_market_key") or row.get("prop") or ""),
                selection=str(row.get("selection") or ""),
                line_value=float(row.get("market_line")) if row.get("market_line") is not None else None,
            ),
        )
    if isinstance(batter_profile, dict):
        _append_unique_reason(
            reasons,
            _hitter_opponent_team_reason(
                batter_profile,
                opponent_team_id,
                opponent_label,
                int(season_value),
                str(row.get("prop_market_key") or row.get("prop") or ""),
                selection=str(row.get("selection") or ""),
                line_value=float(row.get("market_line")) if row.get("market_line") is not None else None,
            ),
        )
        _append_unique_reason(
            reasons,
            _hitter_recent_form_reason(
                batter_profile,
                int(season_value),
                str(row.get("prop_market_key") or row.get("prop") or ""),
                selection=str(row.get("selection") or ""),
                line_value=float(row.get("market_line")) if row.get("market_line") is not None else None,
            ),
        )
    if isinstance(batter_profile, dict) and isinstance(pitcher_profile, dict):
        if len(reasons) < 2:
            _append_unique_reason(reasons, _hitter_pitch_mix_reason(batter_profile, pitcher_profile))
        if len(reasons) < 2:
            _append_unique_reason(reasons, _hitter_platoon_reason(batter_profile, pitcher_profile))
    return reasons[:3]


def _refresh_row_reasons(
    row: Dict[str, Any],
    game_lookup: Dict[Tuple[str, int, int], Dict[str, Any]],
    pitcher_lookup: Dict[Tuple[str, int, int], Dict[str, Any]],
    hitter_lookup: Dict[Tuple[str, int, int], Dict[str, Any]],
) -> Tuple[Dict[str, Any], bool]:
    market = str(row.get("market") or "")
    if market in {"totals", "ml"}:
        sim_obj = _find_sim_obj(row, game_lookup)
    elif market == "pitcher_props":
        sim_obj = _find_sim_obj(row, pitcher_lookup)
    else:
        sim_obj = _find_sim_obj(row, hitter_lookup)
    if not isinstance(sim_obj, dict):
        return dict(row), False
    roster_snapshot = _load_roster_snapshot(row.get("date"), row.get("game_pk"), row.get("game_number"))
    if market in {"totals", "ml"}:
        baseball_reasons = _refresh_game_baseball_reasons(row, sim_obj, roster_snapshot)
    elif market == "pitcher_props":
        baseball_reasons = _refresh_pitcher_baseball_reasons(row, sim_obj, roster_snapshot)
    else:
        baseball_reasons = _refresh_hitter_baseball_reasons(row, sim_obj, roster_snapshot)
    annotated = _annotate_recommendation({**row, "baseball_reasons": baseball_reasons})
    changed = (
        list(annotated.get("reasons") or []) != list(row.get("reasons") or [])
        or str(annotated.get("reason_summary") or "") != str(row.get("reason_summary") or "")
    )
    return annotated, changed


def _refresh_card(card: Dict[str, Any], date_str: str) -> Tuple[Dict[str, Any], int, int]:
    refreshed = copy.deepcopy(card)
    game_lookup = _sim_lookup(_ROOT / "data" / "daily" / "sims" / date_str)
    pitcher_lookup = _sim_lookup(_ROOT / "data" / "daily_pitcher_props" / "sims" / date_str)
    hitter_lookup = _sim_lookup(_ROOT / "data" / "daily_hitter_props" / "sims" / date_str)

    updated_rows = 0
    scanned_rows = 0
    markets = refreshed.get("markets")
    if not isinstance(markets, dict):
        return refreshed, scanned_rows, updated_rows

    for market_payload in markets.values():
        if not isinstance(market_payload, dict):
            continue
        for field_name in ("recommendations", "other_playable_candidates"):
            rows = market_payload.get(field_name)
            if not isinstance(rows, list):
                continue
            new_rows: List[Dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    new_rows.append(row)
                    continue
                scanned_rows += 1
                updated_row, changed = _refresh_row_reasons(row, game_lookup, pitcher_lookup, hitter_lookup)
                if changed:
                    updated_rows += 1
                new_rows.append(updated_row)
            market_payload[field_name] = new_rows
    return refreshed, scanned_rows, updated_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh only reason text for an existing locked-policy card.")
    parser.add_argument("--date", required=True, help="Slate date in YYYY-MM-DD format.")
    parser.add_argument(
        "--card-path",
        default="",
        help="Optional explicit locked-policy card path. Default: data/daily/daily_summary_<date>_locked_policy.json",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute changes without writing the card.")
    args = parser.parse_args()

    token = str(args.date).replace("-", "_")
    card_path = Path(args.card_path).resolve() if str(args.card_path).strip() else (_ROOT / "data" / "daily" / f"daily_summary_{token}_locked_policy.json")
    card = _read_json(card_path)
    if not isinstance(card, dict):
        raise SystemExit(f"Locked-policy card is missing or unreadable: {_rel(card_path)}")

    refreshed, scanned_rows, updated_rows = _refresh_card(card, str(args.date))
    if not args.dry_run:
        _write_json(card_path, refreshed)

    action = "Would update" if args.dry_run else "Updated"
    print(f"{action} {updated_rows} of {scanned_rows} rows in {_rel(card_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())