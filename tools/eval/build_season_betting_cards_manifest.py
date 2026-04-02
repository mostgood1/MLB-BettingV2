from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.market_pitcher_props import normalize_pitcher_name
from sim_engine.prob_calibration import apply_prob_calibration, apply_prop_prob_calibration
from tools.daily_update_multi_profile import (
    DEFAULT_HITTER_STAKE_U,
    DEFAULT_LOCK_POLICY,
    DEFAULT_OFFICIAL_CAPS,
    DEFAULT_OFFICIAL_CAP_PROFILE,
    DEFAULT_OFFICIAL_HITTER_SUBCAPS,
    DEFAULT_STANDARD_STAKE_U,
    HITTER_MARKET_ORDER,
    HITTER_MARKET_SPECS,
    HITTER_PREDICTION_FIELDS,
    PITCHER_MARKET_SPECS,
    _cap_text,
    _annotate_recommendation,
    _get_hitter_prob,
    _has_hitter_subcaps,
    _hitter_edge_min_for_market,
    _hitter_edge_min_overrides,
    _is_hitter_prediction_eligible,
    _iter_pitcher_market_names,
    _locked_policy_selected_counts,
    _mean_support_for_selection,
    _no_vig_two_way,
    _normalized_hitter_subcaps,
    _normalized_official_caps,
    _normalized_pitcher_market,
    _official_cap_profile_name,
    _passes_mean_alignment,
    _policy_with_overrides,
    _prob_over_line_from_dist,
    _rank_and_cap,
    _rank_and_cap_unique_players,
    _selected_side_prob_from_over_prob,
    _selection_allowed,
    _select_hitter_props_market,
    _select_hitter_recommendations,
    _select_moneyline_side,
    _select_market_side,
    _selected_player_keys,
    _subtract_selected_rows,
)
from tools.eval.settle_locked_policy_cards import _combined_summary, _settle_card


SETTLED_MARKET_ORDER: Tuple[str, ...] = (
    "totals",
    "ml",
    "pitcher_props",
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
)


def _settlement_player_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _settlement_line_key(value: Any) -> Optional[float]:
    line = _safe_float(value)
    if line is None:
        return None
    return round(float(line), 4)


def _settlement_market_key(item: Dict[str, Any]) -> str:
    market = str(item.get("market") or "").strip().lower()
    prop = str(item.get("prop") or "").strip().lower()
    if market == "hitter_props" and prop in HITTER_MARKET_ORDER:
        return prop
    return market


def _settlement_lookup_key(item: Dict[str, Any]) -> Tuple[Optional[int], str, str, Optional[float], str]:
    market = _settlement_market_key(item)
    line_key = _settlement_line_key(item.get("market_line"))
    player_key = _settlement_player_key(item.get("player_name") or item.get("pitcher_name"))
    if market == "ml":
        line_key = None
        player_key = ""
    elif market == "totals":
        player_key = ""
    return (
        _safe_int(item.get("game_pk")),
        market,
        str(item.get("selection") or "").strip().lower(),
        line_key,
        player_key,
    )


def _annotated_reco(
    reco: Dict[str, Any],
    settled_lookup: Dict[Tuple[Optional[int], str, str, Optional[float], str], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    item = dict(reco)
    matches = settled_lookup.get(_settlement_lookup_key(item)) or []
    if matches:
        item["settlement"] = dict(matches.pop(0))
    return item


def _recommendations_by_game(card: Optional[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    if not isinstance(card, dict):
        return {}

    grouped: Dict[int, Dict[str, Any]] = defaultdict(
        lambda: {
            "totals": None,
            "ml": None,
            "pitcher_props": [],
            "hitter_props": [],
            "extra_pitcher_props": [],
            "extra_hitter_props": [],
        }
    )
    markets = card.get("markets") or {}
    if not isinstance(markets, dict):
        return {}

    def _append_reco(bucket: Dict[str, Any], market_name: str, reco: Dict[str, Any], *, tier: str) -> None:
        item = dict(reco)
        item["recommendation_tier"] = tier
        if market_name == "totals":
            bucket["totals"] = item
        elif market_name == "ml":
            bucket["ml"] = item
        elif market_name == "pitcher_props":
            bucket["extra_pitcher_props" if tier == "candidate" else "pitcher_props"].append(item)
        else:
            bucket["extra_hitter_props" if tier == "candidate" else "hitter_props"].append(item)

    for market_name, section in markets.items():
        if not isinstance(section, dict):
            continue
        recos = section.get("recommendations") or []
        extra_recos = section.get("other_playable_candidates") or []
        if isinstance(recos, list):
            for reco in recos:
                if not isinstance(reco, dict):
                    continue
                game_pk = _safe_int(reco.get("game_pk"))
                if not game_pk or int(game_pk) <= 0:
                    continue
                _append_reco(grouped[int(game_pk)], str(market_name), reco, tier="official")
        if isinstance(extra_recos, list):
            for reco in extra_recos:
                if not isinstance(reco, dict):
                    continue
                game_pk = _safe_int(reco.get("game_pk"))
                if not game_pk or int(game_pk) <= 0:
                    continue
                _append_reco(grouped[int(game_pk)], str(market_name), reco, tier="candidate")

    for bucket in grouped.values():
        bucket["pitcher_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
        bucket["hitter_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
        bucket["extra_pitcher_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
        bucket["extra_hitter_props"].sort(key=lambda reco: (_safe_int(reco.get("rank")) or 9999, -(reco.get("edge") or 0.0)))
    return dict(grouped)


def _season_betting_games_payload(card: Dict[str, Any], settled_card: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    recos_by_game = _recommendations_by_game(card)
    settled_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    playable_settled_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    unresolved_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    playable_unresolved_rows_by_game: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    settled_lookup: Dict[Tuple[Optional[int], str, str, Optional[float], str], List[Dict[str, Any]]] = defaultdict(list)
    playable_lookup: Dict[Tuple[Optional[int], str, str, Optional[float], str], List[Dict[str, Any]]] = defaultdict(list)

    for row in settled_card.get("_settled_rows") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            settled_rows_by_game[int(game_pk)].append(dict(row))
        settled_lookup[_settlement_lookup_key(row)].append(dict(row))

    for row in settled_card.get("_playable_settled_rows") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            playable_settled_rows_by_game[int(game_pk)].append(dict(row))
        playable_lookup[_settlement_lookup_key(row)].append(dict(row))

    for row in settled_card.get("unresolved_recommendations") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            unresolved_rows_by_game[int(game_pk)].append(dict(row))

    for row in settled_card.get("playable_unresolved_recommendations") or []:
        if not isinstance(row, dict):
            continue
        game_pk = _safe_int(row.get("game_pk"))
        if game_pk and int(game_pk) > 0:
            playable_unresolved_rows_by_game[int(game_pk)].append(dict(row))

    out: Dict[int, Dict[str, Any]] = {}
    for game_pk, bucket in recos_by_game.items():
        totals = bucket.get("totals")
        ml = bucket.get("ml")
        totals_item = _annotated_reco(totals, settled_lookup) if isinstance(totals, dict) else None
        ml_item = _annotated_reco(ml, settled_lookup) if isinstance(ml, dict) else None
        pitcher_props = [_annotated_reco(row, settled_lookup) for row in (bucket.get("pitcher_props") or [])]
        hitter_props = [_annotated_reco(row, settled_lookup) for row in (bucket.get("hitter_props") or [])]
        extra_pitcher_props = [_annotated_reco(row, playable_lookup) for row in (bucket.get("extra_pitcher_props") or [])]
        extra_hitter_props = [_annotated_reco(row, playable_lookup) for row in (bucket.get("extra_hitter_props") or [])]
        settled_rows = list(settled_rows_by_game.get(int(game_pk), []))
        playable_settled_rows = list(playable_settled_rows_by_game.get(int(game_pk), []))
        all_settled_rows = list(settled_rows) + list(playable_settled_rows)
        unresolved_rows = list(unresolved_rows_by_game.get(int(game_pk), []))
        playable_unresolved_rows = list(playable_unresolved_rows_by_game.get(int(game_pk), []))
        all_unresolved_rows = list(unresolved_rows) + list(playable_unresolved_rows)
        official_count = int(bool(totals_item)) + int(bool(ml_item)) + len(pitcher_props) + len(hitter_props)
        playable_count = len(extra_pitcher_props) + len(extra_hitter_props)

        out[int(game_pk)] = {
            "markets": {
                "totals": totals_item,
                "ml": ml_item,
                "pitcherProps": pitcher_props,
                "hitterProps": hitter_props,
                "extraPitcherProps": extra_pitcher_props,
                "extraHitterProps": extra_hitter_props,
            },
            "results": _results_from_rows(settled_rows),
            "playable_results": _results_from_rows(playable_settled_rows),
            "all_results": _results_from_rows(all_settled_rows),
            "settled_rows": settled_rows,
            "playable_settled_rows": playable_settled_rows,
            "all_settled_rows": all_settled_rows,
            "unresolved_rows": unresolved_rows,
            "playable_unresolved_rows": playable_unresolved_rows,
            "all_unresolved_rows": all_unresolved_rows,
            "counts": {
                "official": int(official_count),
                "playable": int(playable_count),
                "pitcher": int(len(pitcher_props)),
                "hitter": int(len(hitter_props)),
                "extra_pitcher": int(len(extra_pitcher_props)),
                "extra_hitter": int(len(extra_hitter_props)),
            },
            "flags": {
                "hasAnyRecommendations": bool(official_count or playable_count),
                "hasOfficialRecommendations": bool(official_count),
                "hasPlayableCandidates": bool(playable_count),
            },
        }
    return out


def _day_payload_output_path(payload_dir: Path, date_str: str) -> Path:
    token = str(date_str or "").replace("-", "_")
    return payload_dir / f"season_betting_day_{token}.json"


def _static_day_payload(
    *,
    season: int,
    profile_name: str,
    card_path: Path,
    report_path: Path,
    card: Dict[str, Any],
    settled_card: Dict[str, Any],
    summary: Dict[str, Any],
    payload_path: Path,
) -> Dict[str, Any]:
    return {
        "season": int(season),
        "date": str(card.get("date") or ""),
        "profile": str(profile_name or "retuned"),
        "available_profiles": [str(profile_name or "retuned")],
        "found": True,
        "source_kind": "season_manifest_static",
        "card_source": _relative_path_str(card_path),
        "report_source": _relative_path_str(report_path),
        "payload_source": _relative_path_str(payload_path),
        "summary": dict(summary),
        "cap_profile": card.get("cap_profile"),
        "selected_counts": dict(settled_card.get("selected_counts") or summary.get("selected_counts") or {}),
        "playable_selected_counts": dict(settled_card.get("playable_selected_counts") or {}),
        "all_selected_counts": dict(settled_card.get("all_selected_counts") or {}),
        "results": dict(settled_card.get("results") or {}),
        "playable_results": dict(settled_card.get("playable_results") or {}),
        "all_results": dict(settled_card.get("all_results") or {}),
        "games": _season_betting_games_payload(card, settled_card),
    }


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_json_dict(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        obj = _read_json(path)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _relative_path_str(path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(_ROOT.resolve())).replace("\\", "/")
    except Exception:
        return str(path.resolve()).replace("\\", "/")


def _blank_summary() -> Dict[str, Any]:
    return {
        "n": 0,
        "wins": 0,
        "losses": 0,
        "stake_u": 0.0,
        "profit_u": 0.0,
        "roi": None,
    }


def _summary_from_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    stake_u = sum(float(row.get("stake_u") or 0.0) for row in rows)
    profit_u = sum(float(row.get("profit_u") or 0.0) for row in rows)
    wins = sum(1 for row in rows if row.get("result") == "win")
    count = len(rows)
    return {
        "n": int(count),
        "wins": int(wins),
        "losses": int(count - wins),
        "stake_u": round(float(stake_u), 4),
        "profit_u": round(float(profit_u), 4),
        "roi": round(float(profit_u) / float(stake_u), 4) if float(stake_u) > 0.0 else None,
    }


def _rows_by_market(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        market_name = str(row.get("market") or "").strip()
        if market_name:
            grouped[market_name].append(row)
    return grouped


def _results_from_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    grouped = _rows_by_market(rows)
    results: Dict[str, Any] = {
        market_name: _summary_from_rows(grouped.get(market_name, []))
        for market_name in SETTLED_MARKET_ORDER
    }
    hitter_rows = [row for row in rows if str(row.get("market") or "") in HITTER_MARKET_ORDER]
    results["hitter_props"] = _summary_from_rows(hitter_rows)
    results["combined"] = _summary_from_rows(rows)
    return results


def _month_label(month_key: str) -> str:
    try:
        return datetime.strptime(month_key, "%Y-%m").strftime("%b %Y")
    except Exception:
        return str(month_key)


def _median(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    n = len(ordered)
    mid = n // 2
    if (n % 2) == 1:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def _stddev(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    mean_value = float(sum(values) / len(values))
    variance = sum((float(value) - mean_value) ** 2 for value in values) / len(values)
    return float(math.sqrt(variance))


def _daily_stats(day_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    profits = [float(row.get("profit_u") or 0.0) for row in day_rows]
    cards_with_bets = [row for row in day_rows if int((row.get("results") or {}).get("combined", {}).get("n") or 0) > 0]
    best = max(day_rows, key=lambda row: float(row.get("profit_u") or 0.0), default=None)
    worst = min(day_rows, key=lambda row: float(row.get("profit_u") or 0.0), default=None)
    return {
        "cards": int(len(day_rows)),
        "cards_with_bets": int(len(cards_with_bets)),
        "cards_without_bets": int(len(day_rows) - len(cards_with_bets)),
        "positive_days": int(sum(1 for value in profits if value > 0.0)),
        "negative_days": int(sum(1 for value in profits if value < 0.0)),
        "flat_days": int(sum(1 for value in profits if abs(value) <= 1e-12)),
        "mean_u": round(float(sum(profits) / len(profits)), 4) if profits else None,
        "median_u": round(float(_median(profits) or 0.0), 4) if profits else None,
        "std_u": round(float(_stddev(profits) or 0.0), 4) if profits else None,
        "best_day": (
            {
                "date": str(best.get("date") or ""),
                "profit_u": round(float(best.get("profit_u") or 0.0), 4),
            }
            if best is not None
            else {"date": None, "profit_u": None}
        ),
        "worst_day": (
            {
                "date": str(worst.get("date") or ""),
                "profit_u": round(float(worst.get("profit_u") or 0.0), 4),
            }
            if worst is not None
            else {"date": None, "profit_u": None}
        ),
    }


def _resolve_path(value: str, *, default: Path) -> Path:
    raw = str(value or "").strip()
    path = Path(raw) if raw else default
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


def _override_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in updates.items():
        if value is None:
            continue
        out[str(key)] = value
    return out


def _iter_report_paths(batch_dir: Path, selected_dates: Sequence[str], max_days: int) -> List[Path]:
    wanted = {str(date_value).strip() for date_value in selected_dates if str(date_value).strip()}
    paths = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    if wanted:
        paths = [path for path in paths if path.stem.replace("sim_vs_actual_", "") in wanted]
    if max_days > 0:
        paths = paths[: int(max_days)]
    return paths


def _date_slug(date_str: str) -> str:
    return str(date_str or "").strip().replace("-", "_")


def _canonical_daily_card_path(date_str: str) -> Optional[Path]:
    candidate = (_ROOT / "data" / "daily" / f"daily_summary_{_date_slug(date_str)}_locked_policy.json").resolve()
    if candidate.exists() and candidate.is_file():
        return candidate
    return None


def _authoritative_source_mode(preferred_canonical: bool, source_modes: Sequence[str]) -> str:
    unique = {str(mode or "").strip() for mode in source_modes if str(mode or "").strip()}
    if not unique:
        return "season_eval_batch_reconstruction"
    if unique == {"canonical_daily_locked_policy"}:
        return "canonical_daily_locked_policy"
    if unique == {"season_eval_batch_reconstruction"}:
        return "season_eval_batch_reconstruction"
    if preferred_canonical:
        return "mixed_authoritative_daily"
    return "mixed_source"


def _day_token(date_str: str) -> str:
    return str(date_str).strip().replace("-", "_")


def _odds_paths(date_str: str) -> Dict[str, Path]:
    token = _day_token(date_str)
    odds_root = _ROOT / "data" / "market" / "oddsapi"
    return {
        "game_lines": odds_root / f"oddsapi_game_lines_{token}.json",
        "hitter_lines": odds_root / f"oddsapi_hitter_props_{token}.json",
        "pitcher_lines": odds_root / f"oddsapi_pitcher_props_{token}.json",
    }


def _base_game_row_from_report(date_str: str, game: Dict[str, Any], market_game: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    away = game.get("away") or {}
    home = game.get("home") or {}
    return {
        "date": str(date_str),
        "game_pk": _safe_int(game.get("game_pk")),
        "away": str(away.get("name") or ""),
        "home": str(home.get("name") or ""),
        "away_abbr": str(away.get("abbr") or ""),
        "home_abbr": str(home.get("abbr") or ""),
        "double_header": None,
        "game_number": None,
        "event_id": (market_game or {}).get("event_id"),
        "commence_time": (market_game or {}).get("commence_time"),
    }


def _load_game_lines_lookup(path: Path) -> Dict[Tuple[str, str], Dict[str, Any]]:
    doc = _read_json_dict(path)
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in doc.get("games") or []:
        if not isinstance(row, dict):
            continue
        away_team = str(row.get("away_team") or "").strip()
        home_team = str(row.get("home_team") or "").strip()
        if not away_team or not home_team:
            continue
        out[(away_team, home_team)] = row
    return out


def _lineup_id_sets(game: Dict[str, Any]) -> Dict[str, set[int]]:
    out: Dict[str, set[int]] = {"away": set(), "home": set()}
    for key in ("confirmed_lineup_ids", "projected_lineup_ids"):
        block = game.get(key) or {}
        if not isinstance(block, dict):
            continue
        for side in ("away", "home"):
            for raw_value in block.get(side) or []:
                parsed = _safe_int(raw_value)
                if parsed and parsed > 0:
                    out[side].add(int(parsed))
    return out


def _batter_side(game: Dict[str, Any], batter_id: int) -> Optional[str]:
    lineup_sets = _lineup_id_sets(game)
    in_away = int(batter_id) in lineup_sets["away"]
    in_home = int(batter_id) in lineup_sets["home"]
    if in_away and not in_home:
        return "away"
    if in_home and not in_away:
        return "home"
    if in_away:
        return "away"
    if in_home:
        return "home"
    return None


def _extract_report_hitter_predictions(game: Dict[str, Any], hr_calibration: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    pred: Dict[str, Dict[str, Any]] = {}

    def _rec_for(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        name = str(row.get("name") or "").strip()
        if not name:
            return None
        batter_id = _safe_int(row.get("batter_id"))
        if not batter_id or batter_id <= 0:
            return None
        player_key = normalize_pitcher_name(name)
        if not player_key:
            return None
        rec = pred.setdefault(
            player_key,
            {
                "name": name,
                "batter_id": int(batter_id),
            },
        )
        side = _batter_side(game, int(batter_id))
        if side:
            team = (game.get(side) or {})
            rec["team"] = str(team.get("abbr") or team.get("name") or "")
            rec["team_name"] = str(team.get("name") or "")
            rec["team_side"] = str(side)
            rec["is_lineup_batter"] = True
        else:
            rec.setdefault("is_lineup_batter", False)
        for key in ("ab_mean", "pa_mean"):
            value = _safe_float(row.get(key))
            if value is None:
                continue
            prev = _safe_float(rec.get(key))
            if prev is None or float(value) > float(prev):
                rec[key] = float(value)
        return rec

    props_block = game.get("hitter_props_likelihood") or {}
    if isinstance(props_block, dict):
        for prop_key, (cal_key, raw_key) in HITTER_PREDICTION_FIELDS.items():
            rows = props_block.get(prop_key) or []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                rec = _rec_for(row)
                if rec is None:
                    continue
                value = row.get(cal_key, row.get(raw_key))
                parsed = _safe_float(value)
                if parsed is not None:
                    rec[prop_key] = float(parsed)
                hr_raw = _safe_float(row.get("p_hr_1plus"))
                if hr_raw is not None:
                    hr_cal = float(apply_prop_prob_calibration(float(hr_raw), hr_calibration, prop_key="hr_1plus"))
                    prev_hr = _safe_float(rec.get("hr_1plus"))
                    if prev_hr is None or float(hr_cal) > float(prev_hr):
                        rec["hr_1plus"] = float(hr_cal)

    hr_block = game.get("hitter_hr_likelihood") or {}
    overall = hr_block.get("overall") if isinstance(hr_block, dict) else []
    if isinstance(overall, list):
        for row in overall:
            if not isinstance(row, dict):
                continue
            rec = _rec_for(row)
            if rec is None:
                continue
            hr_value = row.get("p_hr_1plus_cal", row.get("p_hr_1plus"))
            parsed = _safe_float(hr_value)
            if parsed is not None:
                rec["hr_1plus"] = float(parsed)

    return pred


def _collect_report_game_recommendations(
    report_obj: Dict[str, Any],
    policy: Dict[str, Any],
    warnings: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {"totals": [], "ml": []}
    date_str = str((report_obj.get("meta") or {}).get("date") or "").strip()
    if not date_str:
        warnings.append("Missing report date in meta")
        return out

    odds_paths = _odds_paths(date_str)
    game_lines_path = odds_paths["game_lines"]
    if not game_lines_path.exists():
        warnings.append(f"Missing game lines: {_relative_path_str(game_lines_path)}")
        return out

    line_lookup = _load_game_lines_lookup(game_lines_path)
    for game in report_obj.get("games") or []:
        if not isinstance(game, dict):
            continue
        away_name = str(((game.get("away") or {}).get("name") or "")).strip()
        home_name = str(((game.get("home") or {}).get("name") or "")).strip()
        if not away_name or not home_name:
            continue
        market_game = line_lookup.get((away_name, home_name))
        if not isinstance(market_game, dict):
            warnings.append(f"Missing game-line match for {away_name} at {home_name}")
            continue

        base = _base_game_row_from_report(date_str, game, market_game)
        full = ((game.get("segments") or {}).get("full") or {})
        totals_market = ((market_game.get("markets") or {}).get("totals") or {})
        total_line = _safe_float(totals_market.get("line"))
        mean_total = _safe_float(full.get("mean_total_runs"))
        p_over_total = None
        if total_line is not None:
            p_over_total = _prob_over_line_from_dist(full.get("total_runs_dist") or {}, float(total_line))
        if total_line is not None and mean_total is not None and p_over_total is not None:
            side_pick = _select_market_side(
                float(p_over_total),
                totals_market.get("over_odds"),
                totals_market.get("under_odds"),
                float(policy.get("totals_edge_min") or 0.0),
            )
            if side_pick is not None and _selection_allowed(side_pick.get("selection"), policy.get("totals_side")):
                selection = str(side_pick.get("selection") or "")
                if _passes_mean_alignment(mean_total, total_line, selection, policy.get("totals_diff_min")):
                    out["totals"].append(
                        _annotate_recommendation(
                            {
                                **base,
                                "market": "totals",
                                "selection": selection,
                                "edge": float(side_pick["edge"]),
                                "market_line": float(total_line),
                                "model_mean_total": float(mean_total),
                                "model_prob_over": float(p_over_total),
                                "market_prob_over": side_pick.get("market_prob_over"),
                                "market_prob_under": side_pick.get("market_prob_under"),
                                "market_prob_mode": side_pick.get("market_prob_mode"),
                                "market_no_vig_prob_over": side_pick.get("market_no_vig_prob_over"),
                                "selected_side_market_prob": side_pick.get("selected_side_market_prob"),
                                "selected_side_model_prob": _selected_side_prob_from_over_prob(p_over_total, selection),
                                "mean_support": _mean_support_for_selection(mean_total, total_line, selection),
                                "odds": side_pick.get("odds"),
                                "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                            }
                        )
                    )

        h2h_market = ((market_game.get("markets") or {}).get("h2h") or {})
        home_prob = _safe_float(full.get("home_win_prob"))
        away_prob = _safe_float(full.get("away_win_prob"))
        if home_prob is None or away_prob is None:
            continue
        denom = float(home_prob + away_prob)
        if denom <= 0.0:
            continue
        home_prob = float(home_prob) / denom
        side_pick = _select_moneyline_side(
            home_prob,
            h2h_market.get("home_odds"),
            h2h_market.get("away_odds"),
            float(policy["ml_edge_min"]),
            policy.get("ml_side"),
        )
        if side_pick is not None:
            out["ml"].append(
                _annotate_recommendation(
                    {
                        **base,
                        "market": "ml",
                        "selection": str(side_pick.get("selection") or "home"),
                        "edge": float(side_pick["edge"]),
                        "model_prob": float(home_prob),
                        "selected_side_model_prob": side_pick.get("selected_side_model_prob"),
                        "selected_side_market_prob": side_pick.get("selected_side_market_prob"),
                        "market_no_vig_prob": side_pick.get("market_no_vig_prob"),
                        "odds": side_pick.get("odds"),
                        "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                    }
                )
            )

    return out


def _collect_report_pitcher_recommendations(
    report_obj: Dict[str, Any],
    policy: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    date_str = str((report_obj.get("meta") or {}).get("date") or "").strip()
    if not date_str:
        return rows
    outs_prob_calibration = ((report_obj.get("meta") or {}).get("outs_prob_calibration") or {})
    so_prob_calibration = ((report_obj.get("meta") or {}).get("so_prob_calibration") or {})

    for game in report_obj.get("games") or []:
        if not isinstance(game, dict):
            continue
        base = _base_game_row_from_report(date_str, game)
        pitcher_props = game.get("pitcher_props") or {}
        starter_names = game.get("starter_names") or {}

        for side in ("away", "home"):
            side_props = pitcher_props.get(side) or {}
            pred = side_props.get("pred") or {}
            market = side_props.get("market") or {}
            for market_name in _iter_pitcher_market_names(policy):
                market_spec = PITCHER_MARKET_SPECS.get(str(market_name)) or {}
                market_key = str(market_spec.get("market_key") or "")
                props_market = market.get(market_key) or {}
                line_value = _safe_float(props_market.get("line"))
                if line_value is None:
                    continue
                dist_key = str(market_spec.get("dist_key") or "")
                p_raw = _prob_over_line_from_dist(pred.get(dist_key) or {}, float(line_value))
                if p_raw is None:
                    continue
                calibration = so_prob_calibration if str(market_name) == "strikeouts" else outs_prob_calibration
                p_over = float(apply_prob_calibration(float(p_raw), calibration))
                side_pick = _select_market_side(
                    p_over,
                    props_market.get("over_odds"),
                    props_market.get("under_odds"),
                    float(policy["pitcher_edge_min"]),
                )
                if side_pick is None or not _selection_allowed(side_pick.get("selection"), policy.get("pitcher_side")):
                    continue
                mean_key = str(market_spec.get("mean_key") or "")
                mean_value = pred.get(mean_key)
                if not _passes_mean_alignment(mean_value, line_value, side_pick.get("selection"), 0.0):
                    continue
                rows.append(
                    _annotate_recommendation(
                        {
                            **base,
                            "market": "pitcher_props",
                            "pitcher_name": str(starter_names.get(side) or ""),
                            "team": str(((game.get(side) or {}).get("abbr") or (game.get(side) or {}).get("name") or "")),
                            "team_side": str(side),
                            "prop": str(market_name),
                            "selection": str(side_pick.get("selection") or ""),
                            "edge": float(side_pick["edge"]),
                            "market_line": float(line_value),
                            "model_prob_over": float(p_over),
                            "market_prob_over": side_pick.get("market_prob_over"),
                            "market_prob_under": side_pick.get("market_prob_under"),
                            "market_prob_mode": side_pick.get("market_prob_mode"),
                            "market_no_vig_prob_over": side_pick.get("market_no_vig_prob_over"),
                            "selected_side_market_prob": side_pick.get("selected_side_market_prob"),
                            "selected_side_model_prob": _selected_side_prob_from_over_prob(p_over, side_pick.get("selection")),
                            "mean_support": _mean_support_for_selection(mean_value, line_value, side_pick.get("selection")),
                            mean_key: mean_value,
                            "market_alternates": list(props_market.get("alternates") or []),
                            "odds": side_pick.get("odds"),
                            "stake_u": float(DEFAULT_STANDARD_STAKE_U),
                        }
                    )
                )

    return rows


def _collect_report_hitter_recommendations(
    report_obj: Dict[str, Any],
    policy: Dict[str, Any],
    warnings: List[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    date_str = str((report_obj.get("meta") or {}).get("date") or "").strip()
    if not date_str:
        warnings.append("Missing report date in meta")
        return rows

    odds_paths = _odds_paths(date_str)
    hitter_lines_path = odds_paths["hitter_lines"]
    if not hitter_lines_path.exists():
        warnings.append(f"Missing hitter lines: {_relative_path_str(hitter_lines_path)}")
        return rows

    hitter_odds_doc = _read_json_dict(hitter_lines_path)
    hitter_odds = {
        normalize_pitcher_name(str(name)): markets
        for name, markets in ((hitter_odds_doc.get("hitter_props") or {}) or {}).items()
        if normalize_pitcher_name(str(name))
    }
    hr_calibration = ((report_obj.get("meta") or {}).get("hitter_hr_prob_calibration") or {})
    skipped_unmapped = 0

    for game in report_obj.get("games") or []:
        if not isinstance(game, dict):
            continue
        pred = _extract_report_hitter_predictions(game, hr_calibration)
        if not pred:
            continue
        base = _base_game_row_from_report(date_str, game)

        for player_key, rec in pred.items():
            if not _is_hitter_prediction_eligible(rec):
                continue
            if not str(rec.get("team") or "").strip():
                skipped_unmapped += 1
                continue
            markets = hitter_odds.get(player_key)
            if not isinstance(markets, dict):
                continue
            for market_key, market_spec in HITTER_MARKET_SPECS.items():
                props_market = _select_hitter_props_market(str(market_key), markets.get(market_key) or {})
                line_value = _safe_float(props_market.get("line"))
                if line_value is None:
                    continue
                p_over = _get_hitter_prob(str(market_key), float(line_value), rec)
                if p_over is None:
                    continue
                side_pick = _select_market_side(
                    float(p_over),
                    props_market.get("over_odds"),
                    props_market.get("under_odds"),
                    _hitter_edge_min_for_market(policy, str(market_spec["market"])),
                )
                if side_pick is None:
                    continue
                mean_value = rec.get(str(market_spec.get("mean_key") or ""))
                if not _passes_mean_alignment(mean_value, line_value, side_pick["selection"], 0.0):
                    continue
                rows.append(
                    _annotate_recommendation(
                        {
                            **base,
                            "market": str(market_spec["market"]),
                            "market_label": str(market_spec["label"]),
                            "market_group": "hitter_props",
                            "player_name": rec.get("name"),
                            "team": rec.get("team"),
                            "team_side": rec.get("team_side"),
                            "batter_id": rec.get("batter_id"),
                            "prop": str(market_key),
                            "prop_market_key": str(market_key),
                            "selection": side_pick["selection"],
                            "edge": float(side_pick["edge"]),
                            "market_line": float(line_value),
                            "model_prob_over": float(p_over),
                            "market_prob_over": side_pick["market_prob_over"],
                            "market_prob_under": side_pick["market_prob_under"],
                            "market_prob_mode": side_pick["market_prob_mode"],
                            "market_no_vig_prob_over": side_pick["market_no_vig_prob_over"],
                            "selected_side_market_prob": float(side_pick["selected_side_market_prob"]),
                            "selected_side_model_prob": _selected_side_prob_from_over_prob(p_over, side_pick["selection"]),
                            "mean_support": _mean_support_for_selection(mean_value, line_value, side_pick["selection"]),
                            str(market_spec.get("mean_key") or ""): mean_value,
                            "pa_mean": rec.get("pa_mean"),
                            "ab_mean": rec.get("ab_mean"),
                            "lineup_order": rec.get("lineup_order"),
                            "market_alternates": list(props_market.get("alternates") or []),
                            "odds": side_pick["odds"],
                            "stake_u": float(DEFAULT_HITTER_STAKE_U),
                        }
                    )
                )

    if skipped_unmapped > 0:
        warnings.append(f"Skipped {int(skipped_unmapped)} hitter candidate rows with no inferred lineup team")
    return rows


def _card_output_paths(cards_dir: Path, date_str: str) -> Path:
    token = _day_token(date_str)
    return cards_dir / f"daily_summary_{token}_locked_policy.json"


def _build_card_from_report(
    report_path: Path,
    report_obj: Dict[str, Any],
    *,
    batch_dir: Path,
    cards_dir: Path,
    policy: Dict[str, Any],
    market_caps: Dict[str, Optional[int]],
    hitter_subcaps: Dict[str, Optional[int]],
) -> Tuple[Path, Dict[str, Any]]:
    meta = report_obj.get("meta") or {}
    date_str = str(meta.get("date") or "").strip()
    season = int(_safe_int(meta.get("season")) or 0)
    caps = _normalized_official_caps(market_caps)
    normalized_hitter_subcaps = _normalized_hitter_subcaps(hitter_subcaps)
    hitter_selection_mode = "submarket_caps" if _has_hitter_subcaps(normalized_hitter_subcaps) else "shared_cap"
    cap_profile = _official_cap_profile_name(caps, normalized_hitter_subcaps)

    warnings: List[str] = []
    raw_game_rows = _collect_report_game_recommendations(report_obj, policy, warnings)
    pitcher_rows = _collect_report_pitcher_recommendations(report_obj, policy)
    hitter_rows = _collect_report_hitter_recommendations(report_obj, policy, warnings)

    markets: Dict[str, Any] = {}
    for market_name in ("totals", "ml"):
        rows = list(raw_game_rows.get(market_name) or [])
        selected = _rank_and_cap(rows, caps.get(market_name))
        markets[market_name] = {
            "raw_candidates_n": int(len(rows)),
            "selected_n": int(len(selected)),
            "cap": (int(caps[market_name]) if caps.get(market_name) is not None else None),
            "stake_u": float(DEFAULT_STANDARD_STAKE_U),
            "recommendations": selected,
        }

    selected_pitcher_rows = _rank_and_cap_unique_players(pitcher_rows, caps.get("pitcher_props"))
    extra_pitcher_rows = _subtract_selected_rows(pitcher_rows, selected_pitcher_rows)
    markets["pitcher_props"] = {
        "raw_candidates_n": int(len(pitcher_rows)),
        "selected_n": int(len(selected_pitcher_rows)),
        "other_playable_candidates_n": int(len(extra_pitcher_rows)),
        "cap": (int(caps["pitcher_props"]) if caps.get("pitcher_props") is not None else None),
        "stake_u": float(DEFAULT_STANDARD_STAKE_U),
        "one_prop_per_player": True,
        "recommendations": selected_pitcher_rows,
        "other_playable_candidates": extra_pitcher_rows,
    }

    hitter_raw_by_market: Dict[str, List[Dict[str, Any]]] = {market_name: [] for market_name in HITTER_MARKET_ORDER}
    for row in hitter_rows:
        market_name = str(row.get("market") or "")
        hitter_raw_by_market.setdefault(market_name, []).append(row)

    selected_hitter_rows, selected_hitter_by_market, hitter_selection_mode = _select_hitter_recommendations(
        hitter_rows,
        caps.get("hitter_props"),
        normalized_hitter_subcaps,
        blocked_player_keys=_selected_player_keys(selected_pitcher_rows),
    )

    for market_name in HITTER_MARKET_ORDER:
        rows = list(hitter_raw_by_market.get(market_name) or [])
        selected = list(selected_hitter_by_market.get(market_name) or [])
        extra = _subtract_selected_rows(rows, selected)
        market_cap = normalized_hitter_subcaps.get(market_name) if hitter_selection_mode == "submarket_caps" else None
        markets[market_name] = {
            "raw_candidates_n": int(len(rows)),
            "selected_n": int(len(selected)),
            "other_playable_candidates_n": int(len(extra)),
            "cap": (int(market_cap) if market_cap is not None else None),
            "cap_mode": ("submarket" if hitter_selection_mode == "submarket_caps" else "shared_group"),
            "shared_cap_bucket": "hitter_props",
            "stake_u": float(DEFAULT_HITTER_STAKE_U),
            "one_prop_per_player": True,
            "recommendations": selected,
            "other_playable_candidates": extra,
        }

    market_groups = {
        "hitter_props": {
            "raw_candidates_n": int(len(hitter_rows)),
            "selected_n": int(len(selected_hitter_rows)),
            "other_playable_candidates_n": int(
                sum(len(markets.get(market_name, {}).get("other_playable_candidates") or []) for market_name in HITTER_MARKET_ORDER)
            ),
            "cap": (int(caps["hitter_props"]) if caps.get("hitter_props") is not None else None),
            "selection_mode": hitter_selection_mode,
            "one_prop_per_player": True,
            "stake_u": float(DEFAULT_HITTER_STAKE_U),
            "submarkets": list(HITTER_MARKET_ORDER),
            "submarket_caps": {
                market_name: (int(value) if value is not None else None)
                for market_name, value in normalized_hitter_subcaps.items()
            },
            "selected_counts": {
                market_name: int(len(selected_hitter_by_market.get(market_name) or []))
                for market_name in HITTER_MARKET_ORDER
            },
        }
    }

    hitter_policy: Dict[str, Any] = {
        "side": "best_edge_side",
        "no_vig_edge_min": float(policy["hitter_edge_min"]),
        "selection_mode": hitter_selection_mode,
        "one_prop_per_player": True,
        "shared_cap_bucket": "hitter_props",
        "aggregate_cap": (int(caps["hitter_props"]) if caps.get("hitter_props") is not None else None),
        "submarkets": list(HITTER_MARKET_ORDER),
    }
    hitter_edge_overrides = _hitter_edge_min_overrides(policy)
    if hitter_edge_overrides:
        hitter_policy["no_vig_edge_min_by_submarket"] = dict(hitter_edge_overrides)
    if _has_hitter_subcaps(normalized_hitter_subcaps):
        hitter_policy["submarket_caps"] = {
            market_name: (int(value) if value is not None else None)
            for market_name, value in normalized_hitter_subcaps.items()
        }

    odds_paths = _odds_paths(date_str)
    card = {
        "date": str(date_str),
        "season": int(season),
        "generated_at": datetime.now().isoformat(),
        "tool": "tools/eval/build_season_betting_cards_manifest.py",
        "selection_source": _relative_path_str(report_path),
        "source_batch": _relative_path_str(batch_dir),
        "source_mode": "season_eval_batch_reconstruction",
        "policy": {
            "totals": {
                "side": str(policy.get("totals_side") or "best_edge_side"),
                "calibrated_no_vig_edge_min": float(policy.get("totals_edge_min") or 0.0),
                "mean_support_min": float(policy["totals_diff_min"]),
            },
            "ml": {"side": str(policy.get("ml_side") or "best_edge_side"), "no_vig_edge_min": float(policy["ml_edge_min"])},
            "hitter_props": hitter_policy,
            "pitcher_props": {
                "market": str(_normalized_pitcher_market(policy.get("pitcher_market"))),
                "side": str(policy["pitcher_side"]),
                "one_prop_per_player": True,
                "calibrated_no_vig_edge_min": float(policy["pitcher_edge_min"]),
            },
        },
        "cap_profile": cap_profile,
        "caps": dict(caps),
        "hitter_subcaps": {
            market_name: (int(value) if value is not None else None)
            for market_name, value in normalized_hitter_subcaps.items()
        },
        "staking": {
            "totals": float(DEFAULT_STANDARD_STAKE_U),
            "ml": float(DEFAULT_STANDARD_STAKE_U),
            "pitcher_props": float(DEFAULT_STANDARD_STAKE_U),
            "hitter_props": float(DEFAULT_HITTER_STAKE_U),
        },
        "notes": [
            "This card was reconstructed from the finished season-eval batch using the current official locked-policy thresholds and caps.",
            "Official sides are picked from the sim distribution first, with market edge used as a secondary ranking input.",
            "Totals and player props must keep their projected mean on the same side of the betting line before they can be promoted.",
            "Totals, moneyline, and pitcher props are graded at 1.0u; hitter props are graded at 0.25u.",
            (
                "Hitter submarkets are capped independently at "
                f"HR {_cap_text(normalized_hitter_subcaps.get('hitter_home_runs'))} / "
                f"Hits {_cap_text(normalized_hitter_subcaps.get('hitter_hits'))} / "
                f"Total Bases {_cap_text(normalized_hitter_subcaps.get('hitter_total_bases'))} / "
                f"Runs {_cap_text(normalized_hitter_subcaps.get('hitter_runs'))} / "
                f"RBIs {_cap_text(normalized_hitter_subcaps.get('hitter_rbis'))}, "
                f"with a {_cap_text(caps.get('hitter_props'))}-pick aggregate hitter ceiling."
            )
            if hitter_selection_mode == "submarket_caps"
            else "Hitter submarkets share one aggregate hitter_props cap.",
            "Pitcher props rank the best qualified outs/strikeouts lanes into the shared pitcher bucket."
        ],
        "inputs": {
            "report": _relative_path_str(report_path),
            "game_lines": _relative_path_str(odds_paths["game_lines"]),
            "pitcher_lines": _relative_path_str(odds_paths["pitcher_lines"]),
            "hitter_lines": _relative_path_str(odds_paths["hitter_lines"]),
        },
        "warnings": sorted(set(str(item) for item in warnings if str(item).strip())),
        "market_groups": market_groups,
        "markets": markets,
        "combined": {
            "raw_candidates_n": int(sum(int((row.get("raw_candidates_n") or 0)) for row in markets.values())),
            "selected_n": int(sum(int((row.get("selected_n") or 0)) for row in markets.values())),
        },
    }

    card_path = _card_output_paths(cards_dir, date_str)
    return card_path, card


def _selected_counts_with_defaults(card: Dict[str, Any]) -> Dict[str, int]:
    counts = dict(_locked_policy_selected_counts(card) or {})
    for market_name in ("totals", "ml", "pitcher_props", "hitter_props", *HITTER_MARKET_ORDER):
        counts[market_name] = int(counts.get(market_name) or 0)
    counts["combined"] = int((card.get("combined") or {}).get("selected_n") or 0)
    return counts


def _manifest_day_entry(
    *,
    card_path: Path,
    report_path: Path,
    card: Dict[str, Any],
    settled_card: Dict[str, Any],
    payload_path: Optional[Path] = None,
) -> Dict[str, Any]:
    rows = list(settled_card.get("_settled_rows") or [])
    results = _results_from_rows(rows)
    return {
        "date": str(card.get("date") or ""),
        "month": str(card.get("date") or "")[:7],
        "card_path": _relative_path_str(card_path),
        "report_path": _relative_path_str(report_path),
        "payload_path": _relative_path_str(payload_path),
        "cap_profile": str(card.get("cap_profile") or DEFAULT_OFFICIAL_CAP_PROFILE),
        "selected_counts": _selected_counts_with_defaults(card),
        "results": results,
        "settled_n": int(settled_card.get("settled_n") or 0),
        "unresolved_n": int(settled_card.get("unresolved_n") or 0),
        "warnings": list(card.get("warnings") or []),
        "profit_u": results["combined"]["profit_u"],
        "roi": results["combined"]["roi"],
    }


def _aggregate_selected_counts(days: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    totals: Dict[str, int] = {
        market_name: 0
        for market_name in ("totals", "ml", "pitcher_props", "hitter_props", *HITTER_MARKET_ORDER, "combined")
    }
    for row in days:
        counts = row.get("selected_counts") or {}
        for key in totals:
            totals[key] += int(counts.get(key) or 0)
    return totals


def _monthly_entries(days: Sequence[Dict[str, Any]], settled_cards: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    card_lookup = {str(card.get("date") or ""): card for card in settled_cards}
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for day in days:
        buckets[str(day.get("month") or "")].append(day)

    out: List[Dict[str, Any]] = []
    for month_key in sorted(buckets):
        month_days = list(buckets[month_key])
        month_cards = [card_lookup.get(str(day.get("date") or "")) for day in month_days]
        month_cards = [card for card in month_cards if isinstance(card, dict)]
        month_rows: List[Dict[str, Any]] = []
        for card in month_cards:
            month_rows.extend(card.get("_settled_rows") or [])
        month_results = _results_from_rows(month_rows)
        out.append(
            {
                "month": str(month_key),
                "label": _month_label(str(month_key)),
                "selected_counts": _aggregate_selected_counts(month_days),
                "results": month_results,
                "daily": _daily_stats(month_days),
            }
        )
    return out


def _render_recap_markdown(manifest: Dict[str, Any]) -> str:
    meta = manifest.get("meta") or {}
    summary = manifest.get("summary") or {}
    results = summary.get("results") or {}
    daily = summary.get("daily") or {}
    months = manifest.get("months") or []

    def _fmt(value: Any, digits: int = 4) -> str:
        if value is None:
            return "n/a"
        if isinstance(value, int):
            return str(value)
        return f"{float(value):.{digits}f}"

    lines: List[str] = []
    lines.append(f"# {meta.get('title') or 'Season Betting Card Recap'}")
    lines.append("")
    lines.append(f"- Season: {meta.get('season')}")
    lines.append(f"- Generated: {meta.get('generated_at')}")
    lines.append(f"- Batch: {meta.get('batch_dir')}")
    lines.append(f"- Cards Dir: {meta.get('cards_dir')}")
    lines.append(f"- Source Mode: {meta.get('source_mode')}")
    lines.append(f"- Cap Profile: {meta.get('cap_profile')}")
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append(
        "- Cards: "
        f"{summary.get('cards')} total, {daily.get('cards_with_bets')} with bets, {daily.get('cards_without_bets')} without bets"
    )
    lines.append(
        "- Recommendations: "
        f"selected {summary.get('selected_counts', {}).get('combined')} / "
        f"settled {summary.get('settled_recommendations')} / "
        f"unresolved {summary.get('unresolved_recommendations')}"
    )
    combined = results.get("combined") or {}
    hitter_combined = results.get("hitter_props") or {}
    lines.append(
        "- Combined ROI: "
        f"{_fmt(combined.get('roi'))} on {combined.get('stake_u')}u staked, profit {combined.get('profit_u')}u "
        f"({combined.get('wins')}-{combined.get('losses')})"
    )
    lines.append(
        "- Hitter Props ROI: "
        f"{_fmt(hitter_combined.get('roi'))} on {hitter_combined.get('stake_u')}u staked, profit {hitter_combined.get('profit_u')}u"
    )
    lines.append(
        "- Daily Units: "
        f"mean {_fmt(daily.get('mean_u'))}, median {_fmt(daily.get('median_u'))}, std {_fmt(daily.get('std_u'))}, "
        f"best {((daily.get('best_day') or {}).get('date') or 'n/a')} ({_fmt((daily.get('best_day') or {}).get('profit_u'))}u), "
        f"worst {((daily.get('worst_day') or {}).get('date') or 'n/a')} ({_fmt((daily.get('worst_day') or {}).get('profit_u'))}u)"
    )
    lines.append("")
    lines.append("## Market Breakdown")
    lines.append("")
    lines.append("| Market | Bets | Stake (u) | Profit (u) | ROI |")
    lines.append("| --- | ---: | ---: | ---: | ---: |")
    for market_name in ("combined", "totals", "ml", "pitcher_props", "hitter_props", *HITTER_MARKET_ORDER):
        block = results.get(market_name) or {}
        label = {
            "combined": "Combined",
            "totals": "Totals",
            "ml": "Moneyline",
            "pitcher_props": "Pitcher Props",
            "hitter_props": "Hitter Props",
            "hitter_home_runs": "Hitter HR",
            "hitter_hits": "Hitter Hits",
            "hitter_total_bases": "Hitter TB",
            "hitter_runs": "Hitter Runs",
            "hitter_rbis": "Hitter RBIs",
        }.get(market_name, market_name)
        lines.append(
            f"| {label} | {block.get('n', 0)} | {_fmt(block.get('stake_u'))} | {_fmt(block.get('profit_u'))} | {_fmt(block.get('roi'))} |"
        )
    if months:
        lines.append("")
        lines.append("## Monthly Breakdown")
        lines.append("")
        lines.append("| Month | Cards | Bets | Profit (u) | ROI | Hitter ROI |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for month in months:
            month_results = month.get("results") or {}
            combined_month = month_results.get("combined") or {}
            hitter_month = month_results.get("hitter_props") or {}
            month_daily = month.get("daily") or {}
            lines.append(
                f"| {month.get('label')} | {month_daily.get('cards', 0)} | {combined_month.get('n', 0)} | {_fmt(combined_month.get('profit_u'))} | {_fmt(combined_month.get('roi'))} | {_fmt(hitter_month.get('roi'))} |"
            )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build season betting-card recap artifacts from a finished season eval batch")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--batch-dir", required=True, help="Finished season eval batch directory")
    ap.add_argument("--out", default="", help="Output JSON manifest path")
    ap.add_argument("--recap-md", default="", help="Output markdown recap path")
    ap.add_argument("--cards-dir", default="", help="Directory to write reconstructed daily locked-policy cards")
    ap.add_argument("--day-payload-dir", default="", help="Directory to write precomputed per-day betting payload JSON")
    ap.add_argument("--profile-name", default="", choices=("", "baseline", "retuned"), help="Profile label recorded in per-day betting payloads")
    ap.add_argument("--title", default="", help="Optional title override")
    ap.add_argument("--date", action="append", default=[], help="Optional date filter; can be passed multiple times")
    ap.add_argument("--max-days", type=int, default=0, help="Optional cap for smoke runs")
    ap.add_argument("--totals-diff-min", type=float, default=None, help="Override totals mean-minus-line threshold")
    ap.add_argument("--ml-edge-min", type=float, default=None, help="Override moneyline no-vig edge threshold")
    ap.add_argument("--hitter-edge-min", type=float, default=None, help="Override hitter prop edge threshold")
    ap.add_argument("--hitter-runs-edge-min", type=float, default=None, help="Override hitter runs edge threshold")
    ap.add_argument("--hitter-rbi-edge-min", type=float, default=None, help="Override hitter RBI edge threshold")
    ap.add_argument("--pitcher-edge-min", type=float, default=None, help="Override pitcher outs edge threshold")
    ap.add_argument("--totals-cap", type=int, default=None, help="Override totals daily cap")
    ap.add_argument("--ml-cap", type=int, default=None, help="Override moneyline daily cap")
    ap.add_argument("--pitcher-cap", type=int, default=None, help="Override pitcher props daily cap")
    ap.add_argument("--hitter-cap", type=int, default=None, help="Override aggregate hitter props daily cap")
    ap.add_argument("--hitter-hr-cap", type=int, default=None, help="Override hitter HR daily subcap")
    ap.add_argument("--hitter-hits-cap", type=int, default=None, help="Override hitter hits daily subcap")
    ap.add_argument("--hitter-tb-cap", type=int, default=None, help="Override hitter total bases daily subcap")
    ap.add_argument("--hitter-runs-cap", type=int, default=None, help="Override hitter runs daily subcap")
    ap.add_argument("--hitter-rbi-cap", type=int, default=None, help="Override hitter RBI daily subcap")
    ap.add_argument(
        "--prefer-canonical-daily",
        default="off",
        choices=("on", "off"),
        help="When on, use canonical data/daily locked-policy cards when they already exist instead of reconstructing those dates from eval reports.",
    )
    return ap.parse_args()


def main() -> int:
    args = _parse_args()

    season = int(args.season)
    batch_dir = _resolve_path(str(args.batch_dir), default=_ROOT / "data" / "eval" / "batches")
    if not batch_dir.exists() or not batch_dir.is_dir():
        raise SystemExit(f"Batch dir not found: {batch_dir}")

    season_dir = _ROOT / "data" / "eval" / "seasons" / str(season)
    manifest_out = _resolve_path(
        str(args.out),
        default=season_dir / "season_betting_cards_manifest.json",
    )
    recap_md = _resolve_path(
        str(args.recap_md),
        default=season_dir / "season_betting_cards_recap.md",
    )
    cards_dir = _resolve_path(
        str(args.cards_dir),
        default=season_dir / "locked_cards",
    )
    normalized_profile = str(args.profile_name or "").strip().lower()
    if normalized_profile not in ("baseline", "retuned"):
        normalized_profile = "retuned" if "retuned" in manifest_out.name.lower() else "baseline"
    day_payload_dir = _resolve_path(
        str(args.day_payload_dir),
        default=(season_dir / ("betting_day_payloads_retuned" if normalized_profile == "retuned" else "betting_day_payloads")),
    )

    report_paths = _iter_report_paths(batch_dir, list(args.date or []), int(args.max_days or 0))
    if not report_paths:
        raise SystemExit(f"No per-day reports found in {batch_dir}")

    all_reports = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    policy = _policy_with_overrides(
        DEFAULT_LOCK_POLICY,
        scalar_updates={
            "totals_diff_min": args.totals_diff_min,
            "ml_edge_min": args.ml_edge_min,
            "hitter_edge_min": args.hitter_edge_min,
            "pitcher_edge_min": args.pitcher_edge_min,
        },
        hitter_edge_updates={
            "hitter_runs": args.hitter_runs_edge_min,
            "hitter_rbis": args.hitter_rbi_edge_min,
        },
    )
    caps = _normalized_official_caps(
        _override_dict(
            dict(DEFAULT_OFFICIAL_CAPS),
            {
                "totals": args.totals_cap,
                "ml": args.ml_cap,
                "pitcher_props": args.pitcher_cap,
                "hitter_props": args.hitter_cap,
            },
        )
    )
    hitter_subcaps = _normalized_hitter_subcaps(
        _override_dict(
            dict(DEFAULT_OFFICIAL_HITTER_SUBCAPS),
            {
                "hitter_home_runs": args.hitter_hr_cap,
                "hitter_hits": args.hitter_hits_cap,
                "hitter_total_bases": args.hitter_tb_cap,
                "hitter_runs": args.hitter_runs_cap,
                "hitter_rbis": args.hitter_rbi_cap,
            },
        )
    )
    prefer_canonical_daily = str(args.prefer_canonical_daily or "off").strip().lower() == "on"

    settled_cards: List[Dict[str, Any]] = []
    day_entries: List[Dict[str, Any]] = []
    source_modes: List[str] = []

    for report_path in report_paths:
        date_str = str(report_path.stem.replace("sim_vs_actual_", "")).strip()
        if prefer_canonical_daily:
            canonical_card_path = _canonical_daily_card_path(date_str)
            if canonical_card_path is not None:
                canonical_card = _read_json_dict(canonical_card_path)
                if canonical_card:
                    settled_card = _settle_card(canonical_card_path)
                    payload_path = _day_payload_output_path(day_payload_dir, date_str)
                    summary_row = _manifest_day_entry(
                        card_path=canonical_card_path,
                        report_path=report_path,
                        card=canonical_card,
                        settled_card=settled_card,
                        payload_path=payload_path,
                    )
                    _write_json(
                        payload_path,
                        _static_day_payload(
                            season=season,
                            profile_name=normalized_profile,
                            card_path=canonical_card_path,
                            report_path=report_path,
                            card=canonical_card,
                            settled_card=settled_card,
                            summary=summary_row,
                            payload_path=payload_path,
                        ),
                    )
                    settled_cards.append(settled_card)
                    day_entries.append(summary_row)
                    source_modes.append("canonical_daily_locked_policy")
                    continue
        report_obj = _read_json_dict(report_path)
        if not report_obj:
            continue
        card_path, card = _build_card_from_report(
            report_path,
            report_obj,
            batch_dir=batch_dir,
            cards_dir=cards_dir,
            policy=policy,
            market_caps=caps,
            hitter_subcaps=hitter_subcaps,
        )
        _write_json(card_path, card)
        settled_card = _settle_card(card_path)
        payload_path = _day_payload_output_path(day_payload_dir, date_str)
        summary_row = _manifest_day_entry(
            card_path=card_path,
            report_path=report_path,
            card=card,
            settled_card=settled_card,
            payload_path=payload_path,
        )
        _write_json(
            payload_path,
            _static_day_payload(
                season=season,
                profile_name=normalized_profile,
                card_path=card_path,
                report_path=report_path,
                card=card,
                settled_card=settled_card,
                summary=summary_row,
                payload_path=payload_path,
            ),
        )
        settled_cards.append(settled_card)
        day_entries.append(summary_row)
        source_modes.append("season_eval_batch_reconstruction")

    all_rows: List[Dict[str, Any]] = []
    for card in settled_cards:
        all_rows.extend(card.get("_settled_rows") or [])

    combined = _combined_summary(settled_cards)
    summary_results = _results_from_rows(all_rows)
    for settled_card in settled_cards:
        settled_card["results"] = _results_from_rows(settled_card.get("_settled_rows") or [])

    summary = {
        "cards": int(len(day_entries)),
        "cards_processed": int(len(day_entries)),
        "selected_counts": _aggregate_selected_counts(day_entries),
        "settled_recommendations": int(len(all_rows)),
        "unresolved_recommendations": int((combined.get("combined") or {}).get("unresolved_recommendations") or combined.get("unresolved_recommendations") or 0),
        "results": summary_results,
        "daily": _daily_stats(day_entries),
        "combined": combined.get("combined") or summary_results.get("combined") or _blank_summary(),
        "market_results": combined.get("markets") or {},
    }

    months = _monthly_entries(day_entries, settled_cards)

    manifest = {
        "meta": {
            "season": int(season),
            "generated_at": datetime.now().isoformat(),
            "title": str(args.title).strip() or f"MLB {season} Betting Card Recap",
            "batch_dir": _relative_path_str(batch_dir),
            "cards_dir": _relative_path_str(cards_dir),
            "day_payload_dir": _relative_path_str(day_payload_dir),
            "source_mode": _authoritative_source_mode(prefer_canonical_daily, source_modes),
            "prefer_canonical_daily": bool(prefer_canonical_daily),
            "cap_profile": _official_cap_profile_name(caps, hitter_subcaps),
            "policy": dict(policy),
            "caps": dict(caps),
            "hitter_subcaps": dict(hitter_subcaps),
            "partial": bool(len(report_paths) != len(all_reports)),
            "processed_reports": int(len(report_paths)),
            "available_reports": int(len(all_reports)),
        },
        "status": ("partial" if len(report_paths) != len(all_reports) else "complete"),
        "summary": summary,
        "months": months,
        "days": day_entries,
    }

    recap_text = _render_recap_markdown(manifest)
    _write_json(manifest_out, manifest)
    _write_text(recap_md, recap_text)

    print(json.dumps({
        "manifest": _relative_path_str(manifest_out),
        "recap_md": _relative_path_str(recap_md),
        "cards_dir": _relative_path_str(cards_dir),
        "cards": len(day_entries),
        "combined": summary_results.get("combined") or {},
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())