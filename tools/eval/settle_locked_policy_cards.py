from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT_ENV = str(os.environ.get("MLB_BETTING_DATA_ROOT") or "").strip()
DATA_ROOT = Path(DATA_ROOT_ENV).resolve() if DATA_ROOT_ENV else None
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sim_engine.market_pitcher_props import normalize_pitcher_name


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _resolve_path(value: str) -> Path:
    path = Path(str(value))
    if not path.is_absolute():
        parts = path.parts
        if DATA_ROOT is not None and parts and str(parts[0]).lower() == "data":
            path = DATA_ROOT.joinpath(*parts[1:])
        else:
            path = REPO_ROOT / path
    return path.resolve()


def _feed_live_path(date: str, game_pk: int) -> Path:
    year = str(date).split("-", 1)[0]
    if DATA_ROOT is not None:
        return (DATA_ROOT / "raw" / "statsapi" / "feed_live" / year / date / f"{int(game_pk)}.json.gz").resolve()
    return (REPO_ROOT / "data" / "raw" / "statsapi" / "feed_live" / year / date / f"{int(game_pk)}.json.gz").resolve()


def _iter_paths(values: Sequence[str], patterns: Sequence[str]) -> List[Path]:
    out: List[Path] = []
    seen = set()
    for value in values:
        path = _resolve_path(value)
        if path.is_file() and path not in seen:
            out.append(path)
            seen.add(path)
    for pattern in patterns:
        rel_pattern = str(pattern or "").strip()
        if not rel_pattern:
            continue
        for path in REPO_ROOT.glob(rel_pattern):
            resolved = path.resolve()
            if resolved.is_file() and resolved not in seen:
                out.append(resolved)
                seen.add(resolved)
    return sorted(out)


def _selected_counts_for_key(card: Dict[str, Any], reco_key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for market_name, market_info in ((card.get("markets") or {}) or {}).items():
        rows = market_info.get(reco_key) if isinstance(market_info, dict) else []
        out[str(market_name)] = int(len(rows or []))
    out["combined"] = int(sum(out.values()))
    return out


def _selected_counts(card: Dict[str, Any]) -> Dict[str, int]:
    return _selected_counts_for_key(card, "recommendations")


def _playable_selected_counts(card: Dict[str, Any]) -> Dict[str, int]:
    return _selected_counts_for_key(card, "other_playable_candidates")


def _merged_selected_counts(*counts_blocks: Dict[str, int]) -> Dict[str, int]:
    merged: Dict[str, int] = {}
    keys = {str(key) for counts in counts_blocks for key in counts.keys()}
    for key in sorted(keys):
        merged[str(key)] = int(sum(int((counts or {}).get(key) or 0) for counts in counts_blocks))
    return merged


def _american_profit(odds: Any, stake_u: float) -> float:
    raw = str(odds or "").strip()
    if not raw:
        raise ValueError("missing odds")
    value = int(raw)
    stake = float(stake_u)
    if value > 0:
        return round(stake * float(value) / 100.0, 4)
    if value < 0:
        return round(stake * 100.0 / abs(float(value)), 4)
    raise ValueError(f"invalid odds: {odds}")


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _read_feed_file(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            loaded = json.load(fh)
    except Exception:
        return None
    return loaded if isinstance(loaded, dict) and loaded else None


def _load_feed(date: str, game_pk: int) -> Dict[str, Any]:
    primary_path = _feed_live_path(date, game_pk)
    candidate_paths = [primary_path]
    fallback_path = (REPO_ROOT / "data" / "raw" / "statsapi" / "feed_live" / str(date).split("-", 1)[0] / str(date) / f"{int(game_pk)}.json.gz").resolve()
    if fallback_path not in candidate_paths:
        candidate_paths.append(fallback_path)
    stale_feed: Optional[Dict[str, Any]] = None
    for path in candidate_paths:
        loaded = _read_feed_file(path)
        if not isinstance(loaded, dict):
            continue
        if _feed_is_final(loaded):
            if path != primary_path:
                try:
                    primary_path.parent.mkdir(parents=True, exist_ok=True)
                    with gzip.open(primary_path, "wt", encoding="utf-8") as fh:
                        json.dump(loaded, fh)
                except Exception:
                    pass
            return loaded
        if stale_feed is None:
            stale_feed = loaded
    from sim_engine.data.statsapi import StatsApiClient, fetch_game_feed_live

    client = StatsApiClient.with_default_cache(ttl_seconds=15 * 60)
    fetched = fetch_game_feed_live(client, int(game_pk))
    if isinstance(fetched, dict) and fetched:
        try:
            primary_path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(primary_path, "wt", encoding="utf-8") as fh:
                json.dump(fetched, fh)
        except Exception:
            pass
        return fetched
    if isinstance(stale_feed, dict) and stale_feed:
        return stale_feed
    raise FileNotFoundError(str(primary_path))


def _team_side(feed: Dict[str, Any], team_abbr: str) -> Optional[str]:
    teams = (feed.get("gameData") or {}).get("teams") or {}
    away_abbr = str(((teams.get("away") or {}).get("abbreviation") or "")).upper()
    home_abbr = str(((teams.get("home") or {}).get("abbreviation") or "")).upper()
    target = str(team_abbr or "").upper()
    if target == away_abbr:
        return "away"
    if target == home_abbr:
        return "home"
    return None


def _player_stats(feed: Dict[str, Any], side: str, player_name: str, stat_group: str) -> Optional[Dict[str, Any]]:
    players = (((((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}).get(side) or {}).get("players") or {})
    target = normalize_pitcher_name(str(player_name or ""))
    if not target:
        return None
    for row in players.values():
        person = row.get("person") or {}
        name = person.get("fullName") or person.get("name") or ""
        if normalize_pitcher_name(str(name)) != target:
            continue
        return ((row.get("stats") or {}).get(stat_group) or {})
    return None


def _settle_over_under(actual: float, line: float, selection: str) -> Optional[bool]:
    sel = str(selection or "").strip().lower()
    if sel == "over":
        return float(actual) > float(line)
    if sel == "under":
        return float(actual) < float(line)
    return None


def _is_final_game_status(status_text: Any) -> bool:
    token = str(status_text or "").strip().lower()
    if not token:
        return False
    return token in {"final", "completed early", "game over"} or token.startswith("final") or token.startswith("completed")


def _feed_is_final(feed: Dict[str, Any]) -> bool:
    status = (feed.get("gameData") or {}).get("status") or {}
    return _is_final_game_status(status.get("abstractGameState")) or _is_final_game_status(status.get("detailedState"))


HITTER_STAT_KEYS: Dict[str, str] = {
    "hitter_hits": "hits",
    "hitter_total_bases": "totalBases",
    "hitter_home_runs": "homeRuns",
    "hitter_runs": "runs",
    "hitter_rbis": "rbi",
}

PITCHER_PROP_STAT_KEYS: Dict[str, str] = {
    "strikeouts": "strikeOuts",
    "outs": "outs",
    "earned_runs": "earnedRuns",
    "walks": "baseOnBalls",
    "batters_faced": "battersFaced",
    "pitches": "pitchesThrown",
    "hits": "hits",
}


def _normalize_pitcher_prop(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "": "strikeouts",
        "k": "strikeouts",
        "ks": "strikeouts",
        "so": "strikeouts",
        "strikeout": "strikeouts",
        "strikeouts": "strikeouts",
        "out": "outs",
        "outs": "outs",
        "er": "earned_runs",
        "earned_run": "earned_runs",
        "earned_runs": "earned_runs",
        "earnedruns": "earned_runs",
        "bb": "walks",
        "walk": "walks",
        "walks": "walks",
        "bf": "batters_faced",
        "batter_faced": "batters_faced",
        "batters_faced": "batters_faced",
        "battersfaced": "batters_faced",
        "pitch": "pitches",
        "pitches": "pitches",
        "hit": "hits",
        "hits": "hits",
    }
    normalized = aliases.get(token, token)
    return normalized if normalized in PITCHER_PROP_STAT_KEYS else ""


def _resolve_pitcher_prop(rec: Dict[str, Any]) -> str:
    prop_key = _normalize_pitcher_prop(rec.get("prop"))
    if prop_key:
        return prop_key
    if _safe_float(rec.get("outs_mean")) is not None:
        return "outs"
    if _safe_float(rec.get("so_mean")) is not None:
        return "strikeouts"
    if _safe_float(rec.get("er_mean")) is not None:
        return "earned_runs"
    if _safe_float(rec.get("walks_mean")) is not None:
        return "walks"
    if _safe_float(rec.get("batters_faced_mean")) is not None:
        return "batters_faced"
    if _safe_float(rec.get("pitches_mean")) is not None:
        return "pitches"
    if _safe_float(rec.get("hits_mean")) is not None:
        return "hits"
    return ""


def _summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
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
        "roi": (round(float(profit_u) / float(stake_u), 4) if float(stake_u) > 0 else None),
    }


def _market_summaries(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_market: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        market = str(row.get("market") or "")
        if market:
            by_market[market].append(row)
    return {market: _summary(market_rows) for market, market_rows in sorted(by_market.items())}


def _results_from_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    results = _market_summaries(rows)
    hitter_rows = [row for row in rows if str(row.get("market") or "") in HITTER_STAT_KEYS]
    if hitter_rows:
        results["hitter_props"] = _summary(hitter_rows)
    results["combined"] = _summary(rows)
    return results


def _settle_card(path: Path) -> Dict[str, Any]:
    card = _read_json(path)
    date = str(card.get("date") or "").strip()
    selected_counts = _selected_counts(card) if isinstance(card, dict) else {}
    playable_selected_counts = _playable_selected_counts(card) if isinstance(card, dict) else {}
    all_selected_counts = _merged_selected_counts(selected_counts, playable_selected_counts)
    feed_cache: Dict[int, Dict[str, Any]] = {}
    settled_rows: List[Dict[str, Any]] = []
    playable_settled_rows: List[Dict[str, Any]] = []
    unresolved_rows: List[Dict[str, Any]] = []
    playable_unresolved_rows: List[Dict[str, Any]] = []

    for market_name, market_info in ((card.get("markets") or {}) or {}).items():
        if not isinstance(market_info, dict):
            continue
        for reco_key, tier_name, tier_settled_rows, tier_unresolved_rows in (
            ("recommendations", "official", settled_rows, unresolved_rows),
            ("other_playable_candidates", "candidate", playable_settled_rows, playable_unresolved_rows),
        ):
            recs = market_info.get(reco_key) or []
            if not isinstance(recs, list):
                continue
            for rec in recs:
                if not isinstance(rec, dict):
                    continue
                market = str(rec.get("market") or market_name)
                game_pk = int(rec.get("game_pk") or 0)
                line = float(rec.get("market_line") or 0.0)
                selection = str(rec.get("selection") or "")
                stake_u = float(rec.get("stake_u") or 0.0)
                odds = rec.get("odds")
                player_label = rec.get("player_name") or rec.get("pitcher_name") or None
                prop_key = _resolve_pitcher_prop(rec)
                try:
                    feed = feed_cache.setdefault(game_pk, _load_feed(date, game_pk))
                    if not _feed_is_final(feed):
                        raise LookupError("game not final")
                    actual_value: Any
                    won: Optional[bool]
                    if market == "totals":
                        teams = (((feed.get("liveData") or {}).get("linescore") or {}).get("teams") or {})
                        away_runs = float(((teams.get("away") or {}).get("runs")) or 0.0)
                        home_runs = float(((teams.get("home") or {}).get("runs")) or 0.0)
                        actual_value = float(away_runs + home_runs)
                        won = _settle_over_under(float(actual_value), line, selection)
                    elif market == "ml":
                        teams = (((feed.get("liveData") or {}).get("linescore") or {}).get("teams") or {})
                        away_runs = float(((teams.get("away") or {}).get("runs")) or 0.0)
                        home_runs = float(((teams.get("home") or {}).get("runs")) or 0.0)
                        actual_value = "home" if home_runs > away_runs else "away" if away_runs > home_runs else "tie"
                        won = str(actual_value) == selection
                    elif market == "pitcher_props":
                        side = _team_side(feed, str(rec.get("team") or ""))
                        if not side:
                            raise LookupError("missing pitcher team side")
                        pitching = _player_stats(feed, side, str(rec.get("pitcher_name") or ""), "pitching")
                        stat_key = PITCHER_PROP_STAT_KEYS.get(prop_key)
                        if not stat_key:
                            raise LookupError(f"unsupported pitcher prop: {rec.get('prop')}")
                        actual_value = float((pitching or {}).get(stat_key) or 0.0)
                        won = _settle_over_under(float(actual_value), line, selection)
                    elif market in HITTER_STAT_KEYS:
                        side = _team_side(feed, str(rec.get("team") or ""))
                        if not side:
                            raise LookupError("missing hitter team side")
                        batting = _player_stats(feed, side, str(rec.get("player_name") or ""), "batting")
                        actual_value = float((batting or {}).get(HITTER_STAT_KEYS[market]) or 0.0)
                        won = _settle_over_under(float(actual_value), line, selection)
                    else:
                        raise LookupError(f"unsupported market: {market}")
                    if won is None:
                        raise LookupError("unresolved outcome")
                    profit_u = _american_profit(odds, stake_u) if bool(won) else -float(stake_u)
                    tier_settled_rows.append(
                        {
                            "path": str(path),
                            "date": date,
                            "game_pk": game_pk,
                            "market": market,
                            "player_name": player_label,
                            "pitcher_name": rec.get("pitcher_name") or player_label,
                            "team": rec.get("team"),
                            "prop": prop_key or rec.get("prop"),
                            "selection": selection,
                            "market_line": line,
                            "odds": odds,
                            "stake_u": stake_u,
                            "actual": actual_value,
                            "result": "win" if bool(won) else "loss",
                            "profit_u": round(float(profit_u), 4),
                            "recommendation_tier": tier_name,
                        }
                    )
                except Exception as exc:
                    tier_unresolved_rows.append(
                        {
                            "path": str(path),
                            "date": date,
                            "game_pk": game_pk,
                            "market": market,
                            "player_name": player_label,
                            "pitcher_name": rec.get("pitcher_name") or player_label,
                            "team": rec.get("team"),
                            "prop": prop_key or rec.get("prop"),
                            "selection": selection,
                            "market_line": line,
                            "reason": str(exc),
                            "recommendation_tier": tier_name,
                        }
                    )

    all_settled_rows = list(settled_rows) + list(playable_settled_rows)
    results = _results_from_rows(settled_rows)
    playable_results = _results_from_rows(playable_settled_rows)
    all_results = _results_from_rows(all_settled_rows)

    return {
        "path": str(path),
        "date": date,
        "cap_profile": card.get("cap_profile"),
        "selected_counts": selected_counts,
        "playable_selected_counts": playable_selected_counts,
        "all_selected_counts": all_selected_counts,
        "results": results,
        "playable_results": playable_results,
        "all_results": all_results,
        "settled_n": int(len(settled_rows)),
        "playable_settled_n": int(len(playable_settled_rows)),
        "all_settled_n": int(len(all_settled_rows)),
        "unresolved_n": int(len(unresolved_rows)),
        "playable_unresolved_n": int(len(playable_unresolved_rows)),
        "all_unresolved_n": int(len(unresolved_rows) + len(playable_unresolved_rows)),
        "unresolved_recommendations": unresolved_rows,
        "playable_unresolved_recommendations": playable_unresolved_rows,
        "all_unresolved_recommendations": list(unresolved_rows) + list(playable_unresolved_rows),
        "_settled_rows": settled_rows,
        "_playable_settled_rows": playable_settled_rows,
        "_all_settled_rows": all_settled_rows,
    }


def _combined_summary(cards: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    official_rows: List[Dict[str, Any]] = []
    playable_rows: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []
    unresolved = 0
    playable_unresolved = 0
    for card in cards:
        unresolved += int(card.get("unresolved_n") or 0)
        playable_unresolved += int(card.get("playable_unresolved_n") or 0)
        for row in (card.get("_settled_rows") or []):
            official_rows.append(row)
            all_rows.append(row)
        for row in (card.get("_playable_settled_rows") or []):
            playable_rows.append(row)
            all_rows.append(row)

    return {
        "cards": int(len(cards)),
        "unresolved_recommendations": int(unresolved),
        "playable_unresolved_recommendations": int(playable_unresolved),
        "all_unresolved_recommendations": int(unresolved + playable_unresolved),
        "markets": _market_summaries(official_rows),
        "combined": _summary(official_rows),
        "playable_markets": _market_summaries(playable_rows),
        "playable_combined": _summary(playable_rows),
        "all_markets": _market_summaries(all_rows),
        "all_combined": _summary(all_rows),
    }


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Exact settlement for locked-policy cards using cached StatsAPI feed_live boxscores.")
    ap.add_argument("--locked-policy", action="append", default=[], help="Locked-policy JSON path. Can be passed multiple times.")
    ap.add_argument(
        "--glob",
        action="append",
        default=[],
        help="Workspace-relative glob for locked-policy JSON paths. Can be passed multiple times.",
    )
    ap.add_argument("--out", default="", help="Optional output JSON path.")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    paths = _iter_paths(list(args.locked_policy or []), list(args.glob or []))
    if not paths:
        raise SystemExit("No locked-policy files found. Pass --locked-policy or --glob.")

    cards: List[Dict[str, Any]] = []
    for path in paths:
        cards.append(_settle_card(path))

    combined = _combined_summary(cards)
    for card in cards:
        card.pop("_settled_rows", None)
        card.pop("_playable_settled_rows", None)
        card.pop("_all_settled_rows", None)

    output = {
        "cards": cards,
        "combined": combined,
    }
    if str(args.out).strip():
        _write_json(_resolve_path(str(args.out)), output)
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())