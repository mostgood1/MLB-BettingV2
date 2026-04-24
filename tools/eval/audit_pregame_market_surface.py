from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.daily_update_multi_profile import (  # noqa: E402
    DEFAULT_OFFICIAL_CAP_PROFILE,
    HITTER_MARKET_ORDER,
    HITTER_MARKET_SPECS,
    PITCHER_MARKET_SPECS,
    SHADOW_HITTER_MARKET_SPECS,
    SHADOW_PITCHER_MARKET_SPECS,
)
from tools.eval.settle_locked_policy_cards import _settle_card  # noqa: E402


GAME_MARKETS: Tuple[str, ...] = ("totals", "ml")
FRONTEND_ONLY_MARKETS: Tuple[str, ...] = ("nrfi", "yrfi")
PLANNED_UNSUPPORTED_MARKETS: Tuple[str, ...] = ()


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _normalize_pitcher_prop(value: Any) -> str:
    token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "": "",
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
        "bb": "walks_allowed",
        "walk": "walks_allowed",
        "walks": "walks_allowed",
        "walks_allowed": "walks_allowed",
        "hit": "hits_allowed",
        "hits": "hits_allowed",
        "hits_allowed": "hits_allowed",
        "bf": "batters_faced",
        "batters_faced": "batters_faced",
        "battersfaced": "batters_faced",
        "pitch": "pitches",
        "pitches": "pitches",
    }
    return aliases.get(token, token)


def _official_market_key(row: Dict[str, Any]) -> str:
    market = str(row.get("market") or "").strip().lower()
    if market == "pitcher_props":
        prop = _normalize_pitcher_prop(row.get("prop"))
        if prop:
            return f"pitcher_{prop}"
        return "pitcher_unknown"
    return market


def _summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    stake_u = sum(float(row.get("stake_u") or 0.0) for row in rows)
    profit_u = sum(float(row.get("profit_u") or 0.0) for row in rows)
    wins = sum(1 for row in rows if str(row.get("result") or "") == "win")
    count = len(rows)
    return {
        "n": int(count),
        "wins": int(wins),
        "losses": int(count - wins),
        "stake_u": round(float(stake_u), 4),
        "profit_u": round(float(profit_u), 4),
        "roi": round(float(profit_u) / float(stake_u), 4) if float(stake_u) > 0.0 else None,
    }


def _by_market(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_official_market_key(row)].append(row)
    return {key: _summary(grouped[key]) for key in sorted(grouped.keys())}


def _daily_profit_by_market(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    per_market: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        market = _official_market_key(row)
        date_key = str(row.get("date") or "")
        per_market[market][date_key] += float(row.get("profit_u") or 0.0)
    out: Dict[str, Dict[str, float]] = {}
    for market, market_days in per_market.items():
        values = list(market_days.values())
        out[market] = {
            "days": float(len(values)),
            "avg_day_u": round(sum(values) / len(values), 4) if values else 0.0,
            "worst_day_u": round(min(values), 4) if values else 0.0,
            "best_day_u": round(max(values), 4) if values else 0.0,
        }
    return out


def _market_inventory() -> Dict[str, Any]:
    official_markets = {
        "game": list(GAME_MARKETS),
        "pitcher": [f"pitcher_{name}" for name in PITCHER_MARKET_SPECS.keys()],
        "hitter": list(HITTER_MARKET_ORDER),
    }
    shadow_markets = {
        "pitcher": [f"pitcher_{name}" for name in SHADOW_PITCHER_MARKET_SPECS.keys()],
        "hitter": [spec.get("market") for spec in SHADOW_HITTER_MARKET_SPECS.values()],
    }
    return {
        "official": official_markets,
        "shadow_only": shadow_markets,
        "frontend_only": list(FRONTEND_ONLY_MARKETS),
        "planned_unsupported": list(PLANNED_UNSUPPORTED_MARKETS),
        "notes": {
            "yrfi_nrfi": "Present as frontend first-inning signals, not part of current official settled pregame card evaluation.",
            "earned_runs": "Referenced in helper/settlement code but not in the current official or shadow pitcher market spec maps.",
            "hitter_hits_runs_rbis": "H+R+R is now wired as a hitter market, but existing historical cards will only include it after odds backfill and card regeneration.",
        },
    }


def _normalize_shadow_submarket(value: Any) -> str:
    token = str(value or "").strip().lower()
    aliases = {
        "batter_strikeouts": "hitter_strikeouts",
        "hits_allowed": "pitcher_hits_allowed",
        "walks_allowed": "pitcher_walks_allowed",
    }
    return aliases.get(token, token)


def _summarize_live_market_block(block: Dict[str, Any]) -> Dict[str, Any]:
    recs = list(block.get("recommendations") or [])
    grouped: Dict[str, int] = defaultdict(int)
    for rec in recs:
        grouped[_normalize_shadow_submarket(rec.get("prop") or rec.get("market"))] += 1
    return {
        "raw_candidates_n": int(block.get("raw_candidates_n") or 0),
        "selected_n": int(block.get("selected_n") or 0),
        "submarkets": sorted({_normalize_shadow_submarket(name) for name in (block.get("submarkets") or [])}),
        "selected_by_submarket": {key: grouped[key] for key in sorted(grouped)},
    }


def _load_latest_live_card(live_card_glob: str) -> Dict[str, Any]:
    live_glob_path = Path(live_card_glob)
    if not live_glob_path.is_absolute():
        live_glob_path = REPO_ROOT / live_card_glob
    parent = live_glob_path.parent
    pattern = live_glob_path.name
    matches = sorted(parent.glob(pattern)) if parent.exists() else []
    if not matches:
        return {
            "available": False,
            "reason": f"No live card files matched {live_glob_path}",
        }

    live_path = matches[-1]
    live_card = _read_json(live_path)
    shadow_markets = dict((live_card.get("shadow_markets") or {})) if isinstance(live_card, dict) else {}
    return {
        "available": True,
        "path": str(live_path),
        "date": str(live_card.get("date") or ""),
        "cap_profile": str(live_card.get("cap_profile") or ""),
        "warnings": list(live_card.get("warnings") or []),
        "official_selected_counts": dict(live_card.get("selected_counts") or {}),
        "playable_selected_counts": dict(live_card.get("playable_selected_counts") or {}),
        "shadow_market_counts": {
            market_name: _summarize_live_market_block(block)
            for market_name, block in shadow_markets.items()
            if isinstance(block, dict)
        },
    }


def _load_settled_rows(cards_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    official_rows: List[Dict[str, Any]] = []
    playable_rows: List[Dict[str, Any]] = []
    unresolved_cards: List[str] = []

    for card_path in sorted(cards_dir.glob("daily_summary_*_locked_policy.json")):
        settled = _settle_card(card_path)
        current_official = list(settled.get("_settled_rows") or [])
        current_playable = list(settled.get("_playable_settled_rows") or [])
        official_rows.extend(current_official)
        playable_rows.extend(current_playable)
        unresolved_n = _safe_int(settled.get("unresolved_n")) or 0
        if unresolved_n > 0:
            unresolved_cards.append(card_path.name)

    return {
        "official": official_rows,
        "playable": playable_rows,
        "unresolved_cards": unresolved_cards,
    }


def _load_shadow_payload_rows(payload_dir: Path) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    payload_dates: List[str] = []
    for payload_path in sorted(payload_dir.glob("season_betting_day_*.json")):
        payload = _read_json(payload_path)
        if not isinstance(payload, dict):
            continue
        payload_dates.append(str(payload.get("date") or ""))
        games = payload.get("games") or {}
        if not isinstance(games, dict):
            continue
        for game_payload in games.values():
            if not isinstance(game_payload, dict):
                continue
            for row in game_payload.get("shadow_settled_rows") or []:
                if isinstance(row, dict):
                    rows.append(dict(row))
    return {
        "rows": rows,
        "payload_dates": [date for date in payload_dates if date],
    }


def _recommend_variable_mix(official_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    market_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in official_rows:
        market_rows[_official_market_key(row)].append(row)

    candidates: List[Dict[str, Any]] = []
    for market, rows in market_rows.items():
        stats = _summary(rows)
        day_stats = _daily_profit_by_market(rows).get(market) or {}
        roi = _safe_float(stats.get("roi"))
        if roi is None:
            continue
        candidates.append(
            {
                "market": market,
                "n": stats.get("n"),
                "roi": roi,
                "profit_u": stats.get("profit_u"),
                "worst_day_u": day_stats.get("worst_day_u"),
                "avg_day_u": day_stats.get("avg_day_u"),
            }
        )

    candidates.sort(key=lambda item: (float(item.get("roi") or 0.0), float(item.get("profit_u") or 0.0)), reverse=True)

    core = [
        item for item in candidates
        if int(item.get("n") or 0) >= 8 and float(item.get("roi") or 0.0) > 0.10
    ]
    diversifiers = [
        item for item in candidates
        if int(item.get("n") or 0) >= 5 and 0.0 < float(item.get("roi") or 0.0) <= 0.10
    ]

    return {
        "core_markets": core[:4],
        "diversifiers": diversifiers[:3],
        "avoid_or_deemphasize": [
            item for item in candidates if float(item.get("roi") or 0.0) <= 0.0
        ],
        "suggested_shape": {
            "style": "variable card",
            "rule": "Do not force every lane daily. Allocate only to markets with positive season-to-date settled edge and keep at least two market families active when qualified.",
            "base_profile": {
                "totals": 0,
                "moneyline": "0-1 only when exceptional",
                "pitcher_props": "0-2 depending on qualified edge",
                "hitter_props": "6-10 when qualified, centered on the best-performing hitter submarkets",
            },
        },
    }


def build_audit(cards_dir: Path, live_card_glob: str, payload_dir: Path) -> Dict[str, Any]:
    settled = _load_settled_rows(cards_dir)
    official_rows = settled["official"]
    playable_rows = settled["playable"]
    all_rows = list(official_rows) + list(playable_rows)
    shadow_payload = _load_shadow_payload_rows(payload_dir) if payload_dir.exists() else {"rows": [], "payload_dates": []}
    shadow_rows = list(shadow_payload.get("rows") or [])
    inventory = _market_inventory()
    latest_live_card = _load_latest_live_card(live_card_glob)

    official_markets_seen = sorted({_official_market_key(row) for row in official_rows})
    playable_markets_seen = sorted({_official_market_key(row) for row in playable_rows})
    all_supported = set(inventory["official"]["game"]) | set(inventory["official"]["pitcher"]) | set(inventory["official"]["hitter"]) | set(inventory["shadow_only"]["pitcher"]) | set(inventory["shadow_only"]["hitter"])

    return {
        "cards_dir": str(cards_dir),
        "official_cap_profile": DEFAULT_OFFICIAL_CAP_PROFILE,
        "inventory": inventory,
        "coverage": {
            "official_markets_seen": official_markets_seen,
            "playable_markets_seen": playable_markets_seen,
            "supported_but_unseen": sorted(all_supported - set(official_markets_seen) - set(playable_markets_seen)),
            "unresolved_cards": list(settled["unresolved_cards"]),
            "historical_limitations": {
                "shadow_markets_archived": bool(shadow_rows),
                "shadow_payload_days": len(list(shadow_payload.get("payload_dates") or [])),
                "reason": (
                    "Shadow-settled rows were found in archived season betting-day payloads. Historical shadow-market summaries below are based on those payload rows."
                    if shadow_rows
                    else "The current season betting-day payload archive does not yet contain shadow-settled rows, so historical ROI for shadow-only lanes still cannot be reconstructed from archived season artifacts alone."
                ),
            },
        },
        "official_results": {
            "combined": _summary(official_rows),
            "by_market": _by_market(official_rows),
            "daily": _daily_profit_by_market(official_rows),
        },
        "playable_results": {
            "combined": _summary(playable_rows),
            "by_market": _by_market(playable_rows),
            "daily": _daily_profit_by_market(playable_rows),
        },
        "all_results": {
            "combined": _summary(all_rows),
            "by_market": _by_market(all_rows),
            "daily": _daily_profit_by_market(all_rows),
        },
        "shadow_archive_results": {
            "combined": _summary(shadow_rows),
            "by_market": _by_market(shadow_rows),
            "daily": _daily_profit_by_market(shadow_rows),
            "payload_dates": list(shadow_payload.get("payload_dates") or []),
        },
        "latest_live_card_surface": latest_live_card,
        "variable_card_recommendation": _recommend_variable_mix(official_rows),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit the pregame market surface for official and shadow card profitability")
    parser.add_argument(
        "--cards-dir",
        default="data/eval/seasons/2026/locked_cards_retuned",
        help="Directory containing settled locked-policy cards to audit",
    )
    parser.add_argument(
        "--out",
        default="data/eval/_tmp_pregame_market_surface_audit.json",
        help="Where to write the audit JSON",
    )
    parser.add_argument(
        "--live-card-glob",
        default="data/daily/daily_summary_*_locked_policy.json",
        help="Glob for live locked-policy cards used to inspect current shadow-market coverage",
    )
    parser.add_argument(
        "--payload-dir",
        default="data/eval/seasons/2026/betting_day_payloads_retuned",
        help="Directory containing archived season betting-day payloads",
    )
    args = parser.parse_args()

    cards_dir = Path(args.cards_dir)
    if not cards_dir.is_absolute():
        cards_dir = (REPO_ROOT / cards_dir).resolve()
    if not cards_dir.exists():
        raise FileNotFoundError(f"Cards directory not found: {cards_dir}")

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (REPO_ROOT / out_path).resolve()

    payload_dir = Path(args.payload_dir)
    if not payload_dir.is_absolute():
        payload_dir = (REPO_ROOT / payload_dir).resolve()

    audit = build_audit(cards_dir, args.live_card_glob, payload_dir)
    _write_json(out_path, audit)
    print(json.dumps({
        "out": str(out_path),
        "official_combined": audit["official_results"]["combined"],
        "official_markets_seen": audit["coverage"]["official_markets_seen"],
        "playable_markets_seen": audit["coverage"]["playable_markets_seen"],
        "shadow_archive_results": audit["shadow_archive_results"],
        "latest_live_card_surface": audit["latest_live_card_surface"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())