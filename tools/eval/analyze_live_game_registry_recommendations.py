from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tools.eval.analyze_live_game_lens_recommendations import (  # noqa: E402
    _american_profit,
    _feed_is_final,
    _feed_live_path,
    _group_summary,
    _read_feed,
    _safe_float,
    _safe_int,
    _settle_moneyline,
    _settle_spread,
    _settle_total,
    _slug_to_date,
    _summarize,
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_final_feed(date_str: str, game_pk: int) -> Optional[Dict[str, Any]]:
    feed = _read_feed(_feed_live_path(date_str, int(game_pk)))
    if not isinstance(feed, dict) or not _feed_is_final(feed):
        return None
    return feed


def _final_score(feed: Dict[str, Any]) -> Dict[str, float]:
    linescore = ((feed.get("liveData") or {}).get("linescore") or {}) if isinstance(feed, dict) else {}
    teams = linescore.get("teams") if isinstance(linescore.get("teams"), dict) else {}
    away = float(_safe_float(((teams.get("away") or {}).get("runs"))) or 0.0)
    home = float(_safe_float(((teams.get("home") or {}).get("runs"))) or 0.0)
    return {
        "away": away,
        "home": home,
        "total": away + home,
        "homeMargin": home - away,
    }


def _iter_registry_paths(live_lens_dir: Path) -> List[Path]:
    base = live_lens_dir / "game_registry"
    if not base.exists():
        return []
    return sorted(base.glob("live_game_registry_*.json"))


def _settle_snapshot(market_type: str, selection: str, line: Any, actual: Dict[str, float]) -> Optional[bool]:
    market_key = str(market_type or "").strip().lower()
    pick = str(selection or "").strip().lower()
    if market_key == "moneyline":
        return _settle_moneyline(pick, actual)
    if market_key == "spread":
        return _settle_spread(pick, line, actual)
    if market_key == "total":
        return _settle_total(pick, line, actual)
    return None


def _matchup_from_feed(feed: Dict[str, Any]) -> str:
    teams = ((feed.get("gameData") or {}).get("teams") or {}) if isinstance(feed, dict) else {}
    away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
    home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
    away_abbr = str(away.get("abbreviation") or away.get("teamName") or "").strip()
    home_abbr = str(home.get("abbreviation") or home.get("teamName") or "").strip()
    return f"{away_abbr} at {home_abbr}".strip()


def _snapshot_row(*, date_str: str, game_pk: int, matchup: str, lane_key: str, market_type: str, snapshot_kind: str, entry: Dict[str, Any], snapshot: Dict[str, Any], actual: Dict[str, float]) -> Optional[Dict[str, Any]]:
    selection = str(entry.get("selection") or snapshot.get("selection") or "").strip().lower()
    market_line = _safe_float(entry.get("marketLine"))
    if market_type == "spread" and market_line is None:
        market_line = _safe_float(snapshot.get("marketLine"))
    if market_type == "total" and market_line is None:
        market_line = _safe_float(snapshot.get("marketLine"))
    win = _settle_snapshot(market_type, selection, market_line, actual)
    odds = _safe_int(snapshot.get("odds"))
    profit = _american_profit(odds) if win is True else (-1.0 if win is False and odds is not None else None)
    return {
        "date": date_str,
        "gamePk": int(game_pk),
        "matchup": matchup,
        "segment": str(lane_key or "").strip().lower(),
        "lane": f"{str(market_type or '').strip().lower()}:{str(snapshot_kind)}",
        "pick": selection,
        "line": market_line,
        "odds": odds,
        "win": win,
        "edge": _safe_float(snapshot.get("edge")),
        "profit": profit,
        "snapshot_kind": str(snapshot_kind),
        "first_seen_at": entry.get("firstSeenAt"),
        "last_seen_at": entry.get("lastSeenAt"),
        "seen_count": _safe_int(entry.get("seenCount")),
    }


def _iter_settled_rows(live_lens_dir: Path, *, min_date: str, max_date: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for registry_path in _iter_registry_paths(live_lens_dir):
        try:
            payload = _read_json(registry_path)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        date_str = str(payload.get("date") or _slug_to_date(registry_path.stem.replace("live_game_registry_", ""))).strip()
        if min_date and date_str < min_date:
            continue
        if max_date and date_str > max_date:
            continue
        entries = payload.get("entries") if isinstance(payload.get("entries"), dict) else {}
        final_feed_cache: Dict[int, Optional[Dict[str, Any]]] = {}
        for entry in entries.values():
            if not isinstance(entry, dict):
                continue
            game_pk = _safe_int(entry.get("gamePk"))
            if game_pk is None:
                continue
            if int(game_pk) not in final_feed_cache:
                final_feed_cache[int(game_pk)] = _load_final_feed(date_str, int(game_pk))
            final_feed = final_feed_cache.get(int(game_pk))
            if not isinstance(final_feed, dict):
                continue
            actual = _final_score(final_feed)
            matchup = _matchup_from_feed(final_feed)
            lane_key = str(entry.get("laneKey") or "").strip().lower()
            market_type = str(entry.get("marketType") or "").strip().lower()
            first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
            last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
            if isinstance(first_snapshot, dict) and first_snapshot:
                row = _snapshot_row(
                    date_str=date_str,
                    game_pk=int(game_pk),
                    matchup=matchup,
                    lane_key=lane_key,
                    market_type=market_type,
                    snapshot_kind="first_seen",
                    entry=entry,
                    snapshot=first_snapshot,
                    actual=actual,
                )
                if isinstance(row, dict):
                    out.append(row)
            if isinstance(last_snapshot, dict) and last_snapshot:
                row = _snapshot_row(
                    date_str=date_str,
                    game_pk=int(game_pk),
                    matchup=matchup,
                    lane_key=lane_key,
                    market_type=market_type,
                    snapshot_kind="last_seen",
                    entry=entry,
                    snapshot=last_snapshot,
                    actual=actual,
                )
                if isinstance(row, dict):
                    out.append(row)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Settle archived live game-registry recommendations against final feeds.")
    parser.add_argument("--live-lens-dir", default="data/live_lens")
    parser.add_argument("--min-date", default="")
    parser.add_argument("--max-date", default="")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    live_lens_dir = (REPO_ROOT / str(args.live_lens_dir)).resolve()
    rows = _iter_settled_rows(live_lens_dir, min_date=str(args.min_date or ""), max_date=str(args.max_date or ""))
    payload = {
        "live_lens_dir": str(live_lens_dir),
        "min_date": str(args.min_date or ""),
        "max_date": str(args.max_date or ""),
        "summary": _summarize(rows),
        "by_date": _group_summary(rows, "date"),
        "by_segment": _group_summary(rows, "segment"),
        "by_lane": _group_summary(rows, "lane"),
        "rows": rows,
    }
    out_path = Path(str(args.out)).resolve() if str(args.out or "").strip() else None
    if isinstance(out_path, Path):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()