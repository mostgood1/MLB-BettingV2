from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        number = float(value)
    except Exception:
        return None
    return number if number == number else None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _slug_to_date(token: str) -> str:
    parts = str(token or "").split("_")
    if len(parts) == 3:
        return "-".join(parts)
    return str(token or "")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _feed_live_path(date_str: str, game_pk: int) -> Path:
    season = str(date_str).split("-", 1)[0]
    return (REPO_ROOT / "data" / "raw" / "statsapi" / "feed_live" / season / date_str / f"{int(game_pk)}.json.gz").resolve()


def _read_feed(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _is_final_game_status(value: Any) -> bool:
    token = str(value or "").strip().lower()
    if not token:
        return False
    return token in {"final", "completed early", "game over"} or token.startswith("final") or token.startswith("completed")


def _feed_is_final(feed: Dict[str, Any]) -> bool:
    status = (feed.get("gameData") or {}).get("status") or {}
    return _is_final_game_status(status.get("abstractGameState")) or _is_final_game_status(status.get("detailedState"))


def _load_final_feed(date_str: str, game_pk: int) -> Optional[Dict[str, Any]]:
    feed = _read_feed(_feed_live_path(date_str, int(game_pk)))
    if not isinstance(feed, dict) or not _feed_is_final(feed):
        return None
    return feed


def _segment_score(feed: Dict[str, Any], key: str) -> Optional[Dict[str, float]]:
    linescore = ((feed.get("liveData") or {}).get("linescore") or {}) if isinstance(feed, dict) else {}
    innings = linescore.get("innings") if isinstance(linescore.get("innings"), list) else []

    def _inning_sum(limit: Optional[int]) -> Dict[str, float]:
        away = 0.0
        home = 0.0
        subset = innings if limit is None else innings[: max(0, int(limit))]
        for row in subset:
            if not isinstance(row, dict):
                continue
            away += float(_safe_float(((row.get("away") or {}).get("runs"))) or 0.0)
            home += float(_safe_float(((row.get("home") or {}).get("runs"))) or 0.0)
        return {
            "away": away,
            "home": home,
            "total": away + home,
            "homeMargin": home - away,
        }

    if key in {"live", "full"}:
        teams = linescore.get("teams") if isinstance(linescore.get("teams"), dict) else {}
        away = float(_safe_float(((teams.get("away") or {}).get("runs"))) or 0.0)
        home = float(_safe_float(((teams.get("home") or {}).get("runs"))) or 0.0)
        return {
            "away": away,
            "home": home,
            "total": away + home,
            "homeMargin": home - away,
        }
    if key == "first1":
        return _inning_sum(1)
    if key == "first3":
        return _inning_sum(3)
    if key == "first5":
        return _inning_sum(5)
    if key == "first7":
        return _inning_sum(7)
    return None


def _american_profit(odds: Any) -> Optional[float]:
    value = _safe_int(odds)
    if value is None or value == 0:
        return None
    if value > 0:
        return float(value) / 100.0
    return 100.0 / abs(float(value))


def _settle_moneyline(pick: str, actual: Dict[str, float]) -> Optional[bool]:
    token = str(pick or "").strip().lower()
    margin = float(actual.get("homeMargin") or 0.0)
    if token == "home":
        return margin > 0.0
    if token == "away":
        return margin < 0.0
    if token == "draw":
        return abs(margin) < 1e-9
    return None


def _settle_spread(pick: str, line: Any, actual: Dict[str, float]) -> Optional[bool]:
    token = str(pick or "").strip().lower()
    home_line = _safe_float(line)
    margin = float(actual.get("homeMargin") or 0.0)
    if home_line is None:
        return None
    adjusted_home = margin + float(home_line)
    if token == "home":
        return adjusted_home > 0.0
    if token == "away":
        return adjusted_home < 0.0
    return None


def _settle_total(pick: str, line: Any, actual: Dict[str, float]) -> Optional[bool]:
    token = str(pick or "").strip().lower()
    total_line = _safe_float(line)
    total = float(actual.get("total") or 0.0)
    if total_line is None:
        return None
    if token == "over":
        return total > float(total_line)
    if token == "under":
        return total < float(total_line)
    return None


def _iter_report_paths(live_lens_dir: Path, *, use_render_sync: bool) -> Iterable[Path]:
    if use_render_sync:
        base = live_lens_dir / "render_sync"
        if base.exists():
            yield from sorted(base.glob("live_lens_reports_*.json"))
        return
    yield from sorted(live_lens_dir.glob("live_lens_report_*.json"))


def _games_from_report(payload: Dict[str, Any], *, use_render_sync: bool) -> List[Dict[str, Any]]:
    if use_render_sync:
        latest_report = payload.get("latestReport") if isinstance(payload.get("latestReport"), dict) else {}
        games = latest_report.get("games") if isinstance(latest_report.get("games"), list) else []
        return [game for game in games if isinstance(game, dict)]
    games = payload.get("games") if isinstance(payload.get("games"), list) else []
    return [game for game in games if isinstance(game, dict)]


def _iter_settled_rows(live_lens_dir: Path, *, use_render_sync: bool, min_date: str, max_date: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for report_path in _iter_report_paths(live_lens_dir, use_render_sync=use_render_sync):
        try:
            payload = _read_json(report_path)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        date_str = str(payload.get("date") or _slug_to_date(report_path.stem.split("reports_")[-1].split("report_")[-1])).strip()
        if min_date and date_str < min_date:
            continue
        if max_date and date_str > max_date:
            continue
        final_feed_cache: Dict[int, Optional[Dict[str, Any]]] = {}
        for game in _games_from_report(payload, use_render_sync=use_render_sync):
            game_pk = _safe_int(game.get("gamePk"))
            if game_pk is None:
                continue
            if int(game_pk) not in final_feed_cache:
                final_feed_cache[int(game_pk)] = _load_final_feed(date_str, int(game_pk))
            final_feed = final_feed_cache.get(int(game_pk))
            if not isinstance(final_feed, dict):
                continue
            matchup = game.get("matchup") if isinstance(game.get("matchup"), dict) else {}
            away_abbr = str((((matchup.get("away") or {}).get("abbr")) or "")).strip()
            home_abbr = str((((matchup.get("home") or {}).get("abbr")) or "")).strip()
            for lens_row in (game.get("gameLens") or []):
                if not isinstance(lens_row, dict):
                    continue
                segment_key = str(lens_row.get("key") or "").strip().lower()
                actual = _segment_score(final_feed, segment_key)
                if not isinstance(actual, dict):
                    continue
                markets = lens_row.get("markets") if isinstance(lens_row.get("markets"), dict) else {}

                ml_market = markets.get("moneyline") if isinstance(markets.get("moneyline"), dict) else {}
                ml_pick = str(ml_market.get("pick") or "").strip().lower()
                if ml_pick:
                    win = _settle_moneyline(ml_pick, actual)
                    odds = ml_market.get("homeOdds") if ml_pick == "home" else ml_market.get("awayOdds") if ml_pick == "away" else ml_market.get("drawOdds")
                    out.append({
                        "date": date_str,
                        "gamePk": int(game_pk),
                        "matchup": f"{away_abbr} at {home_abbr}".strip(),
                        "segment": segment_key,
                        "lane": "moneyline",
                        "pick": ml_pick,
                        "line": None,
                        "odds": _safe_int(odds),
                        "win": win,
                        "edge": _safe_float(ml_market.get("edge")),
                    })

                spread_market = markets.get("spread") if isinstance(markets.get("spread"), dict) else {}
                spread_pick = str(spread_market.get("pick") or "").strip().lower()
                if spread_pick:
                    spread_line = _safe_float(spread_market.get("homeLine"))
                    win = _settle_spread(spread_pick, spread_line, actual)
                    odds = spread_market.get("homeOdds") if spread_pick == "home" else spread_market.get("awayOdds")
                    out.append({
                        "date": date_str,
                        "gamePk": int(game_pk),
                        "matchup": f"{away_abbr} at {home_abbr}".strip(),
                        "segment": segment_key,
                        "lane": "spread",
                        "pick": spread_pick,
                        "line": spread_line,
                        "odds": _safe_int(odds),
                        "win": win,
                        "edge": _safe_float(spread_market.get("edge")),
                    })

                total_market = markets.get("total") if isinstance(markets.get("total"), dict) else {}
                total_pick = str(total_market.get("pick") or "").strip().lower()
                if total_pick:
                    total_line = _safe_float(total_market.get("line"))
                    win = _settle_total(total_pick, total_line, actual)
                    odds = total_market.get("overOdds") if total_pick == "over" else total_market.get("underOdds")
                    out.append({
                        "date": date_str,
                        "gamePk": int(game_pk),
                        "matchup": f"{away_abbr} at {home_abbr}".strip(),
                        "segment": segment_key,
                        "lane": "total",
                        "pick": total_pick,
                        "line": total_line,
                        "odds": _safe_int(odds),
                        "win": win,
                        "edge": _safe_float(total_market.get("edge")),
                    })
    return out


def _summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    settled = [row for row in rows if row.get("win") in {True, False} and row.get("odds") is not None]
    n = len(settled)
    wins = sum(1 for row in settled if bool(row.get("win")))
    profit = 0.0
    for row in settled:
        if bool(row.get("win")):
            profit += float(_american_profit(row.get("odds")) or 0.0)
        else:
            profit -= 1.0
    avg_edge_values = [float(row.get("edge")) for row in settled if _safe_float(row.get("edge")) is not None]
    return {
        "n": n,
        "wins": wins,
        "win_rate": round(wins / n, 4) if n else None,
        "roi": round(profit / n, 4) if n else None,
        "avg_edge": round(sum(avg_edge_values) / len(avg_edge_values), 4) if avg_edge_values else None,
    }


def _group_summary(rows: List[Dict[str, Any]], field: str) -> Dict[str, Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get(field) or "")
        groups.setdefault(key, []).append(row)
    return {key: _summarize(group_rows) for key, group_rows in sorted(groups.items()) if key}


def main() -> None:
    parser = argparse.ArgumentParser(description="Settle archived live game-lens recommendations against completed-game final feeds.")
    parser.add_argument("--live-lens-dir", default="data/live_lens")
    parser.add_argument("--source", choices=("report", "render_sync"), default="report")
    parser.add_argument("--min-date", default="")
    parser.add_argument("--max-date", default="")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    live_lens_dir = (REPO_ROOT / str(args.live_lens_dir)).resolve()
    rows = _iter_settled_rows(
        live_lens_dir,
        use_render_sync=(str(args.source) == "render_sync"),
        min_date=str(args.min_date or ""),
        max_date=str(args.max_date or ""),
    )
    payload = {
        "source": str(args.source),
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
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({
        "summary": payload["summary"],
        "by_date": payload["by_date"],
        "by_segment": payload["by_segment"],
        "by_lane": payload["by_lane"],
        "rows": len(rows),
        "out": str(out_path) if out_path is not None else None,
    }, indent=2))


if __name__ == "__main__":
    main()