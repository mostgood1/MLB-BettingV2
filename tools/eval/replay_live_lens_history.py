from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _date_range(start_date: date, end_date: date) -> List[str]:
    out: List[str] = []
    current = start_date
    while current <= end_date:
        out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def _safe_counts(payload: Dict[str, Any]) -> Dict[str, int]:
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    return {
        "games": int(counts.get("games") or 0),
        "live": int(counts.get("live") or 0),
        "final": int(counts.get("final") or 0),
        "pregame": int(counts.get("pregame") or 0),
        "props": int(counts.get("props") or 0),
        "archivedLiveProps": int(counts.get("archivedLiveProps") or 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay historical live-lens reports into an alternate live_lens root.")
    parser.add_argument("--min-date", required=True)
    parser.add_argument("--max-date", required=True)
    parser.add_argument("--out-live-lens-dir", required=True)
    parser.add_argument("--refresh-markets", choices=("off", "on"), default="off")
    parser.add_argument("--snapshot-mode", choices=("historical", "pregame"), default="historical")
    parser.add_argument("--compact", choices=("off", "on"), default="off")
    args = parser.parse_args()

    start_date = _parse_date(str(args.min_date))
    end_date = _parse_date(str(args.max_date))
    if end_date < start_date:
        raise SystemExit("max-date must be on or after min-date")

    out_dir = Path(str(args.out_live_lens_dir)).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    os.environ["MLB_LIVE_LENS_DIR"] = str(out_dir)

    from tools.web import flask_frontend as ff  # noqa: E402

    days = _date_range(start_date, end_date)
    results: List[Dict[str, Any]] = []
    ok = True
    original_loader = ff._load_live_lens_feed
    original_cards_loader = ff._load_live_lens_cards
    if str(args.snapshot_mode) == "pregame":
        def _pregame_loader(game_pk: int, d: str):
            return None

        ff._load_live_lens_feed = _pregame_loader
        def _pregame_cards_loader(d: str, *, artifacts=None, archive=None, schedule_games=None):
            cards = original_cards_loader(d, artifacts=artifacts, archive=archive, schedule_games=schedule_games)
            out: List[Dict[str, Any]] = []
            for raw_card in cards:
                if not isinstance(raw_card, dict):
                    continue
                card = dict(raw_card)
                card["status"] = {"abstract": "Preview", "detailed": "Pre-Game"}
                card["score"] = {"away": None, "home": None}
                out.append(card)
            return out

        ff._load_live_lens_cards = _pregame_cards_loader
    try:
        for day in days:
            try:
                payload = ff._persist_live_lens_tick(
                    day,
                    trigger=f"historical_replay:{str(args.snapshot_mode)}",
                    refresh_markets=(str(args.refresh_markets) == "on"),
                )
                counts = _safe_counts(payload)
                results.append(
                    {
                        "date": day,
                        "ok": True,
                        "counts": counts,
                        "reportPath": str(ff._live_lens_report_path(day)),
                    }
                )
            except Exception as exc:
                ok = False
                results.append(
                    {
                        "date": day,
                        "ok": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    finally:
        ff._load_live_lens_feed = original_loader
        ff._load_live_lens_cards = original_cards_loader

    compact_result: Dict[str, Any] | None = None
    if str(args.compact) == "on":
        compact_result = ff._compact_live_lens_days(str(args.min_date), str(args.max_date), apply_changes=True)

    print(
        json.dumps(
            {
                "ok": bool(ok),
                "out_live_lens_dir": str(out_dir),
                "min_date": str(args.min_date),
                "max_date": str(args.max_date),
                "snapshot_mode": str(args.snapshot_mode),
                "days": results,
                "compact": compact_result,
            },
            indent=2,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())