from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


_ROOT = Path(__file__).resolve().parents[2]


def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _token_to_date(token: str) -> str:
    # oddsapi_*_YYYY_MM_DD.json
    parts = token.split("_")
    if len(parts) >= 3:
        y, m, d = parts[-3], parts[-2], parts[-1]
        if len(y) == 4 and len(m) == 2 and len(d) == 2:
            return f"{y}-{m}-{d}"
    return token


@dataclass
class CoverageRow:
    date: str
    games: int
    pitcher_players: int
    pitcher_k: int
    pitcher_outs: int
    hitter_players: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "games": self.games,
            "pitcher_players": self.pitcher_players,
            "pitcher_k": self.pitcher_k,
            "pitcher_outs": self.pitcher_outs,
            "hitter_players": self.hitter_players,
        }


def summarize(market_dir: Path) -> List[CoverageRow]:
    by_date: Dict[str, Dict[str, Any]] = {}

    for p in market_dir.glob("oddsapi_pitcher_props_*.json"):
        token = p.stem
        date = _token_to_date(token)
        doc = _read_json(p) or {}
        props = doc.get("pitcher_props") or {}
        if not isinstance(props, dict):
            props = {}
        pitcher_players = 0
        k = 0
        outs = 0
        for _name, mk in props.items():
            if not isinstance(mk, dict):
                continue
            pitcher_players += 1
            if isinstance(mk.get("strikeouts"), dict) and mk["strikeouts"].get("line") is not None:
                k += 1
            if isinstance(mk.get("outs"), dict) and mk["outs"].get("line") is not None:
                outs += 1
        by_date.setdefault(date, {})["pitcher"] = {
            "players": pitcher_players,
            "k": k,
            "outs": outs,
        }

    for p in market_dir.glob("oddsapi_hitter_props_*.json"):
        token = p.stem
        date = _token_to_date(token)
        doc = _read_json(p) or {}
        props = doc.get("hitter_props") or {}
        if not isinstance(props, dict):
            props = {}
        by_date.setdefault(date, {})["hitter"] = {
            "players": int(len(props)),
        }

    for p in market_dir.glob("oddsapi_game_lines_*.json"):
        token = p.stem
        date = _token_to_date(token)
        doc = _read_json(p) or {}
        games = doc.get("games") or []
        if not isinstance(games, list):
            games = []
        by_date.setdefault(date, {})["games"] = {"count": int(len(games))}

    out: List[CoverageRow] = []
    for date, parts in by_date.items():
        games = int(((parts.get("games") or {}).get("count") or 0))
        pit = parts.get("pitcher") or {}
        hit = parts.get("hitter") or {}
        out.append(
            CoverageRow(
                date=str(date),
                games=games,
                pitcher_players=int(pit.get("players") or 0),
                pitcher_k=int(pit.get("k") or 0),
                pitcher_outs=int(pit.get("outs") or 0),
                hitter_players=int(hit.get("players") or 0),
            )
        )

    out.sort(key=lambda r: r.date)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize V2 OddsAPI coverage in data/market/oddsapi")
    ap.add_argument("--market-dir", default=str(_ROOT / "data" / "market" / "oddsapi"))
    ap.add_argument("--min-games", type=int, default=1)
    ap.add_argument("--min-pitcher-k", type=int, default=10)
    ap.add_argument("--min-pitcher-outs", type=int, default=10)
    ap.add_argument("--sample", type=int, default=0, help="If >0, sample N qualifying dates")
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--out", default=None, help="Optional output JSON path")
    ap.add_argument("--out-date-file", default=None, help="Optional output date file (one YYYY-MM-DD per line); writes sampled if --sample>0 else qualifying")
    args = ap.parse_args()

    market_dir = Path(args.market_dir)
    rows = summarize(market_dir)

    qualifying = [
        r
        for r in rows
        if r.games >= int(args.min_games)
        and r.pitcher_k >= int(args.min_pitcher_k)
        and r.pitcher_outs >= int(args.min_pitcher_outs)
    ]

    payload: Dict[str, Any] = {
        "market_dir": str(market_dir),
        "dates_total": len(rows),
        "dates_qualifying": len(qualifying),
        "qualifying": [r.to_dict() for r in qualifying],
    }

    if int(args.sample) > 0:
        rng = random.Random(int(args.seed))
        sample_n = min(int(args.sample), len(qualifying))
        sampled = rng.sample(qualifying, k=sample_n) if sample_n > 0 else []
        payload["sampled"] = [r.to_dict() for r in sorted(sampled, key=lambda r: r.date)]

    if args.out_date_file:
        out_path = Path(str(args.out_date_file))
        if not out_path.is_absolute():
            out_path = (Path.cwd() / out_path).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if int(args.sample) > 0:
            dates = [r["date"] for r in (payload.get("sampled") or [])]
        else:
            dates = [r["date"] for r in (payload.get("qualifying") or [])]
        out_path.write_text("\n".join(dates) + ("\n" if dates else ""), encoding="utf-8")

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        print(json.dumps(payload, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
