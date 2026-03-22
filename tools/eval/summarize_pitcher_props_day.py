from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parents[2]

# Ensure V2 root is importable for market line helpers.
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.market_pitcher_props import (
    load_pitcher_prop_lines,
    normalize_pitcher_name,
    no_vig_over_prob,
)


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _dist_to_items(dist: Dict[str, Any]) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    if not isinstance(dist, dict):
        return out
    for k, v in dist.items():
        try:
            kk = int(k)
            vv = int(v)
        except Exception:
            continue
        if vv <= 0:
            continue
        out.append((kk, vv))
    out.sort(key=lambda x: x[0])
    return out


def _quantile_from_dist(dist: Dict[str, Any], q: float) -> Optional[float]:
    items = _dist_to_items(dist)
    if not items:
        return None
    total = sum(v for _, v in items)
    if total <= 0:
        return None
    target = q * total
    acc = 0
    for k, v in items:
        acc += v
        if acc >= target:
            return float(k)
    return float(items[-1][0])


def _mean_sd_from_dist(dist: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    items = _dist_to_items(dist)
    if not items:
        return None, None
    n = sum(v for _, v in items)
    if n <= 0:
        return None, None
    mu = sum(float(k) * float(v) for k, v in items) / float(n)
    var = sum(float(v) * (float(k) - mu) ** 2 for k, v in items) / float(n)
    return float(mu), float(math.sqrt(max(0.0, var)))


def _prob_ge(dist: Dict[str, Any], threshold: int) -> Optional[float]:
    items = _dist_to_items(dist)
    if not items:
        return None
    n = sum(v for _, v in items)
    if n <= 0:
        return None
    tot = sum(v for k, v in items if k >= int(threshold))
    return float(tot) / float(n)


def _prob_over_line(dist: Dict[str, Any], line: Any) -> Optional[float]:
    try:
        ln = float(line)
    except Exception:
        return None
    items = _dist_to_items(dist)
    if not items:
        return None
    n = sum(v for _, v in items)
    if n <= 0:
        return None
    over = sum(v for k, v in items if float(k) > ln)
    return float(over) / float(n)


def _starter_name_from_raw(season: int, date_str: str, game_pk: int, side: str, starter_id: int) -> str:
    try:
        import gzip

        p = _ROOT / "data" / "raw" / "statsapi" / "feed_live" / str(int(season)) / str(date_str) / f"{int(game_pk)}.json.gz"
        if not p.exists():
            return ""
        with gzip.open(p, "rt", encoding="utf-8") as f:
            feed = json.load(f)
        box = (((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}).get(str(side)) or {}
        players = box.get("players") or {}
        pobj = players.get(f"ID{int(starter_id)}") or {}
        person = pobj.get("person") or {}
        return str(person.get("fullName") or "")
    except Exception:
        return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize starter pitcher props from a sim-vs-actual day report")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", type=int, default=0)
    ap.add_argument("--report", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    season = int(args.season) if int(args.season or 0) > 0 else int(str(args.date).split("-")[0])

    report_path = Path(args.report) if str(args.report).strip() else (_ROOT / "data" / "eval" / f"sim_vs_actual_{args.date}.json")
    data = _load_json(report_path)

    # Load market lines (fallback if report doesn't already include them)
    market_lines, market_meta = load_pitcher_prop_lines(str(args.date), prefer="auto")

    out_path = Path(args.out) if str(args.out).strip() else (_ROOT / "data" / "eval" / f"pitcher_props_{args.date}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []

    for g in data.get("games") or []:
        game_pk = int(g.get("game_pk") or 0)
        away = g.get("away") or {}
        home = g.get("home") or {}

        for side in ("away", "home"):
            team = away if side == "away" else home
            team_abbr = str(team.get("abbr") or "")

            pp = ((g.get("pitcher_props") or {}).get(side) or {})
            starter_id = _safe_int(pp.get("starter_id"))
            if not starter_id:
                continue

            actual = pp.get("actual") or {}
            pred = pp.get("pred") or {}

            a_so = _safe_int(actual.get("so"))
            a_outs = _safe_int(actual.get("outs"))

            so_dist = pred.get("so_dist") or {}
            outs_dist = pred.get("outs_dist") or {}

            so_mean, so_sd = _mean_sd_from_dist(so_dist)
            outs_mean, outs_sd = _mean_sd_from_dist(outs_dist)

            so_p25 = _quantile_from_dist(so_dist, 0.25)
            so_p50 = _quantile_from_dist(so_dist, 0.50)
            so_p75 = _quantile_from_dist(so_dist, 0.75)
            outs_p25 = _quantile_from_dist(outs_dist, 0.25)
            outs_p50 = _quantile_from_dist(outs_dist, 0.50)
            outs_p75 = _quantile_from_dist(outs_dist, 0.75)

            # Common betting-style lines are halves. Over X.5 == P(stat >= X+1).
            so_line_half = None
            so_over = None
            if so_mean is not None:
                so_line_half = int(math.floor(float(so_mean))) + 0.5
                so_over = _prob_ge(so_dist, int(math.floor(float(so_mean))) + 1)

            outs_line_half = None
            outs_over = None
            if outs_mean is not None:
                outs_line_half = int(math.floor(float(outs_mean))) + 0.5
                outs_over = _prob_ge(outs_dist, int(math.floor(float(outs_mean))) + 1)

            name = _starter_name_from_raw(season, str(args.date), game_pk, side, int(starter_id))

            # Market lines/odds (prefer embedded in report if present)
            market = pp.get("market") if isinstance(pp, dict) else None
            if not market:
                nk = normalize_pitcher_name(name)
                market = {"name_key": nk, **(market_lines.get(nk) or {})} if nk in market_lines else None

            so_mkt = (market or {}).get("strikeouts") if isinstance(market, dict) else None
            outs_mkt = (market or {}).get("outs") if isinstance(market, dict) else None

            so_mkt_line = (so_mkt or {}).get("line") if isinstance(so_mkt, dict) else None
            outs_mkt_line = (outs_mkt or {}).get("line") if isinstance(outs_mkt, dict) else None

            so_mkt_p_over = _prob_over_line(so_dist, so_mkt_line) if so_mkt_line is not None else None
            outs_mkt_p_over = _prob_over_line(outs_dist, outs_mkt_line) if outs_mkt_line is not None else None

            so_mkt_y = (None if (a_so is None or so_mkt_line is None) else (1 if float(a_so) > float(so_mkt_line) else 0))
            outs_mkt_y = (None if (a_outs is None or outs_mkt_line is None) else (1 if float(a_outs) > float(outs_mkt_line) else 0))

            so_mkt_imp = no_vig_over_prob((so_mkt or {}).get("over_odds"), (so_mkt or {}).get("under_odds")) if isinstance(so_mkt, dict) else None
            outs_mkt_imp = no_vig_over_prob((outs_mkt or {}).get("over_odds"), (outs_mkt or {}).get("under_odds")) if isinstance(outs_mkt, dict) else None

            rows.append(
                {
                    "date": str(args.date),
                    "game_pk": game_pk,
                    "side": side,
                    "team": team_abbr,
                    "starter_id": int(starter_id),
                    "starter_name": name,
                    "actual_so": a_so,
                    "pred_so_mean": so_mean,
                    "pred_so_sd": so_sd,
                    "pred_so_p25": so_p25,
                    "pred_so_p50": so_p50,
                    "pred_so_p75": so_p75,
                    "pred_so_line_half": so_line_half,
                    "pred_so_prob_over": so_over,
                    "market_so_line": so_mkt_line,
                    "market_so_over_odds": (so_mkt or {}).get("over_odds") if isinstance(so_mkt, dict) else None,
                    "market_so_under_odds": (so_mkt or {}).get("under_odds") if isinstance(so_mkt, dict) else None,
                    "market_so_p_over_model": so_mkt_p_over,
                    "market_so_p_over_no_vig": so_mkt_imp,
                    "market_so_edge": (None if (so_mkt_p_over is None or so_mkt_imp is None) else float(so_mkt_p_over) - float(so_mkt_imp)),
                    "market_so_y_over": so_mkt_y,
                    "so_error": (None if (a_so is None or so_mean is None) else float(so_mean) - float(a_so)),
                    "actual_outs": a_outs,
                    "pred_outs_mean": outs_mean,
                    "pred_outs_sd": outs_sd,
                    "pred_outs_p25": outs_p25,
                    "pred_outs_p50": outs_p50,
                    "pred_outs_p75": outs_p75,
                    "pred_outs_line_half": outs_line_half,
                    "pred_outs_prob_over": outs_over,
                    "market_outs_line": outs_mkt_line,
                    "market_outs_over_odds": (outs_mkt or {}).get("over_odds") if isinstance(outs_mkt, dict) else None,
                    "market_outs_under_odds": (outs_mkt or {}).get("under_odds") if isinstance(outs_mkt, dict) else None,
                    "market_outs_p_over_model": outs_mkt_p_over,
                    "market_outs_p_over_no_vig": outs_mkt_imp,
                    "market_outs_edge": (None if (outs_mkt_p_over is None or outs_mkt_imp is None) else float(outs_mkt_p_over) - float(outs_mkt_imp)),
                    "market_outs_y_over": outs_mkt_y,
                    "outs_error": (None if (a_outs is None or outs_mean is None) else float(outs_mean) - float(a_outs)),
                }
            )

    # Sort by abs SO error (largest first) for readability in CSV
    def abs_key(r: Dict[str, Any]) -> float:
        e = r.get("so_error")
        try:
            return abs(float(e))
        except Exception:
            return -1.0

    rows.sort(key=abs_key, reverse=True)

    headers = [
        "date",
        "game_pk",
        "side",
        "team",
        "starter_id",
        "starter_name",
        "actual_so",
        "pred_so_mean",
        "pred_so_sd",
        "pred_so_p25",
        "pred_so_p50",
        "pred_so_p75",
        "pred_so_line_half",
        "pred_so_prob_over",
        "market_so_line",
        "market_so_over_odds",
        "market_so_under_odds",
        "market_so_p_over_model",
        "market_so_p_over_no_vig",
        "market_so_edge",
        "market_so_y_over",
        "so_error",
        "actual_outs",
        "pred_outs_mean",
        "pred_outs_sd",
        "pred_outs_p25",
        "pred_outs_p50",
        "pred_outs_p75",
        "pred_outs_line_half",
        "pred_outs_prob_over",
        "market_outs_line",
        "market_outs_over_odds",
        "market_outs_under_odds",
        "market_outs_p_over_model",
        "market_outs_p_over_no_vig",
        "market_outs_edge",
        "market_outs_y_over",
        "outs_error",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # Print a small console summary
    ok_so = [r for r in rows if isinstance(r.get("so_error"), (int, float))]
    ok_outs = [r for r in rows if isinstance(r.get("outs_error"), (int, float))]

    def mae(rs: List[Dict[str, Any]], key: str) -> Optional[float]:
        vals: List[float] = []
        for r in rs:
            try:
                vals.append(abs(float(r[key])))
            except Exception:
                pass
        if not vals:
            return None
        return sum(vals) / len(vals)

    print(f"Wrote: {out_path}")
    print("Starters:", len(rows))
    print("SO MAE:", mae(ok_so, "so_error"))
    print("OUTS MAE:", mae(ok_outs, "outs_error"))
    print("Top 5 SO misses:")
    for r in rows[:5]:
        print(
            f"  {r['team']} {r['starter_name']} (id {r['starter_id']}): "
            f"pred_so_mean={r['pred_so_mean']:.2f} actual_so={r['actual_so']} err={r['so_error']:.2f}; "
            f"pred_outs_mean={r['pred_outs_mean']:.1f} actual_outs={r['actual_outs']} err={r['outs_error']:.1f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
