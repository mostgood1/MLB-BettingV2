from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sim_engine.prob_calibration import apply_prob_calibration
from tools.eval.eval_sim_day_vs_actual import _prob_over_line_from_dist, no_vig_over_prob
from sim_engine.market_pitcher_props import market_side_probabilities


MARKETS: Tuple[str, ...] = ("totals", "ml", "hitter_props", "pitcher_props")

DEFAULT_POLICY: Dict[str, Any] = {
    "totals_side": "over",
    "totals_diff_min": 0.4,
    "ml_side": "home",
    "ml_edge_min": 0.01,
    "hitter_edge_min": 0.0,
    "pitcher_market": "outs",
    "pitcher_side": "over",
    "pitcher_edge_min": 0.01,
}

DEFAULT_CAP_SCENARIOS: Dict[str, Dict[str, int]] = {
    "uncapped": {},
    "hitter20": {"hitter_props": 20},
    "soft": {"totals": 3, "ml": 2, "pitcher_props": 4, "hitter_props": 25},
    "medium": {"totals": 2, "ml": 1, "pitcher_props": 3, "hitter_props": 15},
    "tight": {"totals": 1, "ml": 1, "pitcher_props": 2, "hitter_props": 10},
    "tight_p3": {"totals": 1, "ml": 1, "pitcher_props": 3, "hitter_props": 10},
}

STANDARD_STAKE_U = 1.0
HITTER_STAKE_U = 0.25

HITTER_SUBMARKETS: Tuple[str, ...] = (
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
)

HITTER_MARKET_SPECS: Dict[str, Dict[str, str]] = {
    "batter_home_runs": {"submarket": "hitter_home_runs", "prob_base": "hr", "actual_field": "actual_hr_y"},
    "batter_hits": {"submarket": "hitter_hits", "prob_base": "hits", "actual_field": "actual_hits"},
    "batter_total_bases": {
        "submarket": "hitter_total_bases",
        "prob_base": "total_bases",
        "actual_field": "actual_total_bases",
    },
    "batter_runs_scored": {"submarket": "hitter_runs", "prob_base": "runs", "actual_field": "actual_runs"},
    "batter_rbis": {"submarket": "hitter_rbis", "prob_base": "rbi", "actual_field": "actual_rbi"},
}

HITTER_BACKTEST_PROP_FIELDS: List[Tuple[str, str]] = [
    ("hits_1plus", "actual_hits"),
    ("hits_2plus", "actual_hits"),
    ("hits_3plus", "actual_hits"),
    ("runs_1plus", "actual_runs"),
    ("runs_2plus", "actual_runs"),
    ("runs_3plus", "actual_runs"),
    ("rbi_1plus", "actual_rbi"),
    ("rbi_2plus", "actual_rbi"),
    ("rbi_3plus", "actual_rbi"),
    ("rbi_4plus", "actual_rbi"),
    ("total_bases_1plus", "actual_total_bases"),
    ("total_bases_2plus", "actual_total_bases"),
    ("total_bases_3plus", "actual_total_bases"),
    ("total_bases_4plus", "actual_total_bases"),
    ("total_bases_5plus", "actual_total_bases"),
]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _norm_name(value: str) -> str:
    value = (value or "").lower().strip()
    value = value.replace("'", "")
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _profit_from_american(odds: Any, won: bool, stake_u: float = STANDARD_STAKE_U) -> Optional[float]:
    if odds is None:
        return None
    raw = str(odds).strip()
    if not raw:
        return None
    if raw[0] == "+":
        raw = raw[1:]
    value = float(raw)
    stake = float(stake_u)
    if won:
        return stake * ((value / 100.0) if value > 0 else (100.0 / abs(value)))
    return -stake


def _imp_from_american(odds: Any) -> Optional[float]:
    if odds is None:
        return None
    raw = str(odds).strip()
    if not raw:
        return None
    if raw[0] == "+":
        raw = raw[1:]
    value = float(raw)
    return 100.0 / (value + 100.0) if value > 0 else abs(value) / (abs(value) + 100.0)


def _no_vig_two_way(a_odds: Any, b_odds: Any) -> Tuple[Optional[float], Optional[float]]:
    pa = _imp_from_american(a_odds)
    pb = _imp_from_american(b_odds)
    if pa is None or pb is None:
        return None, None
    total = pa + pb
    if total <= 0:
        return None, None
    return pa / total, pb / total


def _percentile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    rank = max(0.0, min(1.0, float(q))) * (len(values) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(values) - 1)
    frac = rank - lo
    return float(values[lo] * (1.0 - frac) + values[hi] * frac)


def _read_dates(paths: Iterable[Path]) -> List[str]:
    dates = set()
    for path in paths:
        for raw in path.read_text(encoding="utf-8").splitlines():
            value = raw.strip()
            if value and not value.startswith("#"):
                dates.add(value)
    return sorted(dates)


def _half_line_to_threshold(line: float) -> Optional[int]:
    try:
        line_value = float(line)
    except Exception:
        return None
    threshold = int(round(line_value + 0.5))
    if threshold < 1:
        return None
    expected_line = float(threshold) - 0.5
    if abs(line_value - expected_line) > 1e-9:
        return None
    return int(threshold)


def _hitter_prob_key(prop_base: str, threshold: int) -> str:
    return f"{str(prop_base)}_{int(threshold)}plus"


def _get_hitter_prob(market_key: str, line: float, rec: Dict[str, Optional[float]]) -> Optional[float]:
    spec = HITTER_MARKET_SPECS.get(str(market_key))
    if not isinstance(spec, dict):
        return None
    threshold = _half_line_to_threshold(line)
    if threshold is None:
        return None
    prop_base = str(spec.get("prob_base") or "").strip()
    if not prop_base:
        return None
    if prop_base == "hr" and int(threshold) != 1:
        return None
    return rec.get(_hitter_prob_key(prop_base, int(threshold)))


def _base_row(
    date: str,
    market: str,
    edge: float,
    profit_u: float,
    *,
    stake_u: float = STANDARD_STAKE_U,
    submarket: Optional[str] = None,
) -> Dict[str, Any]:
    row = {
        "date": date,
        "market": market,
        "edge": float(edge),
        "stake_u": float(stake_u),
        "profit_u": float(profit_u),
        "result": "win" if float(profit_u) > 0 else "loss",
    }
    if submarket:
        row["submarket"] = str(submarket)
    return row


def _score_game_batch(root: Path, batch_dir: Path, policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    odds_root = root / "data" / "market" / "oddsapi"
    for report_path in sorted(batch_dir.glob("sim_vs_actual_*.json")):
        obj = _read_json(report_path)
        date = str((obj.get("meta") or {}).get("date") or "").strip()
        if not date:
            continue
        token = date.replace("-", "_")
        line_path = odds_root / f"oddsapi_game_lines_{token}.json"
        if not line_path.exists():
            continue
        lines = _read_json(line_path).get("games") or []
        lookup = {(g.get("away_team"), g.get("home_team")): g for g in lines}
        for game in obj.get("games") or []:
            away = (game.get("away") or {}).get("name")
            home = (game.get("home") or {}).get("name")
            market_game = lookup.get((away, home))
            if not market_game:
                continue

            full = (game.get("segments") or {}).get("full") or {}
            actual = full.get("actual") or {}
            away_runs = int(actual.get("away") or 0)
            home_runs = int(actual.get("home") or 0)
            actual_total = away_runs + home_runs

            totals_market = ((market_game.get("markets") or {}).get("totals") or {})
            total_line = totals_market.get("line")
            if total_line is not None:
                pred_total = float(full.get("mean_total_runs") or 0.0)
                total_edge = pred_total - float(total_line)
                if total_edge >= float(policy["totals_diff_min"]) and abs(actual_total - float(total_line)) > 1e-9:
                    won = actual_total > float(total_line)
                    profit_u = _profit_from_american(totals_market.get("over_odds"), won)
                    if profit_u is not None:
                        rows.append(_base_row(date, "totals", total_edge, profit_u, stake_u=STANDARD_STAKE_U))

            h2h_market = ((market_game.get("markets") or {}).get("h2h") or {})
            home_prob = float(full.get("home_win_prob") or 0.0)
            away_prob = float(full.get("away_win_prob") or 0.0)
            total_prob = home_prob + away_prob
            if total_prob > 0:
                home_prob /= total_prob
                away_prob /= total_prob
                home_nv, away_nv = _no_vig_two_way(h2h_market.get("home_odds"), h2h_market.get("away_odds"))
                if home_nv is not None and away_nv is not None:
                    home_edge = home_prob - home_nv
                    if home_edge >= float(policy["ml_edge_min"]):
                        won = home_runs > away_runs
                        profit_u = _profit_from_american(h2h_market.get("home_odds"), won)
                        if profit_u is not None:
                            rows.append(_base_row(date, "ml", home_edge, profit_u, stake_u=STANDARD_STAKE_U))
    return rows


def _score_hitter_batch(root: Path, batch_dir: Path, policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    odds_root = root / "data" / "market" / "oddsapi"
    for report_path in sorted(batch_dir.glob("sim_vs_actual_*.json")):
        obj = _read_json(report_path)
        date = str((obj.get("meta") or {}).get("date") or "").strip()
        if not date:
            continue
        token = date.replace("-", "_")
        hitter_odds_path = odds_root / f"oddsapi_hitter_props_{token}.json"
        if not hitter_odds_path.exists():
            continue
        hitter_odds = _read_json(hitter_odds_path).get("hitter_props") or {}
        hitter_odds = {_norm_name(k): v for k, v in hitter_odds.items()}

        for game in obj.get("games") or []:
            pred: Dict[str, Dict[str, Optional[float]]] = {}

            def rec_for(name: str) -> Optional[Dict[str, Optional[float]]]:
                key = _norm_name(name)
                if not key:
                    return None
                if key not in pred:
                    pred[key] = {
                        "hr_1plus": None,
                        "actual_hits": None,
                        "actual_runs": None,
                        "actual_rbi": None,
                        "actual_total_bases": None,
                        "actual_hr_y": None,
                    }
                return pred[key]

            hitter_bt = game.get("hitter_props_backtest") or {}
            for prop_key, actual_key in HITTER_BACKTEST_PROP_FIELDS:
                for row in ((hitter_bt.get(prop_key) or {}).get("scored") or []):
                    rec = rec_for(str(row.get("name") or ""))
                    if rec is None:
                        continue
                    prob = row.get("p_cal")
                    actual = row.get("actual")
                    if isinstance(prob, (int, float)):
                        rec[prop_key] = float(prob)
                    if isinstance(actual, (int, float)):
                        rec[actual_key] = float(actual)

            hr_bt = game.get("hitter_hr_backtest") or {}
            for row in (hr_bt.get("scored_overall") or []):
                rec = rec_for(str(row.get("name") or ""))
                if rec is None:
                    continue
                prob = row.get("p_hr_1plus_cal", row.get("p_hr_1plus"))
                actual = row.get("y_hr_1plus")
                if isinstance(prob, (int, float)):
                    rec["hr_1plus"] = float(prob)
                if isinstance(actual, (int, float)):
                    rec["actual_hr_y"] = float(actual)

            for player_name, rec in pred.items():
                market = hitter_odds.get(player_name)
                if not isinstance(market, dict):
                    continue
                for market_key, market_spec in HITTER_MARKET_SPECS.items():
                    props_market = market.get(market_key) or {}
                    line = props_market.get("line")
                    if line is None:
                        continue
                    line_value = float(line)
                    p_over = _get_hitter_prob(market_key, line_value, rec)
                    if p_over is None:
                        continue
                    side_probs = market_side_probabilities(props_market.get("over_odds"), props_market.get("under_odds"))
                    if not side_probs:
                        continue

                    candidates: List[Tuple[bool, float]] = []
                    market_prob_over = side_probs.get("over")
                    if isinstance(market_prob_over, (int, float)) and props_market.get("over_odds") is not None:
                        edge_over = float(p_over) - float(market_prob_over)
                        if edge_over >= float(policy["hitter_edge_min"]):
                            candidates.append((True, float(edge_over)))
                    market_prob_under = side_probs.get("under")
                    if isinstance(market_prob_under, (int, float)) and props_market.get("under_odds") is not None:
                        edge_under = float(1.0 - float(p_over)) - float(market_prob_under)
                        if edge_under >= float(policy["hitter_edge_min"]):
                            candidates.append((False, float(edge_under)))
                    if not candidates:
                        continue

                    pick_over, edge = max(candidates, key=lambda row: (float(row[1]), 1 if row[0] else 0))

                    actual_value = rec.get(str(market_spec.get("actual_field") or ""))
                    if actual_value is None or abs(float(actual_value) - line_value) < 1e-9:
                        continue

                    odds = props_market.get("over_odds") if pick_over else props_market.get("under_odds")
                    won = float(actual_value) > line_value if pick_over else float(actual_value) < line_value
                    profit_u = _profit_from_american(odds, won, stake_u=HITTER_STAKE_U)
                    if profit_u is not None:
                        rows.append(
                            _base_row(
                                date,
                                "hitter_props",
                                edge,
                                profit_u,
                                stake_u=HITTER_STAKE_U,
                                submarket=str(market_spec["submarket"]),
                            )
                        )
    return rows


def _score_pitcher_batch(root: Path, batch_dir: Path, policy: Dict[str, Any], outs_calibration: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    odds_root = root / "data" / "market" / "oddsapi"
    for report_path in sorted(batch_dir.glob("sim_vs_actual_*.json")):
        obj = _read_json(report_path)
        date = str((obj.get("meta") or {}).get("date") or "").strip()
        if not date:
            continue
        token = date.replace("-", "_")
        pitcher_odds_path = odds_root / f"oddsapi_pitcher_props_{token}.json"
        if not pitcher_odds_path.exists():
            continue
        pitcher_odds = _read_json(pitcher_odds_path).get("pitcher_props") or {}
        pitcher_odds = {_norm_name(k): v for k, v in pitcher_odds.items()}

        for game in obj.get("games") or []:
            starters = game.get("starter_names") or {}
            pitcher_props = game.get("pitcher_props") or {}
            for side in ("away", "home"):
                pred = (pitcher_props.get(side) or {}).get("pred") or {}
                actual = (pitcher_props.get(side) or {}).get("actual") or {}
                starter_name = _norm_name(str(starters.get(side) or ""))
                market = pitcher_odds.get(starter_name)
                if not isinstance(market, dict):
                    continue
                outs_market = market.get("outs") or {}
                line = outs_market.get("line")
                if line is None:
                    continue
                line_value = float(line)
                p0 = _prob_over_line_from_dist(pred.get("outs_dist") or {}, line_value)
                if p0 is None:
                    continue
                p_over = apply_prob_calibration(float(p0), outs_calibration)
                p_imp = no_vig_over_prob(outs_market.get("over_odds"), outs_market.get("under_odds"))
                if p_imp is None:
                    continue
                edge = float(p_over) - float(p_imp)
                if edge < float(policy["pitcher_edge_min"]):
                    continue
                actual_outs = actual.get("outs")
                if actual_outs is None or abs(float(actual_outs) - line_value) < 1e-9:
                    continue
                won = float(actual_outs) > line_value
                profit_u = _profit_from_american(outs_market.get("over_odds"), won)
                if profit_u is not None:
                    rows.append(_base_row(date, "pitcher_props", edge, profit_u, stake_u=STANDARD_STAKE_U))
    return rows


def _summarize_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    count = len(rows)
    wins = sum(1 for row in rows if row["result"] == "win")
    stake = sum(float(row.get("stake_u") or STANDARD_STAKE_U) for row in rows)
    profit = sum(float(row["profit_u"]) for row in rows)
    return {
        "n": count,
        "wins": wins,
        "losses": count - wins,
        "stake_u": round(stake, 3),
        "profit_u": round(profit, 3),
        "roi": round(profit / stake, 4) if stake else None,
    }


def _summarize_by_market(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    by_market = {market: [] for market in MARKETS}
    for row in rows:
        by_market[row["market"]].append(row)
    out = {market: _summarize_rows(by_market[market]) for market in MARKETS}
    out["combined"] = _summarize_rows(rows)
    return out


def _summarize_hitter_submarkets(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = {submarket: [] for submarket in HITTER_SUBMARKETS}
    for row in rows:
        if row.get("market") != "hitter_props":
            continue
        submarket = str(row.get("submarket") or "")
        if submarket not in grouped:
            grouped[submarket] = []
        grouped[submarket].append(row)
    return {submarket: _summarize_rows(grouped[submarket]) for submarket in grouped}


def _daily_units(rows: Sequence[Dict[str, Any]], dates: Sequence[str]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {
        date: {"totals_u": 0.0, "ml_u": 0.0, "hitter_u": 0.0, "pitcher_u": 0.0}
        for date in dates
    }
    key_map = {
        "totals": "totals_u",
        "ml": "ml_u",
        "hitter_props": "hitter_u",
        "pitcher_props": "pitcher_u",
    }
    for row in rows:
        date = row["date"]
        if date not in out:
            out[date] = {"totals_u": 0.0, "ml_u": 0.0, "hitter_u": 0.0, "pitcher_u": 0.0}
        out[date][key_map[row["market"]]] += float(row["profit_u"])
    for date in sorted(out):
        out[date]["all_u"] = round(sum(out[date].values()), 3)
        for key in ("totals_u", "ml_u", "hitter_u", "pitcher_u"):
            out[date][key] = round(out[date][key], 3)
    return out


def _bootstrap(rows: Sequence[Dict[str, Any]], dates: Sequence[str], iterations: int, seed: int) -> Dict[str, Any]:
    day_stats: Dict[str, Dict[str, Tuple[float, float]]] = {}
    for date in dates:
        day_stats[date] = {market: (0.0, 0.0) for market in MARKETS}
    for row in rows:
        date = row["date"]
        if date not in day_stats:
            day_stats[date] = {market: (0.0, 0.0) for market in MARKETS}
        stake, profit = day_stats[date][row["market"]]
        day_stats[date][row["market"]] = (
            stake + float(row.get("stake_u") or STANDARD_STAKE_U),
            profit + float(row["profit_u"]),
        )

    market_rois: Dict[str, List[float]] = {market: [] for market in MARKETS}
    market_rois["combined"] = []
    rng = random.Random(seed)
    date_list = list(dates)
    for _ in range(int(iterations)):
        sampled = [date_list[rng.randrange(len(date_list))] for _ in range(len(date_list))]
        stakes = {market: 0.0 for market in MARKETS}
        profits = {market: 0.0 for market in MARKETS}
        for date in sampled:
            for market in MARKETS:
                stake, profit = day_stats[date][market]
                stakes[market] += stake
                profits[market] += profit
        combined_stake = sum(stakes.values())
        combined_profit = sum(profits.values())
        if combined_stake > 0:
            market_rois["combined"].append(combined_profit / combined_stake)
        for market in MARKETS:
            if stakes[market] > 0:
                market_rois[market].append(profits[market] / stakes[market])

    out: Dict[str, Any] = {}
    for market, roi_values in market_rois.items():
        sorted_values = sorted(roi_values)
        point = _summarize_rows([row for row in rows if market == "combined" or row["market"] == market])["roi"]
        if not sorted_values:
            out[market] = {
                "point_roi": point,
                "median_roi": None,
                "ci95": [None, None],
                "p_positive": None,
            }
            continue
        out[market] = {
            "point_roi": point,
            "median_roi": round(_percentile(sorted_values, 0.5) or 0.0, 4),
            "ci95": [
                round(_percentile(sorted_values, 0.025) or 0.0, 4),
                round(_percentile(sorted_values, 0.975) or 0.0, 4),
            ],
            "p_positive": round(sum(1 for value in sorted_values if value > 0.0) / len(sorted_values), 4),
        }
    return out


def _apply_caps(rows: Sequence[Dict[str, Any]], caps: Dict[str, int]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["date"], row["market"])] .append(row)

    out: List[Dict[str, Any]] = []
    for key in sorted(grouped):
        date, market = key
        limit = caps.get(market)
        group = sorted(grouped[key], key=lambda row: (-float(row["edge"]), row["profit_u"]))
        if limit is None:
            out.extend(group)
        else:
            out.extend(group[: max(0, int(limit))])
    return out


def _scenario_summary(rows: Sequence[Dict[str, Any]], dates: Sequence[str], iterations: int, seed: int) -> Dict[str, Any]:
    daily = _daily_units(rows, dates)
    daily_all = [daily[date]["all_u"] for date in dates]
    worst_day = min(((date, daily[date]["all_u"]) for date in dates), key=lambda item: item[1]) if dates else (None, None)
    best_day = max(((date, daily[date]["all_u"]) for date in dates), key=lambda item: item[1]) if dates else (None, None)
    mean_daily = sum(daily_all) / len(daily_all) if daily_all else None
    if daily_all:
        variance = sum((value - mean_daily) ** 2 for value in daily_all) / len(daily_all)
        daily_std = variance ** 0.5
    else:
        daily_std = None
    return {
        "results": _summarize_by_market(rows),
        "hitter_submarkets": _summarize_hitter_submarkets(rows),
        "combined_bootstrap": _bootstrap(rows, dates, iterations, seed)["combined"],
        "daily": {
            "mean_u": round(mean_daily, 3) if mean_daily is not None else None,
            "std_u": round(daily_std, 3) if daily_std is not None else None,
            "best_day": {"date": best_day[0], "u": round(best_day[1], 3) if best_day[1] is not None else None},
            "worst_day": {"date": worst_day[0], "u": round(worst_day[1], 3) if worst_day[1] is not None else None},
        },
    }


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Analyze locked three-profile policy results with bootstrap and cap scenarios")
    ap.add_argument("--game-batch-dir", required=True, help="Batch dir for game-side profile")
    ap.add_argument("--pitcher-batch-dir", required=True, help="Batch dir for pitcher props profile")
    ap.add_argument("--hitter-batch-dir", required=True, help="Batch dir for hitter props profile")
    ap.add_argument("--date-file", action="append", required=True, help="Date-set file, can be passed multiple times")
    ap.add_argument(
        "--outs-calibration",
        default="data/tuning/outs_calibration/default.json",
        help="Calibration JSON for pitcher outs market",
    )
    ap.add_argument("--bootstrap-iterations", type=int, default=5000, help="Bootstrap iterations")
    ap.add_argument("--bootstrap-seed", type=int, default=20260310, help="Bootstrap RNG seed")
    ap.add_argument("--out", default="", help="Optional output JSON path")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    root = Path(__file__).resolve().parents[2]
    game_batch_dir = (root / args.game_batch_dir).resolve() if not Path(args.game_batch_dir).is_absolute() else Path(args.game_batch_dir)
    pitcher_batch_dir = (root / args.pitcher_batch_dir).resolve() if not Path(args.pitcher_batch_dir).is_absolute() else Path(args.pitcher_batch_dir)
    hitter_batch_dir = (root / args.hitter_batch_dir).resolve() if not Path(args.hitter_batch_dir).is_absolute() else Path(args.hitter_batch_dir)
    date_files = [((root / value).resolve() if not Path(value).is_absolute() else Path(value)) for value in args.date_file]
    outs_calibration_path = (root / args.outs_calibration).resolve() if not Path(args.outs_calibration).is_absolute() else Path(args.outs_calibration)

    policy = dict(DEFAULT_POLICY)
    dates = _read_dates(date_files)
    outs_calibration = _read_json(outs_calibration_path)

    rows: List[Dict[str, Any]] = []
    rows.extend(_score_game_batch(root, game_batch_dir, policy))
    rows.extend(_score_hitter_batch(root, hitter_batch_dir, policy))
    rows.extend(_score_pitcher_batch(root, pitcher_batch_dir, policy, outs_calibration))

    base_summary = _summarize_by_market(rows)
    base_bootstrap = _bootstrap(rows, dates, int(args.bootstrap_iterations), int(args.bootstrap_seed))
    by_date_units = _daily_units(rows, dates)

    cap_scenarios: Dict[str, Any] = {}
    for index, (name, caps) in enumerate(DEFAULT_CAP_SCENARIOS.items()):
        scenario_rows = _apply_caps(rows, caps)
        cap_scenarios[name] = {
            "caps": caps,
            **_scenario_summary(scenario_rows, dates, int(args.bootstrap_iterations), int(args.bootstrap_seed) + 1000 + index),
        }

    output = {
        "date_count": len(dates),
        "policy": policy,
        "staking": {
            "totals": float(STANDARD_STAKE_U),
            "ml": float(STANDARD_STAKE_U),
            "pitcher_props": float(STANDARD_STAKE_U),
            "hitter_props": float(HITTER_STAKE_U),
        },
        "profiles": {
            "game": game_batch_dir.name,
            "pitcher": pitcher_batch_dir.name,
            "hitter": hitter_batch_dir.name,
        },
        "results": base_summary,
        "hitter_submarkets": _summarize_hitter_submarkets(rows),
        "bootstrap": base_bootstrap,
        "by_date_units": by_date_units,
        "cap_scenarios": cap_scenarios,
    }

    if str(args.out).strip():
        out_path = (root / args.out).resolve() if not Path(args.out).is_absolute() else Path(args.out)
        _write_json(out_path, output)
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())