from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.eval.analyze_locked_policy import DEFAULT_POLICY, _score_game_batch, _summarize_rows


COUNT_KEYS: Sequence[str] = (
    "totals",
    "ml",
    "pitcher_props",
    "hitter_props",
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
    "combined",
)

RESULT_KEYS: Sequence[str] = (
    "totals",
    "ml",
    "pitcher_props",
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
    "hitter_props",
    "combined",
)

DEFAULT_THRESHOLDS = (0.8, 1.0, 1.2)
DEFAULT_CAPS = (1, 2)


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _parse_float_list(raw: str, fallback: Sequence[float]) -> List[float]:
    items = [piece.strip() for piece in str(raw or "").split(",") if piece.strip()]
    if not items:
        return [float(value) for value in fallback]
    return [float(value) for value in items]


def _parse_int_list(raw: str, fallback: Sequence[int]) -> List[int]:
    items = [piece.strip() for piece in str(raw or "").split(",") if piece.strip()]
    if not items:
        return [int(value) for value in fallback]
    return [int(value) for value in items]


def _resolve_path(raw: str) -> Path:
    path = Path(str(raw or "").strip())
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def _resolve_batch_dir(batch_dir_raw: str, season_manifest_raw: str) -> Path:
    if str(batch_dir_raw or "").strip():
        return _resolve_path(batch_dir_raw)

    season_manifest = _read_json(_resolve_path(season_manifest_raw))
    batch_dir_value = str(((season_manifest.get("meta") or {}).get("batch_dir")) or "").strip()
    if not batch_dir_value:
        raise ValueError(f"season manifest missing meta.batch_dir: {season_manifest_raw}")
    return _resolve_path(batch_dir_value)


def _zero_result() -> Dict[str, Any]:
    return {
        "n": 0,
        "wins": 0,
        "losses": 0,
        "stake_u": 0.0,
        "profit_u": 0.0,
        "roi": None,
    }


def _result_block(*, n: int, wins: int, losses: int, stake_u: float, profit_u: float) -> Dict[str, Any]:
    stake = round(float(stake_u), 4)
    profit = round(float(profit_u), 4)
    return {
        "n": int(n),
        "wins": int(wins),
        "losses": int(losses),
        "stake_u": stake,
        "profit_u": profit,
        "roi": round(profit / stake, 4) if abs(stake) > 1e-12 else None,
    }


def _normalize_result(block: Any) -> Dict[str, Any]:
    if not isinstance(block, dict):
        return _zero_result()
    return _result_block(
        n=int(block.get("n") or 0),
        wins=int(block.get("wins") or 0),
        losses=int(block.get("losses") or 0),
        stake_u=float(block.get("stake_u") or 0.0),
        profit_u=float(block.get("profit_u") or 0.0),
    )


def _combine_results(*blocks: Any) -> Dict[str, Any]:
    n = 0
    wins = 0
    losses = 0
    stake_u = 0.0
    profit_u = 0.0
    for block in blocks:
        current = _normalize_result(block)
        n += int(current["n"])
        wins += int(current["wins"])
        losses += int(current["losses"])
        stake_u += float(current["stake_u"])
        profit_u += float(current["profit_u"])
    return _result_block(n=n, wins=wins, losses=losses, stake_u=stake_u, profit_u=profit_u)


def _subtract_results(left: Any, right: Any) -> Dict[str, Any]:
    lhs = _normalize_result(left)
    rhs = _normalize_result(right)
    return _result_block(
        n=int(lhs["n"] - rhs["n"]),
        wins=int(lhs["wins"] - rhs["wins"]),
        losses=int(lhs["losses"] - rhs["losses"]),
        stake_u=float(lhs["stake_u"] - rhs["stake_u"]),
        profit_u=float(lhs["profit_u"] - rhs["profit_u"]),
    )


def _normalize_counts(block: Any) -> Dict[str, int]:
    raw = block if isinstance(block, dict) else {}
    counts = {key: int(raw.get(key) or 0) for key in COUNT_KEYS}
    if counts["hitter_props"] <= 0:
        counts["hitter_props"] = (
            counts["hitter_home_runs"]
            + counts["hitter_hits"]
            + counts["hitter_total_bases"]
            + counts["hitter_runs"]
            + counts["hitter_rbis"]
        )
    if counts["combined"] <= 0:
        counts["combined"] = counts["totals"] + counts["ml"] + counts["pitcher_props"] + counts["hitter_props"]
    return counts


def _combine_counts(*blocks: Any) -> Dict[str, int]:
    totals = {key: 0 for key in COUNT_KEYS}
    for block in blocks:
        current = _normalize_counts(block)
        for key in COUNT_KEYS:
            totals[key] += int(current.get(key) or 0)
    return totals


def _subtract_counts(left: Any, right: Any) -> Dict[str, int]:
    lhs = _normalize_counts(left)
    rhs = _normalize_counts(right)
    return {key: int(lhs.get(key, 0) - rhs.get(key, 0)) for key in COUNT_KEYS}


def _normalize_results_map(block: Any) -> Dict[str, Dict[str, Any]]:
    raw = block if isinstance(block, dict) else {}
    out = {key: _normalize_result(raw.get(key)) for key in RESULT_KEYS}
    hitter_rows = [out["hitter_home_runs"], out["hitter_hits"], out["hitter_total_bases"], out["hitter_runs"], out["hitter_rbis"]]
    out["hitter_props"] = _combine_results(*hitter_rows)
    if not isinstance(raw, dict) or "combined" not in raw:
        out["combined"] = _combine_results(out["totals"], out["ml"], out["pitcher_props"], out["hitter_props"])
    return out


def _results_delta(left: Dict[str, Dict[str, Any]], right: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {key: _subtract_results(left.get(key), right.get(key)) for key in RESULT_KEYS}


def _totals_summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    summary = _summarize_rows(rows)
    summary["avg_edge"] = round(sum(float(row.get("edge") or 0.0) for row in rows) / len(rows), 4) if rows else None
    summary["days"] = len({str(row.get("date") or "") for row in rows}) if rows else 0
    return summary


def _totals_rows_by_day(batch_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    print(f"[backtest] scoring totals candidates from {batch_dir}", file=sys.stderr, flush=True)
    t0 = time.perf_counter()
    game_rows = _score_game_batch(REPO_ROOT, batch_dir, dict(DEFAULT_POLICY))
    totals_rows = [row for row in game_rows if str(row.get("market") or "") == "totals"]
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in totals_rows:
        by_day[str(row.get("date") or "")].append(row)
    for date_key in by_day:
        by_day[date_key] = sorted(
            by_day[date_key],
            key=lambda row: (-float(row.get("edge") or 0.0), float(row.get("profit_u") or 0.0)),
        )
    print(
        f"[backtest] totals candidate scoring finished in {round(time.perf_counter() - t0, 2)}s",
        file=sys.stderr,
        flush=True,
    )
    return by_day


def _official_day_snapshot(day_row: Dict[str, Any], replace_existing_totals: bool) -> Dict[str, Any]:
    counts = _normalize_counts(day_row.get("selected_counts"))
    results = _normalize_results_map(day_row.get("results"))
    if replace_existing_totals:
        counts["combined"] -= counts["totals"]
        counts["totals"] = 0
        results["combined"] = _subtract_results(results["combined"], results["totals"])
        results["totals"] = _zero_result()
    return {
        "date": str(day_row.get("date") or ""),
        "counts": counts,
        "results": results,
    }


def _aggregate_days(days: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {key: 0 for key in COUNT_KEYS}
    results = {key: _zero_result() for key in RESULT_KEYS}
    combined_daily: List[tuple[str, float]] = []
    totals_daily: List[tuple[str, float]] = []
    totals_days = 0
    for day in days:
        day_counts = _normalize_counts(day.get("selected_counts"))
        for key in COUNT_KEYS:
            counts[key] += int(day_counts.get(key) or 0)
        day_results = _normalize_results_map(day.get("results"))
        for key in RESULT_KEYS:
            results[key] = _combine_results(results[key], day_results.get(key))
        combined_profit = float((day_results.get("combined") or {}).get("profit_u") or 0.0)
        totals_profit = float((day_results.get("totals") or {}).get("profit_u") or 0.0)
        combined_daily.append((str(day.get("date") or ""), combined_profit))
        totals_daily.append((str(day.get("date") or ""), totals_profit))
        if int((day_results.get("totals") or {}).get("n") or 0) > 0:
            totals_days += 1

    daily_profit_values = [value for _, value in combined_daily]
    mean_profit = round(sum(daily_profit_values) / len(daily_profit_values), 4) if daily_profit_values else None
    variance = (
        sum((value - (sum(daily_profit_values) / len(daily_profit_values))) ** 2 for value in daily_profit_values) / len(daily_profit_values)
        if daily_profit_values
        else None
    )
    std_profit = round(float(variance) ** 0.5, 4) if variance is not None else None
    best_day = max(combined_daily, key=lambda item: item[1]) if combined_daily else (None, None)
    worst_day = min(combined_daily, key=lambda item: item[1]) if combined_daily else (None, None)
    best_totals_day = max(totals_daily, key=lambda item: item[1]) if totals_daily else (None, None)
    worst_totals_day = min(totals_daily, key=lambda item: item[1]) if totals_daily else (None, None)

    return {
        "selected_counts": counts,
        "results": results,
        "daily": {
            "cards": int(len(days)),
            "days_with_totals": int(totals_days),
            "mean_u": mean_profit,
            "std_u": std_profit,
            "best_day": {"date": best_day[0], "u": round(best_day[1], 4) if best_day[1] is not None else None},
            "worst_day": {"date": worst_day[0], "u": round(worst_day[1], 4) if worst_day[1] is not None else None},
            "best_totals_day": {
                "date": best_totals_day[0],
                "u": round(best_totals_day[1], 4) if best_totals_day[1] is not None else None,
            },
            "worst_totals_day": {
                "date": worst_totals_day[0],
                "u": round(worst_totals_day[1], 4) if worst_totals_day[1] is not None else None,
            },
        },
    }


def _scenario_key(threshold: float, cap: int) -> str:
    return f"threshold={threshold:g}|cap={int(cap)}"


def _build_scenarios(
    manifest: Dict[str, Any],
    totals_candidates_by_day: Dict[str, List[Dict[str, Any]]],
    thresholds: Sequence[float],
    caps: Sequence[int],
    replace_existing_totals: bool,
) -> Dict[str, Any]:
    day_rows = [row for row in (manifest.get("days") or []) if isinstance(row, dict)]
    official_days = [_official_day_snapshot(row, replace_existing_totals) for row in day_rows]
    official_summary = _aggregate_days(
        [
            {
                "date": day["date"],
                "selected_counts": day["counts"],
                "results": day["results"],
            }
            for day in official_days
        ]
    )

    scenarios: Dict[str, Any] = {}
    for threshold in thresholds:
        for cap in caps:
            print(
                f"[backtest] evaluating totals promotion threshold>={threshold:g} cap={int(cap)}",
                file=sys.stderr,
                flush=True,
            )
            promoted_rows: List[Dict[str, Any]] = []
            scenario_days: List[Dict[str, Any]] = []
            for day in official_days:
                date_key = str(day.get("date") or "")
                candidates = totals_candidates_by_day.get(date_key) or []
                chosen_rows = [row for row in candidates if float(row.get("edge") or 0.0) >= float(threshold)][: max(0, int(cap))]
                promoted_rows.extend(chosen_rows)
                promoted_totals = _totals_summary(chosen_rows)
                promoted_counts = _normalize_counts({"totals": promoted_totals["n"], "combined": promoted_totals["n"]})
                promoted_results_map = {key: _zero_result() for key in RESULT_KEYS}
                promoted_results_map["totals"] = _normalize_result(promoted_totals)
                promoted_results_map["combined"] = _normalize_result(promoted_totals)
                combined_counts = _combine_counts(day.get("counts"), promoted_counts)
                combined_results = dict(day.get("results") or {})
                combined_results["totals"] = _combine_results(combined_results.get("totals"), promoted_results_map["totals"])
                combined_results["combined"] = _combine_results(combined_results.get("combined"), promoted_results_map["combined"])
                scenario_days.append(
                    {
                        "date": date_key,
                        "official_selected_counts": day.get("counts"),
                        "official_results": day.get("results"),
                        "promoted_totals": promoted_totals,
                        "selected_counts": combined_counts,
                        "results": combined_results,
                    }
                )

            combined_summary = _aggregate_days(scenario_days)
            promoted_summary = {
                "selected_counts": _normalize_counts({"totals": len(promoted_rows), "combined": len(promoted_rows)}),
                "results": {key: _zero_result() for key in RESULT_KEYS},
                "daily": combined_summary["daily"],
            }
            promoted_totals_summary = _totals_summary(promoted_rows)
            promoted_summary["results"]["totals"] = _normalize_result(promoted_totals_summary)
            promoted_summary["results"]["combined"] = _normalize_result(promoted_totals_summary)
            promoted_summary["totals_summary"] = promoted_totals_summary

            scenarios[_scenario_key(threshold, cap)] = {
                "params": {
                    "totals_edge_min": float(threshold),
                    "totals_cap_per_day": int(cap),
                    "replace_existing_totals": bool(replace_existing_totals),
                },
                "official": official_summary,
                "promoted_totals": promoted_summary,
                "combined": combined_summary,
                "delta_vs_official": {
                    "selected_counts": _subtract_counts(combined_summary["selected_counts"], official_summary["selected_counts"]),
                    "results": _results_delta(combined_summary["results"], official_summary["results"]),
                },
                "days": scenario_days,
            }
    return scenarios


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Backtest promoted totals added onto an official season betting-card manifest")
    ap.add_argument(
        "--official-manifest",
        default="data/eval/seasons/2025/season_betting_cards_retuned_manifest.json",
        help="Season betting-card manifest for the official board baseline",
    )
    ap.add_argument(
        "--season-manifest",
        default="data/eval/seasons/2025/season_eval_manifest.json",
        help="Season eval manifest used to discover meta.batch_dir when --batch-dir is omitted",
    )
    ap.add_argument("--batch-dir", default="", help="Optional batch dir override")
    ap.add_argument(
        "--thresholds",
        default=",".join(str(value) for value in DEFAULT_THRESHOLDS),
        help="Comma-separated totals edge thresholds to evaluate",
    )
    ap.add_argument(
        "--caps",
        default=",".join(str(value) for value in DEFAULT_CAPS),
        help="Comma-separated per-day totals caps to evaluate",
    )
    ap.add_argument(
        "--keep-existing-totals",
        action="store_true",
        help="Keep any totals already present in the official manifest instead of replacing totals with the promoted scenario",
    )
    ap.add_argument("--out", default="", help="Optional output JSON path")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    official_manifest_path = _resolve_path(args.official_manifest)
    official_manifest = _read_json(official_manifest_path)
    batch_dir = _resolve_batch_dir(args.batch_dir, args.season_manifest)
    thresholds = _parse_float_list(args.thresholds, DEFAULT_THRESHOLDS)
    caps = _parse_int_list(args.caps, DEFAULT_CAPS)
    replace_existing_totals = not bool(args.keep_existing_totals)

    totals_candidates_by_day = _totals_rows_by_day(batch_dir)
    scenarios = _build_scenarios(official_manifest, totals_candidates_by_day, thresholds, caps, replace_existing_totals)

    output = {
        "meta": {
            "official_manifest": str(official_manifest_path),
            "batch_dir": str(batch_dir),
            "official_cap_profile": str(((official_manifest.get("meta") or {}).get("cap_profile")) or ""),
            "official_policy": dict(((official_manifest.get("meta") or {}).get("policy")) or {}),
            "official_caps": dict(((official_manifest.get("meta") or {}).get("caps")) or {}),
            "replace_existing_totals": bool(replace_existing_totals),
            "thresholds": [float(value) for value in thresholds],
            "caps": [int(value) for value in caps],
        },
        "scenarios": scenarios,
    }
    if str(args.out).strip():
        _write_json(_resolve_path(args.out), output)
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())