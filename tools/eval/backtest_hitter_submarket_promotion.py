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

from tools.eval.analyze_locked_policy import DEFAULT_POLICY, _score_hitter_batch, _summarize_rows
from tools.eval.backtest_totals_promotion import (
    _combine_counts,
    _combine_results,
    _normalize_counts,
    _normalize_result,
    _normalize_results_map,
    _parse_float_list,
    _parse_int_list,
    _read_json,
    _resolve_batch_dir,
    _resolve_path,
    _results_delta,
    _subtract_counts,
    _subtract_results,
    _write_json,
    _zero_result,
)


DEFAULT_THRESHOLDS = (0.0, 0.05, 0.08, 0.1)
DEFAULT_CAPS = (1,)
SUPPORTED_SUBMARKETS = ("hitter_home_runs", "hitter_hits", "hitter_total_bases", "hitter_runs", "hitter_rbis")


def _submarket_summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    summary = _summarize_rows(rows)
    summary["avg_edge"] = round(sum(float(row.get("edge") or 0.0) for row in rows) / len(rows), 4) if rows else None
    summary["days"] = len({str(row.get("date") or "") for row in rows}) if rows else 0
    return summary


def _submarket_rows_by_day(batch_dir: Path, submarket: str) -> Dict[str, List[Dict[str, Any]]]:
    print(f"[backtest] scoring {submarket} candidates from {batch_dir}", file=sys.stderr, flush=True)
    t0 = time.perf_counter()
    hitter_rows = _score_hitter_batch(REPO_ROOT, batch_dir, dict(DEFAULT_POLICY))
    market_rows = [row for row in hitter_rows if str(row.get("submarket") or "") == str(submarket)]
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in market_rows:
        by_day[str(row.get("date") or "")].append(row)
    for date_key in by_day:
        by_day[date_key] = sorted(
            by_day[date_key],
            key=lambda row: (-float(row.get("edge") or 0.0), float(row.get("profit_u") or 0.0)),
        )
    print(
        f"[backtest] {submarket} candidate scoring finished in {round(time.perf_counter() - t0, 2)}s",
        file=sys.stderr,
        flush=True,
    )
    return by_day


def _official_day_snapshot(day_row: Dict[str, Any], submarket: str, replace_existing_submarket: bool) -> Dict[str, Any]:
    counts = _normalize_counts(day_row.get("selected_counts"))
    results = _normalize_results_map(day_row.get("results"))
    if replace_existing_submarket:
        counts["hitter_props"] -= counts[submarket]
        counts["combined"] -= counts[submarket]
        counts[submarket] = 0
        results["hitter_props"] = _subtract_results(results["hitter_props"], results[submarket])
        results["combined"] = _subtract_results(results["combined"], results[submarket])
        results[submarket] = _zero_result()
    return {
        "date": str(day_row.get("date") or ""),
        "counts": counts,
        "results": results,
    }


def _aggregate_days(days: Sequence[Dict[str, Any]], submarket: str) -> Dict[str, Any]:
    counts = _normalize_counts({})
    results = _normalize_results_map({})
    combined_daily: List[tuple[str, float]] = []
    hitter_daily: List[tuple[str, float]] = []
    submarket_daily: List[tuple[str, float]] = []
    days_with_submarket = 0
    for day in days:
        day_counts = _normalize_counts(day.get("selected_counts"))
        counts = _combine_counts(counts, day_counts)
        day_results = _normalize_results_map(day.get("results"))
        for key in results:
            results[key] = _combine_results(results[key], day_results.get(key))
        combined_profit = float((day_results.get("combined") or {}).get("profit_u") or 0.0)
        hitter_profit = float((day_results.get("hitter_props") or {}).get("profit_u") or 0.0)
        submarket_profit = float((day_results.get(submarket) or {}).get("profit_u") or 0.0)
        combined_daily.append((str(day.get("date") or ""), combined_profit))
        hitter_daily.append((str(day.get("date") or ""), hitter_profit))
        submarket_daily.append((str(day.get("date") or ""), submarket_profit))
        if int((day_results.get(submarket) or {}).get("n") or 0) > 0:
            days_with_submarket += 1

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
    best_hitter_day = max(hitter_daily, key=lambda item: item[1]) if hitter_daily else (None, None)
    worst_hitter_day = min(hitter_daily, key=lambda item: item[1]) if hitter_daily else (None, None)
    best_submarket_day = max(submarket_daily, key=lambda item: item[1]) if submarket_daily else (None, None)
    worst_submarket_day = min(submarket_daily, key=lambda item: item[1]) if submarket_daily else (None, None)

    return {
        "selected_counts": counts,
        "results": results,
        "daily": {
            "cards": int(len(days)),
            "days_with_submarket": int(days_with_submarket),
            "mean_u": mean_profit,
            "std_u": std_profit,
            "best_day": {"date": best_day[0], "u": round(best_day[1], 4) if best_day[1] is not None else None},
            "worst_day": {"date": worst_day[0], "u": round(worst_day[1], 4) if worst_day[1] is not None else None},
            "best_hitter_day": {
                "date": best_hitter_day[0],
                "u": round(best_hitter_day[1], 4) if best_hitter_day[1] is not None else None,
            },
            "worst_hitter_day": {
                "date": worst_hitter_day[0],
                "u": round(worst_hitter_day[1], 4) if worst_hitter_day[1] is not None else None,
            },
            "best_submarket_day": {
                "date": best_submarket_day[0],
                "u": round(best_submarket_day[1], 4) if best_submarket_day[1] is not None else None,
            },
            "worst_submarket_day": {
                "date": worst_submarket_day[0],
                "u": round(worst_submarket_day[1], 4) if worst_submarket_day[1] is not None else None,
            },
        },
    }


def _scenario_key(submarket: str, threshold: float, cap: int) -> str:
    return f"{submarket}|threshold={threshold:g}|cap={int(cap)}"


def _build_scenarios(
    manifest: Dict[str, Any],
    submarket_candidates_by_day: Dict[str, List[Dict[str, Any]]],
    submarket: str,
    thresholds: Sequence[float],
    caps: Sequence[int],
    replace_existing_submarket: bool,
) -> Dict[str, Any]:
    day_rows = [row for row in (manifest.get("days") or []) if isinstance(row, dict)]
    official_days = [_official_day_snapshot(row, submarket, replace_existing_submarket) for row in day_rows]
    official_summary = _aggregate_days(
        [
            {
                "date": day["date"],
                "selected_counts": day["counts"],
                "results": day["results"],
            }
            for day in official_days
        ],
        submarket,
    )

    scenarios: Dict[str, Any] = {}
    for threshold in thresholds:
        for cap in caps:
            print(
                f"[backtest] evaluating {submarket} promotion threshold>={threshold:g} cap={int(cap)}",
                file=sys.stderr,
                flush=True,
            )
            promoted_rows: List[Dict[str, Any]] = []
            scenario_days: List[Dict[str, Any]] = []
            for day in official_days:
                date_key = str(day.get("date") or "")
                candidates = submarket_candidates_by_day.get(date_key) or []
                chosen_rows = [row for row in candidates if float(row.get("edge") or 0.0) >= float(threshold)][: max(0, int(cap))]
                promoted_rows.extend(chosen_rows)
                promoted_submarket = _submarket_summary(chosen_rows)
                promoted_counts = _normalize_counts(
                    {
                        submarket: promoted_submarket["n"],
                        "hitter_props": promoted_submarket["n"],
                        "combined": promoted_submarket["n"],
                    }
                )
                promoted_results_map = _normalize_results_map({})
                promoted_results_map[submarket] = _normalize_result(promoted_submarket)
                promoted_results_map["hitter_props"] = _normalize_result(promoted_submarket)
                promoted_results_map["combined"] = _normalize_result(promoted_submarket)

                combined_counts = _combine_counts(day.get("counts"), promoted_counts)
                combined_results = dict(day.get("results") or {})
                combined_results[submarket] = _combine_results(combined_results.get(submarket), promoted_results_map[submarket])
                combined_results["hitter_props"] = _combine_results(
                    combined_results.get("hitter_props"), promoted_results_map["hitter_props"]
                )
                combined_results["combined"] = _combine_results(combined_results.get("combined"), promoted_results_map["combined"])

                scenario_days.append(
                    {
                        "date": date_key,
                        "official_selected_counts": day.get("counts"),
                        "official_results": day.get("results"),
                        "promoted_submarket": promoted_submarket,
                        "selected_counts": combined_counts,
                        "results": combined_results,
                    }
                )

            combined_summary = _aggregate_days(scenario_days, submarket)
            promoted_summary = {
                "selected_counts": _normalize_counts(
                    {
                        submarket: len(promoted_rows),
                        "hitter_props": len(promoted_rows),
                        "combined": len(promoted_rows),
                    }
                ),
                "results": _normalize_results_map({}),
                "daily": combined_summary["daily"],
            }
            promoted_submarket_summary = _submarket_summary(promoted_rows)
            promoted_summary["results"][submarket] = _normalize_result(promoted_submarket_summary)
            promoted_summary["results"]["hitter_props"] = _normalize_result(promoted_submarket_summary)
            promoted_summary["results"]["combined"] = _normalize_result(promoted_submarket_summary)
            promoted_summary["submarket_summary"] = promoted_submarket_summary

            scenarios[_scenario_key(submarket, threshold, cap)] = {
                "params": {
                    "submarket": str(submarket),
                    "edge_min": float(threshold),
                    "cap_per_day": int(cap),
                    "replace_existing_submarket": bool(replace_existing_submarket),
                },
                "official": official_summary,
                "promoted_submarket": promoted_summary,
                "combined": combined_summary,
                "delta_vs_official": {
                    "selected_counts": _subtract_counts(combined_summary["selected_counts"], official_summary["selected_counts"]),
                    "results": _results_delta(combined_summary["results"], official_summary["results"]),
                },
                "days": scenario_days,
            }
    return scenarios


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Backtest promoted hitter submarket picks added onto an official season betting-card manifest")
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
        "--submarket",
        default="hitter_runs",
        choices=list(SUPPORTED_SUBMARKETS),
        help="Hitter submarket to promote onto the official board",
    )
    ap.add_argument(
        "--thresholds",
        default=",".join(str(value) for value in DEFAULT_THRESHOLDS),
        help="Comma-separated edge thresholds to evaluate",
    )
    ap.add_argument(
        "--caps",
        default=",".join(str(value) for value in DEFAULT_CAPS),
        help="Comma-separated per-day caps to evaluate",
    )
    ap.add_argument(
        "--keep-existing-submarket",
        action="store_true",
        help="Keep any existing official picks for this submarket instead of replacing them before applying promoted rows",
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
    submarket = str(args.submarket)
    replace_existing_submarket = not bool(args.keep_existing_submarket)

    submarket_candidates_by_day = _submarket_rows_by_day(batch_dir, submarket)
    scenarios = _build_scenarios(
        official_manifest,
        submarket_candidates_by_day,
        submarket,
        thresholds,
        caps,
        replace_existing_submarket,
    )

    output = {
        "meta": {
            "official_manifest": str(official_manifest_path),
            "batch_dir": str(batch_dir),
            "official_cap_profile": str(((official_manifest.get("meta") or {}).get("cap_profile")) or ""),
            "official_policy": dict(((official_manifest.get("meta") or {}).get("policy")) or {}),
            "official_caps": dict(((official_manifest.get("meta") or {}).get("caps")) or {}),
            "submarket": submarket,
            "replace_existing_submarket": bool(replace_existing_submarket),
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