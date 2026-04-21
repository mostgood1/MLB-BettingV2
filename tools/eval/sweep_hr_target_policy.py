from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from tools.eval.summarize_hr_target_eval_range import _iter_dates, _parse_date
from tools.eval.build_hr_target_eval_artifact import build_artifact


ROOT = Path(__file__).resolve().parents[2]


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _parse_float_list(raw: str, default: Sequence[float]) -> List[float]:
    text = str(raw or "").strip()
    if not text:
        return [float(v) for v in default]
    out: List[float] = []
    for part in text.split(","):
        token = part.strip()
        if not token:
            continue
        out.append(float(token))
    return out or [float(v) for v in default]


def _parse_int_list(raw: str, default: Sequence[int]) -> List[int]:
    text = str(raw or "").strip()
    if not text:
        return [int(v) for v in default]
    out: List[int] = []
    for part in text.split(","):
        token = part.strip()
        if not token:
            continue
        out.append(int(token))
    return out or [int(v) for v in default]


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT)).replace("\\", "/")
    except Exception:
        return str(path.resolve()).replace("\\", "/")


def _scenario_key(scenario: Dict[str, Any]) -> str:
    return (
        f"min_prob={float(scenario['min_prob']):.3f}|min_support={float(scenario['min_support_score']):.1f}|"
        f"high_support={float(scenario['high_support_score']):.1f}|high_support_min_prob={float(scenario['high_support_min_prob']):.3f}|"
        f"max_game={int(scenario['max_per_game'])}|max_team={int(scenario['max_per_team'])}"
    )


def _candidate_sort_key(row: Dict[str, Any]) -> Tuple[float, float, float, float]:
    return (
        float(_safe_float(row.get("hr_target_score")) or 0.0),
        float(_safe_float(row.get("p_hr_1plus")) or 0.0),
        float(_safe_float(row.get("hr_support_score")) or 0.0),
        float(_safe_float(row.get("pa_mean")) or 0.0),
    )


def _row_eligible(row: Dict[str, Any], scenario: Dict[str, Any]) -> bool:
    p = _safe_float(row.get("p_hr_1plus"))
    support = _safe_float(row.get("hr_support_score"))
    if p is None or support is None:
        return False
    primary_reason = str(row.get("primary_reason") or "").strip().lower()
    if primary_reason == "prediction_ineligible":
        return False
    min_prob = float(scenario["min_prob"])
    min_support = float(scenario["min_support_score"])
    high_support = float(scenario["high_support_score"])
    high_support_min_prob = float(scenario["high_support_min_prob"])
    required_prob = high_support_min_prob if support >= high_support else min_prob
    if support < min_support:
        return False
    return float(p) >= float(required_prob)


def _select_for_scenario(rows: List[Dict[str, Any]], scenario: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = [dict(row) for row in rows if _row_eligible(row, scenario)]
    candidates.sort(key=_candidate_sort_key, reverse=True)
    selected: List[Dict[str, Any]] = []
    per_team: Dict[str, int] = {}
    max_per_game = int(scenario["max_per_game"])
    max_per_team = int(scenario["max_per_team"])
    for row in candidates:
        if len(selected) >= max_per_game:
            break
        team = str(row.get("team") or "")
        if team and int(per_team.get(team, 0)) >= max_per_team:
            continue
        per_team[team] = int(per_team.get(team, 0)) + 1
        selected.append(row)
    return selected


def _scenario_summary(selected_rows: List[Dict[str, Any]], available_rows: List[Dict[str, Any]], scenario: Dict[str, Any]) -> Dict[str, Any]:
    settled = [row for row in selected_rows if bool(row.get("settled")) and _safe_int(row.get("y_hr_1plus")) is not None]
    wins = sum(int(_safe_int(row.get("y_hr_1plus")) or 0) for row in settled)
    losses = len(settled) - wins
    return {
        "policy": dict(scenario),
        "candidate_rows": int(len(available_rows)),
        "selected_rows": int(len(selected_rows)),
        "settled_rows": int(len(settled)),
        "wins": int(wins),
        "losses": int(losses),
        "hit_rate": (round(float(wins) / float(len(settled)), 4) if settled else None),
        "avg_p": (
            round(sum(float(_safe_float(row.get("p_hr_1plus")) or 0.0) for row in settled) / float(len(settled)), 4) if settled else None
        ),
        "avg_support": (
            round(sum(float(_safe_float(row.get("hr_support_score")) or 0.0) for row in settled) / float(len(settled)), 2) if settled else None
        ),
    }


def _load_range_rows(start: str, end: str, season: int) -> Dict[str, List[Dict[str, Any]]]:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for date_str in _iter_dates(start_date, end_date):
        try:
            artifact = build_artifact(date=str(date_str), season=int(season))
        except Exception:
            continue
        rows = artifact.get("rows") or []
        if not isinstance(rows, list):
            continue
        date_rows = [row for row in rows if isinstance(row, dict)]
        if date_rows:
            out[str(date_str)] = date_rows
    return out


def sweep_range(*, start: str, end: str, season: int, scenarios: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    rows_by_date = _load_range_rows(start, end, int(season))
    scenario_results: List[Dict[str, Any]] = []
    for scenario in scenarios:
        selected_all: List[Dict[str, Any]] = []
        candidate_n = 0
        day_breakdown: List[Dict[str, Any]] = []
        for date_str, day_rows in sorted(rows_by_date.items()):
            by_game: Dict[int, List[Dict[str, Any]]] = {}
            for row in day_rows:
                game_pk = _safe_int(row.get("game_pk"))
                if game_pk is None:
                    continue
                by_game.setdefault(int(game_pk), []).append(row)
            day_selected: List[Dict[str, Any]] = []
            for game_rows in by_game.values():
                available = [row for row in game_rows if _row_eligible(row, scenario)]
                candidate_n += len(available)
                chosen = _select_for_scenario(game_rows, scenario)
                day_selected.extend(chosen)
                selected_all.extend(chosen)
            settled_day = [row for row in day_selected if bool(row.get("settled")) and _safe_int(row.get("y_hr_1plus")) is not None]
            wins_day = sum(int(_safe_int(row.get("y_hr_1plus")) or 0) for row in settled_day)
            day_breakdown.append(
                {
                    "date": date_str,
                    "selected_rows": int(len(day_selected)),
                    "settled_rows": int(len(settled_day)),
                    "wins": int(wins_day),
                    "hit_rate": (round(float(wins_day) / float(len(settled_day)), 4) if settled_day else None),
                }
            )
        summary = _scenario_summary(selected_all, selected_all, scenario)
        summary["candidate_rows"] = int(candidate_n)
        summary["days"] = day_breakdown
        summary["scenario_key"] = _scenario_key(scenario)
        scenario_results.append(summary)

    scenario_results.sort(
        key=lambda row: (
            float(row.get("hit_rate") or -1.0),
            int(row.get("wins") or 0),
            -int(row.get("selected_rows") or 0),
        ),
        reverse=True,
    )
    return {
        "meta": {
            "season": int(season),
            "start": str(start),
            "end": str(end),
            "generated_at": datetime.now().isoformat(),
            "days_found": int(len(rows_by_date)),
            "scenario_count": int(len(scenarios)),
        },
        "scenarios": scenario_results,
        "best_by_hit_rate": dict(scenario_results[0]) if scenario_results else None,
        "best_by_wins": (
            dict(max(scenario_results, key=lambda row: (int(row.get("wins") or 0), float(row.get("hit_rate") or -1.0)))) if scenario_results else None
        ),
    }


def _build_markdown(report: Dict[str, Any], *, json_path: Path) -> str:
    meta = report.get("meta") or {}
    lines: List[str] = []
    lines.append(f"# HR Target Policy Sweep — {meta.get('start')} to {meta.get('end')}")
    lines.append("")
    lines.append(f"- season: {meta.get('season')}")
    lines.append(f"- source json: {_relative(json_path)}")
    lines.append(f"- days found: {meta.get('days_found')}")
    lines.append(f"- scenarios: {meta.get('scenario_count')}")
    lines.append("")

    for title, block in (("Best by hit rate", report.get("best_by_hit_rate") or {}), ("Best by wins", report.get("best_by_wins") or {})):
        if not block:
            continue
        policy = block.get("policy") or {}
        lines.append(f"## {title}")
        lines.append("")
        lines.append(
            f"- policy: min_prob={policy.get('min_prob')} | min_support={policy.get('min_support_score')} | high_support={policy.get('high_support_score')} | high_support_min_prob={policy.get('high_support_min_prob')} | max_game={policy.get('max_per_game')} | max_team={policy.get('max_per_team')}"
        )
        lines.append(
            f"- settled_rows: {block.get('settled_rows')} | wins: {block.get('wins')} | losses: {block.get('losses')} | hit_rate: {block.get('hit_rate')} | avg_p: {block.get('avg_p')} | avg_support: {block.get('avg_support')}"
        )
        lines.append("")

    rows: List[List[str]] = []
    for block in (report.get("scenarios") or [])[:20]:
        policy = block.get("policy") or {}
        rows.append(
            [
                str(policy.get("min_prob")),
                str(policy.get("min_support_score")),
                str(policy.get("high_support_score")),
                str(policy.get("high_support_min_prob")),
                str(policy.get("max_per_game")),
                str(policy.get("max_per_team")),
                str(block.get("settled_rows") or 0),
                str(block.get("wins") or 0),
                str(block.get("hit_rate") or ""),
            ]
        )
    lines.append("## Top Scenarios")
    lines.append("")
    if rows:
        lines.append(
            "| min_prob | min_support | high_support | high_support_min_prob | max_game | max_team | settled_rows | wins | hit_rate |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for row in rows:
            lines.append("| " + " | ".join(row) + " |")
    else:
        lines.append("(No scenarios evaluated.)")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Sweep HR target threshold policies over persisted eval rows.")
    ap.add_argument("--start", required=True, help="Start date in YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="End date in YYYY-MM-DD")
    ap.add_argument("--season", required=True, type=int, help="Season year")
    ap.add_argument("--min-probs", default="0.10,0.12,0.14,0.16", help="Comma-separated min probability values")
    ap.add_argument("--min-supports", default="40,50,60", help="Comma-separated min support-score values")
    ap.add_argument("--high-support-scores", default="65,70,75", help="Comma-separated high-support score values")
    ap.add_argument("--high-support-min-probs", default="0.10,0.12,0.14", help="Comma-separated high-support min prob values")
    ap.add_argument("--max-per-games", default="2,3", help="Comma-separated per-game caps")
    ap.add_argument("--max-per-teams", default="1,2", help="Comma-separated per-team caps")
    ap.add_argument("--out-json", default="", help="Optional JSON output path")
    ap.add_argument("--out-md", default="", help="Optional markdown output path")
    args = ap.parse_args()

    scenarios: List[Dict[str, Any]] = []
    for min_prob in _parse_float_list(args.min_probs, [0.10, 0.12, 0.14, 0.16]):
        for min_support in _parse_float_list(args.min_supports, [40.0, 50.0, 60.0]):
            for high_support in _parse_float_list(args.high_support_scores, [65.0, 70.0, 75.0]):
                for high_support_min_prob in _parse_float_list(args.high_support_min_probs, [0.10, 0.12, 0.14]):
                    for max_per_game in _parse_int_list(args.max_per_games, [2, 3]):
                        for max_per_team in _parse_int_list(args.max_per_teams, [1, 2]):
                            scenarios.append(
                                {
                                    "min_prob": float(min_prob),
                                    "min_support_score": float(min_support),
                                    "high_support_score": float(high_support),
                                    "high_support_min_prob": float(high_support_min_prob),
                                    "max_per_game": int(max_per_game),
                                    "max_per_team": int(max_per_team),
                                }
                            )

    report = sweep_range(start=str(args.start), end=str(args.end), season=int(args.season), scenarios=scenarios)
    default_base = ROOT / "data" / "eval" / "hr_targets" / str(int(args.season)) / f"hr_target_policy_sweep_{args.start}_to_{args.end}"
    out_json = Path(str(args.out_json)).resolve() if str(args.out_json or "").strip() else default_base.with_suffix(".json")
    out_md = Path(str(args.out_md)).resolve() if str(args.out_md or "").strip() else default_base.with_suffix(".md")
    _write_json(out_json, report)
    _write_text(out_md, _build_markdown(report, json_path=out_json) + "\n")
    print(f"Wrote JSON: {out_json}")
    print(f"Wrote MD: {out_md}")
    print(json.dumps(report.get("best_by_hit_rate") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())