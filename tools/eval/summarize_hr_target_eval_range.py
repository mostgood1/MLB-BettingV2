from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from tools.eval.analyze_hr_target_eval import (
    _bucket_summary,
    _md_table,
    _reason_breakdown,
    _selected_rank_method_comparison,
    _selected_signal_summary,
    _summarize_rows,
)
from tools.eval.build_hr_target_eval_artifact import build_artifact


ROOT = Path(__file__).resolve().parents[2]


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()


def _iter_dates(start: date, end: date) -> Iterable[str]:
    current = start
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def _eval_out_path(season: int, date_str: str) -> Path:
    return (ROOT / "data" / "eval" / "hr_targets" / str(int(season)) / f"hr_targets_eval_{date_str}.json").resolve()


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT)).replace("\\", "/")
    except Exception:
        return str(path.resolve()).replace("\\", "/")


def _load_or_build_day(date_str: str, season: int) -> Dict[str, Any]:
    out_path = _eval_out_path(int(season), str(date_str))
    artifact = build_artifact(date=str(date_str), season=int(season))
    _write_json(out_path, artifact)
    return artifact


def _range_rank_method_comparison(selected_rows: List[Dict[str, Any]], *, top_ns: Iterable[int] = (3, 5, 10)) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in selected_rows:
        if not bool(row.get("settled")) or row.get("y_hr_1plus") is None:
            continue
        day = str(row.get("date") or "").strip()
        if not day:
            continue
        grouped.setdefault(day, []).append(row)

    def _ordered(rows: List[Dict[str, Any]], method: str) -> List[Dict[str, Any]]:
        if method == "prob":
            return sorted(
                rows,
                key=lambda row: (
                    float(row.get("p_hr_1plus") or 0.0),
                    float(row.get("hr_target_score") or 0.0),
                    float(row.get("hr_support_score") or 0.0),
                ),
                reverse=True,
            )
        return sorted(
            rows,
            key=lambda row: (
                float(row.get("hr_target_score") or 0.0),
                float(row.get("p_hr_1plus") or 0.0),
                float(row.get("hr_support_score") or 0.0),
            ),
            reverse=True,
        )

    def _aggregate(method: str, top_n: int) -> Dict[str, Any]:
        picked: List[Dict[str, Any]] = []
        for _, rows in sorted(grouped.items()):
            picked.extend(_ordered(rows, method)[: max(0, int(top_n))])
        settled_rows = len(picked)
        wins = sum(int(row.get("y_hr_1plus") or 0) for row in picked)
        return {
            "top_n": int(top_n),
            "days": int(len(grouped)),
            "settled_rows": int(settled_rows),
            "wins": int(wins),
            "hit_rate": (round(float(wins) / float(settled_rows), 4) if settled_rows else None),
            "avg_p": (round(sum(float(row.get("p_hr_1plus") or 0.0) for row in picked) / float(settled_rows), 4) if settled_rows else None),
            "avg_support": (round(sum(float(row.get("hr_support_score") or 0.0) for row in picked) / float(settled_rows), 2) if settled_rows else None),
        }

    return {
        "coverage": "selected_rows_only_daily_top_n",
        "days": int(len(grouped)),
        "score_order": [_aggregate("score", int(top_n)) for top_n in top_ns],
        "prob_order": [_aggregate("prob", int(top_n)) for top_n in top_ns],
    }


def summarize_range(*, start: str, end: str, season: int) -> Dict[str, Any]:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    day_docs: List[Dict[str, Any]] = []
    selected_rows: List[Dict[str, Any]] = []
    excluded_rows: List[Dict[str, Any]] = []
    excluded_example_rows: List[Dict[str, Any]] = []

    for date_str in _iter_dates(start_date, end_date):
        try:
            doc = _load_or_build_day(date_str, int(season))
        except Exception as exc:
            day_docs.append({"date": date_str, "found": False, "error": f"{type(exc).__name__}: {exc}"})
            continue

        rows = doc.get("rows") or []
        rows = [row for row in rows if isinstance(row, dict)]
        selected = [row for row in rows if str(row.get("selection_status") or "") == "selected"]
        excluded = [row for row in rows if str(row.get("selection_status") or "") == "excluded"]
        excluded_examples = [row for row in rows if str(row.get("selection_status") or "") == "excluded_example"]
        selected_rows.extend(selected)
        excluded_rows.extend(excluded)
        excluded_example_rows.extend(excluded_examples)
        day_docs.append(
            {
                "date": date_str,
                "found": True,
                "coverage": dict(doc.get("coverage") or {}),
                "selected_overall": _summarize_rows(selected),
                "excluded_overall": _summarize_rows(excluded),
                "excluded_examples_overall": _summarize_rows(excluded_examples),
                "source_artifact": doc.get("source_artifact"),
            }
        )

    exclusion_pool = excluded_rows if excluded_rows else excluded_example_rows
    return {
        "meta": {
            "season": int(season),
            "start": str(start),
            "end": str(end),
            "generated_at": datetime.now().isoformat(),
            "days_requested": int((end_date - start_date).days + 1),
            "days_found": int(len([day for day in day_docs if bool(day.get("found"))])),
        },
        "selected_overall": _summarize_rows(selected_rows),
        "selected_probability_buckets": _bucket_summary(selected_rows, "p_hr_1plus", [0.0, 0.05, 0.10, 0.15, 0.20]),
        "selected_support_buckets": _bucket_summary(selected_rows, "hr_support_score", [0.0, 50.0, 60.0, 70.0, 100.0], integer_output=True),
        "selected_signal_summary": _selected_signal_summary(selected_rows),
        "selected_rank_method_comparison": _range_rank_method_comparison(selected_rows),
        "excluded_overall": _summarize_rows(excluded_rows),
        "excluded_examples_overall": _summarize_rows(excluded_example_rows),
        "excluded_reason_breakdown": _reason_breakdown(exclusion_pool),
        "days": day_docs,
    }


def _build_markdown(summary: Dict[str, Any], *, json_path: Path) -> str:
    meta = summary.get("meta") or {}
    lines: List[str] = []
    lines.append(f"# HR Target Eval Range Recap — {meta.get('start')} to {meta.get('end')}")
    lines.append("")
    lines.append(f"- season: {meta.get('season')}")
    lines.append(f"- source json: {_relative(json_path)}")
    lines.append(f"- days found: {meta.get('days_found')} / {meta.get('days_requested')}")
    lines.append("")

    for title, block in (
        ("Selected overall", summary.get("selected_overall") or {}),
        ("Excluded overall", summary.get("excluded_overall") or {}),
        ("Excluded examples overall", summary.get("excluded_examples_overall") or {}),
    ):
        lines.append(f"## {title}")
        lines.append("")
        lines.append(
            f"- settled_rows: {block.get('settled_rows')} | wins: {block.get('wins')} | losses: {block.get('losses')} | hit_rate: {block.get('hit_rate')} | avg_p: {block.get('avg_p')} | avg_support: {block.get('avg_support')} | brier: {block.get('brier')} | logloss: {block.get('logloss')}"
        )
        lines.append("")

    prob_rows = [
        [str(row.get("bucket") or ""), str(row.get("n") or 0), str(row.get("wins") or 0), str(row.get("hit_rate") or ""), str(row.get("avg_value") or "")]
        for row in (summary.get("selected_probability_buckets") or [])
    ]
    lines.append("## Selected Probability Buckets")
    lines.append("")
    lines.append(_md_table(["bucket", "n", "wins", "hit_rate", "avg_p"], prob_rows) if prob_rows else "(No settled selected rows.)")
    lines.append("")

    support_rows = [
        [str(row.get("bucket") or ""), str(row.get("n") or 0), str(row.get("wins") or 0), str(row.get("hit_rate") or ""), str(row.get("avg_value") or "")]
        for row in (summary.get("selected_support_buckets") or [])
    ]
    lines.append("## Selected Support Buckets")
    lines.append("")
    lines.append(_md_table(["bucket", "n", "wins", "hit_rate", "avg_support"], support_rows) if support_rows else "(No settled selected rows.)")
    lines.append("")

    signal = summary.get("selected_signal_summary") or {}
    lines.append("## Selected Signal Summary")
    lines.append("")
    lines.append(
        f"- settled_rows: {signal.get('settled_rows')} | pearson_support_vs_success: {signal.get('pearson_support_vs_success')} | spearman_support_vs_success: {signal.get('spearman_support_vs_success')} | pearson_prob_vs_success: {signal.get('pearson_prob_vs_success')} | pearson_score_vs_success: {signal.get('pearson_score_vs_success')}"
    )
    lines.append("")

    comparison = summary.get("selected_rank_method_comparison") or {}
    score_rows = [
        [str(row.get("top_n") or 0), str(row.get("settled_rows") or 0), str(row.get("wins") or 0), str(row.get("hit_rate") or ""), str(row.get("avg_p") or ""), str(row.get("avg_support") or "")]
        for row in (comparison.get("score_order") or [])
    ]
    prob_rows = [
        [str(row.get("top_n") or 0), str(row.get("settled_rows") or 0), str(row.get("wins") or 0), str(row.get("hit_rate") or ""), str(row.get("avg_p") or ""), str(row.get("avg_support") or "")]
        for row in (comparison.get("prob_order") or [])
    ]
    lines.append("## Selected Rank Method Comparison")
    lines.append("")
    lines.append(f"- coverage: {comparison.get('coverage') or ''}")
    lines.append("")
    lines.append("### Score Order")
    lines.append("")
    lines.append(_md_table(["top_n", "settled_rows", "wins", "hit_rate", "avg_p", "avg_support"], score_rows) if score_rows else "(No settled selected rows.)")
    lines.append("")
    lines.append("### Probability Order")
    lines.append("")
    lines.append(_md_table(["top_n", "settled_rows", "wins", "hit_rate", "avg_p", "avg_support"], prob_rows) if prob_rows else "(No settled selected rows.)")
    lines.append("")

    reason_rows = [
        [
            str(row.get("reason") or ""),
            str(row.get("settled_rows") or 0),
            str(row.get("wins") or 0),
            str(row.get("hit_rate") or ""),
            str(row.get("avg_p") or ""),
            str(row.get("avg_support") or ""),
        ]
        for row in (summary.get("excluded_reason_breakdown") or [])
    ]
    lines.append("## Excluded Reason Breakdown")
    lines.append("")
    lines.append(_md_table(["reason", "settled_rows", "wins", "hit_rate", "avg_p", "avg_support"], reason_rows) if reason_rows else "(No excluded rows.)")
    lines.append("")

    day_rows = []
    for day in summary.get("days") or []:
        if not isinstance(day, dict):
            continue
        selected = day.get("selected_overall") or {}
        excluded = day.get("excluded_overall") or {}
        day_rows.append(
            [
                str(day.get("date") or ""),
                "yes" if bool(day.get("found")) else "no",
                str(selected.get("settled_rows") or 0),
                str(selected.get("wins") or 0),
                str(selected.get("hit_rate") or ""),
                str(excluded.get("settled_rows") or 0),
                str(excluded.get("wins") or 0),
                str(((day.get("coverage") or {}).get("excluded_rows") or "")),
            ]
        )
    lines.append("## Daily Breakdown")
    lines.append("")
    lines.append(
        _md_table(["date", "found", "sel_n", "sel_wins", "sel_hit_rate", "excl_n", "excl_wins", "excl_coverage"], day_rows)
        if day_rows
        else "(No days processed.)"
    )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize HR target eval artifacts across a date range.")
    ap.add_argument("--start", required=True, help="Start date in YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="End date in YYYY-MM-DD")
    ap.add_argument("--season", required=True, type=int, help="Season year")
    ap.add_argument("--out-json", default="", help="Optional summary JSON output path")
    ap.add_argument("--out-md", default="", help="Optional summary markdown output path")
    args = ap.parse_args()

    summary = summarize_range(start=str(args.start), end=str(args.end), season=int(args.season))
    default_base = ROOT / "data" / "eval" / "hr_targets" / str(int(args.season)) / f"hr_targets_eval_summary_{args.start}_to_{args.end}"
    out_json = Path(str(args.out_json)).resolve() if str(args.out_json or "").strip() else default_base.with_suffix(".json")
    out_md = Path(str(args.out_md)).resolve() if str(args.out_md or "").strip() else default_base.with_suffix(".md")
    _write_json(out_json, summary)
    _write_text(out_md, _build_markdown(summary, json_path=out_json) + "\n")
    print(f"Wrote JSON: {out_json}")
    print(f"Wrote MD: {out_md}")
    print(json.dumps(summary.get("selected_overall") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())