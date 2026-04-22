from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _fmt(value: Any, nd: int = 3) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.{nd}f}"
    return str(value)


def _relative(path: Optional[Path]) -> str:
    if path is None:
        return ""
    try:
        return str(path.resolve().relative_to(ROOT)).replace("\\", "/")
    except Exception:
        return str(path.resolve()).replace("\\", "/")


def _bucket_label(value: float, edges: Sequence[float]) -> str:
    for idx in range(len(edges) - 1):
        lo = float(edges[idx])
        hi = float(edges[idx + 1])
        if value < hi or idx == len(edges) - 2:
            if idx == len(edges) - 2:
                return f"[{lo:.2f}+ ]"
            return f"[{lo:.2f}, {hi:.2f})"
    return f"[{float(edges[-1]):.2f}+ ]"


def _mean(values: Iterable[float]) -> Optional[float]:
    collected = [float(value) for value in values]
    if not collected:
        return None
    return float(sum(collected) / len(collected))


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mean_x = float(sum(xs) / len(xs))
    mean_y = float(sum(ys) / len(ys))
    num = sum((float(x) - mean_x) * (float(y) - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((float(x) - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((float(y) - mean_y) ** 2 for y in ys))
    if den_x <= 0.0 or den_y <= 0.0:
        return None
    return float(num / (den_x * den_y))


def _rankdata(values: Sequence[float]) -> List[float]:
    ordered = sorted(enumerate(values), key=lambda item: float(item[1]))
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index
        while end + 1 < len(ordered) and float(ordered[end + 1][1]) == float(ordered[index][1]):
            end += 1
        avg_rank = float(index + end + 2) / 2.0
        for pos in range(index, end + 1):
            ranks[ordered[pos][0]] = avg_rank
        index = end + 1
    return ranks


def _spearman(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    return _pearson(_rankdata(xs), _rankdata(ys))


def _brier(p: float, y: int) -> float:
    diff = float(p) - float(y)
    return float(diff * diff)


def _logloss(p: float, y: int, eps: float = 1e-12) -> float:
    pp = min(1.0 - eps, max(eps, float(p)))
    return float(-math.log(pp if int(y) else (1.0 - pp)))


def _summarize_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    settled = [row for row in rows if bool(row.get("settled")) and _safe_int(row.get("y_hr_1plus")) is not None]
    probs = [_safe_float(row.get("p_hr_1plus")) for row in settled]
    probs = [float(value) for value in probs if value is not None]
    ys = [_safe_int(row.get("y_hr_1plus")) for row in settled]
    ys = [int(value) for value in ys if value is not None]
    supports = [_safe_float(row.get("hr_support_score")) for row in settled]
    supports = [float(value) for value in supports if value is not None]
    return {
        "rows": int(len(rows)),
        "settled_rows": int(len(settled)),
        "wins": int(sum(ys)),
        "losses": int(len(ys) - sum(ys)),
        "hit_rate": (round(float(sum(ys)) / float(len(ys)), 4) if ys else None),
        "avg_p": (round(_mean(probs), 4) if probs else None),
        "avg_support": (round(_mean(supports), 2) if supports else None),
        "brier": (
            round(_mean(_brier(float(p), int(y)) for p, y in zip(probs, ys)), 4) if probs and len(probs) == len(ys) else None
        ),
        "logloss": (
            round(_mean(_logloss(float(p), int(y)) for p, y in zip(probs, ys)), 4) if probs and len(probs) == len(ys) else None
        ),
    }


def _bucket_summary(rows: List[Dict[str, Any]], field: str, edges: Sequence[float], *, integer_output: bool = False) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not bool(row.get("settled")):
            continue
        value = _safe_float(row.get(field))
        y = _safe_int(row.get("y_hr_1plus"))
        if value is None or y is None:
            continue
        label = _bucket_label(float(value), edges)
        bucket = buckets.setdefault(label, {"bucket": label, "n": 0, "wins": 0, "sum_value": 0.0})
        bucket["n"] = int(bucket["n"]) + 1
        bucket["wins"] = int(bucket["wins"]) + int(y)
        bucket["sum_value"] = float(bucket["sum_value"]) + float(value)
    out: List[Dict[str, Any]] = []
    for label, bucket in buckets.items():
        n = int(bucket.get("n") or 0)
        wins = int(bucket.get("wins") or 0)
        avg_value = float(bucket.get("sum_value") or 0.0) / float(n) if n > 0 else None
        out.append(
            {
                "bucket": label,
                "n": n,
                "wins": wins,
                "hit_rate": (round(float(wins) / float(n), 4) if n > 0 else None),
                "avg_value": (round(avg_value, 3 if not integer_output else 2) if avg_value is not None else None),
            }
        )
    return sorted(out, key=lambda row: str(row.get("bucket") or ""))


def _top_rank_summary(rows: List[Dict[str, Any]], top_n: int) -> Dict[str, Any]:
    ranked = [row for row in rows if bool(row.get("settled")) and _safe_int(row.get("slate_rank")) is not None]
    ranked.sort(key=lambda row: int(_safe_int(row.get("slate_rank")) or 10**9))
    top_rows = ranked[: max(0, int(top_n))]
    summary = _summarize_rows(top_rows)
    summary["top_n"] = int(top_n)
    return summary


def _reason_breakdown(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        reason = str(row.get("primary_reason") or "unknown")
        grouped.setdefault(reason, []).append(row)
    out: List[Dict[str, Any]] = []
    for reason, block in grouped.items():
        summary = _summarize_rows(block)
        out.append({"reason": reason, **summary})
    return sorted(out, key=lambda row: (-int(row.get("settled_rows") or 0), str(row.get("reason") or "")))


def _selected_signal_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    settled = [row for row in rows if bool(row.get("settled")) and _safe_int(row.get("y_hr_1plus")) is not None]
    supports: List[float] = []
    probs: List[float] = []
    scores: List[float] = []
    outcomes: List[float] = []
    for row in settled:
        outcome = _safe_int(row.get("y_hr_1plus"))
        support = _safe_float(row.get("hr_support_score"))
        prob = _safe_float(row.get("p_hr_1plus"))
        score = _safe_float(row.get("hr_target_score"))
        if outcome is None:
            continue
        outcomes.append(float(outcome))
        supports.append(float(support) if support is not None else float("nan"))
        probs.append(float(prob) if prob is not None else float("nan"))
        scores.append(float(score) if score is not None else float("nan"))

    def _paired(metric_values: Sequence[float], target_values: Sequence[float]) -> Tuple[List[float], List[float]]:
        paired = [
            (float(metric), float(target))
            for metric, target in zip(metric_values, target_values)
            if not math.isnan(float(metric)) and not math.isnan(float(target))
        ]
        return [item[0] for item in paired], [item[1] for item in paired]

    support_x, support_y = _paired(supports, outcomes)
    prob_x, prob_y = _paired(probs, outcomes)
    score_x, score_y = _paired(scores, outcomes)
    return {
        "settled_rows": int(len(outcomes)),
        "pearson_support_vs_success": (round(_pearson(support_x, support_y), 4) if _pearson(support_x, support_y) is not None else None),
        "spearman_support_vs_success": (round(_spearman(support_x, support_y), 4) if _spearman(support_x, support_y) is not None else None),
        "pearson_prob_vs_success": (round(_pearson(prob_x, prob_y), 4) if _pearson(prob_x, prob_y) is not None else None),
        "spearman_prob_vs_success": (round(_spearman(prob_x, prob_y), 4) if _spearman(prob_x, prob_y) is not None else None),
        "pearson_score_vs_success": (round(_pearson(score_x, score_y), 4) if _pearson(score_x, score_y) is not None else None),
        "spearman_score_vs_success": (round(_spearman(score_x, score_y), 4) if _spearman(score_x, score_y) is not None else None),
    }


def _top_n_summary(rows: Sequence[Dict[str, Any]], top_n: int) -> Dict[str, Any]:
    selected = list(rows[: max(0, int(top_n))])
    settled = [row for row in selected if bool(row.get("settled")) and _safe_int(row.get("y_hr_1plus")) is not None]
    wins = sum(int(_safe_int(row.get("y_hr_1plus")) or 0) for row in settled)
    return {
        "top_n": int(top_n),
        "rows": int(len(selected)),
        "settled_rows": int(len(settled)),
        "wins": int(wins),
        "hit_rate": (round(float(wins) / float(len(settled)), 4) if settled else None),
        "avg_support": (round(_mean(float(_safe_float(row.get("hr_support_score")) or 0.0) for row in settled), 2) if settled else None),
        "avg_p": (round(_mean(float(_safe_float(row.get("p_hr_1plus")) or 0.0) for row in settled), 4) if settled else None),
    }


def _selected_rank_method_comparison(rows: List[Dict[str, Any]], *, top_ns: Sequence[int] = (3, 5, 10)) -> Dict[str, Any]:
    settled = [row for row in rows if bool(row.get("settled")) and _safe_int(row.get("y_hr_1plus")) is not None]
    by_score = sorted(
        settled,
        key=lambda row: (
            float(_safe_float(row.get("hr_target_score")) or 0.0),
            float(_safe_float(row.get("p_hr_1plus")) or 0.0),
            float(_safe_float(row.get("hr_support_score")) or 0.0),
        ),
        reverse=True,
    )
    by_prob = sorted(
        settled,
        key=lambda row: (
            float(_safe_float(row.get("p_hr_1plus")) or 0.0),
            float(_safe_float(row.get("hr_target_score")) or 0.0),
            float(_safe_float(row.get("hr_support_score")) or 0.0),
        ),
        reverse=True,
    )
    return {
        "coverage": "selected_rows_only",
        "settled_rows": int(len(settled)),
        "score_order": [_top_n_summary(by_score, int(top_n)) for top_n in top_ns],
        "prob_order": [_top_n_summary(by_prob, int(top_n)) for top_n in top_ns],
    }


def analyze(doc: Dict[str, Any]) -> Dict[str, Any]:
    rows = doc.get("rows") or []
    if not isinstance(rows, list):
        raise ValueError("Artifact rows must be a list")
    selected = [row for row in rows if isinstance(row, dict) and str(row.get("selection_status") or "") == "selected"]
    excluded = [row for row in rows if isinstance(row, dict) and str(row.get("selection_status") or "") == "excluded"]
    excluded_examples = [row for row in rows if isinstance(row, dict) and str(row.get("selection_status") or "") == "excluded_example"]
    exclusion_pool = excluded if excluded else excluded_examples
    near_threshold_pool = [row for row in exclusion_pool if bool(row.get("near_threshold"))]

    return {
        "meta": {
            "date": doc.get("date"),
            "season": doc.get("season"),
            "source_artifact": doc.get("source_artifact"),
            "coverage": dict(doc.get("coverage") or {}),
        },
        "selected_overall": _summarize_rows(selected),
        "selected_top5": _top_rank_summary(selected, 5),
        "selected_top10": _top_rank_summary(selected, 10),
        "selected_probability_buckets": _bucket_summary(selected, "p_hr_1plus", [0.0, 0.05, 0.10, 0.15, 0.20]),
        "selected_support_buckets": _bucket_summary(selected, "hr_support_score", [0.0, 50.0, 60.0, 70.0, 100.0], integer_output=True),
        "selected_signal_summary": _selected_signal_summary(selected),
        "selected_rank_method_comparison": _selected_rank_method_comparison(selected),
        "excluded_overall": _summarize_rows(excluded),
        "excluded_examples_overall": _summarize_rows(excluded_examples),
        "excluded_near_threshold_overall": _summarize_rows(near_threshold_pool),
        "excluded_reason_breakdown": _reason_breakdown(exclusion_pool),
    }


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def build_markdown(analysis: Dict[str, Any], *, artifact_path: Path) -> str:
    meta = analysis.get("meta") or {}
    lines: List[str] = []
    lines.append(f"# HR Target Eval Recap — {meta.get('date')}")
    lines.append("")
    lines.append(f"- season: {meta.get('season')}")
    lines.append(f"- source artifact: {meta.get('source_artifact') or _relative(artifact_path)}")
    lines.append(f"- exclusion coverage: {((meta.get('coverage') or {}).get('excluded_rows') or 'unknown')}")
    lines.append("")

    for title, block in (
        ("Selected overall", analysis.get("selected_overall") or {}),
        ("Selected top 5", analysis.get("selected_top5") or {}),
        ("Selected top 10", analysis.get("selected_top10") or {}),
        ("Excluded overall", analysis.get("excluded_overall") or {}),
        ("Excluded examples overall", analysis.get("excluded_examples_overall") or {}),
        ("Excluded near-threshold examples", analysis.get("excluded_near_threshold_overall") or {}),
    ):
        lines.append(f"## {title}")
        lines.append("")
        lines.append(
            f"- settled_rows: {block.get('settled_rows')} | wins: {block.get('wins')} | losses: {block.get('losses')} | hit_rate: {_fmt(block.get('hit_rate'))} | avg_p: {_fmt(block.get('avg_p'))} | avg_support: {_fmt(block.get('avg_support'), 2)} | brier: {_fmt(block.get('brier'))} | logloss: {_fmt(block.get('logloss'))}"
        )
        lines.append("")

    prob_rows = []
    for row in analysis.get("selected_probability_buckets") or []:
        prob_rows.append([
            str(row.get("bucket") or ""),
            str(row.get("n") or 0),
            str(row.get("wins") or 0),
            _fmt(row.get("hit_rate")),
            _fmt(row.get("avg_value")),
        ])
    lines.append("## Selected Probability Buckets")
    lines.append("")
    lines.append(_md_table(["bucket", "n", "wins", "hit_rate", "avg_p"], prob_rows) if prob_rows else "(No settled selected rows.)")
    lines.append("")

    support_rows = []
    for row in analysis.get("selected_support_buckets") or []:
        support_rows.append([
            str(row.get("bucket") or ""),
            str(row.get("n") or 0),
            str(row.get("wins") or 0),
            _fmt(row.get("hit_rate")),
            _fmt(row.get("avg_value"), 2),
        ])
    lines.append("## Selected Support Buckets")
    lines.append("")
    lines.append(_md_table(["bucket", "n", "wins", "hit_rate", "avg_support"], support_rows) if support_rows else "(No settled selected rows.)")
    lines.append("")

    signal = analysis.get("selected_signal_summary") or {}
    lines.append("## Selected Signal Summary")
    lines.append("")
    lines.append(
        f"- settled_rows: {signal.get('settled_rows')} | pearson_support_vs_success: {_fmt(signal.get('pearson_support_vs_success'))} | spearman_support_vs_success: {_fmt(signal.get('spearman_support_vs_success'))} | pearson_prob_vs_success: {_fmt(signal.get('pearson_prob_vs_success'))} | pearson_score_vs_success: {_fmt(signal.get('pearson_score_vs_success'))}"
    )
    lines.append("")

    comparison = analysis.get("selected_rank_method_comparison") or {}
    score_rows = [
        [
            str(row.get("top_n") or 0),
            str(row.get("settled_rows") or 0),
            str(row.get("wins") or 0),
            _fmt(row.get("hit_rate")),
            _fmt(row.get("avg_p")),
            _fmt(row.get("avg_support"), 2),
        ]
        for row in (comparison.get("score_order") or [])
    ]
    prob_comp_rows = [
        [
            str(row.get("top_n") or 0),
            str(row.get("settled_rows") or 0),
            str(row.get("wins") or 0),
            _fmt(row.get("hit_rate")),
            _fmt(row.get("avg_p")),
            _fmt(row.get("avg_support"), 2),
        ]
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
    lines.append(_md_table(["top_n", "settled_rows", "wins", "hit_rate", "avg_p", "avg_support"], prob_comp_rows) if prob_comp_rows else "(No settled selected rows.)")
    lines.append("")

    reason_rows = []
    for row in analysis.get("excluded_reason_breakdown") or []:
        reason_rows.append([
            str(row.get("reason") or ""),
            str(row.get("settled_rows") or 0),
            str(row.get("wins") or 0),
            _fmt(row.get("hit_rate")),
            _fmt(row.get("avg_p")),
            _fmt(row.get("avg_support"), 2),
        ])
    lines.append("## Excluded Reason Breakdown")
    lines.append("")
    lines.append(_md_table(["reason", "settled_rows", "wins", "hit_rate", "avg_p", "avg_support"], reason_rows) if reason_rows else "(No excluded example rows.)")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze a persisted HR target evaluation artifact.")
    ap.add_argument("--in", dest="inp", required=True, help="Path to hr_targets_eval_*.json")
    ap.add_argument("--out-json", default="", help="Optional analysis JSON output path")
    ap.add_argument("--out-md", default="", help="Optional markdown recap path")
    args = ap.parse_args()

    artifact_path = Path(str(args.inp)).resolve()
    doc = _read_json(artifact_path)
    if not isinstance(doc, dict):
        raise ValueError("Input artifact must be a JSON object")
    analysis = analyze(doc)

    out_json = Path(str(args.out_json)).resolve() if str(args.out_json or "").strip() else artifact_path.with_suffix(".analysis.json")
    out_md = Path(str(args.out_md)).resolve() if str(args.out_md or "").strip() else artifact_path.with_suffix(".md")
    _write_json(out_json, analysis)
    _write_text(out_md, build_markdown(analysis, artifact_path=artifact_path) + "\n")
    print(f"Wrote JSON: {out_json}")
    print(f"Wrote MD: {out_md}")
    print(json.dumps(analysis.get("selected_overall") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())