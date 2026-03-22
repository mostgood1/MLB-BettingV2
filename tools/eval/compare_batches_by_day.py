from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _fmt(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float):
        return f"{x:.6g}"
    return str(x)


def _direction_better(direction: str, candidate: float, baseline: float) -> Optional[bool]:
    d = (direction or "").strip().lower()
    if d == "lower":
        return candidate < baseline
    if d == "higher":
        return candidate > baseline
    if d == "lower_abs":
        return abs(candidate) < abs(baseline)
    return None


def _near_zero(x: float, eps: float) -> bool:
    return abs(float(x)) < float(eps)


def _eps_for_metric_path(path: str) -> float:
    p = (path or "").strip().lower()
    if "avg_edge_vs_no_vig" in p:
        return 1e-2
    return 1e-6


def _sratio(num: float, denom: float, eps: float) -> Optional[float]:
    if not (math.isfinite(float(num)) and math.isfinite(float(denom))):
        return None
    if num < 0 or denom < 0:
        return None
    return float((num + eps) / (denom + eps))


def _harm_rel(direction: str, candidate: float, baseline: float, *, eps: float) -> Optional[float]:
    """Return relative harm where positive means candidate worse.

    For direction:
    - lower: harm_rel = cand/base - 1
    - higher: harm_rel = base/cand - 1  (equivalently -(cand/base-1) for small deltas)
    - lower_abs: harm_rel = abs(cand)/abs(base) - 1
    """
    d = (direction or "").strip().lower()

    if not math.isfinite(float(baseline)):
        return None
    if d == "lower":
        if baseline < eps and baseline >= 0 and candidate >= 0:
            r = _sratio(candidate, baseline, eps)
            return float(r - 1.0) if r is not None else None
        if _near_zero(baseline, eps):
            return None
        return float(candidate / baseline - 1.0)
    if d == "higher":
        if not math.isfinite(float(candidate)):
            return None
        if candidate < eps and candidate >= 0 and baseline >= 0:
            r = _sratio(baseline, candidate, eps)
            return float(r - 1.0) if r is not None else None
        if _near_zero(candidate, eps):
            return None
        return float(baseline / candidate - 1.0)
    if d == "lower_abs":
        b = abs(baseline)
        c = abs(candidate)
        if b < eps:
            r = _sratio(c, b, eps)
            return float(r - 1.0) if r is not None else None
        if _near_zero(b, eps):
            return None
        return float(c / b - 1.0)
    return float(candidate / baseline - 1.0)


def _load_objective(path: Path) -> Dict[str, Any]:
    obj = _read_json(path)
    if not isinstance(obj, dict) or not isinstance(obj.get("metrics"), list):
        raise ValueError("Objective must be a JSON object with metrics: []")
    return obj


def _report_files(batch_dir: Path) -> List[Path]:
    return sorted(batch_dir.glob("sim_vs_actual_*.json"))


def _date_key(report_path: Path, report: Dict[str, Any]) -> str:
    meta = report.get("meta") or {}
    d = str(meta.get("date") or "").strip()
    if d:
        return d
    name = report_path.name
    return name.replace("sim_vs_actual_", "").replace(".json", "")


def _get_games_weight(report: Dict[str, Any], segment: str) -> float:
    agg = ((report.get("aggregate") or {}).get(segment) or {})
    try:
        g = int(agg.get("games") or 0)
    except Exception:
        g = 0
    if g > 0:
        return float(g)
    games_arr = report.get("games")
    if isinstance(games_arr, list):
        return float(len(games_arr))
    return 0.0


def _extract_metric(report: Dict[str, Any], path: str) -> Tuple[Optional[float], float]:
    """Extract (value, weight) for an objective metric path from a per-day report."""

    if path.startswith("full_weighted."):
        key = path.split(".", 1)[1]
        val = (((report.get("aggregate") or {}).get("full") or {}).get(key))
        w = _get_games_weight(report, "full")
        return (float(val) if _is_num(val) else None, float(w))

    if path.startswith("first5_weighted."):
        key = path.split(".", 1)[1]
        val = (((report.get("aggregate") or {}).get("first5") or {}).get(key))
        w = _get_games_weight(report, "first5")
        return (float(val) if _is_num(val) else None, float(w))

    if path.startswith("first3_weighted."):
        key = path.split(".", 1)[1]
        val = (((report.get("aggregate") or {}).get("first3") or {}).get(key))
        w = _get_games_weight(report, "first3")
        return (float(val) if _is_num(val) else None, float(w))

    if path.startswith("pitcher_props_at_market_lines_weighted."):
        suffix = path.split(".", 1)[1]
        if "_" not in suffix:
            return (None, 0.0)
        prefix, key = suffix.split("_", 1)
        prop_root = (((report.get("assessment") or {}).get("full_game") or {}).get("pitcher_props_at_market_lines") or {})
        if prefix == "so":
            block = prop_root.get("strikeouts") or {}
        elif prefix == "outs":
            block = prop_root.get("outs") or {}
        else:
            return (None, 0.0)

        try:
            n = int(block.get("n") or 0)
        except Exception:
            n = 0
        val = block.get(key)
        return (float(val) if _is_num(val) else None, float(n))

    if path.startswith("hitter_hr_likelihood_topn_weighted."):
        key = path.split(".", 1)[1]
        hr = (((report.get("assessment") or {}).get("full_game") or {}).get("hitter_hr_likelihood_topn") or {})
        try:
            n = int(hr.get("n") or 0)
        except Exception:
            n = 0
        # Per-day reports historically used keys: {brier, logloss, avg_p, emp_rate}
        # while some objective summaries use {hr_brier, hr_logloss}. Support both.
        val = hr.get(key)
        if val is None and isinstance(key, str) and key.startswith("hr_"):
            val = hr.get(key[3:])
        return (float(val) if _is_num(val) else None, float(n))

    if path.startswith("hitter_props_likelihood_topn_weighted."):
        suffix = path.split(".", 1)[1]
        hp_root = (((report.get("assessment") or {}).get("full_game") or {}).get("hitter_props_likelihood_topn") or {})
        if not isinstance(hp_root, dict):
            return (None, 0.0)

        metric_names = ("brier", "logloss", "avg_p", "emp_rate")
        metric = None
        prop = None
        for m in metric_names:
            tag = "_" + m
            if suffix.endswith(tag):
                metric = m
                prop = suffix[: -len(tag)]
                break
        if not metric or not prop:
            return (None, 0.0)

        block = hp_root.get(prop) or {}
        if not isinstance(block, dict):
            return (None, 0.0)
        try:
            n = int(block.get("n") or 0)
        except Exception:
            n = 0
        val = block.get(metric)
        return (float(val) if _is_num(val) else None, float(n))

    raise KeyError(f"Unsupported objective path for per-day report: {path}")


def to_markdown(report: Dict[str, Any]) -> str:
    obj_name = report.get("objective")
    base_dir = report.get("baseline_batch_dir")
    cand_dir = report.get("candidate_batch_dir")

    lines: List[str] = []
    lines.append(f"# Batch Attribution by Day: {obj_name}")
    lines.append("")
    lines.append(f"- Baseline: `{base_dir}`")
    lines.append(f"- Candidate: `{cand_dir}`")
    lines.append("")

    def _table(
        rows: List[Dict[str, Any]],
        title: str,
        headers: List[str],
        getters: List,
    ) -> None:
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "|".join(["---"] + ["---:" for _ in headers[1:]]) + "|")
        for r in rows:
            cells: List[str] = []
            for g in getters:
                try:
                    cells.append(_fmt(g(r)))
                except Exception:
                    cells.append("")
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    def _delta_for_row(row: Dict[str, Any], path: str) -> Optional[float]:
        x = (row.get("metrics") or {}).get(path) or {}
        if x.get("status") == "ok" and _is_num(x.get("delta")):
            return float(x["delta"])
        return None

    worst = list(report.get("top_worst_days") or [])
    best = list(report.get("top_best_days") or [])
    worst_title = str(report.get("top_worst_title") or "Worst Days")
    best_title = str(report.get("top_best_title") or "Best Days")

    core_headers = [
        "date",
        "contrib",
        "full brier",
        "full margin mae",
        "full total mae",
        "outs logloss",
        "outs brier",
        "so logloss",
    ]
    core_getters = [
        lambda r: r.get("date"),
        lambda r: r.get("contrib_total"),
        lambda r: (r.get("metric_quick") or {}).get("full_brier_home_win_delta"),
        lambda r: (r.get("metric_quick") or {}).get("full_mae_run_margin_delta"),
        lambda r: (r.get("metric_quick") or {}).get("full_mae_total_runs_delta"),
        lambda r: (r.get("metric_quick") or {}).get("outs_logloss_delta"),
        lambda r: (r.get("metric_quick") or {}).get("outs_brier_delta"),
        lambda r: (r.get("metric_quick") or {}).get("so_logloss_delta"),
    ]

    if worst:
        _table(worst, worst_title, core_headers, core_getters)
    if best:
        _table(best, best_title, core_headers, core_getters)

    # Optional: if hitter-props metrics are present in the objective, add a focused table.
    any_hitter_paths = False
    for p in (report.get("metric_total_common_weight") or {}).keys():
        if str(p).startswith("hitter_hr_likelihood_topn_weighted.") or str(p).startswith("hitter_props_likelihood_topn_weighted."):
            any_hitter_paths = True
            break

    if any_hitter_paths:
        hitter_headers = [
            "date",
            "contrib",
            "HR logloss",
            "H 1+ logloss",
            "H 2+ logloss",
            "R 1+ logloss",
            "RBI 1+ logloss",
            "SB 1+ logloss",
        ]
        hitter_getters = [
            lambda r: r.get("date"),
            lambda r: r.get("contrib_total"),
            lambda r: _delta_for_row(r, "hitter_hr_likelihood_topn_weighted.hr_logloss"),
            lambda r: _delta_for_row(r, "hitter_props_likelihood_topn_weighted.hits_1plus_logloss"),
            lambda r: _delta_for_row(r, "hitter_props_likelihood_topn_weighted.hits_2plus_logloss"),
            lambda r: _delta_for_row(r, "hitter_props_likelihood_topn_weighted.runs_1plus_logloss"),
            lambda r: _delta_for_row(r, "hitter_props_likelihood_topn_weighted.rbi_1plus_logloss"),
            lambda r: _delta_for_row(r, "hitter_props_likelihood_topn_weighted.sb_1plus_logloss"),
        ]
        if worst:
            _table(worst, worst_title + " (hitter props focus)", hitter_headers, hitter_getters)
        if best:
            _table(best, best_title + " (hitter props focus)", hitter_headers, hitter_getters)

    lines.append("## Notes")
    lines.append("")
    lines.append("- `contrib` is an approximate objective-weighted relative harm score aggregated across metrics.")
    lines.append("- Positive `contrib` => candidate worse on that date; negative => candidate better.")
    lines.append("- Use the date to open `sim_vs_actual_<date>.json` in each batch folder and inspect game-level differences.")
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Attribute batch-level metric deltas to specific days by comparing per-day sim_vs_actual_*.json files."
    )
    ap.add_argument("--objective", required=True, help="Objective JSON (data/tuning/objectives/*.json)")
    ap.add_argument("--baseline-batch-dir", required=True, help="Baseline batch folder with sim_vs_actual_*.json")
    ap.add_argument("--candidate-batch-dir", required=True, help="Candidate batch folder with sim_vs_actual_*.json")
    ap.add_argument(
        "--out-prefix",
        default="",
        help="If set, writes <prefix>.json and <prefix>.md (otherwise prints markdown)",
    )
    ap.add_argument("--top-n", type=int, default=15, help="Number of best/worst days to include")
    args = ap.parse_args()

    objective = _load_objective(Path(args.objective))
    metrics = [m for m in (objective.get("metrics") or []) if isinstance(m, dict)]

    base_dir = Path(args.baseline_batch_dir)
    cand_dir = Path(args.candidate_batch_dir)
    base_files = _report_files(base_dir)
    cand_files = _report_files(cand_dir)

    if not base_files:
        raise ValueError(f"No baseline reports found under: {base_dir}")
    if not cand_files:
        raise ValueError(f"No candidate reports found under: {cand_dir}")

    base_by_date: Dict[str, Tuple[Path, Dict[str, Any]]] = {}
    for p in base_files:
        obj = _read_json(p)
        base_by_date[_date_key(p, obj)] = (p, obj)

    cand_by_date: Dict[str, Tuple[Path, Dict[str, Any]]] = {}
    for p in cand_files:
        obj = _read_json(p)
        cand_by_date[_date_key(p, obj)] = (p, obj)

    all_dates = sorted(set(base_by_date.keys()) | set(cand_by_date.keys()))

    # First pass: compute common weights per metric across all dates.
    metric_total_w: Dict[str, float] = {}
    for d in all_dates:
        base = base_by_date.get(d)
        cand = cand_by_date.get(d)
        if not base or not cand:
            continue
        _, bobj = base
        _, cobj = cand

        for m in metrics:
            path = str(m.get("path") or "").strip()
            if not path:
                continue
            try:
                bval, bw = _extract_metric(bobj, path)
                cval, cw = _extract_metric(cobj, path)
            except KeyError:
                continue
            if bval is None or cval is None:
                continue
            w = float(min(bw, cw)) if (bw > 0 and cw > 0) else float(max(bw, cw))
            if w <= 0:
                continue
            metric_total_w[path] = metric_total_w.get(path, 0.0) + float(w)

    per_day_rows: List[Dict[str, Any]] = []

    for d in all_dates:
        base = base_by_date.get(d)
        cand = cand_by_date.get(d)
        row: Dict[str, Any] = {
            "date": d,
            "baseline_path": str(base[0]) if base else None,
            "candidate_path": str(cand[0]) if cand else None,
            "metrics": {},
        }

        contrib_total = 0.0
        for m in metrics:
            path = str(m.get("path") or "").strip()
            direction = str(m.get("direction") or "lower").strip().lower()
            w_obj = float(m.get("weight") or 0.0)
            if not path or w_obj <= 0:
                continue

            if not base or not cand:
                row["metrics"][path] = {"status": "missing_day"}
                continue

            _, bobj = base
            _, cobj = cand
            try:
                bval, bw = _extract_metric(bobj, path)
                cval, cw = _extract_metric(cobj, path)
            except KeyError:
                row["metrics"][path] = {"status": "unsupported"}
                continue

            if bval is None or cval is None:
                row["metrics"][path] = {
                    "status": "missing_metric",
                    "baseline": bval,
                    "candidate": cval,
                    "baseline_weight": bw,
                    "candidate_weight": cw,
                }
                continue

            delta = float(cval - bval)
            pct = (float(cval / bval - 1.0)) if bval != 0 else None
            better = _direction_better(direction, float(cval), float(bval))

            w = float(min(bw, cw)) if (bw > 0 and cw > 0) else float(max(bw, cw))
            total_w = float(metric_total_w.get(path) or 0.0)
            share = (w / total_w) if total_w > 0 else 0.0
            eps = _eps_for_metric_path(path)
            harm_rel = _harm_rel(direction, float(cval), float(bval), eps=eps)
            contrib = float(w_obj * share * harm_rel) if harm_rel is not None else None
            if isinstance(contrib, float) and math.isfinite(contrib):
                contrib_total += contrib

            row["metrics"][path] = {
                "status": "ok",
                "direction": direction,
                "weight": w_obj,
                "baseline": float(bval),
                "candidate": float(cval),
                "delta": delta,
                "pct": pct,
                "better": better,
                "common_weight": w,
                "common_weight_share": share,
                "harm_rel": harm_rel,
                "contrib": contrib,
            }

        # Quick-access deltas for markdown table.
        def _delta_for(p: str) -> Optional[float]:
            x = (row.get("metrics") or {}).get(p) or {}
            if x.get("status") == "ok" and _is_num(x.get("delta")):
                return float(x["delta"])
            return None

        row["contrib_total"] = float(contrib_total)
        row["metric_quick"] = {
            "full_brier_home_win_delta": _delta_for("full_weighted.brier_home_win"),
            "full_mae_run_margin_delta": _delta_for("full_weighted.mae_run_margin"),
            "full_mae_total_runs_delta": _delta_for("full_weighted.mae_total_runs"),
            "outs_logloss_delta": _delta_for("pitcher_props_at_market_lines_weighted.outs_logloss"),
            "outs_brier_delta": _delta_for("pitcher_props_at_market_lines_weighted.outs_brier"),
            "so_logloss_delta": _delta_for("pitcher_props_at_market_lines_weighted.so_logloss"),
        }

        per_day_rows.append(row)

    # Rank days by contrib_total.
    # contrib_total is defined so: positive => candidate worse, negative => candidate better.
    top_n = max(1, int(args.top_n))
    worse_days = [r for r in per_day_rows if float(r.get("contrib_total") or 0.0) > 0.0]
    better_days = [r for r in per_day_rows if float(r.get("contrib_total") or 0.0) < 0.0]

    worse_sorted = sorted(worse_days, key=lambda r: float(r.get("contrib_total") or 0.0), reverse=True)
    better_sorted = sorted(better_days, key=lambda r: float(r.get("contrib_total") or 0.0))  # most negative first

    if worse_sorted:
        top_worst = worse_sorted[:top_n]
        worst_title = "Worst Days (candidate worse)"
    else:
        # If candidate is never worse, still show the dates with the smallest improvement.
        per_day_sorted = sorted(per_day_rows, key=lambda r: float(r.get("contrib_total") or 0.0), reverse=True)
        top_worst = per_day_sorted[:top_n]
        worst_title = "Least Improved Days (closest to neutral)"

    if better_sorted:
        top_best = better_sorted[:top_n]
        best_title = "Best Days (candidate better)"
    else:
        per_day_sorted = sorted(per_day_rows, key=lambda r: float(r.get("contrib_total") or 0.0))
        top_best = per_day_sorted[:top_n]
        best_title = "Most Harmful Days (closest to neutral)"

    out_obj: Dict[str, Any] = {
        "objective": objective.get("name"),
        "baseline_batch_dir": str(base_dir),
        "candidate_batch_dir": str(cand_dir),
        "dates": all_dates,
        "metric_total_common_weight": metric_total_w,
        "per_day": per_day_rows,
        "top_worst_days": top_worst,
        "top_best_days": top_best,
        "top_worst_title": worst_title,
        "top_best_title": best_title,
    }

    md = to_markdown(out_obj)

    out_prefix = str(args.out_prefix or "").strip()
    if out_prefix:
        out_prefix_path = Path(out_prefix)
        _write_json(out_prefix_path.with_suffix(".json"), out_obj)
        _write_text(out_prefix_path.with_suffix(".md"), md)
        print(f"Wrote: {out_prefix_path.with_suffix('.json')}")
        print(f"Wrote: {out_prefix_path.with_suffix('.md')}")
    else:
        print(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
