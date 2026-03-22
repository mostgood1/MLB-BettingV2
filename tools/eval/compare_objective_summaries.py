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


def _get(obj: Dict[str, Any], dotted: str) -> Any:
    cur: Any = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _fmt(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float):
        return f"{x:.6g}"
    return str(x)


def _direction_better(direction: str, candidate: float, baseline: float) -> Optional[bool]:
    d = direction.strip().lower()
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
        # Edge metrics naturally cluster near 0; ratio-to-baseline is extremely unstable.
        # Use a larger smoothing scale so these terms behave like a mild regularizer.
        return 1e-2
    return 1e-6


def _ratio(num: float, denom: float, eps: float) -> Optional[float]:
    if not (math.isfinite(float(num)) and math.isfinite(float(denom))):
        return None
    if _near_zero(denom, eps):
        return None
    return float(num) / float(denom)


def _sratio(num: float, denom: float, eps: float) -> Optional[float]:
    """Smoothed ratio: (num+eps)/(denom+eps).

    This is used to prevent ratio explosions when denom is small but non-zero
    (common for edge metrics near 0).
    """
    if not (math.isfinite(float(num)) and math.isfinite(float(denom))):
        return None
    if num < 0 or denom < 0:
        # Fallback: we only expect non-negative for the metrics we smooth.
        return _ratio(num, denom, eps)
    return float((num + eps) / (denom + eps))


def _score_term(
    direction: str,
    candidate: float,
    baseline: float,
    *,
    eps: float,
) -> Optional[float]:
    d = direction.strip().lower()
    if d == "lower":
        return _sratio(candidate, baseline, eps) if _near_zero(baseline, eps) else _ratio(candidate, baseline, eps)
    if d == "higher":
        return _sratio(baseline, candidate, eps) if _near_zero(candidate, eps) else _ratio(baseline, candidate, eps)
    if d == "lower_abs":
        c = abs(candidate)
        b = abs(baseline)
        return _sratio(c, b, eps) if _near_zero(b, eps) else _ratio(c, b, eps)
    return _ratio(candidate, baseline, eps)


def compare(
    *,
    objective: Dict[str, Any],
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    score_sum = 0.0
    used_w = 0.0

    for m in list(objective.get("metrics") or []):
        if not isinstance(m, dict):
            continue

        path = str(m.get("path") or "").strip()
        direction = str(m.get("direction") or "lower").strip().lower()
        weight = float(m.get("weight") or 0.0)
        if not path or weight <= 0:
            continue

        c = _get(candidate, path)
        b = _get(baseline, path)

        row: Dict[str, Any] = {
            "path": path,
            "direction": direction,
            "weight": weight,
        }

        if not _is_num(c) or not _is_num(b):
            row.update({"status": "missing", "candidate": c, "baseline": b})
            rows.append(row)
            continue

        c = float(c)
        b = float(b)
        eps = _eps_for_metric_path(path)
        delta = c - b
        pct = (c / b - 1.0) if (not _near_zero(b, eps)) else None
        better = _direction_better(direction, c, b)

        term = _score_term(direction, c, b, eps=eps)

        if term is not None and math.isfinite(term):
            score_sum += weight * term
            used_w += weight

        row.update(
            {
                "status": "ok",
                "baseline": b,
                "candidate": c,
                "delta": delta,
                "pct": pct,
                "better": better,
                "eps": eps,
                "term": term,
                "weighted": (weight * term) if term is not None else None,
            }
        )
        rows.append(row)

    score = score_sum / used_w if used_w > 0 else None

    def _meta(s: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "reports": s.get("reports"),
            "days": s.get("days"),
            "total_games": s.get("total_games"),
            "batch_dir": s.get("batch_dir"),
        }

    return {
        "objective": objective.get("name"),
        "normalize": objective.get("normalize"),
        "baseline_meta": _meta(baseline),
        "candidate_meta": _meta(candidate),
        "score": score,
        "weights_used": used_w,
        "rows": rows,
    }


def _starter_sources_from_summary(summary: Dict[str, Any]) -> Tuple[Dict[str, float], Dict[str, int], Optional[int]]:
    ss = summary.get("starter_sources")
    if not isinstance(ss, dict):
        return {}, {}, None

    shares_in = ss.get("shares")
    counts_in = ss.get("counts")
    total_in = ss.get("total_starters")

    shares: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    total: Optional[int] = int(total_in) if isinstance(total_in, int) else None

    if isinstance(counts_in, dict):
        for k, v in counts_in.items():
            try:
                counts[str(k)] = int(v)
            except Exception:
                continue

    if isinstance(shares_in, dict):
        for k, v in shares_in.items():
            try:
                fv = float(v)
                if math.isfinite(fv) and fv >= 0:
                    shares[str(k)] = fv
            except Exception:
                continue

    if not shares and counts and total and total > 0:
        shares = {k: (v / total) for k, v in counts.items() if v >= 0}

    return shares, counts, total


def _starter_mix_report(
    *,
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    warn_tv_threshold: float,
) -> Optional[Dict[str, Any]]:
    b_has = isinstance(baseline.get("starter_sources"), dict)
    c_has = isinstance(candidate.get("starter_sources"), dict)
    if not b_has and not c_has:
        return None

    b_shares, b_counts, b_total = _starter_sources_from_summary(baseline)
    c_shares, c_counts, c_total = _starter_sources_from_summary(candidate)

    keys = sorted(set(b_shares.keys()) | set(c_shares.keys()) | set(b_counts.keys()) | set(c_counts.keys()))

    rows: List[Dict[str, Any]] = []
    l1 = 0.0
    comparable = 0
    max_abs_delta: Optional[float] = None
    for k in keys:
        b = b_shares.get(k)
        c = c_shares.get(k)
        delta = None
        if isinstance(b, float) and isinstance(c, float):
            delta = c - b
            l1 += abs(delta)
            comparable += 1
            absd = abs(delta)
            max_abs_delta = absd if max_abs_delta is None else max(max_abs_delta, absd)

        rows.append(
            {
                "source": k,
                "baseline_share": b,
                "candidate_share": c,
                "delta": delta,
                "baseline_count": b_counts.get(k),
                "candidate_count": c_counts.get(k),
            }
        )

    tv: Optional[float] = (0.5 * l1) if comparable > 0 else None
    warn = bool(tv is not None and math.isfinite(tv) and tv >= float(warn_tv_threshold))

    return {
        "available": bool(comparable > 0),
        "baseline_total_starters": b_total,
        "candidate_total_starters": c_total,
        "tv": tv,
        "max_abs_delta": max_abs_delta,
        "warn": warn,
        "warn_tv_threshold": warn_tv_threshold,
        "rows": rows,
    }


def to_markdown(report: Dict[str, Any], *, baseline_path: str, candidate_path: str) -> str:
    lines: List[str] = []
    lines.append(f"# Objective Compare: {report.get('objective')}")
    lines.append("")
    lines.append(f"- Baseline: `{baseline_path}`")
    lines.append(f"- Candidate: `{candidate_path}`")
    if report.get("score") is not None:
        lines.append(f"- Objective score (lower is better): `{_fmt(report.get('score'))}`")
    lines.append("")

    bmeta = report.get("baseline_meta") or {}
    cmeta = report.get("candidate_meta") or {}
    lines.append("## Meta")
    lines.append("")
    lines.append("| | Baseline | Candidate |")
    lines.append("|---|---:|---:|")
    for k in ("reports", "days", "total_games"):
        lines.append(f"| {k} | {_fmt(bmeta.get(k))} | {_fmt(cmeta.get(k))} |")
    lines.append("")

    starter_mix = report.get("starter_mix")
    if isinstance(starter_mix, dict):
        lines.append("## Starter Mix")
        lines.append("")
        if not starter_mix.get("available"):
            lines.append("- Starter mix unavailable (missing starter_sources.shares in one or both summaries).")
            lines.append("")
        tv = starter_mix.get("tv")
        max_abs_delta = starter_mix.get("max_abs_delta")
        warn = starter_mix.get("warn")
        lines.append(
            f"- TV distance: `{_fmt(tv)}` (warn threshold `{_fmt(starter_mix.get('warn_tv_threshold'))}`)"
            + (" **WARNING**" if warn else "")
        )
        lines.append(f"- Max abs share delta: `{_fmt(max_abs_delta)}`")
        lines.append("")
        lines.append("| source | baseline share | candidate share | delta | baseline count | candidate count |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for r in starter_mix.get("rows") or []:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(r.get("source")),
                        _fmt(r.get("baseline_share")),
                        _fmt(r.get("candidate_share")),
                        _fmt(r.get("delta")),
                        _fmt(r.get("baseline_count")),
                        _fmt(r.get("candidate_count")),
                    ]
                )
                + " |"
            )
        lines.append("")

    lines.append("## Metrics")
    lines.append("")
    lines.append(
        "| metric | dir | w | baseline | candidate | delta | % | better |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")

    for r in report.get("rows") or []:
        if r.get("status") != "ok":
            lines.append(
                f"| {r.get('path')} | {r.get('direction')} | {_fmt(r.get('weight'))} |  |  |  |  | {r.get('status')} |"
            )
            continue

        pct = r.get("pct")
        pct_str = f"{pct * 100:.3f}%" if isinstance(pct, float) and math.isfinite(pct) else ""
        better = r.get("better")
        better_str = "yes" if better is True else "no" if better is False else ""

        lines.append(
            "| "
            + " | ".join(
                [
                    str(r.get("path")),
                    str(r.get("direction")),
                    _fmt(r.get("weight")),
                    _fmt(r.get("baseline")),
                    _fmt(r.get("candidate")),
                    _fmt(r.get("delta")),
                    pct_str,
                    better_str,
                ]
            )
            + " |"
        )

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Compare two summary.json files using an objective (same metric list/directions as scorer)."
    )
    ap.add_argument("--objective", required=True, help="Objective JSON (data/tuning/objectives/*.json)")
    ap.add_argument("--candidate-summary", required=True, help="Path to candidate summary.json")
    ap.add_argument(
        "--baseline-summary",
        default="",
        help="Optional baseline summary.json; defaults to objective.baseline_summary",
    )
    ap.add_argument(
        "--out-prefix",
        default="",
        help="If set, writes <prefix>.json and <prefix>.md (otherwise prints markdown)",
    )
    ap.add_argument(
        "--starter-mix-warn-tv",
        type=float,
        default=0.10,
        help="Warn in report if starter mix TV distance >= this threshold (requires starter_sources in summaries)",
    )
    args = ap.parse_args()

    obj_path = Path(args.objective)
    objective = _read_json(obj_path)
    if not isinstance(objective, dict) or not isinstance(objective.get("metrics"), list):
        raise ValueError("Objective must be a JSON object with metrics: []")

    cand_path = Path(args.candidate_summary)
    base_path_str = str(args.baseline_summary or "").strip() or str(objective.get("baseline_summary") or "").strip()
    base_path = Path(base_path_str)

    candidate = _read_json(cand_path)
    baseline = _read_json(base_path)
    if not isinstance(candidate, dict) or not isinstance(baseline, dict):
        raise ValueError("candidate and baseline must both be JSON objects")

    report = compare(objective=objective, baseline=baseline, candidate=candidate)
    report["starter_mix"] = _starter_mix_report(
        baseline=baseline,
        candidate=candidate,
        warn_tv_threshold=float(args.starter_mix_warn_tv),
    )

    baseline_label = str(base_path)
    candidate_label = str(cand_path)
    md = to_markdown(report, baseline_path=baseline_label, candidate_path=candidate_label)

    out_prefix = str(args.out_prefix or "").strip()
    if out_prefix:
        out_prefix_path = Path(out_prefix)
        _write_json(out_prefix_path.with_suffix(".json"), report)
        _write_text(out_prefix_path.with_suffix(".md"), md)
        print(f"Wrote: {out_prefix_path.with_suffix('.json')}")
        print(f"Wrote: {out_prefix_path.with_suffix('.md')}")
    else:
        print(md)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
