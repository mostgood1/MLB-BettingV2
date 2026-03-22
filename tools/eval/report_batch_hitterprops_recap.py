from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PROPS: List[str] = [
    "hr_1plus",
    "hits_1plus",
    "hits_2plus",
    "hits_3plus",
    "doubles_1plus",
    "triples_1plus",
    "runs_1plus",
    "runs_2plus",
    "runs_3plus",
    "rbi_1plus",
    "rbi_2plus",
    "rbi_3plus",
    "rbi_4plus",
    "total_bases_1plus",
    "total_bases_2plus",
    "total_bases_3plus",
    "total_bases_4plus",
    "total_bases_5plus",
    "sb_1plus",
]

PROP_DISPLAY: Dict[str, str] = {
    "hr_1plus": "HR 1+",
    "hits_1plus": "H 1+",
    "hits_2plus": "H 2+",
    "hits_3plus": "H 3+",
    "doubles_1plus": "2B 1+",
    "triples_1plus": "3B 1+",
    "runs_1plus": "R 1+",
    "runs_2plus": "R 2+",
    "runs_3plus": "R 3+",
    "rbi_1plus": "RBI 1+",
    "rbi_2plus": "RBI 2+",
    "rbi_3plus": "RBI 3+",
    "rbi_4plus": "RBI 4+",
    "total_bases_1plus": "TB 1+",
    "total_bases_2plus": "TB 2+",
    "total_bases_3plus": "TB 3+",
    "total_bases_4plus": "TB 4+",
    "total_bases_5plus": "TB 5+",
    "sb_1plus": "SB 1+",
}


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def _clip_prob(p: float, eps: float = 1e-12) -> float:
    return float(min(1.0 - eps, max(eps, float(p))))


def _logit(p: float) -> float:
    pp = _clip_prob(p)
    return float(math.log(pp) - math.log(1.0 - pp))


def _sigmoid(x: float) -> float:
    try:
        return float(1.0 / (1.0 + math.exp(-float(x))))
    except OverflowError:
        return 0.0 if float(x) < 0 else 1.0


def _apply_affine_logit(p: float, *, a: float, b: float) -> float:
    x = _logit(float(p))
    return _clip_prob(_sigmoid(float(a) * x + float(b)))


def _scale_affine_logit_params_toward_identity(a: float, b: float, scale: float) -> Tuple[float, float]:
    s = float(scale)
    if not math.isfinite(s):
        raise ValueError(f"non-finite scale: {scale}")
    s = float(max(-1.0, min(2.0, s)))
    aa = float(a)
    bb = float(b)
    return (1.0 + (aa - 1.0) * s, bb * s)


def _load_affine_logit_params(path: Path) -> Tuple[float, float]:
    obj = _read_json(path)
    if not isinstance(obj, dict):
        raise ValueError(f"Invalid calibration JSON: {path}")
    if not bool(obj.get("enabled", True)):
        return (1.0, 0.0)
    if str(obj.get("mode") or "").strip() not in ("", "affine_logit"):
        raise ValueError(f"Unsupported calibration mode in {path}: {obj.get('mode')}")
    a = float(obj.get("a") or 1.0)
    b = float(obj.get("b") or 0.0)
    return (a, b)


def _load_prop_calibration(path: Path) -> Dict[str, Tuple[float, float]]:
    obj = _read_json(path)
    if not isinstance(obj, dict):
        raise ValueError(f"Invalid calibration JSON: {path}")
    props = obj.get("props")
    if not isinstance(props, dict):
        raise ValueError(f"Invalid props calibration JSON: {path}")
    out: Dict[str, Tuple[float, float]] = {}
    for k, blk in props.items():
        if not isinstance(blk, dict):
            continue
        if not bool(blk.get("enabled", True)):
            continue
        if str(blk.get("mode") or "").strip() not in ("", "affine_logit"):
            continue
        a = float(blk.get("a") or 1.0)
        b = float(blk.get("b") or 0.0)
        out[str(k)] = (a, b)
    return out


def _fmt(x: Any, nd: int = 4) -> str:
    if x is None:
        return ""
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    lines: List[str] = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    return "\n".join(lines)


def _load_summary(batch_dir: Path) -> Dict[str, Any]:
    p = batch_dir / "summary.json"
    obj = _read_json(p)
    if not isinstance(obj, dict):
        raise ValueError(f"Invalid summary.json: {p}")
    return obj


_DAY_RE = re.compile(r"^sim_vs_actual_(\d{4}-\d{2}-\d{2})$")


def _parse_day_from_sim_vs_actual_path(path: Path) -> Optional[str]:
    m = _DAY_RE.match(path.stem)
    if not m:
        return None
    return str(m.group(1))


def _read_date_set(path: Path) -> set[str]:
    dates: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = str(raw).strip()
        if not s or s.startswith("#"):
            continue
        dates.add(s)
    return dates


def _iter_day_files(
    batch_dir: Path,
    *,
    include_dates: Optional[set[str]] = None,
    exclude_dates: Optional[set[str]] = None,
) -> List[Path]:
    files = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    if not include_dates and not exclude_dates:
        return files

    out: List[Path] = []
    for fp in files:
        day = _parse_day_from_sim_vs_actual_path(fp)
        if day is None:
            continue
        if include_dates is not None and day not in include_dates:
            continue
        if exclude_dates is not None and day in exclude_dates:
            continue
        out.append(fp)
    return out


def _collect_topk_metrics(
    batch_dir: Path,
    k: int,
    *,
    include_dates: Optional[set[str]] = None,
    exclude_dates: Optional[set[str]] = None,
    hr_calib: Optional[Tuple[float, float]] = None,
    props_calib: Optional[Dict[str, Tuple[float, float]]] = None,
) -> Dict[str, Dict[str, float]]:
    """Compute top-k per game hit rate + avg calibrated p across a batch.

    Reads per-game backtest scored lists, which are already sorted by p_cal.
    """

    files = _iter_day_files(batch_dir, include_dates=include_dates, exclude_dates=exclude_dates)
    out: Dict[str, Dict[str, float]] = {p: {"n": 0.0, "hits": 0.0, "p_sum": 0.0} for p in PROPS}

    def add(prop: str, row: Dict[str, Any]) -> None:
        if prop == "hr_1plus":
            p_raw = _safe_float(row.get("p_hr_1plus"))
            y = int(row.get("y_hr_1plus") or 0)
            if p_raw is None:
                return
            if hr_calib is None:
                p_use = _safe_float(row.get("p_hr_1plus_cal"))
            else:
                a, b = hr_calib
                p_use = _apply_affine_logit(float(p_raw), a=float(a), b=float(b))
        else:
            p_raw = _safe_float(row.get("p"))
            y = int(row.get("y") or 0)
            if p_raw is None:
                return
            if props_calib is None or prop not in props_calib:
                p_use = _safe_float(row.get("p_cal"))
            else:
                a, b = props_calib[prop]
                p_use = _apply_affine_logit(float(p_raw), a=float(a), b=float(b))

        if p_use is None:
            return
        out[prop]["n"] += 1.0
        out[prop]["hits"] += 1.0 if y == 1 else 0.0
        out[prop]["p_sum"] += float(p_use)

    for fp in files:
        day = _read_json(fp)
        games = day.get("games") if isinstance(day, dict) else None
        if not isinstance(games, list):
            continue

        for g in games:
            if not isinstance(g, dict):
                continue

            hb = g.get("hitter_hr_backtest") or {}
            scored_hr = hb.get("scored_overall") if isinstance(hb, dict) else None
            if isinstance(scored_hr, list) and scored_hr:
                rows: List[Dict[str, Any]] = [r for r in scored_hr if isinstance(r, dict)]
                if hr_calib is not None:
                    a, b = hr_calib
                    rows.sort(key=lambda rr: _apply_affine_logit(float(rr.get("p_hr_1plus") or 0.0), a=a, b=b), reverse=True)
                for r in rows[:k]:
                    add("hr_1plus", r)

            hp = g.get("hitter_props_backtest")
            if not isinstance(hp, dict):
                continue
            for prop in PROPS[1:]:
                blk = hp.get(prop) or {}
                scored = blk.get("scored") if isinstance(blk, dict) else None
                if isinstance(scored, list) and scored:
                    rows2: List[Dict[str, Any]] = [r for r in scored if isinstance(r, dict)]
                    if props_calib is not None and prop in props_calib:
                        a, b = props_calib[prop]
                        rows2.sort(key=lambda rr: _apply_affine_logit(float(rr.get("p") or 0.0), a=a, b=b), reverse=True)
                    for r in rows2[:k]:
                        add(prop, r)

    # finalize
    fin: Dict[str, Dict[str, float]] = {}
    for prop in PROPS:
        n = float(out[prop]["n"])
        hits = float(out[prop]["hits"])
        p_sum = float(out[prop]["p_sum"])
        fin[prop] = {
            "n": n,
            "hit_rate": (hits / n) if n > 0 else float("nan"),
            "avg_p_cal": (p_sum / n) if n > 0 else float("nan"),
        }
    return fin


def _prop_display(prop: str) -> str:
    return PROP_DISPLAY.get(prop, prop)


def _logloss(ps: List[float], ys: List[int]) -> float:
    n = 0
    s = 0.0
    for p, y in zip(ps, ys):
        pp = _clip_prob(float(p))
        yy = 1 if int(y) == 1 else 0
        s += -(yy * math.log(pp) + (1 - yy) * math.log(1.0 - pp))
        n += 1
    return float(s / max(1, n))


def _brier(ps: List[float], ys: List[int]) -> float:
    n = 0
    s = 0.0
    for p, y in zip(ps, ys):
        pp = float(p)
        yy = 1.0 if int(y) == 1 else 0.0
        s += (pp - yy) * (pp - yy)
        n += 1
    return float(s / max(1, n))


def _collect_topn_scoring(
    batch_dir: Path,
    *,
    include_dates: Optional[set[str]] = None,
    exclude_dates: Optional[set[str]] = None,
    hr_calib: Optional[Tuple[float, float]] = None,
    props_calib: Optional[Dict[str, Tuple[float, float]]] = None,
) -> Dict[str, Dict[str, float]]:
    """Compute Brier/logloss/avg_p/emp_rate over the stored top-8 lists.

    If calibration params are provided, re-applies them to raw p.
    """

    files = _iter_day_files(batch_dir, include_dates=include_dates, exclude_dates=exclude_dates)
    out: Dict[str, Dict[str, float]] = {}

    def add(prop: str, p: float, y: int) -> None:
        blk = out.setdefault(prop, {"n": 0.0, "p_sum": 0.0, "y_sum": 0.0, "brier_sum": 0.0, "logloss_sum": 0.0})
        pp = _clip_prob(float(p))
        yy = 1 if int(y) == 1 else 0
        blk["n"] += 1.0
        blk["p_sum"] += float(pp)
        blk["y_sum"] += float(yy)
        blk["brier_sum"] += float(pp - yy) * float(pp - yy)
        blk["logloss_sum"] += -float(yy) * math.log(pp) - float(1 - yy) * math.log(1.0 - pp)

    for fp in files:
        day = _read_json(fp)
        games = day.get("games") if isinstance(day, dict) else None
        if not isinstance(games, list):
            continue
        for g in games:
            if not isinstance(g, dict):
                continue

            hb = g.get("hitter_hr_backtest") or {}
            scored_hr = hb.get("scored_overall") if isinstance(hb, dict) else None
            if isinstance(scored_hr, list):
                for r in scored_hr:
                    if not isinstance(r, dict):
                        continue
                    p_raw = _safe_float(r.get("p_hr_1plus"))
                    y = int(r.get("y_hr_1plus") or 0)
                    if p_raw is None:
                        continue
                    if hr_calib is None:
                        p_use = _safe_float(r.get("p_hr_1plus_cal"))
                    else:
                        a, b = hr_calib
                        p_use = _apply_affine_logit(float(p_raw), a=a, b=b)
                    if p_use is None:
                        continue
                    add("hr_1plus", float(p_use), y)

            hp = g.get("hitter_props_backtest")
            if not isinstance(hp, dict):
                continue
            for prop in PROPS[1:]:
                blk = hp.get(prop) or {}
                scored = blk.get("scored") if isinstance(blk, dict) else None
                if not isinstance(scored, list):
                    continue
                for r in scored:
                    if not isinstance(r, dict):
                        continue
                    p_raw = _safe_float(r.get("p"))
                    y = int(r.get("y") or 0)
                    if p_raw is None:
                        continue
                    if props_calib is None or prop not in props_calib:
                        p_use = _safe_float(r.get("p_cal"))
                    else:
                        a, b = props_calib[prop]
                        p_use = _apply_affine_logit(float(p_raw), a=a, b=b)
                    if p_use is None:
                        continue
                    add(prop, float(p_use), y)

    fin: Dict[str, Dict[str, float]] = {}
    for prop, blk in out.items():
        n = float(blk.get("n") or 0.0)
        fin[prop] = {
            "n": n,
            "brier": float(blk["brier_sum"]) / n if n > 0 else float("nan"),
            "logloss": float(blk["logloss_sum"]) / n if n > 0 else float("nan"),
            "avg_p": float(blk["p_sum"]) / n if n > 0 else float("nan"),
            "emp_rate": float(blk["y_sum"]) / n if n > 0 else float("nan"),
        }
    return fin


def _summary_prop_rows(
    summary: Dict[str, Any],
    *,
    recomputed_topn: Optional[Dict[str, Dict[str, float]]] = None,
) -> List[List[str]]:
    if recomputed_topn is not None:
        hr = recomputed_topn.get("hr_1plus") or {}
        hp = recomputed_topn
    else:
        hr = summary.get("hitter_hr_likelihood_topn_weighted") or {}
        hp = summary.get("hitter_props_likelihood_topn_weighted") or {}

    rows: List[List[str]] = []

    def add_row(name: str, brier: Any, logloss: Any, avg_p: Any, emp: Any, n: Any) -> None:
        rows.append(
            [
                name,
                _fmt(_safe_float(brier)),
                _fmt(_safe_float(logloss)),
                _fmt(_safe_float(avg_p)),
                _fmt(_safe_float(emp)),
                str(int(float(n or 0))) if _safe_float(n) is not None else "",
            ]
        )

    add_row(
        "HR 1+",
        (hr.get("brier") if recomputed_topn is not None else hr.get("hr_brier")),
        (hr.get("logloss") if recomputed_topn is not None else hr.get("hr_logloss")),
        (hr.get("avg_p") if recomputed_topn is not None else hr.get("hr_avg_p")),
        (hr.get("emp_rate") if recomputed_topn is not None else hr.get("hr_emp_rate")),
        (hr.get("n") if recomputed_topn is not None else hr.get("hr_brier_weight")),
    )

    for p in PROPS[1:]:
        name = _prop_display(p)
        if recomputed_topn is not None:
            blk = hp.get(p) or {}
            add_row(name, blk.get("brier"), blk.get("logloss"), blk.get("avg_p"), blk.get("emp_rate"), blk.get("n"))
        else:
            def k(prefix: str, key: str) -> str:
                return f"{prefix}_{key}"

            add_row(
                name,
                hp.get(k(p, "brier")),
                hp.get(k(p, "logloss")),
                hp.get(k(p, "avg_p")),
                hp.get(k(p, "emp_rate")),
                hp.get(k(p, "brier_weight")),
            )

    return rows


def _summary_core_lines(summary: Dict[str, Any]) -> List[str]:
    fw = summary.get("full_weighted") or {}
    f5 = summary.get("first5_weighted") or {}
    f3 = summary.get("first3_weighted") or {}
    lines: List[str] = []

    def ln(title: str, blk: Dict[str, Any]) -> None:
        lines.append(
            f"- {title}: brier={_fmt(_safe_float(blk.get('brier_home_win')), nd=4)}, "
            f"totals_mae={_fmt(_safe_float(blk.get('mae_total_runs')), nd=3)}, "
            f"margin_mae={_fmt(_safe_float(blk.get('mae_run_margin')), nd=3)}"
        )

    ln("Full", fw)
    ln("First5", f5)
    ln("First3", f3)
    return lines


def _topk_rows(topk: Dict[str, Dict[str, float]], label: str) -> List[List[str]]:
    rows: List[List[str]] = []
    for prop in PROPS:
        blk = topk.get(prop) or {}
        rows.append(
            [
                _prop_display(prop),
                label,
                str(int(float(blk.get("n") or 0.0))),
                _fmt(_safe_float(blk.get("hit_rate")), nd=3),
                _fmt(_safe_float(blk.get("avg_p_cal")), nd=3),
            ]
        )
    return rows


def _render_one(
    label: str,
    batch_dir: Path,
    *,
    include_dates: Optional[set[str]] = None,
    exclude_dates: Optional[set[str]] = None,
    hr_calib: Optional[Tuple[float, float]] = None,
    props_calib: Optional[Dict[str, Tuple[float, float]]] = None,
) -> str:
    summary = _load_summary(batch_dir)
    files = _iter_day_files(batch_dir, include_dates=include_dates, exclude_dates=exclude_dates)
    top1 = _collect_topk_metrics(batch_dir, k=1, include_dates=include_dates, exclude_dates=exclude_dates, hr_calib=hr_calib, props_calib=props_calib)
    top3 = _collect_topk_metrics(batch_dir, k=3, include_dates=include_dates, exclude_dates=exclude_dates, hr_calib=hr_calib, props_calib=props_calib)
    topn = (
        _collect_topn_scoring(batch_dir, include_dates=include_dates, exclude_dates=exclude_dates, hr_calib=hr_calib, props_calib=props_calib)
        if (hr_calib or props_calib)
        else None
    )

    reports = int(summary.get("reports") or 0)
    days_summary = int(summary.get("days") or 0)
    games_summary = int(summary.get("total_games") or 0)

    days = len(files)
    games = 0
    for fp in files:
        day = _read_json(fp)
        gs = day.get("games") if isinstance(day, dict) else None
        if isinstance(gs, list):
            games += len(gs)

    lines: List[str] = []
    lines.append(f"## {label}")
    lines.append("")
    lines.append(f"- batch_dir: {str(batch_dir.as_posix())}")
    if include_dates is not None or exclude_dates is not None:
        lines.append(f"- days (filtered): {days}, games (filtered): {games}")
        lines.append(f"- days (summary): {days_summary} (reports={reports}), games (summary): {games_summary}")
    else:
        lines.append(f"- days: {days_summary} (reports={reports}), games: {games_summary}")
    lines.append("")
    lines.append("### Core")
    lines.extend(_summary_core_lines(summary))

    lines.append("")
    lines.append("### Hitter Props (top-8 lists) — calibration + scoring")
    lines.append(
        _md_table(
            ["prop", "brier", "logloss", "avg_p", "emp_rate", "n"],
            _summary_prop_rows(summary, recomputed_topn=topn),
        )
    )

    lines.append("")
    lines.append("### Best Picks Per Game")
    rows = _topk_rows(top1, "top1") + _topk_rows(top3, "top3")
    lines.append(_md_table(["prop", "bucket", "n", "hit_rate", "avg_p_cal"], rows))

    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate a Markdown recap of hitter props performance for a batch.")
    ap.add_argument("--random50-batch", required=True, help="Batch dir containing sim_vs_actual_*.json + summary.json")
    ap.add_argument("--random10-batch", default="", help="Optional holdout batch dir")
    ap.add_argument("--hr-calib", default="", help="Optional hitter HR calibration JSON to re-apply (affects top-8 + top1/top3)")
    ap.add_argument("--props-calib", default="", help="Optional hitter props calibration JSON to re-apply (affects top-8 + top1/top3)")
    ap.add_argument(
        "--props-calib-scale",
        action="append",
        default=[],
        help=(
            "Optional scale override(s) for props calibration as prop=scale (repeatable). "
            "scale=1 keeps params as-is; scale=0 turns them into identity. "
            "Special keys: all=<s>."
        ),
    )
    ap.add_argument("--include-dates", default="", help="Optional text file of YYYY-MM-DD dates to include (one per line)")
    ap.add_argument("--exclude-dates", default="", help="Optional text file of YYYY-MM-DD dates to exclude (one per line)")
    ap.add_argument("--out", default="", help="Output markdown path")
    ap.add_argument("--title", default="Hitter Props Recap", help="Report title")
    args = ap.parse_args()

    r50 = Path(args.random50_batch)
    r10 = Path(args.random10_batch) if str(args.random10_batch).strip() else None

    hr_calib: Optional[Tuple[float, float]] = None
    props_calib: Optional[Dict[str, Tuple[float, float]]] = None
    if str(args.hr_calib).strip():
        hr_calib = _load_affine_logit_params(Path(str(args.hr_calib)))
    if str(args.props_calib).strip():
        props_calib = _load_prop_calibration(Path(str(args.props_calib)))

    props_scales = list(getattr(args, "props_calib_scale", []) or [])
    if props_scales:
        if props_calib is None:
            raise SystemExit("--props-calib-scale requires --props-calib")
        for item in props_scales:
            s = str(item or "").strip()
            if not s:
                continue
            if "=" not in s:
                raise SystemExit(f"Invalid --props-calib-scale (expected prop=scale): {s}")
            k, v = s.split("=", 1)
            k = str(k).strip()
            if not k:
                raise SystemExit(f"Invalid --props-calib-scale (empty prop): {s}")
            try:
                sc = float(str(v).strip())
            except Exception as e:
                raise SystemExit(f"Invalid --props-calib-scale {k}={v}: {e}")

            if k == "all":
                for pk in list(props_calib.keys()):
                    a0, b0 = props_calib[pk]
                    a1, b1 = _scale_affine_logit_params_toward_identity(float(a0), float(b0), float(sc))
                    props_calib[pk] = (float(a1), float(b1))
                continue

            if k not in props_calib:
                raise SystemExit(f"Unknown prop for --props-calib-scale: {k}")
            a0, b0 = props_calib[k]
            a1, b1 = _scale_affine_logit_params_toward_identity(float(a0), float(b0), float(sc))
            props_calib[k] = (float(a1), float(b1))

    include_dates: Optional[set[str]] = None
    exclude_dates: Optional[set[str]] = None
    if str(args.include_dates).strip():
        include_dates = _read_date_set(Path(str(args.include_dates)))
    if str(args.exclude_dates).strip():
        exclude_dates = _read_date_set(Path(str(args.exclude_dates)))

    lines: List[str] = []
    lines.append(f"# {str(args.title)}")
    lines.append("")
    lines.append(
        _render_one(
            "Random50",
            r50,
            include_dates=include_dates,
            exclude_dates=exclude_dates,
            hr_calib=hr_calib,
            props_calib=props_calib,
        )
    )

    if r10 is not None:
        lines.append("")
        lines.append(
            _render_one(
                "Random10 (holdout)",
                r10,
                include_dates=include_dates,
                exclude_dates=exclude_dates,
                hr_calib=hr_calib,
                props_calib=props_calib,
            )
        )

    out_path = Path(args.out) if str(args.out).strip() else (r50.parent / "_recap_hitterprops.md")
    _write_text(out_path, "\n".join(lines) + "\n")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
