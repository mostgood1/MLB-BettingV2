from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _write_json(p: Path, obj: Any) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(p)


def _wavg(sum_wx: float, sum_w: float) -> Optional[float]:
    if sum_w <= 0:
        return None
    return float(sum_wx / sum_w)


def _get(d: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _acc_wavg(acc: Dict[str, Tuple[float, float]], key: str, x: float, w: float) -> None:
    sx, sw = acc.get(key, (0.0, 0.0))
    acc[key] = (sx + float(x) * float(w), sw + float(w))


def _source_key(x: Any) -> str:
    s = str(x or "").strip().lower()
    return s if s else "missing"


def _acc_count(acc: Dict[str, int], key: str, n: int = 1) -> None:
    acc[key] = int(acc.get(key, 0) + int(n))


def _shares(counts: Dict[str, int]) -> Dict[str, float]:
    tot = int(sum(int(v) for v in (counts or {}).values() if isinstance(v, int)))
    if tot <= 0:
        return {}
    out: Dict[str, float] = {}
    for k, v in (counts or {}).items():
        if not isinstance(v, int):
            continue
        out[str(k)] = float(v) / float(tot)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize a batch eval folder (roll up sim_vs_actual_*.json)")
    ap.add_argument("--batch-dir", required=True, help="Path to batch folder")
    ap.add_argument("--out", default="", help="Optional output JSON path")
    ap.add_argument(
        "--backfill-starter-provenance",
        choices=["on", "off"],
        default="off",
        help="If on, run tools/eval/backfill_starter_provenance.py on the batch before summarizing (best-effort).",
    )
    args = ap.parse_args()

    batch_dir = Path(args.batch_dir)
    if not batch_dir.exists():
        print(f"Missing batch dir: {batch_dir}")
        return 2

    if str(args.backfill_starter_provenance) == "on":
        tool = (Path(__file__).resolve().parent / "backfill_starter_provenance.py").resolve()
        if tool.exists():
            cmd = [sys.executable, str(tool), "--batch-dir", str(batch_dir)]
            r = subprocess.run(cmd, check=False)
            if r.returncode != 0:
                print(f"Warning: backfill_starter_provenance failed (exit {r.returncode}); continuing")
        else:
            print(f"Warning: backfill tool not found: {tool}; continuing")

    reports = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    if not reports:
        print(f"No reports found under: {batch_dir}")
        return 2

    # Weighted averages
    full_acc: Dict[str, Tuple[float, float]] = {}
    f5_acc: Dict[str, Tuple[float, float]] = {}
    f3_acc: Dict[str, Tuple[float, float]] = {}
    props_acc: Dict[str, Tuple[float, float]] = {}
    hitter_hr_acc: Dict[str, Tuple[float, float]] = {}
    hitter_props_acc: Dict[str, Tuple[float, float]] = {}

    total_days = 0
    total_games = 0

    # Starter provenance counts across all reports (counts starter *slots*, i.e. away+home).
    starter_source_counts: Dict[str, int] = {}

    per_day: List[Dict[str, Any]] = []

    for rp in reports:
        obj = _read_json(rp)
        total_days += 1

        # aggregate.* contains day-level means; weight by games
        agg_full = _get(obj, ["aggregate", "full"]) or {}
        agg_f5 = _get(obj, ["aggregate", "first5"]) or {}
        agg_f3 = _get(obj, ["aggregate", "first3"]) or {}

        try:
            g_full = int(agg_full.get("games") or 0)
        except Exception:
            g_full = 0
        if g_full <= 0:
            # fallback: count games array
            games_arr = obj.get("games") or []
            g_full = int(len(games_arr)) if isinstance(games_arr, list) else 0

        total_games += g_full

        # Starter provenance (if present in per-game rows)
        day_starter_sources: Dict[str, int] = {}
        games_arr = obj.get("games") or []
        if isinstance(games_arr, list) and games_arr:
            for g in games_arr:
                if not isinstance(g, dict):
                    continue
                rs = g.get("roster_starters")
                if not isinstance(rs, dict) or not rs:
                    # Old reports won't have this block.
                    continue
                a_src = _source_key(rs.get("away_source"))
                h_src = _source_key(rs.get("home_source"))
                _acc_count(starter_source_counts, a_src, 1)
                _acc_count(starter_source_counts, h_src, 1)
                _acc_count(day_starter_sources, a_src, 1)
                _acc_count(day_starter_sources, h_src, 1)

        for k in ("brier_home_win", "mae_total_runs", "mae_run_margin"):
            v = agg_full.get(k)
            if isinstance(v, (int, float)) and g_full > 0:
                _acc_wavg(full_acc, k, float(v), float(g_full))

        for k in ("brier_home_win", "mae_total_runs", "mae_run_margin"):
            v = agg_f5.get(k)
            if isinstance(v, (int, float)) and g_full > 0:
                _acc_wavg(f5_acc, k, float(v), float(g_full))

        for k in ("brier_home_win", "mae_total_runs", "mae_run_margin"):
            v = agg_f3.get(k)
            if isinstance(v, (int, float)) and g_full > 0:
                _acc_wavg(f3_acc, k, float(v), float(g_full))

        # Market-line prop scoring
        # - brier/logloss: weight by n rows scored for those metrics
        # - accuracy: weight by number of non-push rows (push rows are excluded from accuracy)
        props = _get(obj, ["assessment", "full_game", "pitcher_props_at_market_lines"]) or {}
        push_policy = str(props.get("push_policy") or "")
        so = (props.get("strikeouts") or {}) if isinstance(props, dict) else {}
        outs = (props.get("outs") or {}) if isinstance(props, dict) else {}

        for prefix, block in (("so", so), ("outs", outs)):
            try:
                n = int(block.get("n") or 0)
            except Exception:
                n = 0
            if n <= 0:
                continue
            for k in ("brier", "logloss"):
                v = block.get(k)
                if isinstance(v, (int, float)):
                    _acc_wavg(props_acc, f"{prefix}_{k}", float(v), float(n))

            v_edge = block.get("avg_edge_vs_no_vig")
            if isinstance(v_edge, (int, float)):
                try:
                    n_edge = int(block.get("n_edge") or 0)
                except Exception:
                    n_edge = 0
                if n_edge <= 0:
                    n_edge = n
                if n_edge > 0:
                    _acc_wavg(props_acc, f"{prefix}_avg_edge_vs_no_vig", float(v_edge), float(n_edge))

            v_acc = block.get("accuracy")
            if isinstance(v_acc, (int, float)):
                try:
                    n_acc = int(block.get("n_accuracy") or 0)
                except Exception:
                    n_acc = 0
                if n_acc <= 0:
                    try:
                        pushes = int(block.get("pushes") or 0)
                    except Exception:
                        pushes = 0
                    # Back-compat inference:
                    # - skip: push rows were excluded from brier/logloss too, so n already excludes pushes.
                    # - half/loss: push rows were included in brier/logloss n, but excluded from accuracy.
                    if push_policy in ("half", "loss"):
                        n_acc = max(n - pushes, 0)
                    else:
                        n_acc = n
                if n_acc > 0:
                    _acc_wavg(props_acc, f"{prefix}_accuracy", float(v_acc), float(n_acc))

        # Hitter HR likelihood scoring (weight by n)
        hr = _get(obj, ["assessment", "full_game", "hitter_hr_likelihood_topn"]) or {}
        try:
            n_hr = int(hr.get("n") or 0)
        except Exception:
            n_hr = 0
        if n_hr > 0:
            for k in ("brier", "logloss", "avg_p", "emp_rate"):
                v = hr.get(k)
                if isinstance(v, (int, float)):
                    _acc_wavg(hitter_hr_acc, f"hr_{k}", float(v), float(n_hr))

        # Hitter props likelihood scoring (weight by n per prop)
        hp = _get(obj, ["assessment", "full_game", "hitter_props_likelihood_topn"]) or {}
        if isinstance(hp, dict) and hp:
            for prop, block in hp.items():
                if not isinstance(block, dict):
                    continue
                try:
                    n_hp = int(block.get("n") or 0)
                except Exception:
                    n_hp = 0
                if n_hp <= 0:
                    continue
                for k in ("brier", "logloss", "avg_p", "emp_rate"):
                    v = block.get(k)
                    if isinstance(v, (int, float)):
                        _acc_wavg(hitter_props_acc, f"{str(prop)}_{k}", float(v), float(n_hp))

        # Light per-day row
        meta = obj.get("meta") or {}
        per_day.append(
            {
                "date": meta.get("date") or rp.name.replace("sim_vs_actual_", "").replace(".json", ""),
                "games": g_full,
                "full": {k: agg_full.get(k) for k in ("brier_home_win", "mae_total_runs", "mae_run_margin")},
                "starter_sources": (day_starter_sources or None),
            }
        )

    def finalize(acc: Dict[str, Tuple[float, float]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, (sx, sw) in acc.items():
            out[k] = _wavg(sx, sw)
            out[k + "_weight"] = sw
        return out

    summary = {
        "batch_dir": str(batch_dir),
        "reports": int(len(reports)),
        "days": int(total_days),
        "total_games": int(total_games),
        "starter_sources": {
            "counts": starter_source_counts,
            "shares": _shares(starter_source_counts),
            "total_starters": int(sum(int(v) for v in starter_source_counts.values() if isinstance(v, int))),
        },
        "full_weighted": finalize(full_acc),
        "first5_weighted": finalize(f5_acc),
        "first3_weighted": finalize(f3_acc),
        "pitcher_props_at_market_lines_weighted": finalize(props_acc),
        "hitter_hr_likelihood_topn_weighted": finalize(hitter_hr_acc),
        "hitter_props_likelihood_topn_weighted": finalize(hitter_props_acc),
        "per_day": per_day,
    }

    out_path = str(args.out or "").strip()
    if out_path:
        _write_json(Path(out_path), summary)
        print(f"Wrote: {out_path}")
    else:
        # Always persist a summary.json into the batch folder for downstream tooling,
        # while preserving the original behavior of printing JSON to stdout.
        _write_json(batch_dir / "summary.json", summary)
        print(json.dumps(summary, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
