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

from tools.eval.analyze_locked_policy import DEFAULT_POLICY, _score_game_batch, _score_hitter_batch, _summarize_rows


DEFAULT_TOTALS_THRESHOLDS = (0.4, 0.6, 0.8, 1.0, 1.2, 1.4)
DEFAULT_HITTER_THRESHOLDS = (0.0, 0.02, 0.05, 0.08, 0.1)
DEFAULT_TOP_K = (1, 2)


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


def _resolve_batch_dir(batch_dir_raw: str, season_manifest_raw: str) -> Path:
    if str(batch_dir_raw or "").strip():
        batch_dir = Path(batch_dir_raw)
        if not batch_dir.is_absolute():
            batch_dir = REPO_ROOT / batch_dir
        return batch_dir.resolve()

    manifest_path = Path(season_manifest_raw)
    if not manifest_path.is_absolute():
        manifest_path = REPO_ROOT / manifest_path
    manifest = _read_json(manifest_path.resolve())
    meta = manifest.get("meta") or {}
    batch_dir_value = str(meta.get("batch_dir") or "").strip()
    if not batch_dir_value:
        raise ValueError(f"season manifest missing meta.batch_dir: {manifest_path}")
    batch_dir = Path(batch_dir_value)
    if not batch_dir.is_absolute():
        batch_dir = REPO_ROOT / batch_dir
    return batch_dir.resolve()


def _summarize(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    summary = _summarize_rows(rows)
    summary["avg_edge"] = round(sum(float(row.get("edge") or 0.0) for row in rows) / len(rows), 4) if rows else None
    summary["days"] = len({str(row.get("date") or "") for row in rows}) if rows else 0
    return summary


def _top_k_per_day(rows: Sequence[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get("date") or "")].append(row)

    selected: List[Dict[str, Any]] = []
    for date_key in sorted(buckets):
        group = sorted(
            buckets[date_key],
            key=lambda row: (-float(row.get("edge") or 0.0), float(row.get("profit_u") or 0.0)),
        )
        selected.extend(group[: max(0, int(k))])
    return selected


def _threshold_sweep(rows: Sequence[Dict[str, Any]], thresholds: Sequence[float]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for threshold in thresholds:
        filtered = [row for row in rows if float(row.get("edge") or 0.0) >= float(threshold)]
        out[str(threshold)] = _summarize(filtered)
    return out


def _market_report(rows: Sequence[Dict[str, Any]], thresholds: Sequence[float], top_k_values: Sequence[int]) -> Dict[str, Any]:
    top_k = {str(int(value)): _summarize(_top_k_per_day(rows, int(value))) for value in top_k_values}
    return {
        "all": _summarize(rows),
        "top_k_per_day": top_k,
        "threshold_sweep": _threshold_sweep(rows, thresholds),
    }


def _build_report(batch_dir: Path, totals_thresholds: Sequence[float], hitter_thresholds: Sequence[float], top_k_values: Sequence[int]) -> Dict[str, Any]:
    policy = dict(DEFAULT_POLICY)

    print(f"[inspect] scoring game markets from {batch_dir}", file=sys.stderr, flush=True)
    t0 = time.perf_counter()
    game_rows = _score_game_batch(REPO_ROOT, batch_dir, policy)
    t1 = time.perf_counter()

    print(f"[inspect] scoring hitter markets from {batch_dir}", file=sys.stderr, flush=True)
    hitter_rows = _score_hitter_batch(REPO_ROOT, batch_dir, policy)
    t2 = time.perf_counter()

    totals_rows = [row for row in game_rows if str(row.get("market") or "") == "totals"]
    runs_rows = [row for row in hitter_rows if str(row.get("submarket") or "") == "hitter_runs"]
    rbi_rows = [row for row in hitter_rows if str(row.get("submarket") or "") == "hitter_rbis"]

    return {
        "meta": {
            "batch_dir": str(batch_dir),
            "policy": policy,
            "timing_sec": {
                "game": round(t1 - t0, 2),
                "hitter": round(t2 - t1, 2),
                "total": round(t2 - t0, 2),
            },
            "row_counts": {
                "game_rows": int(len(game_rows)),
                "hitter_rows": int(len(hitter_rows)),
                "totals": int(len(totals_rows)),
                "hitter_runs": int(len(runs_rows)),
                "hitter_rbis": int(len(rbi_rows)),
            },
        },
        "markets": {
            "totals": _market_report(totals_rows, totals_thresholds, top_k_values),
            "hitter_runs": _market_report(runs_rows, hitter_thresholds, top_k_values),
            "hitter_rbis": _market_report(rbi_rows, hitter_thresholds, top_k_values),
        },
    }


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Inspect totals, hitter runs, and hitter RBI market viability from published season batch reports"
    )
    ap.add_argument(
        "--season-manifest",
        default="data/eval/seasons/2025/season_eval_manifest.json",
        help="Season manifest used to discover meta.batch_dir when --batch-dir is omitted",
    )
    ap.add_argument("--batch-dir", default="", help="Optional batch dir override")
    ap.add_argument(
        "--totals-thresholds",
        default=",".join(str(value) for value in DEFAULT_TOTALS_THRESHOLDS),
        help="Comma-separated totals edge thresholds",
    )
    ap.add_argument(
        "--hitter-thresholds",
        default=",".join(str(value) for value in DEFAULT_HITTER_THRESHOLDS),
        help="Comma-separated hitter edge thresholds",
    )
    ap.add_argument(
        "--top-k",
        default=",".join(str(value) for value in DEFAULT_TOP_K),
        help="Comma-separated top-k-per-day slices to summarize",
    )
    ap.add_argument("--out", default="", help="Optional output JSON path")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    batch_dir = _resolve_batch_dir(args.batch_dir, args.season_manifest)
    totals_thresholds = _parse_float_list(args.totals_thresholds, DEFAULT_TOTALS_THRESHOLDS)
    hitter_thresholds = _parse_float_list(args.hitter_thresholds, DEFAULT_HITTER_THRESHOLDS)
    top_k_values = _parse_int_list(args.top_k, DEFAULT_TOP_K)

    report = _build_report(batch_dir, totals_thresholds, hitter_thresholds, top_k_values)
    if str(args.out).strip():
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = REPO_ROOT / out_path
        _write_json(out_path.resolve(), report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())