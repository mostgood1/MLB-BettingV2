from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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
        return int(float(value))
    except Exception:
        return None


def _safe_ratio(numerator: Any, denominator: Any) -> Optional[float]:
    num = _safe_float(numerator)
    den = _safe_float(denominator)
    if num is None or den is None or float(den) <= 0.0:
        return None
    return float(num) / float(den)


def _mean(values: Sequence[float]) -> Optional[float]:
    vals = [float(v) for v in values]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def _rmse(values: Sequence[float]) -> Optional[float]:
    vals = [float(v) for v in values]
    if not vals:
        return None
    return float(math.sqrt(sum(v * v for v in vals) / len(vals)))


def _median(values: Sequence[float]) -> Optional[float]:
    vals = sorted(float(v) for v in values)
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return float(vals[mid])
    return float((vals[mid - 1] + vals[mid]) / 2.0)


def _bucket_projected_outs(value: Any) -> str:
    outs = _safe_float(value)
    if outs is None:
        return "unknown"
    if outs < 12.0:
        return "outs_lt_12"
    if outs < 15.0:
        return "outs_12_14"
    if outs < 18.0:
        return "outs_15_17"
    if outs < 21.0:
        return "outs_18_20"
    return "outs_21_plus"


def _bucket_projected_pitches(value: Any) -> str:
    pitches = _safe_float(value)
    if pitches is None:
        return "unknown"
    if pitches < 65.0:
        return "pitches_lt_65"
    if pitches < 80.0:
        return "pitches_65_79"
    if pitches < 95.0:
        return "pitches_80_94"
    return "pitches_95_plus"


def _bucket_projected_efficiency(row: Dict[str, Any]) -> str:
    outs = _safe_float(row.get("pred_outs_mean"))
    pitches = _safe_float(row.get("pred_pitches_mean"))
    if outs is None or pitches is None or outs <= 0.0:
        return "unknown"
    per_out = float(pitches) / float(outs)
    if per_out < 4.0:
        return "efficient"
    if per_out <= 4.8:
        return "neutral"
    return "stress"


def _compact_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date": row.get("date"),
        "game_pk": row.get("game_pk"),
        "team": row.get("team"),
        "starter_id": row.get("starter_id"),
        "simulated_starter_id": row.get("simulated_starter_id"),
        "starter_mismatch": bool(row.get("starter_mismatch")),
        "starter_name": row.get("starter_name"),
        "pred_outs_mean": row.get("pred_outs_mean"),
        "actual_outs": row.get("actual_outs"),
        "outs_error": row.get("outs_error"),
        "pred_pitches_mean": row.get("pred_pitches_mean"),
        "actual_pitches": row.get("actual_pitches"),
        "pitches_error": row.get("pitches_error"),
        "pred_pitches_per_out": row.get("pred_pitches_per_out"),
        "actual_pitches_per_out": row.get("actual_pitches_per_out"),
        "efficiency_gap": row.get("efficiency_gap"),
        "market_outs_line": row.get("market_outs_line"),
        "market_outs_edge": row.get("market_outs_edge"),
    }


def _metric_summary(rows: Sequence[Dict[str, Any]], pred_key: str, actual_key: str, error_key: str) -> Dict[str, Any]:
    errs = [float(v) for v in (_safe_float(row.get(error_key)) for row in rows) if v is not None]
    abs_errs = [abs(v) for v in errs]
    preds = [float(v) for v in (_safe_float(row.get(pred_key)) for row in rows) if v is not None]
    actuals = [float(v) for v in (_safe_float(row.get(actual_key)) for row in rows) if v is not None]
    return {
        "n": len(errs),
        "pred_mean": round(_mean(preds), 3) if preds else None,
        "actual_mean": round(_mean(actuals), 3) if actuals else None,
        "bias": round(_mean(errs), 3) if errs else None,
        "mae": round(_mean(abs_errs), 3) if abs_errs else None,
        "rmse": round(_rmse(errs), 3) if errs else None,
        "median_abs_error": round(_median(abs_errs), 3) if abs_errs else None,
    }


def _group_summary(rows: Sequence[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(key_fn(row) or "unknown"), []).append(row)
    out: List[Dict[str, Any]] = []
    for bucket, bucket_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        out.append(
            {
                "bucket": bucket,
                "rows": len(bucket_rows),
                "outs": _metric_summary(bucket_rows, "pred_outs_mean", "actual_outs", "outs_error"),
                "pitches": _metric_summary(bucket_rows, "pred_pitches_mean", "actual_pitches", "pitches_error"),
            }
        )
    return out


def _efficiency_summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    gaps = [float(v) for v in (_safe_float(row.get("efficiency_gap")) for row in rows) if v is not None]
    pred = [float(v) for v in (_safe_float(row.get("pred_pitches_per_out")) for row in rows) if v is not None]
    actual = [float(v) for v in (_safe_float(row.get("actual_pitches_per_out")) for row in rows) if v is not None]
    abs_gaps = [abs(v) for v in gaps]
    return {
        "n": len(gaps),
        "pred_mean": round(_mean(pred), 3) if pred else None,
        "actual_mean": round(_mean(actual), 3) if actual else None,
        "gap": round(_mean(gaps), 3) if gaps else None,
        "mae": round(_mean(abs_gaps), 3) if abs_gaps else None,
        "rmse": round(_rmse(gaps), 3) if gaps else None,
        "median_abs_gap": round(_median(abs_gaps), 3) if abs_gaps else None,
    }


def _build_rows(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for game in report.get("games") or []:
        game_pk = _safe_int(game.get("game_pk"))
        away = game.get("away") or {}
        home = game.get("home") or {}
        for side in ("away", "home"):
            pp = ((game.get("pitcher_props") or {}).get(side) or {})
            pred = pp.get("pred") or {}
            actual = pp.get("actual") or {}
            if not isinstance(pred, dict) or not isinstance(actual, dict):
                continue
            pred_outs = _safe_float(pred.get("outs_mean"))
            actual_outs = _safe_float(actual.get("outs"))
            pred_pitches = _safe_float(pred.get("pitches_mean"))
            actual_pitches = _safe_float(actual.get("pitches"))
            if pred_outs is None or actual_outs is None:
                continue
            team = away if side == "away" else home
            market = pp.get("market") or {}
            outs_market = (market.get("outs") or {}) if isinstance(market, dict) else {}
            row = {
                "date": report.get("date"),
                "game_pk": game_pk,
                "side": side,
                "team": team.get("abbr") or team.get("name"),
                "starter_id": _safe_int(pp.get("starter_id")),
                "simulated_starter_id": _safe_int(pp.get("simulated_starter_id")),
                "starter_mismatch": bool(pp.get("starter_mismatch")),
                "starter_name": pp.get("starter_name") or pred.get("starter_name") or actual.get("starter_name"),
                "pred_outs_mean": pred_outs,
                "actual_outs": actual_outs,
                "outs_error": float(pred_outs) - float(actual_outs),
                "pred_pitches_mean": pred_pitches,
                "actual_pitches": actual_pitches,
                "pitches_error": (None if pred_pitches is None or actual_pitches is None else float(pred_pitches) - float(actual_pitches)),
                "pred_pitches_per_out": _safe_ratio(pred_pitches, pred_outs),
                "actual_pitches_per_out": _safe_ratio(actual_pitches, actual_outs),
                "efficiency_gap": (
                    None
                    if _safe_ratio(pred_pitches, pred_outs) is None or _safe_ratio(actual_pitches, actual_outs) is None
                    else float(_safe_ratio(pred_pitches, pred_outs) or 0.0) - float(_safe_ratio(actual_pitches, actual_outs) or 0.0)
                ),
                "market_outs_line": _safe_float(outs_market.get("line")),
                "market_outs_edge": _safe_float(outs_market.get("edge")),
            }
            rows.append(row)
    return rows


def _build_starter_resolution_summary(report: Dict[str, Any], rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    mismatch_rows = [row for row in rows if bool(row.get("starter_mismatch"))]
    mismatch_examples: List[Dict[str, Any]] = []
    for game in report.get("games") or []:
        game_pk = _safe_int(game.get("game_pk"))
        for side in ("away", "home"):
            pp = ((game.get("pitcher_props") or {}).get(side) or {})
            if not bool(pp.get("starter_mismatch")):
                continue
            team = ((game.get(side) or {}).get("abbr") or (game.get(side) or {}).get("name"))
            mismatch_examples.append(
                {
                    "game_pk": game_pk,
                    "side": side,
                    "team": team,
                    "starter_id": _safe_int(pp.get("starter_id")),
                    "simulated_starter_id": _safe_int(pp.get("simulated_starter_id")),
                    "starter_name": pp.get("starter_name"),
                }
            )
    return {
        "rows_with_scored_preds": len(rows),
        "starter_mismatch_rows": len(mismatch_rows),
        "starter_mismatch_examples": mismatch_examples[:8],
    }


def _build_summary(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    worst_outs = sorted(
        [row for row in rows if _safe_float(row.get("outs_error")) is not None],
        key=lambda row: abs(float(row.get("outs_error") or 0.0)),
        reverse=True,
    )[:12]
    worst_pitches = sorted(
        [row for row in rows if _safe_float(row.get("pitches_error")) is not None],
        key=lambda row: abs(float(row.get("pitches_error") or 0.0)),
        reverse=True,
    )[:12]
    worst_efficiency = sorted(
        [row for row in rows if _safe_float(row.get("efficiency_gap")) is not None],
        key=lambda row: abs(float(row.get("efficiency_gap") or 0.0)),
        reverse=True,
    )[:12]
    return {
        "rows": len(rows),
        "overall": {
            "outs": _metric_summary(rows, "pred_outs_mean", "actual_outs", "outs_error"),
            "pitches": _metric_summary(rows, "pred_pitches_mean", "actual_pitches", "pitches_error"),
            "efficiency": _efficiency_summary(rows),
        },
        "by_projected_outs": _group_summary(rows, lambda row: _bucket_projected_outs(row.get("pred_outs_mean"))),
        "by_projected_pitches": _group_summary(rows, lambda row: _bucket_projected_pitches(row.get("pred_pitches_mean"))),
        "by_projected_efficiency": _group_summary(rows, _bucket_projected_efficiency),
        "examples": {
            "largest_outs_misses": [_compact_row(row) for row in worst_outs],
            "largest_pitch_misses": [_compact_row(row) for row in worst_pitches],
            "largest_efficiency_gaps": [_compact_row(row) for row in worst_efficiency],
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate pitcher outs and pitch-count accuracy from a sim-vs-actual report.")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--report", default="", help="Optional sim-vs-actual JSON path.")
    ap.add_argument("--out", default="", help="Optional output JSON path.")
    args = ap.parse_args()

    report_path = Path(args.report) if str(args.report).strip() else (_ROOT / "data" / "eval" / f"sim_vs_actual_{args.date}.json")
    if not report_path.exists():
        raise FileNotFoundError(f"sim-vs-actual report not found: {report_path}")

    report = _load_json(report_path)
    rows = _build_rows(report)
    summary = {
        "date": str(args.date),
        "report_path": str(report_path),
        "starter_resolution": _build_starter_resolution_summary(report, rows),
        **_build_summary(rows),
    }

    out_path = Path(args.out) if str(args.out).strip() else (_ROOT / "data" / "eval" / f"pitcher_outs_accuracy_{args.date}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())