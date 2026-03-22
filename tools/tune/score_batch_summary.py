from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _get(obj: Dict[str, Any], dotted: str) -> Any:
    cur: Any = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def _load_objective(path: Path) -> Dict[str, Any]:
    obj = _read_json(path)
    if not isinstance(obj, dict):
        raise ValueError("Objective file must be a JSON dict")
    if not isinstance(obj.get("metrics"), list):
        raise ValueError("Objective file must contain metrics: []")
    return obj


def _score_one(
    candidate: Dict[str, Any],
    baseline: Dict[str, Any],
    metrics: List[Dict[str, Any]],
) -> Tuple[float, Dict[str, Any]]:
    used_w = 0.0
    score_sum = 0.0
    rows: List[Dict[str, Any]] = []

    for m in metrics:
        path = str(m.get("path") or "").strip()
        direction = str(m.get("direction") or "lower").strip().lower()
        w = float(m.get("weight") or 0.0)
        if not path or w <= 0:
            continue

        c = _get(candidate, path)
        b = _get(baseline, path)
        if not _is_num(c) or not _is_num(b):
            rows.append({"path": path, "weight": w, "direction": direction, "status": "missing"})
            continue

        c = float(c)
        b = float(b)
        term: Optional[float]

        if direction == "lower":
            # Lower is better => improvement should yield term > 1.0
            term = b / c if c != 0 else None
        elif direction == "higher":
            # Higher is better => improvement should yield term > 1.0
            term = c / b if b != 0 else None
        elif direction == "lower_abs":
            # Smaller absolute value is better => improvement should yield term > 1.0
            term = abs(b) / abs(c) if c != 0 else None
        else:
            term = b / c if c != 0 else None

        if term is None or not math.isfinite(term):
            rows.append({"path": path, "weight": w, "direction": direction, "candidate": c, "baseline": b, "status": "bad_term"})
            continue

        used_w += w
        score_sum += w * term
        rows.append(
            {
                "path": path,
                "weight": w,
                "direction": direction,
                "candidate": c,
                "baseline": b,
                "term": term,
                "weighted": w * term,
                "status": "ok",
            }
        )

    score = score_sum / used_w if used_w > 0 else float("nan")
    detail = {
        "score": score,
        "weights_used": used_w,
        "metrics": rows,
    }
    return score, detail


def main() -> int:
    ap = argparse.ArgumentParser(description="Score a batch summary against a baseline using a multi-metric objective")
    ap.add_argument("--candidate-summary", required=True, help="Path to candidate summary.json")
    ap.add_argument("--objective", required=True, help="Objective JSON (see data/tuning/objectives)")
    ap.add_argument("--baseline-summary", default="", help="Optional override baseline summary.json")
    ap.add_argument("--out", default="", help="Optional output JSON path")
    args = ap.parse_args()

    cand_path = Path(args.candidate_summary)
    obj_path = Path(args.objective)

    objective = _load_objective(obj_path)
    baseline_path = Path(args.baseline_summary) if str(args.baseline_summary).strip() else Path(str(objective.get("baseline_summary") or ""))

    if not cand_path.exists():
        print(f"Missing candidate summary: {cand_path}")
        return 2
    if not baseline_path.exists():
        print(f"Missing baseline summary: {baseline_path}")
        return 2

    candidate = _read_json(cand_path)
    baseline = _read_json(baseline_path)
    if not isinstance(candidate, dict) or not isinstance(baseline, dict):
        print("Both candidate and baseline summaries must be JSON objects")
        return 2

    score, detail = _score_one(candidate, baseline, list(objective.get("metrics") or []))

    out_obj = {
        "objective": objective.get("name") or obj_path.name,
        "candidate_summary": str(cand_path),
        "baseline_summary": str(baseline_path),
        "score": score,
        "detail": detail,
    }

    out_path = str(args.out or "").strip()
    if out_path:
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(out_obj, indent=2), encoding="utf-8")
        print(f"Wrote: {p}")
    else:
        print(json.dumps(out_obj, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
