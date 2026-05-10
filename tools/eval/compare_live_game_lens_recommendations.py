from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        number = float(value)
    except Exception:
        return None
    return number if number == number else None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _american_profit(odds: Any) -> Optional[float]:
    value = _safe_int(odds)
    if value is None or value == 0:
        return None
    if value > 0:
        return float(value) / 100.0
    return 100.0 / abs(float(value))


def _row_key(row: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("date") or ""),
            str(row.get("gamePk") or ""),
            str(row.get("segment") or ""),
            str(row.get("lane") or ""),
        ]
    )


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date": str(row.get("date") or ""),
        "gamePk": _safe_int(row.get("gamePk")),
        "matchup": str(row.get("matchup") or ""),
        "segment": str(row.get("segment") or ""),
        "lane": str(row.get("lane") or ""),
        "pick": str(row.get("pick") or ""),
        "line": _safe_float(row.get("line")),
        "odds": _safe_int(row.get("odds")),
        "win": row.get("win") if row.get("win") in {True, False} else None,
        "edge": _safe_float(row.get("edge")),
    }


def _rows_map(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized = _normalize_row(row)
        out[_row_key(normalized)] = normalized
    return out


def _metric_delta(left: Dict[str, Any], right: Dict[str, Any], key: str) -> Optional[float]:
    lhs = _safe_float(left.get(key))
    rhs = _safe_float(right.get(key))
    if lhs is None or rhs is None:
        return None
    return round(float(rhs) - float(lhs), 4)


def _summary_delta(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "n_delta": (_safe_int(right.get("n")) or 0) - (_safe_int(left.get("n")) or 0),
        "wins_delta": (_safe_int(right.get("wins")) or 0) - (_safe_int(left.get("wins")) or 0),
        "win_rate_delta": _metric_delta(left, right, "win_rate"),
        "roi_delta": _metric_delta(left, right, "roi"),
        "avg_edge_delta": _metric_delta(left, right, "avg_edge"),
    }


def _block_delta(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    keys = sorted({str(key) for key in left.keys()} | {str(key) for key in right.keys()})
    return {
        key: _summary_delta(
            left.get(key) if isinstance(left.get(key), dict) else {},
            right.get(key) if isinstance(right.get(key), dict) else {},
        )
        for key in keys
    }


def _shared_support_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    settled = [row for row in rows if row.get("win") in {True, False} and row.get("odds") is not None]
    n = len(settled)
    wins = sum(1 for row in settled if bool(row.get("win")))
    profit = 0.0
    for row in settled:
        if bool(row.get("win")):
            profit += float(_american_profit(row.get("odds")) or 0.0)
        else:
            profit -= 1.0
    avg_edge_values = [float(row.get("edge")) for row in settled if _safe_float(row.get("edge")) is not None]
    return {
        "n": n,
        "wins": wins,
        "win_rate": round(wins / n, 4) if n else None,
        "roi": round(profit / n, 4) if n else None,
        "avg_edge": round(sum(avg_edge_values) / len(avg_edge_values), 4) if avg_edge_values else None,
    }


def _churn_summary(left_rows: Dict[str, Dict[str, Any]], right_rows: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    left_keys = set(left_rows.keys())
    right_keys = set(right_rows.keys())
    shared_keys = sorted(left_keys & right_keys)
    added_keys = sorted(right_keys - left_keys)
    removed_keys = sorted(left_keys - right_keys)

    changed_pick = 0
    changed_line = 0
    changed_odds = 0
    changed_any = 0
    flipped_outcome = 0
    shared_left: List[Dict[str, Any]] = []
    shared_right: List[Dict[str, Any]] = []
    changed_examples: List[Dict[str, Any]] = []

    for key in shared_keys:
        left = left_rows[key]
        right = right_rows[key]
        shared_left.append(left)
        shared_right.append(right)
        pick_changed = str(left.get("pick") or "") != str(right.get("pick") or "")
        line_changed = _safe_float(left.get("line")) != _safe_float(right.get("line"))
        odds_changed = _safe_int(left.get("odds")) != _safe_int(right.get("odds"))
        any_changed = pick_changed or line_changed or odds_changed
        if pick_changed:
            changed_pick += 1
        if line_changed:
            changed_line += 1
        if odds_changed:
            changed_odds += 1
        if any_changed:
            changed_any += 1
            if len(changed_examples) < 25:
                changed_examples.append(
                    {
                        "key": key,
                        "matchup": right.get("matchup") or left.get("matchup"),
                        "left": left,
                        "right": right,
                    }
                )
        if left.get("win") in {True, False} and right.get("win") in {True, False} and bool(left.get("win")) != bool(right.get("win")):
            flipped_outcome += 1

    return {
        "left_only": len(removed_keys),
        "right_only": len(added_keys),
        "shared": len(shared_keys),
        "changed_any": changed_any,
        "changed_pick": changed_pick,
        "changed_line": changed_line,
        "changed_odds": changed_odds,
        "flipped_outcome": flipped_outcome,
        "changed_any_rate": round(changed_any / len(shared_keys), 4) if shared_keys else None,
        "changed_pick_rate": round(changed_pick / len(shared_keys), 4) if shared_keys else None,
        "outcome_flip_rate": round(flipped_outcome / len(shared_keys), 4) if shared_keys else None,
        "shared_left_summary": _shared_support_summary(shared_left),
        "shared_right_summary": _shared_support_summary(shared_right),
        "shared_delta": _summary_delta(_shared_support_summary(shared_left), _shared_support_summary(shared_right)),
        "changed_examples": changed_examples,
    }


def _churn_by_field(left_rows: Dict[str, Dict[str, Any]], right_rows: Dict[str, Dict[str, Any]], field: str) -> Dict[str, Dict[str, Any]]:
    grouped_left: Dict[str, Dict[str, Dict[str, Any]]] = {}
    grouped_right: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for key, row in left_rows.items():
        grouped_left.setdefault(str(row.get(field) or ""), {})[key] = row
    for key, row in right_rows.items():
        grouped_right.setdefault(str(row.get(field) or ""), {})[key] = row
    out: Dict[str, Dict[str, Any]] = {}
    for group_key in sorted(set(grouped_left.keys()) | set(grouped_right.keys())):
        if not group_key:
            continue
        out[group_key] = _churn_summary(grouped_left.get(group_key, {}), grouped_right.get(group_key, {}))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two settled live game-lens evaluation artifacts.")
    parser.add_argument("--left", required=True, help="Baseline settled game-lens evaluation JSON")
    parser.add_argument("--right", required=True, help="Candidate settled game-lens evaluation JSON")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    left_path = Path(str(args.left)).resolve()
    right_path = Path(str(args.right)).resolve()
    left = _read_json(left_path)
    right = _read_json(right_path)
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise SystemExit("Both inputs must be JSON objects.")

    left_rows = _rows_map(left.get("rows") or [])
    right_rows = _rows_map(right.get("rows") or [])

    payload = {
        "left": str(left_path),
        "right": str(right_path),
        "left_summary": dict(left.get("summary") or {}),
        "right_summary": dict(right.get("summary") or {}),
        "summary_delta": _summary_delta(dict(left.get("summary") or {}), dict(right.get("summary") or {})),
        "by_segment_delta": _block_delta(dict(left.get("by_segment") or {}), dict(right.get("by_segment") or {})),
        "by_lane_delta": _block_delta(dict(left.get("by_lane") or {}), dict(right.get("by_lane") or {})),
        "by_date_delta": _block_delta(dict(left.get("by_date") or {}), dict(right.get("by_date") or {})),
        "churn": _churn_summary(left_rows, right_rows),
        "churn_by_segment": _churn_by_field(left_rows, right_rows, "segment"),
        "churn_by_lane": _churn_by_field(left_rows, right_rows, "lane"),
    }

    out_path = Path(str(args.out)).resolve() if str(args.out or "").strip() else None
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "summary_delta": payload["summary_delta"],
                "churn": {
                    key: value
                    for key, value in dict(payload["churn"] or {}).items()
                    if key != "changed_examples"
                },
                "out": str(out_path) if out_path is not None else None,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()