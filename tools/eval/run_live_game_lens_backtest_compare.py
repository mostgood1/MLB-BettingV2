from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tools.eval.analyze_live_game_lens_recommendations import (  # noqa: E402
    _group_summary,
    _iter_settled_rows,
    _summarize,
)
from tools.eval.compare_live_game_lens_recommendations import (  # noqa: E402
    _block_delta,
    _churn_by_field,
    _churn_summary,
    _rows_map,
    _summary_delta,
)


def _build_eval_payload(live_lens_dir: Path, *, source: str, min_date: str, max_date: str) -> Dict[str, Any]:
    rows = _iter_settled_rows(
        live_lens_dir,
        use_render_sync=(str(source) == "render_sync"),
        min_date=str(min_date or ""),
        max_date=str(max_date or ""),
    )
    return {
        "source": str(source),
        "live_lens_dir": str(live_lens_dir),
        "min_date": str(min_date or ""),
        "max_date": str(max_date or ""),
        "summary": _summarize(rows),
        "by_date": _group_summary(rows, "date"),
        "by_segment": _group_summary(rows, "segment"),
        "by_lane": _group_summary(rows, "lane"),
        "rows": rows,
    }


def _build_compare_payload(left: Dict[str, Any], right: Dict[str, Any], *, left_label: str, right_label: str) -> Dict[str, Any]:
    left_rows = _rows_map(left.get("rows") or [])
    right_rows = _rows_map(right.get("rows") or [])
    return {
        "left": str(left_label),
        "right": str(right_label),
        "left_summary": dict(left.get("summary") or {}),
        "right_summary": dict(right.get("summary") or {}),
        "summary_delta": _summary_delta(dict(left.get("summary") or {}), dict(right.get("summary") or {})),
        "by_date_delta": _block_delta(dict(left.get("by_date") or {}), dict(right.get("by_date") or {})),
        "by_segment_delta": _block_delta(dict(left.get("by_segment") or {}), dict(right.get("by_segment") or {})),
        "by_lane_delta": _block_delta(dict(left.get("by_lane") or {}), dict(right.get("by_lane") or {})),
        "churn": _churn_summary(left_rows, right_rows),
        "churn_by_segment": _churn_by_field(left_rows, right_rows, "segment"),
        "churn_by_lane": _churn_by_field(left_rows, right_rows, "lane"),
    }


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Settle and compare live game-lens recommendation performance for baseline and candidate live-lens roots.")
    parser.add_argument("--left-live-lens-dir", required=True)
    parser.add_argument("--right-live-lens-dir", required=True)
    parser.add_argument("--left-source", choices=("report", "render_sync"), default="report")
    parser.add_argument("--right-source", choices=("report", "render_sync"), default="report")
    parser.add_argument("--min-date", default="")
    parser.add_argument("--max-date", default="")
    parser.add_argument("--out-dir", default="data/eval/live_game_lens_compare")
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    left_dir = (REPO_ROOT / str(args.left_live_lens_dir)).resolve()
    right_dir = (REPO_ROOT / str(args.right_live_lens_dir)).resolve()
    out_dir = (REPO_ROOT / str(args.out_dir)).resolve()
    label = str(args.label or "").strip()
    slug = label if label else f"{str(args.min_date or 'min').replace('-', '_')}__{str(args.max_date or 'max').replace('-', '_')}"

    left_payload = _build_eval_payload(left_dir, source=str(args.left_source), min_date=str(args.min_date or ""), max_date=str(args.max_date or ""))
    right_payload = _build_eval_payload(right_dir, source=str(args.right_source), min_date=str(args.min_date or ""), max_date=str(args.max_date or ""))
    compare_payload = _build_compare_payload(left_payload, right_payload, left_label=str(left_dir), right_label=str(right_dir))

    left_out = out_dir / f"baseline_{slug}.json"
    right_out = out_dir / f"candidate_{slug}.json"
    compare_out = out_dir / f"compare_{slug}.json"
    _write_json(left_out, left_payload)
    _write_json(right_out, right_payload)
    _write_json(compare_out, compare_payload)

    print(
        json.dumps(
            {
                "baseline_out": str(left_out),
                "candidate_out": str(right_out),
                "compare_out": str(compare_out),
                "baseline_summary": left_payload.get("summary"),
                "candidate_summary": right_payload.get("summary"),
                "summary_delta": compare_payload.get("summary_delta"),
                "churn": {
                    key: value
                    for key, value in dict(compare_payload.get("churn") or {}).items()
                    if key != "changed_examples"
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()