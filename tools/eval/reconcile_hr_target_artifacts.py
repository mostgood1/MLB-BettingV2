from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.daily_update_multi_profile import (  # noqa: E402
    _collect_daily_hr_targets,
    _hr_target_policy_config,
    _prefer_richer_hr_targets_doc,
    _read_json,
    _write_json,
)


def _date_slug(date_str: str) -> str:
    return str(date_str or "").strip().replace("-", "_")


def _iter_existing_dates(daily_dir: Path, season: int) -> List[str]:
    prefix = f"daily_summary_{int(season)}_"
    dates = set()
    for path in daily_dir.glob(f"{prefix}*_profile_bundle.json"):
        token = path.name[len("daily_summary_") : -len("_profile_bundle.json")]
        if len(token) == 10:
            dates.add(token.replace("_", "-"))
    for path in daily_dir.glob(f"{prefix}*_hr_targets.json"):
        token = path.name[len("daily_summary_") : -len("_hr_targets.json")]
        if len(token) == 10:
            dates.add(token.replace("_", "-"))
    return sorted(dates)


def _resolve_profile_path(root: Path, value: Any) -> Optional[Path]:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def _load_json_doc(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        doc = _read_json(path)
    except Exception:
        return None
    return doc if isinstance(doc, dict) else None


def _doc_rows(doc: Optional[Dict[str, Any]]) -> int:
    return len([row for row in ((doc or {}).get("rows") or []) if isinstance(row, dict)]) if isinstance(doc, dict) else 0


def _doc_games(doc: Optional[Dict[str, Any]]) -> int:
    if not isinstance(doc, dict):
        return 0
    try:
        return int((((doc.get("counts") or {}).get("games") or 0)))
    except Exception:
        return 0


def _reconcile_date(root: Path, season: int, date_str: str, *, write: bool) -> Dict[str, Any]:
    daily_dir = (root / "data" / "daily").resolve()
    slug = _date_slug(date_str)
    artifact_path = daily_dir / f"daily_summary_{slug}_hr_targets.json"
    bundle_path = daily_dir / f"daily_summary_{slug}_profile_bundle.json"

    current_doc = _load_json_doc(artifact_path)
    bundle_doc = _load_json_doc(bundle_path) or {}
    profiles = bundle_doc.get("profiles") if isinstance(bundle_doc.get("profiles"), dict) else {}
    hitter_profile = profiles.get("hitter_props_recos") if isinstance(profiles.get("hitter_props_recos"), dict) else {}

    rebuilt_doc: Optional[Dict[str, Any]] = None
    sim_dir = _resolve_profile_path(root, hitter_profile.get("sim_dir"))
    snapshot_dir = _resolve_profile_path(root, hitter_profile.get("snapshot_dir"))
    if isinstance(sim_dir, Path) and sim_dir.exists() and sim_dir.is_dir():
        rebuilt_doc = _collect_daily_hr_targets(
            sim_dir,
            snapshot_dir if isinstance(snapshot_dir, Path) and snapshot_dir.exists() and snapshot_dir.is_dir() else None,
            date=str(date_str),
            season=int(season),
            hr_target_policy=_hr_target_policy_config("default"),
        )
        rebuilt_doc["source_profile"] = "hitter_props_recos"

    selected_doc = _prefer_richer_hr_targets_doc(current_doc, rebuilt_doc)
    changed = isinstance(selected_doc, dict) and selected_doc is not current_doc
    if changed and write:
        _write_json(artifact_path, selected_doc)
        bundle_doc["hr_targets"] = {
            "artifact_path": f"data/daily/{artifact_path.name}",
            "games": int(_doc_games(selected_doc)),
            "rows": int(_doc_rows(selected_doc)),
            "policy_preset": str(((selected_doc.get("policy") or {}).get("preset") or "default")),
            "error": None,
        }
        _write_json(bundle_path, bundle_doc)

    return {
        "date": str(date_str),
        "artifact_path": str(artifact_path.relative_to(root)).replace("\\", "/"),
        "bundle_path": str(bundle_path.relative_to(root)).replace("\\", "/") if bundle_path.exists() else None,
        "current_rows": int(_doc_rows(current_doc)),
        "current_games": int(_doc_games(current_doc)),
        "current_source": (current_doc or {}).get("source_sim_dir") if isinstance(current_doc, dict) else None,
        "rebuilt_rows": int(_doc_rows(rebuilt_doc)),
        "rebuilt_games": int(_doc_games(rebuilt_doc)),
        "rebuilt_source": (rebuilt_doc or {}).get("source_sim_dir") if isinstance(rebuilt_doc, dict) else None,
        "changed": bool(changed),
        "selected_rows": int(_doc_rows(selected_doc)),
        "selected_games": int(_doc_games(selected_doc)),
        "selected_source": (selected_doc or {}).get("source_sim_dir") if isinstance(selected_doc, dict) else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit and reconcile daily HR target artifacts against hitter-props source sims.")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--start-date", default="")
    ap.add_argument("--end-date", default="")
    ap.add_argument("--write", choices=("on", "off"), default="on")
    ap.add_argument("--report-out", default="")
    args = ap.parse_args()

    daily_dir = (REPO_ROOT / "data" / "daily").resolve()
    dates = _iter_existing_dates(daily_dir, int(args.season))
    if str(args.start_date or "").strip():
        dates = [date for date in dates if str(date) >= str(args.start_date)]
    if str(args.end_date or "").strip():
        dates = [date for date in dates if str(date) <= str(args.end_date)]

    results = [_reconcile_date(REPO_ROOT, int(args.season), date, write=(str(args.write) == "on")) for date in dates]
    changed = [row for row in results if bool(row.get("changed"))]
    summary = {
        "season": int(args.season),
        "dates": results,
        "dates_checked": int(len(results)),
        "dates_changed": int(len(changed)),
        "changed_dates": [str(row.get("date")) for row in changed],
    }
    if str(args.report_out or "").strip():
        report_path = Path(str(args.report_out)).resolve()
        _write_json(report_path, summary)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())