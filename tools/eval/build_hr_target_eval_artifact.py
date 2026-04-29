from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT_ENV = str(os.environ.get("MLB_BETTING_DATA_ROOT") or "").strip()
DATA_ROOT = Path(DATA_ROOT_ENV).resolve() if DATA_ROOT_ENV else None
TRACKED_DATA_ROOT = (REPO_ROOT / "data").resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.eval.settle_locked_policy_cards import _feed_is_final, _load_feed, _player_stats


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _relative_path_str(path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(REPO_ROOT)).replace("\\", "/")
    except Exception:
        return str(path.resolve()).replace("\\", "/")


def _resolve_path(value: str) -> Path:
    path = Path(str(value or "").strip())
    if not path.is_absolute():
        parts = path.parts
        if DATA_ROOT is not None and parts and str(parts[0]).lower() == "data":
            path = DATA_ROOT.joinpath(*parts[1:])
        else:
            path = REPO_ROOT / path
    return path.resolve()


def _hr_target_candidates(date: str) -> List[Path]:
    slug = str(date or "").strip().replace("-", "_")
    if not slug:
        return []
    candidates: List[Path] = []
    data_roots = [root for root in (DATA_ROOT, TRACKED_DATA_ROOT) if isinstance(root, Path)]
    for data_root in data_roots:
        candidates.extend(
            [
                (data_root / "daily" / f"daily_summary_{slug}_hr_targets.json").resolve(),
                (data_root / "_tmp_live_subcap_random_day" / f"daily_summary_{slug}_hr_targets.json").resolve(),
                (data_root / "_tmp_live_subcap_smoke" / f"daily_summary_{slug}_hr_targets.json").resolve(),
            ]
        )
    seen = set()
    out: List[Path] = []
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        if path.exists() and path.is_file():
            out.append(path)
    if out:
        return out
    for data_root in data_roots:
        pattern = f"**/daily_summary_{slug}_hr_targets.json"
        for path in sorted(data_root.glob(pattern)):
            resolved = path.resolve()
            if resolved in seen or not resolved.is_file():
                continue
            seen.add(resolved)
            out.append(resolved)
    return out


def _daily_subdir_candidates(subdir: str, date: str) -> List[Path]:
    candidates: List[Path] = []
    data_roots = [root for root in (DATA_ROOT, TRACKED_DATA_ROOT) if isinstance(root, Path)]
    for data_root in data_roots:
        for top_level in ("daily_hitter_props", "daily"):
            candidate = (data_root / str(top_level) / str(subdir) / str(date)).resolve()
            if candidate.exists() and candidate.is_dir():
                candidates.append(candidate)
    seen = set()
    out: List[Path] = []
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        out.append(path)
    return out


def _rebuild_hr_targets_doc_if_needed(doc: Dict[str, Any], *, artifact_path: Path, date: str, season: int) -> Dict[str, Any]:
    diagnostics = doc.get("diagnostics") or {}
    excluded_rows = diagnostics.get("excluded_rows") if isinstance(diagnostics, dict) else None
    if isinstance(excluded_rows, list) and excluded_rows:
        return doc

    sim_candidates = _daily_subdir_candidates("sims", str(date))
    snapshot_candidates = _daily_subdir_candidates("snapshots", str(date))
    if not sim_candidates:
        return doc

    try:
        from tools.daily_update_multi_profile import _collect_daily_hr_targets

        rebuilt = _collect_daily_hr_targets(
            sim_candidates[0],
            snapshot_candidates[0] if snapshot_candidates else None,
            date=str(date),
            season=int(season),
        )
    except Exception:
        return doc
    if not isinstance(rebuilt, dict):
        return doc
    rebuilt_source = str(rebuilt.get("source_sim_dir") or "").replace("\\", "/").lower()
    rebuilt["source_profile"] = (
        "hitter_props_recos"
        if "/daily_hitter_props/" in rebuilt_source or rebuilt_source.startswith("data/daily_hitter_props/")
        else doc.get("source_profile")
    )
    rebuilt["source_artifact"] = _relative_path_str(artifact_path)
    return rebuilt


def _team_side(feed: Dict[str, Any], team_abbr: str) -> Optional[str]:
    teams = (feed.get("gameData") or {}).get("teams") or {}
    away_abbr = str(((teams.get("away") or {}).get("abbreviation") or "")).upper()
    home_abbr = str(((teams.get("home") or {}).get("abbreviation") or "")).upper()
    target = str(team_abbr or "").upper()
    if target == away_abbr:
        return "away"
    if target == home_abbr:
        return "home"
    return None


def _dedupe_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        key = (
            _safe_int(row.get("game_pk")),
            _safe_int(row.get("batter_id")),
            str(row.get("player_name") or "").strip().lower(),
            str(row.get("selection_status") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _settle_row(
    row: Dict[str, Any],
    *,
    date: str,
    feed_cache: Dict[int, Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    out = dict(row)
    game_pk = _safe_int(row.get("game_pk"))
    player_name = str(row.get("player_name") or "").strip()
    if game_pk is None or not player_name:
        out["settlement_status"] = "unavailable"
        out["settled"] = False
        out["actual_hr"] = None
        out["y_hr_1plus"] = None
        out["result"] = "unavailable"
        return out

    if int(game_pk) not in feed_cache:
        try:
            feed_cache[int(game_pk)] = _load_feed(str(date), int(game_pk))
        except Exception:
            feed_cache[int(game_pk)] = None
    feed = feed_cache.get(int(game_pk))
    if not isinstance(feed, dict):
        out["settlement_status"] = "unavailable"
        out["settled"] = False
        out["actual_hr"] = None
        out["y_hr_1plus"] = None
        out["result"] = "unavailable"
        return out

    if not _feed_is_final(feed):
        out["settlement_status"] = "pending"
        out["settled"] = False
        out["actual_hr"] = None
        out["y_hr_1plus"] = None
        out["result"] = "pending"
        return out

    side = str(row.get("team_side") or "").strip().lower()
    if side not in {"away", "home"}:
        side = str(_team_side(feed, str(row.get("team") or "")) or "")
    if side not in {"away", "home"}:
        out["settlement_status"] = "unavailable"
        out["settled"] = False
        out["actual_hr"] = None
        out["y_hr_1plus"] = None
        out["result"] = "unavailable"
        return out

    stats = _player_stats(feed, side, player_name, "batting")
    actual_hr = _safe_int((stats or {}).get("homeRuns"))
    if actual_hr is None:
        out["settlement_status"] = "unavailable"
        out["settled"] = False
        out["actual_hr"] = None
        out["y_hr_1plus"] = None
        out["result"] = "unavailable"
        return out

    out["settlement_status"] = "final"
    out["settled"] = True
    out["actual_hr"] = int(actual_hr)
    out["y_hr_1plus"] = 1 if int(actual_hr) >= 1 else 0
    out["result"] = "win" if int(actual_hr) >= 1 else "loss"
    return out


def build_artifact(*, date: str, season: int, input_path: Optional[str] = None) -> Dict[str, Any]:
    if input_path:
        artifact_path = _resolve_path(input_path)
    else:
        matches = _hr_target_candidates(date)
        if not matches:
            raise FileNotFoundError(f"No HR targets artifact found for {date}")
        artifact_path = sorted(matches, key=lambda path: (path.stat().st_mtime, str(path)), reverse=True)[0]

    doc = _read_json(artifact_path)
    if not isinstance(doc, dict):
        raise ValueError("HR targets artifact must be a JSON object")
    doc = _rebuild_hr_targets_doc_if_needed(doc, artifact_path=artifact_path, date=str(date), season=int(season))

    selected_rows: List[Dict[str, Any]] = []
    raw_rows = doc.get("rows") or []
    if isinstance(raw_rows, list):
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            selected_rows.append(
                {
                    **dict(row),
                    "selection_status": "selected",
                    "selection_source": "rows",
                }
            )

    excluded_example_rows: List[Dict[str, Any]] = []
    excluded_rows: List[Dict[str, Any]] = []
    diagnostics = doc.get("diagnostics") or {}
    excluded_values = diagnostics.get("excluded_rows") if isinstance(diagnostics, dict) else []
    if isinstance(excluded_values, list):
        for row in excluded_values:
            if not isinstance(row, dict):
                continue
            excluded_rows.append(
                {
                    **dict(row),
                    "selection_status": "excluded",
                    "selection_source": "diagnostics.excluded_rows",
                }
            )
    examples = diagnostics.get("excluded_examples") if isinstance(diagnostics, dict) else []
    if isinstance(examples, list):
        for row in examples:
            if not isinstance(row, dict):
                continue
            excluded_example_rows.append(
                {
                    **dict(row),
                    "selection_status": "excluded_example",
                    "selection_source": "diagnostics.excluded_examples",
                }
            )

    rows = _dedupe_rows([*selected_rows, *excluded_rows, *excluded_example_rows])
    feed_cache: Dict[int, Optional[Dict[str, Any]]] = {}
    settled_rows = [_settle_row(row, date=str(date), feed_cache=feed_cache) for row in rows]

    selected_settled = [row for row in settled_rows if str(row.get("selection_status")) == "selected" and bool(row.get("settled"))]
    excluded_settled = [
        row for row in settled_rows if str(row.get("selection_status")) in {"excluded", "excluded_example"} and bool(row.get("settled"))
    ]
    excluded_full_settled = [row for row in settled_rows if str(row.get("selection_status")) == "excluded" and bool(row.get("settled"))]
    excluded_example_settled = [row for row in settled_rows if str(row.get("selection_status")) == "excluded_example" and bool(row.get("settled"))]
    result_counts = Counter(str(row.get("result") or "unavailable") for row in selected_settled)
    excluded_result_counts = Counter(str(row.get("result") or "unavailable") for row in excluded_settled)
    excluded_full_result_counts = Counter(str(row.get("result") or "unavailable") for row in excluded_full_settled)
    excluded_example_result_counts = Counter(str(row.get("result") or "unavailable") for row in excluded_example_settled)
    settlement_status_counts = Counter(str(row.get("settlement_status") or "unavailable") for row in settled_rows)
    exclusion_reason_counts = Counter(
        str(row.get("primary_reason") or "")
        for row in settled_rows
        if str(row.get("selection_status") or "") in {"excluded", "excluded_example"} and str(row.get("primary_reason") or "")
    )
    near_threshold_examples = [
        row for row in settled_rows if str(row.get("selection_status") or "") in {"excluded", "excluded_example"} and bool(row.get("near_threshold"))
    ]
    excluded_rows_coverage = "full" if excluded_rows else "examples_only"

    return {
        "date": str(date),
        "season": int(season),
        "generated_at": __import__("datetime").datetime.now().isoformat(),
        "tool": "tools/eval/build_hr_target_eval_artifact.py",
        "source_artifact": _relative_path_str(artifact_path),
        "source_profile": doc.get("source_profile"),
        "coverage": {
            "selected_rows": "full",
            "excluded_rows": excluded_rows_coverage,
        },
        "policy": dict(doc.get("policy") or {}),
        "source_counts": dict(doc.get("counts") or {}),
        "summary": {
            "rows": int(len(settled_rows)),
            "selected_rows": int(len([row for row in settled_rows if str(row.get("selection_status")) == "selected"])),
            "selected_settled_rows": int(len(selected_settled)),
            "selected_wins": int(result_counts.get("win", 0)),
            "selected_losses": int(result_counts.get("loss", 0)),
            "selected_hit_rate": (
                round(float(result_counts.get("win", 0)) / float(len(selected_settled)), 4) if selected_settled else None
            ),
            "excluded_rows": int(len([row for row in settled_rows if str(row.get("selection_status")) == "excluded"])),
            "excluded_settled_rows": int(len(excluded_full_settled)),
            "excluded_wins": int(excluded_full_result_counts.get("win", 0)),
            "excluded_losses": int(excluded_full_result_counts.get("loss", 0)),
            "excluded_hit_rate": (
                round(float(excluded_full_result_counts.get("win", 0)) / float(len(excluded_full_settled)), 4) if excluded_full_settled else None
            ),
            "excluded_example_rows": int(len([row for row in settled_rows if str(row.get("selection_status")) == "excluded_example"])),
            "excluded_example_settled_rows": int(len(excluded_example_settled)),
            "excluded_example_wins": int(excluded_example_result_counts.get("win", 0)),
            "excluded_example_losses": int(excluded_example_result_counts.get("loss", 0)),
            "excluded_example_hit_rate": (
                round(float(excluded_example_result_counts.get("win", 0)) / float(len(excluded_example_settled)), 4) if excluded_example_settled else None
            ),
            "near_threshold_example_rows": int(len(near_threshold_examples)),
            "near_threshold_example_settled_rows": int(len([row for row in near_threshold_examples if bool(row.get("settled"))])),
            "settlement_status_counts": {str(key): int(value) for key, value in sorted(settlement_status_counts.items())},
            "excluded_primary_reason_counts": {str(key): int(value) for key, value in sorted(exclusion_reason_counts.items())},
        },
        "rows": settled_rows,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a persisted HR target evaluation artifact with realized outcomes.")
    ap.add_argument("--date", required=True, help="Slate date in YYYY-MM-DD")
    ap.add_argument("--season", required=True, type=int, help="Season year")
    ap.add_argument("--in", dest="inp", default="", help="Optional HR targets artifact path")
    ap.add_argument(
        "--out",
        default="",
        help="Output path (default: data/eval/hr_targets/<season>/hr_targets_eval_<date>.json)",
    )
    args = ap.parse_args()

    out_path = (
        _resolve_path(args.out)
        if str(args.out or "").strip()
        else (REPO_ROOT / "data" / "eval" / "hr_targets" / str(int(args.season)) / f"hr_targets_eval_{str(args.date).strip()}.json").resolve()
    )
    artifact = build_artifact(date=str(args.date), season=int(args.season), input_path=(str(args.inp).strip() or None))
    _write_json(out_path, artifact)
    print(f"Wrote: {out_path}")
    print(json.dumps(artifact.get("summary") or {}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())