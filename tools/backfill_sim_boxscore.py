from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.data.roster_artifact import read_game_roster_artifact
from sim_engine.models import GameConfig
from sim_engine.simulate import simulate_game


def _today_iso() -> str:
    return date.today().isoformat()


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _as_boxscore(game_result: Any) -> Dict[str, Any]:
    return {
        "away_score": int(getattr(game_result, "away_score")),
        "home_score": int(getattr(game_result, "home_score")),
        "innings_played": int(getattr(game_result, "innings_played")),
        "away_inning_runs": [int(x) for x in (getattr(game_result, "away_inning_runs", None) or [])],
        "home_inning_runs": [int(x) for x in (getattr(game_result, "home_inning_runs", None) or [])],
        "batter_stats": getattr(game_result, "batter_stats", None),
        "pitcher_stats": getattr(game_result, "pitcher_stats", None),
    }


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _extract_idx_from_sim_filename(p: Path) -> Optional[int]:
    # sim_<idx>_<AWAY>_at_<HOME>_pk<game_pk>_g?.json
    stem = p.stem
    if not stem.startswith("sim_"):
        return None
    rest = stem[len("sim_") :]
    idx_s = rest.split("_", 1)[0]
    try:
        return int(idx_s)
    except Exception:
        return None


def _needs_boxscore(sim_obj: Dict[str, Any]) -> bool:
    pbp = sim_obj.get("pbp")
    if not isinstance(pbp, dict):
        return True
    box = pbp.get("boxscore")
    if not isinstance(box, dict):
        return True
    batter_stats = box.get("batter_stats")
    pitcher_stats = box.get("pitcher_stats")
    if isinstance(batter_stats, dict) or isinstance(pitcher_stats, dict):
        return False
    return True


def _roster_artifact_path_for_sim(sim_path: Path, roster_obj_dir: Path) -> Path:
    # sim_0_DET_at_BAL_pk123_g1.json -> roster_obj_0_DET_at_BAL_pk123_g1.json
    name = sim_path.name
    if not name.startswith("sim_"):
        return roster_obj_dir / name
    return roster_obj_dir / ("roster_obj_" + name[len("sim_") :])


def _build_pbp_obj(*, boxscore: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "pbp_mode": "off",
        "pbp_truncated": False,
        "pbp": [],
        "boxscore": boxscore,
    }


def backfill_date(*, date_iso: str, out_root: Path, seed: int, limit: int = 0, dry_run: bool = False) -> Tuple[int, int]:
    sim_dir = out_root / "sims" / date_iso
    snapshot_dir = out_root / "snapshots" / date_iso
    roster_obj_dir = snapshot_dir / "roster_objs"

    if not sim_dir.exists():
        raise FileNotFoundError(f"sim_dir not found: {sim_dir}")
    if not roster_obj_dir.exists():
        raise FileNotFoundError(f"roster_obj_dir not found: {roster_obj_dir}")

    sim_files = sorted([p for p in sim_dir.glob("sim_*.json") if p.is_file()])
    if limit and limit > 0:
        sim_files = sim_files[: int(limit)]

    updated = 0
    skipped = 0

    for sim_path in sim_files:
        sim_obj = _load_json(sim_path)
        if not _needs_boxscore(sim_obj):
            skipped += 1
            continue

        idx = _extract_idx_from_sim_filename(sim_path)
        if idx is None:
            print(f"SKIP (bad filename idx): {sim_path.name}")
            skipped += 1
            continue

        roster_path = _roster_artifact_path_for_sim(sim_path, roster_obj_dir)
        if not roster_path.exists():
            print(f"SKIP (missing roster artifact): {roster_path.name}")
            skipped += 1
            continue

        rr = read_game_roster_artifact(roster_path)
        away_roster = rr["away"]
        home_roster = rr["home"]

        cfg = GameConfig(rng_seed=(int(seed) + int(idx) * 100000 + 999), pbp="off", pbp_max_events=0)
        r1 = simulate_game(away_roster, home_roster, cfg)

        sim_obj["pbp"] = _build_pbp_obj(boxscore=_as_boxscore(r1))

        if dry_run:
            print(f"DRYRUN update: {sim_path.name}")
        else:
            _write_json_atomic(sim_path, sim_obj)
            print(f"Updated: {sim_path.name}")
        updated += 1

    return updated, skipped


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill pbp.boxscore into sim_*.json files using roster artifacts")
    ap.add_argument("--date", default=_today_iso())
    ap.add_argument("--out", default=str(_ROOT / "data" / "daily"))
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--limit", type=int, default=0, help="If >0, only process the first N sim files")
    ap.add_argument("--dry-run", choices=["on", "off"], default="off")

    args = ap.parse_args()

    updated, skipped = backfill_date(
        date_iso=str(args.date),
        out_root=Path(str(args.out)),
        seed=int(args.seed),
        limit=int(args.limit),
        dry_run=str(args.dry_run).lower() == "on",
    )
    print(f"Done. updated={updated} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
