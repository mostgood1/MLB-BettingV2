from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_BOOT_ROOT = Path(__file__).resolve().parents[2]
if str(_BOOT_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOT_ROOT))

from tools.eval.build_season_eval_manifest import _ROOT, build_manifest, write_manifest_artifacts


def _resolve_batch_dir(path_str: str) -> Path:
    path = Path(str(path_str))
    if not path.is_absolute():
        path = (_ROOT / path).resolve()
    return path


def _publish_once(*, season: int, batch_dir: Path, out: str, recap_md: str, title: str, game_types: str) -> dict:
    manifest = build_manifest(
        season=int(season),
        batch_dir=batch_dir,
        title=str(title),
        game_types=str(game_types),
    )
    write_manifest_artifacts(manifest, season=int(season), out=str(out), recap_md=str(recap_md))
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description="Publish a rolling partial season eval manifest while a batch is running")
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--batch-dir", required=True, help="Path to data/eval/batches/<batch>")
    ap.add_argument("--out", default="", help="Output manifest JSON path")
    ap.add_argument("--recap-md", default="", help="Output markdown recap path")
    ap.add_argument("--title", default="", help="Optional season title")
    ap.add_argument("--game-types", default="R", help="Comma-separated schedule game types represented by this manifest")
    ap.add_argument("--poll-seconds", type=float, default=300.0, help="Seconds between partial publishes in watch mode")
    ap.add_argument("--once", action="store_true", help="Publish once and exit")
    args = ap.parse_args()

    batch_dir = _resolve_batch_dir(str(args.batch_dir))
    sleep_seconds = max(5.0, float(args.poll_seconds))

    while True:
        manifest = _publish_once(
            season=int(args.season),
            batch_dir=batch_dir,
            out=str(args.out),
            recap_md=str(args.recap_md),
            title=str(args.title),
            game_types=str(args.game_types),
        )
        overview = manifest.get("overview") or {}
        meta = manifest.get("meta") or {}
        progress = meta.get("progress") or {}
        completed = progress.get("completed_reports") or overview.get("reports") or 0
        expected = progress.get("expected_reports")
        last_date = overview.get("last_date") or "?"
        status = meta.get("status") or "unknown"
        if expected:
            print(f"Published {status} season manifest: {completed}/{expected} reports through {last_date}")
        else:
            print(f"Published {status} season manifest: {completed} reports through {last_date}")
        if args.once or not meta.get("partial"):
            break
        time.sleep(sleep_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())