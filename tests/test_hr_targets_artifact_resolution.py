import json
import tempfile
import unittest
from pathlib import Path

from tools.web import flask_frontend as frontend


def _build_doc(rows: int, games: int) -> dict:
    return {
        "counts": {"rows": rows, "games": games},
        "rows": [{"playerName": f"Player {index}"} for index in range(rows)],
        "policy": {"preset": "default"},
        "source_profile": "hitter_props_recos",
    }


class HrTargetsArtifactResolutionTests(unittest.TestCase):
    def test_resolver_promotes_richer_tracked_artifact_into_canonical_data_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            canonical_root = temp_root / "canonical_data"
            tracked_root = temp_root / "tracked_data"
            canonical_path = canonical_root / "daily" / "daily_summary_2026_05_01_hr_targets.json"
            tracked_path = tracked_root / "daily" / "daily_summary_2026_05_01_hr_targets.json"
            canonical_path.parent.mkdir(parents=True, exist_ok=True)
            tracked_path.parent.mkdir(parents=True, exist_ok=True)
            canonical_path.write_text(json.dumps(_build_doc(rows=9, games=9)), encoding="utf-8")
            tracked_path.write_text(json.dumps(_build_doc(rows=30, games=15)), encoding="utf-8")

            original_data_dir = frontend._DATA_DIR
            original_tracked_data_dir = frontend._TRACKED_DATA_DIR
            try:
                frontend._DATA_DIR = canonical_root
                frontend._TRACKED_DATA_DIR = tracked_root

                resolved_path, resolved_doc = frontend._resolve_hr_targets_artifact("2026-05-01")
            finally:
                frontend._DATA_DIR = original_data_dir
                frontend._TRACKED_DATA_DIR = original_tracked_data_dir

            self.assertEqual(resolved_path, canonical_path)
            self.assertIsInstance(resolved_doc, dict)
            self.assertEqual((resolved_doc.get("counts") or {}).get("rows"), 30)

            promoted_doc = json.loads(canonical_path.read_text(encoding="utf-8"))
            self.assertEqual((promoted_doc.get("counts") or {}).get("rows"), 30)
            self.assertEqual((promoted_doc.get("counts") or {}).get("games"), 15)


if __name__ == "__main__":
    unittest.main()