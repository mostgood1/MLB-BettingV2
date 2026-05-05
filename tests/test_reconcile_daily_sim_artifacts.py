import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from tools.eval import reconcile_daily_sim_artifacts as reconcile


class ReconcileDailySimArtifactsTests(unittest.TestCase):
    def test_main_skips_missing_feed_live_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sim_dir = Path(tmpdir) / "sims"
            sim_dir.mkdir(parents=True, exist_ok=True)
            sim_path_missing = sim_dir / "sim_missing.json"
            sim_path_valid = sim_dir / "sim_valid.json"
            sim_path_missing.write_text("{}", encoding="utf-8")
            sim_path_valid.write_text("{}", encoding="utf-8")
            out_path = Path(tmpdir) / "report.json"

            args = Namespace(
                date="2026-05-04",
                season=2026,
                sim_dir=str(sim_dir),
                out=str(out_path),
                prop_lines_source="auto",
                market_push_policy="skip",
                so_prob_calibration="off",
                outs_prob_calibration="off",
                hitter_hr_prob_calibration="off",
                hitter_props_prob_calibration="off",
            )
            written = {}

            def fake_read_json(path: Path):
                if path == sim_path_missing:
                    return {"game_pk": 111, "sim": {"sims": 100}}
                if path == sim_path_valid:
                    return {"game_pk": 222, "sim": {"sims": 100}}
                raise AssertionError(f"Unexpected path: {path}")

            def fake_write_json(path: Path, obj):
                written["path"] = path
                written["report"] = obj

            with patch.object(reconcile, "_parse_args", return_value=args), \
                 patch.object(reconcile, "_read_json", side_effect=fake_read_json), \
                 patch.object(reconcile, "_write_json", side_effect=fake_write_json), \
                 patch.object(reconcile, "load_pitcher_prop_lines", return_value=({}, {"source": "auto", "path": None, "pitchers": 0})), \
                 patch.object(reconcile, "load_feed_live_from_raw", side_effect=[None, {"liveData": {}}]), \
                 patch.object(reconcile, "_build_game_row", return_value={"segments": {}}), \
                 patch.object(reconcile, "_build_assessment", return_value={}), \
                 patch.object(reconcile, "_aggregate_segments", return_value={"games": 1}):
                rc = reconcile.main()

            self.assertEqual(0, rc)
            self.assertEqual(out_path.resolve(), written["path"])
            report = written["report"]
            self.assertEqual(1, report["meta"]["skipped_games"])
            self.assertEqual(1, report["failures_n"])
            self.assertEqual("feed_live_missing", report["failures"][0]["error"])


if __name__ == "__main__":
    unittest.main()