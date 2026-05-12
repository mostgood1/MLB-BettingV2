import csv
import gzip
import tempfile
import unittest
from datetime import date
from pathlib import Path

from tools.datasets.build_statcast_player_feature_set import build_feature_set
from tools.datasets.build_statcast_player_quality import build_quality


class StatcastDatasetBuilderTests(unittest.TestCase):
    def _write_rows(self, root: Path) -> None:
        season_dir = root / "2025" / "03"
        season_dir.mkdir(parents=True, exist_ok=True)
        out_path = season_dir / "sample.csv.gz"

        fieldnames = [
            "pitch_type",
            "game_date",
            "pitcher",
            "batter",
            "events",
            "description",
            "type",
            "stand",
            "zone",
            "hc_x",
            "hc_y",
            "launch_speed",
            "launch_angle",
            "release_speed",
            "release_spin_rate",
            "release_extension",
            "pfx_x",
            "pfx_z",
            "estimated_ba_using_speedangle",
            "estimated_woba_using_speedangle",
            "launch_speed_angle",
        ]
        rows = [
            {
                "pitch_type": "FF",
                "game_date": "2025-03-31",
                "pitcher": "1",
                "batter": "101",
                "events": "",
                "description": "ball",
                "type": "B",
                "stand": "R",
                "zone": "14",
                "hc_x": "",
                "hc_y": "",
                "launch_speed": "",
                "launch_angle": "",
                "release_speed": "96.5",
                "release_spin_rate": "2400",
                "release_extension": "6.4",
                "pfx_x": "0.1",
                "pfx_z": "1.2",
                "estimated_ba_using_speedangle": "",
                "estimated_woba_using_speedangle": "",
                "launch_speed_angle": "",
            },
            {
                "pitch_type": "FF",
                "game_date": "2025-03-31",
                "pitcher": "1",
                "batter": "101",
                "events": "",
                "description": "ball",
                "type": "B",
                "stand": "R",
                "zone": "14",
                "hc_x": "",
                "hc_y": "",
                "launch_speed": "",
                "launch_angle": "",
                "release_speed": "96.1",
                "release_spin_rate": "2390",
                "release_extension": "6.5",
                "pfx_x": "0.2",
                "pfx_z": "1.1",
                "estimated_ba_using_speedangle": "",
                "estimated_woba_using_speedangle": "",
                "launch_speed_angle": "",
            },
            {
                "pitch_type": "FF",
                "game_date": "2025-03-31",
                "pitcher": "1",
                "batter": "101",
                "events": "single",
                "description": "hit_into_play",
                "type": "X",
                "stand": "R",
                "zone": "5",
                "hc_x": "140",
                "hc_y": "160",
                "launch_speed": "102.0",
                "launch_angle": "26.0",
                "release_speed": "97.0",
                "release_spin_rate": "2410",
                "release_extension": "6.6",
                "pfx_x": "0.1",
                "pfx_z": "1.3",
                "estimated_ba_using_speedangle": "0.710",
                "estimated_woba_using_speedangle": "0.930",
                "launch_speed_angle": "6",
            },
            {
                "pitch_type": "SL",
                "game_date": "2025-03-31",
                "pitcher": "2",
                "batter": "102",
                "events": "",
                "description": "called_strike",
                "type": "S",
                "stand": "L",
                "zone": "8",
                "hc_x": "",
                "hc_y": "",
                "launch_speed": "",
                "launch_angle": "",
                "release_speed": "85.2",
                "release_spin_rate": "2500",
                "release_extension": "6.0",
                "pfx_x": "-0.4",
                "pfx_z": "0.2",
                "estimated_ba_using_speedangle": "",
                "estimated_woba_using_speedangle": "",
                "launch_speed_angle": "",
            },
            {
                "pitch_type": "SL",
                "game_date": "2025-03-31",
                "pitcher": "2",
                "batter": "102",
                "events": "",
                "description": "called_strike",
                "type": "S",
                "stand": "L",
                "zone": "8",
                "hc_x": "",
                "hc_y": "",
                "launch_speed": "",
                "launch_angle": "",
                "release_speed": "85.0",
                "release_spin_rate": "2495",
                "release_extension": "6.1",
                "pfx_x": "-0.3",
                "pfx_z": "0.3",
                "estimated_ba_using_speedangle": "",
                "estimated_woba_using_speedangle": "",
                "launch_speed_angle": "",
            },
            {
                "pitch_type": "SL",
                "game_date": "2025-03-31",
                "pitcher": "2",
                "batter": "102",
                "events": "field_out",
                "description": "hit_into_play",
                "type": "X",
                "stand": "L",
                "zone": "7",
                "hc_x": "100",
                "hc_y": "170",
                "launch_speed": "88.0",
                "launch_angle": "5.0",
                "release_speed": "84.8",
                "release_spin_rate": "2480",
                "release_extension": "6.0",
                "pfx_x": "-0.5",
                "pfx_z": "0.1",
                "estimated_ba_using_speedangle": "0.200",
                "estimated_woba_using_speedangle": "0.210",
                "launch_speed_angle": "3",
            },
        ]

        with gzip.open(out_path, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_builders_use_launch_speed_angle_for_barrels_and_real_bb_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            raw_root = Path(tmp_dir)
            self._write_rows(raw_root)

            feature_data = build_feature_set(
                raw_root=raw_root,
                season=2025,
                start_date=date(2025, 3, 1),
                end_date=date(2025, 3, 31),
                min_pitches_pitcher=1,
                min_pitches_batter=1,
                min_pitches_pitch_type=1,
                min_bip_ev=1,
            )
            quality_data = build_quality(
                raw_root=raw_root,
                season=2025,
                start_date=date(2025, 3, 1),
                end_date=date(2025, 3, 31),
                min_pitches_pitcher=1,
                min_pitches_batter=1,
                min_bip_ev=1,
            )

        self.assertEqual(1.0, feature_data["pitchers"]["1"]["overall"]["barrel_rate"])
        self.assertEqual(1.0, feature_data["batters"]["101"]["overall"]["barrel_rate"])
        self.assertGreater(feature_data["pitchers"]["1"]["mult_overall"]["bb"], 1.0)
        self.assertLess(feature_data["pitchers"]["2"]["mult_overall"]["bb"], 1.0)

        self.assertEqual(1.0, quality_data["pitchers"]["1"]["barrel_rate"])
        self.assertEqual(1.0, quality_data["batters"]["101"]["barrel_rate"])
        self.assertGreater(quality_data["pitchers"]["1"]["mult"]["bb"], 1.0)
        self.assertLess(quality_data["pitchers"]["2"]["mult"]["bb"], 1.0)


if __name__ == "__main__":
    unittest.main()