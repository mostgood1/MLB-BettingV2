import unittest

from tools.eval.analyze_live_prop_trajectories import _build_trajectory_rows_for_key


class LivePropTrajectoryTests(unittest.TestCase):
    def test_build_trajectory_rows_tracks_update_quality(self) -> None:
        entry = {
            "gamePk": 123,
            "owner": "Sample Pitcher",
            "market": "pitcher_props",
            "prop": "outs",
            "selection": "under",
            "marketLine": 15.5,
            "actual": 12.0,
            "seenCount": 3,
        }
        observations = [
            {
                "recordedAt": "2026-05-09T14:00:00Z",
                "seenCount": 1,
                "snapshot": {"liveProjection": 14.0, "actualSoFar": 0.0, "marketLine": 15.5},
                "gameState": {"progressFraction": 0.05, "inning": 1, "halfInning": "top", "outs": 0, "score": {"away": 0, "home": 0}},
            },
            {
                "recordedAt": "2026-05-09T14:10:00Z",
                "seenCount": 2,
                "snapshot": {"liveProjection": 12.8, "actualSoFar": 2.0, "marketLine": 15.5},
                "gameState": {"progressFraction": 0.1, "inning": 1, "halfInning": "top", "outs": 2, "score": {"away": 0, "home": 0}},
            },
            {
                "recordedAt": "2026-05-09T14:20:00Z",
                "seenCount": 3,
                "snapshot": {"liveProjection": 13.4, "actualSoFar": 3.0, "marketLine": 15.5},
                "gameState": {"progressFraction": 0.15, "inning": 2, "halfInning": "top", "outs": 0, "score": {"away": 0, "home": 0}},
            },
        ]

        rows = _build_trajectory_rows_for_key(
            date_str="2026-05-09",
            key="sample-key",
            entry=entry,
            observations=observations,
            source_name="render_truth_trajectory",
        )

        self.assertEqual(3, len(rows))
        self.assertIsNone(rows[0]["abs_error_delta"])
        self.assertAlmostEqual(1.2, rows[1]["abs_error_delta"])
        self.assertTrue(rows[1]["distance_improved_vs_prev"])
        self.assertEqual("down", rows[1]["projection_move_direction"])
        self.assertAlmostEqual(-0.6, rows[2]["abs_error_delta"])
        self.assertFalse(rows[2]["distance_improved_vs_prev"])
        self.assertEqual("up", rows[2]["projection_move_direction"])


if __name__ == "__main__":
    unittest.main()