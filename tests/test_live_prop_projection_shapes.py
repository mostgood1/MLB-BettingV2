import unittest

from tools.eval.analyze_live_prop_projection_shapes import _build_shape_row, _shape_metrics


class LivePropProjectionShapeTests(unittest.TestCase):
    def test_shape_metrics_marks_convergence_toward_final(self) -> None:
        metrics = _shape_metrics(10.0, 8.5, 8.0)

        self.assertEqual(-2.0, metrics["first_error"])
        self.assertEqual(-0.5, metrics["last_error"])
        self.assertEqual(2.0, metrics["first_abs_error"])
        self.assertEqual(0.5, metrics["last_abs_error"])
        self.assertEqual(1.5, metrics["abs_error_delta"])
        self.assertTrue(metrics["distance_improved"])
        self.assertFalse(metrics["crossed_final"])

    def test_build_shape_row_supports_hitter_props(self) -> None:
        row = _build_shape_row(
            date_str="2026-05-09",
            key="sample-hitter-row",
            entry={
                "gamePk": 123,
                "owner": "Sample Hitter",
                "market": "hitter_props",
                "prop": "total_bases",
                "selection": "over",
                "marketLine": 1.5,
                "firstSeenAt": "2026-05-09T20:00:00Z",
                "lastSeenAt": "2026-05-09T21:00:00Z",
                "seenCount": 3,
            },
            first_snapshot={
                "liveProjection": 1.9,
                "actualSoFar": 0.0,
                "liveEdge": 0.08,
                "modelMean": 1.7,
                "reasonSummary": "Early traffic",
            },
            last_snapshot={
                "liveProjection": 1.2,
                "actualSoFar": 1.0,
                "actual": 1.0,
                "liveEdge": -0.02,
                "modelMean": 1.25,
                "reasonSummary": "Plate appearances running out",
            },
            game_state={
                "progressFraction": 0.28,
                "inning": 3,
                "halfInning": "bottom",
                "outs": 1,
                "score": {"away": 2, "home": 1},
            },
            team_side="home",
            source_name="render_sync_archive",
        )

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("hitter_props", row["market"])
        self.assertEqual("total_bases", row["prop"])
        self.assertEqual(1.9, row["first_live_projection"])
        self.assertEqual(1.2, row["last_live_projection"])
        self.assertEqual(1.0, row["final_actual"])
        self.assertAlmostEqual(0.9, row["first_abs_error"])
        self.assertAlmostEqual(0.2, row["last_abs_error"])
        self.assertEqual("improved", row["shape_result"])
        self.assertEqual("down", row["projection_move_direction"])

    def test_build_shape_row_captures_pitcher_workload_fields(self) -> None:
        row = _build_shape_row(
            date_str="2026-05-09",
            key="sample-pitcher-row",
            entry={
                "gamePk": 456,
                "owner": "Sample Pitcher",
                "market": "pitcher_props",
                "prop": "outs",
                "selection": "under",
                "marketLine": 17.5,
            },
            first_snapshot={
                "liveProjection": 14.8,
                "actualSoFar": 6.0,
                "actual": 6.0,
                "pitchCount": 31,
                "battersFaced": 11,
                "outsRecorded": 6,
                "pitchesPerBatter": 2.8,
                "expectedPitchesPerBatter": 3.1,
                "strikeRate": 0.62,
                "strikeoutRate": 0.18,
                "timesThroughOrder": 1.2,
            },
            last_snapshot={
                "liveProjection": 16.1,
                "actualSoFar": 9.0,
                "actual": 12.0,
                "pitchCount": 52,
                "battersFaced": 18,
                "outsRecorded": 9,
                "pitchesPerBatter": 2.9,
                "expectedPitchesPerBatter": 3.1,
                "strikeRate": 0.59,
                "strikeoutRate": 0.17,
                "timesThroughOrder": 2.0,
            },
            game_state={"progressFraction": 0.4, "inning": 4, "halfInning": "top", "outs": 0, "score": {"away": 1, "home": 0}},
            team_side="home",
            source_name="render_truth_registry",
        )

        assert row is not None
        self.assertEqual(31, row["first_pitch_count"])
        self.assertEqual(52, row["last_pitch_count"])
        self.assertEqual(11, row["first_batters_faced"])
        self.assertEqual(18, row["last_batters_faced"])
        self.assertEqual(6, row["first_outs_recorded"])
        self.assertEqual(9, row["last_outs_recorded"])
        self.assertAlmostEqual(0.62, row["first_strike_rate"])
        self.assertAlmostEqual(0.17, row["last_strikeout_rate"])


if __name__ == "__main__":
    unittest.main()