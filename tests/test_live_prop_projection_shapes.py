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


if __name__ == "__main__":
    unittest.main()