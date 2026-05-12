import unittest

from tools.eval.eval_sim_day_vs_actual import _resolve_pitcher_prop_prediction
from tools.eval.evaluate_pitcher_outs_accuracy import _build_rows, _build_starter_resolution_summary


class EvalPitcherPropStarterResolutionTests(unittest.TestCase):
    def test_resolve_pitcher_prop_prediction_hides_requested_starter_on_mismatch(self) -> None:
        resolved = _resolve_pitcher_prop_prediction(
            {
                "808967": {"outs_mean": 15.0, "pitches_mean": 78.0},
                "605483": {"outs_mean": 0.0, "pitches_mean": 0.0},
            },
            requested_starter_id=605483,
            simulated_starter_id=808967,
        )

        self.assertTrue(resolved["starter_mismatch"])
        self.assertIsNone(resolved["pred"])
        self.assertEqual(808967, resolved["simulated_starter_id"])
        self.assertEqual(15.0, (resolved["pred_simulated_starter"] or {}).get("outs_mean"))

    def test_build_rows_skips_missing_requested_starter_prediction(self) -> None:
        report = {
            "date": "2026-05-09",
            "games": [
                {
                    "game_pk": 823955,
                    "away": {"abbr": "ATL"},
                    "home": {"abbr": "LAD"},
                    "pitcher_props": {
                        "away": {
                            "starter_id": 675911,
                            "actual": {"outs": 18, "pitches": 91},
                            "pred": {"outs_mean": 15.0, "pitches_mean": 74.0},
                        },
                        "home": {
                            "starter_id": 605483,
                            "simulated_starter_id": 808967,
                            "starter_mismatch": True,
                            "starter_name": "Blake Snell",
                            "actual": {"outs": 9, "pitches": 77},
                            "pred": None,
                            "pred_simulated_starter": {"outs_mean": 15.0, "pitches_mean": 78.0},
                        },
                    },
                }
            ],
        }
        rows = _build_rows(report)

        self.assertEqual(1, len(rows))
        self.assertEqual("ATL", rows[0]["team"])

        resolution = _build_starter_resolution_summary(report, rows)
        self.assertEqual(1, resolution["rows_with_scored_preds"])
        self.assertEqual(0, resolution["starter_mismatch_rows"])
        self.assertEqual(1, len(resolution["starter_mismatch_examples"]))
        self.assertEqual(605483, resolution["starter_mismatch_examples"][0]["starter_id"])


if __name__ == "__main__":
    unittest.main()