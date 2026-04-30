import unittest

from tools.web.flask_frontend import _game_lens_total_market, _segment_projection


class GameLensSegmentTests(unittest.TestCase):
    def test_segment_closes_at_exact_target_fraction(self) -> None:
        result = _segment_projection(
            pregame_away=4.5,
            pregame_home=4.5,
            actual_away=5.0,
            actual_home=4.0,
            progress_fraction=7 / 9,
            target_innings=7,
        )
        self.assertTrue(result["closed"])
        self.assertIsNone(result["away"])
        self.assertIsNone(result["home"])
        self.assertIsNone(result["total"])
        self.assertIsNone(result["homeMargin"])

    def test_segment_remains_open_before_target_fraction(self) -> None:
        result = _segment_projection(
            pregame_away=4.5,
            pregame_home=4.5,
            actual_away=5.0,
            actual_home=4.0,
            progress_fraction=0.70,
            target_innings=7,
        )
        self.assertFalse(result["closed"])
        self.assertIsNotNone(result["away"])
        self.assertIsNotNone(result["home"])
        self.assertIsNotNone(result["total"])
        self.assertIsNotNone(result["homeMargin"])

    def test_total_market_does_not_recommend_over_after_line_already_cleared(self) -> None:
        market = _game_lens_total_market(
            label="Full Game",
            projection_total=7.33,
            progress={"remainingOuts": 2, "label": "Bottom 9"},
            actual_home=3.0,
            actual_away=4.0,
            closed=False,
            total_line=6.5,
            total_over_odds=100,
            total_under_odds=-120,
            snapshot=None,
        )
        self.assertIsNone(market["pick"])
        self.assertIn("already cleared the posted total", market["reason"])

    def test_total_market_does_not_recommend_under_after_line_already_lost(self) -> None:
        market = _game_lens_total_market(
            label="Full Game",
            projection_total=5.17,
            progress={"remainingOuts": 1, "label": "Bottom 9"},
            actual_home=3.0,
            actual_away=5.0,
            closed=False,
            total_line=6.5,
            total_over_odds=-105,
            total_under_odds=-115,
            snapshot=None,
        )
        self.assertIsNone(market["pick"])
        self.assertIn("run past the posted total", market["reason"])


if __name__ == "__main__":
    unittest.main()