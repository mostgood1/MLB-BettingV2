import unittest

from tools.web.flask_frontend import _segment_projection


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


if __name__ == "__main__":
    unittest.main()