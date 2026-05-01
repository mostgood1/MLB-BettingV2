import unittest

from tools import daily_update_multi_profile as multi_profile


class RfiSignalThresholdTests(unittest.TestCase):
    def test_yrfi_signal_uses_retuned_recent_window_thresholds(self) -> None:
        row = {
            "nrfi_prob": 0.50,
            "away_runs_mean": 0.50,
            "home_runs_mean": 0.45,
            "away_win_prob": 0.24,
            "home_win_prob": 0.18,
        }

        signal = multi_profile._rfi_signal_from_first1_row(row)

        self.assertIsInstance(signal, dict)
        self.assertEqual(signal.get("label"), "F1 YRFI")
        self.assertEqual(signal.get("tone"), "yrfi")
        self.assertEqual(signal.get("nrfiProb"), 0.5)
        self.assertEqual(signal.get("meanTotalRuns"), 0.95)
        self.assertEqual(signal.get("maxSideLeadProb"), 0.24)


if __name__ == "__main__":
    unittest.main()
