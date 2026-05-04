import unittest

from tools import daily_update


class DailyUpdateGitPushTests(unittest.TestCase):
    def test_managed_artifact_conflict_is_auto_resolvable_even_when_not_owned(self) -> None:
        result = daily_update._daily_update_rebase_conflict_is_auto_resolvable(
            "data/daily/sims/2026-05-04/sim_0_BOS_at_DET_pk824283_g1.json",
            set(),
        )

        self.assertTrue(result)

    def test_unmanaged_conflict_still_requires_manual_intervention(self) -> None:
        result = daily_update._daily_update_rebase_conflict_is_auto_resolvable(
            "tools/daily_update.py",
            set(),
        )

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()