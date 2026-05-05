import unittest
from pathlib import Path

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

    def test_prior_day_eval_failure_is_nonfatal_for_ui_daily_push_path(self) -> None:
        report = {
            "season": 2026,
            "warnings": [],
            "errors": [],
        }
        prior_eval_stage = {
            "status": "ok",
            "date": "2026-05-04",
        }

        publish_stage = daily_update._mark_ui_daily_prior_eval_failure_nonfatal(
            report=report,
            prior_eval_stage=prior_eval_stage,
            prior_date="2026-05-04",
            reason="exit 1",
            season_batch_dir=Path("data/eval/batches/season_2026_ui_daily_live"),
        )

        self.assertEqual("warning", prior_eval_stage["status"])
        self.assertEqual([], report["errors"])
        self.assertEqual(1, len(report["warnings"]))
        self.assertIn("prior-day eval report refresh failed for 2026-05-04", report["warnings"][0])
        self.assertIn("continuing without season manifest publish", report["warnings"][0])
        self.assertEqual("skipped", publish_stage["status"])
        self.assertEqual("prior-day eval report refresh failed", publish_stage["reason"])


if __name__ == "__main__":
    unittest.main()