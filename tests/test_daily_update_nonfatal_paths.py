import argparse
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import daily_update


class DailyUpdateNonfatalPathTests(unittest.TestCase):
    def test_roster_artifact_statcast_mode_mismatch_is_rejected(self) -> None:
        artifact_meta = {
            "date": "2026-05-11",
            "stats_season": 2026,
            "spring_mode": False,
            "statcast_starter_splits": "off",
            "roster_builder": {
                "as_of_date": "2026-05-11",
                "away_probable_pitcher_id": 101,
                "home_probable_pitcher_id": 202,
                "away_lineup_ids": [1, 2, 3],
                "home_lineup_ids": [4, 5, 6],
            },
        }

        matches, reason = daily_update._roster_artifact_matches_inputs(
            artifact_meta,
            date_str="2026-05-11",
            stats_season=2026,
            spring_mode=False,
            statcast_starter_splits="on",
            away_probable_pitcher_id=101,
            home_probable_pitcher_id=202,
            away_lineup_ids=[1, 2, 3],
            home_lineup_ids=[4, 5, 6],
        )

        self.assertFalse(matches)
        self.assertEqual("statcast_starter_splits_mismatch", reason)

    def test_prior_day_live_lens_missing_render_credentials_is_skipped(self) -> None:
        args = argparse.Namespace(
            sync_live_lens="on",
            live_lens_base_url="",
            live_lens_cron_token="",
            live_lens_timeout_seconds=45,
            live_lens_sync_out="",
        )

        with mock.patch.object(daily_update, "_infer_render_base_url", return_value=""), mock.patch.object(
            daily_update, "_env_first", return_value=""
        ), mock.patch.object(daily_update, "_load_json_if_exists", return_value={}):
            stage = daily_update._prior_day_live_lens_stage(args, "2026-05-04")

        self.assertEqual("skipped", stage["status"])
        self.assertIn("Render base URL or cron token is unavailable", stage["reason"])
        self.assertEqual(["live_lens_base_url", "live_lens_cron_token"], stage["missing_credentials"])

    def test_refresh_live_pitcher_corrections_missing_render_credentials_is_skipped(self) -> None:
        args = argparse.Namespace(
            refresh_live_pitcher_corrections="on",
            live_lens_base_url="",
            live_lens_cron_token="",
            live_lens_timeout_seconds=45,
        )

        with mock.patch.object(daily_update, "_infer_render_base_url", return_value=""), mock.patch.object(
            daily_update, "_env_first", return_value=""
        ):
            stage = daily_update._refresh_live_pitcher_corrections_stage(args, max_date_str="2026-05-04")

        self.assertEqual("skipped", stage["status"])
        self.assertIn("Render base URL or cron token is unavailable", stage["reason"])
        self.assertEqual(["live_lens_base_url", "live_lens_cron_token"], stage["missing_credentials"])

    def test_refresh_live_pitcher_corrections_no_archived_rows_is_skipped(self) -> None:
        args = argparse.Namespace(
            refresh_live_pitcher_corrections="on",
            live_lens_base_url="https://mlb-betting-v2.onrender.com",
            live_lens_cron_token="token",
            live_lens_timeout_seconds=45,
        )

        proc = mock.Mock(
            returncode=1,
            stdout="",
            stderr=(
                "RuntimeError: render live-lens sync completed but no archived observation rows were found "
                "between 2026-03-25 and 2026-03-26"
            ),
        )

        with mock.patch.object(daily_update, "_run_logged_subprocess", return_value=proc):
            stage = daily_update._refresh_live_pitcher_corrections_stage(args, max_date_str="2026-03-26")

        self.assertEqual("skipped", stage["status"])
        self.assertIn("no archived live-lens observation rows", stage["reason"])
        self.assertEqual("skipped", stage["artifacts"]["outs"]["status"])
        self.assertEqual("skipped", stage["artifacts"]["strikeouts"]["status"])
        self.assertEqual(
            "data/eval/live_pitcher_outs_correction.json",
            stage["artifacts"]["outs"]["artifact_path"],
        )

    def test_remove_path_if_exists_ignores_empty_locked_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "snapshots"
            locked_child = root / "roster_objs"
            locked_child.mkdir(parents=True)
            original_rmdir = Path.rmdir

            def fake_rmdir(path_self: Path) -> None:
                if path_self == locked_child:
                    raise PermissionError("locked")
                if path_self == root:
                    raise OSError("directory not empty")
                original_rmdir(path_self)

            with mock.patch.object(Path, "rmdir", autospec=True, side_effect=fake_rmdir):
                removed_any, warning_text = daily_update._remove_path_if_exists(root)

        self.assertTrue(removed_any)
        self.assertIsNone(warning_text)

    def test_git_push_skips_when_non_managed_changes_already_exist(self) -> None:
        with mock.patch.object(daily_update, "_git_staged_paths", return_value=[]), mock.patch.object(
            daily_update, "_git_current_change_set"
        ) as change_set_mock, mock.patch.object(daily_update, "_git_run") as git_run_mock:
            result = daily_update._maybe_git_push_daily_update(
                repo_ROOT_DIR=Path("."),
                date_str="2026-05-12",
                workflow="ui-daily",
                preexisting_changes={"sim_engine/data/build_roster.py", "tests/test_advanced_statcast_metric_wiring.py"},
                enabled=True,
                remote="origin",
                branch="main",
                commit_message="Daily update 2026-05-12",
            )

        self.assertEqual("skipped", result["status"])
        self.assertEqual("preexisting non-artifact repository changes", result["reason"])
        self.assertIn("sim_engine/data/build_roster.py", result["non_managed_preexisting_paths"])
        change_set_mock.assert_not_called()
        git_run_mock.assert_not_called()

    def test_render_validation_skips_without_expected_commit(self) -> None:
        args = argparse.Namespace(
            validate_render_frontend="on",
            render_validation_base_url="https://mlb-betting-v2.onrender.com",
            render_validation_cron_token="token",
            live_lens_base_url="",
            live_lens_cron_token="",
            render_validation_timeout_seconds=45,
        )

        with mock.patch.object(daily_update, "subprocess") as subprocess_mock:
            stage = daily_update._run_render_frontend_validation_stage(
                args,
                date_str="2026-05-12",
                season=2026,
                expected_commit="",
            )

        self.assertEqual("skipped", stage["status"])
        self.assertEqual("render validation skipped because no published commit is available", stage["reason"])
        subprocess_mock.run.assert_not_called()


if __name__ == "__main__":
    unittest.main()