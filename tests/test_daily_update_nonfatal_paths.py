import argparse
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import daily_update


class DailyUpdateNonfatalPathTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()