import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import daily_update_multi_profile as daily_update
from tools.web import flask_frontend as frontend


def _build_doc(rows: int, games: int) -> dict:
    return {
        "counts": {"rows": rows, "games": games},
        "rows": [{"playerName": f"Player {index}"} for index in range(rows)],
        "policy": {"preset": "default"},
        "source_profile": "hitter_props_recos",
    }


class HrTargetsArtifactResolutionTests(unittest.TestCase):
    def test_resolver_promotes_richer_tracked_artifact_into_canonical_data_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            canonical_root = temp_root / "canonical_data"
            tracked_root = temp_root / "tracked_data"
            canonical_path = canonical_root / "daily" / "daily_summary_2026_05_01_hr_targets.json"
            tracked_path = tracked_root / "daily" / "daily_summary_2026_05_01_hr_targets.json"
            canonical_path.parent.mkdir(parents=True, exist_ok=True)
            tracked_path.parent.mkdir(parents=True, exist_ok=True)
            canonical_path.write_text(json.dumps(_build_doc(rows=9, games=9)), encoding="utf-8")
            tracked_path.write_text(json.dumps(_build_doc(rows=30, games=15)), encoding="utf-8")

            original_data_dir = frontend._DATA_DIR
            original_tracked_data_dir = frontend._TRACKED_DATA_DIR
            try:
                frontend._DATA_DIR = canonical_root
                frontend._TRACKED_DATA_DIR = tracked_root

                resolved_path, resolved_doc = frontend._resolve_hr_targets_artifact("2026-05-01")
            finally:
                frontend._DATA_DIR = original_data_dir
                frontend._TRACKED_DATA_DIR = original_tracked_data_dir

            self.assertEqual(resolved_path, canonical_path)
            self.assertIsInstance(resolved_doc, dict)
            self.assertEqual((resolved_doc.get("counts") or {}).get("rows"), 30)

            promoted_doc = json.loads(canonical_path.read_text(encoding="utf-8"))
            self.assertEqual((promoted_doc.get("counts") or {}).get("rows"), 30)
            self.assertEqual((promoted_doc.get("counts") or {}).get("games"), 15)

    def test_collect_hr_targets_backfills_near_threshold_candidates_when_board_is_sparse(self) -> None:
        sim_obj = {
            "game_pk": 1001,
            "schedule": {"game_number": 1},
            "sim": {
                "hitter_hr_likelihood_topn": {
                    "overall": [
                        {"batter_id": 11, "name": "Qualified Bat", "team": "NYY", "p_hr_1plus": 0.21, "lineup_order": 2, "pa_mean": 4.5, "ab_mean": 4.0},
                        {"batter_id": 22, "name": "Near Threshold", "team": "BOS", "p_hr_1plus": 0.15, "lineup_order": 3, "pa_mean": 4.3, "ab_mean": 3.9},
                        {"batter_id": 33, "name": "Too Thin", "team": "BOS", "p_hr_1plus": 0.08, "lineup_order": 6, "pa_mean": 3.8, "ab_mean": 3.4},
                    ]
                }
            },
        }

        def support_for(rec: dict, _context_fields: dict) -> dict:
            name = rec.get("name")
            if name == "Qualified Bat":
                return {"score": 82.0, "raw_score": 82.0, "label": "A", "reasons": ["elite power"], "metrics": {"barrel": 14}}
            if name == "Near Threshold":
                return {"score": 64.0, "raw_score": 64.0, "label": "B", "reasons": ["strong matchup"], "metrics": {"barrel": 11}}
            return {"score": 38.0, "raw_score": 38.0, "label": "C", "reasons": ["weak support"], "metrics": {"barrel": 5}}

        def exclusion_for(rec: dict, _context_fields: dict, _hr_prob: float, _support_score: float, _policy: dict) -> list[str]:
            name = rec.get("name")
            if name == "Near Threshold":
                return ["below_min_prob"]
            if name == "Too Thin":
                return ["below_min_support_score"]
            return []

        def matchup_for(_sim_obj: dict, rec: dict, _roster_snapshot: dict | None) -> dict:
            team = rec.get("team")
            if team == "NYY":
                return {"team": "NYY", "team_side": "away", "opponent": "BOS", "team_id": 1}
            return {"team": "BOS", "team_side": "home", "opponent": "NYY", "team_id": 2}

        def context_for(rec: dict, matchup_ctx: dict, _roster_snapshot: dict | None, season: int) -> dict:
            return {
                "opponent": matchup_ctx.get("opponent"),
                "lineup_status": "expected",
                "batter_id": rec.get("batter_id"),
                "season": season,
            }

        with tempfile.TemporaryDirectory() as temp_dir:
            sim_dir = Path(temp_dir)
            with (
                patch.object(daily_update, "_iter_sim_records", return_value=[sim_obj]),
                patch.object(daily_update, "_base_game_row", return_value={"date": "2026-05-01", "game_pk": 1001, "away_abbr": "NYY", "home_abbr": "BOS"}),
                patch.object(daily_update, "_lookup_hitter_matchup_context", side_effect=matchup_for),
                patch.object(daily_update, "_hitter_recommendation_context_fields", side_effect=context_for),
                patch.object(daily_update, "_hitter_hr_target_support", side_effect=support_for),
                patch.object(daily_update, "_hitter_hr_target_exclusion_reasons", side_effect=exclusion_for),
                patch.object(daily_update, "_reason_paragraph", side_effect=lambda reasons, max_sentences=2: " / ".join(reasons[:max_sentences])),
            ):
                doc = daily_update._collect_daily_hr_targets(sim_dir, None, date="2026-05-01", season=2026)

        rows = doc.get("rows") or []
        self.assertEqual((doc.get("counts") or {}).get("rows"), 2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].get("player_name"), "Qualified Bat")
        self.assertEqual(rows[1].get("player_name"), "Near Threshold")
        self.assertTrue(bool(rows[1].get("fallback_selected")))
        self.assertEqual(rows[1].get("source"), "hitter_hr_likelihood_fallback")
        game_targets = ((doc.get("games") or [{}])[0].get("targets") or [])
        self.assertEqual(len(game_targets), 2)


if __name__ == "__main__":
    unittest.main()