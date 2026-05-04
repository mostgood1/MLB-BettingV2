import unittest
from pathlib import Path
from unittest.mock import patch

from tools.web import flask_frontend
from tools.web.flask_frontend import (
    _build_cards_api_payload,
    _cards_payload_signature,
    _cards_list_from_sources,
    _payload_cache_get_or_build,
    _season_betting_day_payload,
    _should_load_cards_archive_context,
    _supplement_recos_by_game_with_betting_games,
)


class CardsExtraMarketSupportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._season_betting_path = Path("data/daily/season_frontend/season_betting_day_2026_2026_05_04_retuned.json")
        self._season_betting_original = self._season_betting_path.read_text(encoding="utf-8") if self._season_betting_path.exists() else None

    def tearDown(self) -> None:
        flask_frontend._PAYLOAD_CACHE.clear()
        if self._season_betting_original is None:
            self._season_betting_path.unlink(missing_ok=True)
            return
        self._season_betting_path.parent.mkdir(parents=True, exist_ok=True)
        self._season_betting_path.write_text(self._season_betting_original, encoding="utf-8")

    def test_payload_cache_rebuilds_when_signature_changes_within_ttl(self) -> None:
        first = _payload_cache_get_or_build(
            "cards_api_context",
            "2026-05-04-test",
            signature=("before",),
            max_age_seconds=60.0,
            builder=lambda: {"value": "before"},
        )
        second = _payload_cache_get_or_build(
            "cards_api_context",
            "2026-05-04-test",
            signature=("after",),
            max_age_seconds=60.0,
            builder=lambda: {"value": "after"},
        )

        self.assertEqual(first["value"], "before")
        self.assertEqual(second["value"], "after")

    def test_supplement_recos_adds_missing_extra_markets_for_existing_game(self) -> None:
        recos_by_game = {
            824039: {
                "totals": None,
                "ml": None,
                "pitcher_props": [],
                "hitter_props": [],
                "extra_pitcher_props": [],
                "extra_hitter_props": [],
            }
        }
        betting_games = {
            824039: {
                "markets": {
                    "totals": None,
                    "ml": None,
                    "pitcherProps": [],
                    "hitterProps": [],
                    "extraPitcherProps": [{"game_pk": 824039, "pitcher_name": "Davis Martin", "odds": "+102"}],
                    "extraHitterProps": [{"game_pk": 824039, "player_name": "Sample Hitter", "odds": "+140"}],
                }
            }
        }

        merged = _supplement_recos_by_game_with_betting_games(recos_by_game, betting_games)

        self.assertEqual(len(merged[824039]["extra_pitcher_props"]), 1)
        self.assertEqual(len(merged[824039]["extra_hitter_props"]), 1)

    def test_cards_treat_extra_only_markets_as_recommendations(self) -> None:
        cards = _cards_list_from_sources(
            d="2026-05-04",
            schedule_games=[
                {
                    "gamePk": 824039,
                    "gameType": "R",
                    "gameDate": "2026-05-05T01:38:00Z",
                    "officialDate": "2026-05-04",
                    "status": {"abstractGameState": "Preview", "detailedState": "Scheduled"},
                    "teams": {
                        "away": {"team": {"id": 145, "abbreviation": "CWS", "name": "Chicago White Sox"}},
                        "home": {"team": {"id": 108, "abbreviation": "LAA", "name": "Los Angeles Angels"}},
                    },
                }
            ],
            outputs_by_game={},
            recos_by_game={
                824039: {
                    "totals": None,
                    "ml": None,
                    "pitcher_props": [],
                    "hitter_props": [],
                    "extra_pitcher_props": [{"game_pk": 824039, "pitcher_name": "Davis Martin", "odds": "+102"}],
                    "extra_hitter_props": [],
                }
            },
        )

        self.assertEqual(len(cards), 1)
        self.assertTrue(cards[0]["flags"]["hasAnyRecommendations"])
        self.assertTrue(cards[0]["flags"]["hasPitcherProps"])
        self.assertEqual(len(cards[0]["markets"]["extraPitcherProps"]), 1)

    def test_build_cards_api_payload_supplements_from_betting_payload(self) -> None:
        schedule_games = [
            {
                "gamePk": 824039,
                "gameType": "R",
                "gameDate": "2026-05-05T01:38:00Z",
                "officialDate": "2026-05-04",
                "status": {"abstractGameState": "Preview", "detailedState": "Scheduled"},
                "teams": {
                    "away": {"team": {"id": 145, "abbreviation": "CWS", "name": "Chicago White Sox"}},
                    "home": {"team": {"id": 108, "abbreviation": "LAA", "name": "Los Angeles Angels"}},
                },
            }
        ]
        betting_payload = {
            "found": True,
            "games": {
                824039: {
                    "markets": {
                        "totals": None,
                        "ml": None,
                        "pitcherProps": [],
                        "hitterProps": [],
                        "extraPitcherProps": [{"game_pk": 824039, "pitcher_name": "Davis Martin", "odds": "+102"}],
                        "extraHitterProps": [{"game_pk": 824039, "player_name": "Sample Hitter", "odds": "+140"}],
                    }
                }
            },
        }

        with patch.object(flask_frontend, "_season_betting_day_payload", return_value=betting_payload), patch.object(
            flask_frontend,
            "_schedule_games_for_date",
            return_value=schedule_games,
        ):
            payload = _build_cards_api_payload(
                "2026-05-04",
                artifacts={"locked_policy": {}, "game_summary": None, "rfi_targets": None, "daily_ladders": None},
                archive={},
                game_line_index={},
            )

        cards = payload.get("cards") or []
        self.assertEqual(len(cards), 1)
        self.assertTrue(cards[0]["flags"]["hasAnyRecommendations"])
        self.assertTrue(cards[0]["flags"]["hasPitcherProps"])
        self.assertTrue(cards[0]["flags"]["hasHitterProps"])
        self.assertEqual(len(cards[0]["markets"]["extraPitcherProps"]), 1)
        self.assertEqual(len(cards[0]["markets"]["extraHitterProps"]), 1)

    def test_current_day_cards_still_load_archive_context_when_season_card_exists(self) -> None:
        artifacts = {"locked_policy": {"games": []}, "game_summary": {"games": []}}
        self._season_betting_path.parent.mkdir(parents=True, exist_ok=True)
        self._season_betting_path.write_text("{}", encoding="utf-8")

        with patch.object(
            flask_frontend,
            "_load_season_betting_manifest",
            return_value=("retuned", Path("data/eval/seasons/2026/season_betting_cards_retuned_manifest.json"), {"days": []}, {"retuned": "x"}),
        ), patch.object(
            flask_frontend,
            "_resolve_season_betting_day_card_path",
            return_value=self._season_betting_path,
        ):
            self.assertTrue(_should_load_cards_archive_context("2026-05-04", artifacts))

    def test_season_betting_day_payload_prefers_manifest_card_for_current_day(self) -> None:
        legacy_card = {"markets": {}}
        manifest_card = {
            "markets": {
                "hitter_props": {
                    "recommendations": [
                        {
                            "game_pk": 824039,
                            "player_name": "Sample Hitter",
                            "odds": "+140",
                        }
                    ]
                }
            }
        }
        legacy_card_path = Path("data/daily/daily_summary_2026_05_04_locked_policy.json")
        manifest_card_path = Path("data/eval/seasons/2026/season_betting_cards/2026-05-04.json")
        settled_stub = {
            "selected_counts": {"combined": 1},
            "results": {},
            "playable_results": {},
            "all_results": {},
            "_settled_rows": [],
            "_playable_settled_rows": [],
            "_all_settled_rows": [],
            "unresolved_recommendations": [],
            "playable_unresolved_recommendations": [],
        }

        with patch.object(
            flask_frontend,
            "_load_cards_artifacts",
            return_value={
                "locked_policy_path": legacy_card_path,
                "locked_policy": legacy_card,
                "settlement_path": None,
                "settlement": None,
                "embedded_settlement_summary": None,
            },
        ), patch.object(
            flask_frontend,
            "_load_season_betting_manifest",
            return_value=(
                "retuned",
                Path("data/eval/seasons/2026/season_betting_cards_retuned_manifest.json"),
                {"days": [{"date": "2026-05-04", "card_path": str(manifest_card_path)}]},
                {"retuned": "x"},
            ),
        ), patch.object(
            flask_frontend,
            "_resolve_season_betting_day_payload_path",
            return_value=None,
        ), patch.object(
            flask_frontend,
            "_resolve_season_betting_day_card_path",
            return_value=manifest_card_path,
        ), patch.object(
            flask_frontend,
            "_load_json_file",
            side_effect=lambda path: manifest_card if path == manifest_card_path else None,
        ), patch.object(
            flask_frontend,
            "_pending_settlement_from_card",
            return_value=settled_stub,
        ):
            payload = _season_betting_day_payload(2026, "2026-05-04", "")

        self.assertTrue(payload["found"])
        self.assertEqual(payload["card_source"], str(manifest_card_path).replace("\\", "/"))
        self.assertIn(824039, payload["games"])
        self.assertEqual(payload["source_kind"], "season_manifest")

    def test_cards_payload_signature_tracks_season_betting_day_artifact(self) -> None:
        artifacts = {
            "profile_bundle_path": None,
            "hr_targets_path": None,
            "locked_policy_path": None,
            "game_summary_path": None,
            "daily_ladders_path": None,
            "settlement_path": None,
            "ops_report_path": None,
            "lineups_path": None,
            "sim_dir": None,
        }
        archive = {"report_path": None, "card_path": None}
        game_line_index = {"path": None}

        before = _cards_payload_signature("2026-05-04", artifacts, archive, game_line_index)
        self._season_betting_path.parent.mkdir(parents=True, exist_ok=True)
        self._season_betting_path.write_text('{"found": true, "games": {}}', encoding="utf-8")
        after = _cards_payload_signature("2026-05-04", artifacts, archive, game_line_index)

        self.assertNotEqual(before, after)

    def test_refresh_current_day_market_backed_artifacts_republishes_and_rewrites_frontend(self) -> None:
        republish_result = {"ok": True, "manifest": "updated"}
        purge_result = {"deleted": ["data/daily/season_frontend/season_betting_day_2026_2026_05_04_retuned.json"]}
        frontend_result = {"artifacts": {"season_betting_day": {"found": True}}}

        with patch.object(flask_frontend, "_publish_season_manifests", return_value=republish_result) as publish_mock, patch.object(
            flask_frontend,
            "_purge_current_day_season_frontend_artifacts",
            return_value=purge_result,
        ) as purge_mock, patch.object(
            flask_frontend,
            "write_current_day_season_frontend_artifacts",
            return_value=frontend_result,
        ) as frontend_mock:
            result = flask_frontend._refresh_current_day_market_backed_artifacts(
                "2026-05-04",
                season=2026,
                betting_profile="retuned",
            )

        publish_mock.assert_called_once()
        purge_mock.assert_called_once_with(2026, "2026-05-04", betting_profile="retuned")
        frontend_mock.assert_called_once_with(2026, "2026-05-04", betting_profile="retuned")
        self.assertEqual(result["republish"], republish_result)
        self.assertIsNone(result["republish_error"])
        self.assertEqual(result["purged_frontend"], purge_result)
        self.assertEqual(result["frontend"], frontend_result)


if __name__ == "__main__":
    unittest.main()