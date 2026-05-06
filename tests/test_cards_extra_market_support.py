import unittest
from pathlib import Path
from unittest.mock import patch

from tools.web import flask_frontend
from tools.web.flask_frontend import (
    _attach_cards_starter_ladder_badges,
    _build_cards_api_payload,
    _cards_payload_signature,
    _cards_list_from_sources,
    _load_game_line_market_context,
    _load_hitter_ladder_market_context,
    _load_pitcher_ladder_market_context,
    _prebuilt_pitcher_ladders_payload,
    _payload_cache_get_or_build,
    _project_live_pitcher_value,
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

    def test_payload_cache_skips_oversized_payloads(self) -> None:
        build_counter = {"count": 0}

        def build_payload() -> dict:
            build_counter["count"] += 1
            return {"value": "x" * 200}

        with patch.object(flask_frontend, "_PAYLOAD_CACHE_MAX_ITEM_BYTES", 32):
            first = _payload_cache_get_or_build(
                "cards_api_context",
                "oversized-test",
                max_age_seconds=60.0,
                builder=build_payload,
            )
            second = _payload_cache_get_or_build(
                "cards_api_context",
                "oversized-test",
                max_age_seconds=60.0,
                builder=build_payload,
            )

        self.assertEqual("x" * 200, first["value"])
        self.assertEqual("x" * 200, second["value"])
        self.assertEqual(2, build_counter["count"])
        self.assertNotIn(("cards_api_context", "oversized-test"), flask_frontend._PAYLOAD_CACHE)

    def test_payload_cache_evicts_old_entries_when_byte_budget_is_exceeded(self) -> None:
        first_payload = {"value": "a" * 120}
        second_payload = {"value": "b" * 120}
        size_budget = flask_frontend._estimate_payload_cache_size_bytes(first_payload) + flask_frontend._estimate_payload_cache_size_bytes(second_payload) - 1

        with patch.object(flask_frontend, "_PAYLOAD_CACHE_MAX_ENTRIES", 8), patch.object(
            flask_frontend,
            "_PAYLOAD_CACHE_MAX_BYTES",
            size_budget,
        ), patch.object(flask_frontend, "_PAYLOAD_CACHE_MAX_ITEM_BYTES", 4096):
            _payload_cache_get_or_build(
                "cards_api_context",
                "byte-budget-first",
                max_age_seconds=60.0,
                builder=lambda: first_payload,
            )
            _payload_cache_get_or_build(
                "cards_api_context",
                "byte-budget-second",
                max_age_seconds=60.0,
                builder=lambda: second_payload,
            )

        self.assertNotIn(("cards_api_context", "byte-budget-first"), flask_frontend._PAYLOAD_CACHE)
        self.assertIn(("cards_api_context", "byte-budget-second"), flask_frontend._PAYLOAD_CACHE)

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
        ladders_result = {"generatedAt": "2026-05-04T12:00:00", "groups": {"pitcher": {}}}

        with patch.object(flask_frontend, "_publish_season_manifests", return_value=republish_result) as publish_mock, patch.object(
            flask_frontend,
            "_purge_current_day_season_frontend_artifacts",
            return_value=purge_result,
        ) as purge_mock, patch.object(
            flask_frontend,
            "write_current_day_season_frontend_artifacts",
            return_value=frontend_result,
        ) as frontend_mock, patch.object(
            flask_frontend,
            "write_daily_ladders_artifact",
            return_value=ladders_result,
        ) as ladders_mock:
            result = flask_frontend._refresh_current_day_market_backed_artifacts(
                "2026-05-04",
                season=2026,
                betting_profile="retuned",
            )

        publish_mock.assert_called_once()
        purge_mock.assert_called_once_with(2026, "2026-05-04", betting_profile="retuned")
        frontend_mock.assert_called_once_with(2026, "2026-05-04", betting_profile="retuned")
        ladders_mock.assert_called_once_with("2026-05-04")
        self.assertEqual(result["republish"], republish_result)
        self.assertIsNone(result["republish_error"])
        self.assertEqual(result["purged_frontend"], purge_result)
        self.assertEqual(result["frontend"], frontend_result)
        self.assertEqual(result["daily_ladders"], ladders_result)

    def test_attach_cards_starter_ladder_badges_can_seed_missing_probables_from_ladders(self) -> None:
        cards = [
            {
                "gamePk": 823143,
                "probable": {"away": None, "home": None},
            }
        ]
        daily_ladders = {
            "groups": {
                "pitcher": {
                    "strikeouts": {
                        "rows": [
                            {
                                "gamePk": 823143,
                                "side": "home",
                                "pitcherId": 669302,
                                "pitcherName": "Logan Gilbert",
                                "marketLine": 6.5,
                                "overLineProb": 0.345,
                                "ladder": [
                                    {"total": 7, "hitProb": 0.32},
                                    {"total": 8, "hitProb": 0.21},
                                ],
                            }
                        ]
                    },
                    "outs": {
                        "rows": [
                            {
                                "gamePk": 823143,
                                "side": "home",
                                "pitcherId": 669302,
                                "pitcherName": "Logan Gilbert",
                                "marketLine": 17.5,
                                "overLineProb": 0.51,
                                "ladder": [
                                    {"total": 18, "hitProb": 0.51},
                                    {"total": 21, "hitProb": 0.23},
                                ],
                            }
                        ]
                    },
                }
            }
        }

        _attach_cards_starter_ladder_badges(cards, daily_ladders)

        home_probable = cards[0]["probable"]["home"]
        self.assertIsInstance(home_probable, dict)
        self.assertEqual(home_probable["id"], 669302)
        self.assertEqual(home_probable["fullName"], "Logan Gilbert")
        self.assertGreaterEqual(len(home_probable.get("ladderBadges") or []), 1)

    def test_attach_cards_starter_ladder_badges_can_fill_missing_market_line_from_live_context(self) -> None:
        cards = [
            {
                "gamePk": 824039,
                "probable": {
                    "home": {"id": 663999, "fullName": "José Soriano"},
                },
            }
        ]
        daily_ladders = {
            "groups": {
                "pitcher": {
                    "strikeouts": {
                        "rows": [
                            {
                                "gamePk": 824039,
                                "side": "home",
                                "pitcherId": 663999,
                                "pitcherName": "José Soriano",
                                "marketLine": None,
                                "ladder": [
                                    {"total": 6, "hitProb": 0.41},
                                    {"total": 7, "hitProb": 0.28},
                                ],
                            }
                        ]
                    },
                    "outs": {
                        "rows": [
                            {
                                "gamePk": 824039,
                                "side": "home",
                                "pitcherId": 663999,
                                "pitcherName": "José Soriano",
                                "marketLine": None,
                                "ladder": [
                                    {"total": 18, "hitProb": 0.26},
                                    {"total": 19, "hitProb": 0.18},
                                ],
                            }
                        ]
                    },
                }
            }
        }
        pitcher_market_ctx = {
            "displayLines": {
                flask_frontend.normalize_pitcher_name("José Soriano"): {
                    "strikeouts": {"line": 6.5},
                    "outs": {"line": 17.5},
                }
            }
        }

        _attach_cards_starter_ladder_badges(cards, daily_ladders, pitcher_market_ctx)

        home_probable = cards[0]["probable"]["home"]
        badges = home_probable.get("ladderBadges") or []
        self.assertEqual([badge.get("stat") for badge in badges], ["strikeouts"])
        self.assertEqual(badges[0].get("label"), "K up to 7")

    def test_project_live_pitcher_value_keeps_meaningful_remaining_runway_midgame(self) -> None:
        projection = _project_live_pitcher_value(
            prop="strikeouts",
            team_side="away",
            actual_value=4,
            model_mean=6.5,
            progress_fraction=0.48,
            market_line=5.5,
            actual_row={"BF": 21, "P": 76, "SO": 4, "OUTS": 15},
            model_row={"batters_faced_mean": 26, "pitches_mean": 97},
            pitcher_profile={"id": 1, "stamina_pitches": 98},
            current_profile={"id": 1},
            bullpen_profiles=[{"availability_mult": 0.95, "leverage_skill": 0.62}],
            snapshot={
                "current": {"inning": 6, "halfInning": "top", "count": {"outs": 0}},
                "teams": {
                    "away": {"totals": {"R": 2}},
                    "home": {"totals": {"R": 1}},
                },
            },
        )

        self.assertIsNotNone(projection)
        self.assertGreater(float(projection), 5.0)

    def test_project_live_pitcher_outs_skips_correction_too_early(self) -> None:
        correction_artifact = {
            "feature_names": ["line_gap", "model_gap", "actual_so_far", "inning", "game_outs", "progress_fraction", "game_state_parsed_flag"],
            "model": {
                "intercept": -8.0,
                "weights": {},
                "feature_centers": {},
                "feature_scales": {},
            },
        }

        with (
            patch.object(flask_frontend, "_is_live_pitcher_outs_correction_enabled", return_value=True),
            patch.object(flask_frontend, "_load_live_pitcher_outs_correction_artifact", return_value=correction_artifact),
        ):
            projection = _project_live_pitcher_value(
                prop="outs",
                team_side="home",
                actual_value=2,
                model_mean=15.351,
                progress_fraction=2.0 / 54.0,
                market_line=16.5,
                actual_row={"BF": 4, "P": 16, "OUTS": 2},
                model_row={"batters_faced_mean": 24.0, "pitches_mean": 92.0},
                pitcher_profile={"id": 1, "stamina_pitches": 95},
                current_profile={"id": 1},
                bullpen_profiles=[{"availability_mult": 0.95, "leverage_skill": 0.62}],
                snapshot={
                    "current": {"inning": 1, "halfInning": "top", "count": {"outs": 2}},
                    "teams": {
                        "away": {"totals": {"R": 0}},
                        "home": {"totals": {"R": 0}},
                    },
                },
            )

        self.assertIsNotNone(projection)
        self.assertGreater(float(projection), 12.0)

    def test_api_cron_refresh_oddsapi_markets_serializes_nested_paths(self) -> None:
        with (
            flask_frontend.app.test_client() as client,
            patch.object(flask_frontend, "_require_cron_auth", return_value=None),
            patch.object(flask_frontend, "_today_iso", return_value="2026-05-05"),
            patch.object(
                flask_frontend,
                "_refresh_oddsapi_markets",
                return_value={
                    "ok": True,
                    "result": {"snapshot_dir": Path("data/daily/snapshots/2026-05-05")},
                },
            ),
            patch.object(
                flask_frontend,
                "_refresh_current_day_market_backed_artifacts",
                return_value={
                    "frontend": {"path": Path("data/daily/season_frontend/example.json")},
                    "daily_ladders": {"path": Path("data/daily/ladders/daily_ladders_2026_05_05.json")},
                },
            ),
        ):
            response = client.get("/api/cron/refresh-oddsapi-markets?date=2026-05-05")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["result"]["snapshot_dir"], "data/daily/snapshots/2026-05-05")
        self.assertEqual(payload["currentDayArtifacts"]["frontend"]["path"], "data/daily/season_frontend/example.json")
        self.assertEqual(payload["currentDayArtifacts"]["daily_ladders"]["path"], "data/daily/ladders/daily_ladders_2026_05_05.json")

    def test_api_cron_cache_usage_reports_payload_cache_bytes(self) -> None:
        with flask_frontend._PAYLOAD_CACHE_LOCK:
            flask_frontend._PAYLOAD_CACHE.clear()
            flask_frontend._PAYLOAD_CACHE[("cards_api", "2026-05-06")] = {
                "signature": None,
                "createdAt": 100.0,
                "accessedAt": 120.0,
                "sizeBytes": 321,
                "payload": {"ok": True},
            }

        try:
            with (
                flask_frontend.app.test_client() as client,
                patch.object(flask_frontend, "_require_cron_auth", return_value=None),
            ):
                response = client.get("/api/cron/cache-usage")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(1, payload["payload_cache"]["entries"])
            self.assertEqual(321, payload["payload_cache"]["total_bytes"])
            self.assertEqual("cards_api", payload["payload_cache"]["entries_by_cache"][0]["cache"])
            self.assertEqual(321, payload["payload_cache"]["largest_entries"][0]["size_bytes"])
        finally:
            with flask_frontend._PAYLOAD_CACHE_LOCK:
                flask_frontend._PAYLOAD_CACHE.clear()

    def test_pitcher_ladder_market_context_merges_next_day_live_rollover_lines(self) -> None:
        current_path = Path("data/market/oddsapi/oddsapi_pitcher_props_2026_05_04.json")
        rollover_path = Path("data/market/oddsapi/oddsapi_pitcher_props_2026_05_05.json")

        def fake_load_json(path: Path):
            path_str = str(path).replace("\\", "/")
            if path_str.endswith("2026_05_04.json"):
                return {"mode": "live", "pitcher_props": {"davis martin": {"outs": {"line": 17.5}}}}
            if path_str.endswith("2026_05_05.json"):
                return {"mode": "live", "pitcher_props": {"jose soriano": {"outs": {"line": 17.5}, "strikeouts": {"line": 6.5}}}}
            return None

        with (
            patch.object(flask_frontend, "_resolve_oddsapi_market_file", side_effect=[current_path, rollover_path]),
            patch.object(flask_frontend, "_load_json_file", side_effect=fake_load_json),
            patch.object(flask_frontend, "_resolve_pregame_oddsapi_market_file", return_value=None),
            patch.object(flask_frontend, "_resolve_earliest_archived_oddsapi_market_file", return_value=None),
            patch.object(flask_frontend, "_first_seen_pitcher_market_lines_from_registry", return_value={}),
            patch.object(flask_frontend, "_schedule_status_counts", return_value={"known": True, "live": 3}),
            patch.object(flask_frontend, "_is_current_local_date", return_value=True),
        ):
            context = _load_pitcher_ladder_market_context("2026-05-04")

        self.assertIn("jose soriano", context["currentLines"])
        self.assertIn("jose soriano", context["displayLines"])
        self.assertEqual(context["currentLines"]["jose soriano"]["outs"]["line"], 17.5)

    def test_hitter_ladder_market_context_merges_next_day_live_rollover_lines(self) -> None:
        current_path = Path("data/market/oddsapi/oddsapi_hitter_props_2026_05_04.json")
        rollover_path = Path("data/market/oddsapi/oddsapi_hitter_props_2026_05_05.json")

        def fake_load_json(path: Path):
            path_str = str(path).replace("\\", "/")
            if path_str.endswith("2026_05_04.json"):
                return {"mode": "live", "hitter_props": {"mike trout": {"batter_hits": {"line": 1.5}}}}
            if path_str.endswith("2026_05_05.json"):
                return {"mode": "live", "hitter_props": {"shohei ohtani": {"batter_hits": {"line": 1.5}, "batter_total_bases": {"line": 2.5}}}}
            return None

        with (
            patch.object(flask_frontend, "_resolve_oddsapi_market_file", side_effect=[current_path, rollover_path]),
            patch.object(flask_frontend, "_load_json_file", side_effect=fake_load_json),
            patch.object(flask_frontend, "_resolve_pregame_oddsapi_market_file", return_value=None),
            patch.object(flask_frontend, "_resolve_earliest_archived_oddsapi_market_file", return_value=None),
            patch.object(flask_frontend, "_schedule_status_counts", return_value={"known": True, "live": 2}),
            patch.object(flask_frontend, "_is_current_local_date", return_value=True),
        ):
            context = _load_hitter_ladder_market_context("2026-05-04")

        self.assertIn("shohei ohtani", context["currentLines"])
        self.assertIn("shohei ohtani", context["displayLines"])
        self.assertEqual(context["currentLines"]["shohei ohtani"]["batter_total_bases"]["line"], 2.5)

    def test_game_line_market_context_merges_next_day_live_rollover_rows(self) -> None:
        current_path = Path("data/market/oddsapi/oddsapi_game_lines_2026_05_04.json")
        rollover_path = Path("data/market/oddsapi/oddsapi_game_lines_2026_05_05.json")

        def fake_load_json(path: Path):
            path_str = str(path).replace("\\", "/")
            if path_str.endswith("2026_05_04.json"):
                return {
                    "mode": "live",
                    "games": [
                        {"event_id": "1", "away_team": "CWS", "home_team": "LAA", "markets": {"total": {"line": 8.5}}}
                    ],
                }
            if path_str.endswith("2026_05_05.json"):
                return {
                    "mode": "live",
                    "games": [
                        {"event_id": "2", "away_team": "SD", "home_team": "SF", "markets": {"total": {"line": 7.5}}}
                    ],
                }
            return None

        with (
            patch.object(flask_frontend, "_resolve_oddsapi_market_file", side_effect=[current_path, rollover_path]),
            patch.object(flask_frontend, "_load_json_file", side_effect=fake_load_json),
            patch.object(flask_frontend, "_resolve_pregame_oddsapi_market_file", return_value=None),
            patch.object(flask_frontend, "_resolve_earliest_archived_oddsapi_market_file", return_value=None),
            patch.object(flask_frontend, "_schedule_status_counts", return_value={"known": True, "live": 2}),
            patch.object(flask_frontend, "_is_current_local_date", return_value=True),
        ):
            context = _load_game_line_market_context("2026-05-04")

        event_ids = {str(row.get("event_id") or "") for row in (context.get("currentRows") or [])}
        self.assertEqual(event_ids, {"1", "2"})
        self.assertIn("2026_05_05", str(context.get("displaySource") or ""))

    def test_prebuilt_pitcher_ladders_payload_backfills_stale_market_fields_from_live_context(self) -> None:
        artifact_doc = {
            "groups": {
                "pitcher": {
                    "strikeouts": {
                        "rows": [
                            {
                                "gamePk": 824039,
                                "pitcherId": 663999,
                                "pitcherName": "José Soriano",
                                "side": "home",
                                "simCount": 1000,
                                "marketLine": None,
                                "marketLinesByStat": [],
                                "ladder": [
                                    {"total": 6, "exactCount": 207, "hitProb": 0.207},
                                    {"total": 7, "exactCount": 120, "hitProb": 0.12},
                                ],
                            }
                        ],
                        "summary": {"games": 1},
                    }
                }
            },
            "generatedAt": "2026-05-04T23:59:00Z",
        }
        market_ctx = {
            "currentMode": "live",
            "displayLines": {
                flask_frontend.normalize_pitcher_name("José Soriano"): {
                    "strikeouts": {"line": 6.5, "over_odds": -110, "under_odds": -120},
                    "outs": {"line": 17.5, "over_odds": -150, "under_odds": 105},
                }
            },
            "currentLines": {
                flask_frontend.normalize_pitcher_name("José Soriano"): {
                    "strikeouts": {"line": 6.5, "over_odds": -110, "under_odds": -120},
                    "outs": {"line": 17.5, "over_odds": -150, "under_odds": 105},
                }
            },
            "pregameLines": {},
        }

        with (
            patch.object(flask_frontend, "_load_daily_ladders_artifact", return_value=(Path("data/daily/ladders/daily_ladders_2026_05_04.json"), artifact_doc)),
            patch.object(flask_frontend, "_load_pitcher_ladder_market_context", return_value=market_ctx),
        ):
            payload = _prebuilt_pitcher_ladders_payload("2026-05-04", "strikeouts", "edge", selected_game_value="824039")

        self.assertIsInstance(payload, dict)
        rows = payload.get("rows") or []
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("marketLine"), 6.5)
        self.assertGreaterEqual(len(rows[0].get("marketLinesByStat") or []), 2)


if __name__ == "__main__":
    unittest.main()