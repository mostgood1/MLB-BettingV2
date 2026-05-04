import unittest

from tools.web.flask_frontend import (
    _cards_list_from_sources,
    _supplement_recos_by_game_with_betting_games,
)


class CardsExtraMarketSupportTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()