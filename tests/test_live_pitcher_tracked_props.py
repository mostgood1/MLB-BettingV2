import unittest

from tools.web.flask_frontend import _prop_lens_rows, normalize_pitcher_name


class LivePitcherTrackedPropsTests(unittest.TestCase):
    def test_tracked_pitcher_props_use_pitcher_model_when_sim_boxscore_missing(self) -> None:
        card = {
            "status": {"abstract": "Live"},
            "away": {"abbr": "STL", "name": "Cardinals"},
            "home": {"abbr": "MIL", "name": "Brewers"},
            "markets": {
                "pitcherProps": [],
                "extraPitcherProps": [
                    {
                        "market": "pitcher_props",
                        "prop": "outs",
                        "pitcher_name": "Freddy Peralta",
                        "team_side": "home",
                        "market_line": 17.5,
                        "selection": "under",
                        "odds": 105,
                        "edge": 0.01,
                        "outs_mean": 15.166,
                    }
                ],
            },
        }
        snapshot = {
            "status": {"abstractGameState": "Live"},
            "current": {"inning": 6, "halfInning": "top", "count": {"outs": 0}},
            "teams": {
                "away": {"boxscore": {"pitching": []}},
                "home": {
                    "boxscore": {
                        "pitching": [
                            {
                                "name": "Freddy Peralta",
                                "IP": "5.0",
                                "BF": 20,
                                "P": 65,
                                "SO": 6,
                                "BB": 1,
                                "H": 4,
                            }
                        ]
                    }
                },
            },
        }
        sim_context = {
            "propModels": {
                "pitchers": {
                    normalize_pitcher_name("Freddy Peralta"): {
                        "team_side": "home",
                        "model": {
                            "outs_mean": 15.166,
                            "batters_faced_mean": 24.0,
                            "pitches_mean": 91.0,
                        },
                    }
                }
            },
            "boxscore": {"teams": {"home": {"pitching": []}, "away": {"pitching": []}}},
            "roster_snapshot": {},
        }

        rows = _prop_lens_rows(card, snapshot, sim_context)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["playerName"], "Freddy Peralta")
        self.assertEqual(row["marketLabel"], "outs")
        self.assertGreater(row["liveProjection"], 15.0)
        self.assertLess(row["liveProjection"], 18.5)

    def test_tracked_pitcher_strikeouts_use_pitcher_model_when_sim_boxscore_missing(self) -> None:
        card = {
            "status": {"abstract": "Live"},
            "away": {"abbr": "SEA", "name": "Mariners"},
            "home": {"abbr": "SF", "name": "Giants"},
            "markets": {
                "pitcherProps": [],
                "extraPitcherProps": [
                    {
                        "market": "pitcher_props",
                        "prop": "strikeouts",
                        "pitcher_name": "Logan Webb",
                        "team_side": "home",
                        "market_line": 7.5,
                        "selection": "under",
                        "odds": 100,
                        "edge": 0.01,
                        "outs_mean": 18.0,
                        "so_mean": 3.958,
                    }
                ],
            },
        }
        snapshot = {
            "status": {"abstractGameState": "Live"},
            "current": {"inning": 7, "halfInning": "top", "count": {"outs": 0}},
            "teams": {
                "away": {"totals": {"R": 2}, "boxscore": {"pitching": []}},
                "home": {
                    "totals": {"R": 4},
                    "boxscore": {
                        "pitching": [
                            {
                                "name": "Logan Webb",
                                "IP": "6.0",
                                "BF": 24,
                                "P": 86,
                                "SO": 6,
                                "BB": 1,
                                "H": 5,
                            }
                        ]
                    },
                    "starter": {"name": "Logan Webb"},
                },
            },
        }
        sim_context = {
            "propModels": {
                "pitchers": {
                    normalize_pitcher_name("Logan Webb"): {
                        "team_side": "home",
                        "model": {
                            "outs_mean": 18.0,
                            "so_mean": 3.958,
                            "batters_faced_mean": 25.0,
                            "pitches_mean": 92.0,
                        },
                    }
                }
            },
            "boxscore": {"teams": {"home": {"pitching": []}, "away": {"pitching": []}}},
            "roster_snapshot": {},
        }

        rows = _prop_lens_rows(card, snapshot, sim_context)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["playerName"], "Logan Webb")
        self.assertEqual(row["marketLabel"], "strikeouts")
        self.assertGreaterEqual(row["liveProjection"], 6.0)
        self.assertLess(row["liveProjection"], 7.5)


if __name__ == "__main__":
    unittest.main()