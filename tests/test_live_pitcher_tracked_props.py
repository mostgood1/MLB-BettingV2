import unittest

from tools.web.flask_frontend import (
    _PITCHER_LADDER_PROPS,
    _project_live_pitcher_value_details,
    _prop_lens_rows,
    normalize_pitcher_name,
)


class LivePitcherTrackedPropsTests(unittest.TestCase):
    def test_opening_all_pitcher_props_use_pitcher_specific_path_when_boxscore_row_missing(self) -> None:
        snapshot = {
            "current": {"inning": 1, "halfInning": "bottom", "count": {"outs": 0}},
            "teams": {
                "away": {"totals": {"R": 0}},
                "home": {"totals": {"R": 0}},
            },
        }
        model_row = {
            "outs_mean": 16.2,
            "so_mean": 4.1,
            "hits_mean": 5.4,
            "walks_mean": 2.1,
            "er_mean": 2.5,
            "pitches_mean": 92.0,
            "batters_faced_mean": 24.5,
        }

        for prop_key, cfg in _PITCHER_LADDER_PROPS.items():
            mean_key = str(cfg.get("mean_key") or "")
            with self.subTest(prop=prop_key):
                details = _project_live_pitcher_value_details(
                    prop=prop_key,
                    team_side="away",
                    actual_value=0.0,
                    model_mean=model_row[mean_key],
                    progress_fraction=0.0,
                    actual_row=None,
                    model_row=model_row,
                    snapshot=snapshot,
                )
                self.assertIsInstance(details, dict)
                self.assertEqual("pitcher_live_context", details["debug"]["path"])
                self.assertTrue(details["debug"]["synthesized_opening_row"])
                self.assertAlmostEqual(model_row[mean_key], details["projection"], places=3)

    def test_opening_pitcher_strikeouts_keep_pitcher_baseline_when_boxscore_row_missing(self) -> None:
        card = {
            "status": {"abstract": "Live"},
            "away": {"abbr": "PIT", "name": "Pirates"},
            "home": {"abbr": "TEX", "name": "Rangers"},
            "markets": {
                "pitcherProps": [],
                "extraPitcherProps": [
                    {
                        "market": "pitcher_props",
                        "prop": "strikeouts",
                        "pitcher_name": "Bubba Chandler",
                        "team_side": "away",
                        "market_line": 4.5,
                        "selection": "under",
                        "odds": -136,
                        "edge": 0.01,
                        "so_mean": 4.1,
                    }
                ],
            },
        }
        snapshot = {
            "status": {"abstractGameState": "Live"},
            "current": {"inning": 1, "halfInning": "bottom", "count": {"outs": 0}},
            "teams": {
                "away": {
                    "starter": {"name": "Bubba Chandler"},
                    "totals": {"R": 0},
                    "boxscore": {"pitching": []},
                },
                "home": {"totals": {"R": 0}, "boxscore": {"pitching": []}},
            },
        }
        sim_context = {
            "propModels": {
                "pitchers": {
                    normalize_pitcher_name("Bubba Chandler"): {
                        "team_side": "away",
                        "model": {
                            "so_mean": 4.1,
                            "outs_mean": 16.2,
                            "batters_faced_mean": 24.5,
                            "pitches_mean": 92.0,
                        },
                    }
                }
            },
            "boxscore": {"teams": {"home": {"pitching": []}, "away": {"pitching": []}}},
            "roster_snapshot": {},
        }

        rows = _prop_lens_rows(card, snapshot, sim_context)
        self.assertEqual(1, len(rows))
        row = rows[0]
        self.assertEqual("Bubba Chandler", row["playerName"])
        self.assertEqual("strikeouts", row["marketLabel"])
        self.assertGreater(row["liveProjection"], 3.7)
        self.assertLess(row["liveProjection"], 4.3)

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