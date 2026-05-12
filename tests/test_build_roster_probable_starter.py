import unittest
from unittest.mock import patch

import sim_engine.data.build_roster as build_roster
from sim_engine.data.build_roster import build_team_roster
from sim_engine.models import Team


class BuildRosterProbableStarterTests(unittest.TestCase):
    def test_probable_starter_is_not_filtered_by_stale_injury_status(self) -> None:
        probable_pitcher_id = 605483
        fallback_pitcher_id = 808967
        hitter_ids = list(range(1001, 1010))
        roster_entries = [
            {
                "person": {"id": probable_pitcher_id, "fullName": "Blake Snell"},
                "position": {"abbreviation": "P"},
                "status": {"code": "IL10", "description": "Injured List"},
            },
            {
                "person": {"id": fallback_pitcher_id, "fullName": "Fallback Pitcher"},
                "position": {"abbreviation": "P"},
                "status": {"code": "A", "description": "Active"},
            },
        ] + [
            {
                "person": {"id": hitter_id, "fullName": f"Hitter {hitter_id}"},
                "position": {"abbreviation": "1B"},
                "status": {"code": "A", "description": "Active"},
            }
            for hitter_id in hitter_ids
        ]

        def _fake_fetch_person(_client, person_id):
            if int(person_id) in {probable_pitcher_id, fallback_pitcher_id}:
                return {
                    "primaryPosition": {"abbreviation": "P"},
                    "batSide": {"code": "R"},
                    "pitchHand": {"code": "L" if int(person_id) == probable_pitcher_id else "R"},
                    "fullName": "Blake Snell" if int(person_id) == probable_pitcher_id else "Fallback Pitcher",
                }
            return {
                "primaryPosition": {"abbreviation": "1B"},
                "batSide": {"code": "R"},
                "pitchHand": {"code": "R"},
                "fullName": f"Hitter {person_id}",
            }

        def _fake_pitching_stats(_client, person_id, _season):
            if int(person_id) == probable_pitcher_id:
                return {
                    "gamesStarted": 5,
                    "gamesPitched": 5,
                    "gamesFinished": 0,
                    "saves": 0,
                    "battersFaced": 120,
                    "pitchesThrown": 460,
                    "strikeOuts": 35,
                    "baseOnBalls": 10,
                    "hitBatsmen": 1,
                    "homeRuns": 4,
                    "hits": 22,
                }
            return {
                "gamesStarted": 8,
                "gamesPitched": 12,
                "gamesFinished": 0,
                "saves": 0,
                "battersFaced": 240,
                "pitchesThrown": 780,
                "strikeOuts": 60,
                "baseOnBalls": 20,
                "hitBatsmen": 2,
                "homeRuns": 8,
                "hits": 50,
            }

        def _fake_hitting_stats(_client, _person_id, _season):
            return {
                "plateAppearances": 120,
                "strikeOuts": 24,
                "baseOnBalls": 10,
                "hitByPitch": 1,
                "homeRuns": 4,
                "hits": 30,
                "doubles": 6,
                "triples": 1,
                "stolenBases": 2,
                "caughtStealing": 1,
            }

        team = Team(team_id=119, name="Los Angeles Dodgers", abbreviation="LAD")

        with patch.object(build_roster, "fetch_person", side_effect=_fake_fetch_person), patch.object(
            build_roster, "fetch_person_season_pitching", side_effect=_fake_pitching_stats
        ), patch.object(build_roster, "fetch_person_season_hitting", side_effect=_fake_hitting_stats), patch.object(
            build_roster, "_load_manager_tendencies_anykey", return_value={}
        ), patch.object(build_roster, "_apply_statcast_features_to_pitcher", return_value=False), patch.object(
            build_roster, "_apply_statcast_features_to_batter", return_value=False
        ), patch.object(build_roster, "_apply_statcast_quality_to_pitcher", return_value=None), patch.object(
            build_roster, "_apply_statcast_quality_to_batter", return_value=None
        ):
            roster = build_team_roster(
                client=object(),
                team=team,
                season=2026,
                probable_pitcher_id=probable_pitcher_id,
                roster_entries=roster_entries,
                exclude_injured=True,
                enable_batter_platoon=False,
                enable_pitcher_platoon=False,
                use_profile_cache=False,
            )

        self.assertEqual(probable_pitcher_id, roster.lineup.pitcher.player.mlbam_id)
        self.assertEqual("probable", getattr(roster.lineup.pitcher, "starter_selection_source", None))
        self.assertEqual(probable_pitcher_id, getattr(roster.lineup.pitcher, "starter_requested_id", None))

    def test_two_way_probable_starter_builds_pitcher_and_hitter_profiles(self) -> None:
        probable_pitcher_id = 660271
        fallback_pitcher_id = 808967
        hitter_ids = [probable_pitcher_id] + list(range(2001, 2009))
        roster_entries = [
            {
                "person": {"id": probable_pitcher_id, "fullName": "Shohei Ohtani"},
                "position": {"abbreviation": "P"},
                "status": {"code": "A", "description": "Active"},
            },
            {
                "person": {"id": fallback_pitcher_id, "fullName": "Fallback Pitcher"},
                "position": {"abbreviation": "P"},
                "status": {"code": "A", "description": "Active"},
            },
        ] + [
            {
                "person": {"id": hitter_id, "fullName": ("Shohei Ohtani" if hitter_id == probable_pitcher_id else f"Hitter {hitter_id}")},
                "position": {"abbreviation": ("DH" if hitter_id == probable_pitcher_id else "1B")},
                "status": {"code": "A", "description": "Active"},
            }
            for hitter_id in hitter_ids
        ]

        def _fake_fetch_person(_client, person_id):
            if int(person_id) == probable_pitcher_id:
                return {
                    "primaryPosition": {"abbreviation": "DH"},
                    "batSide": {"code": "L"},
                    "pitchHand": {"code": "R"},
                    "fullName": "Shohei Ohtani",
                }
            if int(person_id) == fallback_pitcher_id:
                return {
                    "primaryPosition": {"abbreviation": "P"},
                    "batSide": {"code": "R"},
                    "pitchHand": {"code": "R"},
                    "fullName": "Fallback Pitcher",
                }
            return {
                "primaryPosition": {"abbreviation": "1B"},
                "batSide": {"code": "R"},
                "pitchHand": {"code": "R"},
                "fullName": f"Hitter {person_id}",
            }

        def _fake_pitching_stats(_client, person_id, _season):
            if int(person_id) == probable_pitcher_id:
                return {
                    "gamesStarted": 4,
                    "gamesPitched": 4,
                    "gamesFinished": 0,
                    "saves": 0,
                    "battersFaced": 100,
                    "pitchesThrown": 380,
                    "strikeOuts": 30,
                    "baseOnBalls": 8,
                    "hitBatsmen": 1,
                    "homeRuns": 3,
                    "hits": 18,
                }
            return {
                "gamesStarted": 8,
                "gamesPitched": 10,
                "gamesFinished": 0,
                "saves": 0,
                "battersFaced": 220,
                "pitchesThrown": 720,
                "strikeOuts": 55,
                "baseOnBalls": 18,
                "hitBatsmen": 1,
                "homeRuns": 7,
                "hits": 44,
            }

        def _fake_hitting_stats(_client, person_id, _season):
            if int(person_id) == probable_pitcher_id:
                return {
                    "plateAppearances": 140,
                    "strikeOuts": 30,
                    "baseOnBalls": 12,
                    "hitByPitch": 2,
                    "homeRuns": 8,
                    "hits": 36,
                    "doubles": 7,
                    "triples": 1,
                    "stolenBases": 4,
                    "caughtStealing": 1,
                }
            return {
                "plateAppearances": 120,
                "strikeOuts": 24,
                "baseOnBalls": 10,
                "hitByPitch": 1,
                "homeRuns": 4,
                "hits": 30,
                "doubles": 6,
                "triples": 1,
                "stolenBases": 2,
                "caughtStealing": 1,
            }

        team = Team(team_id=119, name="Los Angeles Dodgers", abbreviation="LAD")

        with patch.object(build_roster, "fetch_person", side_effect=_fake_fetch_person), patch.object(
            build_roster, "fetch_person_season_pitching", side_effect=_fake_pitching_stats
        ), patch.object(build_roster, "fetch_person_season_hitting", side_effect=_fake_hitting_stats), patch.object(
            build_roster, "_load_manager_tendencies_anykey", return_value={}
        ), patch.object(build_roster, "_apply_statcast_features_to_pitcher", return_value=False), patch.object(
            build_roster, "_apply_statcast_features_to_batter", return_value=False
        ), patch.object(build_roster, "_apply_statcast_quality_to_pitcher", return_value=None), patch.object(
            build_roster, "_apply_statcast_quality_to_batter", return_value=None
        ):
            roster = build_team_roster(
                client=object(),
                team=team,
                season=2026,
                probable_pitcher_id=probable_pitcher_id,
                roster_entries=roster_entries,
                exclude_injured=True,
                confirmed_lineup_ids=hitter_ids,
                enable_batter_platoon=False,
                enable_pitcher_platoon=False,
                use_profile_cache=False,
            )

        self.assertEqual(probable_pitcher_id, roster.lineup.pitcher.player.mlbam_id)
        batter_ids = [b.player.mlbam_id for b in roster.lineup.batters]
        self.assertIn(probable_pitcher_id, batter_ids)


if __name__ == "__main__":
    unittest.main()