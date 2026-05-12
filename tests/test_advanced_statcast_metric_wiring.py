import unittest
from pathlib import Path
import random
from types import SimpleNamespace
from unittest.mock import patch

import sim_engine.data.build_roster as build_roster
from sim_engine.data.build_roster import _apply_cached_statcast_pitch_splits, _apply_statsapi_pitch_arsenal, _apply_statcast_pitch_count_stamina_adjustment, _derive_stamina_pitches_from_season_stats, _enrich_statcast_quality_mult
from sim_engine.simulate import simulate_game, _starter_effective_hook, _starter_matchup_hook_adjustment, _statcast_shape_rate_mults
from sim_engine.pitch_model import PitchModelConfig, simulate_pitch
from sim_engine.models import (
    BatterProfile,
    GameConfig,
    Handedness,
    Lineup,
    ManagerProfile,
    PitchCall,
    PitchType,
    PitcherProfile,
    Player,
    Team,
    TeamRoster,
)
from tools.web.flask_frontend import _hitter_ladder_matchup_summary, _pitcher_ladder_matchup_summary
from tools.daily_update_multi_profile import (
    _trim_reason_list,
    _hitter_hr_target_support,
    _hitter_statcast_quality_reason,
    _pitcher_statcast_quality_reason,
)


class AdvancedStatcastMetricWiringTests(unittest.TestCase):
    def test_reliever_stamina_estimate_never_returns_none(self) -> None:
        stamina = _derive_stamina_pitches_from_season_stats(
            {
                "pitchesThrown": 300,
                "gamesPitched": 10,
                "gamesStarted": 0,
            }
        )

        self.assertEqual(27, stamina)

    def test_cached_statcast_pitch_splits_apply_to_nonstarter_pitchers(self) -> None:
        pitcher = PitcherProfile(
            player=Player(
                mlbam_id=4401,
                full_name="Test Reliever",
                primary_position="P",
                bat_side=Handedness.R,
                throw_side=Handedness.R,
            ),
            arsenal={PitchType.FF: 0.6, PitchType.SL: 0.4},
            role="MR",
        )
        splits = SimpleNamespace(
            source="cache",
            n_pitches=188,
            start_date="2026-03-01",
            end_date="2026-05-10",
            whiff_mult={PitchType.FF: 1.08},
            inplay_mult={PitchType.SL: 0.94},
        )

        with patch.object(build_roster, "fetch_pitcher_pitch_splits", return_value=splits):
            applied = _apply_cached_statcast_pitch_splits(
                pitcher,
                season=2026,
                statcast_cache=object(),
                statcast_ttl_seconds=3600,
            )

        self.assertTrue(applied)
        self.assertEqual("cache", pitcher.statcast_splits_source)
        self.assertEqual(188, pitcher.statcast_splits_n_pitches)
        self.assertEqual({PitchType.FF: 1.08}, pitcher.pitch_type_whiff_mult)
        self.assertEqual({PitchType.SL: 0.94}, pitcher.pitch_type_inplay_mult)

    def test_statsapi_pitch_arsenal_applies_to_nonstarter_pitchers(self) -> None:
        pitcher = PitcherProfile(
            player=Player(
                mlbam_id=4402,
                full_name="Arsenal Reliever",
                primary_position="P",
                bat_side=Handedness.R,
                throw_side=Handedness.R,
            ),
            role="SU",
        )
        arsenal = {PitchType.FF: 0.58, PitchType.SL: 0.42}

        with patch.object(build_roster, "fetch_person_pitch_arsenal", return_value=(arsenal, 244)):
            applied = _apply_statsapi_pitch_arsenal(object(), pitcher, season=2026)

        self.assertTrue(applied)
        self.assertEqual(arsenal, pitcher.arsenal)
        self.assertEqual("statsapi_pitchArsenal", pitcher.arsenal_source)
        self.assertEqual(244, pitcher.arsenal_sample_size)

    @staticmethod
    def _sample_pitch_count_profile(
        *,
        batter_pitch_count_mult: float,
        pitcher_pitch_count_mult: float,
        pa_samples: int = 4000,
    ) -> tuple[float, dict[str, int]]:
        rng = random.Random(20260511)
        cfg = PitchModelConfig()
        terminal_counts = {
            "ball": 0,
            "called_strike": 0,
            "swinging_strike": 0,
            "foul": 0,
            "in_play": 0,
            "hbp": 0,
        }
        total_pitches = 0

        for _ in range(pa_samples):
            balls = 0
            strikes = 0
            while True:
                result = simulate_pitch(
                    rng=rng,
                    cfg=cfg,
                    pitch_type=PitchType.FF,
                    pitcher_whiff_mult=1.0,
                    pitcher_inplay_mult=1.0,
                    weather_hr_mult=1.0,
                    weather_inplay_hit_mult=1.0,
                    weather_xb_share_mult=1.0,
                    park_hr_mult=1.0,
                    park_inplay_hit_mult=1.0,
                    park_xb_share_mult=1.0,
                    umpire_called_strike_mult=1.0,
                    batter_k_rate=0.225,
                    batter_bb_rate=0.085,
                    batter_hbp_rate=0.008,
                    batter_hr_rate=0.038,
                    batter_inplay_hit_rate=0.305,
                    batter_xb_hit_share=0.29,
                    batter_pt_mult=1.0,
                    batter_pt_hr_mult=1.0,
                    batter_triple_share_of_xb=0.12,
                    pitcher_k_rate=0.245,
                    pitcher_bb_rate=0.078,
                    pitcher_hbp_rate=0.008,
                    pitcher_hr_rate=0.031,
                    pitcher_inplay_hit_rate=0.288,
                    count=(balls, strikes),
                    batter_pitch_count_mult=batter_pitch_count_mult,
                    pitcher_pitch_count_mult=pitcher_pitch_count_mult,
                )
                total_pitches += 1

                if result.call == PitchCall.BALL:
                    balls += 1
                    if balls >= 4:
                        terminal_counts["ball"] += 1
                        break
                    continue

                if result.call == PitchCall.CALLED_STRIKE:
                    strikes += 1
                    if strikes >= 3:
                        terminal_counts["called_strike"] += 1
                        break
                    continue

                if result.call == PitchCall.SWINGING_STRIKE:
                    strikes += 1
                    if strikes >= 3:
                        terminal_counts["swinging_strike"] += 1
                        break
                    continue

                if result.call == PitchCall.FOUL:
                    if strikes < 2:
                        strikes += 1
                    if strikes >= 3:
                        terminal_counts["foul"] += 1
                        break
                    continue

                if result.call == PitchCall.IN_PLAY:
                    terminal_counts["in_play"] += 1
                    break

                if result.call == PitchCall.HIT_BY_PITCH:
                    terminal_counts["hbp"] += 1
                    break

        return float(total_pitches) / float(pa_samples), terminal_counts

    @staticmethod
    def _make_batter(player_id: int, name: str, quality: dict[str, float]) -> BatterProfile:
        return BatterProfile(
            player=Player(
                mlbam_id=player_id,
                full_name=name,
                primary_position="OF",
                bat_side=Handedness.R,
                throw_side=Handedness.R,
            ),
            k_rate=0.225,
            bb_rate=0.085,
            hbp_rate=0.008,
            hr_rate=0.038,
            inplay_hit_rate=0.305,
            xb_hit_share=0.29,
            statcast_quality_mult=dict(quality),
        )

    @staticmethod
    def _make_pitcher(player_id: int, name: str) -> PitcherProfile:
        return PitcherProfile(
            player=Player(
                mlbam_id=player_id,
                full_name=name,
                primary_position="P",
                bat_side=Handedness.R,
                throw_side=Handedness.R,
            ),
            k_rate=0.245,
            bb_rate=0.078,
            hbp_rate=0.008,
            hr_rate=0.031,
            inplay_hit_rate=0.288,
            stamina_pitches=110,
            role="SP",
            arsenal={PitchType.FF: 1.0},
        )

    @classmethod
    def _make_roster(cls, *, team_id: int, abbr: str, batter_quality: dict[str, float], pitcher_id: int) -> TeamRoster:
        batters = [
            cls._make_batter(1000 + team_id * 20 + idx, f"{abbr} Batter {idx+1}", batter_quality)
            for idx in range(9)
        ]
        pitcher = cls._make_pitcher(pitcher_id, f"{abbr} Starter")
        return TeamRoster(
            team=Team(team_id=team_id, name=f"Team {abbr}", abbreviation=abbr),
            manager=ManagerProfile(),
            lineup=Lineup(batters=batters, pitcher=pitcher, bullpen=[]),
        )

    @classmethod
    def _aggregate_home_starter_workload(cls, batter_quality: dict[str, float], sims: int = 120) -> tuple[float, float, float]:
        away = cls._make_roster(team_id=1, abbr="AWY", batter_quality=batter_quality, pitcher_id=9101)
        home = cls._make_roster(team_id=2, abbr="HME", batter_quality={}, pitcher_id=9202)
        home_pitcher_id = int(home.lineup.pitcher.player.mlbam_id)

        total_pitches = 0.0
        total_bf = 0.0
        total_outs = 0.0
        for seed in range(sims):
            result = simulate_game(
                away,
                home,
                GameConfig(
                    innings=3,
                    extra_innings=0,
                    allow_ties_after_max_innings=True,
                    rng_seed=20260511 + seed,
                    manager_pitching="off",
                    pitcher_rate_sampling=False,
                ),
            )
            row = dict((result.pitcher_stats or {}).get(home_pitcher_id) or {})
            total_pitches += float(row.get("P") or 0.0)
            total_bf += float(row.get("BF") or 0.0)
            total_outs += float(row.get("OUTS") or 0.0)

        mean_pitches = total_pitches / float(sims)
        mean_bf = total_bf / float(sims)
        mean_outs = total_outs / float(sims)
        return mean_pitches, (mean_pitches / max(1.0, mean_bf)), mean_outs

    def test_enrich_statcast_quality_mult_carries_raw_shape_metrics(self) -> None:
        overall = {
            "csw_rate": 0.301,
            "zone_rate": 0.487,
            "chase_swing_rate": 0.274,
            "contact_rate": 0.781,
            "xwoba": 0.389,
            "ev_mean": 92.1,
            "ev_max": 111.8,
            "la_mean": 18.4,
            "pulled_air_rate": 0.168,
            "sweet_spot_rate": 0.374,
            "hardhit_rate": 0.462,
            "barrel_rate": 0.142,
            "pitch_quality": {
                "velo_mean": 95.2,
                "extension_mean": 6.5,
            },
        }
        enriched = _enrich_statcast_quality_mult({"k": 1.04, "bb": 0.98}, overall)
        self.assertEqual(enriched["k"], 1.04)
        self.assertAlmostEqual(enriched["csw_rate"], 0.301)
        self.assertAlmostEqual(enriched["barrel_rate"], 0.142)
        self.assertAlmostEqual(enriched["pitch_velo_mean"], 95.2)
        self.assertAlmostEqual(enriched["pitch_extension_mean"], 6.5)

    def test_statcast_features_blend_current_season_with_prior_signal(self) -> None:
        batter = BatterProfile(
            player=Player(
                mlbam_id=7001,
                full_name="Blend Test Batter",
                primary_position="OF",
                bat_side=Handedness.R,
                throw_side=Handedness.R,
            ),
            k_rate=0.22,
            bb_rate=0.08,
            hbp_rate=0.008,
            hr_rate=0.03,
            inplay_hit_rate=0.28,
            xb_hit_share=0.25,
        )
        current = {
            "batters": {
                "7001": {
                    "id": 7001,
                    "overall": {
                        "pitches": 120,
                        "inplay": 12,
                        "bip_ev": 12,
                        "barrel_rate": 0.0,
                        "hardhit_rate": 0.33,
                        "xwoba": 0.341,
                    },
                    "mult_overall": {"k": 1.01, "bb": 1.0, "hr": 1.0, "inplay": 1.0},
                }
            }
        }
        prior = {
            "batters": {
                "7001": {
                    "id": 7001,
                    "overall": {
                        "pitches": 620,
                        "inplay": 84,
                        "bip_ev": 84,
                        "barrel_rate": 0.14,
                        "hardhit_rate": 0.47,
                        "xwoba": 0.388,
                    },
                    "mult_overall": {"k": 0.97, "bb": 1.08, "hr": 1.12, "inplay": 1.06},
                }
            }
        }

        def _fake_load(season: int):
            if int(season) == 2026:
                return current
            if int(season) == 2025:
                return prior
            return {}

        with patch.object(build_roster, "_load_statcast_features_anykey", side_effect=_fake_load):
            applied = build_roster._apply_statcast_features_to_batter(batter, 2026)

        self.assertTrue(applied)
        self.assertGreater(batter.statcast_quality_mult.get("bb", 1.0), 1.0)
        self.assertGreater(batter.statcast_quality_mult.get("barrel_rate", 0.0), 0.0)
        self.assertGreater(batter.statcast_quality_mult.get("xwoba", 0.0), 0.341)

    def test_statcast_quality_blend_uses_prior_when_current_is_missing_or_neutral(self) -> None:
        pitcher = PitcherProfile(
            player=Player(
                mlbam_id=8001,
                full_name="Blend Test Pitcher",
                primary_position="P",
                bat_side=Handedness.R,
                throw_side=Handedness.R,
            ),
            k_rate=0.24,
            bb_rate=0.08,
            hbp_rate=0.008,
            hr_rate=0.03,
            inplay_hit_rate=0.27,
        )
        current = {
            "pitchers": {
                "8001": {
                    "id": 8001,
                    "pitches": 140,
                    "bip": 18,
                    "bip_ev": 18,
                    "whiff_rate": 0.26,
                    "ball_rate": 0.36,
                    "barrel_rate": 0.0,
                    "hardhit_rate": 0.35,
                    "hr_per_bip": 0.0,
                    "xba": 0.27,
                    "mult": {"k": 1.02, "bb": 1.0, "hr": 1.0, "inplay": 0.99},
                }
            }
        }
        prior = {
            "pitchers": {
                "8001": {
                    "id": 8001,
                    "pitches": 710,
                    "bip": 96,
                    "bip_ev": 96,
                    "whiff_rate": 0.31,
                    "ball_rate": 0.39,
                    "barrel_rate": 0.11,
                    "hardhit_rate": 0.43,
                    "hr_per_bip": 0.06,
                    "xba": 0.31,
                    "mult": {"k": 1.1, "bb": 1.07, "hr": 1.08, "inplay": 1.04},
                }
            }
        }

        def _fake_load(season: int):
            if int(season) == 2026:
                return current
            if int(season) == 2025:
                return prior
            return {}

        with patch.object(build_roster, "_load_statcast_quality_map_anykey", side_effect=_fake_load):
            build_roster._apply_statcast_quality_to_pitcher(pitcher, 2026)

        self.assertGreater(pitcher.statcast_quality_mult.get("bb", 1.0), 1.0)
        self.assertGreater(pitcher.bb_rate, 0.08)

    def test_statcast_features_blend_zero_barrel_rate_toward_league_when_no_prior_exists(self) -> None:
        batter = BatterProfile(
            player=Player(
                mlbam_id=7002,
                full_name="League Blend Batter",
                primary_position="OF",
                bat_side=Handedness.L,
                throw_side=Handedness.R,
            ),
            k_rate=0.22,
            bb_rate=0.08,
            hbp_rate=0.008,
            hr_rate=0.03,
            inplay_hit_rate=0.28,
            xb_hit_share=0.25,
        )
        current = {
            "league": {
                "overall": {
                    "pitcher": {"barrel_rate": 0.085, "hardhit_rate": 0.405, "xwoba": 0.322},
                    "batter": {"whiff_rate": 0.235},
                }
            },
            "batters": {
                "7002": {
                    "id": 7002,
                    "overall": {
                        "pitches": 220,
                        "inplay": 18,
                        "bip_ev": 18,
                        "barrel_rate": 0.0,
                        "hardhit_rate": 0.33,
                        "xwoba": 0.301,
                    },
                    "mult_overall": {"k": 1.01, "bb": 0.99, "hr": 1.0, "inplay": 1.0},
                }
            }
        }

        def _fake_load(season: int):
            if int(season) == 2026:
                return current
            if int(season) == 2025:
                return {}
            return {}

        with patch.object(build_roster, "_load_statcast_features_anykey", side_effect=_fake_load):
            applied = build_roster._apply_statcast_features_to_batter(batter, 2026)

        self.assertTrue(applied)
        self.assertGreater(batter.statcast_quality_mult.get("barrel_rate", 0.0), 0.0)

    def test_statcast_shape_rate_mults_use_discipline_and_stuff(self) -> None:
        class Profile:
            def __init__(self, quality):
                self.statcast_quality_mult = quality

        batter = Profile(
            {
                "csw_rate": 0.305,
                "contact_rate": 0.705,
                "chase_swing_rate": 0.34,
                "zone_rate": 0.47,
                "xwoba": 0.375,
                "ev_max": 112.0,
                "pulled_air_rate": 0.17,
            }
        )
        pitcher = Profile(
            {
                "csw_rate": 0.302,
                "zone_rate": 0.515,
                "chase_swing_rate": 0.325,
                "xwoba": 0.358,
                "ev_mean": 90.3,
                "pitch_velo_mean": 95.1,
                "pitch_extension_mean": 6.45,
            }
        )
        batter_mults = _statcast_shape_rate_mults(batter, role="batter")
        pitcher_mults = _statcast_shape_rate_mults(pitcher, role="pitcher")
        self.assertGreater(batter_mults["k"], 1.0)
        self.assertLess(batter_mults["bb"], 1.0)
        self.assertGreater(batter_mults["hr"], 1.0)
        self.assertLess(batter_mults["pitch_count"], 1.0)
        self.assertGreater(batter_mults["xb"], 1.0)
        self.assertGreater(pitcher_mults["k"], 1.0)
        self.assertLess(pitcher_mults["bb"], 1.0)
        self.assertGreater(pitcher_mults["hr"], 1.0)
        self.assertGreater(pitcher_mults["xb"], 1.0)

    def test_statcast_shape_rate_mults_can_raise_pitch_count_for_disciplined_batter(self) -> None:
        class Profile:
            def __init__(self, quality):
                self.statcast_quality_mult = quality

        batter = Profile(
            {
                "csw_rate": 0.258,
                "contact_rate": 0.792,
                "chase_swing_rate": 0.258,
                "zone_rate": 0.501,
                "xwoba": 0.341,
                "ev_max": 108.8,
                "pulled_air_rate": 0.108,
            }
        )
        batter_mults = _statcast_shape_rate_mults(batter, role="batter")
        self.assertGreater(batter_mults["pitch_count"], 1.0)

    def test_pitch_count_multipliers_increase_average_plate_appearance_length(self) -> None:
        longer_pa_mean, longer_terminals = self._sample_pitch_count_profile(
            batter_pitch_count_mult=1.10,
            pitcher_pitch_count_mult=1.08,
        )
        shorter_pa_mean, shorter_terminals = self._sample_pitch_count_profile(
            batter_pitch_count_mult=0.92,
            pitcher_pitch_count_mult=0.94,
        )

        self.assertGreater(longer_pa_mean, shorter_pa_mean + 0.12)
        self.assertGreater(longer_terminals["ball"], shorter_terminals["ball"])
        self.assertLess(longer_terminals["in_play"], shorter_terminals["in_play"])

    def test_disciplined_statcast_lineup_increases_starter_pitch_count_workload(self) -> None:
        disciplined_quality = {
            "csw_rate": 0.258,
            "contact_rate": 0.792,
            "chase_swing_rate": 0.258,
            "zone_rate": 0.501,
            "xwoba": 0.341,
            "ev_max": 108.8,
            "pulled_air_rate": 0.108,
        }
        aggressive_quality = {
            "csw_rate": 0.305,
            "contact_rate": 0.705,
            "chase_swing_rate": 0.340,
            "zone_rate": 0.470,
            "xwoba": 0.375,
            "ev_max": 112.0,
            "pulled_air_rate": 0.170,
        }

        disciplined_pitches, disciplined_pitches_per_bf, disciplined_outs = self._aggregate_home_starter_workload(disciplined_quality)
        aggressive_pitches, aggressive_pitches_per_bf, aggressive_outs = self._aggregate_home_starter_workload(aggressive_quality)

        self.assertGreater(disciplined_pitches, aggressive_pitches + 1.0)
        self.assertGreater(disciplined_pitches_per_bf, aggressive_pitches_per_bf + 0.03)
        self.assertEqual(disciplined_outs, 9.0)
        self.assertEqual(aggressive_outs, 9.0)

    def test_statcast_pitch_count_pressure_can_raise_starter_stamina(self) -> None:
        pitcher = self._make_pitcher(9001, "Pitch Count Starter")
        pitcher.stamina_pitches = 92
        pitcher.statcast_quality_mult = {
            "bb": 1.08,
            "zone_rate": 0.455,
            "csw_rate": 0.248,
        }

        applied = _apply_statcast_pitch_count_stamina_adjustment(pitcher)

        self.assertTrue(applied)
        self.assertGreater(pitcher.stamina_pitches, 92)

    def test_starter_matchup_hook_adjustment_uses_statcast_and_bvp_pressure(self) -> None:
        class Batter:
            def __init__(self, quality, history):
                self.statcast_quality_mult = quality
                self.vs_pitcher_history = history

        class Lineup:
            def __init__(self, batters):
                self.batters = batters

        class Roster:
            def __init__(self, batters):
                self.lineup = Lineup(batters)

        roster = Roster(
            [
                Batter(
                    {"hr": 1.08, "inplay": 1.05, "k": 0.96, "xwoba": 0.382, "ev_max": 112.0, "pulled_air_rate": 0.168},
                    {202: {"hr_mult": 1.10, "inplay_mult": 1.06, "k_mult": 0.95}},
                ),
                Batter(
                    {"hr": 1.05, "inplay": 1.04, "k": 0.97, "xwoba": 0.368, "ev_max": 110.8, "pulled_air_rate": 0.158},
                    {202: {"hr_mult": 1.06, "inplay_mult": 1.04, "k_mult": 0.97}},
                ),
                Batter(
                    {"hr": 1.04, "inplay": 1.03, "k": 0.98, "xwoba": 0.356, "ev_max": 110.0, "pulled_air_rate": 0.145},
                    {},
                ),
            ]
        )
        adjustment = _starter_matchup_hook_adjustment(roster, 202, 0)
        self.assertLess(float(adjustment["hook_delta"]), 0.0)
        self.assertGreater(float(adjustment["pull_delta"]), 0.0)

    def test_starter_matchup_hook_adjustment_uses_discipline_only_statcast_pressure(self) -> None:
        class Batter:
            def __init__(self, quality):
                self.statcast_quality_mult = quality
                self.vs_pitcher_history = {}

        class Lineup:
            def __init__(self, batters):
                self.batters = batters

        class Roster:
            def __init__(self, batters):
                self.lineup = Lineup(batters)

        disciplined = Roster(
            [
                Batter({"contact_rate": 0.795, "chase_swing_rate": 0.255, "zone_rate": 0.485, "bb": 1.05}),
                Batter({"contact_rate": 0.782, "chase_swing_rate": 0.262, "zone_rate": 0.478, "bb": 1.04}),
                Batter({"contact_rate": 0.776, "chase_swing_rate": 0.268, "zone_rate": 0.482, "bb": 1.03}),
            ]
        )
        aggressive = Roster(
            [
                Batter({"contact_rate": 0.700, "chase_swing_rate": 0.338, "zone_rate": 0.515, "bb": 0.97}),
                Batter({"contact_rate": 0.708, "chase_swing_rate": 0.332, "zone_rate": 0.508, "bb": 0.98}),
                Batter({"contact_rate": 0.712, "chase_swing_rate": 0.326, "zone_rate": 0.504, "bb": 0.99}),
            ]
        )

        disciplined_adjustment = _starter_matchup_hook_adjustment(disciplined, 202, 0)
        aggressive_adjustment = _starter_matchup_hook_adjustment(aggressive, 202, 0)

        self.assertLess(float(disciplined_adjustment["hook_delta"]), float(aggressive_adjustment["hook_delta"]))
        self.assertGreater(float(disciplined_adjustment["pull_delta"]), float(aggressive_adjustment["pull_delta"]))

    def test_starter_effective_hook_recovers_part_of_stamina_gap(self) -> None:
        self.assertEqual(_starter_effective_hook(83, 93, 1.0), 85)
        self.assertEqual(_starter_effective_hook(95, 82, 1.0), 82)
        self.assertEqual(_starter_effective_hook(83, 93, 0.7), 82)

    def test_hitter_hr_target_support_uses_new_shape_metrics(self) -> None:
        rec = {"pa_mean": 4.4, "ab_mean": 3.7, "lineup_order": 2}
        context = {
            "lineup_status": "confirmed",
            "lineup_confidence": 0.92,
            "batter_statcast_hr_mult": 1.06,
            "pitcher_statcast_hr_mult": 1.05,
            "batter_xwoba": 0.391,
            "batter_ev_max": 112.3,
            "batter_pulled_air_rate": 0.171,
            "pitcher_xwoba": 0.356,
            "pitcher_ev_mean": 90.5,
            "park_hr_mult": 1.04,
            "weather_hr_mult": 1.03,
            "opponent_primary_pitch_type": "FF",
            "batter_vs_primary_pitch_type_hr_mult": 1.04,
            "pitcher_primary_pitch_type_hr_mult": 1.03,
        }
        support = _hitter_hr_target_support(rec, context)
        self.assertGreaterEqual(float(support["score"]), 80.0)
        self.assertIn("expected-contact quality", " ".join(support["reasons"]).lower())

    def test_trim_reason_list_preserves_statcast_reason_when_overflowed(self) -> None:
        reasons = [
            "Generic matchup note one.",
            "Generic matchup note two.",
            "Generic matchup note three.",
            "Generic matchup note four.",
            "Generic matchup note five.",
            "His underlying batted-ball quality is still running strong enough to keep the home-run path live.",
            "Against this starter, he has 2 homers in 14 prior plate appearances.",
        ]
        trimmed = _trim_reason_list(reasons)
        self.assertEqual(len(trimmed), 5)
        self.assertTrue(any("underlying batted-ball quality" in reason.lower() for reason in trimmed))
        self.assertTrue(any(reason.lower().startswith("against this starter,") for reason in trimmed))

    def test_hitter_statcast_quality_reason_supports_strikeout_props(self) -> None:
        profile = {
            "statcast_quality_mult": {
                "k": 1.04,
                "csw_rate": 0.295,
                "chase_swing_rate": 0.336,
                "contact_rate": 0.701,
            }
        }
        over_reason = _hitter_statcast_quality_reason(profile, prop="hitter_strikeouts", selection="over")
        under_reason = _hitter_statcast_quality_reason(
            {"statcast_quality_mult": {"k": 0.96, "contact_rate": 0.782}},
            prop="hitter_strikeouts",
            selection="under",
        )
        self.assertIsNotNone(over_reason)
        self.assertIn("strikeout", str(over_reason).lower())
        self.assertIsNotNone(under_reason)
        self.assertTrue(
            ("contact" in str(under_reason).lower()) or ("punchout" in str(under_reason).lower()) or ("strikeout" in str(under_reason).lower())
        )

    def test_hitter_statcast_quality_reason_supports_hits_runs_rbis_props(self) -> None:
        over_reason = _hitter_statcast_quality_reason(
            {"statcast_quality_mult": {"inplay": 1.04, "hr": 1.05}},
            prop="hits_runs_rbis",
            selection="over",
        )
        under_reason = _hitter_statcast_quality_reason(
            {"statcast_quality_mult": {"inplay": 0.96, "k": 1.04}},
            prop="hits_runs_rbis",
            selection="under",
        )
        self.assertIsNotNone(over_reason)
        self.assertTrue(("production" in str(over_reason).lower()) or ("damage" in str(over_reason).lower()))
        self.assertIsNotNone(under_reason)
        self.assertTrue(("under" in str(under_reason).lower()) or ("volume" in str(under_reason).lower()) or ("damage" in str(under_reason).lower()))

    def test_pitcher_statcast_quality_reason_supports_hits_and_walks_props(self) -> None:
        hits_over_reason = _pitcher_statcast_quality_reason(
            {
                "statcast_quality_mult": {
                    "inplay": 1.05,
                    "xwoba": 0.356,
                }
            },
            prop="hits_allowed",
            selection="over",
        )
        walks_under_reason = _pitcher_statcast_quality_reason(
            {
                "statcast_quality_mult": {
                    "bb": 0.96,
                    "zone_rate": 0.507,
                }
            },
            prop="walks_allowed",
            selection="under",
        )
        self.assertIsNotNone(hits_over_reason)
        self.assertIn("hit", str(hits_over_reason).lower())
        self.assertIsNotNone(walks_under_reason)
        self.assertTrue(("walk" in str(walks_under_reason).lower()) or ("free-pass" in str(walks_under_reason).lower()))

    def test_pitcher_statcast_quality_reason_supports_pitch_count_props(self) -> None:
        pitches_over_reason = _pitcher_statcast_quality_reason(
            {
                "statcast_quality_mult": {
                    "bb": 1.05,
                    "zone_rate": 0.452,
                    "csw_rate": 0.255,
                }
            },
            prop="pitches",
            selection="over",
        )
        pitches_under_reason = _pitcher_statcast_quality_reason(
            {
                "statcast_quality_mult": {
                    "bb": 0.96,
                    "zone_rate": 0.507,
                    "csw_rate": 0.294,
                }
            },
            prop="pitches",
            selection="under",
        )
        self.assertIsNotNone(pitches_over_reason)
        self.assertTrue(("pitch" in str(pitches_over_reason).lower()) or ("count" in str(pitches_over_reason).lower()))
        self.assertIsNotNone(pitches_under_reason)
        self.assertTrue(("pitch" in str(pitches_under_reason).lower()) or ("count" in str(pitches_under_reason).lower()))

    def test_hitter_ladder_matchup_summary_surfaces_advanced_statcast_metrics(self) -> None:
        sim_path = Path("synthetic_sim.json")
        snapshot_cache = {
            sim_path: {
                "away": {
                    "lineup": [
                        {
                            "id": 101,
                            "name": "Test Hitter",
                            "bat": "R",
                            "statcast_quality_mult": {
                                "xwoba": 0.388,
                                "ev_max": 112.1,
                                "pulled_air_rate": 0.171,
                                "hr": 1.05,
                                "contact_rate": 0.786,
                                "chase_swing_rate": 0.262,
                                "zone_rate": 0.494,
                            },
                            "vs_pitcher_history": {"202": {"pa": 12.0, "hr": 1.0, "hr_mult": 1.08}},
                        }
                    ]
                },
                "home": {
                    "starter_profile": {
                        "throw": "R",
                        "statcast_quality_mult": {
                            "xwoba": 0.357,
                            "ev_mean": 90.4,
                            "csw_rate": 0.286,
                            "zone_rate": 0.492,
                            "bb": 1.03,
                        }
                    }
                },
            }
        }
        summary = _hitter_ladder_matchup_summary(
            "2026-05-11",
            sim_path,
            {"season": 2026},
            "away",
            101,
            "home_runs",
            snapshot_cache,
        )
        self.assertIsNotNone(summary)
        metrics = dict((summary or {}).get("metrics") or {})
        self.assertIn("batterXwoba", metrics)
        self.assertIn("pitcherXwoba", metrics)
        self.assertIn("matchupPitchCountPressure", metrics)
        self.assertTrue(str((summary or {}).get("summary") or "").strip())

    def test_pitcher_ladder_matchup_summary_supports_non_strikeout_props(self) -> None:
        sim_path = Path("synthetic_pitcher_sim.json")
        snapshot_cache = {
            sim_path: {
                "away": {
                    "starter_profile": {
                        "id": 202,
                        "name": "Test Pitcher",
                        "throw": "R",
                        "statcast_quality_mult": {
                            "inplay": 1.05,
                            "xwoba": 0.361,
                            "ev_mean": 90.8,
                            "zone_rate": 0.505,
                            "bb": 0.96,
                            "csw_rate": 0.294,
                        },
                        "pitch_arsenal": {"FF": 0.56, "SL": 0.28, "CH": 0.16},
                        "pitch_type_inplay_mult": {"FF": 1.05, "SL": 1.02, "CH": 1.01},
                    }
                },
                "home": {
                    "lineup": [
                        {
                            "id": 301,
                            "bat": "L",
                            "vs_pitch_type": {"FF": 1.04, "SL": 1.02, "CH": 1.01},
                            "statcast_quality_mult": {"contact_rate": 0.782, "chase_swing_rate": 0.266, "zone_rate": 0.486, "bb": 1.04},
                        },
                        {
                            "id": 302,
                            "bat": "R",
                            "vs_pitch_type": {"FF": 1.03, "SL": 1.01, "CH": 1.00},
                            "statcast_quality_mult": {"contact_rate": 0.775, "chase_swing_rate": 0.272, "zone_rate": 0.489, "bb": 1.03},
                        },
                    ]
                },
            }
        }
        summary = _pitcher_ladder_matchup_summary(
            "2026-05-11",
            sim_path,
            {"season": 2026},
            "away",
            202,
            "hits_allowed",
            snapshot_cache,
        )
        self.assertIsNotNone(summary)
        metrics = dict((summary or {}).get("metrics") or {})
        self.assertIn("inplayShapeMult", metrics)
        self.assertIn("xwoba", metrics)
        self.assertIn("matchupPitchCountPressure", metrics)
        self.assertTrue(str((summary or {}).get("summary") or "").strip())


if __name__ == "__main__":
    unittest.main()