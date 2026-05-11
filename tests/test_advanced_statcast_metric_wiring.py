import unittest

from sim_engine.data.build_roster import _enrich_statcast_quality_mult
from sim_engine.simulate import _statcast_shape_rate_mults
from tools.daily_update_multi_profile import _hitter_hr_target_support, _hitter_statcast_quality_reason


class AdvancedStatcastMetricWiringTests(unittest.TestCase):
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
        self.assertGreater(pitcher_mults["k"], 1.0)
        self.assertLess(pitcher_mults["bb"], 1.0)
        self.assertGreater(pitcher_mults["hr"], 1.0)

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


if __name__ == "__main__":
    unittest.main()