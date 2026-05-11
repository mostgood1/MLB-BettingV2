import unittest

from tools.eval.inspect_render_live_pitcher_openers import build_render_live_pitcher_openers_summary


class RenderLivePitcherOpenersTests(unittest.TestCase):
    def test_build_summary_flags_strict_and_zero_workload_openers(self) -> None:
        payload = {
            "date": "2026-05-10",
            "generatedAt": "2026-05-10T16:00:00-05:00",
            "firstObservationArchive": [
                {"market": "pitcher_props", "prop": "strikeouts"},
                {"market": "pitcher_props", "prop": "outs"},
            ],
            "observationLog": [
                {
                    "key": "k1",
                    "market": "pitcher_props",
                    "prop": "strikeouts",
                    "owner": "Bubba Chandler",
                    "selection": "under",
                    "recordedAt": "2026-05-10T15:09:02-05:00",
                    "snapshot": {
                        "liveProjection": 2.335,
                        "modelMean": 4.062,
                        "actualSoFar": 0.0,
                        "battersFaced": 0,
                        "pitchCount": None,
                        "outsRecorded": 0,
                    },
                },
                {
                    "key": "k2",
                    "market": "pitcher_props",
                    "prop": "outs",
                    "owner": "Tyler Mahle",
                    "selection": "under",
                    "recordedAt": "2026-05-10T15:10:26-05:00",
                    "snapshot": {
                        "liveProjection": 10.98,
                        "modelMean": 15.538,
                        "actualSoFar": 0.0,
                        "battersFaced": 0,
                        "pitchCount": 5,
                        "outsRecorded": 0,
                    },
                },
                {
                    "key": "k2",
                    "market": "pitcher_props",
                    "prop": "outs",
                    "owner": "Tyler Mahle",
                    "selection": "under",
                    "recordedAt": "2026-05-10T15:12:26-05:00",
                    "snapshot": {
                        "liveProjection": 11.2,
                        "modelMean": 15.538,
                        "actualSoFar": 0.0,
                        "battersFaced": 1,
                        "pitchCount": 9,
                        "outsRecorded": 0,
                    },
                },
            ],
        }

        summary = build_render_live_pitcher_openers_summary(payload)

        self.assertEqual(2, summary["pitcherArchiveCount"])
        self.assertEqual(3, summary["pitcherObservationCount"])
        self.assertEqual(2, summary["firstObservationCount"])
        self.assertEqual(1, summary["strictOpenerLikeCount"])
        self.assertEqual(2, summary["zeroWorkloadLikeCount"])
        strict_rows = summary["strictOpenerLikeRows"]
        self.assertEqual(1, len(strict_rows))
        self.assertEqual("strikeouts", strict_rows[0]["prop"])
        self.assertEqual(-1.727, strict_rows[0]["projectionDelta"])
        zero_rows = summary["zeroWorkloadLikeRows"]
        self.assertEqual({"strikeouts", "outs"}, {row["prop"] for row in zero_rows})
        zero_by_prop = {row["prop"]: row for row in summary["zeroWorkloadLikeByProp"]}
        self.assertEqual(-4.558, zero_by_prop["outs"]["meanProjectionDelta"])
        self.assertEqual(-1.727, zero_by_prop["strikeouts"]["meanProjectionDelta"])


if __name__ == "__main__":
    unittest.main()