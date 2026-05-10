import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.web.flask_frontend import _live_prop_artifacts_payload


class RenderLivePropArtifactsTests(unittest.TestCase):
    def test_live_prop_artifacts_payload_includes_registry_and_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            registry_path = root / "live_prop_registry_2026_05_09.json"
            observation_path = root / "live_prop_observations_2026_05_09.jsonl"
            registry_log_path = root / "live_prop_registry_2026_05_09.jsonl"
            recap_path = root / "live_lens_daily_recap_2026_05_09.json"

            registry_path.write_text(
                json.dumps(
                    {
                        "date": "2026-05-09",
                        "entries": {
                            "k1": {
                                "date": "2026-05-09",
                                "gamePk": 123,
                                "owner": "Sample Hitter",
                                "market": "hitter_props",
                                "prop": "hits",
                                "selection": "over",
                                "marketLine": 0.5,
                                "firstSeenAt": "a",
                                "lastSeenAt": "b",
                                "seenCount": 2,
                                "firstSeenSnapshot": {"liveProjection": 0.9},
                                "lastSeenSnapshot": {"liveProjection": 1.1},
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            observation_path.write_text(
                json.dumps({"key": "k1", "teamSide": "home", "snapshot": {"liveProjection": 0.95}, "gameState": {"inning": 3}}) + "\n",
                encoding="utf-8",
            )
            registry_log_path.write_text(json.dumps({"event": "observed", "key": "k1"}) + "\n", encoding="utf-8")
            recap_path.write_text(json.dumps({"source": "daily_recap", "firstObservationArchive": [{"key": "k1"}]}), encoding="utf-8")

            with patch("tools.web.flask_frontend._live_prop_registry_path", return_value=registry_path), patch(
                "tools.web.flask_frontend._live_prop_observation_log_path",
                return_value=observation_path,
            ), patch("tools.web.flask_frontend._live_prop_registry_log_path", return_value=registry_log_path), patch(
                "tools.web.flask_frontend._live_lens_daily_recap_path",
                return_value=recap_path,
            ):
                payload = _live_prop_artifacts_payload(
                    "2026-05-09",
                    include_observation_log=True,
                    include_registry_log=True,
                )

        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["summary"]["registryEntryCount"])
        self.assertEqual(1, payload["summary"]["firstObservationArchiveCount"])
        self.assertEqual(1, payload["summary"]["observationRowCount"])
        self.assertEqual(1, payload["summary"]["registryLogRowCount"])
        self.assertEqual("k1", payload["firstObservationArchive"][0]["key"])
        self.assertEqual("k1", payload["observationLog"][0]["key"])


if __name__ == "__main__":
    unittest.main()