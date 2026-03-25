import unittest

from energy_analytics.config import load_config
from energy_analytics.ingest import run_ingest
from energy_analytics.status import build_region_status, build_status, format_status_report
from tests.helpers import make_temp_config


class StatusTests(unittest.TestCase):
    def test_status_reports_missing_outputs_for_fresh_temp_config(self) -> None:
        config_path = make_temp_config(include_extended_outputs=True)
        status = build_status([str(config_path)])
        self.assertFalse(status["ready"])
        self.assertEqual(len(status["regions"]), 1)
        self.assertFalse(status["regions"][0]["ready"])
        self.assertGreater(status["regions"][0]["missing_count"], 0)

    def test_status_formatter_includes_region_summary(self) -> None:
        config_path = make_temp_config(include_extended_outputs=True)
        status = build_status([str(config_path)])
        text = format_status_report(status)
        self.assertIn("ERCOT", text)
        self.assertIn("missing=", text)

    def test_region_status_includes_manifest_after_ingest(self) -> None:
        config_path = make_temp_config(include_extended_outputs=True)
        run_ingest(mode_override="sample", config_path=str(config_path))
        region = build_region_status(str(config_path))
        cfg = load_config(str(config_path))
        self.assertEqual(region["region"], cfg["region"])
        self.assertIsNotNone(region["manifest"])
        self.assertEqual(region["manifest"]["record_count"], 4)
        self.assertIn("manifest", region["existing"])
