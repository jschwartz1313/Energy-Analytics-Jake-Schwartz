import json
import unittest
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.ingest import run_ingest
from tests.helpers import make_temp_config


class IngestTests(unittest.TestCase):
    def test_ingest_writes_manifest(self) -> None:
        config_path = make_temp_config()
        run_ingest(mode_override="sample", config_path=str(config_path))
        manifest_path = Path(load_config(str(config_path))["ingestion"]["manifest_output"])
        self.assertTrue(manifest_path.exists())
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(payload.get("record_count"), 4)
        for rec in payload.get("records", []):
            self.assertTrue(rec.get("contract_valid"))


if __name__ == "__main__":
    unittest.main()
