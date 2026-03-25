import unittest
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.ingest import run_ingest
from energy_analytics.transform import PANEL_COLUMNS
from energy_analytics.transform import run_transform
from tests.helpers import make_temp_config


class TransformTests(unittest.TestCase):
    def test_panel_columns_stable(self) -> None:
        self.assertEqual(
            PANEL_COLUMNS,
            [
                "timestamp_utc",
                "region",
                "hub",
                "load_mw",
                "price_usd_mwh",
                "temperature_f",
            ],
        )

    def test_transform_writes_curated_panel_in_temp_workspace(self) -> None:
        config_path = make_temp_config()
        run_ingest(mode_override="sample", config_path=str(config_path))
        run_transform(config_path=str(config_path))
        cfg = load_config(str(config_path))
        curated_path = Path(cfg["curated_output"]["panel_csv"])
        self.assertTrue(curated_path.exists())
        rows = curated_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertGreater(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
