import unittest
from unittest.mock import patch

from energy_analytics.config import load_config
from energy_analytics.finance import run_finance
from energy_analytics.forecast import run_forecast
from energy_analytics.ingest import run_ingest
from energy_analytics.markets import run_markets
from energy_analytics.queue import run_queue_transform
from energy_analytics.transform import run_transform
from tests.helpers import make_temp_config

try:
    from fastapi.testclient import TestClient
    from energy_analytics import api

    API_TESTS_AVAILABLE = True
except ModuleNotFoundError:
    TestClient = None
    api = None
    API_TESTS_AVAILABLE = False

@unittest.skipUnless(API_TESTS_AVAILABLE, "fastapi test dependencies are not installed")
class ApiTests(unittest.TestCase):
    def test_ready_reports_missing_artifacts(self) -> None:
        config_path = make_temp_config(include_extended_outputs=True)
        with patch.dict(api.ISO_CONFIGS, {"ERCOT": str(config_path)}, clear=True):
            client = TestClient(api.app)
            response = client.get("/api/ready")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertFalse(payload["ready"])
            self.assertEqual(payload["isos"][0]["iso"], "ERCOT")
            self.assertFalse(payload["isos"][0]["ready"])
            self.assertGreater(len(payload["isos"][0]["missing"]), 0)

    def test_backtest_returns_model_series_and_best_model(self) -> None:
        config_path = make_temp_config(include_extended_outputs=True)
        run_ingest(mode_override="sample", config_path=str(config_path))
        run_transform(config_path=str(config_path))
        run_forecast(config_path=str(config_path))
        run_queue_transform(config_path=str(config_path))
        run_markets(config_path=str(config_path))
        run_finance(config_path=str(config_path))
        with patch.dict(api.ISO_CONFIGS, {"ERCOT": str(config_path)}, clear=True):
            client = TestClient(api.app)
            response = client.get("/api/ERCOT/backtest")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertIn("naive_predicted", payload)
            self.assertIn("weather_predicted", payload)
            self.assertIn("best_model", payload)
            self.assertEqual(len(payload["actuals"]), len(payload["predicted"]))
            if payload["best_model"] == "weather_linear":
                self.assertEqual(payload["predicted"], payload["weather_predicted"])
            else:
                self.assertEqual(payload["predicted"], payload["naive_predicted"])

    def test_pipeline_run_endpoint_disabled_by_default(self) -> None:
        config_path = make_temp_config(include_extended_outputs=True)
        run_ingest(mode_override="sample", config_path=str(config_path))
        run_transform(config_path=str(config_path))
        run_forecast(config_path=str(config_path))
        run_queue_transform(config_path=str(config_path))
        run_markets(config_path=str(config_path))
        run_finance(config_path=str(config_path))
        with patch.dict(api.ISO_CONFIGS, {"ERCOT": str(config_path)}, clear=True):
            with patch.object(api, "ENABLE_PIPELINE_RUNS", False):
                client = TestClient(api.app)
                response = client.post("/api/run/ERCOT")
                self.assertEqual(response.status_code, 503)
                self.assertIn("disabled", response.json()["detail"])
