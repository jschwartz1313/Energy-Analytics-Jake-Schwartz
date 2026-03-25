from __future__ import annotations

import csv
import math
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.metadata import log_metadata

BACKTEST_COLUMNS = [
    "timestamp_utc",
    "actual_load_mw",
    "naive_forecast_mw",
    "weather_forecast_mw",
    "naive_abs_pct_error",
    "weather_abs_pct_error",
]


def _linear_fit(x: list[float], y: list[float]) -> tuple[float, float]:
    n = len(x)
    if n == 0:
        return (0.0, 0.0)
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    den = sum((xi - x_mean) ** 2 for xi in x)
    if den == 0:
        return (y_mean, 0.0)
    b = num / den
    a = y_mean - b * x_mean
    return (a, b)


def _rmse(actual: list[float], pred: list[float]) -> float:
    if not actual:
        return 0.0
    mse = sum((a - p) ** 2 for a, p in zip(actual, pred)) / len(actual)
    return math.sqrt(mse)


def _mape(actual: list[float], pred: list[float]) -> float:
    pairs = [(a, p) for a, p in zip(actual, pred) if a != 0]
    if not pairs:
        return 0.0
    return sum(abs((a - p) / a) for a, p in pairs) / len(pairs)


def run_forecast(config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    panel_path = Path(cfg["curated_output"]["panel_csv"])
    backtest_path = Path(cfg["forecast_output"]["backtest_csv"])
    metrics_path = Path(cfg["forecast_output"]["backtest_metrics_csv"])
    scenarios_path = Path(cfg["forecast_output"]["scenarios_csv"])
    log_path = cfg["reports"]["metadata_log"]

    rows: list[dict[str, str]] = []
    with panel_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    if len(rows) < 36:
        raise SystemExit("Forecast requires at least 36 hourly observations")

    actual_vals: list[float] = []
    naive_vals: list[float] = []
    weather_vals: list[float] = []
    backtest_rows: list[dict[str, str]] = []

    for i in range(24, len(rows)):
        train = rows[:i]
        test = rows[i]

        actual = float(test["load_mw"])
        naive = float(rows[i - 24]["load_mw"])

        x = [float(r["temperature_f"]) for r in train]
        y = [float(r["load_mw"]) for r in train]
        a, b = _linear_fit(x, y)
        weather = a + (b * float(test["temperature_f"]))

        ape_naive = abs((actual - naive) / actual) if actual else 0.0
        ape_weather = abs((actual - weather) / actual) if actual else 0.0

        actual_vals.append(actual)
        naive_vals.append(naive)
        weather_vals.append(weather)

        backtest_rows.append(
            {
                "timestamp_utc": test["timestamp_utc"],
                "actual_load_mw": f"{actual:.4f}",
                "naive_forecast_mw": f"{naive:.4f}",
                "weather_forecast_mw": f"{weather:.4f}",
                "naive_abs_pct_error": f"{ape_naive:.6f}",
                "weather_abs_pct_error": f"{ape_weather:.6f}",
            }
        )

    backtest_path.parent.mkdir(parents=True, exist_ok=True)
    with backtest_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BACKTEST_COLUMNS)
        writer.writeheader()
        writer.writerows(backtest_rows)

    naive_rmse = _rmse(actual_vals, naive_vals)
    weather_rmse = _rmse(actual_vals, weather_vals)
    naive_mape = _mape(actual_vals, naive_vals)
    weather_mape = _mape(actual_vals, weather_vals)

    with metrics_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["metric", "value"])
        writer.writeheader()
        writer.writerow({"metric": "naive_rmse_mw", "value": f"{naive_rmse:.6f}"})
        writer.writerow({"metric": "weather_rmse_mw", "value": f"{weather_rmse:.6f}"})
        writer.writerow({"metric": "naive_mape", "value": f"{naive_mape:.6f}"})
        writer.writerow({"metric": "weather_mape", "value": f"{weather_mape:.6f}"})
        writer.writerow(
            {
                "metric": "best_model",
                "value": "weather_linear" if weather_rmse <= naive_rmse else "naive_24h",
            }
        )

    # Long-run scenario projection from recent mean load with annual growth assumptions.
    recent = [float(r["load_mw"]) for r in rows[-24:]]
    base_year_load = sum(recent) / len(recent)

    # Growth rates and peak multiplier from config with sensible defaults
    forecast_assumptions = cfg.get("forecast_assumptions", {})
    growth_cfg = forecast_assumptions.get("growth_rates", {})
    scenario_growth = {
        "low":  float(growth_cfg.get("low",  0.01)),
        "base": float(growth_cfg.get("base", 0.03)),
        "high": float(growth_cfg.get("high", 0.06)),
    }
    peak_multiplier = float(forecast_assumptions.get("peak_multiplier", 1.18))

    start_year = int(rows[-1]["timestamp_utc"][0:4]) + 1

    scenario_rows: list[dict[str, str]] = []
    for scen, g in scenario_growth.items():
        for year in range(start_year, start_year + 7):
            n = year - start_year + 1
            projected = base_year_load * ((1 + g) ** n)
            peak = projected * peak_multiplier
            scenario_rows.append(
                {
                    "scenario": scen,
                    "year": str(year),
                    "avg_load_mw": f"{projected:.2f}",
                    "peak_load_mw": f"{peak:.2f}",
                    "annual_growth_rate": f"{g:.4f}",
                }
            )

    with scenarios_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["scenario", "year", "avg_load_mw", "peak_load_mw", "annual_growth_rate"],
        )
        writer.writeheader()
        writer.writerows(scenario_rows)

    log_metadata(
        log_path,
        (
            "forecast:"
            f"backtest_rows={len(backtest_rows)} "
            f"naive_rmse={naive_rmse:.2f} weather_rmse={weather_rmse:.2f}"
        ),
    )


if __name__ == "__main__":
    run_forecast()
