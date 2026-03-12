from __future__ import annotations

import csv
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.metadata import log_metadata

SOLAR_SHAPE_24 = [
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.02,
    0.08,
    0.20,
    0.40,
    0.62,
    0.80,
    0.92,
    1.0,
    0.96,
    0.84,
    0.62,
    0.35,
    0.12,
    0.03,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
]

WIND_SHAPE_24 = [
    0.70,
    0.72,
    0.74,
    0.76,
    0.73,
    0.68,
    0.60,
    0.54,
    0.50,
    0.46,
    0.44,
    0.42,
    0.40,
    0.41,
    0.43,
    0.48,
    0.56,
    0.64,
    0.72,
    0.78,
    0.82,
    0.80,
    0.76,
    0.72,
]

HOURLY_COLUMNS = [
    "timestamp_utc",
    "region",
    "hub",
    "price_usd_mwh",
    "solar_profile",
    "wind_profile",
    "solar_weighted_price",
    "wind_weighted_price",
    "congestion_proxy",
    "is_negative_price_hour",
]


def _hour(ts: str) -> int:
    return int(ts[11:13])


def _moving_average(values: list[float], window: int) -> list[float]:
    out: list[float] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = values[start : i + 1]
        out.append(sum(chunk) / len(chunk))
    return out


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(round((len(s) - 1) * q))
    return s[idx]


def run_markets(config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    panel_path = Path(cfg["curated_output"]["panel_csv"])
    hourly_out = Path(cfg["markets_output"]["hourly_csv"])
    metrics_out = Path(cfg["markets_output"]["metrics_csv"])
    findings_out = Path(cfg["markets_output"]["findings_md"])
    log_path = cfg["reports"]["metadata_log"]

    rows: list[dict[str, str]] = []
    with panel_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)

    prices = [float(r["price_usd_mwh"]) for r in rows]
    rolling_price = _moving_average(prices, window=24)

    enriched: list[dict[str, str]] = []
    solar_num = 0.0
    solar_den = 0.0
    wind_num = 0.0
    wind_den = 0.0
    negative_hours = 0
    congestion_values: list[float] = []

    for idx, row in enumerate(rows):
        hr = _hour(row["timestamp_utc"])
        price = float(row["price_usd_mwh"])
        solar_p = SOLAR_SHAPE_24[hr]
        wind_p = WIND_SHAPE_24[hr]
        solar_w = solar_p * price
        wind_w = wind_p * price
        congestion = abs(price - rolling_price[idx])
        is_neg = 1 if price < 0 else 0

        solar_num += solar_w
        solar_den += solar_p
        wind_num += wind_w
        wind_den += wind_p
        negative_hours += is_neg
        congestion_values.append(congestion)

        enriched.append(
            {
                "timestamp_utc": row["timestamp_utc"],
                "region": row["region"],
                "hub": row["hub"],
                "price_usd_mwh": f"{price:.4f}",
                "solar_profile": f"{solar_p:.4f}",
                "wind_profile": f"{wind_p:.4f}",
                "solar_weighted_price": f"{solar_w:.4f}",
                "wind_weighted_price": f"{wind_w:.4f}",
                "congestion_proxy": f"{congestion:.4f}",
                "is_negative_price_hour": str(is_neg),
            }
        )

    avg_price = sum(prices) / len(prices) if prices else 0.0
    solar_capture = (solar_num / solar_den) if solar_den else 0.0
    wind_capture = (wind_num / wind_den) if wind_den else 0.0

    metrics = [
        ("avg_price_usd_mwh", avg_price),
        ("solar_capture_price_usd_mwh", solar_capture),
        ("wind_capture_price_usd_mwh", wind_capture),
        ("solar_capture_ratio", (solar_capture / avg_price) if avg_price else 0.0),
        ("wind_capture_ratio", (wind_capture / avg_price) if avg_price else 0.0),
        ("negative_price_hours", float(negative_hours)),
        ("total_hours", float(len(rows))),
        ("negative_price_share", (negative_hours / len(rows)) if rows else 0.0),
        ("congestion_proxy_mean", (sum(congestion_values) / len(congestion_values)) if congestion_values else 0.0),
        ("congestion_proxy_p95", _quantile(congestion_values, 0.95)),
    ]

    hourly_out.parent.mkdir(parents=True, exist_ok=True)
    with hourly_out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=HOURLY_COLUMNS)
        writer.writeheader()
        writer.writerows(enriched)

    metrics_out.parent.mkdir(parents=True, exist_ok=True)
    with metrics_out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["metric", "value"])
        writer.writeheader()
        for name, val in metrics:
            writer.writerow({"metric": name, "value": f"{val:.6f}"})

    findings_out.parent.mkdir(parents=True, exist_ok=True)
    insights = [
        (
            "Capture-price signal",
            f"Solar capture price is {solar_capture:.2f} USD/MWh versus wind at {wind_capture:.2f} USD/MWh.",
        ),
        (
            "Congestion proxy",
            f"Mean congestion proxy is {metrics[8][1]:.2f} USD/MWh with a p95 of {metrics[9][1]:.2f} USD/MWh.",
        ),
        (
            "Negative-price risk",
            f"Negative-price hours are {negative_hours} of {len(rows)} ({metrics[7][1]*100:.1f}%).",
        ),
    ]
    with findings_out.open("w", encoding="utf-8") as f:
        f.write("# Milestone 3 Findings\n\n")
        f.write("1. " + insights[0][0] + ": " + insights[0][1] + "\n")
        f.write("2. " + insights[1][0] + ": " + insights[1][1] + "\n")
        f.write("3. " + insights[2][0] + ": " + insights[2][1] + "\n")

    log_metadata(
        log_path,
        (
            "markets:"
            f"hours={len(rows)} "
            f"solar_capture={solar_capture:.3f} "
            f"wind_capture={wind_capture:.3f} "
            f"negative_hours={negative_hours}"
        ),
    )


if __name__ == "__main__":
    run_markets()
