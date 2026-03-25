from __future__ import annotations

import csv
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.metadata import log_metadata


PANEL_COLUMNS = [
    "timestamp_utc",
    "region",
    "hub",
    "load_mw",
    "price_usd_mwh",
    "temperature_f",
]


def _read_table(path: str, key_col: str) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    with Path(path).open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            out[row[key_col]] = row
    return out


def run_transform(config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    raw = cfg["raw_output"]
    staged = Path(cfg["staged_output"]["panel_csv"])
    curated = Path(cfg["curated_output"]["panel_csv"])
    log_path = cfg["reports"]["metadata_log"]

    load_rows = _read_table(raw["load"], "timestamp_utc")
    price_rows = _read_table(raw["price"], "timestamp_utc")
    weather_rows = _read_table(raw["weather"], "timestamp_utc")

    timestamps = sorted(set(load_rows) & set(price_rows) & set(weather_rows))

    # Warn if the inner join drops more than 5% of available timestamps
    total_possible = max(len(load_rows), len(price_rows), len(weather_rows))
    dropped = total_possible - len(timestamps)
    if total_possible > 0 and dropped / total_possible > 0.05:
        msg = (
            f"WARNING: transform dropped {dropped}/{total_possible} rows "
            f"({100 * dropped / total_possible:.1f}%) due to timestamp mismatch. "
            "Check that load, price, and weather data cover the same time range."
        )
        print(msg)
        log_metadata(log_path, msg)

    merged: list[dict[str, str]] = []
    for ts in timestamps:
        l = load_rows[ts]
        p = price_rows[ts]
        w = weather_rows[ts]
        merged.append(
            {
                "timestamp_utc": ts,
                "region": l["region"],
                "hub": p["hub"],
                "load_mw": l["load_mw"],
                "price_usd_mwh": p["price_usd_mwh"],
                "temperature_f": w["temperature_f"],
            }
        )

    for path in (staged, curated):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=PANEL_COLUMNS)
            writer.writeheader()
            writer.writerows(merged)

    log_metadata(log_path, f"transform:panel rows={len(merged)} staged={staged} curated={curated}")


if __name__ == "__main__":
    run_transform()
