from __future__ import annotations

import csv
import json
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _resolve_date(val: str | None, offset_days: int = 0) -> str:
    """Resolve 'auto' (or missing) to a date offset from today.

    Args:
        val: A date string like '2025-01-01', or 'auto', or None.
        offset_days: When val is 'auto'/None, go this many days back from today.
    """
    if not val or val.strip().lower() == "auto":
        return (date.today() - timedelta(days=offset_days)).isoformat()
    return val


def fetch_bytes(url: str, timeout_sec: int = 45, retries: int = 2) -> bytes:
    last_exc: Exception | None = None
    for _ in range(retries + 1):
        try:
            req = urllib.request.Request(url=url, headers={"User-Agent": "EnergyAnalytics/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                return resp.read()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    if last_exc is None:
        raise RuntimeError(f"fetch_bytes failed for {url}")
    raise last_exc


def _to_iso_utc(ts: str) -> str:
    # Open-Meteo hourly timestamps are local or UTC naive YYYY-MM-DDTHH:MM.
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_open_meteo_weather_rows(payload: dict[str, Any], region: str) -> list[dict[str, str]]:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    if len(times) != len(temps):
        raise ValueError("Open-Meteo payload mismatch: time and temperature_2m lengths differ")

    rows: list[dict[str, str]] = []
    for ts, temp in zip(times, temps):
        rows.append(
            {
                "timestamp_utc": _to_iso_utc(ts),
                "region": region,
                "temperature_f": f"{float(temp):.2f}",
            }
        )
    return rows


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def fetch_real_dataset_to_csv(dataset: str, source_cfg: dict[str, Any], out_path: Path, region: str) -> str:
    source_type = source_cfg.get("source_type", "url_csv")

    if source_type == "url_csv":
        url = source_cfg["url"]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(fetch_bytes(url))
        return url

    if source_type == "open_meteo_archive":
        base_url = source_cfg["url"]
        # 'auto' resolves to the trailing 365 days; end is yesterday (archive lag)
        start_date = _resolve_date(source_cfg.get("start_date"), offset_days=365)
        end_date = _resolve_date(source_cfg.get("end_date"), offset_days=1)
        params = {
            "latitude": source_cfg["latitude"],
            "longitude": source_cfg["longitude"],
            "start_date": start_date,
            "end_date": end_date,
            "timezone": source_cfg.get("timezone", "UTC"),
            "hourly": source_cfg.get("hourly", "temperature_2m"),
            "temperature_unit": source_cfg.get("temperature_unit", "fahrenheit"),
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        payload = json.loads(fetch_bytes(url).decode("utf-8"))
        rows = build_open_meteo_weather_rows(payload, region=region)
        _write_csv(out_path, rows, fieldnames=["timestamp_utc", "region", "temperature_f"])
        return url

    raise ValueError(f"Unsupported real_data source_type={source_type} for dataset={dataset}")
