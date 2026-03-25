from __future__ import annotations

import csv
import json
from pathlib import Path

from energy_analytics.config import load_config, resolve_project_path
from energy_analytics.metadata import log_metadata

REQUIRED_COLUMNS = {
    "timestamp_utc",
    "region",
    "hub",
    "load_mw",
    "price_usd_mwh",
    "temperature_f",
}

QUEUE_REQUIRED_COLUMNS = {
    "queue_id",
    "project_name",
    "technology",
    "mw",
    "status",
    "target_cod_year",
    "completion_probability_p50",
    "completion_probability_p90",
}

OUTLOOK_REQUIRED_COLUMNS = {
    "year",
    "technology",
    "project_count",
    "nameplate_mw",
    "expected_online_mw_p50",
    "expected_online_mw_p90",
}

MARKETS_METRIC_REQUIRED = {
    "avg_price_usd_mwh",
    "solar_capture_price_usd_mwh",
    "wind_capture_price_usd_mwh",
    "negative_price_hours",
    "negative_price_share",
    "congestion_proxy_mean",
}

FINANCE_SCENARIO_REQUIRED_COLUMNS = {
    "scenario_id",
    "contract_type",
    "price_case",
    "capex_case",
    "npv_musd",
    "after_tax_npv_musd",
    "irr",
    "min_dscr",
    "avg_dscr",
    "lcoe_usd_mwh",
}


def run_qa(config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    panel_path = Path(cfg["curated_output"]["panel_csv"])
    queue_path = Path(cfg["staged_output"]["queue_csv"])
    queue_outlook_path = Path(cfg["curated_output"]["queue_outlook_csv"])
    queue_calibration_path = Path(cfg["queue_model_output"]["calibration_csv"])
    forecast_backtest_path = Path(cfg["forecast_output"]["backtest_csv"])
    forecast_metrics_path = Path(cfg["forecast_output"]["backtest_metrics_csv"])
    forecast_scenarios_path = Path(cfg["forecast_output"]["scenarios_csv"])
    markets_metrics_path = Path(cfg["markets_output"]["metrics_csv"])
    findings_path = Path(cfg["markets_output"]["findings_md"])
    finance_scenarios_path = Path(cfg["finance_output"]["scenarios_csv"])
    finance_summary_path = Path(cfg["finance_output"]["summary_csv"])
    dashboard_path = resolve_project_path("reports/dashboard/index.html")
    summary_report_path = resolve_project_path("reports/dashboard/summary_report.html")
    ingest_manifest_path = Path(cfg.get("ingestion", {}).get("manifest_output", "reports/ingestion_manifest.json"))
    report_path = Path(cfg["reports"]["qa_report"])
    log_path = cfg["reports"]["metadata_log"]

    failures: list[str] = []
    rows: list[dict[str, str]] = []

    with panel_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - columns
        if missing:
            failures.append(f"Missing required columns: {sorted(missing)}")
        for row in reader:
            rows.append(row)

    if not rows:
        failures.append("Panel has zero rows")

    timestamps = [r["timestamp_utc"] for r in rows]
    if len(set(timestamps)) != len(timestamps):
        failures.append("Duplicate timestamps found")

    for i, row in enumerate(rows, start=1):
        if any(row.get(c, "") == "" for c in REQUIRED_COLUMNS):
            failures.append(f"Row {i}: null/empty field present")
            continue
        try:
            load = float(row["load_mw"])
            price = float(row["price_usd_mwh"])
            temp = float(row["temperature_f"])
        except ValueError:
            failures.append(f"Row {i}: numeric cast failed")
            continue

        if load <= 0:
            failures.append(f"Row {i}: load_mw must be > 0")
        if price < -200 or price > 2000:
            failures.append(f"Row {i}: price_usd_mwh outside expected range")
        if temp < -50 or temp > 140:
            failures.append(f"Row {i}: temperature_f outside expected range")

    queue_rows: list[dict[str, str]] = []
    with queue_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = set(reader.fieldnames or [])
        missing = QUEUE_REQUIRED_COLUMNS - columns
        if missing:
            failures.append(f"Queue table missing required columns: {sorted(missing)}")
        for row in reader:
            queue_rows.append(row)

    if not queue_rows:
        failures.append("Queue normalized table has zero rows")

    for i, row in enumerate(queue_rows, start=1):
        try:
            mw = float(row["mw"])
            p50 = float(row["completion_probability_p50"])
            p90 = float(row["completion_probability_p90"])
        except ValueError:
            failures.append(f"Queue row {i}: numeric cast failed")
            continue
        if mw <= 0:
            failures.append(f"Queue row {i}: mw must be > 0")
        if not (0.0 <= p50 <= 1.0):
            failures.append(f"Queue row {i}: p50 must be in [0,1]")
        if not (0.0 <= p90 <= 1.0):
            failures.append(f"Queue row {i}: p90 must be in [0,1]")
        if p90 > p50:
            failures.append(f"Queue row {i}: p90 cannot exceed p50")

    outlook_rows: list[dict[str, str]] = []
    with queue_outlook_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = set(reader.fieldnames or [])
        missing = OUTLOOK_REQUIRED_COLUMNS - columns
        if missing:
            failures.append(f"Queue outlook missing required columns: {sorted(missing)}")
        for row in reader:
            outlook_rows.append(row)

    if not outlook_rows:
        failures.append("Queue outlook table has zero rows")

    for i, row in enumerate(outlook_rows, start=1):
        try:
            p50_mw = float(row["expected_online_mw_p50"])
            p90_mw = float(row["expected_online_mw_p90"])
        except ValueError:
            failures.append(f"Queue outlook row {i}: numeric cast failed")
            continue
        if p90_mw > p50_mw:
            failures.append(f"Queue outlook row {i}: expected p90 MW cannot exceed p50 MW")

    calibration_rows: list[dict[str, str]] = []
    with queue_calibration_path.open("r", encoding="utf-8", newline="") as f:
        calibration_rows = list(csv.DictReader(f))
    for i, row in enumerate(calibration_rows, start=1):
        try:
            obs = float(row["observed_completion_rate"])
            pred = float(row["mean_predicted_probability"])
            brier = float(row["brier_score"])
        except (ValueError, KeyError):
            failures.append(f"Queue calibration row {i}: parse failure")
            continue
        if not (0.0 <= obs <= 1.0 and 0.0 <= pred <= 1.0):
            failures.append(f"Queue calibration row {i}: rates must be in [0,1]")
        if brier < 0:
            failures.append(f"Queue calibration row {i}: brier score must be >= 0")

    with forecast_backtest_path.open("r", encoding="utf-8", newline="") as f:
        forecast_backtest_rows = list(csv.DictReader(f))
    if len(forecast_backtest_rows) < 10:
        failures.append("Forecast backtest has too few rows")

    forecast_metrics: dict[str, str] = {}
    with forecast_metrics_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            forecast_metrics[row["metric"]] = row["value"]
    for req in ("naive_rmse_mw", "weather_rmse_mw", "naive_mape", "weather_mape", "best_model"):
        if req not in forecast_metrics:
            failures.append(f"Forecast metric missing: {req}")

    with forecast_scenarios_path.open("r", encoding="utf-8", newline="") as f:
        forecast_scen_rows = list(csv.DictReader(f))
    if len(forecast_scen_rows) < 15:
        failures.append("Forecast scenarios table should contain at least 15 rows")

    market_metrics: dict[str, float] = {}
    with markets_metrics_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            try:
                market_metrics[row["metric"]] = float(row["value"])
            except ValueError:
                failures.append(f"Market metric {row['metric']}: numeric cast failed")

    missing_market = MARKETS_METRIC_REQUIRED - set(market_metrics)
    if missing_market:
        failures.append(f"Market metrics missing required rows: {sorted(missing_market)}")

    if market_metrics.get("solar_capture_price_usd_mwh", 0.0) <= 0:
        failures.append("Solar capture price must be > 0")
    if market_metrics.get("wind_capture_price_usd_mwh", 0.0) <= 0:
        failures.append("Wind capture price must be > 0")
    if market_metrics.get("negative_price_hours", 0.0) < 0:
        failures.append("Negative price hours must be >= 0")

    if not findings_path.exists():
        failures.append("Milestone 3 findings note is missing")
    else:
        text = findings_path.read_text(encoding="utf-8").strip()
        if text.count("\n") < 3:
            failures.append("Milestone 3 findings note must include 3 insights")

    finance_rows: list[dict[str, str]] = []
    with finance_scenarios_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = set(reader.fieldnames or [])
        missing = FINANCE_SCENARIO_REQUIRED_COLUMNS - columns
        if missing:
            failures.append(f"Finance scenarios missing required columns: {sorted(missing)}")
        for row in reader:
            finance_rows.append(row)

    if len(finance_rows) < 18:
        failures.append("Finance scenario matrix should contain at least 18 rows")

    for i, row in enumerate(finance_rows, start=1):
        try:
            float(row["npv_musd"])
            float(row["after_tax_npv_musd"])
            float(row["irr"])
            min_dscr = float(row["min_dscr"])
            avg_dscr = float(row["avg_dscr"])
            float(row["lcoe_usd_mwh"])
        except ValueError:
            failures.append(f"Finance scenario row {i}: numeric cast failed")
            continue
        if min_dscr > avg_dscr:
            failures.append(f"Finance scenario row {i}: min_dscr cannot exceed avg_dscr")

    contract_types = {row["contract_type"] for row in finance_rows if "contract_type" in row}
    if {"merchant", "contracted"} - contract_types:
        failures.append("Finance scenario matrix must include merchant and contracted contract types")

    with finance_summary_path.open("r", encoding="utf-8", newline="") as f:
        summary_rows = list(csv.DictReader(f))
    if not summary_rows:
        failures.append("Finance summary table has zero rows")

    manifest_records_count = 0
    if not ingest_manifest_path.exists():
        failures.append(f"Ingestion manifest missing: {ingest_manifest_path}")
    else:
        payload = json.loads(ingest_manifest_path.read_text(encoding="utf-8"))
        recs = payload.get("records", [])
        manifest_records_count = len(recs)
        if manifest_records_count < 4:
            failures.append("Ingestion manifest must contain 4 dataset records")
        for rec in recs:
            if not rec.get("contract_valid", False):
                failures.append(f"Contract invalid in manifest for dataset={rec.get('dataset')}")
            if not rec.get("sha256"):
                failures.append(f"Missing sha256 in manifest for dataset={rec.get('dataset')}")

    for path, label in (
        (dashboard_path, "Dashboard index"),
        (summary_report_path, "Summary report"),
    ):
        if not path.exists():
            failures.append(f"{label} artifact is missing: {path}")

    if dashboard_path.exists():
        dtext = dashboard_path.read_text(encoding="utf-8")
        for section in ("Overview", "Load", "Supply", "Markets", "Finance", "Downloads"):
            if section not in dtext:
                failures.append(f"Dashboard missing required section label: {section}")

    if summary_report_path.exists():
        stext = summary_report_path.read_text(encoding="utf-8")
        if "Base Solar Finance Case" not in stext:
            failures.append("Summary report missing finance section")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Milestone QA Report (M1 + M2 + M3 + M4 + M5)\n\n")
        f.write(f"- Panel rows checked: {len(rows)}\n")
        f.write(f"- Queue rows checked: {len(queue_rows)}\n")
        f.write(f"- Queue outlook rows checked: {len(outlook_rows)}\n")
        f.write(f"- Queue calibration rows checked: {len(calibration_rows)}\n")
        f.write(f"- Forecast backtest rows checked: {len(forecast_backtest_rows)}\n")
        f.write(f"- Forecast scenario rows checked: {len(forecast_scen_rows)}\n")
        f.write(f"- Market metrics checked: {len(market_metrics)}\n")
        f.write(f"- Finance scenarios checked: {len(finance_rows)}\n")
        f.write(f"- Ingestion manifest records: {manifest_records_count}\n")
        f.write(f"- Dashboard artifacts checked: 2\n")
        f.write(f"- Unique timestamps: {len(set(timestamps))}\n")
        f.write(f"- Result: {'FAIL' if failures else 'PASS'}\n\n")
        if failures:
            f.write("## Failures\n")
            for item in failures:
                f.write(f"- {item}\n")
        else:
            f.write("All QA checks passed.\n")

    log_metadata(log_path, f"qa:rows={len(rows)} failures={len(failures)} report={report_path}")
    if failures:
        raise SystemExit("QA failed; inspect reports/qa_report.md")


if __name__ == "__main__":
    run_qa()
