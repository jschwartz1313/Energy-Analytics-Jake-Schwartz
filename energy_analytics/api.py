"""FastAPI backend for Energy Analytics — serves all ISO data as JSON endpoints."""
from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from energy_analytics.config import load_config

app = FastAPI(title="Energy Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ISO_CONFIGS: dict[str, str] = {
    "ERCOT": "config/data_sources.yml",
    "CAISO": "config/caiso.yml",
    "PJM": "config/pjm.yml",
    "MISO": "config/miso.yml",
    "SPP": "config/spp.yml",
    "NYISO": "config/nyiso.yml",
    "ISO-NE": "config/isone.yml",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Data not found: {path}. Run the pipeline first.")
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _cfg(iso: str) -> dict[str, Any]:
    key = iso.upper()
    if key not in ISO_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Unknown ISO: {iso}. Valid: {list(ISO_CONFIGS)}")
    return load_config(ISO_CONFIGS[key])


# ── Static files ──────────────────────────────────────────────────────────────

app.mount("/reports", StaticFiles(directory="reports"), name="reports")
app.mount("/data", StaticFiles(directory="data"), name="data")


@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    p = Path("index.html")
    if not p.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return HTMLResponse(p.read_text(encoding="utf-8"))


# ── ISO list ──────────────────────────────────────────────────────────────────

@app.get("/api/isos")
def list_isos():
    available = []
    for iso, cfg_path in ISO_CONFIGS.items():
        try:
            cfg = load_config(cfg_path)
            panel_path = Path(cfg["curated_output"]["panel_csv"])
            available.append({"iso": iso, "ready": panel_path.exists()})
        except Exception:
            available.append({"iso": iso, "ready": False})
    return {"isos": available}


# ── Panel (load / price / temperature) ───────────────────────────────────────

@app.get("/api/{iso}/panel")
def get_panel(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    timestamps, load, price, temp = [], [], [], []
    for r in rows:
        timestamps.append(r["timestamp_utc"])
        load.append(float(r["load_mw"]))
        price.append(float(r["price_usd_mwh"]))
        temp.append(float(r["temperature_f"]))
    return {"timestamps": timestamps, "load_mw": load, "price_usd_mwh": price, "temperature_f": temp}


# ── Market metrics ────────────────────────────────────────────────────────────

@app.get("/api/{iso}/market")
def get_market(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["markets_output"]["metrics_csv"]))
    metrics = {r["metric"]: float(r["value"]) for r in rows}
    return metrics


# ── Queue outlook ─────────────────────────────────────────────────────────────

@app.get("/api/{iso}/queue")
def get_queue(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["queue_outlook_csv"]))
    by_year: dict[str, dict[str, float]] = {}
    by_tech: dict[str, dict[str, float]] = {}
    for r in rows:
        y, tech = r["year"], r["technology"]
        p50, p90 = float(r["expected_online_mw_p50"]), float(r["expected_online_mw_p90"])
        by_year.setdefault(y, {"p50": 0.0, "p90": 0.0})
        by_year[y]["p50"] += p50
        by_year[y]["p90"] += p90
        by_tech.setdefault(tech, {})
        by_tech[tech][y] = by_tech[tech].get(y, 0.0) + p50
    years = sorted(by_year)
    return {
        "years": years,
        "p50_mw": [by_year[y]["p50"] for y in years],
        "p90_mw": [by_year[y]["p90"] for y in years],
        "by_technology": by_tech,
    }


# ── Load forecast scenarios ───────────────────────────────────────────────────

@app.get("/api/{iso}/forecast")
def get_forecast(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["forecast_output"]["scenarios_csv"]))
    by_scenario: dict[str, dict[str, float]] = {}
    for r in rows:
        sc, yr = r["scenario"], r["year"]
        by_scenario.setdefault(sc, {})[yr] = float(r["avg_load_mw"])
    years = sorted(set(y for sc in by_scenario.values() for y in sc))
    return {
        "years": years,
        "low": [by_scenario.get("low", {}).get(y, 0.0) for y in years],
        "base": [by_scenario.get("base", {}).get(y, 0.0) for y in years],
        "high": [by_scenario.get("high", {}).get(y, 0.0) for y in years],
    }


# ── Finance scenarios ─────────────────────────────────────────────────────────

@app.get("/api/{iso}/finance")
def get_finance(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["finance_output"]["scenarios_csv"]))
    scenarios = []
    for r in rows:
        scenarios.append({
            "scenario_id": int(r["scenario_id"]),
            "contract_type": r["contract_type"],
            "price_case": r["price_case"],
            "capex_case": r["capex_case"],
            "price_multiplier": float(r["price_multiplier"]),
            "capex_multiplier": float(r["capex_multiplier"]),
            "npv_musd": float(r["npv_musd"]),
            "after_tax_npv_musd": float(r["after_tax_npv_musd"]),
            "irr": float(r["irr"]),
            "min_dscr": float(r["min_dscr"]),
            "avg_dscr": float(r["avg_dscr"]),
            "lcoe_usd_mwh": float(r["lcoe_usd_mwh"]),
            "year1_revenue_musd": float(r["year1_revenue_musd"]),
        })

    summary_rows = _read_csv(Path(cfg["finance_output"]["summary_csv"]))
    summary = {r["metric"]: r["value"] for r in summary_rows}

    sens_rows = _read_csv(Path(cfg["finance_output"]["sensitivity_csv"]))
    sensitivity = [{"driver": r["driver"], "delta_npv_musd": float(r["delta_npv_musd"])} for r in sens_rows]

    return {"scenarios": scenarios, "summary": summary, "sensitivity": sensitivity}


# ── Pipeline trigger ──────────────────────────────────────────────────────────

@app.post("/api/run/{iso}")
def run_pipeline(iso: str):
    key = iso.upper()
    if key not in ISO_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Unknown ISO: {iso}")
    cfg_path = ISO_CONFIGS[key]
    stages = ["ingest", "transform", "forecast", "queue", "markets", "finance", "charts"]
    errors = []
    for stage in stages:
        result = subprocess.run(
            [sys.executable, "-m", "energy_analytics", stage, "--config", cfg_path],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            errors.append(f"{stage}: {result.stderr.strip()}")
    if errors:
        raise HTTPException(status_code=500, detail="\n".join(errors))
    return {"status": "ok", "iso": key, "stages_run": stages}


@app.post("/api/run-all")
def run_all_pipelines():
    errors = []
    for iso, cfg_path in ISO_CONFIGS.items():
        stages = ["ingest", "transform", "forecast", "queue", "markets", "finance", "charts"]
        for stage in stages:
            result = subprocess.run(
                [sys.executable, "-m", "energy_analytics", stage, "--config", cfg_path],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                errors.append(f"{iso}/{stage}: {result.stderr.strip()}")
    if errors:
        raise HTTPException(status_code=500, detail="\n".join(errors))
    return {"status": "ok", "isos_run": list(ISO_CONFIGS)}
