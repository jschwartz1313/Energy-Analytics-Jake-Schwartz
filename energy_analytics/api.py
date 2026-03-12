"""FastAPI backend for Energy Analytics — serves all ISO data as JSON endpoints."""
from __future__ import annotations

import csv
import math
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from energy_analytics.config import load_config
from energy_analytics.finance import _build_case

app = FastAPI(title="Energy Analytics API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ISO_CONFIGS: dict[str, str] = {
    "ERCOT": "config/data_sources.yml",
    "CAISO": "config/caiso.yml",
    "PJM":   "config/pjm.yml",
    "MISO":  "config/miso.yml",
    "SPP":   "config/spp.yml",
    "NYISO": "config/nyiso.yml",
    "ISO-NE":"config/isone.yml",
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
            ready = Path(cfg["curated_output"]["panel_csv"]).exists()
        except Exception:
            ready = False
        available.append({"iso": iso, "ready": ready})
    return {"isos": available}


# ── Panel ─────────────────────────────────────────────────────────────────────
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
    return {r["metric"]: float(r["value"]) for r in rows}


# ── Hourly enriched market data ───────────────────────────────────────────────
@app.get("/api/{iso}/hourly")
def get_hourly(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["markets_output"]["hourly_csv"]))
    out = []
    for r in rows:
        out.append({
            "timestamp": r["timestamp_utc"],
            "price": float(r["price_usd_mwh"]),
            "solar_profile": float(r["solar_profile"]),
            "wind_profile": float(r["wind_profile"]),
            "solar_weighted_price": float(r["solar_weighted_price"]),
            "wind_weighted_price": float(r["wind_weighted_price"]),
            "congestion_proxy": float(r["congestion_proxy"]),
            "is_negative": int(r["is_negative_price_hour"]),
        })
    return {"rows": out}


# ── Price duration curve ──────────────────────────────────────────────────────
@app.get("/api/{iso}/price-duration")
def get_price_duration(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    prices = sorted([float(r["price_usd_mwh"]) for r in rows], reverse=True)
    n = len(prices)
    pct = [round(100 * i / n, 2) for i in range(n)]
    return {"percentile": pct, "price_usd_mwh": prices}


# ── Monthly aggregations ──────────────────────────────────────────────────────
@app.get("/api/{iso}/monthly")
def get_monthly(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    buckets: dict[str, dict[str, list]] = defaultdict(lambda: {"load": [], "price": [], "temp": []})
    for r in rows:
        month = r["timestamp_utc"][:7]  # YYYY-MM
        buckets[month]["load"].append(float(r["load_mw"]))
        buckets[month]["price"].append(float(r["price_usd_mwh"]))
        buckets[month]["temp"].append(float(r["temperature_f"]))
    months = sorted(buckets)
    return {
        "months": months,
        "avg_load_mw": [sum(buckets[m]["load"]) / len(buckets[m]["load"]) for m in months],
        "avg_price_usd_mwh": [sum(buckets[m]["price"]) / len(buckets[m]["price"]) for m in months],
        "max_price_usd_mwh": [max(buckets[m]["price"]) for m in months],
        "min_price_usd_mwh": [min(buckets[m]["price"]) for m in months],
        "avg_temp_f": [sum(buckets[m]["temp"]) / len(buckets[m]["temp"]) for m in months],
        "negative_hours": [sum(1 for p in buckets[m]["price"] if p < 0) for m in months],
    }


# ── Hourly price heatmap (hour-of-day × day) ─────────────────────────────────
@app.get("/api/{iso}/heatmap")
def get_heatmap(iso: str, metric: str = "price"):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    # Build matrix: 24 hours × up to 31 days
    by_hour: dict[int, list[float]] = defaultdict(list)
    by_dow: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        ts = r["timestamp_utc"]
        hour = int(ts[11:13])
        val = float(r["price_usd_mwh"] if metric == "price" else r["load_mw"])
        by_hour[hour].append(val)
        # day label
        day = ts[:10]
        by_dow[day][hour].append(val)
    # Return avg by hour-of-day
    hours = list(range(24))
    avg_by_hour = [sum(by_hour[h]) / len(by_hour[h]) if by_hour[h] else 0 for h in hours]
    # Full matrix: days x hours
    days = sorted(by_dow.keys())
    matrix = [[sum(by_dow[d][h]) / len(by_dow[d][h]) if by_dow[d][h] else None for h in hours] for d in days]
    return {"hours": hours, "avg_by_hour": avg_by_hour, "days": days, "matrix": matrix}


# ── Queue detail ──────────────────────────────────────────────────────────────
@app.get("/api/{iso}/queue-detail")
def get_queue_detail(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["staged_output"]["queue_csv"]))
    projects = []
    for r in rows:
        projects.append({
            "queue_id": r["queue_id"],
            "project_name": r["project_name"],
            "technology": r["technology"],
            "mw": float(r["mw"]),
            "status": r["status"],
            "target_cod_year": r["target_cod_year"],
            "p50": float(r["completion_probability_p50"]),
            "p90": float(r["completion_probability_p90"]),
        })
    # Status breakdown
    status_counts: dict[str, float] = defaultdict(float)
    tech_counts: dict[str, float] = defaultdict(float)
    for p in projects:
        status_counts[p["status"]] += p["mw"]
        tech_counts[p["technology"]] += p["mw"]
    return {
        "projects": projects,
        "by_status_mw": dict(status_counts),
        "by_technology_mw": dict(tech_counts),
        "total_mw": sum(p["mw"] for p in projects),
    }


# ── Queue outlook (aggregated) ────────────────────────────────────────────────
@app.get("/api/{iso}/queue")
def get_queue(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["queue_outlook_csv"]))
    by_year: dict[str, dict[str, float]] = defaultdict(lambda: {"p50": 0.0, "p90": 0.0})
    by_tech: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        y, tech = r["year"], r["technology"]
        p50, p90 = float(r["expected_online_mw_p50"]), float(r["expected_online_mw_p90"])
        by_year[y]["p50"] += p50
        by_year[y]["p90"] += p90
        by_tech[tech][y] = by_tech[tech].get(y, 0.0) + p50
    years = sorted(by_year)
    return {
        "years": years,
        "p50_mw": [by_year[y]["p50"] for y in years],
        "p90_mw": [by_year[y]["p90"] for y in years],
        "by_technology": dict(by_tech),
        "cumulative_p50": list(_cumsum([by_year[y]["p50"] for y in years])),
    }


def _cumsum(vals: list[float]) -> list[float]:
    total = 0.0
    for v in vals:
        total += v
        yield total


# ── Load forecast ─────────────────────────────────────────────────────────────
@app.get("/api/{iso}/forecast")
def get_forecast(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["forecast_output"]["scenarios_csv"]))
    by_sc: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        by_sc[r["scenario"]][r["year"]] = float(r["avg_load_mw"])
    years = sorted(set(y for sc in by_sc.values() for y in sc))
    return {
        "years": years,
        "low":  [by_sc.get("low",  {}).get(y, 0.0) for y in years],
        "base": [by_sc.get("base", {}).get(y, 0.0) for y in years],
        "high": [by_sc.get("high", {}).get(y, 0.0) for y in years],
    }


# ── Finance ───────────────────────────────────────────────────────────────────
@app.get("/api/{iso}/finance")
def get_finance(iso: str):
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["finance_output"]["scenarios_csv"]))
    scenarios = [{
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
    } for r in rows]

    summary = {r["metric"]: r["value"] for r in _read_csv(Path(cfg["finance_output"]["summary_csv"]))}
    sens = [{"driver": r["driver"], "delta_npv_musd": float(r["delta_npv_musd"])}
            for r in _read_csv(Path(cfg["finance_output"]["sensitivity_csv"]))]

    return {"scenarios": scenarios, "summary": summary, "sensitivity": sens}


# ── Finance cash flows (computed on the fly) ──────────────────────────────────
@app.get("/api/{iso}/cashflows")
def get_cashflows(iso: str):
    cfg = _cfg(iso)
    assumptions = cfg["finance_assumptions"]
    metrics_path = Path(cfg["markets_output"]["metrics_csv"])
    metrics = {r["metric"]: float(r["value"]) for r in _read_csv(metrics_path)}
    base_capture = metrics.get("solar_capture_price_usd_mwh", 40.0)

    life = int(assumptions["project_life_years"])
    debt_tenor = int(assumptions["debt_tenor_years"])
    capacity_mw = float(assumptions["capacity_mw"])
    cap_factor = float(assumptions["solar_capacity_factor"])
    degradation = float(assumptions["degradation_rate"])
    capex_kw = float(assumptions["capex_per_kw"])
    opex_kw = float(assumptions["fixed_opex_per_kw_year"])
    debt_fraction = float(assumptions["debt_fraction"])
    debt_rate = float(assumptions["debt_rate"])
    contracted_adder = float(assumptions.get("contracted_price_adder_usd_mwh", 2.0))

    capacity_kw = capacity_mw * 1000.0
    capex = capacity_kw * capex_kw
    debt = capex * debt_fraction
    equity = capex - debt
    strike_price = base_capture + contracted_adder

    # Annuity payment
    if debt_rate == 0:
        ann = debt / debt_tenor
    else:
        ann = debt * (debt_rate * (1 + debt_rate)**debt_tenor) / ((1 + debt_rate)**debt_tenor - 1)

    years, revenues, opex_list, debt_svc, eq_cfs, dscr_list = [], [], [], [], [], []
    for yr in range(1, life + 1):
        energy = capacity_mw * 8760 * cap_factor * ((1 - degradation) ** (yr - 1))
        rev = energy * strike_price / 1_000_000
        opex = capacity_kw * opex_kw / 1_000_000
        ds = ann / 1_000_000 if yr <= debt_tenor else 0.0
        eq_cf = rev - opex - ds
        years.append(yr)
        revenues.append(round(rev, 4))
        opex_list.append(round(opex, 4))
        debt_svc.append(round(ds, 4))
        eq_cfs.append(round(eq_cf, 4))
        if ds > 0:
            dscr_list.append(round((rev - opex) / ds, 4))
        else:
            dscr_list.append(None)

    return {
        "years": years,
        "revenue_musd": revenues,
        "opex_musd": opex_list,
        "debt_service_musd": debt_svc,
        "equity_cf_musd": eq_cfs,
        "dscr": dscr_list,
        "equity_invested_musd": round(equity / 1_000_000, 4),
        "total_capex_musd": round(capex / 1_000_000, 4),
        "capacity_mw": capacity_mw,
        "strike_price_usd_mwh": round(strike_price, 2),
    }


# ── Market findings text ──────────────────────────────────────────────────────
@app.get("/api/{iso}/findings")
def get_findings(iso: str):
    cfg = _cfg(iso)
    path = Path(cfg["markets_output"]["findings_md"])
    if not path.exists():
        return {"text": "No findings available."}
    return {"text": path.read_text(encoding="utf-8")}


# ── Forecast backtest metrics ─────────────────────────────────────────────────
@app.get("/api/{iso}/backtest")
def get_backtest(iso: str):
    cfg = _cfg(iso)
    metrics = {r["metric"]: r["value"] for r in _read_csv(Path(cfg["forecast_output"]["backtest_metrics_csv"]))}
    rows = _read_csv(Path(cfg["forecast_output"]["backtest_csv"]))
    actuals = [float(r.get("actual_load_mw", 0)) for r in rows]
    predicted = [float(r.get("predicted_load_mw", 0)) for r in rows]
    timestamps = [r.get("timestamp_utc", r.get("year", str(i))) for i, r in enumerate(rows)]
    return {"metrics": metrics, "timestamps": timestamps, "actuals": actuals, "predicted": predicted}


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
        for stage in ["ingest", "transform", "forecast", "queue", "markets", "finance", "charts"]:
            result = subprocess.run(
                [sys.executable, "-m", "energy_analytics", stage, "--config", cfg_path],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                errors.append(f"{iso}/{stage}: {result.stderr.strip()}")
    if errors:
        raise HTTPException(status_code=500, detail="\n".join(errors))
    return {"status": "ok", "isos_run": list(ISO_CONFIGS)}
