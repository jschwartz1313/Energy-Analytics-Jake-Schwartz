"""FastAPI backend for Energy Analytics — serves all ISO data as JSON endpoints."""
from __future__ import annotations

import csv
import json
import math
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from energy_analytics.config import load_config, resolve_project_path
from energy_analytics.finance import _build_case
from energy_analytics.status import build_region_status, build_status, required_artifacts

# ── Response cache: key → (expires_at, payload) ───────────────────────────────
_CACHE: dict[str, tuple[float, Any]] = {}
CACHE_TTL = 300  # seconds
ENABLE_PIPELINE_RUNS = os.getenv("ENERGY_ANALYTICS_ENABLE_PIPELINE_RUNS", "").lower() in {"1", "true", "yes"}
PIPELINE_STAGES = ["ingest", "transform", "forecast", "queue", "markets", "finance", "charts"]


def _cache_get(key: str) -> Any | None:
    entry = _CACHE.get(key)
    if entry and time.monotonic() < entry[0]:
        return entry[1]
    return None


def _cache_set(key: str, value: Any) -> None:
    _CACHE[key] = (time.monotonic() + CACHE_TTL, value)


def _sanitize_error(msg: str) -> str:
    """Strip absolute file paths from error messages before returning to clients."""
    msg = re.sub(r"/[^\s\"']*?/([^/\s\"']+\.(?:py|yml|csv|json))", r"\1", msg)
    msg = re.sub(r"[A-Za-z]:\\[^\s\"']*?\\([^\\s\"']+)", r"\1", msg)
    return msg[:500]


app = FastAPI(title="Energy Analytics API", version="2.1.0")

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

# Fallback carbon intensities used when not defined in ISO config
_DEFAULT_INTENSITY: dict[str, float] = {
    "ERCOT":  880.0,
    "CAISO":  490.0,
    "PJM":    840.0,
    "MISO":  1020.0,
    "SPP":    890.0,
    "NYISO":  390.0,
    "ISO-NE": 540.0,
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


def _required_artifacts(cfg: dict[str, Any]) -> list[Path]:
    return list(required_artifacts(cfg).values())


def _missing_artifacts(iso: str) -> list[str]:
    cfg = _cfg(iso)
    return [str(path) for path in _required_artifacts(cfg) if not path.exists()]


def _readiness_payload() -> dict[str, Any]:
    statuses = []
    for iso, config_path in ISO_CONFIGS.items():
        try:
            region_status = build_region_status(config_path)
            statuses.append({"iso": iso, "ready": region_status["ready"], "missing": list(region_status["missing"].values())[:5]})
        except Exception as exc:  # noqa: BLE001
            statuses.append({"iso": iso, "ready": False, "missing": [_sanitize_error(str(exc)) or "config load failed"]})
    not_ready = [status for status in statuses if not status["ready"]]
    return {
        "ready": not not_ready,
        "error": None if not not_ready else f"{len(not_ready)} ISO(s) missing required artifacts.",
        "isos": statuses,
    }


def _require_ready(iso: str | None = None) -> None:
    if iso is None:
        payload = _readiness_payload()
        if not payload["ready"]:
            raise HTTPException(status_code=503, detail=payload["error"])
        return
    missing = _missing_artifacts(iso)
    if missing:
        raise HTTPException(
            status_code=503,
            detail={
                "message": f"{iso.upper()} artifacts are not ready. Run the pipeline out-of-band before serving this endpoint.",
                "missing": missing[:5],
            },
        )


def _require_pipeline_runs_enabled() -> None:
    if not ENABLE_PIPELINE_RUNS:
        raise HTTPException(
            status_code=503,
            detail="Pipeline execution endpoints are disabled in the API process. Run the pipeline from CI, cron, or a worker job.",
        )


def _invalidate_iso_cache(iso: str) -> None:
    key = iso.upper()
    stale = [cache_key for cache_key in _CACHE if key in cache_key or cache_key == "correlation"]
    for cache_key in stale:
        _CACHE.pop(cache_key, None)


def _run_pipeline_stages(cfg_path: str) -> list[str]:
    errors = []
    for stage in PIPELINE_STAGES:
        result = subprocess.run(
            [sys.executable, "-m", "energy_analytics", stage, "--config", cfg_path],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            errors.append(f"{stage}: {_sanitize_error(result.stderr.strip())}")
    return errors


# ── Static files ──────────────────────────────────────────────────────────────
app.mount("/reports", StaticFiles(directory=resolve_project_path("reports")), name="reports")
app.mount("/data", StaticFiles(directory=resolve_project_path("data")), name="data")


@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    p = resolve_project_path("index.html")
    if not p.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return HTMLResponse(p.read_text(encoding="utf-8"))


# ── Readiness check ───────────────────────────────────────────────────────────
@app.get("/api/ready")
def check_ready():
    return _readiness_payload()


@app.get("/api/status")
def get_status():
    return build_status([cfg_path for cfg_path in ISO_CONFIGS.values()])


# ── ISO list ──────────────────────────────────────────────────────────────────
@app.get("/api/isos")
def list_isos():
    payload = _readiness_payload()
    return {"isos": payload["isos"]}


def _read_text_lines(path: Path, tail: int = 50) -> list[str]:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Artifact not found: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    return lines[-tail:]


def _artifact_url(path: Path) -> str:
    root = resolve_project_path(".")
    relative = path.resolve().relative_to(root.resolve())
    return "/" + str(relative).replace("\\", "/")


@app.get("/api/{iso}/artifacts")
def get_artifacts(iso: str):
    cfg = _cfg(iso)
    region_status = build_region_status(ISO_CONFIGS[iso.upper()])
    artifacts = []
    for name, path in required_artifacts(cfg).items():
        artifacts.append(
            {
                "name": name,
                "path": str(path),
                "exists": path.exists(),
                "size_bytes": path.stat().st_size if path.exists() else None,
                "url": _artifact_url(path) if path.exists() else None,
            }
        )
    return {
        "iso": iso.upper(),
        "region": region_status["region"],
        "ready": region_status["ready"],
        "artifacts": artifacts,
    }


@app.get("/api/{iso}/manifest")
def get_manifest(iso: str):
    cfg = _cfg(iso)
    manifest_path = Path(cfg["ingestion"]["manifest_output"])
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail=f"Manifest not found for {iso.upper()}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


@app.get("/api/{iso}/qa")
def get_qa_report(iso: str):
    cfg = _cfg(iso)
    report_path = Path(cfg["reports"]["qa_report"])
    lines = _read_text_lines(report_path, tail=400)
    text = "\n".join(lines)
    result = "PASS" if "Result: PASS" in text else "FAIL" if "Result: FAIL" in text else "UNKNOWN"
    return {"iso": iso.upper(), "result": result, "text": text, "path": str(report_path)}


@app.get("/api/{iso}/metadata-log")
def get_metadata_log(iso: str, tail: int = 50):
    cfg = _cfg(iso)
    tail = max(1, min(tail, 500))
    log_path = Path(cfg["reports"]["metadata_log"])
    lines = _read_text_lines(log_path, tail=tail)
    return {"iso": iso.upper(), "path": str(log_path), "lines": lines}


# ── Panel ─────────────────────────────────────────────────────────────────────
@app.get("/api/{iso}/panel")
def get_panel(iso: str):
    _require_ready(iso)
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
    _require_ready(iso)
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["markets_output"]["metrics_csv"]))
    return {r["metric"]: float(r["value"]) for r in rows}


# ── Hourly enriched market data ───────────────────────────────────────────────
@app.get("/api/{iso}/hourly")
def get_hourly(iso: str):
    _require_ready(iso)
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
    _require_ready(iso)
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    prices = sorted([float(r["price_usd_mwh"]) for r in rows], reverse=True)
    n = len(prices)
    pct = [round(100 * i / n, 2) for i in range(n)]
    return {"percentile": pct, "price_usd_mwh": prices}


# ── Monthly aggregations ──────────────────────────────────────────────────────
@app.get("/api/{iso}/monthly")
def get_monthly(iso: str):
    cache_key = f"monthly:{iso.upper()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    _require_ready(iso)
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    buckets: dict[str, dict[str, list]] = defaultdict(lambda: {"load": [], "price": [], "temp": []})
    for r in rows:
        month = r["timestamp_utc"][:7]  # YYYY-MM
        buckets[month]["load"].append(float(r["load_mw"]))
        buckets[month]["price"].append(float(r["price_usd_mwh"]))
        buckets[month]["temp"].append(float(r["temperature_f"]))
    months = sorted(buckets)
    result = {
        "months": months,
        "avg_load_mw": [sum(buckets[m]["load"]) / len(buckets[m]["load"]) for m in months],
        "avg_price_usd_mwh": [sum(buckets[m]["price"]) / len(buckets[m]["price"]) for m in months],
        "max_price_usd_mwh": [max(buckets[m]["price"]) for m in months],
        "min_price_usd_mwh": [min(buckets[m]["price"]) for m in months],
        "avg_temp_f": [sum(buckets[m]["temp"]) / len(buckets[m]["temp"]) for m in months],
        "negative_hours": [sum(1 for p in buckets[m]["price"] if p < 0) for m in months],
    }
    _cache_set(cache_key, result)
    return result


# ── Hourly price heatmap (hour-of-day × day) ─────────────────────────────────
@app.get("/api/{iso}/heatmap")
def get_heatmap(iso: str, metric: str = "price"):
    cache_key = f"heatmap:{iso.upper()}:{metric}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    _require_ready(iso)
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
    result = {"hours": hours, "avg_by_hour": avg_by_hour, "days": days, "matrix": matrix}
    _cache_set(cache_key, result)
    return result


# ── Queue detail ──────────────────────────────────────────────────────────────
@app.get("/api/{iso}/queue-detail")
def get_queue_detail(iso: str):
    _require_ready(iso)
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
    _require_ready(iso)
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
    _require_ready(iso)
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
    _require_ready(iso)
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
    _require_ready(iso)
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
    _require_ready(iso)
    cfg = _cfg(iso)
    path = Path(cfg["markets_output"]["findings_md"])
    if not path.exists():
        return {"text": "No findings available."}
    return {"text": path.read_text(encoding="utf-8")}


# ── Forecast backtest metrics ─────────────────────────────────────────────────
@app.get("/api/{iso}/backtest")
def get_backtest(iso: str):
    _require_ready(iso)
    cfg = _cfg(iso)
    metrics = {r["metric"]: r["value"] for r in _read_csv(Path(cfg["forecast_output"]["backtest_metrics_csv"]))}
    rows = _read_csv(Path(cfg["forecast_output"]["backtest_csv"]))
    actuals = [float(r.get("actual_load_mw", 0)) for r in rows]
    naive_predicted = [float(r.get("naive_forecast_mw", 0)) for r in rows]
    weather_predicted = [float(r.get("weather_forecast_mw", 0)) for r in rows]
    best_model = metrics.get("best_model", "weather_linear")
    predicted = weather_predicted if best_model == "weather_linear" else naive_predicted
    timestamps = [r.get("timestamp_utc", r.get("year", str(i))) for i, r in enumerate(rows)]
    return {
        "metrics": metrics,
        "timestamps": timestamps,
        "actuals": actuals,
        "predicted": predicted,
        "naive_predicted": naive_predicted,
        "weather_predicted": weather_predicted,
        "best_model": best_model,
    }


# ── Pipeline trigger ──────────────────────────────────────────────────────────
@app.post("/api/run/{iso}")
def run_pipeline(iso: str):
    _require_pipeline_runs_enabled()
    key = iso.upper()
    if key not in ISO_CONFIGS:
        raise HTTPException(status_code=404, detail=f"Unknown ISO: {iso}")
    cfg_path = ISO_CONFIGS[key]
    errors = _run_pipeline_stages(cfg_path)
    if errors:
        raise HTTPException(status_code=500, detail="\n".join(errors))
    _invalidate_iso_cache(key)
    return {"status": "ok", "iso": key, "stages_run": PIPELINE_STAGES}


@app.post("/api/run-all")
def run_all_pipelines():
    _require_pipeline_runs_enabled()
    errors = []
    for iso, cfg_path in ISO_CONFIGS.items():
        errors.extend(f"{iso}/{error}" for error in _run_pipeline_stages(cfg_path))
    if errors:
        raise HTTPException(status_code=500, detail="\n".join(errors))
    _CACHE.clear()
    return {"status": "ok", "isos_run": list(ISO_CONFIGS)}


# ── Cross-ISO price correlation matrix ───────────────────────────────────────
@app.get("/api/correlation")
def get_correlation():
    """Compute Pearson correlation matrix of hourly prices across all ISOs."""
    cached = _cache_get("correlation")
    if cached is not None:
        return cached

    _require_ready()
    iso_prices: dict[str, list[float]] = {}

    for iso, cfg_path in ISO_CONFIGS.items():
        try:
            cfg = load_config(cfg_path)
            panel_path = Path(cfg["curated_output"]["panel_csv"])
            if not panel_path.exists():
                continue
            with panel_path.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))
            prices = [float(r["price_usd_mwh"]) for r in rows]
            if prices:
                iso_prices[iso] = prices
        except Exception:
            continue

    isos = sorted(iso_prices)
    if not isos:
        return {"isos": [], "matrix": []}

    # Trim all series to minimum shared length
    min_len = min(len(iso_prices[iso]) for iso in isos)
    trimmed = {iso: iso_prices[iso][:min_len] for iso in isos}

    def _pearson(x: list[float], y: list[float]) -> float:
        n = len(x)
        if n == 0:
            return 0.0
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        num = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        den_x = math.sqrt(sum((v - mean_x) ** 2 for v in x))
        den_y = math.sqrt(sum((v - mean_y) ** 2 for v in y))
        if den_x == 0 or den_y == 0:
            return 0.0
        return round(num / (den_x * den_y), 6)

    matrix = [
        [_pearson(trimmed[row_iso], trimmed[col_iso]) for col_iso in isos]
        for row_iso in isos
    ]

    result = {"isos": isos, "matrix": matrix}
    _cache_set("correlation", result)
    return result


# ── Grid carbon intensity ─────────────────────────────────────────────────────
@app.get("/api/{iso}/emissions")
def get_emissions(iso: str):
    """Estimate hourly grid carbon intensity scaled by price relative to average."""
    cfg = _cfg(iso)
    key = iso.upper()
    _require_ready(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))

    # Read from config; fall back to hardcoded defaults for backward compatibility
    base_intensity = float(
        cfg.get("carbon_intensity_lbs_mwh", _DEFAULT_INTENSITY.get(key, 700.0))
    )
    prices = [float(r["price_usd_mwh"]) for r in rows]
    timestamps = [r["timestamp_utc"] for r in rows]

    n = len(prices)
    avg_price = sum(prices) / n if n > 0 else 1.0

    co2_intensity_list: list[float] = []
    co2_tons_per_hour_list: list[float] = []

    for price in prices:
        if price < 0:
            scale = 0.3
        else:
            raw_scale = 0.8 + 0.4 * (price / avg_price) if avg_price != 0 else 0.8
            scale = max(0.7, min(1.4, raw_scale))
        intensity = round(base_intensity * scale, 4)
        # lbs to tons: divide by 2000; MWh generated assumed 1 MWh per hour per MW
        # intensity is lbs/MWh; co2_tons = intensity * 1 MWh / 2000
        co2_tons = round(intensity / 2000.0, 6)
        co2_intensity_list.append(intensity)
        co2_tons_per_hour_list.append(co2_tons)

    avg_intensity = round(sum(co2_intensity_list) / len(co2_intensity_list), 4) if co2_intensity_list else 0.0
    total_co2_tons = round(sum(co2_tons_per_hour_list), 4)

    return {
        "timestamps": timestamps,
        "co2_intensity_lbs_mwh": co2_intensity_list,
        "co2_tons_per_hour": co2_tons_per_hour_list,
        "avg_intensity": avg_intensity,
        "total_co2_tons": total_co2_tons,
        "base_intensity_lbs_mwh": base_intensity,
    }


# ── Battery storage arbitrage ─────────────────────────────────────────────────
@app.get("/api/{iso}/storage")
def get_storage(iso: str):
    """100MW / 4hr battery storage daily arbitrage analysis."""
    _require_ready(iso)
    cfg = _cfg(iso)
    rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))

    capacity_mw = 100.0
    duration_hrs = 4
    rte = 0.85

    # Group by day
    by_day: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        day = r["timestamp_utc"][:10]
        by_day[day].append(float(r["price_usd_mwh"]))

    days_sorted = sorted(by_day)
    daily_revenues: list[float] = []
    daily_spreads: list[float] = []

    for day in days_sorted:
        day_prices = by_day[day]
        if len(day_prices) < duration_hrs * 2:
            daily_revenues.append(0.0)
            daily_spreads.append(0.0)
            continue
        sorted_asc = sorted(day_prices)
        sorted_desc = sorted(day_prices, reverse=True)
        charge_prices = sorted_asc[:duration_hrs]
        discharge_prices = sorted_desc[:duration_hrs]
        sum_charge = sum(charge_prices)
        sum_discharge = sum(discharge_prices)
        # Revenue = (discharge energy * rte - charge cost) * capacity_mw
        # Each hour: 1 MWh per MW, so total MWh = capacity_mw * duration_hrs
        # Revenue ($) = (sum_discharge * rte - sum_charge) * capacity_mw
        revenue_usd = (sum_discharge * rte - sum_charge) * capacity_mw
        revenue_k = revenue_usd / 1000.0
        daily_revenues.append(round(revenue_k, 4))
        daily_spreads.append(round(sum_discharge / duration_hrs - sum_charge / duration_hrs, 4))

    n_days = len(daily_revenues)
    avg_daily_revenue_k = sum(daily_revenues) / n_days if n_days > 0 else 0.0
    # Annualize: scale by 365 / actual days in dataset
    annual_revenue_usd = avg_daily_revenue_k * 365.0 * 1000.0
    annual_revenue_musd = round(annual_revenue_usd / 1_000_000, 4)

    total_capex_musd = round(capacity_mw * 1000.0 * duration_hrs * 300.0 / 1_000_000, 4)  # $300/kWh

    avg_daily_spread = round(sum(daily_spreads) / n_days, 4) if n_days > 0 else 0.0
    simple_payback_yrs = round(total_capex_musd / annual_revenue_musd, 2) if annual_revenue_musd > 0 else None

    return {
        "days": days_sorted,
        "daily_revenue_k": daily_revenues,
        "annual_revenue_musd": annual_revenue_musd,
        "total_capex_musd": total_capex_musd,
        "capacity_mw": capacity_mw,
        "duration_hrs": duration_hrs,
        "rte": rte,
        "avg_daily_spread": avg_daily_spread,
        "simple_payback_yrs": simple_payback_yrs,
    }


# ── Custom finance assumptions ────────────────────────────────────────────────
@app.post("/api/finance/custom")
def custom_finance(
    iso: str = "ERCOT",
    overrides: dict[str, Any] = Body(default={}),
):
    """Run finance model with custom assumption overrides."""
    _require_ready(iso)
    cfg = _cfg(iso)
    assumptions = dict(cfg["finance_assumptions"])

    metrics_path = Path(cfg["markets_output"]["metrics_csv"])
    metrics = {r["metric"]: float(r["value"]) for r in _read_csv(metrics_path)}
    base_capture = metrics.get("solar_capture_price_usd_mwh", 40.0)

    # Fields that can be overridden with their valid numeric ranges
    override_schema: dict[str, tuple[float, float]] = {
        "capex_per_kw":          (400.0,   3000.0),
        "solar_capacity_factor": (0.05,    0.50),
        "debt_fraction":         (0.0,     0.95),
        "debt_rate":             (0.01,    0.20),
        "equity_discount_rate":  (0.03,    0.30),
        "project_life_years":    (5.0,     40.0),
        "itc_rate":              (0.0,     0.50),
    }
    unknown = [k for k in overrides if k not in override_schema]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown override field(s): {unknown}. Valid: {sorted(override_schema)}",
        )
    for field, (lo, hi) in override_schema.items():
        if field not in overrides:
            continue
        try:
            val = float(overrides[field])
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail=f"Field '{field}' must be a number.")
        if not (lo <= val <= hi):
            raise HTTPException(
                status_code=422,
                detail=f"Field '{field}' value {val} out of range [{lo}, {hi}].",
            )
        assumptions[field] = val

    result = _build_case(base_capture, assumptions, 1.0, 1.0, "contracted")

    return {
        "npv_musd": round(result["npv"] / 1_000_000, 4),
        "after_tax_npv_musd": round(result["after_tax_npv"] / 1_000_000, 4),
        "irr": round(result["irr"], 4),
        "min_dscr": round(result["min_dscr"], 4),
        "avg_dscr": round(result["avg_dscr"], 4),
        "lcoe_usd_mwh": round(result["lcoe"], 4),
    }


# ── Cron refresh trigger ──────────────────────────────────────────────────────
@app.post("/api/cron/refresh")
def cron_refresh():
    """Trigger full all-regions pipeline refresh (for external cron services)."""
    _require_pipeline_runs_enabled()
    errors = []
    for iso, cfg_path in ISO_CONFIGS.items():
        errors.extend(f"{iso}/{error}" for error in _run_pipeline_stages(cfg_path))
    if errors:
        raise HTTPException(status_code=500, detail="\n".join(errors))
    _CACHE.clear()
    return {"status": "ok", "message": "Pipeline refresh complete"}
