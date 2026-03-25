# Energy Analytics Portfolio (Resume-Grade Build)

This repository implements a full analytics workflow for regional power-market analysis, from ingestion to forecasting, market metrics, project finance, and dashboard outputs.

## Scope
- Data foundation (load, hub prices, weather)
- Queue normalization and expected online MW (P50/P90)
- Load forecast modeling with rolling backtests
- Market metrics (capture price, congestion proxy, negative-price metrics)
- Solar finance model (merchant + contracted structures)
- Dashboard and summary-report artifacts
- Reproducibility layer (schema contracts, manifests, snapshots, CI)
- Source adapters with stable endpoint handling (including Open-Meteo archive weather API)

## Architecture
See `docs/architecture.md` for a system diagram and reproducibility notes.

## Quick Start

```bash
make all
make test
```

## Command Reference

```bash
# Ingestion modes
make ingest          # deterministic sample mode
make ingest-real     # pull configured live URLs only
make ingest-hybrid   # live pull with sample fallback

# Core pipeline
make transform
make forecast
make queue
make markets
make finance
make charts
make dashboard
make qa
make status
make status-all
```

## Key Artifacts
- Ingestion provenance: `reports/ingestion_manifest.json`
- Curated panel: `data/curated/ercot_hourly_panel.csv`
- Forecast backtest: `data/marts/ercot_load_backtest.csv`
- Forecast scenarios: `data/marts/ercot_load_forecast_scenarios.csv`
- Queue outlook: `data/curated/ercot_queue_expected_online_mw.csv`
- Queue calibration: `data/marts/ercot_queue_calibration.csv`
- Market metrics: `data/marts/ercot_market_metrics.csv`
- Finance scenarios: `data/marts/ercot_finance_scenarios.csv`
- Dashboard: `reports/dashboard/index.html`
- QA report: `reports/qa_report.md`

## Documentation
- Assumptions table: `docs/assumptions.md`
- Data dictionary: `docs/data_dictionary.md`
- Load method: `docs/method_load.md`
- Queue method: `docs/method_queue.md`
- Markets method: `docs/method_markets.md`
- Finance method: `docs/method_finance.md`

## Engineering Quality
- CI pipeline: `.github/workflows/ci.yml`
- Pre-commit hooks: `.pre-commit-config.yaml`
- Formatting/lint config: `pyproject.toml`
- Locked runtime dep snapshot: `requirements-lock.txt`

## Stable Endpoint Hardening
- Weather real-data ingestion uses structured adapter config in `config/data_sources.yml`.
- Current weather adapter targets Open-Meteo Archive API and normalizes hourly JSON into contracted CSV schema.
- Hybrid mode falls back to sample data if endpoint retrieval or contract validation fails.
