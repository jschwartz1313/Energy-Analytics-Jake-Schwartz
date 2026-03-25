from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from energy_analytics.config import load_config, resolve_project_path

DEFAULT_CONFIGS = [
    "config/data_sources.yml",
    "config/caiso.yml",
    "config/pjm.yml",
    "config/miso.yml",
    "config/spp.yml",
    "config/nyiso.yml",
    "config/isone.yml",
]


def _required_artifacts(cfg: dict[str, Any]) -> dict[str, Path]:
    return {
        "panel": Path(cfg["curated_output"]["panel_csv"]),
        "queue_staged": Path(cfg["staged_output"]["queue_csv"]),
        "queue_outlook": Path(cfg["curated_output"]["queue_outlook_csv"]),
        "forecast_backtest": Path(cfg["forecast_output"]["backtest_csv"]),
        "forecast_metrics": Path(cfg["forecast_output"]["backtest_metrics_csv"]),
        "forecast_scenarios": Path(cfg["forecast_output"]["scenarios_csv"]),
        "market_metrics": Path(cfg["markets_output"]["metrics_csv"]),
        "market_hourly": Path(cfg["markets_output"]["hourly_csv"]),
        "finance_scenarios": Path(cfg["finance_output"]["scenarios_csv"]),
        "finance_summary": Path(cfg["finance_output"]["summary_csv"]),
        "finance_sensitivity": Path(cfg["finance_output"]["sensitivity_csv"]),
        "manifest": Path(cfg["ingestion"]["manifest_output"]),
        "qa_report": Path(cfg["reports"]["qa_report"]),
    }


def required_artifacts(cfg: dict[str, Any]) -> dict[str, Path]:
    return _required_artifacts(cfg)


def build_region_status(config_path: str) -> dict[str, Any]:
    cfg = load_config(config_path)
    artifacts = _required_artifacts(cfg)
    existing = {name: str(path) for name, path in artifacts.items() if path.exists()}
    missing = {name: str(path) for name, path in artifacts.items() if not path.exists()}

    manifest_summary: dict[str, Any] | None = None
    manifest_path = artifacts["manifest"]
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_summary = {
            "record_count": payload.get("record_count", 0),
            "generated_at": payload.get("manifest_generated_at_utc"),
        }

    return {
        "region": cfg["region"],
        "hub": cfg["hub"],
        "config_path": str(resolve_project_path(config_path)),
        "ready": not missing,
        "existing_count": len(existing),
        "missing_count": len(missing),
        "existing": existing,
        "missing": missing,
        "manifest": manifest_summary,
    }


def build_status(config_paths: list[str] | None = None) -> dict[str, Any]:
    statuses: list[dict[str, Any]] = []
    configs = config_paths or DEFAULT_CONFIGS

    for config_path in configs:
        statuses.append(build_region_status(config_path))

    return {
        "ready": all(status["ready"] for status in statuses),
        "regions": statuses,
    }


def format_status_report(status: dict[str, Any]) -> str:
    lines = [f"ready={status['ready']}"]
    for region in status["regions"]:
        lines.append(
            f"{region['region']}: ready={region['ready']} existing={region['existing_count']} missing={region['missing_count']}"
        )
        if region["missing"]:
            lines.append("  missing=" + ", ".join(sorted(region["missing"].keys())))
        if region["manifest"]:
            lines.append(
                f"  manifest_records={region['manifest']['record_count']} generated_at={region['manifest']['generated_at']}"
            )
    return "\n".join(lines)
