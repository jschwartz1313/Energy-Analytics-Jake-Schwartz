from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from energy_analytics.config import load_config


def make_temp_config(*, include_extended_outputs: bool = False) -> Path:
    base_cfg = load_config()
    temp_dir = Path(tempfile.mkdtemp())

    base_cfg["ingestion"]["manifest_output"] = str(temp_dir / "reports" / "ingestion_manifest.json")
    base_cfg["ingestion"]["raw_snapshot_dir"] = str(temp_dir / "data" / "raw" / "snapshots")
    base_cfg["reports"]["metadata_log"] = str(temp_dir / "reports" / "ingestion_metadata.log")
    base_cfg["reports"]["qa_report"] = str(temp_dir / "reports" / "qa_report.md")
    base_cfg["reports"]["charts_dir"] = str(temp_dir / "reports" / "charts")

    for section in (
        "raw_output",
        "staged_output",
        "curated_output",
        "forecast_output",
        "markets_output",
        "queue_model_output",
        "finance_output",
    ):
        if section not in base_cfg:
            continue
        section_dir = section.replace("_output", "")
        base_cfg[section] = {
            key: str(temp_dir / "data" / section_dir / Path(path).name)
            for key, path in base_cfg[section].items()
        }

    if include_extended_outputs:
        base_cfg["markets_output"]["findings_md"] = str(temp_dir / "reports" / "market_findings.md")
        base_cfg["finance_output"]["sensitivity_chart_svg"] = str(temp_dir / "reports" / "charts" / "finance.svg")

    config_path = temp_dir / "config.yml"
    config_path.write_text(yaml.safe_dump(base_cfg, sort_keys=False), encoding="utf-8")
    return config_path
