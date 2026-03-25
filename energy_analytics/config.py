from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent

_PATH_SECTIONS = {
    "sample_data",
    "raw_output",
    "staged_output",
    "curated_output",
    "forecast_output",
    "markets_output",
    "queue_model_output",
    "finance_output",
    "reports",
}

_PATH_KEYS = {
    "contracts_path",
    "manifest_output",
    "raw_snapshot_dir",
    "qa_report",
    "metadata_log",
    "charts_dir",
}


def project_root() -> Path:
    return PROJECT_ROOT


def resolve_project_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def _looks_like_local_path(value: str) -> bool:
    if value.startswith(("http://", "https://")):
        return False
    return "/" in value or value.startswith(".")


def _resolve_config_paths(value: Any, parent_key: str | None = None, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            child_key: _resolve_config_paths(child_value, parent_key=key, key=child_key)
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [_resolve_config_paths(item, parent_key=parent_key) for item in value]
    if isinstance(value, str) and _looks_like_local_path(value):
        if parent_key in _PATH_SECTIONS or key in _PATH_KEYS:
            return str(resolve_project_path(value))
    return value


def load_config(path: str = "config/data_sources.yml") -> dict[str, Any]:
    config_path = resolve_project_path(path)
    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return _resolve_config_paths(raw)
