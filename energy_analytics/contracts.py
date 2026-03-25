from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from energy_analytics.config import resolve_project_path


def load_contracts(path: str = "config/schema_contracts.yml") -> dict[str, Any]:
    with resolve_project_path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _is_type(value: str, type_name: str) -> bool:
    if type_name == "string":
        return value != ""
    if type_name == "float":
        try:
            float(value)
            return True
        except ValueError:
            return False
    if type_name == "int":
        try:
            int(value)
            return True
        except ValueError:
            return False
    if type_name == "date":
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    if type_name == "datetime":
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False
    return True


def validate_csv_contract(path: Path, contract: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required_columns: list[str] = contract.get("required_columns", [])
    column_types: dict[str, str] = contract.get("column_types", {})

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = set(reader.fieldnames or [])

        missing = [c for c in required_columns if c not in columns]
        if missing:
            errors.append(f"missing_columns={missing}")
            return errors

        for i, row in enumerate(reader, start=2):
            for col in required_columns:
                if row.get(col, "") == "":
                    errors.append(f"row={i} col={col} empty")
            for col, type_name in column_types.items():
                val = row.get(col, "")
                if val == "":
                    continue
                if not _is_type(val, type_name):
                    errors.append(f"row={i} col={col} type={type_name} value={val}")

            if len(errors) >= 25:
                errors.append("error_limit_reached")
                return errors

    return errors
