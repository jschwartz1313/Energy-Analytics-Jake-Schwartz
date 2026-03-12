from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.metadata import log_metadata

QUEUE_COLUMNS = [
    "queue_id",
    "project_name",
    "technology",
    "mw",
    "status",
    "queue_date",
    "target_cod",
    "target_cod_year",
    "bus",
    "county",
    "completion_probability_p50",
    "completion_probability_p90",
]

OUTLOOK_COLUMNS = [
    "year",
    "technology",
    "project_count",
    "nameplate_mw",
    "expected_online_mw_p50",
    "expected_online_mw_p90",
]

CALIBRATION_COLUMNS = [
    "technology",
    "historical_projects",
    "historical_operational_projects",
    "observed_completion_rate",
    "mean_predicted_probability",
    "brier_score",
]

HEURISTIC_STATUS_PROB = {
    "operational": 1.00,
    "under_construction": 0.85,
    "active": 0.45,
    "in_study": 0.30,
    "submitted": 0.20,
    "suspended": 0.12,
    "withdrawn": 0.00,
    "cancelled": 0.00,
}

TERMINAL_STATUS = {"operational", "withdrawn", "cancelled"}


TECH_MAP = {
    "solar pv": "solar",
    "solar": "solar",
    "wind": "wind",
    "bess": "storage",
    "battery": "storage",
    "storage": "storage",
}

STATUS_MAP = {
    "submitted": "submitted",
    "active": "active",
    "in study": "in_study",
    "under construction": "under_construction",
    "operational": "operational",
    "withdrawn": "withdrawn",
    "cancelled": "cancelled",
    "suspended": "suspended",
}


def _normalize_technology(raw: str) -> str:
    return TECH_MAP.get(raw.strip().lower(), "other")


def _normalize_status(raw: str) -> str:
    return STATUS_MAP.get(raw.strip().lower(), "active")


def _blend_probability(status_prob: float, tech_rate: float | None) -> float:
    if tech_rate is None:
        return status_prob
    return round((0.55 * status_prob) + (0.45 * tech_rate), 4)


def _infer_tech_completion_rates(rows: list[dict[str, str]]) -> dict[str, float]:
    current_year = datetime.now(timezone.utc).year
    numer: dict[str, float] = defaultdict(float)
    denom: dict[str, float] = defaultdict(float)
    for row in rows:
        year = int(row["target_cod_year"])
        status = row["status"]
        if year >= current_year or status not in TERMINAL_STATUS:
            continue
        tech = row["technology"]
        denom[tech] += 1.0
        if status == "operational":
            numer[tech] += 1.0

    rates: dict[str, float] = {}
    for tech, d in denom.items():
        if d >= 2:
            rates[tech] = round(numer[tech] / d, 4)
    return rates


def _calibration_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    current_year = datetime.now(timezone.utc).year
    bucket: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "n": 0.0,
            "observed": 0.0,
            "pred_sum": 0.0,
            "brier_sum": 0.0,
        }
    )

    for row in rows:
        year = int(row["target_cod_year"])
        status = row["status"]
        if year >= current_year or status not in TERMINAL_STATUS:
            continue
        tech = row["technology"]
        pred = float(row["completion_probability_p50"])
        obs = 1.0 if status == "operational" else 0.0

        b = bucket[tech]
        b["n"] += 1.0
        b["observed"] += obs
        b["pred_sum"] += pred
        b["brier_sum"] += (pred - obs) ** 2

    out: list[dict[str, str]] = []
    for tech, b in sorted(bucket.items()):
        n = b["n"]
        out.append(
            {
                "technology": tech,
                "historical_projects": str(int(n)),
                "historical_operational_projects": str(int(b["observed"])),
                "observed_completion_rate": f"{(b['observed']/n if n else 0.0):.4f}",
                "mean_predicted_probability": f"{(b['pred_sum']/n if n else 0.0):.4f}",
                "brier_score": f"{(b['brier_sum']/n if n else 0.0):.4f}",
            }
        )
    return out


def run_queue_transform(config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    raw_path = Path(cfg["raw_output"]["queue"])
    staged_path = Path(cfg["staged_output"]["queue_csv"])
    outlook_path = Path(cfg["curated_output"]["queue_outlook_csv"])
    calibration_path = Path(cfg["queue_model_output"]["calibration_csv"])
    log_path = cfg["reports"]["metadata_log"]

    normalized_rows: list[dict[str, str]] = []
    with raw_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            tech = _normalize_technology(row["technology_raw"])
            status = _normalize_status(row["status_raw"])
            target_year = row["target_cod"][:4]
            normalized_rows.append(
                {
                    "queue_id": row["queue_id"],
                    "project_name": row["project_name"],
                    "technology": tech,
                    "mw": row["mw"],
                    "status": status,
                    "queue_date": row["queue_date"],
                    "target_cod": row["target_cod"],
                    "target_cod_year": target_year,
                    "bus": row["bus"],
                    "county": row["county"],
                    "completion_probability_p50": "0.0",
                    "completion_probability_p90": "0.0",
                }
            )

    tech_rates = _infer_tech_completion_rates(normalized_rows)

    for row in normalized_rows:
        status_prob = HEURISTIC_STATUS_PROB[row["status"]]
        tech_prob = tech_rates.get(row["technology"])
        p50 = _blend_probability(status_prob, tech_prob)

        # P90 here is conservative expected-completion volume.
        p90 = max(round(p50 - 0.20, 4), 0.0)
        if row["status"] == "operational":
            p50 = 1.0
            p90 = 1.0
        if row["status"] in {"withdrawn", "cancelled"}:
            p50 = 0.0
            p90 = 0.0

        row["completion_probability_p50"] = f"{p50:.4f}"
        row["completion_probability_p90"] = f"{p90:.4f}"

    staged_path.parent.mkdir(parents=True, exist_ok=True)
    with staged_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=QUEUE_COLUMNS)
        writer.writeheader()
        writer.writerows(normalized_rows)

    grouped: dict[tuple[int, str], dict[str, float]] = defaultdict(
        lambda: {
            "project_count": 0.0,
            "nameplate_mw": 0.0,
            "expected_online_mw_p50": 0.0,
            "expected_online_mw_p90": 0.0,
        }
    )

    for row in normalized_rows:
        year = int(row["target_cod_year"])
        tech = row["technology"]
        mw = float(row["mw"])
        p50 = float(row["completion_probability_p50"])
        p90 = float(row["completion_probability_p90"])

        bucket = grouped[(year, tech)]
        bucket["project_count"] += 1
        bucket["nameplate_mw"] += mw
        bucket["expected_online_mw_p50"] += mw * p50
        bucket["expected_online_mw_p90"] += mw * p90

    outlook_rows: list[dict[str, str]] = []
    for (year, tech), vals in sorted(grouped.items()):
        outlook_rows.append(
            {
                "year": str(year),
                "technology": tech,
                "project_count": str(int(vals["project_count"])),
                "nameplate_mw": f"{vals['nameplate_mw']:.2f}",
                "expected_online_mw_p50": f"{vals['expected_online_mw_p50']:.2f}",
                "expected_online_mw_p90": f"{vals['expected_online_mw_p90']:.2f}",
            }
        )

    outlook_path.parent.mkdir(parents=True, exist_ok=True)
    with outlook_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTLOOK_COLUMNS)
        writer.writeheader()
        writer.writerows(outlook_rows)

    calibration_rows = _calibration_rows(normalized_rows)
    calibration_path.parent.mkdir(parents=True, exist_ok=True)
    with calibration_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CALIBRATION_COLUMNS)
        writer.writeheader()
        writer.writerows(calibration_rows)

    log_metadata(
        log_path,
        (
            "queue_transform:"
            f"normalized_rows={len(normalized_rows)} "
            f"outlook_rows={len(outlook_rows)} "
            f"calibration_rows={len(calibration_rows)} "
            f"empirical_tech_rates={tech_rates}"
        ),
    )


if __name__ == "__main__":
    run_queue_transform()
