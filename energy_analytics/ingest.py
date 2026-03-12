from __future__ import annotations

import shutil
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.contracts import load_contracts, validate_csv_contract
from energy_analytics.metadata import log_metadata
from energy_analytics.provenance import build_manifest_record, now_utc_iso, write_manifest
from energy_analytics.sources import fetch_real_dataset_to_csv

DATASETS = ("load", "price", "weather", "queue")


def _copy_sample(sample_path: Path, out_path: Path) -> tuple[str, str]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(sample_path, out_path)
    return ("sample", str(sample_path))


def _write_snapshot(out_path: Path, snapshot_root: Path) -> None:
    snapshot_root.mkdir(parents=True, exist_ok=True)
    stamp = now_utc_iso().replace(":", "-")
    snap = snapshot_root / f"{out_path.stem}_{stamp}{out_path.suffix}"
    shutil.copy2(out_path, snap)


def run_ingest(mode_override: str | None = None, config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    ingest_cfg = cfg.get("ingestion", {})
    mode = mode_override or ingest_cfg.get("mode", "sample")
    allow_fallback = bool(ingest_cfg.get("allow_real_to_sample_fallback", True))
    enforce_contracts = bool(ingest_cfg.get("enforce_contracts", True))

    sample_src = cfg["sample_data"]
    real_src = cfg.get("real_data", {})
    raw_dst = cfg["raw_output"]
    log_path = cfg["reports"]["metadata_log"]
    manifest_path = Path(ingest_cfg.get("manifest_output", "reports/ingestion_manifest.json"))
    snapshot_root = Path(ingest_cfg.get("raw_snapshot_dir", "data/raw/snapshots"))

    contracts = load_contracts(ingest_cfg.get("contracts_path", "config/schema_contracts.yml"))
    manifest_records: list[dict[str, object]] = []

    for dataset in DATASETS:
        out_path = Path(raw_dst[dataset])
        source_type = ""
        source_ref = ""
        contract = contracts.get(dataset, {})

        if mode == "sample":
            source_type, source_ref = _copy_sample(Path(sample_src[dataset]), out_path)
            errors = validate_csv_contract(out_path, contract) if contract else []
        elif mode == "real":
            if dataset not in real_src:
                raise SystemExit(f"Missing real source config for dataset={dataset}")
            source_ref = fetch_real_dataset_to_csv(dataset, real_src[dataset], out_path, region=cfg["region"])
            source_type = "real"
            errors = validate_csv_contract(out_path, contract) if contract else []
        elif mode == "hybrid":
            try:
                if dataset not in real_src:
                    raise RuntimeError("missing real source config")
                source_ref = fetch_real_dataset_to_csv(dataset, real_src[dataset], out_path, region=cfg["region"])
                source_type = "real"
                errors = validate_csv_contract(out_path, contract) if contract else []
                if enforce_contracts and errors:
                    raise RuntimeError(f"contract validation failed for real source: {errors[:5]}")
            except Exception as exc:
                if not allow_fallback:
                    raise
                source_type, source_ref = _copy_sample(Path(sample_src[dataset]), out_path)
                errors = validate_csv_contract(out_path, contract) if contract else []
                log_metadata(log_path, f"ingest:fallback dataset={dataset} reason={exc}")
        else:
            raise SystemExit(f"Unsupported ingestion.mode={mode}; expected sample|real|hybrid")

        if enforce_contracts and errors:
            raise SystemExit(f"Contract validation failed dataset={dataset}: {errors[:5]}")

        _write_snapshot(out_path, snapshot_root)
        rec = build_manifest_record(
            dataset=dataset,
            target_path=out_path,
            source_type=source_type,
            source_ref=source_ref,
            contract_errors=errors,
        )
        manifest_records.append(rec)
        log_metadata(
            log_path,
            (
                f"ingest:{dataset} mode={mode} source_type={source_type} source_ref={source_ref} "
                f"rows={rec['row_count']} bytes={rec['file_bytes']} sha256={rec['sha256'][:12]}"
            ),
        )

    write_manifest(manifest_records, manifest_path)
    log_metadata(log_path, f"ingest_manifest:path={manifest_path} records={len(manifest_records)}")
    log_metadata(log_path, f"ingest complete region={cfg['region']} mode={mode}")


if __name__ == "__main__":
    run_ingest()
