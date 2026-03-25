from __future__ import annotations

import argparse

from energy_analytics.charts import run_charts
from energy_analytics.dashboard import run_dashboard, run_all_iso_dashboard
from energy_analytics.finance import run_finance
from energy_analytics.forecast import run_forecast
from energy_analytics.ingest import run_ingest
from energy_analytics.markets import run_markets
from energy_analytics.qa import run_qa
from energy_analytics.queue import run_queue_transform
from energy_analytics.status import build_status, format_status_report
from energy_analytics.transform import run_transform

ALL_ISO_CONFIGS = [
    "config/data_sources.yml",
    "config/caiso.yml",
    "config/pjm.yml",
    "config/miso.yml",
    "config/spp.yml",
    "config/nyiso.yml",
    "config/isone.yml",
]


def _run_pipeline(config_path: str) -> None:
    run_ingest(config_path=config_path)
    run_transform(config_path=config_path)
    run_forecast(config_path=config_path)
    run_queue_transform(config_path=config_path)
    run_markets(config_path=config_path)
    run_finance(config_path=config_path)
    run_charts(config_path=config_path)
    run_qa(config_path=config_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Energy analytics pipeline")
    parser.add_argument(
        "command",
        choices=[
            "ingest",
            "ingest-real",
            "ingest-hybrid",
            "transform",
            "forecast",
            "queue",
            "markets",
            "finance",
            "charts",
            "dashboard",
            "qa",
            "status",
            "status-all",
            "run-all",
            "all-regions",
        ],
        help="Pipeline command",
    )
    parser.add_argument(
        "--config",
        default="config/data_sources.yml",
        help="Path to config YAML (default: config/data_sources.yml)",
    )
    args = parser.parse_args()

    if args.command == "ingest":
        run_ingest(config_path=args.config)
    elif args.command == "ingest-real":
        run_ingest(mode_override="real", config_path=args.config)
    elif args.command == "ingest-hybrid":
        run_ingest(mode_override="hybrid", config_path=args.config)
    elif args.command == "transform":
        run_transform(config_path=args.config)
    elif args.command == "forecast":
        run_forecast(config_path=args.config)
    elif args.command == "queue":
        run_queue_transform(config_path=args.config)
    elif args.command == "markets":
        run_markets(config_path=args.config)
    elif args.command == "finance":
        run_finance(config_path=args.config)
    elif args.command == "charts":
        run_charts(config_path=args.config)
    elif args.command == "dashboard":
        run_all_iso_dashboard(ALL_ISO_CONFIGS)
    elif args.command == "qa":
        run_qa(config_path=args.config)
    elif args.command == "status":
        print(format_status_report(build_status([args.config])))
    elif args.command == "status-all":
        print(format_status_report(build_status(ALL_ISO_CONFIGS)))
    elif args.command == "all-regions":
        for cfg_path in ALL_ISO_CONFIGS:
            print(f"\n=== Running pipeline for {cfg_path} ===")
            _run_pipeline(cfg_path)
        run_all_iso_dashboard(ALL_ISO_CONFIGS)
    else:
        _run_pipeline(args.config)
        run_all_iso_dashboard(ALL_ISO_CONFIGS)


if __name__ == "__main__":
    main()
