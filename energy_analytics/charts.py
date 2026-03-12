from __future__ import annotations

import csv
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.metadata import log_metadata


def _scale(values: list[float], lo: float, hi: float) -> list[float]:
    vmin = min(values)
    vmax = max(values)
    if vmax == vmin:
        return [(lo + hi) / 2 for _ in values]
    return [lo + (v - vmin) * (hi - lo) / (vmax - vmin) for v in values]


def _line_svg(title: str, x_labels: list[str], y_values: list[float], out_path: Path, color: str) -> None:
    width, height = 1000, 360
    left, right, top, bottom = 60, 20, 40, 40
    plot_w = width - left - right
    plot_h = height - top - bottom

    x_step = plot_w / max(1, len(y_values) - 1)
    xs = [left + i * x_step for i in range(len(y_values))]
    ys = [height - y for y in _scale(y_values, bottom, bottom + plot_h)]
    pts = " ".join(f"{x:.2f},{y:.2f}" for x, y in zip(xs, ys))

    y_min, y_max = min(y_values), max(y_values)
    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>\n")
        f.write("<rect x='0' y='0' width='100%' height='100%' fill='white'/>\n")
        f.write(f"<text x='20' y='24' font-size='16' font-family='Arial'>{title}</text>\n")
        f.write(f"<text x='8' y='{top + 5}' font-size='11' font-family='Arial'>{y_max:.1f}</text>\n")
        f.write(f"<text x='8' y='{height - bottom + 5}' font-size='11' font-family='Arial'>{y_min:.1f}</text>\n")
        f.write(
            f"<line x1='{left}' y1='{height-bottom}' x2='{width-right}' y2='{height-bottom}' stroke='#666'/>\n"
        )
        f.write(f"<line x1='{left}' y1='{top}' x2='{left}' y2='{height-bottom}' stroke='#666'/>\n")
        f.write(f"<polyline fill='none' stroke='{color}' stroke-width='2' points='{pts}'/>\n")
        if x_labels:
            f.write(
                f"<text x='{left}' y='{height-8}' font-size='10' font-family='Arial'>{x_labels[0][:13]}</text>\n"
            )
            f.write(
                f"<text x='{width-right-120}' y='{height-8}' font-size='10' font-family='Arial'>{x_labels[-1][:13]}</text>\n"
            )
        f.write("</svg>\n")


def _dual_line_svg(
    title: str,
    x_labels: list[str],
    y_values_a: list[float],
    y_values_b: list[float],
    out_path: Path,
    color_a: str,
    color_b: str,
    legend_a: str,
    legend_b: str,
) -> None:
    merged_min = min(min(y_values_a), min(y_values_b))
    merged_max = max(max(y_values_a), max(y_values_b))
    if merged_max == merged_min:
        merged_max = merged_min + 1

    width, height = 1000, 360
    left, right, top, bottom = 60, 20, 40, 40
    plot_w = width - left - right
    plot_h = height - top - bottom

    x_step = plot_w / max(1, len(y_values_a) - 1)
    xs = [left + i * x_step for i in range(len(y_values_a))]

    def _to_y(v: float) -> float:
        return height - (bottom + (v - merged_min) * plot_h / (merged_max - merged_min))

    pts_a = " ".join(f"{x:.2f},{_to_y(y):.2f}" for x, y in zip(xs, y_values_a))
    pts_b = " ".join(f"{x:.2f},{_to_y(y):.2f}" for x, y in zip(xs, y_values_b))

    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>\n")
        f.write("<rect x='0' y='0' width='100%' height='100%' fill='white'/>\n")
        f.write(f"<text x='20' y='24' font-size='16' font-family='Arial'>{title}</text>\n")
        f.write(f"<text x='8' y='{top + 5}' font-size='11' font-family='Arial'>{merged_max:.1f}</text>\n")
        f.write(f"<text x='8' y='{height - bottom + 5}' font-size='11' font-family='Arial'>{merged_min:.1f}</text>\n")
        f.write(
            f"<line x1='{left}' y1='{height-bottom}' x2='{width-right}' y2='{height-bottom}' stroke='#666'/>\n"
        )
        f.write(f"<line x1='{left}' y1='{top}' x2='{left}' y2='{height-bottom}' stroke='#666'/>\n")
        f.write(f"<polyline fill='none' stroke='{color_a}' stroke-width='2' points='{pts_a}'/>\n")
        f.write(f"<polyline fill='none' stroke='{color_b}' stroke-width='2' points='{pts_b}'/>\n")
        if x_labels:
            f.write(
                f"<text x='{left}' y='{height-8}' font-size='10' font-family='Arial'>{x_labels[0][:13]}</text>\n"
            )
            f.write(
                f"<text x='{width-right-120}' y='{height-8}' font-size='10' font-family='Arial'>{x_labels[-1][:13]}</text>\n"
            )
        f.write(f"<text x='{width-220}' y='26' font-size='11' font-family='Arial' fill='{color_a}'>{legend_a}</text>\n")
        f.write(f"<text x='{width-120}' y='26' font-size='11' font-family='Arial' fill='{color_b}'>{legend_b}</text>\n")
        f.write("</svg>\n")


def _triple_line_svg(
    title: str,
    x_labels: list[str],
    y_values_a: list[float],
    y_values_b: list[float],
    y_values_c: list[float],
    out_path: Path,
    color_a: str,
    color_b: str,
    color_c: str,
    legend_a: str,
    legend_b: str,
    legend_c: str,
) -> None:
    merged_min = min(min(y_values_a), min(y_values_b), min(y_values_c))
    merged_max = max(max(y_values_a), max(y_values_b), max(y_values_c))
    if merged_max == merged_min:
        merged_max = merged_min + 1

    width, height = 1000, 360
    left, right, top, bottom = 60, 20, 40, 40
    plot_w = width - left - right
    plot_h = height - top - bottom
    x_step = plot_w / max(1, len(y_values_a) - 1)
    xs = [left + i * x_step for i in range(len(y_values_a))]

    def _to_y(v: float) -> float:
        return height - (bottom + (v - merged_min) * plot_h / (merged_max - merged_min))

    pts_a = " ".join(f"{x:.2f},{_to_y(y):.2f}" for x, y in zip(xs, y_values_a))
    pts_b = " ".join(f"{x:.2f},{_to_y(y):.2f}" for x, y in zip(xs, y_values_b))
    pts_c = " ".join(f"{x:.2f},{_to_y(y):.2f}" for x, y in zip(xs, y_values_c))

    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>\n")
        f.write("<rect x='0' y='0' width='100%' height='100%' fill='white'/>\n")
        f.write(f"<text x='20' y='24' font-size='16' font-family='Arial'>{title}</text>\n")
        f.write(f"<text x='8' y='{top + 5}' font-size='11' font-family='Arial'>{merged_max:.1f}</text>\n")
        f.write(f"<text x='8' y='{height - bottom + 5}' font-size='11' font-family='Arial'>{merged_min:.1f}</text>\n")
        f.write(
            f"<line x1='{left}' y1='{height-bottom}' x2='{width-right}' y2='{height-bottom}' stroke='#666'/>\n"
        )
        f.write(f"<line x1='{left}' y1='{top}' x2='{left}' y2='{height-bottom}' stroke='#666'/>\n")
        f.write(f"<polyline fill='none' stroke='{color_a}' stroke-width='2' points='{pts_a}'/>\n")
        f.write(f"<polyline fill='none' stroke='{color_b}' stroke-width='2' points='{pts_b}'/>\n")
        f.write(f"<polyline fill='none' stroke='{color_c}' stroke-width='2' points='{pts_c}'/>\n")
        if x_labels:
            f.write(
                f"<text x='{left}' y='{height-8}' font-size='10' font-family='Arial'>{x_labels[0][:13]}</text>\n"
            )
            f.write(
                f"<text x='{width-right-120}' y='{height-8}' font-size='10' font-family='Arial'>{x_labels[-1][:13]}</text>\n"
            )
        f.write(f"<text x='{width-300}' y='26' font-size='11' font-family='Arial' fill='{color_a}'>{legend_a}</text>\n")
        f.write(f"<text x='{width-210}' y='26' font-size='11' font-family='Arial' fill='{color_b}'>{legend_b}</text>\n")
        f.write(f"<text x='{width-120}' y='26' font-size='11' font-family='Arial' fill='{color_c}'>{legend_c}</text>\n")
        f.write("</svg>\n")


def run_charts(config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    panel_path = Path(cfg["curated_output"]["panel_csv"])
    forecast_scenarios_path = Path(cfg["forecast_output"]["scenarios_csv"])
    queue_outlook_path = Path(cfg["curated_output"]["queue_outlook_csv"])
    out_dir = Path(cfg["reports"]["charts_dir"])
    log_path = cfg["reports"]["metadata_log"]
    region = cfg["region"]
    prefix = region.lower().replace("-", "").replace(".", "")

    timestamps: list[str] = []
    loads: list[float] = []
    prices: list[float] = []
    temps: list[float] = []

    with panel_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            timestamps.append(row["timestamp_utc"])
            loads.append(float(row["load_mw"]))
            prices.append(float(row["price_usd_mwh"]))
            temps.append(float(row["temperature_f"]))

    out_dir.mkdir(parents=True, exist_ok=True)
    load_svg = out_dir / f"{prefix}_load.svg"
    price_svg = out_dir / f"{prefix}_price.svg"
    temp_svg = out_dir / f"{prefix}_temperature.svg"

    _line_svg(f"{region} Hourly Load (MW)", timestamps, loads, load_svg, "#0D3D91")
    _line_svg(f"{region} Hub Price (USD/MWh)", timestamps, prices, price_svg, "#1E7A50")
    _line_svg(f"{region} Temperature (F)", timestamps, temps, temp_svg, "#B55000")

    annual: dict[str, tuple[float, float]] = {}
    with queue_outlook_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            year = row["year"]
            p50 = float(row["expected_online_mw_p50"])
            p90 = float(row["expected_online_mw_p90"])
            if year not in annual:
                annual[year] = (0.0, 0.0)
            cur50, cur90 = annual[year]
            annual[year] = (cur50 + p50, cur90 + p90)

    years = sorted(annual.keys())
    annual_p50 = [annual[y][0] for y in years]
    annual_p90 = [annual[y][1] for y in years]
    queue_svg = out_dir / f"{prefix}_queue_expected_online_mw.svg"
    _dual_line_svg(
        f"{region} Queue Expected Online MW by COD Year",
        years,
        annual_p50,
        annual_p90,
        queue_svg,
        "#5B4B8A",
        "#C75000",
        "P50",
        "P90",
    )

    by_scenario: dict[str, dict[str, float]] = {}
    with forecast_scenarios_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            sc = row["scenario"]
            yr = row["year"]
            by_scenario.setdefault(sc, {})
            by_scenario[sc][yr] = float(row["avg_load_mw"])

    years = sorted(set(by_scenario.get("base", {}).keys()) | set(by_scenario.get("low", {}).keys()) | set(by_scenario.get("high", {}).keys()))
    low = [by_scenario.get("low", {}).get(y, 0.0) for y in years]
    base = [by_scenario.get("base", {}).get(y, 0.0) for y in years]
    high = [by_scenario.get("high", {}).get(y, 0.0) for y in years]
    forecast_svg = out_dir / f"{prefix}_load_forecast_scenarios.svg"
    _triple_line_svg(
        f"{region} Load Forecast Scenarios (Avg MW)",
        years,
        low,
        base,
        high,
        forecast_svg,
        "#1E7A50",
        "#0D3D91",
        "#B55000",
        "Low",
        "Base",
        "High",
    )

    log_metadata(log_path, f"charts:created {load_svg} {price_svg} {temp_svg} {queue_svg} {forecast_svg}")


if __name__ == "__main__":
    run_charts()
