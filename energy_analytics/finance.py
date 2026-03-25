from __future__ import annotations

import csv
from pathlib import Path

from energy_analytics.config import load_config
from energy_analytics.metadata import log_metadata

SCENARIO_COLUMNS = [
    "scenario_id",
    "contract_type",
    "price_case",
    "capex_case",
    "price_multiplier",
    "capex_multiplier",
    "npv_musd",
    "after_tax_npv_musd",
    "irr",
    "min_dscr",
    "avg_dscr",
    "lcoe_usd_mwh",
    "year1_revenue_musd",
]


def _read_metric(metrics_path: Path, metric_name: str) -> float:
    with metrics_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row["metric"] == metric_name:
                return float(row["value"])
    raise ValueError(f"Metric not found: {metric_name}")


def _npv(rate: float, cashflows: list[float]) -> float:
    return sum(cf / ((1 + rate) ** t) for t, cf in enumerate(cashflows))


def _irr(cashflows: list[float]) -> float:
    lo = -0.90
    hi = 1.50
    npv_lo = _npv(lo, cashflows)
    npv_hi = _npv(hi, cashflows)
    if npv_lo == 0:
        return lo
    if npv_hi == 0:
        return hi
    if npv_lo * npv_hi > 0:
        return 0.0
    for _ in range(80):
        mid = (lo + hi) / 2
        npv_mid = _npv(mid, cashflows)
        if abs(npv_mid) < 1e-8:
            return mid
        if npv_lo * npv_mid < 0:
            hi = mid
            npv_hi = npv_mid
        else:
            lo = mid
            npv_lo = npv_mid
    return (lo + hi) / 2


def _annuity_payment(principal: float, rate: float, years: int) -> float:
    if years <= 0:
        return 0.0
    if rate == 0:
        return principal / years
    a = (rate * ((1 + rate) ** years)) / (((1 + rate) ** years) - 1)
    return principal * a


def _build_case(
    base_capture: float,
    assumptions: dict[str, float],
    price_multiplier: float,
    capex_multiplier: float,
    contract_type: str,
) -> dict[str, float]:
    life = int(assumptions["project_life_years"])
    debt_tenor = int(assumptions["debt_tenor_years"])
    capacity_mw = float(assumptions["capacity_mw"])
    cap_factor = float(assumptions["solar_capacity_factor"])
    degradation = float(assumptions["degradation_rate"])
    capex_kw = float(assumptions["capex_per_kw"]) * capex_multiplier
    opex_kw = float(assumptions["fixed_opex_per_kw_year"])
    debt_fraction = float(assumptions["debt_fraction"])
    debt_rate = float(assumptions["debt_rate"])
    discount = float(assumptions["equity_discount_rate"])
    tax_rate = float(assumptions.get("tax_rate", 0.25))
    merchant_discount = float(assumptions.get("merchant_basis_discount", 0.92))
    contracted_adder = float(assumptions.get("contracted_price_adder_usd_mwh", 2.0))

    # ITC (Investment Tax Credit): reduces net equity required at close via tax equity monetization.
    # Industry standard for utility-scale solar is 30% under the Inflation Reduction Act.
    # merchant_basis_discount reflects basis risk on uncontracted revenues (documented in config).
    itc_rate = float(assumptions.get("itc_rate", 0.0))

    capacity_kw = capacity_mw * 1000.0
    capex = capacity_kw * capex_kw
    debt = capex * debt_fraction
    equity = capex - debt
    # ITC reduces effective equity basis: tax equity investor contributes ITC value at COD
    net_equity = max(equity - itc_rate * capex, 0.0)
    annual_debt_service = _annuity_payment(debt, debt_rate, debt_tenor)

    annual_energy = capacity_mw * 8760.0 * cap_factor
    if contract_type == "contracted":
        strike_price = (base_capture * price_multiplier) + contracted_adder
    else:
        strike_price = (base_capture * price_multiplier) * merchant_discount

    cfads: list[float] = []
    equity_cfs = [-net_equity]
    equity_cfs_after_tax = [-net_equity]
    debt_dscr: list[float] = []
    discounted_cost = capex
    discounted_energy = 0.0

    for year in range(1, life + 1):
        energy = annual_energy * ((1 - degradation) ** (year - 1))
        revenue = energy * strike_price
        opex = capacity_kw * opex_kw
        cash = revenue - opex
        cfads.append(cash)

        debt_service = annual_debt_service if year <= debt_tenor else 0.0
        equity_cfs.append(cash - debt_service)
        equity_cfs_after_tax.append((cash - debt_service) * (1 - tax_rate))

        if debt_service > 0:
            debt_dscr.append(cash / debt_service)

        discounted_cost += opex / ((1 + discount) ** year)
        discounted_energy += energy / ((1 + discount) ** year)

    npv_equity = _npv(discount, equity_cfs)
    npv_equity_after_tax = _npv(discount, equity_cfs_after_tax)
    irr_equity = _irr(equity_cfs)
    min_dscr = min(debt_dscr) if debt_dscr else 0.0
    avg_dscr = sum(debt_dscr) / len(debt_dscr) if debt_dscr else 0.0
    lcoe = (discounted_cost / discounted_energy) if discounted_energy else 0.0

    return {
        "npv": npv_equity,
        "after_tax_npv": npv_equity_after_tax,
        "irr": irr_equity,
        "min_dscr": min_dscr,
        "avg_dscr": avg_dscr,
        "lcoe": lcoe,
        "year1_revenue": cfads[0] + (capacity_kw * opex_kw),
    }


def _write_sensitivity_chart(rows: list[dict[str, str]], out_path: Path) -> None:
    width, height = 900, 300
    left = 240
    top = 30
    bar_h = 30
    gap = 12

    values = [float(r["delta_npv_musd"]) for r in rows]
    v_abs = max(abs(v) for v in values) if values else 1.0
    scale = 260 / v_abs if v_abs else 1.0
    zero_x = left + 260

    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>\n")
        f.write("<rect x='0' y='0' width='100%' height='100%' fill='white'/>\n")
        f.write("<text x='20' y='20' font-size='16' font-family='Arial'>Finance Sensitivity (Delta NPV, MUSD)</text>\n")
        f.write(f"<line x1='{zero_x}' y1='{top-8}' x2='{zero_x}' y2='{height-20}' stroke='#333'/>\n")
        for i, row in enumerate(rows):
            y = top + i * (bar_h + gap)
            delta = float(row["delta_npv_musd"])
            w = abs(delta) * scale
            x = zero_x - w if delta < 0 else zero_x
            color = "#B55000" if delta < 0 else "#1E7A50"
            f.write(f"<text x='20' y='{y+20}' font-size='12' font-family='Arial'>{row['driver']}</text>\n")
            f.write(f"<rect x='{x:.2f}' y='{y}' width='{w:.2f}' height='{bar_h}' fill='{color}'/>\n")
            f.write(f"<text x='{x + w + 8:.2f}' y='{y+20}' font-size='11' font-family='Arial'>{delta:.2f}</text>\n")
        f.write("</svg>\n")


def run_finance(config_path: str = "config/data_sources.yml") -> None:
    cfg = load_config(config_path)
    metrics_path = Path(cfg["markets_output"]["metrics_csv"])
    scenarios_path = Path(cfg["finance_output"]["scenarios_csv"])
    summary_path = Path(cfg["finance_output"]["summary_csv"])
    sensitivity_path = Path(cfg["finance_output"]["sensitivity_csv"])
    sensitivity_chart_path = Path(cfg["finance_output"]["sensitivity_chart_svg"])
    log_path = cfg["reports"]["metadata_log"]

    assumptions = cfg["finance_assumptions"]
    base_capture = _read_metric(metrics_path, "solar_capture_price_usd_mwh")

    price_cases = [("low", 0.85), ("base", 1.00), ("high", 1.15)]
    capex_cases = [("low", 0.90), ("base", 1.00), ("high", 1.10)]
    contract_cases = ["merchant", "contracted"]

    scenario_rows: list[dict[str, str]] = []
    scenario_id = 1
    base_npv_musd = 0.0

    for contract_type in contract_cases:
        for price_name, price_mult in price_cases:
            for capex_name, capex_mult in capex_cases:
                r = _build_case(base_capture, assumptions, price_mult, capex_mult, contract_type=contract_type)
                npv_musd = r["npv"] / 1_000_000.0
                after_tax_npv_musd = r["after_tax_npv"] / 1_000_000.0
                if contract_type == "contracted" and price_name == "base" and capex_name == "base":
                    base_npv_musd = npv_musd
                scenario_rows.append(
                    {
                        "scenario_id": str(scenario_id),
                        "contract_type": contract_type,
                        "price_case": price_name,
                        "capex_case": capex_name,
                        "price_multiplier": f"{price_mult:.2f}",
                        "capex_multiplier": f"{capex_mult:.2f}",
                        "npv_musd": f"{npv_musd:.4f}",
                        "after_tax_npv_musd": f"{after_tax_npv_musd:.4f}",
                        "irr": f"{r['irr']:.4f}",
                        "min_dscr": f"{r['min_dscr']:.4f}",
                        "avg_dscr": f"{r['avg_dscr']:.4f}",
                        "lcoe_usd_mwh": f"{r['lcoe']:.4f}",
                        "year1_revenue_musd": f"{(r['year1_revenue'] / 1_000_000.0):.4f}",
                    }
                )
                scenario_id += 1

    scenarios_path.parent.mkdir(parents=True, exist_ok=True)
    with scenarios_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SCENARIO_COLUMNS)
        writer.writeheader()
        writer.writerows(scenario_rows)

    base_row = next(
        r
        for r in scenario_rows
        if r["contract_type"] == "contracted" and r["price_case"] == "base" and r["capex_case"] == "base"
    )
    with summary_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["metric", "value"])
        writer.writeheader()
        writer.writerow({"metric": "base_solar_capture_price_usd_mwh", "value": f"{base_capture:.4f}"})
        writer.writerow({"metric": "base_contract_type", "value": "contracted"})
        writer.writerow({"metric": "base_npv_musd", "value": base_row["npv_musd"]})
        writer.writerow({"metric": "base_after_tax_npv_musd", "value": base_row["after_tax_npv_musd"]})
        writer.writerow({"metric": "base_irr", "value": base_row["irr"]})
        writer.writerow({"metric": "base_min_dscr", "value": base_row["min_dscr"]})
        writer.writerow({"metric": "base_lcoe_usd_mwh", "value": base_row["lcoe_usd_mwh"]})

    sens_inputs = [
        ("Price -10%", 0.90, 1.00),
        ("Price +10%", 1.10, 1.00),
        ("Capex -10%", 1.00, 0.90),
        ("Capex +10%", 1.00, 1.10),
        ("Price -10% & Capex +10%", 0.90, 1.10),
    ]
    sensitivity_rows: list[dict[str, str]] = []
    for name, p_mult, c_mult in sens_inputs:
        r = _build_case(base_capture, assumptions, p_mult, c_mult, contract_type="contracted")
        npv_musd = r["npv"] / 1_000_000.0
        sensitivity_rows.append(
            {
                "driver": name,
                "npv_musd": f"{npv_musd:.4f}",
                "delta_npv_musd": f"{(npv_musd - base_npv_musd):.4f}",
            }
        )

    with sensitivity_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["driver", "npv_musd", "delta_npv_musd"])
        writer.writeheader()
        writer.writerows(sensitivity_rows)

    sensitivity_chart_path.parent.mkdir(parents=True, exist_ok=True)
    _write_sensitivity_chart(sensitivity_rows, sensitivity_chart_path)

    log_metadata(
        log_path,
        (
            "finance:"
            f"scenarios={len(scenario_rows)} "
            f"base_npv_musd={base_npv_musd:.3f} "
            f"base_after_tax_npv_musd={float(base_row['after_tax_npv_musd']):.3f} "
            f"base_irr={float(base_row['irr']):.3f}"
        ),
    )


if __name__ == "__main__":
    run_finance()
