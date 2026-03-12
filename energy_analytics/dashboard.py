from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from energy_analytics.config import load_config
from energy_analytics.metadata import log_metadata


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _metric_map(rows: list[dict[str, str]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        out[row["metric"]] = float(row["value"])
    return out


def _scenario_index(rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    idx: dict[str, dict[str, float]] = {}
    for row in rows:
        contract_type = row.get("contract_type", "contracted")
        key = f"{contract_type}|{row['price_case']}|{row['capex_case']}"
        idx[key] = {
            "npv_musd": float(row["npv_musd"]),
            "after_tax_npv_musd": float(row.get("after_tax_npv_musd", row["npv_musd"])),
            "irr": float(row["irr"]),
            "min_dscr": float(row["min_dscr"]),
            "avg_dscr": float(row["avg_dscr"]),
            "lcoe_usd_mwh": float(row["lcoe_usd_mwh"]),
            "year1_revenue_musd": float(row["year1_revenue_musd"]),
        }
    return idx


def _load_iso_data(cfg: dict[str, Any]) -> dict[str, Any]:
    """Load all computed mart outputs for one ISO and return as a data dict."""
    region = cfg["region"]
    hub = cfg["hub"]
    prefix = region.lower().replace("-", "").replace(".", "")

    panel_rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    queue_rows = _read_csv(Path(cfg["curated_output"]["queue_outlook_csv"]))
    market_metrics = _metric_map(_read_csv(Path(cfg["markets_output"]["metrics_csv"])))
    finance_rows = _read_csv(Path(cfg["finance_output"]["scenarios_csv"]))
    scenario_idx = _scenario_index(finance_rows)

    load_points = [float(r["load_mw"]) for r in panel_rows]
    price_points = [float(r["price_usd_mwh"]) for r in panel_rows]
    temp_points = [float(r["temperature_f"]) for r in panel_rows]

    queue_by_year: dict[str, dict[str, float]] = {}
    for r in queue_rows:
        y = r["year"]
        if y not in queue_by_year:
            queue_by_year[y] = {"p50": 0.0, "p90": 0.0}
        queue_by_year[y]["p50"] += float(r["expected_online_mw_p50"])
        queue_by_year[y]["p90"] += float(r["expected_online_mw_p90"])

    years = sorted(queue_by_year.keys())
    queue_p50 = [queue_by_year[y]["p50"] for y in years]
    queue_p90 = [queue_by_year[y]["p90"] for y in years]

    base_fin = scenario_idx.get("contracted|base|base", {})

    return {
        "region": region,
        "hub": hub,
        "prefix": prefix,
        "kpis": {
            "avg_price": market_metrics.get("avg_price_usd_mwh", 0.0),
            "solar_capture": market_metrics.get("solar_capture_price_usd_mwh", 0.0),
            "wind_capture": market_metrics.get("wind_capture_price_usd_mwh", 0.0),
            "congestion_mean": market_metrics.get("congestion_proxy_mean", 0.0),
            "negative_share": market_metrics.get("negative_price_share", 0.0),
            "base_npv": base_fin.get("npv_musd", 0.0),
            "base_irr": base_fin.get("irr", 0.0),
        },
        "series": {
            "load": load_points,
            "price": price_points,
            "temperature": temp_points,
            "years": years,
            "queue_p50": queue_p50,
            "queue_p90": queue_p90,
        },
        "finance_scenarios": scenario_idx,
        "finance_assumptions": cfg.get("finance_assumptions", {}),
    }


def _build_summary_report(
    all_iso_data: dict[str, dict[str, Any]],
) -> Path:
    report_path = Path("reports/dashboard/summary_report.html")
    report_path.parent.mkdir(parents=True, exist_ok=True)

    rows_html = ""
    for region, data in all_iso_data.items():
        k = data["kpis"]
        b = data["finance_scenarios"].get("contracted|base|base", {})
        rows_html += f"""
    <tr>
      <td><b>{region}</b><br><span style='color:#5b727c;font-size:12px'>{data['hub']}</span></td>
      <td>{k['avg_price']:.2f}</td>
      <td>{k['solar_capture']:.2f}</td>
      <td>{k['wind_capture']:.2f}</td>
      <td>{k['congestion_mean']:.2f}</td>
      <td>{k['negative_share']*100:.1f}%</td>
      <td>{b.get('npv_musd',0):.2f}</td>
      <td>{b.get('irr',0)*100:.2f}%</td>
      <td>{b.get('lcoe_usd_mwh',0):.2f}</td>
    </tr>"""

    html = f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <title>Energy Analytics Summary Report</title>
  <style>
    :root {{
      --ink:#0f1f24; --muted:#5b727c; --accent:#0b5f83;
      --bg:#eef4f7; --card:#ffffff; --line:#d9e2e7; --hero-bg:#0b3d52;
    }}
    * {{ box-sizing:border-box; margin:0; padding:0; }}
    body {{ font-family:'IBM Plex Sans','Segoe UI',sans-serif; background:var(--bg); color:var(--ink); line-height:1.6; }}
    .navbar {{ background:#fff; border-bottom:1px solid var(--line); position:sticky; top:0; z-index:100; }}
    .navbar-inner {{ max-width:1100px; margin:0 auto; padding:0 20px; display:flex; align-items:center; justify-content:space-between; height:52px; }}
    .navbar-brand {{ font-weight:700; font-size:15px; color:var(--hero-bg); text-decoration:none; }}
    .navbar-links {{ display:flex; gap:4px; align-items:center; }}
    .navbar-links a {{ font-size:13px; font-weight:500; color:var(--muted); text-decoration:none; padding:6px 12px; border-radius:6px; transition:background .15s,color .15s; }}
    .navbar-links a:hover {{ background:var(--bg); color:var(--ink); }}
    .navbar-links a.active {{ color:var(--accent); font-weight:600; background:#e8f2f8; }}
    .page-header {{ background:linear-gradient(135deg,#0b3d52 0%,#0b5f83 100%); color:#fff; padding:36px 20px 28px; }}
    .page-header-inner {{ max-width:1100px; margin:0 auto; }}
    .page-header h1 {{ font-size:clamp(20px,4vw,30px); font-weight:700; margin-bottom:6px; }}
    .page-header .meta {{ opacity:.75; font-size:13px; }}
    .content {{ max-width:1100px; margin:0 auto; padding:32px 20px 48px; }}
    h2 {{ font-size:18px; font-weight:700; margin:28px 0 12px; color:var(--ink); }}
    h2:first-child {{ margin-top:0; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; background:var(--card); border-radius:12px; overflow:hidden; border:1px solid var(--line); }}
    th,td {{ border-bottom:1px solid var(--line); padding:10px 14px; text-align:left; }}
    th {{ background:#eef3f5; font-weight:600; font-size:12px; white-space:nowrap; }}
    tbody tr:last-child td {{ border-bottom:none; }}
    tbody tr:hover {{ background:#f7fafc; }}
    ul {{ padding-left:20px; }}
    ul li {{ font-size:14px; color:var(--muted); margin-bottom:6px; }}
    footer {{ background:var(--hero-bg); color:rgba(255,255,255,.65); text-align:center; padding:20px; font-size:13px; }}
    footer a {{ color:rgba(255,255,255,.85); }}
  </style>
</head>
<body>
<nav class='navbar'>
  <div class='navbar-inner'>
    <a class='navbar-brand' href='../../index.html'>Energy Analytics</a>
    <div class='navbar-links'>
      <a href='../../index.html'>Home</a>
      <a href='../../index.html#dashboard'>Dashboard</a>
      <a href='#' class='active'>Summary Report</a>
      <a href='https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz' target='_blank' rel='noopener'>GitHub</a>
    </div>
  </div>
</nav>
<div class='page-header'>
  <div class='page-header-inner'>
    <h1>Energy Analytics Portfolio Summary</h1>
    <div class='meta'>All US ISOs/RTOs — Base contracted solar scenario — Generated from processed artifacts only.</div>
  </div>
</div>
<div class='content'>
  <h2>Market &amp; Finance Comparison — All ISOs</h2>
  <table>
    <thead>
      <tr>
        <th>ISO / RTO</th>
        <th>Avg Price ($/MWh)</th>
        <th>Solar Capture ($/MWh)</th>
        <th>Wind Capture ($/MWh)</th>
        <th>Congestion Mean ($/MWh)</th>
        <th>Neg Price %</th>
        <th>Base NPV (MUSD)</th>
        <th>Base IRR</th>
        <th>LCOE ($/MWh)</th>
      </tr>
    </thead>
    <tbody>{rows_html}
    </tbody>
  </table>

  <h2>Limitations</h2>
  <ul>
    <li>Uses sample data and stylized renewable generation profiles for all ISOs.</li>
    <li>Congestion is proxy-based, not a transmission power-flow model.</li>
    <li>Finance assumptions are centralized and scenario-based, not an investment recommendation.</li>
    <li>Queue completion probabilities are heuristic-based using status and technology mix.</li>
  </ul>
</div>
<footer>
  <p>Energy Analytics Portfolio · All US ISOs/RTOs · Data is stylized sample output for demonstration purposes.</p>
  <p style='margin-top:6px;'>
    <a href='../../index.html'>Home</a> ·
    <a href='../../index.html#dashboard'>Dashboard</a> ·
    <a href='https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz' target='_blank' rel='noopener'>GitHub</a>
  </p>
</footer>
</body>
</html>
"""
    report_path.write_text(html, encoding="utf-8")
    return report_path


def run_all_iso_dashboard(config_paths: list[str]) -> None:
    """Build the main index.html dashboard embedding data for all ISOs."""
    all_iso_data: dict[str, dict[str, Any]] = {}
    log_paths: list[str] = []

    for config_path in config_paths:
        try:
            cfg = load_config(config_path)
            region = cfg["region"]
            iso_data = _load_iso_data(cfg)
            all_iso_data[region] = iso_data
            log_paths.append(cfg["reports"]["metadata_log"])
        except Exception as exc:
            print(f"Warning: could not load data for {config_path}: {exc}")

    if not all_iso_data:
        raise SystemExit("No ISO data could be loaded — run the pipeline first")

    summary_report_path = _build_summary_report(all_iso_data)

    # Serialize the multi-ISO data object
    serializable: dict[str, Any] = {}
    for region, data in all_iso_data.items():
        d = dict(data)
        d["finance_assumptions"] = {
            k: float(v) if isinstance(v, (int, float)) else v
            for k, v in d["finance_assumptions"].items()
        }
        serializable[region] = d

    embedded_json = json.dumps(serializable)

    # Build the full index.html as a single-page app
    iso_names = list(all_iso_data.keys())
    first_iso = iso_names[0]
    first_data = all_iso_data[first_iso]

    iso_option_tags = "\n".join(
        f"        <option value='{r}'>{r}</option>" for r in iso_names
    )

    dashboard_path = Path("index.html")

    html = _build_index_html(
        iso_option_tags=iso_option_tags,
        first_iso=first_iso,
        first_data=first_data,
        embedded_json=embedded_json,
    )

    dashboard_path.write_text(html, encoding="utf-8")

    log_msg = f"dashboard:generated iso_count={len(all_iso_data)} isos={list(all_iso_data.keys())} summary={summary_report_path}"
    for lp in log_paths:
        try:
            log_metadata(lp, log_msg)
        except Exception:
            pass


def run_dashboard(config_path: str = "config/data_sources.yml") -> None:
    """Single-ISO dashboard — delegates to run_all_iso_dashboard with one config."""
    run_all_iso_dashboard([config_path])


def _build_index_html(
    iso_option_tags: str,
    first_iso: str,
    first_data: dict[str, Any],
    embedded_json: str,
) -> str:
    fa = first_data.get("finance_assumptions", {})
    capex_val = fa.get("capex_per_kw", 1150)
    opex_val = fa.get("fixed_opex_per_kw_year", 18)
    wacc_val = fa.get("equity_discount_rate", 0.10)
    debt_val = fa.get("debt_rate", 0.06)
    ppa_val = first_data["kpis"].get("solar_capture", 36.46)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Energy Analytics Portfolio</title>
  <style>
    :root {{
      --ink: #0f1f24; --muted: #5b727c; --accent: #0b5f83; --accent2: #b55000;
      --bg: #eef4f7; --card: #ffffff; --line: #d9e2e7; --hero-bg: #0b3d52;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'IBM Plex Sans','Segoe UI',system-ui,sans-serif; background: var(--bg); color: var(--ink); line-height: 1.6; }}

    /* ── Navbar ── */
    .navbar {{ background: #fff; border-bottom: 1px solid var(--line); position: sticky; top: 0; z-index: 100; }}
    .navbar-inner {{ max-width: 1100px; margin: 0 auto; padding: 0 20px; display: flex; align-items: center; justify-content: space-between; height: 52px; }}
    .navbar-brand {{ font-weight: 700; font-size: 15px; color: var(--hero-bg); text-decoration: none; letter-spacing: -.01em; cursor: pointer; }}
    .navbar-links {{ display: flex; gap: 4px; align-items: center; }}
    .navbar-links button {{ font-size: 13px; font-weight: 500; color: var(--muted); background: none; border: none; cursor: pointer; padding: 6px 12px; border-radius: 6px; font-family: inherit; transition: background .15s, color .15s; }}
    .navbar-links button:hover {{ background: var(--bg); color: var(--ink); }}
    .navbar-links button.active {{ color: var(--accent); font-weight: 600; background: #e8f2f8; }}
    .navbar-links a {{ font-size: 13px; font-weight: 500; color: var(--muted); text-decoration: none; padding: 6px 12px; border-radius: 6px; transition: background .15s, color .15s; }}
    .navbar-links a:hover {{ background: var(--bg); color: var(--ink); }}

    /* ── Views ── */
    .view {{ display: none; }}
    .view.active {{ display: block; }}

    /* ── Page-section jump strip ── */
    .page-nav {{ background: var(--card); border-bottom: 1px solid var(--line); display: flex; flex-wrap: wrap; overflow-x: auto; }}
    .page-nav a {{ font-size: 12px; font-weight: 600; color: var(--muted); text-decoration: none; padding: 10px 18px; white-space: nowrap; border-bottom: 2px solid transparent; transition: color .15s, border-color .15s; }}
    .page-nav a:hover {{ color: var(--accent); border-bottom-color: var(--accent); }}

    /* ── Hero ── */
    .hero {{ background: linear-gradient(135deg, #0b3d52 0%, #0b5f83 60%, #0f7ea8 100%); color: #fff; padding: 56px 24px 48px; text-align: center; }}
    .hero-tag {{ display: inline-block; background: rgba(255,255,255,.15); border: 1px solid rgba(255,255,255,.25); border-radius: 999px; font-size: 12px; letter-spacing: .06em; padding: 4px 14px; margin-bottom: 18px; text-transform: uppercase; }}
    .hero h1 {{ font-size: clamp(24px,5vw,42px); font-weight: 700; margin-bottom: 14px; }}
    .hero p {{ max-width: 640px; margin: 0 auto 28px; opacity: .85; font-size: 16px; }}
    .btn-row {{ display: flex; flex-wrap: wrap; gap: 12px; justify-content: center; }}
    .btn {{ display: inline-block; padding: 12px 26px; border-radius: 8px; font-size: 14px; font-weight: 600; text-decoration: none; border: none; cursor: pointer; font-family: inherit; transition: opacity .15s; }}
    .btn:hover {{ opacity: .85; }}
    .btn-primary {{ background: #fff; color: var(--hero-bg); }}
    .btn-outline {{ background: transparent; color: #fff; border: 2px solid rgba(255,255,255,.6); }}

    /* ── Layout ── */
    .container {{ max-width: 1100px; margin: 0 auto; padding: 0 20px; }}
    section {{ padding: 44px 0; }}
    section + section {{ border-top: 1px solid var(--line); }}
    h2 {{ font-size: 22px; font-weight: 700; margin-bottom: 6px; }}
    .section-sub {{ color: var(--muted); font-size: 14px; margin-bottom: 28px; }}

    /* ── KPI strip ── */
    .kpi-strip {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr)); gap: 14px; }}
    .kpi {{ background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 16px 18px; }}
    .kpi .label {{ font-size: 12px; color: var(--muted); margin-bottom: 6px; }}
    .kpi .value {{ font-size: 28px; font-weight: 700; color: var(--accent); }}
    .kpi .unit {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}

    /* ── Charts grid ── */
    .charts-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px,1fr)); gap: 16px; }}
    .chart-card {{ background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 14px; overflow: hidden; }}
    .chart-card h3 {{ font-size: 13px; color: var(--muted); margin-bottom: 10px; text-transform: uppercase; letter-spacing: .05em; }}
    .chart-card img {{ width: 100%; border-radius: 6px; display: block; }}

    /* ── Modules ── */
    .modules-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(230px,1fr)); gap: 14px; }}
    .module-card {{ background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 18px; }}
    .module-letter {{ width: 34px; height: 34px; border-radius: 8px; background: var(--accent); color: #fff; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 16px; margin-bottom: 10px; }}
    .module-card h3 {{ font-size: 14px; font-weight: 700; margin-bottom: 6px; }}
    .module-card p {{ font-size: 13px; color: var(--muted); }}

    /* ── Finance table ── */
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border: 1px solid var(--line); padding: 10px 14px; text-align: left; }}
    th {{ background: #eef3f5; font-weight: 600; }}
    tbody tr:hover {{ background: #f7fafc; }}

    /* ── Docs links ── */
    .docs-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px,1fr)); gap: 10px; }}
    .doc-link {{ display: flex; align-items: center; gap: 10px; background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; text-decoration: none; color: var(--ink); font-size: 13px; font-weight: 600; transition: border-color .15s, box-shadow .15s; }}
    .doc-link:hover {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(11,95,131,.08); }}
    .doc-link span.icon {{ font-size: 18px; }}

    /* ── Tech stack ── */
    .tech-tags {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }}
    .tag {{ background: #deeaf0; color: #0b3d52; border-radius: 6px; font-size: 12px; font-weight: 600; padding: 4px 10px; }}

    /* ── Pipeline ── */
    .pipeline {{ display: flex; flex-wrap: wrap; align-items: center; gap: 6px; font-size: 13px; margin-top: 14px; }}
    .pipe-step {{ background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: 8px 14px; font-weight: 600; }}
    .pipe-arrow {{ color: var(--muted); font-size: 18px; }}

    /* ── ISO selector banner ── */
    .iso-banner {{
      background: #fff; border-bottom: 1px solid var(--line);
      padding: 10px 20px; display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
    }}
    .iso-banner label {{ font-size: 13px; font-weight: 600; color: var(--ink); }}
    .iso-tabs {{ display: flex; flex-wrap: wrap; gap: 6px; }}
    .iso-tab {{
      border: 1px solid #c8d5dd; background: #fff; padding: 6px 14px;
      border-radius: 999px; font-size: 12px; font-weight: 600; cursor: pointer;
      font-family: inherit; transition: background .15s, color .15s, border-color .15s;
    }}
    .iso-tab.active {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
    .iso-tab:hover:not(.active) {{ background: var(--bg); }}
    .iso-badge {{
      font-size: 11px; background: #e8f2f8; color: var(--accent);
      border-radius: 4px; padding: 2px 6px; margin-left: 4px;
    }}

    /* ── Dashboard view ── */
    .dash-layout {{ display: grid; grid-template-columns: 280px 1fr; min-height: calc(100vh - 52px); }}
    .dash-side {{ border-right: 1px solid var(--line); padding: 14px; background: #fbfdfe; overflow-y: auto; }}
    .dash-main {{ padding: 18px; }}
    .dash-card {{ background: var(--card); border: 1px solid var(--line); border-radius: 12px; padding: 12px; margin-bottom: 12px; }}
    .dash-card label {{ display: block; font-size: 12px; color: var(--muted); margin: 8px 0 4px; }}
    .dash-card select, .dash-card input {{ width: 100%; padding: 8px; border: 1px solid #c9d6dd; border-radius: 8px; font-family: inherit; }}
    .tabs {{ display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }}
    .tab-btn {{ border: 1px solid #c8d5dd; background: #fff; padding: 7px 10px; border-radius: 999px; font-size: 12px; cursor: pointer; font-family: inherit; }}
    .tab-btn.active {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
    .tab {{ display: none; }}
    .tab.active {{ display: block; }}
    .grid3 {{ display: grid; grid-template-columns: repeat(3,minmax(0,1fr)); gap: 10px; }}
    .dash-kpi {{ background: #fff; border: 1px solid var(--line); border-radius: 12px; padding: 12px; }}
    .dash-kpi .v {{ font-size: 22px; font-weight: 700; margin: 6px 0 2px; }}
    .m {{ color: var(--muted); font-size: 12px; }}
    .chart {{ width: 100%; border: 1px solid var(--line); border-radius: 10px; background: #fff; padding: 6px; display: block; margin-bottom: 10px; }}
    .foot {{ color: var(--muted); font-size: 12px; margin-top: 12px; }}
    .dl a {{ display: block; margin: 4px 0; color: #0a4f6f; text-decoration: none; }}

    /* ── Summary view ── */
    .summary-header {{ background: linear-gradient(135deg, #0b3d52 0%, #0b5f83 100%); color: #fff; padding: 36px 20px 28px; }}
    .summary-header-inner {{ max-width: 1100px; margin: 0 auto; }}
    .summary-header h1 {{ font-size: clamp(20px,4vw,30px); font-weight: 700; margin-bottom: 6px; }}
    .summary-header .meta {{ opacity: .75; font-size: 13px; }}
    .summary-content {{ max-width: 1100px; margin: 0 auto; padding: 32px 20px 48px; }}
    .summary-content h2 {{ font-size: 18px; font-weight: 700; margin: 28px 0 12px; }}
    .summary-content h2:first-child {{ margin-top: 0; }}
    .summary-content ul {{ padding-left: 20px; }}
    .summary-content ul li {{ font-size: 14px; color: var(--muted); margin-bottom: 6px; }}
    .summary-table {{ width: 100%; border-collapse: collapse; font-size: 13px; background: var(--card); border-radius: 12px; overflow: hidden; border: 1px solid var(--line); }}
    .summary-table th, .summary-table td {{ border-bottom: 1px solid var(--line); padding: 10px 14px; text-align: left; }}
    .summary-table th {{ background: #eef3f5; font-weight: 600; font-size: 12px; white-space: nowrap; }}
    .summary-table tbody tr:last-child td {{ border-bottom: none; }}
    .summary-table tbody tr:hover {{ background: #f7fafc; }}

    /* ── Footer ── */
    footer {{ background: var(--hero-bg); color: rgba(255,255,255,.65); text-align: center; padding: 20px; font-size: 13px; }}
    footer a {{ color: rgba(255,255,255,.85); text-decoration: none; }}
    footer button {{ background: none; border: none; color: rgba(255,255,255,.85); cursor: pointer; font-family: inherit; font-size: 13px; }}

    @media (max-width: 900px) {{
      .dash-layout {{ grid-template-columns: 1fr; }}
      .dash-side {{ border-right: 0; border-bottom: 1px solid var(--line); }}
      .grid3 {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 600px) {{
      .hero {{ padding: 40px 16px 36px; }}
      section {{ padding: 30px 0; }}
      .navbar-links button, .navbar-links a {{ padding: 6px 8px; font-size: 12px; }}
    }}
  </style>
</head>
<body>

<!-- ── Navbar ── -->
<nav class="navbar">
  <div class="navbar-inner">
    <span class="navbar-brand" onclick="showView('home')">Energy Analytics</span>
    <div class="navbar-links">
      <button id="nav-home" class="active" onclick="showView('home')">Home</button>
      <button id="nav-dashboard" onclick="showView('dashboard')">Dashboard</button>
      <button id="nav-summary" onclick="showView('summary')">Summary Report</button>
      <a href="https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz" target="_blank" rel="noopener">GitHub</a>
    </div>
  </div>
</nav>

<!-- ════════════════════════════════
     VIEW: HOME
════════════════════════════════ -->
<div id="view-home" class="view active">
  <div class="page-nav">
    <a href="#metrics">Key Metrics</a>
    <a href="#charts">Charts</a>
    <a href="#modules">Modules</a>
    <a href="#finance">Finance</a>
    <a href="#pipeline">Pipeline</a>
    <a href="#docs">Documentation</a>
  </div>
  <div class="hero">
    <div class="hero-tag">Energy Analytics Portfolio</div>
    <h1>US ISO/RTO Analytics Platform</h1>
    <p>An end-to-end analytics pipeline covering all major US power markets — load forecasting,
       interconnection queue analytics, wholesale market metrics, and solar project finance.</p>
    <div class="btn-row">
      <button class="btn btn-primary" onclick="showView('dashboard')">View Interactive Dashboard</button>
      <button class="btn btn-outline" onclick="showView('summary')">Summary Report</button>
      <a class="btn btn-outline" href="https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz" target="_blank" rel="noopener">GitHub Repo</a>
    </div>
  </div>

  <div class="container">
    <section id="metrics">
      <h2>Key Metrics — All ISOs</h2>
      <p class="section-sub">Base scenario outputs across all 7 US ISOs/RTOs.</p>
      <div id="home-iso-tabs" class="iso-tabs" style="margin-bottom:20px;"></div>
      <div id="home-kpi-strip" class="kpi-strip"></div>
    </section>

    <section id="charts">
      <h2>Pipeline Outputs</h2>
      <p class="section-sub">Charts generated from the reproducible pipeline — open the dashboard for interactive controls.</p>
      <div class="charts-grid">
        <div class="chart-card"><h3>Hourly Load (MW)</h3><img id="home-chart-load" src="" alt="Hourly load"></div>
        <div class="chart-card"><h3>Hub Price (USD/MWh)</h3><img id="home-chart-price" src="" alt="Hub price"></div>
        <div class="chart-card"><h3>Temperature (°F)</h3><img id="home-chart-temp" src="" alt="Temperature"></div>
        <div class="chart-card"><h3>Load Forecast Scenarios</h3><img id="home-chart-forecast" src="" alt="Load forecast"></div>
        <div class="chart-card"><h3>Queue Expected Online MW</h3><img id="home-chart-queue" src="" alt="Queue outlook"></div>
        <div class="chart-card"><h3>Finance Sensitivity</h3><img id="home-chart-finance" src="" alt="Finance sensitivity"></div>
      </div>
    </section>

    <section id="modules">
      <h2>Analytics Modules</h2>
      <p class="section-sub">Four modules built in sequence, each producing validated analytical outputs.</p>
      <div class="modules-grid">
        <div class="module-card"><div class="module-letter">A</div><h3>Load &amp; Demand Forecasting</h3><p>Baseline regional load model with temperature sensitivity, day-of-week, and seasonality. Data center growth overlay with Low / Base / High scenarios to 2030.</p></div>
        <div class="module-card"><div class="module-letter">B</div><h3>Interconnection Queue</h3><p>Normalized ISO queue data with completion-probability modeling. Expected online MW by year per technology — P50 / P90 buildout trajectories for solar, wind, and storage.</p></div>
        <div class="module-card"><div class="module-letter">C</div><h3>Market Metrics</h3><p>Generation-weighted capture prices for solar and wind, congestion proxy (basis deviation), negative-price hours, and curtailment indicators.</p></div>
        <div class="module-card"><div class="module-letter">D</div><h3>Project Finance</h3><p>Transparent solar pro-forma with merchant and contracted structures. Outputs: LCOE, NPV, IRR, DSCR, and a full scenario matrix across load × supply × price assumptions.</p></div>
      </div>
    </section>

    <section id="finance">
      <h2>Base Solar Finance — Contracted, 100 MW (by ISO)</h2>
      <p class="section-sub">Stylized pro-forma under base price and base capex assumptions. Not an investment recommendation.</p>
      <div id="home-finance-table"></div>
    </section>

    <section id="pipeline">
      <h2>Pipeline Architecture</h2>
      <p class="section-sub">Layered, reproducible data flow — one command rebuilds all outputs.</p>
      <div class="pipeline">
        <div class="pipe-step">Samples / URLs</div><div class="pipe-arrow">→</div>
        <div class="pipe-step">data/raw</div><div class="pipe-arrow">→</div>
        <div class="pipe-step">data/staged</div><div class="pipe-arrow">→</div>
        <div class="pipe-step">data/curated</div><div class="pipe-arrow">→</div>
        <div class="pipe-step">data/marts</div><div class="pipe-arrow">→</div>
        <div class="pipe-step">Charts · Dashboard · Reports</div>
      </div>
      <p style="margin-top:16px;font-size:13px;color:var(--muted);">
        All transforms are reproducible via <code>make all-regions</code>. Schema contracts, QA checks, and ingestion
        manifests are enforced at each stage. Dependency versions are pinned in <code>requirements-lock.txt</code>.
      </p>
      <div class="tech-tags" style="margin-top:14px;">
        <span class="tag">Python 3.12</span><span class="tag">pandas</span><span class="tag">DuckDB</span>
        <span class="tag">pyarrow / Parquet</span><span class="tag">statsmodels</span><span class="tag">scikit-learn</span>
        <span class="tag">Makefile</span><span class="tag">GitHub Actions CI</span><span class="tag">ruff · black</span><span class="tag">pre-commit</span>
      </div>
    </section>

    <section id="docs">
      <h2>Documentation</h2>
      <p class="section-sub">Method notes, assumptions, and data dictionary for full transparency.</p>
      <div class="docs-grid">
        <a class="doc-link" href="docs/assumptions.md"><span class="icon">📋</span> Assumptions Table</a>
        <a class="doc-link" href="docs/data_dictionary.md"><span class="icon">📖</span> Data Dictionary</a>
        <a class="doc-link" href="docs/architecture.md"><span class="icon">🏗</span> Architecture</a>
        <a class="doc-link" href="docs/method_load.md"><span class="icon">📈</span> Load Model</a>
        <a class="doc-link" href="docs/method_queue.md"><span class="icon">🔌</span> Queue Model</a>
        <a class="doc-link" href="docs/method_markets.md"><span class="icon">💹</span> Markets Model</a>
        <a class="doc-link" href="docs/method_finance.md"><span class="icon">💰</span> Finance Model</a>
        <a class="doc-link" href="reports/qa_report.md"><span class="icon">✅</span> QA Report</a>
      </div>
    </section>
  </div>

  <footer>
    <p>Energy Analytics Portfolio · All US ISOs/RTOs · Data is stylized sample output for demonstration purposes.</p>
    <p style="margin-top:6px;">
      <a href="https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz" target="_blank" rel="noopener">GitHub</a> ·
      <button onclick="showView('dashboard')">Dashboard</button> ·
      <button onclick="showView('summary')">Summary Report</button>
    </p>
  </footer>
</div>

<!-- ════════════════════════════════
     VIEW: DASHBOARD
════════════════════════════════ -->
<div id="view-dashboard" class="view">
  <!-- ISO selector banner -->
  <div class="iso-banner">
    <label>ISO / RTO:</label>
    <div class="iso-tabs" id="dash-iso-tabs"></div>
  </div>
  <div class="dash-layout">
    <aside class="dash-side">
      <div class="dash-card">
        <b>Scenario Controls</b>
        <label>Region</label>
        <select id="region" disabled></select>
        <label title="Load scenario multiplier for KPI projections.">Load scenario</label>
        <select id="load_scn"><option value="0.95">Low</option><option value="1.0" selected>Base</option><option value="1.07">High</option></select>
        <label title="Queue completion assumption used in supply KPIs.">Queue completion</label>
        <select id="queue_scn"><option value="p50" selected>P50</option><option value="p90">P90</option></select>
        <label title="Market tightness modifies implied congestion assumptions.">Market tightness</label>
        <select id="tight_scn"><option value="0.85">Low congestion</option><option value="1.0" selected>Base</option><option value="1.25">High congestion</option></select>
      </div>
      <div class="dash-card">
        <b>Finance Knobs</b>
        <label title="Installed cost per kW.">Capex (USD/kW)</label>
        <input id="capex" type="number" value="{capex_val}" step="25">
        <label title="Fixed annual operating cost per kW.">Opex (USD/kW-yr)</label>
        <input id="opex" type="number" value="{opex_val}" step="1">
        <label title="Discount rate proxy for weighted average cost of capital.">WACC</label>
        <input id="wacc" type="number" value="{wacc_val}" step="0.005">
        <label title="Debt interest rate assumption.">Debt rate</label>
        <input id="debt" type="number" value="{debt_val}" step="0.005">
        <label title="Revenue structure for the project case.">Contract type</label>
        <select id="contract_type"><option value="contracted" selected>Contracted</option><option value="merchant">Merchant</option></select>
        <label title="Power purchase agreement proxy price.">PPA price (USD/MWh)</label>
        <input id="ppa" type="number" value="{ppa_val:.2f}" step="0.5">
        <label title="Annual energy degradation assumption.">Degradation</label>
        <input id="degrade" type="number" value="0.005" step="0.001">
      </div>
    </aside>
    <main class="dash-main">
      <div class="tabs">
        <button class="tab-btn active" data-tab="d-overview">Overview</button>
        <button class="tab-btn" data-tab="d-load">Load</button>
        <button class="tab-btn" data-tab="d-supply">Supply</button>
        <button class="tab-btn" data-tab="d-markets">Markets</button>
        <button class="tab-btn" data-tab="d-finance">Finance</button>
        <button class="tab-btn" data-tab="d-downloads">Downloads</button>
      </div>

      <section id="d-overview" class="tab active">
        <div class="grid3">
          <div class="dash-kpi"><div class="m">Average Price (USD/MWh)</div><div class="v" id="k_avg_price">-</div></div>
          <div class="dash-kpi"><div class="m">Solar Capture (USD/MWh)</div><div class="v" id="k_solar">-</div></div>
          <div class="dash-kpi"><div class="m">Base NPV (MUSD)</div><div class="v" id="k_npv">-</div></div>
        </div>
        <div class="foot">Definitions: capture price = profile-weighted average price; congestion proxy = absolute deviation from 24h moving-average price.</div>
      </section>

      <section id="d-load" class="tab">
        <h3>Load and Weather</h3>
        <img class="chart" id="dash-chart-load" src="" alt="Hourly load chart (MW)">
        <img class="chart" id="dash-chart-temp" src="" alt="Hourly temperature chart (F)">
        <img class="chart" id="dash-chart-forecast" src="" alt="Load forecast scenarios chart (avg MW)">
        <div class="foot">Units: MW for load, F for temperature.</div>
      </section>

      <section id="d-supply" class="tab">
        <h3>Supply Queue Outlook</h3>
        <img class="chart" id="dash-chart-queue" src="" alt="Queue expected online MW by year">
        <div class="dash-kpi"><div class="m">Selected Queue Scenario Total (MW)</div><div class="v" id="k_queue_total">-</div></div>
        <div class="foot">P50/P90 represent expected online capacity under different completion assumptions.</div>
      </section>

      <section id="d-markets" class="tab">
        <h3>Market Metrics</h3>
        <img class="chart" id="dash-chart-price" src="" alt="Hub price chart (USD/MWh)">
        <div class="grid3">
          <div class="dash-kpi"><div class="m">Wind Capture (USD/MWh)</div><div class="v" id="k_wind">-</div></div>
          <div class="dash-kpi"><div class="m">Congestion Mean (USD/MWh)</div><div class="v" id="k_cong">-</div></div>
          <div class="dash-kpi"><div class="m">Negative Price Share (%)</div><div class="v" id="k_neg">-</div></div>
        </div>
        <details><summary>Metric Notes</summary><div class="foot">Negative price share = hours with price &lt; 0 divided by total hours in modeled period.</div></details>
      </section>

      <section id="d-finance" class="tab">
        <h3>Project Finance</h3>
        <div class="grid3">
          <div class="dash-kpi"><div class="m">Scenario IRR</div><div class="v" id="k_irr">-</div></div>
          <div class="dash-kpi"><div class="m">Min DSCR</div><div class="v" id="k_dscr">-</div></div>
          <div class="dash-kpi"><div class="m">LCOE (USD/MWh)</div><div class="v" id="k_lcoe">-</div></div>
        </div>
        <img class="chart" id="dash-chart-finsen" src="" alt="Finance sensitivity chart">
        <div class="foot">Finance KPIs are scenario-linked to price/capex controls and provide directional screening outputs.</div>
      </section>

      <section id="d-downloads" class="tab">
        <h3>Downloads and Report</h3>
        <div id="downloads-list" class="dl"></div>
        <div class="foot">Dashboard runtime uses processed tables only and does not fetch external data.</div>
      </section>
    </main>
  </div>
</div>

<!-- ════════════════════════════════
     VIEW: SUMMARY REPORT
════════════════════════════════ -->
<div id="view-summary" class="view">
  <div class="summary-header">
    <div class="summary-header-inner">
      <h1>Energy Analytics Portfolio Summary</h1>
      <div class="meta">All US ISOs/RTOs — Base contracted solar scenario — Generated from processed artifacts only.</div>
    </div>
  </div>
  <div class="summary-content">
    <h2>Market &amp; Finance Comparison — All ISOs</h2>
    <div id="summary-table-container"></div>
    <h2>Limitations</h2>
    <ul>
      <li>Uses sample data and stylized renewable generation profiles for all ISOs.</li>
      <li>Congestion is proxy-based, not a transmission power-flow model.</li>
      <li>Finance assumptions are centralized and scenario-based, not an investment recommendation.</li>
      <li>Queue completion probabilities are heuristic-based using status and technology mix.</li>
    </ul>
  </div>
  <footer>
    <p>Energy Analytics Portfolio · All US ISOs/RTOs · Data is stylized sample output for demonstration purposes.</p>
    <p style="margin-top:6px;">
      <a href="https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz" target="_blank" rel="noopener">GitHub</a>
    </p>
  </footer>
</div>

<script>
// ── Multi-ISO data ──
const ALL_DATA = {embedded_json};
const ISO_NAMES = Object.keys(ALL_DATA);
let currentISO = ISO_NAMES[0];

// ── View switcher ──
function showView(name) {{
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.navbar-links button').forEach(b => b.classList.remove('active'));
  document.getElementById('view-' + name).classList.add('active');
  document.getElementById('nav-' + name).classList.add('active');
  window.scrollTo(0, 0);
  if (name === 'dashboard') refreshDashboard();
  if (name === 'summary') renderSummaryTable();
  if (name === 'home') renderHomeKPIs();
}}

// ── ISO switcher ──
function switchISO(region) {{
  currentISO = region;
  // Update all ISO tab buttons
  document.querySelectorAll('.iso-tab').forEach(btn => {{
    btn.classList.toggle('active', btn.dataset.iso === region);
  }});
  document.getElementById('region').value = region;
  updateCharts();
  refreshDashboard();
  updateDownloads();
}}

function buildISOTabs(containerId) {{
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = '';
  for (const iso of ISO_NAMES) {{
    const btn = document.createElement('button');
    btn.className = 'iso-tab' + (iso === currentISO ? ' active' : '');
    btn.dataset.iso = iso;
    btn.textContent = iso;
    btn.onclick = () => switchISO(iso);
    container.appendChild(btn);
  }}
}}

// ── Chart path helper ──
function chartPrefix(region) {{
  return 'reports/charts/' + region.toLowerCase().replace(/-/g,'').replace(/\\./g,'');
}}

function updateCharts() {{
  const p = chartPrefix(currentISO);
  const setImg = (id, suffix) => {{
    const el = document.getElementById(id);
    if (el) el.src = p + suffix + '.svg';
  }};
  // Dashboard charts
  setImg('dash-chart-load', '_load');
  setImg('dash-chart-temp', '_temperature');
  setImg('dash-chart-forecast', '_load_forecast_scenarios');
  setImg('dash-chart-queue', '_queue_expected_online_mw');
  setImg('dash-chart-price', '_price');
  setImg('dash-chart-finsen', '_finance_sensitivity');
  // Home charts
  setImg('home-chart-load', '_load');
  setImg('home-chart-price', '_price');
  setImg('home-chart-temp', '_temperature');
  setImg('home-chart-forecast', '_load_forecast_scenarios');
  setImg('home-chart-queue', '_queue_expected_online_mw');
  setImg('home-chart-finance', '_finance_sensitivity');
}}

// ── Downloads ──
function updateDownloads() {{
  const iso = currentISO.toLowerCase().replace(/-/g,'').replace(/\\./g,'');
  const el = document.getElementById('downloads-list');
  if (!el) return;
  const files = [
    ['market metrics CSV', `data/marts/${{iso}}_market_metrics.csv`],
    ['load backtest CSV', `data/marts/${{iso}}_load_backtest.csv`],
    ['load scenarios CSV', `data/marts/${{iso}}_load_forecast_scenarios.csv`],
    ['finance scenarios CSV', `data/marts/${{iso}}_finance_scenarios.csv`],
    ['finance sensitivity CSV', `data/marts/${{iso}}_finance_sensitivity.csv`],
    ['queue calibration CSV', `data/marts/${{iso}}_queue_calibration.csv`],
    ['queue outlook CSV', `data/curated/${{iso}}_queue_expected_online_mw.csv`],
  ];
  el.innerHTML = files.map(([label, path]) =>
    `<a href="${{path}}" download>Download ${{label}}</a>`
  ).join('') + `<a href="#" onclick="showView('summary');return false;">Open summary report</a>`;
}}

// ── Dashboard KPI refresh ──
function fmt(n, d=2) {{ return Number(n).toFixed(d); }}

function scenarioKeyFromControls() {{
  const price = document.getElementById('tight_scn').value;
  const contractType = document.getElementById('contract_type').value;
  const capex = Number(document.getElementById('capex').value);
  const data = ALL_DATA[currentISO];
  const baseCapex = data.finance_assumptions.capex_per_kw || 1150;
  let priceCase = 'base';
  if (Number(price) < 1) priceCase = 'low';
  if (Number(price) > 1) priceCase = 'high';
  let capexCase = 'base';
  if (capex < baseCapex) capexCase = 'low';
  if (capex > baseCapex) capexCase = 'high';
  return contractType + '|' + priceCase + '|' + capexCase;
}}

function refreshDashboard() {{
  const data = ALL_DATA[currentISO];
  if (!data) return;
  const loadMult = Number(document.getElementById('load_scn').value);
  const tightMult = Number(document.getElementById('tight_scn').value);
  const queueMode = document.getElementById('queue_scn').value;

  const avgPrice = data.kpis.avg_price * tightMult;
  const solar = data.kpis.solar_capture * tightMult;
  const wind = data.kpis.wind_capture * tightMult;
  const cong = data.kpis.congestion_mean * tightMult;
  const neg = Math.min(data.kpis.negative_share * tightMult, 1);

  document.getElementById('k_avg_price').textContent = fmt(avgPrice);
  document.getElementById('k_solar').textContent = fmt(solar);
  document.getElementById('k_wind').textContent = fmt(wind);
  document.getElementById('k_cong').textContent = fmt(cong);
  document.getElementById('k_neg').textContent = fmt(neg * 100, 1);

  const qArr = queueMode === 'p50' ? data.series.queue_p50 : data.series.queue_p90;
  const qTotal = qArr.reduce((a,b)=>a+b,0) * loadMult;
  document.getElementById('k_queue_total').textContent = fmt(qTotal, 1);

  const key = scenarioKeyFromControls();
  const s = data.finance_scenarios[key] || data.finance_scenarios['contracted|base|base'];
  if (s) {{
    document.getElementById('k_npv').textContent = fmt(s.npv_musd, 2);
    document.getElementById('k_irr').textContent = fmt(s.irr, 3);
    document.getElementById('k_dscr').textContent = fmt(s.min_dscr, 2);
    document.getElementById('k_lcoe').textContent = fmt(s.lcoe_usd_mwh, 2);
  }}
}}

// ── Home KPIs ──
function renderHomeKPIs() {{
  const strip = document.getElementById('home-kpi-strip');
  if (!strip) return;
  const data = ALL_DATA[currentISO];
  if (!data) return;
  const k = data.kpis;
  const b = data.finance_scenarios['contracted|base|base'] || {{}};
  strip.innerHTML = `
    <div class="kpi"><div class="label">Avg Hub Price</div><div class="value">${{k.avg_price.toFixed(2)}}</div><div class="unit">USD / MWh</div></div>
    <div class="kpi"><div class="label">Solar Capture</div><div class="value">${{k.solar_capture.toFixed(2)}}</div><div class="unit">USD / MWh</div></div>
    <div class="kpi"><div class="label">Wind Capture</div><div class="value">${{k.wind_capture.toFixed(2)}}</div><div class="unit">USD / MWh</div></div>
    <div class="kpi"><div class="label">Congestion (mean)</div><div class="value">${{k.congestion_mean.toFixed(2)}}</div><div class="unit">USD / MWh</div></div>
    <div class="kpi"><div class="label">Base IRR</div><div class="value">${{(k.base_irr*100).toFixed(1)}}%</div><div class="unit">Contracted · 100 MW solar</div></div>
    <div class="kpi"><div class="label">Base LCOE</div><div class="value">${{(b.lcoe_usd_mwh||0).toFixed(2)}}</div><div class="unit">USD / MWh</div></div>
  `;
  // Home finance table
  const tbl = document.getElementById('home-finance-table');
  if (tbl) {{
    let rows = '';
    for (const [iso, d] of Object.entries(ALL_DATA)) {{
      const bf = d.finance_scenarios['contracted|base|base'] || {{}};
      rows += `<tr>
        <td><b>${{iso}}</b> <span style="color:#5b727c;font-size:12px">${{d.hub}}</span></td>
        <td>${{d.kpis.avg_price.toFixed(2)}}</td>
        <td>${{(bf.npv_musd||0).toFixed(2)}}</td>
        <td>${{((bf.irr||0)*100).toFixed(2)}}%</td>
        <td>${{(bf.min_dscr||0).toFixed(2)}}×</td>
        <td>${{(bf.lcoe_usd_mwh||0).toFixed(2)}}</td>
        <td>${{(bf.year1_revenue_musd||0).toFixed(2)}}</td>
      </tr>`;
    }}
    tbl.innerHTML = `<table>
      <thead><tr><th>ISO/RTO</th><th>Avg Price ($/MWh)</th><th>NPV (MUSD)</th><th>IRR</th><th>Min DSCR</th><th>LCOE ($/MWh)</th><th>Yr1 Revenue (MUSD)</th></tr></thead>
      <tbody>${{rows}}</tbody>
    </table>`;
  }}
}}

// ── Summary table ──
function renderSummaryTable() {{
  const container = document.getElementById('summary-table-container');
  if (!container) return;
  let rows = '';
  for (const [iso, data] of Object.entries(ALL_DATA)) {{
    const k = data.kpis;
    const b = data.finance_scenarios['contracted|base|base'] || {{}};
    rows += `<tr>
      <td><b>${{iso}}</b><br><span style="color:#5b727c;font-size:12px">${{data.hub}}</span></td>
      <td>${{k.avg_price.toFixed(2)}}</td>
      <td>${{k.solar_capture.toFixed(2)}}</td>
      <td>${{k.wind_capture.toFixed(2)}}</td>
      <td>${{k.congestion_mean.toFixed(2)}}</td>
      <td>${{(k.negative_share*100).toFixed(1)}}%</td>
      <td>${{(b.npv_musd||0).toFixed(2)}}</td>
      <td>${{((b.irr||0)*100).toFixed(2)}}%</td>
      <td>${{(b.lcoe_usd_mwh||0).toFixed(2)}}</td>
    </tr>`;
  }}
  container.innerHTML = `<table class="summary-table">
    <thead><tr>
      <th>ISO / RTO</th><th>Avg Price ($/MWh)</th><th>Solar Capture</th><th>Wind Capture</th>
      <th>Congestion Mean</th><th>Neg Price %</th><th>Base NPV (MUSD)</th><th>Base IRR</th><th>LCOE ($/MWh)</th>
    </tr></thead>
    <tbody>${{rows}}</tbody>
  </table>`;
}}

// ── Dashboard tab switching ──
for (const btn of document.querySelectorAll('.tab-btn')) {{
  btn.addEventListener('click', () => {{
    btn.closest('.tabs').querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(s => s.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(btn.dataset.tab).classList.add('active');
  }});
}}

// ── Controls ──
for (const id of ['load_scn','queue_scn','tight_scn','contract_type','capex','opex','wacc','debt','ppa','degrade']) {{
  const el = document.getElementById(id);
  if (el) el.addEventListener('input', refreshDashboard);
}}

// ── Init ──
buildISOTabs('dash-iso-tabs');
buildISOTabs('home-iso-tabs');
updateCharts();
updateDownloads();
renderHomeKPIs();
refreshDashboard();

// populate readonly region selector
const regionSel = document.getElementById('region');
if (regionSel) {{
  regionSel.innerHTML = ISO_NAMES.map(r => `<option value="${{r}}">${{r}}</option>`).join('');
  regionSel.value = currentISO;
}}

// Hash-based routing
const hashMap = {{ '#dashboard': 'dashboard', '#summary': 'summary', '#home': 'home' }};
if (hashMap[location.hash]) showView(hashMap[location.hash]);
</script>
</body>
</html>
"""
