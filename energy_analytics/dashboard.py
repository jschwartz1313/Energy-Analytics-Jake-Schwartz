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


def _build_summary_report(cfg: dict[str, Any], metrics: dict[str, float], base_fin: dict[str, float]) -> Path:
    report_path = Path("reports/dashboard/summary_report.html")
    report_path.parent.mkdir(parents=True, exist_ok=True)

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
    .navbar-inner {{ max-width:900px; margin:0 auto; padding:0 20px; display:flex; align-items:center; justify-content:space-between; height:52px; }}
    .navbar-brand {{ font-weight:700; font-size:15px; color:var(--hero-bg); text-decoration:none; }}
    .navbar-links {{ display:flex; gap:4px; align-items:center; }}
    .navbar-links a {{ font-size:13px; font-weight:500; color:var(--muted); text-decoration:none; padding:6px 12px; border-radius:6px; transition:background .15s,color .15s; }}
    .navbar-links a:hover {{ background:var(--bg); color:var(--ink); }}
    .navbar-links a.active {{ color:var(--accent); font-weight:600; background:#e8f2f8; }}
    .page-header {{ background:linear-gradient(135deg,#0b3d52 0%,#0b5f83 100%); color:#fff; padding:36px 20px 28px; }}
    .page-header-inner {{ max-width:900px; margin:0 auto; }}
    .page-header h1 {{ font-size:clamp(20px,4vw,30px); font-weight:700; margin-bottom:6px; }}
    .page-header .meta {{ opacity:.75; font-size:13px; }}
    .content {{ max-width:900px; margin:0 auto; padding:32px 20px 48px; }}
    h2 {{ font-size:18px; font-weight:700; margin:28px 0 12px; color:var(--ink); }}
    h2:first-child {{ margin-top:0; }}
    .card-row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:8px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px 18px; }}
    .card .label {{ font-size:12px; color:var(--muted); margin-bottom:4px; }}
    .card .value {{ font-size:22px; font-weight:700; color:var(--accent); }}
    .card .unit {{ font-size:12px; color:var(--muted); margin-top:2px; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; background:var(--card); border-radius:12px; overflow:hidden; border:1px solid var(--line); }}
    th,td {{ border-bottom:1px solid var(--line); padding:10px 14px; text-align:left; }}
    th {{ background:#eef3f5; font-weight:600; font-size:13px; }}
    tbody tr:last-child td {{ border-bottom:none; }}
    tbody tr:hover {{ background:#f7fafc; }}
    ul {{ padding-left:20px; }}
    ul li {{ font-size:14px; color:var(--muted); margin-bottom:6px; }}
    footer {{ background:var(--hero-bg); color:rgba(255,255,255,.65); text-align:center; padding:20px; font-size:13px; }}
    footer a {{ color:rgba(255,255,255,.85); }}
    @media (max-width:600px) {{ .navbar-links a {{ padding:6px 8px; font-size:12px; }} .content {{ padding:24px 16px 40px; }} }}
  </style>
</head>
<body>
<nav class='navbar'>
  <div class='navbar-inner'>
    <a class='navbar-brand' href='../../index.html'>Energy Analytics</a>
    <div class='navbar-links'>
      <a href='../../index.html'>Home</a>
      <a href='index.html'>Dashboard</a>
      <a href='#' class='active'>Summary Report</a>
      <a href='https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz' target='_blank' rel='noopener'>GitHub</a>
    </div>
  </div>
</nav>
<div class='page-header'>
  <div class='page-header-inner'>
    <h1>Energy Analytics Portfolio Summary</h1>
    <div class='meta'>Region: {cfg['region']} &nbsp;|&nbsp; Hub: {cfg['hub']} &nbsp;|&nbsp; Generated from processed artifacts only.</div>
  </div>
</div>
<div class='content'>
  <h2>Market Highlights</h2>
  <div class='card-row'>
    <div class='card'><div class='label'>Average Hub Price</div><div class='value'>{metrics.get('avg_price_usd_mwh', 0.0):.2f}</div><div class='unit'>USD / MWh</div></div>
    <div class='card'><div class='label'>Solar Capture Price</div><div class='value'>{metrics.get('solar_capture_price_usd_mwh', 0.0):.2f}</div><div class='unit'>USD / MWh</div></div>
    <div class='card'><div class='label'>Wind Capture Price</div><div class='value'>{metrics.get('wind_capture_price_usd_mwh', 0.0):.2f}</div><div class='unit'>USD / MWh</div></div>
    <div class='card'><div class='label'>Congestion Proxy Mean</div><div class='value'>{metrics.get('congestion_proxy_mean', 0.0):.2f}</div><div class='unit'>USD / MWh</div></div>
    <div class='card'><div class='label'>Negative Price Share</div><div class='value'>{100*metrics.get('negative_price_share', 0.0):.1f}%</div><div class='unit'>of modeled hours</div></div>
  </div>

  <h2>Base Solar Finance Case</h2>
  <table>
    <thead><tr><th>Metric</th><th>Value</th><th>Notes</th></tr></thead>
    <tbody>
      <tr><td>NPV</td><td>{base_fin['npv_musd']:.2f} MUSD</td><td>10% WACC, 20-yr project life</td></tr>
      <tr><td>After-tax NPV</td><td>{base_fin.get('after_tax_npv_musd', base_fin['npv_musd']):.2f} MUSD</td><td>25% corporate tax rate proxy</td></tr>
      <tr><td>IRR</td><td>{base_fin['irr']:.3f}</td><td>Contracted base scenario</td></tr>
      <tr><td>Min DSCR</td><td>{base_fin['min_dscr']:.3f}×</td><td>60% debt, 6% rate, 15-yr tenor</td></tr>
      <tr><td>LCOE</td><td>{base_fin['lcoe_usd_mwh']:.2f} USD/MWh</td><td>1,150 USD/kW capex, 18 USD/kW-yr opex</td></tr>
      <tr><td>Year 1 Revenue</td><td>{base_fin['year1_revenue_musd']:.2f} MUSD</td><td>Capture price × generation × PPA premium</td></tr>
    </tbody>
  </table>

  <h2>Limitations</h2>
  <ul>
    <li>Uses sample data and stylized renewable generation profiles.</li>
    <li>Congestion is proxy-based, not a transmission power-flow model.</li>
    <li>Finance assumptions are centralized and scenario-based, not an investment recommendation.</li>
  </ul>
</div>
<footer>
  <p>Energy Analytics Portfolio · {cfg['region']} region · Data is stylized sample output for demonstration purposes.</p>
  <p style='margin-top:6px;'>
    <a href='../../index.html'>Home</a> ·
    <a href='index.html'>Dashboard</a> ·
    <a href='https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz' target='_blank' rel='noopener'>GitHub</a>
  </p>
</footer>
</body>
</html>
"""
    report_path.write_text(html, encoding="utf-8")
    return report_path


def run_dashboard() -> None:
    cfg = load_config()
    panel_rows = _read_csv(Path(cfg["curated_output"]["panel_csv"]))
    queue_rows = _read_csv(Path(cfg["curated_output"]["queue_outlook_csv"]))
    market_metrics = _metric_map(_read_csv(Path(cfg["markets_output"]["metrics_csv"])))
    finance_rows = _read_csv(Path(cfg["finance_output"]["scenarios_csv"]))
    scenario_idx = _scenario_index(finance_rows)

    out_dir = Path("reports/dashboard")
    out_dir.mkdir(parents=True, exist_ok=True)
    dashboard_path = out_dir / "index.html"

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

    base_fin = scenario_idx["contracted|base|base"]
    summary_report_path = _build_summary_report(cfg, market_metrics, base_fin)

    embedded = {
        "region": cfg["region"],
        "hub": cfg["hub"],
        "kpis": {
            "avg_price": market_metrics.get("avg_price_usd_mwh", 0.0),
            "solar_capture": market_metrics.get("solar_capture_price_usd_mwh", 0.0),
            "wind_capture": market_metrics.get("wind_capture_price_usd_mwh", 0.0),
            "congestion_mean": market_metrics.get("congestion_proxy_mean", 0.0),
            "negative_share": market_metrics.get("negative_price_share", 0.0),
            "base_npv": base_fin["npv_musd"],
            "base_irr": base_fin["irr"],
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
    }

    html = f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width,initial-scale=1'>
  <title>Energy Analytics Dashboard</title>
  <style>
    :root {{
      --ink:#0f1f24;
      --muted:#5b727c;
      --accent:#0b5f83;
      --accent2:#b55000;
      --bg:#f5f8f9;
      --card:#ffffff;
      --line:#d9e2e7;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:linear-gradient(130deg,#eef4f7,#f8fbfc); color:var(--ink); font-family:'IBM Plex Sans','Segoe UI',sans-serif; }}
    header {{ padding:12px 20px; border-bottom:1px solid var(--line); background:rgba(255,255,255,.9); position:sticky; top:0; z-index:10; backdrop-filter:blur(4px); display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px; }}
    .header-left h1 {{ margin:0; font-size:20px; }}
    .sub {{ color:var(--muted); font-size:13px; margin-top:2px; }}
    .header-nav {{ display:flex; gap:4px; align-items:center; }}
    .header-nav a {{ font-size:12px; font-weight:500; color:var(--muted); text-decoration:none; padding:6px 10px; border-radius:6px; transition:background .15s,color .15s; white-space:nowrap; }}
    .header-nav a:hover {{ background:var(--bg); color:var(--ink); }}
    .header-nav a.active {{ color:var(--accent); font-weight:600; background:#e8f2f8; }}
    @media (max-width:600px) {{ .header-nav {{ display:none; }} }}
    .layout {{ display:grid; grid-template-columns:280px 1fr; min-height:calc(100vh - 74px); }}
    .side {{ border-right:1px solid var(--line); padding:14px; background:#fbfdfe; }}
    .main {{ padding:18px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:12px; }}
    label {{ display:block; font-size:12px; color:var(--muted); margin:8px 0 4px; }}
    select,input {{ width:100%; padding:8px; border:1px solid #c9d6dd; border-radius:8px; }}
    .tabs {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:10px; }}
    .tab-btn {{ border:1px solid #c8d5dd; background:#fff; padding:7px 10px; border-radius:999px; font-size:12px; cursor:pointer; }}
    .tab-btn.active {{ background:var(--accent); color:#fff; border-color:var(--accent); }}
    .tab {{ display:none; }}
    .tab.active {{ display:block; }}
    .grid3 {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; }}
    .kpi {{ background:#fff; border:1px solid var(--line); border-radius:12px; padding:12px; }}
    .kpi .v {{ font-size:22px; font-weight:700; margin:6px 0 2px; }}
    .m {{ color:var(--muted); font-size:12px; }}
    .chart {{ width:100%; border:1px solid var(--line); border-radius:10px; background:#fff; padding:6px; }}
    details {{ margin-top:6px; }}
    .foot {{ color:var(--muted); font-size:12px; margin-top:12px; }}
    .dl a {{ display:block; margin:4px 0; color:#0a4f6f; text-decoration:none; }}
    @media (max-width:900px) {{ .layout{{grid-template-columns:1fr;}} .side{{border-right:0;border-bottom:1px solid var(--line);}} .grid3{{grid-template-columns:1fr;}} }}
  </style>
</head>
<body>
<header>
  <div class='header-left'>
    <h1>Energy Analytics Dashboard</h1>
    <div class='sub'>Processed-data dashboard for {cfg['region']} ({cfg['hub']}) covering milestones 1-5.</div>
  </div>
  <nav class='header-nav'>
    <a href='../../index.html'>Home</a>
    <a href='#' class='active'>Dashboard</a>
    <a href='summary_report.html'>Summary Report</a>
    <a href='https://github.com/jschwartz1313/Energy-Analytics-Jake-Schwartz' target='_blank' rel='noopener'>GitHub</a>
  </nav>
</header>
<div class='layout'>
  <aside class='side'>
    <div class='card'>
      <b>Scenario Controls</b>
      <label title='Select market region/zone for all pages.'>Region selector</label>
      <select id='region'><option>{cfg['region']}</option></select>
      <label title='Load scenario multiplier for KPI projections.'>Load scenario</label>
      <select id='load_scn'><option value='0.95'>Low</option><option value='1.0' selected>Base</option><option value='1.07'>High</option></select>
      <label title='Queue completion assumption used in supply KPIs.'>Queue completion</label>
      <select id='queue_scn'><option value='p50' selected>P50</option><option value='p90'>P90</option></select>
      <label title='Market tightness modifies implied congestion assumptions.'>Market tightness</label>
      <select id='tight_scn'><option value='0.85'>Low congestion</option><option value='1.0' selected>Base</option><option value='1.25'>High congestion</option></select>
    </div>
    <div class='card'>
      <b>Finance Knobs</b>
      <label title='Installed cost per kW.'>Capex (USD/kW)</label>
      <input id='capex' type='number' value='{cfg['finance_assumptions']['capex_per_kw']}' step='25'>
      <label title='Fixed annual operating cost per kW.'>Opex (USD/kW-yr)</label>
      <input id='opex' type='number' value='{cfg['finance_assumptions']['fixed_opex_per_kw_year']}' step='1'>
      <label title='Discount rate proxy for weighted average cost of capital.'>WACC</label>
      <input id='wacc' type='number' value='{cfg['finance_assumptions']['equity_discount_rate']}' step='0.005'>
      <label title='Debt interest rate assumption.'>Debt rate</label>
      <input id='debt' type='number' value='{cfg['finance_assumptions']['debt_rate']}' step='0.005'>
      <label title='Revenue structure for the project case.'>Contract type</label>
      <select id='contract_type'><option value='contracted' selected>Contracted</option><option value='merchant'>Merchant</option></select>
      <label title='Power purchase agreement proxy price.'>PPA price (USD/MWh)</label>
      <input id='ppa' type='number' value='{market_metrics.get('solar_capture_price_usd_mwh',0.0):.2f}' step='0.5'>
      <label title='Annual energy degradation assumption.'>Degradation</label>
      <input id='degrade' type='number' value='{cfg['finance_assumptions']['degradation_rate']}' step='0.001'>
    </div>
  </aside>
  <main class='main'>
    <div class='tabs'>
      <button class='tab-btn active' data-tab='overview'>Overview</button>
      <button class='tab-btn' data-tab='load'>Load</button>
      <button class='tab-btn' data-tab='supply'>Supply</button>
      <button class='tab-btn' data-tab='markets'>Markets</button>
      <button class='tab-btn' data-tab='finance'>Finance</button>
      <button class='tab-btn' data-tab='downloads'>Downloads</button>
    </div>

    <section id='overview' class='tab active'>
      <div class='grid3'>
        <div class='kpi'><div class='m'>Average Price (USD/MWh)</div><div class='v' id='k_avg_price'>-</div></div>
        <div class='kpi'><div class='m'>Solar Capture (USD/MWh)</div><div class='v' id='k_solar'>-</div></div>
        <div class='kpi'><div class='m'>Base NPV (MUSD)</div><div class='v' id='k_npv'>-</div></div>
      </div>
      <div class='foot'>Definitions: capture price = profile-weighted average price; congestion proxy = absolute deviation from 24h moving-average price.</div>
    </section>

    <section id='load' class='tab'>
      <h3>Load and Weather</h3>
      <img class='chart' src='../charts/ercot_load.svg' alt='Hourly load chart (MW)'>
      <img class='chart' src='../charts/ercot_temperature.svg' alt='Hourly temperature chart (F)'>
      <img class='chart' src='../charts/ercot_load_forecast_scenarios.svg' alt='Load forecast scenarios chart (avg MW)'>
      <div class='foot'>Units: MW for load, F for temperature.</div>
    </section>

    <section id='supply' class='tab'>
      <h3>Supply Queue Outlook</h3>
      <img class='chart' src='../charts/ercot_queue_expected_online_mw.svg' alt='Queue expected online MW by year'>
      <div class='kpi'><div class='m'>Selected Queue Scenario Total (MW)</div><div class='v' id='k_queue_total'>-</div></div>
      <div class='foot'>P50/P90 represent expected online capacity under different completion assumptions.</div>
    </section>

    <section id='markets' class='tab'>
      <h3>Market Metrics</h3>
      <img class='chart' src='../charts/ercot_price.svg' alt='Hub price chart (USD/MWh)'>
      <div class='grid3'>
        <div class='kpi'><div class='m'>Wind Capture (USD/MWh)</div><div class='v' id='k_wind'>-</div></div>
        <div class='kpi'><div class='m'>Congestion Mean (USD/MWh)</div><div class='v' id='k_cong'>-</div></div>
        <div class='kpi'><div class='m'>Negative Price Share (%)</div><div class='v' id='k_neg'>-</div></div>
      </div>
      <details><summary>Metric Notes</summary><div class='foot'>Negative price share = hours with price < 0 divided by total hours in modeled period.</div></details>
    </section>

    <section id='finance' class='tab'>
      <h3>Project Finance</h3>
      <div class='grid3'>
        <div class='kpi'><div class='m'>Scenario IRR</div><div class='v' id='k_irr'>-</div></div>
        <div class='kpi'><div class='m'>Min DSCR</div><div class='v' id='k_dscr'>-</div></div>
        <div class='kpi'><div class='m'>LCOE (USD/MWh)</div><div class='v' id='k_lcoe'>-</div></div>
      </div>
      <img class='chart' src='../charts/ercot_finance_sensitivity.svg' alt='Finance sensitivity chart'>
      <div class='foot'>Finance KPIs are scenario-linked to price/capex controls and provide directional screening outputs.</div>
    </section>

    <section id='downloads' class='tab'>
      <h3>Downloads and Report</h3>
      <div class='dl'>
        <a href='../../data/marts/ercot_market_metrics.csv' download>Download market metrics CSV</a>
        <a href='../../data/marts/ercot_load_backtest.csv' download>Download load backtest CSV</a>
        <a href='../../data/marts/ercot_load_forecast_scenarios.csv' download>Download load scenarios CSV</a>
        <a href='../../data/marts/ercot_finance_scenarios.csv' download>Download finance scenarios CSV</a>
        <a href='../../data/marts/ercot_finance_sensitivity.csv' download>Download finance sensitivity CSV</a>
        <a href='../../data/marts/ercot_queue_calibration.csv' download>Download queue calibration CSV</a>
        <a href='../../data/curated/ercot_queue_expected_online_mw.csv' download>Download queue outlook CSV</a>
        <a href='summary_report.html' target='_blank'>Open auto-generated summary report</a>
      </div>
      <div class='foot'>Dashboard runtime uses processed tables only and does not fetch external data.</div>
    </section>
  </main>
</div>

<script>
const DATA = {json.dumps(embedded)};

function fmt(n, d=2) {{ return Number(n).toFixed(d); }}
function scenarioKeyFromControls() {{
  const price = document.getElementById('tight_scn').value;
  const contractType = document.getElementById('contract_type').value;
  const capex = Number(document.getElementById('capex').value);
  const baseCapex = {cfg['finance_assumptions']['capex_per_kw']};
  let priceCase = 'base';
  if (Number(price) < 1) priceCase = 'low';
  if (Number(price) > 1) priceCase = 'high';
  let capexCase = 'base';
  if (capex < baseCapex) capexCase = 'low';
  if (capex > baseCapex) capexCase = 'high';
  return contractType + '|' + priceCase + '|' + capexCase;
}}

function refresh() {{
  const loadMult = Number(document.getElementById('load_scn').value);
  const tightMult = Number(document.getElementById('tight_scn').value);
  const queueMode = document.getElementById('queue_scn').value;

  const avgPrice = DATA.kpis.avg_price * tightMult;
  const solar = DATA.kpis.solar_capture * tightMult;
  const wind = DATA.kpis.wind_capture * tightMult;
  const cong = DATA.kpis.congestion_mean * tightMult;
  const neg = Math.min(DATA.kpis.negative_share * tightMult, 1);

  document.getElementById('k_avg_price').textContent = fmt(avgPrice);
  document.getElementById('k_solar').textContent = fmt(solar);
  document.getElementById('k_wind').textContent = fmt(wind);
  document.getElementById('k_cong').textContent = fmt(cong);
  document.getElementById('k_neg').textContent = fmt(neg * 100, 1);

  const qArr = queueMode === 'p50' ? DATA.series.queue_p50 : DATA.series.queue_p90;
  const qTotal = qArr.reduce((a,b)=>a+b,0) * loadMult;
  document.getElementById('k_queue_total').textContent = fmt(qTotal, 1);

  const key = scenarioKeyFromControls();
  const s = DATA.finance_scenarios[key] || DATA.finance_scenarios['contracted|base|base'];
  document.getElementById('k_npv').textContent = fmt(s.npv_musd, 2);
  document.getElementById('k_irr').textContent = fmt(s.irr, 3);
  document.getElementById('k_dscr').textContent = fmt(s.min_dscr, 2);
  document.getElementById('k_lcoe').textContent = fmt(s.lcoe_usd_mwh, 2);
}}

for (const btn of document.querySelectorAll('.tab-btn')) {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(s => s.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(btn.dataset.tab).classList.add('active');
  }});
}}

for (const id of ['load_scn','queue_scn','tight_scn','contract_type','capex','opex','wacc','debt','ppa','degrade']) {{
  document.getElementById(id).addEventListener('input', refresh);
}}
refresh();
</script>
</body>
</html>
"""

    dashboard_path.write_text(html, encoding="utf-8")
    log_metadata(
        cfg["reports"]["metadata_log"],
        f"dashboard:generated dashboard={dashboard_path} summary={summary_report_path}",
    )


if __name__ == "__main__":
    run_dashboard()
