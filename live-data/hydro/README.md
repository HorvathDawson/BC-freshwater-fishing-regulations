# Hydrometric Gauge Data — Proof of Concept

Fetches water level / discharge data from **Environment and Climate Change
Canada's Wateroffice** real-time web services, stores it in a local SQLite
database (`hydro.db`), and renders raw plots in the browser.

> ⚠️ Proof of concept only. Eventually this data will be stored in Cloudflare
> and surfaced on the fishing website. Wherever it is displayed, the ECCC
> **attribution string must be shown verbatim** (see below).

No third-party Python dependencies — standard library only (`urllib`,
`sqlite3`, `csv`, `http.server`, `concurrent.futures`). Uses a capped thread pool to parallelize network requests for fast data fetching while keeping database writes safe on the main thread. Plots use Chart.js from a CDN.

## Workflow

To keep the database lightweight while maintaining both long-term context and high-resolution current conditions, the POC uses two composite commands — this is the only supported CLI surface:

* **`bootstrap`** — Initial heavy lift. Discovers stations, pulls 18 months of daily means (long-term context), 14 days of 5-minute unit values (current conditions), and all BCRFC forecasts. Network requests are multithreaded (up to 8 concurrent workers) to speed up the pull.
* **`update`** — Maintenance pull. Refreshes the last 14 days of both daily means and 5-minute unit values, plus the latest BCRFC forecasts. Each `update` first purges the previous forecast batch (both the summary row and the per-station forecast series) before storing the new one, so stale forecast data never lingers alongside a fresh pull.

BCRFC forecasts (CLEVER / COFFEE / ELF) come from two sources per model: an ArcGIS feature service (per-station summary bounds + hydrograph PDF link) and that model's own forecast CSV (the real daily/hourly forecast time series, fetched and parsed the same way for all three models — see `fetch_forecast_series` in `hydro_poc.py`).

## Usage

```bash
cd live-data/hydro

# 1. Initial setup (Stations, 18mo daily means, 14d 5-min values, forecasts)
python hydro_poc.py bootstrap --bc

# 2. Recurring update (Run on a cron / GitHub Action to refresh the last 14 days)
python hydro_poc.py update --bc

# Station selection flags (both commands): --bc | --all | --province XX | --stations 08MF005 08HB048
# --no-series skips downloading the per-station forecast CSVs (summary bounds only)

# 3. View it
python serve.py       # open http://localhost:8765