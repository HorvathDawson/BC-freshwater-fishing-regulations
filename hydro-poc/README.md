# Hydrometric Gauge Data — Proof of Concept

Fetches water level / discharge data from **Environment and Climate Change
Canada's Wateroffice** real-time web services, stores it in a local SQLite
database (`hydro.db`), and renders raw plots in the browser.

> ⚠️ Proof of concept only. Eventually this data will be stored in Cloudflare
> and surfaced on the fishing website. Wherever it is displayed, the ECCC
> **attribution string must be shown verbatim** (see below).

No third-party Python dependencies — standard library only (`urllib`,
`sqlite3`, `csv`, `http.server`, `concurrent.futures`). Uses a capped thread pool to parallelize network requests for fast data fetching while keeping database writes safe on the main thread. Plots use Chart.js from a CDN.

## Workflows & Stages

To keep the database lightweight while maintaining both long-term context and high-resolution current conditions, the POC uses composite workflows:

* **`bootstrap`** — Initial heavy lift. Discovers stations, pulls 18 months of daily means (long-term context), 14 days of 5-minute unit values (current conditions), and all forecasts. Network requests are multithreaded (up to 8 concurrent workers) to speed up the massive data pull.
* **`update`** — Maintenance pull. Fetches the last 14 days of both daily means and 5-minute unit values, plus the latest forecasts, to keep the "head" of the dataset fresh. 

You can also run the individual stages manually:
1. **`stations`** — discover stations from the authoritative **Real-time Station
   Search** (province-tagged: official name, province, number, data-availability,
   operation schedule). No bounding box needed.
2. **`bulk`** — one-time historical pull (default: daily means, last 18 months).
3. **`realtime`** — recurring pull of the latest real-time (unit) values.
4. **`forecast`** — BCRFC model forecasts (CLEVER / COFFEE / ELF) from their
   ArcGIS feature services, joined to gauges by station number.

## Usage

```bash
cd hydro-poc

# ==========================================
# RECOMMENDED WORKFLOW
# ==========================================

# 1. Initial setup (Stations, 18mo daily means, 14d 5-min values, forecasts)
python hydro_poc.py bootstrap --bc

# 2. Recurring update (Run on a cron / GitHub Action to refresh the last 14 days)
python hydro_poc.py update --bc

# ==========================================
# GRANULAR / MANUAL COMMANDS
# ==========================================

# 1. Discover stations (province-tagged). Default = all provinces.
python hydro_poc.py stations                  # every province
python hydro_poc.py stations --provinces BC    # BC only
python hydro_poc.py stations --provinces BC --coords   # + lat/lon from KML

# 2. Bulk historical pull. Default = a small BC sample, daily means, 18 months.
python hydro_poc.py bulk                       # sample of 5 stations
python hydro_poc.py bulk --bc                   # ALL BC stations (province=BC)
python hydro_poc.py bulk --stations 08MF005 08HB048
python hydro_poc.py bulk --all                 # EVERY station 

# Unit values (5-min) instead of daily means (18-month availability):
python hydro_poc.py bulk --bc --parameters 46 47 --months 18

# 3. Latest real-time values (only params 46 & 47 exist here)
python hydro_poc.py realtime --bc
python hydro_poc.py realtime --all

# 4. BCRFC model forecasts (CLEVER / COFFEE / ELF)
python hydro_poc.py forecast                 # all three models (+ daily/hourly series)
python hydro_poc.py forecast --models CLEVER # one model
python hydro_poc.py forecast --no-series      # skip forecast series CSVs

# 5. View it
python serve.py       # open http://localhost:8765