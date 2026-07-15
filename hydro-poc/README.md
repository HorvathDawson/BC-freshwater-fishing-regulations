# Hydrometric Gauge Data — Proof of Concept

Fetches water level / discharge data from **Environment and Climate Change
Canada's Wateroffice** real-time web services, stores it in a local SQLite
database (`hydro.db`), and renders raw plots in the browser.

> ⚠️ Proof of concept only. Eventually this data will be stored in Cloudflare
> and surfaced on the fishing website. Wherever it is displayed, the ECCC
> **attribution string must be shown verbatim** (see below).

No third-party Python dependencies — standard library only (`urllib`,
`sqlite3`, `csv`, `http.server`). Plots use Chart.js from a CDN.

## Stages

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

# 1. Discover stations (province-tagged). Default = all provinces.
python hydro_poc.py stations                  # every province
python hydro_poc.py stations --provinces BC    # BC only
python hydro_poc.py stations --provinces BC --coords   # + lat/lon from KML

# 2. Bulk historical pull. Default = a small BC sample, daily means, 18 months.
python hydro_poc.py bulk                       # sample of 5 stations
python hydro_poc.py bulk --bc                   # ALL BC stations (province=BC)
python hydro_poc.py bulk --stations 08MF005 08HB048
python hydro_poc.py bulk --all                 # EVERY station (large + slow!)

# Unit values (5-min) instead of daily means (18-month availability):
python hydro_poc.py bulk --bc --parameters 46 47 --months 18

# 3. Latest real-time values (only params 46 & 47 exist here)
python hydro_poc.py realtime --bc
python hydro_poc.py realtime --all

# 4. BCRFC model forecasts (CLEVER / COFFEE / ELF)
python hydro_poc.py forecast                 # all three models (+ ELF daily series)
python hydro_poc.py forecast --models CLEVER # one model
python hydro_poc.py forecast --no-series      # skip ELF daily-series CSVs

# 5. View it
python serve.py       # open http://localhost:8765
```

In the viewer, pick a **Forecast model** to filter the station list to stations
that have that forecast (and observed data). The observed week is always shown;
then:
- **ELF** — the **real daily forecast** (QFOR min/avg/max time series) is drawn
  extending past the observations, pulled from BCRFC's per-station CSV.
- **CLEVER / COFFEE** — only period min/avg/max summaries are published, so a
  bounds band is shown with a link to the full hydrograph PDF.

The overlay applies when a discharge parameter (6 or 47) is selected.

Re-running `realtime` on a schedule (cron / GitHub Action) is how the
production version would keep the "current conditions" fresh.

## Parameters

| id | meaning                       | availability |
|----|-------------------------------|--------------|
| 3  | Water level (daily mean)      | past 60 months |
| 6  | Discharge (daily mean)        | past 60 months |
| 46 | Water level (unit / 5-min)    | past 18 months |
| 47 | Discharge (unit / 5-min)      | past 18 months |

All service times are **UTC**.

## Data structure (`hydro.db`)

- `stations(station_id, name, province, lat, lon, data_available,
  operation_schedule, updated_at)` — province comes from the station-search table;
  lat/lon are optional (backfilled via `stations --coords`).
- `readings(station_id, ts, parameter, value, grade, symbol, approval,
  qualifier, source, attribution)`
- `forecasts(model, station_id, station_name, issued_at, horizon_days,
  obs_value, obs_rp, forecast_value, forecast_min, forecast_ave, forecast_max,
  forecast_rp, hydrograph_url, lat, lon, raw, attribution, fetched_at)` —
  BCRFC CLEVER/COFFEE/ELF summaries, joined to gauges by `station_id`.
- `forecast_series(model, station_id, date, qobs, qfor_min, qfor_ave, qfor_max,
  hobs, hfor_min, hfor_ave, hfor_max, fetched_at)` — ELF full daily forecast
  time series (from BCRFC per-station CSVs).

> Note: some gov.bc.ca hosts serve an incomplete TLS chain that conda's Python
> can't verify. Install `certifi` (`pip install certifi`) for clean fetching;
> otherwise the script retries those public endpoints without verification and
> prints a warning.

`source` is `bulk` or `realtime`; `attribution` stores the exact ECCC string
for that batch so the website can render it.

## Attribution (REQUIRED)

Per the [Wateroffice FAQ](https://wateroffice.ec.gc.ca/contactus/faq_e.html):

- **Real-time:** _"Extracted from the Environment and Climate Change Canada
  Real-time Hydrometric Data web site
  (https://wateroffice.ec.gc.ca/mainmenu/real_time_data_index_e.html) on [DATE]"_
- **Historical:** _"Extracted from the Environment and Climate Change Canada
  Historical Hydrometric Data web site
  (https://wateroffice.ec.gc.ca/mainmenu/historical_data_index_e.html) on [DATE]"_
- **HYDAT.mdb:** _"Extracted from Environment and Climate Change Canada's
  HYDAT.mdb, released on [DATE]"_

## Approval & Grade

**Approval** — `PROVISIONAL` (best available, subject to change) ·
`FINAL` (confirmed, meets all quality standards).

**Grade** — `10 ICE`, `20 ESTIMATED`, `30 PARTIAL DAY`, `40 DRY`,
`50 REVISED` (see `hydro_poc.py` docstring for full descriptions).

## Web service endpoints used

- Station Search (province-tagged list): `…/search/real_time_results_e.html?search_type=province&province=BC`
- Current Conditions (KML, coords only): `…/services/current_conditions/xml/inline`
- Recent Real-Time Data (CSV): `…/services/recent_real_time_data/csv/inline`
- Real-Time Data (CSV, windowed): `…/services/real_time_data/csv/inline`

Notes: split large requests into fewer stations or shorter time windows.
