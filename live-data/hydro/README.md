# Hydrometric Gauge Data — Proof of Concept

Fetches water level / discharge data from **Environment and Climate Change
Canada's Wateroffice** real-time web services, stores it in a local SQLite
database (`hydro.db`), and renders raw plots in the browser.

> ⚠️ Proof of concept only. Eventually this data will be stored in Cloudflare
> and surfaced on the fishing website. Wherever it is displayed, the ECCC
> **attribution string must be shown verbatim** (see below). BC River Forecast
> Centre forecasts (CLEVER / COFFEE / ELF) are a **separate source** — Province
> of British Columbia — and must carry their own attribution + copyright link
> (https://www2.gov.bc.ca/gov/content/home/copyright) and the RFC disclaimer
> ("Users should use the information on this website with caution and at their
> own risk."). Both strings come from `attribution()` and are surfaced together
> in `/api/attribution`, the viewer footer, and `stations.json`.

No third-party Python dependencies — standard library only (`urllib`,
`sqlite3`, `csv`, `http.server`, `concurrent.futures`). Uses a capped thread pool to parallelize network requests for fast data fetching while keeping database writes safe on the main thread. Plots use Chart.js from a CDN.

## Workflow

Data splits into two cadences — slow-changing station **metadata** and live
**readings / forecasts / conditions** — and the two CLI commands map onto that
split. This is the only supported CLI surface.

### `bootstrap` — full setup (rare; heavy)

Run once, after deleting `hydro.db`, or periodically to refresh station
metadata. In order:

1. **`fetch_stations()`** — discover BC stations (Real-time Station Search
   HTML), writing name / availability / operation schedule.
2. **`fetch_hydat.sync()`** (`fetch_hydat.py`) — merge ECCC's bulk **HYDAT**
   SQLite release (every station Canada operates, ~266MB, refreshed ~every 2
   weeks) to backfill authoritative `lat`/`lon`, `hyd_status` (Active/
   Discontinued), and drainage area. Also the **discovery completeness
   backstop** — see "Discovery" below. Skips the download if already at the
   latest release (tracked in `hydat_sync`); `python fetch_hydat.py --force`
   overrides. **While the release is extracted** it also computes the
   **percentile climatology** (see below) from HYDAT's full daily record — no
   extra download, same slow cadence.
3. **`fetch_current_conditions()`** — Current Conditions KML: live percentile
   classification per station (see below) + coordinate backfill for anything
   HYDAT still left `NULL` (KML never overwrites a HYDAT coordinate).
4. **`backfill_missing_coords()`** — last resort for the handful HYDAT + KML
   both miss (brand-new stations, level-only stations): scrape lat/lon (DMS)
   from each station's own Wateroffice report page. Tagged `coord_source='report'`.
5. **`fetch_bulk()` ×2** — 18 months of daily means (params 3/6) for long-term
   context, then 14 days of 5-minute unit values (params 46/47) for
   high-resolution recent flow → `readings`.
6. **`fetch_forecasts()`** — BCRFC CLEVER / COFFEE / ELF → `forecasts` +
   `forecast_series`.

### Discovery — why the roster is a union

No single source lists every active real-time BC station. The **Real-time
Search** scrape is live but drops a few stations; the **HYDAT** release is
authoritative but lags for brand-new stations. So the roster is their
**union**: `fetch_stations()` seeds it from Search, then `fetch_hydat.sync()`
INSERTs any BC station HYDAT flags **real-time + active** (`REAL_TIME=1 AND
HYD_STATUS='A'`) that Search missed. Discontinued / non-real-time HYDAT rows
are never inserted — HYDAT carries the entire historical roster and must not
drive discovery on its own; only its real-time-active rows are eligible to
add. (`coord_source` records where each coordinate came from: `hydat` |
`kml` | `report`.)

### `update` — maintenance pull (frequent; cheap)

Run on a cron / GitHub Action. Metadata steps (1–2) are **skipped** — station
list and coordinates barely change. It refreshes only live data:

1. **`fetch_current_conditions()`** — refresh percentile classifications.
2. **`fetch_bulk()` ×2** — last 14 days of daily means + 5-minute unit values.
3. **`fetch_forecasts()`** — latest forecasts. Each run first purges the
   previous forecast batch (summary row + per-station series) so stale forecast
   data never lingers.

### Current conditions (percentile)

`fetch_current_conditions()` parses each station's **"Current Condition"** from
the KML feed — how today's flow ranks against the historical record for this
day of year — into `current_conditions`: the class (`Normal`, `Below normal`,
`Much above normal`, `All-time low for this day`, …), the percentile band
(`10th – 24th percentile` → `percentile_low`/`percentile_high`), and the latest
observed discharge/stage. The viewer shows this as a colour-coded banner with a
percentile marker above the chart.

### Percentile climatology (seasonal envelope)

Where `current_conditions` gives one live label ("today ranks Below normal"),
the **climatology** is the same idea drawn as a continuous curve over a season,
so you can see how average this year's conditions have been and where they're
trending — the ECCC KML only carries the single day's label.

`fetch_hydat.compute_climatology()` builds it from HYDAT's full multi-decade
daily record (`DLY_FLOWS` = discharge, `DLY_LEVELS` = level). For each station
and each **day-of-year** it pools every historical observation for that day
(±2 days, wrapping the year boundary, to smooth the envelope) and stores a
percentile band — **P0 / P10 / P25 / P50 / P75 / P90 / P100** — in
`flow_climatology`, with the period-of-record in `flow_climatology_meta`.

Data-integrity gates (a sparse envelope invites a false "record low" read):
- A `(station, parameter)` is published only with **≥ 10 years** of record.
- A single day-of-year percentile is emitted only when its pooled sample
  reaches **≥ 10 observations**; sparser days stay `NULL`.

This is **slow-changing** — recomputed only when a new HYDAT release is synced
(bootstrap / `fetch_hydat --force`), never by the frequent `update`. It fully
replaces the prior climatology (a new release supersedes it wholesale). A DB
synced before this feature existed self-heals: the next sync notices the empty
table and rebuilds without needing `--force`. The bands carry the **HYDAT.mdb**
attribution string (`attribution('hydat', …)`), distinct from the realtime one.

The viewer's **Seasonal view** toggle draws today's flow (and last year's, thin)
against these shaded bands, windowed ±45 days around today, log y-axis for
discharge. Backed by `GET /api/climatology?station=..&parameter=..`.

### Forecasts

BCRFC forecasts (CLEVER / COFFEE / ELF) come from two sources per model: an
ArcGIS feature service (per-station summary bounds + hydrograph PDF link) and
that model's own forecast CSV (the real daily/hourly forecast time series,
fetched and parsed the same way for all three — see `fetch_forecast_series`).
The viewer renders the **full** forecast horizon connected to the observed
line (a daily model like COFFEE always shows all 5 days).

## Export for the cloud (`export_hydro.py`)

`hydro.db` is the local working store; the app is served compact JSON shaped
by **`export_hydro.py`** into `output/hydro/` (gitignored), ready to upload to
Cloudflare R2 (same static-artifact pattern as `in_season.json`). Data is split
into three tiers by how fast it changes, so a frequent cron only rewrites the
small fast-moving files and never regenerates the historical record:

| Artifact | Contents | Size | Cadence |
|---|---|---|---|
| `stations.json` | roster + metadata + **latest value + percentile** per station (map index / headline) | ~210 KB | frequent |
| `recent/<id>.json` | per station: discharge & level at **15-min for 3 days**, **hourly for days 3-14**, + forecast series | ~10-46 KB (~8 KB gzip) | frequent |
| `history/<id>.json` | per station: daily-mean discharge & level, long record | ~10-20 KB | daily |
| `climatology/<id>.json` | per station: day-of-year percentile envelope (P0–P100) from HYDAT's full record — the seasonal chart's bands | ~17 KB (~2.5 KB gzip) | daily (rides history scope) |

The 5-min unit values in `hydro.db` are **downsampled** on export (kept on the
:00/:15/:30/:45 marks, then on-the-hour) — they never leave the DB at full
resolution, which is what keeps the cloud payload small.

```bash
python export_hydro.py                  # --scope all  (bootstrap / manual)
python export_hydro.py --scope realtime # stations.json + recent/  (frequent cron)
python export_hydro.py --scope history  # history/ + climatology/  (daily cron)
```

### Recommended cloud cadence (not yet wired)

Run the fetch on a **GitHub Actions cron** (free, no Worker subrequest/CPU
limits — a full `update` is ~68s wall / ~21s CPU with ~1,825 outbound fetches,
which does **not** fit a Cloudflare Worker), then `export_hydro.py` and upload
`output/hydro/` to R2:
- **Frequent job** (e.g. hourly): `update` (conditions + unit values +
  forecasts) → `export_hydro.py --scope realtime` → upload `stations.json` +
  `recent/`.
- **Daily job**: `update` daily means → `export_hydro.py --scope history` →
  upload `history/` + `climatology/`. The climatology table only repopulates
  when `fetch_hydat` syncs a new HYDAT release (~biweekly, bootstrap-only), so
  this daily re-serialize is cheap and idempotent — it just picks up the fresh
  envelope the day after a sync. It is deliberately **not** in the frequent
  realtime scope.
- **Gauge→FWA link** (`match_fwa.py`): *not* part of either recurring job — it
  only changes when the atlas rebuilds, so run it alongside `--step atlas`. The
  frequent job's `export_hydro` re-resolves the stored fid→reach_id against the
  current deploy shards each run, so gauge links track regenerated reaches for
  free. `serve.py --cron` mirrors this split (see Usage).

The app reads the JSON directly from R2 (no query layer) — `stations.json` for
the map + headline, `recent/<id>.json` on gauge selection, `history/<id>.json`
for the long view.

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

# 3b. Dry-run the cron locally — serve.py can drive the recurring cycle itself.
#     Each --cron tick runs `hydro_poc update --bc` then
#     `export_hydro --scope realtime` (which re-resolves gauge fid→reach_id
#     against the current deploy shards). Status is published at /api/cron and
#     the exported artifacts at /hydro/<file> (e.g. /hydro/stations.json).
python serve.py --cron 300                   # fetch+export every 5 min
python serve.py --cron 900 --match-on-start  # also refresh gauge→fid once at start
python serve.py --cron 900 --match-every 4   # re-run the heavy match_fwa every 4th cycle