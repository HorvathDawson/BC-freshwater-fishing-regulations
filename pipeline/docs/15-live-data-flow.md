# Live data: what calculates what, and what it needs

> **The bundle says what a thing IS. A feed says what it is DOING. Nothing appears in both, and a feed is addressed
> by an id the bundle already holds.**

A feed job never reads the bundle, never holds the atlas and never matches anything: it fetches by id and writes
`id -> data`.

## Gauges

### Producers

Cadences are `app/workers/feeds/wrangler.toml`.

| Producer | Runs | Reads | Writes |
|---|---|---|---|
| `data/fetch_data.py --layers hydrometric_stations` | by hand, when the feed's `roster` notice asks (`index.json` `maintenance`) | ECCC OGC API + ECCC's real-time list | `data/source/bc_hydrometric_stations.json` |
| `pipeline.gauges.generate.match` | by hand, a few times a year; needs a completed atlas | the roster, `graph.pkl`, `geometries.pkl`, `aliases.json` | `data/generated/gauges/match.json` (raw, every station) |
| `pipeline.gauges.generate.promote --only <station…>` | by hand, per reviewed station | the raw match + the review | `data/curated/gauges/matches.json` + the queue `data/generated/gauges/candidates.json`; a full promote refuses demotions without `--accept-demotions` (AGENTS 38) |
| `pipeline.gauges.consume.shed` | every bundle build | `graph.pkl` + the frozen matches | `section_gauge`, `lake_gauge`, `section_down` |
| `pipeline.gauges.feed.climatology` (the `clim` build stage) | when HYDAT publishes (the feed's `hydat` notice): `scripts/update-hydat.sh`, or the manual workflow `feeds-hydat.yml` | the HYDAT release (sqlite) + accepted datum changes | `clim.json`: pentad envelopes p10-p90, recent years, stats, ratings; uploaded to R2 `feeds/live/clim.json`; the bundle snapshots it as `gauge_clim` / `gauge_stats` |
| `pipeline.gauges.feed.hydat_daily --index` | with the climatology | the HYDAT release | `hydat_daily/<station>.json` -> R2 `feeds/hydat/<station>.json`, the daily means a past day's flow is read from (`dayFlow.ts`) |
| the feeds Worker, `runGauges` (`app/workers/feeds/src/gauges/publish.ts`) | every 30 min by day, hourly at night | ECCC readings, BCRFC forecasts, `clim.json` | `feeds/live/index.json`, `feeds/live/<station>.json`, `feeds/live/obs.json` |

The Worker is the one gauge publisher. `pipeline/gauges/feed/publish.py` is its parity spec and the local dev feed only.
Inside its gauge run it asks HYDAT's listing once a day and ECCC's real-time list every run, and writes what is due into
`index.json` `maintenance` (`hydat`, `roster`; AGENTS 64).

### Where the percentile comes from

A percentile is today's discharge placed inside the envelope for today's day of year.

```
HYDAT ──> clim.json ──> R2 feeds/live/clim.json ──> the Worker's gauge run ──> percentile ──> index.json
              └──> the bundle build snapshots it ──> gauge_clim, gauge_stats
ECCC (30 min) ──> <station>.json + obs.json   (no envelope needed)
```

- **The Worker never opens the bundle** (128 MB of memory). The envelope is its own artifact, one file for every
  station, because the run needs every station at once to write the index. The run keeps only the stations it asks
  and drops `stats` (`publish.ts`, the `slim` step).
- The envelope is pentad-sampled and p10-p90: the extremes do not interpolate, and a record maximum is one storm.
- **The percentile is computed once, by the publisher**, so every client agrees and a map never waits on SQLite to
  colour a dot. The bundle keeps `gauge_clim` for a different question: the seasonal band behind a hydrograph, and
  what a saved spot can say offline.
- **A station that does not answer** keeps its last good index entry marked `stale` (with `staleSince`) while its
  observation is under `CARRY_MAX_MS` (3 days) old, then drops out. `obs.json` holds every station written this run
  with its rows of the last `OBS_WINDOW_MS` (6 h), so a phone keeps a week of measured line offline.
- **The date picker does not colour gauges.** The index holds today only; the picker is a regulations control.
  Conditions are now; a saved spot keeps its own percentile from the day it was made.

### What each artifact holds

```
bundle
  gauge         station, name, lon/lat, area_km2, mag, matched_by, match_m
  section_gauge section -> ONE station + trust band          (stream stations only)
  lake_gauge    lake item -> station                          (a LEVEL, not a discharge)
  section_down  the trace, shed members only
  gauge_clim    pentad envelope p10..p90
  gauge_stats   from_year, to_year, years, days, parameter
feed (R2 feeds/)
  live/index.json      {station: {percentile, observedAt, forecast, stale?, staleSince?}}, listed, maintenance
  live/<station>.json  now + recent rows + this year / last year daily + BCRFC forecast runs; no envelope
  live/obs.json        every station written this run, its last 6 h of rows
  live/clim.json       the envelope the run ranks against (an input, written by the climatology)
  hydat/<station>.json the stored daily means
```

- **No liveness column anywhere in the bundle.** A station is transmitting when it is in `index.json`. The publisher
  asks the stations on ECCC's real-time list as of that run and publishes it as `index.json.listed`: in `listed` but
  not in `stations` is a live gauge quiet this run; not in `listed` has no real-time data at ECCC. A listed station
  the roster has no row for is an owner notice, once per station. `GaugeLink.live` is three-valued: no feed is
  `null`, not `false`.
- Only two things survive past 14 days: the envelope with its record-of-years stats (bundle) and this year + last
  year daily (feed). `gauge_stats` exists because "below normal" backed by 97 years means more than by 11.
- A gauge's zero that moved is a datum change: `pipeline/docs/GAUGE-DATUM-CHANGES.md`.

### Where a gauge is: one frozen fact, two consumers

Everything about a station's position is `data/curated/gauges/matches.json`; nothing matches anywhere else. A row
holds a stream station's `wsc` + `blk` + `measure`, a lake station's `wbk`, or `status=unresolved` with the reason.

- `pipeline.atlas.build` adds a `gauge` point anchor at the station's coordinate, scoped by `wsc`, after
  `data/curated/waters/cuts.json`, so rivers are cut at their gauges.
- `pipeline.deliver.bundle` takes the node on `blk` whose measure range holds `measure` (or `lake:{wbk}`) in that
  build's graph.
- **The keys are FWA's, never a node id.** A node id moves whenever a river is sectioned differently; `wsc`, `blk`,
  `measure` and `wbk` survive any re-cut.
- **It is two-pass.** The matcher compares a station's name with every name a node carries, including
  `name_variants.json`, which a build applies; so the match needs a completed build. Re-run it after a station moves
  or starts reporting; until then the cuts are one build behind.
- **The matcher only reads the shared names**; it never writes a name file. Overrides bind to a node id, never a name;
  `aliases.json` is expected to stay empty.
- **A tributary gauge never speaks for its mainstem.** A reach qualifies only if its watershed code is the gauge's or
  descends from it; representativeness is policy (`pipeline/gauges/README.md`).

## The forecast

The BC River Forecast Centre's three seasonal models ride in the same feed files (`pipeline/gauges/feed/forecast.py`),
each an ArcGIS layer read in one request: CLEVER (freshet, hourly, 10 days), COFFEE (fall floods, daily, 5 days), ELF
(low flow, daily, 30 days).

- Outside its season a model publishes nothing, so no forecast is the normal case. Where two overlap, the fresher issue
  time wins.
- It never fetches the per-station CSVs (three requests, not ~450 every half hour).
- It never lands on a level chart (the models output discharge; `gaugeSeries` attaches it only to an m³/s chart) and
  is never drawn as an observation.
- The Province's attribution is required verbatim wherever a forecast appears: `forecast.py` `ATTRIBUTION` (the feed
  carries it) and the app's attribution list, identical.

## Stocking and bathymetry

Both are prepared, not fed: `pipeline/stocking/README.md` and `pipeline/bathymetry/README.md` say what each needs.
Their rules here: a match is an exact key join first (FIDQ's `WATERBODY_IDENTIFIER` and a survey sheet's code are the
FWA `WATERBODY_KEY_GROUP_CODE_50K`), names only confirm; two differently named waters in range is `ambiguous`, never a
coin toss; an identifier no name corroborates matches as `unconfirmed`; and **no match ever writes back into a name
file**. The lake colour modes read the release's optional `lakes` file (`pipeline/deliver/lake_facts.py`); no feed
produces it yet, so the app greys its Stocked and Depth toggles.
