# Live data: what calculates what, and what it needs

Three artifacts on three clocks. The rule that keeps them apart is one sentence:

> **The bundle says what a thing IS. A feed says what it is DOING. Nothing appears in both,
> and a feed is addressed by an id the bundle already holds.**

A cron therefore never reads the bundle, never holds the atlas, and never matches anything.
It fetches by id and writes `id → data`.

---

## Gauges

### Producers

| Producer | Runs | Reads | Writes |
|---|---|---|---|
| `data/fetch_data.py --layers hydrometric_stations` | per build | ECCC OGC API + today's transmitting roster | `data/bc_hydrometric_stations.json` |
| `pipeline.hydro.match` | per build | that roster, `graph.pkl`, `geometries.pkl`, `aliases.json` | `pipeline/gauge_match.json` (every station, with provenance) |
| `pipeline.hydro.shed` | per build | `graph.pkl` + the match | `section_gauge`, `lake_gauge`, `section_down` |
| hydro job — HYDAT tier | **yearly**, gated on the release listing date | the 266 MB HYDAT release | `gauge/clim.json` — 325 KB, all stations |
| hydro job — reading tier | **30 min** | ECCC readings, BCRFC CLEVER | `gauge/index.json`, `gauge/{station}.json` |

**Both feed artifacts are on the 30-minute clock.** Only the envelope is yearly, and it is
not a feed — it ships in the bundle. The per-station file needs no HYDAT at all: last year
coarse, this year coarse, 72 h fine and the forecast all come straight from ECCC and BCRFC,
exactly as v1 did it.

### Where the percentile comes from

A percentile is *today's discharge* placed inside *the envelope for today's day of year*. Two
inputs, and only one producer has both:

```
HYDAT (yearly, 266 MB) ──> gauge/clim.json  (325 KB, ALL stations)
                                  ├──> 30-min cron reads it ──> percentile ──> gauge/index.json
                                  └──> bundle build snapshots it ──> gauge_clim

ECCC (30 min) ──> gauge/{station}.json   (no envelope needed at all)
```

**The cron never opens the bundle**, and this is the constraint that decides the shape. An R2
worker and a GitHub Actions runner both have small memory budgets, and the bundle is 63 MB.
So the envelope is published as **its own artifact**, one file for every station.

**It fits, and by a wide margin.** The envelope was measured at **325 KB** for all ~450
stations: pentad-sampled rather than daily, p10–p90 rather than p0–p100 (the extremes do not
interpolate — 82% error — and a record maximum is one storm, not a function of day-of-year).
A cron holding 325 KB plus 450 readings fits anywhere.

ONE FILE, NOT ONE PER STATION. The 30-minute job needs every station at once to write the
index; per-station envelopes would be 450 fetches to produce one 35 KB file.

**Why the percentile is precomputed rather than left to the client.** On web the bundle is
range-read over HTTP, so a map that needed the envelope to pick a colour would block its
first paint on SQLite. The index is one 35 KB fetch. And the publisher computing it once
means every client agrees by construction; two clients on different bundle versions
computing it themselves would not.

**Why the bundle keeps `gauge_clim` anyway.** It answers a different question. The index
gives a NUMBER — "this dot is at the 4th percentile today". The envelope gives a SHAPE — the
seasonal band behind a hydrograph, and what a spot saved last October can still say about
that day, offline, with no feed.

### What each artifact holds

### The date picker does not colour gauges

A natural next question is "the user picks a day — can the map colour gauges for it?" The
index holds today's percentile, so for any other day it cannot, and it should not be made to:
a whole-map historical view would need one index per day.

The split that resolves it is already in the app. **The date picker is a REGULATIONS
control** — those are seasonal, they live in the bundle, and any date works offline.
**Conditions are now**, because you cannot fish last Tuesday. A saved spot keeps its own
frozen percentile from the day it was made, which is how a past day gets answered at all.

If a per-river historical view is wanted later, the per-station file already carries two
years of daily means — that is a one-station question, answered on tap, not a map-wide one.

```
bundle
  gauge         station, name, lon/lat, area_km2, mag, matched_by, match_m
  section_gauge section -> ONE station + trust band          (stream stations only)
  lake_gauge    lake item -> station                          (a LEVEL, not a discharge)
  section_down  the trace, shed members only
  gauge_clim    pentad envelope p10..p90
  gauge_stats   from_year, to_year, years, days, parameter

feed
  gauge/index.json      30 min   {station: {percentile, observedAt, forecast:[d1,d2,d3]}}
  gauge/{station}.json  30 min   now + 72h fine + this year daily + last year daily
                                 + BCRFC forecast runs.  NO envelope, no HYDAT.
```

**No liveness column anywhere in the bundle.** Not `realtime`, not `active`. A station is
transmitting iff it is present in `gauge/index.json`. Any boolean baked into a per-build
artifact is right the week it is cut and wrong months later — slowly enough that nobody
notices. `GaugeLink.live` is therefore three-valued: absent feed means `null`, not `false`.

### Long-term data

Only two things survive past 14 days, and neither is a raw archive:

| | Where | Why |
|---|---|---|
| pentad envelope + record-of-years stats | bundle | the map needs a percentile for water nobody opened |
| this year + last year, daily | feed, per station | the two lines of the seasonal chart |
| anything older | **nowhere** | it is already in the envelope |

`gauge_stats` exists because "below normal for the date" means something different backed by
97 years than by 11, and a percentile with no stated record claims more authority than it has.

### The matcher reads every name a node answers to

Not just its display name — and this replaced a hand-written alias file entirely.

That file briefly held seven entries: Arrow, Revelstoke, Duncan, Nechako, Blakeny,
Blackwater, West Road. Every one turned out to be a name the atlas **already carried** in
`name_tuples`, which is what `name_variants.json` feeds. The node displayed as "Lower Arrow
Lake" carries "Arrow Reservoir"; "Intata Reach" carries "Nechako Reservoir"; "West Road
(Blackwater) River" carries "West Road River". Comparing against `display_name` alone threw
all of that away and then asked a person to type it back in by hand.

This is the right direction of dependency: the matcher **consumes** the shared names and
never writes to them. Letting a matcher edit that file is what corrupted v1's display names.

**Overrides bind to a node id, never to a name.** A name-keyed override is a second guess at
the same ambiguous question: it breaks when two waters share the string, and silently follows
the wrong one when the FWA renames something. `aliases.json` is expected to stay empty.

The **complete** record is `pipeline/gauge_match.json` — every station with its status and
how it resolved. A new station that matches needs no action; one that does not appears as
`unresolved` in the build's own summary line.

It replaced a `gauge_nodes.json` written into the build directory, and the difference is the
point: that file keyed each station to a `node_id`, which is build output and moves whenever
the sectionizer cuts differently — so it was stale the moment the next build ran, while
sitting in the build directory looking authoritative. The committed file addresses a station
by its published coordinate plus the FWA's own `wsc`/`wbk`, none of which re-sectioning can
move. Stale copies may still exist under older `output/v2/*` directories; nothing reads
them.

---

## Stocking

Identical shape, different id. FIDQ's `waterbody_id` is a stable TEXT key, so it addresses
the feed directly.

### Matching is an EXACT KEY JOIN, not a name search

FIDQ publishes a `WATERBODY_IDENTIFIER` (`02322SAJR`) which is the same string as the FWA's
own `WATERBODY_KEY_GROUP_CODE_50K` — a column **already present** in the `FWA_LAKES_POLY`
layer this pipeline fetches. v1's notes record that putting this join first "resolves the
large majority of rows on its own".

So the cascade is:

| Tier | How | Notes |
|---|---|---|
| 1 | FIDQ identifier → FWA group code | exact. A group code covering several waterbody keys breaks the tie on distance to FIDQ's own anchor point |
| 2 | name + 2 km radius | for what the identifier cannot answer |
| 3 | override → **node id** | last resort, binds to the water not to a name |

Two rules carried over from v1, both because they were learned the hard way:

- **An identifier match that no name corroborates is flagged, not hidden.** v1 called it
  `_confirmed_by_name()`. An exact key agreeing with no name is more likely a stale
  identifier than a surprise. It matches, and it carries `unconfirmed`.
- **Two differently-named waters in range is `ambiguous`, never a coin toss.** BC has many
  lakes sharing a name, and a stocking record on the wrong one tells somebody there are fish
  where there are none. The row names its candidates so it can be curated.

**Still to do:** the graph does not yet carry `WATERBODY_KEY_GROUP_CODE_50K` onto its lake
nodes, so tier 1 has nothing to join against until it does. That column, plus the FIDQ fetch,
are what `stock_water` is waiting on.

| Producer | Runs | Reads | Writes |
|---|---|---|---|
| FIDQ fetch | per build | `a100.gov.bc.ca/pub/fidq` | waterbody roster + releases |
| stocking match | per build | roster, `graph.pkl`, `geometries.pkl` | `stock_water` |
| stocking job | weekly | FIDQ releases, **by waterbody id** | `stocking/index.json`, `stocking/{id}.json` |

```
bundle
  stock_water  waterbody_id -> item_id, name, lon/lat, matched_by, match_m
  stock_code   species and life-stage integers -> labels

feed
  stocking/index.json         {waterbody_id: last release date}     -- map recency
  stocking/{waterbody_id}.json every release ever for that water    -- one fetch, on tap
```

Species and stage travel as integer codes: "Rainbow Trout" written out 200,000 times is
3.4 MB of one string. The dictionary is in the bundle because it is identity, not activity.

**The `release` table is retired.** It is keyed on a NAME, which is the one join that cannot
be trusted — two lakes share a name and neither gets the right fish.

---

## Bathymetry — deliberately not a feed

Survey sheets do not change. `chart` is bundle-only, and the matching happens in the bundler
with the same provenance discipline as the two above. v1's bathymetry matching is the reason
this document insists on `matched_by`: bad matches there were "fixed" by editing shared name
variants, which corrupted display names elsewhere. **A bathymetry match may never write back
into a name file.**

## The forecast

A fourth source, and the only one in this document that is not a measurement.

    BC River Forecast Centre (Province of BC)  ->  pipeline/hydro/forecast.py
                                               ->  rides in the same feed files

Three seasonal models, each an ArcGIS FeatureServer layer read in **one request**:

| model  | season      | step   | horizon | asked |
|--------|-------------|--------|---------|-------|
| CLEVER | freshet     | hourly | 10 days | how HIGH |
| COFFEE | fall floods | daily  | 5 days  | how HIGH |
| ELF    | low flow    | daily  | 30 days | how LOW  |

Outside its season a model publishes nothing, so **a station with no forecast today is the
normal case**, not a failure. Where two models overlap at the shoulders of their seasons the
fresher issue time wins — which is why `_issued` normalises the Centre's own wording
("Updated at: 09:38 AM Tue 2026-09-01") into something that sorts.

Three things this deliberately does NOT do:

- **It does not fetch the per-station CSVs.** The Centre publishes one per station and v1
  read all of them: ~450 requests every thirty minutes against a provincial endpoint, for
  numbers the summary layer already carries. Three requests is polite.
- **It never lands on a level chart.** The models output discharge. The Fraser at Mission
  reports stage — about 1.8 m — and its outlook is 3,157 m³/s; on one axis that is two units
  in one frame. `gaugeSeries` attaches a forecast only where the chart is already in m³/s.
- **It is never drawn as an observation.** Past the dotted rule, fainter band, dashed line —
  three cues, because one is a legend nobody read.

The Province's attribution is required **verbatim** wherever a forecast appears. It lives in
`pipeline/hydro/forecast.ATTRIBUTION` and in the app's attribution list, and the two must
stay identical.

## Where a gauge IS — one frozen fact, two consumers

Everything about a station's position lives in **`pipeline/gauge_match.json`**, and nothing
matches anything anywhere else.

```
data/bc_hydrometric_stations.json        fetched roster (fetch_data --layers hydrometric_stations)
  │
  │   python -m pipeline.hydro.match --build <a completed build>
  ▼
pipeline/gauge_match.json                COMMITTED. One row per station:
  │                                        streams  wsc + blk + measure
  │                                        lakes    wbk
  │                                        neither  status=unresolved, with the reason
  │
  ├──► pipeline.build      a `gauge` point anchor at the station's coordinate, scoped by
  │                        `wsc`, appended AFTER pipeline/splits.json and resolved by the
  │                        same resolver → rivers get cut at their gauges
  │
  └──► pipeline.bundle     the node on `blk` whose measure range contains `measure`
                           (or `lake:{wbk}`) in THAT build's graph → sheds, gauge table
```

**Why the keys are FWA's and not ours.** A node id is `{blk}:{down_m}` — build output, and it
moves the moment a river is sectioned differently, which the gauge cuts do deliberately on
the very next build. A match frozen as node ids is stale by construction. `wsc`, `blk`,
`measure` and `wbk` all come out of the source data and survive any amount of re-cutting, so
each consumer derives what it needs and no cached derivative can disagree with the original.

**Why it is two-pass.** Matching compares a station's name against every name a node carries,
and those include `name_variants.json`, which is applied *during* a build. So the match needs
a completed build, and the build needs the match. Re-run the matcher after a station moves or
a new one starts reporting; until then the cuts are one build behind, which is stated rather
than hidden.

**Why a tributary gauge cannot speak for its mainstem.** A shed is bounded by drainage as
well as by the flow walk: a reach qualifies only if its watershed code IS the gauge's or
descends from it. Walking downstream from a tributary arrives at the mainstem, and the
magnitude ratio cannot refuse it because the ratio is symmetric and the question is not —
SLESSE CREEK NEAR VEDDER CROSSING was speaking for 13 reaches of the Chilliwack off a creek
carrying a seventh of the river, 7 of them rated `fair`. Measured: 3,416 of 118,331 rows
refused, 642 of them previously `good`.
