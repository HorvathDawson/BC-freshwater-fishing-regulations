# Handoff — match hydro gauges to FWA streams / polygons

## STATUS (implemented — 2026-07)

The first-pass matcher is built, validated, and the name-variation follow-up is
done. Current headline: **434/450 (96.4%) clean single matches, 4 zero, 12
ambiguous** (up from 92.4%/26/8 before name variations + the diacritic fix).

**What exists now:**

- **`match_fwa.py`** — the gauge→FWA matcher. Reprojects each gauge to
  EPSG:3005, does a 5 km radius query over an STRtree of the atlas, parses the
  ECCC name on its qualifier keyword, and name-matches against **every** name a
  feature carries: FWA gnis_name **plus** any display-override / `name_variants`
  from `pipeline/matching/feature_display_names.json`. Writes `gauge_fwa_match`
  and `gauge_fwa_match_summary` into `hydro.db`. `match_all(atlas)` accepts a
  pre-loaded atlas so callers can share the ~5 GB pickle load.

- **Single source of truth for aliases** — `feature_display_names.json` is the
  ONE place name variations live; both the front-end search and this matcher
  read it (never defined twice). Extended this cycle:
  - `FeatureDisplayName` gained `linear_feature_ids` (per-segment override) and
    `name_variants` (searchable + matchable aliases that do NOT replace the
    label). `display_name` is now optional.
  - `DisplayNameResolver` gained fid-level overrides, alias accessors
    (`variants_for_stream` / `variants_for_polygon`), and **variant→label
    promotion**: a `name_variants` entry becomes the display label only when the
    feature has no other name (override or gnis). So an unnamed stream named
    after its gauge shows that name; a reservoir keeps its lake name and the
    reservoir name stays a searchable alias.
  - `reach_builder` merges these aliases into each reach's searchable
    `name_variants` (source `"alias"`), so they're findable in the app.
  - `normalize_name` now folds diacritics (Pouce Coupé/Doré/Barrière matched).

**Entries added to `feature_display_names.json`:**
  - 11 formerly-unnamed streams named after their gauge (variant-promoted):
    Dairy, Icy, M3, Nautley, Nordic, Quinsam Diversion, Renegade, Thautil
    Corner, Two Forty, Whitesail Middle, Kelly.
  - Cedar Creek (alias of FWA "Blakeny Creek"), Cahilty Lake (variant-promoted).
  - 5 reservoir aliases: Revelstoke/Duncan/Arrow(Upper+Lower)/Nechako.
  - **Two Forty-One Creek** — a `linear_feature_ids` display override on the 21
    segments of blue line 356569726 *above* Greyback Lake (route measure ≥
    26850; FWA mislabels them "Penticton Creek"). Kept as an override (not a
    variant) because it must **beat** a wrong inherited gnis, not fill an empty
    one.

## DONE (2026-07) — export wiring + ambiguous/tidal resolution

All three "remaining work" items below are now implemented. Headline unchanged
(**434 exactly-one / 1 zero / 12 ambiguous**) but **449/450 gauges are now
export-linked** — only `08HB087` (Comox Harbour) stays null on purpose.

- **`match_fwa.py`** now resolves a single **primary** match per gauge and
  writes an `is_primary` column on `gauge_fwa_match` (plus a `resolution` tag:
  `single` / `closest` / `override` / `ambiguous`). All candidate rows are
  still written for review; `is_primary=1` marks the one the export uses.
  - **12 ambiguous → closest-distance tiebreak.** `matched` is sorted ascending
    by distance; index 0 becomes primary (`resolution='closest'`). Every one of
    the 12 was an obvious near-vs-far pick (e.g. 27 m vs 2973 m).
  - **3 tidal Fraser → manual override** (`_MANUAL_STREAM_OVERRIDES`). The tidal
    reach is excluded from the atlas, so these name-match nothing. Each is
    linked by hand to the nearest **named** Fraser mainstem segment fid
    (`08HB087` Comox Harbour left unmatched):
    - `08MH028` (Steveston) → fid `701651753`
    - `08MH044` (Whonock)   → fid `701649123`
    - `08MH126` (Port Mann) → fid `701651908`

- **The stream join key is the nearest-segment `fid`, resolved to `reach_id`
  at export time** (see the fid→reach_id section below — this replaced the
  earlier coarse `gnis_id` join). `gnis_id` is gone from `gauge_fwa_match`.
  Polygons/lakes still join by `waterbody_key`.

- **`export_hydro.py::build_stations_index()`** attaches an `fwa` object per
  station from the primary match: `{layer, fwa_id, reach_id, waterbody_key,
  distance_m, resolution}`. Stream links carry `fwa_id` + resolved `reach_id`
  (415 stations, all resolved), polygon links carry `waterbody_key`
  (34 stations); the app picks by `layer`. No-match stations stay `fwa: null`.

Run order to reproduce: `python -m pipeline --step atlas` →
`python live-data/hydro/match_fwa.py` → `python live-data/hydro/export_hydro.py`.
(`match_fwa` needs only the atlas pickle; the frequent hydro cron re-runs just
`export_hydro`, which re-resolves fid→reach_id against the current deploy
shards — so it stays in sync with independently-regenerated reaches.)

### DONE (2026-07) — link via nearest segment fid → reach_id

`gnis_id` was coarse: **one `gnis_id` spans multiple reaches** (a long river is
split into several reaches, and inherited-name side channels share the
mainstem's `gnis_id`), so a `gnis_id` join was ambiguous about *which* reach a
gauge sits on. **Now fixed — the stream join key is `fid` → `reach_id`:**

- `match_fwa.py` records the **nearest segment `fid`** as `fwa_id`. Because
  reaches are built only from open-air `streams` (never `under_lake_streams`),
  a gauge whose nearest segment is under a lake would never resolve — so when
  the primary match is `under_lake_streams`, the matcher swaps in the nearest
  open-air segment on the **same blue-line-key** (`build_openair_blk_index` /
  `_openair_fid`). That fid is in a reach. This is what took stream resolution
  from 405/415 → **415/415**.
- `export_hydro.py::_FidReachResolver` resolves `fid` → `reach_id` against the
  deploy shards the app already serves — `shards/v{N}/fids/{sha3}.json`, where
  `sha3 = sha256(fid)[:3]` (4096 buckets; `shard_prefix` in
  `pipeline/deploy/r2_sharder.py`). Only the ~390 buckets the gauge fids fall
  into are read, each cached: **~0.1 s for all 415** gauges. Path comes from
  `config.yaml` (`output.pipeline.deploy` + `shard_version`).
- **Why this keeps the frequent hydro cron seamless with regenerated reaches:**
  the gauge→`fid` match is stable (native FWA id, only changes with the atlas),
  while the volatile `fid`→`reach_id` half is resolved at export time against
  whatever shards are currently deployed. `reach_id` itself is *not* stored on
  the gauge — storing it would go stale every reach rebuild. The tidal Fraser
  overrides resolve the same way (all three share reach `f55259d0cc0b`).

## DONE (2026-07) — local cron harness + viewer

- **`serve.py --cron N`** drives the recurring job locally as a stand-in for the
  production cron. Each tick runs the real commands as subprocesses:
  `hydro_poc.py update --bc` (fetch head) → `export_hydro.py --scope realtime`
  (which re-resolves every gauge's `fid`→`reach_id` against the current deploy
  shards). Status is published at `GET /api/cron` (per-step ok/duration/message)
  and the exported artifacts at `GET /hydro/<file>` (e.g. `/hydro/stations.json`).
  The heavy gauge→`fid` `match_fwa` pass is opt-in — `--match-on-start` /
  `--match-every N` — since it only changes when the atlas rebuilds.
- **`index.html`** now draws a dashed **"now"** vertical line on the chart (inline
  Chart.js plugin), so the observed/forecast boundary is obvious.

### Timing note — `update` is network-bound, ~60–70 s *every* run (not just first)

`hydro_poc.py update --bc` is **not** a light head-poll: `fetch_bulk` and
`fetch_forecasts` fetch **per station** over an 8-worker pool, so `--bc`
(~450 stations × daily+unit params + forecast series) is ~1,825 outbound
requests → ~68 s wall each run. It scales with station count, so a smaller
`--stations …`/`--province` set is proportionally faster. Only the initial
`bootstrap` is heavier still (18 mo of daily means). To make the *frequent* cron
cheaper without touching the daily/history job, trim what it fetches — e.g.
`--no-series` (skip per-station forecast CSVs) and/or drop daily means from the
frequent tick (they belong to the daily job). `export_hydro` itself is cheap
(~2–6 s; the fid→reach resolution is ~0.1 s).

## Goal

For each hydrometric gauge in `hydro.db`, find the real FWA waterbody it sits
on, using **location + name** together. This first pass is a measurement
experiment: **how many gauges get a clean, unambiguous name match** among the
waterbodies physically near them. Don't over-engineer it yet — the point is to
see the straight-match rate before deciding whether name variations / fuzzy
logic are worth adding.

Background and prior findings live in
[FWA_MATCHING_NOTES.md](FWA_MATCHING_NOTES.md) — read it first. Key relevant
conclusions from that work:
- Gauge names follow ECCC's fixed `"<WATERBODY NAME> <QUALIFIER> <PLACE>"`
  convention (`NEAR` / `AT` / `ABOVE` / `BELOW`, plus a few edge cases). A
  regex split on the first qualifier keyword isolates the waterbody name for
  ~99% of stations.
- The WSC station-number prefix and FWA watershed *group* code correlate only
  ~70% — **not** a reliable identifier. Don't use it here. (The name+location
  approach below is the one to pursue.)

## Input data (already built — do not re-fetch)

`live-data/hydro/hydro.db` was fully regenerated. Relevant:
- `stations`: **450 rows, all with `lat`/`lon`** (WGS84 / EPSG:4326).
  `coord_source` records provenance: 448 `hydat`, 2 `report` (scraped from the
  per-station Wateroffice page). The earlier "2 stations with no location"
  gap is closed — every station now has a coordinate, so the location-based
  matching below applies to all 450.
- One caveat: `08HDX05` (Quinsam auxiliary) has coords + metadata but **0
  readings** (the real-time CSV endpoint serves nothing for it) — it'll still
  match spatially, just won't have a hydrograph.

### Pipeline state (context — the rest is done)

Everything upstream of this task is built and clean (see
[README.md](README.md)):
- **`hydro_poc.py`** `bootstrap` / `update` — roster (Real-time Search ∪ HYDAT
  real-time-active), HYDAT metadata sync, current-conditions percentiles,
  readings, forecasts.
- **`fetch_hydat.py`** — bulk HYDAT metadata + discovery backstop.
- **`export_hydro.py`** — shapes `hydro.db` into R2-ready JSON tiers
  (`stations.json` headline, `recent/<id>.json`, `history/<id>.json`).

This gauge→FWA match is the remaining piece: it produces the **link between a
gauge and the waterbody a user actually taps in the app**. That's its whole
purpose — so the output must ultimately land in the export, not just `hydro.db`
(see "Output" below).

## What to reuse (accessor-only — no raw geopandas/pyogrio, no direct GPKG reads)

- **FWA geometry + names**: load the cached **`FreshWaterAtlas`** pickle
  (`pipeline/atlas/freshwater_atlas.py`), path from `config.yaml`
  `output.pipeline.atlas` = `output/pipeline/atlas/atlas.pkl`
  (`FreshWaterAtlas.load(path)`). It exposes, all keyed by stable id with real
  geometry already in **EPSG:3005**:
  - `atlas.streams: Dict[fid, StreamRecord]` — `.display_name` (gnis_name),
    `.geometry` (LineString), `.blk`, `.waterbody_key`.
  - `atlas.lakes` / `.wetlands` / `.manmade: Dict[waterbody_key, PolygonRecord]`
    — `.display_name` (GNIS_NAME_1), `.geometry` (Polygon).
  - Also `atlas.under_lake_streams` — include in the stream search too (a gauge
    at a lake outlet can sit exactly on that boundary).
  The atlas is the right source: real per-feature geometry (so
  point-to-line / point-to-polygon distance and a 5km radius query both work
  directly via shapely), already built and fast to load, same atlas the
  production build trusts. If the pickle doesn't exist, build it once via the
  normal pipeline (or `FreshWaterAtlas(graph_path, gpkg_path)`), don't reach
  into the GPKG directly here.
- **Name normalization**: `pipeline/matching/base_entry_builder.py::normalize_name()`
  — unicode-fold, drop parentheticals, collapse whitespace, uppercase, and
  **keeps the LAKE/CREEK/RIVER type word** (so "Goose Lake" never collides with
  "Goose Creek"). Use it for BOTH sides of every comparison. Keeping the type
  word is exactly what makes the lake-vs-stream disambiguation below work.
- **Qualifier split**: reuse the same keyword list the notes file / earlier
  scratch work used — split the gauge `name` on the first whole-word occurrence
  of (longest-first) `UPSTREAM OF`, `DOWNSTREAM OF`, `ABOVE THE`, `BELOW THE`,
  `NEAR`, `AT`, `ABOVE`, `BELOW`. The part before it is the waterbody name
  (which still includes its type word, e.g. `"ATLIN LAKE"`, `"ADAMS RIVER"`).
- **Spatial radius / closest**: an STRtree over the atlas geometries (the atlas
  build and `pipeline/recurring/anglerinfo/waterbody_matcher.py` both already
  build one this way) for the 5km query; `pipeline/recurring/anglerinfo/`'s
  own `_closest()` is the precedent for distance ranking if you need a
  tiebreak.

## Algorithm (per gauge that has a location)

1. Reproject the gauge `lat`/`lon` (EPSG:4326) to **EPSG:3005** (`pyproj`
   Transformer, `always_xy=True`) — matches the atlas CRS.
2. **Find all unique waterbodies within a 5km radius** of the gauge point,
   across `streams` + `under_lake_streams` + `lakes` + `wetlands` + `manmade`.
   "Unique waterbody" = dedupe candidates by identity, not by segment:
   - polygons → by `waterbody_key`
   - streams → by `display_name` + `blk` (a named river is many segments;
     collapse to one candidate per name). Skip unnamed features (`display_name`
     empty) — they can't name-match anyway.
   Keep each unique candidate's normalized name + its layer/type.
3. Parse the gauge name → strip the qualifier → `normalize_name()` it →
   `gauge_wb_name` (e.g. `"ATLIN LAKE"`).
4. **Name match**: keep only candidates whose normalized name **appears in**
   `gauge_wb_name` (substring match, both normalized). Per the user: the
   waterbody name should exist within the qualifier-stripped gauge name.
   Because the type word is preserved on both sides, this naturally does the
   critical disambiguation:
   - gauge `"ATLIN LAKE"` matches the **lake polygon** "ATLIN LAKE", **not** a
     nearby stream "ATLIN RIVER" (RIVER ∉ "ATLIN LAKE").
   - gauge `"ADAMS RIVER"` matches the **stream** "ADAMS RIVER", not "ADAMS
     LAKE".
   Watch the "X Lake with X Creek flowing out" case explicitly in a test: when
   the gauge is the lake, only the `...LAKE` polygon should survive, not the
   `...CREEK`/`...RIVER` stream. Verify this holds.
5. The result per gauge is the set of **unique waterbodies with a matching
   name** within 5km. Record the count.

## Output / what to measure

Write results into `hydro.db` (new table, e.g. `gauge_fwa_match`) and/or a
summary print:
- Per gauge: `station_id`, parsed name, # unique waterbodies in 5km, # that
  name-matched, and the matched waterbody id(s) + layer (`streams` / `lakes` /
  `wetlands` / `manmade`) + `waterbody_key` (or stream fid/blk) + distance.
- **Headline number**: how many of the 450 gauges got **exactly one** name
  match (clean), how many got **zero**, how many got **>1** (ambiguous —
  needs a tiebreak, e.g. closest, or name variations).

### Then wire the clean matches into the export

Once the straight-match rate is good, surface the link so the app can go
**waterbody (tapped) → its gauge(s) → live readings/percentile/forecast**:
- Add the resolved reach key (and layer) to each station in
  `export_hydro.py::build_stations_index()` (an `fwa` field on the station
  entry in `stations.json`). Only emit confident (primary) matches; leave
  no-match ones null for review.
- **The reach key is per-layer**: streams join on the nearest segment `fid`,
  resolved to `reach_id` at export time against the deploy `fids` shards (see
  the fid→reach_id section above); polygons/lakes on `waterbody_key`. Don't
  invent a new id scheme.

## Then decide (don't build preemptively)

Only after seeing the straight-match rate:
- If many gauges get **zero** matches, add **name variations** — the
  generic-suffix handling in
  `pipeline/recurring/anglerinfo/waterbody_matcher.py` (`_name_variants`,
  `name_variant_index`, `_expand_lake_abbreviations`) is the ready precedent
  (e.g. gauge "ABBOTT" vs FWA "ABBOTT LAKE", or "L."→"LAKE" abbreviations).
- If many get **>1**, add a distance tiebreak (`_closest()`), keeping the
  type-word constraint from step 4 first.

## Guardrails carried over from earlier findings

- Keep the **5km cap** meaningful — the earlier prototype's bug was "closest
  wins" with no distance cap, which matched a Maple Ridge "Cedar Creek" to a
  Similkameen one 250km away. The radius query here structurally prevents that;
  don't reintroduce an uncapped nearest-match fallback.
- Everything through `FWADataAccessor` / the cached atlas — no direct GPKG
  reads, matching how every other matcher in this repo works.
