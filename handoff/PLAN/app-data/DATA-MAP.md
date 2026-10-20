# DATA-MAP — every data product, from source to screen (as of 2026-10-20)

PLANNING ONLY. Branch `redesign/stream-sections` @ 49c1f299. Read-only research. Nothing was built, regenerated or
edited. Sizes are `ls -l` / `dbstat` on the live `data/generated/` tree, or the audit's
`scratchpad/audit/03-size.md` numbers. File:line refs are at HEAD. Where a fact comes from a research report
and was not re-checked here, the text says **(per research)**.

Companion files:
- `MINIMAL-PACKAGING.md`: the target.
- `APP-PREP.md`: the tickets.
- `QUESTIONS.md`: the owner's calls.

---

## 0. The answer in five lines

1. **The app ships the whole bundle today.** At startup it downloads `bundle.sqlite` whole (102.7 MB, served as
   `/province.sqlite`), identity-encoded, on web and native alike. With it come `status_index.bin` (1.66 MB) and three
   small files. The two pmtiles archives (1.45 GB) are range-read. Every URL is `http://localhost:39217`
   (`app/apps/mobile/src/App.tsx:35-37,57,75,77`).
2. **About 70 of the bundle's 103 MB is never read by the app.** These are the rule, licensing and topology tables
   (`section_ruleset` alone is 42.3 MB).
3. **The regulations data the app will need is all in `regs.sqlite` (14.2 MB).** It reads nothing from the bundle at
   run time (`pipeline/deliver/store/decode.py:28` opens only the store). It is not wired into the app yet.
4. **No shipped artifact holds section geometry.** Geometry lives only in the tiles (feature id = `sid`). The
   prototype's map uses a separate projected-SVG export (`watermap.js`) that covers 3 waters.
5. **Middle-man data dominates.** The chain is: reach jsonl (20.4 GB) → bundle (103 MB) → verdicts (47 MB) → export
   JSON (6.9 MB) → answers JSON (38 MB) → regs.sqlite (14.2 MB). Only the last file and a slice of the bundle carry
   anything a user sees.

---

## 1. The transform chain

`★` marks a file the app reads today. `◇` marks a file the app is specified to read (integration T01/T03). `✕` marks a
middle-man: a file whose only readers are other generators, tests or the prototype.

```
data/source/*  (FWA, ECCC/HYDAT, Protomaps bc.pmtiles 4.36 GB, steelhead roster …)        data/curated/* (authored + reviewed)
      │                                                                                          │
      ▼                                                                                          ▼
 python -m pipeline.atlas.build --full   (atlas/build.py:320 main)                     corpus: entries/catalogue/region-*.json
  data/generated/atlas/full                                                                     │
   graph.pkl 614 MB · geometries.pkl 2.17 GB · waterbody_polys.pkl 763 MB                       │
   registry.json 115 MB · section_handles.txt 26 MB (sid = line no.)                            │
   blk_chains.pkl 2.57 GB ✕(no reader) · splits.resolved.json · region_home.json · area_catalog.gpkg
      │                                                                                          │
      ├──────────────► python -m pipeline.atlas.reach.cli  (reach/io.py:74-173 write_run) ◄─────┘
      │                 data/generated/reaches/full  ≈ 20.4 GB
      │                  rule_section.jsonl 20.1 GB ✕ · licensing_section.jsonl 136 MB ✕ · steelhead_presence 122 MB ✕
      │                  tidal/outside_bc/straddle/... ✕ · rule_extent.jsonl ✕(prototype only) · report.json (digests)
      │                          │
      │                          ▼
      │        python -m pipeline.deliver all   (deliver/__main__.py:113-140; each step a subprocess, in place)
      │         1 bundle        → bundle/bundle.sqlite 102.7 MB ★ (app reads ~18 of its tables)
      │         2 verdicts      → bundle/verdicts.sqlite 46.7 MB ✕ (never shipped)
      │         3 status_index  → bundle/status_index.bin 1.66 MB ★
      │         4 export        → regs/ui-rules-export.json 6.9 MB + ui-rules-guide.json 1.1 MB ✕
      │         5 answers       → regs/ui-rules-answers.json 38.0 MB (+ .gz 1.78 MB) ✕
      │         6 store         → bundle/regs.sqlite 14.2 MB ◇ (lossless answers + export subset + sid→akey/place)
      │         7 vintage.report(GENERATED.tiles, bundle): tiles↔bundle handles + registry only
      │
      └──────────────► python -m pipeline.deliver.tiles   (deliver/tiles/__main__.py:12-91)
                        tiles/layers/*.geojsonl ≈ 4.3 GB ✕(tippecanoe input; also read by water_map_export)
                        tiles/atlas.pmtiles 530.7 MB ★ (range) · tiles/atlas.meta.json 97 B ★
       python -m pipeline.deliver.tiles.basemap   (NOT in `pipeline build`; last run Sep 25)
                        tiles/basemap.pmtiles 918.4 MB ★ (range)
       python -m pipeline.deliver.tiles.boundary  (NOT in build) → data/source/bc_outside.geojson 62.5 KB ★ (redundant)
       python -m pipeline.gauges.feed.publish     (publish.py:654-733; every 30 min in prod)
                        gauges/feeds/ 432 files 20.9 MB: index.json ★, {station}.json ★ (lazy), stats.json ✕, clim.json 8.4 MB ✕(→ bundle gauge_clim)

PROTOTYPE / ORACLE CHAIN (nothing here ships)
 ui-rules-{export,guide,answers}.json → page_data.py → reference/page_v36.html 5.3 MB (tracked)
   → app/design/regs-v11/tools/snap.mjs → v36snap.js 15.4 MB → oracle.json, regs-diagram/data.js
 bundle + status_index + answers + export + tiles/layers → tools/water_map_export.py → regs-v11/watermap.js 288 KB
 regs.sqlite + the prototype (headless Chrome) → regs-v11/port/sync.mjs → port/golden/ ≈ 10.9 MB (golden 8.5 MB, hydrate 1.3,
   taps 0.47, tokens 0.09, inventory 0.06)
```

`python -m pipeline build` (`pipeline/build.py:38,138-146`) runs these stages in order: reach → tiles → deliver. Each
stage is skipped when it is up to date. The strict vintage check runs at `build.py:268-273`. The atlas is rebuilt only
with `--atlas`, and the basemap and mask never.

---

## 2. Product by product

**"Ships today"** was checked in the app code.
- **Web** is Expo web built from `apps/mobile`. `apps/web` holds no code.
- **Native** has the same fetches, but its map is a placeholder: `packages/map/src/Map.native.tsx:16-26`. Native
  therefore range-reads no tiles today.

### 2.1 Atlas build: `data/generated/atlas/full`

| | |
|---|---|
| producer | `python -m pipeline.atlas.build --full --out <dir>_next` (`pipeline/atlas/build.py:320` main; pickles written at :987–992); `pipeline.atlas.promote`; sidecars from `pipeline.atlas.sidecars` |
| size | graph.pkl 614 MB, geometries.pkl 2.17 GB, waterbody_polys.pkl 763 MB, blk_chains.pkl 2.57 GB, registry.json 115 MB, section_handles.txt 26 MB (1,957,202 lines), splits.resolved.json 1.4 MB, area_catalog.gpkg 38 MB |
| consumers | reach run, bundle build (graph, geometries for `place_water` and gauges, registry → `item`), tiles export (`export.py:138`), curation app, DFO tools |
| app | none directly. Only the sid space reaches the app, through tile feature ids and the status index. |
| duplicates | `registry.json` items reappear as bundle `item` / `alias` / `item_section`, and again as regs `item` |
| middle-man? | No: it is the source. **Dead output:** `blk_chains.pkl` (2.57 GB) has no run-time reader; only a determinism hash in `test_no_slivers` reads it (per research). |

**IDs.**
- `sid` is the 1-based line number in `section_handles.txt`, sorted by blk then measure, with lakes last.
- `digest_for` = sha256[:16] = `d768d6e135e62b99`. It is stamped into reach `report.json`, bundle `meta.section_handles`,
  `atlas.meta.json`, the status index header and regs `meta`.
- `sid` is **not durable across atlas builds** (AGENTS 5: 94%).
- `item_id` is durable. `item.ord` is the bundle's insertion rowid and is **not durable** (per research A3).

### 2.2 Reach run: `data/generated/reaches/full`

| | |
|---|---|
| producer | `python -m pipeline.atlas.reach.cli --build … --out …`; `reach/io.py:74-173 write_run` |
| size | ≈ 20.4 GB, of which `rule_section.jsonl` is 20.1 GB (one row per rule × section) |
| consumers | bundle only (`rules.py intern_sets` → `section_ruleset` / `ruleset`; `licensing.py`; steelhead, tidal, outside_bc, straddle). `rule_extent.jsonl` → `water_map_export` only. Diagnostics have no reader (human audit). |
| app | none |
| duplicates | `rule_section.jsonl` is the fully expanded form of the bundle's interned rulesets |
| middle-man? | **Yes, all of it.** Every jsonl is input to the bundle. `report.json` carries the pairing digests. |

### 2.3 Tiles: `atlas.pmtiles`, `atlas.meta.json`, `layers/*.geojsonl`

| | |
|---|---|
| producer | `python -m pipeline.deliver.tiles` (`deliver/tiles/__main__.py:12-91`). One writer, `export._writer` (`export.py:29-50`), emits GeoJSON in EPSG:4326 at 6 dp. tippecanoe runs in two bands (z4–8, z9–14) joined with tile-join, using `--simplification=10 --detect-shared-borders --no-feature-limit --no-tile-size-limit`. Sidecar at `__main__.py:111-127`. |
| size | atlas.pmtiles 530,738,094 B (z14 is 58%); atlas.meta.json 97 B; layers ≈ 4.3 GB |
| feature properties | `stream {section_id, name?, ord}`, `lake {section_id, name, water, area_m2}`, `wetland {section_id, name, water}`, `under_lake {}`, park/eco_reserve/wma/indigenous_land/no_access/sign_zone/watershed `{area_id, name, …}`, `mu {mu_id}`, `region` (18 features: **9 carry no `region_id`**, a bug), `parcel {}`, `basin {basin_id}`. **No `item_id`** (removed 2026-10-03). **No topology.** No top-level feature id: the app sets `promoteId: section_id` (per research D). |
| consumers | app map (web): `packages/map/src/runtime-style.ts`, `Map.web.tsx`. Feature-state carries `status` (from the status index), `standing` (conditions) and `selected`. `querySourceFeatures` is used to frame a pick (`Map.web.tsx:501`). The layers are also read by `water_map_export.py:707`, `runtiming/generate/index.py:135` and tests. |
| ships today | **web: range reads** of atlas + basemap. `atlas.meta.json` is fetched whole for `useVintage` (`ui/src/hooks.ts:483`). Native: the meta file only (no map). |
| duplicates | the sid ↔ geometry binding, which exists only here (good) |
| gap | `atlas.meta.json` is not bound to the pmtiles bytes (audit F6), so swapping the archive under an old sidecar passes every check |

### 2.4 Basemap: `basemap.pmtiles`

The producer is `python -m pipeline.deliver.tiles.basemap`: a `pmtiles extract` of `data/source/bc.pmtiles` (BC + 5 km,
z ≤ 14), then a tile-join keeping earth/water/landcover/landuse/roads/places/boundaries. It is **not a stage of
`pipeline build`**. It is 918,378,038 B, range-read on web. Landuse is 61.7% of it, and kinds the style never draws are
about 12% (audit §3.1 #3). The glyphs and sprites come from the Protomaps CDN, so a shipped app needs its own copy.

### 2.5 Mask: `bc_outside.geojson`

The producer is `python -m pipeline.deliver.tiles.boundary` (not in the build). It is 62,563 B, fetched whole
(`App.tsx:37`) and added as an `outside` fill. **It is redundant:** the style's `outside-bc` background layer already is
the mask (per research D). This is a middle-man fetch that can go.

### 2.6 `bundle.sqlite`

| | |
|---|---|
| producer | `python -m pipeline.deliver bundle`. Writers: `pipeline/deliver/bundle/build.py` (meta :940, item :217, alias :240, item_section :261, section_touch :378, place :426, place_water :498, gauges :582–759 incl. section_down :759), `rules.py` (entry, rule, ruleset, section_ruleset :1354, tidal, outside_bc, straddle, rule_key, section_home, steelhead_*), `licensing.py`, `derived.py` (rule_ix, part, part_section, region_place, mu_set, unnamed_part), `spans.py`, `nearer.py` (item_down, trib_nearer) |
| size | 102,703,104 B, 54 tables |
| ships today | **Yes, whole, at every launch**, on web and native. `engine.web.ts:66-85` / `engine.native.ts:59-87` fetch then `arrayBuffer`. The checks are `res.ok`, `Content-Length` (`engine.web.ts:73`, `engine.native.ts:66`) and the SQLite magic. There is no digest, no format version (audit F3) and no cache. It blocks the Shell. |
| app reads | `meta`, `item`, `alias`, `item_section`, `place`, `place_water` 4.9 MB, `entry` (EXISTS only), `gauge`, `gauge_clim` 4.1 MB, `section_gauge` 2.5 MB, `section_panel` 5.0 MB, `panel_member`, `basin_member`, `lake_gauge`, `section_down` 1.3 MB, plus `chart` / `release` / `stock_water`, which are **empty** (0 rows; nothing writes them, `build.py:902-905`) |
| app never reads (≈ 70 MB) | `section_ruleset` 42.3 MB (1.96 M rows, 5,865 distinct tuples), `ruleset` + `ruleset_by_entry` 19 MB, `section_licensing` 7.8 MB, `rule`, `rule_ix`, `rule_key`, licensing tables, `split`, `section_span`, `span_end`, `section_touch`, `item_down`, `trib_nearer`, `part`, `part_section`, `unnamed_part`, `steelhead_*`, `province_except`, `mu_set`, `outside_bc`, `tidal`, `section_straddle`, `section_home`, `region_place`, `gauge_stats`, `item_alias` |
| pipeline readers of those | `status_index.py` (section_ruleset, tidal, outside_bc, item_section, :209 verdicts); store build (item, part, part_section, unnamed_part, region_place, section_ruleset.up/place); export/answers (ruleset, rule, rule_key, licensing); `water_map_export` (section_down, part, part_section) |
| geometry | **none per section, by design** (`schema.sql:8-13`: "geometry → the TILES"). Only points (place, gauge, areas) and 1-D measures (`section_span` lo_m/hi_m, `split.at`). |
| topology | `section_down` (sid → down_sid) exists **only inside gauge sheds** (108,363 rows) and is what the app's `traceToGauge` walks. `item_down` (first named water downstream), `trib_nearer`, `section_touch`, `part.rep_sid` are pipeline-only. Nothing describes upstream. |
| duplicates | its rule/licensing tables are re-encoded into verdicts, export, answers and regs. Its `item` / `place` overlap regs `item` / `place`. |
| middle-man? | **Mostly.** It is the pipeline's internal database (rule 56: "the bundle is the only source") and ships whole for lack of a packaging step. |

### 2.7 `verdicts.sqlite`

The producer is `python -m pipeline.deliver verdicts`. It is 46.7 MB. Its readers are `status_index.py:209`,
`export_ui_rules.py:5648` and `answers.py:155/:421`. **It is never shipped. Middle-man** (by design, AGENTS 56).

### 2.8 `status_index.bin` (BCSI v2)

The producer is `python -m pipeline.deliver status_index` (`pipeline/deliver/status_index.py`). It is 1,656,954 B
(361 KB br).
- **Layout:** the header is magic + version 2 + `section_handles` + `reach_digest`, then 366 day-profiles. Each sid gets
  one of the codes base/own/closed/tidal/outside.
- **Ships today:** fetched whole, identity-encoded. The URL is derived from the atlas URL (`ui/src/hooks.ts:507-545`).
  It is decoded by `core/src/statusIndex.ts` and refused on a handles/reach mismatch, after which the map paints flow
  colours anyway (`Shell.tsx:264-267`, audit 02#8).
- **Consumer:** map status colouring (feature-state by sid).
- **Duplicates:** it is a projection of verdicts + `section_ruleset`. regs.sqlite's display frames carry the same
  open/closed facts per answer key, at a coarser grain.
- **Middle-man?** No: a user-facing product. It is a separate file only for historical reasons.

### 2.9 `ui-rules-export.json` + `ui-rules-guide.json`

The producer is `pipeline/tools/export_ui_rules.py` (6.5 k lines, encoded through `export_codec.py`). The files are
6,886,949 B + 1,075,250 B.
- **Consumers:** answers, store (`store/common.py:170-186` copies a subset), `page_data.py`, `water_map_export`, and
  about 35 tests.
- **Ships:** nothing reads them. **Middle-man.** The words the app needs — rule/licence texts, entries, species, conduct,
  rulesets — are already inside regs.sqlite (`rule`, `lic`, `entry`, `ruleset`, `lset`, `static export.*`).

### 2.10 `ui-rules-answers.json` (+ `.gz`)

The producer is `python -m pipeline.deliver answers` (`pipeline/deliver/answers/`). It is 38,032,637 B; the `.gz` is
1,777,825 B.
- **Consumers:** store build (which refuses unless it decodes byte-identical, `store/build.py:203-206`),
  `page_data.py:71`, `water_map_export.py:668`.
- **Ships:** no. **Middle-man**, kept as the human-readable form and the prototype's input. Its sha (`925ce67f…`) is
  pinned in regs `meta.answers_sha256`.

### 2.11 `regs.sqlite` (regs-store/4)

| | |
|---|---|
| producer | `python -m pipeline.deliver store` (`pipeline/deliver/store/build.py`, atomic `os.replace` :212; `schema.sql`) |
| size | 14,237,696 B (zdict blobs); br11 4.89 MB. Biggest tables: `section_akey` 4.17 MB, `section_place` 2.21 MB (plain u16/u8 blocks; deflating them gives −33% on disk), `rows_row` 1.29, `part` 0.91, `item` 0.80, `zdict` 0.62, `str` 0.62 MB |
| contents | (1) answers/2, losslessly; (2) an export subset (`rule`, `lic`, `entry`, `ruleset`, `lset`, `static`); (3) `section_akey` (sid → answer key, 8 KB blocks of 4,096 u16; 0 = outside B.C.) and `section_place` (sid → place). `meta`: format, `section_handles` d768…, `reach_digest` cad1…, `rule_ids_sha256` 16be…, `store_digest`, `answers_sha256`, `layout`, `counts` (akeys 2,268, cells 9,175, items 19,641, parts 20,461). |
| at read time | **needs nothing from bundle.sqlite.** `decode.py:28` opens only the store. The only shared key is `sid`, the same space as tile feature ids and the status index. |
| consumers | the port tooling (`port/taps.py`, `verify_map.py`). App: none yet. Integration T01–T04 specify it as a second file beside `/province.sqlite`. |
| does not hold | the part → sid list (only sid → akey). `part` has a `sections` count, not the list. Highlighting a part on the map therefore needs the bundle's `part_section` (63,263 rows) or an equivalent. |

### 2.12 Gauge data

| product | producer | size | app |
|---|---|---|---|
| `data/curated/gauges/matches.json` (REVIEWED) | `pipeline.gauges.generate.match` / `promote` (human-run) | 1.3 MB | none. Read by the atlas build (gauge cuts) and the bundle build. **Many comments still cite the retired `pipeline/gauge_match.json`** (`bundle/build.py:541`, `atlas/build.py:688`, `check_curated.py:8,112` fallback); memory note `gauge-match-one-frozen-file` is stale on the path. |
| bundle `gauge`, `section_gauge`, `lake_gauge`, `section_panel`, `panel_member`, `basin_member`, `section_down`, `gauge_clim` | bundle build :582–759 | ≈ 14 MB together | **read** (Conditions, DonorPanel, SpotCapture, map standing colours, charts) |
| `feeds/index.json` | `pipeline.gauges.feed.publish` (`publish.py:654-733`) | 157 KB, 425 stations | **fetched at startup** (5-min TTL) |
| `feeds/{station}.json` | same | 8–38 KB each | **lazy**, per panel |
| `feeds/stats.json` | same | 185 KB | never fetched. **Dead output.** |
| `feeds/clim.json` | same | 8.4 MB | not fetched. It becomes bundle `gauge_clim`. Middle-man. |

Missing `fetchedAt` is shown as live: `packages/data/src/feed/http.ts:214` (`stamp = s ? Date.parse(s) : Date.now()`),
and the same at `bundle/source.ts:393`. `core` `freshness()` is never called.

### 2.13 Stocking and bathymetry

The bundle tables `stock_water`, `stock_code`, `release` and `chart` hold **0 rows**, and nothing writes them. The
app's queries exist but return empty, so the Layers sheet's "Stocked" mode has no producer. Only the audit tools exist.

### 2.14 Fish assets and fonts

- **Fish:** `app/apps/mobile/assets/fish/`: 39 WebP at 800×400 plus `manifest.json`, 1.53 MB in the binary, added by hand
  (eefa976c). No app code loads them yet. Permissions are pending (integration T43).
- **Fonts:** 7 TTFs from `@expo-google-fonts` (779 KB) in the binary. Map glyphs come from the Protomaps CDN.

### 2.15 The prototype and oracle chain (none of it ships)

| file | producer | size | readers | status |
|---|---|---|---|---|
| `pipeline/deliver/answers/reference/page_v36.{html,js}` | `page_data.py` (from the three regs JSON files) | 5.3 MB html, js 180 KB | snap.mjs, `port/hydrate.mjs` | stamped, no `--check` (audit F8) |
| `app/design/regs-v11/v36snap.js` | `regs-v11/tools/snap.mjs` | 15.4 MB | regs-v11.html, check.mjs | unstamped |
| `regs-v11/oracle.json`, `regs-diagram/data.js` | oracle.mjs / build.mjs | — | check.mjs / the diagram | unstamped; data.js lags |
| `regs-v11/watermap.js` | `pipeline/tools/water_map_export.py` | 288 KB, **3 waters** | regs-v11 map overlay | stamped. Projected SVG paths, Douglas–Peucker; a geometry copy outside the tiles. |
| `regs-v11/port/golden/*` | `port/sync.mjs` (golden.mjs, tokens.mjs, inventory.mjs, taps.py, hydrate.mjs) | ≈ 10.9 MB | integration tests (planned) | the port's expected strings |

All five are **design/test artifacts**. They are legitimate as tests, but none may become an app input. The two that
must die with the port are `v36snap.js` and `watermap.js`: the app reads the store and the tiles instead.

---

## 3. What the app fetches today, side by side

| # | URL (`App.tsx`) | web | native | when | checks |
|---|---|---|---|---|---|
| 1 | `/province.sqlite` = bundle.sqlite | whole, sql.js | whole, `deserializeDatabaseAsync` | startup, blocking | Content-Length, magic |
| 2 | `/sql-wasm…wasm` | 0.66 MB | — | startup | — |
| 3 | `/feeds/live/index.json` | ✓ | ✓ | startup, 5-min TTL | none (`fetchedAt` fabricated) |
| 4 | `/atlas.meta.json` | ✓ | ✓ | startup | vintage vs bundle handles |
| 5 | `/status_index.bin` | whole | whole | startup | handles + reach_digest |
| 6 | `/atlas.pmtiles`, `/basemap.pmtiles` | Range | — (no native map) | per view | none |
| 7 | `/bc_outside.geojson` | ✓ | — | startup | none (redundant) |
| 8 | Protomaps glyphs/sprites, EOX satellite | CDN | — | lazy | — |
| 9 | `/feeds/live/{station}.json` | lazy | lazy | per panel | none |
| 10 | Open-Meteo | lazy | lazy | spot capture | — |
| — | `/regs.sqlite` | **no route** (`serve-tiles.mjs`) | — | — | — |

Cold start is 105,081,128 B on the wire (audit 03 §1).

---

## 4. Middle-man summary

| middle-man | bytes | why it exists | fate in the target (MINIMAL-PACKAGING) |
|---|---:|---|---|
| reach jsonl (rule_section …) | 20.4 GB | reach → bundle hand-off | stays pipeline-internal (out of scope; never shipped) |
| `blk_chains.pkl` | 2.57 GB | none at run time | delete the output (pipeline cleanup, not app) |
| bundle rule/licensing/topology tables | ≈ 70 MB | answers/status/export inputs | stay in the bundle; **never ship** |
| `verdicts.sqlite` | 46.7 MB | the traced reader, once | stays pipeline-internal |
| `ui-rules-export.json` + guide | 8 MB | answers input, the export subset, tests | not shipped; later folded into the answers step (M6) |
| `ui-rules-answers.json` | 38 MB | store input, prototype input | not shipped; becomes an optional debug dump (M6) |
| `regs.sqlite` as a separate file | 14.2 MB | the store | **merged into the one app package** |
| `status_index.bin` as a separate file | 1.66 MB | map colours | **a blob inside the app package** |
| `atlas.meta.json` | 97 B | vintage pairing | **replaced by the manifest** |
| `bc_outside.geojson` | 63 KB | the old mask | **dropped** (style background is the mask) |
| `feeds/stats.json`, `feeds/clim.json` | 8.6 MB | clim → bundle | `stats.json` dropped; `clim.json` stays a bundle input, not served |
| `watermap.js`, `v36snap.js`, oracle, regs-diagram | ≈ 16 MB | prototype | dev/test only, until the port lands |
