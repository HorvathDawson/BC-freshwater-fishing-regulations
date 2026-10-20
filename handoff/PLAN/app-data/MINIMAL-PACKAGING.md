# MINIMAL-PACKAGING — the target: one bundling step, three shipped files

PLANNING ONLY. Builds on `DATA-MAP.md` and on the audit's measured sizes (`scratchpad/audit/03-size.md`, also
`handoff/PLAN/AUDIT-2026-10-20.md` §3). It supersedes the 2026-10-10 "unified app data layout" (atlas.sqlite +
regs.sqlite + climate.sqlite + index; see `~/.claude/…/handoff/PLAN/APP-REGS-INTEGRATION.md`). That layout used four
files and joined them in TypeScript; the owner asked for the fewest artifacts and transforms. Figures marked **est.**
are extrapolations, not measurements.

---

## 1. The recommendation in one table

`python -m pipeline.deliver all` ends with a new step, **`package`**. It writes `data/generated/dist/<vintage>/`, and
that directory is the whole production delivery, the same for the web app and the mobile app:

| # | file | what | size on device | on the wire | cadence |
|---|---|---|---:|---:|---|
| 1 | `manifest.json` | the only mutable URL: format, vintage digests, each file's url / decoded bytes / sha256, the regulations year | < 2 KB | < 1 KB | every publish |
| 2 | `app-<sha16>.sqlite` | **the one app data package** (`app-pack/1`): waters, search, gauges + climate, panels, the status index (as a blob), the regulations store, and the per-water map topology | **≈ 41 MB est.** | **≈ 13 MB br est.** (≈ 16 MB gzip) | every mill run |
| 3 | `atlas-<handles16>.pmtiles` | ALL water geometry (sections keyed by `sid`), areas, MUs, basins | 530.7 MB (≈ 295 MB est. after the z13 overzoom, audit §3.1 #2) | range reads only | every atlas build |
| — | `basemap-<v>.pmtiles` | roads/places/landcover (Protomaps) | 918 MB (≈ 807 MB est. after the landuse filter) | range reads | rarely (not a build stage) |
| — | `feeds/live/…` | live gauge readings and forecasts | — | index 157 KB + 8–38 KB per station | every 30 min (a separate publisher, not a package) |

**Today** the app downloads the 102.7 MB bundle, the 1.66 MB status index, `atlas.meta.json` and the mask, all
identity-encoded: 105 MB on every launch. Regulations are not even included yet. **Target:** about 13 MB once per
vintage, cached on native, regulations included.

**What goes away from the shipped set:**
- `bundle.sqlite` as a download;
- `status_index.bin` and `atlas.meta.json` as separate files;
- `bc_outside.geojson`;
- the planned separate `/regs.sqlite`.

**What never ships:** `ui-rules-answers.json`, `ui-rules-export.json` / `guide`, `verdicts.sqlite`,
`v36snap.js` / `watermap.js` / the oracle. These were never shipped, and the plan makes sure they never start.

Inside the app binary, not data: fonts (0.78 MB, or 0.35 MB subset), fish WebP (1.52 MB), map glyphs/sprites (a copy
of the Protomaps CDN assets, needed for a shipped app), and the sql.js wasm (web build asset).

---

## 2. The app package `app-pack/1`

One SQLite file. **Per-water data inside it is keyed by `item.ord` and `sid`. Both are valid only inside one vintage,
which is why the package and the atlas are paired by digest (§5).** Neither id is ever saved by the app: spots and
deep links bind to `item_id` (AGENTS 5).

| group | tables | from | bytes raw (measured unless est.) |
|---|---|---|---:|
| **meta** | `meta` (one table; keys namespaced: `pack.*`, `bundle.*`, `regs.*`) | packager | — |
| **waters + search** | `item`, `alias`, `item_section` (WITHOUT ROWID), `place`, `place_water` | bundle | E6 slice |
| **gauges + conditions** | `gauge`, `gauge_clim` (float16), `section_gauge`, `lake_gauge`, `section_panel`, `panel_member`, `basin_member`, `section_down` | bundle | E6 slice |
| (subtotal: the audit's **E6** client slice) | | | **26.6 MB** (br11 6.3 MB) |
| **map colours** | `status_index` (one row: the BCSI v2 bytes unchanged, so `core/statusIndex.ts` decodes it as today) | status index step | 1.66 MB (0.36 MB br) |
| **regulations** | the regs-store/4 tables under a `regs_` prefix: `regs_str`, `regs_static`, `regs_akey`, `regs_cell`, frames, `regs_item`, `regs_part`, `regs_picker`, `regs_section_akey`, `regs_section_place`, `regs_rule`, `regs_lic`, `regs_entry`, `regs_ruleset`, `regs_lset`, `regs_zdict`, … | store step | 14.2 MB as built; **9.5 MB** with the two block tables deflated (audit §3.1 smaller wins), ≈ 5.1 MB br |
| **map topology** (new, §4) | `part_section` (ord, sid, part_ix), `part_geo`, `part_mark`, `water_exit` | bundle (new columns/tables) | part_section 0.77 MB measured; the other three ≈ 2.5 MB **est.** |
| **dropped** | `entry` (replace its one EXISTS use with an `item.has_rows` flag), `chart` / `release` / `stock_water` (0 rows; return when a producer exists), every rule/licensing/topology table in DATA-MAP §2.6 | — | −76 MB |

**Total ≈ 41 MB on device, ≈ 13 MB brotli-11 (est.: the sum of the measured parts; one file compresses about as well
as the parts).**

### 2.1 Why the trimmed bundle can rely on `regs.sqlite`

The audit made the E6 trim conditional on "the regs integration reads `regs.sqlite`, never `section_ruleset`"
(owner decision §7 #4). It does, and here is why that is safe:

1. **Everything the Regulations page draws is in the store.**
   - The store is answers/2 re-encoded losslessly. The build refuses unless the decode is byte-identical,
     `pipeline/deliver/store/build.py:203-206`, and it pins `meta.answers_sha256`.
   - It also holds the export subset of rule, licence, entry, ruleset, species and conduct words.
   - The integration spec reads only the store: `handoff/PLAN/app-integration/contracts.md`, tickets T03–T09.
2. **The store's only link to the rest of the data is `sid`.** `regs_section_akey` maps every sid to an answer key,
   which covers every section in B.C., unnamed water included (S1). `sid` is the same id as the tile feature id
   (`promoteId: section_id`) and the status index key. The rulesets have already been decided into frames, so the app
   never needs `section_ruleset` / `ruleset` / `section_licensing`. Those tables exist so the pipeline can *compute*
   answers; nothing on a screen reads them.
3. **The store needs nothing from the bundle at read time.** `decode.py:28` opens only the store. Its `item_ord` space
   is the bundle's today: 19,641 of 19,641 regs items have the same ord as the bundle's `item` (checked 2026-10-20 by
   joining on `item_id`). The packager asserts this instead of relying on it.

**The one thing the store lacks is part → sids.** It has only sid → akey, and an akey is shared by many waters. The map
needs to highlight a part. That is `part_section` (63,263 rows, 0.77 MB), which the package carries.

### 2.2 Why ONE file, not app + regs as two

- **For one file:**
  - one sha, one download, one atomic swap and one pairing check;
  - the regs tables cannot drift from the app tables (today that is a three-digest handshake, T03);
  - both platforms open the same thing;
  - the integration's `RegsStore` simply opens over the same `Db`.
- **Against:**
  - a regulations-only fix re-downloads about 13 MB instead of about 5 MB;
  - web parses the regs tables before first paint.
  - Neither matters much. Every mill run rebuilds both halves together (the bundle and the store are one `deliver all`).
    sql.js `deserialize` of a 41 MB file costs one copy, which T03/D9 already measure.
- **If web first paint measures badly:** `regs_*` moves to a second file with the same manifest mechanism (Q2). That is
  a packager flag, not a redesign.

### 2.3 Lazy loading per region or per section: not now

- **Per region:** the sid-keyed tables cut per region are R6 28% of the whole and a median of about 1.3 MB zstd (audit
  §2b). Lazy regions would save about 8 MB br for a one-region user. The price is a fetch whenever search or a pan
  crosses a region, a sharded manifest, and a cross-region search index.
- **Per section:** the frames are already deduplicated per answer key (2,268 keys), so per-section loading buys nothing.
- **Decision:** revisit only if the measured first-run on a mid phone over 4G is over budget (Q3).

---

## 3. Delivery

### 3.1 The manifest, not `Content-Length`

```json
{
  "format": "app-manifest/1",
  "vintage": { "section_handles": "d768d6e135e62b99", "registry_digest": "8c77ba1cd2dc3929",
               "reach_digest": "cad1c21fe6eb9ba9", "rule_ids_sha256": "16be720be08b0fca" },
  "package": { "url": "app-2753a73afa9f20b9.sqlite", "format": "app-pack/1", "schema_sha256": "…",
               "bytes": 41000000, "sha256": "…" },
  "atlas":   { "url": "atlas-d768d6e135e62b99.pmtiles", "bytes": 530738094, "sha256": "…" },
  "basemap": { "url": "basemap-2025-09-25.pmtiles", "bytes": 918378038, "sha256": "…" },
  "feeds":   { "url": "feeds/live/", "format": "feed/1" },
  "regulations": { "year": "2025-2027", "valid_until": "2027-03-31" }
}
```

- **Integrity:** the app verifies **decoded bytes and sha256** against the manifest. The `Content-Length` checks in
  `engine.web.ts:73` and `engine.native.ts:66` go: with `Content-Encoding` that header is the compressed size (audit
  top-10 #7). One shared `fetchVerified(url, {bytes, sha256})` replaces the duplicated bodies of about 25 lines.
- **Encoding:** the package is served with `Content-Encoding: br`, falling back to `gzip`. Browsers and iOS decode both.
  Android OkHttp decodes only gzip unless a brotli interceptor is added. **Default:** br on web, gzip on Android until
  the interceptor lands (16 MB vs 13 MB; Q4). zstd is not used (no RN support).
- **Caching:** digest-named URLs are immutable (`Cache-Control: public, max-age=31536000, immutable`), and
  `manifest.json` gets `max-age=300`. **No clocks in the manifest** (AGENTS 19): it is a pure function of its inputs.
- **Host:** R2, as the code comments say. No upload script exists; ticket P10 adds `publish` (human-run).
- **Tiles:** stay HTTP Range reads of gzip MVT tiles (MapLibre Native has no other tile codec, audit §3.2).

### 3.2 Versioning and the vintage pairing

- **`pack.format = app-pack/1`, `pack.schema_sha256`** = sha of the package DDL. The DDL is generated into
  `app/packages/data/src/pack.generated.ts` by `python -m pipeline.tools.emit_pack --check` (AGENTS 40), so a schema
  edit fails CI on both sides. The app refuses a format it does not know.
- **Regs section versions** stay where T03 reads them (`regs.layout.versions`, `glossary.version`). T03's refusal logic
  is unchanged.
- **The pairing:** the manifest names the atlas by its `section_handles` digest, and the package carries the same digest
  in `meta`.
  - The app refuses to colour or highlight when the package's `bundle.section_handles` ≠ the atlas archive's.
  - The atlas archive's own metadata JSON gets `section_handles` + `registry_digest` written at tile build. This closes
    audit F6: today `atlas.meta.json` is not bound to the pmtiles bytes.
  - With that, `atlas.meta.json` and `useVintage`'s extra fetch go.
  - The flow colours must also wait for `ok === true` (audit 02#8, `Shell.tsx:264-267`).
- **One promote:** `deliver all` builds into an `--out-dir` and promotes the six pipeline files plus `dist/` only after
  every step passes, including the **card gate** (P05) and the strict vintage check over ALL artifacts
  (`vintage.report_all`). Today `deliver/__main__.py:118-135` overwrites in place, and the end check covers tiles↔bundle
  only.

### 3.3 Offline and caching on native

- **Startup:** fetch `manifest.json` with a 5 s timeout and an `AbortController`.
  - Unreachable: open the **last good package** from `FileSystem.documentDirectory/pack/`, show its age, and keep the
    tiles' HTTP cache.
  - Changed sha: download in the background to `app-<sha>.sqlite.part`, verify the sha, `rename` to the final name,
    then open it **at the next launch, or at a screen boundary when no sheet is open**.
  - Keep the previous package until the new one has opened and passed its format and vintage checks, then delete it.
  - The atlas URL changes in the same manifest, so a cached old tile never meets a new package.
- **Open from disk, not `deserializeDatabaseAsync`.** expo-sqlite opens a file in a given directory (SDK 51+
  `openDatabaseAsync(name, options, directory)`; **NOT VERIFIED on this project's Expo version** — the P11 spike checks
  it). Native memory then holds pages, not 41 MB.
- **Tiles offline:** not in this plan. The native map is unbuilt (`Map.native.tsx:16-26`). The regs page and Conditions
  history work offline from the package; the map needs the network (or MapLibre's ambient cache). Per-region offline
  pmtiles extracts are an owner call (Q5).
- **Expiry:** `regulations.valid_until` is shown when passed. Today `validUntil` is never shown (audit 02#8).

### 3.4 Web

- `manifest.json` → Cache Storage keyed by the immutable URL → sql.js `new SQL.Database(bytes)`.
- The pmtiles are range-read as today.
- Memory is about 41 MB in the wasm heap, against about 103 MB today. P11 measures the first open on a phone browser.
- A range-read VFS is not needed at this size.

---

## 4. Geometry only from the tiles: highlight, framing, up/downstream

**Rule: the app stores no section geometry.** Lines and polygons come from `atlas.pmtiles` (feature id = `sid`). The
package carries only **topology and camera hints**: a handful of numbers per part, computed once by the pipeline, which
already loads `geometries.pkl` in the bundle build.

| need | how | data |
|---|---|---|
| highlight the part being read | feature-state `selected` on stream/lake/wetland features whose id ∈ the part's sids | `part_section(ord, sid, part_ix)` |
| frame the part / the whole water | `fitBounds` | `part_geo.w,s,e,n` (lon/lat, 4 × f32); the whole water = the union (computed at pack time into `item_geo`) |
| status colours on the water map | as the main map | `status_index` |
| ▼ Downstream / ▲ Upstream | step along the water's **part graph**, mouth first. Side channels and lake splits are why it is a graph, not i ± 1 (research C). | `part_geo.order` (mouth-first rank) + `part_mark(ord, mark_ix, lon, lat, label_s, down_part, up_parts)`. The marks are the boundaries where two parts touch within 60 m, each with its split label ("Vedder Crossing"), as `water_map_export.py` computes them today. |
| leave the water downstream (mouth → the receiving water) | "Flows into the Fraser ›" opens that water on the part it enters | `water_exit(ord, down_item_ord, down_part_ix)`, from `item_down` + `section_down` |
| "Conditions here" | open Conditions on the section at the part's midpoint, with weather at that point | `part_geo.mid_sid`, `mid_lon`, `mid_lat` |
| part labels, short labels, hints, km | already in the store | `regs_part.label_s/short_s/hint_s/km` |

**The midpoint is defined in the pipeline.**
- **Stream part:** the sid that contains the point at half the part's length, measured along its main stem (by `route
  measure` where `section_span` has it, otherwise by summed section length in mouth-first order). `mid_lon/mid_lat` is
  that point.
- **Lake part:** `mid_sid` is the part's `rep_sid` (bundle `part.rep_sid`), the conditions use `lake_gauge` as today, and
  the point is the polygon's representative point.
- **Why stored, not computed on device:** the app cannot compute a length-weighted midpoint without geometry, and tiles
  are clipped per tile.

**What replaces what:**
- `usePickFocus`'s place-ring trilateration and the `querySourceFeatures` re-fit loop (`ui/src/search.ts:145`,
  `Map.web.tsx:501`) are replaced by `part_geo` / `item_geo` bounds.
- `watermap.js` (a projected-SVG copy of three waters) is retired: the app draws the real tiles.
- WaterScreen's two "Conditions" doors use `sections[0]`, which assumes the first sid is the mouth
  (`WaterScreen.tsx:87-88,115-134`). They are replaced by `mid_sid`.

**Size:** 20,461 parts × (rank, mid_sid, 2 + 4 floats) ≈ 0.6 MB, marks ≈ 1–1.5 MB, `water_exit` ≈ 0.2 MB.
About 2.5 MB raw, under 1 MB br. **est.** P09 measures it.

---

## 5. One bundling step: the producers after the change

```
pipeline build  (reach → tiles → deliver)
  deliver all --out-dir <tmp>
    bundle      → bundle.sqlite            (pipeline DB; + part_geo, part_mark, water_exit — P09)
    verdicts    → verdicts.sqlite          (pipeline only)
    status_index→ status_index.bin         (kept as a file for the pipeline tests; copied into the package)
    export      → ui-rules-export.json …   (pipeline only)
    answers     → ui-rules-answers.json    (pipeline only)
    card_check  → FAILS THE CHAIN on any problem (P05)
    store       → regs.sqlite              (pipeline only, read by the port tooling)
    package     → dist/<vintage>/{manifest.json, app-<sha>.sqlite}   + the atlas named by digest
    vintage.report_all(strict)  → promote everything, or nothing
```

`package` is a copy-and-assemble step with **no logic**:
- it `ATTACH`es the bundle and the store;
- copies the E6 tables, the `regs_*` tables and the topology tables;
- inserts the status blob;
- writes `meta`, `VACUUM INTO`, then hashes.
- A parity test runs **every query in `app/packages/data/src/bundle/queries.ts`** against both the bundle and the
  package and requires identical rows.
- The regs tables are byte-copied, so `store check` still holds.

**Later, removing the remaining middle-men (M6, pipeline refactor, not needed for the app):**
- `store` writes its tables straight into the package. `regs.sqlite` as a file stays only while the port tooling reads
  it (`port/taps.py`, `verify_map.py`).
- `answers` hands its model to `store` in memory. `ui-rules-answers.json` becomes `--debug-json`, written only for the
  prototype and page chain until the port lands.
- `export` folds into `answers` in-process: the export's tests read `export_ui_rules.build()` in memory, as AGENTS 58
  already says. The JSON pair becomes a debug dump.
- `feeds/stats.json` is dropped (never fetched). `bc_outside.geojson` and `pipeline.deliver.tiles.boundary` are dropped
  from serving.

---

## 6. Migration from today

| step | what | app change | pipeline change | proves |
|---|---|---|---|---|
| **M0** | guard the chain | — | `meta.format = bundle/N` + `schema_sha256` (audit F3); card gate in `deliver all` (P05); `--out-dir` + promote-all (P07) | a failed step leaves the live set untouched |
| **M1** | packager v1 | — | `deliver package` → `dist/`: E6 + `regs_*` + status blob + `part_section`; `emit_pack --check`; query-parity test (P08) | the package answers every app query like the bundle |
| **M2** | the app reads the package | `config.ts` endpoints (no localhost literals); `fetchVerified` + manifest; `PackSource` = `makeBundleSource` over the package Db; status from the blob; stop fetching `/province.sqlite`, `/status_index.bin`, `/atlas.meta.json`, `/bc_outside.geojson`; `serve-tiles.mjs` serves `dist/` (P10, P11) | cold start 105 MB → ≈ 13 MB |
| **M3** | regulations | integration T01/T03 open `RegsStore` over the package Db (table prefix `regs_`); no `/regs.sqlite` route | — | T03's checks pass on the package |
| **M4** | topology + water map | the regs-mode map (P15), "Conditions here" (P14) | `part_geo`, `part_mark`, `water_exit` in the bundle + package (P09) | the Vedder/Cowichan/KLWA water maps match the prototype's part order and marks |
| **M5** | native caching | disk cache, atomic swap, offline open; open from file (P11) | `publish` to R2 (P10) | airplane-mode launch opens the last package |
| **M6** | retire the middle-men | — | store → package directly; answers/export JSON → debug dumps; drop `stats.json`, the mask | `deliver all` writes only pipeline DBs + `dist/` |

Each step lands green on its own. M2 can ship before M3: the regs tables simply sit unused in the package until the
integration lands.

---

## 7. Sizes summary (today → target)

| | today | target |
|---|---:|---:|
| cold-start download | 105.1 MB identity | ≈ 13 MB br (≈ 16 MB gzip) **est.**, once per vintage on native |
| on device | 102.7 MB bundle (in memory) + 1.66 MB index | ≈ 41 MB package **est.** (native: on disk, paged) |
| regulations | not shipped (would add 14.2 MB + a second handshake) | inside the package |
| geometry | tiles (+ a 288 KB SVG copy for 3 waters in the prototype) | tiles only, + ≈ 2.5 MB topology/camera hints **est.** |
| files the app fetches at startup | 5 (+ wasm on web) | 2 (manifest, package; + wasm on web) |
