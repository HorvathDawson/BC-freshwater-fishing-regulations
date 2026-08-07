# Split-locator curation — one-waterbody-at-a-time handoff

Goal: resolve every regulation **split boundary** (the phrases that cut a waterbody into
differently-regulated reaches) to a coordinate, one reg entry ("waterbody") at a time.

## Where the data lives

- **Canonical:** `stream_sections/docs/waterbody-splits.json` — grouped cards (one per split-bearing
  reg entry) + `drift`. **Edit only via** `load_curation()` / `save_curation()` in
  `stream_sections/oneoff/waterbody_splits.py`; `save_curation` rebuilds the grouped file and the
  two generated views (`waterbody-splits.md`, `-regs.md`). Never hand-edit the JSON/MD.
- `load_curation()` returns **flat rows**; mutate by `id`, then `save_curation(rows)`.
- Source spine (read-only): `output/pipeline/extraction/synopsis_raw_data.json` (raw regs) +
  `synopsis_parsed.json` (parsed boundaries).
- FWA geodata: `data/bc_fisheries_data.gpkg` (EPSG:3005) — layers `streams`
  (`GNIS_NAME`, `BLUE_LINE_KEY`, `FWA_WATERSHED_CODE`, `DOWNSTREAM_ROUTE_MEASURE` mouth→source),
  `lakes` (`GNIS_NAME_1`, `WATERBODY_KEY`), `wmu` (`WILDLIFE_MGMT_UNIT_ID`), `waterfalls`, `parks_bc`.
  Use the project venv: `PYTHONPATH=. .venv/bin/python`.

## Row shape (flat)

`id, name_verbatim, region, mus, src, locator_text, full_regulation, anchor_kind, resolver_hint,`
`status, target, coord [lon,lat], offset, label, notes, entry_id`

- **status**: `todo | curated | manual | not_applicable | deferred`. A card is COMPLETE when every
  boundary locator is resolved (curated/manual/na).
- **anchor_kind**: `point | confluence | line | lake | lake_io | area_boundary | mu_boundary |`
  `buffer | not_a_split | unclassified`.
- **coord** is `[lon, lat]`. (Chat tables usually show `lat, lon` — don't mix them up on write.)

## The model that matters (learned the hard way)

1. **`coord` = the BASE feature; the offset is applied later by the split script — never baked in.**
   A row like "signs 300 m below X Falls" stores `coord` = X Falls and
   `offset = {anchor_label, anchor:[lon,lat], m, dir:"upstream"|"downstream"}`. The sectionizer walks
   `m` metres along the river at split time.
2. **Lake-outlet anchors** additionally get `target = {type:"lake_outlet", wbk:<WATERBODY_KEY>, lake}`;
   the coord is a temporary reference — the splitter resolves the exact outlet from the WBK.
3. **Confluence anchors** carry `target = {blk:<BLUE_LINE_KEY>, wsc:<FWA_WATERSHED_CODE>}` of the
   **tributary** (self-validating on WSC descent). All 112 current confluences have this — keep it so.
4. **Feature-locators are NOT offsets.** If the boundary *is* a mappable feature (a bridge/dam/falls/
   powerline/confluence) and a distance only *locates* it ("logging bridge ~2.4 km d/s of X Lake"),
   the coord is that feature and there is **no** offset block.
5. **Same named feature ⇒ one coord.** If two rows reference "Keenleyside Dam", they must share the
   exact coord. Audit with `scratchpad/audit_features.py` (0 m spread expected).
6. **`-a`/`-b` endpoint rows** split a "from A to B" reach into two rows. Some link to the reg by a
   non-empty `locator_text` substring; **empty-`locator_text` endpoint rows link by `label` tokens**
   (`label_matches`) — do **not** rename those labels or the row silently drops from the rebuild.
   Put descriptive names in `offset.anchor_label`, not in a linking `label`.
7. **n/a** the non-splits: boat/vessel/engine restrictions ("no power boats in wetlands"), residual
   zones ("All other parts"), and pure lake-edge boundaries (auto lake-split handles those).
8. **Whole-waterbody review flag.** When the *entire* reg entry has been walked with the human (not
   just individual locators), set `reviewed = "<date>"` on every row of the entry; `build()` surfaces
   it as card-level `reviewed`. Offset is valid on **any** `anchor_kind`, including `point`.

## The workflow per waterbody

1. Pick an incomplete entry: `PYTHONPATH=. .venv/bin/python stream_sections/oneoff/wb_present.py --todo`
   (most-todo first). **Do one whole waterbody at a time** — never leave a reg entry half-done.
2. Render it: `wb_present.py "NAME" [mu]` → full verbatim reg with boundaries **bold + numbered**,
   then each locator's rows with coord + `[🛰]`/`[🗺]` links.
3. **Best-effort resolve every todo boundary first**, and flag likely n/a:
   - Rivers/lakes/confluences/falls/outlets → **FWA** (gpkg). Confluence = nearest point of main
     river to the named tributary; lake outlet = river ∩ lake boundary.
   - Man-made crossings (highway/road/rail/bridge/dam/powerline) → **OSM Overpass**. Use a temp
     script with `HDR={'User-Agent': '...'}` + retry/backoff + ≥2.5 s pacing; intersect the OSM way
     with the FWA river (MU-clip via `wmu` to beat name collisions). See
     `stream_sections/oneoff/osm_bridges.py`.
4. Present candidates to the human with **both 🛰 (`maps.google.com/?q=LAT,LON&t=k`) and 🗺 OSM
   (`openstreetmap.org/?mlat=LAT&mlon=LON#map=15/LAT/LON`) links** — the human prefers OSM to verify.
   Show existing/curated coords too so completeness is scannable. **Apply only on confirmation.**
5. Write via a small scratchpad script → `save_curation`. **Always re-check the row count** after
   save (a broken `label` link on an empty-`locator_text` row will silently drop it).
6. Commit (`rtk git ...`, co-author trailer), then `graphify update .`.

## Current state (2026-08-07)

765 rows · ~283 curated · ~172 n/a · ~32 deferred · ~278 todo. Done recently: Fraser 3-14,
Columbia, the `offset` field + 39-row offset retrofit, coord=base migration, Site C dam fix,
lake/confluence targets. `scratchpad/` has the reusable resolvers: `resolve_bases.py`,
`lake_targets.py`, `audit_features.py`, `elk_bridges.py`.

### NEXT UP: Elk River (upstream of Elko Dam) · MU 4-2, 4-23 — candidates already computed

Reg C&R reaches: Lower Elk Lake→Forsyth Cr, Line Creek Br→CPR Br (Sparwood), Hwy 3 Br (Hosmer)→
N. Hwy 3 Br (Fernie), Morrissey Br→Elko Dam. Bridges from OSM ∩ Elk (lat, lon):

- Line Creek Bridge `49.88184, -114.87645` (`e38cd2-line-creek-bridge`)
- CPR Bridge at Sparwood `49.75340, -114.88343` (`-cpr-bridge-sparwood`; alt 2.5 km N `49.81155, -114.87247`)
- Hwy 3 bridge at Hosmer `49.58600, -114.96975` (`-hwy3-bridge-hosmer`)
- northern Hwy 3 bridge at Fernie `49.51907, -115.06154` (`-hwy3-bridge-fernie-n`)
- bridge at Morrissey `49.39186, -115.01668` (`-morrissey-bridge`)
- "All other parts" (`91575c`) → **n/a** (residual zone)
- Forsyth Cr end of the "Lower Elk Lake→Forsyth Cr" reach has **no row** (Lower Elk Lake is auto-n/a);
  FWA confluence ≈ `50.22491, -114.96080` — confirm the correct Forsyth Creek, then add a row if wanted.

Awaiting human confirmation of the 5 bridges + the "All other parts" n/a + whether to add Forsyth Cr.
