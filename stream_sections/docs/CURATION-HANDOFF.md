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
9. **Closure-area polygon.** A sign-bounded No-Fishing *area* (not a reach cut) is split at its real
   boundary points (e.g. `-a`/`-b` at the two shore signs) AND records `polygon = [[lon,lat],...]`
   (ordered corners per the reg) on the row(s) so the area can be drawn later. Round-trips like offset.
10. **Lake-split = defer, but VERIFY don't assume.** A *lake* divided into closure areas → defer (see
    [[lake-splits-deferred]] equivalent note). But confirm it really is a lake-area split before
    deferring; a river with a bounded-area closure (e.g. Fraser Landstrom/Croft) is still curated as
    split points + polygon.

## Presenting an entry (per the human's ask)

Show **every** item in the entry — including already-curated (green) rows — each with its status,
coord, and an **OSM link** (`openstreetmap.org/?mlat=LAT&mlon=LON#map=16/LAT/LON`; the human verifies
on OSM, not Google). Try to resolve every todo first (FWA/OSM). Watch for **duplicate splits within a
waterbody** (parser makes multiple rows for one physical boundary, e.g. a shared bridge/line, or a
bare word like "signs" over-matching several reaches) — give true duplicates the **same coord**, or
un-link an over-matched row by making its `locator_text` specific to its reach.

## The interactive curation loop (how to run it WITH the human)

This is a **human-in-the-loop** loop. Each iteration = one waterbody. The human confirms every
entry before it's written, but you do NOT wait to be told to start the next one ("after one is done
auto do next").

1. **Pick** the next incomplete entry — `wb_present.py --todo` (most-todo first). Do ONE whole
   waterbody at a time; never leave a reg entry half-done. If it's a lake-split, defer (rule 10).
2. **Resolve** every todo boundary best-effort BEFORE presenting. Flag likely n/a. Detect duplicate
   splits (same physical boundary → same coord; un-link a bare-word over-match by making its
   `locator_text` reach-specific). **Do this in a subagent** (see below) to keep tool output out of
   the main context.
   - FWA (gpkg) for rivers/lakes/confluences/falls/outlets: confluence = nearest point of main river
     to the named tributary (+ tributary `blk/wsc` target); lake outlet = river ∩ lake boundary
     (+ lake `WATERBODY_KEY` target).
   - OSM Overpass for man-made crossings (hwy/road/rail/bridge/dam/powerline/weir): `HDR` User-Agent
     + retry/backoff + ≥2.5 s pacing; intersect the OSM way with the FWA river (MU-clip via `wmu`).
     See `stream_sections/oneoff/osm_bridges.py`. Falls not in FWA → try FISS/OSM or ask for a pin.
3. **Present ALL items** in the entry — including already-curated (green) rows — each with status,
   coord, and an **OSM link** (`openstreetmap.org/?mlat=LAT&mlon=LON#map=16/LAT/LON`; the human
   verifies on OSM, not Google). Show the verbatim reg with boundaries **bold + numbered**. Give a
   best-guess candidate for every todo, flagged by confidence; say which need a human pin.
4. **Wait** for the human: `all good`, or corrections (they paste coords / OSM way ids / pins /
   "n/a" / "defer"). **Apply only on confirmation — never assume.**
5. **Apply** via a small scratchpad script → `save_curation`. Set `reviewed = "<today>"` on every
   row of the entry. **Re-check the row count** after save (a broken empty-`locator_text` `label`
   link silently drops a row — restore from HEAD and fix if the count moves unexpectedly).
6. **Commit** (`rtk git add …`, co-author trailer) + `graphify update .`.
7. **Auto-advance** to the next entry.

### Use subagents to keep the main context lean (important)

The geodata resolution (loading the EPSG:3005 gpkg, Overpass queries with retry, computing
confluences/outlets/offsets) emits a LOT of tool output. To minimize main-context tokens, **spawn a
subagent to do step 2** and return only the compact result:

- Give it: entry name + MU, the todo rows (`id, locator_text, anchor_kind`), and the recipe (FWA
  layers + OSM Overpass patterns). Tell it to return **only** a table
  `{row_id → candidate [lat,lon], method, confidence}` (+ blk/wsc or lake WBK targets) — NOT the raw
  query dumps.
- Subagents start cold, so include essentials in the prompt: gpkg path + CRS, venv
  (`PYTHONPATH=. .venv/bin/python`), `HDR` User-Agent + retry/backoff + pacing, the `[lon,lat]`
  convention, and the graphify rule for any code reading.
- Keep **apply** (`save_curation`) + **commit** in the main agent — they're cheap and need the
  human-confirmed coords.

A graphify PreToolUse hook fires on every Bash/Read — it targets **source-code** exploration, so it
does not apply to FWA/OSM geometry scripts or data reads; run those normally.

## Current state (2026-08-08)

767 rows · **368 curated · 181 n/a · 45 deferred · 165 todo** · 8 manual. **40 entries carry
`reviewed`.** Fields: `offset`, `polygon`, `reviewed`. Site C dam fixed; 39-row offset retrofit;
lake/confluence targets; named-feature audit clean (0 m spread). `scratchpad/` reusable resolvers:
`resolve_bases.py`, `lake_targets.py`, `audit_features.py`, `elk_bridges.py`, plus per-entry
`apply_*.py` scripts (one per waterbody — copy the pattern).

Reviewed/handled earlier: Fraser 3-14, Columbia, Elk (u/s Elko), Chilliwack/Vedder, Kokish, Cowichan,
Fraser (u/s CPR Mission), Little Qualicum, Shuswap River, Nitinat, Campbell 2-4, Coquitlam, Nicomekl,
Serpentine, Horsefly, Nechako 7-12.
Handled 2026-08-08 batch: Peace 7-31, Campbell 1-10, Heber 1-9, Qualicum 1-6, Quinsam 1-6,
Silverhope 2-2, Seton 3-16 (extent n/a), Alexander 4-23, Michel 4-23, Sand 4-22, Lodgepole 4-2,
Baker 5-13, Babine 6-8, Stellako, Crooked 7-24, Amor De Cosmos 1-10, Keogh 1-13, Puntledge 1-6,
Toquart 1-8. Deferred (lake-splits): Quesnel Lake 5-15, Vaseux Lake 8-1, Great Central Lake 1-7
(ref points for the dam + Ash Main Bridge saved on the deferred rows).

### Patterns learned this batch (apply going forward)

- **Tidal-boundary / variant anchors** (user ask): where a boundary sits at the tidal limit, ALSO add
  the highway/rail bridge at that limit as a **`manual`** anchor (e.g. Qualicum Hwy 19A). It lives as
  recoverable `drift` (its `locator_text` matches no source locator → `build()` stores the full row,
  `flatten` restores it). Record `[name-variants: X | tidal boundary]` in `notes`; a later pass will
  collapse co-located anchors (e.g. cement block / tidal boundary; power station / lake outlet).
- **Extent-clause rows** (name parenthetical like "(includes BC Hydro Power Canal…)" or
  "(including two lagoons…)") are NOT internal splits. Default = n/a with a note to include the
  man-made/extra feature — BUT the human may prefer `defer` as a lake-split (Vaseux). Ask.
- **"upstream/downstream of easternmost Hwy N bridge"** split into two separate reg entries → same
  bridge coord for both entries (Alexander, Michel). Easternmost = largest longitude of the multiple
  Hwy∩creek crossings.
- **Multiple same-name creeks/rivers in BC** → clip the FWA layer to a region box before merging
  (Alexander/Michel/Sand each had province-wide duplicates).
- **Boat/vessel reach ≠ auto-n/a if the human wants it mapped** (Stellako "no powered boats François
  Lake→falls" — they wanted the reach). Confirm before n/a-ing a boat restriction.
- **FISS obstacle** is the falls fallback when FWA `waterfalls` + OSM both miss (Quinsam, Toquart):
  the human pastes obstacle id + lat/lon; store `[FISS obstacle NNNNN, WSC …]` in notes.
- **FWA gaps**: some West-Coast VI rivers (Toquart) aren't in the gpkg at all → resolve from OSM only.

### Review queue (staged candidates — NOT yet applied)

`stream_sections/docs/curation-queue.md` holds **auto-resolved candidate coords pending human
review**. They are NOT in `waterbody-splits.json` yet — the resolver stages them so a human can
review a batch, then they get applied via `save_curation`. Workflow: resolve many single-anchor
todos (bridge/dam/confluence) in bulk with `scratchpad/queue_resolver.py` (writes a dated batch to
the queue doc, one line per row: entry, row_id, candidate coord, method, OSM link, or 🔴 UNRESOLVED →
needs pin). To finalize: the human reviews a batch in the queue; then apply the confirmed coords to
the rows (`load_curation`→set coord/status/label→`save_curation`), set `reviewed`, and delete that
batch from the queue. `queue_resolver.py`'s `BATCH` list is the recipe format (entry, mu, row_id,
kind, fwa_river, box, road/dam filter, note) — extend it to stage more.

### NEXT UP

`wb_present.py --todo` → next tier: **Nathan (Beaver) Ck 2-4, Pitt R 2-8, Sakinaw Lake 2-5, Salmon R
2-4, Weaver Lake & Ck 2-19, Clearwater R 3-40/3-46, …** Still open: **"ELK RIVER'S TRIBUTARIES"**
(unresolved `EXCEPT Coal Creek d/s of old MF&M Railway`). Follow the loop; defer lake-splits (verify
first); confirm every candidate before writing.
