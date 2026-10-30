# T299 — Water names on the map again; basemap place names by importance and BC only

Status: DONE 6c9e8c570, 34a1f817f (2026-10-30; basemap + atlas rebuilds and the release are the owner's batch, see Builds owed).

Source: owner: "make it so stream names show up on map still? Archived version had the display names show up on the vector
tile of streams and same for lakes etc. this is important to add back. Also the density of the OSM pmtile town names /
place names on the base tile feels off it puts a ton of names on the screen but should show most important first and hide
a lot until zoomed in a lot more. Also the names of places etc outside of BC (the BC polygon) should have been filtered out
of the base tile."

## Work
1. **Water names:** stream and lake labels drawn from our own atlas tiles (display name, incl. T224 accepted names), along
   the line for streams, centred for lakes; priority by size (stream order / lake area) so big waters label first and small
   ones only when zoomed in; collision-aware; theme tokens; match the archived app's behaviour (archive/webapp) as the
   reference. Tile attribute added in the tiles export if missing (tiles + bundle same vintage).
2. **Place-name density:** basemap `places` shown by importance (OSM `population_rank` / `pmap:min_zoom` / kind: city >
   town > village > hamlet > locality), far fewer at low zoom, more as you zoom in; style-side filter + symbol-sort-key.
3. **Outside BC:** drop `places` (and other labels: roads names, POIs if any) outside the BC polygon in the basemap ship stage
   (T280 two-stage build: add to the prune stage), so no Washington/Alberta/Alaska names. Basemap rebuild via heavy.sh.

## Acceptance
Screenshots at z7/z9/z11/z13 before/after (Lower Mainland + a border area); no labels outside BC; water names readable.

Depends: T280. Size: M. Owner: no.

## Done (2026-10-30)

**Why the water names were missing — three causes, all fixed:**
1. *Collision order was inverted.* MapLibre places symbols from the TOP layer down; the app put the basemap's labels
   over ours and sorted our labels "lowest priority number first", believing first wins. So every town, road, park and
   reserve name claimed its spot before a river. Now: stream + lake names are the top layers (placed first), our area
   names stay under the basemap's (a town outranks a park; "Tidal water" had taken Vancouver's spot), label priority sort
   fixed in `build-style.mjs` (lowest number on top). `tile-contract.test.ts` had pinned the wrong order; corrected.
2. *Sections are too short.* A line label needs one feature longer than the name; sections are cut at every boundary and
   confluence. New decorative tile layer **`stream_name`**: per zoom band z8–z13 the named sections drawn by that zoom,
   line-merged per name (`export.export_stream_names`, derived from `stream.geojsonl`, so `--tiles-only` rebuilds it).
   Built in its own tippecanoe pass on the 4096 grid (`tippe.ALONE`) so it can't pin the regulated streams' vertices.
3. *Too wiggly for `text-max-angle`* (MapLibre sums every corner under the label). Each band's lines are simplified
   4 px at that zoom + 3 Chaikin passes; label max angle 25 → 45. Lower Mainland z11: 1 → 8 streams named.

Stream labels now start at z8 with an order ladder (ord ≥6 z8, ≥5 z9, ≥4 z10, ≥3 z11, all z12) and a sort key, so
big rivers are named first. Lakes keep their area ladder. Atlas +17 MB (185 → 203 MB measured on the scratch join).

**Place density:** `places_locality` shows a city always, a town from z7, a village from z9, a hamlet from z10,
anything else (OSM `locality`, farms) from z12; bigger kind then bigger population placed first (`labels.ts`
`placesByImportance`). Lower Mainland z7: 111 place names → 8.

**Outside BC:** basemap ship stage cut 7 (`basemap_grid.confine_tile`): label points (`places`, island and sea
points) outside BC's unbuffered outline dropped; road lines with no vertex in BC lose name/ref/shield keys (still
drawn). Scratch build 214.9 MB (−0.1 MB). Shots: 0 basemap names outside BC (was up to 39 at Blaine z7).

**Shots** (headless, `app/tools/label-shots.mjs`, new): `T299-shots/before/` (live tiles + old style) and
`T299-shots/after/` (scratch basemap + scratch atlas + new style), Lower Mainland / Blaine / Osoyoos, z7/9/11/13,
light + dark; per-shot counts in each `report.json`. Known trade-offs: a lake name now beats a town's (Osoyoos at
z11); the tidal Fraser is named "Tidal water" by the `tidal_water` layer (not this ticket); a border-crossing road
(I-5, US97) keeps its shield.

## Builds owed (owner batch, one at a time)
1. Basemap: `scripts/heavy.sh basemap 30 -- .venv/bin/python -m pipeline.deliver.tiles.basemap` (~5 min; or promote
   the identical scratch build `data/generated/scratch/basemap-T299.pmtiles`, backing up the live one, then
   `--release-only`).
2. Atlas: `scripts/heavy.sh tiles 30 -- .venv/bin/python -m pipeline.deliver.tiles --tiles-only` (derives
   `stream_name` from the current layer files; new tile version, same sid space).
3. Release + APK: the style change ships in the app. A new app over old tiles draws no stream names (no `stream_name`
   layer) — ship them together.

## Follow-up: "Seattle" on a phone (owner, app 0.3.0, staging r-51d80e68f056)
The shipped basemap (basemap-6960ed933a82, same bytes on staging R2) carries no US name (decoded z0–9) and the composed
style draws none over it, so the phone was drawing an older basemap; the phone had no style-side guard (only the web
confined basemap labels, `web/outsideMask.ts`). Fix: every basemap symbol layer now also filters `within` BC's outline,
in the style itself, on web and phone (`labels.ts` `inBC`; the outline is generated, `python -m
pipeline.tools.emit.bc_outline [--check]` → `app/packages/map/src/bc-outline.generated.ts`, 438 vertices, simplified
1 km and pushed out 500 m so White Rock and Osoyoos stay). Proof: the pre-T299 basemap (b515359fc62f, which named 39
places outside BC at Blaine z7) renders 0 with the new style alone (`T299-shots/old-basemap-guard/`). Tests:
`packages/map/src/bc-outline.test.ts` (border towns in, Seattle/Bellingham/Spokane/Calgary/Juneau/Oroville/Hyder out;
every basemap symbol layer confined in `runtimeStyle` and `nativeStyle`). Ships in the APK; no tile build. Emulator
check for the APK batch: no basemap label outside BC at z6 over Seattle.
