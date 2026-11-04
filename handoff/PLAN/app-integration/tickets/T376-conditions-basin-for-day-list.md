# T376 — The watershed a Conditions view is about: map centre, a water's own, and names

Status: DONE (e21cdbeac; the main part of the hook wiring was swept into e604eb5ae) 2026-11-04 — needs a bundle + release rebuild to ship (was: OPEN — data chat, for the UI chat's forecast-day list (T367, 2026-11-03); in 0.5.0 (batch 4)).

The UI chat draws a list of forecast days (+1…+10), each with one watershed's standing from `basinStandings(basins, idx,
quantity, horizon)` (data/src/standing.ts; the same values as the zoomed-out heat map). It needs from us:
1. **Browse:** the watershed the Conditions map is centred on, on the map colouring (e.g. `col.centreBasin: { id, name }
   | null`), updated when the map stops moving; null when the centre is in no basin. The same ids `basinStandings` uses
   (`basin_id` = the FWA WATERSHED_GROUP_CODE, `pipeline/deliver/tiles/basins.py`).
2. **A water:** in a water's Conditions view, that water's own basin id + name (by item or section) — never the map
   centre. No fallback between the two (owner rule: no hidden fallbacks).
3. **Names:** a display name per basin ("Chilliwack River watershed"). The source layer carries WATERSHED_GROUP_NAME;
   `atlas.registry.basins.basin_name` already names `area:basin:` ids for the bundle. Ship id → name with the release
   (one small table; say its size), the same names both places.
Work: Python ships the names (+ each item's / section's basin if the app has no join yet); `packages/map` / `data` expose
`centreBasin` (point-in-basin from the tiles' basin layer or the shipped table) and the water's basin; tests; tell the UI
chat (T367) the field names. Size: S–M.

**Done (e21cdbeac).** Bundle tables `basin` (ix, basin_id, name — all 246 groups, WATERSHED_GROUP_NAME as sourced, no
"watershed" added), `basin_frame`, `basin_grid` (1/32° × 1/64° cells, LEB128 runs; `pipeline/deliver/bundle/basin_grid.py`,
written beside `basin_member`), copied whole into the release's `conditions` (additive, still conditions/1): ~42 kB gz
(conditions 873 → ~915 kB of its 950 kB gate). The tiles' basin layer could not serve: it is in the tiles at z4–z7 only
and drawn only below the handover. A per-section join was measured at 1.15 MB gz (sids sort by blue line, not place), so
a water's basin is its place on the grid instead. `atlas.registry.basins.basin_name` is not relevant: its ids are FWA
code prefixes for regulation areas (6 names), not watershed groups. Fields (T377): `col.centreBasin` (`{ id, name } |
null`, undefined off Conditions / not yet), `col.centreBasinError`, `col.noteCentre` (BrowseHost `onMoved`); a water's:
`useWaterBasin(source, { item, tapSid, at })` → `Async<Basin | null>` (the tap point, else `waterPoint`; throws, never the
centre); `WaterSource.basinAt({ lon, lat })`. Needs `python -m pipeline.deliver bundle` then the release.
