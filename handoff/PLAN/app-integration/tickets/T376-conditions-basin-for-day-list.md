# T376 — The watershed a Conditions view is about: map centre, a water's own, and names

Status: OPEN — data chat, for the UI chat's forecast-day list (T367, 2026-11-03); in 0.5.0 (batch 4).

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
