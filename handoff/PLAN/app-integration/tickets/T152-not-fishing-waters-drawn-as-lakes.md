# T152 — Sewage basins, tailing ponds and dumps are drawn as fishable lakes

Status: PARTIAL — code done 2026-10-24; the tiles-window build, shots and after-numbers are left (see Outcome).

Source: T148 census (handoff/PLAN/sloughs/v2/census.md table B), 2026-10-23.

## Problem
All 275 FWA manmade polygons coded EA26700110 (sewage settling basin, 214), AP90300100 (mine tailing pond, 59) and
AP09200000 (dump, 2) are in the lake tile layer (minzoom 8–14) and draw as lakes. None is an item, named, gauged or
bound by a named-water rule, but 12 Fort Nelson sewage basins carry the "Liard River watershed" area rule and 4 tailing
ponds sit on named creeks (Alkali, Pukaist, MacDonald, Wilfred; 93–231 ha, visible from z8). The app tells an angler
these are lakes with the region's rules.

## Work
One decision (registry: a water's fishing status from its FWA feature code): these codes are NOT_FISHING_WATER — not an
item, never bound by an area/zone rule, drawn as a non-water (or a distinct "not fishing water" fill) in the tiles, never
searchable. A pinned list of the codes. Measure: bindings removed (area rules), tiles change (rides the tiles window).

Rebuild: atlas registry + reach + deliver; tiles (tiles window). Owner question: draw them at all (default: a muted
non-water fill with no tap)?

## Outcome
Code done 2026-10-24 (tiles window); takes effect at the next atlas build → reach → tiles → deliver.
- One decision, one place: `pipeline/common/fishing_water.py` `NOT_FISHING_WATER` (EA26700110, AP90300100,
  AP09200000, pinned). The atlas build stamps the code on the polygon's node (`StreamNode.not_fishing`,
  `atlas.build.get_not_fishing_codes` / `stamp_not_fishing`); the node stays in the graph (a creek routed through a
  tailing pond keeps its topology).
- Reach: a fourth global limit, `classify.apply_limits` `not_fishing_water` (`NOT_FISHING_WATER_SUBTRACTED`), read by
  `build_reach` itself (so the review app gets it too) and by the steelhead twins: no zone, area, water rule or
  licensing record binds one; a rule left with only such water is unresolved `no_sections`, never empty.
  `POLICY_VERSION` 32 → 33.
- Tiles: out of `lake`; drawn into a new decorative layer `not_fishing_water` (no attributes, no id, not tappable),
  which the app does not draw yet (`tile-contract.test.ts` lists it undrawn). None is named, so none is an item or
  searchable.
- Expected binding change (census table B, live bundle): all 275 lose the zone rule sets they carried; 12 Fort Nelson
  sewage basins also lose the "Liard River watershed" area rule; no water rule bound any of them (regulated 0), so no
  named water's binding moves. Verify after the reach run: `rule_diagnostic.jsonl` kind `not_fishing_water`.
- Tests: `test_not_fishing_water.py` (codes, stamp, zone/area limit with mutation, unresolved-not-empty, tile export),
  `test_reach_builder` policy pin, `test_tiles` inventory.
- Owner question Q125 (draw them at all?) is in OWNER-TODO: built as NOT drawn by our tiles (the basemap may still show
  OSM water there); a muted fill is one style entry on `not_fishing_water`.
