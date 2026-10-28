# T152 — Sewage basins, tailing ponds and dumps are drawn as fishable lakes

Status: DONE 2026-10-27 — shipped by the 10-27 atlas build + reach + tiles (b6b5000e171023c9.0d7666619a1c) in release
r-c706f5eaa2a3; after-numbers and a shot below (T152 lane, 2026-10-27 night).

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
- **Q125 answered (owner 2026-10-24): drawn as a muted fill, never as a lake — built 2026-10-24.** Style layer
  `not_fishing_water` (`app/packages/map/style/layers.source.json`, after `lake_flowing`): a static fill in its own
  ground-colour token `color.notfishing.fill` (light/cvd #BDB3A0, dark #9A8C75; `opacity.notfishing` 0.45 / 0.35), no
  `featureIdProperty`, not highlightable, in no tap group, not in the visible set, not promoted; covered by the
  Conditions field like the other water fills. Web and native both draw it from the one generated style (native is
  `runtimeStyle` + `applyLook`); style hash 86ad2d77 → 744419e1. Pinned: `packages/map/src/not-fishing-water.test.ts`
  (web + native fill over its own source-layer, never the lake colour, no id/tap/highlight), `tools/tile-contract.test.ts`
  (no longer undrawn), `tools/layer-order.test.ts` (a fill under the boundaries). Shows once the next atlas build ships
  the layer.

## After (measured on the live atlas / reach / tiles / release r-c706f5eaa2a3, 2026-10-27)
Nothing rebuilt for this — the T202 atlas-tiles build (aeba83ca) and its reach re-run carried the stamp, so registry +
reach + tiles + deliver are all live.
- Tiles: `layers/not_fishing_water.geojsonl` 275 features (z9–13 in atlas.pmtiles); 0 of their rings are in
  `lake.geojsonl`. The app draws the muted fill (style layer `not_fishing_water`, :39218 verified via
  queryRenderedFeatures). Shot: `T152-shots/highland-valley-tailings-z11.5-light.png` (Highland Valley Copper ponds,
  grey-tan, not lake-blue).
- Reach: `rule_diagnostic.jsonl` kind `not_fishing_water` 245 rows — 242 zone rule sets (zp 78, z7 53, z6 23, z4 17,
  z5 17, z3 15, z2 14, z8 13, z1 12) + `liard_river_watershed.r1–r3` (12 Fort Nelson basins each); 67 rules lost all
  275 polygons, the rest lost the ones in their area. No named water's binding moved (as predicted: regulated 0).
