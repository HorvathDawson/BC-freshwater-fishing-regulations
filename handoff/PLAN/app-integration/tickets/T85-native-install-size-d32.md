# T85 — Native install size: atlas z13 overzoom and the basemap landuse filter (D32)

Status: PARTIAL — code done 2026-10-24; the tiles-window build, shots and after-numbers are left (see Outcome).

Source: decisions D32 and app-data/QUESTIONS.md: about −345 MB per native install, "scheduled before the first public
native release". T64 names only parcels, basins and "S2 easy wins".

## Work
Measure both changes; decide; build them in the same tile build as T64 if they fit (one tile build). Never replace the
tiles dir: promote file by file. Tiles and bundle share a vintage.

## Acceptance
Size table before/after; no visible loss at z13+ (shots, including the overzoom corner-cutting noted in
implement_progress).

Depends: T64 (same window). Heavy, scripts/heavy.sh. Size: M. Owner: no.

## Outcome
Measured, decided, built in code 2026-10-24; takes effect at the tiles-window build (atlas tiles + basemap). Size table:
`handoff/PLAN/app-data/S2-tile-size-easy-wins.md` "Measured 2026-10-24".
- **Atlas z13 + overzoom.** `tiles/ladder.MAX_ZOOM` 14 → 13 (bottom rung `(0, 13)`; every layer's maxzoom follows),
  `tippe.SIMPLIFICATION_AT_MAX` 5 at the archive's top zoom (≈ 4 m, the old z14's ground error, so the z18 overzoom is
  no coarser than today); app `ATLAS_MAX_ZOOM` 13, `MAGNITUDE_LADDER` mirrored, `FIT_MAX_ZOOM` kept at 14 (no fit
  changes), camera z18. Estimate about −140 MB (z14 was 315.7 MB, but z13 now carries every reach) — unmeasured until
  the build. An archive built to z14 still serves (the source asks for z13).
- **Basemap filter** (`tiles/basemap.feature_filter`, `tile-join -j`): keep only the landuse kinds the style draws
  (+ `wetland`, kept as T64's possible stand-in) and of `water` only river lines (OSM `stream` lines: −242 MB, drawn by
  the style only at z14+ under our FWA streams) — about −268 MB of 918. Held to the style by
  `app/tools/basemap-filter.test.ts` (mutation-checked) and `pipeline/tests/test_basemap_filter.py`.
- Not done, recorded with numbers: dropping the `section_id` property (~6.6%).
- **Basemap z13 (owner 2026-10-24: "if no feature is lost"): measured NOT lossless — the basemap stays z14.** The
  zoom-cap audit (`tiles/basemap.audit_zoom_cap`, `python -m pipeline.deliver.tiles.basemap --audit-zoom 13`) builds
  two small real extents twice through the build's own path (`_trim`: extract + feature filter), to z14 and to z13, and
  compares every feature kind (layer, kind, kind_detail) and named feature: Squamish loses 6 kinds (path sidewalk /
  crossing, other living_street / ladder, rail narrow_gauge, landuse pedestrian — a drawn kind) and 426 names (nearly
  all minor-road and path names: Protomaps names minor roads only from z14); Lac Le Jeune loses 75 names, its forest
  service roads among them. `MAX_ZOOM` stays 14; `--max-zoom 13` is refused while the audit loses anything. The −289 MB
  is forgone unless the owner accepts the loss (OWNER-TODO). Tests: `pipeline/tests/test_basemap_zoom_audit.py`
  (tippecanoe fixtures: a z14-only kind and name reported lost, a filtered-out line not counted, a clean cap passes,
  the build refuses a lossy cap).
- Still to verify after the build: shots at z13–z18 (Fraser Valley bends, Vancouver dense tiles), the size table's
  "after" column.

## Parked (owner 2026-10-24: "we can look at this more later")
Basemap z13 stays open as a later decision, not dropped. Measured loss at z13 vs z14:
- Squamish: 6 feature kinds gone (pedestrian landuse — drawn — plus sidewalks, crossings, living streets) and 426
  names, nearly all street and path names.
- Lac Le Jeune: 75 names, including forest service roads (the ones an angler drives to reach a lake).
- Saving forgone: about −289 MB of the 650 MB basemap.
Options to weigh when it is picked up: accept the loss; keep z14 only where it adds features (urban / road-dense tiles)
and cap the rest at z13; or carry the lost names (FSRs first) into a small z14 label layer over a z13 basemap.
Re-measure with `python -m pipeline.deliver.tiles.basemap --audit-zoom 13`.
