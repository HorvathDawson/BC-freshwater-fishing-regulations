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
- Not done, recorded with numbers: basemap z13 (−289 MB more), dropping the `section_id` property (~6.6%).
- Still to verify after the build: shots at z13–z18 (Fraser Valley bends, Vancouver dense tiles), the size table's
  "after" column.
