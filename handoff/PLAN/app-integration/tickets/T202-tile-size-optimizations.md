# T202 — tile size optimizations (last band)

Status: DONE — basemap d4589cb5 / 6f25c3c9, atlas aeba83ca + b17dfa1f (web promoteId fix) (release r-c706f5eaa2a3, 2026-10-27)

Source: owner 2026-10-25 — "another item in last band is the tile size optimizations."

## Starting point
After the tiles window (T64 wetlands out, T85 atlas z13 + overzoom, basemap trimmed to what the style draws, T75
basemap released): measured sizes in T64's S2 table (`python -m pipeline.tools.audit.tile_sizes`).

## Work (measure each, keep only what loses nothing the owner cares about)
- Basemap z13 (T85 "Parked"): accept the loss, z14 only where it adds features (urban / road-dense tiles), or a small
  z14 label layer (forest service roads first) over a z13 basemap — re-measure with `--audit-zoom 13`.
- Per-layer attribute trimming and coordinate precision at each zoom; drop properties the app never reads.
- Simplification tolerances per zoom (river bends at z13 overzoom; T85 notes).
- Tile compression / dedupe settings; split archives for web vs native if it pays (T182 pattern).
- Contour layer budget for T201, run timing layer for T199 — sized here before they ship.
Each change: size table before/after, a visual check at the owner's spots, no feature lost unless the owner accepts it.

Band: last ("Data feeds at the end"), with T199–T201.

## Owner 2026-10-27 — basemap z13 rule
Go to z13, but KEEP what anglers use outside towns: trails/paths, forest service roads and resource roads, their names,
access points. DROP what only matters in cities: sidewalks, crossings, pedestrian areas, living streets, urban street-name
density. Measure the saving and prove (audit-zoom) that no trail/FSR feature or name is lost; urban losses are accepted.

## Basemap: built 2026-10-27 (owner decisions on `handoff/PLAN/tiles/T202-basemap-audit.md`)
- **Built:** z13 cap, with the z13 roads rebuilt from z14 (`tiles/basemap_roads.py`), so every trail, forest service
  road, resource road and residential name survives. The owner accepted the urban losses: sidewalks, crossings,
  living streets, subway, pedestrian areas, oneway arrows at z16+, and the Deadman's Island, Richmond Island and
  Coal Harbour labels.
- **Dropped, never read:** `sort_rank`, lake/river label points, earth cliff lines.
- **Dropped, owner call:** the wetland stand-in. To bring it back, see `basemap.LANDUSE_STANDIN`.
- **Zoom audit:** clean on all 6 extents. It now runs before every capped build.
- **Size:** basemap **918.4 → 322.5 MB**. Promoted file by file, released as `basemap-859406091819`. The old file is
  kept at `data/generated/scratch/basemap-pre-T202-20260925.pmtiles` until the owner looks.
- **Grid coarsening:** investigated only, not shipped. The numbers and shots are in the audit doc.
- **Grid coarsening SHIPPED (owner 2026-10-27):** landuse at 1024, water and earth at 2048 (`tiles/basemap_grid.py`).
  The basemap is now **274.5 MB** (`basemap-147bd82d051f`), the audit is clean, and the shots differ by ≤0.07% of
  pixels.

## Atlas: built 2026-10-27 (owner decisions on `handoff/PLAN/tiles/T202-regulation-audit.md`, rounds 1 + 2)
**atlas.pmtiles 239.3 → 176.1 MB (−63.2, −26%)**, tile version `b6b5000e171023c9.0d7666619a1c`. Measured with
`python -m pipeline.tools.audit.tile_sizes` on the old and new archives.

| change | where | saving (measured per layer; the shared stream/lake bytes split by the audit's bbox shares) |
|---|---|---|
| no `section_id` property (MVT id only; web no longer promotes the sections) | `tippe._tile_props`, `runtime-style.ts` | ≈ −18 |
| `water` omitted when it is the layer's own kind; `area_m2` dropped on unnamed lakes < 10 ha | `tippe._tile_props` | ≈ −2 (smaller than the audit's −6: the outline width reads `area_m2` on every lake, so only small unnamed lakes, where the width is within 0.065 px of the fallback, lose it) |
| grid 2048 at z4–12, `-S 5` (same ground tolerance) | `tippe.LOW_DETAIL` | ≈ −7 (z≤12 −11.6 in all, incl. mu/region below) |
| z13 grid 1024, `-S 1.25` (≈5.5 m line) | `tippe.TOP_DETAIL`, `SIMPLIFICATION_AT_MAX` | ≈ −26 |
| mu to z12 in its own pass, overzoomed (`sources.mus`) | `layers.mu` `own_pass`, style | **−8.2** (13.5 → 5.3) |
| watershed (Liard) decorative, drawn as a wash | `layers.watershed`, style | **−1.0** (2.1 → 1.2) |
| wma: Creston only (`tile_names` in areas.json) | `export_admin` | **−0.2** |
| `region.name` dropped | `layers.region` | **−0.3** |
| parcel: no extra pass (the 2048 low-zoom grid is the best MB per metre: +1 m, sub-pixel) | — | −1.6 (with the grids) |

Per zoom (MB): z13 168.3 → 116.7, z12 40.6 → 34.2, z11 18.4 → 15.2, z10 7.3 → 6.1, z9 2.6 → 2.2, z4–8 1.7 → 1.4.
Per layer: stream 146.4 → 111.1, lake 42.2 → 26.9, under_lake 15.0 → 14.2, parcel 10.6 → 9.0, mu 13.5 → 5.3, park 5.6 → 5.3.

**App:** `watershed` drawn as a darker green wash (`color.watershed.fill` / `opacity.watershed`, light + dark + cvd),
`sign_zone` drawn with a dashed closed-colour edge and a slight infill (`dash.sign_zone`, `opacity.sign_zone`), not tapped
like every other area. `mu` reads the `mus` source (maxzoom 12). Sections are keyed by `feature.id` on the web too.

**Shots** (`T202-shots/`, light + dark, before = old archive, after = new; the basemap lane changed the ground in between):
Fraser Valley z11, Vedder z16/z18 (after only; the before spot missed the river — the geometry overlays
`geom-vedder-z14/z17.png` and `geom-fraser-hope-*` draw old vs new z13 lines on one plot, they coincide), Vancouver z13,
Chilcotin z12/z15, Liard z8/z11, both sign zones z15, an MU boundary (2-3/2-4) z14/z17. Nothing lost.

**To reverse mu** (if the unit edge is off close in): see decisions.md 2026-10-27 T202 (layers.py `mu` maxzoom 13, drop
`own_pass`; style `mu` back on `atlas`, drop `sources.mus`; rebuild tiles).

Not done (owner): brotli, the uncompressed native archive. Contours (T201) and run timing (T199) are sized when they ship.

**Fix after ship (b17dfa1f):** the web atlas source kept an object `promoteId` for the area layers; GL JS applies an object
promoteId to EVERY source-layer, so streams read `properties[undefined]` and lost their MVT ids (no paint, no taps).
The atlas now promotes nothing (the area layers there are static, as on the phone). Verified: ids on 54/54 + 34/34
stream features at Fraser Valley z11/z13, map paints, a Vedder tap opens its regs (`T202-shots/fix-*.png`).
