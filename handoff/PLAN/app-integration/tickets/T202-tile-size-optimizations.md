# T202 — tile size optimizations (last band)

Status: OPEN

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
