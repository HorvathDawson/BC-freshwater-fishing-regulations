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
