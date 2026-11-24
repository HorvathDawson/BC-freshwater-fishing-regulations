# T1024 — "Weird highlighting on the lake" (Gravel Slough)

Status: IN REVIEW — fixed in 6704a3504 + d298683a7 + 50b018058 (lane L29), needs the 0.9.0 tile build; owner: OWNER-TODO "T1024 Gravel Slough highlight: look at the before/after shots"

Source: STAGING report `3444007425e9d3bd`, 2026-11-23 05:32Z, Android 0.8.5 (staging build), water · map, Gravel Slough
(`gnis:8009`) part 0, zoom 14.3 at 49.202, -121.904. Verbatim: "Weird highlighting on the lake here".

## Work
1. Reproduce on the emulator and web (Gravel Slough, Map tab, part 0, zoom ~14): shoot what highlights. A slough is a
   stream by rule 55; check whether a lake polygon nearby (or the slough's own polygon) takes the part's selection
   highlight, or the T467 shoreline / T1000 out-of-B.C. drawing is involved.
2. Fix where it comes from; before/after shots.

## Found (lane L29, 2026-11-23)
Reproduced on web against staging (the same tiles, `4ae7b62955bc9990.fc51e82f7c91`): the "lake" is the slough's own
POLYGON (`lake` source-layer, `water` = "stream", drawn by `lake_flowing`: four section pieces at 49.199–49.206,
-121.906…-121.890, the reported spot). On the part's selection two outlines lit it: the lake's selection RING
(`selection-ring-lake`, no filter, so it took stream polygons too) round EVERY section's piece of the polygon — a box
per piece, with a bar across the slough at each section seam — and, inside it, the CASING of the slough's route, which
the tiles write into the `stream` layer under the same section id (`export_streams`). The earlier "not reproduced" pass
looked at z14 on the NE loop, where the polygon is a few px wide; at z14 it is a notch at each seam, at z15–16 it is plain. Not
data, not the T467 shoreline or the T1000 outside drawing.

Fix, round 1 (6704a3504): the rings took standing water only. Review of #81 found the gap: 22 stream polygons never
get a line (20 Okanagan River Oxbows pieces, Adskwatim Creek 1544569, Taylor Slough 1798879: no sidecar line) and 1,992
are drawn 1–5 zooms before it (476 over 0.1 km²: Gun Creek polygon z8 / line z13, Gravel Slough 1360583 z10 / z12), so
the selection lit nothing there. Measured on the live layer files: the same 22 / 1,992 / 476.

Fix, round 2 (d298683a7): every stream polygon carries `lz` in the tiles — its route line's first zoom in `stream`
(`export_streams` reports it per polygon, `export_waterbodies` writes it; none when there is no line; a stream polygon
the streams export did not report is refused). The ring (`paint.ts` `ringPaint`) shows below `lz`, or always without
one; from `lz` the line's casing lights the water (the seam-free case). A step per tile zoom in `line-opacity` (no
`["zoom"]` in a filter), accepted by the spec validator for web and native (`tools/style-valid.test.ts`). The water map's
part halo and the T518 in-season outline are the same `OUTLINE_SHOWN`, so they follow. Data check
`pipeline/deliver/tiles/stream_polygon_lines.py`, run by the tile build after the export, on the files as written: every
stream polygon carries `lz` = its line's first zoom, or no `lz` and no line; no `lz` on standing water.

Tests (mutation-proved): `app/tools/stream-polygon-ring.test.ts` evaluates the ring per zoom, three themes, `selected` /
`part` / `inseason` (4 mutations, each 18 fail); `pipeline/tests/test_tile_stream_polygon_lz.py` runs the real
exporters on a small atlas and the check on hand-written files (6 mutations caught).

**The 0.9.0 tile build must include:** a full water export with this code (`python -m pipeline.deliver.tiles`, not
`--tiles-only` over old layer files: the check refuses layer files without `lz`, 17,049 offenders on today's files)
and the app built from this branch together with those tiles. App with old tiles = today's staging look (stream polygons
ringed over the casing again, no worse than staging); new tiles with an old app = `lz` ignored.

Shots (`T1024-shots/`, phone 412 px, water Map tab): `gravel-before-after.png` (z11/12/14/16), `oxbows-before-after.png`
(z11/13/15), `gun-before-after.png` (z8/11/13). Before = the staging site; after = this branch's dev web over staging
data with the staging atlas given `lz` from the live layer files (a scratch copy, the exact values the new export
writes). Not run on the emulator.

Stub routes (50b018058, the safe default from the review): a stream polygon whose route is a STUB — shorter than 0.3 of the
polygon's long side (its minimum rotated rectangle's) — is reported as no line, so it gets no `lz` and is always
ringed; the stub still draws as the creek. 239 polygons on the live atlas (2026-11-23; 14 over 0.05 km²), e.g.
Okanagan River Oxbows 1281106 (67 m route in a 57,000 m² loop). The build check now requires: `lz` = the line's first
zoom on a route that is no stub, or no `lz` with no line or a stub (5% slack each side of 0.3: the check measures the
written WGS84 geometry, the export BC Albers). Tests: 4 more mutations caught.

Depends: none. Size: S. Owner: look at the shots.
