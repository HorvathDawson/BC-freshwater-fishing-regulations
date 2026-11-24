# T1024 — "Weird highlighting on the lake" (Gravel Slough)

Status: IN REVIEW — fixed in 6704a3504 + d298683a7 + 50b018058 (lane L29, in the 0.9.0 tile build) + 3781d22a5, ff79377f1, da40871a5 (stub outline, lane L10 0.9.1: needs the 0.9.1 tile build); owner: OWNER-TODO "T1024 Gravel Slough highlight: look at the before/after shots" (+ `T1024-shots/stub-oxbows-*.png`)

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

## Stub polygons: one outline, not two (lane L10, 0.9.1, 3781d22a5)
The #81 review: a stub polygon has no `lz`, so it is ringed wherever it is drawn, and where its stub line is drawn the
stub's own selection casing ran inside the ring — the double outline again. Measured on the live 0.9.0 layer files: 270
stream polygons carry no `lz`, 21 with no line and 249 with a stub (one line each); 201 stubs are drawn BEFORE their
polygon (McNaughton Creek: line z7, polygon z10), 24 at the same zoom, 24 after (the Oxbows 1281106: polygon z11, line
z13). So the casing cannot just skip stubs: below the polygon's zoom the stub is all there is of the water.

Fix: the export writes `pz` on the stub line, the first zoom its polygon is drawn at (`export_streams`, from the
`poly_z` it already computed; absent on every other line). The selection casing and look C's glow (`paint.ts`
`casedOnly`, the one rule, wrapping both opacities) are opacity 0 on a line from its `pz`: a step per tile zoom, as the
ring's `lz`. Not a filter: one was tried and split the casing's bucket from the stream's (`lab.test.ts`, the map lab's
one-bucket switch, caught it in the app suite); the opacity version is pixel-identical in every shot. The ring is unchanged. Browse selection, part halo and the
T518 in-season outline are all that casing, so all three follow. The casing and ring are runtime layers
(`runtime-style.ts`), not `layers.source.json`: `pnpm style:build` changes nothing. Build check
(`stream_polygon_lines.py`): every line of a polygon with no `lz` carries `pz` = the polygon's first zoom as written;
no other line carries `pz` (`stub_without_pz`, `pz_not_the_polygon_zoom`, `pz_off_a_stub`). Run over the live layer
files given the values the export writes: 17,070 stream polygons, 249 stub lines with `pz`, no offenders. Today's files
(no `pz`) are refused, so the 0.9.1 tile build must be a full water export again.

Mixes (pixel-diffed, Oxbows z12/z14/z16, `stub-oxbows-matrix.png`): old app + new tiles = today (pz ignored); new app +
old tiles = today (no line has pz). Only new + new changes, and only on the stub.

Tests: `app/tools/stream-polygon-ring.test.ts` evaluates the casing's and the glow's opacity per tile zoom (selected and part) (plain line,
side channel, stub pz 10, stub pz 4), three themes; `pipeline/tests/test_tile_stream_polygon_lz.py` the export's `pz` and
five new check cases; `selection-look` / `parity` pin the look inside `casedOnly`. 10 mutations, all caught (4 app, 2 export, 4 check).

Shots (phone 412 px, water Map tab, part 1 lit, z12/z14/z16): `T1024-shots/stub-oxbows-light.png`,
`stub-oxbows-dark.png`. Before = today's app over the live 0.9.0 atlas; after = this branch over a scratch copy of that
atlas with `pz` written into the stub lines' tiles (835 tiles, the exact values above). Web only, not the emulator.

Review of #111 (2026-11-24): (1) `pz` only on a stub that lies INSIDE its polygon (within 5 m, `INSIDE_M`): a stub
that leaves it gets nothing, so the tapped line stays cased; the check proves it from the written geometry
(`pz_outside_its_polygon`; `stub_without_pz` only for an inside stub). The review measured 172 of 249 stubs more than
25% outside (Oxbows 0 m inside, Nazko 397 m away); that does not reproduce on the same live files: measured twice, on the
written WGS84 layer files (local metres) and on the atlas's own BC Albers `waterbody_polys.pkl` / `geometries.pkl` by
node id, ALL 249 stubs lie wholly inside their own polygon (7,760 m of 7,760 m; Oxbows 1281106 lake:328985783 67.6 m
inside, Nazko 1706141 lake:329431032 163.2 m, Khimatlque 1588954 lake:329313096 163.9 m; distance 0). So the guard
changes no tile today. (2) Shots `stub-nazko-{light,dark}.png` (z10/z12/z15): its 163 m stub crosses the polygon's
neck; before, a casing across the ringed polygon; after, the ring alone. (3) `stream-polygon-ring.test.ts`: unlit and
in-season rows on lines without `pz` (dropping the `["has","pz"]` guard gives MapLibre's fallback opacity 1 on every
unlit line: 4 tests fail). (4) Tile size: `pz` adds 8.7 KB uncompressed to 835 tiles (at most 22 B a tile) of a 203 MB
archive.

**The 0.9.1 tile build must include:** a full water export with this code (as 0.9.0's did for `lz`), and the app from
this branch.

Depends: none. Size: S. Owner: look at the shots.
