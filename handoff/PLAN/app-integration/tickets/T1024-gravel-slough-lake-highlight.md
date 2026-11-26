# T1024 — "Weird highlighting on the lake" (Gravel Slough)

Status: IN REVIEW — fixed in 6704a3504 + d298683a7 + 50b018058 (lane L29, in the 0.9.0 tile build) + 3781d22a5, ff79377f1, da40871a5 (stub outline, lane L10 0.9.1: needs the 0.9.1 tile build); owner: see ## Owner (+ `T1024-shots/stub-oxbows-*.png`)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1024 | Staging report: "weird highlighting on the lake" at Gravel Slough | IN REVIEW — owner: see ## Owner (+ stub-oxbows shots) | 6704a3504, d298683a7, 50b018058, 3781d22a5, ff79377f1, da40871a5 | ring a stream polygon only below its line's zoom (`lz`, 0.9.0); a stub inside its polygon carries `pz` and is not cased inside the ring (249): needs the 0.9.1 tile build |

Source: STAGING report `3444007425e9d3bd`, 2026-11-23 05:32Z, Android 0.8.5 (staging build), water · map, Gravel Slough
(`gnis:8009`) part 0, zoom 14.3 at 49.202, -121.904. Verbatim: "Weird highlighting on the lake here".

## What
Reproduced on web against staging (tiles `4ae7b62955bc9990.fc51e82f7c91`): the "lake" is the slough's own POLYGON
(`lake` source-layer, `water` = "stream", drawn by `lake_flowing`). On the part's selection two outlines lit it: the
lake's selection RING (`selection-ring-lake`, no filter, so it took stream polygons too) round EVERY section's piece of
the polygon, and inside it the CASING of the slough's route (`export_streams`, same section id). Not data, not the T467
shoreline or the T1000 outside drawing.

Done:
- 0.9.0 (6704a3504, d298683a7, 50b018058, lane L29): every stream polygon carries `lz` (its route line's first zoom in
  `stream`); the ring (`paint.ts` `ringPaint`) shows below `lz`, or always without one; a STUB route (shorter than 0.3 of
  the polygon's long side; 239 on the live atlas) gets no `lz`. Build check `pipeline/deliver/tiles/stream_polygon_lines.py`.
  Tests `app/tools/stream-polygon-ring.test.ts`, `pipeline/tests/test_tile_stream_polygon_lz.py`. Shots
  `T1024-shots/{gravel,oxbows,gun}-before-after.png` (web, not the emulator).
- 0.9.1 (3781d22a5, ff79377f1, da40871a5, lane L10): a stub line INSIDE its polygon (within 5 m, `INSIDE_M`) carries `pz`
  = the polygon's first zoom; the selection casing and look C's glow (`paint.ts` `casedOnly`) are opacity 0 from `pz`, so
  a stub polygon gets one outline, not two (249 stubs, all wholly inside their polygon, measured twice). Check cases
  `stub_without_pz`, `pz_not_the_polygon_zoom`, `pz_off_a_stub`, `pz_outside_its_polygon`. Old app + new tiles = today;
  new app + old tiles = today (`stub-oxbows-matrix.png`). Shots `T1024-shots/stub-oxbows-{light,dark}.png`,
  `stub-nazko-{light,dark}.png` (web only).

## Left
- The owner's look (below).
- **The 0.9.1 tile build must include:** a full water export with this code (`python -m pipeline.deliver.tiles`, not
  `--tiles-only` over old layer files: the check refuses files with no `pz`), and the app from this branch.

Depends: none. Size: S. Owner: look at the shots.

## Owner

- [ ] 2026-11-23 — Look at the Gravel Slough highlight (`handoff/PLAN/app-integration/T1024-shots/gravel-before-after.png`, `oxbows-before-after.png`, `gun-before-after.png`, and the `stub-*` pair) and say if it reads right: a drawn slough's line is outlined alone, no ring round each polygon piece; an undrawn one keeps its ring.
