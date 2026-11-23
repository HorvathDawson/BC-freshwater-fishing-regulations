# T1024 — "Weird highlighting on the lake" (Gravel Slough)

Status: IN REVIEW — fixed in 6704a3504 (lane L29); owner: OWNER-TODO "T1024 Gravel Slough highlight: look at the before/after shots"

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

Fix (6704a3504): the two selection rings take only standing water — the `lake` / `wetland` fills' own filter
(`water != stream`). A stream's polygon is lit by its route's casing alone, as every other stream; a lake or wetland is
ringed as before. One style (`runtimeStyle`) for web and native, so Android takes the same change. Test
`app/tools/stream-polygon-ring.test.ts` evaluates each ring's filter with MapLibre's evaluator against the fills, every
theme; proved by mutation (no filter: 3 fail; `water == lake`: 3 fail).

Shots (`T1024-shots/`, phone 412 px, Map tab, part 0): `before-z14/z15/z16-staging.png` (the staging site) and
`after-z14/z15/z16.png` (this branch's dev web against staging data; the dev server's fallback font differs, the map
is the point). Not run on the emulator (web shows it; the style is shared).

Depends: none. Size: S. Owner: look at the shots.
