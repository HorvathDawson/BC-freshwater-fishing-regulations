# T419 — Salmon River (Region 2) is not simplified enough when zoomed out

Status: IN REVIEW — b40de633d: `SIMPLIFICATION` 5 -> 10 (z4-z12; z13 unchanged). Left: the next tile build, the same
views before/after + tile bytes, then the owner's look.

## Work
1. Look at the Salmon River (Region 2, Fraser Valley / Langley) on the map at each zoom from z8 up to z13 (emulator +
   web). Screenshot the bad zooms. Find why its line keeps full detail when zoomed out: tippecanoe simplification on
   the stream layer (`pipeline/deliver/tiles`), a per-feature detail/`-pS`-style setting, the part line, or how many
   sections it is split into (many short pieces simplify badly).
2. Check whether it is one water or a class (meandering lowland streams with many short sections), and list others.
3. Fix with a measured before/after: screenshots at the same zooms, tile sizes. A tiles change needs a tile build
   (heavy, one at a time) and ships with a data release; tiles and bundle must share a vintage.
Size: S to look, M if the fix is a tiles change. Owner: phone/web look after the fix.

## Look (2026-11-06, chat 3, staging r-5a5999c94588; shots in T419-shots/)
- **Not many short pieces:** the Langley Salmon River is 10 features; the main stem is ONE order-4 line of 420 points,
  drawn from z9. Only 17 of its points are shared with other streams (tributary mouths), so
  `--no-simplification-of-shared-nodes` is not what keeps it detailed.
- **The tiles do simplify it:** points of "Salmon River" in the shipped tiles — z9 33, z10 108, z11 198, z12 316,
  z13 786. At z10 that is a point every ~230 m, and the tolerance (`SIMPLIFICATION` 5 units of the 2,048 low grid) is
  ~95 m, about 0.6 px. So every meander bend bigger than a pixel survives, and a tightly meandering lowland river
  draws as a sawtooth at z9–z11 (`t419-out2.png`), most of all under the thick highlight / closed hatch.
- **A class, not one water:** the angular look shows on other meandering streams at the same zoom
  (`t419-z10-plain.png`, the closed streams east of Chilliwack).
- **Fix to try (tiles change → tile build, ships with a release):** a larger tolerance BELOW z12 only (e.g. 5 → 10–15
  units at z≤11; z12–13 unchanged), measured before/after on the same views and the tile bytes. Optionally a round
  line-join on the stream layers. Owner looks at the shots before it ships.

## After (2026-11-06, staging r-4230bc0c1dd5, `SIMPLIFICATION` 5 → 10, 117f12472)
- Same views replayed on staging: `t419-open-after.png`, `t419-out2-after.png`, `t419-z10-plain-after.png`;
  side-by-side crop of the zoomed-out view: `t419-out2-compare.png` (left before, right after).
- Salmon River points per zoom: z9 33→20, z10 108→71, z11 198→128, z12 316→210, z13 790→790.
- Tiles 202.8 MB → 194.1 MB (−8.8 MB). Smoke 18/18 on staging.
- The sawtooth is mostly gone at z10; the line keeps its shape. Owner look left: if it is still too angular,
  next step is a round line-join on the stream layers (app style, no tile build).

## Variants measured (2026-11-06, owner: "oxbows dont become a straight line ... other streams got over simplified")
Fraser Valley clip (4,577 stream features, -122.75..-121.50 x 48.98..49.36), tiny tiles per variant, headless
MapLibre GL JS, plain style of the app's widths. Sheets (one per zoom, labelled rows): `t419-variants-z8.png` ...
`t419-variants-z12.png`. A = S5 two bands (before), B = S10 (staging), C/D = S5/S10 + `--visvalingam`, E = S5 one pass
per zoom, L = per-zoom passes + smoothing (`pipeline/deliver/tiles/smooth.py`: Gaussian over arc length, 4 px, 3
passes, every shared node and line end pinned). Stream vertices z9 / z10 / z11 / z12 (whole clip):
A 1,697 / 3,903 / 7,874 / 16,636 · B 1,513 / 3,093 / 5,760 / 11,657 · D 1,582 / 3,181 / 5,660 / 11,002 ·
E 987 / 2,871 / 6,729 / 15,508 · **L 648 / 1,770 / 4,469 / 11,437**. Tile KB z10: A 15.9, B 14.4, L 11.9.
Salmon River turning per 100 px at z10 (the sawtooth): A 1,241°, B 812°, D 453°, L 244°. Gentle streams' 95th-pct
distance from the true line: A ~0.8 px, B ~1.4–1.7 px (chorded), L ~1.5 px (rounded, on course).
Visvalingam keeps the zigzag (DP's problem is not the fix). Pan frame time at 4× CPU throttle (800×600, z9–z11):
8.3 ms for A, B and L alike — at the vsync cap; this stream layer alone is not the frame-time cost in GL JS, the gain
is vertices/bytes per tile (parse, upload, memory).
**Chosen: L.** In the pipeline: `SMOOTH_STREAMS`, per zoom below the top a WATER pass (stream, under_lake, lake,
wetland, tidal_water, not_fishing_water; `WATER_SIMPLIFICATION` 5) and an OTHER pass (`SIMPLIFICATION` stays 10 — at 5
the parcels cost +37% bytes at z11), the top zoom unsmoothed. (L at S=10 measured too: gentle streams stray ~2.2 px.)

## Full build (2026-11-06, heavy.sh, NOT promoted/uploaded)
`data/generated/side/t419/tiles/atlas.pmtiles`, tile version 269777be890cc2b3.55f8a4cfc1b5 (same section handles as
the live r-4230bc0c1dd5 bundle), 194.3 MB (staging 194.1, before T419 202.8), 721 s. Fraser Valley tiles
(-122.75..-121.50 x 48.98..49.36), staging → this: stream vertices z8 1,161→1,110 · z9 3,496→1,047 · z10 4,577→2,245 ·
z11 6,338→4,676 · z12 13,769→12,804 · z13 unchanged; all layers z9 6,749→4,666, z10 12,224→10,811, z11/z12 flat;
bytes z9 21.5→18.7 KB, z10 61.4→59.3 KB, z11/z12 ±1%. Left: promote with the next release, owner look on staging. App complement (not done; app/packages/map not
touched): the `stream` layer has no `line-join` (miter); round joins soften what kinks are left (last row of each sheet).
