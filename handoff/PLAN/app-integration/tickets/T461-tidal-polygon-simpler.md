# T461 — The tidal-water polygon: simpler, quieter

Status: PARTIAL 2026-11-06 — f3038a2 (the quieter look, owner approved). Left: the polygon's simplification at the tile build, with the next release build (tiles and bundle share a vintage).

Owner: "the tidal polygon might be overwhelming stuff? should it be simplified more?"

## Today
`tidal_water` (layers.source.json, T166): the DFO tidal-waters polygon (the whole BC coast, estuaries, every island
hole) plus the footprints of stream pieces removed below a tidal boundary. It draws from z9 as a teal wash with an edge
from 0.4 px. The coastline is very detailed, so it is a lot of vertices and a busy edge along every shore and island,
and likely part of the frame cost at z9–z11 near the coast (T420).

## Work
1. Measure: vertices / bytes of `tidal_water` per tile at z9–z14 (the 10 heaviest tiles), and its share of frame time
   at z9–z11 over the Fraser estuary and Howe Sound (test bed, the pan proxy).
2. Simplify at the tile build: a per-zoom tolerance (coarse at z9–z10, finer by z13), drop island holes and slivers
   under an area threshold at low zooms, merge the stream footprints into the polygon. The tap answer must not move.
   A point inside tidal water stays inside at z ≥ 12; at the tidal boundary of a river (e.g. the Fraser at the
   Pattullo/Mission line, wherever the release puts it) the edge stays exact at z ≥ 12. That is where it matters for
   the rules: freshwater rules stop there.
3. Quieter look: no edge line below z12 (wash only), or a lighter wash. Shots at z9/z11/z13, before and after, sent
   to the owner, whose call the look is.
4. Tiles and bundle share a vintage (memory): a tile rebuild only with the next release build, never alone on staging.
Related: T166, T420 (frame cost), T457 (lake look). Size: M. Owner: approve the look.

## Look done (2026-11-06, f3038a2)
No edge below z12 (the outline ramp is `[[11, 0], [12, 1.0]]`; it ran along every shore and island from z6) and a
lighter wash (`opacity.tidal` light 0.4 → 0.32, dark 0.45 → 0.38). Shots of the Fraser estuary and Howe Sound at z9 / z11 /
z13 were approved by the owner. Seen on the way: in dark mode at z9, islands are a little fainter without the edge. The
repeated "Tidal water" labels (one per stream footprint) are the other busy part; not changed, since the owner kept them.
Left: Work 1–2 (measure, then simplify at the tile build) with the next release build.
