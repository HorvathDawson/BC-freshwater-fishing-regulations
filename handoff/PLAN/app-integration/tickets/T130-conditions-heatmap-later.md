# T130 — Conditions: zoom out further before the basin heat map takes over (+ Change chip under Level)

Status: DONE — b4f3a40f, cd95bb63, review c9d48ea7 (lane wf_58c33d34-a73-1). Handover z8 (was z10), one number: the basin layer's
maxzoom; the Change chip stays greyed "Flow only" under Level/Temperature and a tap moves to Flow. No tile rebuild
needed; T64's build picks up BASIN_HANDOVER_Z = 8 and simply stops writing z8 basin tiles.

## Owner
"make it so conditions needs more zoom out to get heat map; should be able to zoom out quite a bit more first."
Also (same session): the Change chip "disappeared" — it is hidden under Level/Temperature (Flow only) and that choice now
persists, which reads as a bug.

## Today
The basin field (heat map) draws below the handover zoom z10 (owner 2026-10-22 earlier: "the watershed colouring
happens at a slightly higher zoom"); rivers carry the colour from z10. `basins` source stops at z8 and overzooms
(layers.source.json `basins` $comment; BASIN_HANDOVER_Z in pipeline/deliver/tiles/layers.py; app tap handling; the
seam layer's width curve, tools/handover.test.ts).

## Want
1. Rivers keep their colours to a LOWER zoom; the heat map only once zoomed well out. Find the lowest handover where
   the coloured rivers still read (which stream orders the tiles carry at z7/z8 — tile minzooms by Strahler order — and
   whether thin rivers at z7 are legible); propose a number (e.g. z7 or z8), measured with screenshots at z6–z10 in
   light/dark. If the tiles lack rivers below z9 for small orders, the field may need to show under them (per-order
   handover) rather than leaving blank land — decide with shots. Keep the handover in ONE constant shared by tiles,
   style and tap handling (the "one function" rule); the seam curve follows it.
2. Change chip: under Level/Temperature show it greyed with "Flow only" (core words) instead of dropping it; tapping it
   switches to Flow (or says why). Whole-value test.

## Acceptance
Shots before/after at z6–z10; handover tests updated; tap handling agrees (a tap below the handover hits a basin, above
it a river); if a tile rebuild is needed (basin maxzoom), it rides T64's tile window.

## Owner questions (new, with the default built)
- Q-T130a: the handover is z8 — rivers from z8, the heat map below. At z7 the coloured rivers break into hairline
  fragments on blank land (shot), so z8 is the lowest that reads. Go lower with a per-order field under the big rivers?
  Default: no — one answer per zoom (owner 2026-10-22, no double layer).
- Q-T130b: the arrows on the rivers still start at z10 (their own floor), so z8–z10 shows coloured rivers without flow
  arrows. Default: as built.
