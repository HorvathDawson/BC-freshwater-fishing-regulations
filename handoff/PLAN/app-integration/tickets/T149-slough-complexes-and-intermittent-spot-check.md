# T149 — Review page: mark slough complexes in bulk; spot-check the intermittent lakes

Status: OPEN — after T148.

Source: owner 2026-10-23: "likely stream ones that are all in a region on the same river often are part of the same
wetland like slough areas. we could use this to label items in bulk" and "the intermittent polygons maybe I could do a
spot check on them and assuming the spot check shows all not lakes we can turn them all into streams regs".

## Work
1. COMPLEXES: group candidates by same river (stream item) + same region + physically connected (touching, or joined by
   the river's own lines within a short gap). Each complex: name (from any named piece), pieces, area, map with every
   outline. One mark sets every piece; pieces can be flipped as exceptions; existing per-piece marks always win; a
   complex whose owner marks all agree shows "pre-filled from your marks"; mixed marks flag the grouping.
2. INTERMITTENT SPOT CHECK (GB15300140 Lake–Intermittent, 32,717): split (a) on a river's course (main-flow line through
   it, or a stream in and out) — the slough/side-channel candidates; (b) isolated — expected seasonal potholes, stay lake
   water under region rules; (c) named lakes — stay lakes. A tab with ~40 stratified (a) samples (by region/size; as
   complexes where they group) and ~10 (b) samples, imagery + inside lines + code. If (a) comes back nearly all
   "not a lake", fold group (a) into its river as stream water with an exceptions list — DRY RUN with counts and
   examples to the owner before any registry change (memory: bulk reclassify needs tight filters + confirm).
3. Marks read back with ArtifactData; same artifact (https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8), existing marks
   kept (same ids).

Owner: the marks; the go on any bulk fold. Rebuild after folding: registry → atlas → binding diff → reach + deliver.
