# T149 — Review page: mark slough complexes in bulk; spot-check the intermittent lakes

Status: DONE — e5ba7972 (page sources + marks backup); published as version 2 of the review page (2026-10-23; RETIRED
2026-10-28 — slough review is the review app's Sloughs tab only, see T55; final DB in `handoff/PLAN/sloughs/marks-final-2026-10-28.json`). The owner's 98 marks read identical before and after the
publish; marked records always show whatever the class filter. 144 complexes (359 pieces; only stream-ish classes
group: touch, or ≤ 150 m with the river's own line near the gap); Endako's complex flags its 199 ha marsh hub.
Intermittent: (a) on a course 10,676 · (b) isolated 21,939 · (c) named 102; 40 (a) + 10 (b) samples in collection
"spotcheck". Next: the owner marks; a fold of group (a) only after a dry run the owner approves.

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
3. Marks read back with ArtifactData; same artifact (since retired), existing marks
   kept (same ids).

Owner: the marks; the go on any bulk fold. Rebuild after folding: registry → atlas → binding diff → reach + deliver.
