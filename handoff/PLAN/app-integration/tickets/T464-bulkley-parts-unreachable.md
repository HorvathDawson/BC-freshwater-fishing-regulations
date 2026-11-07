# T464 — Bulkley River: 6 of its 12 parts cannot be reached by ▲/▼

Status: OPEN (found 2026-11-06 while counting T460's changes; true before T460).

On the live release r-67b456b5e64b, stepping ▲/▼ from the Bulkley River's lowest part through `loadWaterGroup` reaches
6 of its 12 parts. Every other water with marks (649) reaches all its parts. The parts list still opens each part; only
the steps miss them.

## Work
1. Print the Bulkley's parts, marks (`part_mark`) and stretches, and find which parts no mark leads to. Check whether
   their marks are missing, point the wrong way, or the parts sit in another water's group.
2. If it's the release's marks, open a pipeline ticket (P09 marks). If it's core's reading, fix it in `waterNav.ts`
   with a test on the Bulkley's rows.
3. Add a release test: every part of every water is reached from its mouth (the T460 count did this by hand).

Related: T460. Size: S–M.
