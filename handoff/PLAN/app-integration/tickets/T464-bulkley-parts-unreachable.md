# T464 — Bulkley River: 6 of its 12 parts cannot be reached by ▲/▼

Status: DONE 2026-11-06 — 1470734, 723f43116 (found while counting T460's changes; true before T460). Owner: phone check on 0.6.8.

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

## Found and fixed (2026-11-06, 1470734 + 723f43116)
**Cause (app, not the release):** the upper Bulkley's "Morice River confluence → Source" is 7 stretches, and 6 of
them are one section each upstream (about 126.05–126.15 W). The release writes no mark between stretches that a gap
parts. Each of those 6 stepped ▼ to the one before it by the order. But the main stretch's ▲ had a mark of its own (to
the side channel), so `stepsFrom` never asked the order, and nothing stepped up into them.
**Fix (`waterNav.ts` `fallsBackTo`):** a part also steps to the next part in the order when that part is another stretch
of the same part and its own step back falls to this one by the order (no mark that way). So ▲ and ▼ agree. Two wider
rules were tried and measured first. Neither shipped:
- any next stretch of the same part: 17 waters changed, and it skipped the part between two stretches (the Nahatlatch,
  T460's kind).
- any part next in the order: 10 waters, with side channels stepping to unrelated parts.
**Release-wide** (r-67b456b5e64b, 650 waters with marks, every step before vs after): **1 water changes, the Bulkley**,
and every part of every water is now reached from its mouth (was: the Bulkley, 6 of 12). Mutation-checked: the unit
test fails without the step, and fails without the same-part limit.
**Release test (723f43116):** `tools/every-part-reachable.test.ts` walks every water with 2+ parts on the live release. It
fails on the old code naming the Bulkley's six stretches. It is LIVE-skipped (listed in no-silent-skips).
