# Fraser DFO marks written through grouping (2026-11-14, T742)

Read-only diagnosis of `data/curated/review/marks.jsonl` (uncommitted lines as of 22:46) and the T185 apply commits
on `data/curated/dfo_salmon/places/region-2.json` (b325fb216 .. 4976df1cb). Group membership at each step was
recomputed with `review.dfo.groups` on each commit's places files. Line numbers are marks.jsonl lines.

## What went wrong (plain words)

1. **A group save carried one extent onto wordings that name a different end.** At 22:36 the owner saved
   "from the downstream side of the CPR Bridge at Mission upstream to the Highway 1 bridge at Hope" and its group
   member "mainstem waters downstream of the CPR Bridge at Mission to the Highway 1 Bridge at Hope" as
   **CPR Mission -> Spuzzum Creek** (the extent of the "upstream of the CPR Bridge" places). Both wordings end at
   **Hope**. One click wrote both rows (lines 2554, 2555).
2. **The group save replaced the owner's own earlier answer without saying so.** Line 2546 (22:2x) had the owner
   put "mainstem waters downstream of the CPR Bridge ... to Hope" as Mission -> Hope (a new cut at the Highway 1
   bridge, "unsure"). Line 2555, written as a group member from another wording's screen, replaced it with
   Mission -> Spuzzum. The pre-save line only says "takes this extent", never "replaces what you saved".
3. **"Same place" in the water list is chained by identical extent.** `checks.same_place` unions places that hold
   the identical extent, transitively. Once Mission -> Spuzzum sat on "upstream of the CPR Bridge" (x2) and on
   "mainstem waters from the downstream side ... to Hope" (line 2552), all five wordings (both "upstream", both
   "... to Hope" ones, and "from the downstream side ...") were shown under one "These 5 wordings are the same
   place" heading. That heading is what looked "all messed up".
4. **"Not the same place" breaks the wording's real link too.** A `not_variant_of X` mark clears the wording's
   `duplicate_of` whatever it pointed at, and sets it contested (no alike-word grouping, either side). At 22:37
   "upstream of the CPR Bridge at Mission" (~2, live) was taken apart from "mainstem waters from the downstream
   side ..." (line 2557) and that wiped its real link to "mainstem waters upstream of the CPR Bridge at Mission, BC"
   (commit 4b9b49c9b). He re-linked it at 22:44 (line 2559, bf9b97b2d); the next "not the same place" a minute
   later (line 2560, 4976df1cb) wiped it again. Now both "upstream" wordings are contested and in no group.
5. **Two groupings that disagree.** The save path (`review.dfo.groups`) only lets a dormant wording join a live one;
   the water list also follows `duplicate_of` the other way. The live "mainstem waters downstream of the CPR
   Bridge ... to Hope" has `duplicate_of` -> the dormant "mainstem waters from the downstream side ... to Hope"
   (confirmed): the list shows them together, the save path treats the dormant one as its own place.
6. **Older wordings are hidden in the queue.** `checks._dfo_places` skips every non-head group member, so a member's
   state is only a count on its head ("N older wordings unconfirmed").
7. **Double save.** Lines 2549 and 2550 are the same "not the same place" mark twice (one click, two posts).

## Fraser DFO marks written by a group save (4) — do not trust their extent

| line | wording (location id) | says | group | time |
|---|---|---|---|---|
| 2547 | 2:fraser-river:mainstem-waters-upstream-of-the-cp~2 (live) "mainstem waters upstream of the CPR Bridge at Mission" | verified, Mission -> Spuzzum | grp:...upstream-of-the-cp~2 | 2026-11-14 ~22:23 |
| 2548 | 2:fraser-river:mainstem-waters-upstream-of-the-cp (dormant) "... at Mission, BC" | verified, Mission -> Spuzzum (member) | grp:...upstream-of-the-cp~2 | ~22:23 |
| 2554 | 2:fraser-river:from-the-downstream-side-of-the-cp (dormant) "... upstream to the Highway 1 bridge at Hope" | verified, Mission -> Spuzzum: **wrong end** | grp:...mainstem-waters-downstream-of-the | ~22:36 |
| 2555 | 2:fraser-river:mainstem-waters-downstream-of-the (live) "... to the Highway 1 Bridge at Hope" | verified, Mission -> Spuzzum (member): **wrong end, replaced line 2546** | grp:...mainstem-waters-downstream-of-the | ~22:36 |

The two "upstream" ones (2547/2548) are plausibly right (both wordings say only "upstream of the CPR Bridge"), but
were one click: re-check each.

## Other Fraser marks to re-check (not group-written, but caught up in it)

- 2552 2:fraser-river:mainstem-waters-from-the-downstrea "... from the downstream side ... to the Highway 1 Bridge
  at Hope, BC": verified Mission -> Spuzzum — same wrong end as 2554/2555.
- 2546 superseded by 2555 (his own Mission -> Hope answer is no longer in force).
- 2549/2550, 2556, 2557, 2560: "not the same place" marks; 2557 and 2560 each also wiped a real link (defect 4).
- 2559: "same place" (~2 with upstream-of-the-cp), undone by 2560.

Group marks elsewhere (older, not Fraser): Nanaimo 2442-2443 (11-02), Chilliwack-Vedder 2479-2482 and 2500-2503
(11-09), Stave 2525-2526 (11-09). Shown in the app as "marked in a group" after T742.
