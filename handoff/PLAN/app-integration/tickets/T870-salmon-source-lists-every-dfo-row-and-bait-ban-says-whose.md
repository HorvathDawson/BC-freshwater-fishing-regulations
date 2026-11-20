# T870 — the salmon card's Source (DFO) lists every DFO row behind its answers; each bait-ban line says whose it is

Status: DONE — c9dc6ba69, a6f79b38a (closed on the owner's word 2026-11-20; see the last section)
data release + feeds worker deploy carry it; pictures not taken (see Built).

Owner 2026-11-18 ~20:25 (via the app chat), Mamquam River, What you can keep, salmon card (screenshot
/Users/dawson.horvath/.claude/uploads/699dcd1b-ff73-4ed1-a69c-69feb621d839/7017ce4c-image.png), verbatim: "The source
dfo here doesn't show the rows that actually produce the results above. It should have all the rows that apply in
source  Also the bait ban here is confusing idk where it is from for example." ~20:27, verbatim: "Why is the bait ban
all year defined from that month to that month too".

The card shows "Salmon fishing in daylight only: …", "Bait ban (Apr 1–Mar 31)", "B.C. rules, also for salmon: Bait
ban: no bait of any kind." The Source (DFO) fold lists only the area-closure header ("There is no fishing for salmon in
waters of Region 2 flowing into Areas 28 and 29 except for the opportunities listed in the table below") and the chum
FN1167 notice.

## Fix
1. Source (DFO): every DFO row and header note that produced an answer on the card (the table rows for this water,
   the daylight note, each species' row, the notices), not only the closure header. The salmon file carries which DFO
   rows/notes each verdict came from; the fold lists them all.
2. Bait ban: each line says whose it is (DFO's row/note vs the provincial book page) and the same ban is said once.
3. A window that is the whole year (Apr 1–Mar 31) reads "all year", as core/regs/bait.ts `BAIT.WHEN_ALL_YEAR` already
   words the provincial list. Find which side formats it (the feed's `when` or salmon.ts near gearLead) and fix it there.

## Built (lane data/t869-t870-salmon, c9dc6ba69, 2026-11-19)
1. PROVENANCE: each DFO place's table rows AS PRINTED, the rows in force (`deliver.salmon.printed_rows`: a commented-out
   table's hidden rows left out) ride the release (`payload.pr`, location id -> rows) and the live feed
   (`locs[lid].pr`, `live.rows_by_location` / worker `rowsByLocation`, carried with a held place). Optional on both:
   an app before T870 ignores it; under a feed without it (a worker not yet deployed) a fed place lists no rows, never
   the release's. Core `salmonCard` `sources`: one group per place of the page's chains ("Region 2 · Mamquam River":
   "All | Apr 1 to Mar 31 | Bait ban", "Coho | Sep 15 to Jan 31 | 1 per day, hatchery marked only", "Chum | Oct 30 to
   Dec 31 | Non-retention — FN1167"; "Region 2 · Region 2 waters flowing into Areas 28 and 29": the default sentence),
   then "Region 2 Lower Mainland · page header": the notes the card's lines were read from (daylight -> hours, daily /
   possession / annual -> the bucket, min sizes / adult -> the sizes, Squamish aggregate -> its limit). Dropped notes
   (facility, measuring) stay out. The fold shows them under "DFO's rows behind these answers", before the rows' words.
   Every place of the chain is listed (the cascade's wider places too), not only the one answering today.
2. WHOSE: DFO gear lines end "— DFO" ("Bait ban (all year) — DFO"); DFO's plain bait ban and the B.C. rules' bait ban
   on one day are ONE line, "Bait ban: no bait of any kind — DFO (all year) and B.C. rules", and the B.C. line leaves
   bait out (fly-only stays). Core `gearLines`; the data `gear` is unchanged. Only a DFO line that is exactly "Bait ban
   (…)" merges; "No bait (all year)" (R6/R7 straddler) is said as DFO's line beside the B.C. one.
3. ALL YEAR: a DFO gear row whose dates hold every day reads "(all year)" (`deliver.salmon.gear_text`, worker
   `gearText`) — fixed where it is formatted (Python + worker). Verdict texts ("No fishing for chinook (Apr 1–Mar 31)")
   are unchanged: not asked, and they feed T882's "To be determined" reading and the goldens.
- Tests: `test_salmon_t869_t870.py`, core `salmonSources.test.ts` + `salmonFeed.test.ts` (feed merge, refusal),
  ui-native `SalmonCard.t870.test.tsx`; dfo vectors, `salmonFeed.golden.ts`, `salmonStraddle.golden.ts` re-cut; the
  live feed's size budget (`test_dfo_live`) 112 kB -> 135 kB for `pr` (~21 kB raw).
- Pictures: NOT taken — the live release has neither `pr` nor the new words, so a 390 px shot now would not show the
  change; take them on staging after the data release (Mamquam, Coquitlam).


## Owner 2026-11-19 ~06:40: the feed size budget
On the 112 -> 135 kB raise, verbatim: "okay we will work to shrink it down later." -> T905.

## Closed 2026-11-20

Closed 2026-11-20 on the owner's word ("Couldn't we just assume most of these are done already?"): 0.8.2 staging list item 1 is taken as checked (on production since 0.8.3).

Status line before closing: IN REVIEW — on production since 0.8.3 (data r-6258ffded676, feeds worker 0a3205f6; built c9dc6ba69, a6f79b38a; first on staging r-379e72fc9524); feed size budget raised, shrink is T905; owner: OWNER-TODO "Test 0.8.2 on staging (2026-11-19 morning)" (item 1).
