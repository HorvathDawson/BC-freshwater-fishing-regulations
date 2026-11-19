# T870 — the salmon card's Source (DFO) lists every DFO row behind its answers; each bait-ban line says whose it is

Status: OPEN (main chat: the salmon file's provenance; app chat: the render). After production unless the owner says
otherwise.

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
