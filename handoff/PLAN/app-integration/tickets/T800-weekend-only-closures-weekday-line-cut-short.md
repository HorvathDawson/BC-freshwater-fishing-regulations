# T800 — A weekend-only closure reads wrongly from Monday to Friday: the "at certain times" line is cut at the first bracket (Skeena Section 4 zones say no "who" or name no zone; Stellako, Suskwa)

Status: OPEN (taken by the app chat 2026-11-16, not started; app core: `app/packages/core/src/regs/caveats.ts` about line 256).

Source: the Skeena Section 4 licence check of 2026-11-16 on release r-46e4355a7277 (owner: "we should handle the weekend
zones properly too. Make sure licensing info is done properly there").

## What is wrong
On Saturdays and Sundays the closure note is right ("Closed to some anglers … between the Shegunia River confluence
and the Sedan Creek confluence"). From Monday to Friday, Jul 1 to Dec 31, the "At certain times" line is built with
`describeShort()`, which for this kind of rule falls back to the printed text and cuts everything from the first " (":
- zone (b): "Chimdemash Creek confluence to 1.5 km upstream of Zymoetz River confluence, July 1 - Dec 31, · Sat, Sun"
  (it does not say who is closed out, so it reads as a weekend closure for everyone);
- zone (a): "Angling prohibited for non-guided non-resident aliens in Skeena River Section 4 on Saturdays and Sundays
  in two zones: · Sat, Sun" (it never names the zone).
It touches every weekend-only closure in the province.

## What to build
Use the rule's ready-made sentence (`r.export.label`, as the Saturday note at line 190 does). Test first on Skeena
zones a and b, the Stellako and the Suskwa. Owner 2026-11-16 on the related look (an angler "From outside Canada"
still sees "Open" on a zone Saturday, the closure as a note): "Note is enough (Recommended)".
