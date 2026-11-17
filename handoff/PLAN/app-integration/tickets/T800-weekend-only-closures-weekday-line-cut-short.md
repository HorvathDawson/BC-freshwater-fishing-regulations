# T800 — A weekend-only closure reads wrongly from Monday to Friday: the "at certain times" line is cut at the first bracket (Skeena Section 4 zones say no "who" or name no zone; Stellako, Suskwa)

Status: IN PROGRESS (app chat; fixed and tested on branch `app-fix/small-looks-1116` @ cec527747, NOT merged: it rides the "small fixes" merge, which still needs the Android benches).

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

## Fixed on a branch, 2026-11-16 (the app chat; `app-fix/small-looks-1116`, not merged)
- 8b6bad68c: `timedOf` part (c) in `caveats.ts` uses the rule's own sentence (`r.export.label`), as the "some anglers"
  note does; the unit test in `caveats.test.ts` moved with it.
- cec527747: `app/conformance/src/regs/t800.release.test.ts` on the live release r-46e4355a7277: Skeena zone (b)
  (Chimdemash Creek to 1.5 km above the Zymoetz), zone (a) (Sedan Creek to the Shegunia) and the Suskwa are pinned
  word for word (Wednesday Aug 12 line == Saturday Aug 15 note); a province-wide walk (two Wednesdays, Aug 12 and
  Oct 14 2026, each against its Friday, Saturday and Sunday) finds every such line equal to its note (more than 100
  matched; about 110 s). With the old wording put back the three pinned cases fail.
- The Stellako shows no such line on the two weeks walked, so it is not pinned (not looked into further).
- In a scratchpad worktree the live-release tests are skipped unless `RELEASE_DIR=<main checkout>/data/release/<LATEST>`.
- Seen on the way, NOT looked into (data, the main chat's): Limonite Creek's page carries both Zymoetz weekend rules
  ("downstream of the Limonite Creek confluence" and "upstream of …"); 27 and 21 parts carry them.
