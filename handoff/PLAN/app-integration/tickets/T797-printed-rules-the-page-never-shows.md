# T797 — Printed rules the page never shows: "No angling from powered boats" (7 rules), dead fin fish as sturgeon bait, the Shuswap pier exemptions, "wild" on the Pitt Lake caveat

Status: OPEN (display: app core page / pipeline display model; the rules are parsed and stored).

Source: general audit 2026-11-16, answers checker D1, B2, B3, B4, E5, E6 (`handoff/PLAN/audit-2026-11-16/answers.md`).

1. **"No angling from powered boats" is never shown** (errs OPEN): Atnarko / Bella Coola mainstems (p.43), Horsefly
   from Woodjam Bridge (p.45), Chilcotin, Chilko above Henry's Crossing (Sept 11-30), Clearwater below Falls Creek,
   Nicomekl and Serpentine above the dyke gates. Parsed (`method:angling@angler=in_powered_boat`) and worded in the
   store's `display_rule`; the page draws `in_boat`, not `in_powered_boat`.
2. **Dead fin fish as sturgeon bait** (p.8, p.23; rulings W2, G1): Region 2 Fraser parts 1 to 4 and lower Harrison
   show "Not allowed: Fish or fish parts" with no note (errs closed). The lower Pitt was not run.
3. **Shuswap Lake**: the community-pier exemptions at Salmon Arm (p.32) are an undrawn part "held as a note" that is
   never drawn.
4. **Pitt Lake head**: the book says "Wild trout/char catch and release"; the caveat says "release all".
5. Smaller: other-fish rows with a size sub-limit (Region 4 kokanee "no more than 5 over 30 cm"; Zone B pike and
   walleye) show no hint on the row (it is one tap away); the Cowichan boats line drops "on parts" from the speed
   limit; part labels on the Halfway and Stellako.

## Item 1 traced (the app chat, 2026-11-16 05:00): it is the app's core, the stored model is right
On r-46e4355a7277 the stored gear answer holds `{"while": ["in_powered_boat"]}` with its label ("No angling (from a
powered boat) — downstream of Woodjam Bridge" on the Horsefly; "… upstream of dyke gates" on the Nicomekl); the page's
boats line is empty. `app/packages/core/src/regs/boats.ts:108` `inBoatRules` keeps a clause only when its `while`
includes exactly "in_boat". Fix (the app chat's, with the next app build; no data release): select both tokens, a test
on a powered-boat water, and a check that no other `angler` token is dropped the same way (`alone_in_boat`,
`from_shore`). Waits on the owner's go.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Go-ahead card: "Go on all (Recommended)" (item 1 is fixed by the app chat on branch app-fix/t797-powered-boats; items 2 to 5 are pipeline display).
