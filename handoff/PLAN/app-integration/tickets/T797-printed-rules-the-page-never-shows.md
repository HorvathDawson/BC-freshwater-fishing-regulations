# T797 — Printed rules the page never shows: "No angling from powered boats" (7 rules), dead fin fish as sturgeon bait, the Shuswap pier exemptions, "wild" on the Pitt Lake caveat

Status: PARTIAL (item 1 merged 7c077fdef (e39bcfed6); items 2 and 4 landed 45fff5e63 (8d7f17709); item 2's one-`says`-per-allowance 6c82b6d53, both allowances shown per the owner (1d3789b08); item 3, the Shuswap pier exemption, fixed as T789 K8 (dbaa009b9); left (Claude): item 5 (size sub-limit hint on other-fish rows, the Cowichan boats line's "on parts", the Halfway and Stellako part labels))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T797 | Printed rules the page never shows: "No angling from powered boats" (7 rules), dead fin fish as sturgeon bait, the Shuswap pier exemptions, "wild" on the Pitt Lake caveat | PARTIAL (items 1-4 landed: 7c077fdef, 45fff5e63, 6c82b6d53, dbaa009b9 (T789 K8); left (Claude): item 5) | e39bcfed6, 8d7f17709, 6c82b6d53 | general audit 2026-11-16, answers checker D1, B2, B3, B4, E5, E6 (`handoff/PLAN/audit-2026-11-16/answers.md`) |

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

## Item 1 fixed on a branch, 2026-11-16 11:31 (app chat; owner's go 10:36: "Go on all (Recommended)")
Branch `app-fix/t797-powered-boats` (not merged; app code, so it lands as its own merge with the Android benches).
- **Fix:** `core/src/regs/boats.ts` `inBoatRules` selects the circumstances in `BOAT_WHILE` (`in_boat`,
  `in_powered_boat`). The chip is the stored label, as for a boat: "No angling (from a powered boat) — downstream of
  Woodjam Bridge". It leads the boats line and the Methods "Boats" list and opens its rule sheet. No data release.
- **How wide it was (r-46e4355a7277, every shown water, every part, five days of the year; 103,415 answers):** 47
  parts of 35 waters hold a powered-boat rule and none drew it: "… upstream of dyke gates" 31 (the Nicomekl and
  Serpentine with 27 side streams), "No angling (from a powered boat)" 11 (Atnarko 7 parts, Bella Coola 2, Chilcotin
  2), "… downstream of the Falls Creek confluence" 3 (Clearwater), "…, Sep 11-Sep 30 — upstream of Henry Crossing
  bridge" 1 (Chilko, shown on its dates only, as the Kispiox's dated rule is), "… downstream of Woodjam Bridge" 1
  (Horsefly, the part below the bridge only).
- **No other token is dropped:** the frames hold five `while` tokens: `in_boat` (106 parts, drawn), `in_powered_boat`
  (47, now drawn), and `alone_in_boat`, `ice_fishing`, `set_lining` (rod counts and methods the Methods lines word;
  listed as `OTHER_WHILE`). `from_shore` is in the catalogue's vocabulary and in no frame.
- **Tests:** `boats.test.ts` (a powered-boat rule is a chip; the three other circumstances are not);
  `t670.release.test.ts` on the live release: the Horsefly below Woodjam Bridge has the line and the part above has
  none; the Chilko above Henry's Crossing has it on Sep 20 and not on Jul 15; and a province-wide walk: every rule
  held for an angler in a boat, powered or not, is on its page's boats line, and no `while` token is one neither list
  knows (so a new token fails the walk instead of vanishing). Two mutations, each caught: the old selection (3 tests
  fail), a known token removed from the lists (the walk names it).
- **A golden page moved, as intended:** `bc1_501` (Bella Coola, May 1) gains the chip, the Methods row and the rule
  sheet (38 lines added, none removed); two Methods display tests that listed that page's boat rows now list the new
  row first. `check:fast` rc 0; core, conformance, site builder, `ui-native/src/regs`, regs-web: all pass.
- **For the main chat (data, not changed here):** the Nicomekl / Serpentine rule reaches 27 side streams (Anderson
  Creek, Cloverdale Canal, …) with the words "upstream of dyke gates"; worth a look whether the book means the two
  rivers only.
- **Left:** the merge (benches + the full app suite); a "What's new" line with the next app version.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Go-ahead card: "Go on all (Recommended)" (item 1 is fixed by the app chat on branch app-fix/t797-powered-boats; items 2 to 5 are pipeline display).

## Item 1 fixed on a branch, 2026-11-16 (the app chat; NOT merged)
`app-fix/t797-powered-boats` @ e39bcfed6 (core `regs/boats.ts`: `BOAT_WHILE` = in_boat, in_powered_boat;
`OTHER_WHILE` = alone_in_boat, ice_fishing, set_lining; tests on the Horsefly and the Chilko and a province-wide
walk in `conformance/src/regs/t670.release.test.ts` that fails on a token no list knows; golden `bc1_501.json`
re-cut). Stacked on merge 3 as `merge/t797-powered-boats` @ 7c077fdef: `pnpm check` passed there (645 files,
6823 tests, 11:25). Six benches 12:28: regs-open, install, memory, tap-stalls PASS; bg-install failed at the
notification steps (the bench's own race, being fixed on `merge/t756-t760`); crash-bed failed two steps on "Maestro
Android driver did not start up in time" (the tool, the app was up on the map). OWED before it lands: crash-bed and
bg-install again on the same APK (tag `t797`, `--allow-stale`) once the bench fix is in, then it lands after merge 3.
Data note sent to the main chat: the Nicomekl / Serpentine rule reaches 27 side streams.

## Benches complete, 2026-11-16 15:12 (app chat, session -9c)
`merge/t797-powered-boats` with merge 3 landed in (cfe83b679): crash-bed PASS (1/1, 21:58Z run) and bg-install PASS
(1/1, 22:01Z run) on the same `t797` APK (`--allow-stale`: only bench files and the dev mini-release changed since it
was built). With the 12:28 runs, all six benches pass. Ready to land after the main chat's clear.

2026-11-17 ~15:52, owner on item 2: "Show it (Recommended)": the dead-fin-fish-for-sturgeon allowance shows on sturgeon waters (the app reverses D18 a; the pipeline already computes also_allowed). OWNER-TODO 15:52.
## Items 2-5 traced (lane/bugs-080b, 2026-11-17): the app's words, not the pipeline's
- Item 2 (dead fin fish for sturgeon): the pipeline's bait row carries it (`gear.py` `also_allowed`, the sturgeon rules'
  `when_targeting: WSG`); the app does not show `also_allowed` on purpose (`core/src/regs/bait.ts` header: decision D18 a).
  Showing it is the app's change, and a reversal of D18 a: the owner's call.
- Item 4 (Pitt Lake head "release all"): `pitt_lake.r3` keeps `origin: wild`; the in-part caveat's words are composed
  in the app (`core/src/regs/caveats.ts` `inPartBlock` / `answerTxt`, which has no origin). The app's.
- Item 5: the size-limit hint (E5) and the Cowichan boats line (E6) are the app's row words; the part labels (E7, Halfway,
  Stellako) not traced. Item 3 (Shuswap pier exemptions, an undrawn part) not traced.

2026-11-17 ~16:30, owner (app chat dialog, item 2 follow-up): the same `also_allowed` field also holds the set-lining
allowance (Region 6 and Zone 7A lakes: "Dead fin fish may be used, while set lining, in lakes"). Picked, verbatim label:
"Show both (Recommended)": every allowance the book prints under a bait ban shows on its waters. Duplicate labels on a
Fraser part (row rule + bait.r3) become one line when the pipeline emits one `says` per allowance (main chat, bugs-080c).

## Built (app), items 2 and 4 (branch `app-fix/t801-t797`, 2026-11-17; not merged)
- **Item 2** (owner "Show it", reversing D18 a; `decisions.md` D18 updated): `core/src/regs/bait.ts` says each bait
  element's stored `also_allowed` under the item, in its rule's label (`allowanceRules`, `noteWithAllowances`), each
  label once, and the bait source line cites those rules. On r-46e4355a7277 the Fraser (Region 2) and lower Harrison ✕
  "Fish or fish parts" say "Dead fin fish may be used, when fishing for white sturgeon — …". Two side effects to look
  at: (a) a Fraser part holding both the Fraser row's rule and the province's `bait.r3` says two near-identical labels
  (one `says` per allowance from the pipeline would make it one line); (b) the same field carries the set-lining
  allowance, so Region 6 / Zone 7A lakes now say "Dead fin fish may be used, while set lining, in lakes — …" (golden
  `x35p_1010`). Tests: `bait.test.ts`; `conformance/src/regs/t797.release.test.ts` (Fraser and Harrison say it, the
  Vedder does not); goldens `edge_x15p2_1010`, `x35p_1010` re-cut. Mutation (labels dropped) fails 2.
- **Item 4**: `core/src/regs/caveats.ts` `inPartItem` says the rule's stored `parts.conditions` after its `what`, joined
  as its label joins them: the Pitt Lake head reads "Trout and char — release all, wild only" (pitt_lake.r3 is the only
  undrawn-part rule with conditions in the release). Tests: `caveats.test.ts` (Pitt Lake); mutation fails 1.
