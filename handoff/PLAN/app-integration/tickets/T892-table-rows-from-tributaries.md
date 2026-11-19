# T892 — "What you can keep" table: a row that comes from another water's tributary rule says so

Status: PARTIAL — in 0.8.2 (8caa80c99, three reviews); left (Claude): the trout table's own rows need a per-row line from the pipeline (main chat), 1,777 tables partial.

Owner 2026-11-19 ~00:10 (app chat), verbatim: "The table should say if it came from tributaries too.."

Earlier (2026-11-18 ~23:35, T883): "Like tributary rules (...) yes label as from tributarie inheritance." T883 labels
those steps on the "How this was decided" sheet ("<that water> · its tributaries"). This ticket carries the same fact to
the table: a row (or its DFO / B.C. mark) whose rule is another water's row carried down to its tributaries names it
("from <that water>'s tributary rule" or the shortest wording that reads right), from the same input T883 reads, never
re-derived. Rows from the water's own rules and region rules are unchanged. Pictures at 390 px, light and dark, of a
water with such a row, before it lands.

## What was built (2026-11-19 ~00:33, lane app/t892-table-tributary)
- Wording: one line under the row's name, sub colour, tokens only: **"Dutch Creek’s rule for its tributaries"** (two
  waters: "A’s and B’s rules for their tributaries"); the B.C. tag is unchanged. Read once: in the row's text (an Other
  fish row) or in the fold's spoken name (the trout fold).
- Same input as T883, by construction: the row's line is the sheet's own flow (`ladderOf` / `troutFlow` → `flowOf` →
  `FlowVM.down`), checked against the row's STORED answers, one per fish × origin it covers (each fish's hatchery and
  wild answer; a twin trout row its own origin): in every one the rule that wins it (`win`) and, where its number was
  lowered (`narrow`), each rule stored as narrowing it is a rule the part's ruleset holds `via` trib (rank 1) and a
  deciding step the rail keeps; the line names those waters as the kicker does. So a step that only sets one origin's
  size or cap (Lonzo Creek's hatchery size rule on Shamrock and Kilgard Creeks, review 2) or decides some fish only (a
  Bass row whose largemouth are the region's) labels nothing; nor does a row where another rule stored as lowering the
  number ("narrows", or "limit" on a subcap; `narrow` can stay unset) is a deciding step on the rail (review 3). A
  region size cap beside a tributary rule (Nygaard Creek's "1 over 50 cm") still gets the line: the owner's call. `OtherFishRowVM.from`, `GroupVM.from` (a quota group's
  Details fold, set in `groupOf`); a name ending in s takes only ’ ("Twin Lakes’ rule").
- Trout and char: the fold row says it when EVERY trout row's `FlowVM.down` names the same waters
  (`KeepVM.troutFrom`, sheets/trout.ts `troutFrom`). The trout table's own rows get no app words (T810 contract, guarded
  by `trout.release.test.ts`): a table only some of whose rows come down from tributaries (1,775 of 1,948 tables in
  r-fc137c4040fa, e.g. Littlejohn Creek's Dolly Varden from Kootenay Lake — Main Body) says it on those fish sheets only.
  To say it on the table's rows, Python would carry it per row (the T883 data spec's `steps[].level`, or a `TRow` line).
- Checked over the whole release by `app/tools/release-tributary-lines.test.ts` (Tier 2, `scripts/test-full.sh`), against
  each sheet's rail as drawn (`conformance/src/regs/tributaryEvidence.ts`): a line ⇒ the rail's setting steps are all
  tributary ones for every origin; no line while the rail names one ⇒ proved partial (the rail or the stored answer shows
  another rule setting some fish × origin), else it fails. r-fc137c4040fa, 20,959 parts: lines on 708 Other fish rows and
  171 trout folds (Shamrock and Kilgard Creeks dropped); proved partial: 84 Other fish rows, 1,777 trout tables; 0 failures;
  each quota group checked against its own rail too (none in this release has a line or a tributary step).
- Tests: `flow.test.ts` (FlowVM.down / downLine whole strings: the half-Bass row, a hatchery-only size step, won vs
  lowered, a rail-dropped tributary rule, "Twin Lakes’"; troutFrom incl. a twin row), `others.test.ts` (one inherited
  row, one own row), `keep.test.ts` (`groupOf` with a group decided by a tributary rule; fails without the wiring),
  `KeepTab.test.tsx` (Other fish row, trout fold, quota-group fold: drawn, spoken once), `trout.release.test.ts` (keepOf's
  `troutFrom` over every vector; none on Shamrock Creek, whose rows' sheets name Lonzo Creek). No frozen fixture changed.
- Review of 8bdb9cbf0 (five findings), review 2 of 2b0c80429 (the trout origin blocker, the group wiring test, the
  sweep's partial bucket) and review 3 of c33e06d91 (an unset `narrow`, the groups in the sweep): all fixed.

## Pictures (390, light + dark; staging-data release r-fc137c4040fa, Nov 19)
Scratchpad (session 699dcd1b) `t892-shots/`: `t892-laundry-keep-390-{light,dark}.png` (Laundry Creek: the trout fold
and Burbot from Dutch Creek), `t892-laundry-sheet-390-*` (its Burbot sheet, T883's "DUTCH CREEK · ITS TRIBUTARIES"),
`t892-littlejohn-keep-390-*` (Bass and Yellow perch from Duck Lake; trout table open, partial: no line) and
`t892-littlejohn-sheet-390-*` (its Bass sheet). Mamquam River has no such row.

## Owner 2026-11-19 ~01:25: a row whose number is the tributary rule's keeps the line
Case (review 3): Nygaard Creek's trout row — the number comes from Bella Coola River's rule for its tributaries, the
"1 over 50 cm" size cap from Region 5. Asked whether the row keeps "Bella Coola River’s rule for its tributaries".
Owner, on "Keep the line: it names the rule that sets the number you can keep; How this was decided shows both rules",
verbatim: "this is good". So a region size cap does not remove the line (as built).
