# T892 — "What you can keep" table: a row that comes from another water's tributary rule says so

Status: IN PROGRESS — left (Claude): owner sees pictures, then merge into app/integrate-082. Lane app/t892-table-tributary (built 2026-11-19 ~00:33).

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
- Same input as T883, nothing re-derived: core `flow.ts` `downOf` takes the row's items (`decided.ts` `rowItems` =
  `rowSources`, the rows frame's stored roles) and names each deciding step whose rule the part's ruleset holds `via`
  trib (rank 1) — exactly the steps the sheet's rail kicks "<that water> · its tributaries". `OtherFishRowVM.from`.
- Trout and char: the fold row says it when EVERY trout row's rail (`troutFlow`) names the same waters
  (`KeepVM.troutFrom`, sheets/trout.ts `troutFrom`). The trout table's own rows get no app words (T810 contract, guarded
  by `trout.release.test.ts`): a table only some of whose rows come down from tributaries (1,775 of 1,948 tables in
  r-fc137c4040fa, e.g. Littlejohn Creek's Dolly Varden from Kootenay Lake — Main Body) says it on those fish sheets only.
  To say it on the table's rows, Python would carry it per row (the T883 data spec's `steps[].level`, or a `TRow` line).
- Checked over the whole release (20,963 parts): every row line equals its sheet's kickers, 0 mismatches; 792 Other fish
  rows and 173 trout folds carry a line. Water's own and region rows unchanged.
- Tests: `flow.test.ts` (downOf / downLine / troutFrom whole strings), `others.test.ts` (one inherited row, one own row),
  `KeepTab.test.tsx` (the line drawn, spoken once, none on the fixture waters). No frozen fixture changed.

## Pictures (390, light + dark; staging-data release r-fc137c4040fa, Nov 19)
Scratchpad (session 699dcd1b) `t892-shots/`: `t892-laundry-keep-390-{light,dark}.png` (Laundry Creek: the trout fold
and Burbot from Dutch Creek), `t892-laundry-sheet-390-*` (its Burbot sheet, T883's "DUTCH CREEK · ITS TRIBUTARIES"),
`t892-littlejohn-keep-390-*` (Bass and Yellow perch from Duck Lake; trout table open, partial: no line) and
`t892-littlejohn-sheet-390-*` (its Bass sheet). Mamquam River has no such row.
