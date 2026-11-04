# T372 — Partial coverage: one rule for trout and salmon, resolved in Python

Status: OPEN — before 0.5.0 (owner 2026-11-03: "the trout way seems more right, everything in python keeping single
source of truth").

A water with a place the map does not draw yet (salmon: a DFO place not bound, 134 of 462 chains in r-4df5bccc9360;
trout: a rule `classify` could not place, `item.unresolved` / `not_yet_mapped`) cannot say which stretch that place
covers. Today the two disagree:
- **Trout/char (keep):** the pipeline stores the answer and, apart, the undrawn-part rules with whether each is in force
  today (`settled.inpart`, caveats.ts): the card keeps its answer and adds "Check: rules for parts of this water that
  couldn't be placed on the map".
- **Salmon (change):** core `salmon.ts` compares per species and day (`agrees`, the year bar's `at1`) and turns a row
  into "check DFO" where an undrawn place answers otherwise — decided on the phone.

Work (the trout way, owner): the salmon card keeps the drawn answer; the undrawn places become a caveat block like
trout's (each place, its answer today, DFO's words). The pipeline ships what the card says — per chain, per species,
the days an undrawn place answers otherwise (run-length like `sp`: ~6 kB raw on 134 chains, ~1–2 kB gz est.) — and
both `live.compose` and the app's `composeChain` carry it; `agrees` / `at1` go. The map keeps painting such sections
`check` (already Python's). One wording for both families (UI chat: the block's look, T367). Golden + salmon/5.
**REUSE (owner 2026-11-03: "make sure code is reused as much as possible to keep consistent"):** no second
mechanism. Salmon's undrawn places go through the SAME pipeline step and app code as trout's: the stored undrawn-part
verdicts (`settled.inpart` / `not_yet_mapped`), the same caveat selection and words (core `regs/caveats.ts`
`uncertainHtml` port, `describe`, `timeCond`), the same VM and component. Where salmon's shape differs (DFO places,
chains), adapt it to the trout input in Python — never fork the caveat code. Delete what salmon then no longer needs
(`agrees`, `at1`, `undrawnChains`' card use). Start by mapping both paths side by side (files, functions) in this ticket.
Size: M.

## Map of both paths (2026-11-04, read-only survey)
- **Trout:** Python marks an undrawn rule `not_yet_mapped` (export/read.py:189-205, facts.py:107); the ladder never lets
  it compete (ladder.py:390-394, :777, :812); the verdict is stored per frame (answers/encode.py:724-733 → store
  `ladder_verdict`). App: settle.ts:61-73,101-102 `inpart [{rule, now}]` → caveats.ts:88-114 `inPartOf` → `CaveatVM`
  (model.ts:205-216) → ui-native `KeepBlocks.tsx` `CaveatBlock`. The answer is never changed (card.ts:188). Map:
  never colours or closes a section (status_index.py:22,30).
- **Salmon:** salmon.py:416-472 (`unmapped`, chain `u`, `undrawn_on`), chain_calendar :295-315 paints the WHOLE water
  `check` every day (:314; live.compose :130-132; app composeChain salmonFeed.ts:121). App: salmon.ts `undrawnChains`
  :837, `agrees` :664-669 (row → "check DFO"), `at1` :818-821 (bar → check), `partial` VM :751, `unmapped` rows :748;
  SalmonCard.tsx :94-105, :183-188.
- **Found on the way:** the trout "? Check: rules for parts of this water that couldn't be placed" block is UNFED —
  the store's `item.unresolved` is unresolved LICENSING (schema.sql:97, store/build.py:371), and inputs.ts:49-50 says
  `item.uncertain` is not loaded. Fix inside T372 or ticket it.

## Plan
1. Python ships per undrawn place what a caveat item needs (label/where = the place, DFO's words, its windows) and
   whether it is in force per day (from its own chain runs); both `salmon.build` and `live.compose`.
2. Core: generalise `inPartOf` (and `item()`, `areaOf`) to take caveat items rather than `Settled` + `RuleView`; trout
   builds them from `inpart` as today, salmon from the shipped places — ONE function, one `CaveatVM`, one `CaveatBlock`.
3. Salmon keeps the drawn answer; `agrees`, `at1`'s downgrade and the `partial` VM go; `unmapped` rows / `unplaced`
   become caveat items. UI chat: SalmonCard draws `CaveatBlock` (T367), then the old field is deleted.
4. Map paint: owner decision (below). Format: salmon/5 if the paint or the file changes.
