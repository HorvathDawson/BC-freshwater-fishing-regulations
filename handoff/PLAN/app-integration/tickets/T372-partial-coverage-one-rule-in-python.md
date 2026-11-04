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
Size: M.
