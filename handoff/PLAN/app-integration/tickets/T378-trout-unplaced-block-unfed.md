# T378 — Trout's "couldn't be placed" caveat block gets no data

Status: DONE (owner 2026-11-04: in 0.5.0) — the block deleted (the ticket's second answer).

Core `uncertainOf` (regs/caveats.ts) draws "? Check: rules for parts of this water that couldn't be placed on the map"
from `inp.item.uncertain`. Nothing fills it: the store's `item.unresolved` is unresolved LICENSING (schema.sql:97,
store/build.py:371), and data `regs/inputs.ts` says `item.uncertain` is not loaded. So the block never shows.

Work: decide in Python which rules of a water could not be placed (the `classify` misses / `not_yet_mapped` without a
part), store them per item, load them as `item.uncertain`, and pin the block with a fixture water. If the answer is that
every such rule already reaches the in-part block (`settled.inpart`), delete `uncertainOf` and its words instead.

## Done 2026-11-04
Every rule whose part is not drawn is carried on its water as `not_yet_mapped` (pipeline `undrawn_part`:
deliver/bundle/read/facts.py, export/read.py; "carry them on the water, never widen") and drawn by the in-part block
(`settled.inpart`, "Outlet area only (see map 1): …"). The separate "couldn't be placed" block read `item.uncertain`,
which nothing filled: `uncertainOf`, `friendlyWhy`, the "uncertain" caveat kind, `item.uncertain`, its words and the
conformance selector deleted; keep order: every caveat before the rows.
