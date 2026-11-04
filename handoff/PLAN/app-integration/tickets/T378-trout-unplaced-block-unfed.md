# T378 — Trout's "couldn't be placed" caveat block gets no data

Status: OPEN (found in T372's survey, 2026-11-04; after 0.5.0 unless the owner pulls it in).

Core `uncertainOf` (regs/caveats.ts) draws "? Check: rules for parts of this water that couldn't be placed on the map"
from `inp.item.uncertain`. Nothing fills it: the store's `item.unresolved` is unresolved LICENSING (schema.sql:97,
store/build.py:371), and data `regs/inputs.ts` says `item.uncertain` is not loaded. So the block never shows.

Work: decide in Python which rules of a water could not be placed (the `classify` misses / `not_yet_mapped` without a
part), store them per item, load them as `item.uncertain`, and pin the block with a fixture water. If the answer is that
every such rule already reaches the in-part block (`settled.inpart`), delete `uncertainOf` and its words instead.
