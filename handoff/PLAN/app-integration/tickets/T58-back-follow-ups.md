# T58 — Back: the leftovers from the back-steps-out-a-layer work

Status: OPEN.

1. A split lake can be stacked twice: open area 1, tap a tributary, "Flows into" lands in area 2 → the lake twice in
   the stack. The opener must know a split lake's sibling areas (loader `siblingOrds`) so `sameWater` matches.
2. Parts-sheet rows do nothing: `RegsScreen` never passes `onPart` to `SheetHost`.
3. A slow map tap can open a water after the reader pressed back / × (the lookup finishes late). Drop a stale open.
4. (Done when T-search-back merges) the ‹ label names where back really goes.

Acceptance: route tests + back.test walks for each; mutation pins.
