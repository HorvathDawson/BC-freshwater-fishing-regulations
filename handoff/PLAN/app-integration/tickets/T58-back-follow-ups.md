# T58 — Back: the leftovers from the back-steps-out-a-layer work

Status: DONE — 08df886f (items 1-3; item 4 by cc4fcee1, f74b50a4). 1: frames keep a split lake's areas (`group`,
`siblingItems` via `regsGroupOf(loader)`), asked only in water mode; a failed read opens the water alone. 3: `waterOpener`
drops an open whose route moved meanwhile (back / × / anything), later of two opens wins. 2: Parts row → `onPart` →
the frame's part, sheet closes. Left: nothing in this ticket. Note: the parts sheet is reachable in the app only by the
start link today (the header's Parts button shows only when `canMap` is false — T71's change makes it reachable). Review (641ed1e5):
the Shell's "Flows into" → `opener.flow` was unpinned (reverting it failed no test) — now a back.test walk, mutation-checked.
No fixture water flows into a split-lake area, so item 1's group on a "Flows into" is pinned at the route/opener level
(back-layers `splitLake`, openWater.test `flow`), and in a render only via the tap path.

1. A split lake can be stacked twice: open area 1, tap a tributary, "Flows into" lands in area 2 → the lake twice in
   the stack. The opener must know a split lake's sibling areas (loader `siblingOrds`) so `sameWater` matches.
2. Parts-sheet rows do nothing: `RegsScreen` never passes `onPart` to `SheetHost`.
3. A slow map tap can open a water after the reader pressed back / × (the lookup finishes late). Drop a stale open.
4. (Done when T-search-back merges) the ‹ label names where back really goes.

Acceptance: route tests + back.test walks for each; mutation pins.
