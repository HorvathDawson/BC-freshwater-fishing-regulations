# T695 — Fold more small widenings into the stream: draw the stream line, not the polygon, where it is the stream

Status: POSTPONED (deferred, owner 2026-11-13, verbatim: "Then the last 2 items for after closed testing should be moved to deferred with stocked lakes and bathy map etc.."). Moved to the deferred group with stocked lakes and the bathymetry map; earlier 2026-11-12: "this is after closed build though".

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T695 | Fold more small widenings into the stream; draw the stream line instead of the unnamed polygons a river runs through (the 456, Q124/T153; real lakes stay lakes) | POSTPONED (deferred, owner 2026-11-13: moved to deferred with stocked lakes and bathy map; earlier "after closed build" 2026-11-12) | — | RULINGS E27; measure first; links T686 (candidate 9), T678 D1 |

Source: owner 2026-11-12, verbatim: "the unnamed polygons depends if it is actually a lake... I thought we already
captured most of these. We could do another ticket in future to combine more small widenings as part of the stream. In
that ticket it could also remove those lakes and instead replace it with the stream line below it instead. Since if it is
a stream we could have only stream lines in theory and save on polygon size etc... this is after closed build though."
RULINGS E27 (Q124 answered: keep as built now).

## What (today)
- The 456 unnamed lake/manmade polygons a river enters and leaves (T153; 439 Lake-Definite, 9 Indefinite, 8 reservoir at
  the census) stay lakes but take the river's rules (`extent.river_widenings`, flag
  `UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER`, Q124); still polygons in the tiles and the water list.
- The pond fold (T678 D1): 16,119 ponds folded into 4,258 stream items. The 456 are the ones D1 did not fold; which of them
  D1 already took is the first thing to measure (the census is older than D1).
- T686: the folded ponds' sections made `core`, `conditions` and `first_search` break their budgets (RULINGS O5); fewer
  polygons and sections per water is one of its candidates (1, 8); this ticket is the geometry side.

## Work
1. Fold more small widenings into the stream item (same rule as D1, a wider net: an unnamed polygon the river's main flow
   runs through, small enough to be a widening, not a real lake).
2. Where a polygon is only the stream, DRAW the stream line (the line below it) and drop the polygon from the tiles and
   the shipped water list. A real lake stays a lake: "depends if it is actually a lake", the test is the owner's, not area.
3. **Measure first (no shipped change):** how many of the 456 are still lakes on the live release after D1; per polygon:
   area, the river's width either side, length of main flow through it, a name anywhere (GNIS, wbk, the book). Tile and
   bundle bytes saved (atlas pmtiles layers, `core` / `water` / `geo` rows) against what is lost (a shoreline an angler
   fishes from, a lake step in the water page, a map highlight shape). A binding diff on a trial build (rules should not
   move; only shapes and item membership). A sample of 20 for the owner (polygon over stream line), borderline ones named.

## Rules
- Nothing changes before closed testing; no atlas rebuild for this until then.
- An atlas rebuild and a release carry it (item ids move: AGENTS 5/6, entry resync); the owner sees the borderline set first.
- A named lake never takes the river's row and never goes.

Depends: T678 (D1 shipped), T686 (the shrink survey, to rank it), closed testing started. Size: L. Owner: yes (the borderline
set: lake or stream). Default: no change; the 456 stay as built.
