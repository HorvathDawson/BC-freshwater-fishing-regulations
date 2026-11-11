# T631 — Reach run: follow-ups from T546

Status: OPEN

Source: T546 report 2026-11-11.

## Work
1. The release check runs the binding diff even when the rules digest is unchanged, so a DFO-only change is diffed
   (today 21 DFO places moved with the digest unchanged; Morice loses 11 sections). Coordinate with T547.
2. The bundle carries a "check" flag for a water with unplaced pieces.
3. An id dry-run before a candidate registry is promoted.
4. Trace the 3 unplaced pieces on one Skeena DFO place (`straddles_the_cut`). DFO area: hand to chat 10.
5. (T632 leftovers) The atlas braid sweep refuses the three Skeena side channels near the Kitsumkalum mouth
   ("line misses"); fix the sweep so they place. Until then they wait in `unplaced_piece_calls.json`.
6. (T632) Check the Noons Creek and Seymour River unplaced pieces against their no-fishing closures: is any closed
   water left undrawn?
7. (T632) DFO placement ignores `owned=`.
8. (T632) `1:somass-river-tributaries:whole` binds 5.5 km of the Somass mainstem. Trace; DFO area, hand to chat 10.

## Acceptance
- A DFO-only placement change shows in the release check's diff. A candidate registry with a dead curated id is refused before promote.

Depends: T546. Size: M.
