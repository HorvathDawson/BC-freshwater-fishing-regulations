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

## Item 3 done 2026-11-11 (a3518f5d2, the next-rebuild lane)
`pipeline.atlas.promote` now runs the id dry run: every string in every curated `*.json` (outside `review/`) that is an
item id of the current atlas and that the candidate no longer has is listed, `GONE` (no such item) or `ABSORBED` (an
alias of another item; the curated files name survivors only). `--dry-run` prints the list by file; a real promote
refuses while it holds one (`--accept-dead-ids` after reading it). It also prints the boundary ids the candidate lost,
by family. Tests: `pipeline/tests/test_next_rebuild.py` (`test_promote_names_every_curated_id…`,
`test_lost_boundaries_are_listed_by_family`). First use: `handoff/PLAN/app-integration/next-rebuild-results.md`.
Items 1, 2, 4 to 8 stay open.
