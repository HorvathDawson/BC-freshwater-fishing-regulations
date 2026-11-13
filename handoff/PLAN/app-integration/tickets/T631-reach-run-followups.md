# T631 — Reach run: follow-ups from T546

Status: PARTIAL — items 1 and 3 done (197686263, a3518f5d2); left (Claude): item 2 (bundle flag + its app half, rides the next release), items 4–8 (traces; item 5 needs an atlas rebuild)

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

## Item 1 done 2026-11-12
`pipeline/release.py` `stage_checks` (and the `--dry-run` rehearsal) key the binding diff by the run files' CONTENT
(`run_content`: every `BASELINE_FILES` file the diff reads, DFO places, tidal, outside, diagnostics) beside the rules
digest (`run_diff_key`). "Same reach run" now means same digest AND same files; a DFO-only change is diffed and stops
for `--accept-diff`. Tests: `test_release_command.py::test_a_dfo_only_change_is_diffed_though_the_rules_digest_is_unchanged`
(mutation: hashing nothing makes it fail) and `test_the_same_run_by_digest_and_files_is_not_diffed`. A ledger row written
with the old key re-runs the diff once on resume (harmless: same content, same accept).

## Item 5 held for the owner 2026-11-12 (next-atlas lane)
Not built. The three Skeena side channels (360220078, 360220098, 360220099) at the DFO place "mainstem waters near the
Kitsumkalum River mouth" are ACCEPTED in `waters/unplaced_piece_calls.json` with the owner's ruling of 2026-11-11 (map
A3): "out of the place". "Refused: line misses" is the sweep's designed answer (`braid_sweep.py` header: a straddle the
cut's perpendicular misses is reported, never guessed). Making the sweep cut them would put their lower halves INSIDE
the DFO place: that changes where the place binds, against the standing ruling, and needs a new sweep rule (medium).
Recommended default: close item 5 as answered by the owner's ruling (the channels stay out). Ask only if the owner
wants them cut.



## Item 5: the facts, for the owner (2026-11-12 night; no build, no change)
Owner asked: "If it is just braids then it should be split like normal braid items?" Pictures (Esri imagery, same
framing): `bcfish-session-backup-2026-11-12/skeena-channels/` A-today-outside, B-whole-inside, C-carried-across-no-cut,
D-bad-edge-measure (outside the repo; the owner's backup folder).
- **Not tidal.** The Skeena's tidal boundary is at m 6,405; this cut is at m 123,961 (117 km upstream). The coast rule
  (coast/2) does not apply here.
- **(1) Ordinary braid channels?** Yes: unnamed, the Skeena's own watershed code (400-…), FWA edge types 1100/1350/1450;
  the sweep classes them as the host's own water (`through_own`: 360220098, 360220099; 360220078 hangs off them).
- **(2) Why not cut.** The sweep cuts a braid only where the cut's own line crosses it (the perpendicular to the ±100 m
  chord, `REACH_M` 3 km, clipped at the host's first re-crossing); it never projects the cut onto a channel. The line
  passes 1,457 m (…099), 1,550 m (…098) and 2,262 m (…078) from them, and an unclipped 8 km perpendicular crosses none.
  The real reason they "straddle": graph edge 778568 joins …099's mouth to the Skeena with x/y AT the mouth but route
  measure m 119,499 (3.35 km downstream, inside the place); by geometry every junction of the three is at Skeena m
  125,470–128,641, all ABOVE the cut, and FWA's own codes agree (20.19–20.37 % up the Skeena; the Kitsumkalum is 19.54 %).
- **(3) The options.**

| option | rule in plain words | in the DFO place | other braids province-wide it would change |
|---|---|---|---|
| A today | the sweep cuts only where the cut's line crosses; the network stays as accepted | none of the three (7.5 km out) | 0 |
| B whole inside | a missed network goes with its lower recorded junction | all three, whole (7.5 km added above the place's top boundary) | 1 more "line misses" case (Squamish Powerhouse Channel at Ashlu Creek, 6 channels) |
| C carry the cut across by river measure | cut each channel where its share of length equals the cut's share between its two junctions | none: the cut (m 123,961) is below every channel's span (m 125,470–128,641), so no cut lands; = A | the same 2 "line misses" cases are evaluated; here nothing changes |
| D fix the edge | a junction lands on the piece its own x/y lies on (re-home by geometry, as the sweep already does for FROM ends) | none, = A, and the straddle disappears (nothing left to accept) | 298 junctions province-wide land on another piece of their receiving line (median 559 m off, 83 over 1 km, 255 lines); a broad change, needs its own ticket + an atlas build + a binding diff |

Recommendation: A stands (the geometry says the channels are above the place); D is the root-cause fix, as its own ticket.

## Item 2 held 2026-11-12 night (not small)
Today the accepted unplaced pieces are 5 (2 withheld closure pieces, Noons and Seymour; the 3 Skeena channels above).
A "check" flag needs: the reach run's `unclassified` pieces into a new bundle table (rule 56: the bundle reads, never
recomputes), the store / a release file to carry it (not `water`: 99.7 % of its gate, T686; regs-sid as T672 1b did is
the candidate), and NEW page words ("part of this water could not be placed; check the book") that the owner has not
seen. Each half alone is dead data or a dead read. Plan: one change after the owner words the line; size M.
