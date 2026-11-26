# T631 — Reach run: follow-ups from T546

Status: PARTIAL — items 1 and 3 done (197686263, a3518f5d2); item 5 answered by the owner 2026-11-12 (A: the three Skeena channels stay outside the DFO place; nothing to build); left (Claude): item 2 (the check flag: a bundle table + new page words, one change after the owner words the line), items 4, 6, 7, 8 (traces)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T631 | Reach run follow-ups: binding diff on DFO-only changes, check flag for unplaced pieces, id dry-run before promote | PARTIAL — left (Claude): items 2, 4, 6–8 | 197686263, a3518f5d2 | item 1: the release check keys the diff by run files, a DFO-only change is diffed; item 3 done; item 2 held 2026-11-12 night (not small: new bundle table + release file + new page … (detail: ticket file) |

Source: T546 report 2026-11-11.

Done: item 1 (`pipeline/release.py` `stage_checks` keys the binding diff by the run files' content, `run_content` /
`run_diff_key`; a DFO-only change is diffed and stops for `--accept-diff`), item 3 (a3518f5d2: `pipeline.atlas.promote`
runs the id dry run, `GONE` / `ABSORBED`, refuses without `--accept-dead-ids`; first use
`handoff/PLAN/app-integration/next-rebuild-results.md`). Item 5 answered: owner asked "If it is just braids then it should
be split like normal braid items?"; answer A stands (the three Skeena channels 360220078, 360220098, 360220099 stay outside
the DFO place, ACCEPTED in `waters/unplaced_piece_calls.json`; by geometry every junction is at Skeena m 125,470–128,641,
above the cut at m 123,961). Root cause, its own ticket (D): graph edge 778568 joins …099's mouth to the Skeena with x/y AT
the mouth but route measure m 119,499; re-homing a junction onto the piece its own x/y lies on touches 298 junctions
province-wide (median 559 m off, 83 over 1 km, 255 lines): atlas build + binding diff.

## Work (left)
2. The bundle carries a "check" flag for a water with unplaced pieces. Held 2026-11-12 night (not small): today the
   accepted unplaced pieces are 5 (2 withheld closure pieces, Noons and Seymour; the 3 Skeena channels). Needs the reach
   run's `unclassified` pieces into a new bundle table (rule 56: the bundle reads, never recomputes), the store / a release
   file to carry it (not `water`: 99.7 % of its gate, T686; regs-sid as T672 1b did is the candidate), and NEW page words
   ("part of this water could not be placed; check the book") that the owner has not seen. Each half alone is dead data or
   a dead read. Plan: one change after the owner words the line; size M.
4. Trace the 3 unplaced pieces on one Skeena DFO place (`straddles_the_cut`). DFO area: hand to chat 10.
6. (T632) Check the Noons Creek and Seymour River unplaced pieces against their no-fishing closures: is any closed
   water left undrawn?
7. (T632) DFO placement ignores `owned=`.
8. (T632) `1:somass-river-tributaries:whole` binds 5.5 km of the Somass mainstem. Trace; DFO area, hand to chat 10.

## Acceptance
- A DFO-only placement change shows in the release check's diff. A candidate registry with a dead curated id is refused before promote.

Depends: T546. Size: M.

## Ledger detail (moved out of the ledger row, T540, 2026-11-22)
item 1: the release check keys the diff by run files, a DFO-only change is diffed; item 3 done; item 2 held 2026-11-12 night (not small: new bundle table + release file + new page words); item 5 answered by the owner 2026-11-12 (A: stay outside, no change)
