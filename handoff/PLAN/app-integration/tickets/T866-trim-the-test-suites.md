# T866 — Trim the test suites: combine the overlapping ones, remove true duplicates

Status: IN PROGRESS (main chat 2026-11-22: proposal written, owner picked three of four; two lanes).

Owner 2026-11-18 ~19:10 (to the app chat), verbatim: "We should make a ticket for after the releases to try and clean up
tests and reduce there being way too many tests. We could combine a bunch and maybe remove some that are duplicates".

Today (the 0.8.1 web tier, 2026-11-18): app 727 vitest files / 8,517 tests (993 skipped); pipeline ~9,280 collected
(7,633 in the fast tier, 1,650 deselected as slow/needs-data); the full tier takes ~8 min on the Mac, and a single
landing often fails it on a pin that restates what another test already holds.

Build:
1. Measure first: per file, test count, run time, and what it asserts; find the overlap (the same view model or rule
   pinned in several files, the same string pinned in core + ui + shell tests, near-identical render tests per screen,
   pipeline tests that re-pin a curated value another test already proves). One table, by area.
2. Combine where a group pins the same behaviour through the same seam: one parameterised test per behaviour.
3. Remove true duplicates only: a test whose every assertion another test also makes (prove it per removal: the kept
   test fails under the mutation the removed one caught). No coverage loss in the guards that earned their place
   (no-silent-skips, test anchors, surfaces/T60, one-colour-pathway, ticket guard, the mutation pins).
4. Report before/after: files, tests, tier time; the owner sees the list of what went before it lands.

## Owner 2026-11-22, dialog on the proposal (`handoff/PLAN/audit-2026-11-21/t866-proposal.md`)
Picked: "Speedups, remove nothing (Rec.)", "Remove 5 true duplicates", "Fold the 30 wording files". Not picked: parallel
pipeline tests (pytest-xdist). So: (1) app-ci vitest split across 4 runners; ~15 website tests on the build's bare mode
(same outputs checked); the 21 s stall timer test and the 48-page full-scale render test made fast without losing their
mutation; (2) the 5 duplicates D1-D5 removed, each proven redundant by a mutation first; (3) the 30 wording-round files
folded into one table per water and day.
