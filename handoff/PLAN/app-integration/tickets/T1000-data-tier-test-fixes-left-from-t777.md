# T1000 — Two data-tier test fixes left from T777 (a stretch-end test on the live tree; in-season tests between a promote and a release)

Status: OPEN.

Source: T777's 2026-11-16 additions, left when T777 closed (lane test/T777-test-isolation, 2026-11-22): both are
`needs_atlas` tests, so neither runs on CI's tier or in a worktree without the atlas, and the T931 lane had
`test_review_closed_areas.py` and `test_curation_review_screen.py` open in the same hours.

## Work
1. `test_review_closed_areas.py::test_a_stretch_end_placed_by_hand_is_resolved_on_the_water_and_stamped` still reads the
   live tree (it was loosened 2026-11-16 to hold either way). Rebuild it on data it makes: a hand-placed end on a small
   invented water (or the mini world), so a new cut on the Deadman never moves it.
2. `test_curation_review_inseason.py` (10 tests) and `test_curation_review_screen.py::test_re_resolve…` fail whenever
   the served atlas is newer than the live release (between a promote and a release): they resolve on LATEST. That
   state is expected; the tests must say so, as `conftest.predates` does for an old bundle: fail with the reason (atlas
   build X is newer than LATEST's tiles Y: release, or point RELEASE_DIR at the candidate), never a bare mismatch.

## Acceptance
- 1: the test passes on a machine whose atlas has a cut near the Deadman end, and on one without (mutation: plant one).
- 2: with an atlas newer than LATEST the files fail with one line naming both vintages and what to run; with a
  matching pair they pass. Run under the data tier (`scripts/test-full.sh` or the chunked runner with `-m "not slow"`).

Depends: T777. Size: S. Owner: no.
