# T1001 — Two data-tier test fixes left from T777 (a stretch-end test on the live tree; in-season tests between a promote and a release)

Status: DONE (f3ecf0c8e, lane L6, 2026-11-22).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1001 | Two data-tier test fixes left from T777 (the Deadman stretch-end test on the live tree; in-season tests between a promote and a release) | DONE | f3ecf0c8e | all (depends T777; from T777's 2026-11-16 additions) |

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

## Done (lane L6, 2026-11-22, f3ecf0c8e)
- **Found first:** since T777 the `wt` fixture served NO atlas (`no_served_build`), so all three `needs_atlas` tests of
  `test_review_closed_areas.py` failed on every machine (`/api/dfo/locations/…` answered a missing registry). `wt` now
  serves the atlas under test (`under_test.atlas`) to a test marked `needs_atlas`, and none to any other (as on CI).
- **1:** the nearest-cut check reads cuts the TEST plants: the line's own cuts taken out (the stretch's other end kept),
  one planted 50 m (near) and one 250 m (far) from the hand end on the side away from the other end; the expected
  answer is computed from those. A cut built near the Deadman cannot move it. The rest of the test (resolved, stamped,
  refused off the water) still runs on the live row.
- **2:** `conftest.one_vintage(atlas, release)`: the in-season module (`env`) and the screen's re-resolve test fail with
  one line, "atlas build X (section handles A) is not the release under test Y's (tiles B): … release it (python -m
  pipeline release --staging), or point RELEASE_DIR at the candidate". Both now resolve on the release under test
  (`under_test.release`, RELEASE_DIR or LATEST: `resolve.release_dir` patched in the fixture) and carry `needs_bundle`.
  `test_one_vintage.py` pins the message and the matching pair (no data).
- **Runs (unfiltered, through heavy.sh):** `test_review_closed_areas.py` + `test_review_write_through.py -m ""`: 37 passed
  (the stretch-end test after one fix); `test_curation_review_inseason.py` + the screen's in-season / re-resolve tests:
  14 passed (served atlas = LATEST's vintage today). The mismatch path was proved by the unit test only, not on a real
  promote.
