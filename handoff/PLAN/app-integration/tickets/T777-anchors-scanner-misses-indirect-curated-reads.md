# T777 — The test-anchors scanner misses tests that read live curated state through app code

Status: OPEN (pipeline tests; guard).

Source: the full CI tier red on chat 13's head, 2026-11-15 night (4 tests), then `test_dfo_closures` the same way
after `apply closed_areas`.

## What is wrong
Five tests read the owner's live curated files (marks, DFO places, closed areas), not through their own `open` but by
calling app code that reads them. They went red as soon as the owner saved. The anchors scanner looks only for direct
reads in the test file, so it passed them. Fixed by hand tonight (`sandbox_tree(fresh_review)`, `curated_root`).

## What to build
- A run-time guard for the pipeline CI tier: the curated root is pointed at an empty sandbox by default (an autouse
  fixture); a test that needs curated data builds it or asks for the live tree by an explicit marker.
- Run the tier once with the guard and fix whatever else reads the live tree.

## Added 2026-11-16 01:10 (data tests on the new build)
- `test_review_closed_areas.py::test_a_stretch_end_placed_by_hand…` stood on "the Deadman proposal's upper end is far
  from every cut"; the cut was built on 2026-11-15 and the test went red. Loosened to what holds either way (the
  nearest cut is named; `near` means within 200 m). It still reads the live tree: rebuild it on data it makes.
- CHILD PROCESSES CRASH (SIGSEGV) when a test process that has loaded the atlas forks: `git init` in a fixture, a
  `node --test` child, and `multiprocessing` children all died with signal 11 in one run and passed on the next
  (crash reports: a forked python in `_os_log_preferences_refresh` under sqlite / proj close: fork without exec after
  native libraries are loaded). Six tests in `test_review_marks.py`, `test_review_queue.py`, `test_review_dfo.py`
  failed that way in one group run and all pass alone. Fix: spawn, never fork, in tests (multiprocessing start
  method "spawn"; `subprocess` children are safe once the parent has not got the atlas loaded: keep git / node
  fixtures out of processes that load it, or load the atlas in a child).
- The pipeline tests' own temp folders (`fixture-release-*`, 3,265 of them, and `pytest-of-…`, 2.1 GB) are never
  cleaned from the system temp folder: clean on exit, as the app side now does (T764).
- Two in-season review tests (`test_curation_review_inseason.py`, 10 tests; `test_curation_review_screen.py`
  `test_re_resolve…`) fail whenever the served atlas is newer than the live release (between a promote and a release):
  they resolve on LATEST. Expected state, but the tests should say so (skip with the reason) instead of failing.
