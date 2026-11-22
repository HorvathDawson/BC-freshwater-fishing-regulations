# T777 — The test-anchors scanner misses tests that read live curated state through app code

Status: DONE — 64325c44a, cde142554 (lane test/T777-test-isolation, 2026-11-22; merge pending). The two data-tier
items of 2026-11-16 (the Deadman stretch-end test, the in-season tests between a promote and a release) moved to T1001.

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

## Built (lane test/T777-test-isolation, 2026-11-22; also T993 plan item 6 "Tests never read machine state")
- `pipeline/tests/machine_state.py`, a pytest plugin the repo-root `conftest.py` registers for every run:
  - THE CURATED SANDBOX: before collection `CURATED_ROOT` points at a session copy of the tracked curated files (as the
    checkout has them) with an empty review (no marks, closed list or closed areas). The owner's saves no longer move
    any test; a stray write lands in the copy. A data test (`needs_*` / `slow`) gets the live tree back for its run.
  - THE GUARD: an audit hook (PEP 578). A no-data test that opens the real `data/curated`, or a path under
    `data/generated` / `data/release` that git does not track, fails naming the path and the marker to add. The read is
    refused at the call and recorded, so a swallowed refusal still fails. A write to the real curated tree is refused
    for every test, data tests too. Listings are not refused (the code scanners and the mini build's watched-dir
    listing read no content). This is the run-time half the anchors scanner lacked: it sees reads through app code.
  - THE RUN'S TEMP FOLDER (the 2026-11-16 item): `TMPDIR` = `<tmp>/pyt-<pid>`, removed at the end, stale ones removed
    at the next start; `tmp_path_retention_policy = failed` in pytest.ini. The `fixture-release-*` folders are the app
    side's (`app/conformance/src/fixtureRelease.ts`); since T764 they go in the vitest run's own folder (none newer
    than 2026-11-20 03:35 in the system temp folder).
  - Mutation pin: `test_machine_state.py` plants each case in a child pytest (a direct read of `data/curated`, a
    swallowed read, an untracked generated read, the release pointer, a write by a no-data and by a data test) and
    their twins that pass (the marked read, a read through `CURATED`, a tracked generated file, a listing, a data
    test copying the tree out).
- `curated.make_root` copies the process's own tree (the sandbox in a test run), never past it to the live files.
- Fallout, each a real read of the live tree or a write into a real one, fixed: `inputs.backfill`'s refusal moved onto
  the git proof; the DFO wording notice names the registry by its repo path; `added_streams/clean.py` made
  `data/generated/municipal` on every import (an unused constant, removed); `test_lake_facts` wrote
  `data/generated/synopsis` on every run; `test_stepcache` wrote its scratch into `data/generated` (now `/.test-scratch/`,
  git-ignored); two DFO tripwires read the scraped rules unmarked (on CI they passed on nothing; now `needs_source`);
  `test_marks_never_orphaned`, `test_slough_fold`, `test_review_proposals` (Allco), `test_review_write_through` (closed
  list) stood on the live marks / closed list (now a frozen mark, or the committed files from git HEAD); five more read
  `data/curated` by a hand-built path (now `under_test.curated()` / `under_test.committed()` / `under_test.repo_path()`).
- T978's part (the data checks out of the app build's fast-fail, `FAST_FAIL_DATA=1`) was already done (a323ef2af); not
  redone. `scripts/fast-fail.sh` is untouched.
- Tests: the pipeline no-data tier as CI runs it (`scripts/pytest-chunked.sh -- -m "<pipeline-ci Tests markers>"`,
  through heavy.sh): all passed, 79 chunks in 590 s (before the guard: 37 chunks, 556 s, also all passed; the first run
  with the guard failed 17 chunks, every one a read or write named above). The data tier was not run (no data in the
  lane's worktree): a data test that reads curated data through a constant captured at import now reads the sandbox copy
  (same tracked content, empty review).

