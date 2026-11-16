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
