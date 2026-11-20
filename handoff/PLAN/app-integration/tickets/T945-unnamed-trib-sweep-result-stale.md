# T945 — The committed unnamed-tributary sweep result is stale against the live bundle

Status: OPEN.

Source: lane/t936-t931 (`scripts/test-full.sh`, slow tier), 2026-11-20.

## What
`pipeline/tests/test_unnamed_trib_sweep.py::test_the_sweep_is_the_committed_result_and_nothing_is_unexplained` fails:
the committed result was swept on the bundle of reach 4f1bdab9d319246f (before the 11-15 rebuild); the live bundle
(r-6258ffded676, reach 6ca9a300dad3d95e) gives `unnamed_streams` 1179668 against the committed 1179627 (41 more).

## Work
Rerun `python -m pipeline.tools.audit.unnamed_trib_sweep --write-result` through `scripts/heavy.sh`, and explain the 41
streams (and any new miss) in T57, as the test's message asks. Likely related: the 11-15 dead-end walk (93a481b71) and
the T753 lake loops, which changed which pieces count.
Depends: none. Size: S. Heavy job.
