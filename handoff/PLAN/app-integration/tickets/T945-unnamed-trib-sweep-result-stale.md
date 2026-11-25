# T945 — The committed unnamed-tributary sweep result is stale against the live bundle

Status: DONE — 3f8cddad8; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — result rewritten on lane/lc-reach-leftovers; left (Claude): main chat review + merge.)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T945 | Committed unnamed-tributary sweep result stale against the live bundle (41 streams) | DONE | 3f8cddad8 | — |

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

## Done (lane LC, 2026-11-20)
All 41 explained (T57 "2026-11-20"), none by the reach: the sweep on the live bundle moves ONLY `unnamed_streams`
(1,179,627 -> 1,179,668) and its stamps; frontier 146, by_reason and by_rule are unchanged, 0 unexplained.
`unnamed_trib_sweep --write-result` (heavy.sh) written. The T944 fix moves no rule (reach digest stays 6ca9a300), so
the result stays current on the next run.
