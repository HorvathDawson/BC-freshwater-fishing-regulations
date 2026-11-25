# T1102 — `before_push --changed` skips the reach tests for a change to the DFO salmon reach step: a real break passed the hook

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1102 | `before_push --changed` skips the reach tests for a change to the DFO salmon reach step (a real break passed the hook) | OPEN — 0.9.2 | | 0.9.1 chat 2026-11-24, T1029 lane |

Source: 0.9.1 chat 2026-11-24 ~23:40, from the T1029 lane: a change to `pipeline/atlas/reach/dfo_salmon.py` did not
make `scripts/before_push.py --changed` run `test_reach_drops_nothing` or `test_review_proposals`, and a real break
went through the hook (caught later).

## Work
1. Find why: the changed-area map (which paths select which test groups) does not tie that file to those tests.
2. Fix the map for the reach step, then check the whole map the same way: for every pipeline package, a change to
   one file selects the tests that import it (derive it from imports where possible, not a hand list).
3. A test of the selector: for a list of (changed file → test that must run) pairs, this pair included.
4. Say in the hook's "not run here" line what was skipped by area, as today, so a skip is never silent.

## Acceptance
The selector test fails without the fix; a deliberate break in `dfo_salmon.py` is stopped by the hook.

Severity: medium (the pre-push gate let a pipeline break through). Release: 0.9.2. Model: Sonnet. Size: S. Owner: no.
