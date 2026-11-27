# T1213 — two chart render tests off the wall clock

Status: DONE — bda55c4, 89a38ad, 16b61b7f1 (tests only; reviewed).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1213 | two chart render tests off the wall clock | DONE | bda55c4, 89a38ad, 16b61b7f1 | — |

Source: planning chat 2026-11-26, from two lanes' full pre-PR runs the same day. `app/packages/ui/src/chart.render.test.tsx`:
the "(T1043) a bandless 72 h chart …" tests failed once in a 10,096-test run (3/3 alone); "a switch draws each station
only on its own year" (a 30 ms sleep) failed 1 of 9,090 (20/20 alone). Owner's standing rules: no longer timeout, no retry, no skip.

## Work
1. Cause: the code under test has no timer (`useAsync` is promises plus a sequence guard), so it is not a race in the code.
   The tests wait on the wall clock: `waitFor` (1 s default timeout, 50 ms poll) for every state, a real 5 ms
   `setTimeout` inside the fake feed, and fixed 20 ms / 30 ms sleeps to prove a negative (a starved worker overruns the
   `waitFor` clock; the sleeps prove nothing about a slow machine).
2. Fix (tests only, local to the T1043 describe): wait by counted `act` rounds on the condition, hold each station's feed
   read as a promise the test releases, and replace the sleeps with a fixed number of flush rounds.

## Acceptance
- The file passes 20 times in a row under CPU load; the T1043 tests no longer use `waitFor` or `setTimeout`.
- Left as it is, not load-sensitive: "(T256) without a feed" keeps a 20 ms sleep and asserts little (review, 2026-11-26);
  a real check there (one `gaugeSeries` call, state "loading") is a follow-up, not this ticket.
- Mutations still fail them: `yearHere` ignoring the key, `yearPending` forced false (switch test), `bandless` ignoring the band (banded test).
- No app code changed.
Depends: none. Size: S. Owner: no. Default: n/a.
