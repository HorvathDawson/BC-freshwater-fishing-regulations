# T477 — ui-native render tests fail by wall-clock date (horizon day labels)

Status: DONE 2026-11-07 (0bd5ae373). Cause: tests built expected chips from the device `new Date()` while the app reads `bcNow()` (B.C. clock, UTC-7); near midnight the two days differ. Fix: tests use `bcNow()`; proven with Date faked to 23:30 and 00:30 PT (95/95 each). Was: (found running the T469/T471/T476 suites near midnight PT/MST).

Failing (3–6 depending on the hour): ConditionsLayout P16 horizon chips, DonorPanel horizons, MapScreen corner, and
others. They expect "Sat 7" and get "Sun 8". The horizon label comes from the device date, not the test's fixed day, so
the result depends on when the suite runs.

Do: the tests (or the VM they render) take the day from `clock.ts` `now()`, pinned per test with a fixed instant. A
check (`tools/clock.test.ts`-style) that no horizon/label code reads the device date. Then prove it by running the
suite with the clock faked to 23:30 PT and 00:30 PT.
