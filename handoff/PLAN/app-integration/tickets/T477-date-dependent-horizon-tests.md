# T477 — ui-native render tests fail by wall-clock date (horizon day labels)

Status: OPEN 2026-11-07 (found running the T469/T471/T476 suites near midnight PT/MST).

Failing (3–6 depending on the hour): ConditionsLayout P16 horizon chips, DonorPanel horizons, MapScreen corner, and
others. They expect "Sat 7" and get "Sun 8". The horizon label comes from the device date, not the test's fixed day, so
the result depends on when the suite runs.

Do: the tests (or the VM they render) take the day from `clock.ts` `now()`, pinned per test with a fixed instant. A
check (`tools/clock.test.ts`-style) that no horizon/label code reads the device date. Then prove it by running the
suite with the clock faked to 23:30 PT and 00:30 PT.
