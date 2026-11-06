# T431 — The feeds worker's own tests run nowhere; its silent catches

Status: DONE 2026-11-05 — 9f002b2e2. The worker's vitest runs in `app-ci.yml` and first in `feeds.sh` (`tools/feeds-gate.test.ts`);
the failure scan covers `workers/feeds/src` and the "logged below" catches now log (`tell`); `scheduled()` + `runTimingCheck`
tested (`test/scheduled.test.ts`); the two skipped DFO suites + harness/typed_dump removed; REPORT_SALT required (503 / no deploy).

Source: `handoff/PLAN/audit-2026-11-05/` (findings: app 2, 3/4, 7; tests 3, 4; `*.verified.md` has the evidence per finding).

## What happens
26 test files (incl. the DFO and gauge Python-parity suites) are in no vitest project, not in `app-ci.yml`, and
`deploy/staging/feeds.sh:10` deploys with no test step. `tools/failures.test.ts:24` (the no-silent-fallback scan)
skips `workers/feeds/src` (42 catch sites); `index.ts:~388/406/437/457` `catch { /* logged below */ }` drop a failed
storage write and the failed-send count. Two `describe.skip` DFO suites outlived T350 (DONE 4fb701fb7); the cron
dispatcher `scheduled()` and the `runTimingCheck` wrapper have no test. `REPORT_SALT` falls back to a constant in the
repo (configured nowhere).

## Fix
Worker vitest in `app-ci.yml` and as `feeds.sh`'s first step; the failure scan covers the worker (reasons per
catch); the skipped suites removed (keep `dfo.scenario.ts` helpers: 8 tests import them); tests for `scheduled()`
and `runTimingCheck`; `REPORT_SALT` required (a secret, as on prod's placeholder).

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
