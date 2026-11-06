# T430 — The crash and problem-report formats have no parity test (one field loses every report)

Status: OPEN — from the audit (T333–T335, 2026-11-05). Severity: **high**. Verified by a second reader.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: app 5; `*.verified.md` has the evidence per finding).

## What happens
Typed twice: `ui/src/crashReport.ts:27`, `core/menu/report.ts`, `core/waterViews.ts:16` vs the worker's
`crashes.ts:36-38,100-101` / `reports.ts` (ROUTE_KINDS, VIEWS copied). The worker refuses unknown keys; the app's
outbox marks a 400 refused and never queues it again (`data/src/report/outbox.ts:6-8,73`).

## Fix
`app/tools/wire.test.ts`: `crashPayload()` and `reportPayload()` through `validateCrash`/`validateReport` for every
enum value; the enum sets compared; one shared source for the lists if the package boundaries allow.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
