# T291 — Test problem reports and crash reports are tagged and ignored; staging reports cleaned up

Status: PARTIAL — built, tested, staging cleaned; worker deployed and verified live 2026-10-29; left: an APK rebuild that sends the tag (owner 2026-10-29: "can we clean up reports too? can add a test tag so that we can ignore test ones").

## Work
1. A `test` tag on problem reports and crash reports: set by every tool that sends one (testbed scenarios report/crash,
   smoke-staging, bad-net, request-walk, any unit/e2e that posts), and by a dev/staging build's hidden switch if useful.
   The feeds worker stores tagged ones apart (e.g. `private/reports-test/`, crash kinds marked test), never counts them in
   the digest/alerts, and purges them after 7 days.
2. The review tool's reports view hides test ones by default (toggle to show).
3. Clean up staging now: list the existing staging reports + crash kinds, delete the ones that are clearly from tests
   (testbed/smoke ids, the forced T242 check), keep anything that looks real; report counts.
4. A real user can never set the tag (worker ignores it from production builds unless the request carries the run token, or
   the tag is only honoured on staging).

## Acceptance
Tool-sent reports land tagged; digest ignores them; review view hides them; staging cleaned (counts before/after).

Depends: T237, T242. Size: S. Owner: no (worker deploy is the owner's command).

## Built
- **Tag**: `test: true` on `report/1` and `crash/1`. Set by: the test bed's APK (`session.sh` builds with `EXPO_PUBLIC_TEST_REPORTS=1`) and any local dev
  build (env `local`); `createReportOutbox({test})` / `createCrashReporter({test})` add it at send time (the payload pin tests are unchanged).
- **Worker** (`reports.ts`, `crashes.ts`, `index.ts`): honoured ONLY when `FEEDS_ENV === "staging"` (a var the staging worker already has; a production
  worker ignores the field and files the report as an ordinary one, so a real user can never tag). Tagged reports go to `private/reports-test/<day>/<id>.json`,
  tagged error kinds to `private/crashes-test/<hash>.json`; neither is read by the digest. `GET /reports` and `GET /crashes` return real ones only unless
  `?test=1` (then each carries `test: true`). The daily purge deletes tagged reports after 7 days and tagged kinds 7 days after last seen. Tests:
  `workers/feeds/test/reports.test.ts` + `crashes.test.ts` (staging stores apart / production ignores / digest / listing / purge),
  `data/src/report/outbox.test.ts` (the sender tags).
- **Review app Reports tab** (`ReportsView.tsx`, `reports.py`): the backend asks the worker with `test=1`; the tab hides test reports by default (and does not count
  them in the badge or the status line), "show test reports (n)" shows them. A synced list now also drops cached reports that the worker no longer lists
  inside its 90-day window, so the browser's cached copies of deleted test reports go on the next Sync.
- **Staging cleaned (2026-10-29, rclone r2:bc-fishing-regulations-staging/private/)**: reports 7 -> 0 (3 test bed, 2 "T237 e2e" web, 2 curl smoke tests; none
  looked real); crash kinds 7 -> 0 (3 local dev-server errors incl. Metro TransformError, 1 staging-web hot-reload error, the forced T242 check x2, the test
  bed `am crash` exit record). Rate-limit counters (`limits/`) left. Nothing real existed.

## Verified live (2026-10-29, worker deployed by the owner)
curl POST /report and /crash with `test:true` -> 201 each; they landed in `private/reports-test/2026-10-29/` and `private/crashes-test/`, nothing in `private/reports/` or `private/crashes/`; both objects then deleted. `GET /reports` without `?test=1` needs the run token (not used); its filtering is pinned by `reports.test.ts`.

## Left
- The test bed needs a rebuilt APK (the next `session.sh --build`) to send the tag.
