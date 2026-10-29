# T237 — report a problem: an endpoint and a review-app queue

Status: PARTIAL — 12a75940 (web + worker + review tab done, e2e on staging; native device not run).

Source: owner 2026-10-28 — approved proposal P3 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
1. Today "Report a problem" (menu/report.ts, ReportPage) composes an email / share sheet and sends nothing itself. Add a
   "Send" that POSTs the same report (what the user wrote + only the diagnostics they left on: water, part, section, day,
   app build, release, tiles, platform) to a small Cloudflare endpoint (feeds worker route or its own worker).
2. Endpoint: validates size/shape, rate-limits per IP (T249), stores one JSON per report under `private/reports/` in R2,
   never echoes PII; owner notification through the feeds worker outbox (T225 events: one digest line per report).
3. Review app: a "Reports" tab listing them (newest first) with a link that opens the water/part in the app and a
   status (new / fixed / not a problem).
4. Offline: queue the report on the device and send on reconnect (the email path stays as the fallback).
## Acceptance
Report from the web + a phone lands in R2 and in the review tab; a 2nd report in 10 s from one IP is refused; no field the
user switched off is sent (test pins the payload); offline report is sent after reconnect.
Depends: T225 (worker + outbox), T249 (rate limits). Size: M.
Owner: yes — where reports go (default: R2 `private/reports/` + daily digest email); privacy wording on the form.

## Built (12a75940; part of the app wiring landed in 5436081e6, swept by the T248 lane's commit)
- **App** (`core/menu/report.ts` `reportPayload` / `emailProblem` / `REPORT_REASONS`, words in `menu/words.ts`;
  `ui/report.ts` `useReportSend`; `ui-native/menu/ReportPage.tsx`): reason (Wrong rule / Wrong place on the map /
  Missing water / Wrong name / Something else), what's wrong, optional email (validated), the diagnostics rows with
  switches, **Send report**, confirmation ("Thank you…"), offline ("saved on this device and will be sent…"), refused
  (said, form kept), privacy line; email / share kept as "Or send it yourself". Context sent per row left on: water
  item id, part + label, place, day, screen/tab, map colouring (standard/salmon/runs), app version (T251) + env (T232),
  platform, release, tiles, recent errors. The **section handle** goes with "Where you were" and is never shown
  (removed from the visible lines and the email body). Entry points: the regs page foot link (the tap sheet's regs
  view and every water/part/section page), More → Report a problem, **About → Report a problem** (new).
- **Offline**: `@app/data` `createReportOutbox` (localStorage / `report-outbox.json`): offline, network error, 5xx,
  408, 429 → queued; other 4xx → refused. Flushed by the T231 update service (`UpdateOptions.outbox`) on every online
  check and the moment the network returns. Ids are made on the device; the worker stores by id (idempotent re-send).
- **Endpoint** (`app/workers/feeds/src/reports.ts`, deployed to bc-fishing-feeds-staging): `POST /report` (CORS,
  ≤16 KB, `report/1` shape, unknown context keys refused), rate limit 1 per 10 s + 30/day per IP (salted SHA-256 of
  the IP in `private/reports/limits/`; secret `REPORT_SALT` set) behind the `REPORT_LIMITER` binding (1/10 s), stored
  `private/reports/<day>/<id>.json` with only `received` added (no IP / UA / country), answers `{ok,id}`.
  `GET /reports?since=` (run token). The gauge run (every 30 min) turns new reports into `report` digest events (not
  urgent; the reporter's email never in the email). The data worker 404s `private/` (checked).
- App config: `REPORT_URLS` in `apps/mobile/src/env.ts` — local + staging → the staging worker, production → none
  (email/share only) until a production worker exists; `EXPO_PUBLIC_REPORT_URL` overrides (`off` disables).
- **Review app**: TopNav "Reports" tab (key 9 / g r, `#reports/<id>`): Sync (`GET /api/reports` → the worker with
  `app/workers/feeds/.run-token` or `CURATION_REPORTS_TOKEN`), list newest first with filter, status new / looking /
  fixed / won't fix kept in localStorage, context table, map of the water (`MapPanel`) and the tapped section in
  magenta when the report's tiles are this build's vintage (handle → node via `section_handles`; another vintage is
  never resolved). Read only: two GET routes, no file write (safety pins unchanged and passing).

## Verified
- Tests: `core/src/menu.test.ts` (payload pinned whole; switched-off rows send nothing), `data/src/report/outbox.test.ts`
  (queue/flush/refuse/restart + update-service flush on reconnect), `ui-native/src/menu/ReportSend.test.tsx`,
  `workers/feeds/test/reports.test.ts` (store, 429 in 10 s, other IP ok, 30/day, bad/oversized, idempotent, routes,
  digest), `pipeline/tests/test_curation_review_reports.py` (+ the safety suite).
- Staging e2e 2026-10-29: a local web build (`EXPO_PUBLIC_APP_ENV=staging expo export`) on the Vedder page sent a
  report (201), a second report made **offline** was queued and sent on reconnect (outbox empty after); curl: 2nd POST
  within 10 s → 429, malformed → 400, `GET /reports` without token → 403. The review tab synced all three and showed
  the water on its map. Shots `../T237-shots/`.

## Not done
- Native (phone) not run on a device; the outbox's native store and NetInfo path are covered only by the shared code.
- Production endpoint (owner: where reports go in prod; T86/T240). Privacy wording is Claude's draft (owner to approve).
- A section-target (unnamed water) report not sent e2e; resolution covered by the backend test.
- Test reports on staging R2 (ids e2e0000000000001, 6ed6aef2d971b275, 8a29a81206c5b9ce) left in place; delete if wanted.
