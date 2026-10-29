# T237 — report a problem: an endpoint and a review-app queue

Status: OPEN — not started.

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
