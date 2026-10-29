# T271 — Problem reports deleted automatically after 180 days

Status: DONE in code 2026-10-29; worker deploy is the owner's (owner 2026-10-29: "yes this sounds good"; privacy@canifishthis.ca mailbox is set up by the owner).

## Work
1. Feeds worker (app/workers/feeds) daily cron deletes report objects older than 180 days (by stored timestamp), staging bucket
   for staging, prod config ready; logs the count in /status; a failure goes to the daily digest.
2. Privacy page: "reports are deleted 180 days after they are received"; contact line = privacy@canifishthis.ca (no
   placeholder). Remove the OWNER-TODO items for the mailbox + retention.

## Acceptance
Unit test: 181-day-old deleted, 179-day-old kept; staging run shows the count; page wording matches.

Depends: none. Size: S. Owner: no (prod worker deploy is the owner's command).

## Outcome (2026-10-29)
- `purgeReports` (app/workers/feeds/src/reports.ts): daily on the :25 cron beside the backup; deletes private/reports/<day>/<id>.json whose stored
  `received` is >180 days old; result in `private/feeds/report-retention.json`, shown as `reportRetention` in /status.json; failure -> non-urgent health
  event (daily digest). `POST /run/purge-reports` (Bearer) for a manual run. test/retention.test.ts: 181 d deleted, 179 d kept, failure reported.
- Privacy page: "deleted automatically 180 days after they are received"; privacy@canifishthis.ca already present.
- Owner: `npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml`, then POST /run/purge-reports (Bearer RUN_TOKEN) and check /status.json `reportRetention`.
