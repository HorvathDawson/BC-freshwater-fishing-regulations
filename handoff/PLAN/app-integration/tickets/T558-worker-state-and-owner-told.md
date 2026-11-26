# T558 — Feeds worker: a failed state read is not written back empty; the owner hears when crons, backup or email stop; queued reports are not lost

Status: PARTIAL — afbdaf2d2 (W3 reads), 924221934 (W8 + item 5), dd7032d6c (W9), 026ca775b (W13 etag), 069d61d45 (item 3 channel), e777f33e8 (item 2 daily jobs watched), cb1ac2397 (item 6 no window), 306056ce9 (item 4 worker half). left (Claude): item 2's outside heartbeat (a second worker), item 4's app outbox (refused queued report shown, send() honest), item 7 (review app), item 8 (report/3 locator).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T558 | Feeds worker: a failed state read is not written back empty; the owner hears when crons, backup or email stop; queued reports are not lost | PARTIAL — left: outside heartbeat, app outbox half of item 4, items 7 (review app) and 8 | afbdaf2d2, 924221934, dd7032d6c, 026ca775b, 069d61d45, e777f33e8, cb1ac2397, 306056ce9 | data pipeline review 2026-11-09 (verified); rank 14, severity 3, 8 findings; + T608C W3, W8, W9, W13 |

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 14 of 17; highest severity 3 of 5.

Line numbers are the reviewers' and may be off by a few. Fixes are proposals: re-read the code before building. Also
covers T608C W3, W8, W9, W13 (`handoff/PLAN/app-integration/T608C-findings.md`).

Done: 1 failed read never written back empty (afbdaf2d2 W3, 026ca775b W13 etag); 2 daily jobs watched (e777f33e8); 3
email channel (069d61d45); 4 worker half (306056ce9); 5 status before notify + cron logs (924221934, W8); 6 no date
window (cb1ac2397); W9 purge failures (dd7032d6c).

## Work (left)
2. **Outside heartbeat** (Severity 3) (a second worker; no GitHub schedule): an outside check (the site or data worker, or an uptime
   ping on /status.json `updated`) that alerts when no feed has run within its cadence, so the whole worker stopping is
   noticed.
4. **App outbox half.** (Severity 3, reaches an angler.) app/packages/data/src/report/outbox.ts:66-73: any status other than 2xx, 5xx, 408, 429 is
   'refused'. flush() (outbox.ts:96-104) removes a refused report from the queue and returns only the sent count; its
   one background caller discards even that (update/service.ts:457 `void o.outbox.flush().catch(() => {})`). No
   reportFailure. send() with a non-empty queue (outbox.ts:80-84) then answers `{kind:'sent'}` because the id is no
   longer queued, even when it was refused. Fix: treat only 400/413 with a JSON `error` body from our worker as
   permanent; keep everything else queued; on a permanent refusal call reportFailure and keep the text for the reader;
   send() returns the real outcome.
7. **Review app Reports tab.** (Severity 2.) curation-review/backend/reports.py:37 DEFAULT_URL is the staging worker's /reports; the
   token defaults to app/workers/feeds/.run-token (reports.py:38, 49-56); one tab cannot show both. Triage state is
   localStorage only (reports.py:17-18). No backend route for GET /crashes (app.py:1300-1319 has reports only). fetch()
   defaults to 90 days while reports live 180 (reports.py:60, worker RETENTION_DAYS). Fix: make the source explicit in
   the tab (staging / production, both listed, each with its own token); store triage state in a small file beside
   marks; add a read-only crashes list.
8. **report/3 locator.** (Severity 2.) A report's `section` is a section handle of one tiles vintage (core/src/menu/report.ts:89,
   194-195, 242) plus water id, part and the MAP CENTRE rounded to 3 places (report.ts:111-115), not the tap point; the
   review app resolves the handle only for this build's handle digest (curation-review/backend/reports.py:86-88,
   132-137, 158-159). Example: a report made on tiles d5242c0c70509877.* shows vintage 'other' and no geometry once the
   review app serves a newer atlas. Fix: add a vintage-free locator to report/3: the tap's lon/lat (4 places) or the
   section's blue-line key and measure from the tiles; or keep old releases' handle tables so the review app can
   translate (release id is in the report).

## Acceptance
A failed or corrupt R2 read of status.json / notify state stops the write (test). An outside heartbeat (worker-started, no GitHub schedule) tells the owner when the worker or the backup stops. feeds.sh refuses a deploy with no email secret. A queued report the endpoint refuses is shown to the user as not sent.

Depends: —. Size: M. Owner: no.
