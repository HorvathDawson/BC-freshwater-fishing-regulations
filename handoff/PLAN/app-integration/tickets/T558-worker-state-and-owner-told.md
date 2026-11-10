# T558 — Feeds worker: a failed state read is not written back empty; the owner hears when crons, backup or email stop; queued reports are not lost

Status: OPEN.

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 14 of 17; highest severity 3 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Today
### 1. A failed R2 read of status.json or notify/state.json is treated as 'no state' and then written back empty
Severity 3, fragility, confirmed; area feeds-worker.
- Evidence: status.ts:80-89 readStatus catches every error (including a thrown R2 get) and returns {feeds:{}}; index.ts:379-384 then records one job and puts it, erasing the other feeds' lastSuccess, failures, announced and 7-day health. events.ts:81-89 readState does the same; deliver (events.ts:166, 225) writes it back, erasing `told` (a year of dedupe keys) and `pending` (queued digest events). dispatch.ts:45-50: a read error gives last=null, so the workflow is started again. All puts are unconditional (io.ts:82-86, no etag), so two runs ending together also lose one write.
- Second opinion: Code is as claimed; a corrupt (unparseable) object takes the same path. Overstated: the 'two runs ending together' race is unlikely because the crons are staggered (:05/:35, :15, :45, :25) and feeds.sh runs the jobs one after another. The daily backup of private/ limits the loss to one day if someone notices. No evidence it has happened.
- Proposed fix: Separate 'object absent' (null) from 'read failed' (throw). On a failed read, skip the write and log it. Use R2 conditional put (onlyIf etag) with one retry for status.json and notify/state.json.

### 2. Nothing notices when the crons stop or the backup does not run
Severity 3, missing-check, confirmed; area feeds-worker.
- Evidence: status.ts:92-98 refresh() (staleness) is called only from record() inside a job run (status.ts:143) and on GET /status (index.ts:546), so a 'stale' alert needs some job to run. index.ts:420-427 backup() tells the owner only when runBackup returns not-ok; no code compares private/feeds/backup.json `at` with now, and statusHtml (status.ts:230-262) does not show the backup at all (JSON only). The same holds for the purge, skill and run-timing jobs.
- Example: Staging backup.json: at 2026-11-09T09:25:50Z, ok, 159 objects. If the 09:25 cron stopped firing, this would stay 'ok: true' indefinitely.
- Second opinion: One mitigation the finder under-weighted: record() refreshes every feed, so a lost DFO or in-season cron is caught as 'stale' by the next gauge run. The uncovered cases are the whole worker stopping and the non-feed jobs (backup, purge, skill, run timing). I did not re-read the staging backup.json example.
- Proposed fix: In every run(), also check backup.json's age (over 36 h gives one urgent health event) and show it on /status. Add one outside check (the site or data worker, or an uptime ping on /status.json `updated`) that alerts when no feed has run within its cadence.

### 3. With no email secret, every owner notice is logged and recorded as told; deploys do not check for it
Severity 3, missing-check, partly; area feeds-worker. Related ticket: T240.
- Evidence: events.ts:254-277 send(): when emailTo/emailFrom/sendEmail are not all set and there is no webhook, it logs and returns 'log'; deliver (events.ts:198-206) then marks the keys told. NOTIFY_EMAIL_TO is a secret (wrangler.toml comment; not in [vars]). deploy/staging/feeds.sh:14 and deploy/prod/feeds.sh:27 verify only REPORT_SALT. The channel is visible only on the authed POST /notify/preview (index.ts:575), not on /status.
- Second opinion: The behaviour is by design, not a bug, and prod set-up already asks for the secret (feeds-placeholder.sh, the T240 checklist). What is missing is a mechanical check in feeds.sh and the channel on /status. The same gap applies if the EMAIL binding is absent (sendEmail undefined). Fold into T240 as one line.
- Proposed fix: Have both deploy scripts fail when NOTIFY_EMAIL_TO is missing, as they do for REPORT_SALT. Show the channel on /status.json. In production, do not mark keys told when the channel is 'log'.

### 4. A queued problem report that the endpoint later refuses is dropped without a word; a retry over UTC midnight is stored twice
Severity 3, wrong-or-missing-data, confirmed, reaches an angler; area gaps.
- Evidence: app/packages/data/src/report/outbox.ts:66-73: any status other than 2xx, 5xx, 408, 429 is 'refused'. flush() (outbox.ts:96-104) removes a refused report from the queue ('either way it leaves the queue') and returns only the sent count; its one background caller discards even that (update/service.ts:457 `void o.outbox.flush().catch(() => {})`). No reportFailure. send() with a non-empty queue (outbox.ts:80-84) then answers `{kind:'sent'}` because the id is no longer queued, even when it was refused. Separately the worker dedupes by `private/reports/<received day>/<id>` (reports.ts:197-203), so the same id re-sent the next UTC day is a second object.
- Example: Worker deployed without the route, or a Cloudflare challenge answering 403, or 413: every report made offline is deleted from the phone on the next flush and the reader was told it would be sent.
- Second opinion: Holds. The header comment shows dropping refused reports is deliberate for direct sends, where the reader is told; the defect is the queued path and the false 'sent'. A further way to lose a report: the worker's own 400 from validateReport (for example an older app's payload after a schema change) deletes a queued report the same way. The duplicate shows in GET /reports and the Reports tab but not in the digest, which dedupes on report:<id>. The 403/413 examples follow from the code path; none was observed.
- Proposed fix: Treat only 400/413 with a JSON `error` body from our worker as permanent; keep everything else queued. On a permanent refusal call reportFailure and keep the text for the reader. Make send() return the real outcome. Dedupe on the worker by id across the last two day folders.

### 5. Status write depends on the notification step, and cron runs log neither the error nor the run log
Severity 2, fragility, confirmed; area feeds-worker.
- Evidence: index.ts:374-387: one try block does readStatus, record, deliver(...) and only then priv.put(STATUS_KEY). Anything thrown in between (email render/compose on an odd event) skips the status write, and the only trace is log('status/notify: not written'). index.ts:628: the scheduled handler prints only {job, ok, wallMs, meter}; r.error and r.log are dropped. `announced` is set at :381 before deliver runs.
- Second opinion: Real but hypothetical: no template bug is shown. Slightly overstated: on the normal path the run's error is stored in status.json lastError, so the cron log gap only matters when the status write itself is skipped. Line numbers are off by a few (374-387 is 375-390, 628 is 627, 381 is 384).
- Proposed fix: Write status.json before deliver, and again after it for `announced`. In scheduled(), console.error r.error and the tail of r.log whenever the run is not ok or the status write failed.

### 6. The owner is told about a report or a new crash kind only if a gauge run tells it within two days; older ones are never told
Severity 2, fragility, partly; area gaps.
- Evidence: reports.ts:292-310 reportEvents scans `days(now, 2)` (today and yesterday, UTC, by received-day folder) and at most 200 keys a day; crashes.ts:226-239 crashEvents tells a kind only when its `first` is today or yesterday. Both are called from the gauge run only (header comments reports.ts:11, crashes.ts:18). Anything not yet in the outbox when its folder leaves the two-day window is never raised again; it stays visible only to someone who opens the review app's Reports tab and presses Sync, and crashes have no tab at all (GET /crashes by hand).
- Example: The gauge job failing before its notify step for a weekend (the feeds-worker review found it fails whole at 00:05 UTC and that status depends on the notification step): Friday's reports and crash kinds never reach the digest.
- Second opinion: The window and the caps are real, but the gauge run fires every 30 minutes, so a report is missed only if every run for about 48 hours fails before this step (or readState throws each time), or more than 200 reports land in one day folder. That is an edge case with no observed instance. The 'fails for a weekend' example leans on another reviewer's finding I did not check. The fix is still cheap and right: drop the date window, since the outbox key already dedupes.
- Proposed fix: Tell every stored report/kind whose key the outbox does not know, with no date window (the outbox key already dedupes); list by the retention window or keep a small 'untold' index. Add a status line 'reports stored / told'.

### 7. The review app's Reports tab reads the staging worker only, keeps review state in the browser, and has no view of crashes
Severity 2, missing-check, partly; area gaps.
- Evidence: curation-review/backend/reports.py:37 DEFAULT_URL is the staging worker's /reports; the token defaults to app/workers/feeds/.run-token (reports.py:38, 49-56). Production's endpoint is decided (ledger Q171: https://feeds.canifishthis.ca/report) but neither T240 nor T404 names switching or adding the production source. The reviewer's status (new / looking / fixed / won't fix) is localStorage only (reports.py:17-18). There is no backend route for GET /crashes (app.py:1300-1319 has reports only). fetch() defaults to 90 days while reports live 180 (reports.py:60, worker RETENTION_DAYS).
- Example: After the production switch every Play build posts to the prod bucket; pressing Sync still lists staging and shows nothing new, with no error.
- Second opinion: 'Staging only' is overstated: the URL and token can be set by environment variable, and the API returns the source URL. What is true is that the default is staging, nothing in the production switch list (T404, Q171) says to change it, and one tab cannot show both. Production is not live yet, so no report is being missed today. Browser-only triage state is a documented design choice (reports.py:17-18). Having no crashes view is a missing feature, not a defect. Best handled as a line added to T404's switch list.
- Proposed fix: Make the source explicit in the tab (staging / production, both listed, each with its own token) and add it to T404's switch list; store triage state in a small file beside marks; add a read-only crashes list.

### 8. A problem report's 'where' is a section handle of one tiles vintage; after the next atlas rebuild the tapped place cannot be recovered
Severity 2, fragility, partly; area gaps.
- Evidence: The app sends `section` = the tapped section's handle (core/src/menu/report.ts:89, 194-195, 242: 'only meaningful beside release and tiles'), plus water id, part number and the MAP CENTRE rounded to 3 places (report.ts:111-115), not the tap point. The review app resolves the handle only when the report's tiles id has this build's handle digest; otherwise 'never resolved' (curation-review/backend/reports.py:86-88, 132-137, 158-159). Handles are re-dealt on each atlas build (common/section_handles.py), and reports are kept 180 days.
- Example: A 'wrong place on the map' report made on tiles d5242c0c70509877.* shows vintage 'other' and no geometry as soon as the review app serves a newer atlas; only the water id and a part number (also vintage-bound) remain.
- Second opinion: The mechanism is as described, but 'cannot be recovered' is too strong. The report still carries the water id, place name, part and a map centre to about 100 m, plus the release and tiles ids, so the handle can be translated by hand while that release's handle table exists (live and previous releases are kept). What is lost is automatic resolution and the map highlight in the Reports tab. I did not read common/section_handles.py to confirm that handles are re-dealt on every build; I relied on the reports.py docstring.
- Proposed fix: Add a vintage-free locator to report/3: the tap's lon/lat (4 places) or the section's blue-line key and measure from the tiles; or keep old releases' handle tables so the review app can translate (release id is in the report).

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
A failed or corrupt R2 read of status.json / notify state stops the write (test). An outside heartbeat (worker-started, no GitHub schedule) tells the owner when the worker or the backup stops. feeds.sh refuses a deploy with no email secret. A queued report the endpoint refuses is shown to the user as not sent.

Depends: —. Size: M. Owner: no.

## Added 2026-11-10 from the T608C audit
See `handoff/PLAN/app-integration/T608C-findings.md`: W3 (failed read written back empty, six sites), W8 (cron-path log lines never printed; a failed status/outbox step leaves no record), W9 (purge failures unread), W13 (status/notify state written with no lock). Do these with this ticket.
