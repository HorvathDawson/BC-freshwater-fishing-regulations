# T559 — Privacy: backups and rate-limit records are deleted when the privacy page says they are

Status: IN PROGRESS — cfcdf7e07 (W2 backups: personal records daily-only within the page's dates, never monthly; prune + POST /run/forget-report; deploys check daily/ ≤ 30 d), fb5f387a3 (W6 counters purged daily). Owner parts answered: the digest may carry a report's words, trimmed (T627, owner 2026-11-10 "we want a nice easy to read summary", DONE); the production backup bucket was created 2026-11-18 with staging's lifecycle (daily 30 d, monthly 400 d) on the owner's "Yes, create it", first production backup run ok. Reads done by lane L3 2026-11-20 (below): both backups ok, daily-only, `personalHeldBack` 31 staging / 8 production, `pruned` 0 on both (nothing past its window yet). left (Claude): main chat review; the production bucket's lifecycle rules were not re-read (no Cloudflare login in the lane).

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 15 of 17; highest severity 3 of 5.

Line numbers are the reviewers' and may be off by a few; "Second opinion" is what the verifier corrected. Fixes are proposals: re-read the code before building.

## Owner 2026-11-09 (Q179)
"sure": shorten / purge the backups to match the privacy page. Owner: no.

## Today
### 1. Backups keep problem reports (with emails) and crash records long after the privacy page says they are deleted
Severity 3, divergence, confirmed; area gaps.
- Evidence: app/workers/feeds/src/backup.ts:17 BACKUP_PREFIXES copies all of `private/` (so private/reports/<day>/*.json and private/crashes/*.json) into daily/ and, on the 1st, monthly/ (backup.ts:72, 82). wrangler.toml:32-33 sets retention daily 30 d, monthly 400 d. The purge (reports.ts:241-266, crashes.ts:266-281) deletes only from the live bucket. The privacy page (app/tools/seo/html.ts:152, 177, 190-191) says reports are deleted 180 days after receipt 'including any email' and error records 90 days after last seen. Nothing in T240 or T404 mentions it.
- Example: A report received on Jan 31 with an email is copied into monthly/2027-02/ on Feb 1; the live copy is purged about Jul 30, the monthly copy lives until about Mar 2028 (400 d).
- Second opinion: Holds as stated. Two additions: the daily/ copies also outlive the purge by up to 30 days, and the 'deleted within 30 days on request' promise is broken by the monthly copy as well. The 30/400-day figures come from comments in wrangler.toml and backup.ts; the bucket's actual lifecycle rules are set outside the repo and were not checked.
- Proposed fix: Leave private/reports/ and private/crashes/ out of the monthly snapshot (daily only, 30 d) or out of the backup, and state the backup window on the privacy page. Add a test that every backed-up prefix holding personal data has a backup retention no longer than the page states.

### 2. Per-IP rate-limit objects (salted IP hash + last-seen time) are never deleted; the privacy page says they are kept 'for a day's counting'
Severity 2, divergence, confirmed; area gaps.
- Evidence: reports.ts:160-170 writes private/reports/limits/<hash>.json {last, day, n} per IP; crashes.ts:27, 175 does the same under private/crashes/limits/. purgeReports skips them on purpose (reports.ts:238-239, 250 'limits/ and anything else is not a report'); purgeCrashes iterates kindKeys, which filters limits out (crashes.ts:206-207, 271). No other code deletes them, and backup.ts:17 copies them too. Privacy page html.ts:151 and 177: 'we keep only a salted one-way hash of it for a day's counting'. Also purgeCrashes' result is discarded: index.ts:442 only logs a throw, a `failed` count tells nobody.
- Second opinion: The 'for a day's counting' wording is only in the crash section of the privacy page (html.ts ~177). The report section (~151) says only that a salted one-way hash is kept, with no duration, so the contradiction is with the crash wording alone. On the discarded purge result it is slightly worse than stated: purgeCrashes swallows its own errors and returns ok:false without throwing, so a failed crash purge is not even logged.
- Proposed fix: In the daily purge delete limits/*.json whose `day` is older than yesterday (both prefixes); feed purgeCrashes' ok/failed into the same status key and digest line the report purge uses.

## Work
Fix the items above in severity order, one commit per item where they are separable. Anything found and not done becomes a new ticket.

## Acceptance
A purged report or crash record is also gone from daily/monthly backups within the stated period (or the page states the real period, owner call). Rate-limit objects expire. The bucket lifecycle rules are read and recorded.

Depends: —. Size: S. Owner: yes (Q179).

## Added 2026-11-10 from the T608C audit
See `handoff/PLAN/app-integration/T608C-findings.md`: W2 (backups keep reports about 400 days against the promised 180/30), W6 (rate-limit counter objects never deleted against "a day's counting"). Do these with this ticket.

## L3 2026-11-20 reads (public `/status.json`, read-only; owner's go 01:56)
Staging (`bc-fishing-feeds-staging…/status.json`), backup 2026-11-19T09:25:31Z: ok, `prefixes ["daily/2026-11-19/"]` (daily only, no monthly), copied 179, `personalHeldBack` **31**, `pruned` 0, error null. `reportRetention`: ok, limits deleted 6 / kept 4, crashes deleted 3, testReports kept 3.
Production (`feeds.canifishthis.ca/status.json`), backup 2026-11-19T09:25:13Z: ok, daily only, copied 67, `personalHeldBack` **8**, `pruned` 0, error null. `reportRetention`: ok, reports kept 0, limits kept 8, crashes deleted 0.
Reading: the daily-only rule and the held-back count work on both; `pruned` is 0 because the daily set is two days old (nothing past its window), so the prune is proven by the unit tests, not yet by a live run. NOT read: either bucket's lifecycle rules (needs a Cloudflare login).
