# T559 — Privacy: backups and rate-limit records are deleted when the privacy page says they are

Status: IN PROGRESS — cfcdf7e07, fb5f387a3; W2 backups + W6 counters merged; reads 11-20 held back 31/8, pruned 0; left (Claude): re-read the production bucket's lifecycle rules (needs a Cloudflare login). (Earlier note: IN PROGRESS — cfcdf7e07 (W2 backups: personal records daily-only within the page's dates, never monthly; prune + POST /run/forget-report; deploys check daily/ ≤ 30 d), fb5f387a3 (W6 counters purged daily). Owner parts answered: the digest may carry a report's words, trimmed (T627, owner 2026-11-10 "we want a nice easy to read summary", DONE); the production backup bucket was created 2026-11-18 with staging's lifecycle (daily 30 d, monthly 400 d) on the owner's "Yes, create it", first production backup run ok. Reads done by lane L3 2026-11-20 (below): both backups ok, daily-only, `personalHeldBack` 31 staging / 8 production, `pruned` 0 on both (nothing past its window yet). left (Claude): main chat review; the production bucket's lifecycle rules were not re-read (no Cloudflare login in the lane).)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T559 | Privacy: backups and rate-limit records are deleted when the privacy page says they are | IN PROGRESS — left (Claude): re-read the production bucket's lifecycle rules (needs a Cloudflare login) | cfcdf7e07, fb5f387a3 | proof recorded in the ticket (L3 2026-11-20); lifecycle rules not re-read |

Source: data pipeline review 2026-11-09 (owner-led; 14 areas + a gap check, every finding re-read against the code and release data by a second reviewer). Rank 15 of 17; highest severity 3 of 5.

## Owner 2026-11-09 (Q179)
"sure": shorten / purge the backups to match the privacy page. Owner: no.

## Done (cfcdf7e07 W2, fb5f387a3 W6; `handoff/PLAN/app-integration/T608C-findings.md` W2 / W6)
- Backups (`app/workers/feeds/src/backup.ts`): personal records (`private/reports/`, `private/crashes/`) daily-only
  within the privacy page's dates, never monthly; prune + POST /run/forget-report; deploys check daily/ ≤ 30 d. The page
  (app/tools/seo/html.ts) says reports are deleted 180 days after receipt 'including any email', error records 90 days
  after last seen, 'deleted within 30 days on request'.
- Rate-limit counters (`private/reports/limits/`, `private/crashes/limits/`) purged daily (the page: 'for a day's
  counting').
- The production backup bucket was created 2026-11-18 with staging's lifecycle (daily 30 d, monthly 400 d) on the
  owner's "Yes, create it"; first production backup run ok.

## Work (left)
Re-read and record both buckets' lifecycle rules (needs a Cloudflare login; set outside the repo, never checked).

## Acceptance
A purged report or crash record is also gone from daily/monthly backups within the stated period (or the page states the real period, owner call). Rate-limit objects expire. The bucket lifecycle rules are read and recorded.

Depends: —. Size: S. Owner: yes (Q179).

## L3 2026-11-20 reads (public `/status.json`, read-only; owner's go 01:56)
Staging (`bc-fishing-feeds-staging…/status.json`), backup 2026-11-19T09:25:31Z: ok, `prefixes ["daily/2026-11-19/"]` (daily only, no monthly), copied 179, `personalHeldBack` **31**, `pruned` 0, error null. `reportRetention`: ok, limits deleted 6 / kept 4, crashes deleted 3, testReports kept 3.
Production (`feeds.canifishthis.ca/status.json`), backup 2026-11-19T09:25:13Z: ok, daily only, copied 67, `personalHeldBack` **8**, `pruned` 0, error null. `reportRetention`: ok, reports kept 0, limits kept 8, crashes deleted 0.
Reading: the daily-only rule and the held-back count work on both; `pruned` is 0 because the daily set is two days old (nothing past its window), so the prune is proven by the unit tests, not yet by a live run. NOT read: either bucket's lifecycle rules (needs a Cloudflare login).
