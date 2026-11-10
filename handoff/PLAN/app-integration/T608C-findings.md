# T608 C — The feeds worker from the outside: findings

Read-only review of `app/workers/feeds/` at commit `775f68f0c` (2026-11-10). Nothing was changed, deployed or run.
Four plain GETs were made to staging's public routes (`/status.json`, `/status`, `/report`, `/`). No POST, no token use.

## Summary

- **20 findings: 2 high, 6 medium, 12 low.** No way was found for an anonymous caller to read private data, run a job,
  send mail at will, or get past the run token. The token check, the HTML/email escaping and the report validation hold.
- **The three worst**
  1. **W1** — the per-address limits on `POST /report` and `POST /crash` can be walked around with IPv6, and there is no
     overall daily cap. A flood is stored without limit, can jam the daily digest, and hides real reports.
  2. **W2** — the privacy page says a report is gone after 180 days (30 days on request). The daily backup copies every
     report into a second bucket whose monthly copies are kept about 400 days, and the digest email carries the words too.
  3. **W3** — a storage read that fails is treated as "nothing stored yet" in eight places. One bad read can wipe the
     queued digest and the status, and (with a DFO fetch failure in the same run) publish a region with no places.
- **Blocking Play closed testing on production?** No security hole blocks it. My judgment: settle **W2 and W6** first,
  because the privacy page is a published promise and production is where real testers' reports land (a wording change
  or a small code change, either works). W1's two small fixes are worth shipping in the same deploy. W18 is a
  practical gap: the review app's Reports tab reads staging unless told otherwise.

## Findings, ranked

Size: S = under an hour, M = half a day. Paths are under `app/workers/feeds/` unless they start elsewhere.

| id | sev | where | what can happen | evidence | fix | size |
|----|-----|-------|-----------------|----------|-----|------|
| W1 | High | `src/reports.ts:156-177,209` `src/crashes.ts:202-203` `src/guard.ts:27-29` | Every limit is keyed on the full address. One home IPv6 line owns 2^64 addresses, so each request can look like a new caller: 30 reports and 40 crash kinds a day become unlimited. Each one stores a report (up to 16 KB) plus a counter object that is never deleted. There is no overall cap. Knock-on: the digest reads only the first 1,000 keys of a day (`reports.ts:300`), so later real reports are never told; every told report sits in `notify/state.json` (`pending`) in full, rewritten each gauge run; one oversized digest that fails to send is retried whole each run and holds back the other digest lines. | Key is `${cls}:${ip}` and `sha256(salt:ip)`; no code groups IPv6 by /64; no count of the day's total anywhere. | Key IPv6 on its /64. Add a daily total (say 300 reports, 500 new crash kinds): past it answer 429 and tell the owner once. Cap the digest's report lines and say "and N more". | S–M |
| W2 | High | `src/backup.ts:17,76-86` vs `app/tools/seo/html.ts:152,189-191` | The privacy page: "Reports are deleted automatically 180 days after they are received, including any email in them" and, on request, "within 30 days". The backup copies all of `private/` (reports, emails, crash kinds, counters) to `daily/` (kept 30 days) and on the 1st to `monthly/` (kept about 400 days, per the comments). A purged or owner-deleted report lives on in monthly copies for over a year. The digest email also carries the report's words, install id and map centre (`reports.ts:325-339`) into the owner's mailbox, which nothing purges. | `BACKUP_PREFIXES = ["private/", …]`; staging's last backup copied 176 objects. Lifecycle days are from comments (`wrangler.toml:32-33`), not checked on Cloudflare. | Either leave `private/reports/` and `private/crashes/` out of `monthly/` (the 30-day daily copy then fits the promise), or say the backup period on the privacy page. Add "delete from backups" to the delete-on-request steps. | S |
| W3 | Med | `src/events.ts:81-88` `src/status.ts:80-88` `src/dfo/live.ts:301-308,347` `src/gauges/publish.ts:771-778` `src/dispatch.ts:44-49` `src/index.ts:193-197` | A read that throws (R2 having a bad minute) is caught and answered as "empty". Then the empty state is written back. Outbox: every "already told" key and the whole queued digest are gone, so old items are told again and queued ones may never be. Status: every feed reads "never run" until its next run, still HTTP 200. DFO: the last feed reads as `{}`, so a region whose fetch also fails that run is published with no places at all. Gauges: stations that failed are dropped, not carried. GitHub: the same HYDAT version can start its workflow twice. | Each is `try { get } catch { return fresh }`; `null` (absent) and a thrown error take the same path. | One helper that returns null only for "absent" and rethrows anything else; let the run fail (the last good files stand) and say so. | M |
| W4 | Med | `src/inseason.ts:145-147,53-58` | The B.C. page is fetched with redirects followed, and any page with an `accordion-container` holding 3-column tables is read as in-season rows. The first table row is skipped without reading it. If the URL is ever redirected to another gov.bc.ca page of the same build, its tables are published as regulation changes (the row-drop guard only catches *fewer* rows). The owner is emailed the diff at once, so it would be seen, but the app shows it meanwhile. | No check of `r.url` / `r.redirected`; no check that the header cells say water / change / effective; no check that section names are "Provincial" or "Region N". | Refuse a redirected answer; require the three header words and known section names, else fail the run as `parse`. | S |
| W5 | Med | `src/gauges/publish.ts:484-500,779` | If `feeds/live/clim.json` is missing, the run carries on with no envelope: every gauge is published with no rank ("how high for the date"), and the run is recorded ok. Only the share-card step mentions it, as a digest line about cards. | `climText` null → `clim = null` → `envelope = {}`; `withPercentile` is a count, never a gate. Staging today: 420 of 430 ranked. | Missing `clim.json`, or ranked stations falling to a small share of last run's, fails the run (index kept). | S |
| W6 | Med | `src/reports.ts:165-175,244-255` `src/crashes.ts:235,335` vs `app/tools/seo/html.ts:151,177` | The privacy page says the address hash is kept "for a day's counting". The counter objects (`private/reports/limits/…`, `private/crashes/limits/…`: salted hash, last-seen time to the millisecond, day, count) are never deleted by any purge, and are in the backups too. | `purgeReports` skips anything not in a day folder; `kindKeys` filters `limits/` out before `purgeCrashes`. | In the daily purge, delete limit objects whose `day` is older than yesterday. | S |
| W7 | Med | `deploy/staging/rotate-run-token.sh:20,31,38` `app/workers/feeds/.gitignore:6-8` | Rotation writes the old token to `.run-token.prev`. That name is not git-ignored (`git check-ignore` says so). It exists during every rotation and stays if the script fails; if the secret upload failed, it is the live token. Another session's `git add -A` would commit it. | `.gitignore` lists `.run-token` and `.run-token.prod` exactly. | Ignore `.run-token*` (and `.dev.vars`). | S |
| W8 | Med | `src/index.ts:291-293,388-390,630` | In a scheduled run the lines "reports: not read", "cards: not drawn" and "status/notify: not written" go into `r.log`, which the cron path never prints. If the status/outbox step fails, nothing anywhere records it. In-season is worst: the new snapshot is already saved, so the diff that should have been emailed is never computed again. | Cron logs only `{job, ok, wallMs, meter}`. `runInSeason` writes `latest.json` before the events are handed over. | `console.error` those three lines. Save the in-season snapshot only after its events are queued (or queue first). | S |
| W9 | Low | `src/index.ts:441-442` `src/crashes.ts:330-345` | A crash purge that cannot list or delete returns `ok: false`, and nobody reads it: no status, no email. Same for the 7-day test-report purge (per-key failures swallowed). The 90-day promise could lapse unseen. | The `try/catch` only catches a throw; `purgeCrashes` never throws. | Record both results beside `report-retention.json`; tell on failure like the report purge. | S |
| W10 | Low | `src/reports.ts:274-293` `src/crashes.ts:255-273` | `GET /reports` stops at 500 and at 366 days, `GET /crashes` at the first page of kinds, and both skip unreadable objects, without saying so. The review app cannot tell a full list from a cut one. | No `truncated` or `skipped` field in either answer. | Add both fields; show them in the Reports tab. | S |
| W11 | Low | `src/reports.ts:185-193` `src/crashes.ts:191-195` | The size check trusts `Content-Length`; without that header the whole body is read into memory before the check. The check counts characters, not bytes (a 16,384-character body can be about 49 KB). A crash's `app` value of `constructor` stores a junk version entry. | `await req.text()` then `raw.length`. | Read the stream with a byte cap; build `versions` with `Object.create(null)` or refuse odd names. | S |
| W12 | Low | `src/index.ts:546-556` | `/status` and `/status.json` are open to anyone: stack traces with function names and line numbers, internal file names and format versions, how many reports are stored, backup size, which stations fail. No personal data. The HTML page sends no `X-Content-Type-Options`, frame or CSP header; the 405s send no `Allow`. | Seen on staging: `lastError` held a 5-frame stack; `reportRetention.kept: 11`; `backup.bytes`. | Trim errors to their first line in the served copy, or put the detail behind the token. Add the three headers. | S |
| W13 | Low | `src/index.ts:377-387,416` `src/events.ts:166,225` | `status.json` and `notify/state.json` are read, changed and written with no lock. Two jobs at once (a hand-run job during a cron, backup and purge telling in the same minute) and the last writer wins: one job's record or told keys are lost. Today's runs take 2–13 s and sit 10 minutes apart, so crons alone do not overlap. The per-address counter has the same race (parallel POSTs can pass more than the day's count). | Staging wall times: gauges 13.0 s, DFO 7.0 s, in-season 2.2 s, backup 55.8 s. | Write with an R2 etag condition and retry once. | M |
| W14 | Low | `src/inseason.ts:246-247,411-412` | A missing `feeds/inseason/matches.json` (not a broken one) publishes every row as unmatched and records the run ok. The app then lists every notice "not on the map yet" and none on its water. | `matchedWaters(null)` returns null; only a wrong format throws. | Treat missing as a failed run once a matched feed has ever been published. | S |
| W15 | Low | `src/index.ts:617` `src/gauges/publish.ts:482,1040` | With the token, `POST /run/gauges?limit=5` rewrites the public `index.json` with 5 stations until the next cron. A slip, not an attack. | `ids.slice(0, limit)`, then the index is written from `ids`. | Honour `limit` only with `dryNotify`, or never write the index when it is set. | S |
| W16 | Low | `src/guard.ts:51-55` `deploy/prod/feeds.sh:36` `wrangler.toml:23` | One token reads reports (emails included), runs every job and sends mail; it is shared by the review app, the APK script and the test bed. Production has no rotation script. Scripts pass it on the `curl` command line (visible to other processes on the Mac). The owner's Gmail address is in a committed comment although `NOTIFY_EMAIL_TO` is kept as a secret. | Compare is constant-time and correct; token is 192 bits; guessing is capped at 10 a minute per address. | A second, read-only token for `GET /reports` and `/crashes`. A production twin of the rotation script. `curl -H @file`. | M |
| W17 | Low | `src/dfo/fetch.ts:162` `src/inseason.ts:147` `src/gauges/publish.ts:55-63,67-87` `src/runtiming.ts:16-23` | Upstream bodies are read whole with no size cap (128 MB worker memory), redirects are followed everywhere, and ECCC values are taken as given (no range or date check). A date string on ECCC's HYDAT listing or the NuSEDS record starts a GitHub workflow that uploads to R2 with no owner step (at most one a day / one a week). | Government hosts; DFO pages are well guarded (size floor, three sentinels, reconcile against entries). | Cap body size per source; refuse negative or absurd flows and future timestamps. | M |
| W18 | Low | `curation-review/backend/reports.py:37` | The review app's Reports tab reads the **staging** worker by default. Reports from the production app reach the owner only as digest lines unless `CURATION_REPORTS_URL` and the production token are set. | `DEFAULT_URL` is the staging address. | A staging/production switch in the tab, or read both. | S |
| W19 | Low | `src/reports.ts:204-206` `src/crashes.ts:206-207,336` | On staging, anyone can add `test: true`: the report or crash is stored where the digest and the default list never show it. Test crash kinds live 7 days after last seen, so re-sending keeps them. Production ignores the field (checked). | `isTest` needs only `FEEDS_ENV === "staging"`. | Accept `test` only with the run token, or cap the test store's size. | S |
| W20 | Low | `src/crashes.ts:281-289` | Each gauge run (about 40 a day) re-reads every crash kind the outbox has not told. Kinds that are never told (network, expected, background, older than two days) are read again every run for 90 days. Small cost today; grows with W1. | `known()` is true only for told or queued keys. | Decide from the list alone (keep a small "seen" index), or skip kinds whose key is older than two days. | S |

## Checked and found sound

- **Token**: constant-time compare; never true when unset; rate limit runs before the token check. `.run-token` and
  `.run-token.prod` are git-ignored, untracked, and have no commit in any branch's history. No secret value is logged
  by the worker; the GitHub token appears only in an outgoing header.
- **Escaping**: every value put into the status page and the HTML email goes through `esc`; email subjects with
  non-ASCII or control characters are base64-encoded, so a report cannot add mail headers.
- **Validation**: unknown keys refused per format version; lengths capped; install id and report id are fixed-shape hex;
  an existing report id is never overwritten. The reporter's email is never put in a digest.
- **CORS**: `*` only on `/report`, `/crash` and 429s (POST and OPTIONS). `/status.json` sends none. Seen on staging.
- **Cache headers**: every worker answer is `no-store` or uncached. DFO and in-season files are stored with
  `max-age=300, stale-while-revalidate=3600`: an old answer can last about 65 minutes, no longer.
- **Stale marking**: a failed DFO region is published with `stale` and its failure count; a failed gauge keeps its
  last entry marked `stale` for at most 3 days; an empty or shrunken in-season page writes nothing.

## Route inventory

All routes pass the per-address limiter first (per Cloudflare location, lets requests through if the limiter errors).
"Public" limit: 60 a minute. "Auth" limit: 10 a minute, counted before the token is looked at.

| route | method | who | reads | writes / spends | limits and caps |
|-------|--------|-----|-------|-----------------|-----------------|
| `/report` | OPTIONS | anyone | nothing | nothing | public; CORS `*` |
| `/report` | POST | anyone | `{ok, id}` or an error word; whether an id exists today | 1 read; if accepted 1 read + 2 writes (counter, report up to 16 KB); a digest line next day | public + 1 per 10 s (edge) + 10 s gap and 30 a day (stored counter); 16 KB; see W1, W11 |
| `/crash` | OPTIONS | anyone | nothing | nothing | public; CORS `*` |
| `/crash` | POST | anyone | `{ok}` | 2 reads + 2 writes (counter, kind up to about 8 KB); a digest line for a new defect | public + 1 s gap and 40 a day; 8 KB; `env: "local"` answered 202, not stored |
| `/report`, `/crash` | other | anyone | 405 | nothing | public |
| `/status` | GET | anyone | HTML status of the three feeds | 4 reads | public; `no-store`; see W12 |
| `/status.json` | GET | anyone | the same as JSON, plus backup, purge and model-score results | 4 reads | public; `no-store` |
| `/reports` | GET | token | stored reports since `?since` (emails included); `?test=1` | up to 366 lists + 500 reads | auth; `since` shape checked; see W10 |
| `/crashes` | GET | token | crash kinds since `?since`; `?class`, `?test`, `?background` | 1–2 lists + up to about 1,000 reads | auth; `since` and `class` checked |
| `/notify/test` | POST | token | which channel sent | one email to the owner | auth |
| `/notify/apk` | POST | token | which channel sent | one email to the owner; `url` must be https, fields clipped | auth |
| `/notify/preview` | POST | token | the queued digest as it would be sent | 1 read; sends nothing | auth |
| `/run/gauges` `/run/dfo` `/run/inseason` | POST | token | the run's result, with stack on error | a full feed run (about 440 fetches for gauges), emails as due; `?regions`, `?limit`, `?dryNotify` | auth; see W15 |
| `/run/backup` | POST | token | the backup result | copies about 176 objects (141 MB on staging) | auth |
| `/run/purge-reports` | POST | token | the purge result | lists `private/reports/`, deletes old ones | auth |
| `/run/skill` | POST | token | the model score | reads the archive month; one digest line; `?month` shape checked | auth |
| `/run/runtiming` | POST | token | the check's result | 1 fetch; may start a GitHub workflow | auth |
| `/run/check-github` | POST | token | whether the GitHub token works | 1 fetch to GitHub; starts nothing | auth |
| anything else | any | anyone | 404 (a wrong method on a `/run/…` path is 404 before the token is looked at) | nothing | public or auth by path |
| cron (9 schedules) | — | Cloudflare | — | gauges every 30/60 min; DFO and in-season every 6 h Jun–Oct, else daily; backup + purge daily 09:25 UTC; model score monthly; run timing weekly | — |

Stored about a person: a report (words, optional email, contact tick, install id, water/part/section, day, screen, map
layer, zoom and centre, app/platform/release, up to 20 recent error lines, time received) for 180 days; a crash kind
(error name, message, stack, up to 100 install ids, water id and pane, versions, count, first/last time) until 90 days
after it was last seen; a salted address hash with last-seen time and day count (never deleted: W6). No address, user
agent or country is stored. Test store (staging only): reports 7 days by day folder, crash kinds 7 days after last seen.

## Not checked

- The **production** worker, its bucket, its secrets and its backup bucket's lifecycle rules (never touched).
- Any token or secret value; whether Cloudflare's own logs keep the `Authorization` header (observability is on).
- Anything needing a POST or a token on staging: the limits, the 429s, the size caps and the 403s are from the code only.
- The account's subrequest limit per run. If W1's flood grows the report and crash lists, the gauge run's extra reads
  count against it; the number was not looked up.
- Cloudflare's size limit for one email (W1's "digest too big to send" is reasoned, not seen).
- Who serves `feeds/…` to the app and with what cache headers: that code is not in `app/workers/` (gauge files are
  stored with no cache header of their own).
- Open-Meteo: this worker never calls it; it is the app's own request.
- The insides of the DFO reader (`dfo/typed.ts`, `parse.ts`, `html.ts`, `untangle.ts`), the forecast, scoring and card
  drawing code: read for failure handling only, not for slow-input or wrong-answer cases.
- The GitHub workflows the worker starts, and whether the repository is public (matters for W16's address).
- No tests were run. T595, T597, T548 and T569 were left alone as asked; T595 is not on staging yet, so staging's
  `/crashes` differs from the code read here.
