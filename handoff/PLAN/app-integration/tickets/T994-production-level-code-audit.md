# T994 — Production-level audit: non-standard practices and data-flow issues across the code

Status: IN PROGRESS (main chat 2026-11-21: read-only audit agent first; findings become numbered lines here or new tickets).

Source: owner 2026-11-21, verbatim: "You could also get an agent to do a basic audit for non standard practices or
issues with the flow of data etc. we want to make our code production level. Also clean up gha workflows and make sure
we just waste less time with bad ci/cd of failed runs." (The GitHub Actions half is in T993's scope.)

## Work
1. Read-only audit of `pipeline/`, `app/`, `curation-review/`, `deploy/`, `scripts/`: non-standard practices
   (error handling, config, typing, logging, dependency use, dead code, copy-pasted logic), and data-flow issues
   (a fact computed twice, a reader of curated files at run time, a hidden fallback, an untyped boundary).
   Start from the 2026-11-16 audit (`handoff/PLAN/audit-2026-11-16/`) and report only what it and the open tickets
   do not already hold.
2. Each finding: file:line, what is wrong, why it matters in production, the fix, its size. Ranked.
3. The owner sees the list; fixes go into this chat's lanes or new tickets.

## Acceptance
A ranked findings list with no item already in an open ticket; each kept finding fixed or ticketed.

Depends: —. Size: M. Owner: reads the ranked list.

## Audit done 2026-11-21 (read-only agent): 30 findings, none already ticketed
Report: `handoff/PLAN/audit-2026-11-21/t994-audit.md` (ranked, file:line, fix, size; "(sub)" = sub-auditor reading not
re-opened by the lead). Top 5: `update-hydat.sh --upload` publishes ungated to any bucket; review backend CSRF on
`POST /api/inseason/resolve`; worker backups keep report text 400 days (privacy promise is 180); a failed in-season feed
read resets history as a first run; one failed read wipes the water-temperature history (worker + Python twin).

Fix lanes (disjoint files):
- **F1 feeds worker + its Python twins:** findings 3, 4, 5, 23, 24, 25, 26, 30.
- **F2 pipeline + review backend:** 1, 2, 14, 15, 16, 17, 21, 27.
- **F3 app data seam:** 8, 11, 12, 13, 22.
- **With T993 (workflows, deploy scripts):** 6, 7, 9, 10, 18, 19, 20, 28, 29.

## Lane F1 (feeds worker + Python twins), branch `lane/t994-f1-worker`, 2026-11-21
Every finding was read against the code first; all eight held. Each fix has a test that fails without it.
- **3** (report words in 400-day backups) → `0a610610c`. Every outbox copy (daily and monthly) now leaves out the queued
  `report` / `crash` events (`backup.ts scrubOutbox`, `events.ts PERSONAL_EVENT_KINDS`). A restored outbox raises them
  again from the stored records. The prune scrubs copies made before the rule. An outbox that does not parse is not
  copied and the run fails. `forget-report` also takes the report's queued event out of the live outbox
  (`dropPending`). The privacy page is unchanged: the fix keeps its 180 / 90 / 30-day words true.
- **4** (in-season feed read) → `67e723567`. Read through `readStored`. Only an absent file or another format counts
  as a first feed.
- **5** (temperature history wipe, worker + Python) → `5b6dd066b`. The temperature history, a station's
  year-to-date file and (Python) the previous index: an absent file is a first run, and any other file that does not
  read fails the run, naming the file. One shared fixture: `app/workers/feeds/test/fixtures/state-reads.json`, read by
  `state-reads.test.ts` and `pipeline/tests/test_feed_state_reads.py`. Two Python tests that pinned the old reset were
  rewritten.
- **26** (reminder dedupe resends) → `5b6dd066b`, the same fixture. A sent-keys file that does not read sends nothing
  that run: every due key is `failed` and `error` says why. Both twins changed. The Python writes the file with
  `atomic_write`. The worker's copy never runs in production (`notifyUrl: null`; the outbox tells the owner).
- **23** (APNs token per wake) → `bd4400513`. The provider token is kept at `private/push/apns-jwt.json` (never backed
  up) and reused for 50 minutes with the same key id and team. It is dropped when APNs answers 403
  Expired/InvalidProviderToken. Apple's 20-minute rule was not checked against the live service.
- **25** (push register total) → `cefdb3fc5`. A NEW token counts against `PUSH_NEW_TOKENS_PER_DAY_TOTAL` (1,000 a day);
  a known token re-registering does not. Past the total the route answers 429, and `capEvents` tells the owner once.
- **24** (backup subrequests) → `a6d97778a`. Every bucket call is counted against `BACKUP_SUBREQUESTS` (6,000; the
  purge shares the invocation). The state is copied first and the personal records last. Copying stops `PRUNE_RESERVE`
  short of the budget, the manifest names what was left (`partial`), and a prune that would pass the budget fails the
  run. An object over the size cap or a partial snapshot fails the run; an object gone between the list and the get
  does not (the purge runs beside the backup). NOT done: incremental report copies. A daily snapshot stays whole for
  `restore-test.sh`, so a store past about 4,500 reports now fails the backup every day, which is told, not silent.
  The purge's own per-crash-kind reads are not budgeted.
- **30** (copy-paste in public routes) → `21c1a6ed5`. `jsonResponse`, `readCapped` and `readJsonBody` live in `io.ts`
  (push gains the Content-Length check). `checkToken` answers `{ok: false}` on a network error.
