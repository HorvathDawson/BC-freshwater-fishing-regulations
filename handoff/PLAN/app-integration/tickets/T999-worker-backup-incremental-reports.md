# T999 — Feeds worker backup: copy reports incrementally so the daily backup holds past ~4,500 reports

Status: DONE (3377f8a8b, lane L5 2026-11-22; staging worker deployed with it; acceptance 3 reworded, below).

Source: T994 finding 24 (`handoff/PLAN/audit-2026-11-21/t994-audit.md` §24), fixed in part by `a6d97778a` (T994 lane
F1, wave 1, 2026-11-21). Its note: "NOT done: incremental report copies. A daily snapshot stays whole for
`restore-test.sh`, so a store past about 4,500 reports now fails the backup every day, which is told, not silent."

## Work
1. `app/workers/feeds/src/backup.ts` `runBackup`: copy only the personal records (reports, crashes) that are new or
   changed since the last complete snapshot. The new snapshot's manifest names the earlier snapshot each unchanged
   record lives in (content hash or etag), so one manifest still describes a whole store.
2. `pruneBackups`: never delete a snapshot that a kept manifest still points into. Keep the 180-day privacy promise:
   a record past its retention leaves every snapshot, however it got there.
3. `restore-test.sh` (and the restore path) read through the manifest chain. A broken link fails the restore, naming
   it.
4. Budget the purge's per-crash-kind reads too (the other half of the #24 note).

## Acceptance
- A fixture store of 10,000 reports backs up on day 1 in several runs (or within budget, said), and day 2 with 10
  new reports uses far fewer calls (counted against `BACKUP_SUBREQUESTS`).
- A restore from day 2's manifest equals the store.
- A pruned base snapshot that is still referenced is refused (mutation-pinned).
- No report text outlives 180 days in any snapshot.

Depends: none. Size: M. Owner: no. Default: as above.

## Built (2026-11-22, lane L5, branch `fix/T996-apns-check`, 3377f8a8b)
- **`runBackup`** (`app/workers/feeds/src/backup.ts`): the state is copied whole as before; a report or error kind whose
  R2 etag equals the last manifest's is NOT copied again: today's manifest (now `backup/2`) names it with `in` = the
  snapshot that holds its copy (one hop, never a chain), plus its `etag` and `gone` (the privacy date). The snapshot built
  on is the last run's (`private/feeds/backup.json` `day`, 2 reads), said as `base` in the result and on /status.
  A second run the same day goes on from the first (day 1 of a big store = several `POST /run/backup`, each PARTIAL and
  told until whole).
- **Why a copy is still refreshed:** the backup bucket's lifecycle deletes a `daily/` object 30 days after it is
  WRITTEN, whoever names it. So each record is copied afresh at most every `REFRESH_DAYS` (7) days, spread by its key
  (about 1/7 of the store a day); a refresh the budget cannot fit keeps its earlier copy (`deferred`) while that is under
  28 days old. Every snapshot is therefore whole for at least 23 days of its 30.
- **The prune** dates an error kind from its manifest's `gone` (one read a snapshot, not one a copy: item 4), and a copy
  it deletes (privacy date, deleted on request) is taken out of EVERY manifest that names it, the `in` ones too
  (`pruned` says where it pointed). A manifest that does not parse fails the run, named, after the deletes.
- **`deploy/staging/restore-test.sh`** fetches the records named from earlier snapshots (server-side, one
  `--files-from-raw` list per snapshot) and fails on a gone link, naming the record and the snapshot it was in
  (checked against a local fixture with one link removed: `FAIL … BROKEN LINK / MISSING ['…/b.json (in daily/2026-11-21/)']`).

## Acceptance, as proved (`test/backup.incremental.test.ts`, 6 tests; mutation-checked: refs not dropped, etag ignored,
## no refresh, never reuse, prune reading copies — each fails a test)
- 10,000 reports: day 1 whole after 2-6 runs (asserted) under `BACKUP_SUBREQUESTS` (each PARTIAL and told); day 2 with 10 new reports:
  4 state + 10 new + ~1/7 refreshed copied, the rest named, under a fifth of day 1's calls.
- A restore from day 2's manifest equals the store.
- **Reworded:** "a pruned base snapshot that is still referenced is refused" cannot hold: nothing of ours deletes a
  snapshot (the lifecycle does, and cannot be refused). What holds instead, pinned: a copy the prune deletes leaves every
  manifest that names it, and over 200 simulated days (30-day lifecycle, 180-day live purge) every snapshot under 23 days
  old restores whole.
- No report text outlives 180 days in any snapshot (the same 200-day run, checked every day).

## Staging (2026-11-23 01:40Z and 01:45Z UTC, `POST /run/backup`, worker e11978f9e)
Run 1: `base daily/2026-11-22/` (a `backup/1` manifest, no etags) → 185 copied, 0 reused, 1,350 subrequests, ok. Run 2 the
same day: `base daily/2026-11-23/` → 99 copied (the state), **86 reused** (every report and error kind), 1,178
subrequests, ok. The `daily/2026-11-23/_manifest.json` is `backup/2`, 86 personal entries each with etag + gone. The first
cross-day `in` links appear at the 2026-11-24 09:25 cron (the 11-23 one builds on today's own manifest); the restore test
over those links is not yet run against real R2 (see the report).
