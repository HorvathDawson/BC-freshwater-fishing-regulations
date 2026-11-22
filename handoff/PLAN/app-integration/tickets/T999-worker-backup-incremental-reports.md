# T999 — Feeds worker backup: copy reports incrementally so the daily backup holds past ~4,500 reports

Status: OPEN (not urgent: today's store is far below the limit; the failure is told, not silent).

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
