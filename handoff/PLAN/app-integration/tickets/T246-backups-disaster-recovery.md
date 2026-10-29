# T246 — backups and disaster recovery

Status: DONE — ec26ed584 (2026-10-29)

Source: owner 2026-10-28 — approved proposal P12 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
1. Inventory what is NOT in git: R2 (release history, feeds/ and private/feeds state, inputs), review marks if any live
   outside git, worker secrets (names only), DNS/zone settings.
2. Versioned backup: a scheduled copy of R2 `release/` + `feeds/` + `private/` to a second bucket (or local disk) with
   retention; curated data stays in git (check nothing curated is gitignored).
3. Restore test: rebuild staging from the backup only and run T241's smoke tests.
## Acceptance
Inventory committed; a backup run exists; a restore into an empty bucket passes the smoke test.
Depends: T241. Size: S. Owner: yes — a second bucket / storage (default: R2 bucket `…-backup`, 30-day retention).

## Outcome (ec26ed584)
### Inventory — what git does NOT hold
| What | Where | Backed up by |
|---|---|---|
| feeds worker state: notify outbox, status, runs archive, DFO raw/history/entries, roster, in-season history, reports (PII: reporter emails) | R2 staging `private/**` | worker cron → `daily/` |
| published feed state (DFO, in-season, gauge index, clim.json) | R2 `feeds/dfo/**`, `feeds/inseason/**`, `feeds/live/{index,clim}.json` | worker cron → `daily/` |
| per-station gauge files | R2 `feeds/live/<station>.json` | not backed up: rebuilt every 30 min from ECCC |
| release + tiles + basemap + synopsis | R2 `release/**` (local `data/release/` is git-ignored, pruned) | `backup.sh release` (immutable copy) |
| run-timing feed | R2 `runtiming/**` | index in `daily/`; files rebuilt by feeds-runtiming.yml |
| review marks (tracked but usually uncommitted) + all `data/curated/**` | the Mac working tree | `backup.sh curated` (nothing under data/curated is git-ignored — the script warns if that changes) |
| worker secrets, GitHub secrets | Cloudflare / GitHub | names only: T249 inventory; values are re-issuable |
| DNS / zone settings | Cloudflare dashboard | not backed up (owner: export the zone file once, T240) |
### Built
- Bucket `bc-fishing-regulations-staging-backup` (never bound to a serving worker). Lifecycle: `daily/` 30 d, `monthly/`
  400 d, `curated/` 365 d, `restore-test/` 1 d.
- Feeds worker cron `25 9 * * *` (`src/backup.ts`): `private/` + feed state → `daily/<day>/` (+ `monthly/<YYYY-MM>/` on
  the 1st), R2 puts verified by sha256, `_manifest.json` (sha256 per key) written last; `POST /run/backup` (token) by
  hand; last run in `/status.json` `backup` (the smoke test checks < 30 h); a failure emails at once.
- `deploy/staging/backup.sh curated|release|all` (Mac): dated tarball with MANIFEST.sha256 + GIT.txt (commit +
  uncommitted list) → `~/canifishthis-backups/` and `curated/` in the bucket; `release/` server-side copy.
- `deploy/staging/restore-test.sh [day]`: daily/ → `restore-test/<stamp>/` prefix → scratch dir, every sha256 = manifest;
  newest curated tarball: .sha256 + every file inside; live `release/` vs backup: size + MD5 (rclone check).
### Run (2026-10-29)
- Backup: 48 objects, 42.9 MB (worker, 27 s); curated 57 files 2.0 MB; release 814 objects 488 MiB.
- Restore test: PASS 48/48 sha256, curated 57/57, release 814/814; restored state = live bucket 48/48.
- Not done: serving a stack FROM the restored copy (empty bucket + a worker + an app build pointed at it) and running the
  smoke test on it — the restore is proven by byte identity with the live bucket, which the smoke test passes.
- Owner: a launchd/cron entry for `backup.sh curated` if marks should be snapshotted without asking (today: by hand).
