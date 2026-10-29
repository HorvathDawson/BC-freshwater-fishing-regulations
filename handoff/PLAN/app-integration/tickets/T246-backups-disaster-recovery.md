# T246 — backups and disaster recovery

Status: OPEN — not started.

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
