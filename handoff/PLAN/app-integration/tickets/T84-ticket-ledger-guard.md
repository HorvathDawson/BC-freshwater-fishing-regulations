# T84 — Ticket-ledger guard (audit N3)

Status: DONE — 0e53c60e, caf5ef52. `app/tools/check-tickets.mjs` (`pnpm tickets`, first step of `pnpm check`) + `app/tools/check-tickets.test.ts` (13 tests: green on the real ledger, mutation-fails on a removed row, a removed/pushed-down Status line, a removed file, an unticketed commit T-number, DONE without a hash, a duplicated row/unknown status, two files with one id). Commit subjects are scanned only from the commit that began the tickets folder (older "T3a" is another plan). Deliberately NOT checked: ledger status == file status (a lane closes its file before the merger updates the row). Review caf5ef52: row/file mutation tests ran against live git subjects (failed once T84's own commit existed) and the duplicate-row test keyed on the row reading OPEN — both now history- and status-independent; added the duplicate-file-id rule. Merger: set the T84 ledger row to DONE / 0e53c60e, caf5ef52.

Source: owner 2026-10-22: tickets are the sole source of future work. Nothing checks that the ledger stays whole.

## Work
A script in `pnpm check` (or pipeline CI) that fails when: a `tickets/T*.md` file has no `Status:` line in its first
lines; a ticket file is missing from the README ledger table or vice versa; a T-number in commit subjects is missing
from the table; a ticket is DONE with no commit hash.

## Acceptance
Green on the current ledger; fails on a removed row and on a removed Status line (mutation).

Depends: the README ledger. Size: S. Owner: no.
