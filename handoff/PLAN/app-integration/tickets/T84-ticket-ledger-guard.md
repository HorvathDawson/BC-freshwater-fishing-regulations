# T84 — Ticket-ledger guard (audit N3)

Status: OPEN.

Source: owner 2026-10-22: tickets are the sole source of future work. Nothing checks that the ledger stays whole.

## Work
A script in `pnpm check` (or pipeline CI) that fails when: a `tickets/T*.md` file has no `Status:` line in its first
lines; a ticket file is missing from the README ledger table or vice versa; a T-number in commit subjects is missing
from the table; a ticket is DONE with no commit hash.

## Acceptance
Green on the current ledger; fails on a removed row and on a removed Status line (mutation).

Depends: the README ledger. Size: S. Owner: no.
