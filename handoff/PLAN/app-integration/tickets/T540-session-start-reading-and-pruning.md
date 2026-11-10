# T540 — Cut what a new chat reads: closed ledger rows, old STATUS entries, the dead order list

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the files). Rank 8.

## Today
- Start-of-chat reading as instructed is about 245 KB (~60k tokens): `tickets/README.md` 132 KB, `AGENTS.md` 73 KB,
  `OWNER-TODO.md` 33 KB, plus the restart page. STATUS.md is another 121 KB (57 dated entries) when read whole. None has a
  pruning rule.
- In the ledger, 461 of 534 rows are DONE/DROPPED: ~88 KB against ~18 KB for everything still open. Single rows run to
  900 characters, repeating the ticket file.
- README `:581-601` "Order for open tickets" dates from 10-22: 73 of the 80 ids it names are closed and 31 open tickets
  are not in it. The real order is on the restart page.
- Owner questions (`:603-648`): 30 of 38 open rows sit on DONE/DROPPED tickets. Built-on-default is by design (owner call
  11); the rows on DROPPED tickets (Q27, Q42, Q161) are dead.
- AGENTS.md `:374-772` "Reading the regulations" is 53 % of the file.

## Work
1. Closed rows move to `tickets/DONE.md`; `check-tickets.mjs` reads both. Cap the ledger's last cell (detail stays in the
   ticket file).
2. STATUS.md keeps the last 7 days; older entries move to `STATUS-archive.md` when a restart page is written. Size guard
   in fast-fail.
3. Delete the order section; one line points at the restart page.
4. Closing a ticket moves its unanswered questions to "Shipped on default (reverse any)", one line each; retire the three
   dead ones.
5. Owner call (default: leave as is): move "Reading the regulations" out of AGENTS.md into a file read only for
   regulation work.

## Acceptance
- Instructed start reading ≤ 120 KB, measured and written here. `pnpm run tickets` passes on the split ledger.

Depends: T539 (same guard file). Size: M. Owner: yes (item 5 only). Default: AGENTS.md unchanged.
