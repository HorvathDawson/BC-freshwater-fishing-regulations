# T540 — Cut what a new chat reads: closed ledger rows, old STATUS entries, the dead order list

Status: PARTIAL — a7df2f98b, 666683208, ea0d3d488, 116872b43 (items 1-4 done; item 2's rule + guard 2026-11-12); left (Claude): cap the ledger's last cell (~200 characters, 27 rows over it); item 5 is the owner's, default unchanged (Q177)

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

## Done 2026-11-11
- Item 3: the order section is a two-line pointer to the restart page (a7df2f98b).
- Item 4: 24 questions on closed tickets moved to "Shipped on default (reverse any)", one line each; Q27, Q42, Q161
  retired with their DROPPED tickets (a7df2f98b). Q31 (answered) stays in its row.
- Item 2: `STATUS.md` keeps 2026-11-04 onward; 19 entries (2026-10-30 to 11-03) moved unchanged to `STATUS-archive.md`
  (666683208). 155 KB to 127 KB. Left: the rule "move older entries when a restart page is written" and the size guard in fast-fail.

## Item 1 as it stood before ea0d3d488 (kept for the record; built as written below)
`app/tools/check-tickets.mjs` (T84) reads only `README.md` under `## Ledger` (`ledgerRows`), and fails with "ticket file has no
ledger row in README.md" for every `T*.md` whose row is elsewhere. Moving 400+ closed rows to `tickets/DONE.md` therefore
fails `pnpm check` until the guard changes (a code change, not made here). What it needs:
1. `readRepo` also reads `tickets/DONE.md` and passes it as `doneLedger`; `checkLedger` runs `ledgerRows` over README and
   DONE.md and unions the rows (a row in both is the existing "appears more than once" problem).
2. A rule that DONE.md holds only DONE/DROPPED rows (a row whose status is anything else in DONE.md fails, so an open
   ticket cannot hide there), and the file-vs-row status match (rule 5) runs on both.
3. `ledgerRows` takes the heading as a parameter, or DONE.md uses the same `## Ledger` heading.
4. `app/tools/check-tickets.test.ts`: a case for a ticket whose row is only in DONE.md (passes), an OPEN ticket in DONE.md
   (fails), a row in both (fails).
Then move rows that were DONE/DROPPED before 2026-11-11, never the rows of tickets other lanes are editing today (T462, T549,
T554, T555, T585, T597, T603, T606, T628, T631, T633, T661, T664-T672), and cap the last cell at about 200 characters.
Acceptance measure (not taken yet): start-of-chat reading is README 132 KB + AGENTS 73 KB + OWNER-TODO + restart page; the
split is what brings it under 120 KB.

## Done 2026-11-11, item 1 (ea0d3d488)
- `app/tools/check-tickets.mjs` reads `tickets/DONE.md` beside `README.md` (`readRepo` → `doneLedger`; the same `## Ledger`
  table): rows unioned, a row in both fails, DONE.md may hold only DONE / DROPPED rows, the file-vs-row status match and
  the DONE hash rule run on both, a missing DONE.md is an error. `check-tickets.test.ts` "T540 closed rows live in
  DONE.md": only in DONE.md passes; OPEN / IN PROGRESS / POSTPONED there fails; in both fails; in neither fails.
- Moved 502 rows: DONE or DROPPED, the row AND the ticket file unchanged since the last commit of 2026-11-10 (5ba659394).
  Stayed in README: 141 rows (every open one, and the closed rows touched 2026-11-11: T462, T507, T549, T554, T557, T628,
  T662, T663, T675). They move once their lanes are done with them.
- Measured: README 167.7 KB → 60.6 KB. Instructed start reading now: README 60.6 + AGENTS 79.1 + OWNER-TODO 47.9 +
  restart page 4.8 + START-HERE 0.9 = 193 KB. NOT under the 120 KB acceptance: AGENTS (item 5, owner) and OWNER-TODO
  (grew from 33 KB) are what is left; the ledger alone cannot bring it there.
- Not done: the last-cell cap (about 200 characters); the STATUS size guard and its move rule (item 2's leftovers).

## Done 2026-11-12 (116872b43)
- Item 2's leftovers: `scripts/status_archive.py --move` moves STATUS entries dated more than 7 days back (B.C. date) to
  the top of `STATUS-archive.md`, unchanged (it refuses to write unless both files hold the same entries after);
  `--check` is the fast-fail step "STATUS.md under its size limit" (128,000 B). Ran once: 5 entries of 2026-11-04 moved,
  STATUS.md 125 KB -> 119 KB.
- Left: the ledger's last-cell cap (27 rows over 200 characters today; the detail must be in each ticket file first).
