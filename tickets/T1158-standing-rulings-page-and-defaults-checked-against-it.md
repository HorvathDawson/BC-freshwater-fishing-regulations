# T1158 — A standing-rulings page of the owner's pattern-setting answers; every default is checked against it before it is built

Status: PARTIAL — the page is written (`docs/OWNER-RULINGS.md`, 2026-11-26); owner: see ## Owner; then the AGENTS and README lines

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1158 | A standing-rulings page of the owner's pattern-setting answers; every default is checked against it before it is built | PARTIAL — the page is written; owner: see ## Owner | — | owner: read `docs/OWNER-RULINGS.md` once; then the AGENTS line and the tickets README line (item 2); S |

Source: the read-only "fewer questions" pass of 2026-11-25 night (notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `fewer-questions.md`, a
35-row table of the questions asked during 0.10.0 and a 14-row sample for 0.9.0). Its main finding: **the "Part N
of M" hold of 0.10.0 was avoidable.** The label was Claude's own recommended default and contradicted an older
answer of his (T780, 2026-11-20: part names say the place alone); the open-colour question (T1145) was a leftover
of his T1028 answer of 2026-11-23. The tickets README says "owner calls never block, build the default", and
nothing checks a default against what he has already ruled. Owner, 2026-11-25: "maybe we could have it figure out
how to make our release cycle faster?" Related: T1159, the memory notes `everything-added-is-a-proposal`,
`no-silent-fallbacks`, `record-answers-before-moving-on`.

## Work
1. One page, `docs/RULINGS.md` (150 lines at most): each ruling in one or two lines with his words, the
   date and the ticket. First entries (from the notes' section 3b and tonight): every part of a water has a real
   name or a distance, never a number ("there should be no partt x of n nameas anywhewre in the app");
   everything Claude adds for review is a proposal; no silent fallbacks; a river that is the line between two
   management units or regions is in both and takes either's closure (Campbell River), which is NOT the same as an
   area rule reaching far outside its area (T1149); 10 m imagery is "useless"; the app is free; production
   commands are only his.
2. A line in AGENTS.md and in the tickets README: before a default is offered or built, read RULINGS.md; a
   default that contradicts a ruling is not a default.
3. He confirms the page once (one sitting); after that a chat adds a ruling the same turn he gives it.

## Acceptance
The page exists and he has confirmed it; the next release's questions include none that a ruling already
answers (checked at the cut by the planning chat). Severity: process. Size: S. Owner: one read.

## 2026-11-26: the page is written
`docs/OWNER-RULINGS.md` (not `docs/RULINGS.md`: `pipeline/docs/RULINGS.md` already holds the book rulings, and a
ruling moves there when it is built). It holds the working rules of item 1 and the data rulings of the night of
2026-11-25 and of 2026-11-26 (Campbell, Rancheria, the seven T1118 bindings, audit picks P1 to P4), each with his
words and date. Left out for want of a date in the record: "10 m imagery is useless". None of its data rulings is
built yet: they ride one data build after his audit review.

## Owner

- [ ] 2026-11-25 — Read the standing-rulings page once it is drafted and correct anything that is not what you meant. Default: it stands as drafted from your recorded words.
