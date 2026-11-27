# T1078 — A water closed all year by two rules names only one in its banner (Kirkup Creek)

Status: IN PROGRESS — owner approved A (as built) 2026-11-26; what's left: the PR merge. Code f0e5f29ed (hardening lane): the all-year closer is cited alone; the "together" case is built (0 waters today).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1078 | A water closed all year by two rules names only one in its banner (Kirkup Creek) | IN PROGRESS | f0e5f29ed | the PR merge (owner approved the one primary closure, 2026-11-26) |

Source: the 0.9.1 chat, 2026-11-24, PR #131 (T1052) census: Kirkup Creek (`gnis:12661`), the "Closed all year" banner cites only the Nov 1–Jun 14 closure; `banner.ts` lists only today's closer when nothing reopens.

## Work
1. When nothing reopens, the banner cites every rule that together closes the year, each with its dates and source.
2. State how many waters are closed all year by more than one rule (the census has the list).

## Acceptance
Kirkup Creek's banner names both closures; a test on a two-closer water fails without the change; the count is in the ticket.

Severity: low to medium (the water is correctly closed; one source is not shown). Release: 0.9.1 if T1052's lane can take it in the same code (it rewrites this block), else 0.9.2. Model: Opus. Size: S. Owner: no.

## Done (hardening lane, 2026-11-26, f0e5f29ed)
- `core/src/regs/banner.ts` `yearChain`: when nothing reopens, the banner reads the year's stored closers (the frames'
  decided `closers`, display 18; nothing is chosen in the app). A closure that is a closer on EVERY day closes the year
  alone and is cited alone (the day's first when it is one: the banner as it was). When none does, every closer of the
  year is cited, from the selected day round the year, each with its dates, level and page, under the proposal
  "No fishing for any game fish all year. Together these closures cover it:" (`WORDS.ALL_YEAR_SPAN` + `TOGETHER`).
- Tests (`banner.test.ts`, own hand-built frames, no live row): two closures together (Nov 1–Jun 14 + Jun 15–Oct 31),
  three together, one closure all year beside a dated one (cited alone), the one-rule case unchanged. The four new
  tests fail with the old chain; the "alone" test fails with the alone branch removed.

## Found: Kirkup Creek is not the "together" case (census, candidate r-71d09ad82e81, Nov 26, 20,956 parts)
- Kirkup Creek (`gnis:12661`) is closed every day by `jordan_river.r4` ("No Fishing from Kirkup Creek downstream,
  including Kirkup Creek"), which the banner cites; `jordan_river.r1` (Nov 1–Jun 14) and Region 4's spring stream
  closure (Apr 1–Jun 14) close it on their days too. On r-0aec88ed3edb the banner cited r1 instead.
- Closed all year: 1,148 parts. Closed by closures only TOGETHER (none alone): 0. So f0e5f29ed changes no banner on
  this release; it guards the case.
- Cited every closer of the year (the ticket's literal "names both"): 499 parts on 499 waters get 2–4 lines (437 two,
  57 three, 5 four; Kirkup three), and T1052's one block ("Closed to all fishing all year, salmon included") drops from
  958 parts to 588: a dated closure beside the all-year one is no salmon closure, so `closedWithSalmon` keeps the two
  statements. Not built: it reverses an owner pick for 370 waters.

## 2026-11-26: the owner's pick changes item 1
The banner cites ONE closer, and that one must be the primary closure (not every rule that closes the year). The
lane checks which rule is primary on Kirkup Creek and on the other two-closer waters of the census.

- Primary closure checked (2026-11-26): `yearChain` cites the closure that is a closer on every day of the year (Kirkup
  Creek: `jordan_river.r4`), not the dated one that happened to be the day's first closer; a test pins it with the dated
  rule listed first (`banner.test.ts`, fails if the alone branch is removed).

## Owner

- [x] 2026-11-26 — Name every rule that closes the year, or one → "keep one just make sure it is the primary closure" (2026-11-26)
- [x] 2026-11-26 — Kirkup Creek: A (as built, one closer) vs B (every closure) → "yes do primary closure. the as built is better then mockup" (2026-11-26)
