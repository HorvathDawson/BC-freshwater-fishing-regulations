# T1078 — A water closed all year by two rules names only one in its banner (Kirkup Creek)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1078 | A water closed all year by two rules names only one in its banner (Kirkup Creek) | OPEN — 0.10.0 if T1052's lane can take it in the same code (it rewrites this block), else 0.10.1 | | 0.9.1 chat 2026-11-24 |

Source: the 0.9.1 chat, 2026-11-24, PR #131 (T1052) census: Kirkup Creek (`gnis:12661`), the "Closed all year" banner cites only the Nov 1–Jun 14 closure; `banner.ts` lists only today's closer when nothing reopens.

## Work
1. When nothing reopens, the banner cites every rule that together closes the year, each with its dates and source.
2. State how many waters are closed all year by more than one rule (the census has the list).

## Acceptance
Kirkup Creek's banner names both closures; a test on a two-closer water fails without the change; the count is in the ticket.

Severity: low to medium (the water is correctly closed; one source is not shown). Release: 0.9.1 if T1052's lane can take it in the same code (it rewrites this block), else 0.9.2. Model: Opus. Size: S. Owner: no.

## 2026-11-26: the owner's pick changes item 1
The banner cites ONE closer, and that one must be the primary closure (not every rule that closes the year). The
lane checks which rule is primary on Kirkup Creek and on the other two-closer waters of the census.

## Owner

- [x] 2026-11-26 — Name every rule that closes the year, or one → "keep one just make sure it is the primary closure" (2026-11-26)
