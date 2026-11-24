# T1052 — A water closed to all fishing says it twice (a Salmon row and a game-fish "Closed all year" block)

Status: OPEN — 0.9.1; owner: OWNER-TODO "T1052 fully closed water: one statement" (which words), then an app build.

Source: in-app report `19a865256d6dd70d` (staging feeds worker, the owner, 2026-11-24 18:47 UTC), reason "other", on
Nelson Creek `gnis:20408`, part 0, "Today, Nov 24", Regulations view, web, app 0.9.0 (build 43, 127bf43), release
r-0aec88ed3edb. No screenshot attached. The words, verbatim:

> the closed tab herew with seperate salmon and game fish when fully closed seems weird should just say all closed including salmon and give explaination rathe rthen multiple headers maybe etc etc just has a lot of words the 2 schedule calendars are okay though

Triage: lane L19, 2026-11-24. Picture (staging, 390 px): `handoff/PLAN/app-integration/T1052-shots/report-now.png`.
Eagle Creek (`gnis:9405`, p.23) draws the same page.

## What the screen shows

Two status strips ("SALMON · B.C. SYNOPSIS ✕ Closed", "GAME FISH · B.C. ✕ Closed": the owner keeps these), then
"Species and limits" with one row, "Salmon closed" + "Closed to all fishing by the B.C. Synopsis (p.25): No Fishing (all
year)" + "B.C. Synopsis p.25 · Nelson Creek", then a second block "Closed all year" + "No fishing for any game fish. This
water" + its source line. One closure (`r2:nelson_creek@2-8::nelson_creek.r1`, book p.25: "NELSON CREEK ✱ No
Fishing") is said twice, in two vocabularies, under two headings.

## Cause

Two composers each say their half: the salmon row's words come from the salmon facts (`app/packages/core/src/salmon.ts`
`bookToday`, ~line 1476, T796: "Closed to all fishing by the rule book (p.N): …", from `pipeline/deliver/salmon.py`
`BOOK_WORDS`), and the closed day's banner from `app/packages/core/src/regs/banner.ts` (`WORDS.CLOSED_ALL_YEAR`,
`NO_FISHING_RUN`, `words.ts:75-77`). Nothing joins them when every fish, salmon included, is closed by the same rule.
Rule 23: the closed statement must come from ONE function in core.

## What is right

The book closes everything here ("No Fishing", ✱ tributaries; p.4 key: the ✱ means the regulation applies to tributary
streams too); the page should say so once, salmon included, with the reason and the page. The words are the owner's.

## Options

- **A (recommended):** keep the two strips; in place of the Salmon row and the "Closed all year" block, one block:
  heading "Closed to all fishing all year, salmon included"; line "The B.C. Synopsis row for Nelson Creek (and its
  tributaries) reads “No Fishing”."; then the source line "B.C. Synopsis p.25 ↗ · Who says so ›". On a walked tributary
  the line names the row it inherits ("Nelson Creek's row covers its tributaries: “No Fishing”.").
- **B:** smallest change: drop the Salmon row's sentence and make the banner "Closed all year" / "No fishing for any
  fish, salmon included. This water".
- **C:** A plus the two strips merged into one "All fish" strip (the owner said the two calendars are fine: not recommended).

Only when the same closure(s) close salmon and every game fish on the day shown; a page closed for one group keeps today's look.

## Work (after the pick)

1. One core function composes the all-closed statement (rule 23, sentences in core, rule 33 whole-string tests); the
   salmon row and the banner ask it first. Check whether core can tell "the same closure" from the frames it has; if not,
   the display frame carries a flag (then a data release).
2. Tests (Nelson Creek, Eagle Creek, a tributary of Nelson, a page with only salmon closed) as whole strings, mutation-pinned;
   before/after shots light + dark.

Size: M. Needs: owner pick; app build (a data release only if a frame flag is needed). Safe for 0.9.1.
Depends: none. Owner: yes (the words). Default: A, built only once picked.
