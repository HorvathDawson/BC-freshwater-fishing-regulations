# T762 — The trout "Through the year" line stops at the next closure and never says when keeping returns ("…Then closed from Oct 1.")

Status: OPEN (app chat; core composition. The new wording is the owner's to confirm, and the trout display redesign under review may replace this line: build with or after his pick).

Source: trout display review 2026-11-15, data note 2 (`handoff/PLAN/design/trout-display-review-2026-11-15.md`); traced
read-only by the app chat on release r-f749475d0962, 2026-11-15 22:45.

## What happens
- Fraser River, Region 5 boundary to Region 3 boundary (`gnis:39325`, part 14) is closed for trout Oct 1 to Oct 31
  (`r3:fraser_river@3-14` rule `fraser_river.r4`, printed p.30; Region 5 prints the same window, `fraser_river.r1`).
- The shipped answer is right: the trout row's year is keep 273 days, closed 31, keep 61. On Oct 15 the page correctly
  says "Don't fish for them until Oct 31. If one bites, carefully release it. You can keep them from Nov 1."
- On a KEEP day (Sep 15, Nov 1, Nov 15: all the same) the group's year line reads "You can keep them until Sep 30.
  Then closed from Oct 1." No end of the closure, no reopening, and after Oct 31 no "next year" on "until Sep 30".
- Cause: `app/packages/core/src/regs/plainrow.ts:94` (`YearWords.runTxt`). When today's run is "keep" it returns
  `WORDS.KEEP_UNTIL_THEN` (`words.ts:149`) after one step and never runs the loop at lines 97 to 105 that names the
  next run's end and "You can keep them from …". A straight port of the prototype (`app/fixtures/reference/page_v36.html:1492`).
- How wide: 94 answer keys (about 1,795 parts) show a year line in this form, counted on each key's first keep day:
  79 "…Then release only from …", 20 "…Then closed from …". Every one drops at least the end of the next run and when
  keeping returns. Examples: Fraser parts 9, 14, 15 (keep resumes Nov 1, unsaid); Fraser part 12 ("until Mar 30. Then
  closed from Mar 31."; stored: keep again Jun 21 to Sep 30, closed October, keep Nov and Dec); Boardman Creek
  `blk:356559786` ("until Aug 31. Then closed from Sep 1."; stored: closed to Oct 31, release Nov to Mar, closed Apr 1
  to Jun 14, keep from Jun 15); Meadow Creek Diversion `blk:356368998`, 428 parts (the spring closure and the Jun 15
  return unsaid).
- Not checked: counts are one date per answer key (a key whose first keep day shows no line could be missed); how many
  of the 94 the owner reads as misleading rather than only incomplete.

## Work
1. `runTxt`'s keep branch goes on the way the other branch does: the next run with its range, the runs after it, then
   "You can keep them from X", with " next year" where the date is next year's (also on "until").
2. New words in `words.ts`; the sentences composed in core and pinned whole (AGENTS 33).
3. Update the pinned tests and fixtures as whole strings: `plainrow.test.ts:44,59`, `ui-native/src/regs/KeepTab.test.tsx:71-72`,
   the `glenn_501` / `glenn_615` regs fixtures (their cases also omit the spring closure and the Jun 15 return).
4. The wording goes to the owner as a proposal before it ships (To-Do row), with three real waters.

## Acceptance
- Fraser part 14 on Nov 15 reads, for example, "You can keep them until Sep 30 next year. Then closed Oct 1 to Oct 31.
  You can keep them from Nov 1."
- Boardman Creek on Jul 15 names Sep 1 to Oct 31 closed, Nov 1 to Mar 31 release only, Apr 1 to Jun 14 closed, and
  keeping from Jun 15.
- Whole-sentence tests: keep, closed, keep; keep, release, closed, keep; a run that wraps the New Year.

Size: S to M. Area: app core (the app chat). No pipeline change.
