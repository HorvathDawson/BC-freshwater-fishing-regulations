# T49 — the map's day: a calendar icon instead of the stepper strip
Phase 4 · Depends on the layers-B lane (legend strip rework), T23 (DaySheet / MapDay, the one `mapDay`), G1 back · Size 0.5–1 day

## Owner (2026-10-21)
"we need to fix date picker on regs map. could just be a calendar icon."

## Today
`app/packages/ui-native/src/MapDay.tsx` is a stepper (« » a month, ‹ › a day, the label resets to today) inside the Map
tab's legend strip. The same MapDay is reused inside the Regulations page's "Which day?" sheet (`regs/DaySheet.tsx`).
The strip is cramped (it overflowed at 390 px — layers-menu inventory), and the stepper is slow for a date weeks away.

## Goal
- On the map: a CALENDAR ICON button (with the day it shows, e.g. "Today" or "Sat, Jun 15", when it isn't today) where
  the stepper was, or on the map rail if the layers-B design puts it there. One tap opens a date sheet.
- The date sheet: a month grid calendar (today marked, the chosen day selected, ‹ › months, "Today" shortcut), plus
  the quick ‹ › day steps for "tomorrow / yesterday". It sets the ONE `mapDay` the Shell owns — the same day the
  Regulations page's date pill uses (D7) — so the map and the regs page never disagree.
- The Regulations page's "Which day?" sheet uses the same calendar component (one component, two entry points).
- Back closes the sheet (useBackLayer / <Sheet>); words in the words tables; 44 px targets; each day cell has an
  accessible label in words ("Saturday, June 15"); keyboard on web; respects the season range the data covers
  (days outside the release's calendar year are disabled with a reason line, never silently wrong).

## Acceptance
- Render tests: the icon shows "Today" / the chosen day; picking a day in the grid sets `mapDay` and the map colours
  for it (status day), and the regs page's pill shows the same day; "Today" resets to null; back closes; out-of-range
  days disabled. Mutation: the map and regs reading different days fails a test.
- Web shots at 390x844 light/dark and desktop, into `T49-shots/`.
