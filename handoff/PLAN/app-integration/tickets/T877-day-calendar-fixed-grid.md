# T877 — the "Which day?" calendar: six weeks every month, every day under its weekday

Status: IN PROGRESS (lane app/t877-calendar-grid, worktree wt-t877; left (Claude): merge into app/integrate-081-web, then
the look on a phone build).

Owner 2026-11-18 ~20:46, verbatim: "The calendar shifts around". Phone shots of the Regulations page's "Which day?" sheet
on Babine Lake — Main Body, August 2026 and July 2026
(/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/069827ef-image.png,
/Users/dawson.horvath/.claude/uploads/b8ddfca9-44eb-400e-a9bf-bd89b4b0f359/277b9b7a-image.png).

Seen:
1. The grid had as many rows as the month spans (Aug 2026 six, Jul 2026 five, Feb 2026 four), so the sheet's height, its
   top and the page behind it jumped on every month step.
2. A short first or last week spread its days across the row: Aug 31 (a Monday) under Tue, Jul 1 (a Wednesday) left of
   Wed, Jul 26–31 evenly spaced. Cause: every day cell was `flex: 1` with a 44 px `minWidth`, every blank `flex: 1` with
   none; Yoga (native layout) grows each item from its min, so in a part-filled row the days took 44 + a share and the
   blanks only the share. The view model's columns were right; the drawing was not. The browser's flex algorithm
   happens to hide it, so web and the jsdom tests never showed it.

Fix:
- `core/src/dayPicker.ts` `monthVM`: always six weeks of seven (blanks before the 1st and after the last).
- `ui-native/src/DayCalendar.tsx`: every cell — day, blank and week head — is one fixed seventh of the row (`CELL`,
  width 100/7 %, the 44 px floor kept on all alike), never `flex: 1`. Month stepping, today/chosen marks, labels, keys
  unchanged.

Other month grids: none. `DayCalendar` is the app's only month/weekday grid; both entry points (the map's day strip
calendar segment and the Regulations date pill) open it through `regs/DaySheet`. Searched the weekday table users
(`WEEKDAY_NAMES`, `weekdayAbbr`, `DAY_PICKER.weekdays`), `.weeks`, "calendar" across app/, the SEO site tools and
curation-review: the year strips (`YearStrip`, `SalmonYearStrip`), `LeadUpStrip` and `WeekBox` are not month grids.

Tests (fail before, pass after): `core/src/dayPicker.test.ts` "T877 …" (six weeks every month of 2026, every day's column
its weekday, Feb/Jul/Aug drawn); `ui-native/src/DayCalendar.test.tsx` "T877 …" (Feb, Jul, Aug, Oct 2026: six rows of
seven, every cell a fixed seventh that does not grow, each day in its weekday's column; the heads the same sevenths).
