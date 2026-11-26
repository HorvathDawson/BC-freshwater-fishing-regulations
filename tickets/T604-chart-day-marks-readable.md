# T604 — The chart's day marks are readable: no overlapping labels, today is obvious

Status: IN PROGRESS — owner's look at the web shots taken as done on his word 2026-11-20; left (Claude): emulator shots | 90e41efc6

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T604 | The chart's day marks are readable: no overlapping labels, today is obvious (two-days chart) | IN PROGRESS — owner's look taken as done (2026-11-20); left (Claude): emulator shots | 90e41efc6 | owner 2026-11-10 on build 25, with a shot |

Source: owner 2026-11-10 on build 25 (Fraser, CPR Bridge at Mission → Landstrom Bar, regulations day Wed 11 Nov):
"The only issue is the chart for 2 days. It has overlapping text and not obvious what current day is." Shot:
`docs/app-integration/T604-shots/owner-phone-build25-before.png`.

## Done (90e41efc6)
- ONE layout, `core/src/chartMarks.ts` `layoutDayMarks`, called from `buildHydrograph` so the shape carries the placed
  labels; the web and phone charts are the one `Hydrograph` component. Measured widths; first place that is inside the
  plot, meets no placed label and covers no rule: right of its rule, left, then row 1, row 2. Today is placed first. Test
  `chartMarks.test.ts` (fuzz over every pair of rule positions; fails when two boxes intersect).
- Today: solid ink rule, bold "Today". Conditions day: line colour, dashed. Regulations day: quiet grey, dashed, "Wed 11 Nov"
  (title case, as the text line above). Same day twice: one mark (Conditions'). Item 3: all three are drawn; with three
  rules within ~25 px a label may cross a neighbouring rule, never another label.
- Item 4: the scroller starts below the pinned band (measured: scroll top 214 px, band bottom 214 px), so nothing sits
  "under" it; the owner's shot is a scroll position. What was wrong is the hint: on a phone it ran to three lines beside
  the buttons and was cut. It is now said whole on its own line under the zoom row (the read-out keeps the row).
- Shots (web, 412 px wide, dark): one day apart, same day, a week apart, past the window, a Conditions day + regs day,
  adjacent days. Emulator not run. Left: a phone look.

## Owner answer 2026-11-20

Owner 2026-11-20, "Couldn't we just assume most of these are done already?": the owner's look at the T604 web shots and the phone is taken as done. Not closed: the emulator shots are still Claude's step.

## Emulator shots 2026-11-20 (L4, Android emulator, build of 124d79efa, light)
Coquitlam River -> Conditions, the chart at 1080x2400, shots in `docs/app-integration/L4-android-shots-2026-11-20/T604/`:
`two-days-regs-day-is-today.png` (one mark: bold "Today", solid rule), `two-days-regs-day-tomorrow.png` ("Today" at the end of the
solid reading line, "Sat 21 Nov" quiet, dashed, to its right; no overlap; the hint "Pinch to zoom, tap to read a value" whole
on the line under the controls), `two-days-regs-day-past-window.png` (11 days on: Today only, no regs mark). Not shot: a week
apart, adjacent days, a Conditions day plus a regs day (the unit test covers them; the web shots of 11-20 show them). Dark not shot.

Left (Claude): the emulator shots not yet taken (a week apart, adjacent days, a Conditions day plus a regs day; dark).

## Acceptance
- The owner's case (today + the next day) reads cleanly on the phone; a test fails if two mark labels' boxes intersect.

Depends: T582. Size: S. Owner: looks at the shots.

## Owner

- [ ] 2026-11-20 — Look at the two-days chart marks (`docs/app-integration/L4-android-shots-2026-11-20/T604/`) and say if the day marks read well.
