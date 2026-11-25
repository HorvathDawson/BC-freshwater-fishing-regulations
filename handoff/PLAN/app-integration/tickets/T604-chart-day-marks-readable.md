# T604 — The chart's day marks are readable: no overlapping labels, today is obvious

Status: IN PROGRESS — owner's look at the web shots taken as done on his word 2026-11-20; left (Claude): emulator shots | 90e41efc6

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T604 | The chart's day marks are readable: no overlapping labels, today is obvious (two-days chart) | IN PROGRESS — owner's look taken as done (2026-11-20); left (Claude): emulator shots | 90e41efc6 | owner 2026-11-10 on build 25, with a shot |

Source: owner 2026-11-10 on build 25 (Fraser, CPR Bridge at Mission → Landstrom Bar, regulations day Wed 11 Nov):
"The only issue is the chart for 2 days. It has overlapping text and not obvious what current day is." Shot:
`handoff/PLAN/app-integration/T604-shots/owner-phone-build25-before.png`.

## Today (from the shot)
- Two vertical rules sit a day apart: "now" (dotted, grey) and the regulations day (dotted, cyan). Their labels
  "TODAY" and "WED 11 NOV" are both centred on their rules at the same height and print over each other
  ("WED 11 NOV" + "TODAY" reads "WED 11: NOIDAY").
- Nothing says which rule is now: both are thin dotted lines; the cyan one (the regulations day) is the stronger.
- The chart's hint line "Pinch to zoom, drag to move along," runs under the pinned access band at the top of the scroll.

## Work
1. Labels never overlap: when two marks are closer than their labels' widths, place them apart (one left-aligned to
   its rule and one right-aligned, or two rows), measured, not guessed; the same at every chart zoom and span, on web
   and phone, from one layout function with a unit test over close/far/edge cases (also a mark at the chart's edge).
2. Today is the obvious one: "Today" labelled at the now rule where the solid reading line ends, with the stronger
   treatment; the regulations day is the quieter mark, labelled with its day ("Wed 11 Nov") and matching the text line
   above the chart ("Regulations day: Wed 11 Nov, marked on the chart"). When the two are the same day: one mark.
3. T582 leftover: with a Conditions day chosen as well, all marks are drawn (today, Conditions day, regulations day)
   or the ones not drawn are named; decide by what stays readable and say so in the shots.
4. The hint line does not sit under the pinned band (check the scroll offset/padding of the chart card).
5. Shots, web and emulator: marks one day apart, same day, a week apart, regulations day past the window.

## Acceptance
- The owner's case (today + the next day) reads cleanly on the phone; a test fails if two mark labels' boxes intersect.

Depends: T582. Size: S. Owner: looks at the shots.

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

Status line before this answer: IN REVIEW — owner looks at the web shots in ../T604-shots/ (`*-after.png`), then on the phone; emulator not run | 90e41efc6

## Emulator shots 2026-11-20 (L4, Android emulator, build of 124d79efa, light)
Coquitlam River -> Conditions, the chart at 1080x2400, shots in `handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T604/`:
`two-days-regs-day-is-today.png` (one mark: bold "Today", solid rule), `two-days-regs-day-tomorrow.png` ("Today" at the end of the
solid reading line, "Sat 21 Nov" quiet, dashed, to its right; no overlap; the hint "Pinch to zoom, tap to read a value" whole
on the line under the controls), `two-days-regs-day-past-window.png` (11 days on: Today only, no regs mark). Not shot: a week
apart, adjacent days, a Conditions day plus a regs day (the unit test covers them; the web shots of 11-20 show them). Dark not shot.

## Owner

- [ ] 2026-11-20 — Look at the two-days chart marks (`handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T604/`) and say if the day marks read well.
