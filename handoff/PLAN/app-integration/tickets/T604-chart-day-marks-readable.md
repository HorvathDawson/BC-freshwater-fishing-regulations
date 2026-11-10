# T604 — The chart's day marks are readable: no overlapping labels, today is obvious

Status: IN REVIEW — owner looks at the web shots in ../T604-shots/ (`*-after.png`), then on the phone; emulator not run | 90e41efc6

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
