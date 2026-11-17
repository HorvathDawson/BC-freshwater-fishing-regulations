# T838 — Drop the loose "Conditions: today" line above the estimate card

Status: OPEN (app, web + phone; app chat)

Source: owner 2026-11-17 ~11:46, app chat, with a web screenshot of a spot's "Estimate for this spot" card (verbatim):
"having random conditions today text here is weird too". Dialog 11:48: "Drop it (Recommended)".

## Work
- `ConditionsPanel.tsx` (~L410): remove the `conditions-day-label` line (`DAY_LABELS.conditions`). The card's Now / Forecast /
  Change switch names the day (the Forecast segment shows the picked day, `HorizonPicker`).
- Keep the T582 link line, shown only when the regulations day is not today: "Regulations day: <day>, marked on the
  chart." / "<day> is past the forecast."
- Update the T582 F2 gate (`ui-native/src/ConditionsDay.test.tsx`): the day is named by the switch, the link line stays.
  Drop `DAY_LABELS.conditions` if nothing else reads it (knip).

Refines T582 (each tab names its day): on Conditions the switch does it. Size: S.
