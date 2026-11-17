# T838 — Drop the loose "Conditions: today" line above the estimate card

Status: IN PROGRESS (built on branch app-fix/t838-day-label, not merged)

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

## Built (T838)
- `ConditionsPanel.tsx`: the `conditions-day-label` line and its wrapper are gone; the `conditions-day-link` line stays, only when the regulations day is not today.
- `DAY_LABELS.conditions` removed from core `dayLink.ts` (nothing else read it); its assertions dropped from `dayLink.test.ts`.
- T582 F2 gate `ConditionsDay.test.tsx`: asserts no loose day label (a mutation pin) and the link line as before.
- Test-bed scenarios (`two-days`, `route-scroll`, `saved-spots`) used the label as their "Conditions is up" marker; they now wait for `gauge-card` and `two-days` no longer reads the label. Not run (no emulator this session).
- Web shots: local data has no live feed, so Conditions renders the "no estimate" path, which never carried the line; before/after look the same. Shots in the scratchpad t838-shots. The estimate card path is proved by the gate test.
