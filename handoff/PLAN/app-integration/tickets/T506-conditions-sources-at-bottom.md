# T506 — Conditions: sources and notes at the bottom

Status: IN REVIEW — 84bf729f4, in build 22 (3d807829d); owner: OWNER-TODO "Phone check with build 22" (Conditions: sources last).
Earlier: IN REVIEW — 84bf729f4; owner sees it on the next build. Owner 2026-11-08 (phone).

In the Conditions pane the sources and notes sit above (or between) the readings. Move them to the bottom of the
pane, after every reading/chart, as the regs pane does with its source fold.

## Do
- Every Conditions view (gauge, weather, the water's panel); the order is readings first, then notes, then sources.
- Shots before/after; one load path (no layout jump when the sources load late).

## Done (84bf729f4)
The basin's day strip (`BasinDays`, WaterViews' `footer`) drew AFTER the "Sources and notes" fold. `DonorPanel` takes it as
`after`, drawn after the readings and before the fold, in both layouts (estimate / no estimate). Test in
`DonorPanel.groups.test.tsx`. ui-native suite passes, tsc clean.
