# T971 — Conditions: picking Level in the gauge card also changes the estimate card above it

Status: OPEN.

Source: L4 Android device-looks pass 2026-11-20 (T728 look, emulator).

## What was seen
Coquitlam River → Conditions. The estimate card at the top reads "Normal for the time of year · Lower than 7 days in 10".
Pick Level in the gauge card below: the estimate card's headline becomes "Below normal for the time of year" (the bar and
"Lower than 8 days in 10 …" move too) and its Change button gains the line "Flow only". Back on the map nothing changed (T728
holds: the map's layer is its own). Shots (`handoff/PLAN/app-integration/L4-android-shots-2026-11-20/T728/`):
`T728-1-card-opens.png` (Flow) and `T728-2-card-level.png` (Level).

## Why it matters
"Flow leads, Level auxiliary" (owner 2026-11-10): the headline verdict is the lead gauge's, ranked in Flow. A reader who
taps Level to look at the stage sees the top verdict flip from Normal to Below normal. T728's rule is that the card's pick is
the card's: whether the estimate card above is part of "the card" is not said anywhere.

## Work
Decide (owner question in OWNER-TODO): the estimate card stays on Flow whatever the gauge card shows, or it follows the pick
and says so. Then one rule, a test, a shot.

Depends: T728. Size: S. Owner: decides.
