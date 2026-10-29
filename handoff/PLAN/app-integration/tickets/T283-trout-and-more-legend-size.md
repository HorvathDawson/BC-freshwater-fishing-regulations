# T283 — "Standard" mode renamed "Trout & more"; map legend text larger

Status: OPEN (owner 2026-10-29: "the name standard keeps irritating me" → "trout & more i think is good"; "the legend text
should be a bit bigger for each map too. rn it is hard to read").

## Work
1. Every user-visible "Standard" for the map mode becomes "Trout & more" (mode switch, walkthroughs, Help, report form's
   "Map colours", About/Updates, SEO/site copy if any). Internal keys (`standard`) stay; words live in the core words tables.
   Check the switch still fits at 360 px.
2. The legend under each map mode (Trout & more, Salmon, Runs) uses a larger type step (e.g. TYPE.small → TYPE.body or a new
   legend token, ≥15 px) with swatches scaled to match; the legend still overlays the map (T275: map rect unchanged).

## Acceptance
Phone shots per mode before/after at 360 and 390 px; grep shows no user-visible "Standard" for the mode; tests updated.

Depends: T275, T276. Size: S. Owner: no.
