# T300 — Conditions, Runs and legend polish

Status: DONE bb242bbce, 312b365c7 2026-10-30, except the emulator gesture pass (heavy lock held by T298; run when free).

## Work
1. **Never default to the ELF forecast** on a gauge chart (pick CLEVER/COFFEE/other first; ELF only if the user picks it or it
   is the only one — and then say so).
2. **Model info text under the chart:** collapsed and hidden by default (one "About this forecast" toggle).
3. **Forecast credits / disclaimers / "data provided by… statement":** moved to the bottom of the page under one collapsed
   "Sources and notes" menu.
4. **Conditions map:** remove the ELF low-water statement from the map (it takes up space; it lives in the collapsed notes).
5. **Gauge pane's map tab:** fix the glitchy pan / pinch-zoom gestures (gesture conflict with the sheet scroll, double
   handlers, re-renders resetting the camera) — smooth like the main map.
6. **Runs legend:** a vertical heat-scale legend bar on the right side of the map showing what the orange (and its range)
   means, with ticks/labels; plain words.
7. **Legend:** when Fish obstacles is on, its symbols appear in the legend; shorten every legend item's words (fewer,
   clearer words) so the legend takes less space; keep T275 (legend overlays, map rect fixed).
8. **More tab sync** (owner 2026-10-30): the More tab gets the same sync / check-for-updates button as the map (same
   component, same states), next to the data freshness list.

## Acceptance
Phone + desktop screenshots before/after for each item; gestures checked on the emulator.

Depends: none. Size: M. Owner: no.

## Outcome
1. `seriesOf` default = publisher order CLEVER, COFFEE, ELF (`CHANGE_POLICY.order`); ELF only if picked/only one, then "Only ELF forecasts this gauge." shown.
2. Run line + ELF low-water text under one shut "About this forecast".
3. Disclaimer, Province attribution, low-water statement, credits all inside the fold, renamed "Sources and notes".
4. Change legend's low-water line removed from the map (in the notes).
5. Gestures: headless web with touch emulation (`T300-shots/gestures.mjs`) pans/pinches fine before and after on both the Conditions route map and the water Map view, so no web fault reproduced; `touch-action: none` added to the map box anyway. Native cause found by reading: the view-swipe Pan went active on a drag that began on the map (shield only set a flag), so RNGH cancelled the map's native touch. Now manual activation on devices; a press on a shielded thing fails the pan. Needs the emulator pass to confirm.
6. Runs: vertical key (`RunsKey`) at the map's right, overlay, four states. Note: the map is categorical (no continuous heat scale exists), so the bar shows the states.
7. `MAP_KEY` (core `mapKey.ts`) short words; obstacle marks join the key when that layer is on.
8. More tab: `SyncButton` (exported from TopBar) beside the Data list.
Shots: `T300-shots/` before-*/after-* (phone+desktop). Tests: `T300.legend.test.tsx`, `series-default-model.test.ts`, ConditionsLayout, MapDay updated.
