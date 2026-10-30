# T300 — Conditions, Runs and legend polish

Status: OPEN (owner 2026-10-30).

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
