# T920 — Conditions: the gauge map goes below the gauge chart

Status: DONE 2026-11-19 (b49e10e49 + 20dd416f9 review fixes; benches 44/44, d28344298). Follow-up noted below: the map surface's dark backing.

Owner 2026-11-19 ~16:30 (main chat), with a phone screenshot of a water's Conditions (Capilano River, dark mode;
`T920-shots/owner-2026-11-19-capilano-conditions.png`), verbatim: "add a ticket to move the gauge map below the chart on
conditions".

## Today (0.8.3)
Conditions reads, top to bottom: "Estimate for this spot" card (Now / Forecast / Change, the band, the sentence) → the
gauge map (DonorMap: "you are here", "gauge upstream: its water flows down to you") → the gauge card ("Capilano River at
Canyon": 72 h / Year, reading, chart, legend, Alert me) → "1 gauge on this water".

## Change
Move the gauge map (with its key line) below the gauge chart card, so the estimate and the reading come first. Same on
the web (one load path, the same order whatever the way in). Phone and web pictures at 390 light/dark before it lands.

## Also seen in the screenshot (check, don't assume)
The map box is plain white in dark mode, with only the +/− buttons drawn: either caught before its first frame or the
blank-route-map case (T603 `route-scroll`). Reproduce on the emulator (Capilano, Conditions, dark); report before fixing.

## Built (main chat, 2026-11-19 evening; uncommitted until the owner looks)
- `DonorPanel.tsx`: `{chart}` then `{mapBlock}` in both the answer and the no-estimate layouts (one place, every case, so
  T568's rule "one place in the sheet" holds); `DonorPanel.map.test.tsx` now pins the map UNDER the chart (both cases).
  `DonorPanel.map` + `ConditionsLayout` + `DonorPanel.groups` vitest: 16 passed.
- Pictures (sent to the owner): scratchpad `t920/vedder-{390-light,390-dark,1280-light}.png` — Vedder River, local web.
  Order: estimate → gauge card + chart → map + key → gauge list (desktop: the list in the right column).
- The white box (investigated, not fixed): on web in dark the map draws (dark basemap, routes, pins). The app's map
  container is `palette.tint` (`MiniMap.tsx:68`), so the white is most likely the native MapLibre surface before its
  first frame / style on Android (`Map.native.tsx` `onStyle`), with the +/− being the app's own overlay. Not reproduced:
  needs the Android emulator with `route-scroll` (T603/T831 history). Left (Claude): emulator check.

## Owner 2026-11-19 ~16:49 (main chat, on the Vedder dark picture, the gauge list circled; `T920-shots/owner-2026-11-19-legend-ask.jpg`)
Verbatim: "Turn this into just a legend not a selector for the chart. Then make the chart gauge selector more obvious?"
Map under the chart taken as accepted (the owner built on that picture).
- Built: the gauge list is a LEGEND: `DonorPanel` lost `onSelect`; a row is a plain `View` (role text, its label without
  "Show this gauge's chart"), no marked row, no "TAP A GAUGE TO SEE ITS CHART" (the word is removed from core). Content
  unchanged (each gauge's reading, facts, relation — kept, not filtered). The map still marks the charted gauge (`selected`).
- Built: the chart's gauge picker (`GaugeTitle`, `gauge-pick`) is a bordered field like a select: tint fill, the
  gauge's name, "N gauges" in the accent and a larger chevron; the border turns accent while open. One gauge: plain title.
- Tests: `DonorPanel.test.tsx` pins the legend (no button, no hint); the layout/chart order tests find the list by its
  label. Conditions + DonorPanel + core conditions + ui panel + spot vitest: 337 passed; ui-native typecheck clean.

## Owner 2026-11-19 (main chat, after the legend change)
Verbatim: "Make the legend a bit more simple but still showing how much each contribute to the stream overall estimate at top."
- Built: each legend row is ONE line — the dot (its pin's tone), the gauge's name (station id where it has none), and its
  share ("70% of this estimate" / "not counted today"). No longer drawn on the row (by the owner's ask): what the gauge
  reads, its facts (upstream · drains · apart · reaches · dam-controlled) and its relation line. The row's label keeps
  station, up/downstream, size apart and share; the dam note and each gauge's "because" stay in the fold.
- Left (Claude): `DonorRowVM` `reads` / `lastReport` / `facts` / `relation` are now drawn nowhere; drop them from
  `@app/ui` `donorView` (and their tests) once the owner accepts the look.
- Tests: 6 `DonorPanel.test.tsx` cases and the `ConditionsLayout` content list moved to the one-line legend (two new T920
  pins: one line per gauge with its share; no reading on the legend). 337 passed; `tsc -b` clean.

## Owner 2026-11-19 (main chat): chart navigation
Verbatim: "Could we make the chart navigation (switching between views) more logical too". Offered two layouts in words;
owner, verbatim: "i want mock ups to decide use industry standard. what would a user expect on a big banking app or
similar etc etc". Mock-ups being made (scratchpad `t920-nav/`): A finance-app standard (Flow|Level segmented above the
chart, time tabs under it, one "Forecast ▾" control, no zoom buttons), B labelled rows, C light reshuffle, beside today's.
Nothing built for this until the owner picks.
Owner, on the round-1 mock-ups (A finance-app, B labelled rows, C light reshuffle), verbatim: "None of them feel right and
the chart should be taller too I think." Round 2 being made (scratchpad `t920-nav/r2-*`).
Round 2 (`t920-nav/r2-contact-sheet.png`: D stocks app, D's Forecast sheet, E weather/river app). Owner picked
**"D: stocks app"**: a small Flow | Level switch under the big number; a TALL chart (~340 px plot at 390 wide, y labels
inside on the right); under it ONE row: "3 days" · "Year" tabs and a "Forecast ▾" pill opening a sheet (tick CLEVER /
COFFEE / ELF; "Show forecast" on/off; one explainer line); a compact legend; the observed/release line. Being built.

## Built (layout D), 2026-11-19 (uncommitted until the owner looks)
- Gauge card, top to bottom: title row (picker + Share; the switches left it) → reading → a small Flow | Level switch
  right-aligned under it (`chart-quantity`, only where both exist) → the TALLER chart → ONE row: "3 days" · "Year" tabs and
  a "Forecast ▾" pill at its right end (outlined in the chosen model's own colour while one is drawn; only where a model
  runs, not under Change) → a plain legend → About this forecast, foot, Alert me (unchanged).
- Chart: `chartHeight` ≈ 1.2 × width, 260–420 (340 px plot at a 390 window; 342 on desktop). The card now measures its
  INSIDE width, so the plot draws 1:1. Y labels inside the plot on the right (`DEFAULT_BOX` pads 4/4, a 34 px top band); the
  unit, one short faint hint ("Pinch to zoom, tap to read a value") — replaced by the read-out while a value is read — and
  −/+/⤢ (28 px, quiet) sit in that band over the plot. Pinch/drag/double-tap/wheel unchanged.
- Forecast sheet (`ChartNav.tsx` `ForecastSheet`, the app's `Sheet`): "Forecast", a checkbox per running model
  ("CLEVER · 10 days", `pickModel`, several at once), a separator, a "Show forecast" switch (new core `showForecast`; off =
  `realtimeOnly`), the explainer (core words). The sheet's open state lives in `ConditionsPanel`, and its models come from the
  model-free series, so a tick (which reloads the chart) never empties or shuts it.
- Legend: reading, normal range, 10th–90th, median, prior years, one "<MODEL> forecast" per model drawn (its colour and
  dash, read from the shape). Nothing in it is pressed.
- Words (core): `spanShort` "3 days" (keys kept), `key.model`, `forecast.*`, short `zoomHint`; `key.realtime*` removed.
- Tools: `play-shots` 11-cond-chart ticks the models in the sheet (and throws on one left off), `store-shots/checks`
  reads `chart-key-CLEVER`, testbed `gauge-level` scrolls to "3 days".
- Deviations: the two bands stay two legend entries — "normal range" is the middle half (FLOW_LEGEND's "normal"), with
  "10th–90th" beside it; one swatch for both would call the 10th–90th "normal". Zoom buttons kept (accessibility), small,
  in the band over the plot rather than on the data. Square corners/switch as the rest of the app (the mock's are round).
- Tests: new `ChartNav.test.tsx` (tabs read "3 days"/"Year"; the sheet ticks several, Show forecast off/on); legend has no
  pressable; Flow | Level after the reading; ConditionsLayout T398 through the sheet. 30 files / 374 tests passed; `tsc -b`,
  knip, platform, colours, check-tickets clean.
- Pictures: scratchpad `t920/d-390-light.png`, `d-390-dark.png`, `d-1280-light.png`, `d-390-light-sheet.png` (Vedder,
  local web on spare ports 39317/39318). Not verified: native (Android/iOS) rendering of the band and the sheet.

## Owner 2026-11-19 (main chat, on the layout D pictures)
Verbatim: "i hate the forecast selector i likerd the old legend forecast selector. also we dont need the data release code
just the last date.the fonts are all weird and we lost other detil in the gauge legend we also cant switch between year
and recent view anymore... and level/flow selector is messy"
Revision (round 3): the Forecast pill + sheet removed, the legend's model toggles back (CLEVER / COFFEE / ELF / Real-time
only, as before); Flow | Level and 72 h | Year back in the old segmented style, one row directly above the chart; the
taller chart and the zoom band kept; "Data release r-…" dropped from the gauge card (the observed date + "checked" stay);
the gauge legend gets a second line back (what it reads + its facts) and keeps the other-water relation line; the
year/recent switch bug found and fixed with a rendered test; pictures with the app's own fonts (the serif was the local
shot setup).

## Built (round 3), 2026-11-19 (uncommitted until the owner looks)
- Forecast pill + sheet and the pill span tabs removed (`ChartNav.tsx` deleted, `showForecast` and the `forecast.*` words
  gone). The legend's model toggles + "Real-time only" are back exactly as at HEAD; the plain keys beside them stay
  ("normal range" is the middle half).
- Flow | Level and 72 h | Year: the old segmented `ChartControls`, together on ONE row (`chart-toggles`) directly over the
  chart, after the reading; drawn whether or not a chart is (a gauge silent in the asked quantity keeps the other one tap
  away, T817). Span word "72 h" again.
- Kept: the taller chart, y labels inside on the right, the band over the plot (unit, short hint / read-out, small zoom).
  Axis labels now draw in the app's face (an svg text with no family was the browser's serif on the web).
- Gauge card foot: the "Data release r-…" line is gone (`DataProvenanceContext` is still provided by App.tsx, now read
  nowhere); "This gauge's own reading · observed … · checked … ago" stays.
- Gauge legend rows: line 1 dot · name · share; line 2 `detail` (@app/ui `donorView`: what it reads, a quiet gauge's last
  report, its facts, one line); line 3 the relation (faint) where there is one.
- (a tester via the owner: "this can be made into a drop-down box") the gauge legend — count line, ON THIS WATER /
  ESTIMATING FROM OTHER WATERS groups, rows, silent line — is ONE fold, closed by default (DonorPanel's `Fold`, now with a
  `label`): "4 gauges behind this estimate" (core `gaugesFold`). Order: chart card → map + key → gauges fold → watershed
  card → Sources and notes. A water with no estimate keeps its count line open in the refusal block (nothing to fold).
- Year switch: NOT reproduced on this build (served web, Vedder): Year draws the year chart ("the whole year against its
  record", Jan/Apr/Jul/Oct), 72 h draws the 72 h chart again, Level draws metres, Flow back. New rendered tests pin it
  (`ConditionsChart.test.tsx` "the chart's toggles change the chart"). Layout D's code is gone, so its path was not
  retested; on the phone (0.8.3, native) it needs an emulator check.
- Fonts in the pictures: the dev server has no `/assets/fonts/` (the WOFF2 files are copied in only by `tools/web-shell.mjs`
  at export, checked against `fonts.json`), so a deploy is fine; the shot script now serves `tools/seo/fonts/` for those
  requests.
- Tests: 46 files / 543 passed; `tsc -b`, knip, platform, colours, check-tickets clean. Pictures (app fonts): scratchpad
  `t920/r3-390-light.png`, `r3-390-dark.png`, `r3-1280-light.png`, `r3-390-light-year.png`, `r3-390-light-gauges-open.png`.

## Tester via owner 2026-11-19 (`T922-shots/tester-2026-11-19-4.png`, Squamish River Conditions)
Tester, verbatim: "I would say most people are not interested in this info and this can be made into a drop-down box or
something similar. Keep the page less messy" and "most people just want to glance at it and say yep it's fishable right
now". Owner: "I was in the middle of reworking that page so I could add it into a drop down". Added to round 3: the gauge
legend (count line, groups, rows with their detail lines) goes in ONE fold, closed by default, like "Sources and notes";
the map + key stay open.

## Owner 2026-11-19 (main chat, on the round 3 pictures `t920/r3-*.png`): "This looks good."
Round 3 accepted. Left (Claude): drop the now-unread `DataProvenanceContext` (and the dropped DonorRowVM-era leftovers
if any), Android benches (Year ↔ 72 h, Flow ↔ Level, the white map box in dark), tier + independent review, then land
with T919 (a, b), T921 and T917.

## Android benches 2026-11-19 21:29 (lane 2)
44/44 PASS at 1f989bb48 (patched MapLibre, same build id as 0.8.3): map tap → page 341–377 ms (one 648 ms), quick pinch
56–57.5 fps, zoom-out 48–56.7 fps, no pinch gap > 100 ms (report `app/tools/testbed/out/runs/2026-11-20T02-59-17/`). Dark
checks at 16d81f6b6 (Vedder, Capilano): order, Year ↔ 72 h, Flow ↔ Level, the gauges fold all pass. The white map box was
NOT reproduced (near-white share 0.5–2.4% across bursts; repeated tab-back and cold open untested). Follow-up, not in this
landing: give `MLMap` (`Map.native.tsx` ~1005) and the `DonorMap` box a `palette.tint` background so a fresh mount (every
`vm.key` change) never shows a bare surface; bench it when made.
