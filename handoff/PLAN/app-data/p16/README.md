# P16: Conditions, same content, better formatted (for owner review)

Shots are **web** (react-native-web, headless Chromium at 420 px wide), taken with the Expo dev server from this worktree
and `app/tools/serve-tiles.mjs` serving the main checkout's data. The data source was the app's default **release
path** (P11), with no `EXPO_PUBLIC_DATA_SOURCE` override, and it served Conditions without problems. The day was 2026-10-20 and the feed's
last reading was 2026-10-15, so every reading says "may be out of date". That is real data, not a layout problem.

| case | before | after (fold shut) | after (fold open) |
|---|---|---|---|
| Gauged river: Chilliwack River near Vedder Crossing (3 reporting donors, 1 quiet) | `before-gauged-chilliwack.png` | `after-gauged-chilliwack.png` | `after-gauged-chilliwack-fold-open.png` |
| Ungauged stream, donor estimate: Borden Creek (one donor 15 times its size) | `before-ungauged-borden-creek.png` | `after-ungauged-borden-creek.png` | `after-ungauged-borden-creek-fold-open.png` |
| No gauge: Cultus Lake ("NO ESTIMATE") | `before-lake-cultus.png` | `after-lake-cultus.png` | `after-lake-cultus-fold-open.png` |

The **after** shots were retaken after the review fixes (commit `a525e29c`), using the same route each time:
Search, then the water's eye button, then the Conditions face. They were taken with headless Chromium through the repo's
Python Playwright, because the Playwright MCP browser was in use by another session. The data server was the main
checkout's `serve-tiles.mjs` on port 39417 and the web server was Expo on port 39418. The data source was the default
release path, with no `EXPO_PUBLIC_DATA_SOURCE` override.

Native (Android/iOS) shots were **not** taken. Running an emulator or simulator is a heavy job, and this workflow forbids
heavy jobs. That acceptance item is still open for a human run.

## What changed, and why

1. **One reading order.** From top to bottom the panel is now: horizon control, then the headline (estimate and range),
   then the lead gauge's reading, then its chart, then the donor map and list, then one fold.
   *Before:* the donor list sat between the headline and the gauge's reading, so a second answer-shaped block (the big
   number) came after a screen of donor rows, and the chart came last. *Now:* the reading sits under the headline as
   its detail, and the chart is next to the reading it plots. The same-gauge-trace fallback (`GaugeTrace`) also moved
   into that detail slot.
2. **The horizon control is the panel header, and it is the only one.** The chips used to sit under the range, between
   the headline and the map. They now sit above the headline, which is the thing they change. The map's own corner copy
   on the Conditions tab is **removed**. That covers the `MapScreen` `horizons` prop, the wiring in `Shell.tsx` and
   `mapModes.horizonsShown`. Its labels were typed in a second time there; core's `HORIZONS` and labels are now the only
   source. Because the map no longer has a control that says which horizon it is painting, `Shell` resets the horizon to
   "Now" when the Conditions panel closes. **Consequence for you to weigh:** the map can no longer be painted as a
   forecast. A reader sees forecasts only in a section's panel. If you want the map's forecast view back, it should come
   back as one shared control, not as a second copy.
3. **One fold, "Notes and sources", closed by default.** It holds, in this order:
   - every row's "Counts this much because…", headed by the station id it belongs to;
   - the **notes**: short record, dam and gauges disagree, in their pre-P16 order, each marked by a rule on its left;
   - the "No gauge sits exactly here…" explainer;
   - the **Centre's chart disclaimer** ("USERS OF THIS DATA MUST ACCEPT…"), shown under the same condition as before
     (a forecast is drawn);
   - the Sources/credits block.

   The words are unchanged. The fold label is the only new string, and it comes from core (`CONDITIONS_WORDS.fold`).
4. **Times are local and labelled, in one format (item 5).** Core's new `localStamp` gives "2026-10-15 at 11:20 PDT".
   That is the same day-and-minute form as the spots' `localDateTime`, plus the zone name. If the runtime has no zone
   names it falls back to the offset ("UTC−7"). The reading's "observed …" and the forecast run's "issued …" both use it.
   The chart's axis ticks are now on the reader's clock too. They used UTC hours with no label. Each tick is not labelled,
   because the run line under the chart names the zone.
5. **Typography on the T22 tokens.** Every ad-hoc `fontSize` override in the panel (10, 10.5, 11, 11.5, 13.5, 22, 34)
   was removed:
   - Kickers use `TYPE.kicker`.
   - The headline is `TYPE.name`, sentences are `TYPE.body`, and small print is `TYPE.meta`, `metaSemi` or `metaBold`
     (12.48 px minimum, up from 10 to 11.5 px).
   - Station ids and readings use the figure face.
   - The reading's number, its unit and the stage sit side by side in one figure style.
6. **Spacing on `SPACE` and `GUTTER`.** The screen and panel use `GUTTER` (16) at the sides, and blocks are separated by a
   hairline and a `SPACE.s4` step. Inside a block the gaps are `SPACE.s1`, `s2` and `s3`. Donor rows are divided by
   rules between rows rather than a box around the list, and their detail lines line up under the station id.
7. **Dead UI removed.** `GaugeBadge.tsx` and its test had no importer. `StatusChip.tsx` (`Chip`) had no importer. Both
   exports were dropped from `index.ts`. **The unused forecast fetch is gone too.** `useConditions` and
   `useStationReading` each fetched the station's 72-hour series for a `forecast` field that nothing read. The chart
   fetches its own series through `useSeries`. A test now asserts that drawing the reading never calls `gaugeSeries`.

## Deviations from the ticket text

The review fixes folded the chart disclaimer and the three notes, as the ticket says. **The lead (2026-10-20) moved the
Centre's disclaimer back under the chart**, visible with the fold shut: the Province requires it verbatim "wherever a
forecast appears" (see the attribution in `App.tsx` and `pipeline/hydro/forecast.py`), and folding a required notice is
the riskier reading. The after shots above were taken before that move, so they show it in the fold. If you are sure
the fold satisfies the requirement, folding it again is a one-block change in `ConditionsPanel.tsx` (and the
`ConditionsLayout.test.tsx` case that pins it visible).

## Same content: how it is guarded

`app/packages/ui-native/src/ConditionsLayout.test.tsx` has two scenes. The estimate scene has three donors, a dam, a
disagreement, a reading and credits. The no-gauge scene is the second.

- Before any layout change, the test's record mode (`P16_RECORD=1`) captured every text line the pre-P16 panel drew in
  both scenes. That record is `ConditionsLayout.before.ts`: 43 lines for the estimate scene and 6 for no-gauge.
- After the change, each scene is drawn, the fold is opened, and every recorded line must still be drawn, matched as a
  whole text node. One change is declared: each `… UTC` stamp in the record is re-said through `localStamp`, which is
  the same instant. A test pins that this touches exactly one recorded line.
- A mutation pin proves the guard is not vacuous. With the fold shut, the guard reports the explainer and the credits
  missing.
- Layout tests cover the block order, the notes and the chart disclaimer in the closed fold (a third, forecast-running
  scene), exactly one horizon control in the panel, the horizon tap being
  handed back, the fold being shut by default and adding the small print when opened, and the no-gauge order.
- I also checked by mutation that swapping the reading and chart blocks fails the order test.
- No horizon control on the map. `MapScreen.test.tsx` passes `horizons` under `@ts-expect-error`, so `tsc` fails if the
  prop returns. It also checks that no chip is drawn.

## Not done / not verified

- **Native screenshots (iOS and Android).** Not taken, because emulators are heavy jobs here. This needs a human run.
- **Water-mode title (item 7).** This depends on P14 and was not done.
- **`Hydrograph` and `ChartControls`** were not restyled. `ChartControls` is shared with the map legend, where its look
  is deliberate.
