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

Native (Android/iOS) shots were **not** taken. The emulator is a heavy job and was off limits while the tile rebuild ran.

## What changed, and why

1. **One reading order.** From top to bottom the panel is now: horizon control, then the headline (estimate, range and
   its caveats), then the lead gauge's reading, then its chart, then the donor map and list, then one fold.
   *Before:* the donor list sat between the headline and the gauge's reading, so a second answer-shaped block (the big
   number) came after a screen of donor rows, and the chart came last. *Now:* the reading sits under the headline as
   its detail, and the chart is next to the reading it plots. The same-gauge-trace fallback (`GaugeTrace`) also moved
   into that detail slot.
2. **The horizon control is the panel header.** The chips used to sit under the range, between the headline and the
   map. They now sit above the headline, which is the thing they change. There is still exactly one horizon control on
   screen: the map legend's copy in `Shell.tsx` is already hidden while a Conditions section is open
   (`showLegend` requires `condSection === null`), so it was left alone. See "Not done".
3. **One fold, "Notes and sources", closed by default.** It holds every row's "Counts this much because…" (now headed
   by the station id it belongs to), the "No gauge sits exactly here…" explainer and the Sources/credits block. The
   words are unchanged. The fold label is the only new string, and it comes from core (`CONDITIONS_WORDS.fold`).
4. **Caveats sit with the headline.** The disagreement, dam and short-record notes now appear directly under the range
   they qualify, each with a rule on the left. The disagreement rule is the restricted tone and the others are a
   hairline. They used to sit under the donor list. See "Deviations" for why they are not in the fold.
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
   exports were dropped from `index.ts`.

## Deviations from the ticket text (please decide)

- **The chart disclaimer is not in the fold.** "USERS OF THIS DATA MUST ACCEPT ALL RESPONSIBILITY…" is the Province's
  notice, and the code that carries it says it must appear verbatim wherever the forecast is drawn. It stays under the
  chart, inside `Hydrograph`. The Sources block (attribution) did go into the fold, as the ticket asked.
- **The three caveats are not in the fold.** These are the dam, short-record and disagreement notes. An existing pinned
  test ("says when the water is dam-controlled, above the working and not only in it") and the component's own
  rationale keep the dam note visible. A reader deciding whether to trust the headline should not have to open a fold
  to learn that the river follows a release schedule. Folding them is a one-line move in `DonorPanel.tsx` if you want it.

## Same content: how it is guarded

`app/packages/ui-native/src/ConditionsLayout.test.tsx` has two scenes. The estimate scene has three donors, a dam, a
disagreement, a reading and credits. The no-gauge scene is the second.

- Before any layout change, the test's record mode (`P16_RECORD=1`) captured every text line the pre-P16 panel drew in
  both scenes. That record is `ConditionsLayout.before.ts`: 43 lines for the estimate scene and 6 for no-gauge.
- After the change, each scene is drawn, the fold is opened, and every recorded line must still be drawn, matched as a
  whole text node.
- A mutation pin proves the guard is not vacuous. With the fold shut, the guard reports the explainer and the credits
  missing.
- Layout tests cover the block order, the caveats above the reading, exactly one horizon control, the horizon tap being
  handed back, the fold being shut by default and adding the small print when opened, and the no-gauge order.
- I also checked by mutation that swapping the reading and chart blocks fails the order test.

## Not done / not verified

- **Times (ticket item 5).** The reading's "observed … UTC" and the forecast run's "issued … UTC" are still labelled
  UTC. Those are core's sentences (`readingAgeLine`, `forecastRunLine`), pinned there. Making them local is a content
  change, which is out of scope under "same content".
- **Water-mode title (item 7)** depends on P14 and was not done.
- **"The unused forecast fetch" (item 8)** was not identified or removed.
- **Native screenshots** were not taken (see above).
- **`Hydrograph` and `ChartControls`** were not restyled. `ChartControls` is shared with the map legend, where its look
  is deliberate (it has to read over the map), and `Hydrograph` cannot be render-tested (RENDER-TESTS.md).
