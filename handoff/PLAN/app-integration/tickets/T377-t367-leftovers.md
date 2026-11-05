# T377 — T367 leftovers (UI)

Status: DONE in code 2026-11-05 (5b69686c2, 8658433c3, f42d32696, aea5620a1, f421b8bca, c1114b75d, 6326964ad; item 4 with the item-1 chat). Phone check with the next APK; owner design calls listed below.

1. **Forecast-day list by watershed** (owner idea, T367 round 3 A2): a list of days with each day's standing for a basin
   (`basinStandings`); tap a day to show it. Browse uses the map-centre basin, a water its own basin, no fallback.
   T376 (e21cdbeac) gives: browse `col.centreBasin` (`{ id, name } | null`; undefined off Conditions or not yet answered)
   and `col.centreBasinError`; a water `useWaterBasin(source, { item, tapSid, at: frame.at })` → `Async<Basin | null>`
   (@app/ui `basin.ts`); `id` is `basinStandings`' key, `name` the source's (a word around it goes in core words).
2. From the map-UI research (unverified sources, proposals): a tappable legend (tap a class to isolate it), a
   collapsible legend bar, peak markers on the Runs histogram, a second cue besides colour for closed / open.
3. The hydrograph draws one model at a time (its legend entries pick it); drawing several at once needs chart logic.
4. Dead code: `changeSpark` / `ChangeRunVM.spark` (always null now) and core `mapDayChips` are no longer drawn — remove.

## 2026-11-04
Item 4 done: `changeSpark` / `ChangeSparkVM` / `ChangeRunVM.spark` and core `mapDayChips` / `MapDayChip` removed (their
tests with them; `changeChart` keeps the handover breaks). Item 1 next (0.5.0); items 2–3 after 0.5.0.
Item 1 done (5b69686c2): core `basinDaysVM` (`BASIN_DAYS_WORDS`: "<name> watershed", the standing words of `standingWord`,
"No gauge reading" + its key), @app/ui `useBasinDays` (`basinStandings` for the one group asked, per `DAY_HORIZONS`;
`centreBasinAsk` for browse), ui-native `BasinDays` (map-ramp colour per day, each day a 44 × 44 button named in full, a
tap = `pickDay`). Browse Conditions: a strip card under the legend's ramp in the bottom bar (Flow/Level, proven pair).
A water's Conditions view: a row card after the panel (ConditionsPanel `footer`), its own group via `useWaterBasin`. No
fallback; failure = `PartPending` failed face. Pin: `basinDays.render.test.tsx` (every render's days are the asked group's).

## 2026-11-05 — items 2 and 3 (pulled in by the owner's session)
- **2a tappable key (8658433c3).** A tap on a regulations class (Closed / Own rules / Base rules / No access; Salmon's
  four) shows it alone on the map; tap it again (or the one shown) for all. Core `isolateDay` moves every other class's
  labels to `none` (the neutral "not asked" grey); the gear marks never ride `none`. The pick lapses with the colouring
  (Standard ⇄ Salmon, another tab, water coloured by something else). Other entries fade to 40 %; the pressed one is in
  ink (same width: T396's two rows hold). Gear marks / Not fishable are not tappable. Pin: `T377.legend.test.tsx`,
  `map/src/isolate.test.ts`, `core/src/mapKey.test.ts`.
- **2b fold (f42d32696).** A ▾/▴ button at the end of the Trout & more | Salmon row hides the key's rows (the switch
  stays); one choice across both modes, open by default, not remembered across launches. Only where that row exists
  (the regulations bar); the lake colourings' and Conditions' keys have no fold.
- **2c Runs peaks (aea5620a1, f421b8bca).** A dot over each chosen species' busiest pentad, in its colour (the season's in
  ink when unstacked); core `seasonPeak`, words `RUN_WORDS.slider.peak`. Pin: `RunsControls.peaks.test.tsx`.
- **2d closed's second cue (c1114b75d).** A dashed stripe in the paper's colour down a closed stream (runtime layer
  `stream-closed`; tokens `color.cue.closed` = `@color.outside`, `dash.cue.closed` [2, 3]; 40 % of the line's width),
  Standard and Salmon days; the legend's Closed swatch striped to match. Lakes keep colour alone. Pin:
  `map/src/closed-cue.test.ts`, `tools/profile-style.test.ts` (20 layers validated).
- **3 several models at once (6326964ad).** Each model's legend entry is now a switch (checkbox): the chosen model keeps
  its ribbon and notes, the others are traces beside it in their own dash (`FORECAST_DASHES`, swatch to match); a tap on
  the chosen one hands the ribbon to the next drawn; the last one off is Real-time only. T398 kept (ELF-only opens
  measured; Real-time only always offered). The read-out reads the chosen model only. Pins: `Hydrograph.models.test.tsx`,
  `ui/src/change.test.ts` (`pickModel`), `ui/src/hydrograph.test.ts`.

Owner design calls (conservative readings taken; say if any should change): 2a isolate = grey the rest (not hide) and
Salmon too; 2b fold only on the regulations bar, open by default, not persisted; 2d the stripe's look (paper dashes
inside the red line) — a proposal from unverified research; 3 secondary models as thinner dashed traces in the gauge
colour, no ribbon. Needs the phone: the stripe's legibility at z8–z12 on both themes, the fold's tap target, peak dots
over the histogram, three dash patterns telling apart at chart size.
