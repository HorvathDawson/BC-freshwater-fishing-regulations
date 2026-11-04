# T377 — T367 leftovers (UI)

Status: OPEN (filed 2026-11-04 at T367's close). Items 1 (5b69686c2) and 4 done; items 2–3 after 0.5.0 unless the owner
pulls them in.

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
