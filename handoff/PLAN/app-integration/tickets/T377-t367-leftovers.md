# T377 — T367 leftovers (UI)

Status: OPEN (filed 2026-11-04 at T367's close). Item 1 is in 0.5.0, after T376; the rest after 0.5.0 unless the owner
pulls them in.

1. **Forecast-day list by watershed** (owner idea, T367 round 3 A2): a list of days with each day's standing for a basin
   (`basinStandings`); tap a day to show it. Browse uses the map-centre basin, a water its own basin, no fallback.
   Needs T376's `centreBasin`, a water's basin and basin names.
2. From the map-UI research (unverified sources, proposals): a tappable legend (tap a class to isolate it), a
   collapsible legend bar, peak markers on the Runs histogram, a second cue besides colour for closed / open.
3. The hydrograph draws one model at a time (its legend entries pick it); drawing several at once needs chart logic.
4. Dead code: `changeSpark` / `ChangeRunVM.spark` (always null now) and core `mapDayChips` are no longer drawn — remove.
