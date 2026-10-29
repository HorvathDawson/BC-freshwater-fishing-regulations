# T261 — run timing v2: its own Runs tab, a date slider, watershed groups zoomed out, species chips, DFO counts

Status: PARTIAL — app + feed (counts, field) built and on staging; native device run + owner look open

Source: owner 2026-10-29 — run timing (T199) must be "really nice": its own bottom tab like Map / Conditions, a date
slider, areas coloured zoomed out and streams zoomed in, multi-select species with the fish art, a scale of the run
where the data has numbers ("no counts" otherwise, never invented), legend / tap sheet / credits / offline polish.
Follows T199 (feed + the old Run timing map mode).

## Built

**Tab.** Map · Conditions · **Runs** · Spots · More (`@app/core` `BROWSE_TABS`, icon Lucide `fish-symbol`). Run timing left
the Standard · Salmon switch (`RegsLayer` is standard | salmon). The Runs map has the maps' top bar (search, sync, the
bell — unchanged in place, owner 2026-10-29), no Layers sheet, and taps open the runs sheet (only at or above the field's
handover, z8).

**Species chips** (top-left, `RunsControls` `RunSpeciesChips`): checkboxes with the ocean-phase fish from `FISH_IMAGES`
(`RUN_SPECIES_CODE`), several at once, the last one cannot be cleared, steelhead the default, kept per device
(`openRunSpeciesStore`, web key / phone file). Painting several species: spawn > pass > off per section and group.

**Date slider** (the foot, `RunDaySlider` + `RunDayCaption`): 73 pentads, drag / tap / arrows / Page / Home / End
(`sliderKey`), a today tick and a Today button, Play (Lucide play / pause, 650 ms a pentad, loops), month letters, and the
season behind the track (share of watershed groups with the species local / passing per pentad, `RunTimingFacts.season`,
scaled to its own peak). The map repaints from a 140 ms-throttled pentad; the caption and sheet follow live.

**Zoomed out / in.** Below z8 the watershed groups (the Conditions field's `basin` layer, `basin_id` = FWA group code)
take a new `runs` mode (feature-state `rt` by basin id, provider `runs_field`; field `drawnIn` += runs), above it the
streams as before (spawn solid, pass dashed). Same hand-off as Conditions (`fieldAnswers` includes Runs). Light theme
`color.run.pass` moved #E8590C → #D99A00: as a fill it was indistinguishable from spawn.

**Sheet** (`RunsSheet`, words in core `runSheet`): fish art, a year chart per run (73 cells in the map's colours, the
slider's day marked), in / not in the water on the slider's days, dates in the source's terms, DFO survey years, the
counts (a bar per counted year on the legend's log scale, latest, trend from >= 6 years) or "No counts", the other
species folded behind "N runs of other species here", credit with terms, Open this water.

**Legend** (`RunsLegend`): states (lines zoomed in, fills zoomed out), the field note, the run-size key (10 … 100k),
loading / stale / failed, "Offline: the run timing saved on this device" from the last capture, short credit.

**Feed (additive, formats stay /1)** — `pipeline/runtiming`: DFO runs carry `n` = [[year, NATURAL_SPAWNERS_TOTAL]] (latest
12 counted years, from every NuSEDS row of the record, 0 kept); runs doc `counts` names the field; paint carries `basins`
{ids, m: [[slot, local pentads, passing pentads]]} from a new base column `wg` (section point → watershed group;
`python -m pipeline.runtiming.feed.base --add-basins <base>` augments an existing base). PSF runs get no counts: its
written permission covers timing curves and reach assignments only. Built against 10cbcb72e9f907cf and published to
staging 2026-10-29 (runs-b5c435f91ca5, paint-9ec741de8362; 1,519 DFO runs with counts, 163 of 246 groups painted);
augmented base uploaded to `private/runtiming/base/`.

## Tests
`core/runTiming.test.ts` (pentad states for several slots, the field, season, kept species, pentad words, slider keys,
counts + trend, the sheet whole), `ui/mapModes.test.ts` (Runs modes, field, unproven pair), `map/native-style.test.ts`
(basin run states in the phone's style), `ui-native/Runs.shell.test.tsx` (tab bar, top bar, chips + store, slider → map,
the sheet with counts and the fold), tab-bar strings in MapRail / WaterMode / back tests, pipeline
`test_runtiming_feed.py` / `test_runtiming_parse.py` (19 pass). Shots: `T261-shots/` (desktop light + dark, phone light +
dark; zoomed in / out / sheet), web against the local dev server with the new feed.

## Open / owner
- DONE 2026-10-29 (owner: yes): DFO's NATURAL_ADULT_SPAWNERS ships as a SECOND series (`na`, doc `counts.dfo.adult`),
  beside the total (`n`), never summed or merged. Sheet: "Total spawners" / "Adult spawners", each with bars, latest,
  trend; the total leads, adults alone where no total. Legend note says "total, else adults". Runs with total 1,519,
  adult 3,353, both 1,492 (either 3,380; of 4,886 runs; parse records 1,651 / 3,596 / 1,620 of 4,944). Staging:
  paint-8af119a9e0b2, runs-989e47c3c739, waters-c8a289a39afb (handles 10cbcb72e9f907cf = LATEST r-0fa7927b16e5).
  Shots: phone-dark-sheet-two-series, phone-dark-legend-scale-note.
- PSF spawner abundance (steelhead and every CU) would need PSF's permission extended; not shipped.
- Native device run not done (web + phone-web only); on a phone the zoomed-out field's states go through the style
  (a style swap per throttled pentad) — measure on a device.
- The local tile server (:39217) predates T199's `/runtiming/` route: the dev app reads the feed only after it restarts.
