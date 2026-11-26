# T261 — run timing v2: its own Runs tab, a date slider, watershed groups zoomed out, species chips, DFO counts

Status: PARTIAL — 6df3861ea, a87b6a58a: app + feed (counts, adult series, field) built and on staging 2026-10-29; the Runs tab is in every native build since (core tabs.ts BROWSE_TABS). left (Claude): the native device run (Runs tab, slider, chips, sheet on the emulator, shots beside T261-shots/) and the counts-coverage note; then the owner look on the next phone check.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T261 | run timing v2: its own Runs tab (bell stays), date slider by pentad (Play, today, season), watershed groups zoomed out, multi-species chips with fish art (kept), DFO counts in the sheet ("No counts" otherwise), feed `n` + `basins` on staging | PARTIAL | 6df3861ea, a87b6a58a | left (Claude): emulator run of the Runs tab + counts-coverage note; then the owner look |

Source: owner 2026-10-29 — run timing (T199) must be "really nice": its own bottom tab like Map / Conditions, a date
slider, areas coloured zoomed out and streams zoomed in, multi-select species with the fish art, a scale of the run
where the data has numbers ("no counts" otherwise, never invented), legend / tap sheet / credits / offline polish.
Follows T199 (feed + the old Run timing map mode).

Done (6df3861ea, a87b6a58a; staging 2026-10-29): Runs tab (Map · Conditions · **Runs** · Spots · More, `@app/core`
`BROWSE_TABS`; bell unchanged in place, owner 2026-10-29), species chips (`RunSpeciesChips`, steelhead default, kept per
device), date slider by pentad (`RunDaySlider`, 73 pentads, Today, Play), watershed groups below z8 (`basin` layer,
`runs` mode) and streams above, `RunsSheet` (counts or "No counts"), `RunsLegend`. Feed additive, formats stay /1
(`pipeline/runtiming`): DFO `n` = [[year, NATURAL_SPAWNERS_TOTAL]] and the second series `na` (NATURAL_ADULT_SPAWNERS,
owner: yes 2026-10-29, never summed) + paint `basins`; staging paint-8af119a9e0b2, runs-989e47c3c739,
waters-c8a289a39afb (handles 10cbcb72e9f907cf = LATEST r-0fa7927b16e5). Earlier staging: runs-b5c435f91ca5,
paint-9ec741de8362. Tests: `core/runTiming.test.ts`, `ui/mapModes.test.ts`, `map/native-style.test.ts`,
`ui-native/Runs.shell.test.tsx`, pipeline `test_runtiming_feed.py` / `test_runtiming_parse.py`. Shots (web + phone-web):
`T261-shots/` (phone-dark-sheet-two-series, phone-dark-legend-scale-note).

## Work (left)
- Native device run (web + phone-web only so far): Runs tab, slider, chips, sheet on the emulator, shots beside
  T261-shots/. On a phone the zoomed-out field's states go through the style (a style swap per throttled pentad) —
  measure on a device.
- Counts-coverage note: runs with total 1,519, adult 3,353, both 1,492 (either 3,380; of 4,886 runs; parse records
  1,651 / 3,596 / 1,620 of 4,944); 163 of 246 groups painted. PSF runs get no counts: its written permission covers
  timing curves and reach assignments only (PSF spawner abundance would need PSF's permission extended; not shipped).
- Then the owner look on the next phone check.
- The local tile server (:39217) predates T199's `/runtiming/` route: the dev app reads the feed only after it restarts.
