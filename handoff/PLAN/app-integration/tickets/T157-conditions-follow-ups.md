# T157 — Conditions follow-ups from T143 and T144

Status: DONE — 071a0952 (lane-T157)

Source: T143 and T144 adversarial reviews, 2026-10-23.

## Work
1. The gauge switcher still says "quiet" for a gauge on a flow-only day under Level (it should say what the sheet says).
2. 07FC008 and 08FB012 have a Change and a forecast but no record to rank against: their days ahead still read
   "No forecast … reaches" — say "no record to compare with" instead.
3. The map and the section's Level view show no low-water caveat when the Change comes from ELF (only the panel does).
4. The day line drops sharply at the COFFEE → CLEVER handover; draw the handover (a mark or a gap), not a fall.
Size: S–M. Ships: app (and feed for 4 if the series changes).

## As built (app only; the feed is unchanged)
Measured on a copy of the live feed (`data/generated/gauges/feeds`, fetchedAt 2026-10-23T20:41Z, already republished
with T143/T144): forecast + Change + no day ranked = exactly 07FC008 and 08FB012; 43 stations' Change is ELF alone
(`lowWater`); 122 joined CLEVER+COFFEE changes have COFFEE's reach (10-27) inside the line.

1+2. **One reading of why a member is silent on a day ahead** — `@app/data` `aheadWhy(row, quantity, horizon)`:
   `flow-only` (asked Level, the feed ranks the day in flow), `no-record` (a fresh run — the index's `forecast` and
   its record-free `change` — and NO day ahead ranked: the publisher ranks against the HYDAT envelope, so a station
   with none has none), else undefined (no run reaches it). Both readers take it from there: `silentWhy` (the panel's
   refusal: flow-forecast-only, else the new `forecast-unranked`, else no-forecast) and `DonorRow.aheadWhy` (each
   member). Core `donorState` gains `flow-only` and `forecast-unranked` (only ahead; never over a reading or the
   real-time list), and every switch over it says them: row reads ("Has a flow forecast this far ahead, but no level
   forecast ranked against its record." / "Has a forecast this far ahead, but no record to compare it with."), dot and
   key (`ROUTE_MAP_WORDS.flowOnly` "flow forecast only", `.noRecord` "no record to compare with"), screen-reader row
   label, silent line ("Not counted this far ahead: X (flow forecast only) · …" when such a member is silent).
   - New refusal `forecast-unranked`: "The gauges here have a forecast this far ahead, but no record to compare it
     with." — a refusal about the day (chips kept; Change answers for those stations).
   - **The switcher** (`@app/ui` `gaugeChoices(rows, lead, {ref, horizon})`) now says each member's state in its dot's
     words (`donorMapLabel`) at the sheet's day and horizon, and `gaugeChoiceLabel` (moved to core `routeMap.ts`) the
     same. It said "quiet" / "not counted today" for every member that did not count; now e.g. "reporting, not ranked",
     "not a real-time gauge", "no forecast", "flow forecast only".
   - `feed/http.ts` `IndexFile.forecast` was typed as an array; the publisher writes a boolean (fixed; nothing read it).
3. **ELF caveat on the map and the section.** Core `changeDotLabel(ratio, lowWater)` → "0.9× today · low-water model";
   `gaugeGeoJSON` passes the index row's `change.lowWater`. The Change legend (Shell) carries `CHANGE_LEGEND.lowWater`:
   '"low-water model": forecast by ELF alone, which has no rain in it and cannot see a rise coming.' The section's own
   answer: `DonorRow.lowWater` (set by `answerFrom` under Change for a counted member whose change is ELF's alone) →
   `DonorVM.lowWater` = core `changeLowWaterNote(stations, all)` ("Part of this change comes from ELF alone (08MG013).
   ELF is a low-water model: …"), drawn under the range in `DonorPanel` (`testID="section-low-water"`, not folded).
4. **The handover is drawn.** `changeSpark` takes the station change; at each joined model's reach before the line's end
   the path breaks (a moveTo, never a line drawn across) and `handovers: [{x, model}]` marks the gap; the screen-reader
   label adds "; COFFEE's forecast ends Tue". `ConditionsPanel` draws a dashed tick in the gap (`testID="change-handover"`).
   A one-model line is never broken. No feed change needed: the days were already per day.

## Tests (whole values; mutation → tests that fail, each run)
- `donorState` ignoring `aheadWhy` → one-donor-state (states, order, the cut), core silentLine "names why…", ui
  switcher "says what the sheet says…", conformance "each member says 'flow forecast only' under Level at +3" and
  "says 'no record to compare with'…" (7 tests).
- `aheadWhy` dropping its `row.change` guard → both liveFeedHorizons tallies and the 07FC008/08FB012 test (08MH098, a
  stale run with no change, joins them).
- `gaugeChoices` back to `donorShare` → both switcher tests in `panel.gauges.test.ts`.
- `changeSpark`'s break dropped → ui "draws the COFFEE to CLEVER handover as a gap with a tick" and the joined block
  test. (Reasoned, not run: `handovers` not drawn → the mounted "marks where COFFEE stops" test, which pins the tick.)
- `gaugeGeoJSON` not passing `lowWater` → gaugePoints "marks a gauge whose change is ELF's alone". `DonorPanel` not
  drawing `vm.lowWater` → its mounted test. `all` always false / note at every horizon → ui "says where the change
  comes from ELF alone" (reasoned from the asserts, not run).
- New pins on the owner's feed snapshot (conformance, `feed-index-2026-10-23T0350Z.json`): tallies now carry
  `forecast-unranked: 2` at every horizon (from no-forecast); the Chilliwack panel under Level +3: the 4 reporting
  members "flow forecast only", the 4 unlisted "not a real-time gauge".
- Existing whole-value tests updated to the new words: switcher labels (ConditionsChart ×2), `handovers` on spark VMs,
  `ROUTE_MAP_WORDS`, the NoEstimate table.

## Shots (`../T157-shots/`, `conditions-shots.mjs`; own Expo :39571 against the owner's :39217, GET only, live feed)
- `map-change-lower-fraser--phone-dark.png` / `-light.png` — Change map: the Harrison gauge's dot "0.9× today ·
  low-water model", the legend's low-water line.
- `harrison-change--desktop-dark.png` — Harrison River under Change: "Part of this change comes from ELF alone
  (08MG013). …" under the answer; the switcher's "no forecast" members.
- `englishman-change--desktop-dark--scrolled.png` — Englishman River (08HB002, CLEVER+COFFEE): the day line breaks
  after COFFEE's last day with a dashed tick, no cliff; "COFFEE runs to Tue; after that, CLEVER alone."
- `chilliwack-level3--desktop-dark.png` — Chilliwack under Level +3: switcher "08MH016 flow forecast only", the
  unlisted gauges "not a real-time gauge". (On today's republished feed Level +3 answers from 3 gauges, so the
  whole-sheet flow-only refusal is pinned in tests, not shot.)

## Owner questions (defaults built)
- Q1 The switcher now names each silent member's state ("not reporting", "no forecast", "flow forecast only", "not a
  real-time gauge") instead of "quiet". Default: the state words (the map dot's). Alternative: back to "quiet" except
  on a day ahead.
- Q2 The map's low-water mark is on the gauge dot ("· low-water model") plus one legend line, always shown under
  Change. Default built. Alternative: show the legend line only when such a dot is on screen.
- Q3 The handover is a gap and a dashed tick, unlabelled on the drawing (the note line under it names it). Alternative:
  a small "COFFEE ends" label at the tick.
- Q4 "No record" is inferred in the app (a fresh run, a Change, no day ranked). Default: app-only, no republish.
  Alternative: the publisher writes an explicit `record: false` on the index row.

## Checks
- `REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh` (read-only `.venv` symlink to the main checkout,
  removed after): tickets → boundaries → platform → style → colours → … → deps → typecheck green; vitest
  8047 passed / 4 failed — exactly the 4 known x4p_1010 regs parity tests (caveats ×2, keep, sheets; T100's Shuswap map
  page). The report also listed 4 more failures (`packages/map/src/sections.test.ts` shared state handle,
  `tools/water-frame.test.ts` MUTATION, `conformance/src/regs/harness.parity` "frozen VM fixtures are fresh",
  `tools/regs-text-scale`): none touches this change; each passes when rerun alone (152 + 3 passed) — load flakes
  under the full parallel run.
- Targeted: core 962, ui/data/ui-native/conformance subsets all green after the changes.

## Rebuild
App build only. No feed republish, no bundle, no tiles.

## Left / follow-ups
- `useConditionsView`'s lead/quiet head (`quietHead`) and a gauged lake's own head do not get `aheadWhy` (no row for a
  lake's own station): a gauged lake with no record would still say "No forecast for this gauge that far ahead.".
- The Conditions MAP under Level +1/+3/+5 still has no word that the forecast is in flow (T143 follow-up, untouched).
