# T157 — Conditions follow-ups from T143 and T144

Status: DONE — 071a0952 (lane-T157); adversarial review a78dabfd

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
- `englishman-change--desktop-dark.png` — Englishman River (08HB002, CLEVER+COFFEE): the day line breaks
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
- A gauged lake's own head (`lakeQuietHead`) does not get `aheadWhy` (no panel row for a lake's own station): a gauged
  lake with no record would still say "No forecast for this gauge that far ahead.". (A panel member's `quietHead` does:
  it reads the `DonorRow`, which carries `aheadWhy`.)
- `changeSpark`: a stretch of ONE day either side of a handover (a model reaching only today, or the line's last day but
  one) is a lone moveTo and draws nothing. Not on the live feed (all 122 handovers are 5 days | 5 days, measured), so
  not fixed.
- The route map's own dots in the sheet (`donorMapLabel`) under Change carry no "low-water model" mark; the section's
  note under the answer says it.
- `aheadWhy`'s "no-record" does not check that a run reaches the day asked (true on today's feed: 07FC008's ELF runs to
  11-20, 08FB012's to 11-20); an explicit `record: false` from the publisher (Q4) would settle it.
- The Conditions MAP under Level +1/+3/+5 still has no word that the forecast is in flow (T143 follow-up, untouched).

## Adversarial review (2026-10-23, lane-T157)
Re-read the diff against the four asks; re-ran every touched suite; mutated; read the live feed (`data/generated/gauges/
feeds`, 20:41Z) and the builder's shots.
- **Confirmed sound:** exactly 07FC008 / 08FB012 are forecast + Change + no `ahead` (08MH098 is forecast without a Change
  and stays "no forecast"); every index station is `listed`, so "not a real-time gauge" can never hide a flow-only
  member; `silentWhy`'s flow-only branch is the old `contributionsOf(…discharge…)` test member for member (it is only
  called with an area); the 122 joined changes all break 5 | 5 days, the tick sits in the gap (Englishman shot).
- **Fixed — the low-water note named gauges the answer left out.** `DonorRow.lowWater` was set on any member reading a
  change, and `lowWaterNote` counted `donorCounted` (reads), so a reserve member that STOOD BACK (core `answering`, weight
  0) was named in "Part of this change comes from ELF alone (…)", and a stood-back non-ELF reserve turned an all-ELF answer
  into "Part of". Now `answerFrom` sets `lowWater` only for a member not stood back, and the note counts core's new
  `donorVotes` (counted AND weight > 0) — the rule `shortRecordNote` already used inline, now one function both call.
  Test: ui `change.test.ts` "never names a reserve gauge that stood back from the answer". Mutations (each run): no
  `!back` -> fails; `donorCounted` back in `lowWaterNote` -> fails; both -> fails.
- **Fixed — a mixed silent line lost a member's reason.** With a flow-only or no-record member silent, the kicker became
  "Not counted this far ahead", and a member no run reaches then read only "Liard River (last 20 Oct, 21:35 PDT)" — as
  if it too had a forecast. It now says "Liard River (no forecast; last 20 Oct, 21:35 PDT)" under that kicker (unchanged
  under "No forecast this far ahead"). Test: core "names why a member with a forecast is not counted a day ahead" (whole
  line). Mutation: `silentMember` ignoring the kicker -> fails. Not shot: panels ship in a binary and I found no real
  panel with that mix on today's feed.
- **Shots:** the three `--scrolled` files were pixel-identical to their unscrolled pair (decoded and diffed: no pixel
  differs; the desktop sheet did not scroll); removed, and the handover reference above now names the file it is in.
  `conditions-shots.mjs` still writes them.
- (Review resumed after a machine restart; the two fixes above re-verified: their tests pass, and each mutation named
  was re-run and fails.)
- **Fixed — "Peak Sun" read as the handover tick's label.** Drove the real app (own Expo :39575, the owner's :39217 feed,
  20:41Z) through `conditions-shots.mjs`: on Englishman River the day line's words row was `space-between`, so the mark's
  words sat centred — and a 5 | 5 day joined line (all 122 on the feed) puts the handover tick exactly mid-line, so
  "Peak Sun" stood directly under the dashed tick while the peak dot was a third of the way along. The words now stand
  toward the mark: two spacers either side share the free width as the mark divides the line (`mark.x` and
  `ends[1].x − mark.x`, the VM's own units). Test: ui-native ConditionsPanel "marks where COFFEE stops…" pins the split
  (33 %, mark on the 2nd of 4 days). Mutation: equal spacers (centred again) -> fails. Shot re-taken:
  `englishman-change--desktop-dark.png` ("Peak Sun" under the dot, the tick unlabelled in the gap).
- Seen in the real app and as the ticket asks: Chilliwack under Level +3, the switcher's 08MH016 "flow forecast only"
  and the unlisted members "not a real-time gauge", as the sheet's rows; Harrison under Change, "Part of this change comes
  from ELF alone (08MG013)" under the range with the stood-back fix in.
- Checks (after the fixes): `REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh — every gate green (tickets,
  boundaries, platform, style, colours, … typecheck); vitest 8048 passed / 4 failed, exactly the 4 known x4p_1010 regs
  parity tests (caveats ×2, keep, sheets: Shuswap "Rule book p.28"). `pytest pipeline/tests/test_update_hydat_script.py`
  unfiltered: 8 passed. Targeted core conditions / ui change / ui-native ConditionsPanel / check-boundaries /
  status-colours-distinct green.
- Rebuild: app build only, as before.

