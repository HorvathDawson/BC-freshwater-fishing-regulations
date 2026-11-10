# T580 findings: one water, one answer (web vs phone, Flow vs Level, Now vs a day ahead)

This was a read-only audit at HEAD after 1c57e7c07 (T567/T568). No source was edited and nothing was built or run. The
exact T567/T568 defects (the donor map's place and camera, the card's figure and today labels, and the per-quantity lead
on a forecast day, T567 b) are left out because they are already handled.

- **CONFIRMED**: traced end to end by reading the code.
- **PLAUSIBLE**: the code allows it, but it was not traced through every hop.

Classes:
1. web vs native
2. layout that depends on data
3. one choice made more than once
4. a value shown out of its context
5. entry path or kept state

The list is ranked by what an angler would see wrong, worst first.

## Findings

### F1. A level-only lead under Flow turns the gauge card into a Level card, and the map and estimate stay on Flow (class 4, CONFIRMED)

- **Where:**
  - `app/packages/ui/src/answers.ts:77` `chartParameter` returns `undefined` when the station lacks the picked quantity.
  - `app/packages/ui-native/src/ConditionsPanel.tsx:114-118` then sets `shown = param ?? vm.reading?.quantity ?? "level"`.
  - The T567 guard at `ConditionsPanel.tsx:191` (`otherOnly = reading.quantity !== shown && …`) never fires, because `shown` has already become the station's own quantity.
- **What the angler sees:**
  - The map is set to Flow. On a gauged lake (speaker `lake`, a level station) or a water whose lead falls on a level-only donor (237 stations measure stage only), the card shows a level figure.
  - The card also shows a coloured standing dot and "Normal · 50th percentile", a level chart, and a quantity control lit on "Level" (its only option).
  - Above the card, the estimate and the map are on Flow. For a lake under Flow the map draws the "no baseline" dash (`data/src/standing.ts:118-121`) beside a ranked level standing on the card.
- **Fix:** when the caller owns the quantity, `shown` is the asked quantity, always.
  - A station without it says `OTHER_QUANTITY_WORDS[has].reads`, the words the row and pin use.
  - The chart offers "Show Level", which sets the one Show choice. It does not quietly draw level.
- **Gate:** add a conformance test. For each lead kind (panel donor with both quantities / flow only / level only, a lake station) × Flow / Level × Now / +3, assert that the card's quantity is the asked one or the refusal words appear, and never the other quantity's figure.

### F2. One water screen keeps two day states, so Regulations and Conditions answer different days (class 3 and 5, CONFIRMED)

- **Where:**
  - `Shell.tsx:159` holds `mapDay`, the "ONE DAY" read by Regulations and the water's Map view (`shell/WaterModeHost.tsx:60,83`).
  - `shell/useMapColouring.ts:84` holds a separate `hz` horizon counted from today, read by Conditions (`WaterModeHost.tsx:54`).
  - `WaterConditions` never passes `day`, so `ConditionsPanel` runs on `day = null`, which means today (`WaterViews.tsx:111`, `ui/src/panel/conditions.ts:131`).
- **What the angler sees:**
  - They pick Saturday with the date pill on Regulations and swipe to Conditions. Conditions answers Now, or whatever horizon was last picked on the Conditions map.
  - In the other direction, picking +3 on Conditions leaves Regulations on today.
  - Neither view says the other is on another day.
- **Fix:** this needs an owner decision.
  - (a) One day: horizon = mapDay − today when it falls inside the forecast range. Otherwise Conditions says that day has no forecast.
  - (b) Keep two days, but each view's header names its day, and switching views says so.
- **Gate:** a Shell test. Set the day on one view, switch view, and assert the same day (or the stated difference).

### F3. After a restart, web and phone restore different state, so the same water gets a different lead gauge (class 5 and 1, CONFIRMED)

- **Where:**
  - On the web, the horizon and day are written into the address (`apps/mobile/src/systemBack.web.ts:115`) and restored (`App.tsx:625-626`, `initialHorizon`).
  - On the phone, `systemBack.native.ts:20` does nothing, so a cold start is on Now.
  - The quantity (Show) is kept on both (`openLayerStore`, `spots/store.{web,native}.ts:43`).
  - The lead is chosen per (quantity, day) (`sheetLead` over the rows of `answerFrom(…, quantity, horizon)`, `data/src/standing.ts:74-82`; T567 b).
- **What the angler sees:** at Fraser at Mission with Level kept, a web reload on +3 leads with one gauge and a phone cold start on Now leads with another. This is the owner's "web and phone show different gauges" through kept state, not code.
- **Fix:**
  - The phone keeps the horizon the way it keeps Show, or the web stops restoring it. One rule for both, written in the README "kept state" table.
  - T567 b's decision (one lead per water) removes most of the visible effect.
- **Gate:** a conformance test. Restore from each platform's kept state (address vs store) and assert the same `(quantity, horizon, day)`.

### F4. One standing is worded four ways on one sheet, and a fifth way on the public gauge page (class 3, CONFIRMED; AGENTS 23)

- **Where:**
  - Headline: `core/src/conditions.ts:638` `plainStanding` ("Low for the time of year").
  - Gauge card: `conditions.ts:800-806` `readingStandingLine`, using `standingWord` ("Below normal · 20th percentile for the date").
  - Donor rows: `conditions.ts:485` ("Reads 2 in 10").
  - Map dot: `ui/src/gaugePoints.ts:55` `percentileLabel` ("p20th").
  - Basin day strip: `core/src/basinDays.ts:72` `standingWord` ("Below normal").
  - Public gauge page and share card: `workers/feeds/src/gauges/card/card.ts:230-231`, using `plainStanding` and a rounded `figure()` ("1.55 m").
  - The app card prints the raw value ("1.548 m", `ConditionsPanel.tsx:210`).
- **What the angler sees:** the same gauge at the same moment reads "Low", "Below normal", "2 in 10" and "p20th" on one screen, and the public page shows a different figure.
- **Fix:** one core phrase function per form (headline / stat / row / dot), all from one band table, and one `figure()` shared by the worker and the app.
- **Gate:** a `tools/one-*.test.ts` speaker test. Only core's phrase module may call `standingWord` / `plainStanding` / `percentileLabel`. A parity test checks the card's facts against the app card's facts for one fixture station.

### F5. The water's day strip names a different watershed group depending on how the water was opened (class 3 and 5, CONFIRMED; T369)

- **Where:**
  - `ui-native/src/WaterViews.tsx:90` asks `useWaterBasin` with the raw `{ tapSid, at: from }`.
  - `ui/src/basin.ts:62-71` uses the tap point, or otherwise the water's whole middle (`waterPoint`).
  - A part step clears the tap (`ui/src/route.ts:265`). Search, a deep link and a reload have none.
  - The panel uses the frame's part (`fv.section` / `fv.here`, line 67-69).
- **What the angler sees:** on the Fraser at Mission, a tap shows the Lower Fraser group's days, while a step, a search or a reload of the same part shows the group at the Fraser's middle, with a different name and different days.
- **Fix:** ask the basin with the frame's point (`fv.here`), the one reading of a frame.
- **Same pattern, low:** `WaterScreen.tsx:113-114`, where sunrise, sunset and daylight hours fall back to the whole water's point after a step. Use the frame's point there too.
- **Gate:** add to the T369 entry-path test: tap / step / search / reload of one part give equal `(section, here, basin)`.

### F6. Gauge pick, span and model pick survive a part step but reset on a new water, a reload or a deep link (class 5, CONFIRMED)

- **Where:**
  - `ui/src/panel/conditions.ts:157` holds `picked` in `useState`, never reset on `section`.
  - `ConditionsPanel.tsx:110-121` holds `own`, `span` and `pickState`.
  - `WaterViews.tsx:111` mounts `ConditionsPanel` with no `key`, so it stays mounted across a part step on the same water. The frame view stays ready because the nav rows are per item. A new water unmounts it, since `section` goes null while `nav` loads.
- **What the angler sees:**
  - They pick Hope in the gauge switch on part 3, then step to part 4. If Hope is in part 4's panel it still leads, and the year span and model stay.
  - Opening part 4 any other way leads with the heaviest gauge at 72 h.
- **Fix:** one rule.
  - The gauge pick is per section, so reset it on `section`.
  - Span and model are the reader's, so hold them in the Shell beside Show and keep them on every path.
- **Gate:** a render test. Step A→B vs open B: equal lead, span and model.

### F7. The alert editor's "now" disagrees with the card above it (class 3 and 4, CONFIRMED)

- **Where:**
  - `ConditionsPanel.tsx:333` passes `current` only when `reading.quantity === shown`. The card above shows `reading.figures[shown]` (`:194`, T567 c).
  - The editor's context line reads a third source, `useAlertContext` = the 72 h series' `now` (`ui/src/notify.ts:143-151`).
  - The background check reads `obs.json` (`data/src/notify/check.ts`).
- **What the angler sees:** Hansard (08KA004, declares flow) under Level shows "2.18 m" on the card. "Alert me" opens with an empty value and its own "now" from the series.
- **Fix:** pass `figures[shown]`, and have the editor and card read one current-value function.
- **Gate:** a test that the card figure equals the alert prefill for every (station kind × quantity).

### F8. The card's figure and percentile come from a different file than the rows, dots and estimate (class 4, PLAUSIBLE)

- **Where:**
  - The card reads `StationFile.now` through `feed.now()` (`data/src/feed/http.ts:401-415`; `ui/src/hooks/gauge.ts:72-94`), on a 5-minute TTL.
  - Rows, dots, route colours and map classes read `index.json` (`http.ts:349`), which is held by the update service (`no-store`, `offer`).
- **What the angler sees:** the two can come from different publisher runs. The card says "66th percentile" while the same gauge's row in the table reads a different decile. T567 notes `obs.json` listing only the stations written in a run, so runs are partial.
- **Fix:** the card takes the percentile from the index row (the same `reading()` the rows use) and the figure from the station file only when its `at` matches. Otherwise it says the two times.
- **Gate:** a fixture with a staler index than the station file. Assert that the card's percentile equals the row's.

### F9. A saved spot freezes the Flow lead whatever the angler was looking at (class 3, CONFIRMED, low)

- **Where:** `data/src/spots/dayFlow.ts:305` (`livePanel` = `answerFrom(…, "discharge")`) and `spots/capture.ts:130` (`sheetLead` over those rows).
- **What the angler sees:** a spot saved while the sheet showed Level and led with Hansard records whichever gauge leads under Flow.
- **Fix:** capture the quantity and horizon on screen and the lead shown (`vm.lead`), or say in the record that it is the flow estimate.
- **Gate:** none needed beyond a unit test.

### F10. The browse map drops the gauge dot of a station silent in the quantity; the donor pin and row say why (class 3, CONFIRMED, low; owner decision)

- **Where:** `ui/src/gaugePoints.ts:93-95` (the comment calls it a different roster by design).
- **What the angler sees:** Mission under Flow has no dot on the Conditions map, but has a pin and a row saying "Reports water level only today" in the sheet.
- **Fix:** an owner decision is needed on whether a dot keeps its place in a muted state. If yes, add a muted dot class through `layers.source.json`.
- **Gate:** none.

### F11. A part step swaps the whole Conditions body for a spinner, and the reading block appears late (class 2, CONFIRMED, low)

- **Where:**
  - `ConditionsPanel.tsx:175`: `if (vm.loading) return <LoadingState>`. `useSpeaker` is keyed per section and has no `sameAs`, so each step drops the estimate, the chart, the day strip (`footer`) and the scroll position for a frame.
  - `readingRows` (`:195-234`) is null while the reading loads and is inserted above the chart when it arrives, which causes a layout shift.
- **Fix:**
  - Give `useSpeaker` a `sameAs` per water so a step keeps the last body dimmed.
  - The reading block keeps its place with a one-line placeholder.
- **Gate:** add to the T568-style order test: block order is equal in the loading, ready and refusal states.

### F12. The phone can run an older release than the web for hours or days (class 1, CONFIRMED; found by the web/native lane)

- **Where:**
  - `data/src/release/boot.ts:271-377` and `App.tsx:292,352,373-384`: the phone opens the installed set and applies a staged one only on foreground or a cold start.
  - "Wi-Fi only", a `minAppVersion` above the store build, or a rollback holds it longer.
  - The web always opens the latest (`boot.ts:227-237`).
- **What the angler sees:** after a release that moves gauge matches or panels, the same water has different donors and leads on phone and web.
- **Fix:** show the release id on the Conditions "as of" line in non-production builds, and apply a ready staged set at cold start.
- **Gate:** a boot test that both platforms end on the same `data_release` once staging is done.

### F13. A switchable phone build can keep another environment or channel (class 1, PLAUSIBLE)

- **Where:**
  - `apps/mobile/src/config.ts:22-27,51-55`: the `envStore` / `channelStore` overrides are kept when `EXPO_PUBLIC_ENV_SWITCH=1` (staging APKs).
  - On the "test" channel the gauge index comes from `test/feeds/live/` but station files come from `feeds/live` (`env.ts:136-170`), so the dots and rows disagree with the card on the phone itself.
- **Fix:** print env, channel and release in the "as of" line when overridden, and keep index and station files on one base.
- **Gate:** `resolveEnv` / `resolveChannel` with `switchable: false` ignore the stores. Add a test that the index and station URLs share a base.

### F14. Offline, only the phone answers from a kept week, and its "now" has no standing (class 1, CONFIRMED)

- **Where:** `data/src/spots/store.native.ts:58` vs `store.web.ts:58` (`null`), and `data/src/feed/history.ts:150-159` (a `now` built from `obs.json` rows with `percentile: null`).
- **What the angler sees:** offline, the phone shows a figure with "no record to compare against", while the web shows nothing. The words are wrong, because the record exists and only the kept copy lacks the percentile.
- **Fix:** keep the percentile in the kept file, or say "kept reading, as of …" with no standing claim.
- **Gate:** a history test.

### F15. Two clocks, small (class 1, PLAUSIBLE)

- `core/src/salmonFeed.ts:255` formats "Checked with DFO …" with the device's `getHours` / `getDate`, with no PT label. Hermes uses OS tzdata and Chrome uses ICU (`clock.ts:27`), so the time can show an hour apart.
- `ui/src/panel/conditions.ts:182` builds the forecast-day instant from `today.getFullYear/Month/Date` (device-local fields of `bcNow()`).
- **Fix:** use `bcClockTime` / `localStamp`.
- **Gate:** extend `tools/clock.test.ts` to ban local-field getters on instants.

## Checked and found clean (do not re-audit)

- **Map colour vs sheet:** both go through `speakerFor` → `speakerStanding` / `answerFrom` with one cache (`ui/src/panel/standings.ts`, `data/src/standing.ts`). The route map, the class path and the sheet are one calculation, held by `conformance/src/release.test.ts`.
- **Gauge dots:** they follow the quantity and horizon through the same `reading()` as rows (`gaugePoints.ts:93`).
- **Basin day strip:** it is answered in the panel's quantity and refuses to hold another quantity's days (`basinDays.ts:87-97`).
- **Horizon chips:** one `horizonChips` / `pickHorizon` for the map corner and the panel header. Change under Level moves to Flow in one place.
- **Regulations page:** the target is from the frame part or the tapped section, and the salmon card and access notices are by page subject (T387). The salmon card's place is always drawn (pending / failed).
- **Data-keyed remounts (`key=`) in ui-native:** only `WaterScreen` (retry), `DonorMap` (camera key, T568) and list items. No other remount on a toggle.
- **Web/native pairs:**
  - Release cache, open, install and source: different storage, same bytes (sha checked).
  - Spots stores: same JSON and defaults, except the gauge history (F14).
  - `runTimingStore`, `photoStore`, `connectivity`, `locate`: location accuracy only.
  - `startLink`, `notify`, `appUpdater`, `memoryTrim`: one-platform features.
  - All UI-only pairs (`chartInput`, `partAsk`, `searchFocus`, `touchDevice`, `windowSize`, `platformLine`, `holdViews`, `prewarm`, FishSpinner, WindowInset).
- **Every `INLINE_ALLOWED` entry and `Platform.select`:** sizes, words, flags or prefetch only. None changes an answer, apart from the `App.tsx` gates behind F12.
- **Map halves:** the one-sided native commits (T478, T443, T499) are probes or timing fixes, and the fit and highlight fixes landed in `web/useFit.ts` / `web/useHighlight.ts` in the same commits.

## Not covered

- A line-by-line comparison of `Map.web.tsx` vs `Map.native.tsx` (camera, selection feature-state, tap hit-test layers). I relied on `parity.test.ts`.
- `PdfViewer` / `synopsisPdf`, `share`, `pickPhoto`, `escapeBack` / `escapeCloses`, `swipeActivation`, `announce`, `openOfficial`.
- Run timing (`RunsSheet`, `ui/src/runTiming.ts`), the Salmon colour mode vs the salmon card, `LeadUpStrip` / `LikeVisitStrip` / spot `Record.tsx` (which quantity and gauge they draw), alerts and the notifications page beyond F7, More / About, and the offline maps page.
- Whether the phone's HTTP stack honours the feeds' `max-age=300` the way a browser does (this bears on F8).
- Nothing was run. No vitest file was executed, so each CONFIRMED finding is confirmed by reading only.
