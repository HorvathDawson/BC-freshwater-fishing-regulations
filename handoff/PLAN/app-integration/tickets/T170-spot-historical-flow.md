# T170 — A spot gets the flow of its own day (historical), by refresh or on request

Status: DONE — ef762537 (app) + 298cee48, lane-T170 (with T171). App only: no feed, bundle or release change.

Source: owner 2026-10-23: "spots should be able to get historical flow on a day with a refresh and have option to get
it". Related: T51 follow-up (a spot backdated to an earlier day froze TODAY's gauge reading and panel, because capture
reads the live feed whatever the visit time — so its "flow then" is today's), S1 (spot lead-up), spots/refresh.ts.

## Today
Refresh (app/packages/data/src/spots/refresh.ts) fills a spot's missing WEATHER as of its day and the WEEK BEFORE's daily
means day by day — but the visit day's own reading/percentile comes only from the live feed at capture time. A spot
saved later for an earlier day, or pinned offline, keeps today's reading or none.

## Owner addition (2026-10-23)
"we could add in a way to access hydat for specific date measures when online?" — yes: HYDAT itself is a downloadable
SQLite file (not queryable online), but ECCC serves HYDAT's DAILY MEANS through the MSC GeoMet OGC API
(`api.weather.gc.ca`, collection `hydrometric-daily-mean`, filtered by station number and date; `hydrometric-realtime`
for the recent provisional record). Verify the exact endpoint, fields, licence (Open Government Licence – Canada) and
whether a browser may call it directly (CORS) before building; if not, the feed proxies the one request.

## Work
1. The visit day's flow from history: the station's DAILY value for that date — the feed's daily series (ECCC 30-day
   archive, already refilled by the feed) for recent days; for older days, WHEN ONLINE, one request to ECCC's GeoMet
   `hydrometric-daily-mean` (station + date: HYDAT's published daily mean); offline, the request waits (no history
   file needed unless the owner wants offline backfill — then a per-station history file in the release) — ranked against the same climatology
   the app uses (percentile + words), discharge only for "flow" (never a level standing in, decisions "NEVER MIX
   QUANTITIES"), the donor panel as of that day where the panel is used.
2. When: automatically on refresh for a spot whose visit day ≠ its capture day (backdated) or whose reading is missing;
   and an explicit "Get the flow for that day" action on the spot sheet. A frozen reading taken ON the day is never
   overwritten; a filled one is marked "filled in from the day's record" with its source and date.
3. Say what is known: a day past the archive and not yet in HYDAT reads "not published yet — try again later"; a station
   with no record that day says so. Offline: the action waits and says so.
4. Tests whole-value (backdated spot fills the day's percentile; a same-day spot is untouched; level-only station gives
   no flow); shots of a backdated Chilliwack spot before/after. Ships: app (+ feed/release if a history file is needed).
Size: M.

## Licence (owner 2026-10-23)
HYDAT is under the Open Government Licence – Canada (decisions.md "OWNER 2026-10-23 — data permissions") — the
licence check in the work list is settled; endpoint and CORS still to verify.

## Endpoint, fields, CORS (verified 2026-10-23, this machine)
- `GET https://api.weather.gc.ca/collections/hydrometric-daily-mean/items?f=json&STATION_NUMBER=08MH001&DATE=2024-08-15`
  -> 200, pygeoapi 0.20.0, GeoJSON FeatureCollection; properties `STATION_NUMBER`, `STATION_NAME`, `DATE`,
  `DISCHARGE` (m³/s), `LEVEL` (m), `*_SYMBOL_EN/FR`; values are float32 widened (19.600000381469727). Header
  `Access-Control-Allow-Origin: *`.
- Built on the same collection with an interval so ONE request also tells the cases apart:
  `…&datetime=<day>/..&sortby=DATE&limit=1` returns the first record on or after the day — that day (its values, either
  may be null), a later day (no row for that day), or nothing (not published yet). Verified: 08MH001 2024-08-15 ->
  19.6 m³/s; 2026-08-15 -> nothing; HYDAT for 08MH001 ends 2026-06-30 (that row blank); 08NM116 ends 2024-12-31.
- **Browser**: a `fetch` of that URL from this lane's own Expo page (http://localhost:39611, own headless Chromium)
  returned 200 with the 2024-08-15 feature — a browser can call it directly. **No feed proxy needed**, none built.
- Licence: Open Government Licence – Canada (decisions.md, owner 2026-10-23). The app already credits "Hydrometric
  data: Environment and Climate Change Canada".

## As built (ef762537, 298cee48)
- **The one read** — `flowOnDay(station, date, ports)` (`app/packages/data/src/spots/dayFlow.ts`): a station that
  measures no discharge -> `no-discharge`, nothing asked. Else the feed's "year" series (`gaugeSeries`, the existing
  source): a day on or after the day the feed was written -> `pending` (S1's rule: never freeze a morning); a mean this
  year -> source `feed` (ECCC's daily mean, provisional); a mean in a complete year the feed carries (`priorYears`,
  HYDAT) -> source `hydat`, no request. Else, online, ONE GeoMet request (`geometHydat`): discharge -> `hydat`; level
  only -> `no-discharge`; blank or a later row -> `no-record`; nothing -> `not-published`; unreachable or the device
  offline -> `offline`. Ranked in the series' envelope for that day of the year with core `percentileInBand` (the feed's
  `percentile_of`) — the climatology every reading is ranked in. `dayIn` (leadup.ts) is the one reader of a day in a
  station's year, shared with the week before.
- **On the spot** — `Spot.dayFlow`: `{state: filled, station, date, discharge, percentile, place, source, filledAt}` |
  `{state: none, why: no-discharge|no-record, askedAt}` | `{state: not-published, askedAt}`. `fillDayFlow` writes it;
  `readingIsTheDays` (the reading's own time — else when saved — on the visit's local day, with a value) means it is
  never asked and the reading is never touched. The frozen reading and panel are kept as recorded.
- **When** — automatically by the refresh (`needsRefresh` now includes a spot never asked; `refreshSpot(…, {hydat})`
  also re-asks `not-published`); and "Get the flow for that day" on the spot sheet (`useDayFlow` in @app/ui; App
  `onDayFlow` -> `fillDayFlow` through `useSpots.refresh`'s patch, so an edit made meanwhile wins). Offline: the action
  says "Offline: the flow for that day will be asked for when you are back online." and asks again by itself when the
  device is back (`Connectivity.subscribe`).
- **Said** — `dayFlowLine` (data/spots/words.ts): "19.6 m³/s, at or below the 10th percentile for the date — filled in
  from the day's record (HYDAT daily mean, 15 Aug 2024)."; not published / no record / level-only each in a whole
  sentence; under the frozen estimate of a backdated spot: "The estimate above was read when this spot was saved
  (23 Oct 2026), not on the day of the visit."
- **Like your visit** — the visit's flow is the frozen reading only when it is the day's; else the filled day's
  percentile; else none ("No flow then to compare") — the T51 backdated-spot bug (today's reading as "then") is gone.
- Not done: the donor PANEL as of that day (needs every donor's mean that day; the spot's own station is used). See Q2.

## Shots (`../T170-shots/`, own Expo :39611, own headless Chromium, data :39217 GET only, scratch feed index by interception)
- `spot-light-before.png` / `spot-dark-before.png` — a Chilliwack spot saved 23 Oct 2026 for 15 Aug 2024: refresh line,
  today's 11.2 m³/s with the "not the day" note, "Get the flow for that day", strip with no flow then.
- `spot-light-after.png` / `spot-dark-after.png` — after the action: 19.6 m³/s from HYDAT (here out of the feed's
  priorYears, no request); the strip now compares flow; `*-after-plus2.png` with Sun 25 Oct (+2) opened, flow forecast.
- `spot2019-dark-before/after.png` — visit 15 Aug 2019: the action made ONE GeoMet request from the page
  (`…datetime=2019-08-15%2F..&sortby=DATE&limit=1`) and filled 23.2 m³/s (HYDAT daily mean, 15 Aug 2019).
  (The shot fixture's notes/weather text still say 2024 — fixture text only.)

## Tests and mutation pins
- `packages/data/src/spots/dayFlow.test.ts` (16, whole values): GeoMet URL + the recorded 08MH001 2024-08-15 answer;
  later/none/blank; HTTP failure rejects; feed day / priorYears day with no request; one request older; not published /
  no record / level-only day / day not over; level-only station asks nothing; offline three ways; the backdated spot
  before (flow then none, refresh line) and after a refresh (whole record, words, strip flow, reading untouched); a
  same-day reading untouched (same object, nothing asked); a no-reading spot filled from the feed; level-only settles;
  not-published kept and re-asked; offline unchanged.
- `ui/src/dayFlow.render.test.tsx` (6): views whole; offline waits then asks by itself; offline/pending answers said.
  `ui-native/src/DayFlowBlock.test.tsx` (8): mounted light + dark, action, filled line in ink, offline, absent in a
  draft and on a same-day spot.
- **No second copy**: `app/tools/one-history-read.test.ts` — GeoMet named in dayFlow.ts only; `flowOnDay(` called only
  there; `fillDayFlow(` by refresh.ts and App.tsx only; `dayFlow:` written by dayFlow.ts and refresh.ts only.
- Mutations (each run, each failed): visit flow = the frozen reading whatever its day (3); a level standing in (1);
  level-only station asked (2); a day's reading overwritten (1); offline asks anyway (2); a feed day not over frozen (1);
  no re-ask when back online (1); a second GeoMet reader in refresh.ts (1).
- Existing tests changed by design: lead-up tests' fake now returns a reading of the visit day (a spot with no reading
  of its day now also asks for its flow, so "nothing outstanding" there is about the week); the Spots banner now reads
  "missing the weather or the flow from the day …".

## Adversarial review
- Offline: no request; said; re-asked when the device is back; the refresh leaves the spot unchanged and still offering.
- Level-only station: `no-discharge` with no request; a HYDAT day with a level and no flow is `no-discharge` too.
- Tests that could pass for the wrong reason: the feed-day and priorYears cases also assert HYDAT was NOT asked; the
  percentile pins are `percentileInBand` values recomputed (0.375, 0.625, 0.244), and a mutation ranking a level fails.
- A spot saved with no reading on its own day (a silent gauge) now asks too — `pending` until the day is over, then the
  feed's mean. That raises the "spots to refresh" count for such spots and for every backdated spot until asked.
- The visit's local day vs the station's standard-time day differ by at most an hour around midnight; not handled.
- The frozen DONOR PANEL of a backdated spot is still today's (now labelled so); see Q2.

## Owner questions (default built)
1. Offline backfill (a per-station history file in the release)? Default: no — online only, said when offline.
2. A backdated spot's "What the water was doing" still shows the estimate frozen at save time, now with "read when
   this spot was saved (…), not on the day of the visit". Hide it instead, or recompute the panel for that day?
   Default: keep it, labelled; the day's own flow sits right under it.
3. A same-day spot whose gauge was silent at capture now offers "Get the flow for that day" (filled the next day from
   the feed's daily mean). OK? Default: yes.
