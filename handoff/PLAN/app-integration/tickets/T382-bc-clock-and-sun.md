# T382 — The regulations' "today" is B.C.'s; sunrise and sunset on the B.C. clock

Status: DONE (owner 2026-11-04, in 0.5.0) — 93b7cde1b (one clock, the sweep, the net, the sun card).

## Asks (owner 2026-11-04)
- T368 Q6, corrected: the app's "today" was the PHONE's date (not B.C.'s as first said). Owner: "no bc time makes sense
  i agree with that".
- "we should also add sunrise/set time somewhere using bc time for the location."

## Done (core)
`day.ts` `BC_ZONE` (America/Vancouver, the whole province), `bcNow()` (a Date whose local fields are B.C.'s wall clock,
for deciding the DAY only), `bcClockTime()`; `sun.ts` `sunTimes(lat, lon, y, m, d)` (sunrise equation, ±2 min vs the
NRC almanac at Vancouver's solstices) and `sunLine()` → "Sunrise 07:02 PST · sunset 16:41 PST". Tests: sun.test.ts.

## Left
1. Every day-deciding `new Date()` → `bcNow()` (~20: Shell/WaterScreen `dayRange`, `regsDay`, `mapDayOf`, startLink,
   useMapColouring, salmonColour, inseason, runTiming, horizonChips, conditions, SearchSheet, DayCalendar, weather) —
   never the elapsed-time ones (crash report, report send, observation ages). A net: no `new Date()` deciding a day.
2. Sunrise/sunset line: on a water's Regulations page under the day (the chosen day, at the water's point —
   `source.waterPoint`, the tapped point when there is one), in a card (map UI rule: no loose text). Tests.

## 2026-11-04: B.C. time is UTC−7 all year (owner: "isnt this wrong … they arent mountain time anymore?")
Checked: from 8 Mar 2026 B.C. keeps UTC−7 all year ("Pacific time"; 1 Nov 2026 has no change). The Peace region,
Fort Nelson and Creston were already UTC−7 all year, so now they match. The south-east (Cranbrook, Fernie, Golden,
Invermere, Kimberley …): sources conflict (the province's page: UTC−6 all year with Alberta; news: the East Kootenay
joins UTC−7, Golden keeps Mountain) — local choice, in flux.
- The tz databases in use are STALE: Node's 2026a and Python's zoneinfo here still put `America/Vancouver` at UTC−8
  on 1 Dec 2026. So the clock is written in core (`bcOffsetHours`: −7 from 8 Mar 2026, the old Pacific rules before),
  never read from the device. Label "PT".
- The regulations' day: one clock for the province (UTC−7). Only a south-east town on UTC−6 differs, 00:00–01:00 local.
- The River Forecast Centre's stamps (read as a fixed UTC−8 wall clock: T126, `forecast.py`, the worker's `time.ts`):
  RE-MEASURED 2026-11-04 on the staging feed (280 CLEVER stations, the first 40 hourly steps against the gauges' hourly
  readings): median error read as UTC−8 0.70 %, UTC−7 0.93 %, UTC−6 1.19 %, UTC 2.44 %. The Centre still stamps UTC−8
  (as on 2026-10-22, seven months after the province's change). Nothing to change; re-measure if the fit flips.
- The tz database WAS fixed: IANA tzdata 2026b (2026-04-22) moves America/Vancouver to UTC−7 permanently. It reaches a
  phone through its system updates (Android's time-zone module, iOS updates); this Mac's Node and Python still carry
  2026a. The app keeps its own rule (right whatever the device holds) and could later compare the device's offset
  with it, to learn how many phones are stale.
- Every day-deciding read takes `bcNow()` — ONE source; the net refuses `new Date()` deciding a day (owner: "a single
  source so it is consistent and has no possibility of drift").

## 2026-11-04: done — 93b7cde1b (owner: "unify this so that all clock sources are easier to find later")
- ONE CLOCK: `app/packages/core/src/clock.ts` is the only place app source reads the device clock: `now()` (epoch ms,
  an instant), `bcNow()` / `bcToday()` (the B.C. day; moved here from day.ts with `bcOffsetHours`, `bcClockTime`,
  `BC_TIME_LABEL`, `BC_PERMANENT_FROM`). Injected `now` parameters kept; their defaults come from here.
- The sweep (118 reads, 53 files): day-deciding reads → `bcNow()` (DayCalendar, SearchSheet, Shell/WaterScreen
  `dayRange`, `regsDay`, `mapDayOf`, startLink, place `linkSearch`, useMapColouring (lake + map day), salmonColour,
  inseason, runTiming, horizonChips, conditions day, search status day); every other read → `now()` (ages, timeouts,
  logs, report/crash stamps as `new Date(now())`, probes, fetch timing).
- The net: `app/tools/clock.test.ts` fails on `new Date()` (no args) or `Date.now()` outside clock.ts, with mutation
  pins; `performance.now()` frame timers are listed per file (openProbe, Map.native, web useChannels/useMapInstance,
  ui regs).
- Sun: `useSunLine` (@app/ui `sun.ts`: the frame's tapped point — an unnamed reach's own — else `source.waterPoint`;
  none, no line) → `SunCard` (ui-native `regs/SunCard.tsx`), the first small card of the Regulations page, on the
  chosen day ("Sunrise 08:00 PT · sunset 17:42 PT"). WaterModeHost passes `frame.at`. Tests: sun.render.test.tsx,
  SunCard.test.tsx, sun.test.ts (`bcToday`).
- LEFT ON THE DEVICE'S CALENDAR (their clock read is `now()`; their DAY is still `localDate`): a spot's visit day and
  its weather/day-flow days (spots/visits, dayFlow, capture, weather `today`), and a station chart's "today"
  (sql/shared, UTC). Moving those to the B.C. day changes stored visit dates (a picked local midnight read back
  through a B.C. clock lands a day early east of B.C.), so it needs its own decision.
