# T382 — The regulations' "today" is B.C.'s; sunrise and sunset on the B.C. clock

Status: OPEN (owner 2026-11-04, in 0.5.0). Core done (this ticket's first commit); wiring after T369 lands.

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
