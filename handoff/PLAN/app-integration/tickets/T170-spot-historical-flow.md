# T170 — A spot gets the flow of its own day (historical), by refresh or on request

Status: OPEN

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
