# T528 — River Forecast Centre flood and hazard advisories in Conditions

Status: OPEN — later (not build 22); design first, the owner checks off a mockup before any code

Owner 2026-11-09: "add a ticket to somehow incorporate (https://bcrfc.env.gov.bc.ca/warnings/index.htm) river flood and
hazard advisorys into conditions not now but in a tricket will need to see how to incorporate it into it but should be
clean and will need at least one mockup that i check off on how it will be done please the feed worker getting this
data too."

## Source
BC River Forecast Centre (BC Ministry of Water, Land and Resource Stewardship) warnings page:
https://bcrfc.env.gov.bc.ca/warnings/index.htm. It lists the province's active advisories by basin or region, with their
level: High Streamflow Advisory, Flood Watch, Flood Warning, plus other hazard notices. They are issued and updated
irregularly, usually during freshet and storms. A map of the advisory areas and a bulletin per advisory are linked
from the page.

## What
1. **Research the feed first.** Find what the page offers that a machine can read: a list, RSS, a GeoJSON or ArcGIS
   layer behind the map, and the advisory area polygons or named basins. Record each advisory's fields (level, area,
   issued, updated, bulletin link, ended) and the licence and attribution terms. Scrape the HTML only if nothing better
   exists, and say so.
2. **Feeds worker.** The worker (app/workers/feeds) fetches the advisories on a schedule, as it does for DFO, and
   publishes a small feed: active advisories, each with level, area id, area name, issued and updated times, a link to
   the bulletin, and the time it was checked. The feed carries a checked-at time so staleness is judged the T526 way:
   an advisory list not checked for longer than the agreed limit is shown as out of date. A fetch that fails is
   reported and shown, never silently kept (no silent fallbacks).
3. **Placing it.** Map each advisory area to the waters and gauges it covers. Do this at build time in Python where
   it can be (area polygons → the registry and gauges; cascade resolved in Python). The app gets compact, pre-resolved
   results and holds no geometry logic.
4. **Conditions UI. One clean design, chosen from a mockup the owner checks off before any code.** Candidates for the
   mockup to show:
   - a banner on Conditions and on a water's or gauge's page when an advisory covers it ("Flood Watch — Lower Fraser,
     issued Nov 9"), tapping through to the bulletin;
   - the advisory areas shaded on the Conditions map, by level;
   - an entry in the alerts (T527): "tell me when an advisory starts on my waters".
   Wording comes from the source verbatim, never a computed claim the bulletin does not make.
5. Respect the map UI layout rule (top: search + day; legend in the bottom bar), self-host everything, and keep one
   load path per screen.

## Done when
The owner has checked off a mockup, the worker serves the feed with its checked time, the app shows advisories as
mocked, tests cover placement, staleness and a failed fetch, and phone shots are attached.
