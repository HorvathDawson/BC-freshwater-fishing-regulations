# T567 — Fraser at Mission: the app shows no current readings, the gauge page does

Status: OPEN.

Source: Owner 2026-11-09: "the fraser at mission gauge has no current readings (but the gauge page for it does so there is an issue there?)".

## Work
1. Find why the two disagree for 08MH024. Lead, not confirmed: `data/curated/gauges/tidal_stations.json` says Mission's HYDAT record reads "tidal effects", so the app may treat the station as tidal and withhold its reading while the public page prints it. Other leads from the data review (T550): a station ECCC answers 304 for is absent from that run's obs; a reading's age is not gated.
2. One rule for what a tidal / backwater gauge shows, used by the app and the page (same presenter).
3. Check the other lower-Fraser stations the same way (Whonock, Port Mann, Hope).

## Acceptance
App and gauge page show the same thing for 08MH024, with the reason said if the reading is withheld. A test with a Mission fixture.

Depends: —. Size: S. Owner: no.
