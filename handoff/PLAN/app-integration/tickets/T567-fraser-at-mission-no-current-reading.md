# T567 — Fraser at Mission: the app shows no current readings, the gauge page does

Status: IN REVIEW — fb042cdfa (code + tests; reaches the phone with the next build; owner to confirm there)

Source: Owner 2026-11-09: "the fraser at mission gauge has no current readings (but the gauge page for it does so there is an issue there?)".

## Work
1. Find why the two disagree for 08MH024. Lead, not confirmed: `data/curated/gauges/tidal_stations.json` says Mission's HYDAT record reads "tidal effects", so the app may treat the station as tidal and withhold its reading while the public page prints it. Other leads from the data review (T550): a station ECCC answers 304 for is absent from that run's obs; a reading's age is not gated.
2. One rule for what a tidal / backwater gauge shows, used by the app and the page (same presenter).
3. Check the other lower-Fraser stations the same way (Whonock, Port Mann, Hope).

## Acceptance
App and gauge page show the same thing for 08MH024, with the reason said if the reading is withheld. A test with a Mission fixture.

Depends: —. Size: S. Owner: no.

## Found and fixed 2026-11-09 (fb042cdfa)
- Cause: 08MH024 reports a LEVEL and no flow (staging feed 2026-11-10: `parameter: level`, `now.discharge: null`). The
  app asks one quantity for the whole map, Flow by default, so under Flow the station had nothing to rank: its row said
  "Reporting, but its reading cannot be ranked today" and its water "None of the gauges … has a reading today". Under
  Level it read fine. The public page prints whichever quantity the station has. 61 stations are level only.
- NOT the tide: the tidal flag only adds a sentence under Level. That lead is closed.
- Fix: new donor state `other-quantity` (core `donorState`, data `otherOnly`) and refusals `level-only` / `flow-only`.
  The row, the map dot and the sheet now say "Reports water level only today, not flow. Show Level to see it." A level
  still never stands in for a flow. Test: `livePanel.test.ts` (Mission's feed row), `one-donor-state.test.ts`.
- Left: the alert page's "No reading now" for such a station (goes with T563); the gauge page itself (T564).
- Seen on the way, for T550: staging `obs.json` had 0 stations on the 03:35Z run (it lists only stations written that run).
