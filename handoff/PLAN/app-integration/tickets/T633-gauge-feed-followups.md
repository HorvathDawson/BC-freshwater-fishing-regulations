# T633 — Gauge feed: follow-ups from T550, T557 and T585

Status: OPEN

Source: gauges lane report 2026-11-11.

## Work
1. The worker's FLOOD_WEIGHTS and other policy constants are generated from the Python spec, not hand-copied (rest of T557 item 2).
2. The app refuses a gauge feed file with no format tag, once both workers write `gauge-feed/1`.
3. The feed-contract test runs on the worker's own output.
4. The gauge card says which reading was ranked (`rankedAt`) and when a figure is kept; the "as of" line names release, environment and channel; a staged release is applied at cold start (T585 F12, with T555).
5. A warning when the feed's HYDAT differs from the release's.
6. The web keeps an offline week like the phone (T585 F14, web half).

## Owner
- Is 6 hours the right cutoff for a reading to count as current?
- Run the HYDAT workflow so the datum-shift cut reaches clim.json (45 stations in HYDAT, 21 live), then read the shift list.

Depends: T550, T557, T585. Size: M.

## Owner answers 2026-11-11
- 6 hours stays as the cutoff.
- Run the HYDAT workflow on staging, then send the datum-shift station list to read.
