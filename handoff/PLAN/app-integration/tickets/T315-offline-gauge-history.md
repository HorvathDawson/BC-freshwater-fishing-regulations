# T315 — Offline: keep the last week of gauge readings on the phone

Status: OPEN (owner 2026-10-31: "in offline mode we didn't get the gauge feed data … the phone didn't store the gauge data
for the last week … had no measured line").

## Work
When online, the app caches each viewed (and saved-spot / followed) gauge's recent readings (≥7 days of the feed series +
latest + forecast) on the device, refreshed with the feed; offline, Conditions charts draw the measured line from that
cache with an "as of <time>" note instead of an empty chart. Optionally prefetch the region's gauges at idle on Wi-Fi
(respect the T298 mobile-data setting). Bounded storage (LRU, a few MB). Emulator: view a gauge online, go airplane, the
chart still shows the measured line.

Depends: T254, T256. Size: S–M. Owner: no.

- Owner 2026-10-31 extension: store ALL gauges' observed values on the phone as each feed update arrives (a small delta per update), building a rolling on-device history (≥7 days) for every station — not only viewed ones — separate from the stored HYDAT history.
