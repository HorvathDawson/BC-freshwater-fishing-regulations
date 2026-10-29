# T273 — Panning feels laggy on Android: preload and cache around the view

Status: PARTIAL 2026-10-29 — measured (benchmark + split); the cause is the drawn basemap's tiles, fix needs an owner choice (owner 2026-10-29: "when panning around the app it feels a little bit laggy. maybe we could have better
preloading in the app and caching?").

## Work
1. Profile a scripted pan on the testbed emulator (frame times, JS thread, SQLite queue depth, tile fetches): split the lag
   between map rendering, per-viewport regs/colour queries (T264 serial queue), and tile/network fetches.
2. Fix by cause: cache per-section answers in memory (LRU, keyed by release + day), batch viewport queries into one
   statement, keep work off the JS thread during gestures (debounce until the pan settles), prefetch tiles/answers one
   screen beyond the view and for neighbouring regions (builds on T253).
3. Pin with a repeatable pan benchmark (numbers in the ticket, before/after).

## Acceptance
Benchmark shows fewer dropped frames and no visible colour pop-in on a normal pan; no regression on web.

Depends: T253, T264, T272 (same emulator lane). Size: M. Owner: no.

## Measured (2026-10-29)
Benchmark: `run.mjs pan` (test bed; `PAN_ROUNDS`, `PAN_SWIPES`, `PAN_GROUND=satellite`). Round 1 CIRCLES one place,
round 2 TRAVELS (east ×4, north ×4: new tiles every swipe). It reads three things: the MAP'S OWN SURFACE (MapLibre draws
into a SurfaceView, which `gfxinfo` cannot see: `dumpsys SurfaceFlinger --latency` per swipe → frame intervals while
moving), the UI thread (`gfxinfo`), and the new PAN PROBE (`@app/map native/panProbe.ts`, on in the test-bed build via
`EXPO_PUBLIC_PAN_PROBE=1`): JS-thread lag, tiles labelled, `setFeatureStates` round trips, map renders.

| run (emulator, 16 swipes) | map frames >50 ms | map p95 / p99 | JS lag max | label ack mean |
|---|---|---|---|---|
| drawn map, circling | 25 | 34 / 72 ms | 20 ms | 6 ms |
| drawn map, TRAVELLING | **89** | **87 / 167 ms** | 160 ms (2 ticks) | 5 ms |
| satellite ground, travelling | 12 | 33 / 51 ms | 21 ms | 10 ms |
| drawn map without the basemap's `landuse_park` layer (experiment, not committed), travelling | 10 | 33 / 50 ms | 24 ms | 1 ms |

Split: the JS thread is NOT the lag — no task over 100 ms in normal panning, one Map render per settle, the colouring's
feature-state lands in 1–10 ms, and there is no SQLite on the pan path (the map's colours come from the in-memory status
index; T264's queue serves screens, not the map). The hitches are MapLibre's native work on NEW basemap tiles: over
satellite (the basemap's 56 fill/line layers hidden) they mostly go, and so they do with only `landuse_park` removed —
Protomaps' green-space layer, which carries BC's forest polygons (`wood` is 62.6 % of its bytes, see runtime-style.ts).
One sample per variant on an emulator: indicative, not proof; a real phone should be measured with the same scenario.

So the ticket's suggested fixes (an LRU of section answers, batched viewport queries, debouncing JS) would not move
this. What would, needs the owner:
1. (no visual change intended) rebuild the basemap extract with the landuse/wood polygons simplified and merged at
   z8–12 (pipeline, human-run rebuild), so each new tile carries far fewer vertices; or
2. (visual change) draw `landuse_park`'s wood/forest only from a higher zoom, or from a coarser layer.
Not done: "prefetch one screen out" — MapLibre Native has no viewport-padding prefetch, and its default parent-zoom
prefetch (delta 4) is already on; the binding exposes no knob.
