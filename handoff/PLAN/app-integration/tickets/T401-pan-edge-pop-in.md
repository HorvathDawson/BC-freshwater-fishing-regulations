# T401 — Streams pop in at the screen edges while panning (phone)

Status: PARTIAL — 2026-11-06 option 2 built and measured (29b1e561b): the binding patch exposes `prefetchZoomDelta`; on the stressed emulator deltas 2 and 3 moved neither the moving frames missing tiles nor frame time beyond run-to-run noise (3 runs each of 4 vs 2), so the app keeps MapLibre's default (4). Left: what the owner sees on the phone (a delta-2 A/B APK if wanted), option 3 (render margin). 2026-11-06 the T420 tile-event fix (deb635819) cut the stressed emulator's moving frames missing tiles 22.6 → 17.6 % (z13 travelling) and 26.4 → 16.1 % (circling); still above the normal emulator's ~5 %, so option 2 (`setPrefetchZoomDelta`) is next if the owner still sees it. Earlier: 2026-11-06 reproduced in simulation DURING the glide (stressed emulator: 20–36 % of moving frames lack tiles, 0 % at rest), measured with the new `[drawn]` probe (c1c838f58); not fixed (see the end). Was: OPEN (owner 2026-11-04, 0.5.0 Play internal: "panning doesn't load enough of a buffer of streams outside the
view").

**What the code says:** nothing in the app can widen the area loaded. MapLibre Native loads only the tiles that cover
the viewport; its only prefetch is parent tiles (`prefetchZoomDelta`, default 4), and the binding exposes no knob
(T273's finding). The parent tile drawn while a new one loads holds only the bigger streams (`MAGNITUDE_LADDER`: a
magnitude-5 creek starts at z12), so when the real tile lands the small streams appear — the pop. The atlas has no
tippecanoe `--buffer` set (default 5/256): the buffer clips lines at tile edges, it does not load neighbours, so a
bigger buffer would NOT fix this — no tiles rebuild needed for it.

**Options (owner pick after measuring):**
1. Measure tile arrival on the emulator (`run.mjs pan`, pan probe): time from tile request (`MLRNTileIds`) to drawn.
   If the local archive read/parse is slow, the z13 tiles' size is the lever (rides with the T389 rebuild).
2. Patch the binding to expose `setPrefetchZoomDelta` (smaller delta = parent placeholders closer to the real tile,
   with more streams) — small native patch in `patches/`.
3. Render margin: the native map view drawn larger than the screen (tiles beyond the edge load early) — costs GPU/memory.

Depends: T312 (map feel). Size: S–M. Owner: choose after step 1.

## Measured 2026-11-05 on the emulator (test bed `edge`, out/runs/r1105-*)
After each swipe: screenshots back to back; the camera counts as at rest when two shots match; then the share of the
newly revealed strip still unlike the settled frame.

| run | camera at rest | strip missing at rest |
|---|---|---|
| slow drag (900 ms), river zoom (Coquitlam River) | ~650 ms | 0 % (8/8) |
| fling (120 ms), river zoom | ~1.1 s | 0 % (8/8) |
| fling, close in (Hoy Creek) | ~1.0 s | 0 % (8/8) |

On the emulator nothing pops in once the map stops: the edge is drawn by the time the glide ends (the data is on the
phone, so no network wait). A first attempt read "25–66 % missing" — that was the frame taken mid-glide (offset, not
missing). Not reproduced, so no fix chosen. What could differ on the owner's phone: a slower GPU/CPU decoding tiles
during the glide, or the pop-in is seen DURING the glide (this measures at rest). Next: a screen recording from the
owner's phone of one pan where it happens (which area, zoom, mode), then measure on that.

## Measured DURING the glide (2026-11-06, c1c838f58; numbers and runs in T420 "Measured in simulation")
The pan probe now reads MapLibre's own word per frame (fully vs partially rendered), so pop-in is counted while the camera
moves, not only at rest. Normal emulator: ~5 % of moving frames lack a tile (2.3–7 %), the edge strip 0 % missing at rest,
camera at rest 630–674 ms. **Stressed** emulator (2 efficiency cores): **20–36 % of moving frames lack tiles** on z13/z11
flings, the edge still 0 % missing once at rest (camera at rest 1.1–1.7 s). So the pop-in reproduces in simulation, and it is
DURING the glide on a slow CPU — the at-rest measure of 11-05 could not see it. Switching the colouring's per-tile events
off cut it 30 → 21 % (travelling); the rest is MapLibre parsing new tiles behind the glide. Not fixed: the T420 tile-event
fix first (measured lever), then option 2 (`setPrefetchZoomDelta`) if the parent placeholders are still too bare.

## After the T420 fix (2026-11-06, deb635819; table in T420 "Fixed")
Frames missing tiles while moving, stressed emulator: Coquitlam z13 26.4 → 16.1 % (circling), 22.6 → 17.6 % (travelling);
Fraser Valley z11 9.9 → 14.9 %, 22.6 → 19.4 % (run-to-run noise is a few points); "never full within 8 s of rest" 3 → 0
on the z13 travelling run. The pop-in left is MapLibre parsing new tiles behind the glide on 2 slow cores.

## Option 2: prefetch zoom delta, measured (2026-11-06, 29b1e561b)
**Built.** The binding patch's new `prefetchZoomDelta` prop (Android, `MLRNMapView.applyPrefetchZoomDelta` →
`MapLibreMap.setPrefetchZoomDelta`, android-v13.6.1; −1 = MapLibre's own 4) is set on map ready and on change.
`Map.native.tsx` passes `PREFETCH_ZOOM_DELTA = -1` (unchanged behaviour). iOS: `MLNMapView` has only `prefetchesTiles`
(on/off), no delta, so it ignores the prop. MapLibre also has a PER-SOURCE delta (`Source.setPrefetchZoomDelta`), not
exposed: a delta for the streams source alone is the narrower experiment if this is reopened.

**What the probe can and cannot show.** MapLibre's "fully rendered" frame (`TilePyramid::isLoaded`, android-v13.6.1)
waits on EVERY tile it holds, the prefetched parents included. So a smaller delta (more, closer parents to load) can only
add to "frames missing tiles"; it cannot credit the richer placeholder, which is the visual gain option 2 is for. A
per-frame look at the placeholder was tried (screenrecord during the pan): on the 2-core emulator the recorder itself
made the app stall ("isn't responding") and turned flings into taps (a water's page opened) — invalid, discarded.

**Numbers** (stressed emulator `TESTBED_EMU_ECORES=1 TESTBED_EMU_ARGS="-cores 2"`, `PAN_MS=120`, 16 flings/round; same
tree, the delta via a test-bed build env; runs `app/tools/testbed/out/runs/t401-{d4,d4b,d4c,d2,d2b,d2c,d3}-{coq13,fv11}`;
mean of 3 runs with the range):
| run (circling · travelling) | delta | frames missing tiles while moving | map p50 ms | map frames |
|---|---|---|---|---|
| Coquitlam z13 | 4 (default) | 21.3 (17.6–27.9) · 19.2 (12–24.4) % | 73.6 (60–94) · 69.6 (41–85) | 273 · 381 |
| Coquitlam z13 | 2 | 17.9 (9.5–25) · 20.0 (15.9–22.8) % | 58.5 (41–74) · 60.2 (49–67) | 318 · 393 |
| Coquitlam z13 | 3 (1 run) | 35.5 · 29.9 % | 95.7 · 88.2 | 141 · 304 |
| Fraser Valley z11 | 4 (default) | 15.2 (10.7–20.9) · 26.4 (21.1–31.4) % | 55.8 (49–69) · 66.1 (52–83) | 349 · 339 |
| Fraser Valley z11 | 2 | 11.0 (7.5–15.3) · 21.7 (19.1–23.2) % | 49.5 (34–66) · 59.8 (50–66) | 418 · 380 |
| Fraser Valley z11 | 3 (1 run) | 14.1 · 29.7 % | 67.1 · 79.9 | 285 · 314 |

The same APK varies by up to ±10 points (missing) and ±35 ms (p50) between sessions (the Mac's own load moves the
efficiency cores; the delta-3 run and a delta-4 repeat fell in a slow spell). Delta 2's means are a few points better in
3 of 4 rows, all inside the ranges: no measurable gain, and no measurable cost (the extra z11 parents did not raise frame
time). Not shipped. If the owner still sees bare streams during the glide on the Pixel 9, a delta-2 APK side by side is
the honest check (the emulator cannot judge placeholder richness).
