# T420 — Panning the map feels laggy on web AND app (even a Pixel 9): streams load in slowly, then the pan stutters

Status: PARTIAL — the app's share FIXED 2026-11-06 (deb635819, "Fixed: tile ids once per watch"): stressed emulator z13 travelling flings p50 83.5 → 49.7 ms, map frames 309 → 419, tile events 958 → 37. Web (owner 11-06: ONLY mobile Chrome on the Pixel 9) capped at 2 device px per CSS px (2b891823f): proxy frame time −28 to −32 % at z10–z12. Left: MapLibre's own tile parsing on a slow CPU (~17 % of moving frames still lack a tile), a native pixel-ratio cap (binding patch), a phone check. Measured in simulation 2026-11-06 (c1c838f58): reproduced only on a CPU-starved emulator. Was: OPEN (owner 2026-11-05, on 0.6.1 from Play internal, Pixel 9: "the panning on the map still feels very laggy even on a pixel 9 phone … it is
like the streams load in slow then lag").

Two symptoms, maybe one cause: (1) the stream lines arrive late after a pan (tiles or their layers decoding/painting
slowly), and (2) the pan itself stutters once they arrive (frame drops while the new tiles are styled — main-thread or
render-thread work on tile load: paint-index lookups, feature-state, filter expressions, a re-render from React).

**On the web too** (owner 2026-11-05): the same lag on the web map, so the cause is likely shared — the tiles
(size, layer count, data-driven paint over stream features) or our style/JS — rather than MapLibre Native alone.
Measure both: the web with the Performance panel / MapLibre's `map.showTileBoundaries` + frame timing on the same pan
script, the phone as below.

**"Maybe it needs GPU acceleration"** (owner 2026-11-05): both maps already draw on the GPU (MapLibre GL JS = WebGL;
MapLibre Native = Vulkan or OpenGL ES on Android — the test bed builds an `-opengl` APK too). The usual cause of this
lag is CPU work beside the drawing: decoding and laying out tiles (worker threads), data-driven expressions over many
stream features, feature-state / paint-index updates, JS on the main thread. Still check: which Android backend the
Play build uses (Vulkan vs OpenGL ES, try both on the Pixel 9), and on the web that the browser runs WebGL with
hardware acceleration (chrome://gpu) — a software fallback would explain web lag on its own.

## Work
1. Measure on a real device class (the owner's Pixel 9 report; emulator numbers are not enough): a scripted pan over a
   dense area (Fraser Valley at z10–z12, then Coquitlam/Harrison at z13) with frame timing (`adb shell dumpsys gfxinfo
   <pkg> framestats`, MapLibre's render stats) and time from camera stop to streams drawn. Record before numbers.
2. Find where the time goes: tile fetch/decode (local pmtiles read), style evaluation (data-driven expressions over
   stream features, the paint index / feature-state per section), JS work on `onMoved`/region change (T417 added a
   bounds report on settle; check it costs nothing mid-pan), React re-renders of the map tree during a gesture.
3. Fix what the measurement names; after numbers on the same script. Target: no dropped-frame bursts during a pan,
   streams drawn within ~0.5 s of the camera stopping.
Related: T401 (streams pop in at the edges — the pan buffer), T360 (performance pass), T245 (no lighter z8: decided).
Size: M. Owner: a phone check after (and a screen recording of the lag would help).

## Measured in simulation (2026-11-06, c1c838f58 tooling; owner rule: emulator, not the phone)
Tools: `run.mjs pan` now takes `PAN_AT` (camera link), `PAN_MS` (120 = fling), `PAN_COLD` (page cache dropped, root) and
prints the pan probe's new `[drawn]` lines (MapLibre's own fully/partially rendered frame: frames MISSING TILES while the
camera moves, and camera-at-rest → first full frame). Stress = `TESTBED_EMU_ECORES=1 TESTBED_EMU_ARGS="-cores 2"` (the
emulator on the Mac's efficiency cores, 2 vCPU, 2 GB AVD). CPU split by `simpleperf -e cpu-clock` (root) during 12 flings.
Runs: `app/tools/testbed/out/runs/{base,stress,stress2,stress3,stress-notiles}/`.

| run (16 swipes/round, circling · travelling) | map frames >50 ms | map p50 / p99 ms | missing-tile frames while moving | rest → full p95 |
|---|---|---|---|---|
| normal, start view z~10 | 21 · 8 | 17 / 51 · 17 / 50 | 4.5 % · 4.9 % | 147 · 83 ms |
| normal, Fraser Valley z11 | 10 · 8 | 17 / 50 · 17 / 35 | 6.5 % · 4.7 % | 34 · 49 ms |
| normal, Coquitlam z13 fling | 4 · 8 | 17 / 34 · 17 / 50 | 5.9 % · 7.0 % | 0 · 0 ms |
| **stressed**, Fraser Valley z11, cold page cache | 88 · 242 | 51 / 238 · 70 / 211 | 9.3 % · 5.1 % | 379 · 588 ms (3 moves never full in 8 s) |
| **stressed**, Coquitlam z13 fling | 215 · 268 | 78 / 233 · 87 / 213 | **25.7 % · 30.4 %** | 0 · 176 ms |
| stressed, Coquitlam z13 fling, NO tile events (experiment build) | 210 · 260 | 66 / 183 · **54 / 151** | 22.2 % · **20.8 %** | 124 · 0 ms |
| stressed, 160 flings z11 (pan-long) | 485 · 422 · 536 · 431 | 76–101 / 205–234 | 17–36 % | ≤ 471 ms |

- **Reproduced only under stress.** On the normal emulator the map runs at 60 fps (p50 16.7 ms), streams are fully drawn
  ≤ 150 ms after the camera stops, ~5 % of moving frames lack a tile; JS lag ≤ 27 ms. With 2 slow cores: 10–18 fps while
  panning, 20–36 % of moving frames lack tiles (= T401's pop-in, DURING the glide), JS lag up to 1.8 s on the cold start.
- **Where the time goes (stressed, Coquitlam z13 flings):** JS thread `mqt_v_js` 27 % of samples, MapLibre's render thread
  25 % (+ 9 + 7 % in two more render threads), map workers ~9 %. At Fraser Valley z11 the render thread is 77 % — and a third
  of it is `goldfish_pipe writel`, the emulator's own GPU transport (no such cost on a phone), so the render share is an
  emulator ceiling, not a phone number.
- **The app's lever that measured:** the colouring's per-tile pipeline (patched binding's `MLRNTileIds`: every tile REQUEST,
  re-requests of tiles already held included, crosses to JS with all its ids, then `TileHold.put` + `Colouring.label`). It
  fires ~16–19 times a second even when circling one place (labels 0: all redundant), and ~140/s late in the long run.
  Switching it off (experiment build `EXPO_PUBLIC_TB_NO_TILE_EVENTS=1`, colours then missing) on the same stressed pan:
  travelling map frames 286 → 406 (+42 %), p50 87 → 54 ms, missing-tile frames 30 → 21 %; JS share 27 → 21 %. So it is a real
  but partial cost; the rest is MapLibre's own tile work on a slow CPU.
- **Web** (headless Chrome, hardware GL — ANGLE Metal — phone viewport, 12 drags on staging.canifishthis.ca; scratch
  driver): 60 fps (p99 16.8 ms) at 1×, 4× and 6× CPU throttle with 4G; at most one frame > 50 ms; last tile ≤ 360 ms after
  release on 4G. NOT reproduced on the web; a software-GL browser or a slower GPU was not tried.
- **Not fixed.** Proposed fix (needs a native patch, both platforms' JS): the binding sends a tile's ids ONCE per watch (a
  repeat request sends only `{tag,z,x,y}` so JS refreshes its hold; JS asks for the ids again only for a tile it has dropped),
  then the same stressed run before/after. Also open: the probe build itself adds JS timers (16 ms stall log, 50 ms lag tick)
  and per-frame events, so a probe-off build should be timed with SurfaceFlinger only; a real low-end phone (or the owner's
  Pixel 9 recording) is still the only proof of what the reader sees.


## Fixed: tile ids once per watch (2026-11-06, deb635819)
Two costs, one pipe:
1. **Re-watch on every render (the big one).** `Map.native.tsx` keyed the attach effect on `useNativeColouring`'s result
   OBJECT (new every render), so every camera settle (one render per swipe) called `watchTiles` again — and each watch
   clears the binding's id cache and REPLAYS every tile it has requested, with all their ids, as `MLRNTileIds` events.
   That was the ~16–28 "tile" events a second while circling one place, every one labelling nothing. Keyed on the stable
   `attached` callback now; a watch still follows the first attach and each style load (`gen`).
2. **Repeat requests.** The binding (Android `MLRNVectorSource.kt`, iOS `MLRNVectorSource.m`) keeps an LRU of the tiles
   whose ids it sent in this watch (256); a repeat request sends only `{tag, z, x, y}`; JS's `TileHold` (512 = 2×, enforced)
   moves the tile to the end, so both LRUs see the same order and a repeat always finds its tile
   (`tile-hold.test.ts` drives 50,000 random requests through both). A repeat JS does not hold is REPORTED and answered
   by a re-watch (never silent); right after a hold clear (new archive, re-watch) a miss is expected and not reported.
   iOS half edited, NOT compiled or run (no iOS build in this pass).

Same scripts, same stressed emulator (`TESTBED_EMU_ECORES=1 TESTBED_EMU_ARGS="-cores 2"`, `PAN_MS=120`, 16 swipes/round;
runs `app/tools/testbed/out/runs/t420-{before,after2}-{coq13,fv11}`; before = the 09:00 test-bed APK, HEAD 48efaa5fb):

| run (circling · travelling) | map frames | >50 ms | map p50 / p95 / p99 ms | tile events (+repeats) | missing-tile frames moving | JS lag max |
|---|---|---|---|---|---|---|
| Coquitlam z13, before | 222 · 309 | 177 · 264 | 88.7/187/235 · 83.5/162/194 | 470 · 958 | 26.4 % · 22.6 % | 108 · 181 ms |
| Coquitlam z13, after | **356 · 419** | 161 · 196 | **49.5/113/183 · 49.7/99/131** | **9 (+38) · 37 (+35)** | **16.1 % · 17.6 %** | 85 · 89 ms |
| Fraser Valley z11, before | 310 · 347 | 221 · 248 | 61.7/117/165 · 65/150/185 | 271 · 600 | 9.9 % · 22.6 % | 101 · 104 ms |
| Fraser Valley z11, after | **389 · 391** | 148 · 235 | **47.2/99/137 · 51/118/136** | **11 (+36) · 45 (+44)** | 14.9 % · 19.4 % | 60 · 114 ms |

Streams at rest: rest → full p95 ≤ 206 ms after (≤ 210 before); "never full within 8 s" 3 → 0 on the z13 travelling run.
Colours checked on the after screenshots (base/own rules drawn, no grey streams). An intermediate build with only the
binding dedupe (`t420-after-*`) moved nothing measurable — the re-watch dominated. Still ~50 ms p50 on 2 slow cores: that
is MapLibre's own tile parse/render (the emulator's GPU transport included), not the app's JS.

## Web (2026-11-06, staging.canifishthis.ca, headless Chromium, 12 flings at the default view zoomed in)
| config | fps | p50 / p99 ms | frames >50 ms | long tasks |
|---|---|---|---|---|
| desktop 1440×900 @2, hardware GL (ANGLE Metal), 1× / 4× CPU | 117 / 117 | 8.3 / 9.3 · 8.3 / 16.7 | 1 · 0 | 0 |
| same, 6× CPU | 76 | 9.2 / 49 | 7 | 1 (73 ms) |
| phone 390×844 @3, hardware GL, 6× CPU | 117 | 8.3 / 16.6 | 1 | 1 |
| desktop @2, SOFTWARE GL (SwiftShader), 1× / 4× CPU | 14 / 13 | 91 / 200 · 92 / 200 | 138 · 119 | 0 · 1 |
| desktop @1, software GL | 49 | 24.6 / 33 | 1 | 0 |
| phone @3, software GL | 49 | 8.5 / 50 | 4 | 0 |
The web pays no per-tile app cost that shows: at 6× CPU the main thread's profile is MapLibre's draw loop (useProgram,
uniforms, drawElements); the app's colouring is not in the top 25. The web lags only when the GPU is weak: with software
GL the main thread is 85 % IDLE and frames take ~90 ms — GPU-bound, and fill-heavy (device pixel ratio 2 → 1 takes it
from 14 to 49 fps). So the owner's web lag most likely means their browser draws without hardware acceleration (or on a
weak GPU): `chrome://gpu` on that machine says which. Levers if so: cap MapLibre's `pixelRatio` (e.g. 1 when the WebGL
renderer is SwiftShader/llvmpipe, reported, not silent) and less overdraw/geometry (T419's simplification). Not built.

## Mobile web: the pixel-ratio cap (2026-11-06, 2b891823f)
Owner 11-06: the web lag is ONLY mobile Chrome on the Pixel 9; desktop is fine. Chrome there runs at devicePixelRatio
~2.6, so the canvas is ~1080×2260 device pixels on a phone GPU.

**Proxy, and what it can show.** Headless Chromium, Pixel 9 Chrome viewport (412×915 @ 2.625, mobile, touch), the dev web
build on local data (`globalThis.__map`), 4 programmatic pans (600 ms, linear) round the Fraser Valley, rAF frame
intervals while moving, 2–3 reps. GL by SwiftShader: a GPU that is slow at FILL, so it shows how frame time follows
pixel count and per-layer draw work. It CANNOT show a Mali's own behaviour: tile-based rendering (overdraw of opaque
fills is cheaper there, blending costs bandwidth), its driver, thermal throttling, or Chrome's Android compositor. The
emulator's Chrome was not used: its GL is the Mac's GPU passed through, which is the hardware-GL case (120 fps at any
ratio, 8.4 ms). Script: `.../scratchpad/layercost.py` (session scratchpad; not kept).

| zoom | uncapped (2.625: canvas 1081×2260) mean / p95 ms, frames | capped (2: canvas 824×1722) | change |
|---|---|---|---|
| z10 | 39.2 / 42.2, 188 | **28.2 / 33.6, 257** | −28 % |
| z11 | 37.8 / 42.3, 193 | **26.3 / 33.4, 276** | −30 % |
| z12 | 33.5 / 41.7, 220 | **22.7 / 25.3, 320** | −32 % |
Ratio sweep (same runs before the cap): 2.625 / 2 / 1.5 / 1 → z10 38.8 / 27.1 / 22.2 / 18.6 ms, z11 41.8 / 25.0 / 21.2 / 16.6,
z12 34.0 / 23.4 / 22.3 / 13.1. Most of the gain is at 2; lower costs sharpness for less. The rule (`web/pixelRatio.ts`):
`min(devicePixelRatio, 2)` for every browser, followed when the ratio changes; desktops (1–2) are untouched.

**Per layer (z10–z12, one layer off at a time, then groups at z11, 3 reps).** Nothing in the style moved frame time beyond
the noise (±3–4 ms; layers that draw nothing at that view also read −3 ms): all 7 stream line layers off, the `stream`
layer alone, the stream overlays (gear/no-access/closed/runs), the atlas edges, the area fills (region / mu / sign_zone /
watershed / outside), parks and access fills, all 26 symbol layers, the basemap lines — each within the noise. Only the
WHOLE basemap (≈ 40 layers) read −9 ms at z11/z12. So no style change was made: no merged casings, no low-zoom drop. Round
`line-join` (T419's suggestion against kinks) ADDS triangles at every vertex; it is a look decision, not a lag fix — not done.
`parcel` (T419: ~113k of 132k vertices at z11) is `visibility: none` by default: GL JS and MapLibre Native build no bucket
for a hidden layer, so it costs tile bytes and transfer, not drawing; it was not measurable here.

**Per zoom, for T419:** the frame cost on this proxy is ~the same at z10, z11 and z12 (33–42 ms uncapped) and follows the
pixel count; stream geometry was not a measurable share at these zooms. Fewer vertices still lighten tile parse on a slow
CPU (the native emulator's remaining cost), which this web proxy does not stress.

**Native (same GPU on the Pixel 9).** MapLibre Native draws at the device density (2.625); the RN binding exposes no pixel
ratio, so the same cap natively needs a patch (`MapLibreMapOptions.pixelRatio` on Android). Not built. The style JSON is
shared, and was not changed.

## Zoomed out (z5–z9): the basemap's fill outlines (2026-11-06, owner approved)
Owner: zoomed in "way better", zoomed out still laggy (suspected parks/polygons). Same SwiftShader Pixel 9 proxy, one
change at a time: `fill-antialias` off on the basemap's fills saves 14–15 % of the frame at z7/z9 (on our own fills:
0–1 %); skipping `landuse_park` saves 11–17 % but removes parks (not proposed); making it opaque saves ~0. Picture:
≤ 1.33 % of pixels move (>24/255) over 9 places z5–z11, under T280's 2 % gate. Owner saw the side-by-sides and chose
to ship it (`runtime-style.ts`, both maps). Stressed-emulator runs (`runs/t420low-*`) were too noisy to show it: the
emulator's GPU is the Mac's. Proof stays the owner's phone.

## Zoomed out, z5–z9 (2026-11-06 late, a893b06fe; owner: "still a bit laggy when zoomed out … parks and other polygons?")
Release r-67b456b5e64b (T419 smoothed STREAMS only; polygons untouched).

**Tile volume (release tiles, Lower Mainland bbox −123.3…−121.3, 49.0…49.8; one z7 tile, 8 at z9).** The basemap carries
3–10× the atlas's vertices zoomed out, and its `landuse` (wood/forest/park greens) is most of it:
| zoom | atlas KiB / top layers (vertices) | basemap KiB / landuse · water · roads (vertices) |
|---|---|---|
| z5 (1 tile) | 17 · basin 2.2k, region 0.8k, stream 0.6k | 31 · 6.5k · 0.4k · 0.9k (landcover 3.3k) |
| z7 (1) | 20 · basin 1.1k, mu 1.0k, stream 0.9k, **park 0.5k** | 83 · **15.1k** · 3.9k · 2.7k |
| z9 (8) | 67 · stream 2.6k, tidal 1.8k, mu 1.6k, **park 1.2k** | 283 · **44.6k** · 25.3k · 20.2k |
Province z6 (17 tiles): atlas 29k vertices (basin 16k, park 3.9k); basemap 212k (landuse 76k, water 70k, earth 32k).
`parcel` (T419's ~113k vertices at z11) is `visibility: none`: no bucket is built for a hidden layer, so it draws nothing.

**Draw work per layer (dev web, Pixel 9 viewport, z7 Lower Mainland; deterministic: draws, triangles, share of the screen
each layer paints alone).** 103 layers, 231 draw calls, 95.5k triangles, 5.4 screens of pixels: backgrounds 2.35 (opaque:
cheap, depth-rejected), basemap land fills 1.38 screens / 31k triangles (`landuse_park` alone 0.53 / 25.8k), admin fills
(region + mu 4 % tints) 1.33 / 2.3k, basemap water 0.18 / 12k, **our parks 0.10 / 2.4k**, streams 0.03 / 13k.

**Cost, interleaved A/B of forced full frames (SwiftShader; machine load cancels; % of the frame saved):**
| candidate | z5 prov. | z6 | z7 | z8 | z9 |
|---|---|---|---|---|---|
| `fill-antialias` off on every fill | 13 | 15 | 22 | 21 | 13 |
| … on the basemap's fills only | — | — | **15** | — | **14** |
| … on our fills that have an edge layer | — | — | 0 | — | 1 |
| basemap `landuse_park` not drawn | 2 | −1 | 16 | 17 | 14 |
| `landuse_park` made opaque (no blending) | — | — | −2 | — | 3 |
| region + mu 4 % tints not drawn | 7 | 5 | 4 | 7 | 6 |
| our park fill not drawn | 5 | 2 | 1 | −1 | 1 |
| outside mask not drawn | 4 | 3 | 1 | 1 | 0 |
| every fill not drawn (upper bound) | 32 | 39 | 37 | 40 | 32 |
So the owner's suspicion is half right: POLYGONS are the zoomed-out cost (up to ~40 % of a frame), but it is the BASEMAP's
green fills — their geometry (opaque vs blended made no difference) and their antialias outline pass — not our parks
(≤ 1–5 %) nor the region/MU tints (4–7 %).

**Fixed (a893b06fe, style, both platforms):** the basemap's fills draw no antialias outline (`runtime-style.ts`). Looks:
≤ 1.33 % of pixels move (>24/255) over 9 places z5–z11, under the basemap's own 2 % render gate (T280). Same pan script
(4 pans, Pixel 9 viewport, SwiftShader, interleaved A/B ×3): z5 22.6 → 20.4 ms mean frame (332 → 369 frames), z7 26.6 →
23.1 (280 → 320), z9 30.1 → 27.0 (243 → 272).

**Native emulator (stressed, `pan` PAN_AT −122.5,49.3 z7 / z9 and −125,54 z5; runs `t420low-{before,after}-{lm7,lm9,bc5}`):**
INCONCLUSIVE. Before vs after moved both ways by more than any plausible effect (z7 circling p50 83 → 153 ms, travelling
33 → 24; z9 travelling 102 → 135; z5 travelling 18.8 → 19.5): the 2-E-core emulator's own noise, and its GPU is the Mac's
through `goldfish_pipe`, so a Mali's fill and geometry cost cannot show there. The style change applies to native too.
Seen on the way: low zoom moves are heavy on native even without our JS (z9 travelling: 38–58 % of moving frames lack a
tile; JS lag ≤ 250 ms; map renders 16 per round after the earlier fix).

**Next (not done; proposals change what shows — owner's call):**
1. **Tiles:** simplify basemap `landuse` harder at z ≤ 9 (`basemap_grid.SIMPLIFY`, e.g. 4.0 below z10). It was tuned
   against the 2 % render gate in T280 and 4.0 measured 3.3 % worst shot there, so it is a look change → owner. A basemap
   tile build through heavy.sh, then `app/tools/basemap-shots.mjs`; not started.
2. **Proposal:** start `landuse_park`'s fade later (it ramps 0 → 1 over z6–z11; drawing it from z8 removes 16 % of a z7
   frame, but the green ground disappears at z7).
3. **Proposal:** drop the region and MU 4 % tints (keep their outlines and taps): 4–7 % of a low-zoom frame.
4. Native pixel-ratio cap (T448) — the same GPU on the Pixel 9.
Scripts (session scratchpad, not kept): `tilestats.py`, `drawwork.py`, `abcost.py`, `aadiff.py`, `layercost.py --aab`.
