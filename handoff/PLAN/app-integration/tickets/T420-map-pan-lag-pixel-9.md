# T420 — Panning the map feels laggy on web AND app (even a Pixel 9): streams load in slowly, then the pan stutters

Status: PARTIAL — the app's share FIXED 2026-11-06 (deb635819, "Fixed: tile ids once per watch"): stressed emulator z13 travelling flings p50 83.5 → 49.7 ms, map frames 309 → 419, tile events 958 → 37. Left: MapLibre's own tile parsing on a slow CPU (~17 % of moving frames still lack a tile), a phone check, and the web only lags on a software/weak GPU (see "Web"). Measured in simulation 2026-11-06 (c1c838f58): reproduced only on a CPU-starved emulator. Was: OPEN (owner 2026-11-05, on 0.6.1 from Play internal, Pixel 9: "the panning on the map still feels very laggy even on a pixel 9 phone … it is
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
