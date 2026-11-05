# T312 — Map panning feels slow: calibrate gesture speed and fling

Status: DONE 2026-11-05 — d91f300f6 (owner feel check on the next APK: OWNER-TODO phone checks)
for gestures").

## Work
1. Measure on the emulator: finger travel vs map travel (should be 1:1 under the finger), fling distance/deceleration vs
   Google Maps-like feel, and whether JS work during a gesture (onRegionIsChanging handlers, colouring, prefetch) drops
   frames and makes it feel heavy.
2. Fix: no JS work on every camera tick during gestures (throttle / only on idle); native MapLibre gesture settings for
   fling/deceleration (maplibre-react-native props or the Android MapView gesture settings via a config plugin if needed);
   make the map 1:1 with the finger and a natural fling. Same on web (maplibre-gl dragPan inertia options).
3. Before/after numbers (frame times during a scripted swipe, fling distance) and the owner's phone feel check.

Depends: T273, T280. Size: S–M. Owner: feel check on the phone.

## Audit 2026-10-31 (code only; the emulator measurement waits for the heavy lock, held by another lane)
- Native (`Map.native.tsx`): NO per-camera-tick JS. `onRegionIsChanging` is never bound; `onRegionDidChange` runs once at rest
  (scale, marks shift, stale flags); `onRegionWillChange` is bound only while the location dot is on or the open probe is
  in (one call per gesture start). So a native pan's JS cost is already zero; "slow" is the native gesture/render side:
  the MapLibre props in use are only `dragPan/touchZoom/doubleTapZoom/...` (`gestures.ts nativeMapGestures`) - the 11.5
  binding exposes no fling or deceleration prop, so fling tuning needs a config plugin / native patch (patches/@maplibre__...patch).
- Web: JS on every `move`: `useSpots.tsx` `regroup` (spotMarkers + JSON.stringify of the clusters, then a transform per pin;
  clusters only change with zoom, so only the pin placement needs to run per move) and `markers.ts` `inside` (a style write
  per mark). `useMe` sizes on `zoom` only. Fix not made here: `useSpots.tsx` is the spots lane's file (T307/T308).
- Next: run `node app/tools/testbed/run.mjs pan` (T273 scenario) under scripts/heavy.sh for the before numbers, then try
  the native fling tuning and re-measure.

- 2026-10-31: web `useSpots` now places pins on `move` and regroups (clusters + signature) only on data change and `zoomend`. Native fling tuning and the pan measurement wait for the emulator (owner: `sudo xcodebuild -runFirstLaunch` first).

## Folded in (owner 2026-11-03)
- T353: the fling is still too weak on 0.4.0 (owner 2026-11-02).
- T273: pan lag measured (benchmark + split); the cause is the drawn basemap's tiles and the fix needs an owner choice
  (see T273's file).
One gesture pass on the emulator covers all three; T317 (overlay once-over) follows. In 0.5.0 (owner 2026-11-03),
batch 5 of RELEASE-PLAN-0.5.0.md.

- 2026-11-03: T273's basemap choice is already answered: option 1 (forest simplified, still drawn at low zooms) shipped in T280 (65894d8bf, basemap-b515359fc62f, −52 % green vertices). No owner choice left; batch 5 measures pan on top of it (real phone: T245).
- 2026-11-04: "before" pan run on 0.5.0 code: app/tools/testbed/out/b5-before (T273-pan-1/2.txt). The gestures scenario's
  pinch is not driven on this emulator image (`sendevent: /dev/input/event1: Permission denied` — needs `adb root`).
- 2026-11-04 fling: MapLibre Android 13.6.1 `MapGestureDetector.onFling` (javap): a fling needs hypot(v)/pixelRatio ≥
  `flingThreshold` (default 1000), glides `v/7/tilt + flingAnimationBaseTime` ms (default 150) and moves
  `v × time × 0.28 / 1000` — distance grows with the base time. Patched (MLRNMapView.onMapReady, the maplibre patch):
  base 450 ms, threshold 500. ~+70 % distance at 2000 dp/s; gentler swipes fling. After-numbers on the next emulator
  sitting (testbed `pan`, a scripted fast swipe's travel), feel on the owner's phone.

## Measured + fixed (2026-11-05, test bed `feel` + `perf`, runs t312-a / t312-b / t312-c, emulator n1_pixel)
- **1:1 tracking:** a slow 400 px drag (1500 ms, no fling) moves the map 147 of the finger's 152 dp (×0.97, three drags):
  the 5 dp are MapLibre's touch slop before a move starts; after it the map is exactly under the finger. Nothing to fix.
- **No JS per camera tick:** confirmed (no `onRegionIsChanging`; `[cam]` is one line per settle).
- **Fling:** MapLibre's glide is `v × (v/10.5 + base) × 0.28` ms·dp — at 2000–5000 dp/s it stops at 1/2–1/2.5 of Android's
  own fling (OverScroller, what a list or Google Maps gives). The patch (MLRNMapView.`tuneFling`) now tracks the finger's
  velocity and, as it lifts, sets MapLibre's base time so the glide covers OverScroller's spline distance; threshold 500.

| release speed (300 px flick) | defaults (formula) | fixed 450 ms base (1777a16cb) | per-fling base (now) | Android's own |
|---|---|---|---|---|
| 2286 dp/s | 235 dp | 389 dp | **779–782 dp** | 816 dp |
| 1429 dp/s | 114 dp | 193 dp | **318–348 dp** | 361 dp |
| 952 dp/s | 0 (no fling) | 130 dp | **145–160 dp** | 178 dp |
| 571 dp/s | 0 (no fling) | 61 dp | **55–65 dp** | 73 dp |

- Web untouched (maplibre-gl's own inertia; the owner's complaint was Android). iOS not measured (no iOS fling patch).
- Test bed: `@app/map` native/camProbe.ts (`[cam]` settles, `[move]` app camera moves, the camera link
  `canifishthis://testbed/camera?lon&lat&zoom`, all inert outside the pan-probe build), scenario `feel`.
- Phone: the fling's length and glide on the owner's phone (OWNER-TODO).
