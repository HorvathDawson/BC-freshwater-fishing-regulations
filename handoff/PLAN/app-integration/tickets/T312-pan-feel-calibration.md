# T312 — Map panning feels slow: calibrate gesture speed and fling

Status: OPEN (owner 2026-10-31: "the app panning feels slow almost too. Like we need to calibrate how fast the panning is
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
