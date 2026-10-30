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
