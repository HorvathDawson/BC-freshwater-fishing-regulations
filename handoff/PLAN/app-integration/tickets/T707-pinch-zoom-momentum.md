# T707 — A pinch's release carries its zoom (Android)

Status: DONE — 1468cfba5. Rides the next APK (native: the binding patch). iOS unchanged (read only).

Owner phone report, build 29 (Android, 2026-11-13): "No zoom flick on map in app so zoom is clunky gesture wise".

## Cause (measured, not guessed)
- Nothing of ours turned it off. The binding never touched `isScaleVelocityAnimationEnabled` (default on, MapLibre Android
  13.6.1 `UiSettings`); the T420 native patch is renderer-only; the T452 patch turns off only the FLING animation (pan).
  The `Camera` is uncontrolled (`initialViewState`), and no settle handler moves the camera.
- MapLibre's own gate is the cause (`MapGestureDetector$ScaleGestureListener.onScaleEnd`, read from the 13.6.1 bytecode):
  a release carries only if `spanSinceLast / (|vx| + |vy|) >= 0.004 x density`, where `spanSinceLast` is the span change of
  the LAST touch event before the lift and `vx, vy` the velocity of the FIRST pointer only (gestures 0.0.4
  `ProgressiveGesture`: `VelocityTracker.getXVelocity()` with no id). On batched, resampled phone touches the last event is
  often a sliver, and a thumb that holds still while the finger flicks has no first-pointer speed: the zoom stops dead.
- Emulator A/B, same scenario (`zoom-flick`, raw multi-touch, 6 per kind), MapLibre's gate vs the fix:
  two-finger flick 11/12 vs 12/12; ONE-HAND flick (thumb still, finger flicks) 5/12 vs 12/12; held pinch 0 vs 0.
  With sparser touches (12 ms + process gaps) MapLibre's gate carried 3 of 6 two-finger flicks. A phone's touch timing
  cannot be injected; the one-hand pinch is the measured failure, and the gate's last-event test is why it varies.

## Fix
`app/patches/@maplibre__maplibre-react-native@11.5.0.patch` (pnpm patch-commit; lockfile hash only):
- MapLibre's scale momentum off (`isScaleVelocityAnimationEnabled = false`), the binding's own on (`tunePinch`): at the
  second-last finger's lift both fingers' velocities give the span's rate; it carries if the span moved within 40 ms of
  the lift at >= 225 dp/s per finger (MapLibre's minimum). Size, time and curve are MapLibre's own (`2.5e-4` zoom per
  px/s, max 2.5; `150 * (ln(1 + dz/e^2) + 2)` ms; DecelerateInterpolator), started at the last finger's lift within 150 ms,
  applied per frame through `Transform.setZoom(zoom, focal)` as MapLibre's animator does (`MLRNCameraCache.zoomTo`).
- A finger stops it; an app camera move takes over; its settle (`onRegionDidChange`) is held back to its end (one settle).
- Pan fling (T452), double tap, two-finger tap, quick zoom unchanged; rotation and tilt stay off (policy `MAP_GESTURES`).
  Quick zoom (double-tap-drag) no longer has momentum of its own (it had MapLibre's, same gate).
- Camera probe: `m` samples now from the camera-move listener (the camera-is-changing one never fired), `p` the gate.

## Proof
- Test bed `zoom-flick` (new): every fast release carries (two-finger and one-hand, out and in), a held pinch does not,
  double tap +1, two-finger tap -1, quick zoom zooms. Fix APK: 16/16 two-finger (+0.76 to +1.54 zoom over 294-330 ms),
  then 24/24 with one-hand, control 0, taps pass (runs 2026-11-13T09-1x/09-2x under app/tools/testbed/out/runs/).
- Guard: `app/tools/pinch-momentum.test.ts` reads the patch (MapLibre's off, `tunePinch` in the touch path, the gate and
  the size/curve, settle held back, rotation/tilt never forced on); mutation (MapLibre's turned back on) fails it.

## Open
- Owner phone check on the next build: "map zoom carries momentum after a pinch" (OWNER-TODO).
