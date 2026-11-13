/**
 * (T707) A PINCH'S RELEASE CARRIES ITS ZOOM ON ANDROID — guarded on the patched binding (`patches/@maplibre__…patch`),
 * where the behaviour lives: the code is Kotlin, so this reads what the patch ADDS.
 *
 * WHAT THIS CAUGHT. Owner on build 29: "No zoom flick on map in app so zoom is clunky gesture wise". MapLibre Android's
 * own scale momentum gates a release on the span change of the LAST touch event against the first pointer's speed; on a
 * phone's batched touches that mostly fails, and identical injected flicks on the emulator carried 1 in 3 (testbed
 * `zoom-flick`, 2026-11-13). The binding now carries the pinch itself (`tunePinch`) and MapLibre's own is switched off
 * so the two never both run. Turning MapLibre's back on, dropping `tunePinch` from the touch path, or letting the carry
 * run without a gate (a pinch held still before the lift must stop where it is) fails here. The device proof is the
 * test bed's `zoom-flick` scenario (every fast release carries; a held one does not).
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { MAP_GESTURES } from "@app/core";
import { nativeMapGestures } from "../packages/map/src/gestures";

const PATCH = new URL("../patches/@maplibre__maplibre-react-native@11.5.0.patch", import.meta.url);
/** the Kotlin the patch adds to MLRNMapView.kt (its `+` lines, the marker stripped) */
function added(): string {
  const text = readFileSync(PATCH, "utf8");
  const start = text.indexOf("+++ b/android/src/main/java/org/maplibre/reactnative/components/mapview/MLRNMapView.kt");
  if (start < 0) throw new Error("the patch no longer changes MLRNMapView.kt");
  const end = text.indexOf("\ndiff --git", start);
  return text.slice(start, end < 0 ? undefined : end).split("\n")
    .filter((l) => l.startsWith("+") && !l.startsWith("+++")).map((l) => l.slice(1)).join("\n");
}

describe("T707: a pinch's release carries its zoom (Android binding patch)", () => {
  const kt = added();
  it("MapLibre's own scale momentum is off, and only because the binding's runs in the touch path", () => {
    expect(kt).toMatch(/uiSettings\.isScaleVelocityAnimationEnabled = false/);
    expect(kt).toMatch(/if \(ev != null\) tuneFling\(ev\)\s*\n\s*if \(ev != null\) tunePinch\(ev\)/);
    expect(kt).toMatch(/ACTION_UP \|\| ev\?\.actionMasked == MotionEvent\.ACTION_CANCEL\) pinchLifted\(ev\)/);
    expect(kt).toMatch(/private fun startZoomGlide\(/);
  });
  it("the carry is MapLibre's size and curve, and gated on fingers still moving at the lift", () => {
    expect(kt).toMatch(/\(finger \* 2\.5e-4\)\.coerceAtMost\(2\.5\)/);
    expect(kt).toMatch(/150\.0 \* \(ln\(1\.0 \+ kotlin\.math\.abs\(c\.addZoom\) \/ exp\(2\.0\)\) \+ 2\.0\)/);
    expect(kt).toMatch(/DecelerateInterpolator\(\)/);
    expect(kt).toMatch(/val carries = still <= PINCH_STILL_MS && finger \/ density >= PINCH_MIN_DP_S/);
    expect(kt).toMatch(/const val PINCH_STILL_MS = 40L/);
    expect(kt).toMatch(/const val PINCH_MIN_DP_S = 225\.0/);
    expect(kt).toMatch(/const val PINCH_CARRY_WINDOW_MS = 150L/);
  });
  it("a finger stops it, an app camera move takes over, and its settle is held back until it ends", () => {
    expect(kt).toMatch(/ACTION_DOWN -> \{\s*\n\s*if \(zoomGlide != null\) endZoomGlide\(\)/);
    expect(kt).toMatch(/REASON_API_GESTURE && zoomGlide != null\) endZoomGlide\(\)/);
    expect(kt).toMatch(/addOnCameraIdleListener \{ if \(glide == null && pinchCarry == null && zoomGlide == null\) sendRegionDidChangeEvent\(\) \}/);
  });
  it("only a zoom gesture the policy allows carries; rotation and tilt stay as the policy has them (off)", () => {
    expect(kt).toMatch(/if \(mapLibreMap\?\.uiSettings\?\.isZoomGesturesEnabled != true\) return null/);
    expect(kt).not.toMatch(/isRotateGesturesEnabled = true|isTiltGesturesEnabled = true/);
    expect(nativeMapGestures(MAP_GESTURES)).toMatchObject({ touchZoom: true, doubleTapZoom: true, doubleTapHoldZoom: true,
                                                           touchRotate: false, touchPitch: false });
  });
});
