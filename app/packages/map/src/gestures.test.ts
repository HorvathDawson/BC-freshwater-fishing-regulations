/**
 * One map policy, two bindings (G1): the web map's options and the native map's switches, whole, from MAP_GESTURES —
 * and from a policy with every switch flipped, so a binding that ignored the policy and hard-coded today's answer
 * would fail the second table.
 */
import { describe, expect, it } from "vitest";
import { MAP_GESTURES, type MapGestures } from "@app/core";
import { maplibreGestures, nativeMapGestures, touchRotation } from "./gestures";

const FLIPPED: MapGestures = { pan: false, pinch: false, doubleTapZoom: false, wheel: false, rotate: true,
                               tilt: true, tapSlop: 3 };

describe("the map's gestures", () => {
  it("web: pan, pinch, double-tap and wheel zoom on; rotate and tilt off; a tap moves under 8 px", () => {
    expect({ options: maplibreGestures(), touchRotation: touchRotation() }).toEqual({
      options: { dragPan: true, scrollZoom: true, doubleClickZoom: true, touchZoomRotate: true, dragRotate: false,
                 pitchWithRotate: false, touchPitch: false, keyboard: true, clickTolerance: 8 },
      touchRotation: false,
    });
  });
  it("web: follows the policy, not today's answer", () => {
    expect({ options: maplibreGestures(FLIPPED), touchRotation: touchRotation(FLIPPED) }).toEqual({
      options: { dragPan: false, scrollZoom: false, doubleClickZoom: false, touchZoomRotate: false, dragRotate: true,
                 pitchWithRotate: true, touchPitch: true, keyboard: false, clickTolerance: 3 },
      touchRotation: false,
    });
    expect(touchRotation({ ...MAP_GESTURES, rotate: true })).toBe(true);
  });
  it("native: the same policy as MapLibre Native's switches", () => {
    expect([nativeMapGestures(), nativeMapGestures(FLIPPED)]).toEqual([
      { scrollEnabled: true, zoomEnabled: true, rotateEnabled: false, pitchEnabled: false },
      { scrollEnabled: false, zoomEnabled: false, rotateEnabled: true, pitchEnabled: true },
    ]);
  });
});
