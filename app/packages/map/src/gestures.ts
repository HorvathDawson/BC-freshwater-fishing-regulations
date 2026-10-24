/**
 * THE MAP'S GESTURES, ONE POLICY, TWO BINDINGS (G1). `@app/core` `MAP_GESTURES` says what a finger may do to the map;
 * these turn it into each renderer's own switches, so the browse map and the water map, on the web and on the phone,
 * pan, pinch and tap alike.
 *
 *   maplibreGestures    maplibre-gl's constructor options (`web/useMapInstance.ts`). `clickTolerance` is the tap slop: a press
 *                       that moved further is a pan and never fires a click, so a small pan never selects a section.
 *   nativeMapGestures   MapLibre Native's `Map` props (`@maplibre/maplibre-react-native` 11.5), spread onto the map in
 *                       `Map.native.tsx` (N2). Nothing here names the native SDK.
 */
import { MAP_GESTURES, type MapGestures } from "@app/core";

/** The maplibre-gl `MapOptions` the policy decides. Plain values, so this file needs no maplibre import. */
export interface MaplibreGestureOptions {
  dragPan: boolean;
  scrollZoom: boolean;
  doubleClickZoom: boolean;
  touchZoomRotate: boolean;
  dragRotate: boolean;
  pitchWithRotate: boolean;
  touchPitch: boolean;
  keyboard: boolean;
  clickTolerance: number;
}

export function maplibreGestures(g: MapGestures = MAP_GESTURES): MaplibreGestureOptions {
  return {
    dragPan: g.pan,
    scrollZoom: g.wheel,
    doubleClickZoom: g.doubleTapZoom,
    // two fingers: pinch to zoom; whether they may also turn the map is `touchRotation` below
    touchZoomRotate: g.pinch,
    dragRotate: g.rotate,
    pitchWithRotate: g.tilt,
    touchPitch: g.tilt,
    // the arrow keys pan and +/- zoom; Shift+arrows turn and tilt unless `keyboardRotation` below turns that off
    keyboard: g.pan,
    clickTolerance: g.tapSlop,
  };
}

/** After construction: maplibre-gl turns two-finger ROTATION off on its pinch handler, not in the options. */
export function touchRotation(g: MapGestures = MAP_GESTURES): boolean {
  return g.pinch && g.rotate;
}

/**
 * After construction: maplibre-gl's keyboard handler turns the map with Shift+Left/Right and tilts it with
 * Shift+Up/Down until `keyboard.disableRotation()` is called (one switch for both), whatever `dragRotate` says.
 */
export function keyboardRotation(g: MapGestures = MAP_GESTURES): boolean {
  return g.rotate || g.tilt;
}

/**
 * NavigationControl's compass turns the map when dragged (maplibre's MouseRotateWrapper calls `setBearing` itself,
 * ignoring `dragRotate`), and with no rotation north is always up, so there is nothing for it to show: it is shown
 * only where the map may turn.
 */
export function compassShown(g: MapGestures = MAP_GESTURES): boolean {
  return g.rotate;
}

/**
 * MapLibre Native's switches: the `Map` props of `@maplibre/maplibre-react-native` 11.5 (N1 §8), spread onto the map in
 * `Map.native.tsx`. Plain values, so this file still names no SDK.
 *
 * NO TAP SLOP HERE, and that is not an omission: the binding exposes none. MapLibre Native's own gesture detector decides
 * what is a tap (on Android the system touch slop, ~8 dp on a typical phone), so the "< 8 px" rule is MapLibre's on
 * native and can only be checked there, not set. One-finger double-tap-and-drag zoom (`doubleTapHoldZoom`) goes with
 * double-tap zoom: both are the policy's "double tap zooms". The compass is shown only where the map may turn, as on
 * the web (`compassShown`).
 */
export interface NativeMapGestureProps {
  dragPan: boolean;
  touchZoom: boolean;
  doubleTapZoom: boolean;
  doubleTapHoldZoom: boolean;
  touchRotate: boolean;
  touchPitch: boolean;
  compass: boolean;
}

export function nativeMapGestures(g: MapGestures = MAP_GESTURES): NativeMapGestureProps {
  return { dragPan: g.pan, touchZoom: g.pinch, doubleTapZoom: g.doubleTapZoom, doubleTapHoldZoom: g.doubleTapZoom,
           touchRotate: g.rotate, touchPitch: g.tilt, compass: compassShown(g) };
}
