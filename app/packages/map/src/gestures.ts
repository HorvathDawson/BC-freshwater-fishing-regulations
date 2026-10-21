/**
 * THE MAP'S GESTURES, ONE POLICY, TWO BINDINGS (G1). `@app/core` `MAP_GESTURES` says what a finger may do to the map;
 * these turn it into each renderer's own switches, so the browse map and the water map, on the web and on the phone,
 * pan, pinch and tap alike.
 *
 *   maplibreGestures    maplibre-gl's constructor options (`Map.web.tsx`). `clickTolerance` is the tap slop: a press
 *                       that moved further is a pan and never fires a click, so a small pan never selects a section.
 *   nativeMapGestures   MapLibre Native's `MapView` switches, for `Map.native.tsx` when N1 lands (that file is the
 *                       spike's; this is its typed hook-up point, and nothing here names the native SDK).
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
    // the arrow keys pan and +/- zoom; Shift+arrows would rotate, which the keyboard handler drops with the rest
    keyboard: g.pan,
    clickTolerance: g.tapSlop,
  };
}

/** After construction: maplibre-gl turns two-finger ROTATION off on its pinch handler, not in the options. */
export function touchRotation(g: MapGestures = MAP_GESTURES): boolean {
  return g.pinch && g.rotate;
}

/** MapLibre Native (`@maplibre/maplibre-react-native` `MapView`) switches — for `Map.native.tsx` at N1. */
export interface NativeMapGestureProps {
  scrollEnabled: boolean;
  zoomEnabled: boolean;
  rotateEnabled: boolean;
  pitchEnabled: boolean;
}

export function nativeMapGestures(g: MapGestures = MAP_GESTURES): NativeMapGestureProps {
  return { scrollEnabled: g.pan, zoomEnabled: g.pinch || g.doubleTapZoom, rotateEnabled: g.rotate,
           pitchEnabled: g.tilt };
}
