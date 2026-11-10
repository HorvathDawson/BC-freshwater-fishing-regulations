/**
 * (T621, G15) THE ZOOM BUTTONS' STEP, for the phone's `zoomControls` (the web's are MapLibre's NavigationControl, which
 * steps one level and stops at the camera's limits). Pure, so it is tested in node.
 */
import { CAMERA_MAX_ZOOM, MIN_ZOOM } from "@app/core";

/** One level in or out of `zoom`, held inside the camera's own limits. */
export function zoomStep(zoom: number, dir: 1 | -1): number {
  return Math.min(CAMERA_MAX_ZOOM, Math.max(MIN_ZOOM, Math.round(zoom * 100) / 100 + dir));
}
