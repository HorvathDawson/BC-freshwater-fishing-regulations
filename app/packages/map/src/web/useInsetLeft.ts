/**
 * (T856) THE DESKTOP'S FLOATING NAV RAIL covers the map's left `insetLeft` px (the map runs to the window's edge under it):
 * the camera's padding takes that much off the left, so the centre, a fit-to-water and a fly-to all frame the VISIBLE
 * area. The scale bar is moved right of it by the `--map-inset-left` CSS variable (`controls.css`). 0 when absent, always
 * 0 on a phone (the rail never floats there).
 *
 * Keyed on the map's own keys as well (`useMapInstance`): new endpoints (the release's tiles replacing the dev ones)
 * build a NEW map, which starts with no padding — the old map's left inset must be put on it again.
 */
import { useEffect } from "react";
import type { MapProps } from "../map-props";
import type { WebMapRefs } from "./refs";

export function useInsetLeft({ map }: WebMapRefs, insetLeft: number, at: MapProps["at"]): void {
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const was = m.getPadding();
    if (was.left === insetLeft) return;
    m.setPadding({ ...was, left: insetLeft });
  }, [map, insetLeft, at.atlas, at.basemap, at.glyphs, at.sprite, at.assets]);
}
