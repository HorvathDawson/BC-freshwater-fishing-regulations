/**
 * WHAT THE READER DOES TO THE WEB MAP, reported to the caller: a settled camera (`onMoved`), a long press
 * (`onLongPress`), a tap on the map (`onMapPoint`) and a tap on a section (`onPressFeature`).
 */
import { useEffect, useRef } from "react";
import type * as maplibregl from "maplibre-gl";
import { longPressWatch } from "../longPress";
import type { MapProps } from "../map-props";
import type { WebMapRefs } from "./refs";

export function useMoved({ map }: WebMapRefs, onMoved: MapProps["onMoved"]) {
  useEffect(() => {
    const m = map.current;
    if (!m || !onMoved) return;
    // `moveend`, not `move`: one report per gesture rather than one per frame.
    const report = () => {
      const c = m.getCenter();
      onMoved({ lon: c.lng, lat: c.lat, zoom: m.getZoom() });
    };
    m.on("moveend", report);
    return () => { m.off("moveend", report); };
  }, [onMoved]);
}

/*
 * THE LONG PRESS (owner 2026-10-21, `onLongPress`): a held finger timed by the one policy (`longPressWatch`), or the
 * browser's own long press / a right click (`contextmenu`) — whichever comes first, once per press. Any pan, a second
 * finger, a lift or the map starting to move ends it, so it never fights pan or pinch. Keyed on whether there is a
 * listener (the latest one is read through a ref), so an inline arrow from a caller cannot make it thrash.
 */
export function useLongPress({ map, longPress }: WebMapRefs, onLongPress: MapProps["onLongPress"], at: MapProps["at"]) {
  const longPressCb = useRef(onLongPress);
  longPressCb.current = onLongPress;
  useEffect(() => {
    const m = map.current;
    if (!m || onLongPress === undefined) return;
    const w = longPressWatch((x, y) => {
      const at = m.unproject([x, y]);
      longPressCb.current?.(at.lat, at.lng);
    });
    longPress.current = w;
    const fingers = (e: maplibregl.MapTouchEvent) => e.originalEvent.touches.length;
    const down = (e: maplibregl.MapTouchEvent) => w.down(e.point.x, e.point.y, fingers(e));
    const move = (e: maplibregl.MapTouchEvent) => w.move(e.point.x, e.point.y, fingers(e));
    const mouse = (e: maplibregl.MapMouseEvent) => w.down(e.point.x, e.point.y, 0);
    const up = () => w.up();
    // a finger lifted: the mousedown the browser makes up next is this touch. After a long press that lift is
    // cancelled: the drop sheet is now under the finger, and the click the browser would make of it lands on the
    // sheet's scrim — which closes the sheet the press just raised
    const lift = (e: maplibregl.MapTouchEvent) => {
      if (w.fired()) e.originalEvent?.preventDefault?.();
      w.up(true);
    };
    const context = (e: maplibregl.MapMouseEvent) => {
      e.originalEvent?.preventDefault?.();          // the browser's own menu would cover the sheet
      w.context(e.point.x, e.point.y);
    };
    m.on("touchstart", down); m.on("touchmove", move); m.on("touchend", lift); m.on("touchcancel", lift);
    m.on("mousedown", mouse); m.on("movestart", up); m.on("contextmenu", context);
    return () => {
      m.off("touchstart", down); m.off("touchmove", move); m.off("touchend", lift); m.off("touchcancel", lift);
      m.off("mousedown", mouse); m.off("movestart", up); m.off("contextmenu", context);
      w.dispose();
      longPress.current = null;
    };
    // ...and on the map's own keys: new endpoints (the release's tiles replacing the dev ones) build a NEW map, and
    // a listener left on the old one would never hear a press again
  }, [onLongPress !== undefined, at.atlas, at.basemap, at.glyphs, at.sprite, at.assets]);
}

export function useMapPoint({ map, longPress }: WebMapRefs, onMapPoint: MapProps["onMapPoint"]) {
  useEffect(() => {
    const m = map.current;
    if (!m || !onMapPoint) return;
    // the click a long press may leave behind is not also a tap
    const onTap = (e: maplibregl.MapMouseEvent) => {
      if (!longPress.current?.fired()) onMapPoint(e.lngLat.lat, e.lngLat.lng);
    };
    m.on("click", onTap);
    return () => { m.off("click", onTap); };
  }, [onMapPoint]);
}

export function usePressFeature({ map, renderer, longPress }: WebMapRefs, onPressFeature: MapProps["onPressFeature"],
                                onError: MapProps["onError"], onPressObstacle?: MapProps["onPressObstacle"],
                                onPressTidal?: MapProps["onPressTidal"]) {
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r || (!onPressFeature && !onPressObstacle && !onPressTidal)) return;
    const onClick = (e: maplibregl.MapMouseEvent) => {
      if (longPress.current?.fired()) return;       // the click a long press may leave behind is not a tap
      /**
       * A BOX, not a point.
       *
       * A river is a one-pixel line and a finger is about forty-four. Querying the exact
       * pixel means most taps land on nothing, and the ones that do land feel like luck —
       * which is what "hard to click things" is. 12 px each way is roughly a fingertip at
       * this density and still small enough that two rivers rarely both qualify.
       */
      /*
       * ONLY THE SECTION LAYERS, IN `TAP_GROUPS` ORDER (`MapRenderer.press`, shared with the native map).
       *
       * A label, the under-lake route (the topology's path through a lake, not water anybody fishes: a person aiming
       * at it means the LAKE) and the area layers name no section, so they are not asked — every caller turns the id
       * into a SECTION (`Number(id)`), and a park's `area_id` used to come back as NaN. The stream group comes
       * first — a river and the polygons a stream flows through — then still water: tapping a creek inside a lake
       * returns both, and a person aiming at a creek means the creek. The first argument is the group ("stream" or
       * "lake"), the same on both maps: a native feature does not say which style layer it came from.
       */
      const { lat, lng } = e.lngLat;
      // (T173) a fish obstacle mark first: a tap on the mark is the mark's, and opens no water under it
      const { x, y } = e.point;
      void (async () => {
        const ob = onPressObstacle ? await r.pressObstacle(x, y) : null;
        if (ob) { onPressObstacle!(ob); return; }
        const hit = onPressFeature ? await r.press(x, y) : null;
        // WHERE ON THE FEATURE, not just which feature. A river is hundreds of kilometres
        // long and the answer to "how does this spot reach the gauge" starts at the point
        // under the finger — without it the route panel can mark the station and nothing
        // else. The coordinate is snapped to nothing: it is where the person tapped.
        if (hit) { onPressFeature!(hit.layer, hit.id, lat, lng); return; }
        // (T166) no water under the tap: tidal water, if that is what it landed on
        if (onPressTidal && await r.pressTidal(x, y)) onPressTidal();
      })().catch((err: unknown) => onError?.(err instanceof Error ? err : new Error(String(err))));
    };
    m.on("click", onClick);
    return () => { m.off("click", onClick); };
  }, [onPressFeature, onPressObstacle, onPressTidal]);
}
