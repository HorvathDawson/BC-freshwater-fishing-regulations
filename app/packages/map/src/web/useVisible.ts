/**
 * WHICH SECTIONS ARE ON SCREEN on the web map, reported to `onVisible` after movement settles.
 */
import { useEffect, useRef } from "react";
import type { MapProps } from "../map-props";
import type { WebMapRefs } from "./refs";

// Which reaches are on screen. Reported after movement settles rather than per frame:
// a pan fires `idle` once, where `move` fires sixty times a second and would issue a
// query per frame.
export function useVisible({ map, renderer }: WebMapRefs, onVisible: MapProps["onVisible"]) {
  // Held in a ref so a caller passing an inline arrow cannot make this effect thrash: the
  // listener registers once and reads the latest callback when it fires.
  const visibleCb = useRef(onVisible);
  visibleCb.current = onVisible;
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r) return;
    const report = () => {
      const cb = visibleCb.current;
      if (!cb) return;
      /*
       * LAKES COUNT AS WATER HERE, and they did not.
       *
       * This reported stream sections only, so the Conditions view asked the bundle about
       * rivers and never about lakes — and every lake stayed grey, including the ones with
       * a gauge sitting in them. A lake section IS a section: `lake_gauge` reaches it by
       * item rather than by reach, but the id the map holds is the same kind of id.
       *
       * `wetland` is deliberately not here. It is context, nothing is gauged in a marsh,
       * and adding 333,526 features to a query bounded by the viewport would be work for
       * an answer that is always "no station".
       */
      /*
       * THE ID IS NOT STRINGIFIED, and it used to be.
       *
       * `String(f.id)` was harmless while a section id WAS a string. It is an integer
       * handle now, and the bundle answers with a Map keyed by that integer — so a
       * stringified id looked up nothing, `standings` came back empty, and the Conditions
       * map painted every river as unmeasured. No error anywhere: a Map miss is a
       * `undefined`, and "no reading" is a legitimate answer this app draws on purpose.
       *
       * A Set of numbers de-duplicates exactly as well as a Set of strings, so passing the
       * id through unchanged costs nothing and removes the conversion that could be wrong.
       */
      // `VISIBLE_LAYERS` (shared with the native map): streams, lakes, and the lake/wetland polygons a stream flows
      // through — a slough drawn as a wetland polygon is a stream (`wetland_flowing`), and coloured
      void r.visible().then((seen) => { if (seen.length) visibleCb.current?.(seen); });
    };
    m.on("idle", report);
    // `idle` may already have passed by the time this registers — on a tab switch the map
    // is mounted and settled before the effect runs. Ask once immediately.
    if (m.isStyleLoaded()) report();
    return () => { m.off("idle", report); };
    // ASKING AGAIN WHEN SOMEONE STARTS LISTENING is the whole reason this depends on
    // anything. `onVisible` is undefined on the Map tab and defined on Conditions, and the
    // map is NOT remounted between them — so switching tabs left a settled map that would
    // not fire `idle` again until you panned. Nobody had reported what was on screen, the
    // standings map stayed empty, and every river painted as unmeasured until you nudged
    // it. Keyed on whether a listener exists rather than on its identity, so a caller
    // passing an inline arrow still cannot make this thrash.
  }, [onVisible !== undefined]);
}
