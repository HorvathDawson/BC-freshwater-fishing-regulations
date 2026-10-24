declare const __DEV__: boolean | undefined;
/**
 * THE WEB MAP'S RUNTIME SOURCES, filled from props: the gauge dots (`gauges`) and the route overlay (`trace`).
 */
import type { Bbox } from "@app/core";
import { useEffect, useRef, type MutableRefObject } from "react";
import type { MapProps } from "../map-props";
import { collectionBbox, traceOverlay } from "../trace-geometry";
import { EMPTY_TRACE } from "../trace-overlay";
import { traceHandle } from "./handles";
import { EMPTY_FC, whenTraced } from "./ready";
import type { WebMapRefs } from "./refs";

// The gauge points. A GeoJSON source is replaced wholesale rather than diffed: it is a
// few hundred dots and they all change together when the feed ticks.
export function useGauges({ map }: WebMapRefs, gauges: MapProps["gauges"]) {
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    /**
     * RETRY UNTIL THE SOURCE IS THERE, rather than once on `load`.
     *
     * `m.once("load", ...)` was the bug: on the first switch to Conditions the map had
     * loaded minutes earlier, so the callback was registered for an event that had already
     * fired and never ran. The dots appeared only after opening a reach and coming back —
     * because THAT remounted the map and baked the data into the style. `styledata` fires
     * on every style change and is the only event that reliably arrives afterwards.
     *
     * The empty collection is written too, and deliberately: leaving Conditions has to
     * clear the dots, and a stale layer of readings over the regulations map is a set of
     * numbers about a question nobody asked.
     */
    let done = false;
    const apply = () => {
      const src = m.getSource("gauges") as
        { setData?: (d: unknown) => void } | undefined;
      if (!src?.setData) return;
      src.setData(JSON.parse(gauges ?? EMPTY_FC));
      done = true;
      m.off("styledata", apply);
    };
    apply();
    if (!done) m.on("styledata", apply);
    return () => { m.off("styledata", apply); };
  }, [gauges]);
}

/**
 * THE ROUTE OVERLAY — see `trace-overlay.ts` and `trace-geometry.ts`.
 *
 * TWO EFFECTS, two lifetimes. The LINES are copied out of the atlas tiles when the routes
 * change (`trace.key`) and at no other time — a horizon change re-labels the gauges and
 * must not refetch a single tile. The PINS follow every change of their own content (a
 * share moves with the horizon), and land at once: the reader sees where the gauges are
 * while their routes are still being read.
 *
 * `traceBox` is the drawn lines' extent, what a fit frames with them (`useFit`).
 */
export function useTrace({ map, adapter }: WebMapRefs, { trace, at, onError }: Pick<MapProps, "trace" | "at" | "onError">,
                         traceBox: MutableRefObject<Bbox | null>) {
  const tracePins = useRef(trace?.pins ?? []);
  tracePins.current = trace?.pins ?? [];
  const pinKey = JSON.stringify(trace?.pins ?? []);
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    return whenTraced(m, () =>
      adapter.current.setTrace(traceHandle(m), tracePins.current, null));
  }, [pinKey]);

  useEffect(() => {
    const m = map.current;
    if (!m) return;
    let alive = true;
    traceBox.current = null;
    // The previous route goes the moment the subject changes, never lingering under a new one.
    // ONE PENDING WRITE AT A TIME: a clear still waiting on `styledata` when the lines arrive
    // must be cancelled, or it fires afterwards and wipes the route it was clearing the way for.
    let pending = whenTraced(m, () =>
      adapter.current.setTrace(traceHandle(m), tracePins.current, EMPTY_TRACE));
    const routes = (trace?.routes ?? []).filter((r) => r.path.length > 0);
    if (routes.length) {
      traceOverlay(at.atlas, routes).then(({ fc, missing, fetched, tiles, ms }) => {
        if (!alive) return;
        traceBox.current = collectionBbox(fc);
        pending();
        pending = whenTraced(m, () =>
          adapter.current.setTrace(traceHandle(m), tracePins.current, fc));
        // Development only: the measured cost, for the record and for the screenshots' README.
        if (typeof __DEV__ !== "undefined" && __DEV__)
          ((globalThis as { __traceCost?: unknown[] }).__traceCost ??= []).push(
            { key: trace?.key, sections: new Set(routes.flatMap((r) => r.path)).size,
              missing: missing.length, fetched, tiles, ms, features: fc.features.length });
      }).catch((e) => onError?.(e instanceof Error ? e : new Error(String(e))));
    }
    return () => { alive = false; pending(); };
  }, [trace?.key, at.atlas]);
}
