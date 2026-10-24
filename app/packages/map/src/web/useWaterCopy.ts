/**
 * (T175) THE WATER'S OWN LINE, KEPT on the web map — see `water-copy.ts`. Keyed on the water (its ids, and the archive:
 * an id means a section only within one atlas). Each time the camera settles, the map's own pick (`litFeatures`, the
 * one a fit measures with) is asked for the water's sections in the tiles it holds; what a zoom has the first time is
 * kept, given the state its atlas features hold now, and drawn below the zoom the atlas draws it from (the ceiling).
 * With `waterLine`, while no zoom has shown the water, the camera steps in about a point on it (`probeZoom`), never
 * against the reader's own pan. A new water drops the copy and its states at once.
 */
import { useEffect, useMemo, useRef } from "react";
import type { MapProps } from "../map-props";
import { COPY_CASING, COPY_FILL, COPY_LINE, COPY_SOURCE, EMPTY_COPY, copyIds, probeZoom, zoomAround } from "../water-copy";
import { copyTarget } from "./handles";
import { whenCopy } from "./ready";
import type { WebMapRefs } from "./refs";

export function useWaterCopy({ map, renderer, copy }: WebMapRefs,
                             { highlight, water, waterLine, at, onError }:
                               Pick<MapProps, "highlight" | "water" | "waterLine" | "at" | "onError">) {
  const keep = useMemo(() => copyIds(highlight, water, waterLine?.sections),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed on the ids' text
    [(highlight ?? []).join(","), (water ?? []).join(","), (waterLine?.sections ?? []).join(",")]);
  const keepKey = keep.join(",");
  const lineAt = useRef(waterLine?.at ?? null);
  lineAt.current = waterLine?.at ?? null;
  const clamps = waterLine != null;
  const lineIds = useRef<ReadonlySet<number>>(new Set());
  lineIds.current = useMemo(() => new Set(copyIds(waterLine?.sections)),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed on the ids' text
    [(waterLine?.sections ?? []).join(",")]);
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r) return;
    const c = copy.current;
    const to = copyTarget(m);
    // only a copy that held a water has anything to drop (a map that lights nothing never touches the copy source)
    const had = c.ids.size > 0;
    const changed = c.reset(keep, at.atlas) && had;
    let alive = true, touched = false, busy = false, rounds = 0, lastAt = "";
    const source = () => m.getSource(COPY_SOURCE) as { setData?: (d: unknown) => void } | undefined;
    // THE CEILING: the copy stops where the atlas draws the water itself — the deepest kept zoom, or the layer's own
    // floor in this view (the Conditions field stands the rivers down), whichever is deeper
    const ceilings = () => {
      const floorOf = (id: string) => (m.getLayer(id) as { minzoom?: number } | undefined)?.minzoom ?? 0;
      const line = c.ceiling("stream", floorOf("stream")), fill = c.ceiling("lake", floorOf("lake"));
      if (line !== null) for (const id of [COPY_CASING, COPY_LINE]) if (m.getLayer(id)) m.setLayerZoomRange(id, 0, line);
      if (fill !== null && m.getLayer(COPY_FILL)) m.setLayerZoomRange(COPY_FILL, 0, fill);
    };
    const draw = () => whenCopy(m, () => { source()?.setData?.(c.collection()); ceilings(); });
    let pending = changed
      ? whenCopy(m, () => {
          if (m.getSource(COPY_SOURCE)) m.removeFeatureState({ source: COPY_SOURCE });
          source()?.setData?.(EMPTY_COPY);
        })
      : c.size ? draw() : () => {};
    const onStart = (e: { originalEvent?: unknown }) => { if (e.originalEvent) touched = true; };
    const settle = async () => {
      if (!alive || busy || !c.ids.size || !m.getSource("atlas")) return;
      const z = m.getZoom(), ctr = m.getCenter();
      const here = `${z.toFixed(3)}:${ctr.lng.toFixed(5)},${ctr.lat.toFixed(5)}`;
      // NOTHING MOVED, NOTHING NEW: `idle` comes again after our own state lands, over the same tiles
      if (here === lastAt) return;
      lastAt = here;
      busy = true;
      try {
        const found = await r.litFeatures([...c.ids]);
        if (!alive) return;
        const fresh = c.take(found, z);
        if (fresh.length) {
          // what the atlas features hold now (the engine labelled them as their tiles arrived) goes on the copy first
          for (const id of fresh) {
            const st = c.fresh(id, m.getFeatureState({ source: "atlas", sourceLayer: c.layerOf(id)!, id }) ?? {});
            if (st) to.set(id, st);
          }
          pending(); pending = draw();
        } else if (c.size) ceilings();
        // THE WATER decides, not what else is lit: a route's chain down a big river is in the tiles long before its creek
        const line = lineIds.current;
        const next = clamps
          ? probeZoom({ zoom: z, found: found.some((f) => line.has(f.id)), kept: c.keptOf(line), rounds, touched })
          : null;
        if (next !== null) {
          rounds += 1;
          const cam = zoomAround({ lon: ctr.lng, lat: ctr.lat }, lineAt.current, z, next);
          m.easeTo({ center: [cam.lon, cam.lat], zoom: cam.zoom, duration: 300 });
        }
      } catch (e) {
        onError?.(e instanceof Error ? e : new Error(String(e)));
      } finally { busy = false; }
    };
    const onIdle = () => { void settle(); };
    m.on("idle", onIdle);
    m.on("movestart", onStart);
    // a map already settled sends no `idle` for a new water: ask now
    if (c.ids.size && m.loaded()) void settle();
    return () => { alive = false; pending(); m.off("idle", onIdle); m.off("movestart", onStart); };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the water (its ids) and the archive are the key
  }, [keepKey, at.atlas, clamps]);
}
