/**
 * THE CAMERA SENT TO A SUBJECT (`fit`) on the web map, refined to the water's own shape as its tiles arrive.
 */
import { fitAtMost, fitZoomLimit, fitZoomOf, padBbox, refineFit, unionBbox, type Bbox } from "@app/core";
import { useEffect, useRef, type MutableRefObject } from "react";
import type { MapProps } from "../map-props";
import { syncSize } from "../size";
import type { WebMapRefs } from "./refs";
import { cameraMs } from "../motion";

/**
 * SEND THE CAMERA to a subject, then let the water's own shape frame it.
 *
 * The box comes from the bundle's evidence (gauges, stocking sites, town distances) and
 * is not an outline — the Chilliwack's gauges are all on its lower half. So after each
 * move settles, the `highlight` features the tiles have actually delivered are measured
 * and the camera re-fits to them while `refineFit` says there is more to see. Bounded
 * rounds, so a river that runs off every tile set cannot walk the camera forever.
 *
 * The highlight is read through a ref: it arrives with the same subject, and a new array
 * for the same water must not restart the fit.
 *
 * Returns the drawn route overlay's extent, which the route overlay fills once its tiles are in (`useTrace`).
 */
export function useFit({ map, renderer, refit }: WebMapRefs, fit: MapProps["fit"],
                       highlight: MapProps["highlight"]): MutableRefObject<Bbox | null> {
  const lit = useRef(highlight);
  lit.current = highlight;
  /** The drawn route overlay's extent, once its tiles are in — see `useTrace`. */
  const traceBox = useRef<Bbox | null>(null);
  /** (T443) This map has been sent to a subject already: the next fit FOLLOWS one (`fitZoomLimit`). */
  const fittedOnce = useRef(false);
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r || !fit) return;
    let rounds = 0;
    let alive = true;
    let last: Bbox | null = null;
    let moved = false;
    // a pan or zoom by the READER (it carries the DOM event); our own fitBounds does not
    const touched = (e: { originalEvent?: unknown }) => { if (e.originalEvent) moved = true; };
    m.on("movestart", touched);
    refit.current = () => { if (alive && last && !moved) go(last); };
    // (T443) the closest this fit lands: a fit that follows another, from where the camera is as it starts (a refine
    // round or a refit keeps it)
    const from = fittedOnce.current ? m.getZoom() : null;
    if (fit.bbox) fittedOnce.current = true;
    const go = (b: Bbox) => {
      last = b;
      // measure the box BEFORE framing it: a stale canvas centres the water on a size nobody sees
      syncSize(m);
      const el = m.getContainer();
      const pad = Math.round(Math.min(56, el.clientWidth * 0.12, el.clientHeight * 0.12));
      const padding = Math.max(pad, 8);
      const limit = fitZoomLimit(fit, from, fitZoomOf(b, el.clientWidth, el.clientHeight, padding));
      r.fitBounds(fitAtMost(b, limit, el.clientWidth, el.clientHeight, padding), { padding, duration: cameraMs(450) });
    };
    const done = () => { alive = false; m.off("movestart", touched); };
    // A route overlay already drawn is part of the subject: "fit" frames the routes' own
    // shapes as well as their ends, so a river that bends outside its gauges stays on screen.
    const withTrace = (b: Bbox | null) =>
      (traceBox.current ? unionBbox(b, traceBox.current) : b);
    // (T48) an exact camera to return to (search's cancel): put back as it was. The box IS measured first: closing
    // search brings the legend strip back, so the map's box has just shrunk, and GL JS's easeTo pins the centre to the
    // canvas's middle as it was when the ease began — eased on the stale (taller) canvas, the map came back half the
    // strip low, and drifted further with every search.
    if (fit.camera) { syncSize(m); r.easeTo(fit.camera, { duration: cameraMs(450) }); return done; }
    const first = fit.bbox ? withTrace([...fit.bbox] as Bbox) : null;
    if (first) go(first);
    if (!fit.refine) return done;
    const measure = async () => {
      const ids = (lit.current ?? []).map(Number).filter(Number.isFinite);
      if (!alive || !ids.length) return;
      const found = await r.waterBox(ids);
      const next = refineFit(await r.bounds(), found, rounds);
      if (!alive) return;
      if (next) { rounds += 1; go(padBbox(next, 0.4)); }
      else m.off("idle", measure);
    };
    m.on("idle", measure);
    return () => { done(); m.off("idle", measure); };
  }, [fit?.key]);
  return traceBox;
}
