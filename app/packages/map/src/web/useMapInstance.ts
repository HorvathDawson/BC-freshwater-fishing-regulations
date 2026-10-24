declare const __DEV__: boolean | undefined;
/**
 * THE MAP ITSELF: made once per set of archives, with the one gesture policy, MapLibre's own controls, the one error
 * handler, the generated images and the colouring engine fed by each tile's load event.
 */
import { useEffect, useRef } from "react";
import * as maplibregl from "maplibre-gl";
import { CAMERA_BOUNDS, CAMERA_MAX_ZOOM, FIT_MAX_ZOOM, MIN_ZOOM } from "@app/core";
import { webRenderer } from "../adapters/web";
import { Colouring, LABEL_SOURCE_LAYERS } from "../colouring";
import { compassShown, keyboardRotation, maplibreGestures, touchRotation } from "../gestures";
import type { MapProps } from "../map-props";
import { runtimeStyle } from "../runtime-style";
import { followSize } from "../size";
import { sourceTiles, tileIds } from "../tile-ids.web";
import { atlasTarget, copyTarget, publicSections } from "./handles";
import { addImages, isGeneratedImage } from "./images";
import { devCost, EMPTY_FC, forcePublic, registerPMTiles, styledOnce } from "./ready";
import type { WebMapRefs } from "./refs";

type Made = Pick<MapProps, "at" | "theme" | "modes" | "gauges" | "initial" | "bare" | "zoomControls" | "onError"
                          | "onImageryError">;

export function useMapInstance(refs: WebMapRefs,
                               { at, theme, modes, gauges, initial, bare, zoomControls, onError, onImageryError }: Made) {
  const { host, map, renderer, adapter, copy, colouring, pill, refit } = refs;
  // read by the map's one `error` handler, which is bound once at construction
  const imageryError = useRef(onImageryError);
  imageryError.current = onImageryError;
  useEffect(() => {
    if (!host.current) return;
    registerPMTiles();
    const m = new maplibregl.Map({
      container: host.current,
      // `gauges` rides in on the endpoints so the source EXISTS from the first frame;
      // its contents are then replaced imperatively below. Declaring it later would mean
      // rebuilding the whole style every half hour to move a few hundred dots.
      style: runtimeStyle({ ...at, gauges: gauges ?? EMPTY_FC }, theme,
                          modes) as unknown as maplibregl.StyleSpecification,
      center: [initial.lon, initial.lat],
      zoom: initial.zoom,
      // The atlas has nothing below z4 and the basemap is clipped to the province: past
      // either there is no data, only ragged tile edges and bare paper. See CAMERA_BOUNDS.
      // IN, the camera goes past the tiles (z18) and MapLibre overzooms each source from
      // its own deepest zoom, which the source declares (runtime-style) — CAMERA_MAX_ZOOM.
      minZoom: MIN_ZOOM,
      maxZoom: CAMERA_MAX_ZOOM,
      maxBounds: CAMERA_BOUNDS as unknown as maplibregl.LngLatBoundsLike,
      attributionControl: bare ? false : { compact: true },
      // G1: one gesture policy for both maps — pan, pinch, double-tap and wheel zoom; no rotate, no tilt; a press
      // that moves 8 px or more is a pan, never a tap (`@app/core` MAP_GESTURES)
      ...maplibreGestures(),
    });
    if (!touchRotation()) m.touchZoomRotate.disableRotation();
    if (!keyboardRotation()) m.keyboard.disableRotation();

    /*
     * COLLAPSED ON ARRIVAL.
     *
     * `compact: true` makes the attribution collapsible; it does not make it collapsed.
     * MapLibre adds `maplibregl-compact-show` on creation, so the panel opens itself — and
     * once the credits included Environment and Climate Change Canada and the Province
     * alongside OpenStreetMap and Protomaps, that was three lines and 64px of text lying
     * across the bottom of a phone screen, on top of the Layers button.
     *
     * The class is removed rather than the control restyled: the information stays exactly
     * one tap away on the (i), which is what the licences ask for, and the map is a map
     * again. Removed after a frame because MapLibre adds it during its own mount.
     */
    const collapse = () => host.current
      ?.querySelector(".maplibregl-ctrl-attrib")
      ?.classList.remove("maplibregl-compact-show");
    collapse();
    m.once("load", collapse);

    /**
     * MapLibre's OWN controls, not ours.
     *
     * I had hand-written a zoom stack, a compass and a scale bar. They looked right and
     * none of them worked: the buttons had no handlers wired, and the scale bar was fed a
     * camera that only updated on `moveend` and lived in a ref, so it never re-rendered
     * and never changed. All three are solved problems that ship with the renderer —
     * NavigationControl already does zoom, bearing and pitch with the map's own easing,
     * and ScaleControl recomputes on every frame of a pinch.
     *
     * They are restyled to the design in `controls.css`, which is the part that was
     * actually worth writing.
     */
    // See MapProps.bare — an illustration, not something you drive.
    // A bare map may still ask for the zoom buttons (`zoomControls`) — never the compass.
    if (!bare || zoomControls)
      m.addControl(new maplibregl.NavigationControl({
        // the compass turns the map when dragged; with rotation off north is always up (G1)
        showCompass: compassShown() && !bare, visualizePitch: false,
      }), "top-right");
    /*
     * BOTTOM-RIGHT, above the attribution.
     *
     * It was top-left, which is also where the date pill sits — so the two stacked in the
     * same corner, the scale clipped against the top edge of the screen, and the one thing
     * that qualifies every distance on the map was the least legible thing on it. Bottom
     * corners are where every map a reader has used puts a scale, and the top-left is now
     * the date's alone: WHEN is the app's own question, and it deserves the corner.
     */
    if (!bare)
      m.addControl(new maplibregl.ScaleControl({ maxWidth: 92, unit: "metric" }),
                   "bottom-right");
    // A map that fails silently is the worst outcome this app can produce: an empty map
    // looks exactly like water with no regulations on it. Surface every renderer error.
    //
    // EXCEPT THE SATELLITE IMAGERY'S, when the caller handles it (`onImageryError`): imagery is online-only, so an
    // offline device fails every imagery tile, and the caller answers that by drawing the map's own ground. The status
    // travels (GL JS's AJAXError carries it; a network failure has none).
    m.on("error", (e) => {
      const ev = e as { sourceId?: string; error?: Error & { status?: number } };
      if (ev.sourceId === "imagery" && imageryError.current) {
        imageryError.current(typeof ev.error?.status === "number" ? ev.error.status : undefined);
        return;
      }
      onError?.(e.error instanceof Error ? e.error : new Error(e.error?.message ?? "map error"));
    });
    /**
     * THE GAUGE LABEL'S PILL, as pixels (and the hatches and the "you are here" pin: `images.ts`).
     *
     * Added imperatively because a style cannot declare a generated image, and the
     * alternative was forking the basemap's sprite sheet to add one rounded rectangle.
     *
     * REGISTERED HERE, INSIDE THE EFFECT THAT CREATES THE MAP. It used to be its own
     * effect declared above this one, which meant it ran first, found `map.current` still
     * null, returned — and never ran again, because its only dependency was the theme.
     * MapLibre's response to a missing `icon-image` is to draw the text and log a warning,
     * so every label rendered as bare text on the basemap and nothing looked broken.
     *
     * `styleimagemissing` is the belt to that brace: MapLibre fires it the moment a layer
     * asks for an image it does not have, so the pill arrives however the ordering falls.
     */
    const addPill = () => addImages(m, theme);
    pill.current = addPill;
    m.on("styleimagemissing", (e: { id: string }) => {
      if (isGeneratedImage(e.id))
        pill.current?.();
    });
    m.on("load", addPill);
    m.once("load", () => styledOnce.add(m));
    map.current = m;
    const toCopy = copyTarget(m);
    const r0 = webRenderer(m as never, FIT_MAX_ZOOM, { copy: () => copy.current, target: () => toCopy });
    renderer.current = r0;
    /*
     * EACH TILE COLOURED IN ITS OWN LOAD EVENT (colour-latency AP).
     *
     * GL JS fires `sourcedata` with the tile right after initialising the tile's feature-state and before it is ever
     * drawn; state set here is applied in the next frame's source update, so the tile's FIRST frame wears its status.
     * The status used to arrive after `idle` (every tile in, the 300 ms symbol fade over) and a React round trip —
     * half a second of grey on every new tile. Synchronous, no React, ids already seen skipped: about 0.1 ms a pan.
     *
     * The ids come from 5.24's internals (`tile-ids.web.ts`, pinned by a test). Where they are not there, the PUBLIC
     * route: one `querySourceFeatures` scan per burst of tile events, in a microtask — still before the next frame —
     * and on `moveend` as well, because the public route cannot see a cached tile coming back into view.
     */
    const c = new Colouring(adapter.current);
    colouring.current = c;
    const target = atlasTarget(m, r0, copy.current, toCopy);
    let scanQueued = false;
    const scan = (kind: string) => {
      scanQueued = false;
      if (!c.active || !m.getSource("atlas")) return;
      const t0 = performance.now();
      const calls = c.label(target, publicSections(m));
      devCost({ t: t0, ms: performance.now() - t0, calls, kind });
    };
    const onTile = (e: maplibregl.MapSourceDataEvent & { tile?: unknown }) => {
      if (e.sourceId !== "atlas" || !e.tile || !c.active) return;
      const ids = forcePublic() ? null : tileIds(e.tile, LABEL_SOURCE_LAYERS);
      if (ids === null) {
        if (!scanQueued) { scanQueued = true; queueMicrotask(() => scan("public-tile")); }
        return;
      }
      const t0 = performance.now();
      const calls = c.label(target, ids);
      devCost({ t: t0, ms: performance.now() - t0, calls, kind: "tile" });
    };
    const onMoveEnd = () => {
      if (c.active && (forcePublic() || sourceTiles(m, "atlas") === null)) scan("public-moveend");
    };
    m.on("sourcedata", onTile);
    m.on("moveend", onMoveEnd);
    // The canvas follows its container, not the window (see size.ts): a box that settles after mount — search's
    // preview map on the way back from the eye — would otherwise keep the first size and every fit would frame it.
    const unfollow = followSize(m, () => refit.current?.());
    // Development only: a map is the one component you cannot inspect from the React tree,
    // and every question about it ("where is the camera", "did that source load") needs the
    // instance. Never present in a production bundle.
    if (typeof __DEV__ !== "undefined" && __DEV__)
      (globalThis as { __map?: unknown }).__map = m;
    return () => {
      m.off("sourcedata", onTile); m.off("moveend", onMoveEnd);
      unfollow(); m.remove(); map.current = null; colouring.current = null; renderer.current = null;
    };
    // Deliberately keyed on the ARCHIVES only. `theme` and `view` are recoloured by the
    // look effect rather than by rebuilding the map, because remounting would drop the
    // camera and refetch every tile — a theme toggle would look like a crash and a reload.
    // `initial` is a starting camera, not a controlled one; re-reading it here would yank
    // the map back to it on every render.
  },[at.atlas, at.basemap, at.glyphs, at.sprite, at.assets]);
}
