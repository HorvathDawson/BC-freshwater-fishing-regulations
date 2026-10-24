/**
 * THE LOOK (view, theme, modes, groups, hidden and shown layers, the ground) and THE PUSHED DATA, applied to the web map
 * through the one adapter — each once the style has loaded once (`afterLoad`).
 */
import { useEffect, useRef } from "react";
import { withoutEngineKeys } from "../colouring";
import { applyLook } from "../look";
import type { MapProps } from "../map-props";
import { tracePaint } from "../trace-overlay";
import { layerHandle } from "./handles";
import { afterLoad } from "./ready";
import type { WebMapRefs } from "./refs";

type Look = Pick<MapProps, "view" | "theme" | "modes" | "groups" | "hide" | "show" | "basemap">;

export function useLook(refs: WebMapRefs, { view, theme, modes, groups, hide, show, basemap }: Look) {
  const { map, renderer, adapter, pill, lookError } = refs;
  /**
   * THE LOOK, KEYED ON THE LOOK ALONE: its CONTENT, not its props' identities. A caller hands fresh-but-equal objects on
   * every render (the Map tab's `hide={[]}`, a horizon change's new view model), and each one re-ran the whole pass —
   * 139 `setPaintProperty` per horizon click, every one a no-op (engine polish, 2026-10-21; one-engine.render.test
   * "A HORIZON CHANGE").
   */
  const lookKey = JSON.stringify([view, theme, modes ?? null, groups ?? null, hide ?? null, show ?? null,
                                  basemap ?? "map"]);
  const lookNow = useRef({ view, theme, modes, groups, hide, show, basemap });
  lookNow.current = { view, theme, modes, groups, hide, show, basemap };
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r) return;
    const apply = () => {
      const { view, theme, modes, groups, hide, show, basemap } = lookNow.current;
      const handle = layerHandle(m, r);
      // The pill is painted in the theme's paper, so a light one under dark text on a dark
      // map is the exact failure it replaced a halo to avoid. Repainted with everything else.
      pill.current?.();
      // THE LOOK — view, wash, gauge scale, per-layer modes, hidden layers, groups, the ground — is one sequence
      // shared with the native map (`applyLook`); a bad mode or group name is reported, never thrown (a blank screen
      // is not recoverable, a map in its previous colouring is). The ground is here, not in an effect of its own
      // behind `whenStyled`, which dropped a toggle made mid tile burst (one-engine.render.test).
      applyLook(adapter.current, handle, (id) => m.getLayer(id) !== undefined,
                { view, theme, modes, hide, show, groups, basemap: basemap ?? "map" },
                (e) => lookError.current?.(e));
      // The route overlay's paper and ink: runtime layers outside the catalogue, web only (the overlay lives here).
      for (const [id, paint] of Object.entries(tracePaint(theme)))
        if (m.getLayer(id))
          for (const [prop, value] of Object.entries(paint))
            m.setPaintProperty(id, prop as never, value as never);
    };
    // `afterLoad`, not `isStyleLoaded() ? apply() : once("load", apply)`: mid tile burst (a zoom out to the basin
    // field) `isStyleLoaded()` is false on a map that loaded long ago and `load` never fires again (N2 review web walk)
    return afterLoad(m, apply);
    // NOT keyed on `onError`: callers pass an inline arrow (see `lookError`, refs.ts).
  }, [lookKey]);
}

/*
 * PUSHED DATA, apart from the look: feature-state a mode reads that no engine channel sets. A section's status,
 * standing, stocked or survey is NEVER pushed on the web — those are the engine's channels (one pathway,
 * tools/one-colouring-engine); a push of any is refused and reported (`withoutEngineKeys`), so a second pathway cannot creep back in.
 */
export function usePushedData(refs: WebMapRefs, data: MapProps["data"]) {
  const { map, renderer, adapter, lookError } = refs;
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r || !data) return;
    return afterLoad(m, () => {
      const handle = layerHandle(m, r);
      for (const [layerId, values] of Object.entries(withoutEngineKeys(data, (e) => lookError.current?.(e))))
        adapter.current.setData(handle, layerId, values);
    });
  }, [data]);
}
