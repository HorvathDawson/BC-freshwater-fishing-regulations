/**
 * WHAT THE WEB MAP LIGHTS: the `highlight` (selected) and `water` (P15) sections, through the one adapter call — and
 * what was lit last, which the colouring engine's release swap puts back (`useChannels`).
 */
import type { SectionKey } from "@app/core";
import { useEffect, useRef, type MutableRefObject } from "react";
import type { MapProps } from "../map-props";
import type { SelectionCue } from "../paint";
import { whenAtlas } from "./ready";
import { litKey } from "../sections";
import type { WebMapRefs } from "./refs";

export interface Lit {
  wasLit: MutableRefObject<SectionKey[]>;
  wasWater: MutableRefObject<SectionKey[]>;
  wasCue: MutableRefObject<SelectionCue>;
  /** (T459) the ids lit `near` last (a focused water map's receiving water) */
  wasNear: MutableRefObject<SectionKey[]>;
  /** (T518) the ids lit `inseason` last */
  wasInSeason: MutableRefObject<SectionKey[]>;
}

export function useHighlight({ map, renderer, adapter }: WebMapRefs,
                             { highlight, water, selection = "outline", near, inSeason }: Pick<MapProps, "highlight" | "water" | "selection" | "near" | "inSeason">): Lit {
  /**
   * The ids lit `selected` last, and apart from them the ids lit `water` (P15) — cleared by id on the next highlight,
   * and what a release swap puts back (`useChannels`). Kept apart, and `wasLit` meaning what it meant before P15 (the
   * selected ids only), so the one colouring engine's `restore` can never put the water's
   * other parts back as `selected`; the restore re-applies both.
   */
  const wasLit = useRef<SectionKey[]>([]);
  const wasWater = useRef<SectionKey[]>([]);
  const wasCue = useRef<SelectionCue>("outline");
  const wasNear = useRef<SectionKey[]>([]);
  const wasInSeason = useRef<SectionKey[]>([]);
  // (T499) keyed on WHAT is lit (`litKey`), never on the arrays: the same ids in new arrays (a caller's re-render) are
  // not cleared and lit again (the native map drew a frame unlit between the two; here it is redundant work)
  const lit = litKey(highlight, water, selection, near, inSeason);
  const litNow = useRef({ highlight, water, near, inSeason });
  litNow.current = { highlight, water, near, inSeason };
  useEffect(() => {
    const { highlight, water, near, inSeason } = litNow.current;
    const m = map.current, r = renderer.current;
    if (!m || !r || !highlight) return;
    // `whenAtlas`: feature-state needs the atlas source and nothing else. Not `once("load")` (a search lights a water
    // on a map that loaded long ago, and a missed `load` meant the river never lit), and not `whenStyled` (T345): its
    // `isStyleLoaded()` is false through every tile burst — a ▲ / ▼ on a water's map flies the camera, so the new part
    // waited for a `styledata` a settled map never sends, and the OLD part stayed lit where the camera had left it.
    const apply = () => {
      // Clears ONLY the highlight's keys, per feature, from the ids this map lit last time — see `stateHandle`.
      const handle = r.states(() => [...wasLit.current, ...wasWater.current, ...wasNear.current, ...wasInSeason.current]);
      adapter.current.highlight(handle, [...highlight], [...(water ?? [])], selection, [...(near ?? [])], [...(inSeason ?? [])]);
      wasLit.current = [...highlight];
      wasWater.current = [...(water ?? [])];
      wasNear.current = [...(near ?? [])];
      wasInSeason.current = [...(inSeason ?? [])];
      wasCue.current = selection;
    };
    return whenAtlas(m, apply);
  }, [lit, selection]);
  return { wasLit, wasWater, wasCue, wasNear, wasInSeason };
}
