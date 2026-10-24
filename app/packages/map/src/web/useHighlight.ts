/**
 * WHAT THE WEB MAP LIGHTS: the `highlight` (selected) and `water` (P15) sections, through the one adapter call — and
 * what was lit last, which the colouring engine's release swap puts back (`useChannels`).
 */
import type { SectionKey } from "@app/core";
import { useEffect, useRef, type MutableRefObject } from "react";
import type { MapProps } from "../map-props";
import type { SelectionCue } from "../paint";
import { whenStyled } from "./ready";
import type { WebMapRefs } from "./refs";

export interface Lit {
  wasLit: MutableRefObject<SectionKey[]>;
  wasWater: MutableRefObject<SectionKey[]>;
  wasCue: MutableRefObject<SelectionCue>;
}

export function useHighlight({ map, renderer, adapter }: WebMapRefs,
                             { highlight, water, selection = "outline" }: Pick<MapProps, "highlight" | "water" | "selection">): Lit {
  /**
   * The ids lit `selected` last, and apart from them the ids lit `water` (P15) — cleared by id on the next highlight,
   * and what a release swap puts back (`useChannels`). Kept apart, and `wasLit` meaning what it meant before P15 (the
   * selected ids only), so the one colouring engine's `restore` can never put the water's
   * other parts back as `selected`; the restore re-applies both.
   */
  const wasLit = useRef<SectionKey[]>([]);
  const wasWater = useRef<SectionKey[]>([]);
  const wasCue = useRef<SelectionCue>("outline");
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r || !highlight) return;
    // `whenStyled`, not `once("load")`: a search lights a water on a map that loaded long
    // ago, often mid-tile-burst, and a missed `load` meant the river never lit.
    const apply = () => {
      // Clears ONLY the highlight's keys, per feature, from the ids this map lit last time — see `stateHandle`.
      const handle = r.states(() => [...wasLit.current, ...wasWater.current]);
      adapter.current.highlight(handle, [...highlight], [...(water ?? [])], selection);
      wasLit.current = [...highlight];
      wasWater.current = [...(water ?? [])];
      wasCue.current = selection;
    };
    return whenStyled(m, apply);
  }, [highlight, water, selection]);
  return { wasLit, wasWater, wasCue };
}
