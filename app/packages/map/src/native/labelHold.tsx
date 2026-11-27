/**
 * (T876) THE LABEL HOLD ON A PHONE (`../label-hold.ts`): the hold's state as a tiny store, and the style's own label layers
 * ADOPTED by id so their visibility can change without a style swap (a swap is ~225 ms and drops every feature-state).
 *
 * ADOPTION: the engine's layer element whose id the style already holds takes that layer over and sets only what it is
 * given — here `visibility`, nothing else (`HeldLabels`, the engine's own: MapLibre Native's is `maplibre/HeldLabels.tsx`).
 *
 * NEVER UNMOUNTED WHILE THE MAP LIVES: unmounting an adopted layer REMOVES it from the style. So the ids only GROW: every
 * style's held ids are added as they appear (`useAdoptedIds`: a map opened dark has no `pois` until it turns light), none
 * is ever dropped, and a style that lacks one gets it at `visibility: none` (the binding then makes an empty layer of that
 * id, drawn nowhere). Only the store re-renders on a hold change, never the map.
 */
import { useRef } from "react";
import { HOLD_SOURCES, LabelHold, type HeldLayer, type HoldSource } from "../label-hold";

export interface HoldStore {
  machine: LabelHold;
  subscribe(l: () => void): () => void;
  held(): boolean;
}

export function holdStore(): HoldStore {
  const ls = new Set<() => void>();
  let held = false;
  const machine = new LabelHold((h) => { held = h; for (const l of ls) l(); });
  return { machine, subscribe: (l) => { ls.add(l); return () => { ls.delete(l); }; }, held: () => held };
}

/** The ids to adopt, per source: every style's so far, in the order they first appeared, never one dropped. */
export function useAdoptedIds(now: Record<HoldSource, HeldLayer[]>): Record<HoldSource, string[]> {
  const ids = useRef<Record<HoldSource, string[]> | null>(null);
  const was = ids.current;
  const grown = HOLD_SOURCES.some((s) => now[s].some((l) => !was?.[s].includes(l.id)));
  if (!was || grown)
    ids.current = Object.fromEntries(HOLD_SOURCES.map((s) => {
      const kept = was?.[s] ?? [];
      return [s, [...kept, ...now[s].map((l) => l.id).filter((id) => !kept.includes(id))]];
    })) as Record<HoldSource, string[]>;
  return ids.current!;
}
