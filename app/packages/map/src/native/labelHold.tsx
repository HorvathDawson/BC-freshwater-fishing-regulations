/**
 * (T876) THE LABEL HOLD ON A PHONE (`../label-hold.ts`): the hold's state as a tiny store, and the style's own label layers
 * ADOPTED by id so their visibility can change without a style swap (a swap is ~225 ms and drops every feature-state).
 *
 * ADOPTION: the binding's `Layer` whose id the style already holds takes that layer over (`MLRNLayer.addToMap`, both
 * platforms) and sets only what it is given — here `visibility`, nothing else. Each sits under its own source element, so
 * after a style swap the source re-adopts it in the new style with the same visibility (`MLRNSource.addToMap`).
 *
 * NEVER UNMOUNTED WHILE THE MAP LIVES: unmounting an adopted layer REMOVES it from the style. So the ids are fixed at the
 * map's first style (`useAdoptedIds`), and a later style that lacks one gets it at `visibility: none` (the binding then
 * makes an empty layer of that id, drawn nowhere: the dark basemap has no `pois`). Only the store re-renders on a hold
 * change, never the map.
 */
import { useRef, useSyncExternalStore } from "react";
import { Layer } from "@maplibre/maplibre-react-native";
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

/** The ids to adopt, per source: the first style's, for the life of the map. */
export function useAdoptedIds(now: Record<HoldSource, HeldLayer[]>): Record<HoldSource, string[]> {
  const ids = useRef<Record<HoldSource, string[]> | null>(null);
  ids.current ??= Object.fromEntries(HOLD_SOURCES.map((s) => [s, now[s].map((l) => l.id)])) as Record<HoldSource, string[]>;
  return ids.current;
}

/** The adopted label layers of one source: the style's own visibility, or none while the hold is on. `source` is the
 *  enclosing source's id, which the binding hands its children (`cloneReactChildrenWithProps`). */
export function HeldLabels({ store, ids, now, source }:
    { store: HoldStore; ids: readonly string[]; now: readonly HeldLayer[]; source?: string }) {
  const held = useSyncExternalStore(store.subscribe, store.held, store.held);
  return (
    <>
      {ids.map((id) => {
        const base = now.find((l) => l.id === id)?.visibility ?? "none";
        return <Layer key={id} id={id} type="symbol" source={source} layout={{ visibility: held ? "none" : base }} />;
      })}
    </>
  );
}
