/**
 * (T876) THE ADOPTED LABEL LAYERS, MapLibre Native's half of the label hold (`../labelHold.tsx` keeps the store; moved here
 * unchanged by T1154).
 *
 * ADOPTION: the binding's `Layer` whose id the style already holds takes that layer over (`MLRNLayer.addToMap`, both
 * platforms) and sets only what it is given — here `visibility`, nothing else. Each sits under its own source element, so
 * after a style swap the source re-adopts it in the new style with the same visibility (`MLRNSource.addToMap`).
 */
import { useSyncExternalStore } from "react";
import { Layer } from "@maplibre/maplibre-react-native";
import type { HeldLayer } from "../../label-hold";
import type { HoldStore } from "../labelHold";

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
