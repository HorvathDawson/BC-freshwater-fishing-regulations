/**
 * THE LAYER CHOICE, KEPT (owner 2026-10-22: the Spots switch "persists like the other layer toggles"). The Shell's one
 * piece of layer state, read once from the app's own store and written back whole on every change, so a reload — or
 * the next launch — opens the maps the way the reader left them. What is stored, and what a stored value may restore
 * to, is `@app/core` (`saveLayers` / `restoreLayers`); the store is the platform's (`@app/data/spots` `openLayerStore`:
 * a file beside the spots on a phone, a browser key on the web).
 *
 * A write that fails is swallowed: this is a preference, and losing it costs a tap, where refusing the tap would cost
 * the map. (A spot is a record and its store says so; this is not one.)
 */
import { useCallback, useState } from "react";
import { LAYER_DEFAULTS, restoreLayers, saveLayers, type LayerChoice } from "@app/core";
import { spotLabel, type Spot } from "@app/data/spots";

/** Whole text, by one name: what a phone's file and a browser's key both are. */
export interface LayerStore {
  read(): string | null;
  write(text: string): void;
}

export function useLayerChoice(store?: LayerStore):
  readonly [LayerChoice, (change: (c: LayerChoice) => LayerChoice) => void] {
  const [choice, setChoice] = useState<LayerChoice>(() => {
    if (!store) return LAYER_DEFAULTS;
    try { return restoreLayers(store.read()); } catch { return LAYER_DEFAULTS; }
  });
  const set = useCallback((change: (c: LayerChoice) => LayerChoice) => {
    setChoice((c) => {
      const next = change(c);
      if (next !== c && store) { try { store.write(saveLayers(next)); } catch { /* a preference: see above */ } }
      return next;
    });
  }, [store]);
  return [choice, set] as const;
}

/**
 * THE SAVED SPOTS A MAP IS HANDED (owner 2026-10-22: "toggle our spots on and off in the layer menu"): every spot, at
 * its place, under the name its row in the Spots tab shows — while the Layers sheet's Spots switch is on (`layersVM
 * (...).effective.pins.spots`); none while it is off. The colour is the screen's (a palette token).
 */
export function spotPins(spots: readonly Spot[], shown: boolean):
  readonly { lat: number; lon: number; title: string }[] {
  return shown ? spots.map((s) => ({ lat: s.lat, lon: s.lon, title: spotLabel(s) })) : [];
}
