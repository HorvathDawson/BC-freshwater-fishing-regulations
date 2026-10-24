/**
 * THE WEB MAP'S SHARED REFS: the maplibre-gl map and what is made with it, held by `Map.web.tsx` and handed to every
 * hook in this folder. Refs, not state: nothing on screen re-renders when the map changes underneath.
 */
import { useRef, type MutableRefObject } from "react";
import type * as maplibregl from "maplibre-gl";
import { baseAdapter, type MapAdapter } from "../adapters/contract";
import type { WebRenderer } from "../adapters/web";
import type { Colouring } from "../colouring";
import { WaterCopy } from "../water-copy";
import type { LongPressWatch } from "../longPress";
import type { MapProps } from "../map-props";

export interface WebMapRefs {
  host: MutableRefObject<HTMLDivElement | null>;
  map: MutableRefObject<maplibregl.Map | null>;
  /**
   * The contract's renderer over this map (`adapters/web.ts`): feature-state, taps, the visible set, the fit — the
   * same calls `Map.native.tsx` makes of its own, which `parity.test.ts` holds to the same command log.
   */
  renderer: MutableRefObject<WebRenderer | null>;
  adapter: MutableRefObject<MapAdapter>;
  /** (T175) The lines of the water this map lights or holds, kept from the tiles — see `water-copy.ts`. */
  copy: MutableRefObject<WaterCopy>;
  /** The profile colouring of this map (colour-latency AP) — made with the map, fed by its tiles' load events. */
  colouring: MutableRefObject<Colouring | null>;
  /** Re-adds the label pill in the current theme. Set once the map exists. */
  pill: MutableRefObject<(() => void) | null>;
  /** Re-frame the current subject after the container changed size — unless the reader has moved off it. */
  refit: MutableRefObject<(() => void) | null>;
  /** The live long press, so the click it may leave behind is not also a tap. */
  longPress: MutableRefObject<LongPressWatch | null>;
  /**
   * The caller's `onError`, read when a look or data call reports. NOT a dependency of any effect: callers pass an
   * inline arrow, so every render of the screen re-ran the whole look — ~800 paint calls, each a no-op — on every feed
   * refresh (the Conditions measurement, 2026-10-21).
   */
  lookError: MutableRefObject<MapProps["onError"]>;
}

export function useWebMapRefs(onError: MapProps["onError"]): WebMapRefs {
  const refs: WebMapRefs = {
    host: useRef<HTMLDivElement | null>(null),
    map: useRef<maplibregl.Map | null>(null),
    renderer: useRef<WebRenderer | null>(null),
    adapter: useRef(baseAdapter("web")),
    copy: useRef(new WaterCopy()),
    colouring: useRef<Colouring | null>(null),
    pill: useRef<(() => void) | null>(null),
    refit: useRef<(() => void) | null>(null),
    longPress: useRef<LongPressWatch | null>(null),
    lookError: useRef(onError),
  };
  refs.lookError.current = onError;
  return refs;
}
