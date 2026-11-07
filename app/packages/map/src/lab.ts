/**
 * (T420) THE MAP LAB: live A/B switches for judging map performance ON THE OWNER'S PHONE, where the GPU and fill costs
 * the emulator cannot show are real. Each switch changes one thing; flipping it back restores today's map exactly.
 *
 * NEVER IN PRODUCTION (owner/builder 2026-11-06, COORD-068; T476 owner 2026-11-07): on in a development or staging build,
 * i.e. any build that does not say `EXPO_PUBLIC_APP_ENV=production` (production must be named: a build never defaults to it).
 * No flag, no tap-code, no runtime unlock. It is the gate of the Developer card (T476) as a whole. The switches last for
 * the session only — module state, never saved — so a restart is today's map.
 *
 * AT ITS DEFAULTS IT IS TODAY'S MAP: the one default that is not "off" is `hideInvisible` (owner 2026-11-06, after the
 * stressed A/B and a phone check: ON for every build) — it hides only layers drawing nothing, so the picture is the same.
 * `lab.test.ts` pins that, that with it off the style comes back as the very object, and that each switch moves only
 * the layers it names.
 */
import { useSyncExternalStore } from "react";

export interface Lab {
  /** (T448) draw at most 2 device pixels per dp (today's map: on) */
  pixelCap: boolean;
  /** (T401) MapLibre's prefetch zoom delta: 4 = its own (today), 2 = closer, richer placeholders */
  prefetch: 4 | 2;
  /** every stream line layer with one layout, so MapLibre builds ONE bucket per stream tile instead of three */
  streamsOneBucket: boolean;
  /** the basemap's roads drawn (today: on) */
  roads: boolean;
  /** the basemap's landuse greens drawn (today: on) */
  landuse: boolean;
  /** every label and icon drawn (today: on) */
  labels: boolean;
  /** the least time between two feature-state batches, ms (`useColouring`); 0 = one per burst (today) */
  stateGapMs: 0 | 250;
  /** hide a layer whose every opacity is the constant 0: it draws nothing, but MapLibre Native still re-applies every
   *  stored feature-state to it each frame (T420 correction) */
  hideInvisible: boolean;
}

export const LAB_DEFAULT: Readonly<Lab> = Object.freeze({
  pixelCap: true, prefetch: 4, streamsOneBucket: false, roads: true, landuse: true, labels: true, stateGapMs: 0, hideInvisible: true,
});

/** Read once: Metro inlines `EXPO_PUBLIC_*` at build time. */
export const LAB_ON = process.env.EXPO_PUBLIC_APP_ENV?.trim() !== "production";

let now: Readonly<Lab> = LAB_DEFAULT;
const listeners = new Set<() => void>();

export function setLab(change: Partial<Lab>): void {
  now = Object.freeze({ ...now, ...change });
  for (const l of listeners) l();
}

const subscribe = (l: () => void) => { listeners.add(l); return () => { listeners.delete(l); }; };
const read = () => now;

/** The lab's switches; always the defaults in a build without the lab. */
export function useLab(): Readonly<Lab> {
  const lab = useSyncExternalStore(subscribe, read, read);
  return LAB_ON ? lab : LAB_DEFAULT;
}

type Layer = { id: string; type?: string; source?: string; "source-layer"?: string; minzoom?: number;
               layout?: Record<string, unknown> };
type Style = { layers: Layer[] } & Record<string, unknown>;

/** One shared layout for every stream line layer (MapLibre's own line defaults: butt caps, miter joins). */
const STREAM_LAYOUT_KEYS = ["line-cap", "line-join"];

const hidden = (l: Layer): Layer => ({ ...l, layout: { ...l.layout, visibility: "none" } });

/** Every opacity a layer type has; a layer drawing nothing has each of them at the constant 0. */
const OPACITY: Record<string, readonly string[]> = {
  line: ["line-opacity"], fill: ["fill-opacity"], circle: ["circle-opacity", "circle-stroke-opacity"],
  symbol: ["text-opacity", "icon-opacity"],
};
const drawsNothing = (l: Layer & { paint?: Record<string, unknown> }): boolean => {
  const keys = OPACITY[l.type ?? ""];
  return !!keys && keys.every((k) => l.paint?.[k] === 0);
};

/** The style with the lab's switches applied; the SAME object when every style switch is at its default. */
export function labStyle<S>(style: S, lab: Readonly<Lab>): S {
  const s = style as unknown as Style;
  if (!lab.streamsOneBucket && lab.roads && lab.landuse && lab.labels && !lab.hideInvisible) return style;
  const layers = s.layers.map((l) => {
    if (lab.hideInvisible && drawsNothing(l)) return hidden(l);
    if (!lab.labels && l.type === "symbol") return hidden(l);
    if (!lab.roads && l.source === "basemap" && l["source-layer"] === "roads") return hidden(l);
    if (!lab.landuse && l.source === "basemap" && l["source-layer"] === "landuse") return hidden(l);
    if (lab.streamsOneBucket && l.type === "line" && l.source === "atlas" && l["source-layer"] === "stream") {
      const layout = Object.fromEntries(Object.entries(l.layout ?? {}).filter(([k]) => !STREAM_LAYOUT_KEYS.includes(k)));
      // `minzoom: 0` and no minzoom draw the same, but MapLibre keys a bucket on the number: one spelling for all
      return { ...l, minzoom: l.minzoom ?? 0, layout: { ...layout, visibility: layout.visibility ?? "visible" } };
    }
    return l;
  });
  return { ...s, layers } as unknown as S;
}
