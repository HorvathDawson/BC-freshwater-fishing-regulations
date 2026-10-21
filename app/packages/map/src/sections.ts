/**
 * WHAT A SECTION IS ON SCREEN, for both renderers (N2). `Map.web.tsx` and `Map.native.tsx` read taps, the visible set
 * and feature-state targets through these, so the two maps cannot disagree about which feature is which water.
 *
 * THE ID IS THE MVT FEATURE ID. The tiles carry `section_id` as each section feature's own id (P18 tile step, from N1):
 * MapLibre Native ignores the style's `promoteId`, so a promoted property addressed nothing on a phone. GL JS reads the
 * same MVT id when a source-layer is not promoted, so both renderers now name a section by the same integer.
 *
 * ANDROID GIVES IT BACK AS A STRING (`"311274"`: the binding serialises `feature.id()` with `putString`), and a feature
 * with no id as `""` — which `Number()` would turn into section 0. `sectionIdOf` is the one conversion.
 */
import type { MapHandle } from "./adapters/contract";
import { MAP_STYLE, STYLE_META } from "./style";

/** The layers whose features ARE sections: the highlightable ones (`featureIdProperty: section_id`). */
export const SECTION_LAYERS: readonly string[] = STYLE_META.highlightable.map((h) => h.id);

/**
 * A tap's order: the stream group first — a river, and the lake or marsh polygons a stream flows through
 * (`lake_flowing`, `wetland_flowing`), which ARE the stream a finger aims at — then still water. A person aiming at a
 * creek inside a lake means the creek. One query per group (native queries cross the bridge, and a native feature
 * does not say which layer it came from), so the group names the hit: `as`.
 */
export const TAP_GROUPS: readonly { as: string; layers: readonly string[] }[] = [
  { as: "stream", layers: ["stream", "lake_flowing", "wetland_flowing"] },
  { as: "lake", layers: ["lake", "wetland"] },
];

/** Half the tap box. A river is a one-pixel line and a finger about forty-four: 12 px each way is a fingertip. */
export const TAP_RADIUS = 12;

/** The 24 px box around a press, `[[left, top], [right, bottom]]` — GL JS's PointLike pair and the binding's bounds. */
export function tapBox(x: number, y: number): [[number, number], [number, number]] {
  return [[x - TAP_RADIUS, y - TAP_RADIUS], [x + TAP_RADIUS, y + TAP_RADIUS]];
}

/**
 * The water a reader can ask Conditions about: streams, lakes, and the polygons a stream flows through. Not marsh:
 * nothing is gauged in one, and 333,526 wetland features would be work for an answer that is always "no station".
 */
export const VISIBLE_LAYERS: readonly string[] = ["stream", "lake", "lake_flowing", "wetland_flowing"];

/** A rendered feature's id as a section id: an integer ≥ 0 from a number or a numeric string; else null. */
export function sectionIdOf(id: unknown): number | null {
  if (typeof id === "number") return Number.isSafeInteger(id) && id >= 0 ? id : null;
  if (typeof id === "string" && /^\d+$/.test(id)) {
    const n = Number(id);
    return Number.isSafeInteger(n) ? n : null;
  }
  return null;
}

/** The first feature that names a section. */
export function firstSection(features: readonly { id?: unknown }[]): number | null {
  for (const f of features) {
    const id = sectionIdOf(f.id);
    if (id !== null) return id;
  }
  return null;
}

/** Every section named, once, in the order first seen. */
export function visibleSections(features: readonly { id?: unknown }[]): number[] {
  const seen = new Set<number>();
  for (const f of features) {
    const id = sectionIdOf(f.id);
    if (id !== null) seen.add(id);
  }
  return [...seen];
}

/**
 * Where feature-state for a style layer lives: its source and SOURCE-layer. `lake_flowing` draws the `lake`
 * source-layer, so its states are the lake features' — keyed by the style layer's id they would address nothing.
 */
export function stateTarget(layerId: string): { source: string; sourceLayer: string } | null {
  const l = MAP_STYLE.layers.find((x) => x.id === layerId) as
    { source?: string; "source-layer"?: string } | undefined;
  if (!l?.source || !l["source-layer"]) return null;
  return { source: l.source, sourceLayer: l["source-layer"] };
}

/** A feature-state key: a section's number when the key is one (data arrives keyed by strings), else as given. */
export function featureKey(id: string | number): string | number {
  return sectionIdOf(id) ?? id;
}

/** What a renderer does with feature-state: set some keys on one feature, or remove one key from one feature. */
export interface StateSink {
  set(t: { source: string; sourceLayer: string }, id: string | number, state: Record<string, unknown>): void;
  remove(t: { source: string; sourceLayer: string }, id: string | number, key: string): void;
}

/** The feature-state keys a highlight sets (`MapAdapter.highlight`): the part you are on, and the rest of its water. */
export const HIGHLIGHT_KEYS = ["selected", "water"] as const;

/**
 * The adapter's feature-state half over a renderer (`MapHandle`'s other calls do nothing here), shared so both maps
 * address the same feature the same way: the layer's own source-layer (`stateTarget`), the id as a number.
 *
 * CLEARING IS PER FEATURE AND ONLY THE HIGHLIGHT'S KEYS (`selected`, `water`), from the ids lit last time (`lit`).
 * Dropping every state of a source-layer would take the status colours with it, so selecting a river would un-colour
 * the map — and GL JS refuses to remove one KEY from a whole source-layer anyway ("A feature id is required to remove
 * its specific state property"), so the old whole-layer call threw and the old highlight stayed lit.
 */
export function stateHandle(sink: StateSink, lit: () => readonly (string | number)[]): MapHandle {
  return {
    setVisibility: () => {},
    setPaint: () => {},
    setFeatureState: (layerId, featureId, state) => {
      const t = stateTarget(layerId);
      if (t) sink.set(t, featureKey(featureId), state);
    },
    clearFeatureStates: (layerId) => {
      const t = stateTarget(layerId);
      if (t) for (const id of lit()) for (const k of HIGHLIGHT_KEYS) sink.remove(t, featureKey(id), k);
    },
  };
}

/**
 * The source-layers whose features carry a section's MVT id (`stream`, `lake`, `wetland`): the only ones a phone can
 * address by feature-state, because MapLibre Native ignores `promoteId`.
 */
export const SECTION_SOURCE_LAYERS: readonly string[] =
  [...new Set(SECTION_LAYERS.map((id) => stateTarget(id)?.sourceLayer).filter((x): x is string => !!x))].sort();

/** The source-layers a searched water can be drawn in — what a fit's refinement measures (`MapRenderer.waterBox`). */
export const WATER_SOURCE_LAYERS: readonly string[] = ["stream", "lake", "wetland"];

/**
 * A sink that pushes each feature-state value ONCE (N2 review). Several style layers draw one source-layer (`lake` and
 * `lake_flowing` are both the `lake` features), so the adapter addresses each feature once per layer; and the status
 * set is re-sent on every settle. On a phone each push is one TurboModule call per (feature, source-layer), so a value
 * already held is not sent again, and a key never set is not removed. `forget()` when the renderer has dropped them all
 * (a native style swap).
 */
export function onceSink(sink: StateSink): StateSink & { forget(t?: { source: string; sourceLayer: string }): void } {
  /** `<source>/<source-layer>/<id>` → key → JSON of the value held. */
  const held = new Map<string, Map<string, string>>();
  const at = (t: { source: string; sourceLayer: string }, id: string | number) => `${t.source}/${t.sourceLayer}/${id}`;
  return {
    // everything, or ONE source-layer the renderer dropped wholesale (the web's profile release swap, colour-ap)
    forget: (t) => {
      if (!t) { held.clear(); return; }
      const prefix = `${t.source}/${t.sourceLayer}/`;
      for (const k of [...held.keys()]) if (k.startsWith(prefix)) held.delete(k);
    },
    set: (t, id, state) => {
      const has = held.get(at(t, id)) ?? new Map<string, string>();
      const fresh = Object.entries(state).filter(([k, v]) => has.get(k) !== JSON.stringify(v ?? null));
      if (!fresh.length) return;
      for (const [k, v] of fresh) has.set(k, JSON.stringify(v ?? null));
      held.set(at(t, id), has);
      sink.set(t, id, Object.fromEntries(fresh));
    },
    remove: (t, id, key) => {
      // THE SINK IS THE WHOLE TRUTH: a renderer's feature-state is only ever set through it (a fresh map, or a style
      // swap and `forget()`), so a key it does not hold is not on the map, and there is nothing to remove
      const has = held.get(at(t, id));
      if (!has?.delete(key)) return;
      sink.remove(t, id, key);
    },
  };
}
