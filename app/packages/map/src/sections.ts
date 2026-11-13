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
import { notFishingKind, type NotFishingKind, type SectionKey } from "@app/core";
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

/** (T621, G4) Rings narrower than the tap box, innermost first, tried only when the box holds more than one water. */
export const TAP_RINGS: readonly number[] = [2, 6];

/** The box of a given radius around a press (`tapBox` is the outer ring, `TAP_RADIUS`). */
export function tapRing(x: number, y: number, r: number): [[number, number], [number, number]] {
  return [[x - r, y - r], [x + r, y + r]];
}

/**
 * (T621, G4) THE SECTION UNDER A PRESS, NEAREST FIRST, for both renderers. Each engine lists hits in its own order
 * (tile order within a layer), so a creek meeting a river could open either one. The tap box is asked once; only when
 * it holds MORE THAN ONE water are the narrower rings asked, innermost first, and the first ring holding a section
 * decides (still the stream group before still water). No geometry: the phone's bridge cannot project synchronously.
 * A ring that finds nothing (a hit only in the wide box) falls back to the wide box's own first hit.
 */
export async function pickSection(
  query: (box: [[number, number], [number, number]], group: { as: string; layers: readonly string[] })
    => Promise<readonly { id?: unknown }[] | null>,
  x: number, y: number,
): Promise<{ layer: string; id: number } | null> {
  for (const g of TAP_GROUPS) {
    const wide = await query(tapBox(x, y), g);
    if (wide === null) return null;
    const first = firstSection(wide);
    if (first === null) continue;
    if (visibleSections(wide).length > 1)
      for (const r of TAP_RINGS) {
        const id = firstSection((await query(tapRing(x, y, r), g)) ?? []);
        if (id !== null) return { layer: g.as, id };
      }
    return { layer: g.as, id: first };
  }
  return null;
}

/**
 * (T173) THE FISH OBSTACLE MARKS, asked BEFORE the water (`MapRenderer.pressObstacle`): a mark sits on a river, and a
 * finger on the mark means the mark. Only while its switch is on — a hidden layer answers no query.
 */
export const OBSTACLE_LAYERS: readonly string[] = ["obstacle"];

/**
 * (T166) TIDAL WATER, asked LAST (`MapRenderer.pressTidal`): a tap that hits no water section but lands on the `tidal_water`
 * area (the DFO tidal-waters polygon) opens the tidal sheet. Never before a section: freshwater drawn over it answers first.
 */
export const TIDAL_LAYERS: readonly string[] = ["tidal_water"];

/** True when any feature of a tidal query exists (the area carries no tap payload: it is the same sheet everywhere). */
export const anyTidal = (features: readonly unknown[]): boolean => features.length > 0;

/**
 * (T292) NOT FISHING WATER — a sewage lagoon, tailings pond or dump (the decorative `not_fishing_water` layer, no id),
 * asked after the water sections and before tidal water (`MapRenderer.pressNotFishing`): a tap that hits no section but
 * lands on one opens the not-fishing sheet naming its `kind`. Never a tap group: it is no section and has no regulations.
 */
export const NOT_FISHING_LAYERS: readonly string[] = ["not_fishing_water"];

/** The first not-fishing feature's `kind` (null when the tile has none — an older atlas), or null for no feature. */
export function firstNotFishing(features: readonly { properties?: unknown }[]): { kind: NotFishingKind | null } | null {
  if (!features.length) return null;
  const p = features[0]!.properties;
  return { kind: notFishingKind(p && typeof p === "object" ? (p as Record<string, unknown>).kind : undefined) };
}

/** The first obstacle's tile properties, or null (a feature with no properties is not one). */
export function firstObstacle(features: readonly { properties?: unknown }[]): Readonly<Record<string, unknown>> | null {
  for (const f of features)
    if (f.properties && typeof f.properties === "object") return f.properties as Record<string, unknown>;
  return null;
}

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

/**
 * (test bed only, Map.native `findTarget`) A POINT IS THE LIT WATER'S TAP TARGET when the tap there answers a lit section
 * and every still-water feature under the box is the lit water too. (T478) A lit river answered at a lake's tip, and
 * after Back (unlit) the same point opened the lake: another water's still water under the box skips the point.
 * (T689) The lit water's OWN still water does not: a lake is all still water, and skipping it left a lake with no target
 * (build 29, regs-open "Cultus"). A still-water feature with no section id is not known to be lit, so it skips; a failed
 * query (`null`, reported by the caller) is no target.
 */
export function isTapTarget(hit: { id: number } | null, lit: ReadonlySet<number>,
                            stillUnder: readonly { id?: unknown }[] | null): boolean {
  if (!hit || !lit.has(Number(hit.id)) || stillUnder === null) return false;
  return stillUnder.every((f) => { const id = sectionIdOf(f.id); return id !== null && lit.has(id); });
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

/**
 * The style's sources that read the ATLAS archive: `atlas` itself, and the field's `basins` (the same archive, stopped at
 * the last zoom it carries basins at so it overzooms — layers.source.json `sources.basins`).
 */
export const ATLAS_SOURCES: readonly string[] = Object.entries(MAP_STYLE.sources as Record<string, { url?: string }>)
  .filter(([, s]) => s.url === "pmtiles://atlas").map(([id]) => id);

/** A feature-state key: a section's number when the key is one (data arrives keyed by strings), else as given. */
export function featureKey(id: string | number): string | number {
  return sectionIdOf(id) ?? id;
}

/** What a renderer does with feature-state: set some keys on one feature, or remove one key from one feature. */
export interface StateSink {
  set(t: { source: string; sourceLayer: string }, id: string | number, state: Record<string, unknown>): void;
  remove(t: { source: string; sourceLayer: string }, id: string | number, key: string): void;
}

/** The feature-state keys a highlight sets (`MapAdapter.highlight`): what is selected (outlined), the part you are on in
 *  a water's Map view (`part`, T151: marked by dimming the rest), and the rest of its water. */
export const HIGHLIGHT_KEYS = ["selected", "part", "water", "near", "inseason"] as const;

/**
 * (T499, owner 2026-11-08 on 0.6.9: the Fraser's water map, left open, "flickered now and then with no input") WHAT A
 * HIGHLIGHT LIGHTS, as text: the map's highlight effect is keyed on this, never on the arrays. A caller's re-render
 * hands the same ids in NEW arrays (the water map's view model rebuilt by a feed refresh); keyed on the arrays, each
 * one cleared every lit section and lit it again — on a phone, as separate feature-state writes, so a frame was drawn
 * with the whole part unlit (emulator, testbed `map-idle`: the part blinked out ~4.5 min into an idle Map view).
 */
export function litKey(highlight: readonly SectionKey[] | undefined, water: readonly SectionKey[] | undefined,
                       cue: string, near: readonly SectionKey[] | undefined,
                       inSeason?: readonly SectionKey[]): string {
  return highlight
    ? `${highlight.join(",")}|${(water ?? []).join(",")}|${cue}|${(near ?? []).join(",")}|${(inSeason ?? []).join(",")}` : "";
}

/**
 * The adapter's feature-state half over a renderer (`MapHandle`'s other calls do nothing here), shared so both maps
 * address the same feature the same way: the layer's own source-layer (`stateTarget`), the id as a number.
 *
 * CLEARING IS PER FEATURE AND ONLY THE HIGHLIGHT'S KEYS (`HIGHLIGHT_KEYS`), from the ids lit last time (`lit`).
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
 * THE SOURCE-LAYERS WHOSE FEATURES ARE SECTIONS (T124): the ones carrying a section's MVT id, so the only ones
 * feature-state can address (on a phone too — MapLibre Native ignores `promoteId`). Each is also the id of the style
 * layer that draws it plainly, so it doubles as a `MapProps.data` key.
 *
 * SPELLED ONCE, HERE. Every list that means "the per-section layers" is this list: `SECTION_SOURCE_LAYERS`,
 * `WATER_SOURCE_LAYERS`, colouring's `LABEL_SOURCE_LAYERS`, trace-geometry's `TRACE_LAYERS`, and the pushed data's
 * `SECTION_DATA` fan-out (`spreadSections`). Three hand-typed copies used to stand beside a derived one, and the
 * pushed lists disagreed (a wetland pushed for standings but not for the route; `wetland` in one, `wetland_flowing`
 * in another), so a new section layer would have dropped silently out of whichever was not updated.
 * `tools/one-section-layers.test.ts` proves it equals the style's highlightable layers' source-layers and that no
 * other module spells such a list.
 */
const PER_SECTION_LAYERS = ["stream", "lake", "wetland"] as const;
export type PerSectionLayer = typeof PER_SECTION_LAYERS[number];

/** The per-section source-layers (`PER_SECTION_LAYERS`). */
export function perSectionLayers(): readonly PerSectionLayer[] {
  return PER_SECTION_LAYERS;
}

/** The source-layers a phone can address by feature-state: the per-section ones. */
export const SECTION_SOURCE_LAYERS: readonly string[] = perSectionLayers();

/** The source-layers a searched water can be drawn in — what a fit's refinement measures (`MapRenderer.waterBox`). */
export const WATER_SOURCE_LAYERS: readonly string[] = perSectionLayers();

/**
 * THE `MapProps.data` KEY FOR "EVERY SECTION": its values go to every per-section source-layer. A caller (the pushed
 * status, standings and route in `@app/ui`, which cannot import this package) hands a section's values once under
 * this key, and the map fans them out — so no caller names a layer, and none can name a different set.
 */
export const SECTION_DATA = "sections";

/** `MapProps.data` with `SECTION_DATA`'s values fanned out to each per-section layer; every other key as given. */
export function spreadSections<V>(data: Readonly<Record<string, V>>): Record<string, V> {
  const { [SECTION_DATA]: each, ...rest } = data;
  if (each === undefined) return { ...rest };
  return { ...Object.fromEntries(perSectionLayers().map((l) => [l, each])), ...rest };
}

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
