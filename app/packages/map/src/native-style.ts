/**
 * THE STYLE JSON MapLibre Native IS HANDED (N2). Platform-free, so it is tested in node.
 *
 * The web map sets its look imperatively on a live map. The native binding takes a whole style (`mapStyle`), so the
 * same look is written into a COPY of the same `runtimeStyle()` output, through the same adapter and the same
 * `applyLook` sequence (a `MapHandle` whose writes land in the JSON). Two things differ, both because of MapLibre Native:
 *
 *   promoteId    dropped. MapLibre Native ignores it (N1 §2); a section is its MVT id (`./sections.ts`). The area
 *                layers keep their property styling and have no feature id on a phone.
 *   area ids     the layers keyed by a PROPERTY (`basin_id`; not a section, so no MVT id) cannot take feature-state on
 *                a phone. Their per-feature values (`areaData`: the Conditions basin field) are written INTO the style:
 *                every `["feature-state", k]` in such a layer becomes a lookup of the feature's own id in a literal
 *                table — N1's proven `match`-style fallback, applied by a style swap. Missing is null, as feature-state.
 *   images       the hatches, the gauge pill and the route's pin are the web's generated images (`images.ts`), handed
 *                to the binding's `Images` as PNGs under a name per theme (`nativeImageName`: the binding adds a name
 *                once and never replaces it, so a theme swap asks for new names); every reference here is renamed to
 *                match (T74, N2-F1).
 *   values       a colouring channel's style-borne values (`values`: the day's profiles, the salmon day, the basins'
 *                standings) are written in through the same adapter call the web makes (`setChannel`); the basins are
 *                an area layer, so theirs become `areaData`. The per-section labels are feature-state, set by the
 *                engine as each tile arrives (`native/useColouring.ts`), exactly as on the web.
 *
 * A style change is a whole style swap on native (≈ 225 ms measured, N1), which also drops feature-state: the map
 * re-applies status and selection when the new style has loaded (`Map.native.tsx`).
 */
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { applyLook, type Look } from "./look";
import { creditId, MAP_CREDITS_DRAWN } from "./credits";
import type { TileEndpoints } from "./endpoints";
import { runtimeStyle } from "./runtime-style";
import { ATLAS_SOURCES, SECTION_SOURCE_LAYERS, stateTarget } from "./sections";
import { NO_VALUES, STYLE_META, type ChannelValues } from "./style";
import { themedImageRefs } from "./images";
import { COPY_CASING, COPY_FILL, COPY_LINE } from "./water-copy";

type Json = Record<string, unknown>;
type StyleLayer = { id: string; paint?: Json; layout?: Json; minzoom?: number; maxzoom?: number };

export interface NativeStyleInput extends Look {
  at: TileEndpoints;
  /** Required here (a phone's style is built whole); applied by `applyLook` like every other part of the look. */
  basemap: "map" | "satellite";
  /** The gauge points the style starts with (GeoJSON text); later ticks go to the `gauges` source directly. */
  gauges: string;
  /**
   * Per-feature values for the layers a phone cannot address by feature-state (`areaLayer`), as `MapProps.data` holds
   * them: layer → feature id → state. Written into the style; a change is a style swap.
   */
  areaData?: Record<string, Record<string, Record<string, unknown>>>;
  /**
   * (T175) Where the water copy stops, per kind (`WaterCopy.ceiling`): its layers' `maxzoom`. Null (or absent): nothing
   * of that kind is kept, and the layer keeps the style's own range.
   */
  copyCeiling?: { line: number | null; fill: number | null };
  /** The colouring engine's style-borne channel values (`useNativeColouring`); absent: none installed. */
  values?: ChannelValues;
}

/**
 * A style layer whose features have no id on a phone: an atlas layer keyed by a property that is not a section's.
 * Its per-feature values go into the style (`areaData`), never to feature-state, which would address nothing.
 */
export function areaLayer(layerId: string): boolean {
  const prop = areaKey(layerId);
  if (prop === undefined || prop === "section_id") return false;
  const t = stateTarget(layerId);
  return t !== null && ATLAS_SOURCES.includes(t.source) && !SECTION_SOURCE_LAYERS.includes(t.sourceLayer);
}

/** The property an area layer's features are keyed by: its own, or (a follower, the field's seam) its fill's. */
const areaKey = (layerId: string): string | undefined =>
  STYLE_META.featureIds[layerId] ?? STYLE_META.featureIds[STYLE_META.follows?.[layerId] ?? ""];

/** Every `["feature-state", key]` in `expr` replaced by `lookup(key)`. */
function withoutFeatureState(expr: unknown, lookup: (key: string) => unknown): unknown {
  if (!Array.isArray(expr)) return expr;
  if (expr[0] === "feature-state" && typeof expr[1] === "string") return lookup(expr[1]);
  if (expr[0] === "literal") return expr;
  return expr.map((x) => withoutFeatureState(x, lookup));
}

/** The adapter's handle over a style JSON: paint, visibility and zoom range written into the layers. */
function jsonHandle(layers: StyleLayer[]): MapHandle {
  const byId = new Map(layers.map((l) => [l.id, l]));
  return {
    setVisibility: (id, v) => {
      const l = byId.get(id);
      if (l) l.layout = { ...l.layout, visibility: v ? "visible" : "none" };
    },
    setZoomRange: (id, minzoom, maxzoom) => {
      const l = byId.get(id);
      if (!l) return;
      l.minzoom = minzoom;
      if (maxzoom !== undefined) l.maxzoom = maxzoom;
    },
    setPaint: (id, prop, value) => {
      const l = byId.get(id);
      if (l) l.paint = { ...l.paint, [prop]: value };
    },
    // feature-state is not part of a style: it goes to the atlas source on the live map
    setFeatureState: () => {},
    clearFeatureStates: () => {},
  };
}

export function nativeStyle(o: NativeStyleInput): { style: Json; errors: Error[] } {
  // only the modes a layer has reach runtimeStyle (which throws on any other); `applyLook` reports the rest
  const known = Object.fromEntries(Object.entries(o.modes ?? {})
    .filter(([id, mode]) => STYLE_META.colorModes[id]?.[mode] !== undefined));
  // a deep copy: runtimeStyle shares the generated layers' objects with MAP_STYLE, which the web reads too
  const s = JSON.parse(JSON.stringify(runtimeStyle({ ...o.at, gauges: o.gauges }, o.theme, known))) as
    { sources: Record<string, Json>; layers: StyleLayer[] } & Json;
  const errors: Error[] = [];
  const h = jsonHandle(s.layers);
  const ids = new Set(s.layers.map((l) => l.id));
  // a fresh adapter per style: it holds the modes and channel values it painted, and this style is all of them
  const adapter = baseAdapter("native");
  applyLook(adapter, h, (id) => ids.has(id), o, (e) => errors.push(e));
  // THE CHANNELS' VALUES, through the web's own call (after the look, as the engine makes it on the web)
  const values = o.values ?? NO_VALUES;
  for (const ch of Object.keys(values) as (keyof ChannelValues)[])
    if (values[ch] !== null) adapter.setChannel(h, ch, values[ch] as never, o.theme);
  const basins = values.standing?.basins;
  const field = STYLE_META.field?.layer;
  const areaData: NativeStyleInput["areaData"] = basins && field
    ? { ...o.areaData, [field]: { ...o.areaData?.[field],
        ...Object.fromEntries(Object.entries(basins).map(([id, v]) => [id, { standing: v } as Record<string, unknown>])) } }
    : o.areaData;
  // the ground (`o.basemap`) is part of the look: `applyLook` set it, as it does on the web map
  // ESRI'S CREDIT ONLY WHILE ITS IMAGERY IS DRAWN. The web's control lists only sources a visible layer uses; MapLibre
  // Native's attribution is not documented to, and a credit for imagery that is not on screen (offline, or the drawn
  // map chosen) is a claim about the map that is false. The phone's style is rebuilt on every basemap change anyway.
  const imagery = s.sources.imagery;
  if (imagery && o.basemap !== "satellite") delete imagery.attribution;
  // (T235) the same for a mode's or an overlay's credit: only while the look drew its layer (credits.ts)
  for (const c of MAP_CREDITS_DRAWN) {
    const l = s.layers.find((x) => x.id === creditId(c));
    if (l?.layout?.visibility === "none") delete s.sources[creditId(c)]?.attribution;
  }

  for (const id of ATLAS_SOURCES) if (s.sources[id]) delete s.sources[id]!.promoteId;
  for (const l of s.layers) {
    // the generated images under this theme's names (`nativeImages`)
    if (l.paint?.["fill-pattern"] !== undefined)
      l.paint = { ...l.paint, "fill-pattern": themedImageRefs(l.paint["fill-pattern"], o.theme) };
    if (l.layout?.["icon-image"] !== undefined)
      l.layout = { ...l.layout, "icon-image": themedImageRefs(l.layout["icon-image"], o.theme) };
    // AN AREA LAYER'S VALUES, by its own property: `["get", <the feature's id as text>, <the table>]` is null where
    // the table has no entry — exactly what feature-state answers for a feature nobody set
    if (areaLayer(l.id)) {
      const prop = areaKey(l.id)!;
      // a follower (the field's seam) reads the values of the fill it follows
      const rows = areaData?.[l.id] ?? areaData?.[STYLE_META.follows?.[l.id] ?? ""] ?? {};
      const lookup = (key: string) => ["get", ["to-string", ["get", prop]], ["literal",
        Object.fromEntries(Object.entries(rows).filter(([, st]) => st[key] !== undefined)
          .map(([id, st]) => [id, st[key]]))]];
      const each = (props: Json) =>
        Object.fromEntries(Object.entries(props).map(([k, v]) => [k, withoutFeatureState(v, lookup)]));
      if (l.paint) l.paint = each(l.paint);
      if (l.layout) l.layout = each(l.layout);
    }
  }
  // (T175) the water copy stops where the atlas draws the water itself
  for (const l of s.layers) {
    const z = l.id === COPY_FILL ? o.copyCeiling?.fill : l.id === COPY_LINE || l.id === COPY_CASING ? o.copyCeiling?.line
      : undefined;
    if (z !== undefined && z !== null) { l.minzoom = 0; l.maxzoom = z; }
  }
  return { style: s, errors };
}
