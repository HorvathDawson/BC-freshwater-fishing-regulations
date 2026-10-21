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
 *   fill-pattern dropped. The hatches (and the gauge pill) are images generated at run time and added with
 *                `map.addImage` on the web; the binding has no raw-pixel image API, and a patterned layer whose image is
 *                missing draws NOTHING — closures and marsh would vanish. Without the pattern each falls back to its
 *                own `fill-color`, so the area is drawn plain rather than lost; the gauge label asks for no pill and
 *                wears a halo in the paper instead. Follow-up: APP-PREP "N2-F1 — hatches and the gauge pill on native".
 *
 * A style change is a whole style swap on native (≈ 225 ms measured, N1), which also drops feature-state: the map
 * re-applies status and selection when the new style has loaded (`Map.native.tsx`).
 */
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { applyLook, type Look } from "./look";
import { runtimeStyle, type TileEndpoints } from "./runtime-style";
import { colour } from "./chrome";
import { SECTION_SOURCE_LAYERS, stateTarget } from "./sections";
import { STYLE_META } from "./style";

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
}

/**
 * A style layer whose features have no id on a phone: an atlas layer keyed by a property that is not a section's.
 * Its per-feature values go into the style (`areaData`), never to feature-state, which would address nothing.
 */
export function areaLayer(layerId: string): boolean {
  const prop = STYLE_META.featureIds[layerId];
  if (prop === undefined || prop === "section_id") return false;
  const t = stateTarget(layerId);
  return t?.source === "atlas" && !SECTION_SOURCE_LAYERS.includes(t.sourceLayer);
}

/** Every `["feature-state", key]` in `expr` replaced by `lookup(key)`. */
function withoutFeatureState(expr: unknown, lookup: (key: string) => unknown): unknown {
  if (!Array.isArray(expr)) return expr;
  if (expr[0] === "feature-state" && typeof expr[1] === "string") return lookup(expr[1]);
  if (expr[0] === "literal") return expr;
  return expr.map((x) => withoutFeatureState(x, lookup));
}

const adapter = baseAdapter("native");

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
  applyLook(adapter, h, (id) => ids.has(id), o, (e) => errors.push(e));
  // 'PROFILE' IS THE WEB'S per-tile colouring from `MapProps.status`, which this map never reads: it would paint plain
  // with no error (colour-ap review). Said out loud; the Shell gives a phone 'status' (`coloursEachTile`).
  for (const [id, mode] of Object.entries(o.modes ?? {}))
    if (mode === "profile")
      errors.push(new Error(`layer "${id}" is in the "profile" colour mode, which the native map cannot draw `
        + '(no per-tile colouring; give it "status" and pushed data — coloursEachTile)'));
  // the ground (`o.basemap`) is part of the look: `applyLook` set it, as it does on the web map

  const atlas = s.sources.atlas;
  if (atlas) delete atlas.promoteId;
  for (const l of s.layers) {
    if (l.paint && "fill-pattern" in l.paint) {
      const { "fill-pattern": _dropped, ...rest } = l.paint;
      l.paint = rest;
    }
    // AN AREA LAYER'S VALUES, by its own property: `["get", <the feature's id as text>, <the table>]` is null where
    // the table has no entry — exactly what feature-state answers for a feature nobody set
    if (areaLayer(l.id)) {
      const prop = STYLE_META.featureIds[l.id]!;
      const rows = o.areaData?.[l.id] ?? {};
      const lookup = (key: string) => ["get", ["to-string", ["get", prop]], ["literal",
        Object.fromEntries(Object.entries(rows).filter(([, st]) => st[key] !== undefined)
          .map(([id, st]) => [id, st[key]]))]];
      const each = (props: Json) =>
        Object.fromEntries(Object.entries(props).map(([k, v]) => [k, withoutFeatureState(v, lookup)]));
      if (l.paint) l.paint = each(l.paint);
      if (l.layout) l.layout = each(l.layout);
    }
  }
  // THE GAUGE LABEL WITHOUT ITS PILL (N2-F1): no icon asked for (it would be missing), a halo in the paper instead
  const label = s.layers.find((l) => l.id === "gauge-label");
  if (label) {
    label.layout = Object.fromEntries(Object.entries(label.layout ?? {}).filter(([k]) => !k.startsWith("icon-")));
    label.paint = { ...label.paint, "text-halo-color": colour(o.theme, "color.outside"), "text-halo-width": 1.5 };
  }
  return { style: s, errors };
}
