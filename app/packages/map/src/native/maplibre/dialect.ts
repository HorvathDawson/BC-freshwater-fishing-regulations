/**
 * (T1154) MAPLIBRE NATIVE'S DIALECT of the phone's style: what `nativeStyle` (the neutral composition, `native-style.ts`)
 * hands this engine differently, and why. Moved here unchanged from `native-style.ts`; `tools/style-golden.test.ts` holds
 * the composed styles byte for byte across the move.
 *
 *   tiles        the archives as `pmtiles://file://…` (`PMTILES` over the installed files). The binding's patch reads tile
 *                ids only for a `pmtiles://file` address (`MLRNVectorSource.kt`); on any other the map loses its colours
 *                with no error, so `native-style.test.ts` pins it.
 *   promoteId    dropped. MapLibre Native ignores it (N1 §2); a section is its MVT id (`../../sections.ts`). The area
 *                layers keep their property styling and have no feature id on a phone.
 *   area ids     the layers keyed by a PROPERTY (`basin_id`; not a section, so no MVT id) cannot take feature-state on
 *                a phone. Their per-feature values (`areaData`: the Conditions basin field) are written INTO the style:
 *                every `["feature-state", k]` in such a layer becomes a lookup of the feature's own id in a literal
 *                table — N1's proven `match`-style fallback, applied by a style swap. Missing is null, as feature-state.
 *   images       the hatches, the gauge pill and the route's pin are the web's generated images (`images.ts`), handed
 *                to the binding's `Images` as PNGs under a name per theme (`nativeImageName`: the binding adds a name
 *                once and never replaces it, so a theme swap asks for new names); every reference here is renamed to
 *                match (T74, N2-F1).
 */
import type { StyleDialect } from "../../native-style";
import { ATLAS_SOURCES, SECTION_SOURCE_LAYERS, stateTarget } from "../../sections";
import { STYLE_META } from "../../style";
import { themedImageRefs } from "../../images";
import { PMTILES } from "../../tile-access";

type Json = Record<string, unknown>;

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

export const maplibreDialect: StyleDialect = {
  id: "maplibre-native",
  tiles: PMTILES,
  finish(s, { theme, areaData }) {
    for (const id of ATLAS_SOURCES) if (s.sources[id]) delete s.sources[id]!.promoteId;
    for (const l of s.layers) {
      // the generated images under this theme's names (`nativeImages`)
      if (l.paint?.["fill-pattern"] !== undefined)
        l.paint = { ...l.paint, "fill-pattern": themedImageRefs(l.paint["fill-pattern"], theme) };
      if (l.layout?.["icon-image"] !== undefined)
        l.layout = { ...l.layout, "icon-image": themedImageRefs(l.layout["icon-image"], theme) };
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
  },
};
