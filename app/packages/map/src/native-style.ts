/**
 * THE STYLE JSON MapLibre Native IS HANDED (N2). Platform-free, so it is tested in node.
 *
 * The web map sets its look imperatively on a live map. The native binding takes a whole style (`mapStyle`), so the
 * same look is written into a COPY of the same `runtimeStyle()` output, through the same adapter and the same
 * `applyLook` sequence (a `MapHandle` whose writes land in the JSON). Two things differ, both because of MapLibre Native:
 *
 *   promoteId    dropped. MapLibre Native ignores it (N1 §2); a section is its MVT id (`./sections.ts`). The area
 *                layers keep their property styling and have no feature id on a phone.
 *   fill-pattern dropped. The hatches (and the gauge pill) are images generated at run time and added with
 *                `map.addImage` on the web; the binding has no raw-pixel image API, and a patterned layer whose image is
 *                missing draws NOTHING — closures and marsh would vanish. Without the pattern each falls back to its
 *                own `fill-color`, so the area is drawn plain rather than lost. (Open: hatches on native.)
 *
 * A style change is a whole style swap on native (≈ 225 ms measured, N1), which also drops feature-state: the map
 * re-applies status and selection when the new style has loaded (`Map.native.tsx`).
 */
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { applyLook, type Look } from "./look";
import { runtimeStyle, type TileEndpoints } from "./runtime-style";
import { STYLE_META } from "./style";

type Json = Record<string, unknown>;
type StyleLayer = { id: string; paint?: Json; layout?: Json; minzoom?: number; maxzoom?: number };

export interface NativeStyleInput extends Look {
  at: TileEndpoints;
  basemap: "map" | "satellite";
  /** The gauge points the style starts with (GeoJSON text); later ticks go to the `gauges` source directly. */
  gauges: string;
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
  adapter.setBasemap(h, o.basemap);

  const atlas = s.sources.atlas;
  if (atlas) delete atlas.promoteId;
  for (const l of s.layers)
    if (l.paint && "fill-pattern" in l.paint) {
      const { "fill-pattern": _dropped, ...rest } = l.paint;
      l.paint = rest;
    }
  return { style: s, errors };
}
