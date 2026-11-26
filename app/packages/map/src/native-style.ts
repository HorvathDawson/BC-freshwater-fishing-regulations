/**
 * THE STYLE JSON THE PHONE'S MAP IS HANDED (N2). Platform-free, so it is tested in node.
 *
 * The web map sets its look imperatively on a live map. The native binding takes a whole style (`mapStyle`), so the
 * same look is written into a COPY of the same `runtimeStyle()` output, through the same adapter and the same
 * `applyLook` sequence (a `MapHandle` whose writes land in the JSON). What the ENGINE needs differently is its dialect
 * (`StyleDialect`; MapLibre Native's is `native/maplibre/dialect.ts`: the archives' addresses, promoteId dropped, the
 * area layers' values as literal tables, the generated images under a name per theme), applied last.
 *
 *   values       a colouring channel's style-borne values (`values`: the day's profiles, the salmon day, the basins'
 *                standings) are written in through the same adapter call the web makes (`setChannel`); the basins are
 *                an area layer, so theirs become `areaData`. The per-section labels are feature-state, set by the
 *                engine as each tile arrives (`native/useColouring.ts`), exactly as on the web.
 *
 * A style change is a whole style swap on native (≈ 225 ms measured, N1), which also drops feature-state: the map
 * re-applies status and selection when the new style has loaded (`Map.native.tsx`).
 */
import { IMAGERY } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { applyLook, type Look } from "./look";
import { creditId, MAP_CREDITS_DRAWN } from "./credits";
import type { TileEndpoints } from "./endpoints";
import { runtimeStyle } from "./runtime-style";
import { NO_VALUES, STYLE_META, type ChannelValues } from "./style";
import type { TileAccess } from "./tile-access";
import { pinCollection, TRACE_PINS_SOURCE, TRACE_SOURCE, type TracePin } from "./trace-overlay";
import type { TraceCollection } from "./trace-geometry";

type Json = Record<string, unknown>;
type StyleLayer = { id: string; paint?: Json; layout?: Json; minzoom?: number; maxzoom?: number };
/** Per-feature values by layer → feature id → state (`NativeStyleInput.areaData`). */
type AreaData = Record<string, Record<string, Record<string, unknown>>>;

/**
 * (T1154) WHAT ONE ENGINE NEEDS DIFFERENTLY in the style it is handed. One engine exists (MapLibre Native,
 * `native/maplibre/dialect.ts`); the phone's map passes it, and `tools/style-valid.test.ts` validates per dialect.
 */
export interface StyleDialect {
  /** The engine, by name (a test row names it). */
  readonly id: string;
  /** How its sources address the archives (`runtimeStyle`'s `tiles`). */
  readonly tiles: TileAccess;
  /**
   * Does this layer take its per-feature values IN THE STYLE (`areaData`) rather than by feature-state? The phone's map
   * sorts `MapProps.data` by it (`Map.native.tsx`): an engine that cannot address the layer's features answers true.
   */
  areaLayer(layerId: string): boolean;
  /** Its pass over the composed style, after the look, the values and the credits; it writes `s` in place. */
  finish(s: { sources: Record<string, Json>; layers: StyleLayer[] },
         o: { theme: string; areaData: AreaData | undefined }): void;
}

export interface NativeStyleInput extends Look {
  at: TileEndpoints;
  /** Required here (a phone's style is built whole); applied by `applyLook` like every other part of the look. */
  basemap: "map" | "satellite";
  /** The gauge points the style starts with (GeoJSON text); later ticks go to the `gauges` source directly. */
  gauges: string;
  /**
   * (T831) The route map's pins (its gauges' dots and "you are here") the style starts with; later changes go to the
   * `trace-pins` source directly. Baked in for the same reason as `gauges`: the binding's `GeoJSONSource` ADOPTS a source
   * the style already declares and never pushes the data it was mounted with (`MLRNSource.addToMap`, both platforms) —
   * only a later change of its `data` reaches it — so the pins a route map opens with must be in the style itself.
   */
  tracePins?: readonly TracePin[];
  /** (T831) The route lines copied out of the atlas so far (`traceOverlay`), into the `trace` source likewise. */
  traceLines?: TraceCollection;
  /**
   * Per-feature values for the layers a phone cannot address by feature-state (`areaLayer`), as `MapProps.data` holds
   * them: layer → feature id → state. Written into the style; a change is a style swap.
   */
  areaData?: AreaData;
  /** The colouring engine's style-borne channel values (`useNativeColouring`); absent: none installed. */
  values?: ChannelValues;
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

/** The field's values per basin id: its standing and its run state, in one state object each. */
function mergeArea(standing: Readonly<Record<string, number>> | undefined,
                   runs: Readonly<Record<string, string>> | undefined): Record<string, Record<string, unknown>> {
  const out: Record<string, Record<string, unknown>> = {};
  for (const [id, v] of Object.entries(standing ?? {})) out[id] = { standing: v };
  for (const [id, v] of Object.entries(runs ?? {})) out[id] = { ...out[id], rt: v };
  return out;
}

export function nativeStyle(o: NativeStyleInput, dialect: StyleDialect): { style: Json; errors: Error[] } {
  // only the modes a layer has reach runtimeStyle (which throws on any other); `applyLook` reports the rest
  const known = Object.fromEntries(Object.entries(o.modes ?? {})
    .filter(([id, mode]) => STYLE_META.colorModes[id]?.[mode] !== undefined));
  // a deep copy: runtimeStyle shares the generated layers' objects with MAP_STYLE, which the web reads too
  const s = JSON.parse(JSON.stringify(runtimeStyle({ ...o.at, gauges: o.gauges }, o.theme, known, dialect.tiles))) as
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
  // (T261) and the Runs tab's field: each watershed group's run state, beside its standing
  const runBasins = values.runs?.basins;
  const field = STYLE_META.field?.layer;
  const areaData: NativeStyleInput["areaData"] = (basins || runBasins) && field
    ? { ...o.areaData, [field]: { ...o.areaData?.[field], ...mergeArea(basins, runBasins) } }
    : o.areaData;
  // the ground (`o.basemap`) is part of the look: `applyLook` set it, as it does on the web map
  // THE IMAGERY'S CREDIT ONLY WHILE ITS IMAGERY IS DRAWN (the provider in use's source, `IMAGERY.source`: Esri's). The
  // web's control lists only sources a visible layer uses; MapLibre Native's attribution is not documented to, and a
  // credit for imagery that is not on screen (offline, or the drawn map chosen) is a claim about the map that is false.
  // The phone's style is rebuilt on every basemap change anyway.
  const imagery = s.sources[IMAGERY.source];
  if (imagery && o.basemap !== "satellite") delete imagery.attribution;
  // (T235) the same for a mode's or an overlay's credit: only while the look drew its layer (credits.ts)
  for (const c of MAP_CREDITS_DRAWN) {
    const l = s.layers.find((x) => x.id === creditId(c));
    if (l?.layout?.visibility === "none") delete s.sources[creditId(c)]?.attribution;
  }

  // (T831) the route map's pins and lines, as the web's `setTrace` writes them (`pinCollection`, `traceOverlay`)
  const pinSource = s.sources[TRACE_PINS_SOURCE];
  if (pinSource && o.tracePins?.length) pinSource.data = pinCollection(o.tracePins);
  const lineSource = s.sources[TRACE_SOURCE];
  if (lineSource && o.traceLines?.features.length) lineSource.data = o.traceLines;
  // THE ENGINE'S OWN PASS, last: its addresses went in above (`dialect.tiles`)
  dialect.finish(s, { theme: o.theme, areaData });
  // (T175, T399) the water copy stops where the atlas draws each section itself: its layers' filter reads each feature's
  // own `copy_until` (`WaterCopy.collection`) — data, so a phone never swaps its style for it
  return { style: s, errors };
}
