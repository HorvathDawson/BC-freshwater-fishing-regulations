/**
 * The ONE map surface an app may use. Both adapters implement exactly this — anything a
 * platform could do beyond it is a capability the other silently lacks.
 *
 * Note what is absent: no way to add a layer, and no way to set a colour outright.
 * Layers come from the generated catalog; colours come from tokens; the active
 * colouring comes from a view. That is what keeps the platforms looking the same.
 */
import type { Bbox, SectionKey } from "@app/core";
import { pinCollection, TRACE_PINS_SOURCE, TRACE_SOURCE, type TracePin } from "../trace-overlay";
import type { TraceCollection } from "../trace-geometry";
import { COPY_CASING, COPY_FILL, COPY_LINE, type LitFeature } from "../water-copy";
import { BASEMAP_LABELS, OWN_LABELS, basemapLabelPaint, ownLabelOnImagery } from "../labels";
import {
  MAP_STYLE, NO_VALUES, STYLE_META, colorExpression, followerMode, followersOf, isRuntimeLayer,
  resolveTheme, type ChannelValues, type Tokens,
} from "../style";

export type { ChannelValues } from "../style";

/**
 * "MEASURED HERE, BUT NOTHING TO COMPARE IT TO" — the dashed line over a stream whose standing is the -1 sentinel
 * (`runtime-style.ts`). A runtime layer outside the catalogue, so the adapter paints it with the stream's own mode:
 * the `standing` mode reads the number the colour reads, every other mode hides it.
 */
export const NOBASELINE_LAYER = "stream-nobaseline";

/**
 * WHAT IS SELECTED IS OUTLINED, NEVER RECOLOURED (owner 2026-10-21, on the water map's parts: "we should have the color
 * there still but add outline for the current selected one"). The selected sections keep the colour their mode gives
 * them — the regulation status on the Map tab, the standing on Conditions — and are ringed in `color.highlight` (the
 * water you tapped or searched for; held apart from every status colour by tools/water-status-cvd.test.ts, and read on
 * the light and dark grounds and on satellite): a CASING under the stream line, a RING on a lake's or a wetland's
 * outline. Runtime layers (`runtime-style.ts`) on the sections' own source-layers, shown by the same feature-state
 * `selected` the one `highlight` call sets — no second mechanism — and painted with the stream (`paint` below), so a
 * theme or mode change repaints them in the same call.
 */
export const SELECTION_CASING = "selection-casing";
/**
 * HOW A HIGHLIGHT MARKS WHAT IT LIGHTS — one decision, this type (T151, owner 2026-10-23: "the bold in the section map
 * view is also weird and makes the reg legend hard to follow"):
 *   "outline"  a tapped or searched water on the browse map: feature-state `selected` — the casing / ring above, and a
 *              width floor (`width.stream.highlight`).
 *   "dim"      the part you are on in a WATER'S MAP VIEW: feature-state `part` — no casing, no ring, no thicker line; the
 *              water's other parts (`water`) are drawn at `opacity.water.others`, and the part and the others share one
 *              width floor (`width.stream.water`), so every part reads against the legend exactly as the legend draws it.
 */
export type SelectionCue = "outline" | "dim";
const PART = ["boolean", ["feature-state", "part"], false];
const WATER = ["boolean", ["feature-state", "water"], false];
export const SELECTION_RINGS = { lake: "selection-ring-lake", wetland: "selection-ring-wetland" } as const;
/** The outline each side of a selected stream line, and a selected polygon's ring, in px. */
export const CASING_PX = 2.5;
export const RING_PX = 3;
const SELECTED = ["boolean", ["feature-state", "selected"], false];
const OUTLINE_SHOWN = ["case", SELECTED, 1, 0];

/** The stream casing's paint: the stream's own width (`streamWidth`, with its floors) plus the outline each side. */
export function casingPaint(streamWidth: unknown, tokens: Tokens): Record<string, unknown> {
  return { "line-color": tokens["color.highlight"],
           "line-width": withinZoomCurve(streamWidth, (v) => ["+", v, 2 * CASING_PX]),
           "line-opacity": OUTLINE_SHOWN };
}

/** A selected lake's or wetland's ring. */
export function ringPaint(tokens: Tokens): Record<string, unknown> {
  return { "line-color": tokens["color.highlight"], "line-width": RING_PX, "line-opacity": OUTLINE_SHOWN };
}

/**
 * (T175) THE WATER COPY'S LINE (`water-copy.ts`): the stream's own paint for its mode — colour, width, opacity, every
 * feature-state it reads — with the width floored at the highlight's (`width.stream.highlight`): the copy is drawn as a
 * highlight, at zooms where the water's own width is a hairline. Inside the zoom curve, as every floor here is.
 */
export function copyLinePaint(stream: Record<string, unknown>, tokens: Tokens): Record<string, unknown> {
  const floor = Number(tokens["width.stream.highlight"]);
  const w = stream["line-width"];
  return { ...stream, ...(w !== undefined && Number.isFinite(floor)
    ? { "line-width": withinZoomCurve(w, (v) => ["max", v, floor]) } : {}) };
}

/** (T175) A copied lake's fill: the lake's own paint, its fill properties only (a lake's paint is a fill's). */
export function copyFillPaint(lake: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(lake).filter(([k]) => k.startsWith("fill-") && k !== "fill-pattern"));
}

/** The dash's opacity for the stream layer in `mode`: 1 where the stream's standing is the -1 sentinel, else 0. */
export function nobaselineOpacity(mode: string): unknown {
  return mode === "standing" ? ["case", ["==", ["to-number", ["feature-state", "standing"], 0], -1], 1, 0] : 0;
}

/**
 * (T165) NO PUBLIC ACCESS IS A DOTTED GREY LINE (owner 2026-10-23: "maybe the stream color through no access areas should
 * be dotted grey? like under lakes but a little more visible?"). `line-dasharray` cannot vary per feature, so — like
 * `stream-nobaseline` — the dots are their own runtime layer over every stream (`runtime-style.ts`), shown only where
 * the stream's mode paints the section `no_access`, and the `stream` layer itself is hidden there (`paintFor`), so the
 * gaps are gaps. Which sections those are is the colouring's own answer: the profile mode's day (@app/core
 * `dayProfiles`, `no_access` labels) or the categorical mode's `no_access` member — never a second test of access.
 */
export const NOACCESS_LAYER = "stream-noaccess";

/** Where `mode` paints a stream section `no_access`, as a boolean expression; null where the mode never does. */
export function noAccessShown(layerId: string, mode: string, values: ChannelValues | null = NO_VALUES): unknown | null {
  const m = STYLE_META.colorModes[layerId]?.[mode];
  if (!m || !("categories" in m) || !m.categories["no_access"]) return null;
  if (m.scale === "profile") {
    const labels = (values ?? NO_VALUES).status?.no_access ?? [];
    return labels.length ? ["in", ["coalesce", ["feature-state", m.property], ""], ["literal", [...labels]]] : null;
  }
  if (m.scale === "categorical") return ["==", ["feature-state", m.property], "no_access"];
  return null;
}

/** The dots' opacity for the stream layer in `mode`: 1 on a no-access section, else 0 (every mode without one). */
export function noaccessOpacity(mode: string, values: ChannelValues | null = NO_VALUES): unknown {
  const shown = noAccessShown("stream", mode, values);
  return shown === null ? 0 : ["case", shown, 1, 0];
}

/**
 * The dots' paint: `color.status.no_access` (the legend's grey), a little stronger than the under-lake route it is
 * modelled on (`under_lake`: `width.underlake` 0.5 → 1.5 px at `opacity.route`, `dash.route`) — a thicker line at full
 * strength (1.2 → 2.8 px against the route's 0.8 → 2.4 at 0.35), with the route's own dash, so it reads as the same idea ("not water for you") without fading into the ground.
 */
export function noaccessPaint(mode: string, tokens: Tokens, values: ChannelValues | null = NO_VALUES): Record<string, unknown> {
  return {
    "line-color": tokens["color.status.no_access"],
    "line-width": ["interpolate", ["linear"], ["zoom"], 6, 1.2, 10, 1.6, 13, 2.2, 16, 2.8],
    "line-dasharray": tokens["dash.route"],
    "line-opacity": noaccessOpacity(mode, values),
  };
}

/** The thin platform-specific part each adapter supplies. */
export interface MapHandle {
  setVisibility(layerId: string, visible: boolean): void;
  /** Narrow a layer to a zoom range for the current view — see `minzoomByView`. */
  setZoomRange?(layerId: string, minzoom: number, maxzoom?: number): void;
  setPaint(layerId: string, prop: string, value: unknown): void;
  setFeatureState(layerId: string, featureId: SectionKey, state: Record<string, unknown>): void;
  clearFeatureStates(layerId: string): void;
  /**
   * Replace a runtime GeoJSON source's contents (the route overlay). Optional so a handle
   * built for one job (a highlight, the basemap switch) need not stub it.
   */
  setSourceData?(sourceId: string, data: unknown): void;
}

/**
 * WHAT EACH RENDERER OWES THE CONTRACT (N2, audit F14): the platform half, behind one signature, so `Map.web.tsx` and
 * `Map.native.tsx` drive their SDKs through the same calls and `parity.test.ts` can run one op script against both and
 * compare what each SDK was asked to do. `./web.ts` (maplibre-gl) and `./native.ts` (MapLibre Native through the RN
 * binding) implement it; everything they share — the tap box, the tap order, the id conversion, where a layer's
 * feature-state lives — comes from `../sections.ts`, so the two cannot differ in what a section is.
 *
 * Two things are NOT here, on purpose, because the binding makes them declarative on a phone:
 *   the style   the web restyles a live map (`applyLook` on a `MapHandle`); the native map is handed a whole style
 *               (`nativeStyle`, the binding's `mapStyle`), so a look change is a style swap.
 *   the points  the gauges are a GeoJSON source: `setData` on the web, the `<GeoJSONSource data>` prop on a phone.
 * Both ends of each are tested where they live (`native-style.test.ts`, `adapter-contract.render.test.tsx`).
 */
export interface MapRenderer {
  readonly platform: "native" | "web";
  /**
   * Feature-state through the adapter's `MapHandle` (`setData`, `highlight`): the layer's own source-layer, the
   * section's NUMBER id; clearing removes only `selected`, per feature, from `lit()`. On a phone only the section
   * source-layers take it (no MVT id elsewhere; their values are in the style).
   */
  states(lit: () => readonly SectionKey[]): MapHandle;
  /** Frame a box: `[w, s, e, n]`, padding in px, animated over `duration` ms. */
  fitBounds(b: Bbox, opts: { padding: number; duration: number }): void;
  /** (T48) Put the camera at a centre and zoom, animated over `duration` ms — a camera restored exactly (search's cancel). */
  easeTo(c: { lon: number; lat: number; zoom: number }, opts: { duration: number }): void;
  /** The section under a tap at screen `(x, y)`: the 24 px box, `TAP_GROUPS` order; the group names the hit. */
  press(x: number, y: number): Promise<{ layer: string; id: number } | null>;
  /** The sections on screen (`VISIBLE_LAYERS`), as numbers, once each. */
  visible(): Promise<number[]>;
  /** The box of the given sections' features the tiles have delivered (a searched water's own shape), or null. */
  waterBox(ids: readonly number[]): Promise<Bbox | null>;
  /**
   * THE SAME PICK, WITH ITS SHAPES (T175): every piece of the given sections the tiles have delivered, per
   * source-layer (`WATER_SOURCE_LAYERS`), the id a number. `waterBox` is its extent; the water copy keeps its lines.
   */
  litFeatures(ids: readonly number[]): Promise<LitFeature[]>;
  /** The camera's box, `[w, s, e, n]`. */
  bounds(): Promise<Bbox>;
}

export interface MapAdapter {
  readonly platform: "native" | "web";
  style(): typeof MAP_STYLE;
  layerIds(): string[];
  /** Toggle a GROUP — apps never address individual layers. */
  setGroupVisible(h: MapHandle, groupId: string, visible: boolean): void;
  /** Switch the whole picture: which colouring each layer uses. */
  applyView(h: MapHandle, viewId: string, themeName: string, overrides?: Tokens): void;
  /**
   * One layer's colouring, without touching the others.
   *
   * A view is a preset; this is the control the design actually offers. Riffle lets you
   * hold streams on Rules while lakes show Stocked, because they answer different
   * questions — forcing both through a single "view" was my invention, not the design's.
   */
  setLayerMode(h: MapHandle, layerId: string, mode: string, themeName: string,
               overrides?: Tokens): void;
  /** Re-apply colours after a theme or user-palette change, keeping the current view. */
  applyTheme(h: MapHandle, viewId: string, themeName: string, overrides?: Tokens): void;
  /**
   * Highlight by id via feature-state — never by mutating paint: `selected` on `featureIds`, and `water` on `water`
   * (P15: the other parts of the water whose part is selected — drawn at a width floor in their own colour, so the
   * river reads as one water around the part you are on). Every key of both is cleared first. `cue` (T151,
   * `SelectionCue`): "outline" (default) lights `featureIds` `selected`; "dim" — a water's Map view — lights them `part`.
   */
  highlight(h: MapHandle, featureIds: SectionKey[], water?: readonly SectionKey[], cue?: SelectionCue): void;
  /**
   * The ground: the drawn basemap, or satellite imagery over it.
   *
   * Not `setGroupVisible`: the `imagery` group is deliberately not toggleable, so no screen
   * can reach it through `groups`. This is the one switch, on the contract, so the native
   * adapter owes the same behaviour the web one has.
   *
   * AND EVERY NAME FOLLOWS THE GROUND (`labels.ts`): light text on a dark halo over imagery, the theme's inks on the drawn
   * map — ours and the basemap's — when `themeName` is given (the look pass always gives it). Without it only the ground
   * moves.
   */
  setBasemap(h: MapHandle, kind: "map" | "satellite", themeName?: string, overrides?: Tokens): void;
  /**
   * A CHANNEL'S VALUES (the colouring engine, `colouring.ts`). `status`: the day's label sets (@app/core `dayProfiles`),
   * which the `profile` modes paint — repaints exactly the layers now in a profile mode, and is remembered, so a later
   * theme or mode change paints the same day; null paints them plain. `standing`: the basins' standings, set on each
   * basin by its id (the sections' own are the engine's labels) — no paint at all; null clears them. Zero per-section
   * calls here: the sections are the renderer's to label (`Colouring`).
   */
  setChannel<C extends keyof ChannelValues>(h: MapHandle, channel: C, values: ChannelValues[C],
                                            themeName: string, overrides?: Tokens): void;
  /** Push the per-feature data a colour mode reads (status, discharge, …). */
  setData(h: MapHandle, layerId: string, values: Record<string, Record<string, unknown>>): void;
  /**
   * THE ROUTE OVERLAY (owner 2026-10-21): the routes' lines, copied out of the tiles by
   * `traceOverlay` (the shared helper both renderers call), and the pins. `lines` null
   * leaves the drawn lines as they are — the pins land at once and the lines follow when
   * their tiles are read. On the contract so the native map owes the same overlay.
   */
  setTrace(h: MapHandle, pins: readonly TracePin[], lines: TraceCollection | null): void;
}

/** The MapLibre paint property that carries colour, per layer type. */
// Module-private: an implementation detail of `paintFor`.
function colourPropFor(type: string | undefined): string {
  return type === "line" ? "line-color" : type === "fill" ? "fill-color"
    : type === "symbol" ? "text-color" : "circle-color";
}

/**
 * Everything painted on one layer for one mode: colour, width, opacity.
 *
 * Shared by the runtime style and the adapter ON PURPOSE. The style used to ship with no
 * paint at all and the adapter applied it on `load`, so every map showed a frame or two of
 * MapLibre's defaults — black lines over the basemap — before snapping to the palette.
 * One function means the first frame and every frame after it are painted the same way.
 */
export function paintFor(layerId: string, mode: string, tokens: Tokens,
                         given: ChannelValues | null = NO_VALUES): Record<string, unknown> {
  // null is "nothing installed", the same as `NO_VALUES` — what a caller with no channel at hand passes
  const values = given ?? NO_VALUES;
  /*
   * A PROFILE MODE WITH NO DAY IS THE PLAIN MAP. The day is installed only on a proven pair whose index passed its digest
   * checks (`setChannel`); without one there is no answer to draw, and the app's rule is that "not asked" stays plain
   * rather than going grey or — worse — keeping another release's colours.
   */
  if (values.status === null && STYLE_META.colorModes[layerId]?.[mode]?.scale === "profile")
    return paintFor(layerId, "plain", tokens);
  const type = (MAP_STYLE.layers.find((l) => l.id === layerId) as { type?: string } | undefined)
    ?.type;
  /**
   * SELECTION IS A PAINT RULE — BUT NOT A COLOUR (owner 2026-10-21).
   *
   * `highlight()` writes `{selected: true}` into feature-state. It used to swap the colour here for `color.highlight`,
   * so the part you were on lost the very thing the map is for — its regulation status — and read as "magenta". The
   * colour is now the mode's, always; what is selected is OUTLINED by the selection layers (`SELECTION_CASING`,
   * `SELECTION_RINGS`), and a selected stream is drawn at least `width.stream.highlight` wide (below).
   */
  const selected = SELECTED;
  const isHighlightable = STYLE_META.highlightable.some((h) => h.id === layerId);
  let out: Record<string, unknown> = { [colourPropFor(type)]: colorExpression(layerId, mode, tokens, values) };

  const spec = STYLE_META.widths[layerId];
  const widthKey = type === "line" ? "line-width" : type === "circle" ? "circle-radius" : null;
  if (spec !== undefined && widthKey !== null) {
    if (typeof spec === "string") {
      const w = tokens[spec];
      if (w !== undefined) out[widthKey] = w;
    } else {
      // v1's formula: width is LINEAR in the attribute, and both the intercept and the
      // slope move with zoom. Not a ramp over the attribute — that shape differentiates
      // every stream at every zoom, and the low-zoom province becomes noise.
      //
      // `["get", ...]` and not `["feature-state", ...]`: stream order ships in the tile and
      // never changes, so line weight must not wait on the app pushing anything.
      const scale = Number(tokens[spec.token] ?? 1);
      const z: unknown[] = ["interpolate", ["linear"], ["zoom"]];
      if (spec.mode === "zoom") {
        // NO ATTRIBUTE AT ALL — a curve in the camera and nothing else. The route through a
        // lake is a construction line: it says the river continues, not how big it is, and
        // drawing it at the river's own weight makes it read as more river.
        for (const [at, base] of spec.ramp) z.push(at, scale * base);
      } else if (spec.mode === "sqrt") {
        // v1's lake/area outline: base + k * sqrt(area), clamped. Square-rooted because
        // area grows as the square of a shoreline, so a linear ramp makes one big lake
        // enormous and every small one invisible. MapLibre has no clamp, hence max(min()).
        const attr = ["coalesce", ["to-number", ["get", spec.by]], 10000];
        for (const [at, base, k, max] of spec.ramp)
          z.push(at, ["*", scale,
            ["max", ["min", ["+", base, ["*", k, ["sqrt", attr]]], max ?? base], base]]);
      } else {
        const attr = ["coalesce", ["to-number", ["get", spec.by]], 1];
        for (const [at, base, slope] of spec.ramp)
          z.push(at, ["*", scale, ["+", base, ["*", attr, slope]]]);
      }
      out[widthKey] = z;
    }
    /**
     * A HIGHLIGHTED REACH IS ALSO THICKER — as a FLOOR, applied INSIDE the zoom curve.
     *
     * `max`, not a multiplier: `width.stream.highlight` is a width in pixels (3.2), so
     * multiplying would make the Fraser thirty pixels wide while leaving a creek thinner
     * than the highlight is supposed to guarantee.
     *
     * THE ZOOM CURVE MUST STAY AT THE TOP. MapLibre allows exactly one zoom-based
     * `interpolate` per expression AND requires it to be the outermost one — a
     * "zoom-and-property" function is a zoom curve whose OUTPUTS are data expressions,
     * never the other way round. Both wrong shapes were shipped in turn: first
     * `case(selected, max(<curve>, 3.2), <curve>)` (two curves), then
     * `max(<curve>, case(...))` (one curve, but nested). Each one fails to PARSE, so the
     * layer is dropped and the map comes up unstyled with a single console line.
     * `tools/style-valid.test.ts` now runs the spec's own validator over this.
     */
    if (isHighlightable && type === "line" && tokens["width.stream.highlight"] !== undefined) {
      // P15: the water's other parts (`water`) take their own, lower floor — after `selected`, which wins. T151: the part
      // you are on in a water's Map view (`part`) takes the SAME floor as the others — one width for every part
      const floor: unknown = tokens["width.stream.water"] !== undefined
        ? ["case", selected, Number(tokens["width.stream.highlight"]),
           ["any", PART, WATER], Number(tokens["width.stream.water"]), 0]
        : ["case", selected, Number(tokens["width.stream.highlight"]), 0];
      out[widthKey] = withinZoomCurve(out[widthKey], (v) => ["max", v, floor]);
    }
  }

  const dash = (STYLE_META.dashes ?? {})[layerId];
  if (dash !== undefined && type === "line") {
    const pattern = tokens[dash];
    if (Array.isArray(pattern)) out["line-dasharray"] = pattern;
  }

  /**
   * THE WATER IS THE ANSWER IN THE REGIONAL VIEW — it used to be faded out.
   *
   * `standing` mode below z7 used to drop the rivers to 12% opacity so they would not
   * compete with a haze of blurred discs drawn over the land. That was the wrong thing
   * made quiet to protect the wrong thing: the discs claimed a condition for country no
   * gauge speaks for, while the water that DOES carry a reading was hidden.
   *
   * The discs are gone (see `stream-glow` in runtime-style) and the rivers are at full
   * weight at every zoom. The atlas has already thinned itself to the mainstems by z5, and
   * a mainstem is exactly the water most likely to be gauged — so what is left is what we
   * can actually answer for.
   */

  const edgeOpacity = (STYLE_META.edgeOpacities ?? {})[layerId];
  if (edgeOpacity !== undefined && type === "line") {
    const o = tokens[edgeOpacity];
    if (o !== undefined) out["line-opacity"] = o;
  }

  const opacity = (STYLE_META.opacities ?? {})[layerId];
  if (opacity !== undefined && type) {
    const o = tokens[opacity];
    // a zoom curve where the layer declares one (`opacity.ramp`): the token at full strength, times each factor
    const ramp = (STYLE_META.opacityRamps ?? {})[layerId];
    if (o !== undefined)
      out[`${type}-opacity`] = ramp && typeof o === "number"
        ? ["interpolate", ["linear"], ["zoom"], ...ramp.flatMap(([z, k]) => [z, Math.round(o * k * 1000) / 1000])]
        : o;
  }

  // (T165) a no-access stream section is drawn by its dots (`NOACCESS_LAYER`), never as a solid line under them
  if (layerId === "stream" && type === "line") {
    const shown = noAccessShown(layerId, mode, values);
    if (shown !== null) out["line-opacity"] = withinZoomCurve(out["line-opacity"] ?? 1, (v) => ["case", shown, 0, v]);
  }

  out = dimTheOthers(layerId, type, out, tokens);

  /**
   * A LABEL'S HALO — the paper showing through behind the word.
   *
   * Not part of the colour mode, because it is not an encoding: it is what makes a name
   * legible over landcover, a road and a contour at once. v1 drew every water label with
   * one, and a river name without one is readable on open ground and gone the moment it
   * crosses anything.
   */
  const halo = (STYLE_META.labelHalos ?? {})[layerId];
  if (halo !== undefined) {
    const c = tokens[halo.color];
    if (c !== undefined) {
      out["text-halo-color"] = c;
      out["text-halo-width"] = halo.width;
      out["text-halo-blur"] = 0.5;
    }
  }

  /**
   * A FILL PATTERN, AND ONLY OVER A STATIC COLOURING.
   *
   * MapLibre's `fill-pattern` overrides `fill-color` outright. So hatching the wetland
   * unconditionally would draw "open", "closed" and "we could not parse this" as the same
   * green weave — the answer replaced by the texture. The texture is for the mode that is
   * NOT answering anything, which is exactly `scale: "static"`.
   *
   * The token names a COLOUR, and the image is woven from it at runtime (src/hatch.ts).
   * The adapter registers it under `<layer>-hatch` before the style is applied.
   */
  const pattern = (STYLE_META.patterns ?? {})[layerId];
  if (pattern !== undefined && type === "fill"
      && STYLE_META.colorModes[layerId]?.[mode]?.scale === "static") {
    out["fill-pattern"] = `${layerId}-hatch`;
  }
  return solidField(layerId, type, out, tokens);
}

/**
 * THE FIELD AS ONE SOLID SHEET (`field.solid`, review 2026-10-22). Its basins were simplified one at a time, so they
 * gap and overlap along every shared border; drawn translucent, each overlap was a darker seam and the ground showed
 * through every gap and through the field itself. Opaque, with each colour pre-mixed on the paper at the field's own
 * opacity — the same colour the translucent field had on blank paper — nothing shows through, an overlap is invisible,
 * and the seam drawn under it (its follower, `outline.under`) closes the gaps in the basin's own colour.
 */
function solidField(layerId: string, type: string | undefined, out: Record<string, unknown>,
                    tokens: Tokens): Record<string, unknown> {
  const f = STYLE_META.field;
  if (!f?.solid || (layerId !== f.layer && STYLE_META.follows?.[layerId] !== f.layer)) return out;
  const over = tokens[f.solid.over], k = Number(tokens[f.solid.opacity]);
  if (typeof over !== "string" || !Number.isFinite(k)) return out;
  const prop = type === "line" ? "line-color" : "fill-color";
  return { ...out, [prop]: mixColours(out[prop], over, k), [`${type}-opacity`]: 1 };
}

/**
 * (T151) THE WATER'S OTHER PARTS, DIMMED: on a per-section layer (a highlightable one, or the edge that follows it —
 * feature-state is the source-layer's, so a lake's outline knows its lake is `water`), a line or fill lit `water` is drawn
 * at its own opacity times `opacity.water.others`. Only a water's Map view sets `water` (the "dim" cue), so the browse map
 * is untouched. The colour is never changed: the part you are on is the full-strength one.
 */
export function dimTheOthers(layerId: string, type: string | undefined, out: Record<string, unknown>,
                             tokens: Tokens): Record<string, unknown> {
  const k = Number(tokens["opacity.water.others"]);
  const owner = STYLE_META.follows?.[layerId] ?? layerId;
  if (!Number.isFinite(k) || (type !== "line" && type !== "fill")
      || !STYLE_META.highlightable.some((h) => h.id === owner)) return out;
  const prop = `${type}-opacity`;
  return { ...out, [prop]: withinZoomCurve(out[prop] ?? 1, (v) => ["case", WATER, ["*", v, k], v]) };
}

/** Every `#rrggbb` in `expr` laid over `ground` at `alpha`, per sRGB channel — what the renderer's blend gives. */
export function mixColours(expr: unknown, ground: string, alpha: number): unknown {
  if (Array.isArray(expr)) return expr[0] === "literal" ? expr : expr.map((x) => mixColours(x, ground, alpha));
  if (typeof expr !== "string" || !HEX6.test(expr) || !HEX6.test(ground)) return expr;
  const rgb = (h: string) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16));
  const [a, b] = [rgb(expr), rgb(ground)];
  return "#" + a.map((v, i) => Math.round(v * alpha + b[i]! * (1 - alpha)).toString(16).padStart(2, "0"))
    .join("").toUpperCase();
}
const HEX6 = /^#[0-9a-f]{6}$/i;

/**
 * Apply `f` to what an expression EVALUATES TO, leaving any zoom curve on the outside.
 *
 * A zoom-based `interpolate` has the shape `["interpolate", interp, ["zoom"], at, out, …]`,
 * and MapLibre requires it to be outermost. So a data-dependent adjustment cannot wrap the
 * curve; it has to be pushed into each of the curve's outputs, which is the same value
 * everywhere and legal. Anything that is not such a curve is transformed directly.
 */
function withinZoomCurve(expr: unknown, f: (value: unknown) => unknown): unknown {
  const isZoomCurve = Array.isArray(expr)
    && (expr[0] === "interpolate" || expr[0] === "step")
    && JSON.stringify(expr[expr[0] === "step" ? 1 : 2]) === '["zoom"]';
  if (!isZoomCurve) return f(expr);
  const a = expr as unknown[];
  const head = a[0] === "step" ? 2 : 3;           // step: op, input, default; interpolate: op, interp, input
  return [...a.slice(0, head),
          ...a.slice(head).map((v, i) => ((i % 2 === 0) ? v : f(v)))];
}

/**
 * The zoom floor a layer takes while coloured this way — see `minzoomByMode`.
 *
 * Called from BOTH places a mode is set, because a layer coloured one way and zoomed for
 * another is a layer that vanishes at some zooms and doubles up at others, and neither
 * throws. Reset to the style's own floor whenever the mode has no rule, or the restriction
 * would outlive the mode that asked for it.
 */
function zoomFor(h: MapHandle, layerId: string, mode: string | undefined, fieldOn: boolean): void {
  const byMode = (STYLE_META.minzoomByMode ?? {})[layerId];
  const field = STYLE_META.field;
  const covered = field !== null && field !== undefined && layerId in field.covers;
  if ((!byMode && !covered) || !h.setZoomRange) return;
  const base = covered ? field.covers[layerId]!
    : (MAP_STYLE.layers.find((x) => x.id === layerId) as { minzoom?: number } | undefined)?.minzoom ?? 0;
  /*
   * UNDER THE FIELD, NOTHING (owner 2026-10-22): while the basins are drawn, a covered layer starts where they stop. A
   * floor, never a ceiling — the mode's own rule still applies above it — and released with the field, so the Map tab and
   * the temperature view keep every layer's own floor.
   */
  const floor = Math.max(byMode?.[mode ?? ""] ?? base, covered && fieldOn ? field.handover : 0);
  h.setZoomRange(layerId, floor);
  // the selection's casing is drawn where its stream is, never as an outline round nothing
  if (layerId === "stream") h.setZoomRange(SELECTION_CASING, floor);
}

/** Is the field drawn in this mode (`field.drawnIn`)? */
const fieldDrawnIn = (mode: string | undefined): boolean =>
  mode !== undefined && (STYLE_META.field?.drawnIn ?? []).includes(mode);

/** The style group holding satellite imagery — see `layers.source.json`. */
export const IMAGERY_GROUP = "imagery";

export function baseAdapter(platform: "native" | "web"): MapAdapter {
  const typeOf = (layerId: string) =>
    (MAP_STYLE.layers.find((x) => x.id === layerId) as { type?: string } | undefined)?.type;

  /*
   * WHAT THIS MAP IS PAINTING, remembered — the one piece of state an adapter keeps (colour-latency AP). A `profile`
   * mode needs the day, and the day arrives on its own schedule: a theme toggle or a mode change must paint the day
   * already installed, and a day change must repaint exactly the layers now in a profile mode. Without it either order
   * of arrival leaves a layer painted from a day it was never given (verifier's design risk: "any theme or view toggle
   * silently turns every section to the missing grey").
   */
  const modeOf: Record<string, string> = {};
  let values: ChannelValues = NO_VALUES;
  /** The basins this map holds a standing for (`setChannel("standing")`), so one that loses it is cleared. */
  const basinsHeld = new Set<string>();
  const paint = (h: MapHandle, layerId: string, mode: string, tokens: Tokens) => {
    modeOf[layerId] = mode;
    const out = paintFor(layerId, mode, tokens, values);
    for (const [prop, value] of Object.entries(out)) h.setPaint(layerId, prop, value);
    // the stream's dash reads the stream's own mode (a runtime layer: absent from a style that has none, harmless)
    if (layerId === "stream") {
      h.setPaint(NOBASELINE_LAYER, "line-opacity", nobaselineOpacity(mode));
      h.setPaint(NOBASELINE_LAYER, "line-color", tokens["color.water.ungauged"]);
      // (T165) and so do the no-access dots: shown where this mode (and its day) paints a section no_access
      for (const [prop, value] of Object.entries(noaccessPaint(mode, tokens, values))) h.setPaint(NOACCESS_LAYER, prop, value);
      // the selection's outline goes with the stream: its width is the stream's, its colour the theme's
      for (const [prop, value] of Object.entries(casingPaint(out["line-width"], tokens)))
        h.setPaint(SELECTION_CASING, prop, value);
      for (const ring of Object.values(SELECTION_RINGS))
        for (const [prop, value] of Object.entries(ringPaint(tokens))) h.setPaint(ring, prop, value);
      // (T175) THE WATER COPY'S LINE takes the stream's own paint in the same call — the one colouring pathway
      const copy = copyLinePaint(out, tokens);
      for (const [prop, value] of Object.entries(copy)) h.setPaint(COPY_LINE, prop, value);
      for (const [prop, value] of Object.entries(casingPaint(copy["line-width"], tokens))) h.setPaint(COPY_CASING, prop, value);
    }
    // (T175) and a copied lake the lake's
    if (layerId === "lake") for (const [prop, value] of Object.entries(copyFillPaint(out))) h.setPaint(COPY_FILL, prop, value);
  };
  /** Is the field drawn now — its layer in a mode it draws in? */
  const fieldOn = () => STYLE_META.field != null && fieldDrawnIn(modeOf[STYLE_META.field.layer]);
  /**
   * The field's mode moved: draw it or not (`drawnIn` — its `plain` was a grey wash over the whole Map tab below the
   * handover, and a drawn layer is a fetched source), then give every layer it covers its floor again, in the mode each
   * was last set to. Covered layers no mode ever reached (the wetlands, the basemap's rivers) take their own floor.
   */
  const fieldMoved = (h: MapHandle) => {
    const f = STYLE_META.field;
    if (!f) return;
    h.setVisibility(f.layer, fieldOn());
    // its seam goes with it: drawn in every view's mode, it would otherwise outline the province on the Map tab
    for (const follower of followersOf(f.layer)) h.setVisibility(follower, fieldOn());
    for (const id of Object.keys(f.covers)) zoomFor(h, id, modeOf[id], fieldOn());
  };
  /** The channel a layer's mode reads its values from, if any. */
  const channelOf = (layerId: string, mode: string): keyof ChannelValues | null => {
    const m = STYLE_META.colorModes[layerId]?.[mode];
    return m?.scale === "profile" ? "status" : null;
  };

  const apply = (h: MapHandle, viewId: string, themeName: string, overrides: Tokens = {}) => {
    const view = STYLE_META.views.find((v) => v.id === viewId);
    if (!view) throw new Error(`unknown view "${viewId}"`);
    const tokens = resolveTheme(themeName, overrides);
    for (const [layerId, mode] of Object.entries(view.modes)) {
      if (isRuntimeLayer(layerId)) continue;   // drawn from a feed; see style.ts
      paint(h, layerId, mode, tokens);
      /*
       * AND THE ZOOM RANGE THIS VIEW GIVES IT.
       *
       * The Conditions view stands the rivers down below z9 and lets the basin field answer
       * instead — one answer per zoom, because a hairline coloured from a three-pixel line
       * and a catchment coloured from the same reading are the same claim drawn twice, and
       * the smaller one wins by being on top. Reset to the style's own floor in every other
       * view, or the restriction would persist after the reader switches away.
       */
      zoomFor(h, layerId, mode, fieldOn());
    }
    fieldMoved(h);
  };

  return {
    platform,
    style: () => MAP_STYLE,
    layerIds: () => MAP_STYLE.layers.map((l) => l.id),

    setGroupVisible(h, groupId, visible) {
      const g = STYLE_META.groups.find((x) => x.id === groupId);
      if (!g) throw new Error(`unknown layer group "${groupId}"`);
      if (!g.toggleable) throw new Error(`layer group "${groupId}" is not toggleable`);
      for (const [layerId, gid] of Object.entries(STYLE_META.layerGroup))
        if (gid === groupId) h.setVisibility(layerId, visible);
    },

    applyView: apply,
    applyTheme: apply,

    setLayerMode(h, layerId, mode, themeName, overrides = {}) {
      const modes = STYLE_META.colorModes[layerId];
      if (!modes) throw new Error(`unknown layer "${layerId}"`);
      if (!modes[mode])
        throw new Error(`layer "${layerId}" has no colour mode "${mode}" ` +
                        `(has ${Object.keys(modes).join(", ")})`);
      const tokens = resolveTheme(themeName, overrides);
      paint(h, layerId, mode, tokens);
      // THE ZOOM GOES WITH THE COLOUR. This is the path the app actually uses — it renders
      // one view and sets each layer's mode — so a rule applied only in `applyView` would
      // never fire in the running app.
      zoomFor(h, layerId, mode, fieldOn());
      /*
       * AND EVERY LAYER THAT FOLLOWS IT — a lake's outline. The app names only `lake`, so
       * without this the edge kept its plain water-blue under a status-, stocked- or
       * flow-coloured lake (owner 2026-10-20). Here, in the contract, so both renderers
       * get it from the one call they already make.
       */
      for (const follower of followersOf(layerId)) {
        const m = followerMode(follower, mode);
        paint(h, follower, m, tokens);
        zoomFor(h, follower, m, fieldOn());
      }
      if (layerId === STYLE_META.field?.layer) fieldMoved(h);
    },

    setChannel(h, channel, next, themeName, overrides = {}) {
      values = { ...values, [channel]: next };
      if (channel === "standing") {
        /*
         * THE FIELD'S VALUES, by the basin's own id: one feature-state per catchment (190 of them), and nothing for the
         * style — a basin is a property-keyed feature, so it needs no per-tile label, and a paint change would make the
         * whole atlas re-lay out. A basin that had a value and has none now is told so (null: `missing`).
         */
        const now = (next as ChannelValues["standing"])?.basins ?? {};
        for (const id of basinsHeld) if (!(id in now)) { h.setFeatureState("basin", id, { standing: null }); basinsHeld.delete(id); }
        for (const [id, v] of Object.entries(now)) { h.setFeatureState("basin", id, { standing: v }); basinsHeld.add(id); }
        return;
      }
      const tokens = resolveTheme(themeName, overrides);
      for (const [layerId, mode] of Object.entries(modeOf))
        if (channelOf(layerId, mode) === channel) paint(h, layerId, mode, tokens);
    },

    highlight(h, featureIds, water = [], cue = "outline") {
      // EVERY layer cleared before ANY is set: several layers draw one source-layer (`lake`, `lake_flowing`), so a
      // clear for the second would otherwise take back what the first just set
      for (const { id } of STYLE_META.highlightable) h.clearFeatureStates(id);
      const lit = cue === "dim" ? { part: true } : { selected: true };
      for (const { id } of STYLE_META.highlightable) {
        for (const f of featureIds) h.setFeatureState(id, f, lit);
        for (const f of water) h.setFeatureState(id, f, { water: true });
      }
    },

    setBasemap(h, kind, themeName, overrides = {}) {
      for (const [layerId, gid] of Object.entries(STYLE_META.layerGroup))
        if (gid === IMAGERY_GROUP) h.setVisibility(layerId, kind === "satellite");
      if (themeName === undefined) return;
      const tokens = resolveTheme(themeName, overrides);
      for (const id of OWN_LABELS) {
        if (kind === "satellite")
          for (const [prop, value] of Object.entries(ownLabelOnImagery(id, tokens))) h.setPaint(id, prop, value);
        else paint(h, id, modeOf[id] ?? "plain", tokens);
      }
      for (const id of BASEMAP_LABELS)
        for (const [prop, value] of Object.entries(basemapLabelPaint(id, tokens, kind))) h.setPaint(id, prop, value);
    },

    setData(h, layerId, values) {
      if (!STYLE_META.colorModes[layerId]) throw new Error(`unknown layer "${layerId}"`);
      for (const [featureId, state] of Object.entries(values))
        h.setFeatureState(layerId, featureId, state);
    },

    setTrace(h, pins, lines) {
      h.setSourceData?.(TRACE_PINS_SOURCE, pinCollection(pins));
      if (lines) h.setSourceData?.(TRACE_SOURCE, lines);
    },
  };
}
