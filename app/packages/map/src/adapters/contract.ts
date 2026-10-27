/**
 * The ONE map surface an app may use. Both adapters implement exactly this — anything a
 * platform could do beyond it is a capability the other silently lacks.
 *
 * Note what is absent: no way to add a layer, and no way to set a colour outright.
 * Layers come from the generated catalog; colours come from tokens; the active
 * colouring comes from a view. That is what keeps the platforms looking the same.
 * What each layer is painted with is `../paint.ts`, shared with the style composer.
 */
import type { Bbox, SectionKey } from "@app/core";
import { pinCollection, TRACE_PINS_SOURCE, TRACE_SOURCE, type TracePin } from "../trace-overlay";
import type { TraceCollection } from "../trace-geometry";
import { COPY_CASING, COPY_FILL, COPY_LINE, type LitFeature } from "../water-copy";
import { BASEMAP_LABELS, OWN_LABELS, basemapLabelPaint, ownLabelOnImagery } from "../labels";
import {
  MAP_STYLE, NO_VALUES, STYLE_META, followerMode, followersOf, isRuntimeLayer, profileChannel,
  resolveTheme, type ChannelValues, type Tokens,
} from "../style";
import { casingPaint, copyFillPaint, copyLinePaint, NOACCESS_LAYER, noaccessPaint, NOBASELINE_LAYER, nobaselineOpacity,
         paintFor, ringPaint, SELECTION_CASING, SELECTION_RINGS, type SelectionCue } from "../paint";

export type { ChannelValues } from "../style";

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
    return profileChannel(STYLE_META.colorModes[layerId]?.[mode]);
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
