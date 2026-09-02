/**
 * The ONE map surface an app may use. Both adapters implement exactly this — anything a
 * platform could do beyond it is a capability the other silently lacks.
 *
 * Note what is absent: no way to add a layer, and no way to set a colour outright.
 * Layers come from the generated catalog; colours come from tokens; the active
 * colouring comes from a view. That is what keeps the platforms looking the same.
 */
import {
  MAP_STYLE, STYLE_META, colorExpression, defaultView, resolveTheme, type Tokens,
} from "../style";

/** The thin platform-specific part each adapter supplies. */
export interface MapHandle {
  setVisibility(layerId: string, visible: boolean): void;
  setPaint(layerId: string, prop: string, value: unknown): void;
  setFeatureState(layerId: string, featureId: string, state: Record<string, unknown>): void;
  clearFeatureStates(layerId: string): void;
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
  /** Highlight by id via feature-state — never by mutating paint. */
  highlight(h: MapHandle, featureIds: string[]): void;
  /** Push the per-feature data a colour mode reads (status, discharge, …). */
  setData(h: MapHandle, layerId: string, values: Record<string, Record<string, unknown>>): void;
}

/** The MapLibre paint property that carries colour, per layer type. */
export function colourPropFor(type: string | undefined): string {
  return type === "line" ? "line-color" : type === "fill" ? "fill-color" : "circle-color";
}

/**
 * Everything painted on one layer for one mode: colour, width, opacity.
 *
 * Shared by the runtime style and the adapter ON PURPOSE. The style used to ship with no
 * paint at all and the adapter applied it on `load`, so every map showed a frame or two of
 * MapLibre's defaults — black lines over the basemap — before snapping to the palette.
 * One function means the first frame and every frame after it are painted the same way.
 */
export function paintFor(layerId: string, mode: string, tokens: Tokens):
    Record<string, unknown> {
  const type = (MAP_STYLE.layers.find((l) => l.id === layerId) as { type?: string } | undefined)
    ?.type;
  /**
   * SELECTION IS A PAINT RULE, and it was not one.
   *
   * `highlight()` has always written `{selected: true}` into feature-state, and nothing
   * anywhere read it — so every "show me this route" and every tap highlight set a flag
   * into the void. The route panel drew its two end markers over a map where the water
   * between them was the same colour as all the other water.
   *
   * Wrapped here rather than in the style so it applies to EVERY mode automatically: a
   * highlight has to survive whichever colouring the layer happens to be in, and a per-mode
   * expression would be one more place for the two to drift apart.
   */
  const selected = ["boolean", ["feature-state", "selected"], false];
  const base = colorExpression(layerId, mode, tokens);
  const isHighlightable = STYLE_META.highlightable.some((h) => h.id === layerId);
  const out: Record<string, unknown> = {
    [colourPropFor(type)]: isHighlightable && tokens["color.highlight"] !== undefined
      ? ["case", selected, tokens["color.highlight"], base]
      : base,
  };

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
      if (spec.mode === "sqrt") {
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
    // A HIGHLIGHTED REACH IS ALSO THICKER. Colour alone is not enough on a 1.4 px line
    // over a busy basemap, and the route panel's whole job is to pick a chain of reaches
    // out of hundreds of others.
    //
    // `max`, not a multiplier: `width.stream.highlight` is a width IN PIXELS (3.2), so
    // multiplying by it would make the Fraser thirty pixels wide while leaving a creek
    // thinner than the highlight is supposed to guarantee. A floor gives every selected
    // reach at least that weight and leaves a big river its own.
    if (isHighlightable && type === "line" && tokens["width.stream.highlight"] !== undefined)
      out[widthKey] = ["case", selected,
                       ["max", out[widthKey], Number(tokens["width.stream.highlight"])],
                       out[widthKey]];
  }

  const dash = (STYLE_META.dashes ?? {})[layerId];
  if (dash !== undefined && type === "line") {
    const pattern = tokens[dash];
    if (Array.isArray(pattern)) out["line-dasharray"] = pattern;
  }

  /**
   * THE WATER STANDS ASIDE FOR THE REGIONAL VIEW.
   *
   * In `standing` mode below z7 the atlas has already dropped all but a few mainstems, and
   * what survives is a thin scribble that reads as noise beside the station haze drawn over
   * it (see `gauge-haze`). Fading it out is what lets the low-zoom answer be one thing
   * rather than two competing ones; by z7 the rivers are back at full weight and the haze
   * is gone.
   *
   * Only in this mode. The Regulations view at the same zoom is answering a question about
   * specific water, so its lines must not disappear.
   */
  if (mode === "standing" && type === "line")
    out["line-opacity"] = ["interpolate", ["linear"], ["zoom"], 5, 0.12, 7, 1];

  const opacity = (STYLE_META.opacities ?? {})[layerId];
  if (opacity !== undefined && type) {
    const o = tokens[opacity];
    if (o !== undefined) out[`${type}-opacity`] = o;
  }
  return out;
}

export function baseAdapter(platform: "native" | "web"): MapAdapter {
  const typeOf = (layerId: string) =>
    (MAP_STYLE.layers.find((x) => x.id === layerId) as { type?: string } | undefined)?.type;

  const apply = (h: MapHandle, viewId: string, themeName: string, overrides: Tokens = {}) => {
    const view = STYLE_META.views.find((v) => v.id === viewId);
    if (!view) throw new Error(`unknown view "${viewId}"`);
    const tokens = resolveTheme(themeName, overrides);
    for (const [layerId, mode] of Object.entries(view.modes))
      for (const [prop, value] of Object.entries(paintFor(layerId, mode, tokens)))
        h.setPaint(layerId, prop, value);
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
      for (const [prop, value] of Object.entries(paintFor(layerId, mode, tokens)))
        h.setPaint(layerId, prop, value);
    },

    highlight(h, featureIds) {
      for (const { id } of STYLE_META.highlightable) {
        h.clearFeatureStates(id);
        for (const f of featureIds) h.setFeatureState(id, f, { selected: true });
      }
    },

    setData(h, layerId, values) {
      if (!STYLE_META.colorModes[layerId]) throw new Error(`unknown layer "${layerId}"`);
      for (const [featureId, state] of Object.entries(values))
        h.setFeatureState(layerId, featureId, state);
    },
  };
}

export const DEFAULT_VIEW = defaultView;
