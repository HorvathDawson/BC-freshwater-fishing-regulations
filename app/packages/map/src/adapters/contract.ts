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
      const floor: unknown =
        ["case", selected, Number(tokens["width.stream.highlight"]), 0];
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
    if (o !== undefined) out[`${type}-opacity`] = o;
  }

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
  return out;
}

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
