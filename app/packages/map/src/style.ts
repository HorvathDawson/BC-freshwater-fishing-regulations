/**
 * The generated style plus its runtime metadata. BOTH adapters import this and nothing
 * else. If an adapter needs "one more layer" or "a slightly different colour here", it
 * belongs in layers.source.json — otherwise the platforms diverge and nothing catches it.
 */
import style from "../style/style.json" with { type: "json" };
import meta from "../style/style.meta.json" with { type: "json" };

export interface LayerGroup {
  id: string; label: string; toggleable: boolean; defaultVisible: boolean;
}
export interface MapView {
  id: string; label: string; default?: boolean;
  /** layerId -> colour mode name */
  modes: Record<string, string>;
}
export type TokenRef = { token: string };
export type ColorMode =
  | { label: string; scale: "static"; color: TokenRef }
  | { label: string; scale: "categorical"; property: string; enum?: string;
      categories: Record<string, TokenRef>; missing?: TokenRef }
  | { label: string; scale: "continuous"; property: string;
      stops: [number, TokenRef][]; missing: TokenRef };
/** A token is a colour, a number, or a dash pattern (an array of line-width multiples). */
export type Tokens = Record<string, string | number | number[]>;

export const MAP_STYLE = style as unknown as {
  version: number; sources: Record<string, unknown>; layers: { id: string }[];
};
export const STYLE_META = meta as unknown as {
  groups: LayerGroup[];
  views: MapView[];
  layerGroup: Record<string, string>;
  highlightable: { id: string; featureIdProperty: string }[];
  colorModes: Record<string, Record<string, ColorMode>>;
  /** A plain token, or a ramp over a tile ATTRIBUTE (not feature-state). */
  /** layer id -> the tile property MapLibre must promote into `feature.id`. */
  featureIds: Record<string, string>;
  widths: Record<string, string | {
    token: string;
    /** The tile attribute the width is linear in — Strahler order, for streams. Absent
     *  for mode "zoom", where the width depends on nothing but the camera. */
    by?: string;
    /**
     * "linear": width = base + attr * slope        (streams, by Strahler order)
     * "sqrt":   width = clamp(base + k*sqrt(attr)) (lakes and areas, by area)
     * "zoom":   width = base                       (no attribute; the route through a lake)
     */
    mode?: "linear" | "sqrt" | "zoom";
    /** [zoom, base px, slope-or-k, max px for sqrt]. Scaled by the token. */
    ramp: [number, number, number, number?][];
  }>;
  opacities: Record<string, string>;
  dashes: Record<string, string>;
  /** layer id -> the halo behind its text: a paper token and a width in px. */
  labelHalos: Record<string, { color: string; width: number }>;
  /** layer id -> the colour its hatch is woven from, and how that weave is set. */
  patterns: Record<string, { token: string; ground: number; stripe: number; darken: number;
                             spacing: number; weight: number; cross: boolean }>;
  /** generated edge id -> a token for its line-opacity, when it should not be a hard edge. */
  edgeOpacities: Record<string, string>;
  themes: Record<string, Tokens>;
  /** Layers a view may switch that are added at runtime, not defined here. */
  runtimeLayers: string[];
  tokens: Record<string, { type: string; themeable: boolean }>;
  enums: Record<string, string[]>;
};

/**
 * Layers a view can switch that this file does not define.
 *
 * The gauge dots are drawn from a live feed, not from the atlas, so they have no
 * source-layer, no tile contract and no colour mode here — but a view still has to say
 * whether they are about flow or about water temperature. `runtime-style.ts` reads that
 * out of the same `modes` map as everything else; anything that walks the map and paints
 * by layer id has to step over them, or it asks for a colour mode that was never written.
 */
export const isRuntimeLayer = (layerId: string): boolean =>
  (STYLE_META.runtimeLayers ?? []).includes(layerId);

export const layerIds = (): string[] => MAP_STYLE.layers.map((l) => l.id);
export const toggleableGroups = (): LayerGroup[] => STYLE_META.groups.filter((g) => g.toggleable);
export const views = (): MapView[] => STYLE_META.views;
export const defaultView = (): MapView => {
  const v = STYLE_META.views.find((x) => x.default);
  if (!v) throw new Error("no default view");
  return v;
};

/** A theme by name, merged with a user's colour overrides. */
/** Every theme the generated style defines. The app must not hardcode this list. */
export const themeNames = (): string[] => Object.keys(STYLE_META.themes);

export function resolveTheme(name: string, overrides: Tokens = {}): Tokens {
  const base = STYLE_META.themes[name];
  if (!base) throw new Error(`unknown theme "${name}"`);
  for (const k of Object.keys(overrides)) {
    const def = STYLE_META.tokens[k];
    if (!def) throw new Error(`unknown token "${k}"`);
    if (!def.themeable) throw new Error(`token "${k}" is not themeable`);
  }
  return { ...base, ...overrides };
}

/**
 * The MapLibre paint expression for one layer under one colour mode, with tokens
 * resolved. Categorical and continuous modes read from FEATURE-STATE, so switching
 * view or theme never refetches a tile.
 */
export function colorExpression(layerId: string, modeName: string, t: Tokens): unknown {
  const mode = STYLE_META.colorModes[layerId]?.[modeName];
  if (!mode) throw new Error(`layer "${layerId}" has no colour mode "${modeName}"`);
  const val = (r: TokenRef) => t[r.token];

  if (mode.scale === "static") return val(mode.color);

  const read = ["feature-state", mode.property];
  if (mode.scale === "categorical") {
    const out: unknown[] = ["match", read];
    for (const [k, ref] of Object.entries(mode.categories)) out.push(k, val(ref));
    const fallback = mode.missing ?? mode.categories["unknown"];
    if (!fallback) throw new Error(`categorical mode "${modeName}" has no fallback colour`);
    out.push(val(fallback));
    return out;
  }
  const out: unknown[] = ["case", ["==", read, null], val(mode.missing),
                          ["interpolate", ["linear"], read]];
  const interp = out[3] as unknown[];
  for (const [at, ref] of mode.stops) interp.push(at, val(ref));
  return out;
}
