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
export type Tokens = Record<string, string | number>;

export const MAP_STYLE = style as unknown as {
  version: number; sources: Record<string, unknown>; layers: { id: string }[];
};
export const STYLE_META = meta as unknown as {
  groups: LayerGroup[];
  views: MapView[];
  layerGroup: Record<string, string>;
  highlightable: { id: string; featureIdProperty: string }[];
  colorModes: Record<string, Record<string, ColorMode>>;
  widths: Record<string, string>;
  themes: Record<string, Tokens>;
  tokens: Record<string, { type: string; themeable: boolean }>;
  enums: Record<string, string[]>;
};

export const layerIds = (): string[] => MAP_STYLE.layers.map((l) => l.id);
export const toggleableGroups = (): LayerGroup[] => STYLE_META.groups.filter((g) => g.toggleable);
export const views = (): MapView[] => STYLE_META.views;
export const defaultView = (): MapView => {
  const v = STYLE_META.views.find((x) => x.default);
  if (!v) throw new Error("no default view");
  return v;
};

/** A theme by name, merged with a user's colour overrides. */
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
