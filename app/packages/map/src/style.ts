/**
 * The generated style plus its runtime metadata. BOTH adapters import this and nothing
 * else. If an adapter needs "one more layer" or "a slightly different colour here", it
 * belongs in layers.source.json — otherwise the platforms diverge and nothing catches it.
 */
import { WATER_STATUS, type DayProfiles, type WaterStatus } from "@app/core";
import style from "../style/style.json" with { type: "json" };
import meta from "../style/style.meta.json" with { type: "json" };

export interface LayerGroup {
  id: string; label: string; toggleable: boolean; defaultVisible: boolean;
}
export interface MapView {
  id: string; label: string; default?: boolean;
  /** layerId -> colour mode name */
  modes: Record<string, string>;
  /**
   * Layers this view does not draw at all.
   *
   * Separate from the group toggles, which are a USER's choice among the things a view
   * offers. This is the view's own: the Conditions map does not draw administrative
   * boundaries because "which regulations table governs here" is not the question it asks.
   * The app cannot express that through `groups` — `admin` is deliberately not toggleable —
   * and trying produced a refused call and a boundary that kept drawing.
   */
  hide?: string[];
  /**
   * Layers this view ALONE draws — the mirror of `hide`. The style emits them hidden, so a
   * screen that does not name the view (the Map tab, the mini map, the spot picker) never
   * draws them: the Conditions view's flow arrows.
   */
  show?: string[];
}
export type TokenRef = { token: string };
export type ColorMode =
  | { label: string; scale: "static"; color: TokenRef }
  | { label: string; scale: "categorical"; property: string; enum?: string;
      categories: Record<string, TokenRef>; missing?: TokenRef }
  | { label: string; scale: "continuous"; property: string;
      stops: [number, TokenRef][]; missing: TokenRef }
  /** Categorical one step removed: feature-state holds a profile LABEL, the day says which labels are which member. */
  | { label: string; scale: "profile"; property: string; enum: string;
      categories: Record<string, TokenRef>; missing: TokenRef };

/**
 * THE STANDING CHANNEL'S VALUES, in the ramp's units (percent; -1 the "measured, no baseline" sentinel; NaN or absent
 * where nothing speaks): one per standing class, in class order, and one per basin. What a feed refresh or a horizon
 * change replaces. The colouring engine writes them into feature-state `standing` (a section's from its class, as its
 * tile arrives; a basin's by its id), which the `standing` mode has always read — so the style never changes with them
 * (a data-driven paint change makes MapLibre 5.24 re-lay out every loaded tile; measured in the colour-latency README).
 */
export interface StandingValues {
  /** A class's value (its index is the section's class), NaN where nothing speaks — computed when first asked. */
  section(classIndex: number): number;
  readonly basins: Readonly<Record<string, number>>;
}

/**
 * WHAT EACH CHANNEL'S MODES READ FROM THE STYLE: the day's label sets (status) and the classes' standings (standing).
 * Null where the channel has nothing installed.
 */
export interface ChannelValues {
  status: DayProfiles | null;
  standing: StandingValues | null;
}
export const NO_VALUES: ChannelValues = { status: null, standing: null };

/** A token is a colour, a number, or a dash pattern (an array of line-width multiples). */
export type Tokens = Record<string, string | number | number[]>;

export const MAP_STYLE = style as unknown as {
  version: number; sources: Record<string, unknown>; layers: { id: string }[];
};
export const STYLE_META = meta as unknown as {
  groups: LayerGroup[];
  views: MapView[];
  layerGroup: Record<string, string>;
  /** Generated edge id -> the fill whose colour mode it takes (`outline.follow`). */
  follows: Record<string, string>;
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
  /**
   * The zoom a layer takes while coloured a particular way — `{layer: {mode: minzoom}}`.
   *
   * Keyed on the MODE, not the view, because that is how the app switches: it renders one
   * view and sets each layer's colour mode. `standing` — "what is the water doing" — stands
   * the rivers down below z9 so the basin field can answer instead.
   */
  minzoomByMode: Record<string, Record<string, number>>;
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

/**
 * THE LAYERS THAT TAKE `layerId`'s COLOUR MODE — a lake's outline takes the lake's.
 *
 * An app sets modes per layer and names the FILL (`lake: "status"`); the edge has to be set
 * with it, or the lake is filled by its status and ringed in plain water-blue (owner
 * 2026-10-20: "the outlines of lakes should match their reg color"). The list is the
 * style's (`follows`, from `outline.follow`), never the app's.
 */
export const followersOf = (layerId: string): string[] =>
  Object.entries(STYLE_META.follows ?? {}).filter(([, fill]) => fill === layerId).map(([edge]) => edge);

/**
 * The mode a follower takes when its fill is set to `mode`: the same one, or its own plain
 * where it has no such mode. Every followed mode is generated onto the edge, so the
 * fallback is a guard, not a path.
 */
export const followerMode = (follower: string, mode: string): string =>
  STYLE_META.colorModes[follower]?.[mode] ? mode : "plain";

/**
 * `modes` with every follower given its fill's mode — unless the caller named it outright.
 * For the first frame (`runtimeStyle`); the adapter's `setLayerMode` does the same per call.
 */
export function withFollowers(modes: Readonly<Record<string, string>>): Record<string, string> {
  const out: Record<string, string> = { ...modes };
  for (const [edge, fill] of Object.entries(STYLE_META.follows ?? {}))
    if (out[edge] === undefined && modes[fill] !== undefined) out[edge] = followerMode(edge, modes[fill]!);
  return out;
}

export const layerIds = (): string[] => MAP_STYLE.layers.map((l) => l.id);
export const toggleableGroups = (): LayerGroup[] => STYLE_META.groups.filter((g) => g.toggleable);
export const views = (): MapView[] => STYLE_META.views;

/**
 * Layers a view does not draw. The LIST lives in the style; the TRIGGER is the app's.
 *
 * Not keyed on the rendered view, deliberately. The app renders `view="plain"` and drives
 * everything by setting each layer's MODE — so a rule keyed on "the conditions view" is a
 * rule that never fires, which is exactly how `minzoomByView` sat dead until it was rekeyed
 * to `minzoomByMode`. The screen knows which tab it is on; it asks for the list by name.
 */
export const hiddenLayers = (viewId: string): readonly string[] =>
  STYLE_META.views.find((v) => v.id === viewId)?.hide ?? [];
/** Layers only this view draws (`MapView.show`). The list is the style's; the trigger, as with `hiddenLayers`, the app's. */
export const shownLayers = (viewId: string): readonly string[] =>
  STYLE_META.views.find((v) => v.id === viewId)?.show ?? [];
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
export function colorExpression(layerId: string, modeName: string, t: Tokens,
                                values: ChannelValues | null = NO_VALUES): unknown {
  const mode = STYLE_META.colorModes[layerId]?.[modeName];
  if (!mode) throw new Error(`layer "${layerId}" has no colour mode "${modeName}"`);
  const val = (r: TokenRef) => t[r.token];
  const day = (values ?? NO_VALUES).status;

  if (mode.scale === "static") return val(mode.color);

  if (mode.scale === "profile") {
    /*
     * THE DAY, IN THE STYLE (colour-latency AP). One match over the profile labels the day places — the sets come from
     * @app/core `dayProfiles`, never worked out here (AGENTS 23). A label in no set, or no label at all, is `missing`.
     * `coalesce` to "" so a feature with no state yet is a string like every label (a match takes one label type —
     * the reason the labels are strings, which Android's paint needs anyway). Empty sets are left out: MapLibre refuses
     * an empty label list. With no day there is nothing to place, so every feature is `missing` — and `paintFor` never
     * gets that far, it paints the plain mode instead.
     */
    if (day === null) return val(mode.missing);
    const out: unknown[] = ["match", ["coalesce", ["feature-state", mode.property], ""]];
    for (const [member, ref] of Object.entries(mode.categories)) {
      const labels = day[member as keyof DayProfiles];
      if (labels === undefined) throw new Error(`profile mode "${modeName}" colours "${member}", which no day places`);
      if (labels.length) out.push([...labels], val(ref));
    }
    out.push(val(mode.missing));           // `none` (tidal, outside B.C.) and every unplaced label
    return out.length > 3 ? out : val(mode.missing);
  }

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

/**
 * THE COLOUR OF A WATER'S STATUS in one theme — the ONE resolver every surface uses (a search
 * row, the list of water near a town, and the map's status colouring when regulations return).
 * The status and its token are decided in @app/core (`WATER_STATUS`); the hex is the map's.
 */
export function waterStatusColour(theme: string, status: WaterStatus): string {
  const hex = resolveTheme(theme)[WATER_STATUS[status].token];
  if (typeof hex !== "string") throw new Error(`theme "${theme}" cannot paint ${status}`);
  return hex;
}
