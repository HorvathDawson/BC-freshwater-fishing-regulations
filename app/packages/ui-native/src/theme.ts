import { resolveTheme, type MapChrome } from "@app/map";
/**
 * Tokens for the phone components.
 *
 * OUTCOME COLOURS ARE NOT DEFINED HERE. They are read from the generated map themes, so
 * the pill, the reach stripe and the legend are painted with the exact hex the river beside
 * them is painted with. They used to be typed out twice and the two copies had DIFFERENT
 * VALUES — `#D81E1E` here against `#c0392b` on the map for the same word "closed" — under a
 * comment claiming "the two agree because they name the same outcomes". Naming the same
 * outcome is not agreeing about it.
 *
 * Outcome colour is paired with a dash pattern and always with a WORD, so the answer never
 * depends on hue alone. A colour-blind theme swaps the palette and nothing else changes.
 */
export interface Palette {
  /** The ground behind the app shell — darker than `wash`, so a sheet reads as lifted. */
  page: string;
  card: string; wash: string; tint: string; line: string; line2: string;
  ink: string; sub: string; faint: string;
  closed: string; restricted: string; open: string; unknown: string; quiet: string;
  accent: string; onAccent: string;
  /** Live-feed accent. Distinct from `accent`, which means "you chose this". */
  live: string;
  /** Stocking recency ramp, most recent first. Rule 29: every bucket coloured. */
  stock: readonly [string, string, string, string, string];
  /**
   * Elevation. `boxShadow` — the `shadow*` props are deprecated in React Native 0.76+ and
   * warn on every render on web.
   */
  lift: { boxShadow: string; elevation: number };
  /** Corner radii. See RADIUS — the whole app reads these, nothing writes a literal. */
  r: Radii;
}

/**
 * HOW ROUND THE APP IS, in one place.
 *
 * Every corner used to be a literal at its call site: ten `borderRadius: 999` pills, eight
 * 12s, a 13, a 14, two 11s and a 22. Making the app less round meant finding thirty numbers
 * and hoping; changing one in isolation is how a screen ends up a millimetre off from the
 * one beside it, which nobody reports and everybody feels.
 *
 * SQUARE. A first pass set these to v1's 2-4px and it still read as rounded — at phone
 * scale a 3px radius is not a soft corner, it is a corner that looks like it was meant to
 * be soft and failed. Brutalism is not "less rounded", it is not rounded: the edge is a cut,
 * the border is a hairline of ink, and the shadow is an offset slab with no blur.
 *
 * The three names are kept even though they are all 0. They say what KIND of thing a corner
 * belongs to, so a future decision to soften one class of surface is one edit here rather
 * than a hunt through thirty call sites — which is the state this replaced.
 */
export interface Radii {
  /** Buttons, inputs, sheets, cards. The default. */
  box: number;
  /** Chips, swatches, small marks inside a box. */
  chip: number;
  /** What used to be a full pill. */
  pill: number;
  /** Explicitly nothing, where a call site wants to say so. */
  none: number;
}

export const RADIUS: Radii = { box: 0, chip: 0, pill: 0, none: 0 };

/**
 * A HARD OFFSET SHADOW, not a blur.
 *
 * v1's signature, used on the search bar and every floating panel:
 * `box-shadow: 4px 4px 0 rgba(0,0,0,1)`. It reads as a printed object on paper rather than
 * a pane of glass hovering over one, and it survives being small — a 14px blur on a 28px
 * control is a smudge. The app had `0 4px 14px rgba(21,24,28,0.10)`, which is the generic
 * material elevation this design is explicitly not.
 */
const HARD = (offset: number, colour: string) =>
  ({ boxShadow: `${offset}px ${offset}px 0 ${colour}`, elevation: offset });

/** Outcome colours for one theme, from the generated style. The map is the source. */
const outcomes = (theme: string) => {
  const v = resolveTheme(theme) as Record<string, string>;
  return {
    closed: v["color.status.closed"]!, restricted: v["color.status.restricted"]!,
    open: v["color.status.open"]!, unknown: v["color.status.unknown"]!,
  };
};

export const LIGHT: Palette = {
  page: "#EFEFEC", card: "#FFFFFF", wash: "#F7F7F5", tint: "#EEEFEB",
  line: "#E5E6E1", line2: "#D3D5CF", ink: "#15181C", sub: "#6C737A", faint: "#99A0A6",
  ...outcomes("light"),
  quiet: "#C3C8CD", accent: "#5F26E0", onAccent: "#FFFFFF", live: "#04879B",
  stock: ["#12873F", "#5E9B12", "#B58105", "#8A6A3A", "#8E979E"],
  lift: HARD(3, "rgba(21,24,28,0.90)"), r: RADIUS,
};

export const DARK: Palette = {
  page: "#0A0B0D", card: "#15181B", wash: "#101215", tint: "#1E2227",
  line: "#252A2F", line2: "#343B42", ink: "#F0F2F0", sub: "#98A0A7", faint: "#6E767D",
  ...outcomes("dark"),
  quiet: "#2F363D", accent: "#A97CFF", onAccent: "#100A22", live: "#37D6EA",
  stock: ["#2ED573", "#94D82D", "#FFC93C", "#C79A5E", "#69737B"],
  // On a dark ground a black shadow is invisible, so the offset slab is the LINE
  // colour — the same "printed object" read, achieved with the only contrast there is.
  lift: HARD(3, "rgba(0,0,0,0.85)"), r: RADIUS,
};

/** Blue/orange instead of red/green — and the MAP swaps with it, from the same theme. */
export const CVD: Palette = { ...LIGHT, ...outcomes("cvd") };

export const THEMES = { light: LIGHT, dark: DARK, cvd: CVD } as const;
export type ThemeName = keyof typeof THEMES;

export type OutcomeKey = "closed" | "restricted" | "open" | "unknown";

export function outcomeColour(p: Palette, outcome: OutcomeKey): string {
  return p[outcome];
}

/** Dash pattern per outcome, so the answer survives without colour. */
export function outcomeDash(outcome: OutcomeKey): readonly number[] | undefined {
  return outcome === "unknown" ? [4, 3] : outcome === "restricted" ? [10, 4] : undefined;
}

/**
 * The flow ramp, low-for-the-date to high, read from the map style.
 *
 * ONE DEFINITION. The legend used to carry its own seven hex values while the map's
 * `standing` mode resolved to three shades of one blue: the legend promised red through
 * cyan and the map drew a wash of blue. A legend that disagrees with its map is worse than
 * no legend, because it teaches the reader a scale that is not there.
 */
export function flowRamp(theme: string): readonly string[] {
  const v = resolveTheme(theme) as Record<string, string>;
  // f1..f7 only. The style also carries a stop at -1 for "gauged, but no history to
  // compare against", and that is a STATE rather than a point on the scale — putting it on
  // the legend would imply purple sits below "low", which is not what it means.
  return [1, 2, 3, 4, 5, 6, 7].map((i) => v[`color.flow.f${i}`]!);
}

/**
 * The colours MapLibre's own controls wear. Derived HERE, from the palette, so the zoom
 * stack and the scale bar cannot be on a different theme from the app around them — which
 * they were, because `controls.css` read CSS variables nobody set and every rule quietly
 * used its light-theme fallback.
 */
export function mapChrome(p: Palette, theme: string): MapChrome {
  return {
    ink: p.ink, card: p.card, tint: p.tint, sub: p.sub,
    // A black slab reads on light ground and disappears on dark, so the dark theme leans
    // on pure black against a lighter card instead. Same "printed object", either way.
    shadow: theme === "dark" ? "rgba(0,0,0,0.85)" : "rgba(21,24,28,0.90)",
  };
}
