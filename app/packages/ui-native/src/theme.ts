import { resolveTheme } from "@app/map";
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
}

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
  lift: { boxShadow: "0 4px 14px rgba(21,24,28,0.10)", elevation: 4 },
};

export const DARK: Palette = {
  page: "#0A0B0D", card: "#15181B", wash: "#101215", tint: "#1E2227",
  line: "#252A2F", line2: "#343B42", ink: "#F0F2F0", sub: "#98A0A7", faint: "#6E767D",
  ...outcomes("dark"),
  quiet: "#2F363D", accent: "#A97CFF", onAccent: "#100A22", live: "#37D6EA",
  stock: ["#2ED573", "#94D82D", "#FFC93C", "#C79A5E", "#69737B"],
  lift: { boxShadow: "0 5px 16px rgba(0,0,0,0.50)", elevation: 6 },
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
  return [1, 2, 3, 4, 5, 6, 7].map((i) => v[`color.flow.f${i}`]!);
}
