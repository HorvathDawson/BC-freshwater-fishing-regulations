import { resolveTheme, waterStatusColour, type MapChrome } from "@app/map";
import { WATER_STATUSES, type WaterStatus } from "@app/core";
/**
 * Tokens for the phone components.
 *
 * THE STATUS COLOURS ARE NOT DEFINED HERE. `closed`, `restricted` and `open` are read from
 * the generated map themes (`color.status.*`), so a legend swatch is painted with the exact
 * hex the map beside it uses — the temperature bands, the no-access hatch. They used to be
 * typed out twice and the two copies had DIFFERENT VALUES. They no longer colour water by a
 * regulation outcome: regulations are not integrated (see `regulations.ts` in @app/core).
 *
 * A colour-blind theme swaps the palette and nothing else changes.
 */
export interface Palette {
  /** The ground behind the app shell — darker than `wash`, so a sheet reads as lifted. */
  page: string;
  card: string; wash: string; tint: string; line: string; line2: string;
  ink: string; sub: string; faint: string;
  closed: string; restricted: string; open: string; quiet: string;
  /**
   * A WATER'S STATUS (closed / its own regulations / base only), from the map's own resolver
   * (`waterStatusColour`), so a search row wears the colour the map line beside it does.
   */
  waterStatus: Readonly<Record<WaterStatus, string>>;
  accent: string; onAccent: string;
  /** Live-feed accent. Distinct from `accent`, which means "you chose this". */
  live: string;
  /**
   * A TOWN, not a water. The search results put places in their own group above the
   * waters, and the pin that marks a town on the map wears the same colour, so "this row is
   * that pin" reads without a legend. Warm, because everything that is water is blue or the
   * accent's violet; far from `accent`, which marks "you chose this" on the same small map.
   * Used as TEXT (the group's heading) as well as a mark, so it holds 4.5:1 on the card.
   */
  place: string;
  /**
   * The ground of the places group: a warm band behind the town rows, so the group reads as
   * NOT WATER before a word of it is read. Faint on purpose — `sub` and `faint` grey fall
   * under 4.5:1 on any tint strong enough to see, so secondary text inside the band is
   * drawn in `place`, which holds it; `ink` holds it with room to spare.
   */
  placeBand: string;
  /** Stocking recency ramp, most recent first. Rule 29: every bucket coloured. */
  stock: readonly [string, string, string, string, string];
  /**
   * IDENTITY, NOT MAGNITUDE — one tone per donor in a panel, in weight order.
   *
   * These say "this pin is that row", nothing more. They are deliberately not a ramp: a
   * ramp would encode weight a second time, and the bar and the percentage already do
   * that, so the strongest colour would land on the heaviest donor and read as a severity.
   * Four, because a panel holds at most four members (MAX_MEMBERS in panel.py).
   *
   * Separate from `stock` even though both are four-or-five arbitrary hues: reusing that
   * ramp would mean a change to how stocking recency is drawn silently repainting the
   * gauge map.
   *
   * AND EVERY ONE IS FAR FROM `accent`, which is the colour of the "you are here" marker
   * standing among them. The third of these was a violet at ΔE 11 from it, so on a map with
   * four gauges the reader could not tell which pin was the spot they had tapped — the one
   * thing on that map that is not a gauge. `theme.test.ts` holds the separation.
   */
  donor: readonly [string, string, string, string];
  /** Bathymetry: traced contours, and a scanned sheet. From the map theme. */
  survey: readonly [string, string];
  /** The plain basemap's own colours, for the basemap chooser's preview. */
  water: readonly [string, string];
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

/**
 * Legend swatches that must equal what the MAP draws, from the generated style.
 *
 * `LayersSheet` typed these in: #7BA4B8 for a scanned bathymetric sheet, #AED3E2/#9CC2D6 for
 * the plain basemap. Typed-in means theme-blind — the colour-blind theme showed the light
 * theme's swatches beside a map painted in different colours, which is the legend lying, and
 * the one form of lying a legend can do that nobody notices, because the swatch looks fine.
 */
const legend = (theme: string) => {
  const v = resolveTheme(theme) as Record<string, string>;
  return {
    survey: [v["color.survey.digitised"]!, v["color.survey.sheet"]!] as const,
    water: [v["color.lake.fill"]!, v["color.water.mapped"]!] as const,
  };
};

/** The status colours for one theme, from the generated style. The map is the source. */
const statusColours = (theme: string) => {
  const v = resolveTheme(theme) as Record<string, string>;
  return {
    closed: v["color.status.closed"]!, restricted: v["color.status.restricted"]!,
    open: v["color.status.open"]!,
    waterStatus: Object.fromEntries(WATER_STATUSES.map((s) => [s, waterStatusColour(theme, s)])) as
      Record<WaterStatus, string>,
  };
};

export const LIGHT: Palette = {
  page: "#EFEFEC", card: "#FFFFFF", wash: "#F7F7F5", tint: "#EEEFEB",
  line: "#E5E6E1", line2: "#D3D5CF", ink: "#15181C", sub: "#6C737A",
  // 4.54:1 on the card. #99A0A6 was 2.65:1 — a WCAG AA failure in the DEFAULT theme,
  // found while measuring the colour-blind one. "Faint" is a role, not a licence.
  faint: "#6E757B",
  ...statusColours("light"), ...legend("light"),
  // 3.02:1 — WCAG 1.4.11 for a non-text mark. #C3C8CD was 1.68:1.
  quiet: "#8A9196", accent: "#5F26E0", onAccent: "#FFFFFF", live: "#04879B",
  place: "#A64B00", placeBand: "#FBF0E6",
  stock: ["#12873F", "#5E9B12", "#B58105", "#8A6A3A", "#8E979E"],
  donor: ["#04879B", "#B5480B", "#A81E6B", "#0E7A3D"],
  lift: HARD(3, "rgba(21,24,28,0.90)"), r: RADIUS,
};

export const DARK: Palette = {
  page: "#0A0B0D", card: "#15181B", wash: "#101215", tint: "#1E2227",
  line: "#252A2F", line2: "#343B42", ink: "#F0F2F0", sub: "#98A0A7",
  // 4.51:1 on the dark card. #6E767D was 3.86:1 — on a dark ground "faint" has to get
  // LIGHTER to pass, which is the opposite move from the light theme and easy to miss.
  faint: "#7C858C",
  ...statusColours("dark"), ...legend("dark"),
  quiet: "#2F363D", accent: "#A97CFF", onAccent: "#100A22", live: "#37D6EA",
  place: "#F59E4B", placeBand: "#231A12",
  stock: ["#2ED573", "#94D82D", "#FFC93C", "#C79A5E", "#69737B"],
  donor: ["#37D6EA", "#FF9B54", "#FF7BB8", "#4ADE80"],
  // On a dark ground a black shadow is invisible, so the offset slab is the LINE
  // colour — the same "printed object" read, achieved with the only contrast there is.
  lift: HARD(3, "rgba(0,0,0,0.85)"), r: RADIUS,
};

/**
 * The colour-blind theme. NOT "the light theme with different status hues" — which is
 * exactly what it was, and why it did not work.
 *
 * `{ ...LIGHT, ...outcomes("cvd") }` swapped four colours and inherited everything else, so
 * every ramp added to the palette afterwards silently joined this theme wearing the light
 * theme's hues. Measured (see `tools/cvd.test.ts`), the inheritance shipped:
 *
 *   · `stock` — a five-step RECENCY ramp of green, olive, amber, brown, grey. Two steps sat
 *     ΔE 2.4 apart under protanopia. A reader could not order the thing the ramp exists to
 *     order.
 *   · `donor` — four IDENTITY hues, of which teal and green were ΔE 8.2 apart under
 *     tritanopia, so two pins on the route map were one colour.
 *   · `accent` — #5F26E0, which was ALSO `status.unknown` in this theme. The "you are here"
 *     marker and "no rule found" were the same colour, ΔE 0.0. Nothing catches that by eye,
 *     because each is correct on its own screen.
 *
 * So this theme now names every value it needs. The categorical hues follow Okabe & Ito's
 * Color Universal Design set and the ramp is cividis; both are published schemes built for
 * this, and anchoring on them is why the numbers hold rather than a run of lucky picks.
 */
export const CVD: Palette = {
  ...LIGHT,
  ...statusColours("cvd"), ...legend("cvd"),
  // Okabe–Ito, adjusted only where WCAG 1.4.11 (3:1 for a non-text mark) demanded it.
  // These are fills — a pin, a swatch, a stripe, a dot, a bar — never text.
  donor: ["#373708", "#670848", "#D77782", "#797000"],
  // cividis, trimmed at the light end: its true endpoint (#FFEA46) is invisible on a white
  // card. Sequential, so LIGHTNESS carries the order and no deficiency can flatten it.
  stock: ["#00224E", "#3B496C", "#6A6C71", "#A29A76", "#D3C164"],
  // "Nothing to say", as a mark. #C3C8CD was 1.68:1 against the card — a dot nobody with
  // any vision could find.
  quiet: "#8A9196",
};

export const THEMES = { light: LIGHT, dark: DARK, cvd: CVD } as const;
export type ThemeName = keyof typeof THEMES;

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
  const dark = theme === "dark";
  return {
    ink: p.ink, card: p.card, tint: p.tint, sub: p.sub, accent: p.accent,
    // A black slab reads on light ground and disappears on dark, so the dark theme leans
    // on pure black against a lighter card instead. Same "printed object", either way.
    shadow: dark ? "rgba(0,0,0,0.85)" : "rgba(21,24,28,0.90)",
    // MapLibre bakes near-black strokes into its glyph IMAGES, so on a dark control they
    // are black on near-black and the zoom buttons cannot be read. There is no colour
    // property to change — the image has to be inverted.
    iconFilter: dark ? "invert(1)" : "none",
    // Translucent either way: the scale bar sits over the map and the map should read
    // through it, which is what OpenStreetMap's own bar does.
    scaleBg: dark ? "rgba(21,24,27,0.72)" : "rgba(255,255,255,0.78)",
  };
}
