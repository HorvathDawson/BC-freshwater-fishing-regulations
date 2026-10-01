import { colour, translucent, waterStatusColour,
         mapChrome as chromeOf, type MapChrome } from "@app/map";
import { WATER_STATUSES, type WaterStatus } from "@app/core";
/**
 * The palette the phone components paint with.
 *
 * NOTHING HERE IS A COLOUR. Every value is read from the ONE token set —
 * `packages/map/style/tokens.json` (what a colour means) + `themes/*.json` (its value per
 * theme), generated into the style by `pnpm style:build`. The app's own surfaces are the
 * `color.ui.*` tokens; the status colours, the stocking ramp, the survey and basemap swatches
 * are the very tokens the map paints with, so a legend swatch is the exact hex of the line
 * beside it.
 *
 * WHY THE MAP'S TOKENS ARE THE SOURCE, and not this file. There were two palettes: this one
 * (thirty-odd hexes per theme) and the map's. `@app/map` sits BELOW this package, so it could
 * never read these — which is how the gauge pill's border, MapLibre's controls and the map
 * markers each ended up with a typed-in copy (or a fallback) of a colour defined here. The
 * token files are the only place both sides can read, they are already gated (literals
 * rejected, every theme complete, ΔE and contrast held at build time), and they already carry
 * a colour-blind theme. So the palette moved there, and this file derives.
 *
 * `tools/check-colours.mjs` fails on a colour literal anywhere outside those files.
 * A colour-blind theme swaps the token values and nothing else changes.
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
   * A TOWN, not a water — `color.label.place`. The search results put places in their own
   * group above the waters, and the pin that marks a town on the map wears the same colour,
   * so "this row is that pin" reads without a legend. Neutral, like the basemap's own town
   * names: it was an orange that a deuteranope could not tell from the permit-only amber on
   * the same map (ΔE 0.7). Used as TEXT (the group's heading), so it holds 4.5:1.
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
  /** The satellite basemap's preview: water, forest, scrub. The same in every theme. */
  imagery: readonly [string, string, string];
  /** The dimming behind a bottom sheet. */
  scrim: string;
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
 * `box-shadow: 4px 4px 0` in solid black. It reads as a printed object on paper rather than
 * a pane of glass hovering over one, and it survives being small — a 14px blur on a 28px
 * control is a smudge. The app had a soft 14px material blur, which is the generic elevation
 * this design is explicitly not.
 *
 * The slab is `color.ui.shadow` (the ink, in the light theme; pure black on dark ground,
 * where the ink is pale) at `opacity.ui.shadow`.
 */
const HARD = (offset: number, colour: string) =>
  ({ boxShadow: `${offset}px ${offset}px 0 ${colour}`, elevation: offset });

/** The status colours for one theme: the map's tokens, through the map's own resolver. */
const statusColours = (theme: string) => ({
  closed: colour(theme, "color.status.closed"),
  restricted: colour(theme, "color.status.restricted"),
  open: colour(theme, "color.status.open"),
  waterStatus: Object.fromEntries(WATER_STATUSES.map((s) => [s, waterStatusColour(theme, s)])) as
    Record<WaterStatus, string>,
});

/**
 * One theme's palette, from the tokens.
 *
 * The legend swatches (`survey`, `water`, `stock`) are the map's own tokens, so they cannot
 * disagree with the map they explain. `stock` was five hexes here beside three map tokens,
 * and the legend promised five colours the map could not draw.
 */
export function paletteFor(theme: string): Palette {
  const c = (token: string) => colour(theme, token);
  return {
    page: c("color.ui.page"), card: c("color.ui.card"), wash: c("color.ui.wash"),
    tint: c("color.ui.tint"), line: c("color.ui.line"), line2: c("color.ui.line2"),
    ink: c("color.ui.ink"), sub: c("color.ui.sub"), faint: c("color.ui.faint"),
    ...statusColours(theme),
    quiet: c("color.ui.quiet"), accent: c("color.ui.accent"), onAccent: c("color.ui.on-accent"),
    live: c("color.ui.live"),
    place: c("color.label.place"), placeBand: c("color.ui.place-band"),
    stock: [c("color.stock.season"), c("color.stock.year"), c("color.stock.recent"),
            c("color.stock.decade"), c("color.stock.old")],
    donor: [c("color.ui.donor.1"), c("color.ui.donor.2"), c("color.ui.donor.3"),
            c("color.ui.donor.4")],
    survey: [c("color.survey.digitised"), c("color.survey.sheet")],
    water: [c("color.lake.fill"), c("color.water.mapped")],
    imagery: [c("color.ui.imagery.water"), c("color.ui.imagery.forest"),
              c("color.ui.imagery.scrub")],
    scrim: translucent(theme, "color.ui.shadow", "opacity.ui.scrim"),
    lift: HARD(3, translucent(theme, "color.ui.shadow", "opacity.ui.shadow")),
    r: RADIUS,
  };
}

export const LIGHT: Palette = paletteFor("light");
export const DARK: Palette = paletteFor("dark");

/**
 * The colour-blind theme. NOT "the light theme with different status hues" — which is
 * exactly what it was, and why it did not work.
 *
 * `{ ...LIGHT, ...outcomes("cvd") }` swapped four colours and inherited everything else, so
 * every ramp added to the palette afterwards silently joined this theme wearing the light
 * theme's hues (see `tools/cvd.test.ts` and design/colour.md for what that shipped). Its
 * values now live in `themes/cvd.json`, which names every token like any other theme: the
 * categorical hues follow Okabe & Ito's Color Universal Design set and the stocking ramp is
 * cividis, trimmed at the light end.
 */
export const CVD: Palette = paletteFor("cvd");

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
  return [1, 2, 3, 4, 5, 6, 7].map((i) => colour(theme, `color.flow.f${i}`));
}

/**
 * The colours MapLibre's own controls wear — `mapChrome` in @app/map, from the same
 * `color.ui.*` tokens this palette is built from. `<Map/>` derives it itself when no `chrome`
 * is passed; this is here for a caller that wants it explicitly.
 */
export function mapChrome(theme: string): MapChrome {
  return chromeOf(theme);
}
