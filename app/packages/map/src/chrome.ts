/**
 * Reading the app's chrome out of the one palette.
 *
 * The token set (`style/tokens.json` + `style/themes/*`) holds every colour the app has: the
 * map's paint AND the phone's surfaces (`color.ui.*`). This file is how both sides read it
 * without either typing a value — `palette` in @app/ui-native and MapLibre's own controls
 * below are built from here.
 */
import { resolveTheme } from "./style";
import type { MapChrome } from "./map-props";

/** One colour token's value in a theme. Throws rather than letting a gap fall back. */
export function colour(theme: string, token: string): string {
  const v = resolveTheme(theme)[token];
  if (typeof v !== "string") throw new Error(`theme "${theme}" has no colour ${token}`);
  return v;
}

/** One number token's value in a theme. */
export function amount(theme: string, token: string): number {
  const v = resolveTheme(theme)[token];
  if (typeof v !== "number") throw new Error(`theme "${theme}" has no number ${token}`);
  return v;
}

/**
 * A token colour at an opacity token's alpha, as CSS `rgba()`.
 *
 * Translucency is a token of its own (`opacity.ui.*`) rather than a colour with the alpha
 * baked in, so the slab under a box and the scrim behind a sheet are visibly the same dark,
 * just thinner — and retuning one darkness is one edit.
 */
export function translucent(theme: string, colourToken: string, opacityToken: string): string {
  const h = colour(theme, colourToken).replace("#", "");
  const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16));
  return `rgba(${r}, ${g}, ${b}, ${amount(theme, opacityToken)})`;
}

/** Is this theme's card dark? Decides whether MapLibre's baked-in glyphs need inverting. */
const darkGround = (theme: string) => {
  const h = colour(theme, "color.ui.card").replace("#", "");
  const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16));
  return 0.2126 * r! + 0.7152 * g! + 0.0722 * b! < 128;
};

/**
 * The colours MapLibre's own controls wear, from the theme's chrome tokens — so the zoom
 * stack and the scale bar cannot be on a different theme from the app around them, which
 * they were while `controls.css` carried light-theme fallbacks.
 */
export function mapChrome(theme: string): MapChrome {
  return {
    ink: colour(theme, "color.ui.ink"),
    card: colour(theme, "color.ui.card"),
    tint: colour(theme, "color.ui.tint"),
    sub: colour(theme, "color.ui.sub"),
    accent: colour(theme, "color.ui.accent"),
    shadow: translucent(theme, "color.ui.shadow", "opacity.ui.shadow"),
    // MapLibre bakes near-black strokes into its glyph IMAGES, so on a dark control they
    // are black on near-black. There is no colour property to change — the image has to be
    // inverted.
    iconFilter: darkGround(theme) ? "invert(1)" : "none",
    // Translucent: the scale bar sits over the map and the map should read through it,
    // which is what OpenStreetMap's own bar does.
    scaleBg: translucent(theme, "color.ui.card", "opacity.ui.scale"),
  };
}
