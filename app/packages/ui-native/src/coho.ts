/**
 * (T814) The coho loader's size and sheet, shared by both halves of `CohoLoader` and the screens that lay it out.
 */
import { COHO } from "./coho.generated";
import { THEMES, type Palette, type ThemeName } from "./theme";

/**
 * Its width on the start screen and the curtain, in dp: about the circling fish's weight at 110 dp, as a long fish
 * (192 x 80). The sheet is baked at 2.5 px per dp of this (`tools/build-coho.mjs` FRAME_PX; T852).
 */
export const COHO_WIDTH = 192;

/** The height that goes with a width: the frame's own aspect. */
export const cohoHeight = (width: number): number => (width * COHO.frame.height) / COHO.frame.width;

/** How far the ground (`palette.coho.ground`) reaches past the frame on every side, in dp. */
export const COHO_GROUND_PAD = 10;

/**
 * The sheet printed in this palette's coho ink and red: the theme whose tokens those are. A palette no theme wears has
 * no sheet, and says so (a sheet in other colours would be a guess). (T844) `sheets`: the web passes its own
 * (`COHO_WEB.sheets`, cohoWeb.generated.ts); a phone takes the default.
 */
export function cohoSheet<S extends Record<ThemeName, unknown> = typeof COHO.sheets>(palette: Palette, sheets?: S): S[ThemeName] {
  const name = (Object.keys(THEMES) as ThemeName[])
    .find((t) => THEMES[t].coho.ink === palette.coho.ink && THEMES[t].coho.red === palette.coho.red);
  if (!name) throw new Error(`no coho sheet is printed in ${palette.coho.ink} + ${palette.coho.red} (pnpm coho:build)`);
  return ((sheets ?? COHO.sheets) as S)[name];
}
