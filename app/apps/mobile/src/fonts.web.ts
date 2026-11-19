/**
 * (T910, owner's Lighthouse run 2026-11-19) THE WEB'S FACES ARE WOFF2 FROM OUR OWN `/assets/fonts/`. The web drew the
 * phone's TTF files (expo-google-fonts, 41-56 kB each) at a path with an `@` in it, which the host answers with a 307
 * first: eight redirects, then eight TTF, on every first visit — and a water page, which already draws these WOFF2 files
 * (T409), fetched all eight again as TTF when the app took over. Now each face is the WOFF2 made losslessly from the
 * same TTF (`tools/emit-web-fonts.mjs`, `tools/seo/fonts/`), at the path the water pages use, so a water page and the app
 * share one cached file. `tools/web-shell.mjs` puts the files into the export and refuses an export whose bundle does
 * not name each one. `font-display: swap`: words drawn before a face lands show in the system face, never invisible.
 */
import { FontDisplay, type FontSource } from "expo-font";
import { WEB_FONTS } from "./webFonts.generated";

export const FONT_SOURCES: Readonly<Record<string, FontSource>> = Object.fromEntries(
  Object.entries(WEB_FONTS).map(([face, uri]) => [face, { uri, display: FontDisplay.SWAP }]));
