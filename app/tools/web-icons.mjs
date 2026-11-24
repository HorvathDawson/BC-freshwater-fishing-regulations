/**
 * (T1042) THE SITE'S ICONS FOR GOOGLE SEARCH: a PNG at 48, 96 and 192 px (`/favicon-<n>x<n>.png`) and the 180 px
 * `/apple-touch-icon.png`, drawn from the app's own icon sources (apps/mobile/assets/icon: `favicon.svg`, the 16/32 px cut
 * on its tile, the very art `favicon.ico` carries; `icon.svg`, the opaque full-bleed icon, for the touch icon) with
 * @resvg/resvg-js, the icons' rasteriser (tools/build-icons.mjs). Nothing is drawn here. `/favicon.ico` stays: it holds only
 * 16 and 32 px, and Google Search showed its default globe for it (owner 2026-11-23).
 * developers.google.com/search/docs/appearance/favicon-in-search: a square image of at least 8 px (larger than 48 px is
 * recommended), PNG among the formats, found by `<link rel="icon">` (or `apple-touch-icon`) in the HOME page's head, one
 * per host, a stable URL, crawlable. So the links go in the home page (`withIconLinks`, from tools/web-shell.mjs) and in
 * every page the SEO build makes (tools/seo favicon.ts); the PNGs are the SEO build's files (`drawIcons`).
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Resvg } from "@resvg/resvg-js";

const src = (f) => readFileSync(fileURLToPath(new URL(`../apps/mobile/assets/icon/${f}`, import.meta.url)), "utf8");

export const ICON_SIZES = [48, 96, 192];
export const TOUCH_SIZE = 180;
export const iconPath = (n) => `favicon-${n}x${n}.png`;
export const TOUCH_PATH = "apple-touch-icon.png";
export const ICON_MARK = { open: "<!--icons-->", close: "<!--/icons-->" };
export const ICON_LINKS = ICON_MARK.open +
  ICON_SIZES.map((n) => `<link rel="icon" type="image/png" sizes="${n}x${n}" href="/${iconPath(n)}">`).join("") +
  `<link rel="apple-touch-icon" sizes="${TOUCH_SIZE}x${TOUCH_SIZE}" href="/${TOUCH_PATH}">` + ICON_MARK.close;
const MARKED = /<!--icons-->[\s\S]*?<!--\/icons-->/g;

const png = (svg, w) => new Resvg(svg, { fitTo: { mode: "width", value: w } }).render().asPng();

/** name -> PNG bytes of every icon file, the same bytes on every run */
export function drawIcons() {
  const tile = src("favicon.svg"), full = src("icon.svg");
  return new Map([...ICON_SIZES.map((n) => [iconPath(n), png(tile, n)]), [TOUCH_PATH, png(full, TOUCH_SIZE)]]);
}

/** The page with the icon links straight after `<head>` (once: a second run is the same page). */
export function withIconLinks(html) {
  const t = html.replace(MARKED, "");
  if (!/<head[^>]*>/.test(t)) throw new Error("web-icons: the page has no <head>");
  return t.replace(/<head[^>]*>/, (h) => `${h}${ICON_LINKS}`);
}
