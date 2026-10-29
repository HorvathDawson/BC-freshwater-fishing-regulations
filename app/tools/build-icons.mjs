#!/usr/bin/env node
/**
 * THE APP ICONS, DRAWN FROM THE FAVICON (T244, owner 2026-10-29: "app icon = the favicon").
 *
 *   node tools/build-icons.mjs        -> writes apps/mobile/assets/icon/*.png (committed)
 *
 * One source: `apps/mobile/assets/icon/favicon.svg` — the v1 site's sky-blue line fish with its location pin, byte for byte
 * what https://canifishthis.ca/favicon.svg serves (copied from archive/webapp/public/favicon.svg). Nothing is drawn
 * here: the SVG's own paths are placed on a canvas, in the SVG's own stroke colour, over the app's own ground
 * (`color.ui.card` of the light theme, read from the theme file). Rasterised with @resvg/resvg-js (devDependency);
 * the PNGs are committed, so this is run by hand when the favicon or the sizes change, not by `pnpm check`.
 *
 *   icon-1024.png                 App Store icon: opaque, fish ~72 % of the square
 *   adaptive-foreground.png       Android adaptive foreground: transparent, the fish inside the 66 dp (of 108) safe circle
 *   adaptive-monochrome.png       Android 13 themed icon: the same shape, one flat colour
 *   play-512.png                  Google Play listing icon (rounded by Play)
 *   feature-graphic-1024x500.png  Play feature graphic BASE: the fish on the ground, no text (the owner adds words if wanted)
 *   favicon-48.png                the web app's favicon (expo `web.favicon`)
 * The adaptive background is the ground colour alone: `apps/mobile/app.config.js` reads it from the theme file.
 */
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { Resvg } from "@resvg/resvg-js";
import { fileURLToPath } from "node:url";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const dir = here("../apps/mobile/assets/icon/");
const svg = readFileSync(dir + "favicon.svg", "utf8");
const inner = /<svg[^>]*>([\s\S]*)<\/svg>/.exec(svg)?.[1];
const ink = /stroke:\s*(#[0-9a-fA-F]{6})/.exec(svg)?.[1];
const ground = JSON.parse(readFileSync(here("../packages/map/style/themes/light.json"), "utf8")).values["color.ui.card"];
if (!inner || !ink || !ground) throw new Error("build-icons: cannot read the favicon or the theme");

// The art's bounding box in its 24-unit view box (measured from a render; the pin tops it, the tail ends it).
const BOX = { x0: 0.72, x1: 23.4, y0: 0.29, y1: 23.4 };
const cx = (BOX.x0 + BOX.x1) / 2, cy = (BOX.y0 + BOX.y1) / 2, bw = BOX.x1 - BOX.x0;

/** The art, `frac` of the canvas wide, centred (optionally on a canvas wider than tall), recoloured by `colour`. */
function compose({ w, h, frac, bg, colour }) {
  const k = (frac * Math.min(w, h * (bw / (BOX.y1 - BOX.y0)))) / bw;       // unit -> px
  let art = inner;
  if (colour) art = art.replaceAll(ink, colour);
  const rect = bg ? `<rect width="${w}" height="${h}" fill="${bg}"/>` : "";
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">${rect}` +
    `<g transform="translate(${w / 2 - cx * k} ${h / 2 - cy * k}) scale(${k})" fill="${colour ?? ink}">${art}</g></svg>`;
}
function out(name, opts) {
  const src = compose(opts);
  mkdirSync(dir, { recursive: true });
  writeFileSync(dir + name, new Resvg(src).render().asPng());
  console.log(`wrote assets/icon/${name}`);
}

out("icon-1024.png", { w: 1024, h: 1024, frac: 0.72, bg: ground });
out("adaptive-foreground.png", { w: 1024, h: 1024, frac: 0.5 });
out("adaptive-monochrome.png", { w: 1024, h: 1024, frac: 0.5, colour: ink.replace(/[0-9a-f]/gi, "0") });
out("play-512.png", { w: 512, h: 512, frac: 0.72, bg: ground });
out("feature-graphic-1024x500.png", { w: 1024, h: 500, frac: 0.62, bg: ground });
out("favicon-48.png", { w: 48, h: 48, frac: 0.96 });
