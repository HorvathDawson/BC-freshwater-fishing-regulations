#!/usr/bin/env node
/**
 * THE APP ICONS, FROM ONE CHOSEN MARK (owner 2026-11-08: round 4 "Leap across the sun", a linocut trout leaping across a
 * cut sun inside a map pin, "for now", until a human-made one exists. Before it: T244's v1 sky-blue line fish + pin).
 *
 *   node tools/build-icons.mjs                     -> writes apps/mobile/assets/icon/*.png (committed)
 *   node tools/build-icons.mjs --import <dir>      first copies a proposal's SVGs into apps/mobile/assets/icon/, then builds
 *     (e.g. --import ../handoff/PLAN/app-integration/icon-proposals/round4/1-pin-leap)
 *
 * The app never reads the proposals folder: these copies in assets/icon/ are the sources.
 *   icon.svg          the full icon, ground included (view box = the visible 72 of the 108 adaptive canvas)
 *   foreground.svg    Android adaptive foreground: transparent 108 canvas, the art inside the 66 safe circle
 *   monochrome.svg    Android 13 themed icon: one flat colour, cuts transparent
 *   favicon.svg       the 16/32 px cut (a simpler drawing of the same mark) on its own tile: web + SEO hubs serve it
 *   icon-ground.txt   the icon's ground colour, one hex: app.config.js reads it for the adaptive background
 * Nothing is drawn here; the SVGs are rasterised with @resvg/resvg-js (devDependency). The PNGs are committed, so this is
 * run by hand when the mark or the sizes change, not by `pnpm check`.
 *
 *   icon-1024.png                 App Store / Expo icon: opaque, full bleed
 *   play-512.png                  Google Play listing icon (Play rounds it)
 *   adaptive-foreground.png       foreground.svg at 1024
 *   adaptive-monochrome.png       monochrome.svg at 1024 (also the notification icon, app.json)
 *   favicon-48.png                the web app's favicon (expo `web.favicon`)
 *   feature-graphic-1024x500.png  Play feature graphic BASE: the mark on its ground, no text
 */
import { readFileSync, writeFileSync, mkdirSync, copyFileSync } from "node:fs";
import { resolve } from "node:path";
import { Resvg } from "@resvg/resvg-js";
import { fileURLToPath } from "node:url";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const dir = here("../apps/mobile/assets/icon/");

const imp = process.argv.indexOf("--import");
if (imp > 0) {
  const from = resolve(process.argv[imp + 1] ?? "");
  for (const f of ["icon.svg", "foreground.svg", "monochrome.svg", "favicon.svg"]) copyFileSync(`${from}/${f}`, dir + f);
  copyFileSync(`${from}/background.txt`, dir + "icon-ground.txt");
  console.log(`imported the mark from ${from}`);
}

const read = (f) => readFileSync(dir + f, "utf8");
const ground = read("icon-ground.txt").trim();
if (!/^#[0-9a-f]{6}$/i.test(ground)) throw new Error("build-icons: icon-ground.txt must hold one #rrggbb");

/** An SVG rendered `w` px wide (its own aspect). */
const png = (src, w) => new Resvg(src, { fitTo: { mode: "width", value: w } }).render().asPng();
/** The inner markup and view box of a source SVG, to place it inside another. */
function parts(src) {
  const m = /<svg[^>]*viewBox="([^"]+)"[^>]*>([\s\S]*)<\/svg>/.exec(src);
  if (!m) throw new Error("build-icons: a source SVG has no viewBox");
  return { vb: m[1].split(/\s+/).map(Number), inner: m[2] };
}
function out(name, buf) {
  mkdirSync(dir, { recursive: true });
  writeFileSync(dir + name, buf);
  console.log(`wrote assets/icon/${name}`);
}

out("icon-1024.png", png(read("icon.svg"), 1024));
out("play-512.png", png(read("icon.svg"), 512));
out("adaptive-foreground.png", png(read("foreground.svg"), 1024));
out("adaptive-monochrome.png", png(read("monochrome.svg"), 1024));
out("favicon-48.png", png(read("favicon.svg"), 48));

// Feature graphic base: the adaptive foreground's visible 72 at 0.9 of the height, centred on the ground.
const fg = parts(read("foreground.svg"));
const H = 500, W = 1024, s = (H * 0.9) / 72;
out("feature-graphic-1024x500.png", png(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">` +
  `<rect width="${W}" height="${H}" fill="${ground}"/>` +
  `<g transform="translate(${W / 2 - 54 * s} ${H / 2 - 54 * s}) scale(${s})">${fg.inner}</g></svg>`, W));
