#!/usr/bin/env node
/**
 * GOOGLE PLAY FEATURE GRAPHIC (1024x500, opaque RGB PNG, no alpha) — the favicon fish + the name + one line.
 *   node tools/build-play-graphics.mjs   -> writes handoff/PLAN/app-integration/store/graphics/feature-graphic-1024x500.png
 * Art: apps/mobile/assets/icon/favicon.svg (as build-icons.mjs places it). Colours: light theme tokens (card ground,
 * ink, sub). Fonts: the app's own (Bricolage Grotesque 800 title, Archivo 500 line, from @expo-google-fonts).
 * Text stays inside x 400..940, y 150..350 (Play crops the edges on some surfaces).
 */
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { Resvg } from "@resvg/resvg-js";
import { PNG } from "pngjs";
import { fileURLToPath } from "node:url";
const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const svg = readFileSync(here("../apps/mobile/assets/icon/favicon.svg"), "utf8");
const inner = /<svg[^>]*>([\s\S]*)<\/svg>/.exec(svg)[1];
const ink = /stroke:\s*(#[0-9a-fA-F]{6})/.exec(svg)[1];
const t = JSON.parse(readFileSync(here("../packages/map/style/themes/light.json"), "utf8")).values;
const [ground, text, sub] = [t["color.ui.card"], t["color.ui.ink"], t["color.ui.sub"]];
const fonts = ["bricolage-grotesque/800ExtraBold/BricolageGrotesque_800ExtraBold.ttf", "archivo/500Medium/Archivo_500Medium.ttf"]
  .map((f) => here("../node_modules/@expo-google-fonts/" + f));
const k = 12;                       // art is 24 units; 12 px/unit = 288 px, centred at (200, 250)
const src = `<svg xmlns="http://www.w3.org/2000/svg" width="1024" height="500" viewBox="0 0 1024 500">
<rect width="1024" height="500" fill="${ground}"/>
<g transform="translate(${200 - 12.06 * k} ${250 - 11.85 * k}) scale(${k})" fill="${ink}">${inner}</g>
<text x="400" y="235" font-family="Bricolage Grotesque" font-weight="800" font-size="64" fill="${text}">Can I Fish This?</text>
<text x="402" y="290" font-family="Archivo" font-weight="500" font-size="29" fill="${sub}">BC freshwater fishing rules,</text>
<text x="402" y="328" font-family="Archivo" font-weight="500" font-size="29" fill="${sub}">by the water</text></svg>`;
const rgba = PNG.sync.read(Buffer.from(new Resvg(src, { font: { fontFiles: fonts, loadSystemFonts: false } }).render().asPng()));
const dir = here("../../handoff/PLAN/app-integration/store/graphics/");
mkdirSync(dir, { recursive: true });
writeFileSync(dir + "feature-graphic-1024x500.png", PNG.sync.write(rgba, { colorType: 2, inputColorType: 6, inputHasAlpha: true }));
console.log("wrote feature-graphic-1024x500.png");
