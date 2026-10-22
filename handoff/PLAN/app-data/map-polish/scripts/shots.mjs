// Map-polish shots: every mode at z6 / z9 / z12 / z15, light and dark, phone (390x844) and desktop (1440x900), against
// this worktree's Expo web server; data from the main checkout's serve-tiles.mjs (the live release, read-only).
//
// Usage: node shots.mjs <out dir> <app url> <stem prefix> [filters...]
//   filters narrow the run: any of phone|desktop, light|dark, regs|conditions|plain|satellite, z6|z9|z12|z15, or a
//   scene name (see SCENES). Several filters of one kind are OR'd; kinds are AND'd.
//
// Modes: regs = the Map tab coloured by Regulations; plain = the Map tab with every regulations-coloured layer repainted
// in its plain mode through `__map` (PLAIN_PAINT: `plain-paint.ts`, exactly what `paintFor` paints when no day is
// installed). The first version tried to block the status index by URL; the release path reads status from `paint`, so
// nothing was blocked and the "plain" shots were the regs map (review 2026-10-22). The legend card is the app's own and
// still names Regulations in these frames: the app has no plain mode to switch to;
// satellite = regs over Esri imagery; conditions = the Conditions tab (flow). Private land is switched ON for every
// shot (the owner judged it ON). The camera is set exactly with the dev-only `__map` handle and each shot waits for
// MapLibre's `idle` (all tiles in, no transition), then 600 ms for the label fade.
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";

const [OUT, URL, PREFIX, ...filters] = process.argv.slice(2);
import { readFileSync } from "node:fs";
const PLAIN_PAINT = process.env.PLAIN_PAINT ? JSON.parse(readFileSync(process.env.PLAIN_PAINT, "utf8")) : null;
if (filters.includes("plain") && !PLAIN_PAINT) throw new Error("plain shots need PLAIN_PAINT=<plain-paint.ts output>");

/** Where each zoom is shot: chosen for what that zoom has to carry (see README). */
const SCENES = {
  z6: { lat: 50.6, lon: -122.4, zoom: 6 },        // southern B.C.: regions, the basin field, big lakes
  z9: { lat: 49.25, lon: -122.35, zoom: 9 },      // Lower Mainland + Fraser Valley: towns, reserves, closures, rivers
  z12: { lat: 49.275, lon: -122.6, zoom: 12 },    // Pitt Meadows: Blaney Bog / Codd (closed), Malcolm Knapp (permit), parcels
  z15: { lat: 49.2585, lon: -122.585, zoom: 15 }, // the edge of Blaney Bog: closure edge, private land, a creek
};
/** Either side of the Conditions handover, at the app's home camera (filter `handover`). */
const HANDOVER = {
  "z9.0": { lat: 49.15, lon: -121.85, zoom: 9.0 }, "z9.5": { lat: 49.15, lon: -121.85, zoom: 9.5 },
  "z9.9": { lat: 49.15, lon: -121.85, zoom: 9.9 }, "z10.0": { lat: 49.15, lon: -121.85, zoom: 10.0 },
};
if (filters.includes("handover")) { for (const k of Object.keys(SCENES)) delete SCENES[k]; Object.assign(SCENES, HANDOVER); }
const VIEWPORTS ={ phone: { width: 390, height: 844 }, desktop: { width: 1440, height: 900 } };
const THEMES = ["light", "dark"];
const MODES = ["regs", "plain", "satellite", "conditions"];

const pick = (all) => {
  const f = all.filter((x) => filters.includes(x));
  return f.length ? f : all;
};
const browser = await chromium.launch({ executablePath: exe, headless: true });
const log = [];

const idle = (p) => p.evaluate(() => new Promise((r) => {
  const m = globalThis.__map;
  const t = setTimeout(() => r("timeout"), 45000);
  const done = () => { clearTimeout(t); r("idle"); };
  if (m.loaded() && m.areTilesLoaded() && !m.isMoving()) { m.once("idle", done); m.triggerRepaint(); } else m.once("idle", done);
}));

async function layers(p, radio, privateOn) {
  await p.locator('[aria-label^="Choose layers"]').first().click();
  await p.waitForTimeout(900);
  for (const name of radio) {
    const r = p.locator(`[role=radio][aria-label="${name}"]`).first();
    if (await r.count()) await r.click();
    await p.waitForTimeout(300);
  }
  if (privateOn !== undefined) {
    const s = p.locator('[role=switch][aria-label="Private land"]').first();
    const on = (await s.getAttribute("aria-checked")) === "true";
    if (on !== privateOn) await s.click();
  }
  await p.waitForTimeout(300);
  await p.locator('[aria-label="Done"]').first().click();
  await p.waitForTimeout(900);
}

const modes = pick(MODES);
const runs = [modes.filter((m) => m !== "plain"), modes.filter((m) => m === "plain")].filter((r) => r.length);
for (const run of runs) for (const vp of pick(Object.keys(VIEWPORTS))) for (const theme of pick(THEMES)) {
  const ctx = await browser.newContext({ viewport: VIEWPORTS[vp], deviceScaleFactor: 2, colorScheme: theme });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => log.push({ vp, theme, error: String(e).slice(0, 200) }));
  try {
    await p.goto(URL, { waitUntil: "load", timeout: 300000 });
    await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
    await p.waitForTimeout(4000);
    for (const mode of run) {
      if (mode === "conditions") {
        await layers(p, ["Map", "Regulations"], true);
        await p.getByRole("tab", { name: "Conditions" }).first().click();
      } else {
        await p.getByRole("tab", { name: "Map" }).first().click();
        await p.waitForTimeout(600);
        await layers(p, [mode === "satellite" ? "Satellite" : "Map", "Regulations"], true);
      }
      await p.waitForTimeout(1500);
      for (const z of pick(Object.keys(SCENES))) {
        const s = SCENES[z];
        await p.evaluate(({ lat, lon, zoom }) => globalThis.__map.jumpTo({ center: [lon, lat], zoom }), s);
        if (mode === "plain")
          await p.evaluate((paint) => {
            const m = globalThis.__map;
            for (const [id, props] of Object.entries(paint))
              if (m.getLayer(id)) for (const [k, v] of Object.entries(props)) m.setPaintProperty(id, k, v);
          }, PLAIN_PAINT[theme]);
        const how = await idle(p);
        await p.waitForTimeout(z === "z6" ? 2500 : 1200);
        await idle(p);
        await p.waitForTimeout(600);
        const file = `${OUT}/${PREFIX}-${mode}-${z}-${vp}-${theme}.png`;
        await p.screenshot({ path: file });
        log.push({ file: file.split("/").pop(), how });
      }
    }
  } catch (e) {
    log.push({ vp, theme, failed: String(e).slice(0, 300) });
    await p.screenshot({ path: `${OUT}/${PREFIX}-failure-${vp}-${theme}.png` }).catch(() => {});
  } finally {
    await ctx.close();
  }
}
await browser.close();
console.log(JSON.stringify(log.filter((l) => l.how !== "idle"), null, 1), `\n${log.length} entries`);
