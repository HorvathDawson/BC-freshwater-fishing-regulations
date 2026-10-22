// Lists the atlas features of one source-layer loaded around a camera, with a centroid of their first ring — to find
// places to shoot. Usage: node features.mjs <app url> <lat> <lon> <zoom> <sourceLayer> [props comma list]
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const [URL, lat, lon, zoom, layer, props = "name,kind"] = process.argv.slice(2);
const browser = await chromium.launch({ executablePath: exe, headless: true });
try {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const p = await ctx.newPage();
  await p.goto(URL, { waitUntil: "load", timeout: 300000 });
  await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
  await p.waitForTimeout(3000);
  const out = await p.evaluate(async ([lat, lon, zoom, layer, props]) => {
    const m = globalThis.__map;
    m.jumpTo({ center: [lon, lat], zoom });
    await new Promise((r) => { m.once("idle", r); setTimeout(r, 30000); });
    const seen = new Map();
    for (const f of m.querySourceFeatures("atlas", { sourceLayer: layer })) {
      const key = props.map((k) => f.properties[k]).join(" | ");
      if (seen.has(key)) continue;
      const ring = f.geometry.type === "Polygon" ? f.geometry.coordinates[0]
        : f.geometry.type === "MultiPolygon" ? f.geometry.coordinates[0][0] : f.geometry.coordinates;
      const c = ring && ring.length ? ring.reduce((a, q) => [a[0] + q[0] / ring.length, a[1] + q[1] / ring.length], [0, 0]) : null;
      seen.set(key, c && `${c[1].toFixed(3)},${c[0].toFixed(3)}`);
    }
    return [...seen].map(([k, c]) => `${k}  @ ${c}`);
  }, [Number(lat), Number(lon), Number(zoom), layer, props.split(",")]);
  console.log(out.join("\n"));
} finally { await browser.close(); }
