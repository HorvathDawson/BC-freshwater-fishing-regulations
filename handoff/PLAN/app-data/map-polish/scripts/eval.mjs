// Evaluates an expression against the live map (`m` = the dev-only `__map`) after jumping the camera, optionally on
// the Conditions tab, and prints the JSON result. Usage: node eval.mjs <app url> <lat> <lon> <zoom> <map|conditions> <expr>
import { createRequire } from "node:module";
const require = createRequire("/Users/dawson.horvath/.npm/_npx/9833c18b2d85bc59/node_modules/");
const { chromium } = require("playwright");
const exe = "/Users/dawson.horvath/Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing";
const [URL, lat, lon, zoom, tab, expr] = process.argv.slice(2);
const browser = await chromium.launch({ executablePath: exe, headless: true });
try {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 } });
  const p = await ctx.newPage();
  p.on("pageerror", (e) => console.log("ERR", String(e).slice(0, 300)));
  p.on("console", (msg) => { if (msg.type() === "error") console.log("CONSOLE", msg.text().slice(0, 300)); });
  await p.goto(URL, { waitUntil: "load", timeout: 300000 });
  await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
  await p.waitForTimeout(3000);
  if (tab === "conditions") { await p.getByRole("tab", { name: "Conditions" }).first().click(); await p.waitForTimeout(1500); }
  const out = await p.evaluate(async ([lat, lon, zoom, expr]) => {
    const m = globalThis.__map;
    m.jumpTo({ center: [lon, lat], zoom });
    await new Promise((r) => { m.once("idle", r); setTimeout(r, 30000); });
    return JSON.stringify((0, eval)(`(m) => (${expr})`)(m), null, 1);
  }, [Number(lat), Number(lon), Number(zoom), expr]);
  console.log(out);
} finally { await browser.close(); }
