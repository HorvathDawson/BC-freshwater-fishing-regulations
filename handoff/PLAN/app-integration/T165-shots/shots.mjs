// T165 shots: the built app (no paint rewritten) over the no-access watersheds — Coquitlam, Seymour, Capilano — and a
// close view where a no-access stream's dots and the under-lake route's dots meet, light and dark, phone and desktop.
//
//   VENV_REPO=<main checkout> node shots.mjs <out dir> <app url> [prefix]
import { createRequire } from "node:module";
import { existsSync, mkdirSync, readdirSync } from "node:fs";
import { join } from "node:path";

function playwright() {
  const lib = join(process.env.VENV_REPO, ".venv", "lib");
  for (const py of readdirSync(lib).filter((d) => d.startsWith("python"))) {
    const driver = join(lib, py, "site-packages", "playwright", "driver", "package");
    if (existsSync(join(driver, "package.json"))) return createRequire(join(driver, "x.js"))(driver);
  }
  throw new Error("no Playwright in .venv");
}
const { chromium } = playwright();
const [OUT, URL, PREFIX = "built"] = process.argv.slice(2);
mkdirSync(OUT, { recursive: true });
const VIEWS = JSON.parse(process.env.VIEWS ?? JSON.stringify([
  { id: "coquitlam", center: [-122.79, 49.33], zoom: 11.6 },
  { id: "seymour", center: [-122.96, 49.40], zoom: 11.4 },
  { id: "capilano", center: [-123.10, 49.41], zoom: 11.4 },
  { id: "underlake-coquitlam-lake", center: [-122.785, 49.375], zoom: 13.2 },
]));
const SIZES = (process.env.SIZES ?? "phone,desktop").split(",").map((s) =>
  s === "phone" ? { id: s, viewport: { width: 390, height: 844 } } : { id: s, viewport: { width: 1280, height: 860 } });
const browser = await chromium.launch({ headless: true });
for (const scheme of (process.env.SCHEMES ?? "light,dark").split(","))
  for (const size of SIZES) {
    const ctx = await browser.newContext({ viewport: size.viewport, deviceScaleFactor: 2, colorScheme: scheme });
    const p = await ctx.newPage();
    await p.goto(URL, { waitUntil: "load", timeout: 300000 });
    await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
    await p.waitForTimeout(6000);
    for (const v of VIEWS) {
      await p.evaluate(([c, z]) => new Promise((done) => {
        const m = globalThis.__map; const t = setTimeout(done, 45000);
        m.once("idle", () => { clearTimeout(t); done(); });
        m.jumpTo({ center: c, zoom: z, bearing: 0, pitch: 0 });
      }), [v.center, v.zoom]);
      await p.waitForTimeout(5000);
      await p.screenshot({ path: join(OUT, `${PREFIX}--${v.id}--${size.id}--${scheme}.jpg`), type: "jpeg", quality: 82 });
      console.log(scheme, size.id, v.id);
    }
    await ctx.close();
  }
await browser.close();
