// T146 option shots: the North Shore / Coquitlam watersheds on the regulations map, light and dark, once per no-access
// candidate. The app is this worktree's Expo web build; each candidate is painted by rewriting the live map's paint
// properties that carry the theme's current `color.status.no_access` (the one colouring pathway, so exactly the lines and
// lake fills that would change) — the legend swatch keeps the built colour, so these shots are for the MAP only.
//
//   VENV_REPO=<main checkout> node options-shots.mjs <out dir> <app url> '<json: {id: {light, dark}}>'
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
const [OUT, URL, OPTS] = process.argv.slice(2);
const options = JSON.parse(OPTS);
mkdirSync(OUT, { recursive: true });
const VIEWS = JSON.parse(process.env.VIEWS ?? JSON.stringify([
  { id: "northshore-z10", center: [-122.93, 49.40], zoom: 10 },
  { id: "coquitlam-z12", center: [-122.79, 49.33], zoom: 11.6 },
]));
const browser = await chromium.launch({ headless: true });
for (const scheme of ["light", "dark"]) {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, colorScheme: scheme });
  const p = await ctx.newPage();
  await p.goto(URL, { waitUntil: "load", timeout: 300000 });
  await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
  await p.waitForTimeout(6000);
  const built = await p.evaluate(() => {
    const m = globalThis.__map;
    const saved = {};
    for (const l of m.getStyle().layers)
      for (const [k, v] of Object.entries(l.paint ?? {})) saved[`${l.id}\u0000${k}`] = v;
    globalThis.__saved = saved;
    return Object.keys(saved).length;
  });
  for (const v of VIEWS) {
    for (const [id, hex] of Object.entries(options)) {
      const from = hex.from?.[scheme];
      const to = hex[scheme];
      const n = await p.evaluate(([f, t]) => {
        const m = globalThis.__map; let n = 0;
        for (const [key, val] of Object.entries(globalThis.__saved)) {
          const [layer, prop] = key.split("\u0000");
          const s = JSON.stringify(val);
          const re = new RegExp(f, "gi");
          if (!f || !re.test(s)) { m.setPaintProperty(layer, prop, val); continue; }
          m.setPaintProperty(layer, prop, JSON.parse(s.replace(new RegExp(f, "gi"), t))); n++;
        }
        return n;
      }, [from, to]);
      await p.evaluate(([c, z]) => new Promise((done) => {
        const m = globalThis.__map; const t = setTimeout(done, 45000);
        m.once("idle", () => { clearTimeout(t); done(); });
        m.jumpTo({ center: c, zoom: z, bearing: 0, pitch: 0 });
      }), [v.center, v.zoom]);
      await p.waitForTimeout(5000);
      await p.evaluate(() => new Promise((done) => {
        const m = globalThis.__map; const t = setTimeout(done, 20000);
        if (m.loaded()) { clearTimeout(t); done(); } else m.once("idle", () => { clearTimeout(t); done(); });
      }));
      await p.waitForTimeout(2000);
      await p.screenshot({ path: join(OUT, `${id}--${v.id}--${scheme}.jpg`), type: "jpeg", quality: 82 });
      console.log(scheme, v.id, id, "paint props rewritten:", n, "of", built);
    }
  }
  await ctx.close();
}
await browser.close();
