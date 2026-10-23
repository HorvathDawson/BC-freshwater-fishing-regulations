// T158 shots: Coquitlam Lake (no public access) with the Depth contours switch on, on this worktree's Expo web build
// against the owner's data server (:39217, GET only).
//
// THE TILE SET CARRIES NO CONTOURS: data/generated/tiles/layers/ has no contour.geojsonl (the bathymetry layer was not in
// the last tile build), so the switch draws nothing anywhere today. To show the colour where it matters, each shot adds
// stand-in contours: the lake's own rendered outline scaled toward its centre (0.8, 0.6, 0.4, 0.2), drawn as a line layer
// that copies the `contour` layer's own paint (its line-color and line-width as the built style resolves them) and sits
// just above it. "before" repaints that stand-in in the old theme cyan (dark #4FD0E8, light #2FA9C9).
//
//   VENV_REPO=<main checkout> node contour-shots.mjs <out dir> <app url>
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
const [OUT, URL] = process.argv.slice(2);
mkdirSync(OUT, { recursive: true });
const OLD = { dark: "#4FD0E8", light: "#2FA9C9" };
const VIEW = { center: [-122.79, 49.385], zoom: 12.2 };
const browser = await chromium.launch({ headless: true });
for (const scheme of ["dark", "light"]) {
  const ctx = await browser.newContext({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, colorScheme: scheme });
  const p = await ctx.newPage();
  await p.goto(URL, { waitUntil: "load", timeout: 300000 });
  await p.waitForFunction(() => !!globalThis.__map, null, { timeout: 300000 });
  await p.waitForTimeout(5000);
  // the Layers sheet's "Depth contours" switch, as a reader turns it on
  await p.getByRole("button", { name: /layers/i }).first().click();
  await p.waitForTimeout(1500);
  await p.getByText("Depth contours", { exact: true }).first().click();
  await p.waitForTimeout(800);
  await p.keyboard.press("Escape");
  await p.waitForTimeout(1500);
  const idle = () => p.evaluate(() => new Promise((done) => {
    const m = globalThis.__map; const t = setTimeout(done, 30000);
    if (m.loaded()) { clearTimeout(t); done(); } else m.once("idle", () => { clearTimeout(t); done(); });
  }));
  await p.evaluate(([c, z]) => globalThis.__map.jumpTo({ center: c, zoom: z, bearing: 0, pitch: 0 }), [VIEW.center, VIEW.zoom]);
  await p.waitForTimeout(6000); await idle(); await p.waitForTimeout(2000);
  const said = await p.evaluate(() => {
    const m = globalThis.__map;
    const lakes = m.queryRenderedFeatures(undefined, { layers: ["lake"] })
      .filter((f) => /Coquitlam Lake/.test(f.properties?.name ?? "") || true);
    // the biggest rendered polygon on screen is the lake
    const rings = [];
    for (const f of lakes) {
      const polys = f.geometry.type === "Polygon" ? [f.geometry.coordinates] : f.geometry.type === "MultiPolygon" ? f.geometry.coordinates : [];
      for (const poly of polys) rings.push(poly[0]);
    }
    rings.sort((a, b) => b.length - a.length);
    const ring = rings[0];
    if (!ring) return { error: "no lake rendered" };
    const cx = ring.reduce((s, c) => s + c[0], 0) / ring.length, cy = ring.reduce((s, c) => s + c[1], 0) / ring.length;
    const lines = [0.8, 0.6, 0.4, 0.2].map((k) => ({ type: "Feature", properties: {},
      geometry: { type: "LineString", coordinates: ring.map(([x, y]) => [cx + (x - cx) * k, cy + (y - cy) * k]) } }));
    m.addSource("t158-contours", { type: "geojson", data: { type: "FeatureCollection", features: lines } });
    m.addLayer({ id: "t158-contours", type: "line", source: "t158-contours",
                 paint: { "line-color": m.getPaintProperty("contour", "line-color"),
                          "line-width": m.getPaintProperty("contour", "line-width") ?? 0.8 } });
    return { contourColour: m.getPaintProperty("contour", "line-color"), width: m.getPaintProperty("contour", "line-width"),
             ringPoints: ring.length, lakes: lakes.length, visible: m.getLayoutProperty("contour", "visibility") };
  });
  console.log(scheme, JSON.stringify(said));
  await p.waitForTimeout(1500); await idle(); await p.waitForTimeout(1000);
  await p.screenshot({ path: join(OUT, `after--coquitlam-depth--${scheme}.png`) });
  await p.evaluate((c) => globalThis.__map.setPaintProperty("t158-contours", "line-color", c), OLD[scheme]);
  await p.waitForTimeout(1500); await idle(); await p.waitForTimeout(1000);
  await p.screenshot({ path: join(OUT, `before--coquitlam-depth--${scheme}.png`) });
  await ctx.close();
}
await browser.close();
