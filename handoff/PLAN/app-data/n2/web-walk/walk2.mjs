// N2 review web walk: node walk.mjs <url> <tag> <outDir>  (PW=<path to playwright/index.mjs>, E=<chromium binary>)
const { chromium } = await import(process.env.PW ?? "playwright");
const [url, tag, outDir, exe] = process.argv.slice(2);
const b = await chromium.launch({ executablePath: exe ?? process.env.E, args: ["--use-gl=angle", "--enable-unsafe-swiftshader"] });
const p = await b.newPage({ viewport: { width: 412, height: 880 }, deviceScaleFactor: 2 });
const errs = [];
p.on("console", (m) => { if (m.type() === "error") errs.push(m.text().slice(0, 200)); });
await p.goto(url);
await p.waitForFunction(() => globalThis.__map && globalThis.__map.loaded(), null, { timeout: 120000 });
const idle = () => p.evaluate(() => new Promise((r) => { const m = globalThis.__map; if (m.loaded() && !m.isMoving()) setTimeout(r, 1500); else m.once("idle", () => setTimeout(r, 1500)); }));
const text = () => p.evaluate(() => document.body.innerText.slice(0, 160).replace(/\n+/g, " | "));
await idle();
// a LAKE tap: Cultus Lake
await p.evaluate(() => globalThis.__map.jumpTo({ center: [-121.985, 49.055], zoom: 12 }));
await idle(); await idle();
const lake = await p.evaluate(() => { const m = globalThis.__map; const pt = m.project([-121.985, 49.055]);
  return { x: pt.x, y: pt.y, ids: m.queryRenderedFeatures([[pt.x - 12, pt.y - 12], [pt.x + 12, pt.y + 12]]).filter((f) => f.id !== undefined).map((f) => `${f.layer.id}:${f.id}`).slice(0, 6) }; });
console.log(tag, "lake box", JSON.stringify(lake));
await p.mouse.click(lake.x, lake.y); await p.waitForTimeout(3500);
console.log(tag, "lake tap ->", await text());
await p.screenshot({ path: `${outDir}/web-${tag}-03-lake-tap.jpg`, quality: 70 });
await p.goBack().catch(() => {}); await p.getByText("‹").first().click().catch(() => {});
await p.waitForTimeout(2500);
await p.waitForFunction(() => globalThis.__map && globalThis.__map.loaded(), null, { timeout: 60000 });
// an AREA-only tap: a park interior with no water within 12 px
await p.evaluate(() => globalThis.__map.jumpTo({ center: [-121.75, 49.2], zoom: 12 }));
await idle(); await idle();
const area = await p.evaluate(() => { const m = globalThis.__map; const W = innerWidth, H = innerHeight;
  for (let y = 200; y < H - 200; y += 13) for (let x = 40; x < W - 40; x += 13) {
    const box = [[x - 12, y - 12], [x + 12, y + 12]];
    const all = m.queryRenderedFeatures(box).filter((f) => f.id !== undefined);
    const water = all.filter((f) => ["stream", "lake", "lake_flowing", "wetland", "wetland_flowing", "lake_route"].includes(f.layer.id));
    const areas = all.filter((f) => ["park", "eco_reserve", "no_access", "indigenous_land"].includes(f.layer.id));
    if (!water.length && areas.length) return { x, y, areas: areas.slice(0, 3).map((f) => `${f.layer.id}:${f.id}`) };
  }
  return null; });
console.log(tag, "area-only point", JSON.stringify(area));
if (area) { const before = await text(); await p.mouse.click(area.x, area.y); await p.waitForTimeout(3000);
  console.log(tag, "area tap changed screen:", (await text()) !== before, "->", await text()); }
// CONDITIONS: the visible set coloured by standing, and the basin field below z9
await p.getByText("Conditions", { exact: true }).first().click().catch((e) => console.log("no conditions tab", e.message));
await p.waitForTimeout(3000);
await p.evaluate(() => globalThis.__map.jumpTo({ center: [-121.95, 49.17], zoom: 10 }));
await idle(); await idle(); await p.waitForTimeout(3000);
const cond = await p.evaluate(() => { const m = globalThis.__map;
  const fs = m.queryRenderedFeatures({ layers: ["stream"] }).filter((f) => f.id !== undefined);
  return { streams: fs.length, withStanding: fs.filter((f) => m.getFeatureState({ source: "atlas", sourceLayer: "stream", id: f.id })?.standing !== undefined).length }; });
console.log(tag, "conditions z10", JSON.stringify(cond));
await p.screenshot({ path: `${outDir}/web-${tag}-04-conditions-z10.jpg`, quality: 70 });
await p.evaluate(() => globalThis.__map.jumpTo({ center: [-122.3, 49.6], zoom: 7.5 }));
await idle(); await idle(); await p.waitForTimeout(4000);
const basin = await p.evaluate(() => { const m = globalThis.__map;
  const fs = m.queryRenderedFeatures({ layers: ["basin"] }).filter((f) => f.id !== undefined);
  return { basins: fs.length, withStanding: new Set(fs.filter((f) => m.getFeatureState({ source: "atlas", sourceLayer: "basin", id: f.id })?.standing !== undefined).map((f) => f.id)).size }; });
console.log(tag, "conditions basin z7.5", JSON.stringify(basin));
await p.screenshot({ path: `${outDir}/web-${tag}-05-conditions-basins.jpg`, quality: 70 });
console.log(tag, "console errors", JSON.stringify(errs.slice(0, 8)));
await b.close();
