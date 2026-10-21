// N2 review web walk: node walk.mjs <url> <tag> <outDir>  (PW=<path to playwright/index.mjs>, E=<chromium binary>)
const { chromium } = await import(process.env.PW ?? "playwright");
const [url, tag, outDir] = process.argv.slice(2);
const b = await chromium.launch({ executablePath: process.env.E, args: ["--use-gl=angle", "--enable-unsafe-swiftshader"] });
const p = await b.newPage({ viewport: { width: 412, height: 880 }, deviceScaleFactor: 2 });
const errs = [];
p.on("console", (m) => { if (m.type() === "error") errs.push(m.text().slice(0, 200)); });
await p.goto(url);
await p.waitForFunction(() => globalThis.__map && globalThis.__map.loaded(), null, { timeout: 120000 });
const idle = () => p.evaluate(() => new Promise((r) => { const m = globalThis.__map; if (m.loaded() && !m.isMoving()) setTimeout(r, 1500); else m.once("idle", () => setTimeout(r, 1500)); }));
await idle();
// center on the lower Fraser at z11 so rivers fill the screen
await p.evaluate(() => globalThis.__map.jumpTo({ center: [-121.95, 49.17], zoom: 11 }));
await idle(); await idle();
await p.screenshot({ path: `${outDir}/web-${tag}-01-status.jpg`, quality: 70 });
const info = await p.evaluate(() => {
  const m = globalThis.__map;
  const fs = m.queryRenderedFeatures({ layers: ["stream"] }).filter((f) => f.id !== undefined);
  const states = {};
  let coloured = 0;
  for (const f of fs.slice(0, 400)) { const st = m.getFeatureState({ source: "atlas", sourceLayer: "stream", id: f.id }); if (st && st.status) coloured++; }
  // a long stream line near the middle of the screen
  const W = innerWidth, H = innerHeight;
  let pick = null;
  for (const f of fs) {
    const g = f.geometry; const lines = g.type === "LineString" ? [g.coordinates] : g.type === "MultiLineString" ? g.coordinates : [];
    for (const l of lines) for (const c of l) { const pt = m.project(c); const d = Math.hypot(pt.x - W / 2, pt.y - H * 0.45);
      if (pt.y > 120 && pt.y < H - 160 && (!pick || d < pick.d)) pick = { d, x: pt.x, y: pt.y, id: f.id, name: f.properties.name ?? f.properties.gnis_name ?? null }; }
  }
  return { n: fs.length, idType: typeof fs[0]?.id, coloured, sample: fs.slice(0, 3).map((f) => f.id), pick,
           promote: Object.keys(m.getStyle().sources.atlas.promoteId ?? {}) };
});
console.log(tag, JSON.stringify(info));
// what the tap answers: wrap via a click listener reading the same query the app does (24px box, stream group)
const tapAns = await p.evaluate(({ x, y }) => {
  const m = globalThis.__map;
  const hits = m.queryRenderedFeatures([[x - 12, y - 12], [x + 12, y + 12]], { layers: ["stream", "lake_flowing", "wetland_flowing"].filter((l) => m.getLayer(l)) });
  return hits.map((f) => f.id).slice(0, 5);
}, info.pick);
console.log(tag, "box ids at pick", JSON.stringify(tapAns));
await p.mouse.click(info.pick.x, info.pick.y);
await p.waitForTimeout(4000);
await p.screenshot({ path: `${outDir}/web-${tag}-02-tap.jpg`, quality: 70 });
const heading = await p.evaluate(() => [...document.querySelectorAll('[role="heading"], h1, h2')].map((e) => e.textContent).slice(0, 4));
const bodyText = await p.evaluate(() => document.body.innerText.slice(0, 300).replace(/\n+/g, " | "));
console.log(tag, "after tap", JSON.stringify({ heading, bodyText }));
console.log(tag, "console errors", JSON.stringify(errs.slice(0, 8)));
await b.close();
