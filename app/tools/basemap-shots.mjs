/**
 * THE BASEMAP RENDER GATE (T280): does a rebuilt basemap LOOK like the one it replaces?
 *
 *   node tools/basemap-shots.mjs <before.pmtiles> <after.pmtiles> [--out DIR] [--themes light,dark]
 *        [--zooms 8,10,12,14,16] [--size 512] [--threshold 2] [--chrome PATH] [--keep-all]
 *
 * Draws the app's own basemap layers (`runtimeStyle`: every non-symbol layer over the `basemap` source, plus the
 * backgrounds; our water, masks and labels left out so only the basemap is judged) from both archives, side by side,
 * at every EXTENT x zoom x theme, and counts the pixels that differ by more than 24/255 in any channel (the T202
 * measure). Exit 1 when any shot is over `--threshold` percent. `--out` gets `report.json` and, for each shot over a
 * tenth of the threshold, the before / after / diff PNGs.
 *
 * No browser library: a page served from here renders and compares in the browser and posts its results back; Chrome
 * is started headless (`--chrome`, else $CHROME, else the usual install paths) and stopped when the page is done.
 * Symbols are not drawn: they are not geometry this gate is about, and they would need glyphs.
 */
import { createServer } from "node:http";
import { createReadStream, existsSync, mkdirSync, readFileSync, statSync, writeFileSync, mkdtempSync, rmSync } from "node:fs";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { makeRunner } from "./seo/runner.mjs";

const APP = fileURLToPath(new URL("..", import.meta.url));

/** Fixed BC extents (centres): the T202 shot places, plus forest-heavy interior and coast. */
export const EXTENTS = {
  "vancouver": [-123.12, 49.26],
  "squamish-backcountry": [-123.15, 49.9],
  "chilcotin": [-122.7, 51.8],
  "lac-le-jeune": [-120.47, 50.47],
  "revelstoke-forest": [-118.2, 51.0],
  "prince-george": [-122.75, 53.9],
  "haida-gwaii-coast": [-132.1, 53.25],
};
/** T280: the most of a shot that may differ (pixels off by >24/255). 2 %: one-pixel fill edges and specks at z8-10. */
export const THRESHOLD_PCT = 2;

const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : dflt;
};

function chromePath() {
  const cands = [arg("chrome"), process.env.CHROME,
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    `${process.env.HOME}/Library/Caches/ms-playwright/chromium_headless_shell-1234/chrome-headless-shell-mac-arm64/chrome-headless-shell`,
    "/usr/bin/google-chrome", "/usr/bin/chromium"];
  const p = cands.find((c) => c && existsSync(c));
  if (!p) throw new Error("no Chrome found: pass --chrome PATH or set CHROME");
  return p;
}

async function styles(themes) {
  // the app's TypeScript through the SEO build's loader (vite-node, vitest's own: no new dependency)
  const runner = await makeRunner();
  const { runtimeStyle } = await runner.run(join(APP, "packages/map/src/runtime-style.ts"));
  await runner.close();
  const AT = { atlas: "http://x/atlas.pmtiles", basemap: "BASEMAP", outside: "http://x/o.geojson",
    gauges: '{"type":"FeatureCollection","features":[]}' };
  const out = {};
  for (const t of themes) {
    const s = runtimeStyle(AT, t);
    const layers = s.layers.filter((l) => l.type === "background" || (l.source === "basemap" && l.type !== "symbol"));
    out[t] = { version: 8, sources: { basemap: { ...s.sources.basemap, url: undefined } }, layers };
  }
  return out;
}

/** Serve one file with HTTP Range (what a PMTiles reader needs). */
function sendFile(req, res, path, type) {
  const size = statSync(path).size;
  const m = /bytes=(\d+)-(\d*)/.exec(req.headers.range ?? "");
  if (!m) {
    res.writeHead(200, { "Content-Type": type, "Content-Length": size, "Accept-Ranges": "bytes" });
    createReadStream(path).pipe(res);
    return;
  }
  const start = Number(m[1]);
  const end = m[2] ? Math.min(Number(m[2]), size - 1) : size - 1;
  res.writeHead(206, { "Content-Type": type, "Content-Length": end - start + 1, "Accept-Ranges": "bytes",
    "Content-Range": `bytes ${start}-${end}/${size}` });
  createReadStream(path, { start, end }).pipe(res);
}

const PAGE = `<!doctype html><html><head><meta charset="utf-8">
<style>body{margin:0;display:flex}div.m{position:relative}</style>
<script src="/maplibre-gl.js"></script><script src="/pmtiles.js"></script></head><body>
<div class="m" id="a"></div><div class="m" id="b"></div>
<script>
(async () => {
  const cfg = await (await fetch("/config.json")).json();
  const proto = new pmtiles.Protocol();
  maplibregl.addProtocol("pmtiles", proto.tile);
  for (const id of ["a", "b"]) Object.assign(document.getElementById(id).style, {width: cfg.size + "px", height: cfg.size + "px"});
  const post = (path, body) => fetch(path, {method: "POST", body: JSON.stringify(body)});
  const mk = (id, theme, file) => new maplibregl.Map({container: id, interactive: false, fadeDuration: 0,
    attributionControl: false, canvasContextAttributes: {preserveDrawingBuffer: true}, pixelRatio: 1,
    style: {...cfg.styles[theme], sources: {basemap: {...cfg.styles[theme].sources.basemap, url: "pmtiles://" + location.origin + "/" + file}}},
    center: [0, 0], zoom: 1});
  const idle = (m) => new Promise((r) => { if (m.loaded() && m.areTilesLoaded()) setTimeout(r, 50); m.once("idle", r); });
  const pixels = (m) => { const c = m.getCanvas(); const x = document.createElement("canvas"); x.width = c.width; x.height = c.height;
    const g = x.getContext("2d"); g.drawImage(c, 0, 0); return {g, x, d: g.getImageData(0, 0, c.width, c.height)}; };
  try {
    for (const theme of cfg.themes) {
      const A = mk("a", theme, "before.pmtiles"), B = mk("b", theme, "after.pmtiles");
      await Promise.all([new Promise((r) => A.once("load", r)), new Promise((r) => B.once("load", r))]);
      for (const [place, c] of Object.entries(cfg.extents)) for (const z of cfg.zooms) {
        A.jumpTo({center: c, zoom: z}); B.jumpTo({center: c, zoom: z});
        await Promise.all([idle(A), idle(B)]);
        const pa = pixels(A), pb = pixels(B);
        const n = pa.d.data.length / 4; let diff = 0;
        const dv = new ImageData(pa.d.width, pa.d.height);
        for (let i = 0; i < pa.d.data.length; i += 4) {
          const off = Math.max(Math.abs(pa.d.data[i] - pb.d.data[i]), Math.abs(pa.d.data[i+1] - pb.d.data[i+1]), Math.abs(pa.d.data[i+2] - pb.d.data[i+2])) > 24;
          diff += off; dv.data[i] = off ? 255 : pa.d.data[i] / 3; dv.data[i+1] = off ? 0 : pa.d.data[i+1] / 3; dv.data[i+2] = off ? 0 : pa.d.data[i+2] / 3; dv.data[i+3] = 255;
        }
        const pct = 100 * diff / n;
        const shot = {theme, place, z, pct};
        if (pct > cfg.keepPct) { pb.g.putImageData(dv, 0, 0); shot.png = {before: pa.x.toDataURL(), after: B.getCanvas().toDataURL(), diff: pb.x.toDataURL()}; }
        await post("/shot", shot);
      }
      A.remove(); B.remove();
    }
    await post("/done", {});
  } catch (e) { await post("/done", {error: String(e && e.stack || e)}); }
})();
</script></body></html>`;

async function main() {
  const [before, after] = process.argv.slice(2).filter((a) => !a.startsWith("--")).slice(0, 2).map((p) => resolve(p));
  if (!before || !after) throw new Error("usage: basemap-shots.mjs <before.pmtiles> <after.pmtiles> [--out DIR]");
  const out = resolve(arg("out", "basemap-shots"));
  const themes = arg("themes", "light,dark").split(",");
  const threshold = Number(arg("threshold", THRESHOLD_PCT));
  const cfg = { styles: await styles(themes), themes, extents: EXTENTS, size: Number(arg("size", 512)),
    zooms: arg("zooms", "8,10,12,14,16").split(",").map(Number), keepPct: process.argv.includes("--keep-all") ? -1 : threshold / 10 };
  mkdirSync(out, { recursive: true });
  const shots = [];
  let finish;
  const done = new Promise((r) => { finish = r; });
  const files = { "/before.pmtiles": [before, "application/octet-stream"], "/after.pmtiles": [after, "application/octet-stream"],
    "/maplibre-gl.js": [join(APP, "node_modules/maplibre-gl/dist/maplibre-gl.js"), "text/javascript"],
    "/pmtiles.js": [join(APP, "node_modules/pmtiles/dist/pmtiles.js"), "text/javascript"] };
  const server = createServer((req, res) => {
    if (req.method === "POST") {
      let body = "";
      req.on("data", (c) => { body += c; });
      req.on("end", () => {
        const j = JSON.parse(body);
        res.end("ok");
        if (req.url === "/shot") {
          const name = `${j.theme}-${j.place}-z${j.z}`;
          if (j.png) for (const [k, v] of Object.entries(j.png)) writeFileSync(join(out, `${name}-${k}.png`), Buffer.from(v.split(",")[1], "base64"));
          delete j.png;
          shots.push(j);
          console.log(`  ${j.pct > threshold ? "OVER" : "ok  "} ${name.padEnd(36)} ${j.pct.toFixed(3)}%`);
        } else finish(j);
      });
      return;
    }
    if (req.url === "/") { res.writeHead(200, { "Content-Type": "text/html" }); res.end(PAGE); return; }
    if (req.url === "/config.json") { res.writeHead(200, { "Content-Type": "application/json" }); res.end(JSON.stringify(cfg)); return; }
    const f = files[req.url];
    if (!f) { res.writeHead(404); res.end(); return; }
    sendFile(req, res, ...f);
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${server.address().port}/`;
  const profile = mkdtempSync(join(tmpdir(), "basemap-shots-"));
  const chrome = spawn(chromePath(), ["--headless=new", "--use-angle=swiftshader", "--enable-unsafe-swiftshader",
    "--ignore-gpu-blocklist", "--no-first-run", `--user-data-dir=${profile}`, "--window-size=1100,600", url],
  { stdio: "ignore" });
  const timer = setTimeout(() => finish({ error: "timed out" }), Number(arg("timeout", 1800)) * 1000);
  const end = await done;
  clearTimeout(timer);
  const exited = new Promise((r) => chrome.once("exit", r));
  chrome.kill("SIGTERM");
  await Promise.race([exited, new Promise((r) => setTimeout(r, 5000))]);
  server.close();
  try { rmSync(profile, { recursive: true, force: true }); } catch { /* Chrome's helpers may still hold it */ }
  const worst = shots.reduce((m, s) => Math.max(m, s.pct), 0);
  const over = shots.filter((s) => s.pct > threshold);
  writeFileSync(join(out, "report.json"), JSON.stringify({ before, after, threshold, worst, over: over.length,
    mean: shots.reduce((m, s) => m + s.pct, 0) / (shots.length || 1), shots }, null, 2));
  if (end.error) { console.error(end.error); process.exit(2); }
  console.log(`  ${shots.length} shots, worst ${worst.toFixed(3)}%, ${over.length} over ${threshold}%  (${out})`);
  process.exit(over.length ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(2); });
