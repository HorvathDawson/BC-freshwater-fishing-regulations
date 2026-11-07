/**
 * THE LABEL SHOTS (T299): what names does the map draw, and are any of them outside BC?
 *
 *   node tools/label-shots.mjs --out DIR [--basemap PATH] [--atlas PATH] [--themes light,dark] [--zooms 7,9,11,13]
 *        [--places lower-mainland,osoyoos,blaine] [--width 900] [--height 640] [--chrome PATH]
 *
 * Draws the app's whole composed style (`runtimeStyle`, labels included) over the local archives — the atlas and the
 * basemap (defaults: data/generated/tiles/) with the newest release's fonts and sprites — at every place x zoom x theme,
 * headless (never a visible window), and writes one PNG per shot plus `report.json`: for each shot, how many names each
 * symbol layer placed, and how many basemap names sit OUTSIDE British Columbia (a point inside the outside-BC mask).
 */
import { createServer } from "node:http";
import { createReadStream, existsSync, mkdirSync, readFileSync, statSync, writeFileSync, mkdtempSync, rmSync } from "node:fs";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { makeRunner } from "./seo/runner.mjs";

const APP = fileURLToPath(new URL("..", import.meta.url));
const REPO = fileURLToPath(new URL("../..", import.meta.url));

/** Centres: the Lower Mainland and two border crossings (a town on each side of the line). */
export const PLACES = {
  "lower-mainland": [-122.75, 49.2],
  "osoyoos": [-119.47, 49.03],
  "blaine": [-122.75, 49.0],
  // (T311) Garibaldi Park, and its south edge where the upper Pitt closes — not in the default set
  "garibaldi": [-122.8, 49.95],
  "upper-pitt": [-122.7, 49.74],
  // (T457 / T461) lakes big and small, and tidal water: not in the default set
  "harrison-lake": [-121.85, 49.45],
  "hope-lakes": [-121.35, 49.35],
  "fraser-estuary": [-123.1, 49.1],
  "howe-sound": [-123.3, 49.5],
};
const DEFAULT_PLACES = ["lower-mainland", "osoyoos", "blaine"];

const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : dflt;
};

function chromePath() {
  const cands = [arg("chrome"), process.env.CHROME, "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome", "/usr/bin/chromium"];
  const p = cands.find((c) => c && existsSync(c));
  if (!p) throw new Error("no Chrome found: pass --chrome PATH or set CHROME");
  return p;
}

/** The newest release's map assets folder (fonts + sprites), as the tile server serves it. */
function assetsDir() {
  const root = join(REPO, "data/release/");
  const id = readFileSync(join(root, "LATEST"), "utf8").trim();
  const url = JSON.parse(readFileSync(join(root, id, "manifest.json"), "utf8")).tiles.assets.url;
  return join(root, url);
}

async function styles(themes) {
  const runner = await makeRunner();
  const { runtimeStyle } = await runner.run(join(APP, "packages/map/src/runtime-style.ts"));
  await runner.close();
  const AT = { atlas: "ORIGIN/atlas.pmtiles", basemap: "ORIGIN/basemap.pmtiles", outside: "ORIGIN/outside.geojson",
    assets: "ORIGIN/assets/", gauges: '{"type":"FeatureCollection","features":[]}' };
  return Object.fromEntries(themes.map((t) => [t, runtimeStyle(AT, t)]));
}

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
<style>body{margin:0}#m{position:relative}</style>
<script src="/maplibre-gl.js"></script><script src="/pmtiles.js"></script></head><body><div id="m"></div>
<script>
(async () => {
  const cfg = await (await fetch("/config.json")).json();
  const mask = await (await fetch("/outside.geojson")).json();
  const proto = new pmtiles.Protocol();
  maplibregl.addProtocol("pmtiles", proto.tile);
  Object.assign(document.getElementById("m").style, {width: cfg.width + "px", height: cfg.height + "px"});
  const post = (path, body) => fetch(path, {method: "POST", body: JSON.stringify(body)});
  const fix = (s) => JSON.parse(JSON.stringify(s).split("ORIGIN").join(location.origin));
  const rings = mask.features.flatMap((f) => f.geometry.type === "Polygon" ? f.geometry.coordinates : f.geometry.coordinates.flat());
  const inRing = (p, r) => { let c = false; for (let i = 0, j = r.length - 1; i < r.length; j = i++) {
    const [xi, yi] = r[i], [xj, yj] = r[j];
    if ((yi > p[1]) !== (yj > p[1]) && p[0] < (xj - xi) * (p[1] - yi) / (yj - yi) + xi) c = !c; } return c; };
  const outside = (p) => rings.reduce((c, r) => c !== inRing(p, r), false);
  const idle = (m) => new Promise((r) => { m.once("idle", r); m.triggerRepaint(); });
  try {
    for (const theme of cfg.themes) {
      const style = fix(cfg.styles[theme]);
      const m = new maplibregl.Map({container: "m", interactive: false, fadeDuration: 0, attributionControl: false,
        canvasContextAttributes: {preserveDrawingBuffer: true}, pixelRatio: 1, style, center: [-122.75, 49.2], zoom: 7});
      await new Promise((r) => m.once("load", r));
      const symbols = style.layers.filter((l) => l.type === "symbol").map((l) => l.id);
      for (const [place, c] of Object.entries(cfg.places)) for (const z of cfg.zooms) {
        m.jumpTo({center: c, zoom: z});
        await idle(m);
        await new Promise((r) => setTimeout(r, 300));
        await idle(m);
        const feats = m.queryRenderedFeatures({layers: symbols});
        const byLayer = {}, out = [];
        for (const f of feats) {
          byLayer[f.layer.id] = (byLayer[f.layer.id] || 0) + 1;
          if (f.layer.source === "basemap" && f.geometry.type === "Point" && outside(f.geometry.coordinates))
            out.push(f.layer.id + ":" + (f.properties.name || f.properties["name:en"] || ""));
        }
        await post("/shot", {theme, place, z, byLayer, outside: out, png: m.getCanvas().toDataURL()});
      }
      m.remove();
    }
    await post("/done", {});
  } catch (e) { await post("/done", {error: String(e && e.stack || e)}); }
})();
</script></body></html>`;

async function main() {
  const out = resolve(arg("out", "label-shots"));
  const themes = arg("themes", "light,dark").split(",");
  const zooms = arg("zooms", "7,9,11,13").split(",").map(Number);
  const places = Object.fromEntries(arg("places", DEFAULT_PLACES.join(",")).split(",").map((p) => [p, PLACES[p]]));
  const basemap = resolve(arg("basemap", join(REPO, "data/generated/tiles/basemap.pmtiles")));
  const atlas = resolve(arg("atlas", join(REPO, "data/generated/tiles/atlas.pmtiles")));
  const assets = assetsDir();
  const cfg = { styles: await styles(themes), themes, zooms, places,
    width: Number(arg("width", 900)), height: Number(arg("height", 640)) };
  mkdirSync(out, { recursive: true });
  const shots = [];
  let finish;
  const done = new Promise((r) => { finish = r; });
  const files = { "/basemap.pmtiles": [basemap, "application/octet-stream"], "/atlas.pmtiles": [atlas, "application/octet-stream"],
    "/outside.geojson": [join(REPO, "data/source/bc_outside.geojson"), "application/json"],
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
          const name = `${j.place}-z${j.z}-${j.theme}`;
          writeFileSync(join(out, `${name}.png`), Buffer.from(j.png.split(",")[1], "base64"));
          delete j.png;
          shots.push(j);
          const water = (j.byLayer.stream__label ?? 0) + (j.byLayer.lake__label ?? 0);
          console.log(`  ${name.padEnd(30)} water names ${String(water).padStart(3)}  places ${String(j.byLayer.places_locality ?? 0).padStart(3)}  outside-BC ${j.outside.length}`);
        } else finish(j);
      });
      return;
    }
    const url = decodeURIComponent(req.url.split("?")[0]);
    if (url === "/") { res.writeHead(200, { "Content-Type": "text/html" }); res.end(PAGE); return; }
    if (url === "/config.json") { res.writeHead(200, { "Content-Type": "application/json" }); res.end(JSON.stringify(cfg)); return; }
    if (url.startsWith("/assets/") && !url.includes("..")) {
      const p = join(assets, url.slice("/assets/".length));
      if (existsSync(p)) { sendFile(req, res, p, p.endsWith(".png") ? "image/png" : p.endsWith(".json") ? "application/json" : "application/x-protobuf"); return; }
      res.writeHead(404); res.end(); return;
    }
    const f = files[url];
    if (!f) { res.writeHead(404); res.end(); return; }
    sendFile(req, res, ...f);
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${server.address().port}/`;
  const profile = mkdtempSync(join(tmpdir(), "label-shots-"));
  const chrome = spawn(chromePath(), ["--headless=new", "--use-angle=swiftshader", "--enable-unsafe-swiftshader",
    "--ignore-gpu-blocklist", "--no-first-run", `--user-data-dir=${profile}`, "--window-size=1000,700", url],
  { stdio: "ignore" });
  const timer = setTimeout(() => finish({ error: "timed out" }), Number(arg("timeout", 900)) * 1000);
  const end = await done;
  clearTimeout(timer);
  const exited = new Promise((r) => chrome.once("exit", r));
  chrome.kill("SIGTERM");
  await Promise.race([exited, new Promise((r) => setTimeout(r, 5000))]);
  server.close();
  try { rmSync(profile, { recursive: true, force: true }); } catch { /* Chrome's helpers may still hold it */ }
  writeFileSync(join(out, "report.json"), JSON.stringify({ basemap, atlas, shots }, null, 2));
  if (end.error) { console.error(end.error); process.exit(2); }
  console.log(`  ${shots.length} shots -> ${out}`);
}

main().catch((e) => { console.error(e); process.exit(2); });
