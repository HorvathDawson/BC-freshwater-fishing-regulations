// Colour-latency harness server: the page, maplibre/pmtiles from app/node_modules, and the LIVE tiles + release paint
// READ-ONLY with Range support. Never the owner's ports (39217/39218).
//   node server.mjs <port> <style.json> <cells.json>
import { createReadStream, readFileSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
const wt = fileURLToPath(new URL("../../../../../", import.meta.url));
const MAIN = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/";
const [port, style, cells] = process.argv.slice(2);
const nm = `${wt}app/node_modules/`;
const routes = {
  "/": [`${here}index.html`, "text/html"],
  "/harness.js": [`${here}harness.js`, "text/javascript"],
  "/mvt-ids.js": [`${here}mvt-ids.js`, "text/javascript"],
  "/maplibre-gl.js": [`${nm}maplibre-gl/dist/maplibre-gl.js`, "text/javascript"],
  "/maplibre-gl.css": [`${nm}maplibre-gl/dist/maplibre-gl.css`, "text/css"],
  "/pmtiles.js": [`${nm}pmtiles/dist/pmtiles.js`, "text/javascript"],
  "/atlas.pmtiles": [`${MAIN}data/generated/tiles/atlas.pmtiles`, "application/octet-stream"],
  "/basemap.pmtiles": [`${MAIN}data/generated/tiles/basemap.pmtiles`, "application/octet-stream"],
  "/bc_outside.geojson": [`${MAIN}data/source/bc_outside.geojson`, "application/json"],
  "/paint.bin.gz": [`${MAIN}data/release/r-6960fe8201e3/paint-d0f41712777f.bin.gz`, "application/octet-stream"],
  "/style.json": [style, "application/json"],
  "/cells.json": [cells, "application/json"],
};
createServer((req, res) => {
  const path = new URL(req.url, "http://x").pathname;
  const r = routes[path];
  if (!r) { res.writeHead(404); res.end(); return; }
  const [file, type] = r;
  const size = statSync(file).size;
  const head = { "Content-Type": type, "Access-Control-Allow-Origin": "*", "Accept-Ranges": "bytes",
                 "Cache-Control": "no-store" };
  const m = /bytes=(\d+)-(\d*)/.exec(req.headers.range ?? "");
  if (m) {
    const a = Number(m[1]), b = m[2] ? Math.min(Number(m[2]), size - 1) : size - 1;
    res.writeHead(206, { ...head, "Content-Range": `bytes ${a}-${b}/${size}`, "Content-Length": b - a + 1 });
    createReadStream(file, { start: a, end: b }).pipe(res);
  } else {
    res.writeHead(200, { ...head, "Content-Length": size });
    if (size < 50e6) res.end(readFileSync(file)); else createReadStream(file).pipe(res);
  }
}).listen(Number(port), "127.0.0.1", () => console.log(`harness on http://127.0.0.1:${port}/`));
