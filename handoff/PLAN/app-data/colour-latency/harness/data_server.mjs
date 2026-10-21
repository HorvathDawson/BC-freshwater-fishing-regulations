// The colour-latency DATA server for the real app (the expo web dev build): the live tiles, one release and the live
// gauge feed, READ-ONLY, on a port of its own — never the owner's 39217/39218. Laid out as `app/tools/serve-tiles.mjs`
// lays them out (the app's `EXPO_PUBLIC_DATA_BASE` points here), plus ONE thing it does not have: a switch for the feed's
// index, so a feed refresh can be simulated with no tile and no release touched (the Conditions measurement's "feed
// alone" check).
//
//   node data_server.mjs <port> [release id]          e.g. 39560 r-7b6e802bf22b
//   GET /__feed?index=<path to an index.json>         serve that file as /feeds/live/index.json from now on
//   GET /__feed                                       back to the live one
//   GET /__log                                        the requests served since start (path, range), as JSON
import { createReadStream, existsSync, readFileSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";

const MAIN = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/";
const wt = fileURLToPath(new URL("../../../../../", import.meta.url));
const [port, release = "r-7b6e802bf22b"] = process.argv.slice(2);
const FEEDS = `${MAIN}data/generated/gauges/feeds/`;
let indexOverride = null;
const log = [];

const FILES = {
  "/atlas.pmtiles": `${MAIN}data/generated/tiles/atlas.pmtiles`,
  "/atlas.meta.json": `${MAIN}data/generated/tiles/atlas.meta.json`,
  "/basemap.pmtiles": `${MAIN}data/generated/tiles/basemap.pmtiles`,
  "/bc_outside.geojson": `${MAIN}data/source/bc_outside.geojson`,
};

function fileFor(route) {
  if (FILES[route]) return FILES[route];
  if (/^\/[\w.-]+\.wasm$/.test(route)) return `${wt}app/node_modules/sql.js/dist/${route.slice(1)}`;
  const feed = /^\/feeds\/live\/([\w-]+\.json)$/.exec(route);
  if (feed) return feed[1] === "index.json" && indexOverride ? indexOverride : `${FEEDS}${feed[1]}`;
  if (route === "/release/manifest.json") return `${MAIN}data/release/${release}/manifest.json`;
  if (route.startsWith("/release/")) {
    const rel = decodeURIComponent(route.slice("/release/".length));
    if (rel.split("/").includes("..")) return undefined;
    return `${MAIN}data/release/${rel}`;
  }
  return undefined;
}

createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Range");
  res.setHeader("Access-Control-Expose-Headers", "Content-Range, Content-Length");
  if (req.method === "OPTIONS") return res.writeHead(204).end();
  const url = new URL(req.url ?? "/", "http://x");
  const route = url.pathname;
  if (route === "/__feed") {
    indexOverride = url.searchParams.get("index");
    return res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify({ index: indexOverride }));
  }
  if (route === "/__log") return res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify(log));
  const file = fileFor(route);
  log.push([Date.now(), route, req.headers.range ?? null]);
  if (!file || !existsSync(file)) return res.writeHead(404).end("no such file");
  // the feed is never cached by the browser: the app's own TTL decides when it is asked again
  if (route.startsWith("/feeds/")) res.setHeader("Cache-Control", "no-store");
  if (file.endsWith(".gz")) {
    res.writeHead(200, { "Content-Type": "application/octet-stream", "Content-Encoding": "gzip",
                         "Content-Length": statSync(file).size });
    return req.method === "HEAD" ? res.end() : createReadStream(file).pipe(res);
  }
  const total = statSync(file).size;
  const type = route.endsWith(".wasm") ? "application/wasm" : route.endsWith(".json") ? "application/json"
    : route.endsWith(".geojson") ? "application/geo+json" : "application/octet-stream";
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range ?? "");
  if (!m) {
    res.writeHead(200, { "Content-Type": type, "Accept-Ranges": "bytes", "Content-Length": total });
    return req.method === "HEAD" ? res.end() : total < 50e6 ? res.end(readFileSync(file)) : createReadStream(file).pipe(res);
  }
  const start = m[1] === "" ? total - Number(m[2]) : Number(m[1]);
  const end = m[1] === "" || m[2] === "" ? total - 1 : Math.min(Number(m[2]), total - 1);
  res.writeHead(206, { "Content-Type": type, "Accept-Ranges": "bytes", "Content-Length": end - start + 1,
                       "Content-Range": `bytes ${start}-${end}/${total}` });
  createReadStream(file, { start, end }).pipe(res);
}).listen(Number(port), "127.0.0.1", () => console.log(`data on http://127.0.0.1:${port} (release ${release})`));
