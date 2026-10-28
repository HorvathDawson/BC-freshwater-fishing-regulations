/**
 * Serves the built .pmtiles over HTTP with RANGE support. `pnpm tiles`.
 *
 * A PMTiles archive is read by asking for byte ranges — the client fetches a header, then
 * a directory, then one tile at a time. That is the whole point of the format: 835 MB on
 * disk, a few KB over the wire per tile. So this server exists to do exactly one thing
 * properly, which is honour `Range`. A static server that ignores it "works" by sending the
 * entire archive for every tile request, which looks like a hang rather than an error.
 *
 * Development only. In production the same bytes sit on R2 and the browser range-requests
 * them directly; nothing about the client changes, which is the point of the two-storage
 * design (`design/` and the architecture artifact).
 */
import { createReadStream, statSync, existsSync, readdirSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";

const repo = fileURLToPath(new URL("../..", import.meta.url));
const ARCHIVES = {
  "/atlas.pmtiles": `${repo}data/generated/tiles/atlas.pmtiles`,
  // The tiles' sidecar: its stamped `tile_version` proves the drawn tiles pair with the release (`pairingOf`) while
  // the manifest names no tile set of its own.
  "/atlas.meta.json": `${repo}data/generated/tiles/atlas.meta.json`,
  "/basemap.pmtiles": `${repo}data/generated/tiles/basemap.pmtiles`,
  // The outside-BC mask. A file rather than a tile layer so changing how it looks does
  // not need a fifteen-minute rebuild — see pipeline/deliver/tiles/boundary.py.
  "/bc_outside.geojson": `${repo}data/source/bc_outside.geojson`,
};

/**
 * sql.js's engine, from the same origin as the release files it opens (the web opener, `release/drivers/open.web.ts`).
 *
 * Served as a DIRECTORY rather than one named file: which wasm sql.js asks for depends on
 * which JS build the bundler picked — Metro takes the `browser` field and then requests
 * `sql-wasm-browser.wasm`, not `sql-wasm.wasm`. Hardcoding the name produced a 404 that
 * surfaced as "both async and sync fetching of the wasm failed", which says nothing about
 * a filename. Let it ask for what it wants.
 */
const WASM_DIR = `${repo}app/node_modules/sql.js/dist/`;
/**
 * Deliberately not 8080 or 8787.
 *
 * Metro's default is 8081 and every other dev server in the world wants 3000 or 8080, so a
 * second checkout, a stale process or an unrelated project silently answers instead — and
 * a tile server answering with someone else's HTML is a very confusing five minutes.
 */
const PORT = Number(process.env.TILE_PORT ?? 39217);

for (const [route, file] of Object.entries(ARCHIVES)) {
  if (!existsSync(file)) {
    console.error(`missing: ${file}\n  ${route} will 404. Build it first.`);
    continue;
  }
  console.log(`  ${route}  ${(statSync(file).size / 1e6).toFixed(0)} MB`);
}

/**
 * THE RELEASE (P10), laid out as R2 will hold it: `/release/manifest.json` is the newest `data/release/r-*` (or
 * `RELEASE=<id>`), and every url in it — `<release>/<file>.gz`, `tiles/<version>/…` — resolves under `data/release/`.
 * No path may climb out of it.
 */
const RELEASE_ROOT = process.env.RELEASE_ROOT ? `${process.env.RELEASE_ROOT.replace(/\/$/, "")}/` : `${repo}data/release/`;
function releaseFile(route) {
  if (!route.startsWith("/release/")) return undefined;
  const rel = decodeURIComponent(route.slice("/release/".length));
  if (rel.split("/").includes("..")) return undefined;
  if (rel === "manifest.json") {
    // T72: `data/release/LATEST` (written by the release step) names the release; the newest mtime only without one
    const pointer = `${RELEASE_ROOT}LATEST`;
    const id = process.env.RELEASE ?? (existsSync(pointer) ? readFileSync(pointer, "utf8").trim() : existsSync(RELEASE_ROOT)
      ? readdirSync(RELEASE_ROOT).filter((d) => d.startsWith("r-"))
          .sort((a, b) => statSync(`${RELEASE_ROOT}${b}`).mtimeMs - statSync(`${RELEASE_ROOT}${a}`).mtimeMs)[0]
      : undefined);
    return id ? `${RELEASE_ROOT}${id}/manifest.json` : undefined;
  }
  return `${RELEASE_ROOT}${rel}`;
}

createServer((req, res) => {
  // The map runs on a different port in development, so it is cross-origin to this server.
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Range");
  res.setHeader("Access-Control-Expose-Headers", "Content-Range, Content-Length");
  if (req.method === "OPTIONS") return res.writeHead(204).end();

  const route = (req.url ?? "").split("?")[0];
  const file = ARCHIVES[route]
    // `basename` and not a join: a route is not allowed to walk out of the wasm directory.
    ?? (/^\/[\w.-]+\.wasm$/.test(route) ? `${WASM_DIR}${route.slice(1)}` : undefined)
    // THE GAUGE FEED: whatever `python -m pipeline.gauges.feed.publish` last wrote — real ECCC data, matching the
    // release. (The fixture feed, `packages/data/dev/feeds/gauge`, is the tests' and is read from disk, never served.)
    // In production it sits in R2, and the app changes a base URL rather than a code path.
    ?? (/^\/feeds\/live\/[\w-]+\.json$/.test(route)
          ? `${repo}data/generated/gauges/feeds/${route.split("/").pop()}` : undefined)
    // (T211) THE DFO SALMON FEED: whatever `python -m pipeline.regs.dfo_salmon.live` last wrote (R2 `feeds/dfo/` in prod)
    ?? (/^\/feeds\/dfo\/[\w-]+\.json$/.test(route)
          ? `${repo}data/generated/regs/dfo_salmon/feed/${route.split("/").pop()}` : undefined)
    ?? releaseFile(route);
  if (!file || !existsSync(file)) return res.writeHead(404).end("no such archive");

  // A release data file is stored gzipped and sent as such: the client decodes it and checks the RAW bytes and sha256
  // against the manifest (fetchVerified), as it will from R2.
  if (file.endsWith(".gz")) {
    res.writeHead(200, { "Content-Type": "application/octet-stream", "Content-Encoding": "gzip",
                         "Content-Length": statSync(file).size });
    return req.method === "HEAD" ? res.end() : createReadStream(file).pipe(res);
  }
  const total = statSync(file).size;
  const range = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range ?? "");
  const head = {
    // wasm must be served as application/wasm or the browser refuses to stream-compile it.
    "Content-Type": route.endsWith(".wasm") ? "application/wasm"
      : route.endsWith(".json") ? "application/json"
      : route.endsWith(".geojson") ? "application/geo+json"
      : "application/octet-stream",
    "Accept-Ranges": "bytes",
  };
  // A truncated file opens as a valid SQLite file and then answers some queries and not
  // others. The client checks the length it got against this, so it must be right.

  if (!range) {
    res.writeHead(200, { ...head, "Content-Length": total });
    return req.method === "HEAD" ? res.end() : createReadStream(file).pipe(res);
  }
  const start = range[1] === "" ? total - Number(range[2]) : Number(range[1]);
  const end = range[1] === "" || range[2] === "" ? total - 1 : Number(range[2]);
  if (!(start >= 0 && end < total && start <= end))
    return res.writeHead(416, { "Content-Range": `bytes */${total}` }).end();

  res.writeHead(206, { ...head, "Content-Length": end - start + 1,
                       "Content-Range": `bytes ${start}-${end}/${total}` });
  createReadStream(file, { start, end }).pipe(res);
}).listen(PORT, () => console.log(`tiles on http://localhost:${PORT}`));
