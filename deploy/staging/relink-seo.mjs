// STAGING fast path (T269/T279): after `export:web:staging`, point the stored (gz + br) SEO water pages at the new app
// export's asset names instead of rebuilding them (~8 min vs ~17). Then run deploy/staging/site.sh.
//   node deploy/staging/relink-seo.mjs <seoDir> <oldIndex.html> <newIndex.html>
import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";
import { readFileSync, writeFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import zlib from "node:zlib";

const assets = (f) => [...new Set(readFileSync(f, "utf8").match(/_expo\/static\/[^"']+/g) ?? [])];
const kind = (a) => a.replace(/-[0-9a-f]{32}\./, "-*.");

if (isMainThread) {
  const [seo, oldF, newF] = process.argv.slice(2);
  const olds = assets(oldF), news = assets(newF);
  const map = [];
  for (const o of olds) { const n = news.find((x) => kind(x) === kind(o)); if (n && n !== o) map.push([o, n]); }
  console.log("map", JSON.stringify(map));
  if (!map.length) process.exit(0);
  const files = [];
  const walk = (d) => { for (const f of readdirSync(d)) { const p = join(d, f); if (statSync(p).isDirectory()) walk(p); else if (/index\.html\.(gz|br)$/.test(f)) files.push(p); } };
  walk(join(seo, "water"));
  console.log("files", files.length);
  const N = 4, t0 = Date.now(); let done = 0, changed = 0;
  await Promise.all([...Array(N)].map((_, i) => new Promise((res, rej) => {
    const w = new Worker(new URL(import.meta.url), { workerData: { files: files.filter((_, k) => k % N === i), map } });
    w.on("message", (m) => { done += m.done; changed += m.changed; }); w.on("error", rej); w.on("exit", res);
  })));
  console.log("done", done, "changed", changed, `${((Date.now() - t0) / 1000).toFixed(0)} s`);
} else {
  const { files, map } = workerData;
  let changed = 0;
  for (const f of files) {
    const br = f.endsWith(".br");
    const raw = (br ? zlib.brotliDecompressSync : zlib.gunzipSync)(readFileSync(f)).toString("utf8");
    let s = raw;
    for (const [o, n] of map) s = s.split(o).join(n);
    if (s === raw) continue;
    const out = br
      ? zlib.brotliCompressSync(Buffer.from(s), { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11, [zlib.constants.BROTLI_PARAM_SIZE_HINT]: s.length } })
      : zlib.gzipSync(Buffer.from(s), { level: 9 });
    writeFileSync(f, out); changed++;
  }
  parentPort.postMessage({ done: files.length, changed });
}
