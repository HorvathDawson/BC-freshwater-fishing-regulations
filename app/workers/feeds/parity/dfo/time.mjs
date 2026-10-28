// T225 DFO feed: what one Worker invocation costs. Bundles src/dfo with esbuild (the size the Worker ships), then runs
// `runDfo` over the recorded pages preloaded in memory, through the metered Fetch / Store of src/io.ts.
//
//   node --expose-gc app/workers/feeds/parity/dfo/time.mjs
//
// (a) typical: every region fetched (the Python re-types every page every run, and so does the port), two of them
//     changed since the last feed; (b) worst: no previous feed, all nine real pages typed and joined from scratch;
// (c) single region: region 6 alone (the largest page) — a per-invocation-per-region split if CPU were tight.
import { build } from "../../../../node_modules/esbuild/lib/main.js";
import { gzipSync } from "node:zlib";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const FEEDS = join(HERE, "../..");
const FIX = join(FEEDS, "test/fixtures/dfo");
const tmp = mkdtempSync(join(tmpdir(), "dfo-time-"));

// ── the Worker bundle of the DFO code ────────────────────────────────────────────────────────────────────────────────
writeFileSync(join(tmp, "entry.ts"), `export { runDfo } from ${JSON.stringify(join(FEEDS, "src/dfo/index.ts"))};\n`);
await build({ entryPoints: [join(tmp, "entry.ts")], bundle: true, minify: true, format: "esm", outfile: join(tmp, "dfo.min.js"), platform: "neutral", logLevel: "error" });
const min = readFileSync(join(tmp, "dfo.min.js"));
// a node-importable build of the same code, plus the io helpers
writeFileSync(join(tmp, "run.ts"), `export { runDfo } from ${JSON.stringify(join(FEEDS, "src/dfo/index.ts"))};\nexport { meteredFetch, meteredStore, newMeter } from ${JSON.stringify(join(FEEDS, "src/io.ts"))};\n`);
await build({ entryPoints: [join(tmp, "run.ts")], bundle: true, format: "esm", outfile: join(tmp, "run.mjs"), platform: "neutral", logLevel: "error" });
const { runDfo, meteredFetch, meteredStore, newMeter } = await import(pathToFileURL(join(tmp, "run.mjs")).href);

const SLUGS = ["1", "2", "3", "4", "5a", "5b", "6", "7", "8"];
const pages = Object.fromEntries(SLUGS.map((s) => [s, readFileSync(join(FIX, `pages/region${s}-eng.html`), "utf-8")]));
const entries = Object.fromEntries(SLUGS.map((s) => [s, readFileSync(join(FIX, `entries/region-${s}.json`), "utf-8")]));

class Mem {
  constructor() {
    this.m = new Map();
  }
  async get(k) {
    return this.m.has(k) ? this.m.get(k) : null;
  }
  async put(k, v) {
    this.m.set(k, v);
  }
}

let peak = 0;
const sample = () => {
  const h = process.memoryUsage().heapUsed;
  if (h > peak) peak = h;
};

async function once(label, { prevFeed = null, pageOf = (s) => pages[s], regions = null, priv = new Mem() } = {}) {
  const m = newMeter();
  const feed = new Mem();
  if (prevFeed) for (const [k, v] of prevFeed.m) feed.m.set(k, v);
  for (const s of SLUGS) priv.m.set(`entries/region-${s}.json`, entries[s]);
  const fetch = meteredFetch(async (url) => {
    sample();
    const slug = /region(\w+)-eng\.html$/.exec(url)?.[1];
    const body = pageOf(slug);
    return new Response(body, { headers: { "content-length": String(Buffer.byteLength(body)) } });
  }, m);
  globalThis.gc?.();
  peak = process.memoryUsage().heapUsed;
  const base = peak;
  const c0 = process.cpuUsage();
  const t0 = performance.now();
  await runDfo({ fetch, feed: meteredStore(feed, m), priv: meteredStore(priv, m), now: "2026-10-28T12:00:00Z", regions, notifyUrl: null, fetchOptions: { sleep: async () => {} } });
  const wall = performance.now() - t0;
  const c = process.cpuUsage(c0);
  sample();
  const row = {
    run: label,
    cpu_ms: +((c.user + c.system) / 1000).toFixed(1),
    wall_ms: +wall.toFixed(1),
    subrequests: m.subrequests,
    r2_class_a: m.r2ClassA,
    r2_class_b: m.r2ClassB,
    kb_fetched: +(m.bytesIn / 1024).toFixed(1),
    kb_r2_read: +(m.bytesRead / 1024).toFixed(1),
    kb_r2_written: +(m.bytesWritten / 1024).toFixed(1),
    peak_heap_mb: +(peak / 1048576).toFixed(1),
    heap_growth_mb: +((peak - base) / 1048576).toFixed(1),
  };
  return { row, feed, priv };
}

// warm the JIT the way a long-lived isolate would be (a cold isolate is reported separately below)
const cold = await once("(b) worst, cold isolate: no previous feed, all 9 pages typed");
for (let i = 0; i < 3; i++) await once("warmup");
const worst = await once("(b) worst, warm: no previous feed, all 9 pages typed");
const changed = { 1: readFileSync(join(FIX, "history/region1_20260116024805.html"), "utf-8"), 6: readFileSync(join(FIX, "history/region6_20260413113750.html"), "utf-8") };
const typical = await once("(a) typical: 9 fetched, 2 changed since the last feed", { prevFeed: worst.feed, pageOf: (s) => changed[s] ?? pages[s], priv: worst.priv });
const single = await once("(c) region 6 alone", { prevFeed: worst.feed, regions: ["6"], priv: worst.priv });

console.table([cold.row, worst.row, typical.row, single.row]);
console.log(`bundle (runDfo + deps, esbuild --bundle --minify --format=esm): ${(min.length / 1024).toFixed(1)} KiB, gzip ${(gzipSync(min).length / 1024).toFixed(1)} KiB`);
