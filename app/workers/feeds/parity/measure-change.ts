// T225 follow-up: what the change-aware skip saves, measured in node on recordings (CPU = process.cpuUsage; R2 ops from
// the Meter). Gauges: the full-roster recording (fixtures-full/, local), runs 2 and 3 an hour apart with the CSVs
// unchanged (served 304 on a matching ETag) vs the same runs with the skip off. DFO: 9 committed pages, a second run
// on the same pages, aware vs not.
//   node app/workers/feeds/parity/gauges/run.mjs parity/measure-change.ts [fixtures dir]
import { readFileSync, existsSync } from "node:fs";
import { join, resolve } from "node:path";
import { contentHash, meteredStore, newMeter, type Fetch } from "../src/io";
import { runGauges } from "../src/gauges/publish";
import { runDfo } from "../src/dfo";
import { fixtureFetch, loadRecording, memStore, readTree } from "./gauges/node";

const FEEDS = resolve(new URL(".", import.meta.url).pathname, "..");
const fix = resolve(process.argv[2] ?? join(FEEDS, "fixtures-full/gauges"));
const rec = loadRecording(fix);
const roster = JSON.parse(readFileSync(join(fix, "roster.json"), "utf-8"));
function etagFetch(): Fetch {
  const base = fixtureFetch(rec).fetch;
  return async (url, init) => {
    const r = await base(url, init);
    if (!url.includes("_hourly_hydrometric.csv")) return r;
    const body = await r.text();
    const etag = `"${contentHash(body)}"`;
    if (new Headers(init?.headers).get("If-None-Match") === etag) return new Response(null, { status: 304 });
    return new Response(body, { status: r.status, headers: { ETag: etag } });
  };
}
const cpu = () => { const u = process.cpuUsage(); return (u.user + u.system) / 1000; };
async function gauges(aware: boolean) {
  const seed = (sub: string) => { const m = new Map<string, string>(); for (const [k, v] of readTree(join(fix, "seed", sub))) m.set(k, v); return m; };
  const feed = memStore(seed("feed"));
  const priv = memStore(seed("priv"));
  const out: any[] = [];
  for (let k = 0; k < 3; k++) {
    const m = newMeter();
    const c0 = cpu();
    const s = await runGauges({ fetch: etagFetch(), feed: meteredStore(feed, m), priv: meteredStore(priv, m), now: new Date(Date.parse(rec.now) + k * 3600_000), roster, sleep: async () => {}, changeAware: aware, meter: m, noReleaseCheck: false });
    out.push({ run: k + 1, cpuMs: Math.round(cpu() - c0), r2A: m.r2ClassA, r2B: m.r2ClassB, mbWritten: +(m.bytesWritten / 1e6).toFixed(2), unchanged: m.sourcesUnchanged, answered: s.answered });
  }
  return out;
}
console.log("gauges, skip OFF", JSON.stringify(await gauges(false)));
console.log("gauges, skip ON ", JSON.stringify(await gauges(true)));

const FIX = join(FEEDS, "test/fixtures/dfo");
const slugs = ["1", "2", "3", "4", "5a", "5b", "6", "7", "8"];
async function dfo(aware: boolean) {
  const feed = memStore();
  const priv = memStore();
  const out: any[] = [];
  for (let k = 0; k < 3; k++) {
    const m = newMeter();
    const c0 = cpu();
    await runDfo({
      fetch: async () => new Response(""), feed: meteredStore(feed, m), priv: meteredStore(priv, m), meter: m, changeAware: aware,
      now: `2026-10-28T${String(6 + 6 * k).padStart(2, "0")}:00:00Z`, regions: slugs,
      entries: async (s) => (existsSync(join(FIX, `entries/region-${s}.json`)) ? readFileSync(join(FIX, `entries/region-${s}.json`), "utf-8") : null),
      fetcher: async (s) => [readFileSync(join(FIX, `pages/region${s}-eng.html`), "utf-8"), null],
    });
    out.push({ run: k + 1, cpuMs: Math.round(cpu() - c0), r2A: m.r2ClassA, r2B: m.r2ClassB, skippedWrites: m.writesSkipped, unchanged: m.sourcesUnchanged });
  }
  return out;
}
console.log("dfo, skip OFF", JSON.stringify(await dfo(false)));
console.log("dfo, skip ON ", JSON.stringify(await dfo(true)));
