// T225 gauge feed timing: runGauges against a recording with every byte preloaded into memory (no disk or network in
// the measured path), metered as Cloudflare bills it (src/io.ts). Reports CPU ms (process.cpuUsage), wall ms,
// subrequests, R2 class A/B, bytes, peak heapUsed; cold (first run in this process) and warm (median of the rest).
//   node app/workers/feeds/parity/gauges/run.mjs parity/gauges/time.ts [fixtureDir ...]
import { join, resolve } from "node:path";
import { readFileSync } from "node:fs";
import { meteredFetch, meteredStore, newMeter } from "../../src/io";
import { runGauges } from "../../src/gauges/publish";
import { FEEDS, fixtureFetch, loadRecording, readTree } from "./node";

async function once(dir: string, rec: ReturnType<typeof loadRecording>, seedFeed: Map<string, string>, seedPriv: Map<string, string>, roster: any) {
  const m = newMeter();
  const ff = fixtureFetch(rec);
  // LEAN stores: the seed held as Buffers (off the JS heap, as R2 objects are), decoded per get; puts counted and
  // dropped — so heapUsed is the job's own, not the harness's
  const lean = (seed: Map<string, string>) => {
    const bufs = new Map([...seed].map(([k, v]) => [k, Buffer.from(v)]));
    return {
      async get(k: string) {
        const b = bufs.get(k);
        return b ? b.toString("utf-8") : null;
      },
      async put(k: string, body: string) {
        bufs.set(k, Buffer.from(body));
      },
      async delete(k: string) {
        bufs.delete(k);
      },
    };
  };
  const feed = lean(seedFeed);
  const priv = lean(seedPriv);
  (globalThis as any).gc?.();
  const base = process.memoryUsage().heapUsed;
  if (process.env.SHOW_BASE) console.error("baseline heapUsed MB", Math.round(base / 2 ** 20));
  let peak = 0;
  let live = 0;
  const tick = setInterval(() => (peak = Math.max(peak, process.memoryUsage().heapUsed)), 2);
  const c0 = process.cpuUsage();
  const w0 = performance.now();
  const s = await runGauges({
    fetch: meteredFetch(ff.fetch, m), feed: meteredStore(feed, m), priv: { ...meteredStore(priv, m), delete: priv.delete },
    now: new Date(rec.now), roster, sleep: async () => {},
    // LIVE SET at each stage: a full GC on every log line (only with --expose-gc and LIVE=1)
    log: process.env.LIVE ? (line: string) => {
      (globalThis as any).gc?.();
      const mb = Math.round((process.memoryUsage().heapUsed - base) / 2 ** 20);
      live = Math.max(live, mb);
      console.error(`live ${mb} MB  ${line.slice(0, 70)}`);
    } : undefined,
  });
  const wall = performance.now() - w0;
  const cpu = process.cpuUsage(c0);
  clearInterval(tick);
  peak = Math.max(peak, process.memoryUsage().heapUsed);
  void dir;
  return { cpuMs: (cpu.user + cpu.system) / 1000, wallMs: wall, meter: m, peakHeapMB: (peak - base) / 2 ** 20, answered: s.answered, liveMB: live, misses: ff.misses.length, urls: ff };
}

function median(xs: number[]) {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)]!;
}

const dirs = process.argv.slice(2).length
  ? process.argv.slice(2).map((d) => resolve(d))
  : [join(FEEDS, "test/fixtures/gauges"), join(FEEDS, "fixtures-full/gauges")];

for (const dir of dirs) {
  const rec = loadRecording(dir);
  const feedSeed = readTree(join(dir, "seed/feed"));
  const climSeed = feedSeed.get("clim.json") ?? "{}";
  const privSeed = readTree(join(dir, "seed/priv"));
  const roster = JSON.parse(readFileSync(join(dir, "roster.json"), "utf-8"));
  const runs = [];
  for (let i = 0; i < 6; i++) {
    (globalThis as any).gc?.();
    runs.push(await once(dir, rec, feedSeed, privSeed, roster));
  }
  const cold = runs[0]!;
  const warm = runs.slice(1);
  // the per-run JSON.parse of clim.json (the Worker reads it from R2 every run)
  const clim = climSeed;
  const pc = [];
  for (let i = 0; i < 6; i++) {
    const c0 = process.cpuUsage();
    JSON.parse(clim);
    const c = process.cpuUsage(c0);
    pc.push((c.user + c.system) / 1000);
  }
  const m = cold.meter;
  console.log(JSON.stringify({
    fixtures: dir.replace(FEEDS + "/", ""),
    stations: cold.answered,
    cpuMs: { cold: Math.round(cold.cpuMs), warmMedian: Math.round(median(warm.map((r) => r.cpuMs))) },
    wallMs: { cold: Math.round(cold.wallMs), warmMedian: Math.round(median(warm.map((r) => r.wallMs))) },
    peakHeapAboveFixturesMB: Math.round(Math.max(...runs.map((r) => r.peakHeapMB))),
    subrequests: m.subrequests, r2ClassA: m.r2ClassA, r2ClassB: m.r2ClassB,
    bytesInMB: +(m.bytesIn / 2 ** 20).toFixed(2), bytesReadMB: +(m.bytesRead / 2 ** 20).toFixed(2), bytesWrittenMB: +(m.bytesWritten / 2 ** 20).toFixed(2),
    climJson: { MB: +(clim.length / 2 ** 20).toFixed(2), parseCpuMs: { cold: Math.round(pc[0]!), warmMedian: Math.round(median(pc.slice(1))) } },
    misses: cold.misses,
  }));
}
