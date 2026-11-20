/**
 * T917 — ONE JOB THREAD of the SEO build (tools/seo/jobs.ts starts them): the CPU work that needs no page renderer, off the
 * render threads and the main thread. `zip`: a water page's `.br` + `.gz` written (bytes.mjs `compressPage`; was 31 % of a
 * render thread). `png`: a share card drawn (bytes.mjs `drawCard`; was 153 s serial on the main thread). One job at a time;
 * a turn of the event loop after each card lets resvg's ~3 MB pixmap go (share.ts: a synchronous loop held 59 GB).
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { parentPort, workerData } from "node:worker_threads";
import { compressPage, drawCard } from "./bytes.mjs";

parentPort.on("message", async (m) => {
  try {
    if (m.t === "zip") {
      const raw = Buffer.from(m.html), { gz, br } = compressPage(raw), to = join(m.root, m.file);
      mkdirSync(dirname(to), { recursive: true });
      writeFileSync(`${to}.br`, br);
      writeFileSync(`${to}.gz`, gz);
      parentPort.postMessage({ n: m.n, zip: { gzipBytes: gz.length, brBytes: br.length, rawBytes: raw.length } });
    } else if (m.t === "png") {
      const png = drawCard(m.svg, workerData.card);
      await new Promise((turn) => setImmediate(turn));
      parentPort.postMessage({ n: m.n, png });
    } else throw new Error(`seo: a job thread got an unknown job ${JSON.stringify(m.t)}`);
  } catch (e) {
    parentPort.postMessage({ n: m.n, error: e instanceof Error ? (e.stack ?? e.message) : String(e) });
  }
});
