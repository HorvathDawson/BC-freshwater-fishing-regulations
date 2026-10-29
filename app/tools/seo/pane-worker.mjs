/**
 * T279 — ONE RENDER THREAD of the SEO build (started by tools/seo/build.ts, at most 4): its own module runner, then
 * tools/seo/paneWorker.ts serves the main thread's page requests one at a time.
 */
import { parentPort, workerData } from "node:worker_threads";
import { makeRunner } from "./runner.mjs";

const r = await makeRunner();
const { serve } = await r.run(`${r.app}tools/seo/paneWorker.ts`);
await serve(parentPort, workerData);
