/**
 * T279 — THE RENDER THREADS (how many: build.ts `renderThreads`). The main thread builds the site's model and hubs; each
 * water page is handed to the next free thread (tools/seo/pane-worker.mjs → paneWorker.ts), which renders and checks it and
 * answers with its HTML. One page per thread at a time: a render owns its thread's hook answers.
 * (T917) The page is then compressed and written by the job threads (jobs.ts), and the render thread takes its next page at
 * once. A render thread is not given a page while the job threads have more than `JOB_BACKLOG` pages per thread waiting
 * (the waiting pages' HTML is held here).
 */
import { Worker } from "node:worker_threads";
import { storedSize, type PageFacts } from "./compressed";
import type { JobPool } from "./jobs";
import type { PaneCssParts } from "./pane";
import type { ThreadData, ToThread } from "./paneWorker";
import type { WaterMade } from "./site";

type FromThread = { t: "ready" } | { t: "css"; parts: PaneCssParts }
  | { t: "done"; n: number; html?: string; facts?: PageFacts; problems?: string[]; error?: string };

interface Job { n: number; msg: Extract<ToThread, { t: "page" }>; done: (m: WaterMade) => void; fail: (e: Error) => void }

const JOB_BACKLOG = 4;

export class PanePool {
  private readonly threads: { w: Worker; busy: Job | null; ready: boolean }[] = [];
  private readonly queue: Job[] = [];
  private n = 0;
  private failed: Error | null = null;
  private made = 0;
  private closing = false;
  private readonly started = Date.now();

  /** `script`: the thread's module (a test gives one that dies) */
  constructor(count: number, private readonly data: ThreadData, private readonly jobs: JobPool,
              script: URL = new URL("./pane-worker.mjs", import.meta.url)) {
    jobs.onIdle = () => this.next();
    for (let i = 0; i < count; i++) {
      const w = new Worker(script, { workerData: data });
      const t = { w, busy: null as Job | null, ready: false };
      // (T917 review) a render thread that ends before close() (no 'error' event) fails its page and every waiting one
      w.on("exit", (code: number) => {
        if (this.closing) return;
        const e = new Error(`seo: a render thread exited (code ${code})${t.busy ? ` on ${JSON.stringify(t.busy.msg)}` : ""}; the build stops`);
        this.failed ??= e; t.busy?.fail(e); t.busy = null; t.ready = false;
        for (const j of this.queue.splice(0)) j.fail(e);
      });
      w.on("message", (m: FromThread) => {
        if (m.t === "ready") { t.ready = true; this.next(); return; }
        if (m.t !== "done") return;
        const job = t.busy!;
        t.busy = null;
        if (m.error) job.fail(new Error(`seo: render thread failed on ${JSON.stringify(job.msg)}: ${m.error}`));
        else {
          const facts = m.facts!, problems = m.problems ?? [];
          this.jobs.zip(this.data.out, job.msg.file, m.html!).then((z) => {
            job.done({ html: null, size: storedSize(facts, z), problems });
            if (++this.made % 1000 === 0) console.log(`seo: ${this.made} water pages, ${Math.round((Date.now() - this.started) / 1000)} s`);
          }, (e: Error) => job.fail(e));
        }
        this.next();
      });
      w.on("error", (err: unknown) => { const e = err instanceof Error ? err : new Error(String(err)); this.failed = e; t.busy?.fail(e); t.busy = null; for (const j of this.queue.splice(0)) j.fail(e); });
      this.threads.push(t);
    }
  }

  private next(): void {
    for (const t of this.threads) {
      if (!t.ready || t.busy || !this.queue.length || this.jobs.waiting > JOB_BACKLOG * this.jobs.size) continue;
      t.busy = this.queue.shift()!;
      t.w.postMessage(t.busy.msg);
    }
  }

  page(id: string, region: string | null, head: string, file: string, gaugeLinks = ""): Promise<WaterMade> {
    if (this.failed) return Promise.reject(this.failed);
    return new Promise((done, fail) => {
      const n = ++this.n;
      this.queue.push({ n, msg: { t: "page", n, id, region, head, file, gaugeLinks }, done, fail });
      this.next();
    });
  }

  /** every thread's part of the shared stylesheet (asked once the pages are done) */
  css(): Promise<PaneCssParts[]> {
    if (this.failed) return Promise.reject(this.failed);
    return Promise.all(this.threads.map((t) => new Promise<PaneCssParts>((res, rej) => {
      const gone = (code: number) => { if (!this.closing) rej(new Error(`seo: a render thread exited (code ${code}) before its part of the stylesheet; the build stops`)); };
      const on = (m: FromThread) => { if (m.t === "css") { t.w.off("message", on); t.w.off("exit", gone); res(m.parts); } };
      t.w.on("message", on);
      t.w.once("exit", gone);
      t.w.postMessage({ t: "css" } satisfies ToThread);
    })));
  }

  async close(): Promise<void> { this.closing = true; await Promise.all(this.threads.map((t) => t.w.terminate())); }
}
