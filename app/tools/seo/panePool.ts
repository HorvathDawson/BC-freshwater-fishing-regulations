/**
 * T279 — THE RENDER THREADS (at most 4: the heavy-jobs rule). The main thread builds the site's model and hubs; each water
 * page is handed to the next free thread (tools/seo/pane-worker.mjs → paneWorker.ts), which renders, checks and writes it
 * compressed, and answers with its numbers. One page per thread at a time: a render owns its thread's hook answers.
 */
import { Worker } from "node:worker_threads";
import type { PaneCssParts } from "./pane";
import type { ThreadData, ToThread } from "./paneWorker";
import type { WaterMade } from "./site";

type FromThread = { t: "ready" } | { t: "css"; parts: PaneCssParts }
  | { t: "done"; n: number; size?: Extract<WaterMade, { html: null }>["size"]; problems?: string[]; error?: string };

interface Job { n: number; msg: ToThread; done: (m: WaterMade) => void; fail: (e: Error) => void }

export class PanePool {
  private readonly threads: { w: Worker; busy: Job | null; ready: boolean }[] = [];
  private readonly queue: Job[] = [];
  private n = 0;
  private failed: Error | null = null;
  private made = 0;
  private readonly started = Date.now();

  constructor(count: number, data: ThreadData) {
    for (let i = 0; i < count; i++) {
      const w = new Worker(new URL("./pane-worker.mjs", import.meta.url), { workerData: data });
      const t = { w, busy: null as Job | null, ready: false };
      w.on("message", (m: FromThread) => {
        if (m.t === "ready") { t.ready = true; this.next(); return; }
        if (m.t !== "done") return;
        const job = t.busy!;
        t.busy = null;
        if (m.error) job.fail(new Error(`seo: render thread failed on ${JSON.stringify(job.msg)}: ${m.error}`));
        else job.done({ html: null, size: m.size!, problems: m.problems ?? [] });
        if (++this.made % 1000 === 0) console.log(`seo: ${this.made} water pages, ${Math.round((Date.now() - this.started) / 1000)} s`);
        this.next();
      });
      w.on("error", (err: unknown) => { const e = err instanceof Error ? err : new Error(String(err)); this.failed = e; t.busy?.fail(e); for (const j of this.queue.splice(0)) j.fail(e); });
      this.threads.push(t);
    }
  }

  private next(): void {
    for (const t of this.threads) {
      if (!t.ready || t.busy || !this.queue.length) continue;
      t.busy = this.queue.shift()!;
      t.w.postMessage(t.busy.msg);
    }
  }

  page(id: string, head: string, file: string): Promise<WaterMade> {
    if (this.failed) return Promise.reject(this.failed);
    return new Promise((done, fail) => {
      const n = ++this.n;
      this.queue.push({ n, msg: { t: "page", n, id, head, file }, done, fail });
      this.next();
    });
  }

  /** every thread's part of the shared stylesheet (asked once the pages are done) */
  css(): Promise<PaneCssParts[]> {
    return Promise.all(this.threads.map((t) => new Promise<PaneCssParts>((res) => {
      const on = (m: FromThread) => { if (m.t === "css") { t.w.off("message", on); res(m.parts); } };
      t.w.on("message", on);
      t.w.postMessage({ t: "css" } satisfies ToThread);
    })));
  }

  async close(): Promise<void> { await Promise.all(this.threads.map((t) => t.w.terminate())); }
}
