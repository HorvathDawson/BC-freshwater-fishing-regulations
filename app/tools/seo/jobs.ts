/**
 * T917 — THE JOB THREADS: plain worker threads (tools/seo/jobs-worker.mjs, no module runner, a few MB each) that compress and
 * write the water pages the render threads made, and draw the share cards. Each takes one job at a time from one queue, in
 * the order asked. The bytes are bytes.mjs's, the same functions the in-process paths call.
 */
import { Worker } from "node:worker_threads";
import type { Card } from "./bytes.mjs";

export interface Zipped { readonly gzipBytes: number; readonly brBytes: number; readonly rawBytes: number }
type Ask = { t: "zip"; root: string; file: string; html: string } | { t: "png"; svg: string };
type Msg = Ask & { n: number };
type Reply = { n: number; zip?: Zipped; png?: Uint8Array; error?: string };
interface Job { msg: Msg; done: (r: Reply) => void; fail: (e: Error) => void }

export class JobPool {
  private readonly threads: { w: Worker; busy: Job | null }[] = [];
  private readonly queue: Job[] = [];
  private n = 0;
  private failed: Error | null = null;
  /** called whenever a job finishes (the render pool waits on the backlog: `waiting`) */
  onIdle: () => void = () => {};

  constructor(count: number, card: Card) {
    for (let i = 0; i < count; i++) {
      const w = new Worker(new URL("./jobs-worker.mjs", import.meta.url), { workerData: { card } });
      const t = { w, busy: null as Job | null };
      w.on("message", (r: Reply) => {
        const job = t.busy!;
        t.busy = null;
        if (r.error) job.fail(new Error(`seo: job thread failed on ${job.msg.t} ${job.msg.t === "zip" ? job.msg.file : `#${job.msg.n}`}: ${r.error}`));
        else job.done(r);
        this.next();
        this.onIdle();
      });
      w.on("error", (err: unknown) => {
        const e = err instanceof Error ? err : new Error(String(err));
        this.failed = e; t.busy?.fail(e); t.busy = null;
        for (const j of this.queue.splice(0)) j.fail(e);
      });
      this.threads.push(t);
    }
  }

  /** jobs asked and not yet taken by a thread */
  get waiting(): number { return this.queue.length; }
  get size(): number { return this.threads.length; }

  private next(): void {
    for (const t of this.threads) {
      if (t.busy || !this.queue.length) continue;
      t.busy = this.queue.shift()!;
      t.w.postMessage(t.busy.msg);
    }
  }

  private ask(msg: Ask): Promise<Reply> {
    if (this.failed) return Promise.reject(this.failed);
    return new Promise((done, fail) => {
      this.queue.push({ msg: { ...msg, n: ++this.n }, done, fail });
      this.next();
    });
  }

  /** a water page written compressed under `root` (`<file>.br` + `<file>.gz`, never the raw page) and its byte counts */
  async zip(root: string, file: string, html: string): Promise<Zipped> { return (await this.ask({ t: "zip", root, file, html })).zip!; }

  /** a share card drawn as its PNG */
  async png(svg: string): Promise<Uint8Array> { return (await this.ask({ t: "png", svg })).png!; }

  async close(): Promise<void> { await Promise.all(this.threads.map((t) => t.w.terminate())); }
}
