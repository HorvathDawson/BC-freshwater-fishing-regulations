/**
 * (T237) THE REPORT OUTBOX — a problem report the reader sent, POSTed to the report endpoint (the feeds worker's
 * `POST /report`), or kept on the device and sent later. Platform-free: the store (localStorage / a file) and the
 * network's word are injected.
 *
 *   SEND      `send(p)`: POST now. 2xx → sent. Offline (by the device's word, or a network error), a 5xx, a 408 or a
 *             429 (too many from this address) → QUEUED, kept in the store, and sent by `flush()`. Any other 4xx → the
 *             endpoint refused the report as it is (too big, malformed): said, never queued (it would be refused again).
 *   FLUSH     `flush()`: every queued report, oldest first; stops at the first one that cannot go yet. The update
 *             service (T231) calls it on each check and the moment the network comes back (`UpdateOptions.outbox`), so
 *             a report made offline goes out with the app's own retry, not a timer of its own.
 *   ONCE      a payload carries its own id (made on the device); the endpoint stores by id, so a report re-sent after an
 *             answer was lost is stored once.
 */
import type { ReportPayload } from "@app/core";

export interface ReportStore { read(): string | null; write(text: string): void }

export type ReportSendResult =
  | { kind: "sent" }
  | { kind: "queued"; offline: boolean }
  | { kind: "refused"; error: string };

export interface ReportOutbox {
  send(p: ReportPayload): Promise<ReportSendResult>;
  /** Send what is queued, oldest first. Resolves with how many went. Never throws. */
  flush(): Promise<number>;
  /** Reports waiting on this device. */
  pending(): number;
  subscribe(on: () => void): () => void;
}

/** Kept at most: the oldest are dropped past this (a device that never comes back online must not grow forever). */
const MAX_QUEUED = 50;

export function createReportOutbox(o: {
  url: string;
  store?: ReportStore;
  fetch?: typeof fetch;
  online?: () => boolean;
}): ReportOutbox {
  const f = o.fetch ?? ((u: RequestInfo | URL, i?: RequestInit) => fetch(u, i));
  let queue: ReportPayload[] = [];
  try {
    const t = o.store?.read();
    const q = t ? JSON.parse(t) : null;
    if (Array.isArray(q)) queue = q.filter((p) => p && typeof p.id === "string");
  } catch { /* unreadable: nothing queued */ }
  const listeners = new Set<() => void>();
  const save = () => {
    try { o.store?.write(JSON.stringify(queue)); } catch { /* kept in memory for this run */ }
    for (const l of [...listeners]) l();
  };

  /** one POST: sent, retry later, or refused for good */
  async function post(p: ReportPayload): Promise<ReportSendResult> {
    if (o.online && !o.online()) return { kind: "queued", offline: true };
    let r: Response;
    try {
      r = await f(o.url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p) });
    } catch {
      return { kind: "queued", offline: true };
    }
    if (r.ok) return { kind: "sent" };
    if (r.status >= 500 || r.status === 408 || r.status === 429) return { kind: "queued", offline: false };
    let why = `HTTP ${r.status}`;
    try {
      const j = (await r.json()) as { error?: unknown };
      if (typeof j?.error === "string") why = j.error;
    } catch { /* the status says it */ }
    return { kind: "refused", error: why };
  }

  let flushing: Promise<number> | null = null;
  return {
    async send(p) {
      // a report already queued goes after them, in order
      if (queue.length) {
        queue.push(p);
        save();
        await this.flush();
        return queue.some((q) => q.id === p.id) ? { kind: "queued", offline: !!o.online && !o.online() } : { kind: "sent" };
      }
      const r = await post(p);
      if (r.kind === "queued") {
        queue = [...queue, p].slice(-MAX_QUEUED);
        save();
      }
      return r;
    },
    flush() {
      if (flushing) return flushing;
      flushing = (async () => {
        let sent = 0;
        while (queue.length) {
          const r = await post(queue[0]!);
          if (r.kind === "queued") break;
          // sent, or refused for good (never sendable): either way it leaves the queue
          queue = queue.slice(1);
          if (r.kind === "sent") sent++;
          save();
        }
        return sent;
      })().finally(() => { flushing = null; });
      return flushing;
    },
    pending: () => queue.length,
    subscribe(on) { listeners.add(on); return () => { listeners.delete(on); }; },
  };
}

/** A report id: random, 16 hex characters (the endpoint's key; not a secret). */
export function reportId(rand: () => number = Math.random): string {
  let s = "";
  for (let i = 0; i < 16; i++) s += Math.floor(rand() * 16).toString(16);
  return s;
}
