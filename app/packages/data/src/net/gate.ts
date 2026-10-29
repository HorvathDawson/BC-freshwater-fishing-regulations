/**
 * (T254) THE ORDER ON A POOR LINE: in low-bandwidth mode a download waits until everything before it in the order
 * (`policy.ts` PRIORITY: the answer, then the extras, then what is fetched ahead) has finished — so the tapped water's
 * regulations are not sharing one bar with its salmon slice and a neighbour region's prefetch. Off low mode every
 * download starts at once (as before T254).
 *
 * KEYED: one download per key (a file name). Asking again for a key already waiting joins it, and RAISES it to the
 * asker's priority — a file the prefetch queued that the reader now taps for jumps the queue. A download already under
 * way is never stopped (its bytes are the tap's too).
 *
 * `ahead` downloads also run one at a time in low mode. Nothing here times out: the download itself does (`fetch.ts`).
 */
import type { FetchPriority } from "./policy";

export interface FetchGate {
  run<T>(key: string, priority: FetchPriority, work: () => Promise<T>): Promise<T>;
  /** The mode may have changed: let through what now may go. */
  changed(): void;
  /** What is waiting and what is running, by key (for the tests and the Updates log). */
  state(): { running: readonly { key: string; priority: FetchPriority }[]; waiting: readonly { key: string; priority: FetchPriority }[] };
}

interface Entry {
  key: string;
  priority: FetchPriority;
  work: () => Promise<unknown>;
  ok: (v: unknown) => void;
  fail: (e: unknown) => void;
  promise: Promise<unknown>;
  started: boolean;
}

export function createFetchGate(o: { low: () => boolean }): FetchGate {
  const entries = new Map<string, Entry>();
  const running = () => [...entries.values()].filter((e) => e.started);
  /** may `e` start now? */
  const mayStart = (e: Entry): boolean => {
    if (!o.low()) return true;
    const r = running();
    if (r.some((x) => x.priority < e.priority)) return false;
    // an earlier priority still waiting goes first
    if ([...entries.values()].some((x) => !x.started && x.priority < e.priority)) return false;
    if (e.priority === 2 && r.some((x) => x.priority === 2)) return false;
    return true;
  };
  const pump = () => {
    // lowest priority number first, then first asked
    const queue = [...entries.values()].filter((e) => !e.started).sort((a, b) => a.priority - b.priority);
    for (const e of queue) {
      if (!mayStart(e)) continue;
      e.started = true;
      e.work().then((v) => { entries.delete(e.key); e.ok(v); pump(); },
                    (err: unknown) => { entries.delete(e.key); e.fail(err); pump(); });
    }
  };
  return {
    run<T>(key: string, priority: FetchPriority, work: () => Promise<T>): Promise<T> {
      const had = entries.get(key);
      if (had) {
        if (!had.started && priority < had.priority) { had.priority = priority; pump(); }
        return had.promise as Promise<T>;
      }
      let ok!: (v: unknown) => void, fail!: (e: unknown) => void;
      const promise = new Promise<unknown>((a, b) => { ok = a; fail = b; });
      entries.set(key, { key, priority, work, ok, fail, promise, started: false });
      pump();
      return promise as Promise<T>;
    },
    changed: pump,
    state: () => ({
      running: running().map((e) => ({ key: e.key, priority: e.priority })),
      waiting: [...entries.values()].filter((e) => !e.started).map((e) => ({ key: e.key, priority: e.priority })),
    }),
  };
}
