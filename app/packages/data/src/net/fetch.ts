/**
 * (T254) A DOWNLOAD THAT SURVIVES A BAD LINE: per-size timeouts, retries with backoff, and resume from where it stopped.
 *
 *   TIMEOUTS     `timeoutsFor` (policy.ts): no response within `firstByteMs`, or no new byte for `stallMs`, cuts the try;
 *                so does the whole-file limit. A body that keeps arriving, however slowly, is never cut by the stall rule.
 *   RETRIES      a cut, a network error, a 5xx / 408 / 429 is tried again after `backoffMs`, up to `attempts` tries. A
 *                4xx is an answer, not a failure of the line: thrown at once. While the device says it is OFFLINE the
 *                next try waits for it to come back (`waitOnline`) instead of spending its tries on a dead line — so a
 *                page that asked while the line was down fills itself in when it returns.
 *   RESUME       the bytes a cut try received are kept, and the next try asks for the rest (`Range: bytes=N-`) — only
 *                when the body is the stored bytes themselves (`resumable(head)`: the caller knows the file's first
 *                bytes, e.g. gzip's magic or "%PDF"). A body the browser decoded on the way (`Content-Encoding: gzip`)
 *                has no offsets the server knows, so it starts again from 0. A server that answers a Range with 200
 *                sends the whole file: the kept bytes are dropped. The caller verifies the result (sha256, P10): a
 *                resumed file that is wrong is never accepted.
 *   NOT BLOCKING every wait is a timer and every try is abortable (`signal`): nothing here holds the UI.
 *
 * The meter (`meter.ts`) is told every chunk, scaled to the wire (`wireScale`), so the app's measured rate is the link's.
 */
import { MAX_ATTEMPTS, backoffMs, timeoutsFor, type Timeouts } from "./policy";
import type { NetMeter } from "./meter";

export class NetError extends Error {
  constructor(message: string, readonly kind: "timeout" | "stalled" | "network" | "http" | "offline",
              readonly status: number | null = null, readonly attempts = 1) { super(message); }
}

export interface ResilientOptions {
  fetchImpl?: typeof fetch;
  /** about how many bytes arrive on the wire (the timeouts are sized by it); null: unknown */
  wireBytes?: number | null;
  signal?: AbortSignal;
  /** bytes received so far in all, and the expected total when known (Content-Length, else `wireBytes`) */
  onProgress?: (loaded: number, total: number | null) => void;
  meter?: NetMeter | null;
  /** body bytes → wire bytes for the meter (a decoded gzip body is larger than what crossed the line); default 1 */
  wireScale?: (res: Response, head: Uint8Array) => number;
  /** whether a body that starts with `head` is the stored file itself (so a Range into it means what it says) */
  resumable?: (head: Uint8Array) => boolean;
  attempts?: number;
  /** the limits (default `timeoutsFor(wireBytes)`) */
  limits?: Timeouts;
  /** resolves when the device is online again; absent: the device is never known to be offline */
  waitOnline?: (signal?: AbortSignal) => Promise<void>;
  isOnline?: () => boolean;
  /** extra request headers (the app version, T251) */
  headers?: Record<string, string>;
  /** fetch init beyond headers / signal (e.g. `cache: "force-cache"`) */
  init?: RequestInit;
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  rand?: () => number;
  /** an event per retry, for the Updates log (T251) */
  onRetry?: (e: { attempt: number; waitMs: number; why: string; resumeAt: number }) => void;
}

const abortError = () => { const e = new Error("aborted"); e.name = "AbortError"; return e; };

export const sleepFor = (ms: number, signal?: AbortSignal): Promise<void> => new Promise((ok, fail) => {
  if (signal?.aborted) return fail(abortError());
  const t = setTimeout(() => { signal?.removeEventListener("abort", stop); ok(); }, ms);
  const stop = () => { clearTimeout(t); fail(abortError()); };
  signal?.addEventListener("abort", stop, { once: true });
});

const retryable = (status: number) => status >= 500 || status === 408 || status === 429;

function concat(parts: readonly Uint8Array[], n: number): Uint8Array {
  if (parts.length === 1 && parts[0]!.length === n) return parts[0]!;
  const out = new Uint8Array(n);
  let at = 0;
  for (const p of parts) { out.set(p, at); at += p.length; }
  return out;
}

/** Download `url` whole, through cuts, stalls and drops; throws `NetError` (or the abort) when it cannot. */
export async function fetchResilient(url: string, o: ResilientOptions = {}): Promise<Uint8Array> {
  const fetchImpl = o.fetchImpl ?? fetch;
  const attempts = o.attempts ?? MAX_ATTEMPTS;
  const sleep = o.sleep ?? sleepFor;
  const lim = o.limits ?? timeoutsFor(o.wireBytes ?? null);
  let parts: Uint8Array[] = [];
  let got = 0;
  let resumable = false;
  let last: NetError | null = null;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (o.signal?.aborted) throw abortError();
    if (attempt > 1) {
      // the line is down: wait for it (not a try spent), then go again at once
      if (o.isOnline && !o.isOnline() && o.waitOnline) {
        const t = Date.now();
        await o.waitOnline(o.signal);
        if (Date.now() - t > 100) attempt--;                // a real wait: not a try spent (a waiter that does not wait is)
      }
      else {
        const waitMs = backoffMs(attempt, o.rand);
        o.onRetry?.({ attempt, waitMs, why: last?.message ?? "", resumeAt: resumable ? got : 0 });
        await sleep(waitMs, o.signal);
      }
    }
    if (!resumable) { parts = []; got = 0; }
    const from = got;
    const ctl = new AbortController();
    const onOuter = () => ctl.abort();
    o.signal?.addEventListener("abort", onOuter, { once: true });
    let why: NetError | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const arm = (ms: number, e: NetError) => { if (timer) clearTimeout(timer); timer = setTimeout(() => { why = e; ctl.abort(); }, ms); };
    const total = setTimeout(() => { why = new NetError(`${url}: no whole file in ${lim.totalMs} ms`, "timeout"); ctl.abort(); }, lim.totalMs);
    const h = o.meter?.start() ?? 0;
    try {
      arm(lim.firstByteMs, new NetError(`${url}: no answer in ${lim.firstByteMs} ms`, "timeout"));
      const headers = { ...(o.headers ?? {}), ...(from > 0 ? { Range: `bytes=${from}-` } : {}) };
      const res = await fetchImpl(url, { ...(o.init ?? {}), headers, signal: ctl.signal });
      if (from > 0 && res.status === 200) { parts = []; got = 0; }           // the server sent it whole
      else if (from > 0 && res.status !== 206 && res.ok) { parts = []; got = 0; }
      if (!res.ok) {
        const e = new NetError(`${url}: HTTP ${res.status}`, "http", res.status, attempt);
        if (retryable(res.status) && res.status !== 416) why = e;
        throw e;
      }
      arm(lim.stallMs, new NetError(`${url}: stalled (no byte in ${lim.stallMs} ms)`, "stalled"));
      const len = Number(res.headers?.get?.("content-length"));
      const expect = len > 0 ? got + len : o.wireBytes ?? null;
      const reader = res.body?.getReader?.();
      let scale = 1;
      if (!reader) {
        // no stream (an older runtime): the whole body at once, under the whole-file limit
        const all = new Uint8Array(await res.arrayBuffer());
        if (got === 0) { resumable = !!o.resumable?.(all.subarray(0, 8)); scale = o.wireScale?.(res, all.subarray(0, 8)) ?? 1; }
        parts.push(all); got += all.length;
        o.meter?.chunk(h, all.length * scale);
        o.onProgress?.(got, expect);
      } else {
        let first = got === 0;
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          if (!value?.length) continue;
          if (first) {
            first = false;
            resumable = !!o.resumable?.(value.subarray(0, 8));
            scale = o.wireScale?.(res, value.subarray(0, 8)) ?? 1;
          }
          parts.push(value); got += value.length;
          arm(lim.stallMs, new NetError(`${url}: stalled (no byte in ${lim.stallMs} ms)`, "stalled"));
          o.meter?.chunk(h, value.length * scale);
          o.onProgress?.(got, expect);
        }
      }
      o.meter?.end(h);
      return concat(parts, got);
    } catch (e) {
      o.meter?.end(h);
      if (o.signal?.aborted) throw abortError();
      if (e instanceof NetError && e.kind === "http" && (!retryable(e.status ?? 0) || e.status === 416)) throw e;
      last = why ?? (e instanceof NetError ? e : new NetError(`${url}: ${e instanceof Error ? e.message : String(e)}`,
        o.isOnline && !o.isOnline() ? "offline" : "network"));
    } finally {
      if (timer) clearTimeout(timer);
      clearTimeout(total);
      o.signal?.removeEventListener("abort", onOuter);
    }
  }
  throw new NetError(last?.message ?? `${url}: failed`, last?.kind ?? "network", last?.status ?? null, attempts);
}

/**
 * (T254) A SMALL JSON FILE over a bad line (the gauge feed's index and station files): the response and its body within
 * `limits.totalMs`, else the try is cut; tried `attempts` times with backoff. A 4xx answers at once (`{ status }`, not an
 * error: a 404 means "not transmitting"); a dead line throws `NetError`. The body is read with `res.json()`, so any
 * fetch that answers JSON will do.
 */
export async function fetchJsonResilient<T>(url: string, o: { fetchImpl?: typeof fetch; attempts?: number; limits?: Timeouts; cache?: RequestCache;
                                                                sleep?: (ms: number) => Promise<void>; rand?: () => number } = {}):
    Promise<{ ok: true; value: T } | { ok: false; status: number }> {
  const fetchImpl = o.fetchImpl ?? fetch;
  const attempts = o.attempts ?? 2;
  const lim = o.limits ?? timeoutsFor(null);
  const sleep = o.sleep ?? sleepFor;
  let last: NetError | null = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (attempt > 1) await sleep(backoffMs(attempt, o.rand));
    const ctl = new AbortController();
    let timer: ReturnType<typeof setTimeout> | null = null;
    try {
      const cut = new Promise<never>((_, fail) => {
        timer = setTimeout(() => { ctl.abort(); fail(new NetError(`${url}: no answer in ${lim.totalMs} ms`, "timeout")); }, lim.totalMs);
      });
      const got = await Promise.race([(async () => {
        const res = await fetchImpl(url, { signal: ctl.signal, ...(o.cache ? { cache: o.cache } : {}) });
        if (!res.ok) return { ok: false as const, status: res.status };
        return { ok: true as const, value: (await res.json()) as T };
      })(), cut]);
      if (!got.ok && (got.status >= 500 || got.status === 408 || got.status === 429)) {
        last = new NetError(`${url}: HTTP ${got.status}`, "http", got.status, attempt);
        continue;
      }
      return got;
    } catch (e) {
      last = e instanceof NetError ? e : new NetError(`${url}: ${e instanceof Error ? e.message : String(e)}`, "network");
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
  throw new NetError(last?.message ?? `${url}: failed`, last?.kind ?? "network", last?.status ?? null, attempts);
}
