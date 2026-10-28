// T225: the only I/O the feed jobs see. A job takes a `Fetch` (network) and `Store`s (R2 in the Worker, a directory in
// node for the parity test), so the same code runs on a cron, under `wrangler dev`, and against recorded fixtures.
// `Meter` counts what Cloudflare bills: subrequests, R2 class A (put/list) and class B (get/head) ops, bytes.

export type Fetch = (url: string, init?: RequestInit) => Promise<Response>;

export interface PutOptions {
  contentType?: string;
  cacheControl?: string;
}

export interface Store {
  /** The object's text, or null when it does not exist. */
  get(key: string): Promise<string | null>;
  put(key: string, body: string, opts?: PutOptions): Promise<void>;
}

export interface Meter {
  subrequests: number;
  r2ClassA: number;
  r2ClassB: number;
  bytesIn: number; // fetched from sources
  bytesRead: number; // read from R2
  bytesWritten: number; // written to R2
}

export const newMeter = (): Meter => ({ subrequests: 0, r2ClassA: 0, r2ClassB: 0, bytesIn: 0, bytesRead: 0, bytesWritten: 0 });

const enc = new TextEncoder();

export function meteredFetch(f: Fetch, m: Meter): Fetch {
  return async (url, init) => {
    m.subrequests += 1;
    const res = await f(url, init);
    const len = Number(res.headers.get("content-length") ?? NaN);
    if (Number.isFinite(len)) m.bytesIn += len;
    return res;
  };
}

export function meteredStore<S extends Store & { delete?(key: string): Promise<void> }>(s: S, m: Meter): S {
  return {
    ...(s.delete ? { delete: (key: string) => s.delete!(key) } : {}), // R2 DeleteObject is free

    async get(key) {
      m.r2ClassB += 1;
      const v = await s.get(key);
      if (v != null) m.bytesRead += enc.encode(v).length;
      return v;
    },
    async put(key, body, opts) {
      m.r2ClassA += 1;
      m.bytesWritten += enc.encode(body).length;
      await s.put(key, body, opts);
    },
  } as S;
}

/** A Store over an R2 bucket under `prefix` (e.g. "feeds/live/"). Worker-only: `R2Bucket` is the runtime binding. */
export function r2Store(bucket: R2BucketLike, prefix: string): Store & { delete(key: string): Promise<void> } {
  return {
    async delete(key) {
      await bucket.delete(prefix + key);
    },
    async get(key) {
      const o = await bucket.get(prefix + key);
      return o ? await o.text() : null;
    },
    async put(key, body, opts) {
      await bucket.put(prefix + key, body, {
        httpMetadata: { contentType: opts?.contentType ?? "application/json", cacheControl: opts?.cacheControl },
      });
    },
  };
}

/** The slice of the R2 binding used here (keeps @cloudflare/workers-types out of the build). */
export interface R2BucketLike {
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  put(key: string, body: string, opts?: { httpMetadata?: { contentType?: string; cacheControl?: string } }): Promise<unknown>;
  delete(key: string): Promise<void>;
}
