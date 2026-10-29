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
  /** puts skipped because the object's content had not changed (change-aware runs) */
  writesSkipped: number;
  /** sources answered "not modified" / unchanged, so their parse + compute was skipped */
  sourcesUnchanged: number;
}

export const newMeter = (): Meter => ({ subrequests: 0, r2ClassA: 0, r2ClassB: 0, bytesIn: 0, bytesRead: 0, bytesWritten: 0, writesSkipped: 0, sourcesUnchanged: 0 });

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

// ── change-aware writes (owner 2026-10-28: "only WRITE an object when its content differs") ─────────────────────────

/** A fast 64-bit content hash (two 32-bit FNV-1a/murmur-mixed lanes) — change detection, not security. */
export function contentHash(s: string): string {
  let h1 = 0xdeadbeef ^ s.length;
  let h2 = 0x41c6ce57 ^ s.length;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    h1 = Math.imul(h1 ^ c, 2654435761);
    h2 = Math.imul(h2 ^ c, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (h2 >>> 0).toString(16).padStart(8, "0") + (h1 >>> 0).toString(16).padStart(8, "0");
}

/** What was last written per key: the hash of its STABLE content and when it was written. Kept in a private object. */
export type WriteManifest = Record<string, { h: string; t: number }>;

export interface SkipSameOptions {
  now: number;
  /** the part of a body that matters (default: all of it) — e.g. an index without its run stamp */
  stable?: (key: string, body: string) => string;
  /** rewrite anyway when the last write is older than this (a heartbeat: the app's "checked" age never exceeds it) */
  maxAgeMs?: (key: string) => number | undefined;
  meter?: Meter;
}

/** A Store whose `put` is skipped when the key's stable content hashes the same as its last write. */
export function skipSameStore<S extends Store>(s: S, manifest: WriteManifest, o: SkipSameOptions): S & { changed: boolean } {
  const out = Object.create(s) as S & { changed: boolean };
  out.changed = false;
  out.get = (k: string) => s.get(k);
  out.put = async (key: string, body: string, opts?: PutOptions) => {
    const h = contentHash(o.stable ? o.stable(key, body) : body);
    const was = manifest[key];
    const age = o.maxAgeMs?.(key);
    if (was && was.h === h && (age === undefined || o.now - was.t < age)) {
      if (o.meter) o.meter.writesSkipped += 1;
      return;
    }
    await s.put(key, body, opts);
    manifest[key] = { h, t: o.now };
    out.changed = true;
  };
  return out;
}

export async function loadManifest(priv: Store, key: string): Promise<WriteManifest> {
  try {
    const t = await priv.get(key);
    const m = t === null ? {} : JSON.parse(t);
    return m && typeof m === "object" && !Array.isArray(m) ? m : {};
  } catch {
    return {};
  }
}
