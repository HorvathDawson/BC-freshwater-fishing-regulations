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

/**
 * THE DEPLOYED CODE'S VERSION (Cloudflare version metadata, set once per isolate by the entry points). Mixed into every
 * job's reuse key (`rev`): a deploy recomputes everything once, so a change of output never waits on a hand-bumped
 * constant (owner 2026-11-05: the forecast detail stayed old after a worker release). Empty under test and `wrangler dev`.
 */
let deployId = "";
export function setDeployId(id: string | undefined): void { deployId = id ?? ""; }
export const rev = (base: string): string => (deployId ? `${base}@${deployId}` : base);

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

/**
 * (T558 W13) A store that can write ONLY IF an object is unchanged since it was read: two jobs that read, change and write
 * the same state object (status.json, notify/state.json) at once no longer lose one's change (last writer wins).
 */
export interface VersionedStore extends Store {
  /** the text and its version tag (R2 etag), or null when the object does not exist */
  getVersioned(key: string): Promise<{ text: string; etag: string } | null>;
  /** write while the object is still at `etag`; false = another writer got there first. `etag` null = the object did not
   *  exist when read: written unconditionally (R2 has no "only if absent" here; only the very first write could race) */
  putIfUnchanged(key: string, body: string, etag: string | null, opts?: PutOptions): Promise<boolean>;
}
export const isVersioned = (s: Store): s is VersionedStore =>
  typeof (s as Partial<VersionedStore>).getVersioned === "function" && typeof (s as Partial<VersionedStore>).putIfUnchanged === "function";

/**
 * (T558 W13) Write `next` over `key` read at `etag`; on a conflict re-read and `merge` this run's change onto what the other
 * writer left, once. A second conflict THROWS (the caller says so); a store that cannot version writes plainly (node runs).
 */
export async function writeMerged<T>(store: Store, key: string, next: T, etag: string | null,
                                     merge: (fresh: T | null) => T, opts?: PutOptions): Promise<void> {
  if (!isVersioned(store)) { await store.put(key, JSON.stringify(next), opts); return; }
  if (await store.putIfUnchanged(key, JSON.stringify(next), etag, opts)) return;
  const fresh = await store.getVersioned(key);
  let parsed: T | null = null;
  if (fresh) {
    try { parsed = JSON.parse(fresh.text) as T; } catch (e) { throw new Error(`${key} changed under this run and does not parse (${String(e)})`); }
  }
  if (await store.putIfUnchanged(key, JSON.stringify(merge(parsed)), fresh?.etag ?? null, opts)) return;
  throw new Error(`${key} was changed by another job twice while this run wrote it; this run's change is not saved`);
}

/** A Store over an R2 bucket under `prefix` (e.g. "feeds/live/"). Worker-only: `R2Bucket` is the runtime binding. */
export function r2Store(bucket: R2BucketLike, prefix: string): VersionedStore & { delete(key: string): Promise<void> } {
  return {
    async getVersioned(key) {
      const o = await bucket.get(prefix + key);
      if (!o) return null;
      if (!o.etag) throw new Error(`${prefix + key}: the bucket gave no etag`);
      return { text: await o.text(), etag: o.etag };
    },
    async putIfUnchanged(key, body, etag, opts) {
      const r = await bucket.put(prefix + key, body, {
        httpMetadata: { contentType: opts?.contentType ?? "application/json", cacheControl: opts?.cacheControl },
        ...(etag ? { onlyIf: { etagMatches: etag } } : {}),
      });
      return r !== null;   // R2 answers null when the condition failed
    },
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
  get(key: string): Promise<{ text(): Promise<string>; etag?: string } | null>;
  /** (T558) with `onlyIf`, R2 answers null when the condition failed */
  put(key: string, body: string, opts?: { httpMetadata?: { contentType?: string; cacheControl?: string }; onlyIf?: { etagMatches: string } }): Promise<unknown>;
  delete(key: string): Promise<void>;
  /** (T237) listing a prefix (the reports); optional so the feed jobs' stores need not provide it */
  list?(o: { prefix: string; cursor?: string; limit?: number }): Promise<{ objects: { key: string }[]; truncated: boolean; cursor?: string }>;
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

/**
 * (T558) A stored JSON object: null ONLY when it does not exist. A read that fails (R2 trouble) or a body that does not
 * parse THROWS, naming the key: "could not read" is never "nothing stored yet", because the caller writes its state back
 * and an empty answer written back erases it (the outbox's told keys, every feed's status). The run fails and says so; the
 * last good object stands.
 */
export async function readStored<T = unknown>(store: Pick<Store, "get">, key: string): Promise<T | null> {
  const t = await store.get(key);
  if (t === null) return null;
  try {
    return JSON.parse(t) as T;
  } catch (e) {
    throw new Error(`${key} is stored but does not parse (${e instanceof Error ? e.message : String(e)}); fix or delete it`);
  }
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
