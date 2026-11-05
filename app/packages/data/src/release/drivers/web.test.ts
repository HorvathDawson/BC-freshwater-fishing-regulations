/**
 * P11: THE WEB ADAPTERS, run for real in node — sql.js is the same wasm the browser runs, and Cache Storage is the
 * global the browser has (faked here, and absent, and failing).
 */
import { describe, expect, it, vi } from "vitest";
import initSqlJs from "sql.js";

/** a stored .gz body, by the platform's own CompressionStream (the boundary keeps fflate out of the web adapters) */
const gz = async (b: Uint8Array): Promise<Uint8Array<ArrayBuffer>> =>
  new Uint8Array(await new Response(new Blob([b as BlobPart]).stream().pipeThrough(new CompressionStream("gzip"))).arrayBuffer());

const reported: unknown[] = [];
vi.mock("@app/core", async (orig) => ({ ...(await orig<typeof import("@app/core")>()), reportFailure: (e: unknown) => { reported.push(e); } }));
import { sha256Hex } from "../../sha256";
import { parseManifest } from "../../manifest";
import { releaseOpener } from "./open.web";
import { releaseCache } from "./cache.web";
import { releaseSource } from "./source.web";

const WASM = new URL("../../../../../node_modules/sql.js/dist/", import.meta.url).pathname;

async function sqliteBytes(): Promise<Uint8Array> {
  const SQL = await initSqlJs({ locateFile: (f: string) => `${WASM}${f}` });
  const db = new SQL.Database();
  db.run("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL); INSERT INTO meta VALUES ('format', 'core/1')");
  const out = db.export();
  db.close();
  return out;
}

describe("open.web: sql.js over the verified bytes", () => {
  it("opens the bytes and answers all / get, with the `Db` row shape", async () => {
    const b = await sqliteBytes();
    const db = await releaseOpener({ wasmBase: WASM, installed: () => null })("core", async () => b);
    expect(await db.all("SELECT k, v FROM meta")).toEqual([{ k: "format", v: "core/1" }]);
    expect(await db.get("SELECT v FROM meta WHERE k = ?", "format")).toEqual({ v: "core/1" });
    expect(await db.get("SELECT v FROM meta WHERE k = ?", "nope")).toBeUndefined();
    db.close?.();
  });
});

/** A Cache Storage that keeps Responses in a Map, as a browser would. */
function fakeCaches(opts: { failOpen?: boolean; failPut?: boolean } = {}) {
  const store = new Map<string, Response>();
  return {
    store,
    caches: {
      open: async () => {
        if (opts.failOpen) throw new Error("SecurityError");
        return {
          match: async (url: string) => store.get(url)?.clone(),
          put: async (url: string, r: Response) => { if (opts.failPut) throw new Error("QuotaExceeded"); store.set(url, r); },
        };
      },
    },
  };
}

async function withCaches<T>(c: unknown, fn: () => Promise<T>): Promise<T> {
  const g = globalThis as { caches?: unknown };
  const had = g.caches;
  g.caches = c;
  try { return await fn(); } finally { if (had === undefined) delete g.caches; else g.caches = had; }
}

describe("cache.web: Cache Storage by immutable url, memory when it is not there", () => {
  const b = new Uint8Array([1, 2, 3]);
  it("keeps bytes in Cache Storage", async () => {
    const f = fakeCaches();
    await withCaches(f.caches, async () => {
      const c = await releaseCache();
      expect(c.kind).toBe("cache-storage");
      await c.put("https://r2/r-1/core-abc.sqlite.gz", b);
      expect([...f.store.keys()]).toEqual(["https://r2/r-1/core-abc.sqlite.gz"]);
      expect(await c.get("https://r2/r-1/core-abc.sqlite.gz")).toEqual(b);
      expect(await c.get("https://r2/r-1/other")).toBeNull();
    });
  });
  it("falls back to memory with no Cache Storage, a refused one, or a full one", async () => {
    await withCaches(undefined, async () => {
      const c = await releaseCache();
      expect(c.kind).toBe("memory");
      await c.put("u", b);
      expect(await c.get("u")).toEqual(b);
    });
    await withCaches(fakeCaches({ failOpen: true }).caches, async () => expect((await releaseCache()).kind).toBe("memory"));
    await withCaches(fakeCaches({ failPut: true }).caches, async () => {
      const c = await releaseCache();
      await c.put("u", b);                                      // the quota error is not the reader's problem
      expect(await c.get("u")).toEqual(b);
    });
  });
});

describe("source.web: cache, else fetchVerified into the cache", () => {
  const raw = new TextEncoder().encode("SQLite format 3\u0000 core");
  const m = parseManifest({
    format: "app-manifest/2", data_release: "r-1",
    tiles: { compatible: ["d768d6e135e62b99.*"], current: null, assets: null, mask: null }, basemap: null,
    files: { core: { url: "r-1/core-abc.sqlite.gz", bytes: raw.length, gz: 0, sha256: sha256Hex(raw),
                     format: "core/1", section_handles: "d768d6e135e62b99" } },
    regulations: { valid_until: null, versions: null },
  });
  const server = (log: string[], body: Uint8Array = raw) => (async (url: string) => {
    log.push(url);
    return { ok: true, status: 200, arrayBuffer: async () => body.slice().buffer };
  }) as unknown as typeof fetch;

  it("fetches once (concurrent asks share it), then answers from the cache across page loads", async () => {
    const log: string[] = [];
    const cache = await withCaches(undefined, () => releaseCache());
    const s1 = releaseSource(m, { base: "https://r2/release/", cache, held: null, fetch: server(log) });
    const [a, b] = await Promise.all([s1.bytes("core"), s1.bytes("core")]);
    expect([a, b]).toEqual([raw, raw]);
    expect(log).toEqual(["https://r2/release/r-1/core-abc.sqlite.gz"]);
    const s2 = releaseSource(m, { base: "https://r2/release/", cache, held: null, fetch: server(log) });
    expect(await s2.bytes("core")).toEqual(raw);
    expect(log.length).toBe(1);
  });
  it("a cached copy that is not the manifest's bytes is fetched again; a bad download is refused", async () => {
    const log: string[] = [];
    const cache = await withCaches(undefined, () => releaseCache());
    await cache.put("https://r2/release/r-1/core-abc.sqlite.gz", new TextEncoder().encode("damaged"));
    const s = releaseSource(m, { base: "https://r2/release/", cache, held: null, fetch: server(log) });
    expect(await s.bytes("core")).toEqual(raw);
    expect(log.length).toBe(1);
    const bad = releaseSource(m, { base: "https://r2/x/", cache, held: null,
                                   fetch: server([], new TextEncoder().encode("SQLite format 3\u0000 cor!")) });
    await expect(bad.bytes("core")).rejects.toThrow(/sha256/);
    await expect(bad.bytes("near")).rejects.toThrow("release r-1 has no file near");
  });

  // (T409) the check is the browser's own: WebCrypto's sha256, DecompressionStream for a stored .gz body
  it("a stored .gz body (no Content-Encoding) is ungzipped by DecompressionStream and checked; a damaged one is refused", async () => {
    const cache = await withCaches(undefined, () => releaseCache());
    const s = releaseSource(m, { base: "https://r2/release/", cache, held: null, fetch: server([], await gz(raw)) });
    expect(await s.bytes("core")).toEqual(raw);
    const bad = releaseSource(m, { base: "https://r2/y/", cache, held: null,
                                   fetch: server([], await gz(new TextEncoder().encode("SQLite format 3\u0000 cor!"))) });
    await expect(bad.bytes("core")).rejects.toThrow(/sha256 .*the manifest says/);
    const short = releaseSource(m, { base: "https://r2/z/", cache, held: null, fetch: server([], await gz(raw.subarray(1))) });
    await expect(short.bytes("core")).rejects.toThrow(`${raw.length - 1} bytes, the manifest says ${raw.length}`);
  });
  it("a browser without WebCrypto or DecompressionStream: an error, reported — never the JS hash instead", async () => {
    const g = globalThis as { crypto?: unknown; DecompressionStream?: unknown };
    const cache = await withCaches(undefined, () => releaseCache());
    reported.length = 0;
    const crypto = Object.getOwnPropertyDescriptor(globalThis, "crypto")!;
    Object.defineProperty(globalThis, "crypto", { value: {}, configurable: true });
    try {
      const s = releaseSource(m, { base: "https://r2/a/", cache, held: null, fetch: server([]) });
      await expect(s.bytes("core")).rejects.toThrow("no WebCrypto (crypto.subtle)");
    } finally { Object.defineProperty(globalThis, "crypto", crypto); }
    const ds = g.DecompressionStream;
    delete g.DecompressionStream;
    try {
      const s = releaseSource(m, { base: "https://r2/b/", cache, held: null, fetch: server([], await gz(raw)) });
      await expect(s.bytes("core")).rejects.toThrow("no DecompressionStream");
    } finally { g.DecompressionStream = ds; }
    expect(reported.map((e) => (e as Error).message)).toEqual([
      "release check: this browser has no WebCrypto (crypto.subtle); the release files cannot be checked",
      "release check: this browser has no DecompressionStream; the release files cannot be checked"]);
  });
});
