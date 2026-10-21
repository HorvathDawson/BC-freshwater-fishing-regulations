/**
 * P11: THE WEB ADAPTERS, run for real in node — sql.js is the same wasm the browser runs, and Cache Storage is the
 * global the browser has (faked here, and absent, and failing).
 */
import { describe, expect, it } from "vitest";
import initSqlJs from "sql.js";
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
  const server = (log: string[], body = raw) => (async (url: string) => {
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
});
