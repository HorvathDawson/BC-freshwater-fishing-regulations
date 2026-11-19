// T249: the STAGING data worker (deploy/staging/data-worker.js; production's runs the same file, deploy/prod/data.wrangler.toml)
// never serves private/ — in any spelling — and answers a flood with 429. The live twin of these checks:
// deploy/staging/check-public.mjs (every private key, over HTTP). T912: a gzip-stored file to a client that takes no gzip.
import { describe, expect, it } from "vitest";
// @ts-expect-error — a plain-JS worker module
import worker from "../../deploy/staging/data-worker.js";

const obj = (body: string) => ({
  body, size: body.length, httpEtag: '"e1"',
  writeHttpMetadata(h: Headers) { h.set("Content-Type", "application/json"); },
});
const bucket = (keys: Record<string, string>) => ({
  async get(k: string) { return k in keys ? obj(keys[k]!) : null; },
  async head(k: string) { return k in keys ? obj(keys[k]!) : null; },
});
const KEYS = { "private/feeds/status.json": '{"secret":1}', "feeds/live/index.json": '{"ok":1}', "/private/feeds/status.json": "x", "PRIVATE/a": "x" };
const get = (path: string, env: any, ip = "203.0.113.9") =>
  worker.fetch(new Request(`https://data-staging.example${path}`, { headers: { "CF-Connecting-IP": ip } }), env) as Promise<Response>;

describe("staging data worker (T249)", () => {
  it("serves public keys and 404s every spelling of private/", async () => {
    const env = { BUCKET: bucket(KEYS) };
    expect((await get("/feeds/live/index.json", env)).status).toBe(200);
    for (const p of ["/private/feeds/status.json", "/private%2Ffeeds%2Fstatus.json", "//private/feeds/status.json",
                     "/./private/feeds/status.json", "/PRIVATE/a", "/private", "/private/", "/feeds/%2e%2e/private/feeds/status.json"])
      expect((await get(p, env)).status, p).toBe(404);
  });
  it("(T436) a bad escape is a 404 with CORS, not a bare 500", async () => {
    const r = await get("/feeds/%zz.json", { BUCKET: bucket(KEYS) });
    expect(r.status).toBe(404);
    expect(r.headers.get("Access-Control-Allow-Origin")).toBe("*");
  });
  it("a flood from one IP gets 429 + Retry-After; other IPs are served; no binding = no limit", async () => {
    const seen = new Map<string, number>();
    const RL = { async limit({ key }: { key: string }) { const n = (seen.get(key) ?? 0) + 1; seen.set(key, n); return { success: n <= 3 }; } };
    const env = { BUCKET: bucket(KEYS), RL };
    const codes = [];
    for (let i = 0; i < 5; i++) codes.push((await get("/feeds/live/index.json", env)).status);
    expect(codes).toEqual([200, 200, 200, 429, 429]);
    const r = await get("/feeds/live/index.json", env);
    expect(r.headers.get("Retry-After")).toBe("60");
    expect(r.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect((await get("/feeds/live/index.json", env, "198.51.100.2")).status).toBe(200);
    expect((await get("/feeds/live/index.json", { BUCKET: bucket(KEYS), RL: { limit: async () => { throw new Error("x"); } } })).status).toBe(200);
  });
});

// (T912) a gzip-stored release file as R2 holds it: the stored bytes, their own Content-Encoding, R2's range read
const STORED = Uint8Array.from({ length: 100 }, (_, i) => (i * 7 + 0x1f) & 0xff);
const gzObj = (range?: { offset: number; length?: number }) => {
  const off = range?.offset ?? 0, len = range?.length ?? STORED.length - off;
  return {
    body: STORED.slice(off, off + len), size: STORED.length, httpEtag: '"gz1"',
    ...(range ? { range: { offset: off, length: len } } : {}),
    writeHttpMetadata(h: Headers) { h.set("Content-Type", "application/x-sqlite3"); h.set("Content-Encoding", "gzip"); h.set("Cache-Control", "public, max-age=31536000, immutable"); },
  };
};
const gzBucket = {
  async get(k: string, o?: { range?: { offset: number; length?: number } }) { return k === "release/climate-ab.sqlite.gz" ? gzObj(o?.range) : k in KEYS ? obj((KEYS as any)[k]) : null; },
  async head(k: string) { return k === "release/climate-ab.sqlite.gz" ? gzObj() : null; },
};
// on the edge the worker sees `Accept-Encoding: br, gzip` on every request; the client's own value is request.cf.clientAcceptEncoding
const ask = (opts: { cf?: string; header?: string; range?: string; method?: string; path?: string }) => {
  const h: Record<string, string> = { "CF-Connecting-IP": "203.0.113.9" };
  if (opts.header !== undefined) h["Accept-Encoding"] = opts.header;
  if (opts.range) h.Range = opts.range;
  const req = new Request(`https://data-staging.example/${opts.path ?? "release/climate-ab.sqlite.gz"}`, { method: opts.method ?? "GET", headers: h });
  if (opts.cf !== undefined) Object.defineProperty(req, "cf", { value: { clientAcceptEncoding: opts.cf } });
  return worker.fetch(req, { BUCKET: gzBucket }) as Promise<Response>;
};
const bytes = async (r: Response) => new Uint8Array(await r.arrayBuffer());

describe("staging data worker: a gzip-stored file (T912)", () => {
  it("a client that takes gzip (browser, the app's fetch, iOS): passed through encoded, as before", async () => {
    for (const o of [{ cf: "gzip, deflate, br, zstd", header: "br, gzip" }, { cf: "gzip", header: "br, gzip" }, { header: "gzip" }, { cf: "*", header: "br, gzip" }]) {
      const r = await ask(o);
      expect(r.status, JSON.stringify(o)).toBe(200);
      expect(r.headers.get("Content-Encoding"), JSON.stringify(o)).toBe("gzip");
      expect(r.headers.get("Content-Type")).toBe("application/x-sqlite3");
      expect(r.headers.get("ETag")).toBe('"gz1"');
      expect(r.headers.get("Vary")).toBeNull();
      expect(r.headers.get("Content-Length")).toBeNull();          // the runtime's, as before
      expect(await bytes(r)).toEqual(STORED);
    }
  });
  it("a client that takes no gzip (DownloadManager: identity): the stored bytes, no Content-Encoding, their length + ETag", async () => {
    for (const o of [{ cf: "identity", header: "br, gzip" }, { cf: "", header: "br, gzip" }, { cf: "gzip;q=0, identity", header: "br, gzip" },
                     { cf: "br", header: "br, gzip" }, { header: "identity" }, {}]) {
      const r = await ask(o);
      expect(r.status, JSON.stringify(o)).toBe(200);
      expect(r.headers.get("Content-Encoding"), JSON.stringify(o)).toBeNull();
      expect(r.headers.get("Content-Type")).toBe("application/octet-stream");
      expect(r.headers.get("Content-Length")).toBe(String(STORED.length));
      expect(r.headers.get("ETag")).toBe('"gz1"');
      expect(r.headers.get("Accept-Ranges")).toBe("bytes");
      expect(r.headers.get("Vary")).toBe("Accept-Encoding");
      expect(await bytes(r)).toEqual(STORED);
    }
  });
  it("a Range on that path (a cut system download resuming): 206 with the slice of the stored bytes", async () => {
    let r = await ask({ cf: "identity", header: "br, gzip", range: "bytes=40-" });
    expect(r.status).toBe(206);
    expect(r.headers.get("Content-Encoding")).toBeNull();
    expect(r.headers.get("Content-Range")).toBe("bytes 40-99/100");
    expect(r.headers.get("Content-Length")).toBe("60");
    expect(r.headers.get("ETag")).toBe('"gz1"');
    expect(await bytes(r)).toEqual(STORED.slice(40));
    r = await ask({ cf: "identity", header: "br, gzip", range: "bytes=10-19" });
    expect(r.status).toBe(206);
    expect(r.headers.get("Content-Range")).toBe("bytes 10-19/100");
    expect(r.headers.get("Content-Length")).toBe("10");
    expect(await bytes(r)).toEqual(STORED.slice(10, 20));
  });
  it("HEAD to a client that takes no gzip says the stored length, unencoded; a file not stored gzip is untouched", async () => {
    const h = await ask({ cf: "identity", header: "br, gzip", method: "HEAD" });
    expect(h.headers.get("Content-Encoding")).toBeNull();
    expect(h.headers.get("Content-Length")).toBe("100");
    const j = await ask({ cf: "identity", header: "br, gzip", path: "feeds/live/index.json" });
    expect(j.headers.get("Content-Type")).toBe("application/json");
    expect(j.headers.get("Vary")).toBeNull();
    expect(j.headers.get("Content-Length")).toBeNull();
  });
});
