// T249: the STAGING data worker (deploy/staging/data-worker.js) never serves private/ — in any spelling — and answers a
// flood with 429. The live twin of these checks: deploy/staging/check-public.mjs (every private key, over HTTP).
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
