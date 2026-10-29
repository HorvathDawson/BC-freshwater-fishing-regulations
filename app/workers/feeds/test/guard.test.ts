// T249: rate limits on every route, the run token compared in constant time, and the backup route behind it.
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import { bearerOk, limited, routeClass, sameSecret } from "../src/guard";

/** a limiter that allows `n` calls per key, then refuses */
const limiter = (n: number) => {
  const seen = new Map<string, number>();
  const keys: string[] = [];
  return {
    keys,
    async limit({ key }: { key: string }) {
      keys.push(key);
      const c = (seen.get(key) ?? 0) + 1;
      seen.set(key, c);
      return { success: c <= n };
    },
  };
};
const bucket = () => {
  const m = new Map<string, string>();
  return { m, async get(k: string) { return m.has(k) ? { text: async () => m.get(k)! } : null; }, async put(k: string, b: string) { m.set(k, b); }, async delete(k: string) { m.delete(k); } };
};
const req = (path: string, init: RequestInit & { ip?: string } = {}) =>
  new Request(`https://w.example${path}`, { ...init, headers: { "CF-Connecting-IP": init.ip ?? "203.0.113.7", ...(init.headers as any) } });

describe("rate limits (T249)", () => {
  it("classifies the token routes as auth, everything else public", () => {
    expect(routeClass("/run/gauges")).toBe("auth");
    expect(routeClass("/run/backup")).toBe("auth");
    expect(routeClass("/notify/test")).toBe("auth");
    expect(routeClass("/reports")).toBe("auth");
    expect(routeClass("/status.json")).toBe("public");
    expect(routeClass("/report")).toBe("public");
    expect(routeClass("/anything")).toBe("public");
  });

  it("a flood of /status.json gets 429 with Retry-After once the IP is over its limit; another IP is not", async () => {
    const env = { BUCKET: bucket(), RUN_TOKEN: "x".repeat(48), RL_PUBLIC: limiter(3), RL_AUTH: limiter(1) } as any;
    const codes: number[] = [];
    for (let i = 0; i < 5; i++) codes.push((await worker.fetch(req("/status.json"), env)).status);
    expect(codes).toEqual([200, 200, 200, 429, 429]);
    const r = await worker.fetch(req("/status.json"), env);
    expect(r.headers.get("Retry-After")).toBe("60");
    expect((await worker.fetch(req("/status.json", { ip: "198.51.100.1" }), env)).status).toBe(200);
    expect(env.RL_PUBLIC.keys[0]).toBe("public:203.0.113.7");
  });

  it("the token routes are limited BEFORE the token is checked (guessing is capped)", async () => {
    const env = { BUCKET: bucket(), RUN_TOKEN: "right-token-right-token", RL_PUBLIC: limiter(100), RL_AUTH: limiter(2) } as any;
    const bad = { method: "POST", headers: { Authorization: "Bearer wrong" } };
    expect((await worker.fetch(req("/run/dfo", bad), env)).status).toBe(403);
    expect((await worker.fetch(req("/notify/preview", bad), env)).status).toBe(403);
    expect((await worker.fetch(req("/run/dfo", bad), env)).status).toBe(429);
    expect((await worker.fetch(req("/reports", { headers: { Authorization: "Bearer right-token-right-token" } }), env)).status).toBe(429);
    expect(env.RL_AUTH.keys.every((k: string) => k.startsWith("auth:"))).toBe(true);
  });

  it("no binding, or a binding that throws, lets the request through (never takes the feeds down)", async () => {
    expect(await limited(undefined, req("/x"), "public")).toBeNull();
    expect(await limited({ limit: async () => { throw new Error("down"); } }, req("/x"), "public")).toBeNull();
    const env = { BUCKET: bucket() } as any;
    expect((await worker.fetch(req("/status.json"), env)).status).toBe(200);
  });
});

describe("the run token (T249)", () => {
  it("compares whole strings, any length", () => {
    expect(sameSecret("abc", "abc")).toBe(true);
    expect(sameSecret("abc", "abd")).toBe(false);
    expect(sameSecret("abc", "abcd")).toBe(false);
    expect(sameSecret("", "")).toBe(true);
  });
  it("needs `Bearer <token>` exactly, and refuses everything while no token is set", () => {
    const r = (h?: string) => new Request("https://w/", { headers: h ? { Authorization: h } : {} });
    expect(bearerOk(r("Bearer tok-1"), "tok-1")).toBe(true);
    expect(bearerOk(r("bearer tok-1"), "tok-1")).toBe(false);
    expect(bearerOk(r("tok-1"), "tok-1")).toBe(false);
    expect(bearerOk(r(), "tok-1")).toBe(false);
    expect(bearerOk(r("Bearer "), "")).toBe(false);
    expect(bearerOk(r("Bearer undefined"), undefined)).toBe(false);
  });
  it("after a rotation the old token is refused and the new one accepted (POST /run/backup)", async () => {
    const b = bucket();
    const env = { BUCKET: b, RUN_TOKEN: "old-token" } as any;
    const call = (t: string) => worker.fetch(req("/run/backup", { method: "POST", headers: { Authorization: `Bearer ${t}` } }), env);
    expect((await call("old-token")).status).toBe(500); // accepted; no BACKUP binding under test → 500 {error}
    env.RUN_TOKEN = "new-token";
    expect((await call("old-token")).status).toBe(403);
    expect((await call("new-token")).status).toBe(500);
  });
});
