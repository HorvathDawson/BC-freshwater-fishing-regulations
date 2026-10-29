// T242: POST /crash and GET /crashes — one object per error kind (repeats and other versions fold into it), no IP /
// User-Agent stored, per-IP limits, malformed / oversized / unknown-key payloads refused, the digest tells a new kind once.
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import { compose } from "../src/events";
import { crashEvents, crashHash, handleCrash, purgeCrashes, validateCrash, CRASH_PER_DAY } from "../src/crashes";
import type { R2BucketLike } from "../src/io";

class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v }; }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const crash = (over: Record<string, unknown> = {}) => ({
  format: "crash/1", created: "2026-10-29T20:00:00.000Z", kind: "js", fatal: true, route: "water",
  app: "0.1.0+12", env: "staging", platform: "android 35", release: "r-0fa7927b16e5",
  name: "TypeError", message: "undefined is not an object (evaluating 'x.items')",
  stack: "TypeError: boom\n    at render (address at index.android.bundle:1:123456)\n    at Shell (address at index.android.bundle:1:99)", ...over,
});
const post = (body: unknown, ip = "203.0.113.5") =>
  new Request("https://w.example/crash", { method: "POST", body: typeof body === "string" ? body : JSON.stringify(body),
                                           headers: { "Content-Type": "application/json", "CF-Connecting-IP": ip, "User-Agent": "UA/1" } });
const at = (s: string) => new Date(s);

describe("POST /crash (T242)", () => {
  it("stores one object per kind with no IP or user agent; a repeat and a new version fold into it", async () => {
    const BUCKET = new MemBucket();
    const r = await handleCrash(post(crash()), { BUCKET }, at("2026-10-29T20:00:05Z"));
    expect(r.status).toBe(201);
    expect(await r.json()).toEqual({ ok: true });
    const keys = [...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"));
    expect(keys).toHaveLength(1);
    const stored = JSON.parse(BUCKET.m.get(keys[0]!)!);
    expect(stored).toMatchObject({ format: "crash-kind/1", count: 1, first: "2026-10-29T20:00:05.000Z", versions: { "0.1.0+12": 1 } });
    expect(JSON.stringify(stored)).not.toMatch(/203\.0\.113|UA\/1/);
    // the same error from another build (line/column moved) and another person: the same kind
    const r2 = await handleCrash(post(crash({ app: "0.1.0+13", stack: crash().stack.replace("123456", "654321") }), "198.51.100.9"), { BUCKET }, at("2026-10-30T09:00:00Z"));
    expect(r2.status).toBe(201);
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"))).toEqual(keys);
    expect(JSON.parse(BUCKET.m.get(keys[0]!)!)).toMatchObject({ count: 2, first: "2026-10-29T20:00:05.000Z", last: "2026-10-30T09:00:00.000Z",
                                                                versions: { "0.1.0+12": 1, "0.1.0+13": 1 } });
  });

  it("a different error is another kind", async () => {
    const a = await crashHash(crash());
    expect(await crashHash(crash({ name: "RangeError" }))).not.toBe(a);
    expect(await crashHash(crash({ message: "other thing" }))).not.toBe(a);
  });

  it("refuses a 2nd POST from one IP within the gap and the daily cap, but not another IP", async () => {
    const BUCKET = new MemBucket();
    expect((await handleCrash(post(crash()), { BUCKET }, at("2026-10-29T20:00:00Z"))).status).toBe(201);
    expect((await handleCrash(post(crash()), { BUCKET }, at("2026-10-29T20:00:00.500Z"))).status).toBe(429);
    expect((await handleCrash(post(crash(), "198.51.100.9"), { BUCKET }, at("2026-10-29T20:00:01Z"))).status).toBe(201);
    let t = Date.parse("2026-10-29T21:00:00Z"), last = 201;
    for (let i = 0; i < CRASH_PER_DAY + 1; i++) { last = (await handleCrash(post(crash(), "192.0.2.1"), { BUCKET }, new Date(t))).status; t += 1500; }
    expect(last).toBe(429);
  });

  it("refuses malformed, oversized, unknown-key, wrong-enum payloads and stores nothing", async () => {
    const BUCKET = new MemBucket();
    for (const [body, status] of [
      ["{not json", 400], [crash({ format: "crash/2" }), 400], [crash({ route: "Vedder River" }), 400], [crash({ extra: "x" }), 400],
      [crash({ lat: 49.1 }), 400], [crash({ stack: "x".repeat(2001) }), 400], [crash({ message: "m".repeat(201) }), 400],
      [crash({ fatal: "yes" }), 400], [crash({ name: " " }), 400], ["x".repeat(9000), 413],
    ] as const) {
      expect((await handleCrash(post(body, `10.0.0.${status + String(body).length % 200}`), { BUCKET }, at("2026-10-29T20:00:00Z"))).status).toBe(status);
    }
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"))).toEqual([]);
    expect(validateCrash(crash()).ok).toBe(true);
  });

  it("the worker routes it: OPTIONS 204, GET /crash 405, GET /crashes needs the token", async () => {
    const BUCKET = new MemBucket();
    const env = { BUCKET, RUN_TOKEN: "tok" } as never;
    expect((await worker.fetch(new Request("https://w.example/crash", { method: "OPTIONS" }), env)).status).toBe(204);
    expect((await worker.fetch(new Request("https://w.example/crash"), env)).status).toBe(405);
    expect((await worker.fetch(new Request("https://w.example/crashes"), env)).status).toBe(403);
    await worker.fetch(post(crash()), env);
    const list = await worker.fetch(new Request("https://w.example/crashes?since=2000-01-01", { headers: { authorization: "Bearer tok" } }), env);
    expect(list.status).toBe(200);
    expect(((await list.json()) as { crashes: unknown[] }).crashes).toHaveLength(1);
  });

  it("the digest tells a new kind once, a known one never, and the line has no IP", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash()), { BUCKET }, at("2026-10-29T20:00:00Z"));
    const ev = await crashEvents(BUCKET, () => false, at("2026-10-29T21:00:00Z"), "2026-10-29T21:00:00Z");
    expect(ev).toHaveLength(1);
    expect(ev[0]!.kind).toBe("crash");
    expect(ev[0]!.text).not.toMatch(/203\.0\.113/);
    expect(await crashEvents(BUCKET, (k) => k === ev[0]!.key, at("2026-10-29T21:00:00Z"), "x")).toEqual([]);
    expect(await crashEvents(BUCKET, () => false, at("2026-11-05T21:00:00Z"), "x")).toEqual([]);   // not new any more
    const msgs = compose(ev, true);
    expect(msgs.length).toBeGreaterThan(0);
    expect(msgs[0]!.text).toMatch(/New app errors/);
  });

  it("the purge deletes kinds unseen for 90 days and keeps limits and recent kinds", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash()), { BUCKET }, at("2026-06-01T00:00:00Z"));
    await handleCrash(post(crash({ name: "RangeError" }), "198.51.100.9"), { BUCKET }, at("2026-10-20T00:00:00Z"));
    const r = await purgeCrashes(BUCKET, at("2026-10-29T00:00:00Z"));
    expect(r).toEqual({ ok: true, deleted: 1, failed: 0 });
    expect([...BUCKET.m.keys()].filter((k) => !k.includes("/limits/"))).toHaveLength(1);
  });
});

describe("the test tag (T291)", () => {
  it("is honoured on staging only: stored apart, not in the digest, listed with ?test=1, purged after 7 days", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash({ test: true })), { BUCKET, FEEDS_ENV: "staging" }, at("2026-10-29T20:00:00Z"));
    expect([...BUCKET.m.keys()].filter((k) => k.startsWith("private/crashes-test/"))).toHaveLength(1);
    expect([...BUCKET.m.keys()].filter((k) => /^private\/crashes\/[0-9a-f]{16}/.test(k))).toEqual([]);
    expect(await crashEvents(BUCKET, () => false, at("2026-10-29T21:00:00Z"), "x")).toEqual([]);
    const env = { BUCKET, RUN_TOKEN: "tok", FEEDS_ENV: "staging" } as never;
    const get = async (q: string) => ((await (await worker.fetch(new Request(`https://w.example/crashes?since=2000-01-01${q}`, { headers: { authorization: "Bearer tok" } }), env)).json()) as { crashes: { test?: boolean }[] }).crashes;
    expect(await get("")).toHaveLength(0);
    expect((await get("&test=1"))[0]!.test).toBe(true);
    expect((await purgeCrashes(BUCKET, at("2026-11-10T00:00:00Z"))).deleted).toBe(1);
  });
  it("a production worker (or none) ignores the tag: the kind is an ordinary one and the digest tells it", async () => {
    const BUCKET = new MemBucket();
    await handleCrash(post(crash({ test: true })), { BUCKET, FEEDS_ENV: "production" }, at("2026-10-29T20:00:00Z"));
    expect([...BUCKET.m.keys()].filter((k) => k.includes("crashes-test"))).toEqual([]);
    expect(await crashEvents(BUCKET, () => false, at("2026-10-29T21:00:00Z"), "x")).toHaveLength(1);
  });
});
