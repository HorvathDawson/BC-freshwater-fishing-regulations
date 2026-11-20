// (review 2026-11-20, M2) A wake that throws, or whose record cannot be kept, is never silent: the run writes
// {at, reason, ok: false, error} to the push status (GET /status shows it), and a status that cannot be written at all
// fails the run. Real in-season runs (their first run reads new notices: a wake) over a bucket that fails on cue.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import type { R2BucketLike } from "../src/io";
import { PUSH_STATUS_KEY, TOKENS_PREFIX } from "../src/push";
import { FEED_FORMAT } from "../src/inseason";
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";

class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  ver = new Map<string, number>();
  failPut = new Set<string>();
  failList = new Set<string>();
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v, etag: `e${this.ver.get(k)}` }; }
  async put(k: string, b: string, o?: { onlyIf?: { etagMatches: string } }) {
    if (this.failPut.has(k)) throw new Error(`R2 PUT ${k} 500`);
    if (o?.onlyIf && (!this.m.has(k) || `e${this.ver.get(k)}` !== o.onlyIf.etagMatches)) return null;
    this.m.set(k, b); this.ver.set(k, (this.ver.get(k) ?? 0) + 1); return {};
  }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    if (this.failList.has(o.prefix)) throw new Error(`R2 LIST ${o.prefix} 500`);
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const PAGE = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const runInseason = async (b: MemBucket) => (await worker.fetch(new Request("https://w/run/inseason", { method: "POST", headers: { Authorization: "Bearer t" } }),
  { BUCKET: b, RUN_TOKEN: "t" } as never)).json() as Promise<any>;
const IOS = "a".repeat(64);
/** a bucket whose last in-season feed had no rows: every row of the page is new, so the run wakes */
const seeded = () => {
  const b = new MemBucket();
  b.m.set("feeds/inseason/index.json", JSON.stringify({ format: FEED_FORMAT, rows: [], removed: [] }));
  b.m.set("feeds/inseason/matches.json", JSON.stringify({ format: INSEASON_LINKS_FORMAT, notices: {} }));
  return b;
};

describe("a wake that fails is kept on /status (M2)", () => {
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

  it("the run wakes on its new notices (the case the next two break)", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = seeded();
    const r = await runInseason(b);
    expect(r.push).toMatchObject({ reason: "inseason", ok: true });
    expect(JSON.parse(b.m.get(PUSH_STATUS_KEY)!)).toMatchObject({ reason: "inseason", ok: true });
    expect(r.ok).toBe(true);
  });

  it("a wake that throws (the token list unreadable): {at, reason, ok: false, error} on /status; the run itself stays ok", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = seeded();
    b.failList.add(TOKENS_PREFIX);
    const r = await runInseason(b);
    expect(r.push).toMatchObject({ reason: "inseason", ok: false, error: expect.stringMatching(/R2 LIST private\/push\/tokens\/ 500/) });
    expect(r.stepErrors.join("\n")).toMatch(/push: not sent/);
    const st = JSON.parse(b.m.get(PUSH_STATUS_KEY)!);
    expect(st).toEqual({ at: expect.any(String), reason: "inseason", ok: false, error: expect.stringMatching(/R2 LIST/) });
    expect(r.ok).toBe(true);
    const h = await (await worker.fetch(new Request("https://w/status"), { BUCKET: b } as never)).text();
    expect(h).toContain("not sent: ");
    expect(h).toContain("the last wake was not sent (above)");
  });

  it("a push status that cannot be written fails the run (said in its step errors)", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    vi.spyOn(console, "error").mockImplementation(() => {});
    const b = seeded();
    b.m.set(`${TOKENS_PREFIX}x.json`, JSON.stringify({ format: "push-token/1", platform: "ios", token: IOS, env: "sandbox", at: "t" }));
    b.failPut.add(PUSH_STATUS_KEY);
    const r = await runInseason(b);
    expect(r.ok).toBe(false);
    expect(r.stepErrors.join("\n")).toMatch(/the status was not written/);
    expect(r.stepErrors.join("\n")).toMatch(/the failure was not recorded on \/status either/);
  });

  it("POST /run/push that throws keeps the failure on /status too", async () => {
    const b = new MemBucket();
    b.failList.add(TOKENS_PREFIX);
    const res = await worker.fetch(new Request("https://w/run/push?reason=test", { method: "POST", headers: { Authorization: "Bearer t" } }),
      { BUCKET: b, RUN_TOKEN: "t" } as never);
    expect(res.status).toBe(500);
    expect(JSON.parse(b.m.get(PUSH_STATUS_KEY)!)).toMatchObject({ reason: "test", ok: false, error: expect.stringMatching(/R2 LIST/) });
  });
});
