// T558 (W8, item 5): a step that fails inside a run is printed and kept in the result, never only in a log a cron drops;
// the status is written whatever the outbox does; the in-season snapshot moves on only once its diff is in the outbox.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import type { R2BucketLike } from "../src/io";

class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  failGet = new Set<string>();
  async get(k: string) {
    if (this.failGet.has(k)) throw new Error(`R2 GET ${k} 500`);
    const v = this.m.get(k); return v === undefined ? null : { text: async () => v };
  }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}
const PAGE = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
const runInseason = async (b: MemBucket) =>
  (await worker.fetch(new Request("https://w/run/inseason", { method: "POST", headers: { Authorization: "Bearer t" } }),
    { BUCKET: b, RUN_TOKEN: "t", FEED_NOTIFY_URL: "https://hook.example/x" } as never)).json() as Promise<any>; // eslint-disable-line @typescript-eslint/no-explicit-any

describe("a run's steps (T558 W8)", () => {
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

  it("the outbox unreadable: the status is still written, the step error is printed and returned, the snapshot is held", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    const b = new MemBucket();
    b.failGet.add("private/feeds/notify/state.json");
    const r = await runInseason(b);
    expect(r.stepErrors.join("\n")).toMatch(/notify: the outbox could not take/);
    expect(err.mock.calls.flat().join("\n")).toMatch(/stepError.*outbox could not take/);
    expect(b.m.has("private/feeds/status.json")).toBe(true);
    expect(b.m.has("private/feeds/inseason/latest.json")).toBe(false);       // the diff is computed again next run
    // the outbox back: the snapshot moves on, nothing to say
    b.failGet.clear();
    const again = await runInseason(b);
    expect(again.stepErrors).toEqual([]);
    expect(b.m.has("private/feeds/inseason/latest.json")).toBe(true);
  });

  it("a dry run (?dryNotify) never moves the snapshot", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    const b = new MemBucket();
    await worker.fetch(new Request("https://w/run/inseason?dryNotify=1", { method: "POST", headers: { Authorization: "Bearer t" } }), { BUCKET: b, RUN_TOKEN: "t" } as never);
    expect(b.m.has("private/feeds/inseason/latest.json")).toBe(false);
  });

  it("a cron run that fails prints its error and the tail of its log", async () => {
    vi.stubGlobal("fetch", async () => new Response("down", { status: 503 }));
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const waits: Promise<unknown>[] = [];
    await worker.scheduled({ cron: "45 14 * 1-5,11-12 *" }, { BUCKET: new MemBucket() } as never, { waitUntil: (p) => waits.push(p) });
    await Promise.all(waits);
    expect(err.mock.calls.flat().join("\n")).toMatch(/"job":"inseason","error":".*HTTP 503/);
  });
});
