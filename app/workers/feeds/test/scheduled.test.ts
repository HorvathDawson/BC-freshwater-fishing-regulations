// T431: the cron dispatcher `scheduled()` — every cron in wrangler.toml starts the job its minute field names — and the
// weekly run-timing check driven through it: a failed check is told, and a message that could not be sent is LOGGED
// (it used to fall into `catch { /* logged below */ }` with nothing below logging it).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { cronJob } from "../src/index";
import { FEED_HEARTBEAT_MS } from "../src/inseason";
import { INDEX_HEARTBEAT_MS } from "../src/dfo/live";
import { cadenceMin } from "../src/status";
import type { R2BucketLike } from "../src/io";
import { REPO } from "./dfo.scenario";

class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v }; }
  async put(k: string, b: string) { this.m.set(k, b); }
  async delete(k: string) { this.m.delete(k); }
  async list(o: { prefix: string }) {
    return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false };
  }
}

/** the crons wrangler.toml [triggers] schedules, in order */
const CRONS = (() => {
  const toml = readFileSync(join(REPO, "app/workers/feeds/wrangler.toml"), "utf-8");
  const block = /\ncrons = \[([\s\S]*?)\n\]/.exec(toml)![1]!;
  return [...block.matchAll(/^\s*"([^"]+)"/gm)].map((m) => m[1]!);
})();

describe("scheduled(): which job each cron starts", () => {
  it("every cron in wrangler.toml maps to its job, and every scheduled job has a cron", () => {
    const got = Object.fromEntries(CRONS.map((c) => [c, cronJob(c)]));
    expect(got).toEqual({
      "5,35 0-5,13-23 * * *": "gauges",
      "5 6-12 * * *": "gauges",
      "15 */6 * 6-10 *": "dfo",
      "15 14 * 1-5,11-12 *": "dfo",
      "45 */6 * 6-10 *": "inseason",
      "45 14 * 1-5,11-12 *": "inseason",
      "25 9 * * *": "backup",
      "55 10 1 * *": "skill",
      ...Object.fromEntries(CRONS.filter((c) => c.startsWith("40 ")).map((c) => [c, "runtiming"])),
    });
    expect(new Set(Object.values(got))).toEqual(new Set(["gauges", "dfo", "inseason", "backup", "skill", "runtiming"]));
  });
});

// (T486) a feed rewritten only when older than its heartbeat must be rewritten by EVERY daily run: a 24 h heartbeat let a run
// 23.3 h after the last write skip it, and staging's in-season index aged to 48 h (the smoke caught it, 2026-11-07)
it("each feed's heartbeat is under its slowest cron cadence, so a daily run never skips the rewrite", () => {
  const winter = cadenceMin("inseason", new Date("2026-01-15T14:45:00Z")) * 60_000;
  expect(cadenceMin("dfo", new Date("2026-01-15T14:15:00Z")) * 60_000).toBe(winter);
  // the cron fires within a few minutes of its time: the heartbeat leaves at least 2 h of slack under the cadence
  for (const hb of [FEED_HEARTBEAT_MS, INDEX_HEARTBEAT_MS]) expect(hb).toBeLessThanOrEqual(winter - 2 * 3600_000);
});

describe("scheduled() → the weekly run-timing check (T412)", () => {
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

  const runCron = async (hookStatus: number) => {
    const calls: { url: string; body: string }[] = [];
    vi.stubGlobal("fetch", async (u: string | URL | Request, init?: RequestInit) => {
      const url = String(u instanceof Request ? u.url : u);
      calls.push({ url, body: String(init?.body ?? "") });
      if (url.includes("open.canada.ca")) return new Response("down", { status: 503 });
      return new Response(hookStatus === 200 ? "ok" : "no", { status: hookStatus });
    });
    const BUCKET = new MemBucket();
    const waits: Promise<unknown>[] = [];
    const cron = CRONS.find((c) => cronJob(c) === "runtiming")!;
    await worker.scheduled({ cron }, { BUCKET, FEED_NOTIFY_URL: "https://hook.example/feeds" } as never, { waitUntil: (p) => void waits.push(p) });
    expect(waits).toHaveLength(1);
    return { result: (await waits[0]) as { ok: boolean; error?: string; told: { failed: number } }, calls, BUCKET };
  };

  it("a failed check is told (the owner's webhook gets 'run-timing source check failed')", async () => {
    const { result, calls } = await runCron(200);
    expect(result.ok).toBe(false);
    expect(result.told).toEqual({ failed: 0 });
    const hook = calls.filter((c) => c.url.startsWith("https://hook.example/"));
    expect(hook).toHaveLength(1);
    expect(hook[0]!.body).toContain("run-timing source check failed");
  });

  it("a message that could not be sent is logged with the job's name and kept to send again", async () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    const { result, BUCKET } = await runCron(500);
    expect(result.told.failed).toBe(1);
    expect(err.mock.calls.map((c) => String(c[0]))).toContain(JSON.stringify({ job: "runtiming", notifyFailed: 1 }));
    const state = JSON.parse(BUCKET.m.get("private/feeds/notify/state.json")!);
    expect(state.pending.map((e: { key: string }) => e.key)).toEqual([expect.stringMatching(/^runtiming-failed:/)]);
  });
});
