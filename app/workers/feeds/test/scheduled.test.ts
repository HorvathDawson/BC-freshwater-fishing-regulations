// T431: the cron dispatcher `scheduled()` — every cron in wrangler.toml starts the job its minute field names — and the
// weekly run-timing check driven through it: a failed check is told, and a message that could not be sent is LOGGED
// (it used to fall into `catch { /* logged below */ }` with nothing below logging it).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { cronJob, dayTurn } from "../src/index";
import { STALE_AFTER } from "../../../packages/core/src/staleness";
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
      "35 0 * * *": "gauges",
      "5,35 1-5,13-23 * * *": "gauges",
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

// (T549 items 7, 10; was T486's heartbeat test) DFO and in-season write their index on EVERY run, so its stamp is the
// check time and the oldest it gets is one cadence plus the run. The app's stale line must sit OVER the slowest cadence
// with margin: at exactly 24 h it flickered "likely out of date" every day at the daily write.
it("the app's stale line for DFO and in-season leaves at least 4 h over the slowest cron cadence", () => {
  const winter = cadenceMin("inseason", new Date("2026-01-15T14:45:00Z")) * 60_000;
  expect(cadenceMin("dfo", new Date("2026-01-15T14:15:00Z")) * 60_000).toBe(winter);
  expect(winter).toBe(24 * 3600_000);
  for (const k of ["dfo", "inseason"] as const) expect(STALE_AFTER[k].ms).toBeGreaterThanOrEqual(winter + 4 * 3600_000);
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

// (T550 item 4, T597 item 1) ECCC's /today/ directory answers 404 for every file at its UTC day turn (staging 2026-11-10
// and 2026-11-11 at 00:05): the gauge cron never asks at 00:05, and a whole-run 404 just after midnight is the turn.
describe("the gauge schedule and ECCC's day turn", () => {
  it("no gauge cron fires at 00:05 UTC; 00:35 still does", () => {
    const fires = (c: string, h: number, m: number) => {
      const [min, hr] = c.split(" ");
      const inField = (f: string, v: number) => f === "*" || f.split(",").some((p) => {
        const [a, b] = p.split("-").map(Number);
        return b === undefined ? a === v : a! <= v && v <= b;
      });
      return inField(min!, m) && inField(hr!, h);
    };
    const gauge = CRONS.filter((c) => cronJob(c) === "gauges");
    expect(gauge.some((c) => fires(c, 0, 5))).toBe(false);
    expect(gauge.some((c) => fires(c, 0, 35))).toBe(true);
    expect(gauge.some((c) => fires(c, 1, 5))).toBe(true);
  });

  it("a whole-run 404 within DAY_TURN_MIN of 00:00 UTC is the turn; anything else is a failure", () => {
    const all404 = () => "http_404";
    const at = (iso: string) => new Date(iso);
    expect(dayTurn(at("2026-11-11T00:05:23Z"), 0, ["A", "B"], all404, "http_404")).toBe(true);
    expect(dayTurn(at("2026-11-11T00:35:00Z"), 0, ["A", "B"], all404, "http_404")).toBe(false);
    expect(dayTurn(at("2026-11-11T13:05:00Z"), 0, ["A", "B"], all404, "http_404")).toBe(false);
    expect(dayTurn(at("2026-11-11T00:05:00Z"), 1, ["A"], all404, "http_404")).toBe(false);
    expect(dayTurn(at("2026-11-11T00:05:00Z"), 0, ["A", "B"], (s) => (s === "A" ? "http_5xx" : "http_404"), "http_404")).toBe(false);
    expect(dayTurn(at("2026-11-11T00:05:00Z"), 0, ["A", "B"], all404, undefined)).toBe(false);
  });
});
