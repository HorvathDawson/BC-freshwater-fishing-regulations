// T851 (owner 2026-11-18: "a big issue"): at the 14:15Z run every DFO page answered 5xx once; the feed kept the last
// answers but marked EVERY region `stale`, and nothing asked DFO again until the next day. Now:
//   * `stale` is the AGE of the region's last good read (`checked`) past STALE_AFTER_HOURS (the app's own DFO line), not
//     "the last fetch failed"; `failures` (in a row) still says a check failed, and /status still lists it (`dfoHeld`);
//   * fetchRegion rides out a blip inside the run (5, 15, 45 s between four tries), and stops at once on the run's breaker;
//   * a run that leaves a region unread writes private/feeds/dfo/retry.json and the hourly "50 * * * *" cron runs DFO
//     again for those regions until they are read (then the file goes) or DFO_RETRY_MAX tries have failed.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { DFO_RETRY_KEY, DFO_RETRY_MAX, dfoRetryDue, nextDfoRetry, type DfoRetry } from "../src/index";
import { runDfo } from "../src/dfo";
import { fetchRegion } from "../src/dfo/fetch";
import { regionsWithState, STALE_AFTER_HOURS, staleAt } from "../src/dfo/live";
import { SourceError } from "../src/health";
import type { R2BucketLike } from "../src/io";
import { STALE_AFTER } from "../../../packages/core/src/staleness";
import { MemStore, privStore, REGISTRY_TEXT, REPO } from "./dfo.scenario";

const FIX = join(REPO, "app/workers/feeds/test/fixtures/dfo");
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8");
const entries = (slug: string) => readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
const H = 3600_000;
const noSleep = async () => {};
const NOW = "2026-11-18T14:15:00Z";
const ago = (h: number) => new Date(Date.parse(NOW) - h * H).toISOString().slice(0, 19) + "+00:00";

describe("the stale line", () => {
  it("is the app's DFO line, and the Python's number", () => {
    expect(STALE_AFTER_HOURS * H).toBe(STALE_AFTER.dfo.ms);
    const py = readFileSync(join(REPO, "pipeline/regs/dfo_salmon/live.py"), "utf-8");
    expect(Number(/^STALE_AFTER_HOURS = (\d+)$/m.exec(py)?.[1])).toBe(STALE_AFTER_HOURS);
  });
  it("the in-run backoff is the Python's (four tries, 5 × 3^n s)", () => {
    const py = readFileSync(join(REPO, "pipeline/regs/dfo_salmon/fetch.py"), "utf-8");
    expect([/^_MAX_ATTEMPTS = (\S+)$/m, /^_BACKOFF_BASE = (\S+)$/m, /^_BACKOFF_FACTOR = (\S+)$/m].map((r) => Number(r.exec(py)?.[1]))).toEqual([4, 5, 3]);
  });
  it("judges the last good read's age: inside the line fresh (the line itself included), past it, none or unreadable stale", () => {
    expect([staleAt(ago(18), NOW), staleAt(ago(30), NOW), staleAt(ago(30.01), NOW), staleAt(ago(48), NOW)]).toEqual([false, false, true, true]);
    expect([staleAt(null, NOW), staleAt(undefined, NOW), staleAt("not a time", NOW)]).toEqual([true, true, true]);
    expect(staleAt("2026-11-17T20:16:00Z", NOW)).toBe(false); // "Z" and "+00:00" both read
  });
});

describe("a failed fetch in runDfo", () => {
  const prevIndex = (checked: string, failures = 0) =>
    JSON.stringify({ regions: { "4": { checked, failures, asof: "2025-04-01", sha256: "x", stale: false, held: null } } });
  const failRun = async (checked: string, failures = 0) => {
    const feed = new MemStore();
    feed.m.set("index.json", prevIndex(checked, failures));
    const got = await runDfo({ fetch: async () => new Response(""), feed, priv: privStore(), now: NOW, regions: ["4"],
                               fetcher: async () => { throw new Error("region 4: HTTP 503 after 4 attempts"); } });
    // (T905) `failures` is in the run's private state; the last index here is one from before it (the keys on it), so
    // these runs also read through the cutover's seeding
    return { r4: regionsWithState(got.index, got.state)["4"] as { stale: boolean; failures: number; checked: string }, got };
  };

  it("one failed fetch with a fresh last answer (18 h, the 2026-11-18 case) is NOT stale; it is counted and nobody is emailed", async () => {
    const { r4, got } = await failRun(ago(18));
    expect(r4).toMatchObject({ stale: false, failures: 1, checked: ago(18) });
    expect((got.index.issues as { unread: Record<string, string> }).unread).toEqual({ "4": "stale" }); // not read this run: said
    expect(got.notices.filter((n) => n.key.startsWith("dfo-fetch:"))).toEqual([]);
  });

  it("a failure whose last good read is past the line IS stale, and the owner is told once for the outage", async () => {
    const { r4, got } = await failRun(ago(31), 3);
    expect(r4).toMatchObject({ stale: true, failures: 4 });
    const n = got.notices.filter((x) => x.key.startsWith("dfo-fetch:"));
    expect(n.map((x) => x.key)).toEqual([`dfo-fetch:4:${ago(31)}`]);
    expect(n[0]!.text).toMatch(/^DFO Region 4: no good read since .* \(over 30 h\), 4 fetches in a row failed .*marked stale$/);
  });

  it("a region never read is stale at its first failure", async () => {
    const feed = new MemStore();
    const got = await runDfo({ fetch: async () => new Response(""), feed, priv: privStore(), now: NOW, regions: ["4"],
                               fetcher: async () => { throw new Error("down"); } });
    expect((got.index.regions as Record<string, { stale: boolean }>)["4"]!.stale).toBe(true);
  });

  it("a 5xx then a 200 inside the run is a read: not stale, no failure, `checked` moves", async () => {
    const feed = new MemStore();
    feed.m.set("index.json", prevIndex(ago(18), 1));
    const priv = privStore();
    priv.m.set("entries/region-4.json", entries("4"));
    const answers = [new Response("busy", { status: 502 }), new Response(page("4"))];
    const waits: number[] = [];
    const got = await runDfo({ fetch: async () => answers.shift()!, feed, priv, now: NOW, regions: ["4"],
                               fetchOptions: { sleep: async (ms) => void waits.push(ms), random: () => 0 } });
    const r4 = regionsWithState(got.index, got.state)["4"] as { stale: boolean; failures: number; checked: string };
    expect(r4).toMatchObject({ stale: false, failures: 0 });
    expect(r4.checked).not.toBe(ago(18));
    expect(waits).toEqual([5000]);
  });
});

describe("fetchRegion's in-run retry", () => {
  it("waits 5, 15, 45 s between its four tries (about a minute), then gives up", async () => {
    const waits: number[] = [];
    let n = 0;
    await expect(fetchRegion(async () => (n++, new Response("x", { status: 503 })), "4", { sleep: async (ms) => void waits.push(ms), random: () => 0 }))
      .rejects.toThrow("region 4: HTTP 503 after 4 attempts");
    expect([n, waits]).toEqual([4, [5000, 15000, 45000]]);
  });
  it("a network error then a page is a read", async () => {
    const answers: (() => Response)[] = [() => { throw new TypeError("fetch failed"); }, () => new Response(page("4"))];
    const got = await fetchRegion(async () => answers.shift()!(), "4", { sleep: noSleep });
    expect(got.snap.attempts).toBe(2);
  });
  it("the run's open breaker fails the region at once: no waits, one ask", async () => {
    const waits: number[] = [];
    let n = 0;
    await expect(fetchRegion(async () => { n++; throw new SourceError("circuit", "circuit open for www.pac.dfo-mpo.gc.ca", "dfo-region-4"); }, "4",
                             { sleep: async (ms) => void waits.push(ms) })).rejects.toThrow(/circuit open/);
    expect([n, waits]).toEqual([1, []]);
  });
});

describe("the same-day retry state (retry.json)", () => {
  const at = (h: number) => new Date(Date.parse(NOW) + h * H).toISOString();
  it("a regular run that leaves regions unread starts a retry; nothing unread leaves none", () => {
    expect(nextDfoRetry(null, { at: NOW, retry: false, failed: ["6", "1"], asked: null })).toEqual({ since: NOW, regions: ["1", "6"], tries: 0, last: NOW });
    expect(nextDfoRetry(null, { at: NOW, retry: false, failed: [], asked: null })).toBeNull();
    expect(dfoRetryDue(nextDfoRetry(null, { at: NOW, retry: false, failed: ["1"], asked: null }))).toEqual({ regions: ["1"] });
  });
  it("a run that threw retries what it asked (every region when it asked all)", () => {
    expect(nextDfoRetry(null, { at: NOW, retry: false, failed: null, asked: null })).toMatchObject({ regions: null, tries: 0 });
    expect(dfoRetryDue(nextDfoRetry(null, { at: NOW, retry: false, failed: null, asked: null }))).toEqual({});
    const was: DfoRetry = { since: NOW, regions: ["2"], tries: 3, last: NOW };
    expect(nextDfoRetry(was, { at: at(4), retry: true, failed: null, asked: ["2"] })).toMatchObject({ regions: ["2"], tries: 4 });
  });
  it("each failing retry counts one; after DFO_RETRY_MAX nothing is due until a regular run starts a fresh count", () => {
    let st = nextDfoRetry(null, { at: NOW, retry: false, failed: ["1"], asked: null });
    for (let i = 1; i <= DFO_RETRY_MAX; i++) {
      expect(dfoRetryDue(st)).toEqual({ regions: ["1"] });
      st = nextDfoRetry(st, { at: at(i), retry: true, failed: ["1"], asked: ["1"] });
      expect(st).toMatchObject({ since: NOW, tries: i });
    }
    expect(dfoRetryDue(st)).toBeNull();
    st = nextDfoRetry(st, { at: at(24), retry: false, failed: ["1"], asked: null });
    expect(st).toMatchObject({ since: NOW, tries: 0 });
    expect(dfoRetryDue(st)).toEqual({ regions: ["1"] });
  });
  it("a read ends it", () => {
    const was: DfoRetry = { since: NOW, regions: ["1"], tries: 2, last: NOW };
    expect(nextDfoRetry(was, { at: at(3), retry: true, failed: [], asked: ["1"] })).toBeNull();
    expect(dfoRetryDue(null)).toBeNull();
  });
});

// The cron itself, end to end on an in-memory bucket: it fires only when retry.json says so, and stops after a read.
describe('the "50 * * * *" cron', () => {
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

  class Bucket implements R2BucketLike {
    m = new Map<string, string>();
    async head(k: string) { return this.m.has(k) ? {} : null; }
    async get(k: string) { const v = this.m.get(k); return v === undefined ? null : { text: async () => v, etag: String(v.length) }; }
    async put(k: string, b: string) { this.m.set(k, b); return {}; }
    async delete(k: string) { this.m.delete(k); }
    async list(o: { prefix: string }) { return { objects: [...this.m.keys()].filter((k) => k.startsWith(o.prefix)).sort().map((key) => ({ key })), truncated: false }; }
  }
  const RETRY = `private/feeds/dfo/${DFO_RETRY_KEY}`;
  const cron = async (BUCKET: Bucket) => {
    const waits: Promise<unknown>[] = [];
    await worker.scheduled({ cron: "50 * * * *" }, { BUCKET } as never, { waitUntil: (p) => void waits.push(p) });
    expect(waits).toHaveLength(1);
    await waits[0];
  };

  it("nothing due: no fetch, nothing written", async () => {
    const asked: string[] = [];
    vi.stubGlobal("fetch", async (u: string) => (asked.push(String(u)), new Response("")));
    vi.spyOn(console, "log").mockImplementation(() => {});
    const BUCKET = new Bucket();
    await cron(BUCKET);
    expect([asked, [...BUCKET.m.keys()]]).toEqual([[], []]);
    // past DFO_RETRY_MAX: not due either
    BUCKET.m.set(RETRY, JSON.stringify({ since: NOW, regions: ["4"], tries: DFO_RETRY_MAX, last: NOW }));
    await cron(BUCKET);
    expect(asked).toEqual([]);
  });

  it("fires after a failed run (a failing retry counts one) and stops after a read", async () => {
    const asked: string[] = [];
    vi.stubGlobal("fetch", async (u: string | URL | Request) => {
      const url = String(u instanceof Request ? u.url : u);
      asked.push(url);
      const m = /region(\w+)-eng\.html$/.exec(url);
      return m ? new Response(page(m[1]!)) : new Response("no", { status: 404 });
    });
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    const BUCKET = new Bucket();
    BUCKET.m.set(RETRY, JSON.stringify({ since: NOW, regions: ["4"], tries: 0, last: NOW }));
    // 1. due, but the run cannot start (no wording registry synced): it fails before asking DFO, and counts one try
    await cron(BUCKET);
    expect(JSON.parse(BUCKET.m.get(RETRY)!)).toMatchObject({ since: NOW, regions: ["4"], tries: 1 });
    // 2. inputs in place, DFO answers: region 4 is read, the retry file goes
    BUCKET.m.set("private/feeds/dfo/wording.json", REGISTRY_TEXT);
    BUCKET.m.set("private/feeds/dfo/entries/region-4.json", entries("4"));
    await cron(BUCKET);
    expect(asked.filter((u) => u.includes("dfo-mpo"))).toEqual(["https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/region4-eng.html"]);
    expect(BUCKET.m.has(RETRY)).toBe(false);
    const ix = JSON.parse(BUCKET.m.get("feeds/dfo/index.json")!);
    expect(Object.keys(ix.regions)).toEqual(["4"]); // only the region it was for
    expect(ix.regions["4"]).toMatchObject({ stale: false });
    expect(JSON.parse(BUCKET.m.get("private/feeds/dfo/state.json")!).regions["4"]).toMatchObject({ failures: 0 }); // (T905)
    // 3. nothing due any more: the next hour asks nothing
    const before = asked.length;
    await cron(BUCKET);
    expect(asked.length).toBe(before);
  });
});
