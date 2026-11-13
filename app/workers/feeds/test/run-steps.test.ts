// T558 (W8, item 5): a step that fails inside a run is printed and kept in the result, never only in a log a cron drops;
// the status is written whatever the outbox does; the in-season snapshot moves on only once its diff is in the outbox.
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
import type { R2BucketLike } from "../src/io";

/** an R2 bucket in memory, with etags and `onlyIf` like R2's (a failed condition answers null) */
class MemBucket implements R2BucketLike {
  m = new Map<string, string>();
  ver = new Map<string, number>();
  failGet = new Set<string>();
  async get(k: string) {
    if (this.failGet.has(k)) throw new Error(`R2 GET ${k} 500`);
    const v = this.m.get(k); return v === undefined ? null : { text: async () => v, etag: `e${this.ver.get(k)}` };
  }
  async put(k: string, b: string, o?: { onlyIf?: { etagMatches: string } }) {
    if (o?.onlyIf && (!this.m.has(k) || `e${this.ver.get(k)}` !== o.onlyIf.etagMatches)) return null;
    this.m.set(k, b); this.ver.set(k, (this.ver.get(k) ?? 0) + 1); return {};
  }
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
    b.m.set("feeds/inseason/matches.json", JSON.stringify({ format: INSEASON_LINKS_FORMAT, notices: {} }));   // (T609 W14)
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

// T558 W13: two jobs that read, change and write status.json / notify/state.json at once no longer lose one's change
describe("state written only over what was read (T558 W13)", () => {
  it("writeMerged: a conflict merges this run's change onto the other writer's; a second conflict throws", async () => {
    const { r2Store, writeMerged } = await import("../src/io");
    const b = new MemBucket();
    const s = r2Store(b, "p/");
    await s.put("x.json", JSON.stringify({ a: 1 }));
    const read = (await s.getVersioned("x.json"))!;
    await s.put("x.json", JSON.stringify({ a: 1, other: true }));                 // another job, meanwhile
    await writeMerged(s, "x.json", { a: 2 }, read.etag, (fresh) => ({ ...(fresh as object), a: 2 }));
    expect(JSON.parse(b.m.get("p/x.json")!)).toEqual({ a: 2, other: true });
    const r2 = (await s.getVersioned("x.json"))!;
    const busy = { ...s, putIfUnchanged: async () => false };                     // someone always first
    await expect(writeMerged(busy, "x.json", { a: 3 }, r2.etag, () => ({ a: 3 }))).rejects.toThrow(/changed by another job twice/);
  });

  it("the outbox: a key told by another job while this run sent keeps both", async () => {
    const { deliver, NOTIFY_STATE, readState } = await import("../src/events");
    const { r2Store } = await import("../src/io");
    const b = new MemBucket();
    const s = r2Store(b, "private/feeds/");
    await s.put(NOTIFY_STATE, JSON.stringify({ told: { "old:1": "2026-11-01T00:00:00Z" }, pending: [], lastDigest: null }));
    let raced = false;
    const racing = { ...s, putIfUnchanged: async (k: string, body: string, etag: string | null, o?: object) => {
      if (!raced) { raced = true; await s.put(NOTIFY_STATE, JSON.stringify({ told: { "old:1": "2026-11-01T00:00:00Z", "other:9": "2026-11-10T00:00:00Z" }, pending: [], lastDigest: null })); }
      return s.putIfUnchanged(k, body, etag, o);
    } };
    await deliver([{ key: "dfo-new:1", kind: "dfo", title: "t", text: "x", at: "2026-11-10T00:00:00Z" }],
      { store: racing, config: { ctx: { env: "staging", statusUrl: null }, webhook: "https://hook" }, fetch: async () => new Response("ok"), now: new Date("2026-11-10T01:00:00Z") });
    expect(Object.keys((await readState(s)).told).sort()).toEqual(["dfo-new:1", "old:1", "other:9"]);
  });

  it("the status: another job's record written meanwhile survives this run's", async () => {
    vi.stubGlobal("fetch", async () => new Response(PAGE));
    const b = new MemBucket();
    await runInseason(b);                                     // status has in-season
    const st = JSON.parse(b.m.get("private/feeds/status.json")!);
    const put = b.put.bind(b);
    let raced = false;
    b.put = async (k: string, body: string, o?: { onlyIf?: { etagMatches: string } }) => {
      if (k === "private/feeds/status.json" && o?.onlyIf && !raced) {
        raced = true;                                          // a DFO run lands its record first
        await put(k, JSON.stringify({ ...st, feeds: { ...st.feeds, dfo: { ...st.feeds.inseason, job: "dfo" } } }));
      }
      return put(k, body, o);
    };
    await runInseason(b);
    const after = JSON.parse(b.m.get("private/feeds/status.json")!);
    expect(raced).toBe(true);
    expect(Object.keys(after.feeds).sort()).toEqual(["dfo", "inseason"]);
  });
});

// T558 item 2: nothing noticed when the daily crons stopped (backup.json said "ok" forever)
describe("the daily jobs are watched (T558 item 2)", () => {
  it("a backup or purge older than 36 h is one urgent message per last run; the status page shows both", async () => {
    const { lateJobEvents, DAILY_JOB_LATE_MS } = await import("../src/index");
    const b = new MemBucket();
    const now = new Date("2026-11-10T12:00:00Z");
    expect(await lateJobEvents(b, now, now.toISOString())).toEqual([]);                 // never run: nothing to say
    b.m.set("private/feeds/backup.json", JSON.stringify({ at: "2026-11-09T09:25:00Z", ok: true, copied: 3, bytes: 9, skipped: [] }));
    b.m.set("private/feeds/report-retention.json", JSON.stringify({ at: new Date(now.getTime() - DAILY_JOB_LATE_MS - 1).toISOString(), ok: true, deleted: 0, kept: 1 }));
    const ev = await lateJobEvents(b, now, now.toISOString());
    expect(ev.map((e) => [e.key.split(":").slice(0, 2).join(":"), e.urgent])).toEqual([["late:report purge", true]]);
    // the page reads the real clock: a backup an hour ago, a purge two days ago
    b.m.set("private/feeds/backup.json", JSON.stringify({ at: new Date(Date.now() - 3600_000).toISOString(), ok: true, copied: 3, bytes: 9, skipped: [] }));
    b.m.set("private/feeds/report-retention.json", JSON.stringify({ at: new Date(Date.now() - 48 * 3600_000).toISOString(), ok: true, deleted: 0, kept: 1 }));
    const h = await (await worker.fetch(new Request("https://w/status"), { BUCKET: b } as never)).text();
    expect(h).toContain("<h2>Daily jobs</h2>");
    expect(h).toMatch(/purge<\/td><td class="stale">late/);
    expect(h).toMatch(/backup<\/td><td class="ok">ok/);
    b.failGet.add("private/feeds/backup.json");
    await expect(lateJobEvents(b, now, now.toISOString())).rejects.toThrow(/R2 GET/);
  });
});
