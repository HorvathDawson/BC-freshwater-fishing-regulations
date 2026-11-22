// T558 (W3): a stored state that cannot be READ is never taken for "nothing stored yet" — read as empty and written back,
// it erased the outbox (told keys + queued digest), every feed's status, the DFO places of a failed region, the carried
// gauge stations, the HYDAT dispatch record and the no-data streaks. Absent is a fresh state; a throw or junk fails the run.
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import worker, { noDataStations } from "../src/index";
import { readStored, type PutOptions, type Store } from "../src/io";
import { sendDue, sendDueChecked } from "../src/notify";
import { dirStore, fixtureFetch, loadRecording } from "../parity/gauges/node";
import { deliver, NOTIFY_STATE, readState, type NotifyConfig } from "../src/events";
import { readStatus } from "../src/status";
import { dispatchOnce } from "../src/dispatch";
import { RunHealth } from "../src/health";
import { runDfo } from "../src/dfo";
import { accumulateTemperatures, runGauges, stationFileOf } from "../src/gauges/publish";
import { runInSeason } from "../src/inseason";
import { INSEASON_LINKS_FORMAT } from "../../../packages/core/src/inseason";
import { MemStore, privStore } from "./dfo.scenario";

/** a store whose reads fail (R2 having a bad minute) and that records every write */
class Broken implements Store {
  puts: string[] = [];
  constructor(private why = "R2 GET 500") {}
  async get(): Promise<string | null> { throw new Error(this.why); }
  async put(k: string) { this.puts.push(k); }
}
const CFG: NotifyConfig = { ctx: { env: "staging", statusUrl: null }, webhook: "https://hook" };

describe("a stored state that cannot be read (T558 W3)", () => {
  it("readStored: absent is null; a failed read throws; a body that does not parse throws naming the key", async () => {
    const m = new MemStore();
    expect(await readStored(m, "x.json")).toBeNull();
    await expect(readStored(new Broken(), "x.json")).rejects.toThrow(/R2 GET 500/);
    m.m.set("x.json", "{not json");
    await expect(readStored(m, "x.json")).rejects.toThrow(/x\.json is stored but does not parse/);
  });

  it("the outbox: absent is fresh; unreadable or the wrong shape throws, and deliver writes nothing back", async () => {
    expect(await readState(new MemStore())).toEqual({ told: {}, pending: [], lastDigest: null });
    const b = new Broken();
    await expect(deliver([{ key: "k", kind: "hydat", title: "t", text: "x", at: "2026-11-10T00:00:00Z" }],
      { store: b, config: CFG, fetch: async () => new Response("ok"), now: new Date() })).rejects.toThrow(/R2 GET 500/);
    expect(b.puts).toEqual([]);
    const m = new MemStore(); m.m.set(NOTIFY_STATE, JSON.stringify({ told: {} }));
    await expect(readState(m)).rejects.toThrow(/not an outbox/);
  });

  it("the status: absent is fresh; unreadable or another format throws", async () => {
    expect((await readStatus(new MemStore())).feeds).toEqual({});
    await expect(readStatus(new Broken())).rejects.toThrow(/R2 GET 500/);
    const m = new MemStore(); m.m.set("status.json", JSON.stringify({ format: "other" }));
    await expect(readStatus(m)).rejects.toThrow(/not feeds-status\/1/);
  });

  it("GET /status and /status.json answer 503 naming why, never 'never run' with a 200", async () => {
    const env = { BUCKET: { get: async () => { throw new Error("R2 GET 500"); }, put: async () => {}, delete: async () => {} } };
    for (const p of ["/status", "/status.json"]) {
      const r = await worker.fetch(new Request(`https://w${p}`), env as never);
      expect(r.status).toBe(503);
      expect(await r.text()).toMatch(/could not be read: R2 GET 500/);
    }
  });

  it("the HYDAT dispatch record: a failed read starts nothing", async () => {
    let asked = 0;
    await expect(dispatchOnce({ GITHUB_DISPATCH_TOKEN: "t" }, async () => (asked++, new Response(null, { status: 204 })),
      new Broken(), "feeds-hydat.yml", "2026-10", {}, new Date())).rejects.toThrow(/R2 GET 500/);
    expect(asked).toBe(0);
  });

  it("the no-data streaks: a failed read fails, nothing written", async () => {
    const b = new Broken();
    await expect(noDataStations(b, ["08MH006"], 10, new RunHealth(), "2026-11-10T00:00:00Z")).rejects.toThrow(/R2 GET 500/);
    expect(b.puts).toEqual([]);
  });

  it("DFO: the last feed unreadable fails the run before anything is published", async () => {
    const feed = new MemStore();
    const reads = { get: async (k: string) => { if (k === "index.json") throw new Error("R2 GET 500"); return feed.get(k); }, put: feed.put.bind(feed) };
    const puts: string[] = [];
    const watched = { ...reads, put: async (k: string, b: string) => { puts.push(k); await feed.put(k, b); } };
    await expect(runDfo({ fetch: async () => new Response("ok"), notifyUrl: null, feed: watched, priv: privStore(), now: "2026-11-10T12:00:00Z",
      regions: ["8"], entries: async () => "[]", fetcher: async () => ["<html></html>", null] })).rejects.toThrow(/R2 GET 500/);
    expect(puts).toEqual([]);
  });

  // T549 item 8 (the T357 port; T558 built the read): the two files the run carries from — a body that does not parse
  // is not "no last feed" either (every carried place, the failure count and each unplaced row's first_seen would reset)
  for (const k of ["index.json", "unplaced.json"])
    it(`DFO: a stored ${k} that does not parse fails the run naming it, nothing published`, async () => {
      const feed = new MemStore();
      feed.m.set(k, '{"regions": {"8": ');                                   // a torn write
      const puts: string[] = [];
      const watched = { get: feed.get.bind(feed), put: async (key: string, b: string) => { puts.push(key); await feed.put(key, b); } };
      await expect(runDfo({ fetch: async () => new Response("ok"), notifyUrl: null, feed: watched, priv: privStore(), now: "2026-11-10T12:00:00Z",
        regions: ["8"], entries: async () => "[]", fetcher: async () => ["<html></html>", null] }))
        .rejects.toThrow(new RegExp(`${k.replace(".", "\\.")} is stored but does not parse`));
      expect(puts).toEqual([]);
    });

  // T994 #4: the published in-season feed was read in a bare try/catch: an R2 blip or a torn body read as a first run
  // (every changedAt restamped, the removed history dropped, the missing-links guard skipped) and was written back ok
  it("in-season: the published feed unreadable or torn fails the run, nothing written; another format is a first feed", async () => {
    const page = readFileSync(join(__dirname, "fixtures/inseason/page-2026-10-28.html"), "utf-8");
    const links = JSON.stringify({ format: INSEASON_LINKS_FORMAT, release: "r-1", notices: {} });
    const now = new Date("2026-11-10T12:00:00Z");
    const ok = async () => new Response("ok");
    for (const [why, get] of [
      ["R2 GET 500", async (k: string) => { if (k === "index.json") throw new Error("R2 GET 500"); return k === "matches.json" ? links : null; }],
      ["index.json is stored but does not parse", async (k: string) => (k === "index.json" ? '{"format": "inseason-feed/1", "rows": [' : k === "matches.json" ? links : null)],
    ] as const) {
      const puts: string[] = [];
      await expect(runInSeason({ fetch: ok, priv: new MemStore(), feed: { get, put: async (k: string) => { puts.push(k); } }, html: page, now }))
        .rejects.toThrow(why);
      expect(puts, why).toEqual([]);
    }
    const feed = new MemStore();
    feed.m.set("index.json", JSON.stringify({ format: "inseason-feed/0", rows: [] }));
    feed.m.set("matches.json", links);
    expect((await runInSeason({ fetch: ok, priv: new MemStore(), feed, html: page, now })).feed).toMatchObject({ rows: 32 });
  });

  it("gauges: the last index unreadable fails the run (stations are never dropped instead of carried)", async () => {
    const FIX = join(__dirname, "fixtures/gauges");
    const feed = { get: async (k: string) => { if (k === "index.json") throw new Error("R2 GET 500"); return k === "clim.json" ? "{}" : null; }, put: async () => {} };
    await expect(runGauges({ fetch: async () => new Response("", { status: 404 }), feed, priv: new MemStore(), now: new Date("2026-11-10T12:00:00Z"),
      roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")), sleep: async () => {}, limit: 2 })).rejects.toThrow(/R2 GET 500/);
  });
});

// T609 W5: a missing clim.json published every gauge unranked and recorded the run ok
it("gauges: no clim.json fails the run before anything is written (T609 W5)", async () => {
  const FIX = join(__dirname, "fixtures/gauges");
  const puts: string[] = [];
  const feed = { get: async () => null, put: async (k: string) => { puts.push(k); } };
  await expect(runGauges({ fetch: async () => new Response("", { status: 404 }), feed, priv: new MemStore(), now: new Date("2026-11-10T12:00:00Z"),
    roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")), sleep: async () => {}, limit: 2 })).rejects.toThrow(/clim\.json is missing/);
  expect(puts).toEqual([]);
});

// T994 #5 / #26: the gauge state the run writes back, and the owner-reminder dedupe, read in a bare catch: one failed read
// or torn body wiped the water-temperature history (the only one there is), a station's year-to-date daily record, or the
// sent keys (every reminder again). ONE shared fixture with the Python twin (AGENTS 40): pipeline/tests/test_feed_state_reads.py.
describe("stored gauge and reminder state, the shared fixture (T994 #5, #26)", () => {
  const F = JSON.parse(readFileSync(join(__dirname, "fixtures/state-reads.json"), "utf-8"));
  type Case = { name: string; stored: string | null; result?: unknown; error?: string };
  it("the temperature history: absent starts one, kept is appended, anything that does not read throws", () => {
    const temps = new Map(Object.entries(F.tempHistory.temps as Record<string, number>).map(([s, c]) => [s, ["t", c] as [string, number]]));
    for (const c of F.tempHistory.cases as Case[]) {
      if (c.error) expect(() => accumulateTemperatures(c.stored, temps, F.tempHistory.pent), c.name).toThrow(c.error);
      else expect(accumulateTemperatures(c.stored, temps, F.tempHistory.pent), c.name).toEqual(c.result);
    }
  });
  it("a station's last file: absent is none, kept is read, anything that does not read throws", () => {
    for (const c of F.stationFile.cases as Case[]) {
      if (c.error) expect(() => stationFileOf(F.stationFile.station, c.stored), c.name).toThrow(c.error);
      else expect(stationFileOf(F.stationFile.station, c.stored), c.name).toEqual(c.result);
    }
  });
  it("the reminder dedupe: a state that does not read sends nothing, every due key failed, and says why", async () => {
    for (const c of F.notified.cases as (Case & { sent: string[]; failed: string[]; recorded?: string[] })[]) {
      const state = new MemStore();
      if (c.stored !== null) state.m.set("notified.json", c.stored);
      const posts: string[] = [];
      const r = await sendDue(F.notified.notices, { url: "https://ntfy.sh/x", fetch: async (_u, i) => (posts.push(String(i?.body)), new Response("ok")), state, stateKey: "notified.json" });
      expect({ sent: r.sent, failed: r.failed }, c.name).toEqual({ sent: c.sent, failed: c.failed });
      expect(posts, c.name).toHaveLength(c.sent.length);
      if (c.error) {
        expect(r.error, c.name).toContain(c.error);
        expect(state.m.get("notified.json"), c.name).toBe(c.stored);          // nothing written over it
      } else {
        expect(r.error, c.name).toBeUndefined();
        expect(JSON.parse(state.m.get("notified.json")!).sent, c.name).toEqual(c.recorded);
      }
    }
    // a read that fails (R2) is the same: nothing sent
    const posts: string[] = [];
    const r = await sendDue(F.notified.notices, { url: "u", fetch: async () => (posts.push("x"), new Response("ok")), state: new Broken(), stateKey: "notified.json" });
    expect(r).toMatchObject({ sent: [], error: expect.stringContaining("R2 GET 500") });
    expect(posts).toEqual([]);
  });

  // (review) an `error` nobody reads stops the reminders for good with one log line: every feed calls the checked form,
  // which fails the run (after its feed is written) naming the file
  it("the checked form throws on a state that does not read; every feed calls it, never the bare sendDue", async () => {
    for (const c of F.notified.cases as (Case & { sent: string[] })[]) {
      const state = new MemStore();
      if (c.stored !== null) state.m.set("notified.json", c.stored);
      const go = sendDueChecked(F.notified.notices, { url: "u", fetch: async () => new Response("ok"), state, stateKey: "notified.json" });
      if (c.error) await expect(go, c.name).rejects.toThrow(new RegExp(`${c.error.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}.*The feed was written`));
      else expect((await go).sent, c.name).toEqual(c.sent);
    }
    for (const f of ["gauges/publish.ts", "dfo/live.ts"]) {
      const src = readFileSync(join(__dirname, "../src", f), "utf-8");
      expect(src, f).toMatch(/await sendDueChecked\(/);
      expect(src, f).not.toMatch(/\bsendDue\(/);
    }
  });
  it("gauges: a torn notified.json fails the run AFTER the feed is written, naming the file", async () => {
    const FIX = join(__dirname, "fixtures/gauges");
    const rec = loadRecording(FIX);
    const dir = mkdtempSync(join(tmpdir(), "state-reads-"));
    cpSync(join(FIX, "seed"), dir, { recursive: true });
    writeFileSync(join(dir, "priv", "notified.json"), '{"sent": [');
    const feed = dirStore(join(dir, "feed"));
    const before = readFileSync(join(dir, "feed", "index.json"), "utf-8");
    await expect(runGauges({ fetch: fixtureFetch(rec).fetch, feed, priv: dirStore(join(dir, "priv")), now: new Date(rec.now), notifyUrl: "https://ntfy.sh/x",
      roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")), sleep: async () => {}, changeAware: false }))
      .rejects.toThrow(/notified\.json is stored but does not parse.*The feed was written/);
    expect(readFileSync(join(dir, "feed", "index.json"), "utf-8")).not.toBe(before);
    expect(readFileSync(join(dir, "priv", "notified.json"), "utf-8")).toBe('{"sent": [');
  });

  it("gauges: a torn temperature history or station file fails the run before the index or the history is written", async () => {
    const FIX = join(__dirname, "fixtures/gauges");
    const rec = loadRecording(FIX);
    for (const [where, key, why] of [
      ["priv", "temp_history.json", "temp_history.json is stored but does not parse"],
      ["feed", "07EA004.json", "07EA004.json is stored but does not parse"],
    ] as const) {
      const dir = mkdtempSync(join(tmpdir(), "state-reads-"));
      cpSync(join(FIX, "seed"), dir, { recursive: true });
      writeFileSync(join(dir, where, key), '{"torn": [');
      const feed = dirStore(join(dir, "feed")), priv = dirStore(join(dir, "priv"));
      const puts: string[] = [];
      const watch = <S extends Store>(s: S, p: string): S => ({ ...s, put: async (k: string, b: string, o?: PutOptions) => { puts.push(p + k); await s.put(k, b, o); } });
      await expect(runGauges({ fetch: fixtureFetch(rec).fetch, feed: watch(feed, "feed/"), priv: watch(priv, "priv/"), now: new Date(rec.now),
        roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")), sleep: async () => {}, changeAware: false })).rejects.toThrow(why);
      expect(puts, key).not.toContain("feed/index.json");
      expect(puts, key).not.toContain("priv/temp_history.json");
      expect(readFileSync(join(dir, where, key), "utf-8"), key).toBe('{"torn": [');
    }
  });
});
