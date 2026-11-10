// T558 (W3): a stored state that cannot be READ is never taken for "nothing stored yet" — read as empty and written back,
// it erased the outbox (told keys + queued digest), every feed's status, the DFO places of a failed region, the carried
// gauge stations, the HYDAT dispatch record and the no-data streaks. Absent is a fresh state; a throw or junk fails the run.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import worker, { noDataStations } from "../src/index";
import { readStored, type Store } from "../src/io";
import { deliver, NOTIFY_STATE, readState, type NotifyConfig } from "../src/events";
import { readStatus } from "../src/status";
import { dispatchOnce } from "../src/dispatch";
import { RunHealth } from "../src/health";
import { runDfo } from "../src/dfo";
import { runGauges } from "../src/gauges/publish";
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

  it("gauges: the last index unreadable fails the run (stations are never dropped instead of carried)", async () => {
    const FIX = join(__dirname, "fixtures/gauges");
    const feed = { get: async (k: string) => { if (k === "index.json") throw new Error("R2 GET 500"); return null; }, put: async () => {} };
    await expect(runGauges({ fetch: async () => new Response("", { status: 404 }), feed, priv: new MemStore(), now: new Date("2026-11-10T12:00:00Z"),
      roster: JSON.parse(readFileSync(join(FIX, "roster.json"), "utf-8")), sleep: async () => {}, limit: 2 })).rejects.toThrow(/R2 GET 500/);
  });
});
