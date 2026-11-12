/**
 * (T315) The gauges' last week, kept on the phone: a station file read online, and the publisher's feed-wide newest
 * rows, are what the phone answers with offline — a measured line on the chart, "as of" its age, never an empty chart.
 * Mutations: `fallback` answering `good` alone fails "a new launch, offline"; dropping `noteObs` fails "never opened".
 */
import { describe, expect, it } from "vitest";
import { GAUGE_FEED_FORMAT, setFailureSink } from "@app/core";
import { FILES, KEEP_MS, gaugeHistory, withDays } from "./history";
import { httpFeed, type StationFile } from "./http";
import { seriesOf } from "../sql/shared";
import type { StationId } from "../index";

const NOW = Date.parse("2026-10-30T21:00:00Z");
const mem = () => { let t: string | null = null; return { read: () => t, write: (x: string) => { t = x; }, get text() { return t; } }; };
/** A store read at once that already holds `text`. */
const mem2 = (text: string) => { let t: string | null = text; return { read: () => t, write: (x: string) => { t = x; } }; };
const rowsFrom = (fromIso: string, n: number, q0 = 20) =>
  Array.from({ length: n }, (_, i) => [new Date(Date.parse(fromIso) + i * 1_800_000).toISOString().replace(".000Z", "Z"),
                                       1 + i / 100, q0 + i] as [string, number, number]);
const FILE = (fetchedAt: string, recent: [string, number | null, number | null][]): StationFile => ({
  fetchedAt, station: "08MH001",
  now: { discharge: recent.at(-1)![2], level: recent.at(-1)![1], at: recent.at(-1)![0], percentile: 0.4, parameter: "discharge" },
  recent, daily: [["2026-10-27", 1, 19]], forecasts: { ELF: { model: "ELF" } as never }, priorYears: null,
});
const server = (files: Record<string, unknown>) =>
  (async (url: string) => {
    const key = url.split("/").pop()!;
    // (T557) every file the publisher writes names its format first
    return key in files ? { ok: true, json: async () => ({ format: GAUGE_FEED_FORMAT, ...(files[key] as object) }) }
      : { ok: false, status: 404, json: async () => ({}) };
  }) as unknown as typeof fetch;
const dead = (async () => { throw new TypeError("Network request failed"); }) as unknown as typeof fetch;

describe("(T315) gaugeHistory", () => {
  it("a station file's rows and the feed-wide rows merge into one week, newest wins, older than KEEP_MS dropped", () => {
    const h = gaugeHistory(null, { now: () => NOW });
    h.noteFile("08MH001", FILE("2026-10-30T12:00:00Z", rowsFrom("2026-10-28T12:00:00Z", 4)));
    h.noteObs({ fetchedAt: "2026-10-30T20:30:00Z", stations: {
      "08MH001": [["2026-10-30T12:30:00-08:00", 2, 40], ["2026-10-28T12:30:00Z", 9, 99]],   // a new row; a corrected one
      "08NM050": rowsFrom(new Date(NOW - KEEP_MS - 3 * 3_600_000).toISOString(), 4),             // all too old
    } });
    const f = h.file("08MH001")!;
    expect(f.recent!.map((r) => r[2])).toEqual([20, 99, 22, 23, 40]);
    expect(f.recent!.at(-1)![0]).toBe("2026-10-30T20:30:00Z");             // in the zone the station's rows came in
    expect([f.kept, f.fetchedAt, f.now.discharge, f.now.percentile, !!f.forecasts]).toEqual(
      [true, "2026-10-30T20:30:00.000Z", 40, null, true]);
    expect(h.file("08NM050")).toBeNull();
  });

  it("a station never opened: a file made of the feed-wide rows alone, with the whole days as daily means", () => {
    const h = gaugeHistory(null, { now: () => NOW });
    h.noteObs({ fetchedAt: "2026-10-30T20:30:00Z", stations: { "08NM050": rowsFrom("2026-10-28T00:00:00Z", 48 * 2 + 10) } });
    const f = h.file("08NM050")!;
    expect([f.recent!.length, f.forecasts, f.daily!.map((d) => d[0])]).toEqual([106, null, ["2026-10-28", "2026-10-29"]]);
  });

  it("kept across a launch (the store), and only the most recently viewed FILES whole files", () => {
    const store = mem();
    const h = gaugeHistory(store, { now: () => NOW });
    for (let i = 0; i <= FILES; i++) h.noteFile(`S${i}`, { ...FILE("2026-10-30T12:00:00Z", rowsFrom("2026-10-30T10:00:00Z", 2)), station: `S${i}` });
    h.flush();
    const again = gaugeHistory(store, { now: () => NOW });
    expect([!!again.file("S0")?.forecasts, !!again.file(`S${FILES}`)?.forecasts, again.file("S0")!.recent!.length])
      .toEqual([false, true, 2]);                                       // S0's file aged out; its rows are kept
    expect(store.text!.length).toBeLessThan(40_000);
  });

  it("withDays: never a mean of the day the rows are still in", () => {
    expect(withDays([["2026-10-28", 1, 2]], [["2026-10-28T23:00:00Z", 1, 1], ["2026-10-29T01:00:00Z", 1, 4],
                                            ["2026-10-29T02:00:00Z", 3, 6], ["2026-10-30T01:00:00Z", 1, 1]]))
      .toEqual([["2026-10-28", 1, 2], ["2026-10-29", 2, 5]]);
  });
});

describe("(T315) httpFeed with the kept week", () => {
  const S = "08MH001" as StationId;
  it("online it keeps what it reads; a new launch, offline, answers from the phone — chart drawn, marked kept", async () => {
    const store = mem();
    const h1 = gaugeHistory(store, { now: () => NOW });
    const online = httpFeed("http://x/feeds/live", server({ "08MH001.json": FILE("2026-10-30T20:00:00Z",
      rowsFrom("2026-10-28T20:00:00Z", 96)) }), { history: h1 });
    const live = await online.observations(S);
    expect(live!.kept).toBeUndefined();
    h1.flush();
    // a new launch: nothing in the session, and no network
    let calls = 0;
    const off = httpFeed("http://x/feeds/live", (async () => { calls++; throw new Error("x"); }) as unknown as typeof fetch,
                         { history: gaugeHistory(store, { now: () => NOW }), online: () => false });
    const obs = await off.observations(S);
    expect([obs!.kept, obs!.discharge.length, calls]).toEqual([true, 96, 0]);
    const s = seriesOf(Array.from({ length: 73 }, () => null), obs, "72h", "discharge");
    expect([s!.kept, s!.value.values.filter((v) => v !== null).length > 0]).toEqual([true, true]);
    expect((await off.now(S))!.kept).toBe(true);
    // and the index (the panels' standings): kept from the online read, answered offline with its own fetchedAt
    expect(await off.index()).toBeNull();                                // never read online in this test
    h1.noteIndex({ fetchedAt: "2026-10-30T20:00:00Z", stations: { "08MH001": { percentile: 0.4 } } });
    h1.flush();
    const off2 = httpFeed("http://x/feeds/live", dead, { history: gaugeHistory(store, { now: () => NOW }), online: () => false });
    expect((await off2.index())?.fetchedAt).toBe("2026-10-30T20:00:00Z");
  });

  it("a dead line (said online) falls back to the kept week too; a 404 stays 'not transmitting'", async () => {
    const h = gaugeHistory(null, { now: () => NOW });
    h.noteFile("08MH001", FILE("2026-10-30T20:00:00Z", rowsFrom("2026-10-30T10:00:00Z", 4)));
    const f = httpFeed("http://x/feeds/live", dead, { history: h });
    expect((await f.observations(S))!.kept).toBe(true);
    const missing = httpFeed("http://x/feeds/live", server({}), { history: h });
    expect(await missing.observations("08ZZ999" as StationId)).toBeNull();
  });

  it("syncObservations reads obs.json into every station's week; absent (an older publisher): nothing", async () => {
    const h = gaugeHistory(null, { now: () => NOW });
    const f = httpFeed("http://x/feeds/live", server({ "obs.json": { fetchedAt: "2026-10-30T20:30:00Z",
      stations: { "08NM050": rowsFrom("2026-10-30T18:00:00Z", 4) } } }), { history: h });
    await f.syncObservations!();
    expect(h.file("08NM050")!.recent!.length).toBe(4);
    await httpFeed("http://x/feeds/live", server({}), { history: h }).syncObservations!();
    expect(h.file("08NM050")!.recent!.length).toBe(4);
  });
});

// (T585 F14) Offline, the kept "now" carries the kept index's rank when it is the reading that index ranked, and no
// standing (with `rankedAt`, the reading the rank belongs to) when a newer kept row is the reading now.
describe("(T585 F14) the kept reading's rank", () => {
  const at = "2026-10-30T20:30:00Z";
  const kept = () => {
    const h = gaugeHistory(null, { now: () => NOW });
    const rows = rowsFrom("2026-10-30T19:00:00Z", 4);       // last row 20:30
    h.noteFile("08MH001", FILE("2026-10-30T20:35:00Z", rows));
    return h;
  };
  it("is the kept index's rank for the reading it ranked", async () => {
    const h = kept();
    h.noteIndex({ format: "gauge-feed/1", fetchedAt: "2026-10-30T20:35:00Z",
                  stations: { "08MH001": { percentile: 0.62, observedAt: at } } });
    const r = (await httpFeed("http://x/g", dead, { history: h, online: () => false }).now("08MH001" as StationId))!;
    expect([r.kept, r.value.percentile, r.value.rankedAt]).toEqual([true, 0.62, undefined]);
  });
  it("is none, naming the ranked reading, when a newer kept row is the reading now", async () => {
    const h = kept();
    h.noteIndex({ format: "gauge-feed/1", fetchedAt: "2026-10-30T20:05:00Z",
                  stations: { "08MH001": { percentile: 0.62, observedAt: "2026-10-30T20:00:00Z" } } });
    const r = (await httpFeed("http://x/g", dead, { history: h, online: () => false }).now("08MH001" as StationId))!;
    expect([r.value.percentile, r.value.rankedAt, r.value.at]).toEqual([null, "2026-10-30T20:00:00Z", at]);
  });
});

/**
 * (T633 item 6, T585 F14 the web half) THE WEEK KEPT WHERE IT IS READ LATER (the browser's IndexedDB): the same week as
 * a phone's file. Mutations: answering before the kept week is in fails "opened offline"; a write before it is in fails
 * "writes nothing until"; the kept rows going over the session's fails "the session's read wins".
 */
describe("(T633 item 6) a store that is read later (the web)", () => {
  /** Text kept behind a promise: `load` answers when `release` is called (a slow IndexedDB), or at once. */
  const slow = (kept: string | null, o: { held?: boolean; fail?: Error } = {}) => {
    let text = kept, release!: () => void;
    const gate = o.held ? new Promise<void>((r) => { release = r; }) : Promise.resolve();
    const writes: string[] = [];
    return { store: { read: () => null, write: (t: string) => { text = t; writes.push(t); },
                      load: async () => { await gate; if (o.fail) throw o.fail; return text; } },
             writes, release: () => release(), get text() { return text; } };
  };
  /** What an earlier visit kept: 08MH001's file and 2.5 h of rows. */
  const earlier = () => {
    const m = mem();
    const h = gaugeHistory(m, { now: () => NOW });
    h.noteFile("08MH001", FILE("2026-10-30T12:00:00Z", rowsFrom("2026-10-30T09:30:00Z", 5)));
    h.noteIndex({ fetchedAt: "2026-10-30T12:00:00Z", stations: { "08MH001": { percentile: 0.4 } } });
    h.flush();
    return m.text!;
  };

  it("a page opened offline answers from the week an earlier visit kept — once it is in, never 'nothing kept' before", async () => {
    const s = slow(earlier(), { held: true });
    const feed = httpFeed("https://x/feeds/live", dead, { history: gaugeHistory(s.store, { now: () => NOW }), online: () => false });
    const asked = feed.observations("08MH001" as StationId);
    let answered = false;
    void asked.then(() => { answered = true; });
    await new Promise((r) => setTimeout(r, 5));
    expect(answered).toBe(false);                       // still waiting for the kept week, not answered "nothing"
    s.release();
    const got = await asked;
    expect([got?.kept, got?.discharge.length, (await feed.index())?.fetchedAt]).toEqual([true, 5, "2026-10-30T12:00:00Z"]);
  });

  it("writes nothing until the kept week is in; then one week holds both, and the session's read wins a shared minute", async () => {
    const s = slow(earlier(), { held: true });
    const h = gaugeHistory(s.store, { now: () => NOW });
    // this session reads the station again before IndexedDB has answered: the last kept row again (another figure) + 2 new
    h.noteFile("08MH001", FILE("2026-10-30T13:00:00Z", rowsFrom("2026-10-30T11:30:00Z", 3, 99)));
    h.noteFile("08NM050", { ...FILE("2026-10-30T13:00:00Z", rowsFrom("2026-10-30T12:00:00Z", 2)), station: "08NM050" });
    h.flush();
    expect(s.writes).toEqual([]);
    s.release();
    await h.ready();
    expect(s.writes.length).toBe(1);
    const again = gaugeHistory(mem2(s.text!), { now: () => NOW });
    const f = again.file("08MH001")!;
    expect({ rows: f.recent!.map((r) => r[2]), fetchedAt: f.fetchedAt, other: again.file("08NM050")!.recent!.length,
             index: again.index() })
      .toEqual({ rows: [20, 21, 22, 23, 99, 100, 101], fetchedAt: "2026-10-30T13:00:00.000Z", other: 2,
                 index: { fetchedAt: "2026-10-30T12:00:00Z", stations: { "08MH001": { percentile: 0.4 } } } });
  });

  it("a week that cannot be read is reported; the session keeps its own and writes it", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    const s = slow(null, { fail: new Error("IndexedDB refused the request") });
    const h = gaugeHistory(s.store, { now: () => NOW });
    h.noteFile("08MH001", FILE("2026-10-30T13:00:00Z", rowsFrom("2026-10-30T12:00:00Z", 2)));
    await h.ready();
    h.flush();
    undo();
    expect([seen.map((e) => (e as Error).message), s.writes.length, h.file("08MH001")!.recent!.length])
      .toEqual([["IndexedDB refused the request"], 1, 2]);
  });

  it("a store read at once (a phone's file) is ready at once, as before", async () => {
    const h = gaugeHistory(mem2(earlier()), { now: () => NOW });
    expect(h.file("08MH001")!.recent!.length).toBe(5);
    await h.ready();
  });
});
