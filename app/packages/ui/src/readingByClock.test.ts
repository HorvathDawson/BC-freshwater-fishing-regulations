/**
 * T1205 — A GAUGE READING IS CURRENT BY THE CLOCK, not by the time its feed says it was written. A feed that stopped, a
 * held or kept index on a phone with no network for a day: every reading in it carries a young-looking `observedAt`
 * beside its own old `fetchedAt`, so it used to draw as a live dot and speak as a ranked standing.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { donorReads, GAUGE_FEED_FORMAT, readingCurrentNow, readingCurrentUntil } from "@app/core";
import { answerFrom, httpFeed, type Panel, type StationId } from "@app/data";
import { gaugeGeoJSON } from "./gaugePoints";
import { quietHead } from "./panel/conditions";
import { stationReading } from "./hooks";

const H = 3_600_000;
const FED = Date.parse("2026-10-27T18:00:00Z");           // the stale local feed's write time
const iso = (ms: number) => new Date(ms).toISOString();
const feedFile = (fetchedAt: number, observedAt: number) => ({
  format: GAUGE_FEED_FORMAT, fetchedAt: iso(fetchedAt),
  stations: { "08MH001": { percentile: 0.3, discharge: 0.3, parameter: "discharge", reports: ["discharge"], observedAt: iso(observedAt) } },
});
const serve = (file: unknown) => (async () => ({ ok: true, json: async () => file })) as unknown as typeof fetch;
const pts = [{ station: "08MH001", name: "Chilliwack R", lon: -121.9, lat: 49.1 }];
const PANEL: Panel = { areaKm2: 100, members: [{ station: "08MH001" as StationId, role: "up", areaKm2: 100,
  years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] };
const dots = async (feed: ReturnType<typeof httpFeed>) => gaugeGeoJSON(pts, await feed.index());

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { vi.useRealTimers(); });

describe("a feed written 29 days ago, every timestamp self-consistent", () => {
  it("draws no current dot, and the water card says it last reported (never 0, never live)", async () => {
    vi.setSystemTime(FED + 29 * 24 * H);
    const feed = httpFeed("http://x/g", serve(feedFile(FED, FED - 30 * 60_000)));
    expect(await dots(feed)).toBeNull();
    const row = answerFrom(PANEL, await feed.index()).rows[0]!;
    expect(row.percentile).toBeNull();
    const ref = Date.now();
    expect(donorReads(row, { ref, now: ref })).toBe("Not reporting today.");
    expect(quietHead("Chilliwack R", row, ref, 0, "America/Vancouver").lastReport).toMatch(/^Last reported .*2026/);
  });

  it("a fresh feed is unchanged: the dot, its label, its percentile", async () => {
    vi.setSystemTime(FED + 20 * 60_000);
    const feed = httpFeed("http://x/g", serve(feedFile(FED, FED - 30 * 60_000)));
    const g = JSON.parse((await dots(feed))!);
    expect(g.features).toHaveLength(1);
    expect(g.features[0].properties.ramp).toBe(30);
    expect(answerFrom(PANEL, await feed.index()).rows[0]!.percentile).toBe(0.3);
  });
});

describe("the boundary is the publisher's 6 h plus the feed's own 2 h, by the clock", () => {
  it("readingCurrentNow and readingCurrentUntil agree at the line, and unjudgeable stamps are never judged", () => {
    const t = Date.parse("2026-11-26T10:00:00Z");
    expect(readingCurrentNow(iso(t), t + 8 * H)).toBe(true);
    expect(readingCurrentNow(iso(t), t + 8 * H + 1)).toBe(false);
    expect(readingCurrentUntil(iso(t))).toBe(t + 8 * H);
    for (const bad of [null, undefined, "", "2026-11-26T10:00:00", "not a time"]) {
      expect(readingCurrentNow(bad, t + 999 * H)).toBe(true);
      expect(readingCurrentUntil(bad)).toBeNull();
    }
  });
  it("a reading exactly 8 h old is current; a second older is not", async () => {
    vi.setSystemTime(FED + 3 * H);
    const at = async (observed: number) => await dots(httpFeed("http://x/g", serve(feedFile(FED, observed))));
    expect(await at(Date.now() - 8 * H)).not.toBeNull();
    expect(await at(Date.now() - 8 * H - 1000)).toBeNull();
  });
});

describe("a phone offline for a day", () => {
  it("shows the kept reading as old: no dot, no error, no throw", async () => {
    vi.setSystemTime(FED + 10 * 60_000);
    let online = true;
    const feed = httpFeed("http://x/g", serve(feedFile(FED, FED - 30 * 60_000)), { online: () => online });
    expect(await dots(feed)).not.toBeNull();         // read while connected: live
    online = false;
    vi.setSystemTime(FED + 24 * H);
    feed.refresh?.();
    const idx = await feed.index();
    expect(idx).not.toBeNull();                      // the kept copy still answers: not an error
    expect(idx!.stations["08MH001"]!.observedAt).toBe(iso(FED - 30 * 60_000));   // its last report is still said
    expect(gaugeGeoJSON(pts, idx)).toBeNull();
  });
});

describe("the picked day does not change freshness", () => {
  const unranked = { percentile: null, observedAt: iso(FED - 30 * 60_000) };
  it("a day picked back at the reading's own time does not make a month-old reading current", () => {
    const now = FED + 29 * 24 * H;
    expect(donorReads(unranked, { ref: FED, now })).toBe("Not reporting today.");
  });
  it("a day picked ahead does not make a fresh reading old", () => {
    const now = FED + H;
    expect(donorReads(unranked, { ref: now + 3 * 24 * H, now })).toBe("Reporting, but its reading cannot be ranked today.");
  });
});

const stationFile = (fetchedAt: number, at: number) => ({
  format: GAUGE_FEED_FORMAT, fetchedAt: iso(fetchedAt), station: "08MH001",
  now: { discharge: 15.7, level: 1.4, at: iso(at), percentile: 0.3, parameter: "discharge" },
});

describe("feed.now() and the reading readers built on it (lead card, capture, visits, the gauge page)", () => {
  it("a 29-day-old index row: the figure stays (with its age), the rank and standing do not", async () => {
    vi.setSystemTime(FED + 29 * 24 * H);
    const files = { "index.json": feedFile(FED, FED - 30 * 60_000), "08MH001.json": stationFile(FED, FED - 30 * 60_000) };
    const feed = httpFeed("http://x/g", (async (u: string) => ({ ok: true, json: async () => files[u.split("/").pop() as keyof typeof files] })) as never);
    const got = (await feed.now("08MH001" as StationId))!;
    expect(got.value.discharge).toBe(15.7);
    expect(got.value.percentile).toBeNull();
    expect(got.value.standing).toBe("no-record");
    const c = await stationReading({ gaugeNow: (s) => feed.now(s) }, "08MH001" as StationId);
    expect(c.percentile).toBeNull();
    expect(c.observedAt).toBe(iso(FED - 30 * 60_000));
  });
  it("no index at all: the station file's own rank counts only while its reading is current by the clock", async () => {
    const mk = () => httpFeed("http://x/g", (async (u: string) => u.endsWith("index.json") ? { ok: false, status: 404, json: async () => ({}) }
      : { ok: true, json: async () => stationFile(FED, FED - 30 * 60_000) }) as never);
    vi.setSystemTime(FED + 3 * H);
    expect((await mk().now("08MH001" as StationId))!.value.percentile).toBe(0.3);
    vi.setSystemTime(FED + 29 * 24 * H);
    expect((await mk().now("08MH001" as StationId))!.value.percentile).toBeNull();
  });
});

describe("time passes with no new fetch", () => {
  it("the same held index is judged again at each ask: live, then old, the network asked once", async () => {
    vi.setSystemTime(FED + 20 * 60_000);
    let asked = 0;
    const feed = httpFeed("http://x/g", (async () => { asked += 1; return { ok: true, json: async () => feedFile(FED, FED - 30 * 60_000) }; }) as never,
                          { indexHeld: true });
    const first = await feed.index();
    expect(gaugeGeoJSON(pts, first)).not.toBeNull();
    expect(await feed.index()).toBe(first);                      // nothing crossed the line: the same object
    vi.setSystemTime(FED + 9 * H);
    const later = await feed.index();
    expect(gaugeGeoJSON(pts, later)).toBeNull();
    expect(await feed.index()).toBe(later);                      // and it holds, a poll does not copy every station
    expect(asked).toBe(1);
  });
});
