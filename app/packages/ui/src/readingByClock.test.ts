/**
 * T1205 — A GAUGE READING IS CURRENT BY THE CLOCK, not by the time its feed says it was written. A feed that stopped, a
 * held or kept index on a phone with no network for a day: every reading in it carries a young-looking `observedAt`
 * beside its own old `fetchedAt`, so it used to draw as a live dot and speak as a ranked standing.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { donorReads, GAUGE_FEED_FORMAT, READING_BY_CLOCK_MS } from "@app/core";
import { answerFrom, httpFeed, type Panel, type StationId } from "@app/data";
import { gaugeGeoJSON } from "./gaugePoints";
import { quietHead } from "./panel/conditions";

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
  it("is 8 h", () => expect(READING_BY_CLOCK_MS).toBe(8 * H));
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
