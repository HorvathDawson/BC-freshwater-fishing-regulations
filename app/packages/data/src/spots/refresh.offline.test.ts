/**
 * (T314) A SPOT SAVED OFFLINE FILLS IN WHEN THE PHONE IS BACK ONLINE — its gauges too, not only its weather.
 *
 * owner 2026-10-31 on 0.3.0: "the refresh for spot data isn't working". A spot saved with no signal on its own day kept
 * its station and no reading, and (the feed's index unreachable) no panel; a refresh filled the weather and the week
 * before and never the gauges — the day's flow is "not over" all day, and nothing read the live reading again.
 * Mutations: dropping `fillLive` from `refreshVisit` fails "filled"; capturing without `pendingPanel` fails the panel.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { captureSpot } from "./capture";
import { needsRefresh, refreshSpot } from "./refresh";
import { addVisit, captureVisit, spotVisits, visitSpot } from "./visits";
import { liveFillable, savedGauges } from "./dayFlow";
import type { WaterSource, SectionId } from "../index";
import type { Spot } from "./model";

const NOON = new Date(2026, 9, 30, 12, 0, 0).getTime();          // local noon: the visit day is today all test long
const MEMBERS = { areaKm2: 40, members: [{ station: "08MH001", role: "up", areaKm2: 60, years: { discharge: 40, level: 40 },
                                            regulated: false, sameRiver: true }] };
const source = (): WaterSource => ({
  info: async () => ({ version: "t", validUntil: null }),
  itemForSection: async () => null, water: async () => null, searchNames: async () => [], searchPlaces: async () => [],
  watersNear: async () => [], locate: async () => ({ on: [], near: [] }), gaugeForSection: async () => null,
  panelsFor: async () => new Map([[1 as SectionId, MEMBERS]]), lakeStationsFor: async () => new Map(),
  gaugeNow: async () => ({ fetchedAt: Date.now(), value: { discharge: 30, level: 2, parameter: "discharge", percentile: 0.4,
                                                            at: new Date(Date.now() - 600_000).toISOString() } }),
  gaugeSeries: async () => ({ fetchedAt: Date.now(),
                              value: { from: "2026-01-01", values: Array(366).fill(20), band: [], priorYears: [] } }),
  gaugeParameters: async () => ["discharge", "level"],
  traceToGauge: async () => [], lakeInfo: async () => null, stockingHistory: async () => [],
} as unknown as WaterSource);
const weather = {
  at: async () => ({ tempC: 10, at: null, backfilled: true } as never),
  days: async (_a: number, _b: number, d: readonly string[]) =>
    d.map((date) => ({ date, maxC: 1, minC: 0, precipMm: 0, windMaxKph: 1, code: 1 })),
};
const feed = { index: async () => ({ stations: { "08MH001": { percentile: 0.4, discharge: 30, parameter: "discharge" } } }) as never };
const gauges = (s: Spot) => [s.reading?.discharge ?? null, s.reading?.backfilled ?? false, !!s.panel,
                                                     s.panelDay?.state ?? null, savedGauges(s)];

let online = false;
const capture = () => captureSpot({ source: source(), weather, feed, at: { lat: 49, lon: -121 }, item: null,
  section: 1 as SectionId, waterName: "X", title: "", history: { online: () => online } });

describe("(T314) offline spot → online → filled", () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(NOON); online = false; });
  afterEach(() => { vi.useRealTimers(); });

  it("saved offline today: the gauges are kept pending, then a refresh online fills reading, panel, weather and week", async () => {
    const s = await capture();
    expect(gauges(s)).toEqual([null, false, false, "pending", "pending"]);
    expect([s.weather, needsRefresh(s), liveFillable(s)]).toEqual([null, true, true]);
    online = true;
    vi.setSystemTime(NOON + 3_600_000);
    const r = await refreshSpot(s, weather, source(), feed, { online: () => online });
    expect(gauges(r)).toEqual([30, true, true, null, "live"]);
    expect([r.weather !== null, r.leadUp?.gauge?.length, r.leadUp?.weather?.length, needsRefresh(r)]).toEqual([true, 7, 7, false]);
  });

  it("still offline: the refresh changes nothing and the spot keeps waiting", async () => {
    const s = await capture();
    const dead = { ...source(), gaugeNow: async () => null, gaugeSeries: async () => null,
                   gaugeParameters: async () => { throw new Error("offline"); } } as unknown as WaterSource;
    const r = await refreshSpot(s, { at: async () => null, days: async () => null }, dead,
                                { index: async () => null }, { online: () => false });
    expect([r === s, needsRefresh(r)]).toEqual([true, true]);
  });

  it("the next day a live reading is never taken as the visit's: the day's record fills it instead", async () => {
    const s = await capture();
    vi.setSystemTime(NOON + 86_400_000);
    expect(liveFillable(s)).toBe(false);
    const r = await refreshSpot(s, weather, source(), feed, { online: () => true });
    expect([r.reading?.backfilled ?? false, r.dayFlow?.state, r.panelDay?.state, savedGauges(r)])
      .toEqual([false, "filled", "day", "day"]);
  });

  it("a visit added offline is filled the same way", async () => {
    online = true;
    const spot = await capture();
    online = false;
    const v = await captureVisit(spot, { source: source(), weather, feed, history: { online: () => online } });
    const two = addVisit(spot, v);
    const id = spotVisits(two)[1]!.id;
    expect(gauges(visitSpot(two, id))).toEqual([null, false, false, "pending", "pending"]);
    online = true;
    const r = await refreshSpot(two, weather, source(), feed, { online: () => online });
    expect(gauges(visitSpot(r, id))).toEqual([30, true, true, null, "live"]);
    expect(needsRefresh(r)).toBe(false);
  });
});
