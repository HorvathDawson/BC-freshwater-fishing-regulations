/** T51: the inputs to "Like your visit?" — whole values, LOCAL timestamps. */
import { describe, expect, it } from "vitest";
import { likeDays } from "@app/core";
import { comingConditions, fetchLike, flowAhead, NOTHING_FETCHED, readLike, visitConditions, visitHour, writeLike,
         type LikeFetched, type WeatherAhead } from "./likeVisit";
import { openMeteo } from "./weather";
import type { Spot } from "./model";

const VISIT = new Date(2026, 7, 30, 9, 20).getTime();
const NOW = new Date(2026, 9, 22, 10).getTime();
const base = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: VISIT, updatedAt: VISIT, visitedAt: VISIT, lat: 49.1, lon: -121.96, item: null,
  waterName: null, title: "", notes: "", photos: [], reading: null, weather: null, trace: null, panel: null, ...over,
});
const week = (precip: (number | null)[]) => precip.map((p, i) => ({
  date: `2026-08-${23 + i}`, maxC: 20, minC: 9, precipMm: p, windMaxKph: 10, code: 1 }));

describe("visitConditions — the frozen record only", () => {
  it("the station's own discharge row in the frozen panel; air, wind and cloud at the visit's hour; three days' rain", () => {
    const spot = base({
      reading: { station: "08MH016", discharge: 41.2, level: 1.4, percentile: 0.9, at: "x" },
      panel: { answer: { ok: false, why: "no-record" }, areaKm2: 100, routesReady: false,
               rows: [{ station: "08MH001", percentile: 0.1 }, { station: "08MH016", percentile: 0.42 }] as never },
      weather: { tempC: 15, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3, at: "x",
                 window: { before: null, after: null,
                           at: { at: "2026-08-30T09:00", tempC: 14, cloudPct: 40, pressureHpa: 1014, humidityPct: 70 } } },
      leadUp: { weather: week([9, 9, 9, 9, 0.5, 1, 0.5]), station: "08MH016", gauge: null },
    });
    expect(visitConditions(spot)).toEqual({ flow: 0.42, tempC: 14, rainMm: 2, windKph: 8, cloudPct: 40 });
    expect(visitHour(spot)).toBe("09:00");
  });

  it("never a level: a bare reading's percentile is a flow only when the reading shows a discharge (T106)", () => {
    const r = { station: "08MH016", discharge: 41.2, percentile: 0.6, at: "x" };
    const flow = (reading: object) => visitConditions(base({ reading: { ...r, ...reading } as never })).flow;
    expect([
      flow({ level: 1.4, parameter: "level" }),          // both, declared level (08ME030's shape): the level's rank
      flow({ level: 1.4, parameter: "discharge" }),      // both, declared discharge
      flow({ level: null }),                             // undeclared, discharge only (a spot saved before T106)
      flow({ level: 1.4 }),                              // undeclared, both: the publisher's default, discharge
      flow({ discharge: null, level: 1.4 }),             // undeclared, level only: it ranked the level
    ]).toEqual([null, 0.6, 0.6, 0.6, null]);
  });

  it("a rain window with a day not known is no rain, and no weather is nulls — never a guess", () => {
    expect(visitConditions(base({ leadUp: { weather: week([0, 0, 0, 0, 0, null, 1]), station: null, gauge: null } })))
      .toEqual({ flow: null, tempC: null, rainMm: null, windKph: null, cloudPct: null });
  });
});

describe("flowAhead — the feed index's own percentiles, by day", () => {
  it("now and each horizon on the index's day; discharge only; +2 and +4 not there", () => {
    const idx = { fetchedAt: new Date(2026, 9, 22, 6).toISOString(), stations: {
      "08MH016": { percentile: 0.3, parameter: "level" as const, discharge: 0.5, level: 0.3,
                   ahead: { "1": { discharge: 0.55, level: 0.9 }, "3": { discharge: 0.6 }, "5": { level: 0.1 } } } } };
    expect(flowAhead(idx, "08MH016", NOW)).toEqual({
      "2026-10-22": 0.5, "2026-10-23": 0.55, "2026-10-25": 0.6, "2026-10-27": null, "2026-11-01": null });
    expect(flowAhead(idx, "08XX000", NOW)).toBeNull();
  });
});

describe("comingConditions", () => {
  const ahead: WeatherAhead = {
    days: ["2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23", "2026-10-24", "2026-10-25",
           "2026-10-26"].map((date, i) => ({ date, precipMm: i === 6 ? null : i })),
    hours: [{ at: "2026-10-22T09:00", tempC: 7, windKph: 12, cloudPct: 90 },
            { at: "2026-10-22T10:00", tempC: 9, windKph: 30, cloudPct: 10 }],
  };
  it("rain over the three days before each day; air, wind, cloud at the visit's hour; flow by day", () => {
    const f: LikeFetched = { weather: ahead, weatherAt: NOW, flow: { "2026-10-22": 0.5, "2026-10-23": 0.55 }, flowAt: NOW };
    expect(comingConditions(f, likeDays(NOW), "09:00")).toEqual([
      { date: "2026-10-22", flow: 0.5, tempC: 7, rainMm: 0 + 1 + 2, windKph: 12, cloudPct: 90 },
      { date: "2026-10-23", flow: 0.55, tempC: null, rainMm: 1 + 2 + 3, windKph: null, cloudPct: null },
      { date: "2026-10-24", flow: null, tempC: null, rainMm: 2 + 3 + 4, windKph: null, cloudPct: null },
      { date: "2026-10-25", flow: null, tempC: null, rainMm: 3 + 4 + 5, windKph: null, cloudPct: null },
      // the 25th's rain is not known: the 26th's window has a hole
      { date: "2026-10-26", flow: null, tempC: null, rainMm: null, windKph: null, cloudPct: null },
    ]);
  });
});

describe("fetchLike — both halves over the last fetched", () => {
  const spot = base({ reading: { station: "08MH016", discharge: 1, level: null, percentile: 0.4, at: "x" } });
  const had: LikeFetched = { weather: { days: [], hours: [] }, weatherAt: 1, flow: { "2026-10-21": 0.3 }, flowAt: 2,
                            flowNow: "2026-10-21" };

  it("online: both replaced, stamped now", async () => {
    const w: WeatherAhead = { days: [{ date: "2026-10-22", precipMm: 0 }], hours: [] };
    const got = await fetchLike(spot, had, {
      weather: { ahead: async () => w },
      feed: { index: async () => ({ stations: { "08MH016": { percentile: 0.4, discharge: 0.4 } } }) },
    }, NOW);
    expect(got).toEqual({ offline: false, fetched: { weather: w, weatherAt: NOW, flowAt: NOW, flowNow: "2026-10-22",
      flow: { "2026-10-22": 0.4, "2026-10-23": null, "2026-10-25": null, "2026-10-27": null, "2026-11-01": null } } });
  });

  it("the day the reading NOW is for is the index's own day: a feed published yesterday reads now as yesterday's",
     async () => {
    const got = await fetchLike(spot, NOTHING_FETCHED, { feed: { index: async () => ({
      fetchedAt: new Date(2026, 9, 21, 18).toISOString(),
      stations: { "08MH016": { percentile: 0.4, discharge: 0.4, ahead: { "1": { discharge: 0.45 } } } } }) } }, NOW);
    expect([got.fetched.flowNow, got.fetched.flow]).toEqual(["2026-10-21",
      { "2026-10-21": 0.4, "2026-10-22": 0.45, "2026-10-24": null, "2026-10-26": null, "2026-10-31": null }]);
  });

  it("offline: the last fetched kept with its own age, and said offline", async () => {
    const got = await fetchLike(spot, had, { weather: { ahead: async () => null },
                                             feed: { index: async () => { throw new Error("offline"); } } }, NOW);
    expect(got).toEqual({ offline: true, fetched: had });
  });
});

describe("the last fetched, kept", () => {
  it("round-trips by spot id; a bad or missing cache reads as nothing fetched", () => {
    let text: string | null = null;
    const store = { read: () => text, write: (t: string) => { text = t; } };
    const f: LikeFetched = { ...NOTHING_FETCHED, flow: { "2026-10-22": 0.5 }, flowAt: NOW };
    writeLike(store, "s1", f);
    expect([readLike(store, "s1"), readLike(store, "s2")]).toEqual([f, NOTHING_FETCHED]);
    text = "{not json";
    expect(readLike(store, "s1")).toEqual(NOTHING_FETCHED);
  });
});

describe("openMeteo().ahead", () => {
  it("asks once for three past days and five ahead, local clock, and keeps a missing value null", async () => {
    const urls: string[] = [];
    const fake = (async (url: string) => {
      urls.push(url);
      return { ok: true, json: async () => ({
        daily: { time: ["2026-10-19", "2026-10-20"], precipitation_sum: [1.2, null] },
        hourly: { time: ["2026-10-22T09:00"], temperature_2m: [7.5], wind_speed_10m: ["x"], cloud_cover: [90] } }) };
    }) as unknown as typeof fetch;
    expect(await openMeteo(fake).ahead!(49.1, -121.96)).toEqual({
      days: [{ date: "2026-10-19", precipMm: 1.2 }, { date: "2026-10-20", precipMm: null }],
      hours: [{ at: "2026-10-22T09:00", tempC: 7.5, windKph: null, cloudPct: 90 }] });
    expect(urls).toEqual(["https://api.open-meteo.com/v1/forecast?latitude=49.1&longitude=-121.96"
      + "&daily=precipitation_sum&hourly=temperature_2m,wind_speed_10m,cloud_cover&timezone=auto&past_days=3"
      + "&forecast_days=5"]);
  });

  it("offline is null", async () => {
    const fail = (async () => { throw new Error("offline"); }) as unknown as typeof fetch;
    expect(await openMeteo(fail).ahead!(49.1, -121.96)).toBeNull();
  });
});
