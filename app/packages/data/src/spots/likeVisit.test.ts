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
  it("a feed from before T171 (no aheadDays): now and each horizon on the index's day; discharge only", () => {
    const idx = { fetchedAt: new Date(2026, 9, 22, 6).toISOString(), stations: {
      "08MH016": { percentile: 0.3, parameter: "level" as const, discharge: 0.5, level: 0.3,
                   ahead: { "1": { discharge: 0.55, level: 0.9 }, "3": { discharge: 0.6 }, "5": { level: 0.1 } } } } };
    expect(flowAhead(idx, "08MH016", NOW)).toEqual({ flow: {
      "2026-10-22": 0.5, "2026-10-23": 0.55, "2026-10-25": 0.6, "2026-10-27": null, "2026-11-01": null }, models: {} });
    expect(flowAhead(idx, "08XX000", NOW)).toBeNull();
  });

  /**
   * T171 — THE CHILLIWACK (08MH001) row of the feed republished with `aheadDays` (2026-10-24T02:08Z, this lane's
   * scratch republish): CLEVER every day to 1 Nov, ELF on 2 Nov. +2 and +4 now carry flow.
   */
  const CHILLIWACK = { percentile: 0.05, parameter: "discharge" as const, discharge: 0.05, level: 0.05,
    ahead: { "1": { discharge: 0.05, level: 0.05, model: "CLEVER", levelModel: "ELF" },
             "3": { discharge: 0.05, level: 0.05, model: "CLEVER", levelModel: "ELF" },
             "5": { discharge: 0.05, level: 0.05, model: "CLEVER", levelModel: "ELF" },
             "10": { discharge: 0.05, level: 0.05, model: "ELF" } },
    aheadDays: { from: "2026-10-24", discharge: [0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05],
                 model: ["CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "ELF"],
                 lowWater: [9] } };
  const AT = new Date(2026, 9, 23, 19, 8).getTime();

  it("every forecast day from aheadDays, by its own date, with its model; ELF's day marked low-water", () => {
    expect(flowAhead({ fetchedAt: new Date(AT).toISOString(), stations: { "08MH001": CHILLIWACK } }, "08MH001", AT))
      .toEqual({
        flow: { "2026-10-23": 0.05, "2026-10-24": 0.05, "2026-10-25": 0.05, "2026-10-26": 0.05, "2026-10-27": 0.05,
                "2026-10-28": 0.05, "2026-10-29": 0.05, "2026-10-30": 0.05, "2026-10-31": 0.05, "2026-11-01": 0.05,
                "2026-11-02": 0.05 },
        models: { "2026-10-24": { model: "CLEVER" }, "2026-10-25": { model: "CLEVER" }, "2026-10-26": { model: "CLEVER" },
                  "2026-10-27": { model: "CLEVER" }, "2026-10-28": { model: "CLEVER" }, "2026-10-29": { model: "CLEVER" },
                  "2026-10-30": { model: "CLEVER" }, "2026-10-31": { model: "CLEVER" }, "2026-11-01": { model: "CLEVER" },
                  "2026-11-02": { model: "ELF", lowWater: true } } });
  });

  it("review: when the Centre's tomorrow is the reader's today (just past local midnight), today keeps the reading", () => {
    const row = { percentile: 0.33, discharge: 0.33,
      aheadDays: { from: "2026-10-24", discharge: [0.9, 0.8, null, null, null, null, null, null, null, null],
                   model: ["CLEVER", "CLEVER", null, null, null, null, null, null, null, null] } };
    const at = new Date(2026, 9, 24, 0, 30).getTime();
    const got = flowAhead({ fetchedAt: new Date(at).toISOString(), stations: { X: row } }, "X", at)!;
    expect([got.flow["2026-10-24"], got.flow["2026-10-25"], got.models["2026-10-24"], got.models["2026-10-25"]])
      .toEqual([0.33, 0.8, undefined, { model: "CLEVER" }]);
  });

  it("a day past every model is null and has no model; a varying row keeps each day its own number", () => {
    const row = { percentile: 0.33, discharge: 0.33,
      aheadDays: { from: "2026-10-24", discharge: [0.145, 0.1118, 0.1155, 0.1182, 0.322, null, null, null, null, null],
                   model: ["COFFEE", "COFFEE", "COFFEE", "COFFEE", "ELF", null, null, null, null, null],
                   lowWater: [4] } };
    const got = flowAhead({ fetchedAt: new Date(AT).toISOString(), stations: { "08DB013": row } }, "08DB013", AT)!;
    expect([got.flow["2026-10-24"], got.flow["2026-10-25"], got.flow["2026-10-26"], got.flow["2026-10-27"],
            got.flow["2026-10-28"], got.flow["2026-10-29"]]).toEqual([0.145, 0.1118, 0.1155, 0.1182, 0.322, null]);
    expect([got.models["2026-10-25"], got.models["2026-10-28"], got.models["2026-10-29"]])
      .toEqual([{ model: "COFFEE" }, { model: "ELF", lowWater: true }, undefined]);
  });
});

describe("T171 — the strip's coming days on a republished feed", () => {
  const AT = new Date(2026, 9, 23, 19, 8).getTime();
  it("Chilliwack: +2 and +4 carry flow (CLEVER's), never weather-only", async () => {
    const idx = { fetchedAt: new Date(AT).toISOString(), stations: { "08MH001": {
      percentile: 0.05, parameter: "discharge" as const, discharge: 0.05,
      aheadDays: { from: "2026-10-24", discharge: [0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.1, 0.1, 0.1, 0.2],
                   model: ["CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER", "CLEVER",
                           "ELF"], lowWater: [9] } } } };
    const spot = base({ reading: { station: "08MH001", discharge: 11, level: null, percentile: 0.05, at: "x" } });
    const { fetched } = await fetchLike(spot, NOTHING_FETCHED, { feed: { index: async () => idx } }, AT);
    expect(comingConditions(fetched, likeDays(AT), "09:00").map((c) => [c.date, c.flow, c.flowModel ?? null,
                                                                         c.flowLowWater ?? null])).toEqual([
      ["2026-10-23", 0.05, null, null], ["2026-10-24", 0.05, "CLEVER", false], ["2026-10-25", 0.06, "CLEVER", false],
      ["2026-10-26", 0.07, "CLEVER", false], ["2026-10-27", 0.08, "CLEVER", false]]);
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
      flow: { "2026-10-22": 0.4, "2026-10-23": null, "2026-10-25": null, "2026-10-27": null, "2026-11-01": null },
      flowModels: {} } });
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
