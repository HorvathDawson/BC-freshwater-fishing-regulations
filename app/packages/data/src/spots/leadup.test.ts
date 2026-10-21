/**
 * S1: the week before the visit, frozen onto the spot — daily weather from a weather source, and the matched station's
 * daily means through the RegsSource (the feed's `daily`, joined to the bundle's envelope by `gaugeSeries`). Seven
 * days, oldest first; a day nobody has is a day of nulls, never a neighbour's value. Captured offline, a refresh fills
 * it in later; a spot saved before it existed loads, and a refresh fills that in too.
 */
import { describe, expect, it } from "vitest";
import type { Band } from "@app/core";
import { captureSpot } from "./capture";
import { needsRefresh, refreshReason, refreshSpot } from "./refresh";
import { openSpotsWith, type SpotsFs } from "./core";
import type { DailyWeather } from "./model";
import type { WeatherSource } from "./capture";
import type { Aged, Parameter, RegsSource, SectionId, Series } from "../index";

// Local parts: the week is the reader's own seven days, in any time zone the suite runs in.
const VISIT = new Date(2026, 7, 30, 9).getTime();
const WEEK = ["2026-08-23", "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-29"];
const DOY_AUG_23 = 235;            // 2026 is not a leap year: Aug 23 is day 235, index 234

const day = (date: string, maxC: number | null): DailyWeather =>
  ({ date, maxC, minC: 9, precipMm: 0, windMaxKph: 12, code: 1 });
const blank = (date: string): DailyWeather =>
  ({ date, maxC: null, minC: null, precipMm: null, windMaxKph: null, code: null });

/** The service returns six of the seven days, out of order: the 26th is missing. */
const weather = (got: DailyWeather[] | null = [
  day("2026-08-29", 22), day("2026-08-23", 16), day("2026-08-24", 17), day("2026-08-25", 18),
  day("2026-08-27", 20), day("2026-08-28", 21),
]): WeatherSource & { asked: string[][] } => {
  const asked: string[][] = [];
  return { asked, at: async () => null, days: async (_la, _lo, days) => { asked.push([...days]); return got; } };
};

const BAND: Band = [10, 20, 40, 60, 100];
/** A "year" series in this year: values only on the days given; the envelope the same every day. */
function series(param: Parameter, values: Record<string, number | null>, fetchedAt: number | null = 1):
  Aged<Series> {
  const v: (number | null)[] = Array.from({ length: 300 }, () => null);
  for (const [d, x] of Object.entries(values)) v[DOY_AUG_23 - 1 + WEEK.indexOf(d)] = x;
  return { fetchedAt, value: { step: "1d", from: "2026-01-01", parameter: param, at: [], values: v,
                               band: v.map(() => BAND), now: null, forecast: null, forecasts: [],
                               priorYears: [] } };
}

const source = (over: Partial<RegsSource> = {}): RegsSource => ({
  gaugeForSection: async () => ({ station: "08MH001", name: "Vedder", trust: "good", reachMagnitude: 40,
                                  gaugeMagnitude: 80, lon: -121.96, lat: 49.09 }),
  gaugeNow: async () => null,
  traceToGauge: async () => [],
  gaugeParameters: async () => ["discharge", "level"],
  gaugeSeries: async (_s: string, _span: string, p?: Parameter) => p === "level"
    ? series("level", { "2026-08-23": 1.2, "2026-08-29": 1.0 })
    : series("discharge", { "2026-08-23": 50, "2026-08-24": 40, "2026-08-25": 30, "2026-08-27": 9,
                            "2026-08-28": 120, "2026-08-29": 20 }),
  ...over,
} as unknown as RegsSource);

const base = { at: { lat: 49.0974, lon: -121.9675 }, item: null, section: 7 as SectionId,
               waterName: "Chilliwack River", title: "", now: VISIT, visitedAt: VISIT };

describe("the lead-up, captured", () => {
  it("asks the weather for the seven local days before the visit, and keeps a missing day as nulls", async () => {
    const w = weather();
    const s = await captureSpot({ ...base, source: source(), weather: w });
    expect(w.asked).toEqual([WEEK]);
    expect(s.leadUp!.weather).toEqual([
      day("2026-08-23", 16), day("2026-08-24", 17), day("2026-08-25", 18), blank("2026-08-26"),
      day("2026-08-27", 20), day("2026-08-28", 21), day("2026-08-29", 22)]);
  });

  it("reads the station's daily means for the same seven days, each placed in its envelope; a gap stays null", async () => {
    const s = await captureSpot({ ...base, source: source(), weather: weather() });
    expect(s.leadUp!.station).toBe("08MH001");
    expect(s.leadUp!.gauge).toEqual([
      { date: "2026-08-23", discharge: 50, level: 1.2, dischargePlace: 0.625, levelPlace: "below" },
      { date: "2026-08-24", discharge: 40, level: null, dischargePlace: 0.5, levelPlace: null },
      { date: "2026-08-25", discharge: 30, level: null, dischargePlace: 0.375, levelPlace: null },
      { date: "2026-08-26", discharge: null, level: null, dischargePlace: null, levelPlace: null },
      { date: "2026-08-27", discharge: 9, level: null, dischargePlace: "below", levelPlace: null },
      { date: "2026-08-28", discharge: 120, level: null, dischargePlace: "above", levelPlace: null },
      { date: "2026-08-29", discharge: 20, level: 1.0, dischargePlace: 0.25, levelPlace: "below" },
    ]);
  });

  it("is not backfilled when it was captured with the spot", async () => {
    const s = await captureSpot({ ...base, source: source(), weather: weather() });
    expect(s.leadUp!.backfilled).toBeUndefined();
  });

  it("records no station and no gauge days where no gauge speaks for the water", async () => {
    const s = await captureSpot({ ...base, source: source({ gaugeForSection: async () => null }),
                                  weather: weather() });
    expect(s.leadUp!.station).toBeNull();
    expect(s.leadUp!.gauge).toBeNull();
  });

  it("holds seven days of nulls for a station with no envelope at all — read, and nothing there", async () => {
    const s = await captureSpot({ ...base, weather: weather(),
                                  source: source({ gaugeParameters: async () => [],
                                                   gaugeSeries: async () => null }) });
    expect(s.leadUp!.gauge).toEqual(WEEK.map((date) => (
      { date, discharge: null, level: null, dischargePlace: null, levelPlace: null })));
    expect(needsRefresh({ ...s, weather: { at: "x" } as never })).toBe(false);
  });

  it("offline: the weather and the station's days are null, and the spot asks for a refresh", async () => {
    const s = await captureSpot({
      ...base, weather: weather(null),
      // the feed unreachable: the bundle's envelope answers, but with no observations (an unknown age, P02)
      source: source({ gaugeSeries: async (_s: string, _sp: string, p?: Parameter) =>
        series(p ?? "discharge", {}, null) } as never),
    });
    expect(s.leadUp).toEqual({ weather: null, station: "08MH001", gauge: null });
    expect(needsRefresh({ ...s, weather: { at: "x" } as never })).toBe(true);
  });

  it("with no weather service wired, records that it does not know", async () => {
    const s = await captureSpot({ ...base, source: source() });
    expect(s.leadUp!.weather).toBeNull();
  });
});

describe("the lead-up, backfilled", () => {
  const offline = async () => captureSpot({
    ...base, weather: weather(null),
    source: source({ gaugeSeries: async (_s: string, _sp: string, p?: Parameter) =>
      series(p ?? "discharge", {}, null) } as never),
  });

  it("a refresh fills in a lead-up captured offline, as of the visit, and says it was filled in", async () => {
    const s = await offline();
    const w = weather();
    const got = await refreshSpot({ ...s, weather: { at: "x" } as never }, w, source());
    expect(w.asked).toEqual([WEEK]);
    expect(got.leadUp!.backfilled).toBe(true);
    expect(got.leadUp!.weather![3]).toEqual(blank("2026-08-26"));
    expect(got.leadUp!.gauge![0]).toEqual(
      { date: "2026-08-23", discharge: 50, level: 1.2, dischargePlace: 0.625, levelPlace: "below" });
    expect(needsRefresh(got)).toBe(false);
  });

  it("keeps what it already had and fills only the half that was missing", async () => {
    const s = await captureSpot({ ...base, source: source(), weather: weather(null) });
    const gauge = s.leadUp!.gauge;
    const got = await refreshSpot({ ...s, weather: { at: "x" } as never }, weather(), source({
      gaugeSeries: async () => { throw new Error("must not be asked again"); },
    }));
    expect(got.leadUp!.gauge).toBe(gauge);
    expect(got.leadUp!.weather).toHaveLength(7);
  });

  it("still offline: the spot comes back unchanged, so nothing is written", async () => {
    const s = { ...(await offline()), weather: { at: "x" } as never };
    expect(await refreshSpot(s, weather(null), source({
      gaugeSeries: async (_s: string, _sp: string, p?: Parameter) => series(p ?? "discharge", {}, null),
    } as never))).toBe(s);
  });

  it("says what is missing in one whole sentence", async () => {
    const s = { ...(await offline()), weather: { at: "x" } as never };
    expect(refreshReason(s))
      .toBe("The week before this visit was not recorded — it can be filled in from the days themselves.");
  });
});

describe("a spot saved before the lead-up existed", () => {
  const memFs = (text: string): SpotsFs => {
    const files = new Map([["s.json", text]]);
    return { exists: (n) => files.has(n), read: (n) => files.get(n)!, write: (n, t) => { files.set(n, t); },
             rename: (a, b) => { files.set(b, files.get(a)!); files.delete(a); }, remove: (n) => { files.delete(n); } };
  };
  const old = {
    id: "old", createdAt: VISIT, updatedAt: VISIT, visitedAt: VISIT, lat: 49.09, lon: -121.96, item: null,
    waterName: null, title: "Tamihi run", notes: "", photos: [],
    reading: { station: "08MH001", discharge: 41.2, level: null, percentile: 0.6, at: "2026-08-30T09:00" },
    weather: { tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3, window: null,
               at: "2026-08-30T09:00" },
    trace: null, panel: null,
  };

  it("loads as it was, with no lead-up", async () => {
    const s = await openSpotsWith(memFs(JSON.stringify([old])), "s.json").get("old");
    expect(s).toEqual(old);
    expect(s!.leadUp).toBeUndefined();
  });

  it("is offered a refresh, which fills the week from the station it recorded", async () => {
    const s = (await openSpotsWith(memFs(JSON.stringify([old])), "s.json").get("old"))!;
    expect(needsRefresh(s)).toBe(true);
    const got = await refreshSpot(s, weather(), source());
    expect(got.leadUp!.station).toBe("08MH001");
    expect(got.leadUp!.gauge).toHaveLength(7);
    expect(got.leadUp!.weather).toHaveLength(7);
    expect(got.weather).toBe(s.weather);              // the visit's own weather is never re-fetched
  });

  it("a refresh with no source still fills the weather, and leaves the gauge to be asked again", async () => {
    const s = (await openSpotsWith(memFs(JSON.stringify([old])), "s.json").get("old"))!;
    const got = await refreshSpot(s, weather());
    expect(got.leadUp!.weather).toHaveLength(7);
    expect(got.leadUp!.gauge).toBeNull();
    expect(needsRefresh(got)).toBe(true);
  });
});

