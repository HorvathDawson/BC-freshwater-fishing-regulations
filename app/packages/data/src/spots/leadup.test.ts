/**
 * S1: the week before the visit, frozen onto the spot — daily weather from a weather source, and the matched station's
 * daily means through the RegsSource (the feed's `daily`, joined to the bundle's envelope by `gaugeSeries`). Seven
 * days, oldest first; a day nobody has is a day of nulls, never a neighbour's value. Captured offline, a refresh fills
 * it in later; a spot saved before it existed loads, and a refresh fills that in too.
 */
import { describe, expect, it } from "vitest";
import { placeInBand, type Band } from "@app/core";
import { captureSpot } from "./capture";
import { needsRefresh, refreshReason, refreshSpot } from "./refresh";
import { weekGauge } from "./leadup";
import { openSpotsWith, type SpotsFs } from "./core";
import type { DailyGauge, DailyWeather, Spot } from "./model";
import { LEAD_UP_WORDS } from "./words";
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
/**
 * A "year" series in this year: values only on the days given; the envelope the same every day. Published (fetched)
 * at the visit unless said otherwise, so every day of the week was over when the feed was written.
 */
function series(param: Parameter, values: Record<string, number | null>, fetchedAt: number | null = VISIT):
  Aged<Series> {
  const v: (number | null)[] = Array.from({ length: 300 }, () => null);
  for (const [d, x] of Object.entries(values)) v[DOY_AUG_23 - 1 + WEEK.indexOf(d)] = x;
  return { fetchedAt, value: { step: "1d", from: "2026-01-01", parameter: param, at: [], values: v,
                               band: v.map(() => BAND), now: null, forecast: null, forecasts: [],
                               priorYears: [] } };
}

// WHO SPEAKS (T105): a one-member panel of the Vedder, the station the reach is also matched to.
const VEDDER_PANEL = { areaKm2: 1200, members: [{ station: "08MH001", role: "up", areaKm2: 1230, years: 60,
                                                  regulated: false, sameRiver: true }] };
const source = (over: Partial<RegsSource> = {}): RegsSource => ({
  panelsFor: async (secs: readonly SectionId[]) => new Map(secs.map((s) => [s, VEDDER_PANEL])),
  lakeStationsFor: async () => new Map(),
  gaugeForSection: async () => ({ station: "08MH001", name: "Vedder", trust: "good", reachMagnitude: 40,
                                  gaugeMagnitude: 80, lon: -121.96, lat: 49.09 }),
  // A READING TAKEN ON THE VISIT DAY (T170): so these spots' own flow is the day's, and every "nothing outstanding"
  // below is about the week alone — a spot with no reading of its day would also ask for the day's flow (dayFlow.test)
  gaugeNow: async () => ({ fetchedAt: VISIT, value: { discharge: 20, level: 1.0, percentile: 0.3, standing: "normal",
                                                      at: new Date(VISIT).toISOString() } }),
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
      // a day the feed had published past and holds nothing for: asked, and nothing there — settled
      { date: "2026-08-26", discharge: null, level: null, dischargePlace: null, levelPlace: null, none: true },
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
    const s = await captureSpot({ ...base, source: source({ panelsFor: async () => new Map() }),
                                  weather: weather() });
    expect(s.leadUp!.station).toBeNull();
    expect(s.leadUp!.gauge).toBeNull();
  });

  it("holds seven days of nulls for a station with no envelope at all — read, and nothing there", async () => {
    // the whole week of weather, so only the gauge half is in question
    const s = await captureSpot({ ...base, weather: weather(WEEK.map((d) => day(d, 20))),
                                  source: source({ gaugeParameters: async () => [],
                                                   gaugeSeries: async () => null }) });
    expect(s.leadUp!.gauge).toEqual(WEEK.map((date) => (
      { date, discharge: null, level: null, dischargePlace: null, levelPlace: null, none: true })));
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
    // the 26th the service did not send is still to come: the spot keeps asking for it, and says so
    expect(needsRefresh(got)).toBe(true);
    expect(refreshReason(got)).toBe(LEAD_UP_WORDS.someDaysPending);
    // every day the service did send is there: nothing else is outstanding
    expect(needsRefresh({ ...got, leadUp: { ...got.leadUp!, weather: got.leadUp!.weather!.map((d) =>
      d.maxC === null ? day(d.date, 19) : d) } })).toBe(false);
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

/**
 * T106 REVIEW: the strip shows the reading's DECLARED quantity, and a spot saved offline keeps a reading that declares
 * nothing. At one of the 237 level-only stations its days, filled in by a refresh, then held only levels, the strip
 * looked for flows, and said "The station has no daily means for that week." So the declaration is frozen beside the
 * days whenever they are read: at capture, and by the refresh that reads them.
 */
describe("the lead-up keeps the quantity the feed declared (T106 review)", () => {
  const levelOnly = (now: (() => Promise<unknown>) | null, fetchedAt: number | null = VISIT) => source({
    gaugeParameters: async () => ["level"],
    gaugeSeries: async () => series("level", { "2026-08-23": 1.2, "2026-08-29": 1.0 }, fetchedAt),
    gaugeNow: now ?? (async () => null),
  } as never);
  const declared = async () => ({ fetchedAt: VISIT, value: { discharge: null, level: 1.0, at: "x", percentile: 0.4,
                                                            standing: "normal", parameter: "level" } });

  it("captured: the declaration beside the days; none when the days were not read", async () => {
    const s = await captureSpot({ ...base, source: levelOnly(declared), weather: weather() });
    const off = await captureSpot({ ...base, source: levelOnly(declared, null), weather: weather() });
    expect([s.leadUp!.parameter, s.reading!.parameter, off.leadUp!.parameter]).toEqual(["level", "level", undefined]);
  });

  it("captured offline, refreshed: the refresh that reads the days reads the declaration too", async () => {
    const s = await captureSpot({ ...base, source: levelOnly(null, null),   // the feed unreachable: no reading, no days
                                  weather: weather() });
    expect([s.reading, s.leadUp!.gauge, s.leadUp!.parameter]).toEqual([
      { station: "08MH001", discharge: null, level: null, percentile: null, at: null }, null, undefined]);
    const got = await refreshSpot({ ...s, weather: { at: "x" } as never }, weather(), levelOnly(declared));
    expect([got.leadUp!.gauge![0]!.level, got.leadUp!.parameter]).toEqual([1.2, "level"]);
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


describe("a visit in the first week of January", () => {
  // The week is 27 Dec - 2 Jan. The series is THIS year's (40 days long, as in January); last December is on last
  // year's line — the feed keeps it through January — and is placed against the whole-year envelope.
  const DAYS = ["2026-12-27", "2026-12-28", "2026-12-29", "2026-12-30", "2026-12-31", "2027-01-01", "2027-01-02"];
  const LOW: Band = [1, 2, 4, 6, 10], HIGH: Band = [100, 200, 400, 600, 1000];
  const last = Array.from({ length: 365 }, () => null as number | null);
  last[360] = 3; last[364] = 500;                                   // 27 Dec and 31 Dec 2026
  const now = Array.from({ length: 40 }, () => null as number | null);
  now[0] = 5;                                                       // 1 Jan 2027
  const yearBand = Array.from({ length: 366 }, (_, i) => (i >= 300 ? HIGH : LOW));
  // written on 3 January: every day of the week was over
  const s: Aged<Series> = { fetchedAt: new Date(2027, 0, 3, 12).getTime(), value: {
    step: "1d", from: "2027-01-01", parameter: "discharge", at: [], values: now, band: yearBand.slice(0, 40), yearBand,
    now: null, forecast: null, forecasts: [], priorYears: [{ year: 2026, values: last }] } };
  it("reads last December's days and places them, instead of freezing them as nothing", async () => {
    const got = await weekGauge(source({ gaugeParameters: async () => ["discharge"],
                                         gaugeSeries: async () => s }), "08MH001", DAYS);
    expect(got!.map((d) => d.discharge)).toEqual([3, null, null, null, 500, 5, null]);
    expect(got![0]!.dischargePlace).toBe(placeInBand(3, HIGH));
    expect(got![5]!.dischargePlace).toBe(placeInBand(5, LOW));
  });
});

/**
 * OWNER BUG 2026-10-22: "the refresh for week before isn't working".
 *
 * A week is frozen at capture with the days the sources had THEN. The days they did not have yet — the end of the week
 * when the feed had not been published past it, a day the weather service did not send — were frozen as empty days
 * and counted as recorded, so the refresh never asked for them again and the spot was not even offered one. The other
 * half: a station the feed does not carry left the gauge "not read" for ever, so the spot was offered a refresh that
 * could never fill anything.
 */
describe("the refresh fills the week before (owner bug 2026-10-22)", () => {
  const ALL_WEEK = WEEK.map((d, i) => day(d, 16 + i));
  const fullWeather = () => weather(ALL_WEEK);
  /** Noon on a local day: when the feed was written. */
  const noon = (d: string) => { const [y, m, dd] = d.split("-").map(Number) as [number, number, number];
                                 return new Date(y, m - 1, dd, 12).getTime(); };
  /** The feed as published on the 27th at noon: the 27th was not over, the 28th and 29th had not happened. */
  const early = source({
    gaugeParameters: async () => ["discharge"],
    gaugeSeries: async () => series("discharge",
      { "2026-08-23": 50, "2026-08-24": 40, "2026-08-25": 30, "2026-08-26": 25, "2026-08-27": 9 }, noon("2026-08-27")),
  });
  /** The feed a week later: the whole week, and different numbers for the days the spot already holds. */
  const later = source({
    gaugeParameters: async () => ["discharge"],
    gaugeSeries: async () => series("discharge",
      { "2026-08-23": 51, "2026-08-24": 41, "2026-08-25": 31, "2026-08-26": 26, "2026-08-27": 10, "2026-08-28": 120,
        "2026-08-29": 20 }),
  });
  const pending = (date: string): DailyGauge =>
    ({ date, discharge: null, level: null, dischargePlace: null, levelPlace: null });
  const feedWith = (...stations: string[]) => ({ index: async () => ({ stations: Object.fromEntries(
    stations.map((s) => [s, { percentile: null }])) }) as never });

  it("keeps the days the feed had not published yet as still to come, and offers a refresh for them", async () => {
    const s = await captureSpot({ ...base, source: early, weather: fullWeather() });
    expect(s.leadUp!.gauge!.map((d) => d.discharge)).toEqual([50, 40, 30, 25, null, null, null]);
    // the 27th was still going when the feed was written: its mean so far is not the day's, and is not frozen
    expect(s.leadUp!.gauge!.slice(4)).toEqual([pending("2026-08-27"), pending("2026-08-28"), pending("2026-08-29")]);
    const saved = { ...s, weather: { at: "x" } as never };
    expect(needsRefresh(saved)).toBe(true);
    expect(refreshReason(saved)).toBe(LEAD_UP_WORDS.someDaysPending);
  });

  it("a refresh fills only those days, never touches a day it already held, and says it filled them in", async () => {
    const s = { ...(await captureSpot({ ...base, source: early, weather: fullWeather() })), weather: { at: "x" } as never };
    const w = fullWeather();
    const got = await refreshSpot(s, w, later);
    expect(w.asked).toEqual([]);                                     // the weather was whole: not asked again
    expect(got.leadUp!.gauge!.slice(0, 4)).toEqual(s.leadUp!.gauge!.slice(0, 4));   // 50, 40, 30, 25: as captured
    expect(got.leadUp!.gauge!.map((d) => d.discharge)).toEqual([50, 40, 30, 25, 10, 120, 20]);
    expect(got.leadUp!.weather).toBe(s.leadUp!.weather);
    expect(got.leadUp!.backfilled).toBe(true);
    // only the gauge row was filled in: only it says so
    expect(got.leadUp!.filledIn).toEqual({ gauge: true });
    expect(got.updatedAt).toBeGreaterThanOrEqual(s.updatedAt);
    expect(needsRefresh(got)).toBe(false);
  });

  it("a refresh before the feed has moved on changes nothing, so nothing is written", async () => {
    const s = { ...(await captureSpot({ ...base, source: early, weather: fullWeather() })), weather: { at: "x" } as never };
    expect(await refreshSpot(s, fullWeather(), early)).toBe(s);
  });

  it("asks again for a day the weather service did not send, and keeps every day it did", async () => {
    const s = { ...(await captureSpot({ ...base, source: later, weather: weather() })), weather: { at: "x" } as never };
    expect(s.leadUp!.weather![3]).toEqual(blank("2026-08-26"));
    expect(needsRefresh(s)).toBe(true);
    // the service now has the 26th — and different numbers for the days the spot holds, which it keeps
    const w = weather(WEEK.map((d) => day(d, 30)));
    const got = await refreshSpot(s, w, later);
    expect(w.asked).toEqual([WEEK]);
    expect(got.leadUp!.weather!.map((d) => d.maxC)).toEqual([16, 17, 18, 30, 20, 21, 22]);
    expect(got.leadUp!.gauge).toBe(s.leadUp!.gauge);
    expect(needsRefresh(got)).toBe(false);
  });

  it("a week saved before this fix, with empty days in it, is offered the refresh and filled", async () => {
    const old: Spot = { ...(await captureSpot({ ...base, source: later, weather: fullWeather() })),
                        weather: { at: "x" } as never };
    // as the app froze it then: the last three days empty, with nothing to say they were still to come
    const legacy: Spot = { ...old, leadUp: { ...old.leadUp!, gauge: old.leadUp!.gauge!.map((d, i) =>
      i >= 4 ? pending(d.date) : d) } };
    expect(needsRefresh(legacy)).toBe(true);
    const got = await refreshSpot(legacy, fullWeather(), later);
    expect(got.leadUp!.gauge!.map((d) => d.discharge)).toEqual([51, 41, 31, 26, 10, 120, 20]);
    expect(got.leadUp!.gauge!.slice(0, 4)).toEqual(legacy.leadUp!.gauge!.slice(0, 4));
    expect(needsRefresh(got)).toBe(false);
  });

  it("a day the feed has published past with nothing in it is settled, not asked for for ever", async () => {
    const gap = source({ gaugeParameters: async () => ["discharge"],
                         gaugeSeries: async () => series("discharge", { "2026-08-23": 50 }) });
    const s = { ...(await captureSpot({ ...base, source: gap, weather: fullWeather() })), weather: { at: "x" } as never };
    expect(s.leadUp!.gauge!.slice(1).every((d) => d.none === true)).toBe(true);
    expect(needsRefresh(s)).toBe(false);
  });

  describe("a station the feed does not carry", () => {
    // the envelope is in the release, the feed has no file for the station: no observations, so no age
    const unpublished = source({ gaugeSeries: async (_s: string, _sp: string, p?: Parameter) =>
      series(p ?? "discharge", {}, null) } as never);

    it("is settled at capture as a week with nothing in it — not a refresh that can never fill", async () => {
      const s = await captureSpot({ ...base, source: unpublished, weather: fullWeather(), feed: feedWith("08MF005") });
      expect(s.leadUp!.gauge).toEqual(WEEK.map((date) => ({ ...pending(date), none: true })));
      expect(needsRefresh({ ...s, weather: { at: "x" } as never })).toBe(false);
    });

    it("a spot left waiting on it is settled by the refresh, and stops asking", async () => {
      const s = await captureSpot({ ...base, source: unpublished, weather: fullWeather() });
      const saved = { ...s, weather: { at: "x" } as never };
      expect(saved.leadUp!.gauge).toBeNull();                      // no feed to ask: could not be read
      expect(needsRefresh(saved)).toBe(true);
      const got = await refreshSpot(saved, fullWeather(), unpublished, feedWith("08MF005"));
      expect(got.leadUp!.gauge).toEqual(WEEK.map((date) => ({ ...pending(date), none: true })));
      expect(needsRefresh(got)).toBe(false);
    });

    it("offline, the feed's index is not there either: still not read, and still asked again", async () => {
      const s = { ...(await captureSpot({ ...base, source: unpublished, weather: fullWeather() })),
                  weather: { at: "x" } as never };
      expect(await refreshSpot(s, fullWeather(), unpublished, { index: async () => null } as never)).toBe(s);
    });
  });

  it("through the store: the refresh writes what it filled, and a reload reads it back", async () => {
    const files = new Map<string, string>();
    const fs: SpotsFs = { exists: (n) => files.has(n), read: (n) => files.get(n)!, write: (n, t) => { files.set(n, t); },
                          rename: (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
                          remove: (n) => { files.delete(n); } };
    const store = openSpotsWith(fs, "s.json");
    const s = { ...(await captureSpot({ ...base, source: early, weather: fullWeather() })), weather: { at: "x" } as never };
    await store.put(s);
    // the refresh loop `useSpots` runs (ui/spots.ts): fill each spot, write it through `patch` if it changed
    for (const cur of await store.list()) {
      const filled = await refreshSpot(cur, fullWeather(), later);
      if (filled !== cur) await store.patch(cur.id, (now) => (now.updatedAt === cur.updatedAt ? filled : now));
    }
    const back = await openSpotsWith(fs, "s.json").get(s.id);
    expect(back!.leadUp!.gauge!.map((d) => d.discharge)).toEqual([50, 40, 30, 25, 10, 120, 20]);
    expect(needsRefresh(back!)).toBe(false);
  });
});
