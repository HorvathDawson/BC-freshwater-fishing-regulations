/**
 * T170: a spot gets the flow of its own day — from the feed's daily series, else HYDAT through ECCC GeoMet when online;
 * never over a reading taken on the day; discharge only; offline waits. Whole values, LOCAL timestamps.
 */
import { describe, expect, it } from "vitest";
import { percentileInBand, type Band } from "@app/core";
import { dayFlowAskable, fillDayFlow, flowOnDay, geometHydat, GEOMET_DAILY_MEAN, needsDayFlow, readingIsTheDays,
         type HydatDaily, type HydatDay } from "./dayFlow";
import { needsRefresh, refreshReason, refreshSpot } from "./refresh";
import { visitConditions } from "./likeVisit";
import { DAY_FLOW_WORDS, dayFlowLine } from "./words";
import type { Spot } from "./model";
import type { Aged, Parameter, WaterSource, Series } from "../index";

/**
 * GeoMet's own answer for the Chilliwack at Vedder Crossing on 15 Aug 2024 (fetched 2026-10-23 from
 * `…/hydrometric-daily-mean/items?f=json&STATION_NUMBER=08MH001&datetime=2024-08-15/..&sortby=DATE&limit=1`),
 * trimmed to what is read.
 */
const GEOMET_08MH001_2024_08_15 = { type: "FeatureCollection", numberMatched: 685, numberReturned: 1, features: [{
  id: "08MH001.2024-08-15", type: "Feature",
  properties: { IDENTIFIER: "08MH001.2024-08-15", STATION_NAME: "CHILLIWACK RIVER AT VEDDER CROSSING",
                STATION_NUMBER: "08MH001", PROV_TERR_STATE_LOC: "BC", DATE: "2024-08-15", LEVEL: 2.052999973297119,
                DISCHARGE: 19.600000381469727, DISCHARGE_SYMBOL_EN: null, LEVEL_SYMBOL_EN: null } }] };

describe("geometHydat — one GET for the first record on or after the day", () => {
  const port = (body: unknown, ok = true) => {
    const urls: string[] = [];
    const f = (async (url: string) => { urls.push(url); return { ok, status: ok ? 200 : 503, json: async () => body }; }) as
      unknown as typeof fetch;
    return { urls, hydat: geometHydat(f) };
  };

  it("asks once, by station and day, and reads that day's flow and level", async () => {
    const { urls, hydat } = port(GEOMET_08MH001_2024_08_15);
    expect(await hydat.day("08MH001", "2024-08-15")).toEqual(
      { kind: "day", discharge: 19.6, level: 2.053 });
    expect(urls).toEqual([`${GEOMET_DAILY_MEAN}?f=json&STATION_NUMBER=08MH001&datetime=2024-08-15%2F..&sortby=DATE&limit=1`]);
    expect(GEOMET_DAILY_MEAN).toBe("https://api.weather.gc.ca/collections/hydrometric-daily-mean/items");
  });

  it("a later record only is no row for the day; nothing at all is not published yet; blanks stay null", async () => {
    const later = { features: [{ properties: { DATE: "2024-08-20", DISCHARGE: 18, LEVEL: 2 } }] };
    const blank = { features: [{ properties: { DATE: "2026-06-30", DISCHARGE: null, LEVEL: null } }] };
    expect([await port(later).hydat.day("08MH001", "2024-08-15"), await port({ features: [] }).hydat.day("08MH001", "2026-08-15"),
            await port(blank).hydat.day("08MH001", "2026-06-30")]).toEqual([
      { kind: "later", next: "2024-08-20" }, { kind: "none" }, { kind: "day", discharge: null, level: null }]);
  });

  it("an HTTP failure rejects (the caller says offline), never 'nothing there'", async () => {
    await expect(port({}, false).hydat.day("08MH001", "2024-08-15")).rejects.toThrow("HTTP 503");
  });
});

// ---------------------------------------------------------------------------------------------- the one read ----

const BAND: Band = [10, 20, 40, 60, 100];
const NOW = new Date(2026, 9, 23, 12).getTime();          // the feed was written today
/** A "year" series for 2026: this year's means on the days given, 2025 in priorYears; the same envelope every day. */
function series(values: Record<string, number>, prior: Record<string, number> = {}, fetchedAt: number | null = NOW):
  Aged<Series> {
  const doy = (d: string) => Math.round((Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10))
    - Date.UTC(+d.slice(0, 4), 0, 1)) / 86_400_000);
  const v: (number | null)[] = Array.from({ length: 366 }, () => null);
  const p: (number | null)[] = Array.from({ length: 366 }, () => null);
  for (const [d, x] of Object.entries(values)) v[doy(d)] = x;
  for (const [d, x] of Object.entries(prior)) p[doy(d)] = x;
  return { fetchedAt, value: { step: "1d", from: "2026-01-01", parameter: "discharge", at: [], values: v,
                               band: v.map(() => BAND), now: null, forecast: null, forecasts: [],
                               priorYears: [{ year: 2025, values: p }] } as unknown as Series };
}
const source = (s: Aged<Series> | null = series({ "2026-10-20": 30 }, { "2025-08-15": 50 }),
                params: readonly Parameter[] = ["discharge", "level"]): WaterSource =>
  ({ gaugeParameters: async () => params, gaugeSeries: async () => s } as unknown as WaterSource);
const hydat = (answer: HydatDay | Error): HydatDaily & { asked: string[] } => {
  const asked: string[] = [];
  return { asked, day: async (st, d) => { asked.push(`${st} ${d}`); if (answer instanceof Error) throw answer; return answer; } };
};

describe("flowOnDay — the feed's record first, then HYDAT when online", () => {
  it("a day this year from the feed's daily series, ranked in that day's envelope; HYDAT not asked", async () => {
    const h = hydat({ kind: "none" });
    expect(await flowOnDay("08MH001", "2026-10-20", { source: source(), hydat: h })).toEqual(
      { kind: "flow", discharge: 30, percentile: percentileInBand(30, BAND), place: 0.375, source: "feed" });
    expect([percentileInBand(30, BAND), h.asked]).toEqual([0.375, []]);
  });

  it("a day of a complete year the feed carries (HYDAT's, in priorYears): no request either", async () => {
    const h = hydat({ kind: "none" });
    expect(await flowOnDay("08MH001", "2025-08-15", { source: source(), hydat: h })).toEqual(
      { kind: "flow", discharge: 50, percentile: 0.625, place: 0.625, source: "hydat" });
    expect(h.asked).toEqual([]);
  });

  it("older than the feed: ONE request to HYDAT, ranked in the same envelope", async () => {
    const h = hydat({ kind: "day", discharge: 19.6, level: 2.05 });
    expect(await flowOnDay("08MH001", "2024-08-15", { source: source(), hydat: h })).toEqual(
      { kind: "flow", discharge: 19.6, percentile: percentileInBand(19.6, BAND), place: 0.244, source: "hydat" });
    expect(h.asked).toEqual(["08MH001 2024-08-15"]);
  });

  it("says what is known: not published, no record, a level only that day, the day not over", async () => {
    const ask = (a: HydatDay, date = "2024-08-15") => flowOnDay("08MH001", date, { source: source(), hydat: hydat(a) });
    expect([await ask({ kind: "none" }), await ask({ kind: "later", next: "2024-08-20" }),
            await ask({ kind: "day", discharge: null, level: null }),
            await ask({ kind: "day", discharge: null, level: 2.05 }),
            await ask({ kind: "none" }, "2026-10-23")]).toEqual([
      { kind: "not-published" }, { kind: "no-record" }, { kind: "no-record" }, { kind: "no-discharge" },
      { kind: "pending" }]);
  });

  it("a level-only station gives no flow, and asks nothing — a level never stands in", async () => {
    const h = hydat({ kind: "day", discharge: null, level: 2.05 });
    expect(await flowOnDay("08MH016", "2024-08-15", { source: source(null, ["level"]), hydat: h }))
      .toEqual({ kind: "no-discharge" });
    expect(h.asked).toEqual([]);
  });

  it("offline waits: the device says offline, or HYDAT cannot be reached, or there is no HYDAT port", async () => {
    const h = hydat({ kind: "day", discharge: 19.6, level: null });
    expect([await flowOnDay("08MH001", "2024-08-15", { source: source(), hydat: h, online: () => false }),
            await flowOnDay("08MH001", "2024-08-15", { source: source(), hydat: hydat(new Error("fetch failed")) }),
            await flowOnDay("08MH001", "2024-08-15", { source: source() })])
      .toEqual([{ kind: "offline" }, { kind: "offline" }, { kind: "offline" }]);
    expect(h.asked).toEqual([]);
  });

  it("the feed unreachable (no age): its envelope still ranks HYDAT's value; its days are not read", async () => {
    const h = hydat({ kind: "day", discharge: 50, level: null });
    expect(await flowOnDay("08MH001", "2026-10-20", { source: source(series({ "2026-10-20": 30 }, {}, null)), hydat: h }))
      .toEqual({ kind: "flow", discharge: 50, percentile: 0.625, place: 0.625, source: "hydat" });
  });
});

// ------------------------------------------------------------------------------------------------ on a spot ----

const VISIT = new Date(2024, 7, 15, 9, 20).getTime();
const SAVED = new Date(2026, 9, 23, 11).getTime();
/** A spot saved today for a visit in August 2024: it froze TODAY's reading and panel (the S1 bug). */
const backdated = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: SAVED, updatedAt: SAVED, visitedAt: VISIT, lat: 49.1, lon: -121.96, item: null,
  waterName: "Chilliwack River", title: "", notes: "", photos: [], trace: null,
  reading: { station: "08MH001", discharge: 11.2, level: 1.38, percentile: 0.05, parameter: "discharge",
             at: "2026-10-23T11:20:00-08:00" },
  panel: { answer: { ok: false, why: "no-record" }, areaKm2: 100, routesReady: false,
           rows: [{ station: "08MH001", percentile: 0.05 }] } as never,
  weather: { at: "x" } as never,
  leadUp: { weather: [], station: "08MH001", gauge: [] },
  ...over,
});
const sky = { at: async () => null };

describe("the visit day's flow on a spot", () => {
  it("a backdated spot: today's reading is not the day's, so its flow then is none until filled", () => {
    const s = backdated();
    expect([readingIsTheDays(s), needsDayFlow(s), needsRefresh(s), refreshReason(s), visitConditions(s).flow])
      .toEqual([false, true, true, DAY_FLOW_WORDS.notRecorded, null]);
  });

  it("a refresh fills the day's percentile from HYDAT, marked with its source and date, and the strip compares it", async () => {
    const h = hydat({ kind: "day", discharge: 19.6, level: 2.05 });
    const got = await refreshSpot(backdated(), sky, source(), undefined, { hydat: h });
    expect(got.dayFlow).toEqual({ state: "filled", station: "08MH001", date: "2024-08-15", discharge: 19.6,
                                  percentile: percentileInBand(19.6, BAND), place: 0.244, source: "hydat",
                                  filledAt: got.dayFlow!.state === "filled" ? got.dayFlow!.filledAt : 0 });
    expect(dayFlowLine(got.dayFlow)).toBe("19.6 m³/s, the 24th percentile for the date — filled in from the day's "
      + "record (HYDAT daily mean, 15 Aug 2024).");
    // Q161: the refresh also refills the panel for the trip day (its member asked the same one read)
    expect([visitConditions(got).flow, needsRefresh(got), dayFlowAskable(got), h.asked])
      .toEqual([percentileInBand(19.6, BAND), false, false, ["08MH001 2024-08-15", "08MH001 2024-08-15"]]);
    // the frozen reading is kept as recorded (never shown for the trip day); the save-day panel is replaced by the day's
    expect([got.reading, got.panelDay, got.panel?.rows.map((r) => [r.station, r.percentile])]).toEqual([
      backdated().reading, { state: "day", date: "2024-08-15" }, [["08MH001", percentileInBand(19.6, BAND)]]]);
  });

  it("a spot whose reading was taken ON the day is untouched: nothing asked, the same object back", async () => {
    const day = new Date(2026, 9, 20, 9).getTime();
    const s = backdated({ visitedAt: day, createdAt: day, updatedAt: day,
                          reading: { ...backdated().reading!, at: new Date(2026, 9, 20, 8, 50).toISOString() } });
    const h = hydat({ kind: "day", discharge: 99, level: null });
    expect([readingIsTheDays(s), needsDayFlow(s), visitConditions(s).flow]).toEqual([true, false, 0.05]);
    expect(await refreshSpot(s, sky, source(), undefined, { hydat: h })).toBe(s);
    expect(await fillDayFlow(s, { source: source(), hydat: h })).toEqual({ spot: s, answer: null });
    expect(h.asked).toEqual([]);
  });

  it("a spot pinned with no reading at all asks too — from the feed's own daily mean once the day is over", async () => {
    const day = new Date(2026, 9, 20, 9).getTime();
    const s = backdated({ visitedAt: day, createdAt: day, updatedAt: day,
                          reading: { station: "08MH001", discharge: null, level: null, percentile: null, at: null } });
    const got = await fillDayFlow(s, { source: source(), hydat: hydat({ kind: "none" }) }, SAVED);
    expect(got.spot.dayFlow).toEqual({ state: "filled", station: "08MH001", date: "2026-10-20", discharge: 30,
                                       percentile: 0.375, place: 0.375, source: "feed", filledAt: SAVED });
    expect(dayFlowLine(got.spot.dayFlow)).toBe("30.0 m³/s, the 38th percentile for the date — filled in from the day's "
      + "record (ECCC's daily mean, provisional, 20 Oct 2026).");
  });

  it("a level-only station settles as no flow, said; the strip compares no flow", async () => {
    const got = await fillDayFlow(backdated(), { source: source(null, ["level"]) }, SAVED);
    expect(got.spot.dayFlow).toEqual({ state: "none", station: "08MH001", date: "2024-08-15", why: "no-discharge",
                                       askedAt: SAVED });
    expect([dayFlowLine(got.spot.dayFlow), visitConditions(got.spot).flow, dayFlowAskable(got.spot)]).toEqual([
      "The record for 15 Aug 2024 holds no flow for station 08MH001: it measures water level, and a level never "
        + "stands in for a flow.", null, false]);
  });

  it("not published yet is said, kept, and asked again; offline changes nothing and is asked again", async () => {
    const np = await fillDayFlow(backdated(), { source: source(), hydat: hydat({ kind: "none" }) }, SAVED);
    expect([np.spot.dayFlow, dayFlowLine(np.spot.dayFlow), dayFlowAskable(np.spot), needsRefresh(np.spot)]).toEqual([
      { state: "not-published", station: "08MH001", date: "2024-08-15", askedAt: SAVED },
      "The flow for 15 Aug 2024 is not published yet: it is older than ECCC's recent record and not in HYDAT yet. "
        + "Try again later.", true, true]);    // still due a refresh: its save-day panel is not the trip day's (Q161)
    const later = await refreshSpot(np.spot, sky, source(), undefined, { hydat: hydat({ kind: "day", discharge: 19.6,
                                                                                          level: null }) });
    expect(later.dayFlow?.state).toBe("filled");
    const s = backdated();
    const off = await fillDayFlow(s, { source: source(), hydat: hydat({ kind: "day", discharge: 1, level: null }),
                                       online: () => false });
    expect([off.spot, off.answer, needsDayFlow(off.spot)]).toEqual([s, { kind: "offline" }, true]);
  });
});
