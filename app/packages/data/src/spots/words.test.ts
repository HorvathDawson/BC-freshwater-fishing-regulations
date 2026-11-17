/**
 * The sentences a spot is shown with, pinned whole (P12b, AGENTS 33).
 *
 * The list row, the observation line and the weather trend were each composed inside a component, where a test could
 * only match a fragment it had itself supplied.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Spot, WeatherSample } from "./model";
import { observedLine, spotHeadline, spotPlaceLine, spotRowLines, spotTraceLine, spotWaterLine, weatherTrend } from "./words";

const hour = (cloud: number | null, hPa: number | null): WeatherSample =>
  ({ at: "2026-08-30T08:00", tempC: 14, cloudPct: cloud, pressureHpa: hPa, humidityPct: 70 });
const win = (b: WeatherSample | null, a: WeatherSample | null) => ({ before: b, at: hour(60, 1013), after: a });

describe("weatherTrend", () => {
  it("names both changes when the sky closed and the barometer fell", () => {
    expect(weatherTrend(win(hour(35, 1014), hour(85, 1012))))
      .toBe("clouding over · pressure falling — across the hour either side");
  });
  it("calls an opening sky clearing and a rising barometer rising", () => {
    expect(weatherTrend(win(hour(90, 1012), hour(20, 1014))))
      .toBe("clearing · pressure rising — across the hour either side");
  });
  it("says holding and steady inside the models' own noise", () => {
    expect(weatherTrend(win(hour(58, 1014), hour(63, 1014.5))))
      .toBe("cloud holding · pressure steady — across the hour either side");
  });
  it("says only what both ends know", () => {
    expect(weatherTrend(win(hour(null, 1016), hour(80, 1011))))
      .toBe("pressure falling — across the hour either side");
  });
  it("claims no trend when an end of the window is missing", () => {
    expect(weatherTrend(win(null, hour(85, 1012)))).toBeNull();
    expect(weatherTrend(win(hour(null, null), hour(85, 1012)))).toBeNull();
  });
});

describe("observedLine", () => {
  // (T819 item 10) the spots' one style; the year is said only when it is not this one, so the clock is held in 2026
  beforeEach(() => { vi.useFakeTimers({ now: Date.parse("2026-11-16T12:00:00Z"), toFake: ["Date"] }); });
  afterEach(() => { vi.useRealTimers(); });
  it("says the station, the time and a backfill", () => {
    expect(observedLine("2026-08-30T16:00", true, "08MH016"))
      .toBe("08MH016 · observed Sun 30 Aug, 9 am · filled in afterwards");
  });
  it("(T819 item 7) names the station, its code after it, from the release's names", () => {
    expect(observedLine("2026-08-30T16:00", false, "08MH016", new Map([["08MH016", "Chilliwack River above Slesse Creek"]])))
      .toBe("Chilliwack River above Slesse Creek (08MH016) · observed Sun 30 Aug, 9 am");
  });
  it("(T620 G3) prints B.C. time: a GMT hour with no zone and an instant with Z are the same moment", () => {
    expect(observedLine("2026-08-31T03:00")).toBe("observed Sun 30 Aug, 8 pm");
    expect(observedLine("2026-08-31T03:00:00Z")).toBe("observed Sun 30 Aug, 8 pm");
    expect(observedLine("2026-12-01T03:00:00Z")).toBe("observed Mon 30 Nov, 8 pm");  // permanent daylight time, year-round
    expect(observedLine("2025-08-31T03:00:00Z")).toBe("observed Sat 30 Aug 2025, 8 pm");
  });
  it("says when no time was recorded, and claims no backfill that did not happen", () => {
    expect(observedLine(null, false)).toBe("no observation time recorded");
  });
});

describe("spotHeadline", () => {
  beforeEach(() => { vi.useFakeTimers({ now: Date.parse("2026-11-16T12:00:00Z"), toFake: ["Date"] }); });
  afterEach(() => { vi.useRealTimers(); });
  it("(T819 item 10) dates the spot by the visit, in the spots' one style, and says no coordinates", () => {
    expect(spotHeadline({ createdAt: new Date(2026, 8, 1, 20).getTime(), visitedAt: new Date(2026, 7, 30, 18, 5).getTime() }))
      .toBe("Sun 30 Aug, 6:05 pm");
  });
});

describe("the spot's header lines (T819 item 10)", () => {
  it("says the water only when it is not the title, and the place under Show on map", () => {
    const at = { lat: 49.0974, lon: -121.9675 };
    expect([spotWaterLine({ title: "", waterName: "Chilliwack River", ...at }, "Chilliwack River"),
            spotWaterLine({ title: "Tamihi run", waterName: "Chilliwack River", ...at }, "Tamihi run"),
            spotWaterLine({ title: "x", waterName: null, ...at }, "x"),
            spotPlaceLine(at)])
      .toEqual([null, "on Chilliwack River", null, "49.0974, -121.9675"]);
  });
});

describe("the trace in one sentence (T819 items 6, 7)", () => {
  const t = { station: "08MH001", stationName: "Chilliwack River at Vedder Crossing", trust: "good" as const, reaches: 13,
              metres: 17_000, rel: "down" as const, reachMagnitude: 2110, gaugeMagnitude: 2110, lon: null, lat: null };
  it("names the gauge, its code after it; says nothing when no station was traced", () => {
    expect(spotTraceLine(t)).toBe("Water here flows about 17 km down to Chilliwack River at Vedder Crossing (08MH001).");
    expect(spotTraceLine({ ...t, stationName: null }, new Map([["08MH001", "Vedder"]])))
      .toBe("Water here flows about 17 km down to Vedder (08MH001).");
    expect([spotTraceLine({ ...t, station: null }), spotTraceLine(null)]).toEqual([null, null]);
  });
});

describe("spotRowLines — the list row, by the LOCAL day of the visit", () => {
  const spot = (over: Partial<Spot> = {}): Spot => ({
    // saved minutes later the same local evening (already the next day in UTC)
    id: "s1", createdAt: new Date(2026, 7, 30, 23, 50).getTime(), visitedAt: new Date(2026, 7, 30, 23, 30).getTime(),
    updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null, waterName: null, title: "t", notes: "", photos: [],
    reading: null, weather: null, trace: null, panel: null, ...over,
  });

  it("dates the row by the local day at the water, not the UTC day it was typed up", () => {
    expect(spotRowLines(spot())).toEqual(["2026-08-30 · no gauge · no weather", "49.0974, -121.9675"]);
  });

  it("shows the reading and the weather that were recorded", () => {
    expect(spotRowLines(spot({
      reading: { station: "08MH001", discharge: 15.7, level: null, percentile: 0.038, at: null },
      weather: { window: null, tempC: 14, windKph: null, windDir: null, rain3h: null, pressureHpa: null, code: null,
                 at: null, backfilled: true },
    }))[0]).toBe("2026-08-30 · 15.7 m³/s · lower than almost every day on record · 14°C (filled in)");
  });

  it("leaves out a percentile that was never known", () => {
    expect(spotRowLines(spot({
      reading: { station: "08MH001", discharge: 15.7, level: null, percentile: null, at: null },
    }))[0]).toBe("2026-08-30 · 15.7 m³/s · no weather");
  });

  it("a trip on an earlier day than the save: that day's flow, never the save day's reading (Q161)", () => {
    const later = { createdAt: new Date(2026, 8, 1, 20).getTime(),
                    reading: { station: "08MH001", discharge: 11.2, level: null, percentile: 0.05, at: null } };
    expect([spotRowLines(spot(later))[0],
            spotRowLines(spot({ ...later, dayFlow: { state: "filled", station: "08MH001", date: "2026-08-30",
                                                     discharge: 15.7, percentile: 0.038, place: 0.038, source: "feed",
                                                     filledAt: 0 } }))[0],
            spotRowLines(spot({ ...later, dayFlow: { state: "none", station: "08MH001", date: "2026-08-30",
                                                     why: "no-discharge", askedAt: 0 } }))[0]])
      .toEqual(["2026-08-30 · flow that day not read yet · no weather",
                "2026-08-30 · 15.7 m³/s · lower than almost every day on record · no weather",
                "2026-08-30 · no flow that day · no weather"]);
  });

  it("shows the quantity the reading declares: 08ME030's level beside its level percentile (T106)", () => {
    const both = { station: "08ME030", discharge: 5.46, level: 4.162, percentile: 0.432, at: null };
    expect([spotRowLines(spot({ reading: { ...both, parameter: "level" } }))[0],
            spotRowLines(spot({ reading: { ...both, parameter: "discharge" } }))[0],
            // saved before T106, declaring nothing: discharge, as such a spot always showed
            spotRowLines(spot({ reading: both }))[0],
            // declared level, a level only: shown (it used to read "no gauge")
            spotRowLines(spot({ reading: { ...both, discharge: null, parameter: "level" } }))[0],
            // declared discharge with no discharge: no number under the wrong unit
            spotRowLines(spot({ reading: { ...both, discharge: null, parameter: "discharge" } }))[0]])
      .toEqual(["2026-08-30 · 4.162 m · lower than 6 days in 10 · no weather",
                "2026-08-30 · 5.46 m³/s · lower than 6 days in 10 · no weather",
                "2026-08-30 · 5.46 m³/s · lower than 6 days in 10 · no weather",
                "2026-08-30 · 4.162 m · lower than 6 days in 10 · no weather",
                "2026-08-30 · no gauge · no weather"]);
  });
});
