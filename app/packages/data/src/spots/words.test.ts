/**
 * The sentences a spot is shown with, pinned whole (P12b, AGENTS 33).
 *
 * The list row, the observation line and the weather trend were each composed inside a component, where a test could
 * only match a fragment it had itself supplied.
 */
import { describe, expect, it } from "vitest";
import type { Spot, WeatherSample } from "./model";
import { observedLine, spotHeadline, spotRowLines, weatherTrend } from "./words";

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
  it("says the station, the time and a backfill", () => {
    expect(observedLine("2026-08-30T09:00", true, "08MH016"))
      .toBe("08MH016 · observed 2026-08-30 09:00 · filled in afterwards");
  });
  it("says when no time was recorded, and claims no backfill that did not happen", () => {
    expect(observedLine(null, false)).toBe("no observation time recorded");
  });
});

describe("spotHeadline", () => {
  it("dates the spot by the visit in local time, then says where", () => {
    expect(spotHeadline({ createdAt: new Date(2026, 8, 1, 20).getTime(), visitedAt: new Date(2026, 7, 30, 18, 5).getTime(),
                          lat: 49.0974, lon: -121.9675 }))
      .toBe("2026-08-30 at 18:05 · 49.0974, -121.9675");
  });
});

describe("spotRowLines — the list row, by the LOCAL day of the visit", () => {
  const spot = (over: Partial<Spot> = {}): Spot => ({
    id: "s1", createdAt: new Date(2026, 8, 1, 20).getTime(), visitedAt: new Date(2026, 7, 30, 23, 30).getTime(),
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
    }))[0]).toBe("2026-08-30 · 15.7 m³/s · p4th · 14°C (filled in)");
  });

  it("leaves out a percentile that was never known", () => {
    expect(spotRowLines(spot({
      reading: { station: "08MH001", discharge: 15.7, level: null, percentile: null, at: null },
    }))[0]).toBe("2026-08-30 · 15.7 m³/s · no weather");
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
      .toEqual(["2026-08-30 · 4.162 m · p43rd · no weather",
                "2026-08-30 · 5.46 m³/s · p43rd · no weather",
                "2026-08-30 · 5.46 m³/s · p43rd · no weather",
                "2026-08-30 · 4.162 m · p43rd · no weather",
                "2026-08-30 · no gauge · no weather"]);
  });
});
