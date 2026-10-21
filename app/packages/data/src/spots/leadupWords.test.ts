/**
 * S1: the words a spot's week-before strip and its photographs are shown with, pinned whole (AGENTS 33, P12).
 */
import { describe, expect, it } from "vitest";
import type { DailyGauge, DailyWeather } from "./model";
import { LEAD_UP_WORDS, leadUpDayLabel, leadUpGaugeCells, leadUpGaugeMeta, leadUpGaugeSentence,
         leadUpGaugeTrend, leadUpQuantity, leadUpRainLine, leadUpWeatherCells, leadUpWeatherSentence,
         photoFailed, PHOTO_WORDS, removePhotoLabel, skyWord, staleSpotsLine } from "./words";

const w = (date: string, over: Partial<DailyWeather> = {}): DailyWeather =>
  ({ date, maxC: 18.4, minC: 8.6, precipMm: 4.2, windMaxKph: 21.7, code: 61, ...over });
const g = (date: string, discharge: number | null, place: DailyGauge["dischargePlace"],
           level: number | null = null, levelPlace: DailyGauge["levelPlace"] = null): DailyGauge =>
  ({ date, discharge, level, dischargePlace: place, levelPlace });

describe("a lead-up weather day", () => {
  it("heads its column with the local weekday and day", () => {
    expect(leadUpDayLabel("2026-08-23")).toBe("Sun 23");
  });

  it("is drawn as whole cells", () => {
    expect(leadUpWeatherCells(w("2026-08-23"))).toEqual(
      { day: "Sun 23", sky: "Rain", temps: "18° / 9°", rain: "4.2 mm", wind: "22 km/h" });
  });

  it("draws a day with nothing recorded as dashes, never as a neighbour's values", () => {
    expect(leadUpWeatherCells({ date: "2026-08-26", maxC: null, minC: null, precipMm: null, windMaxKph: null,
                                code: null })).toEqual(
      { day: "Wed 26", sky: "—", temps: "— / —", rain: "—", wind: "—" });
  });

  it("is said whole, for a screen reader", () => {
    expect(leadUpWeatherSentence(w("2026-08-23")))
      .toBe("Sun Aug 23: rain, high 18°C, low 9°C, 4.2 mm of rain, wind up to 22 km/h");
    expect(leadUpWeatherSentence(w("2026-08-24", { precipMm: 0, code: 0, minC: null })))
      .toBe("Mon Aug 24: clear, high 18°C, no rain, wind up to 22 km/h");
    expect(leadUpWeatherSentence({ date: "2026-08-26", maxC: null, minC: null, precipMm: null,
                                   windMaxKph: null, code: null }))
      .toBe("Wed Aug 26: no weather recorded");
  });

  it("names the sky by the WMO code's group", () => {
    expect([0, 1, 2, 3, 45, 51, 61, 66, 71, 80, 85, 95, 7, null].map(skyWord)).toEqual(
      ["Clear", "Fair", "Fair", "Cloud", "Fog", "Drizzle", "Rain", "Ice", "Snow", "Showers", "Snow", "Storm",
       "—", "—"]);
  });
});

describe("the week's rain", () => {
  it("totals what was recorded and counts the wet days (1 mm or more)", () => {
    expect(leadUpRainLine([w("2026-08-23", { precipMm: 4.2 }), w("2026-08-24", { precipMm: 0.4 }),
                           w("2026-08-25", { precipMm: 12 })]))
      .toBe("16.6 mm of rain in the week before, on 2 days.");
  });

  it("says a dry week is a dry week", () => {
    expect(leadUpRainLine([w("2026-08-23", { precipMm: 0 }), w("2026-08-24", { precipMm: 0 })]))
      .toBe("No rain in the week before.");
  });

  it("says how many days it could not count", () => {
    expect(leadUpRainLine([w("2026-08-23", { precipMm: 1 }), w("2026-08-24", { precipMm: null }),
                           w("2026-08-25", { precipMm: null })]))
      .toBe("1 mm of rain in the week before, on 1 day (2 days not recorded).");
  });

  it("says nothing when no day was recorded", () => {
    expect(leadUpRainLine([w("2026-08-23", { precipMm: null })])).toBeNull();
  });
});

describe("the station's daily means", () => {
  const week = [g("2026-08-23", 50, 0.625), g("2026-08-24", null, null), g("2026-08-25", 9.137, "below"),
                g("2026-08-26", 120, "above"), g("2026-08-27", 20, 0.25)];

  it("shows flow where the station has it, and level only where it has no flow", () => {
    expect(leadUpQuantity(week)).toBe("discharge");
    expect(leadUpQuantity([g("2026-08-23", null, null, 1.42, 0.5)])).toBe("level");
    expect(leadUpQuantity([g("2026-08-23", null, null)])).toBeNull();
  });

  it("draws each day as a value and its place in the envelope", () => {
    expect(week.map((d) => leadUpGaugeCells(d, "discharge"))).toEqual([
      { value: "50.0", place: "p63rd" }, { value: "—", place: "" }, { value: "9.14", place: "<p10" },
      { value: "120", place: ">p90" }, { value: "20.0", place: "p25th" }]);
    expect(leadUpGaugeCells(g("2026-08-23", null, null, 1.4231, 0.5), "level"))
      .toEqual({ value: "1.42", place: "p50th" });
  });

  it("says each day whole", () => {
    expect(week.map((d) => leadUpGaugeSentence(d, "discharge"))).toEqual([
      "Sun Aug 23: 50.0 m³/s, the 63rd percentile for the day",
      "Mon Aug 24: no daily mean recorded",
      "Tue Aug 25: 9.14 m³/s, below the 10th percentile for the day",
      "Wed Aug 26: 120 m³/s, above the 90th percentile for the day",
      "Thu Aug 27: 20.0 m³/s, the 25th percentile for the day",
    ]);
    expect(leadUpGaugeSentence(g("2026-08-23", 3.2, null), "discharge"))
      .toBe("Sun Aug 23: 3.20 m³/s, no record to compare it with");
  });

  it("names the week's direction from its first and last known days", () => {
    expect(leadUpGaugeTrend(week, "discharge")).toBe("Flow fell over the week: 50.0 → 20.0 m³/s");
    expect(leadUpGaugeTrend([g("2026-08-23", 20, null), g("2026-08-29", 30, null)], "discharge"))
      .toBe("Flow rose over the week: 20.0 → 30.0 m³/s");
    expect(leadUpGaugeTrend([g("2026-08-23", 41.2, null), g("2026-08-29", 42, null)], "discharge"))
      .toBe("Flow held steady over the week: 41.2 → 42.0 m³/s");
    expect(leadUpGaugeTrend([g("2026-08-23", null, null, 1.2), g("2026-08-29", null, null, 1.21)], "level"))
      .toBe("Level held steady over the week: 1.20 → 1.21 m");
    expect(leadUpGaugeTrend([g("2026-08-23", null, null, 1.2), g("2026-08-29", null, null, 1.35)], "level"))
      .toBe("Level rose over the week: 1.20 → 1.35 m");
  });

  it("claims no direction from fewer than two days", () => {
    expect(leadUpGaugeTrend([g("2026-08-23", 20, null), g("2026-08-24", null, null)], "discharge")).toBeNull();
  });

  it("names the station, and says when the week was filled in afterwards", () => {
    expect(leadUpGaugeMeta("08MH001", false)).toBe("08MH001 · daily means");
    expect(leadUpGaugeMeta("08MH001", true)).toBe("08MH001 · daily means · filled in afterwards");
  });
});

describe("the fixed words", () => {
  it("are whole sentences", () => {
    expect(LEAD_UP_WORDS).toEqual({
      heading: "The week before",
      weatherMissing: "The week's weather was not recorded.",
      gaugeNone: "No gauge speaks for this water, so there are no daily flows for the week.",
      gaugeMissing: "The station's daily means for the week could not be read.",
      gaugeEmpty: "The station has no daily means for that week.",
      weatherFilledIn: "Weather from Open-Meteo.com (CC BY 4.0) · filled in afterwards",
      weatherSource: "Weather from Open-Meteo.com (CC BY 4.0)",
    });
    expect(PHOTO_WORDS).toEqual({
      heading: "Photographs",
      library: "Add from photos",
      camera: "Take a photo",
      remove: "Remove photo",
      missing: "This photo is no longer on this device.",
      adding: "Adding the photo…",
    });
  });

  it("counts the spots with something still to fill in, whole", () => {
    expect([1, 3].map(staleSpotsLine)).toEqual([
      "1 spot is missing the weather from the day it was pinned, or the week before it.",
      "3 spots are missing the weather from the day they were pinned, or the week before it."]);
  });

  it("labels each photo's remove button apart", () => {
    expect([1, 2].map(removePhotoLabel)).toEqual(["Remove photo 1", "Remove photo 2"]);
  });

  it("says a failed photo whole", () => {
    expect(photoFailed(new Error("camera permission refused")))
      .toBe("The photo was not added (camera permission refused). Nothing else changed.");
  });
});
