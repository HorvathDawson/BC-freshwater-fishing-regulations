/**
 * S1: what the week-before strip draws, decided once (rule 25) — every string whole (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import type { DailyGauge, DailyWeather, LeadUp } from "@app/data/spots";
import { leadUpView } from "./leadUp";

const WEEK = ["2026-08-23", "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-29"];
const weather: DailyWeather[] = WEEK.map((date, i) => (
  { date, maxC: 16 + i, minC: 8, precipMm: i === 3 ? null : i === 1 ? 6 : 0, windMaxKph: 10, code: i === 1 ? 61 : 1 }));
const gauge: DailyGauge[] = WEEK.map((date, i) => (
  { date, discharge: i === 2 ? null : 50 - i * 5, level: null,
    dischargePlace: i === 2 ? null : i === 6 ? "below" : 0.5, levelPlace: null }));

describe("leadUpView", () => {
  it("draws a recorded week whole", () => {
    expect(leadUpView({ weather, station: "08MH001", gauge })).toEqual({
      heading: "The week before",
      weather: {
        days: [
          { label: "Sun Aug 23: fair, high 16°C, low 8°C, no rain, wind up to 10 km/h",
            day: "Sun 23", sky: "Fair", temps: "16° / 8°", rain: "0 mm", wind: "10 km/h" },
          { label: "Mon Aug 24: rain, high 17°C, low 8°C, 6 mm of rain, wind up to 10 km/h",
            day: "Mon 24", sky: "Rain", temps: "17° / 8°", rain: "6 mm", wind: "10 km/h" },
          { label: "Tue Aug 25: fair, high 18°C, low 8°C, no rain, wind up to 10 km/h",
            day: "Tue 25", sky: "Fair", temps: "18° / 8°", rain: "0 mm", wind: "10 km/h" },
          { label: "Wed Aug 26: fair, high 19°C, low 8°C, wind up to 10 km/h",
            day: "Wed 26", sky: "Fair", temps: "19° / 8°", rain: "—", wind: "10 km/h" },
          { label: "Thu Aug 27: fair, high 20°C, low 8°C, no rain, wind up to 10 km/h",
            day: "Thu 27", sky: "Fair", temps: "20° / 8°", rain: "0 mm", wind: "10 km/h" },
          { label: "Fri Aug 28: fair, high 21°C, low 8°C, no rain, wind up to 10 km/h",
            day: "Fri 28", sky: "Fair", temps: "21° / 8°", rain: "0 mm", wind: "10 km/h" },
          { label: "Sat Aug 29: fair, high 22°C, low 8°C, no rain, wind up to 10 km/h",
            day: "Sat 29", sky: "Fair", temps: "22° / 8°", rain: "0 mm", wind: "10 km/h" },
        ],
        rain: "6 mm of rain in the week before, on 1 day (1 day not recorded).",
        source: "Weather from Open-Meteo.com (CC BY 4.0)",
      },
      weatherNote: null,
      gauge: {
        unit: "m³/s",
        days: [
          { label: "Sun Aug 23: 50.0 m³/s, the 50th percentile for the day", day: "Sun 23", value: "50.0", place: "p50th" },
          { label: "Mon Aug 24: 45.0 m³/s, the 50th percentile for the day", day: "Mon 24", value: "45.0", place: "p50th" },
          { label: "Tue Aug 25: no daily mean recorded", day: "Tue 25", value: "—", place: "" },
          { label: "Wed Aug 26: 35.0 m³/s, the 50th percentile for the day", day: "Wed 26", value: "35.0", place: "p50th" },
          { label: "Thu Aug 27: 30.0 m³/s, the 50th percentile for the day", day: "Thu 27", value: "30.0", place: "p50th" },
          { label: "Fri Aug 28: 25.0 m³/s, the 50th percentile for the day", day: "Fri 28", value: "25.0", place: "p50th" },
          { label: "Sat Aug 29: 20.0 m³/s, below the 10th percentile for the day", day: "Sat 29", value: "20.0",
            place: "<p10" },
        ],
        trend: "Flow fell over the week: 50.0 → 20.0 m³/s",
        meta: "08MH001 · daily means",
      },
      gaugeNote: null,
    });
  });

  it("says a filled-in week was filled in, on both halves", () => {
    const v = leadUpView({ weather, station: "08MH001", gauge, backfilled: true })!;
    expect([v.weather!.source, v.gauge!.meta]).toEqual([
      "Weather from Open-Meteo.com (CC BY 4.0) · filled in afterwards",
      "08MH001 · daily means · filled in afterwards"]);
  });

  it("says why a half is missing instead of drawing an empty grid", () => {
    const say = (lu: LeadUp) => { const v = leadUpView(lu)!; return [v.weatherNote, v.gaugeNote, !!v.weather, !!v.gauge]; };
    expect(say({ weather: null, station: null, gauge: null })).toEqual([
      "The week's weather was not recorded.",
      "No gauge speaks for this water, so there are no daily flows for the week.", false, false]);
    expect(say({ weather, station: "08MH001", gauge: null })).toEqual([
      null, "The station's daily means for the week could not be read.", true, false]);
    expect(say({ weather, station: "08MH001",
                 gauge: WEEK.map((date) => ({ date, discharge: null, level: null, dischargePlace: null,
                                              levelPlace: null })) })).toEqual([
      null, "The station has no daily means for that week.", true, false]);
  });

  it("draws nothing for a spot with no lead-up — the refresh line says why", () => {
    expect(leadUpView(null)).toBeNull();
    expect(leadUpView(undefined)).toBeNull();
  });
});
