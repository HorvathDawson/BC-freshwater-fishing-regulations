/**
 * What a spot's week-before strip draws (S1), decided here so the component only draws it (rule 25).
 *
 * Every string comes from `@app/data/spots` words; this only chooses which, and says why a half is missing rather
 * than drawing an empty grid (an empty grid of dashes would imply we looked).
 */
import { LEAD_UP_WORDS, leadUpDayLabel, leadUpGaugeCells, leadUpGaugeMeta, leadUpGaugeSentence, leadUpGaugeTrend,
         leadUpQuantity, leadUpRainLine, leadUpWeatherCells, leadUpWeatherSentence,
         type LeadUp } from "@app/data/spots";

export interface LeadUpView {
  heading: string;
  /** Seven columns, oldest first; each `label` is the day said whole (a screen reader, a test). */
  weather: {
    days: { label: string; day: string; sky: string; temps: string; rain: string; wind: string }[];
    rain: string | null;
    source: string;
  } | null;
  weatherNote: string | null;
  gauge: {
    unit: string;
    days: { label: string; day: string; value: string; place: string }[];
    trend: string | null;
    meta: string;
  } | null;
  gaugeNote: string | null;
}

export function leadUpView(lu: LeadUp | null | undefined): LeadUpView | null {
  if (!lu) return null;
  const weather = lu.weather ? {
    days: lu.weather.map((d) => ({ label: leadUpWeatherSentence(d), ...leadUpWeatherCells(d) })),
    rain: leadUpRainLine(lu.weather),
    source: lu.backfilled ? LEAD_UP_WORDS.weatherFilledIn : LEAD_UP_WORDS.weatherSource,
  } : null;
  const q = lu.gauge ? leadUpQuantity(lu.gauge) : null;
  const gauge = lu.gauge && q && lu.station ? {
    unit: q === "level" ? "m" : "m³/s",
    days: lu.gauge.map((d) => ({ label: leadUpGaugeSentence(d, q), day: leadUpDayLabel(d.date),
                                 ...leadUpGaugeCells(d, q) })),
    trend: leadUpGaugeTrend(lu.gauge, q),
    meta: leadUpGaugeMeta(lu.station, lu.backfilled),
  } : null;
  return {
    heading: LEAD_UP_WORDS.heading,
    weather,
    weatherNote: weather ? null : LEAD_UP_WORDS.weatherMissing,
    gauge,
    gaugeNote: gauge ? null
      : lu.station === null ? LEAD_UP_WORDS.gaugeNone
      : lu.gauge === null ? LEAD_UP_WORDS.gaugeMissing
      : LEAD_UP_WORDS.gaugeEmpty,
  };
}
