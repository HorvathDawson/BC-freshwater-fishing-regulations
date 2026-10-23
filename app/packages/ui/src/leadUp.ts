/**
 * What a spot's week-before strip draws (S1), decided here so the component only draws it (rule 25).
 *
 * Every string comes from `@app/data/spots` words; this only chooses which, and says why a half is missing rather
 * than drawing an empty grid (an empty grid of dashes would imply we looked). Days the sources did not have yet when
 * the week was saved are said too, under their row, with the refresh that fills them (owner bug 2026-10-22).
 */
import { LEAD_UP_WORDS, leadUpDayLabel, leadUpGaugeCells, leadUpGaugeMeta, leadUpGaugePending, leadUpGaugeSentence,
         leadUpGaugeTrend, leadUpPendingDays, leadUpQuantity, leadUpUnit, leadUpRainLine, leadUpWeatherCells,
         leadUpWeatherPending, leadUpWeatherSentence, type LeadUp, type SpotReading } from "@app/data/spots";

export interface LeadUpView {
  heading: string;
  /** Seven columns, oldest first; each `label` is the day said whole (a screen reader, a test). */
  weather: {
    days: { label: string; day: string; sky: string; temps: string; rain: string; wind: string }[];
    rain: string | null;
    source: string;
    /** How many days the service had not sent yet, and that a refresh fills them; null when none. */
    pending: string | null;
  } | null;
  weatherNote: string | null;
  gauge: {
    unit: string;
    days: { label: string; day: string; value: string; place: string }[];
    trend: string | null;
    meta: string;
    /** How many days the station had not published yet, and that a refresh fills them; null when none. */
    pending: string | null;
  } | null;
  gaugeNote: string | null;
}

/**
 * `declared` is the spot's own reading: the strip shows the quantity it shows (T106). Where the reading declares none
 * (saved offline, its days filled in by a refresh), the quantity frozen beside the days (`LeadUp.parameter`).
 */
export function leadUpView(lu: LeadUp | null | undefined,
                           declared?: SpotReading | null): LeadUpView | null {
  if (!lu) return null;
  const pending = leadUpPendingDays(lu);
  // "filled in afterwards" under the row a refresh filled; a week filled before `filledIn` existed: both, as it said
  const filled = (half: "weather" | "gauge") => (lu.filledIn ? !!lu.filledIn[half] : !!lu.backfilled);
  const weather = lu.weather ? {
    days: lu.weather.map((d) => ({ label: leadUpWeatherSentence(d), ...leadUpWeatherCells(d) })),
    rain: leadUpRainLine(lu.weather),
    source: filled("weather") ? LEAD_UP_WORDS.weatherFilledIn : LEAD_UP_WORDS.weatherSource,
    pending: pending.weather ? leadUpWeatherPending(pending.weather) : null,
  } : null;
  const q = lu.gauge ? leadUpQuantity(lu.gauge, { parameter: declared?.parameter ?? lu.parameter }) : null;
  const gauge = lu.gauge && q && lu.station ? {
    unit: leadUpUnit(q),
    days: lu.gauge.map((d) => ({ label: leadUpGaugeSentence(d, q), day: leadUpDayLabel(d.date),
                                 ...leadUpGaugeCells(d, q) })),
    trend: leadUpGaugeTrend(lu.gauge, q),
    meta: leadUpGaugeMeta(lu.station, filled("gauge")),
    pending: pending.gauge ? leadUpGaugePending(pending.gauge) : null,
  } : null;
  return {
    heading: LEAD_UP_WORDS.heading,
    weather,
    weatherNote: weather ? null : LEAD_UP_WORDS.weatherMissing,
    gauge,
    gaugeNote: gauge ? null
      : lu.station === null ? LEAD_UP_WORDS.gaugeNone
      : lu.gauge === null ? LEAD_UP_WORDS.gaugeMissing
      // a week with no mean on any day: still to come is said as such, never as a week with nothing in it
      : pending.gauge ? leadUpGaugePending(pending.gauge)
      : LEAD_UP_WORDS.gaugeEmpty,
  };
}
