/**
 * The week before a visit (S1): daily weather, and the matched station's daily means, for the seven local days
 * before it (`leadUpDays` in @app/core).
 *
 * THE GAUGE DAYS COME THROUGH THE EXISTING SOURCE, never a new one. `RegsSource.gaugeSeries(station, "year", p)` is
 * already the join of the feed's `daily` (this year's daily means in the station file) and its `priorYears` with the
 * bundle's envelope for each day of the year; this reads seven days out of it and places each in its envelope
 * (`placeInBand`). A day with no mean stays null.
 *
 * NOTHING HERE INVENTS A VALUE, and "could not ask" is kept apart from "asked, and nothing there": a fetch that failed
 * is `null` (a refresh retries it), a fetch that worked is seven days, any of them null.
 */
import { placeInBand, type Band } from "@app/core";
import type { Parameter, RegsSource, Series, StationId } from "../index";
import type { WeatherSource } from "./capture";
import type { DailyGauge, DailyWeather, LeadUp } from "./model";

const blankDay = (date: string): DailyWeather =>
  ({ date, maxC: null, minC: null, precipMm: null, windMaxKph: null, code: null });

/** The weather for exactly these days, in this order; a day the service did not return is a day of nulls. */
export async function weekWeather(weather: WeatherSource | undefined, lat: number, lon: number,
                                  days: readonly string[]): Promise<DailyWeather[] | null> {
  if (!weather?.days) return null;
  try {
    const got = await weather.days(lat, lon, days);
    if (!got) return null;
    return days.map((d) => got.find((g) => g.date === d) ?? blankDay(d));
  } catch {
    return null;
  }
}

/** Day of the year, 1-based, of a "YYYY-MM-DD" — on the same index `gaugeSeries` builds its days on. */
function dayOfYear(date: string): number {
  const [y, m, d] = date.split("-").map(Number) as [number, number, number];
  return Math.round((Date.UTC(y, m - 1, d) - Date.UTC(y, 0, 1)) / 86_400_000) + 1;
}

/** One day's mean and that day's envelope, out of a "year" series: this year's days, or a prior year's. */
function dayIn(s: Series | null, date: string): { v: number | null; band: Band | null } {
  if (!s) return { v: null, band: null };
  const i = dayOfYear(date) - 1;
  const year = Number(date.slice(0, 4));
  const vals = year === Number(s.from.slice(0, 4)) ? s.values
    : s.priorYears.find((p) => p.year === year)?.values ?? [];
  const v = vals[i] ?? null;
  return { v: v !== null && Number.isFinite(v) ? v : null, band: (s.band[i] as Band | null | undefined) ?? null };
}

/**
 * The station's daily means for these days, each placed in its envelope.
 *
 * Null when they could not be read (the feed unreachable: the series comes back with no observations and so no age,
 * P02), which a refresh retries. A station with no envelope in either quantity has no daily path in the source at
 * all, and gets seven empty days: asked, and nothing there.
 */
export async function weekGauge(source: RegsSource, station: string,
                                days: readonly string[]): Promise<DailyGauge[] | null> {
  const id = station as StationId;
  let params: readonly Parameter[];
  try { params = await source.gaugeParameters(id); } catch { return null; }
  const empty = () => days.map((date) =>
    ({ date, discharge: null, level: null, dischargePlace: null, levelPlace: null }));
  if (!params.length) return empty();
  const read = async (p: Parameter): Promise<Series | null | undefined> => {
    if (!params.includes(p)) return undefined;                          // not measured: nothing to ask
    try {
      const got = await source.gaugeSeries(id, "year", p);
      return got && got.fetchedAt !== null ? got.value : null;          // no age = no observations = not read
    } catch { return null; }
  };
  const [q, lv] = await Promise.all([read("discharge"), read("level")]);
  if (q === null || lv === null) return null;                           // a quantity it measures was not read
  return days.map((date) => {
    const a = dayIn(q ?? null, date), b = dayIn(lv ?? null, date);
    return { date, discharge: a.v, level: b.v,
             dischargePlace: placeInBand(a.v, a.band), levelPlace: placeInBand(b.v, b.band) };
  });
}

/** Both halves for a visit, as `captureSpot` freezes them. */
export async function leadUpFor(source: RegsSource, weather: WeatherSource | undefined,
                                at: { lat: number; lon: number }, station: string | null,
                                days: readonly string[]): Promise<LeadUp> {
  const [w, g] = await Promise.all([
    weekWeather(weather, at.lat, at.lon, days),
    station ? weekGauge(source, station, days) : Promise.resolve(null),
  ]);
  return { weather: w, station, gauge: g };
}

/** Is any half of the week still worth asking for? A spot with no lead-up at all is (one saved before S1). */
export function leadUpMissing(lu: LeadUp | null | undefined): boolean {
  if (!lu) return true;
  return lu.weather === null || (lu.station !== null && lu.gauge === null);
}
