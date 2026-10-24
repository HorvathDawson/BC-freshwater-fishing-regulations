/**
 * The week before a visit (S1): daily weather, and the matched station's daily means, for the seven local days
 * before it (`leadUpDays` in @app/core).
 *
 * THE GAUGE DAYS COME THROUGH THE EXISTING SOURCE, never a new one. `WaterSource.gaugeSeries(station, "year", p)` is
 * already the join of the feed's `daily` (this year's daily means in the station file) and its `priorYears` with the
 * bundle's envelope for each day of the year; this reads seven days out of it and places each in its envelope
 * (`placeInBand`). A day with no mean stays null.
 *
 * NOTHING HERE INVENTS A VALUE, and "could not ask" is kept apart from "asked, and nothing there": a fetch that failed
 * is `null` (a refresh retries it), a fetch that worked is seven days, any of them null.
 *
 * AND A DAY NOT THERE YET IS KEPT APART FROM A DAY WITH NOTHING IN IT (owner bug 2026-10-22). A week is often saved
 * before its sources have caught up with it: the feed is written a few times a day, so the day it was written on is
 * not over and the days after it have not been published. Those days are held empty WITHOUT `none`, and a refresh asks
 * for them again (`leadUpPendingDays`); a day the feed had been published past and holds nothing for is `none: true`,
 * and is never asked for again. A weather day with no value at all is one the service did not send: asked again too.
 */
import { calendarDayOfYear, localDate, placeInBand, type Band } from "@app/core";
import type { Aged, Parameter, WaterSource, Series, StationId } from "../index";
import type { WeatherSource } from "./capture";
import type { DailyGauge, DailyWeather, LeadUp } from "./model";

const blankDay = (date: string): DailyWeather =>
  ({ date, maxC: null, minC: null, precipMm: null, windMaxKph: null, code: null });

/** A gauge day with no mean: still to come (no `none`), or settled with nothing in it. */
const emptyDay = (date: string, settled: boolean): DailyGauge => ({
  date, discharge: null, level: null, dischargePlace: null, levelPlace: null, ...(settled ? { none: true as const } : {}),
});

/** A weather day the service has not sent: every value null. Asked for again. */
export function weatherDayPending(d: DailyWeather): boolean {
  return d.maxC === null && d.minC === null && d.precipMm === null && d.windMaxKph === null && d.code === null;
}

/** A gauge day with no mean that is not settled as having none: asked for again. */
export function gaugeDayPending(d: DailyGauge): boolean {
  return d.discharge === null && d.level === null && !d.none;
}

/**
 * Which of the feed's stations it carries, so a station's days that cannot be read can be told apart: the feed does
 * not carry it (an answer — nothing will come), or it could not be asked (offline — ask again). The live index
 * (`httpFeed().index()`, the same object a capture freezes the estimate from).
 */
export interface FeedIndex {
  index(): Promise<{ stations: Readonly<Record<string, unknown>> } | null>;
}

/** True only when the index was read and the station is not in it; anything uncertain is "ask again". */
async function notCarried(feed: FeedIndex | undefined, station: string): Promise<boolean> {
  if (!feed) return false;
  try {
    const idx = await feed.index();
    return !!idx && !(station in idx.stations);
  } catch {
    return false;
  }
}

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

/**
 * One day's mean and that day's envelope, out of a "year" series: this year's days, or a prior year's. The one reader
 * of a day in a station's year — the week before a visit (`weekGauge`) and the visit day's own flow (`flowOnDay`, T170).
 */
export function dayIn(s: Series | null, date: string): { v: number | null; band: Band | null } {
  if (!s) return { v: null, band: null };
  const i = calendarDayOfYear(date) - 1;
  const year = Number(date.slice(0, 4));
  const vals = year === Number(s.from.slice(0, 4)) ? s.values
    : s.priorYears.find((p) => p.year === year)?.values ?? [];
  const v = vals[i] ?? null;
  // Last December, read in January, lies past `band` (which stops with this year's frame): the whole-year envelope.
  const band = (s.yearBand?.[i] ?? s.band[i]) as Band | null | undefined;
  return { v: v !== null && Number.isFinite(v) ? v : null, band: band ?? null };
}

/**
 * The station's daily means for these days, each placed in its envelope.
 *
 * Null when they could not be read (the feed unreachable: the series comes back with no observations and so no age,
 * P02), which a refresh retries — unless `feed`'s index was read and does not carry the station, which is an answer:
 * seven days settled with nothing in them. A station with no envelope in either quantity has no daily path in the
 * source at all, and gets the same.
 *
 * A day on or after the day the feed was written (its age, P02) is held as still to come, even if the feed has a mean
 * for it: that mean is of a day not yet over, and freezing it would keep a morning as the day.
 */
export async function weekGauge(source: WaterSource, station: string, days: readonly string[],
                                feed?: FeedIndex): Promise<DailyGauge[] | null> {
  const id = station as StationId;
  let params: readonly Parameter[];
  try { params = await source.gaugeParameters(id); } catch { return null; }
  const settled = () => days.map((date) => emptyDay(date, true));
  if (!params.length) return settled();
  const read = async (p: Parameter): Promise<Aged<Series> | null | undefined> => {
    if (!params.includes(p)) return undefined;                          // not measured: nothing to ask
    try {
      const got = await source.gaugeSeries(id, "year", p);
      return got && got.fetchedAt !== null ? got : null;                // no age = no observations = not read
    } catch { return null; }
  };
  const [q, lv] = await Promise.all([read("discharge"), read("level")]);
  if (q === null || lv === null) return (await notCarried(feed, station)) ? settled() : null;
  // the day the feed was written, in the reader's own days (the week's days are local too)
  const written = localDate(Math.max(q?.fetchedAt ?? 0, lv?.fetchedAt ?? 0));
  return days.map((date) => {
    if (date >= written) return emptyDay(date, false);
    const a = dayIn(q?.value ?? null, date), b = dayIn(lv?.value ?? null, date);
    if (a.v === null && b.v === null) return emptyDay(date, true);
    return { date, discharge: a.v, level: b.v,
             dischargePlace: placeInBand(a.v, a.band), levelPlace: placeInBand(b.v, b.band) };
  });
}

/**
 * The quantity the station's feed declares (T106 review), to freeze beside its days: the publisher's `parameter`, read
 * as the feed client carries it, never inferred from the days. Null when the reading cannot be had.
 */
export async function declaredQuantity(source: WaterSource, station: string): Promise<Parameter | null> {
  try { return (await source.gaugeNow(station as StationId))?.value.parameter ?? null; } catch { return null; }
}

/** Both halves for a visit, as `captureSpot` freezes them. */
export async function leadUpFor(source: WaterSource, weather: WeatherSource | undefined,
                                at: { lat: number; lon: number }, station: string | null,
                                days: readonly string[], feed?: FeedIndex): Promise<LeadUp> {
  const [w, g, p] = await Promise.all([
    weekWeather(weather, at.lat, at.lon, days),
    station ? weekGauge(source, station, days, feed) : Promise.resolve(null),
    station ? declaredQuantity(source, station) : Promise.resolve(null),
  ]);
  return { weather: w, station, gauge: g, ...(g && p ? { parameter: p } : {}) };
}

/** How many days of each half are still to come: each is asked for again by a refresh. */
export function leadUpPendingDays(lu: LeadUp | null | undefined): { weather: number; gauge: number } {
  return {
    weather: lu?.weather?.filter(weatherDayPending).length ?? 0,
    gauge: lu?.station !== null ? lu?.gauge?.filter(gaugeDayPending).length ?? 0 : 0,
  };
}

/** Is a whole half of the week still to be asked for? A spot with no lead-up at all is (one saved before S1). */
export function leadUpUnread(lu: LeadUp | null | undefined): boolean {
  if (!lu) return true;
  return lu.weather === null || (lu.station !== null && lu.gauge === null);
}

/** Is any of the week still worth asking for: a whole half, or a day the sources did not have yet? */
export function leadUpMissing(lu: LeadUp | null | undefined): boolean {
  if (leadUpUnread(lu)) return true;
  const p = leadUpPendingDays(lu);
  return p.weather > 0 || p.gauge > 0;
}

/**
 * The days a refresh fetched, onto the week the spot holds: ONLY a day still to come is replaced, and only by a day
 * that is not still to come itself. Every day the spot already holds — a value, or settled as none — stays exactly as
 * captured. The same array back when nothing changed.
 */
export function fillDays<T extends { date: string }>(had: T[] | null, got: T[] | null,
                                                    pending: (d: T) => boolean): T[] | null {
  if (!got) return had;
  if (!had) return got;
  let changed = false;
  const out = had.map((d) => {
    if (!pending(d)) return d;
    const n = got.find((x) => x.date === d.date);
    if (!n || pending(n)) return d;
    changed = true;
    return n;
  });
  return changed ? out : had;
}
