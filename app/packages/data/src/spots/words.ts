/**
 * The sentences a spot is shown with (P12b).
 *
 * They were composed inside `SpotScreen` and `SpotsScreen`, where a test could only match a fragment it had itself
 * supplied (AGENTS 33). Here each one is a whole string a test pins, and the screens draw it.
 */
import { localDate, localDateTime, monthAbbr, percentileLabel, percentileOrdinal } from "@app/core";
import type { DailyGauge, DailyWeather, DayPlace, Spot, WeatherWindow } from "./model";
import { OPEN_METEO_ATTRIBUTION } from "./weather";

/* ---- S1: the week before the visit ------------------------------------------------------------------------------ */

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const DASH = "—";
const parts = (date: string) => {
  const [y, m, d] = date.split("-").map(Number) as [number, number, number];
  return { wd: WEEKDAYS[new Date(y, m - 1, d).getDay()]!, mon: monthAbbr(m - 1), d };
};
/** One decimal where it means something: "4.2", "0", "12". */
const tenth = (v: number): string => String(Number(v.toFixed(1)));

/** "Sun 23" — the head of a lead-up day's column. The day is the reader's own. */
export function leadUpDayLabel(date: string): string {
  const p = parts(date);
  return `${p.wd} ${p.d}`;
}

/** The sky in a word, from the day's WMO code group; a dash for none or a code outside the table. */
export function skyWord(code: number | null): string {
  if (code === null) return DASH;
  if (code === 0) return "Clear";
  if (code === 1 || code === 2) return "Fair";
  if (code === 3) return "Cloud";
  if (code === 45 || code === 48) return "Fog";
  if (code >= 51 && code <= 57) return "Drizzle";
  if (code >= 61 && code <= 65) return "Rain";
  if (code === 66 || code === 67) return "Ice";
  if ((code >= 71 && code <= 77) || code === 85 || code === 86) return "Snow";
  if (code >= 80 && code <= 82) return "Showers";
  if (code >= 95 && code <= 99) return "Storm";
  return DASH;
}

/** A lead-up weather day as the strip draws it. A value nobody recorded is a dash. */
export function leadUpWeatherCells(d: DailyWeather): { day: string; sky: string; temps: string; rain: string;
                                                        wind: string } {
  const deg = (v: number | null) => (v === null ? DASH : `${Math.round(v)}°`);
  return {
    day: leadUpDayLabel(d.date), sky: skyWord(d.code), temps: `${deg(d.maxC)} / ${deg(d.minC)}`,
    rain: d.precipMm === null ? DASH : `${tenth(d.precipMm)} mm`,
    wind: d.windMaxKph === null ? DASH : `${Math.round(d.windMaxKph)} km/h`,
  };
}

/** The day said whole: "Sun Aug 23: rain, high 18°C, low 9°C, 4.2 mm of rain, wind up to 22 km/h". */
export function leadUpWeatherSentence(d: DailyWeather): string {
  const p = parts(d.date);
  const sky = skyWord(d.code);
  const bits = [
    sky === DASH ? null : sky.toLowerCase(),
    d.maxC === null ? null : `high ${Math.round(d.maxC)}°C`,
    d.minC === null ? null : `low ${Math.round(d.minC)}°C`,
    d.precipMm === null ? null : d.precipMm === 0 ? "no rain" : `${tenth(d.precipMm)} mm of rain`,
    d.windMaxKph === null ? null : `wind up to ${Math.round(d.windMaxKph)} km/h`,
  ].filter((b): b is string => b !== null);
  return `${p.wd} ${p.mon} ${p.d}: ${bits.length ? bits.join(", ") : "no weather recorded"}`;
}

/**
 * The week's rain in one line: the total of the days recorded and how many were wet (1 mm or more, the usual line for
 * a wet day), and how many it could not count. Null when no day was recorded.
 */
export function leadUpRainLine(days: readonly DailyWeather[]): string | null {
  const known = days.filter((d) => d.precipMm !== null);
  if (!known.length) return null;
  const missing = days.length - known.length;
  const gap = missing ? ` (${missing} ${missing === 1 ? "day" : "days"} not recorded)` : "";
  const total = known.reduce((s, d) => s + d.precipMm!, 0);
  if (total === 0) return `No rain in the week before${gap}.`;
  const wet = known.filter((d) => d.precipMm! >= 1).length;
  return `${tenth(total)} mm of rain in the week before, on ${wet} ${wet === 1 ? "day" : "days"}${gap}.`;
}

/** Which quantity the strip shows: flow wherever the station has any, level only where it has no flow. */
export function leadUpQuantity(days: readonly DailyGauge[]): "discharge" | "level" | null {
  if (days.some((d) => d.discharge !== null)) return "discharge";
  if (days.some((d) => d.level !== null)) return "level";
  return null;
}

const UNIT = { discharge: "m³/s", level: "m" } as const;
/** A daily mean at the precision the number supports: "9.14", "41.2", "120"; a level to the centimetre. */
const figure = (v: number, q: "discharge" | "level"): string =>
  q === "level" ? v.toFixed(2) : v < 10 ? v.toFixed(2) : v < 100 ? v.toFixed(1) : String(Math.round(v));
const valueOf = (d: DailyGauge, q: "discharge" | "level") => (q === "level" ? d.level : d.discharge);
const placeOf = (d: DailyGauge, q: "discharge" | "level") => (q === "level" ? d.levelPlace : d.dischargePlace);

/** "p63rd", "<p10", ">p90", or nothing — the chip under a day's value. */
const placeChip = (p: DayPlace | null): string =>
  p === null ? "" : p === "below" ? "<p10" : p === "above" ? ">p90" : percentileLabel(p);

/** A lead-up gauge day as the strip draws it. */
export function leadUpGaugeCells(d: DailyGauge, q: "discharge" | "level"): { value: string; place: string } {
  const v = valueOf(d, q);
  return { value: v === null ? DASH : figure(v, q), place: v === null ? "" : placeChip(placeOf(d, q)) };
}

/** The day said whole: "Sun Aug 23: 50.0 m³/s, the 63rd percentile for the day". */
export function leadUpGaugeSentence(d: DailyGauge, q: "discharge" | "level"): string {
  const p = parts(d.date);
  const v = valueOf(d, q);
  if (v === null) return `${p.wd} ${p.mon} ${p.d}: no daily mean recorded`;
  const at = placeOf(d, q);
  const where = at === null ? "no record to compare it with"
    : at === "below" ? "below the 10th percentile for the day"
    : at === "above" ? "above the 90th percentile for the day"
    : `the ${percentileOrdinal(at)} percentile for the day`;
  return `${p.wd} ${p.mon} ${p.d}: ${figure(v, q)} ${UNIT[q]}, ${where}`;
}

/**
 * The week's direction, from its first and last days with a mean: "Flow fell over the week: 50.0 → 20.0 m³/s".
 *
 * A change under a tenth of the flow, or 5 cm of level, is held steady — inside what a daily mean wanders by without
 * the river doing anything. Null from fewer than two days: one number has no direction.
 */
export function leadUpGaugeTrend(days: readonly DailyGauge[], q: "discharge" | "level"): string | null {
  const known = days.map((d) => valueOf(d, q)).filter((v): v is number => v !== null);
  if (known.length < 2) return null;
  const a = known[0]!, b = known[known.length - 1]!;
  const moved = q === "level" ? b - a : a === 0 ? (b > 0 ? 1 : 0) : b / a - 1;
  const step = q === "level" ? 0.05 : 0.1;
  const verb = moved >= step ? "rose" : moved <= -step ? "fell" : "held steady";
  return `${q === "level" ? "Level" : "Flow"} ${verb} over the week: ${figure(a, q)} → ${figure(b, q)} ${UNIT[q]}`;
}

/** Under the gauge row: whose means they are, and whether they were filled in afterwards. */
export function leadUpGaugeMeta(station: string, backfilled?: boolean): string {
  return [station, "daily means", backfilled ? "filled in afterwards" : null].filter(Boolean).join(" · ");
}

/** The Spots list's banner over spots with something still to fill in (the visit's weather, or the week before). */
export function staleSpotsLine(n: number): string {
  return n === 1
    ? "1 spot is missing the weather from the day it was pinned, or the week before it."
    : `${n} spots are missing the weather from the day they were pinned, or the week before it.`;
}

/** The lead-up's fixed sentences. */
export const LEAD_UP_WORDS = {
  heading: "The week before",
  weatherMissing: "The week's weather was not recorded.",
  gaugeNone: "No gauge speaks for this water, so there are no daily flows for the week.",
  gaugeMissing: "The station's daily means for the week could not be read.",
  gaugeEmpty: "The station has no daily means for that week.",
  weatherFilledIn: `${OPEN_METEO_ATTRIBUTION} · filled in afterwards`,
  weatherSource: OPEN_METEO_ATTRIBUTION,
} as const;

/* ---- S1: photographs ---------------------------------------------------------------------------------------------- */

export const PHOTO_WORDS = {
  heading: "Photographs",
  library: "Add from photos",
  camera: "Take a photo",
  remove: "Remove photo",
  missing: "This photo is no longer on this device.",
  adding: "Adding the photo…",
} as const;

/** The remove button on the n-th photo (1-based), so a screen reader can tell them apart. */
export function removePhotoLabel(n: number): string {
  return `${PHOTO_WORDS.remove} ${n}`;
}

/** A photo that could not be added, said whole. The spot and its other photos are untouched. */
export function photoFailed(e: unknown): string {
  const why = e instanceof Error ? e.message : String(e);
  return `The photo was not added (${why}). Nothing else changed.`;
}

/**
 * What changed across the three hours around the visit, in words; null when it cannot be said.
 *
 * Thresholds are the width of the models' own disagreement, so a change below one is noise rather than a trend: 10
 * points of cloud, 1 hPa of pressure. Below both, the honest sentence is that it was holding — which is itself
 * information. Each half is said only when both ends of the window know it.
 */
export function weatherTrend(win: WeatherWindow): string | null {
  const b = win.before, a = win.after;
  if (!b || !a) return null;
  const bits: string[] = [];
  if (b.cloudPct != null && a.cloudPct != null) {
    const d = a.cloudPct - b.cloudPct;
    bits.push(d >= 10 ? "clouding over" : d <= -10 ? "clearing" : "cloud holding");
  }
  if (b.pressureHpa != null && a.pressureHpa != null) {
    const d = a.pressureHpa - b.pressureHpa;
    bits.push(d >= 1 ? "pressure rising" : d <= -1 ? "pressure falling" : "pressure steady");
  }
  if (!bits.length) return null;
  return `${bits.join(" · ")} — across the hour either side`;
}

/** When a frozen value was observed, by whom, and whether it was filled in after the fact. */
export function observedLine(at: string | null, backfilled?: boolean, station?: string | null): string {
  return [
    station ?? null,
    at ? `observed ${at.replace("T", " ").slice(0, 16)}` : "no observation time recorded",
    backfilled ? "filled in afterwards" : null,
  ].filter(Boolean).join(" · ");
}

/** "2026-08-30 at 09:00 · 49.0974, -121.9675" — the visit, in the reader's own time, and the place. */
export function spotHeadline(spot: Pick<Spot, "visitedAt" | "createdAt" | "lat" | "lon">): string {
  return `${localDateTime(spot.visitedAt ?? spot.createdAt)} · ${spot.lat.toFixed(4)}, ${spot.lon.toFixed(4)}`;
}

/**
 * The two lines under a spot's name in the list: the LOCAL day of the visit with what was recorded, then where.
 *
 * The visit, not the record: spots are typed up on the drive home or the week after, and the list used to date
 * them by `createdAt` in UTC — a Sunday evening's fishing read as Monday's.
 */
export function spotRowLines(spot: Spot): [string, string] {
  const g = spot.reading, w = spot.weather;
  const gauge = g?.discharge != null
    ? `${g.discharge} m³/s${g.percentile != null ? ` · ${percentileLabel(g.percentile)}` : ""}`
    : "no gauge";
  const weather = w?.tempC != null ? `${w.tempC}°C${w.backfilled ? " (filled in)" : ""}` : "no weather";
  return [`${localDate(spot.visitedAt ?? spot.createdAt)} · ${gauge} · ${weather}`,
          `${spot.lat.toFixed(4)}, ${spot.lon.toFixed(4)}`];
}
