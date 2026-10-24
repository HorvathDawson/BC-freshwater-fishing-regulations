/**
 * The sentences a spot is shown with (P12b).
 *
 * They were composed inside `SpotScreen` and `SpotsScreen`, where a test could only match a fragment it had itself
 * supplied (AGENTS 33). Here each one is a whole string a test pins, and the screens draw it.
 */
import { CONDITIONS_WORDS, localDate, localDateTime, monthAbbr, weekdayAbbr, percentileLabel, percentileOrdinal, readingIn,
         shownParameter, type DeclaresQuantity, type ShownQuantity } from "@app/core";
import type { DailyGauge, DailyWeather, DayPlace, Spot, SpotReading, WeatherWindow } from "./model";
import { OPEN_METEO_ATTRIBUTION } from "./weather";
import { isBackdated, readingIsTheDays } from "./dayFlow";

/* ---- S1: the week before the visit ------------------------------------------------------------------------------ */

const DASH = "—";
const parts = (date: string) => {
  const [y, m, d] = date.split("-").map(Number) as [number, number, number];
  return { wd: weekdayAbbr(new Date(y, m - 1, d).getDay()), mon: monthAbbr(m - 1), d };
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
  // a day with nothing at all is one the service had not sent yet; a refresh asks for it again (`weatherDayPending`)
  return `${p.wd} ${p.mon} ${p.d}: ${bits.length ? bits.join(", ") : "no weather for this day yet"}`;
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

/**
 * Which quantity the strip shows: the one the spot's reading shows (core `shownParameter`, T106) — never inferred
 * from which days hold which value. Null when no day holds a mean in it.
 */
export function leadUpQuantity(days: readonly DailyGauge[],
                               declared: DeclaresQuantity | null | undefined): ShownQuantity | null {
  const q = shownParameter(declared ?? {});
  return days.some((d) => readingIn(d, q) !== null) ? q : null;
}

// One spelling of the units: the Conditions words' (T80 1).
const UNIT = CONDITIONS_WORDS.unit;
/** The unit the strip's column is headed with: "m³/s" for flow, "m" for level. */
export const leadUpUnit = (q: "discharge" | "level"): string => UNIT[q];
/** A daily mean at the precision the number supports: "9.14", "41.2", "120"; a level to the centimetre. */
const figure = (v: number, q: "discharge" | "level"): string =>
  q === "level" ? v.toFixed(2) : v < 10 ? v.toFixed(2) : v < 100 ? v.toFixed(1) : String(Math.round(v));
const valueOf = (d: DailyGauge, q: "discharge" | "level") => (q === "level" ? d.level : d.discharge);
const placeOf = (d: DailyGauge, q: "discharge" | "level") => (q === "level" ? d.levelPlace : d.dischargePlace);

/**
 * "p63rd", "≤p10", "≥p90", or nothing — the chip under a day's value. AT OR past the edge (T107): the ranking is the
 * feed's `percentile_of`, which clamps a value ON p10 / p90 too, so "<p10" would be false for a dry creek at 0 on p10 0.
 */
const placeChip = (p: DayPlace | null): string =>
  p === null ? "" : p === "below" ? "≤p10" : p === "above" ? "≥p90" : percentileLabel(p);

/** A gauge day the feed had not published yet (no mean, not settled as none): a refresh asks for it again. */
const stillToCome = (d: DailyGauge) => d.discharge === null && d.level === null && !d.none;

/** A lead-up gauge day as the strip draws it; a day still to come says so under its dash. */
export function leadUpGaugeCells(d: DailyGauge, q: "discharge" | "level"): { value: string; place: string } {
  const v = valueOf(d, q);
  return { value: v === null ? DASH : figure(v, q),
           place: v === null ? (stillToCome(d) ? "not yet" : "") : placeChip(placeOf(d, q)) };
}

/** The day said whole: "Sun Aug 23: 50.0 m³/s, the 63rd percentile for the day". */
export function leadUpGaugeSentence(d: DailyGauge, q: "discharge" | "level"): string {
  const p = parts(d.date);
  const v = valueOf(d, q);
  if (v === null && stillToCome(d)) return `${p.wd} ${p.mon} ${p.d}: no daily mean published yet`;
  if (v === null) return `${p.wd} ${p.mon} ${p.d}: no daily mean recorded`;
  const at = placeOf(d, q);
  const where = at === null ? "no record to compare it with"
    : at === "below" ? "at or below the 10th percentile for the day"
    : at === "above" ? "at or above the 90th percentile for the day"
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

/** The Spots list's banner over spots with something still to fill in (the visit's weather or flow (T170), or the week before). */
export function staleSpotsLine(n: number): string {
  return n === 1
    ? "1 spot is missing the weather or the flow from the day it was pinned, or the week before it."
    : `${n} spots are missing the weather or the flow from the day they were pinned, or the week before it.`;
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
  /** The refresh line: a whole half of the week was never read (offline, or saved before the week was kept). */
  notRecorded: "The week before this visit was not recorded — it can be filled in from the days themselves.",
  /** The refresh line: the week was read, but some of its days were not out yet (owner bug 2026-10-22). */
  someDaysPending: "Some days of the week before were not available yet when this was saved — they can be filled in now.",
} as const;

/** Under the weather row: how many of its days the service had not sent yet, and that a refresh asks again. */
export function leadUpWeatherPending(n: number): string {
  return n === 1
    ? "1 day had no weather from Open-Meteo yet. Refresh fills it in."
    : `${n} days had no weather from Open-Meteo yet. Refresh fills them in.`;
}

/** Under the gauge row: how many of its days the station had not published yet, and that a refresh asks again. */
export function leadUpGaugePending(n: number): string {
  return n === 1
    ? "The station had not published 1 of these days yet. Refresh fills it in."
    : `The station had not published ${n} of these days yet. Refresh fills them in.`;
}

/* ---- T170: the visit day's flow, from the day's record ----------------------------------------------------------- */

/** "15 Aug 2024" — a day said in full, for a record's date. */
const fullDay = (date: string): string => {
  const [y, m, d] = date.split("-").map(Number) as [number, number, number];
  return `${d} ${monthAbbr(m - 1)} ${y}`;
};

/** Where a filled day's mean came from, in words. */
const DAY_SOURCE = { feed: "ECCC's daily mean, provisional", hydat: "HYDAT daily mean" } as const;

export const DAY_FLOW_WORDS = {
  heading: "Flow that day",
  /** The refresh line for a spot whose visit day's flow was never asked for. */
  notRecorded: "The flow on the day of this visit was not recorded — it can be filled in from the day's record.",
  action: "Get the flow for that day",
  asking: "Asking for the flow on that day…",
  offline: "Offline: the flow for that day will be asked for when you are back online.",
  pending: "That day is not over in the gauge's record yet. Try again tomorrow.",
} as const;

/**
 * Q161: what "What the water was doing" says on a spot whose trip was on an earlier day than it was saved. Its gauges
 * are that day's record (each station's daily mean), never the readings of the day it was saved.
 */
export const DAY_PANEL_WORDS = {
  /** Under the trip day's panel. */
  ofTheDay: (date: string): string => `Each gauge's daily mean on ${fullDay(date)}, the day of the visit.`,
  /** In place of a panel still to read (offline when added, or saved before the trip day's gauges were kept). */
  pending: (date: string): string =>
    `The gauges for ${fullDay(date)} have not been read from their record yet. Refresh fills them in.`,
  /** A backdated spot whose water has no gauge panel: its one gauge's flow that day is the block below. */
  flowOnly: "This water's gauge reading for the day of the visit is under Flow that day.",
  /** The refresh line. */
  notRecorded: "The gauges for the day of this visit have not been read yet. They can be filled in from the day's record.",
} as const;

/**
 * The visit day's flow said whole, or why there is none: "19.6 m³/s, the 34th percentile for the date — filled in from
 * the day's record (HYDAT daily mean, 15 Aug 2024)." Null when it was never asked for.
 */
export function dayFlowLine(df: Spot["dayFlow"]): string | null {
  if (!df) return null;
  const on = fullDay(df.date);
  if (df.state === "not-published")
    return `The flow for ${on} is not published yet: it is older than ECCC's recent record and not in HYDAT yet. `
      + "Try again later.";
  if (df.state === "none")
    return df.why === "no-discharge"
      ? `The record for ${on} holds no flow for station ${df.station}: it measures water level, and a level never `
        + "stands in for a flow."
      : `Station ${df.station}'s record holds no flow for ${on}.`;
  const p = df.place;
  const where = p === null ? "no record to rank it against"
    : p === "below" ? "at or below the 10th percentile for the date"
    : p === "above" ? "at or above the 90th percentile for the date"
    : `the ${percentileOrdinal(p)} percentile for the date`;
  return `${figure(df.discharge, "discharge")} ${UNIT.discharge}, ${where} — filled in from the day's record `
    + `(${DAY_SOURCE[df.source]}, ${on}).`;
}

/* ---- S1: photographs ---------------------------------------------------------------------------------------------- */

export const PHOTO_WORDS = {
  heading: "Photographs",
  library: "Add from photos",
  camera: "Take a photo",
  remove: "Remove photo",
  missing: "This photo is no longer on this device.",
  adding: "Adding the photo…",
} as const;

/* ---- back, everywhere: leaving a spot being added ---------------------------------------------------------------- */

/** Asked before a back (or Cancel) leaves a spot being added that holds words or photos the reader put in. */
export const DRAFT_WORDS = {
  label: "Discard this spot?",
  title: "Discard this spot?",
  detail: "The name, notes and photographs you added to it will be thrown away.",
  keep: "Keep adding",
  discard: "Discard",
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
 * A saved reading's number in the quantity it shows (core `shownParameter`, T106), with that quantity's unit. Null
 * when it holds no value in it. The list row and the spot screen both read it.
 */
export function spotReadingFigure(r: SpotReading | null | undefined):
  { figure: number; unit: string; quantity: ShownQuantity } | null {
  if (!r) return null;
  const quantity = shownParameter(r);
  const figure = readingIn(r, quantity);
  return figure === null ? null : { figure, unit: UNIT[quantity], quantity };
}

/**
 * Q161: the list row of a spot whose trip was on an earlier day than it was saved — that day's flow from its record,
 * never the reading of the day it was saved.
 */
function tripDayGauge(spot: Spot): string {
  const df = spot.dayFlow;
  if (df?.state === "filled") {
    const pct = df.percentile;             // the day's own ranking (a spot record, not a donor's state)
    return `${figure(df.discharge, "discharge")} ${UNIT.discharge}${pct === null ? "" : ` · ${percentileLabel(pct)}`}`;
  }
  if (df?.state === "none") return "no flow that day";
  return spot.reading?.station || df ? "flow that day not read yet" : "no gauge";
}

/**
 * The two lines under a spot's name in the list: the LOCAL day of the visit with what was recorded, then where.
 *
 * The visit, not the record: spots are typed up on the drive home or the week after, and the list used to date
 * them by `createdAt` in UTC — a Sunday evening's fishing read as Monday's.
 */
export function spotRowLines(spot: Spot): [string, string] {
  const g = spot.reading, w = spot.weather, f = spotReadingFigure(g);
  const gauge = isBackdated(spot) && !readingIsTheDays(spot) ? tripDayGauge(spot)
    : g && f
    ? `${f.figure} ${f.unit}${g.percentile != null ? ` · ${percentileLabel(g.percentile)}` : ""}`
    : "no gauge";
  const weather = w?.tempC != null ? `${w.tempC}°C${w.backfilled ? " (filled in)" : ""}` : "no weather";
  return [`${localDate(spot.visitedAt ?? spot.createdAt)} · ${gauge} · ${weather}`,
          `${spot.lat.toFixed(4)}, ${spot.lon.toFixed(4)}`];
}
