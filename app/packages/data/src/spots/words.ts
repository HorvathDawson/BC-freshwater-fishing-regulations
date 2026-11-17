/**
 * The sentences a spot is shown with (P12b).
 *
 * They were composed inside `SpotScreen` and `SpotsScreen`, where a test could only match a fragment it had itself
 * supplied (AGENTS 33). Here each one is a whole string a test pins, and the screens draw it.
 */
import { bandWord, bcMidnight, CONDITIONS_WORDS, spotDay, standing, traceSentence, type FrozenTrace, latLon, weatherFigure, figure, daysInTen, placeChip, placeSaid,
         readingIn, shownParameter, spotIsoDay, spotStamp, type DeclaresQuantity, type ShownQuantity } from "@app/core";
import { spotLabel, type DailyGauge, type DailyWeather, type DayPlace, type Spot, type SpotReading, type WeatherWindow } from "./model";
import { OPEN_METEO_ATTRIBUTION } from "./weather";
import { isBackdated, readingIsTheDays } from "./dayFlow";

/* ---- S1: the week before the visit ------------------------------------------------------------------------------ */

const DASH = "—";
/** One decimal where it means something: "4.2", "0", "12". */
const tenth = (v: number): string => String(Number(v.toFixed(1)));

/** "Sun 23 Aug" — a lead-up day, in the spots' one style (T819 item 10); the week is the visit's, so no year. */
export function leadUpDayLabel(date: string): string {
  return spotIsoDay(date, bcMidnight(date));
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

/** (T284) The sky as a picture: which of the icon set's sky drawings a day's WMO code takes; null for none. */
export type SkyIcon = "sun" | "cloudSun" | "cloud" | "cloudRain" | "cloudDrizzle" | "cloudSnow" | "cloudFog" | "cloudStorm";
export function skyIcon(code: number | null): SkyIcon | null {
  if (code === null) return null;
  if (code === 0) return "sun";
  if (code === 1 || code === 2) return "cloudSun";
  if (code === 3) return "cloud";
  if (code === 45 || code === 48) return "cloudFog";
  if (code >= 51 && code <= 57) return "cloudDrizzle";
  if ((code >= 61 && code <= 67) || (code >= 80 && code <= 82)) return "cloudRain";
  if ((code >= 71 && code <= 77) || code === 85 || code === 86) return "cloudSnow";
  if (code >= 95 && code <= 99) return "cloudStorm";
  return null;
}

/** (T284) How wet a day was, 0 (none or not recorded) to 3, for the table's tint; the number is always printed too. */
export function rainBand(mm: number | null): 0 | 1 | 2 | 3 {
  return mm === null || mm < 0.2 ? 0 : mm < 2 ? 1 : mm < 8 ? 2 : 3;
}

/**
 * A lead-up weather day as the table draws it (T819 item 8: seven columns at phone width, no sideways scroll): the day
 * as its weekday over its date, the high over the low, the rain and the wind as bare numbers — their units said once in
 * the row's label (`LEAD_UP_WORDS.rows`). A value nobody recorded is a dash.
 */
export function leadUpWeatherCells(d: DailyWeather): { day: string; weekday: string; date: string; sky: string;
                                                        high: string; low: string; rain: string; wind: string;
                                                        icon: SkyIcon | null; band: 0 | 1 | 2 | 3 } {
  const deg = (v: number | null) => (v === null ? DASH : `${Math.round(v)}°`);
  const [weekday = "", date = "", month = ""] = leadUpDayLabel(d.date).split(" ");
  return {
    day: leadUpDayLabel(d.date), weekday, date: `${date} ${month}`.trim(), sky: skyWord(d.code),
    high: deg(d.maxC), low: deg(d.minC),
    rain: d.precipMm === null ? DASH : tenth(d.precipMm),
    wind: d.windMaxKph === null ? DASH : String(Math.round(d.windMaxKph)),
    icon: skyIcon(d.code), band: rainBand(d.precipMm),
  };
}

/** The day said whole: "Sun Aug 23: rain, high 18°C, low 9°C, 4.2 mm of rain, wind up to 22 km/h". */
export function leadUpWeatherSentence(d: DailyWeather): string {
  const sky = skyWord(d.code);
  const bits = [
    sky === DASH ? null : sky.toLowerCase(),
    d.maxC === null ? null : `high ${Math.round(d.maxC)}°C`,
    d.minC === null ? null : `low ${Math.round(d.minC)}°C`,
    d.precipMm === null ? null : d.precipMm === 0 ? "no rain" : `${tenth(d.precipMm)} mm of rain`,
    d.windMaxKph === null ? null : `wind up to ${Math.round(d.windMaxKph)} km/h`,
  ].filter((b): b is string => b !== null);
  // a day with nothing at all is one the service had not sent yet; a refresh asks for it again (`weatherDayPending`)
  return `${leadUpDayLabel(d.date)}: ${bits.length ? bits.join(", ") : "no weather for this day yet"}`;
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
const valueOf = (d: DailyGauge, q: "discharge" | "level") => (q === "level" ? d.level : d.discharge);
const placeOf = (d: DailyGauge, q: "discharge" | "level") => (q === "level" ? d.levelPlace : d.dischargePlace);

/**
 * "6 in 10 days lower", "9+ in 10 days higher", "9+ in 10 days lower", or nothing — the chip under a day's value. AT OR past the edge (T107): the ranking is the
 * feed's `percentile_of`, which clamps a value ON p10 / p90 too, so "<p10" would be false for a dry creek at 0 on p10 0.
 */
const chipOf = (p: DayPlace | null): string =>
  p === null ? "" : placeChip(p);

/** A gauge day the feed had not published yet (no mean, not settled as none): a refresh asks for it again. */
const stillToCome = (d: DailyGauge) => d.discharge === null && d.level === null && !d.none;

/** A lead-up gauge day as the strip draws it; a day still to come says so under its dash. */
export function leadUpGaugeCells(d: DailyGauge, q: "discharge" | "level"): { value: string; place: string } {
  const v = valueOf(d, q);
  return { value: v === null ? DASH : figure(v),
           place: v === null ? (stillToCome(d) ? "not yet" : "") : chipOf(placeOf(d, q)) };
}

/** The day said whole: "Sun Aug 23: 50.0 m³/s, lower than 4 days in 10 for the day". */
export function leadUpGaugeSentence(d: DailyGauge, q: "discharge" | "level"): string {
  const day = leadUpDayLabel(d.date);
  const v = valueOf(d, q);
  if (v === null && stillToCome(d)) return `${day}: no daily mean published yet`;
  if (v === null) return `${day}: no daily mean recorded`;
  const at = placeOf(d, q);
  const where = at === null ? "no record to compare it with" : placeSaid(at, "day");
  return `${day}: ${figure(v)} ${UNIT[q]}, ${where}`;
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
  return `${q === "level" ? "Level" : "Flow"} ${verb} over the week: ${figure(a)} → ${figure(b)} ${UNIT[q]}`;
}

/** Under the gauge row: whose means they are (named, T819), and whether they were filled in afterwards. */
export function leadUpGaugeMeta(station: string, backfilled?: boolean, names?: ReadonlyMap<string, string>): string {
  return [gaugeLabel(station, names), "daily means", backfilled ? "filled in afterwards" : null].filter(Boolean).join(" · ");
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
  /** (T284) the weather table's row labels, said once at the left; (T819 item 8) each with its unit, said once */
  rows: { day: "Day", sky: "Sky", temp: "High / low °C", rain: "Rain mm", wind: "Wind km/h" },
  /** The refresh line: a whole half of the week was never read (offline, or saved before the week was kept). */
  notRecorded: "The week before this visit was not recorded — it can be filled in from the days themselves.",
  /** The refresh line: the week was read, but some of its days were not out yet (owner bug 2026-10-22). */
  someDaysPending: "Some days of the week before were not available yet when this was saved — they'll fill in once published.",
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

/** "Thu 15 Aug 2024" — a record's day, in the spots' one style (T819 item 10), with its year: it is a dated record. */
const fullDay = (date: string): string => `${spotIsoDay(date, bcMidnight(date))} ${date.slice(0, 4)}`;

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
  /** (T368) a real failure (not offline): said, reported, asked again by the action */
  failed: "The flow for that day could not be read.",
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
  const where = p === null ? "no record to rank it against" : placeSaid(p, "date");
  return `${figure(df.discharge)} ${UNIT.discharge}, ${where} — filled in from the day's record `
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
  /** (T284) the full-screen viewer */
  close: "Close photo", previous: "Previous photo", next: "Next photo",
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

/** (T284) A photograph in the grid, named for a screen reader: "Photo 2 of 3". */
export function photoLabel(n: number, of: number): string {
  return `Photo ${n} of ${of}`;
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

/**
 * (T620 G3) "observed 2026-08-30 at 09:00 PT": the instant in B.C.'s clock, named, like every other time the app shows.
 * A reading's time is an instant (gauge: "…Z"); the weather source's hour is GMT with no zone written ("2026-08-30T16:00").
 * A string with no zone is read as UTC, never cut up as if it were already the reader's wall clock.
 */
function observedAt(at: string | null): string {
  const ms = at ? Date.parse(/(Z|[+-]\d\d:?\d\d)$/.test(at) ? at : `${at}Z`) : NaN;
  return Number.isNaN(ms) ? "no observation time recorded" : `observed ${spotStamp(ms)}`;
}

/**
 * (T819 item 7) A station said by NAME, the code after it: "Chilliwack River at Vedder Crossing (08MH001)"; the code
 * alone where the release names none (or before the names are read). `names` is the release's code → name.
 */
export function gaugeLabel(station: string, names?: ReadonlyMap<string, string>): string {
  const name = names?.get(station)?.trim();
  return name ? `${name} (${station})` : station;
}

/** When a frozen value was observed, by whom (named, T819), and whether it was filled in after the fact. */
export function observedLine(at: string | null, backfilled?: boolean, station?: string | null,
                             names?: ReadonlyMap<string, string>): string {
  return [
    station ? gaugeLabel(station, names) : null,
    observedAt(at),
    backfilled ? "filled in afterwards" : null,
  ].filter(Boolean).join(" · ");
}

/**
 * "Sun 30 Aug, 9 am" — the visit, in the spots' one style (T819 item 10). No coordinates: they are said once, under
 * Show on map (`spotPlaceLine`), not in the header.
 */
export function spotHeadline(spot: Pick<Spot, "visitedAt" | "createdAt">): string {
  return spotStamp(spot.visitedAt ?? spot.createdAt);
}

/** (T819 item 10) Where the spot is, as numbers, under Show on map: "49.0974, -121.9675". */
export const spotPlaceLine = (spot: Pick<Spot, "lat" | "lon">): string => latLon(spot);

/** (T819 item 10) "on Chilliwack River" under the title, or null when it would only say the title again. */
export function spotWaterLine(spot: Pick<Spot, "title" | "waterName" | "lat" | "lon">, label: string): string | null {
  const w = spot.waterName?.trim();
  return !w || w.toLowerCase() === label.trim().toLowerCase() ? null : `on ${w}`;
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
    return `${figure(df.discharge)} ${UNIT.discharge}${pct === null ? "" : ` · ${daysInTen(pct)}`}`;
  }
  if (df?.state === "none") return "no flow that day";
  return spot.reading?.station || df ? "flow that day not read yet" : "no gauge";
}

/** The list row's and the draft summary's flow part: the reading as it stood that day, or that there was none. */
function gaugePart(spot: Spot): string {
  const g = spot.reading, f = spotReadingFigure(g);
  return isBackdated(spot) && !readingIsTheDays(spot) ? tripDayGauge(spot)
    : g && f
    ? `${f.figure} ${f.unit}${g.percentile != null ? ` · ${daysInTen(g.percentile)}` : ""}`
    : "no gauge";
}

/** The list row's and the draft summary's weather part: the temperature recorded, or that there was none. */
function weatherPart(spot: Spot): string {
  const w = spot.weather;
  return w?.tempC != null ? `${weatherFigure("temp", w.tempC)}${w.backfilled ? " (filled in)" : ""}` : "no weather";
}

/**
 * (T819 item 11) A SPOT'S ROW IN THE LIST, under its name: the water and the day of the visit, then ONE condition word —
 * where the flow stood that day ("Below normal"), the bare figure where nothing ranked it, or why there is none. The day
 * is the visit's, in the spots' one style (P12b: never the UTC day it was typed up). No coordinates, no weather: the
 * spot's page has them.
 */
export function spotRowLines(spot: Spot, ref?: number): [string, string] {
  const day = spotDay(spot.visitedAt ?? spot.createdAt, ref);
  const water = spot.waterName?.trim();
  const where = water && water.toLowerCase() !== spotLabel(spot).toLowerCase() ? `${water} · ${day}` : day;
  return [where, conditionWord(spot)];
}

/** The row's one condition word (T819 item 11), from the same record `gaugePart` reads. */
function conditionWord(spot: Spot): string {
  if (isBackdated(spot) && !readingIsTheDays(spot)) {
    const df = spot.dayFlow;
    if (df?.state === "filled")
      return df.percentile === null ? `${figure(df.discharge)} ${UNIT.discharge}` : bandWord(standing(df.percentile));
    if (df?.state === "none") return ROW_WORDS.noFlowThatDay;
    return spot.reading?.station || df ? ROW_WORDS.notReadYet : ROW_WORDS.noGauge;
  }
  const g = spot.reading, f = spotReadingFigure(g);
  if (!g || !f) return ROW_WORDS.noGauge;
  return g.percentile != null ? bandWord(standing(g.percentile)) : `${f.figure} ${f.unit}`;
}

/** The list row's words for a spot with no ranked flow (T819 item 11). */
const ROW_WORDS = { noGauge: "No gauge", noFlowThatDay: "No flow that day", notReadYet: "Flow that day not read yet" } as const;

/**
 * (T819 item 5) THE DRAFT'S RECORD IN ONE LINE, on the folded card that opens to the whole record: what the water and
 * the sky were doing, "28.2 m³/s · lower than 8 days in 10 · 6°C".
 */
export function spotRecordSummary(spot: Spot): string {
  return `${gaugePart(spot)} · ${weatherPart(spot)}`;
}

/**
 * (T819 items 3, 6, 7) HOW THE SPOT REACHES ITS GAUGE, in one plain sentence — "Water here flows about 17 km down to
 * Chilliwack River at Vedder Crossing (08MH001)." — the trace's own sentence (core `traceSentence`, the Conditions
 * sheet's), the station named. Null when the trace names no station: nothing known is nothing said.
 */
export function spotTraceLine(t: FrozenTrace | null, names?: ReadonlyMap<string, string>): string | null {
  if (!t || t.station === null) return null;
  return traceSentence(t, t.stationName ? `${t.stationName} (${t.station})` : gaugeLabel(t.station, names));
}
