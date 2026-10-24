/**
 * A spot: what was true at one point on one day.
 *
 * The distinction that shapes everything else here — a spot is a RECORD, not a view. The
 * gauge reading and the weather are copied in at capture time and never recomputed.
 * Re-deriving them later would quietly rewrite history: the note "took two on a bead" would
 * sit under the wrong conditions forever.
 *
 * It is also the only user-authored data in the app, and the only thing that cannot be
 * re-downloaded. `pins.ts` says why it lives in its own store.
 */
import type { FrozenTrace, ShownQuantity } from "@app/core";
import type { PanelAnswer } from "../panel";
import type { ItemId, Panel } from "../index";

/** Weather at the moment of capture. Every field nullable: a spot with no forecast is fine. */
/**
 * One hour's readings.
 *
 * Deliberately the fields that CHANGE over three hours and mean something to a person
 * standing in a river. Wind direction is not here: it belongs to the visit, not to the
 * trend, and three compass bearings in a row read as noise.
 */
export interface WeatherSample {
  /** Hour label, e.g. "2026-08-30T08:00". Null only if the source omitted it. */
  at: string | null;
  tempC: number | null;
  cloudPct: number | null;
  pressureHpa: number | null;
  humidityPct: number | null;
}

export interface WeatherWindow {
  before: WeatherSample | null;
  at: WeatherSample | null;
  after: WeatherSample | null;
}

export interface SpotWeather {
  tempC: number | null;
  windKph: number | null;
  windDir: number | null;
  /** Millimetres in the previous three hours. */
  rain3h: number | null;
  pressureHpa: number | null;
  /** WMO code, so the renderer picks the word and the icon, not the fetcher. */
  code: number | null;
  /**
   * The three hours around the visit — an hour before, the hour itself, an hour after.
   *
   * A WINDOW, NOT A VALUE, because the level answers the wrong question. What matters is
   * the CHANGE: a falling barometer with the cloud thickening is a different afternoon from
   * a rising one with the same 60% overhead, and a single reading at 09:00 cannot tell them
   * apart. Fish move on the change.
   *
   * Null when no window could be fetched. Any individual sample may also be null — the
   * first hour the archive returns for a day has no predecessor — and a null sample must
   * render as unknown, never as its neighbour's value.
   */
  window: WeatherWindow | null;
  /** When this was observed. Null means it was never fetched — NOT "now". */
  at: string | null;
  /**
   * True when the reading was filled in after the fact — pinned offline, backfilled once
   * there was a signal. A backdated value is still honest; pretending it was live is not.
   */
  backfilled?: boolean;
}

/** The gauge reading as it stood that day. */
export interface SpotReading {
  station: string | null;
  discharge: number | null;
  level: number | null;
  /**
   * Which quantity the reading shows and `percentile` is about, as the feed declared it (T106). Absent on a spot
   * saved before then: core's `shownParameter` reads that as discharge, which is what such a spot always showed.
   */
  parameter?: ShownQuantity | null;
  /** Where that sat against this station's own record. */
  percentile: number | null;
  at: string | null;
  backfilled?: boolean;
}

/**
 * One day of the week before a visit, from Open-Meteo's DAILY series (S1).
 *
 * Every field nullable, and a day the service did not return is a day of nulls — never its neighbour's values. A day
 * with EVERY field null is one the service did not have yet (Open-Meteo has every past day in the end): a refresh asks
 * for it again (`leadUpPendingDays`). A day with any value is kept as captured, for ever.
 */
export interface DailyWeather {
  /** The LOCAL day, "2026-08-29". */
  date: string;
  maxC: number | null;
  minC: number | null;
  precipMm: number | null;
  /** The day's strongest 10 m wind, km/h. */
  windMaxKph: number | null;
  /** Open-Meteo's daily WMO code: the most significant weather of the day. The words pick from it. */
  code: number | null;
}

/** Where a day's mean sat in the station's envelope: a percentile (0–1), or which side of the envelope it fell off. */
export type DayPlace = number | "below" | "above";

/**
 * One day of the matched station's daily means (S1). A day the feed has no mean for stays null — and says which null:
 *
 *   `none: true`  the feed had been published past the day and held nothing for it, or does not carry the station at
 *                 all: asked, and nothing there. Settled; never asked again.
 *   no `none`     the day was not over, or not published yet, when the feed was written: still to come. A refresh asks
 *                 for it again — and so for a week saved before `none` existed (owner bug 2026-10-22).
 */
export interface DailyGauge {
  date: string;
  /** m³/s */
  discharge: number | null;
  /** m */
  level: number | null;
  /** Against the bundle's envelope for that day of the year; null where there is no envelope or no value. */
  dischargePlace: DayPlace | null;
  levelPlace: DayPlace | null;
  /** Only on a day with no mean that will not get one (above). */
  none?: true;
}

/**
 * THE WEEK BEFORE THE VISIT, frozen with the rest of the spot (S1).
 *
 * The visit's own hour says what the sky and the river were doing; a week says how they GOT there — three dry days
 * and a falling river is a different afternoon from the same reading the day after a storm.
 */
export interface LeadUp {
  /** Seven days, oldest first. Null when the weather could not be fetched (offline): a refresh retries it. */
  weather: DailyWeather[] | null;
  /** The station the gauge days are from; null when no gauge speaks for the water (nothing to retry). */
  station: string | null;
  /** Seven days, oldest first; null when the station's days could not be read (a refresh retries it). */
  gauge: DailyGauge[] | null;
  /** Filled in after the visit was saved. */
  backfilled?: boolean;
  /**
   * WHICH half a refresh filled in, so the strip says "filled in afterwards" only under the row it is true of: a
   * refresh that fills the gauge's last three days did not touch the weather (owner bug 2026-10-22). Absent on a
   * week filled before this existed, where `backfilled` speaks for both halves, as it always did.
   */
  filledIn?: { weather?: true; gauge?: true };
  /**
   * Which quantity the station's feed declared when the gauge days were read (T106 review), frozen beside them. The
   * strip shows the spot reading's declared quantity, and this one where the reading declares none: a spot saved
   * offline keeps a reading with nothing in it, and its days are filled in later by a refresh. Absent on a week read
   * before this existed, or when the declaration could not be read.
   */
  parameter?: ShownQuantity | null;
}

/**
 * THE VISIT DAY'S FLOW, FROM THE DAY'S RECORD (T170) — for a spot whose reading was not taken on that day (saved later
 * for an earlier day, or with no reading at all). Filled by a refresh or by "Get the flow for that day", never over a
 * reading taken on the day (`readingIsTheDays`). DISCHARGE ONLY: a level never stands in for a flow.
 *
 *   filled         the day's daily mean, where it came from, and its place in the station's envelope for that day
 *   none           asked, and there is none: the station measures no flow (`no-discharge`), or its record holds nothing
 *                  for that day (`no-record`). Settled; not asked again by a refresh.
 *   not-published  past the feed's record and not yet in HYDAT: asked again by every refresh and on request.
 */
export type DayFlow =
  | { state: "filled"; station: string; date: string;
      /** m³/s, the day's mean. */
      discharge: number;
      /** 0–1 against the station's envelope for that day (core `percentileInBand`, the feed's ranking); null: no envelope. */
      percentile: number | null;
      place: DayPlace | null;
      /** `feed`: ECCC's daily means as the feed keeps them (this year, provisional); `hydat`: HYDAT's published daily mean. */
      source: "feed" | "hydat";
      /** When it was filled (device time). */
      filledAt: number }
  | { state: "none"; station: string; date: string; why: "no-discharge" | "no-record"; askedAt: number }
  | { state: "not-published"; station: string; date: string; askedAt: number };

export interface Spot {
  id: string;
  /** When the record was made. Bookkeeping — not when the person was at the water. */
  createdAt: number;
  updatedAt: number;
  /**
   * When they were actually there, which is the only date the record's contents are about.
   *
   * SEPARATE FROM `createdAt` because they are routinely days apart: spots get entered on
   * the drive home, or the following week from a photograph. Everything frozen onto a spot
   * — the weather, the gauge reading — is as of THIS instant, so conflating the two
   * silently attaches Tuesday's river to a Sunday you fished.
   */
  visitedAt: number;

  /** Always stored. Works for the 97.6% of water with no registry item at all. */
  lat: number;
  lon: number;

  /**
   * The water the user picked: the durable id (99.88% across a rebuild), null for unnamed
   * water, which is the ordinary case.
   *
   * NO SECTION ID (owner decision 2026-10-20, AGENTS 5). A spot is a file a person keeps,
   * and a section id must never leave the bundle: it does not survive a rebuild (94%). A
   * spot is a coordinate plus this id. Spots saved with a `section` lose it on load
   * (`fromDisk` in ./migrate).
   */
  item: ItemId | null;
  /** What the water was called when it was pinned. Kept even if the registry renames it. */
  waterName: string | null;

  /** May be empty. `spotLabel()` decides what to show; never render this raw. */
  title: string;
  notes: string;
  /**
   * RELATIVE references to the spot's photographs, "photos/<spot id>-<n>.<ext>" (S1, `./photos`).
   *
   * Photographs are FILES (a phone) or blobs (IndexedDB on the web); only the reference lives in the spot. Relative,
   * never a `file://` path: an app's document directory moves between installs and OS updates, and a stored absolute
   * path would point at nothing. The user's own data — never in a release, never uploaded.
   */
  photos: string[];

  /** Frozen at capture. Null where the app could not know — never faked. */
  reading: SpotReading | null;
  weather: SpotWeather | null;
  /** The route to the gauge as it stood: its facts and reach count, never its section handles. */
  trace: FrozenTrace | null;
  /**
   * THE ESTIMATE, EXACTLY AS THE APP SHOWED IT — frozen.
   *
   * `reading` above is one station's own number. This is what the reader was actually
   * looking at: the donor panel, its weights, its interval and its words, produced by the
   * same `answerFrom` the map and the sheet run. A spot rendered from a second
   * implementation is a spot that disagrees with the app that recorded it, and this file
   * has already been through that with the trace.
   *
   * Null for a spot saved before this existed, and for water nothing can speak for. The
   * screen shows the reading alone in that case, which is what it always did.
   */
  panel: PanelAnswer | null;
  /**
   * The week before the visit: daily weather and the station's daily means (S1). Absent on a spot saved before this
   * existed, which reads exactly as null — not recorded — and a refresh fills it in.
   */
  leadUp?: LeadUp | null;
  /** The visit day's flow from the day's record (T170); absent until asked, and on a spot whose reading is the day's. */
  dayFlow?: DayFlow | null;
  /**
   * WHICH DAY `panel` IS OF, for a spot whose trip day is earlier than the day it was saved (owner 2026-10-24, Q161:
   * "it should save the date that you set as the time of trip in the add a spot"). Absent: `panel` was read live on
   * the trip day itself (a spot saved the day of the trip, or one saved before this existed — then a backdated one is
   * refilled for its day by a refresh, `needsDayPanel`).
   */
  panelDay?: PanelDay | null;
}

/**
 * THE TRIP DAY'S GAUGE PANEL (Q161). `day`: `panel` is the section's panel with each member's DAILY MEAN that day, from
 * its record (`flowOnDay`, ranked with core `percentileInBand`), never a reading taken on the day the spot was saved.
 * `pending`: some member's day could not be read yet (offline, or the day not over) — `panel` is null and `members` is
 * the panel to read again (station ids and catchments only: a spot keeps no section, AGENTS 5); a refresh fills it.
 */
export type PanelDay =
  | { state: "day"; date: string }
  | { state: "pending"; date: string; members: Panel };

/** Where spots live. One file on a phone, browser storage on the web. */
export interface SpotStore {
  list(): Promise<Spot[]>;
  get(id: string): Promise<Spot | null>;
  put(spot: Spot): Promise<void>;
  remove(id: string): Promise<void>;
}

/**
 * What to call a spot on screen.
 *
 * ONE function, because the list, the detail screen and the map pin must agree — and the
 * fallback chain is a judgement, not a formatting detail: the water it is on is a better
 * name than a coordinate, and a coordinate is a better name than "Untitled", which tells
 * a person nothing and takes up the same room.
 */
export function spotLabel(spot: Pick<Spot, "title" | "waterName" | "lat" | "lon">): string {
  if (spot.title.trim() !== "") return spot.title.trim();
  if (spot.waterName) return spot.waterName;
  return `${spot.lat.toFixed(4)}, ${spot.lon.toFixed(4)}`;
}

/** True when the spot has no name of its own — the prompt to add one is worth showing. */
export const isUntitled = (spot: Pick<Spot, "title">): boolean => spot.title.trim() === "";
