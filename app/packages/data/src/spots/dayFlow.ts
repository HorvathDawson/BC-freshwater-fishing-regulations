/**
 * THE VISIT DAY'S FLOW, FROM THE DAY'S RECORD (T170).
 *
 * owner 2026-10-23: "spots should be able to get historical flow on a day with a refresh and have option to get it";
 * "we could add in a way to access hydat for specific date measures when online?". A spot saved later for an earlier
 * day froze TODAY's gauge reading (capture reads the live feed whatever the visit time), and one pinned offline froze
 * none. This fills the visit day's DAILY MEAN instead, from the station's record of that day:
 *
 *   1. the feed's own daily series (`RegsSource.gaugeSeries(station, "year", "discharge")`): this year's daily means as
 *      ECCC published them (provisional), and the last complete HYDAT years the feed carries (`priorYears`);
 *   2. older or missing, WHEN ONLINE: ONE request to ECCC's MSC GeoMet OGC API, collection `hydrometric-daily-mean`
 *      (HYDAT's published daily means; Open Government Licence – Canada), for the first record on or after the day
 *      (`GEOMET_DAILY_MEAN`). Verified 2026-10-23: CORS `Access-Control-Allow-Origin: *`, fields `DATE`, `DISCHARGE`,
 *      `LEVEL` (nulls where the record is blank), and the station's record ends months before today.
 *
 * Ranked against the SAME envelope the app ranks every reading with (the series' band for that day of the year, core
 * `percentileInBand` — the feed's `percentile_of`). DISCHARGE ONLY: a station that measures no flow, or a day whose
 * record holds a level and no flow, gives NO flow — a level never stands in (decisions.md "NEVER MIX QUANTITIES").
 *
 * ONE READ: `flowOnDay` is the only function that asks either source for a past day's flow; the refresh and the
 * spot's "Get the flow for that day" both go through `fillDayFlow`, which asks it. A reading taken ON the visit day is
 * never replaced (`readingIsTheDays`).
 */
import { localDate, percentileInBand, placeInBand } from "@app/core";
import type { Panel, PanelMember, RegsSource, StationId } from "../index";
import { answerFrom, type PanelAnswer } from "../panel";
import { dayIn } from "./leadup";
import { stripRoutes } from "./migrate";
import type { DayFlow, Spot } from "./model";

/** ECCC GeoMet's HYDAT daily means (OGC API – Features). */
export const GEOMET_DAILY_MEAN = "https://api.weather.gc.ca/collections/hydrometric-daily-mean/items";

/** What HYDAT says about a station on a day: its daily mean (either value may be blank); a later record only (no
 *  record that day); or nothing on or after it (not published yet). */
export type HydatDay =
  | { kind: "day"; discharge: number | null; level: number | null }
  | { kind: "later"; next: string }
  | { kind: "none" };

/** HYDAT's daily mean for one station on one day. Rejects when the service cannot be reached. */
export interface HydatDaily { day(station: string, date: string): Promise<HydatDay> }

/** GeoMet serves HYDAT's single-precision values as doubles (19.600000381469727): kept to 4 decimals, what HYDAT holds. */
const num = (v: unknown): number | null =>
  (typeof v === "number" && Number.isFinite(v) ? Math.round(v * 1e4) / 1e4 : null);

/**
 * The GeoMet port: ONE GET — the station's first daily mean ON OR AFTER the day (`datetime=<day>/..`, ascending, one
 * feature). Its date is the day: that day's values. A later date: the record has no row for the day. None: nothing
 * published that far.
 */
export function geometHydat(fetchFn: typeof fetch = fetch): HydatDaily {
  return {
    async day(station, date) {
      const q = new URLSearchParams({ f: "json", STATION_NUMBER: station, datetime: `${date}/..`, sortby: "DATE",
                                      limit: "1" });
      const res = await fetchFn(`${GEOMET_DAILY_MEAN}?${q.toString()}`);
      if (!res.ok) throw new Error(`HYDAT daily mean: HTTP ${res.status}`);
      const body = (await res.json()) as { features?: { properties?: Record<string, unknown> }[] };
      const p = body.features?.[0]?.properties;
      if (!p) return { kind: "none" };
      const day = typeof p.DATE === "string" ? p.DATE.slice(0, 10) : null;
      if (day !== date) return day ? { kind: "later", next: day } : { kind: "none" };
      return { kind: "day", discharge: num(p.DISCHARGE), level: num(p.LEVEL) };
    },
  };
}

/** The answer for one station on one day. */
export type DayFlowAnswer =
  | { kind: "flow"; discharge: number; percentile: number | null; place: ReturnType<typeof placeInBand>;
      source: "feed" | "hydat" }
  | { kind: "no-discharge" }      // the station, or that day's record, holds no flow
  | { kind: "no-record" }         // HYDAT has the station past that day and nothing on it
  | { kind: "not-published" }     // past the feed's record and not in HYDAT yet
  | { kind: "pending" }           // the day is not over in the feed yet
  | { kind: "offline" };          // a source it needed could not be reached: asked again later

export interface DayFlowPorts {
  source: RegsSource;
  /** HYDAT through GeoMet; absent: older days wait (said as offline). */
  hydat?: HydatDaily;
  /** The device's word on being online; `true` where it cannot say. */
  online?: () => boolean;
}

/** THE ONE READ (T170): a station's discharge on a day — the feed's record first, then HYDAT when online. */
export async function flowOnDay(station: string, date: string, ports: DayFlowPorts): Promise<DayFlowAnswer> {
  const id = station as StationId;
  const { source } = ports;
  let params: readonly string[];
  try { params = await source.gaugeParameters(id); } catch { return { kind: "offline" }; }
  if (!params.includes("discharge")) return { kind: "no-discharge" };
  const got = await source.gaugeSeries(id, "year", "discharge").catch(() => null);
  const series = got?.value ?? null;
  const day = dayIn(series, date);                     // the envelope for the day, whether or not the feed was read
  const ranked = (v: number, from: "feed" | "hydat"): DayFlowAnswer =>
    ({ kind: "flow", discharge: v, percentile: percentileInBand(v, day.band), place: placeInBand(v, day.band),
       source: from });
  if (got && got.fetchedAt !== null && series) {
    // a day on or after the day the feed was written is not over in its record (S1's rule): never freeze a morning
    if (date >= localDate(got.fetchedAt)) return { kind: "pending" };
    if (day.v !== null) return ranked(day.v, date.slice(0, 4) === series.from.slice(0, 4) ? "feed" : "hydat");
  }
  if (!ports.hydat || (ports.online && !ports.online())) return { kind: "offline" };
  let h: HydatDay;
  try { h = await ports.hydat.day(station, date); } catch { return { kind: "offline" }; }
  if (h.kind === "none") return { kind: "not-published" };
  if (h.kind === "later") return { kind: "no-record" };
  if (h.discharge !== null) return ranked(h.discharge, "hydat");
  return h.level !== null ? { kind: "no-discharge" } : { kind: "no-record" };
}

/** The station a spot's flow is from: the one it recorded (a spot keeps no section, AGENTS 5). */
export const dayFlowStation = (spot: Spot): string | null =>
  spot.reading?.station ?? spot.leadUp?.station ?? spot.trace?.station ?? null;

/** The visit's own local day. */
export const visitDay = (spot: Pick<Spot, "visitedAt">): string => localDate(spot.visitedAt);

/**
 * Was the spot's reading taken ON the visit day? Its own observation time (else, unreadable, when the spot was saved)
 * on the visit's local day, with a value in it. Such a reading is the day's record and is never replaced.
 */
export function readingIsTheDays(spot: Spot): boolean {
  const r = spot.reading;
  if (!r || (r.discharge === null && r.level === null)) return false;
  const t = r.at ? Date.parse(r.at) : NaN;
  return localDate(Number.isFinite(t) ? t : spot.createdAt) === visitDay(spot);
}

/** A spot whose visit day's flow has never been asked for, and should be: it has a station and no reading of the day. */
export function needsDayFlow(spot: Spot): boolean {
  return !!dayFlowStation(spot) && !readingIsTheDays(spot) && !spot.dayFlow;
}

/** Worth asking (again): never asked, or asked before HYDAT had the day. A settled answer is not asked again. */
export function dayFlowAskable(spot: Spot): boolean {
  return !!dayFlowStation(spot) && !readingIsTheDays(spot)
    && (!spot.dayFlow || spot.dayFlow.state === "not-published");
}

/**
 * Ask for the visit day's flow and lay the answer on the spot: the same spot back when nothing is learned (offline,
 * the day not over) or nothing may be asked (a reading of the day, no station). `now` stamps the record.
 */
export async function fillDayFlow(spot: Spot, ports: DayFlowPorts, now: number = Date.now()):
  Promise<{ spot: Spot; answer: DayFlowAnswer | null }> {
  const station = dayFlowStation(spot);
  if (!station || !dayFlowAskable(spot)) return { spot, answer: null };
  const date = visitDay(spot);
  const answer = await flowOnDay(station, date, ports);
  const rec: DayFlow | null =
    answer.kind === "flow" ? { state: "filled", station, date, discharge: answer.discharge,
                               percentile: answer.percentile, place: answer.place, source: answer.source,
                               filledAt: now }
    : answer.kind === "no-discharge" || answer.kind === "no-record"
      ? { state: "none", station, date, why: answer.kind, askedAt: now }
    : answer.kind === "not-published" ? { state: "not-published", station, date, askedAt: now }
    : null;
  return { spot: rec ? { ...spot, dayFlow: rec, updatedAt: now } : spot, answer };
}

/* ---- Q161: the TRIP DAY's gauge panel ---------------------------------------------------------------------------- */
/*
 * owner 2026-10-24: "it should save the date that you set as the time of trip in the add a spot". A spot added for an
 * earlier day froze the panel as the gauges read when it was SAVED — labelled so (T170), but still not the trip's. Now
 * the panel of a spot whose trip day is earlier than its save day is the section's panel (its member set as now) with
 * each member's DAILY MEAN that day, through the one read (`flowOnDay`: discharge only, ranked with core
 * `percentileInBand`), and answered by the same `answerFrom` the map and the sheet use. A spot added the day of the
 * trip is read live, as always. Nothing here reads a save-day reading for an earlier trip day.
 */

/** The trip's local day is before the day the spot was saved. */
export const isBackdated = (spot: Pick<Spot, "visitedAt" | "createdAt">): boolean =>
  visitDay(spot) < localDate(spot.createdAt);

/** A day's answers as the index `answerFrom` reads: each member with a ranked flow that day, nothing for the rest. */
function dayIndex(members: readonly PanelMember[], answers: readonly DayFlowAnswer[]): Parameters<typeof answerFrom>[1] {
  const stations: Record<string, { percentile: number; discharge: number; parameter: "discharge" }> = {};
  members.forEach((m, i) => {
    const a = answers[i];
    // a day's answer, not a donor's state (that is core's `donorState`, over the rows `answerFrom` makes of this)
    const p = a?.kind === "flow" ? a.percentile : null;
    if (p !== null) stations[m.station] = { percentile: p, discharge: p, parameter: "discharge" };
  });
  return { stations };
}

/**
 * THE PANEL ON A DAY: every member's flow that day (`flowOnDay`), then `answerFrom` over them. `pending` when any member
 * could not be read yet (offline, or the day not over in its record): a panel half made of another day is never
 * frozen. A member with no flow that day (level only, no record, not published) is silent in it, as a quiet gauge is.
 */
export async function panelOnDay(panel: Panel, date: string, ports: DayFlowPorts):
  Promise<{ state: "day"; answer: PanelAnswer } | { state: "pending" }> {
  const answers = await Promise.all(panel.members.map((m) => flowOnDay(m.station, date, ports)));
  if (answers.some((a) => a.kind === "offline" || a.kind === "pending")) return { state: "pending" };
  return { state: "day", answer: stripRoutes(answerFrom(panel, dayIndex(panel.members, answers), "discharge")) };
}

/**
 * A panel back out of a frozen answer — for a spot saved before Q161 (its section is not kept, AGENTS 5): each row's
 * station, side, catchment, record and the two flags. `lead` is not in a row; the member whose frozen share was raised
 * above its plain one (core `leadShares`) led, and keeps leading.
 */
export function panelOfAnswer(p: PanelAnswer): Panel {
  // the members the frozen answer counted (weight over 0): a quiet or stood-back one weighed nothing
  const plain = p.rows.map((r) => (!(r.weight > 0) || !r.factors ? 0
    : r.factors.share * r.factors.role * r.factors.record));
  const total = plain.reduce((a, w) => a + w, 0);
  const led = total > 0 ? p.rows.findIndex((r, i) => r.weight > plain[i]! / total + 1e-9) : -1;
  return { areaKm2: p.areaKm2,
           members: p.rows.map((r, i) => ({ station: r.station, role: r.role, areaKm2: r.areaKm2,
                                            // the frozen answer is a flow panel: its rows carry the discharge
                                            // record, and the day's panel is read in discharge only
                                            years: { discharge: r.years, level: 0 }, regulated: r.regulated,
                                            sameRiver: r.sameRiver, ...(i === led ? { lead: 1 } : {}) })) };
}

/**
 * The trip day's panel for a spot being added for an earlier day: `{panel, panelDay}` for the record. No panel for the
 * section: nothing to freeze. A member that could not be read: `pending`, with the members to read again.
 */
export async function capturePanelOnDay(panel: Panel | undefined, date: string, ports: DayFlowPorts):
  Promise<Pick<Spot, "panel" | "panelDay">> {
  if (!panel) return { panel: null };
  const got = await panelOnDay(panel, date, ports);
  return got.state === "day" ? { panel: got.answer, panelDay: { state: "day", date } }
    : { panel: null, panelDay: { state: "pending", date, members: panel } };
}

/**
 * A spot whose panel is not of its trip day, and could be: a backdated spot with its panel still to read (`pending`),
 * or one saved before Q161 holding a panel read when it was saved. Never a spot whose reading is the trip day's own.
 */
export function needsDayPanel(spot: Spot): boolean {
  if (!isBackdated(spot) || readingIsTheDays(spot)) return false;
  return spot.panelDay ? spot.panelDay.state === "pending" : !!spot.panel;
}

/** Read the trip day's panel onto a spot that needs it (`needsDayPanel`); the same spot back when nothing is learned. */
export async function fillDayPanel(spot: Spot, ports: DayFlowPorts, now: number = Date.now()): Promise<Spot> {
  if (!needsDayPanel(spot)) return spot;
  const date = visitDay(spot);
  const members = spot.panelDay?.state === "pending" ? spot.panelDay.members : panelOfAnswer(spot.panel!);
  const got = await panelOnDay(members, date, ports);
  if (got.state === "day") return { ...spot, panel: got.answer, panelDay: { state: "day", date }, updatedAt: now };
  // still not readable: a spot saved before Q161 is marked pending (its save-day panel is never shown for the trip)
  return spot.panelDay ? spot : { ...spot, panelDay: { state: "pending", date, members }, updatedAt: now };
}

/**
 * WHAT "What the water was doing" SHOWS for a saved spot (decided here, drawn by `SpotScreen`):
 *   live     saved on its trip day (or its reading is the trip day's own): its panel and reading as read then;
 *   day      a backdated spot whose panel is the trip day's — each gauge's daily mean that day;
 *   pending  a backdated spot whose trip-day panel is still to read: its save-day panel and reading are not shown;
 *   flow     a backdated spot with no panel: the gauge's own flow that day is under "Flow that day".
 */
export function savedGauges(spot: Spot): "live" | "day" | "pending" | "flow" {
  if (!isBackdated(spot) || readingIsTheDays(spot)) return "live";
  if (spot.panelDay?.state === "day" && spot.panel) return "day";
  if (spot.panelDay?.state === "pending" || spot.panel) return "pending";
  return "flow";
}
