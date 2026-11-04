/**
 * A SPOT'S VISITS (T294): the first visit is the spot's own fields, exactly as a spot has always been stored; the later
 * ones are `spot.visits`. NOTHING IS MOVED ON DISK, so a spot saved before T294 IS a spot of one visit — the migration
 * is lossless by construction, and an older app reading a newer file still finds the first visit where it always was.
 *
 * EVERY VISIT READS AS A SPOT. `visitSpot` lays one visit's record over the spot, so everything written for a spot —
 * the record's words, the week before, the trip day's flow and panel, the refresh, "Like your visit?" — reads any visit
 * unchanged, and `withVisit` writes what it changed back to that visit alone. The spot's own facts (place, water, name)
 * are shared by every visit.
 */
import { fastPorts } from "./fastFail";
import { isRating, leadUpDays, localDate, reported, reportFailure } from "@app/core";
import type { Spot, Visit } from "./model";
import { leadUpFor } from "./leadup";
import { capturePanelOnDay, dayFlowStation, fillDayFlow, livePanel, panelOfAnswer, pendingPanel, type DayFlowPorts } from "./dayFlow";
import type { CaptureInput, WeatherSource } from "./capture";

/** The fields that are one visit's record. Everything else on a spot belongs to the spot. */
export const VISIT_FIELDS = ["visitedAt", "createdAt", "notes", "photos", "reading", "weather", "trace", "panel", "leadUp",
                             "dayFlow", "panelDay", "rating"] as const;

/**
 * A spot read as one of its visits: the spot with that visit's record laid over it, which visit it is, and NO `visits`
 * (it is one visit; the spot's others are on the spot). Never saved as it is: `withVisit` writes it back.
 */
export type VisitSpot = Spot & { visitId: string };

/** Which visit a spot is read as: its `visitId` when it is a `VisitSpot`, else the first. */
export const visitIdOf = (spot: Spot): string => ("visitId" in spot && typeof (spot as VisitSpot).visitId === "string"
  ? (spot as VisitSpot).visitId : firstVisitId(spot));

/** Change one visit's record through a function written for a spot (a fill, a refresh), and write it back. */
export async function onVisit(spot: Spot, id: string, change: (visit: Spot) => Promise<Spot>): Promise<Spot> {
  const before = visitSpot(spot, id);
  const after = await change(before);
  return after === before ? spot : withVisit(spot, before.visitId, after);
}

/** The first visit's id: its own once promoted (`removeVisit`), else the spot's. */
export const firstVisitId = (spot: Spot): string => spot.visitId ?? spot.id;

/** The first visit, as a visit. */
function firstVisit(spot: Spot): Visit {
  const v = { id: firstVisitId(spot), rating: isRating(spot.rating) ? spot.rating : null } as Visit;
  for (const k of VISIT_FIELDS) if (k !== "rating" && k in spot) (v as unknown as Record<string, unknown>)[k] = spot[k];
  return v;
}

/** Every visit, oldest first: the first (the spot's own fields), then `spot.visits` in time order. */
export function spotVisits(spot: Spot): Visit[] {
  return [firstVisit(spot), ...(spot.visits ?? [])].sort((a, b) => a.visitedAt - b.visitedAt);
}

/** The most recent visit's id: the one a spot opens on. */
export const latestVisitId = (spot: Spot): string => {
  const all = spotVisits(spot);
  return all[all.length - 1]!.id;
};

/**
 * One visit read as a spot. An unknown id (a visit removed meanwhile) reads as the most recent. The first visit is the
 * spot itself, `visits` and all; a later one keeps the spot's place and name and lays its own record over the rest.
 */
export function visitSpot(spot: Spot, id: string | null | undefined): VisitSpot {
  const later = spot.visits?.find((v) => v.id === id);
  const { visits: _visits, visitId: _first, ...own } = spot;
  if (!later) {
    if (id !== firstVisitId(spot) && spot.visits?.length) return visitSpot(spot, latestVisitId(spot));
    return { ...own, visitId: firstVisitId(spot) };
  }
  const { id: _id, ...record } = later;
  // a field the later visit does not hold (an optional one never filled) must not show the first visit's value
  const cleared = Object.fromEntries(VISIT_FIELDS.map((k) => [k, undefined]));
  const out = { ...own, ...cleared, ...record, visitId: later.id } as VisitSpot;
  for (const k of VISIT_FIELDS) if (out[k] === undefined) delete out[k];
  return out;
}

/** The visit's record out of a spot read as that visit (what a refresh or an edit changed). */
function recordOf(s: Spot): Omit<Visit, "id"> {
  const r = { rating: isRating(s.rating) ? s.rating : null } as Omit<Visit, "id">;
  for (const k of VISIT_FIELDS) if (k !== "rating" && k in s && s[k] !== undefined) (r as unknown as Record<string, unknown>)[k] = s[k];
  return r;
}

/**
 * Write a visit back: `changed` is the spot read as visit `id` (`visitSpot`) after a change to its record. The first
 * visit's record is the spot's own fields; a later one replaces its entry. The spot's own facts are taken from `spot`.
 */
export function withVisit(spot: Spot, id: string, changed: Spot): Spot {
  const rec = recordOf(changed);
  const i = spot.visits?.findIndex((v) => v.id === id) ?? -1;
  if (i < 0) {
    if (id !== firstVisitId(spot)) return spot;                   // removed meanwhile: never brought back
    const next: Spot = { ...spot };
    for (const k of VISIT_FIELDS) {
      if (k === "rating") continue;
      if (k in rec) (next as unknown as Record<string, unknown>)[k] = (rec as unknown as Record<string, unknown>)[k];
      else delete (next as unknown as Record<string, unknown>)[k];
    }
    if (rec.rating !== null) next.rating = rec.rating; else if ("rating" in spot) next.rating = null;
    return next;
  }
  const visits = [...spot.visits!];
  visits[i] = { id, ...rec };
  return { ...spot, visits };
}

/** A visit's rating changed (null clears it). */
export function rateVisit(spot: Spot, id: string, rating: number | null, now: number = Date.now()): Spot {
  const r = isRating(rating) ? rating : null;
  return { ...withVisit(spot, id, { ...visitSpot(spot, id), rating: r }), updatedAt: now };
}

/** A new visit added (kept oldest first). */
export function addVisit(spot: Spot, visit: Visit, now: number = Date.now()): Spot {
  const visits = [...(spot.visits ?? []), visit].sort((a, b) => a.visitedAt - b.visitedAt);
  return { ...spot, visits, updatedAt: now };
}

/**
 * One visit removed. The spot's LAST visit cannot be: that is deleting the spot. Removing the first visit promotes the
 * next one into the spot's own fields (with its id kept as `visitId`), so the first visit is always the fields.
 */
export function removeVisit(spot: Spot, id: string, now: number = Date.now()): Spot {
  const later = spot.visits ?? [];
  if (!later.length) return spot;
  if (later.some((v) => v.id === id))
    return { ...spot, visits: later.filter((v) => v.id !== id), updatedAt: now };
  if (id !== firstVisitId(spot)) return spot;
  const [next, ...rest] = [...later].sort((a, b) => a.visitedAt - b.visitedAt) as [Visit, ...Visit[]];
  const base: Spot = { ...spot, visits: rest, visitId: next.id };
  if (!rest.length) delete base.visits;
  return { ...withVisit(base, next.id, visitSpot({ ...spot }, next.id)), updatedAt: now };
}


/** The id a visit's new photos are named after: the spot's for the first visit (as always), else the visit's own. */
export const photoOwner = (spot: Spot, visitId: string): string => (visitId === firstVisitId(spot) ? spot.id : visitId);

export interface CaptureVisitInput {
  source: CaptureInput["source"];
  weather?: WeatherSource;
  feed?: CaptureInput["feed"];
  history?: Omit<DayFlowPorts, "source">;
  /** When they were there; defaults to now. */
  visitedAt?: number;
  now?: number;
  id?: () => string;
}

/**
 * A NEW VISIT TO A SAVED SPOT, read the way a spot is captured (`captureSpot`) — but a spot keeps no section (AGENTS 5),
 * so the water is read through what the spot froze: its gauge panel's members (`panelOfAnswer`), its lead station, and
 * its route to the gauge (the same point, the same route). A visit today reads the live feed; an earlier day reads that
 * day's record (`flowOnDay`, Q161), offline saved as pending and filled by a refresh. Nothing invented: what could not
 * be read is null, and said.
 */
export async function captureVisit(spot: Spot, raw: CaptureVisitInput): Promise<Visit> {
  // (T307) offline the network asks are not made, flaky they are cut short; the reconnect refresh fills the rest
  const input: CaptureVisitInput = { ...raw, ...fastPorts(raw, raw.history?.online) } as CaptureVisitInput;
  const now = input.now ?? Date.now();
  const visitedAt = input.visitedAt ?? now;
  const earlier = localDate(visitedAt) < localDate(now);
  const ports: DayFlowPorts = { source: input.source, ...input.history };
  const first = visitSpot(spot, firstVisitId(spot));
  const station = dayFlowStation(first);
  const members = first.panel ? panelOfAnswer(first.panel) : undefined;
  const at = { lat: spot.lat, lon: spot.lon };

  const panelled = await (async (): Promise<Pick<Spot, "panel" | "panelDay">> => {
    try {
      if (earlier) return await capturePanelOnDay(members, localDate(visitedAt), ports);
      if (!members || !input.feed) return { panel: null };
      // (T314) the index unreadable (offline): the members kept pending, read by a refresh
      const idx = await input.feed.index().then((i) => i ?? null, reported(null));
      return idx ? { panel: livePanel(members, idx) } : pendingPanel(members, localDate(visitedAt));
    } catch (e) {
      reportFailure(e);                                           // (T368 Q7) reported unless offline
      return { panel: null };                                     // a visit must save; a missing estimate is the smaller loss
    }
  })();
  const [reading, weather, leadUp] = await Promise.all([
    earlier || !station ? Promise.resolve(station ? { station, discharge: null, level: null, percentile: null, at: null } : null)
      : readingNow(input.source, station),
    (input.weather?.at(at.lat, at.lon, new Date(visitedAt)) ?? Promise.resolve(null)).catch(reported(null)),
    leadUpFor(input.source, input.weather, at, station, leadUpDays(visitedAt), input.feed),
  ]);
  const visit: Visit = {
    id: input.id?.() ?? `visit_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: now, visitedAt, rating: null, notes: "", photos: [],
    reading, weather, trace: first.trace ?? null, leadUp, ...panelled,
  };
  if (!earlier) return visit;
  // the trip day's own flow, through the one fill a refresh and "Get the flow for that day" use
  const filled = (await fillDayFlow({ ...spot, ...visit, rating: null, visits: undefined } as Spot, ports, now)).spot;
  return filled.dayFlow ? { ...visit, dayFlow: filled.dayFlow } : visit;
}

/** The lead station's reading now; nothing read is a reading of nulls (the station is still the one that speaks). */
async function readingNow(source: CaptureInput["source"], station: string): Promise<Spot["reading"]> {
  try {
    const got = await source.gaugeNow(station as Parameters<typeof source.gaugeNow>[0]);
    if (!got) return { station, discharge: null, level: null, percentile: null, at: null };
    return { station, discharge: got.value.discharge, level: got.value.level, parameter: got.value.parameter,
             percentile: got.value.percentile ?? null, at: got.value.at ?? null };
  } catch (e) {
    reportFailure(e);                                             // (T368 Q7) reported unless offline
    return { station, discharge: null, level: null, percentile: null, at: null };
  }
}
