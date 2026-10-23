/**
 * "Like your visit?" (T51): the inputs to core's `likeDay`, gathered — the visit's from the spot's FROZEN record, the
 * coming days' from the live feed and the Open-Meteo forecast — and the last fetched forecast kept, so the strip works
 * offline and says how old it is.
 *
 * THE FEED IS READ, NEVER RE-FETCHED. The coming days' flow is the feed index's own percentiles (now, and the `ahead`
 * block the Conditions horizons read), through `reading` — the one function the map and the panel read a station's
 * percentile with. The index is the same cached object every other surface asks for.
 *
 * STATION AGAINST STATION. The visit's flow is the spot's own gauge's DISCHARGE percentile that day (its row in the
 * frozen donor panel, which was asked in discharge; else its reading when that reading was a discharge only), and
 * each coming day's is the same station's. A level is never read here (decisions.md "NEVER MIX QUANTITIES").
 *
 * A DAY NO HORIZON REACHES HAS NO FLOW. The feed ranks a forecast for +1, +3, +5 and +10 days; the strip's +2 and +4
 * are scored on the weather alone and say so — never filled from a neighbour.
 */
import { HORIZONS, LIKE_RAIN_DAYS, localDate, shiftDay, type LikeConditions } from "@app/core";
import { reading } from "../panel";
import type { Spot } from "./model";

/** The forecast the coming days' weather is read from: daily rain (with the past days the rain window needs) and
 *  hourly air, wind and cloud, each on the PLACE's local clock ("2026-10-24", "2026-10-24T09:00"). */
export interface WeatherAhead {
  days: { date: string; precipMm: number | null }[];
  hours: { at: string; tempC: number | null; windKph: number | null; cloudPct: number | null }[];
}

/** The station rows `reading` reads, as the feed's index carries them. */
type IndexRow = Parameters<typeof reading>[0];
export interface LikeIndex { fetchedAt?: string; stations: Readonly<Record<string, IndexRow>> }

/** What was last fetched for one spot, each half with when it was fetched (device time). */
export interface LikeFetched {
  weather: WeatherAhead | null;
  weatherAt: number | null;
  /** The station's discharge percentile by LOCAL day, for the days a horizon reaches. */
  flow: Record<string, number | null> | null;
  flowAt: number | null;
  /**
   * The local day `flow`'s reading NOW (horizon 0) is for — the index's own day, which is not always the device's
   * today (a feed published yesterday, a cache fetched yesterday). Only that day's flow is "now"; every other day's
   * is a forecast. Null when no flow was fetched; absent on a cache from before this was kept (read as a forecast).
   */
  flowNow?: string | null;
}

export const NOTHING_FETCHED: LikeFetched =
  { weather: null, weatherAt: null, flow: null, flowAt: null, flowNow: null };

/** The station the visit's flow is from, and the coming days' too. */
export const likeStation = (spot: Spot): string | null => spot.reading?.station ?? spot.leadUp?.station ?? null;

/** The visit's hour of day, local: "09:00". The weather is compared at this hour. */
export function visitHour(spot: Pick<Spot, "visitedAt">): string {
  return `${String(new Date(spot.visitedAt).getHours()).padStart(2, "0")}:00`;
}

/** The visit's conditions, from the frozen record only. */
export function visitConditions(spot: Spot): LikeConditions {
  const station = likeStation(spot);
  const row = station ? spot.panel?.rows.find((r) => r.station === station) : undefined;
  const r = spot.reading;
  // the panel was asked in discharge (capture.ts `panelFor`); a bare reading is a discharge only when it holds no level
  const flow = row?.percentile ?? (r && r.percentile !== null && r.discharge !== null && r.level === null
    ? r.percentile : null);
  const w = spot.weather;
  const lead = spot.leadUp?.weather ?? null;
  const last = lead ? lead.slice(-LIKE_RAIN_DAYS) : [];
  const rain = last.length === LIKE_RAIN_DAYS && last.every((d) => d.precipMm !== null)
    ? last.reduce((s, d) => s + d.precipMm!, 0) : null;
  return {
    flow,
    tempC: w?.window?.at?.tempC ?? w?.tempC ?? null,
    rainMm: rain,
    windKph: w?.windKph ?? null,
    cloudPct: w?.window?.at?.cloudPct ?? null,
  };
}

/**
 * The station's discharge percentile for each day a horizon reaches, keyed by local day from the index's own
 * `fetchedAt` (the day its horizons count from; the device's day when it carries none). Null when the station is not
 * in the index — the feed does not carry it.
 */
export function flowAhead(index: LikeIndex, station: string, now: number): Record<string, number | null> | null {
  const row = index.stations[station];
  if (!row) return null;
  const base = indexDay(index, now);
  return Object.fromEntries(HORIZONS.map((h) => [localDate(shiftDay(base, h).getTime()), reading(row, "discharge", h)]));
}

/** The day the index's horizons count from: its own `fetchedAt`, else the device's now. */
function indexDay(index: LikeIndex, now: number): Date {
  const t = index.fetchedAt ? Date.parse(index.fetchedAt) : NaN;
  return new Date(Number.isFinite(t) ? t : now);
}

/** The coming days' conditions, out of what was last fetched; a day it does not reach is a day of nulls. */
export function comingConditions(f: LikeFetched, days: readonly string[], hour: string):
  (LikeConditions & { date: string })[] {
  const rainOf = (date: string): number | null => {
    if (!f.weather) return null;
    const before = Array.from({ length: LIKE_RAIN_DAYS }, (_, i) => {
      const [y, m, d] = date.split("-").map(Number) as [number, number, number];
      return localDate(new Date(y, m - 1, d - LIKE_RAIN_DAYS + i).getTime());
    });
    const got = before.map((b) => f.weather!.days.find((x) => x.date === b)?.precipMm ?? null);
    return got.every((v) => v !== null) ? got.reduce((s, v) => s + v!, 0) : null;
  };
  return days.map((date) => {
    const h = f.weather?.hours.find((x) => x.at === `${date}T${hour}`);
    return { date, flow: f.flow?.[date] ?? null, tempC: h?.tempC ?? null, rainMm: rainOf(date),
             windKph: h?.windKph ?? null, cloudPct: h?.cloudPct ?? null };
  });
}

/** The ports a fetch needs. Both optional: a missing one leaves its half as last fetched. */
export interface LikeSources {
  weather?: { ahead?(lat: number, lon: number): Promise<WeatherAhead | null> };
  feed?: { index(): Promise<LikeIndex | null> };
}

/**
 * Fetch both halves and lay them over what was last fetched: a half that could not be fetched keeps its last copy
 * and its age, and `offline` says the try failed. A spot with no station has no flow half to fetch.
 */
export async function fetchLike(spot: Spot, had: LikeFetched, src: LikeSources, now: number):
  Promise<{ fetched: LikeFetched; offline: boolean }> {
  const station = likeStation(spot);
  const [w, idx] = await Promise.all([
    src.weather?.ahead ? src.weather.ahead(spot.lat, spot.lon).catch(() => null) : Promise.resolve(null),
    station && src.feed ? src.feed.index().catch(() => null) : Promise.resolve(null),
  ]);
  const flowAsked = !!(station && src.feed);
  const flow = idx && station ? flowAhead(idx, station, now) : undefined;
  const fetched: LikeFetched = {
    weather: w ?? had.weather, weatherAt: w ? now : had.weatherAt,
    flow: flow !== undefined ? flow : had.flow,
    flowAt: flow !== undefined ? now : had.flowAt,
    flowNow: flow === undefined ? had.flowNow ?? null : flow ? localDate(indexDay(idx!, now).getTime()) : null,
  };
  return { fetched, offline: (!!src.weather?.ahead && !w) || (flowAsked && !idx) };
}

/**
 * The last fetched forecasts, one small JSON map by spot id, kept on the device (`openLikeStore`). Unreadable reads
 * as nothing fetched; a refused write is shrugged off — it is a cache, not a record.
 */
export interface LikeStore { read(): string | null; write(text: string): void }

export function readLike(store: LikeStore | undefined, id: string): LikeFetched {
  try {
    const all = JSON.parse(store?.read() ?? "{}") as Record<string, LikeFetched>;
    return all[id] ?? NOTHING_FETCHED;
  } catch {
    return NOTHING_FETCHED;
  }
}

export function writeLike(store: LikeStore | undefined, id: string, f: LikeFetched): void {
  if (!store) return;
  try {
    let all: Record<string, LikeFetched> = {};
    try { all = JSON.parse(store.read() ?? "{}") as Record<string, LikeFetched>; } catch { /* a bad cache is replaced */ }
    all[id] = f;
    store.write(JSON.stringify(all));
  } catch { /* a cache */ }
}
