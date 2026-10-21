/**
 * Filling in what a spot could not know when it was made.
 *
 * A spot pinned with no signal has `weather: null` and possibly no reading. That is the
 * honest record — but it should not stay that way forever, and the app should say so
 * rather than leaving a person to wonder whether the blank is a bug.
 *
 * Both halves are here: a predicate the UI uses to show "needs refresh", and the fill
 * itself, which asks for the values AS OF THE DAY THE SPOT WAS MADE. Fetching today's
 * weather for a spot from last October would be worse than the blank.
 *
 * S1: the WEEK BEFORE the visit is refreshed the same way — its weather and its station's daily means, each only if
 * it is missing, and marked as filled in. A spot saved before the lead-up existed has none, and gets one.
 */
import { leadUpDays } from "@app/core";
import type { RegsSource } from "../index";
import type { LeadUp, Spot } from "./model";
import type { WeatherSource } from "./capture";
import { leadUpMissing, weekGauge, weekWeather } from "./leadup";

/** Is anything on this spot still unknown, and worth another try? */
export function needsRefresh(spot: Spot): boolean {
  // A gauge reading that was never available is not the same as one that cannot exist:
  // a spot on water no station speaks for has `reading: null` forever, and asking again
  // will never change it. The weather, and the week before, are retryable.
  return spot.weather === null || leadUpMissing(spot.leadUp);
}

/** One line for the UI. Null when there is nothing outstanding. */
export function refreshReason(spot: Spot): string | null {
  if (spot.weather === null) return "No weather was recorded — it can be filled in from the day this was pinned.";
  if (leadUpMissing(spot.leadUp))
    return "The week before this visit was not recorded — it can be filled in from the days themselves.";
  return null;
}

/**
 * Fetch what is missing, as of the spot's own date. Returns the same object when there is
 * nothing to do, so a caller can skip the write.
 *
 * `source` reads the station's daily means for the week before; without it, that half is left to be asked again.
 */
export async function refreshSpot(spot: Spot, weather: WeatherSource, source?: RegsSource): Promise<Spot> {
  if (!needsRefresh(spot)) return spot;
  let next = spot;
  if (spot.weather === null) {
    // `visitedAt`, never `createdAt`: a spot typed up a week later must be backfilled with
    // the weather of the day at the water, not of the evening at the kitchen table.
    const got = await weather.at(spot.lat, spot.lon, new Date(spot.visitedAt));
    if (got) next = { ...next, weather: got };   // still no answer: leave the record honest
  }
  if (leadUpMissing(spot.leadUp)) {
    const filled = await fillLeadUp(spot, weather, source);
    if (filled) next = { ...next, leadUp: filled };
  }
  return next === spot ? spot : { ...next, updatedAt: Date.now() };
}

/** The week before, with only its missing halves fetched; null when nothing new was learned. */
async function fillLeadUp(spot: Spot, weather: WeatherSource, source?: RegsSource): Promise<LeadUp | null> {
  const had = spot.leadUp ?? null;
  const days = leadUpDays(spot.visitedAt);
  // The station the spot recorded: a spot keeps no section (AGENTS 5), so it cannot be looked up again.
  const station = had ? had.station : spot.reading?.station ?? spot.trace?.station ?? null;
  const w = had?.weather ?? await weekWeather(weather, spot.lat, spot.lon, days);
  const g = had?.gauge ?? (station && source ? await weekGauge(source, station, days) : null);
  if (w === (had?.weather ?? null) && g === (had?.gauge ?? null)) return null;
  return { weather: w, station, gauge: g, backfilled: true };
}
