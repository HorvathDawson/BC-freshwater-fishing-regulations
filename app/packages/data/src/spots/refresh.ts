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
 *
 * AND DAY BY DAY (owner bug 2026-10-22: "the refresh for week before isn't working"). A week saved before its sources
 * had caught up holds days still to come — the feed not yet published past them, a day the weather service did not
 * send. Those were frozen as if recorded, so the refresh never asked again and the spot was never offered one. Now each
 * such day is asked for again, and only it is filled (`fillDays`): a day the spot holds is never overwritten.
 */
import { leadUpDays } from "@app/core";
import type { RegsSource } from "../index";
import type { LeadUp, Spot } from "./model";
import type { WeatherSource } from "./capture";
import { declaredQuantity, fillDays, gaugeDayPending, leadUpMissing, leadUpUnread, weatherDayPending, weekGauge, weekWeather,
         type FeedIndex } from "./leadup";
import { LEAD_UP_WORDS } from "./words";

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
  if (leadUpUnread(spot.leadUp)) return LEAD_UP_WORDS.notRecorded;
  if (leadUpMissing(spot.leadUp)) return LEAD_UP_WORDS.someDaysPending;
  return null;
}

/**
 * Fetch what is missing, as of the spot's own date. Returns the same object when there is
 * nothing to do, so a caller can skip the write.
 *
 * `source` reads the station's daily means for the week before; without it, that half is left to be asked again.
 * `feed` (the live index) tells a station the feed does not carry from a feed that could not be reached.
 */
export async function refreshSpot(spot: Spot, weather: WeatherSource, source?: RegsSource,
                                  feed?: FeedIndex): Promise<Spot> {
  if (!needsRefresh(spot)) return spot;
  let next = spot;
  if (spot.weather === null) {
    // `visitedAt`, never `createdAt`: a spot typed up a week later must be backfilled with
    // the weather of the day at the water, not of the evening at the kitchen table.
    const got = await weather.at(spot.lat, spot.lon, new Date(spot.visitedAt));
    if (got) next = { ...next, weather: got };   // still no answer: leave the record honest
  }
  if (leadUpMissing(spot.leadUp)) {
    const filled = await fillLeadUp(spot, weather, source, feed);
    if (filled) next = { ...next, leadUp: filled };
  }
  return next === spot ? spot : { ...next, updatedAt: Date.now() };
}

/** The week before, with only its missing halves and days fetched; null when nothing new was learned. */
async function fillLeadUp(spot: Spot, weather: WeatherSource, source?: RegsSource,
                          feed?: FeedIndex): Promise<LeadUp | null> {
  const had = spot.leadUp ?? null;
  const days = leadUpDays(spot.visitedAt);
  // The station the spot recorded: a spot keeps no section (AGENTS 5), so it cannot be looked up again.
  const station = had ? had.station : spot.reading?.station ?? spot.trace?.station ?? null;
  const hw = had?.weather ?? null, hg = had?.gauge ?? null;
  const w = hw && !hw.some(weatherDayPending) ? hw
    : fillDays(hw, await weekWeather(weather, spot.lat, spot.lon, days), weatherDayPending);
  const g = !station || !source || (hg && !hg.some(gaugeDayPending)) ? hg
    : fillDays(hg, await weekGauge(source, station, days, feed), gaugeDayPending);
  if (had && w === hw && g === hg) return null;
  if (!had && w === null && g === null) return null;
  // which half this refresh filled, added to any an earlier one did: each row says "filled in" only if it was
  const filledIn = { ...had?.filledIn, ...(w !== hw ? { weather: true as const } : {}),
                     ...(g !== hg ? { gauge: true as const } : {}) };
  // the declared quantity beside days this refresh read (T106 review): a spot saved offline has none on its reading
  const parameter = had?.parameter ?? (g && g !== hg && station && source ? await declaredQuantity(source, station) : null);
  return { weather: w, station, gauge: g, backfilled: true, filledIn, ...(parameter ? { parameter } : {}) };
}
