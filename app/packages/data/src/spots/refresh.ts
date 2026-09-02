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
 */
import type { Spot } from "./model";
import type { WeatherSource } from "./capture";

/** Is anything on this spot still unknown, and worth another try? */
export function needsRefresh(spot: Spot): boolean {
  // A gauge reading that was never available is not the same as one that cannot exist:
  // a spot on water no station speaks for has `reading: null` forever, and asking again
  // will never change it. Only the weather is retryable today.
  return spot.weather === null;
}

/** One line for the UI. Null when there is nothing outstanding. */
export function refreshReason(spot: Spot): string | null {
  if (spot.weather !== null) return null;
  return "No weather was recorded — it can be filled in from the day this was pinned.";
}

/**
 * Fetch what is missing, as of the spot's own date. Returns the same object when there is
 * nothing to do, so a caller can skip the write.
 */
export async function refreshSpot(spot: Spot, weather: WeatherSource): Promise<Spot> {
  if (!needsRefresh(spot)) return spot;
  // `visitedAt`, never `createdAt`: a spot typed up a week later must be backfilled with
  // the weather of the day at the water, not of the evening at the kitchen table.
  const got = await weather.at(spot.lat, spot.lon, new Date(spot.visitedAt));
  if (!got) return spot;                    // still no answer; leave the record honest
  return { ...spot, weather: got, updatedAt: Date.now() };
}
