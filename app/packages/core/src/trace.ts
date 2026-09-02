/**
 * How a reach reaches its gauge — the shape, and the words for it.
 *
 * ONE definition, because two surfaces show this and they must not disagree. The
 * Conditions panel asks it live; a saved spot replays what was frozen the day it was
 * pinned. Same component, same sentences, different source — which is the only honest way
 * to have both, and the reason none of this lives in either screen.
 *
 * Every field is nullable ON PURPOSE. A trace is assembled from a gauge link, a downstream
 * walk and two magnitudes, and some of those are not built yet. A null renders as "not
 * known" — never as zero, never as an absent row. `share` of null and `share` of 0% are
 * completely different claims about a river.
 */
import type { GaugeTrust } from "./flow";

export interface GaugeTrace {
  /** The station this reach drains through. Null means no station may speak for it. */
  station: string | null;
  stationName: string | null;
  /** How much of the gauge's watershed this reach is. */
  trust: GaugeTrust | null;
  /** The reaches between here and the gauge, this one first. */
  path: readonly string[];
  /** Along-channel distance to the station. Null when nothing has computed it. */
  metres: number | null;
  /** FWA stream magnitudes the trust was derived from. */
  reachMagnitude: number | null;
  gaugeMagnitude: number | null;
  /**
   * Where the station stands, so a route can be DRAWN rather than only described.
   *
   * Null for two different reasons and the panel must not conflate them: ECCC published no
   * coordinate for this station, or this trace was frozen into a saved spot before the app
   * carried coordinates at all. Either way there is no map to draw and the words still work
   * — which is why the panel treats the map as an addition to the sentence, never as the
   * sentence itself.
   */
  lon: number | null;
  lat: number | null;
}

/**
 * Where to point a small map so both ends of the route are on it.
 *
 * CENTRED ON THE STATION, not fitted to the path, and the reason is a gap in the data
 * rather than a preference: the app knows where the gauge is (ECCC publishes it) and does
 * NOT know where the reach is — no section carries a coordinate in the bundle. Fitting a
 * camera to geometry the client does not have would mean inventing one.
 *
 * So the zoom stands in for the distance instead. The hop count is the only measure of
 * "how far" that survives to the client — `metres` is null until something computes
 * along-channel distance — and it is a coarse one, so the steps are coarse too. A reader
 * gets a map that certainly contains the gauge and probably contains their reach, and the
 * facts underneath say which.
 */
export function routeCamera(t: GaugeTrace): { lon: number; lat: number; zoom: number } | null {
  if (t.lon === null || t.lat === null) return null;
  const hops = Math.max(0, t.path.length - 1);
  const zoom = hops === 0 ? 12.5 : hops <= 3 ? 11 : hops <= 10 ? 9.5 : 8;
  return { lon: t.lon, lat: t.lat, zoom };
}

/** The gauge is on this very reach — there is no distance to travel. */
export const onTheReach = (t: GaugeTrace): boolean =>
  t.station !== null && t.path.length <= 1;

/**
 * What fraction of the gauge's watershed this reach is.
 *
 * This is the number the whole trust rule is built on: the Fraser at Hope drains 216,600
 * km² and knows nothing about a creek above Chilliwack. Null when either magnitude is
 * missing — a share computed from a guess is worse than no share.
 */
export function share(t: GaugeTrace): number | null {
  if (!t.reachMagnitude || !t.gaugeMagnitude) return null;
  return t.reachMagnitude / t.gaugeMagnitude;
}

/** "12.3 km", "450 m", or null. Never "0 m" for an unknown. */
export function distanceWord(metres: number | null): string | null {
  if (metres === null) return null;
  return metres >= 1000 ? `${(metres / 1000).toFixed(1)} km` : `${Math.round(metres)} m`;
}

/** "2.4 %", "0.03 %", or null. Two decimals below one percent, where the floors live. */
export function shareWord(t: GaugeTrace): string | null {
  const s = share(t);
  if (s === null) return null;
  const pct = s * 100;
  return `${pct.toFixed(pct < 1 ? 2 : 1)} %`;
}

/**
 * The one-line explanation under the panel.
 *
 * Says what is true, including when that is "we cannot tell you" — the sentence a person
 * needs is different in each case, and a generic one would be wrong in all of them.
 */
export function traceSentence(t: GaugeTrace): string {
  if (t.station === null)
    return "No station drains enough of this water to speak for it. One that did not " +
           "would still give you a number, and the number would be wrong.";
  if (onTheReach(t)) return `The gauge sits on this reach.`;
  const d = distanceWord(t.metres);
  return d === null
    ? `This reach drains through ${t.station}.`
    : `Water here flows ${d} down to ${t.station}.`;
}
