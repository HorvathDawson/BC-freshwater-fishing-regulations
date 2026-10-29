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
import { DIST_STEP, GAUGE_RELS, type GaugeRel } from "./gauge-policy.generated";
import type { SectionKey } from "./section";

export interface GaugeTrace {
  /** The station this reach drains through. Null means no station may speak for it. */
  station: string | null;
  stationName: string | null;
  /** How much of the gauge's watershed this reach is. */
  trust: GaugeTrust | null;
  /**
   * The reaches between here and the gauge, this one first.
   *
   * SectionKey, because a section is an integer handle in the bundle and `core` has no
   * business knowing which. These handles are valid only against the atlas that produced
   * them, so a SAVED SPOT never stores them: it keeps a `FrozenTrace` (below), with the
   * count in place of the path — see AGENTS rule 5.
   */
  path: readonly SectionKey[];
  /** Along-channel distance to the station. Null when nothing has computed it. */
  metres: number | null;
  /**
   * WHERE THE STATION STANDS, seen from this reach (T93, `section_gauge.rel`): on it, downstream or upstream on this
   * same watercourse, or downstream on the river this water flows into. Absent / null on a release cut before T93 and on
   * a spot saved before it — then the words fall back to what the walk alone can say.
   */
  rel?: GaugeRel | null;
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
 * A trace as a SAVED SPOT keeps it: every fact, and the number of reaches in place of the section handles.
 *
 * Owner decision 2026-10-20 and AGENTS rule 5: a saved spot is a file a person keeps across every rebuild, and a
 * section id must never leave the bundle. The handles named reaches of the atlas that produced them and nothing
 * after it; the COUNT is what the words and the camera use, and it stays true.
 */
export interface FrozenTrace extends Omit<GaugeTrace, "path"> {
  reaches: number;
}

/** Either a live trace or a frozen one. Every sentence and camera below takes both. */
export type AnyTrace = GaugeTrace | FrozenTrace;

/** How many reaches lie between here and the gauge, this one included. */
export const reachCount = (t: AnyTrace): number => ("path" in t ? t.path.length : t.reaches);

/** The reaches to highlight on a map. A frozen trace has none: its handles were never kept. */
export const tracePath = (t: AnyTrace): readonly SectionKey[] => ("path" in t ? t.path : []);

/** The one way a live trace becomes a stored one. */
export function freezeTrace(t: GaugeTrace): FrozenTrace {
  const { path, ...facts } = t;
  return { ...facts, reaches: path.length };
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
export function routeCamera(t: AnyTrace, from?: { lat: number; lon: number } | null):
  { lon: number; lat: number; zoom: number } | null {
  if (t.lon === null || t.lat === null) return null;
  if (!from) {
    // Nothing but the station. The hop count is the only measure of "how far" that
    // survives to the client, so the zoom is coarse because the evidence is.
    const hops = Math.max(0, reachCount(t) - 1);
    return { lon: t.lon, lat: t.lat,
             zoom: hops === 0 ? 12.5 : hops <= 3 ? 11 : hops <= 10 ? 9.5 : 8 };
  }
  // BOTH ENDS KNOWN, so the camera is derived rather than guessed: centre on the midpoint,
  // and pick the zoom from the actual separation. Degrees of longitude are narrowed by
  // latitude, so the east-west span is scaled by cos(lat) before the two are compared.
  const lat = (t.lat + from.lat) / 2;
  const lon = (t.lon + from.lon) / 2;
  const dLat = Math.abs(t.lat - from.lat);
  const dLon = Math.abs(t.lon - from.lon) * Math.cos((lat * Math.PI) / 180);
  const spread = Math.max(dLat, dLon);
  // 360 degrees fill the world at z0; each zoom halves it. The 1.6 leaves the markers off
  // the edge of the frame rather than exactly on it, and 512 is MapLibre's tile size — at
  // 256 this came out a whole zoom level too close and put both ends off the frame.
  const zoom = spread <= 0
    ? 12.5
    : Math.max(5, Math.min(13, Math.log2(360 / (spread * 1.6 * 512 / 220))));
  return { lon, lat, zoom };
}

/**
 * Where to point a small map so a WHOLE PANEL fits on it — every donor, and the spot.
 *
 * `routeCamera` above frames one gauge and one point. A panel is up to four gauges that may
 * sit on three different rivers, and framing only the heaviest puts the others off the edge
 * — which draws exactly the picture the panel exists to correct, one gauge standing in for
 * a set.
 *
 * IN WEB MERCATOR, AND PER AXIS. The first version took `max(latitude span, longitude
 * span)` in degrees and fitted it to a single assumed viewport size. Both halves of that
 * are wrong: a degree of latitude occupies about 1.56x more pixels at 50°N than at the
 * equator, so a north-south panel was drawn about half again too large, and a map 420 px
 * wide by 210 tall constrains the two axes differently anyway. Measured on the Fraser: a
 * donor pin landed 136 px ABOVE the top of a 210 px map. Projecting first and fitting each
 * axis to its own dimension is the whole fix.
 *
 * Returns null when there is nothing to frame, so a caller draws no map rather than a map
 * of nowhere.
 */
export function panelCamera(
  points: readonly { lat: number | null; lon: number | null }[],
  from?: { lat: number; lon: number } | null,
  /** The map's size in CSS pixels. Defaults suit the route map under an estimate. */
  size: { width: number; height: number } = { width: 380, height: 210 },
): { lon: number; lat: number; zoom: number } | null {
  const pts = [...points, ...(from ? [from] : [])]
    .filter((p): p is { lat: number; lon: number } => p.lat !== null && p.lon !== null);
  if (!pts.length) return null;

  // Web Mercator, normalised to [0,1] in each axis — the space tiles are cut in, so a span
  // here is a span in tile pixels once multiplied by the world size.
  const CLAMP = 85.05112878;                       // where the projection is cut off
  const toY = (lat: number) => {
    const φ = (Math.min(CLAMP, Math.max(-CLAMP, lat)) * Math.PI) / 180;
    return (1 - Math.log(Math.tan(φ) + 1 / Math.cos(φ)) / Math.PI) / 2;
  };
  const toLat = (y: number) =>
    (Math.atan(Math.sinh(Math.PI * (1 - 2 * y))) * 180) / Math.PI;

  const xs = pts.map((p) => (p.lon + 180) / 360);
  const ys = pts.map((p) => toY(p.lat));
  const x0 = Math.min(...xs), x1 = Math.max(...xs);
  const y0 = Math.min(...ys), y1 = Math.max(...ys);
  const lon = ((x0 + x1) / 2) * 360 - 180;
  const lat = toLat((y0 + y1) / 2);

  /*
   * 512, NOT 256 — MapLibre's world is `tileSize * 2^zoom` and its vector tile size is 512.
   * At 256 every camera came out exactly one zoom level too close, which is a factor of two
   * on every span: measured on the Fraser's panel, the pins needed 311 px of a 210 px map.
   * The web-mapping literature is full of the 256 figure because that is the raster slippy
   * tile, and it is the wrong constant for this renderer.
   */
  const TILE = 512;
  const PAD = 1.35;                 // markers sit off the edge of the frame, not on it
  const fit = (span: number, px: number) =>
    span <= 0 ? Infinity : Math.log2(px / (TILE * span * PAD));
  const zoom = Math.min(fit(x1 - x0, size.width), fit(y1 - y0, size.height));
  // A single point has no span and would fit at any zoom, so it gets a sensible close one.
  return { lon, lat, zoom: Math.max(4, Math.min(13, Number.isFinite(zoom) ? zoom : 12.5)) };
}

/**
 * How far apart two coordinates are, in metres.
 *
 * Equirectangular rather than haversine, which is accurate to well under a percent at the
 * distances this is asked about (a few hundred metres) and is not asked about any others:
 * the one question is "are these two marks the same place", where being out by a metre
 * changes nothing and the extra trigonometry earns nothing.
 */
export function metresApart(a: { lat: number; lon: number },
                            b: { lat: number; lon: number }): number {
  const lat = ((a.lat + b.lat) / 2) * (Math.PI / 180);
  const dLat = (a.lat - b.lat) * 111_320;
  const dLon = (a.lon - b.lon) * 111_320 * Math.cos(lat);
  return Math.hypot(dLat, dLon);
}

/**
 * Close enough that two pins on a small map would sit on top of each other.
 *
 * A gauge IS often the place you tapped — you tapped the river at the station, or the
 * station is the only thing on that reach — and drawing "you are here" under a gauge pin
 * makes the map look like it lost one of them. 250 m is roughly a marker's width at the
 * zoom these maps use.
 */
export const SAME_PLACE_M = 250;

/** The gauge is on this very reach — there is no distance to travel. */
export const onTheReach = (t: AnyTrace): boolean =>
  t.station !== null && (t.rel ? t.rel === "on" : reachCount(t) <= 1);

/**
 * What fraction of the gauge's watershed this reach is.
 *
 * This is the number the whole trust rule is built on: the Fraser at Hope drains 216,600
 * km² and knows nothing about a creek above Chilliwack. Null when either magnitude is
 * missing — a share computed from a guess is worse than no share.
 */
export function share(t: AnyTrace): number | null {
  if (!t.reachMagnitude || !t.gaugeMagnitude) return null;
  return t.reachMagnitude / t.gaugeMagnitude;
}

/** "12.3 km", "450 m", or null. Never "0 m" for an unknown. */
export function distanceWord(metres: number | null): string | null {
  if (metres === null) return null;
  return metres >= 1000 ? `${(metres / 1000).toFixed(1)} km` : `${Math.round(metres)} m`;
}

/**
 * (T93) The release's encoding back to words' inputs: `section_gauge.rel` is an index into `GAUGE_RELS`, `dist_b` a
 * channel-distance bucket (0 under 500 m, 1 under 1 km, b >= 2 about 1 km x DIST_STEP^(b-2) — `shed.dist_bucket`).
 * Null in, null out: a release cut before T93 says nothing.
 */
export function relOfCode(code: number | null): GaugeRel | null {
  return code === null ? null : GAUGE_RELS[code] ?? null;
}
export function metresOfBucket(b: number | null): number | null {
  if (b === null) return null;
  if (b <= 0) return 0;
  if (b === 1) return 750;
  return Math.round(1000 * DIST_STEP ** (b - 2));
}

/** A bucketed distance as it may honestly be said: "under 500 m", "under 1 km", "about 3.4 km", "about 11 km". */
export function aboutWord(metres: number | null): string | null {
  if (metres === null) return null;
  if (metres < 500) return "under 500 m";
  if (metres < 1000) return "under 1 km";
  const km = metres / 1000;
  return `about ${km < 10 ? km.toFixed(1) : Math.round(km)} km`;
}

/** Which way along the channel the station stands, as the distance row's value says it (T93). */
const WAY: Partial<Record<GaugeRel, string>> = {
  down: "downstream", up: "upstream", down_trunk: "downstream, on the river this flows into",
  up_branch: "upstream, on a tributary",
};

/**
 * The distance row's value (T93): "about 11 km downstream", "under 1 km upstream", the bare distance when the release says no
 * direction, or null when there is no distance. Never "0 m" — a reach that starts at the station is "at the gauge".
 */
export function whereWord(t: AnyTrace): string | null {
  if (onTheReach(t)) return TRACE_WORDS.onTheReach;
  const way = t.rel ? WAY[t.rel] : undefined;
  if (way && t.metres !== null && t.metres < 500)
    return t.rel === "up" || t.rel === "up_branch" ? TRACE_WORDS.atTheTop : TRACE_WORDS.atTheFoot;
  // a release that says the relationship says the distance as a bucket: "about", never false precision
  const d = t.rel ? aboutWord(t.metres) : distanceWord(t.metres);
  if (d === null) return null;
  return way ? `${d} ${way}` : d;
}

/** "2.4 %", "0.03 %", or null. Two decimals below one percent, where the floors live. */
export function shareWord(t: AnyTrace): string | null {
  const s = share(t);
  if (s === null) return null;
  const pct = s * 100;
  return `${pct.toFixed(pct < 1 ? 2 : 1)} %`;
}

/**
 * (T180) THE TRACE PANEL'S WORDS (`GaugeTrace`): its title, each fact's label and the words its values are said in, the
 * route map's gauge dot, and what a missing value reads — said, never left empty ("0 %" is a claim; "not known" is not).
 */
export const TRACE_WORDS = {
  title: "How this spot reaches the gauge",
  label: { distance: "distance", reach: "this reach", gauge: "at the gauge", share: "share", trust: "so it shows" },
  onTheReach: "on the reach",
  atTheTop: "at the top of this reach",
  atTheFoot: "at the foot of this reach",
  magnitude: (m: number) => `magnitude ${m}`,
  gaugeDot: "the gauge",
  notKnown: "not known",
} as const;

/**
 * The one-line explanation under the panel.
 *
 * Says what is true, including when that is "we cannot tell you" — the sentence a person
 * needs is different in each case, and a generic one would be wrong in all of them.
 */
export function traceSentence(t: AnyTrace): string {
  if (t.station === null)
    return "No station drains enough of this water to speak for it. One that did not " +
           "would still give you a number, and the number would be wrong.";
  if (onTheReach(t)) return `The gauge sits on this reach.`;
  const d = t.rel ? aboutWord(t.metres) : distanceWord(t.metres);
  // (T93) THE STATION IS UPSTREAM: water here has already passed it — "drains through" and "flows down to" would both
  // be false (the Vedder below 08MH001). The release says so in `rel`; without it the walk's downstream words stand.
  if (t.rel === "up" || t.rel === "up_branch") {
    const on = t.rel === "up" ? "on this water" : "on a tributary of this water";
    return d === null || (t.metres ?? 0) < 500
      ? `${t.station} is just upstream, ${on}: water here has already passed it.`
      : `${t.station} is ${d} upstream, ${on}: water here has already passed it.`;
  }
  return d === null
    ? `This reach drains through ${t.station}.`
    : `Water here flows ${d} down to ${t.station}.`;
}
