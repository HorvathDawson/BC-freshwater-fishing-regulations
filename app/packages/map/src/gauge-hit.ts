/**
 * (T1194) A TAP ON A GAUGE OPENS THAT GAUGE: the one hit rule both engines read (the web's `web/gaugeHit.ts`, the phone's
 * `Map.native.tsx`), so a dot answers the same finger on both. Platform-free.
 *
 * TWO TARGETS, EACH ITS OWN SIZE (review 2026-11-27: a river beside a 100 px pill opened the gauge):
 *   THE DOT   a 44 px box CENTRED ON THE DOT (`dotBox`: the box a finger needs around a 5 px dot): a tap inside it opens that dot, and
 *             where the box and a river overlap the dot wins only inside the box.
 *   THE PILL  its own drawn size and no more: the label layer is asked at the tap POINT (`pillBox`), and a tap on a pill opens THAT
 *             pill's station — never a nearer dot's.
 * The pill is asked first (it is the thing under the finger); else the nearest dot in the box (never a list).
 */
export const GAUGE_DOT_LAYER = "gauge-dot";
export const GAUGE_PILL_LAYER = "gauge-label";
/** The tap target around a dot's centre: the least a touch target may be (AGENTS: every press at least 44 x 44). */
export const GAUGE_TAP_PX = 44;
/** How far the drawn dot reaches from its centre (5 px radius + 2 px ring): a query finds a feature whose shape meets the box. */
export const GAUGE_DOT_REACH = 7;

type Box = [[number, number], [number, number]];
/** The query box that makes a dot's target `GAUGE_TAP_PX` square: the dot's centre within 22 px of the press, either way. */
export function dotBox(x: number, y: number): Box {
  const r = GAUGE_TAP_PX / 2 - GAUGE_DOT_REACH;
  return [[x - r, y - r], [x + r, y + r]];
}
/** The box that asks what is drawn AT the press (a pill's exact drawn size): a 2 px box, for the engine's rounding. */
export function pillBox(x: number, y: number): Box { return [[x - 1, y - 1], [x + 1, y + 1]]; }

/** What a rendered gauge feature must carry to be read: its station and its point (lon, lat). */
export interface GaugeFeature {
  readonly properties?: Readonly<Record<string, unknown>> | null;
  readonly geometry?: { readonly type?: string; readonly coordinates?: unknown } | null;
}
const stationOf = (f: GaugeFeature): string | null => (typeof f.properties?.station === "string" && f.properties.station !== "" ? f.properties.station : null);

/**
 * THE STATION UNDER A PRESS from what the two queries found, `at` the press's `[lon, lat]`: a pill under the finger first (its own
 * station); else the nearest dot (the nearer in ground distance, east-west scaled by the latitude); null for none. A feature with no
 * station is not a gauge (never guessed). Equal distances: the first listed.
 */
export function gaugeHit(pill: readonly GaugeFeature[], dots: readonly GaugeFeature[], at: readonly [lon: number, lat: number]): string | null {
  for (const f of pill) { const st = stationOf(f); if (st !== null) return st; }
  let best: string | null = null, bestD = Infinity;
  const k = Math.cos((at[1] * Math.PI) / 180);
  for (const f of dots) {
    const st = stationOf(f);
    if (st === null) continue;
    const c = f.geometry?.type === "Point" && Array.isArray(f.geometry.coordinates) ? f.geometry.coordinates as number[] : null;
    const d = c && typeof c[0] === "number" && typeof c[1] === "number" ? Math.hypot((c[0] - at[0]) * k, c[1] - at[1]) : Infinity;
    if (best === null || d < bestD) { best = st; bestD = d; }
  }
  return best;
}

/**
 * THE ONE QUESTION both engines ask, over their own query: the pill at the point and the dots in the box, asked TOGETHER (one round
 * trip on a phone, T716), then `gaugeHit`. `query(box, layer)` is the engine's `queryRenderedFeatures`.
 */
export async function queryGauge(query: (box: Box, layer: string) => Promise<readonly GaugeFeature[]> | readonly GaugeFeature[],
                                 x: number, y: number, at: readonly [lon: number, lat: number]): Promise<string | null> {
  const [pill, dots] = await Promise.all([query(pillBox(x, y), GAUGE_PILL_LAYER), query(dotBox(x, y), GAUGE_DOT_LAYER)]);
  return gaugeHit(pill, dots, at);
}
