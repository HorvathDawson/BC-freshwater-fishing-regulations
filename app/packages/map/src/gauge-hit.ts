/**
 * (T1194) A TAP ON A GAUGE DOT OPENS THAT GAUGE: the one hit rule both engines read (the web's `web/gaugeHit.ts`, the
 * phone's `Map.native.tsx`), so a dot answers the same finger on both. Platform-free.
 *
 * THE BOX: 44 px square around the press (`GAUGE_TAP_PX`, half each way), around a 5 px dot: a finger, not a pixel. It is
 * ASKED FIRST on the Conditions tab (a dot sits on its river, and the finger means the dot), so a press beside the box
 * still opens the water (`gauge-hit.test.ts` pins both edges). Two dots under one finger: the NEAREST to the press,
 * never a list. The dot and its label pill are one gauge (`gauge-dot`, `gauge-label`; `properties.station`).
 */
export const GAUGE_TAP_LAYERS = ["gauge-dot", "gauge-label"] as const;
/** The tap box's side: the least a touch target may be (AGENTS: every press at least 44 x 44). */
export const GAUGE_TAP_PX = 44;

/** The box around a press at screen `(x, y)`: `GAUGE_TAP_PX` square. */
export function gaugeBox(x: number, y: number): [[number, number], [number, number]] {
  const r = GAUGE_TAP_PX / 2;
  return [[x - r, y - r], [x + r, y + r]];
}

/** What a rendered gauge feature must carry to be read: its station and its point (lon, lat). */
export interface GaugeFeature {
  readonly properties?: Readonly<Record<string, unknown>> | null;
  readonly geometry?: { readonly type?: string; readonly coordinates?: unknown } | null;
}

/**
 * THE STATION UNDER A PRESS: the nearest dot's station among the features the box found, `at` the press's `[lon, lat]`
 * (the nearer in ground distance, east-west scaled by the latitude: monotonic with screen distance across a box this
 * small); null for none. A feature with no station is not a gauge (never guessed). Equal distances: the first listed.
 */
export function gaugeHit(features: readonly GaugeFeature[], at: readonly [lon: number, lat: number]): string | null {
  let best: string | null = null, bestD = Infinity;
  const k = Math.cos((at[1] * Math.PI) / 180);
  for (const f of features) {
    const st = f.properties?.station;
    if (typeof st !== "string" || st === "") continue;
    const c = f.geometry?.type === "Point" && Array.isArray(f.geometry.coordinates) ? f.geometry.coordinates as number[] : null;
    const d = c && typeof c[0] === "number" && typeof c[1] === "number" ? Math.hypot((c[0] - at[0]) * k, c[1] - at[1]) : Infinity;
    if (best === null || d < bestD) { best = st; bestD = d; }
  }
  return best;
}
