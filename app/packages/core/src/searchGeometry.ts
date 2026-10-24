/**
 * SEARCH'S SECOND QUESTION — PLACE: where on the ground a searched water is, and what the camera opens on (`search.ts`
 * answers the first, ORDER). Pure geometry over what the bundle carries.
 *
 * The bundle carries no geometry for a water's line, so where a water IS has to be worked out from what it does carry:
 * its parts' boxes (`boxOfParts`, the outline), gauges and stocking sites that sit ON the water (exact points), and the
 * precomputed distance from every town within 25 km (a ring each). `fixOf` turns the evidence into a box the camera can
 * open on; the map then refines it against the real tile geometry (`refineFit`), because an estimate is a starting
 * point, not an outline.
 */

/** `[west, south, east, north]`, degrees. MapLibre's `fitBounds` order. */
export type Bbox = readonly [number, number, number, number];

export interface LatLon { lat: number; lon: number }

/**
 * Everything the bundle knows about where a water is. Both lists may be empty.
 *
 * `on` are points ON the water — a hydrometric station on one of its reaches, a stocking
 * site matched to the lake. `near` are towns and the distance from each to the nearest
 * point of the water (`place_water`, precomputed from the real geometry).
 */
export interface WaterFix {
  on: readonly LatLon[];
  near: readonly (LatLon & { km: number })[];
}

/** What the camera should open on, and how much to trust it. */
export interface Extent {
  bbox: Bbox;
  /**
   * `exact`  — built from points on the water itself.
   * `estimate` — inferred from town distances; right to within a few km, which is the
   *              scale of the box, so the map should refine it against the tiles.
   * `outline` — the water's own geometry: the union of its parts' boxes (`boxOfParts`). Nothing to refine.
   */
  basis: "outline" | "exact" | "estimate";
}

const KM_PER_DEG_LAT = 111.32;
const kmPerDegLon = (lat: number) => KM_PER_DEG_LAT * Math.cos((lat * Math.PI) / 180);

/** Great-circle-ish distance in km — equirectangular, which is plenty at 25 km. */
export function distanceKm(a: LatLon, b: LatLon): number {
  const dx = (a.lon - b.lon) * kmPerDegLon((a.lat + b.lat) / 2);
  const dy = (a.lat - b.lat) * KM_PER_DEG_LAT;
  return Math.hypot(dx, dy);
}

/** A box `km` either side of a point. */
export function boxAround(p: LatLon, km: number): Bbox {
  const dLat = km / KM_PER_DEG_LAT;
  const dLon = km / kmPerDegLon(p.lat);
  return [p.lon - dLon, p.lat - dLat, p.lon + dLon, p.lat + dLat];
}

/** The smallest box around some points, or null for none. */
export function bboxOf(points: readonly LatLon[]): Bbox | null {
  if (!points.length) return null;
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  for (const p of points) {
    w = Math.min(w, p.lon); e = Math.max(e, p.lon);
    s = Math.min(s, p.lat); n = Math.max(n, p.lat);
  }
  return [w, s, e, n];
}

/** Two boxes as one. */
export function unionBbox(a: Bbox | null, b: Bbox | null): Bbox | null {
  if (!a) return b;
  if (!b) return a;
  return [Math.min(a[0], b[0]), Math.min(a[1], b[1]),
          Math.max(a[2], b[2]), Math.max(a[3], b[3])];
}

/** Grow a box so neither side is under `minKm` — a lake is not a point. */
export function padBbox(b: Bbox, minKm: number): Bbox {
  const mid = { lat: (b[1] + b[3]) / 2, lon: (b[0] + b[2]) / 2 };
  const halfLat = Math.max((b[3] - b[1]) / 2, minKm / 2 / KM_PER_DEG_LAT);
  const halfLon = Math.max((b[2] - b[0]) / 2, minKm / 2 / kmPerDegLon(mid.lat));
  return [mid.lon - halfLon, mid.lat - halfLat, mid.lon + halfLon, mid.lat + halfLat];
}

/**
 * The point that best explains a set of "the water comes within d km of here" rings.
 *
 * A least-squares fit over a grid: for every candidate point, how far is it from sitting
 * exactly on each ring. For a compact water — a lake, a creek — the nearest points to each
 * town are all inside the water, so the best point is inside it too. For a long river it
 * lands somewhere ON the river near the towns, which is the right place to start the
 * map-side refinement from. Towns are weighted toward the close ones, because a 2 km ring
 * pins the water far harder than a 24 km one.
 */
export function trilaterate(near: readonly (LatLon & { km: number })[]): LatLon | null {
  const rings = [...near].sort((a, b) => a.km - b.km).slice(0, 8);
  if (!rings.length) return null;
  const first = rings[0]!;
  if (rings.length === 1 || first.km < 0.05) return { lat: first.lat, lon: first.lon };
  const cost = (p: LatLon) => rings.reduce((acc, r) => {
    const miss = distanceKm(p, r) - r.km;
    return acc + (miss * miss) / (1 + r.km);
  }, 0);
  // Search the disc the nearest ring allows, then refine around the winner.
  let best: LatLon = { lat: first.lat, lon: first.lon };
  let bestCost = cost(best);
  let span = first.km + 1;
  for (let round = 0; round < 4; round++) {
    const centre = best;
    const N = 20;
    for (let i = -N; i <= N; i++)
      for (let j = -N; j <= N; j++) {
        const p = {
          lat: centre.lat + (i / N) * (span / KM_PER_DEG_LAT),
          lon: centre.lon + (j / N) * (span / kmPerDegLon(centre.lat)),
        };
        const c = cost(p);
        if (c < bestCost) { bestCost = c; best = p; }
      }
    span /= 6;
  }
  return best;
}

/**
 * Where to open the camera for a water, or null when the bundle cannot say.
 *
 * Points ON the water win outright — they are the water. Otherwise the rings are
 * trilaterated and the box is sized to the nearest ring, capped: the estimate is good to
 * about that distance, and a box much wider than that would open a whole valley for a
 * pond.
 */
export function fixOf(fix: WaterFix): Extent | null {
  const on = bboxOf(fix.on);
  if (on) return { bbox: padBbox(on, 3), basis: "exact" };
  const p = trilaterate(fix.near);
  if (!p) return null;
  const nearest = Math.min(...fix.near.map((r) => r.km));
  return { bbox: boxAround(p, Math.min(Math.max(nearest * 0.6, 2), 8)), basis: "estimate" };
}

/**
 * One part of a water as the bundle measures it (`part_geo`): its box in degrees, or — where the builder wrote none —
 * only its midpoint, which then counts as a point of the box.
 */
export interface PartBox {
  w: number | null; s: number | null; e: number | null; n: number | null;
  midLon: number; midLat: number;
}

/**
 * WHERE A WATER IS, FROM ITS OWN GEOMETRY: the union of its parts' boxes. A water split into closure areas (Kootenay
 * Lake, Williston Lake) is handed its areas' parts, so it is placed by them. Null only for a water with no part at all.
 */
export function boxOfParts(parts: readonly PartBox[]): Bbox | null {
  let box: Bbox | null = null;
  for (const p of parts) {
    const own: Bbox = p.w !== null && p.s !== null && p.e !== null && p.n !== null
      ? [p.w, p.s, p.e, p.n] : [p.midLon, p.midLat, p.midLon, p.midLat];
    box = unionBbox(box, own);
  }
  return box;
}

/** The smallest side an outline opens on: a pond is a few metres across, and a camera that tight shows no shore. */
export const OUTLINE_MIN_KM = 0.6;

/** A water's outline as a camera extent — exactly its box, widened only where a side is under `OUTLINE_MIN_KM`. */
export function outlineOf(box: Bbox | null): Extent | null {
  if (!box) return null;
  const mid = (box[1] + box[3]) / 2;
  const wide = (box[2] - box[0]) * kmPerDegLon(mid) >= OUTLINE_MIN_KM
    && (box[3] - box[1]) * KM_PER_DEG_LAT >= OUTLINE_MIN_KM;
  return { bbox: wide ? box : padBbox(box, OUTLINE_MIN_KM), basis: "outline" };
}

/**
 * Where to open on a water: its OUTLINE when the bundle has one (every named water in a release does); otherwise the
 * evidence (`fixOf`: gauges on it, then town rings). Gauges and towns never override the water's own shape.
 */
export function extentOf(box: Bbox | null, fix: WaterFix | null): Extent | null {
  return outlineOf(box) ?? (fix ? fixOf(fix) : null);
}

/**
 * A box around a town and the water near it — what "what is near Smithers" opens on.
 *
 * The radius is the farthest water listed, so every row in the list is on screen, with a
 * floor so a town whose only water is at its doorstep still opens on its valley.
 */
export function townBox(town: LatLon, farthestKm: number): Bbox {
  return boxAround(town, Math.min(Math.max(farthestKm, 4), 25) + 1);
}

/**
 * THE MAP'S SIDE OF THE FIT: given what the camera is showing and what the tiles have
 * revealed of the water so far, is there more to show?
 *
 * The tiles only hold what is loaded, so the first look at a long river sees a piece of
 * it. Fitting to that piece loads more; fitting again reveals more. This says when to stop:
 * when the geometry found is already comfortably inside the view, another fit would only
 * jitter the camera. `rounds` is a hard stop, because a water that runs off the edge of
 * every tile set (the Fraser) would otherwise walk the camera forever.
 */
export function refineFit(view: Bbox, found: Bbox | null, rounds: number,
                          maxRounds = 4): Bbox | null {
  if (!found || rounds >= maxRounds) return null;
  const w = view[2] - view[0], h = view[3] - view[1];
  // Within 2% of an edge counts as touching it: the water probably continues past it.
  const touches = found[0] <= view[0] + w * 0.02 || found[2] >= view[2] - w * 0.02
               || found[1] <= view[1] + h * 0.02 || found[3] >= view[3] - h * 0.02;
  const fw = found[2] - found[0], fh = found[3] - found[1];
  // Far smaller than the view: we are zoomed out on a small water — close in on it.
  const tiny = fw < w * 0.25 && fh < h * 0.25;
  return touches || tiny ? found : null;
}

/**
 * The box around a GeoJSON geometry's coordinates — any depth of nesting, so a line, a
 * multi-line and a polygon with holes all read the same way. What the map measures a
 * searched water's loaded tiles with.
 */
export function bboxOfGeometry(g: { type: string; coordinates?: unknown } | null | undefined):
    Bbox | null {
  if (!g || g.coordinates === undefined) return null;
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  const walk = (c: unknown): void => {
    if (!Array.isArray(c)) return;
    if (typeof c[0] === "number" && typeof c[1] === "number") {
      const [x, y] = c as [number, number];
      if (x < w) w = x; if (x > e) e = x;
      if (y < s) s = y; if (y > n) n = y;
      return;
    }
    for (const k of c) walk(k);
  };
  walk(g.coordinates);
  return w === Infinity ? null : [w, s, e, n];
}
