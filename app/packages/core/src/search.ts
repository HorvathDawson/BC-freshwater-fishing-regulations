/**
 * SEARCH — which water a person meant, and where on the ground it is.
 *
 * Pure: the bundle hands over candidates and facts, this decides the order and the camera.
 * It lives in core because the answer to "which Elk River did they mean" must be the same
 * on the phone, on mobile web and on the desktop, and a ranking written into a component
 * is a ranking the other two surfaces re-invent.
 *
 * TWO QUESTIONS, KEPT APART.
 *
 *  1. ORDER. A match's QUALITY comes first — exact, then prefix, then a word inside the
 *     name, then anywhere — and only among equally good matches does the bigger water win.
 *     "elk" must offer the Elk River that drains 10,000 km² before the one that drains
 *     1,000, but it must never put "Welkin Creek" (a contains match on a big water) above
 *     "Elk Lake" (a prefix match on a small one): nobody typing "elk" meant Welkin.
 *
 *  2. PLACE. The bundle carries no geometry, so where a water IS has to be worked out from
 *     what it does carry: gauges and stocking sites that sit ON the water (exact points),
 *     and the precomputed distance from every town within 25 km (a ring each). `fixOf`
 *     turns those into a box the camera can open on; the map then refines it against the
 *     real tile geometry (`refineFit`), because an estimate is a starting point, not an
 *     outline.
 */

import { sizeSignal } from "./near";

/** `[west, south, east, north]`, degrees. MapLibre's `fitBounds` order. */
export type Bbox = readonly [number, number, number, number];

export interface LatLon { lat: number; lon: number }

/** Lower-case, accents off, whitespace collapsed. "Chilliwack  Riv" == "chilliwack riv". */
export function normalise(s: string): string {
  return s.normalize("NFD").replace(/[̀-ͯ]/g, "")
    .toLowerCase().replace(/[’']/g, "").replace(/\s+/g, " ").trim();
}

/**
 * HOW GOOD A MATCH IS, lower is better. The ladder is the ranking's first key.
 *
 *   0 the name, exactly            "chilliwack river"
 *   2 the name starts with it      "chilliwack riv"
 *   2 an alias, exactly            "vedder river" -> Chilliwack River
 *   3 an alias starts with it
 *   4 a word in the name does      "chilliwack" -> "Upper Chilliwack River"
 *   5 a word in an alias does
 *   6 anywhere in the name         "illiwa"
 *   7 anywhere in an alias
 *   null — no match at all
 *
 * An exact ALIAS ties with a name prefix rather than beating it, and that was measured:
 * two small Elk Lakes carry the alias "Elk", so "elk" put two ponds above the Elk River
 * that drains the Rockies. Tied, size decides — which is what a reader typing "elk" meant.
 */
export function matchTier(query: string, name: string, alias: string | null = null):
    number | null {
  const q = normalise(query);
  if (!q) return null;
  const tierOf = (text: string, isAlias: boolean): number | null => {
    const t = normalise(text);
    const a = isAlias ? 1 : 0;
    if (t === q) return isAlias ? 2 : 0;
    if (t.startsWith(q)) return 2 + a;
    if (t.split(/[\s\-(/]+/).some((w) => w.startsWith(q))) return 4 + a;
    if (t.includes(q)) return 6 + a;
    return null;
  };
  const byName = tierOf(name, false);
  const byAlias = alias ? tierOf(alias, true) : null;
  if (byName === null) return byAlias;
  if (byAlias === null) return byName;
  return Math.min(byName, byAlias);
}

/** What the ranking needs to know about one candidate. */
export interface Candidate {
  name: string;
  /** stream | lake | wetland — which size figure speaks for it (`sizeSignal`). */
  kind: string;
  /** The alias that matched, if the query found it by another name. */
  matchedAs: string | null;
  /**
   * How notable a STREAM is — the FWA stream magnitude of its biggest reach. Null when the
   * bundle has no such figure (lakes carry none). Null sorts as small, never as big.
   */
  size: number | null;
  /** How big a LAKE is, in hectares (`item.area_ha`). Null for a stream. */
  areaHa: number | null;
  /** How many reaches it is drawn as. A tie-break after size: more water, more notable. */
  pieces: number;
}

/**
 * Order candidates for a query: match tier, then size (`sizeSignal` — a lake by its area, a
 * stream by its magnitude, on one log scale, the SAME signal the near-place list ranks by),
 * then pieces, then the shorter name, then alphabetically — so the order is total and a
 * re-render never reshuffles equals. Candidates that do not match at all are dropped rather
 * than sorted last.
 */
export function rankWaters<T extends Candidate>(query: string, hits: readonly T[]): T[] {
  const scored = hits
    .map((h) => ({ h, tier: matchTier(query, h.name, h.matchedAs),
                   size: sizeSignal(h.kind, h.size, h.areaHa) }))
    .filter((x): x is { h: T; tier: number; size: number } => x.tier !== null);
  scored.sort((a, b) =>
    a.tier - b.tier
    || b.size - a.size
    || b.h.pieces - a.h.pieces
    || a.h.name.length - b.h.name.length
    || a.h.name.localeCompare(b.h.name));
  return scored.map((x) => x.h);
}

/**
 * Which hits share a display name with another hit — the ones that need a "where".
 *
 * Two rows reading "Elk River" with nothing else on them are one choice presented twice;
 * the reader cannot pick. So a duplicate always carries its location line, and a unique
 * name does not have to.
 */
export function duplicateNames(hits: readonly { name: string }[]): Set<string> {
  const seen = new Map<string, number>();
  for (const h of hits) seen.set(normalise(h.name), (seen.get(normalise(h.name)) ?? 0) + 1);
  return new Set([...seen].filter(([, n]) => n > 1).map(([k]) => k));
}

/** One town near a water, and how far the water comes to it. */
export interface NearPlace extends LatLon {
  name: string;
  kind: string;
  km: number;
}

/**
 * The town to describe a water by — "near Fernie".
 *
 * Not simply the nearest named place: that is often a locality nobody has heard of — the
 * Elk River's nearest name is "Mosquito Flats", 100 m off, while it runs through Fernie
 * 440 m further on. So each kind of place carries a handicap in km, and the smallest
 * handicapped distance wins: a city a few km off beats a hamlet on the bank, but a
 * locality ON the water still beats a city 20 km away.
 */
const HANDICAP_KM: Record<string, number> = {
  city: 0, town: 0.5, village: 3, suburb: 3, neighbourhood: 4, quarter: 4,
  hamlet: 5, locality: 6,
};
export function describeBy(near: readonly NearPlace[]): NearPlace | null {
  if (!near.length) return null;
  const cost = (p: NearPlace) => p.km + (HANDICAP_KM[p.kind] ?? 6);
  return [...near].sort((a, b) => cost(a) - cost(b) || a.name.localeCompare(b.name))[0]!;
}

/** "near Fernie · 3 km", "at Hope", or "near Fernie · under 1 km". */
export function wherePhrase(p: { name: string; km: number } | null): string | null {
  if (!p) return null;
  if (p.km < 0.05) return `at ${p.name}`;
  return `near ${p.name} · ${p.km < 1 ? "under 1 km" : `${Math.round(p.km)} km`}`;
}

// ---- place ----------------------------------------------------------------------------

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
   */
  basis: "exact" | "estimate";
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
