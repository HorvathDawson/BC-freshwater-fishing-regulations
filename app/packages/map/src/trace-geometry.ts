/**
 * THE ROUTE, COPIED OUT OF THE TILES — so it draws at every zoom.
 *
 * OWNER 2026-10-21: "if it is zoomed out too much the stream for how gauges connect to where
 * you are dont even draw because it is gone at zoom level so we need to copy geometry on
 * click". The route map lit the reaches between the spot and its gauges by feature-state on
 * the atlas, and feature-state can only colour what the tile at the CURRENT zoom carries.
 * The atlas thins itself by stream magnitude (`MAGNITUDE_LADDER`: a magnitude-5 creek first
 * appears at z12) and the Conditions view stands every river down below z9 — so at the zoom
 * that holds a whole panel on screen, most of the route was simply not there to light.
 *
 * GEOMETRY ONLY FROM THE TILES (owner rule: there is no second geometry store). When a
 * section is opened, the route's section ids are looked up in the same atlas archive the map
 * draws, at a zoom where they exist, and their shapes are handed back as GeoJSON for an
 * overlay that shows at every zoom. The MVT feature id IS the section handle (P18), so the
 * match is exact — no name, no proximity, no guess.
 *
 * BOUNDED. A first pass reads the tiles over the route's box at the deepest zoom that keeps
 * the count within `FIRST_BUDGET`; reaches too small to be in those tiles are then sought one
 * zoom deeper at a time, in a 3x3 block around the END they belong to (small water is at the
 * small end of a chain — the spot on a creek, or an upstream gauge on one), until found, z14,
 * or `MAX_TILES`. Decoded geometry is cached BY SECTION ID, so reopening a water reads no
 * tiles at all; raw tiles are cached too, bounded, for neighbouring routes.
 *
 * PURE APART FROM THE FETCH. Everything here takes a `TileSource`; `pmtilesSource` is the
 * one binding to the archive, and works wherever `fetch` does — the same helper for the web
 * map and, through the adapter contract's `setTrace`, the native one.
 */
import type { SectionKey } from "@app/core";
import { PMTiles } from "pmtiles";
import { decodeTile, type TileLayer } from "./mvt";
import { perSectionLayers } from "./sections";

export type LonLat = { lon: number; lat: number };
/** [west, south, east, north] */
export type Bbox = [number, number, number, number];

/** The atlas's top zoom: everything exists at 14 (`MAGNITUDE_LADDER`); nothing exists below 4. */
export const ATLAS_MAX_Z = 14;
/** The first pass's zoom range: below 8 a z4–8 tile has thinned all but mainstems. */
const FIRST_LO = 8;
const FIRST_HI = 12;
/** Tiles in the first pass. Atlas tiles here are ~10–20 KB, so 16 is ~250 KB at worst. */
export const FIRST_BUDGET = 16;
/** Every tile one request may read, all passes together. */
export const MAX_TILES = 40;
/**
 * The layers a route's sections live in: the per-section ones (`perSectionLayers`, T124). `under_lake` carries no id
 * and cannot be matched.
 */
export const TRACE_LAYERS: ReadonlySet<string> = new Set(perSectionLayers());

/** One route: a donor's chain, spot first, gauge last (`PanelRoute.path`). */
export interface TraceRoute {
  /** Where the GAUGE sits relative to the spot (`PanelRoute.role`). */
  role: "up" | "down";
  /** The donor's identity colour, from the app's palette (a token, never a literal here). */
  tone: string;
  path: readonly SectionKey[];
  /** The gauge the reader picked in the list: its line is drawn heavier, the rest thinner. */
  on?: boolean;
  /** The spot — the chain's first end. */
  from: LonLat | null;
  /** The gauge — the chain's last end. */
  to: LonLat | null;
}

export interface TileSource {
  /** One tile's raw MVT bytes (decompressed), or null when the archive has none there. */
  tile(z: number, x: number, y: number): Promise<Uint8Array | null>;
}

/* ------------------------------------------------------------------ tile math ---- */

const clampLat = (lat: number) => Math.max(-85.05112878, Math.min(85.05112878, lat));

/** The tile a point falls in. */
export function tileOf(lon: number, lat: number, z: number): [number, number] {
  const n = 2 ** z;
  const φ = (clampLat(lat) * Math.PI) / 180;
  const x = Math.floor(((lon + 180) / 360) * n);
  const y = Math.floor(((1 - Math.log(Math.tan(φ) + 1 / Math.cos(φ)) / Math.PI) / 2) * n);
  return [Math.max(0, Math.min(n - 1, x)), Math.max(0, Math.min(n - 1, y))];
}

/** Every tile at `z` that the box touches, row by row. */
export function tilesCovering(b: Bbox, z: number): [number, number, number][] {
  const [x0, y0] = tileOf(b[0], b[3], z);           // north-west
  const [x1, y1] = tileOf(b[2], b[1], z);           // south-east
  const out: [number, number, number][] = [];
  for (let y = y0; y <= y1; y++) for (let x = x0; x <= x1; x++) out.push([z, x, y]);
  return out;
}

/** The deepest zoom in [lo, hi] whose cover of the box is within `budget`; `lo` if none is. */
export function zoomForBudget(b: Bbox, budget: number, lo = FIRST_LO, hi = FIRST_HI): number {
  for (let z = hi; z > lo; z--) if (tilesCovering(b, z).length <= budget) return z;
  return lo;
}

/** The box around every known end, padded by `pad` of its size (a river bends outside its ends). */
export function routeBbox(routes: readonly TraceRoute[], pad = 0.2): Bbox | null {
  const pts = routes.flatMap((r) => [r.from, r.to]).filter((p): p is LonLat => p !== null);
  if (!pts.length) return null;
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  for (const p of pts) {
    w = Math.min(w, p.lon); e = Math.max(e, p.lon);
    s = Math.min(s, p.lat); n = Math.max(n, p.lat);
  }
  // At least ~1 km of margin, so a route whose ends coincide still reads its own reach.
  const dx = Math.max((e - w) * pad, 0.015), dy = Math.max((n - s) * pad, 0.01);
  return [w - dx, s - dy, e + dx, n + dy];
}

/** The 3x3 block of tiles around a point at `z` — where a small end's reaches are sought. */
export function blockAround(p: LonLat, z: number): [number, number, number][] {
  const [cx, cy] = tileOf(p.lon, p.lat, z);
  const n = 2 ** z;
  const out: [number, number, number][] = [];
  for (let dy = -1; dy <= 1; dy++)
    for (let dx = -1; dx <= 1; dx++) {
      const x = cx + dx, y = cy + dy;
      if (x >= 0 && y >= 0 && x < n && y < n) out.push([z, x, y]);
    }
  return out;
}

/**
 * Which end a missing section belongs to: the half of the chain it sits in. Small water
 * is at the small end, and the chain runs spot → gauge, so the first half is sought around
 * the spot and the second around the gauge.
 */
export function endFor(route: TraceRoute, index: number): LonLat | null {
  return index < route.path.length / 2 ? route.from ?? route.to : route.to ?? route.from;
}

/* ---------------------------------------------------------- clip and project ---- */

type Pt = [number, number];

/** A polyline clipped to [0, ext]² (Liang–Barsky per segment), as the pieces left inside. */
export function clipLine(line: readonly Pt[], ext: number): Pt[][] {
  const out: Pt[][] = [];
  let cur: Pt[] | null = null;
  for (let i = 1; i < line.length; i++) {
    const [x0, y0] = line[i - 1]!, [x1, y1] = line[i]!;
    const dx = x1 - x0, dy = y1 - y0;
    let t0 = 0, t1 = 1, ok = true;
    for (const [p, q] of [[-dx, x0], [dx, ext - x0], [-dy, y0], [dy, ext - y0]] as const) {
      if (p === 0) { if (q < 0) { ok = false; break; } continue; }
      const t = q / p;
      if (p < 0) { if (t > t1) { ok = false; break; } if (t > t0) t0 = t; }
      else { if (t < t0) { ok = false; break; } if (t < t1) t1 = t; }
    }
    if (!ok) { cur = null; continue; }
    const a: Pt = [x0 + t0 * dx, y0 + t0 * dy], b: Pt = [x0 + t1 * dx, y0 + t1 * dy];
    if (!cur || t0 > 0) { cur = [a]; out.push(cur); }
    cur.push(b);
    if (t1 < 1) cur = null;
  }
  // A segment that only TOUCHES the edge clips to a point; a zero-length piece is not a line.
  return out.filter((l) => l.some((p) => p[0] !== l[0]![0] || p[1] !== l[0]![1]));
}

/** A ring clipped to [0, ext]² (Sutherland–Hodgman), closed; empty when nothing is inside. */
export function clipRing(ring: readonly Pt[], ext: number): Pt[] {
  let pts: Pt[] = ring.slice(0, ring.length > 1 && ring[0]![0] === ring[ring.length - 1]![0]
                                 && ring[0]![1] === ring[ring.length - 1]![1]
                                 ? -1 : undefined);
  const edges: [(p: Pt) => boolean, (a: Pt, b: Pt) => Pt][] = [
    [(p) => p[0] >= 0, (a, b) => [0, a[1] + ((b[1] - a[1]) * (0 - a[0])) / (b[0] - a[0])]],
    [(p) => p[0] <= ext, (a, b) => [ext, a[1] + ((b[1] - a[1]) * (ext - a[0])) / (b[0] - a[0])]],
    [(p) => p[1] >= 0, (a, b) => [a[0] + ((b[0] - a[0]) * (0 - a[1])) / (b[1] - a[1]), 0]],
    [(p) => p[1] <= ext, (a, b) => [a[0] + ((b[0] - a[0]) * (ext - a[1])) / (b[1] - a[1]), ext]],
  ];
  for (const [inside, cross] of edges) {
    if (!pts.length) break;
    const next: Pt[] = [];
    for (let i = 0; i < pts.length; i++) {
      const a = pts[(i + pts.length - 1) % pts.length]!, b = pts[i]!;
      if (inside(b)) { if (!inside(a)) next.push(cross(a, b)); next.push(b); }
      else if (inside(a)) next.push(cross(a, b));
    }
    pts = next;
  }
  return pts.length >= 3 ? [...pts, pts[0]!] : [];
}

/** Twice the signed area in tile units (y down): positive is an MVT exterior ring. */
const ringArea = (r: readonly Pt[]) => {
  let a = 0;
  for (let i = 0, j = r.length - 1; i < r.length; j = i++) a += (r[j]![0] - r[i]![0]) * (r[j]![1] + r[i]![1]);
  return a;
};

const round6 = (v: number) => Math.round(v * 1e6) / 1e6;

/** A tile-unit point to lon/lat. */
export function project(z: number, x: number, y: number, ext: number, p: Pt): Pt {
  const n = 2 ** z;
  const lon = ((x + p[0] / ext) / n) * 360 - 180;
  const lat = (Math.atan(Math.sinh(Math.PI * (1 - (2 * (y + p[1] / ext)) / n))) * 180) / Math.PI;
  return [round6(lon), round6(lat)];
}

/** One section's shape, gathered from every tile it was found in. */
export interface SectionShape {
  lines: Pt[][];
  /** Polygons as [exterior, ...holes] — a lake, or a stream drawn as a polygon. */
  areas: Pt[][][];
  /** The zoom it was read at — deeper is finer. */
  z: number;
}

/** The wanted features of one decoded tile, clipped to the tile and projected. */
export function shapesIn(layers: readonly TileLayer[], z: number, x: number, y: number):
    Map<number, SectionShape> {
  const out = new Map<number, SectionShape>();
  for (const l of layers)
    for (const f of l.features) {
      if (f.id === null || f.type === 1) continue;
      const s = out.get(f.id) ?? { lines: [], areas: [], z };
      if (f.type === 2) {
        for (const ln of f.rings)
          for (const piece of clipLine(ln, l.extent))
            s.lines.push(piece.map((p) => project(z, x, y, l.extent, p)));
      } else {
        let poly: Pt[][] | null = null;
        for (const ring of f.rings) {
          const exterior = ringArea(ring) > 0 || poly === null;
          const clipped = clipRing(ring, l.extent);
          if (exterior) {
            poly = clipped.length ? [clipped.map((p) => project(z, x, y, l.extent, p))] : null;
            if (poly) s.areas.push(poly);
          } else if (poly && clipped.length) {
            poly.push(clipped.map((p) => project(z, x, y, l.extent, p)));
          }
        }
      }
      out.set(f.id, s);
    }
  return out;
}

/* ------------------------------------------------------------------- caching ---- */

/** Sections remembered between opens. A route is tens of sections; this is many routes. */
export const SECTION_CACHE_MAX = 4000;
/** Raw tiles remembered — the neighbouring route usually reads the same ones. */
export const TILE_CACHE_MAX = 64;

/** Bounded insertion-order (oldest-first) cache. */
export class Lru<K, V> {
  private m = new Map<K, V>();
  readonly max: number;
  constructor(max: number) { this.max = max; }
  get(k: K): V | undefined {
    const v = this.m.get(k);
    if (v !== undefined) { this.m.delete(k); this.m.set(k, v); }
    return v;
  }
  set(k: K, v: V): void {
    this.m.delete(k);
    this.m.set(k, v);
    while (this.m.size > this.max) this.m.delete(this.m.keys().next().value as K);
  }
  get size(): number { return this.m.size; }
  clear(): void { this.m.clear(); }
}

/** Per archive: sections by id, and tiles by z/x/y. */
export interface TraceCache {
  sections: Lru<number, SectionShape>;
  tiles: Lru<string, Promise<Map<number, SectionShape>>>;
}
export const newTraceCache = (): TraceCache =>
  ({ sections: new Lru(SECTION_CACHE_MAX), tiles: new Lru(TILE_CACHE_MAX) });

/* -------------------------------------------------------------------- the read ---- */

export interface TraceRead {
  /** Shapes by section id, for every wanted id that was found. */
  shapes: Map<number, SectionShape>;
  /** Wanted ids found nowhere within the budget. */
  missing: number[];
  /** Tiles actually fetched (cache misses), and every z/x/y the plan asked for. */
  fetched: number;
  asked: string[];
}

/**
 * Find every section on the routes, reading as few tiles as the plan allows.
 *
 * `ids` the wanted shapes are matched by; the routes say where to look. Cached sections are
 * answered without a tile; only the rest steer the plan.
 */
export async function readTrace(routes: readonly TraceRoute[], src: TileSource,
                                cache: TraceCache, budget = { first: FIRST_BUDGET,
                                                              max: MAX_TILES }):
    Promise<TraceRead> {
  const wanted = new Set<number>();
  for (const r of routes) for (const s of r.path) wanted.add(Number(s));
  const shapes = new Map<number, SectionShape>();
  for (const id of wanted) {
    const hit = cache.sections.get(id);
    if (hit) shapes.set(id, hit);
  }
  const asked: string[] = [];
  let fetched = 0;
  const missingNow = () => [...wanted].filter((id) => !shapes.has(id));
  if (!missingNow().length) return { shapes, missing: [], fetched, asked };

  const seen = new Set<string>();
  const read = async (tiles: readonly [number, number, number][]) => {
    const fresh = tiles.filter(([z, x, y]) => !seen.has(`${z}/${x}/${y}`))
      .slice(0, Math.max(0, budget.max - seen.size));
    for (const [z, x, y] of fresh) seen.add(`${z}/${x}/${y}`);
    const want = new Set(missingNow());
    const got = await Promise.all(fresh.map(([z, x, y]) => {
      const key = `${z}/${x}/${y}`;
      asked.push(key);
      let p = cache.tiles.get(key);
      if (!p) {
        fetched++;
        // ALL route-layer features with an id are kept for the tile cache, not only the ones
        // wanted now: the next route opened nearby asks for different ids from the same tile.
        p = src.tile(z, x, y).then((b) => (b ? shapesIn(decodeTile(b, TRACE_LAYERS, () => true),
                                                        z, x, y) : new Map()))
          .catch(() => new Map<number, SectionShape>());
        cache.tiles.set(key, p);
      }
      return p;
    }));
    for (const m of got)
      for (const [id, s] of m) {
        if (!want.has(id)) continue;
        const had = shapes.get(id);
        if (!had || had.z < s.z) shapes.set(id, { lines: [...s.lines], areas: [...s.areas], z: s.z });
        else if (had.z === s.z) { had.lines.push(...s.lines); had.areas.push(...s.areas); }
      }
  };

  // PASS 1: the whole route's box, at the deepest zoom the budget allows.
  const box = routeBbox(routes);
  let z = box ? zoomForBudget(box, budget.first) : ATLAS_MAX_Z;
  if (box) await read(tilesCovering(box, z));
  // DEEPER, AROUND THE ENDS that still have something missing.
  while (missingNow().length && z < ATLAS_MAX_Z && seen.size < budget.max) {
    z++;
    const lost = new Set(missingNow());
    const ends = new Map<string, LonLat>();
    for (const r of routes)
      r.path.forEach((s, i) => {
        if (!lost.has(Number(s))) return;
        const e = endFor(r, i);
        if (e) ends.set(`${e.lon.toFixed(5)},${e.lat.toFixed(5)}`, e);
      });
    if (!ends.size) break;
    await read([...ends.values()].flatMap((e) => blockAround(e, z)));
  }
  for (const [id, s] of shapes) cache.sections.set(id, s);
  return { shapes, missing: missingNow(), fetched, asked };
}

/* ------------------------------------------------------------------ the output ---- */

export interface TraceFeature {
  type: "Feature";
  properties: { role: "up" | "down"; tone: string; kind: "line" | "area"; order: number; on: boolean };
  geometry: { type: "MultiLineString"; coordinates: Pt[][] }
          | { type: "MultiPolygon"; coordinates: Pt[][][] };
}
export interface TraceCollection { type: "FeatureCollection"; features: TraceFeature[] }

/**
 * The overlay: one line feature and at most one area feature per route.
 *
 * `order` is the route's index, so the heaviest donor (first in the panel's weight order)
 * draws on top where two routes share a reach — the spot's own reach is on every one.
 */
export function traceCollection(routes: readonly TraceRoute[],
                                shapes: ReadonlyMap<number, SectionShape>): TraceCollection {
  const features: TraceFeature[] = [];
  routes.forEach((r, i) => {
    const lines: Pt[][] = [], areas: Pt[][][] = [];
    for (const s of r.path) {
      const sh = shapes.get(Number(s));
      if (!sh) continue;
      lines.push(...sh.lines);
      areas.push(...sh.areas);
    }
    const props = { role: r.role, tone: r.tone, order: routes.length - i, on: !!r.on };
    if (areas.length)
      features.push({ type: "Feature", properties: { ...props, kind: "area" },
                      geometry: { type: "MultiPolygon", coordinates: areas } });
    if (lines.length)
      features.push({ type: "Feature", properties: { ...props, kind: "line" },
                      geometry: { type: "MultiLineString", coordinates: lines } });
  });
  return { type: "FeatureCollection", features };
}

/** The box around everything the overlay draws — what "fit" frames once it is in. */
export function collectionBbox(fc: TraceCollection): Bbox | null {
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  const take = (p: Pt) => {
    w = Math.min(w, p[0]); e = Math.max(e, p[0]); s = Math.min(s, p[1]); n = Math.max(n, p[1]);
  };
  for (const f of fc.features) {
    if (f.geometry.type === "MultiLineString") f.geometry.coordinates.forEach((l) => l.forEach(take));
    else f.geometry.coordinates.forEach((poly) => poly[0]?.forEach(take));
  }
  return Number.isFinite(w) ? [w, s, e, n] : null;
}

/* ------------------------------------------------------------- the archive ---- */

const archives = new Map<string, { src: TileSource; cache: TraceCache }>();

/**
 * The atlas archive as a `TileSource`, one per URL for the session — the same bytes the
 * map's own `pmtiles://` source reads (an HTTP cache shared with it on the web).
 */
export function atlasTrace(url: string): { src: TileSource; cache: TraceCache } {
  let a = archives.get(url);
  if (!a) {
    const pm = new PMTiles(url);
    a = {
      src: { tile: async (z, x, y) => {
        const r = await pm.getZxy(z, x, y);
        return r ? new Uint8Array(r.data) : null;
      } },
      cache: newTraceCache(),
    };
    archives.set(url, a);
  }
  return a;
}

/**
 * The whole job: the routes' shapes from the atlas at `url`, as an overlay collection.
 * `ms`, `tiles` (every tile the plan read, cached or not) and `fetched` (the cache misses
 * among them) are the measured cost, reported so a caller (and a test) can see it.
 */
export async function traceOverlay(url: string, routes: readonly TraceRoute[],
                                   source?: TileSource):
    Promise<{ fc: TraceCollection; missing: number[]; fetched: number; tiles: number;
              ms: number }> {
  const t0 = Date.now();
  const a = source ? { src: source, cache: atlasTrace(url).cache } : atlasTrace(url);
  const got = await readTrace(routes, a.src, a.cache);
  return { fc: traceCollection(routes, got.shapes), missing: got.missing,
           fetched: got.fetched, tiles: got.asked.length, ms: Date.now() - t0 };
}
