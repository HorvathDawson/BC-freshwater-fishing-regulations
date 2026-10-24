/**
 * THE WATER'S OWN LINE, KEPT — so a water shows at every zoom (T175).
 *
 * Owner 2026-10-24: "the map in conditions tab of a stream sometimes doesn't show the stream because it is too zoomed
 * out it needs to copy the stream line then display at all zooms maybe?" The atlas thins itself by stream magnitude
 * (`MAGNITUDE_LADDER`: a magnitude-5 creek first appears at z12) and the Conditions view stands every river down below
 * z9, so a creek framed to fit its gauges opens at a zoom whose tiles do not carry it: the map shows the valley and not
 * the water.
 *
 * GEOMETRY FROM THE TILES ONLY (owner rule 2026-10-20: no second geometry source). The map's own pick — the one a fit
 * measures a water with (`MapRenderer.litFeatures`: `querySourceFeatures` on the web, the binding's
 * `VectorSource.querySourceFeatures` on a phone) — is asked, each time the camera settles, for the water's sections in
 * the tiles it holds. The first time a zoom has them, their lines are KEPT here and drawn from a GeoJSON source of their
 * own (`COPY_SOURCE`) at every zoom below the one the atlas itself draws them from. Dropped the moment the water changes
 * (a new `reset` key): a copy never outlives its water, and only ids of THIS water are ever kept — nothing here can
 * draw on another water.
 *
 * ONE COLOURING PATHWAY. The copy's layers are painted with the stream's (and the lake's) own paint by the adapter in
 * the same call (`paint` in `adapters/contract.ts`), and every feature-state the map writes for one of the water's
 * sections on the atlas is written to the copy as well (`mirrorSink`, and the web engine's label target): the copy
 * reads `standing`, `p`, `selected`, `water` exactly as the atlas line does. A copy is drawn as a highlight — at least
 * the selected width (`width.stream.highlight`) — because at the zooms it is needed for, the water's own width is a
 * hairline.
 *
 * WHERE NO ZOOM HAS SHOWN THE WATER YET (`probeZoom`): a map asked to hold the water's line (`MapProps.waterLine`)
 * steps in, one zoom at a time about a point on the water, until its tiles carry it — so it opens no further out than
 * the water's own first zoom (frame to fit, clamped), then captures. Bounded, and never against the reader's own pan.
 *
 * Pure apart from what the renderers hand in: `water-copy.test.ts` drives every rule as plain code.
 */
import { ATLAS_MAX_ZOOM, bboxOfGeometry, unionBbox, type Bbox, type SectionKey } from "@app/core";
import type { StateSink } from "./sections";
import { SECTION_SOURCE_LAYERS, perSectionLayers, sectionIdOf, type PerSectionLayer } from "./sections";

/** The copy's GeoJSON source, declared empty in the runtime style and filled by the map. */
export const COPY_SOURCE = "water-copy";
/** A lake's copy, a fill under the lake's own (the lake's paint). */
export const COPY_FILL = "water-copy-fill";
/** The selected outline under a copied stream line (the selection casing's paint). */
export const COPY_CASING = "water-copy-casing";
/** A stream's copy, a line under the stream's own (the stream's paint, floored to the highlight width). */
export const COPY_LINE = "water-copy-line";
/** Every copy layer, in draw order. */
export const COPY_LAYERS = [COPY_FILL, COPY_CASING, COPY_LINE] as const;
/** The source-layers a copy is kept from: a stream's line and a lake's polygon — the per-section ones but the wetland. */
export type CopyLayer = Exclude<PerSectionLayer, "wetland">;
export const COPY_SOURCE_LAYERS: readonly CopyLayer[] =
  perSectionLayers().filter((l): l is CopyLayer => l !== "wetland");
/** At most this many zoom steps in, for one water: z4 to the atlas's z14 is ten. */
export const MAX_PROBES = 10;

/** One section's feature as the map's pick returns it: its id, its source-layer, its shape (one tile's piece of it). */
export interface LitFeature {
  id: number;
  layer: string;
  geometry: { type: string; coordinates?: unknown } | null;
  properties?: Record<string, unknown> | null;
}

/** The copy as GeoJSON: one feature per section, its pieces merged, its tile properties (`ord`) and its `layer`. */
export interface CopyCollection {
  type: "FeatureCollection";
  features: {
    type: "Feature"; id: number;
    properties: Record<string, unknown> & { layer: CopyLayer };
    geometry: { type: "MultiLineString" | "MultiPolygon"; coordinates: unknown[] };
  }[];
}
export const EMPTY_COPY: CopyCollection = { type: "FeatureCollection", features: [] };

interface Held {
  layer: CopyLayer;
  /** each piece's coordinates: a line's vertex list, or a polygon's rings */
  pieces: unknown[];
  properties: Record<string, unknown>;
  /** the camera zoom the pass that took it was made at */
  zoom: number;
  /** the pieces' extent, degrees² — a later pass that saw MORE of the section replaces a partial first one */
  extent: number;
}

/** The ids a map keeps a copy of: what it lights (`highlight`, `water`) and the water it is asked to hold, once each. */
export function copyIds(...sets: (readonly SectionKey[] | null | undefined)[]): number[] {
  const out = new Set<number>();
  for (const s of sets) for (const k of s ?? []) {
    const id = sectionIdOf(k);
    if (id !== null) out.add(id);
  }
  return [...out].sort((a, b) => a - b);
}

const areaOf = (b: Bbox | null) => (b ? Math.max(0, b[2] - b[0]) * Math.max(0, b[3] - b[1]) : 0);

/**
 * THE KEPT COPY of one water. `reset` names the water; `take` keeps what a settle's pick found; `collection` is what the
 * source draws. Nothing else holds geometry.
 */
export class WaterCopy {
  private ids_ = new Set<number>();
  private key_ = "";
  private held = new Map<number, Held>();
  /** What the copy source holds per section (key → JSON): the atlas writes one state on every per-section layer. */
  private states = new Map<number, Map<string, string>>();

  /** The water's ids, as the map was last told them. */
  get ids(): ReadonlySet<number> { return this.ids_; }
  get key(): string { return this.key_; }
  /** How many of its sections are kept. */
  get size(): number { return this.held.size; }
  has(id: number): boolean { return this.held.has(id); }
  /** How many of `ids` are kept — the WATER's own, not what else the map lights (a route's chain to its gauges). */
  keptOf(ids: ReadonlySet<number>): number {
    let n = 0;
    for (const id of ids) if (this.held.has(id)) n += 1;
    return n;
  }
  /** The source-layer a kept section came from (null: not kept). */
  layerOf(id: number): CopyLayer | null { return this.held.get(id)?.layer ?? null; }

  /**
   * A new water (or the same one again): true, and everything kept is dropped, when the ids — or the archive they are
   * ids in (`scope`: a section id means a section only within one atlas) — changed.
   */
  reset(ids: readonly number[], scope = ""): boolean {
    const key = `${scope}|${[...ids].sort((a, b) => a - b).join(",")}`;
    if (key === this.key_) return false;
    this.key_ = key;
    this.ids_ = new Set(ids);
    this.held.clear();
    this.states.clear();
    return true;
  }

  /**
   * The part of `state` the copy source does not hold yet for `id` (null: nothing new, or not this water's) — and it is
   * then held. One state reaches the atlas once per per-section layer (a selection is set on the stream, lake and
   * wetland layers alike); the copy, one source, takes it once.
   */
  fresh(id: number, state: Record<string, unknown>): Record<string, unknown> | null {
    if (!this.ids_.has(id)) return null;
    const has = this.states.get(id) ?? new Map<string, string>();
    const out = Object.entries(state).filter(([k, v]) => has.get(k) !== JSON.stringify(v ?? null));
    if (!out.length) return null;
    for (const [k, v] of out) has.set(k, JSON.stringify(v ?? null));
    this.states.set(id, has);
    return Object.fromEntries(out);
  }

  /** The renderer dropped every state (a phone's style swap): hold none, so the next writes all reach the copy. */
  forgetStates(): void { this.states.clear(); }

  /** Is `key` (absent: any key) held on `id`'s copy — and it is then dropped. False: nothing to remove. */
  drop(id: number, key?: string): boolean {
    const has = this.states.get(id);
    if (!has) return false;
    if (key === undefined) { this.states.delete(id); return has.size > 0; }
    return has.delete(key);
  }

  /**
   * Keep what one settle's pick found at `zoom`. Only this water's ids, only a stream's line or a lake's polygon. A
   * section is kept the FIRST time a zoom has it; a later pass replaces it only when it saw more of it (a section half
   * off screen the first time). Returns the ids newly kept or replaced, in order.
   */
  take(found: readonly LitFeature[], zoom: number): number[] {
    const pass = new Map<number, { layer: CopyLayer; pieces: unknown[]; properties: Record<string, unknown>;
                                   box: Bbox | null }>();
    for (const f of found) {
      if (!this.ids_.has(f.id) || !f.geometry) continue;
      const layer = f.layer as CopyLayer;
      if (!(COPY_SOURCE_LAYERS as readonly string[]).includes(layer)) continue;
      const pieces = piecesOf(layer, f.geometry);
      if (!pieces.length) continue;
      const p = pass.get(f.id) ?? { layer, pieces: [], properties: { ...(f.properties ?? {}) }, box: null };
      if (p.layer !== layer) continue;            // one section, one source-layer: never mix two shapes in one feature
      p.pieces.push(...pieces);
      p.box = unionBbox(p.box, bboxOfGeometry(f.geometry as never));
      pass.set(f.id, p);
    }
    const fresh: number[] = [];
    for (const [id, p] of [...pass].sort((a, b) => a[0] - b[0])) {
      const was = this.held.get(id);
      const extent = areaOf(p.box);
      if (was && extent <= was.extent * 1.0001) continue;
      this.held.set(id, { layer: p.layer, pieces: p.pieces, properties: p.properties, zoom, extent });
      fresh.push(id);
    }
    return fresh;
  }

  /** The kept sections as the copy source's data. */
  collection(): CopyCollection {
    if (!this.held.size) return EMPTY_COPY;
    return {
      type: "FeatureCollection",
      features: [...this.held].sort((a, b) => a[0] - b[0]).map(([id, h]) => ({
        type: "Feature" as const, id,
        properties: { ...h.properties, layer: h.layer },
        geometry: { type: h.layer === "stream" ? "MultiLineString" as const : "MultiPolygon" as const,
                    coordinates: h.pieces },
      })),
    };
  }

  /**
   * THE ZOOM THE COPY STOPS AT, for one kind of layer: where the atlas draws the water itself. A section the tiles had
   * at camera zoom z is in the z = ⌊z⌋ tiles, so the atlas draws it from there up — unless the view stands the layer
   * down further in (`floor`: the Conditions view's z9 for streams), and then the copy carries on to that floor. The
   * deepest kept zoom wins: a section first seen deeper must not vanish between its own zoom and a shallower one's.
   * Null: nothing of that kind is kept.
   */
  ceiling(layer: CopyLayer, floor: number): number | null {
    let z: number | null = null;
    for (const h of this.held.values()) if (h.layer === layer) z = Math.max(z ?? 0, Math.floor(h.zoom));
    return z === null ? null : Math.min(24, Math.max(z, floor));
  }
}

/** A tile piece's coordinates as the parts of a multi-geometry of its kind (a stream: lines; a lake: polygons). */
function piecesOf(layer: CopyLayer, g: { type: string; coordinates?: unknown }): unknown[] {
  const c = g.coordinates;
  if (!Array.isArray(c)) return [];
  if (layer === "stream") return g.type === "LineString" ? [c] : g.type === "MultiLineString" ? [...c] : [];
  return g.type === "Polygon" ? [c] : g.type === "MultiPolygon" ? [...c] : [];
}

/**
 * THE NEXT ZOOM TO TRY, or null: only while NO zoom has shown the water (nothing kept, nothing found now), the reader
 * has not moved the map, the probes are not spent and the atlas holds deeper tiles. One whole zoom in at a time — the
 * first tile zoom deeper than the camera's.
 */
export function probeZoom(o: { zoom: number; found: boolean; kept: number; rounds: number; touched: boolean }):
    number | null {
  if (o.found || o.kept > 0 || o.touched || o.rounds >= MAX_PROBES) return null;
  const next = Math.floor(o.zoom) + 1;
  return next > ATLAS_MAX_ZOOM ? null : next;
}

/** Web Mercator, normalised to [0, 1]. */
const LAT = 85.05112878;
const toX = (lon: number) => (lon + 180) / 360;
const toY = (lat: number) => {
  const p = (Math.min(LAT, Math.max(-LAT, lat)) * Math.PI) / 180;
  return (1 - Math.log(Math.tan(p) + 1 / Math.cos(p)) / Math.PI) / 2;
};
const toLon = (x: number) => x * 360 - 180;
const toLat = (y: number) => (Math.atan(Math.sinh(Math.PI * (1 - 2 * y))) * 180) / Math.PI;

/**
 * The camera centre that zooms from `from` to `to` ABOUT `anchor` — the anchor stays where it is on screen, so a point
 * on the water (the place being read) stays in view as the frame closes in on it. No anchor: about the centre.
 */
export function zoomAround(center: { lon: number; lat: number }, anchor: { lon: number; lat: number } | null,
                           from: number, to: number): { lon: number; lat: number; zoom: number } {
  if (!anchor) return { lon: center.lon, lat: center.lat, zoom: to };
  const k = 2 ** (from - to);
  const ax = toX(anchor.lon), ay = toY(anchor.lat);
  return { lon: toLon(ax + (toX(center.lon) - ax) * k), lat: toLat(ay + (toY(center.lat) - ay) * k), zoom: to };
}

/** Where a copied section's feature-state goes: the copy source, by the section's id. */
export interface CopyTarget {
  set(id: number, state: Record<string, unknown>): void;
  remove(id: number, key?: string): void;
}

/**
 * THE ONE PATHWAY, TWICE WRITTEN: a renderer's state sink that also writes every per-section state it is given for one
 * of the water's sections to the copy. Every other write passes through untouched.
 */
export function mirrorSink(inner: StateSink, copy: () => WaterCopy | null, target: () => CopyTarget | null): StateSink {
  const mirrored = (t: { source: string; sourceLayer: string }, id: string | number): number | null => {
    if (t.source !== "atlas" || !SECTION_SOURCE_LAYERS.includes(t.sourceLayer)) return null;
    const n = sectionIdOf(id);
    return n !== null && copy()?.ids.has(n) ? n : null;
  };
  return {
    set: (t, id, state) => {
      inner.set(t, id, state);
      const n = mirrored(t, id);
      const s = n === null ? null : copy()?.fresh(n, state) ?? null;
      if (n !== null && s) target()?.set(n, s);
    },
    remove: (t, id, key) => {
      inner.remove(t, id, key);
      const n = mirrored(t, id);
      if (n !== null && copy()?.drop(n, key)) target()?.remove(n, key);
    },
  };
}
