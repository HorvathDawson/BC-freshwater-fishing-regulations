/**
 * A Mapbox Vector Tile, read just far enough to lift ONE THING out of it: the geometry of
 * features whose id is on a list.
 *
 * WHY A DECODER HERE AND NOT A PACKAGE. The route overlay (`trace-geometry.ts`) copies a
 * route's reaches out of the atlas tiles so it can draw them at zooms the atlas has already
 * thinned them away (owner 2026-10-21: "copy geometry on click"). maplibre-gl carries a
 * decoder but does not export it, and the native renderer carries a different one in C++.
 * Adding `@mapbox/vector-tile` + `pbf` would put two packages on both platforms for ~150
 * lines of a fixed, versioned format (MVT 2.1), and this reader needs a sliver of it: layer
 * name, extent, feature id, type and geometry. Tags (properties) are skipped unread — the
 * id IS the section handle (P18), so nothing else on the feature is needed.
 *
 * Pure bytes in, coordinates out: no DOM, no renderer, the same on every platform.
 */

/** A feature's geometry in tile units (0..extent), before projection. */
export interface TileFeature {
  id: number | null;
  /** MVT GeomType: 1 point, 2 line, 3 polygon. */
  type: 1 | 2 | 3;
  /** Each ring or line as [x, y] pairs in tile units. */
  rings: [number, number][][];
}

export interface TileLayer {
  name: string;
  extent: number;
  features: TileFeature[];
}

/* ------------------------------------------------------------- protobuf wire ---- */

class Reader {
  pos = 0;
  readonly buf: Uint8Array;
  readonly end: number;
  constructor(buf: Uint8Array, end = buf.length) { this.buf = buf; this.end = end; }

  varint(): number {
    // Feature ids and lengths fit in 53 bits; a section handle is a small integer.
    let result = 0;
    let shift = 0;
    for (;;) {
      if (this.pos >= this.end) throw new Error("mvt: truncated varint");
      const b = this.buf[this.pos++]!;
      result += (b & 0x7f) * 2 ** shift;
      if (b < 0x80) return result;
      shift += 7;
      if (shift > 63) throw new Error("mvt: varint too long");
    }
  }

  /** Skip a field of this wire type. */
  skip(wire: number): void {
    if (wire === 0) this.varint();
    else if (wire === 1) this.pos += 8;
    else if (wire === 2) { const n = this.varint(); this.pos += n; }
    else if (wire === 5) this.pos += 4;
    else throw new Error(`mvt: unknown wire type ${wire}`);
  }

  /** A length-delimited field as its own reader over the same bytes. */
  sub(): Reader {
    const n = this.varint();
    const r = new Reader(this.buf, this.pos + n);
    r.pos = this.pos;
    this.pos += n;
    return r;
  }

  string(): string {
    const n = this.varint();
    const s = new TextDecoder().decode(this.buf.subarray(this.pos, this.pos + n));
    this.pos += n;
    return s;
  }
}

const zigzag = (n: number) => (n % 2 === 0 ? n / 2 : -(n + 1) / 2);

/** MVT 2.1 §4.3: MoveTo / LineTo / ClosePath commands into rings. */
export function decodeGeometry(cmds: readonly number[]): [number, number][][] {
  const rings: [number, number][][] = [];
  let ring: [number, number][] | null = null;
  let x = 0, y = 0, i = 0;
  while (i < cmds.length) {
    const head = cmds[i++]!;
    const op = head & 0x7;
    const count = head >> 3;
    if (op === 7) {                                   // ClosePath
      if (ring && ring.length) ring.push([ring[0]![0], ring[0]![1]]);
      continue;
    }
    for (let k = 0; k < count; k++) {
      x += zigzag(cmds[i++]!);
      y += zigzag(cmds[i++]!);
      if (op === 1) { ring = [[x, y]]; rings.push(ring); }       // MoveTo starts a part
      else if (op === 2 && ring) ring.push([x, y]);               // LineTo extends it
    }
  }
  return rings;
}

/** A feature's id and type, and WHERE its geometry is — not the geometry itself. */
function readFeature(r: Reader): { id: number | null; type: number; geometry: Reader | null } {
  let id: number | null = null, type = 0;
  let geometry: Reader | null = null;
  while (r.pos < r.end) {
    const key = r.varint();
    const field = key >> 3, wire = key & 0x7;
    if (field === 1 && wire === 0) id = r.varint();
    else if (field === 3 && wire === 0) type = r.varint();
    else if (field === 4 && wire === 2) geometry = r.sub();
    else r.skip(wire);
  }
  return { id, type, geometry };
}

function unpack(r: Reader): number[] {
  const out: number[] = [];
  while (r.pos < r.end) out.push(r.varint());
  return out;
}

/**
 * The layers of a tile, with only the features `want` accepts decoded.
 *
 * `layers` limits which layers are read at all (the rest are skipped by length, unparsed);
 * `want` is asked about each feature's id BEFORE its geometry is decoded, so a z12 tile of
 * a few thousand streams costs a scan of ids and the decoding of the handful wanted.
 */
export function decodeTile(bytes: Uint8Array, layers: ReadonlySet<string>,
                           want: (id: number) => boolean): TileLayer[] {
  const out: TileLayer[] = [];
  const top = new Reader(bytes);
  while (top.pos < top.end) {
    const key = top.varint();
    if (key >> 3 !== 3 || (key & 0x7) !== 2) { top.skip(key & 0x7); continue; }
    const lr = top.sub();
    let name = "", extent = 4096;
    const raw: Reader[] = [];
    while (lr.pos < lr.end) {
      const k = lr.varint();
      const field = k >> 3, wire = k & 0x7;
      if (field === 1 && wire === 2) name = lr.string();
      else if (field === 2 && wire === 2) raw.push(lr.sub());
      else if (field === 5 && wire === 0) extent = lr.varint();
      else lr.skip(wire);
    }
    // The name is read before any feature is touched — MVT writes it first in practice but
    // does not promise to, so features are held as unread slices until it is known.
    if (!layers.has(name)) continue;
    const features: TileFeature[] = [];
    for (const fr of raw) {
      const f = readFeature(fr);
      if (f.id === null || !want(f.id)) continue;
      if ((f.type !== 1 && f.type !== 2 && f.type !== 3) || !f.geometry) continue;
      features.push({ id: f.id, type: f.type, rings: decodeGeometry(unpack(f.geometry)) });
    }
    out.push({ name, extent, features });
  }
  return out;
}
