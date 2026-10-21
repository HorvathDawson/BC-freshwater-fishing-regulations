/**
 * A Mapbox Vector Tile WRITER, for tests only — the inverse of `mvt.ts`, so a test can state
 * the tile it reads instead of shipping opaque bytes.
 *
 * Writes exactly the MVT 2.1 fields the reader looks at (layer name, extent, feature id,
 * type, geometry) plus a tag, so the reader is proven to skip what it does not need.
 */
type Pt = [number, number];

const varint = (n: number, out: number[]) => {
  while (n >= 0x80) { out.push((n % 0x80) | 0x80); n = Math.floor(n / 0x80); }
  out.push(n);
};
const key = (field: number, wire: number, out: number[]) => varint((field << 3) | wire, out);
const bytes = (field: number, b: number[], out: number[]) => {
  key(field, 2, out); varint(b.length, out); out.push(...b);
};
const zz = (n: number) => (n >= 0 ? n * 2 : -n * 2 - 1);

/** MoveTo/LineTo(/ClosePath) commands for parts in tile units. */
export function commands(parts: readonly Pt[][], close: boolean): number[] {
  const out: number[] = [];
  let x = 0, y = 0;
  for (const part of parts) {
    const pts = close ? part.slice(0, -1) : part;          // a ring's closing point is implied
    out.push((1 & 0x7) | (1 << 3), zz(pts[0]![0] - x), zz(pts[0]![1] - y));
    x = pts[0]![0]; y = pts[0]![1];
    out.push((2 & 0x7) | ((pts.length - 1) << 3));
    for (const p of pts.slice(1)) { out.push(zz(p[0] - x), zz(p[1] - y)); x = p[0]; y = p[1]; }
    if (close) out.push(7 | (1 << 3));
  }
  return out;
}

export interface FixtureFeature { id: number; type: 2 | 3; parts: Pt[][] }

/** One tile of the given layers. */
export function encodeTile(layers: Record<string, FixtureFeature[]>, extent = 4096): Uint8Array {
  const tile: number[] = [];
  for (const [name, features] of Object.entries(layers)) {
    const layer: number[] = [];
    key(15, 0, layer); varint(2, layer);                      // version
    bytes(1, [...new TextEncoder().encode(name)], layer);
    for (const f of features) {
      const feat: number[] = [];
      key(1, 0, feat); varint(f.id, feat);
      const tags: number[] = []; varint(0, tags); varint(0, tags);
      bytes(2, tags, feat);
      key(3, 0, feat); varint(f.type, feat);
      const g: number[] = [];
      for (const c of commands(f.parts, f.type === 3)) varint(c, g);
      bytes(4, g, feat);
      bytes(2, feat, layer);
    }
    bytes(3, [...new TextEncoder().encode("section_id")], layer);    // keys
    const v: number[] = []; key(6, 0, v); varint(1, v);               // values: uint 1
    bytes(4, v, layer);
    key(5, 0, layer); varint(extent, layer);
    bytes(3, layer, tile);
  }
  return new Uint8Array(tile);
}
