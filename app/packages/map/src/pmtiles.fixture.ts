/**
 * (T831) A PMTiles v3 ARCHIVE WRITER, for tests only — the inverse of what `pmtiles` reads, so a test can put a stated
 * archive on disk (or in memory) and read it back through the same `PMTiles` the map uses. Uncompressed, one root
 * directory, every tile its own entry; `pad` bytes of filler after the tiles make the archive larger than `pmtiles`'
 * first read (16 KiB), so a byte-range reader is proven to be asked for ranges, not the whole file.
 */
import { zxyToTileId } from "pmtiles";

const varint = (n: number, out: number[]) => {
  while (n >= 0x80) { out.push((n % 0x80) | 0x80); n = Math.floor(n / 0x80); }
  out.push(n);
};

const HEADER = 127;

export function encodePmtiles(tiles: { z: number; x: number; y: number; data: Uint8Array }[], pad = 0): Uint8Array {
  const entries = tiles.map((t) => ({ id: zxyToTileId(t.z, t.x, t.y), data: t.data })).sort((a, b) => a.id - b.id);
  let at = 0;
  const placed = entries.map((e) => { const p = { ...e, offset: at }; at += e.data.length; return p; });
  const dir: number[] = [];
  varint(placed.length, dir);
  let last = 0;
  for (const e of placed) { varint(e.id - last, dir); last = e.id; }
  for (const _ of placed) varint(1, dir);
  for (const e of placed) varint(e.data.length, dir);
  placed.forEach((e, i) => varint(i > 0 && e.offset === placed[i - 1]!.offset + placed[i - 1]!.data.length ? 0 : e.offset + 1, dir));
  const tileStart = HEADER + dir.length;
  const tileBytes = at + pad;
  const out = new Uint8Array(tileStart + tileBytes);
  const v = new DataView(out.buffer);
  out.set([0x50, 0x4d, 0x54, 0x69, 0x6c, 0x65, 0x73, 3]);                       // "PMTiles", v3
  const u64 = (o: number, n: number) => v.setBigUint64(o, BigInt(n), true);
  u64(8, HEADER); u64(16, dir.length);                                             // root directory
  u64(24, tileStart); u64(32, 0);                                                  // no metadata
  u64(40, tileStart); u64(48, 0);                                                  // no leaf directories
  u64(56, tileStart); u64(64, tileBytes);                                          // tile data
  u64(72, placed.length); u64(80, placed.length); u64(88, placed.length);
  v.setUint8(96, 1);                                                               // clustered
  v.setUint8(97, 1); v.setUint8(98, 1); v.setUint8(99, 1);                         // no compression, MVT
  v.setUint8(100, 0); v.setUint8(101, 14);                                         // z0–14
  v.setInt32(102, -180e7, true); v.setInt32(106, -85e7, true); v.setInt32(110, 180e7, true); v.setInt32(114, 85e7, true);
  out.set(dir, HEADER);
  for (const e of placed) out.set(e.data, tileStart + e.offset);
  return out;
}
