/**
 * (T369, owner 2026-11-03) A PLACE ON A STREAM: the FWA stream key (`blue_line_key`) and a distance along it (the
 * downstream route measure, metres). A durable FWA location — NOT a section id (AGENTS rule 5): a link may carry it,
 * and the app resolves it to the section of the release it holds that spans the distance. Sections are re-cut between
 * releases; the stream key and its measures are FWA's and do not move.
 *
 * THE RELEASE TABLE (regs-sid, additive — a release cut before it has none, and then no link carries a place):
 *
 *   section_line (block INTEGER PRIMARY KEY, n INTEGER NOT NULL, blk0 INTEGER NOT NULL, m0 INTEGER NOT NULL,
 *                 b BLOB NOT NULL)          + INDEX section_line_head ON section_line (blk0, m0)
 *
 * One row per block of 4096 section handles (`h >> BLOCK_BITS`, as `section_akey`), stream handles only (a lake's
 * handle has no line). `n`: the slots used (the last stream block is short; slot i is handle `block << 12 | i`).
 * `blk0` / `m0`: slot 0's stream key and start — the block's head, so a place's block is ONE indexed read (the atlas's
 * handles are sorted by stream key, then start: the last head at or below the place). `b`: the blob codec (`blob.ts`:
 * 0x01 raw deflate — always: a raw float64's first byte may be a codec byte), inflating to `n` float64 LE stream keys (some are below −2^31), then `n` uint32 LE starts,
 * then `n` uint32 LE ends — each section's measure span [from, to) in whole metres.
 *
 * `encodeLineBlock` is the writer's half, kept beside the reader so the format is pinned in one place (the pipeline's
 * writer is held to it; tests build fixture tables with it).
 */
import { deflateSync } from "fflate";
import { inflateBlob } from "./blob";

export interface StreamPlace { readonly blk: number; readonly m: number }
/** A stream section's line: its stream key and its measure span [from, to), metres. */
export interface SectionLine { readonly blk: number; readonly from: number; readonly to: number }

/** A section's middle, in whole metres: the place a link to it carries. */
export const lineMid = (l: SectionLine): StreamPlace => ({ blk: l.blk, m: Math.round((l.from + l.to) / 2) });

export interface LineBlock { readonly n: number; readonly blk: Float64Array; readonly from: Uint32Array; readonly to: Uint32Array }

export function decodeLineBlock(b: Uint8Array, n: number): LineBlock {
  const raw = inflateBlob(b, null);
  if (raw.length !== n * 16) throw new Error(`section_line: a block of ${n} slots is ${raw.length} bytes, not ${n * 16}`);
  const v = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  const blk = new Float64Array(n), from = new Uint32Array(n), to = new Uint32Array(n);
  for (let i = 0; i < n; i++) {
    blk[i] = v.getFloat64(8 * i, true);
    from[i] = v.getUint32(8 * n + 4 * i, true);
    to[i] = v.getUint32(12 * n + 4 * i, true);
  }
  return { n, blk, from, to };
}

/** The writer's half: `lines[i]` is slot i, deflated under the blob codec's 0x01 head. */
export function encodeLineBlock(lines: readonly SectionLine[]): Uint8Array {
  const n = lines.length, out = new Uint8Array(16 * n), v = new DataView(out.buffer);
  lines.forEach((l, i) => {
    v.setFloat64(8 * i, l.blk, true);
    v.setUint32(8 * n + 4 * i, l.from, true);
    v.setUint32(12 * n + 4 * i, l.to, true);
  });
  const z = deflateSync(out, { level: 9 }), b = new Uint8Array(z.length + 1);
  b[0] = 0x01; b.set(z, 1);
  return b;
}

/** The slot of the section of `blk` whose span holds `m`, in a block whose head is at or below (blk, m); -1: none. */
export function slotAt(b: LineBlock, blk: number, m: number): number {
  let lo = 0, hi = b.n - 1, at = -1;
  while (lo <= hi) {                 // the last slot at or below (blk, m)
    const mid = (lo + hi) >> 1;
    const below = b.blk[mid]! < blk || (b.blk[mid] === blk && b.from[mid]! <= m);
    if (below) { at = mid; lo = mid + 1; } else hi = mid - 1;
  }
  return at >= 0 && b.blk[at] === blk && b.from[at]! <= m && m < b.to[at]! ? at : -1;
}
