/**
 * (T686, owner 2026-11-16: "(a) Three lossless changes") THE THREE PACKED RELEASE TABLES, decoded. The writer is
 * pipeline/deliver/release/packed.py (its module doc is the layout); the encoders here are the same format written the
 * other way, so ONE fixture holds both sides byte for byte (dev/packed-parity.json: `app/tools/packed-parity.test.ts`
 * and `pipeline/tests/test_release_packed.py`). Every decoder THROWS on a blob that does not parse (AGENTS W46): a
 * half-read pointer or town list would be a wrong answer, never "nothing here".
 *
 *   near/2      `place_water (place_id, b)`: one row per town; `b` its waters (ords as gaps, ckm as two byte planes)
 *   down/2      `section_down (block, b)`: one row per 4096 sids; `b` each sid's down_sid, len and lake
 *   clim/3      `gauge_clim.bands` of a climate/2 file: clim/2's float16 bits transposed + delta
 *
 * The app reads the old formats too (`*_ALSO_READS`, the manifest's `readsRelease`): a release cut before T686 (the live
 * one when this app ships, the committed fixture releases) is read the old way, by its manifest's format.
 */

export const NEAR_FORMAT = "near/2";
export const DOWN_FORMAT = "down/2";
export const CLIMATE_FORMAT = "climate/2";
export const CLIM_CODEC = "clim/3";
/** The older formats this app still reads (the manifest's ALSO_READS; pipeline/tools/emit/app_reads.py reads these). */
export const NEAR_ALSO_READS: readonly string[] = ["near/1"];
export const DOWN_ALSO_READS: readonly string[] = ["down/1"];
export const CLIMATE_ALSO_READS: readonly string[] = ["climate/1"];

export const DOWN_BLOCK_BITS = 12;
const CKM_MAX = 2500;

class PackedError extends Error {
  constructor(message: string) { super(message); this.name = "PackedError"; }
}

/** An unsigned LEB128 reader, arithmetic only (no 32-bit bit operations). */
function reader(b: Uint8Array, what: string) {
  let pos = 0;
  return {
    uv(): number {
      let v = 0, mul = 1;
      for (;;) {
        if (pos >= b.length) throw new PackedError(`${what}: the blob ends inside a varint`);
        const c = b[pos++]!;
        v += (c & 0x7f) * mul;
        if (c < 0x80) return v;
        mul *= 128;
      }
    },
    take(n: number): Uint8Array {
      if (pos + n > b.length) throw new PackedError(`${what}: the blob ends ${pos + n - b.length} bytes early`);
      const out = b.subarray(pos, pos + n);
      pos += n;
      return out;
    },
    end(): void {
      if (pos !== b.length) throw new PackedError(`${what}: ${b.length - pos} bytes after the last column`);
    },
  };
}

function writer() {
  const out: number[] = [];
  return {
    out,
    uv(v: number, what: string): void {
      if (!Number.isSafeInteger(v) || v < 0) throw new PackedError(`${what}: ${v} is not an unsigned varint`);
      while (v >= 0x80) { out.push((v % 128) | 0x80); v = Math.floor(v / 128); }
      out.push(v);
    },
  };
}

const zz = (v: number) => (v >= 0 ? 2 * v : -2 * v - 1);
const unzz = (u: number) => (u % 2 === 0 ? u / 2 : -(u + 1) / 2);

// --- near/2 ---------------------------------------------------------------------------------------------------------

/** One town's waters: `ord[i]` at `ckm[i]` hundredths of a km, sorted by (ckm, ord) — near/1's primary-key order. */
export interface NearTown { readonly ord: Int32Array; readonly ckm: Uint16Array }

export function decodeNear(b: Uint8Array): NearTown {
  const r = reader(b, "near/2");
  const n = r.uv();
  const ords = new Int32Array(n);
  let prev = -1;
  for (let i = 0; i < n; i++) { prev = prev + 1 + r.uv(); ords[i] = prev; }
  const lo = r.take(n), hi = r.take(n);
  r.end();
  const idx = Array.from({ length: n }, (_, i) => i);
  const ckmAt = (i: number) => lo[i]! | (hi[i]! << 8);
  for (let i = 0; i < n; i++) if (ckmAt(i) > CKM_MAX) throw new PackedError(`near/2: ckm ${ckmAt(i)} past ${CKM_MAX}`);
  idx.sort((a, c) => ckmAt(a) - ckmAt(c) || ords[a]! - ords[c]!);
  return { ord: Int32Array.from(idx, (i) => ords[i]!), ckm: Uint16Array.from(idx, (i) => ckmAt(i)) };
}

/** The writer's half (tests, the parity fixture): one town's `[ckm, ord]` rows → its blob. */
export function encodeNear(rows: readonly (readonly [number, number])[]): Uint8Array {
  const rs = [...rows].sort((a, c) => a[1] - c[1]);
  const w = writer();
  w.uv(rs.length, "near/2");
  let prev = -1;
  for (const [ckm, o] of rs) {
    if (ckm < 0 || ckm > CKM_MAX) throw new PackedError(`near/2: ckm ${ckm} is outside 0..${CKM_MAX}`);
    if (o <= prev) throw new PackedError(`near/2: water ord ${o} twice for one town`);
    w.uv(o - prev - 1, "near/2");
    prev = o;
  }
  for (const [ckm] of rs) w.out.push(ckm & 0xff);
  for (const [ckm] of rs) w.out.push(ckm >> 8);
  return Uint8Array.from(w.out);
}

/**
 * EVERY town's waters, inverted: per water ord, the towns it comes near (`place`, `ckm`) — what near/1 answered by a
 * scan (`RINGS_FOR_ORDS`, `TOWNS_FOR_ORDS`). Built once from all the blobs; typed arrays, ~2.4 MB for 392k pairs.
 */
export interface NearByOrd {
  /** The towns of water `ord`, as [from, to) into `place` / `ckm` (in town order: place_id ascending). */
  range(ord: number): readonly [number, number];
  readonly place: Int32Array;
  readonly ckm: Uint16Array;
}

export function invertNear(rows: readonly { place_id: number; b: Uint8Array }[]): NearByOrd {
  const towns = rows.map((r) => ({ place: r.place_id, t: decodeNear(r.b) })).sort((a, c) => a.place - c.place);
  let maxOrd = -1, total = 0;
  for (const { t } of towns) { total += t.ord.length; for (const o of t.ord) if (o > maxOrd) maxOrd = o; }
  const start = new Int32Array(maxOrd + 2);
  for (const { t } of towns) for (const o of t.ord) start[o + 1]! += 1;
  for (let i = 1; i < start.length; i++) start[i]! += start[i - 1]!;
  const fill = start.slice(0, Math.max(0, maxOrd + 1));
  const place = new Int32Array(total), ckm = new Uint16Array(total);
  for (const { place: p, t } of towns)
    for (let i = 0; i < t.ord.length; i++) { const k = fill[t.ord[i]!]!++; place[k] = p; ckm[k] = t.ckm[i]!; }
  return {
    place, ckm,
    range: (ord) => (ord >= 0 && ord <= maxOrd ? [start[ord]!, start[ord + 1]!] : [0, 0]),
  };
}

// --- down/2 ---------------------------------------------------------------------------------------------------------

/** One block of down pointers: row i is `sid[i]` → `down[i]`, its length (`len`, null: none) and whether it is a lake. */
export interface DownBlock {
  readonly sid: Int32Array;
  readonly down: Float64Array;
  readonly len: readonly (number | null)[];
  readonly lake: Uint8Array;
}

export function decodeDownBlock(block: number, b: Uint8Array): DownBlock {
  const what = `down/2 block ${block}`;
  const r = reader(b, what);
  const n = r.uv(), base = block * 2 ** DOWN_BLOCK_BITS;
  const sid = new Int32Array(n);
  let prev = base - 1;
  for (let i = 0; i < n; i++) { prev = prev + 1 + r.uv(); sid[i] = prev; }
  if (n && sid[n - 1]! >= base + 2 ** DOWN_BLOCK_BITS) throw new PackedError(`${what}: sid ${sid[n - 1]} is past the block`);
  const down = new Float64Array(n);
  prev = base;
  for (let i = 0; i < n; i++) { prev += unzz(r.uv()); down[i] = prev; }
  const len: (number | null)[] = [];
  for (let i = 0; i < n; i++) { const v = r.uv(); len.push(v === 0 ? null : v - 1); }
  const lake = new Uint8Array(n);
  const lakes = r.uv();
  prev = -1;
  for (let k = 0; k < lakes; k++) {
    prev = prev + 1 + r.uv();
    if (prev >= n) throw new PackedError(`${what}: lake slot ${prev} past ${n} rows`);
    lake[prev] = 1;
  }
  r.end();
  return { sid, down, len, lake };
}

/** The row of `s` in its block (binary search over the block's sids); -1: no pointer from `s`. */
export function downSlot(b: DownBlock, s: number): number {
  let lo = 0, hi = b.sid.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1, v = b.sid[mid]!;
    if (v === s) return mid;
    if (v < s) lo = mid + 1; else hi = mid - 1;
  }
  return -1;
}

export type DownRow = readonly [sid: number, down: number, len: number | null, lake: 1 | null];

/** The writer's half (tests, the parity fixture): one block's rows, sid ascending, → its blob. */
export function encodeDownBlock(block: number, rows: readonly DownRow[]): Uint8Array {
  const base = block * 2 ** DOWN_BLOCK_BITS, w = writer();
  w.uv(rows.length, "down/2");
  let prev = base - 1;
  for (const [s] of rows) {
    if (Math.floor(s / 2 ** DOWN_BLOCK_BITS) !== block || s <= prev)
      throw new PackedError(`down/2: sid ${s} is not after ${prev} in block ${block}`);
    w.uv(s - prev - 1, "down/2");
    prev = s;
  }
  prev = base;
  for (const [, d] of rows) { w.uv(zz(d - prev), "down/2"); prev = d; }
  for (const [, , l] of rows) w.uv(l === null ? 0 : l + 1, "down/2 len");
  const lakes = rows.flatMap((r, i) => (r[3] === 1 ? [i] : []));
  w.uv(lakes.length, "down/2");
  prev = -1;
  for (const i of lakes) { w.uv(i - prev - 1, "down/2"); prev = i; }
  return Uint8Array.from(w.out);
}

// --- clim/3 ---------------------------------------------------------------------------------------------------------

/** A clim/3 blob → the clim/2 blob it was made from ([u8 pentad, 5 × f16 LE] records), byte for byte. */
export function unclim3(b: Uint8Array): Uint8Array {
  if (!b.length) throw new PackedError("clim/3: an empty blob");
  const n = b[0]!;
  if (b.length !== 1 + 11 * n) throw new PackedError(`clim/3: ${b.length} bytes is not a blob of ${n} pentads`);
  const lo = 1 + n, hi = 1 + 6 * n;
  const out = new Uint8Array(11 * n);
  for (let c = 0; c < 5; c++) {
    let prev = 0;
    for (let i = 0; i < n; i++) {
      const k = c * n + i;
      prev = (prev + (b[lo + k]! | (b[hi + k]! << 8))) & 0xffff;
      out[11 * i + 1 + 2 * c] = prev & 0xff;
      out[11 * i + 2 + 2 * c] = prev >> 8;
    }
  }
  for (let i = 0; i < n; i++) out[11 * i] = b[1 + i]!;
  return out;
}

/** The writer's half (tests, the parity fixture): a clim/2 blob → clim/3. */
export function clim3(clim2: Uint8Array): Uint8Array {
  if (clim2.length % 11) throw new PackedError(`clim/3: a clim/2 blob of ${clim2.length} bytes is not whole records`);
  const n = clim2.length / 11;
  if (n > 255) throw new PackedError(`clim/3: ${n} pentads`);
  const h = (i: number, c: number) => clim2[11 * i + 1 + 2 * c]! | (clim2[11 * i + 2 + 2 * c]! << 8);
  const deltas: number[] = [];
  for (let c = 0; c < 5; c++) {
    let prev = 0;
    for (let i = 0; i < n; i++) { deltas.push((h(i, c) - prev) & 0xffff); prev = h(i, c); }
  }
  const out = [n, ...Array.from({ length: n }, (_, i) => clim2[11 * i]!), ...deltas.map((d) => d & 0xff),
               ...deltas.map((d) => d >> 8)];
  return Uint8Array.from(out);
}
