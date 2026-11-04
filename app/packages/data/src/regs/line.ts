/**
 * (T369, owner 2026-11-03) A PLACE ON A STREAM: the FWA stream key (`blue_line_key`) and a distance along it (the
 * downstream route measure, metres). A durable FWA location — NOT a section id (AGENTS rule 5): a link may carry it,
 * and the app resolves it to the section of the release it holds that spans the distance. Sections are re-cut between
 * releases; the stream key and its measures are FWA's and do not move.
 *
 * THE RELEASE FILE `lines` (`lines/1`; pipeline/deliver/release/lines.py — its writer, held to `encodeLineBlock` byte for
 * byte by `app/tools/section-line-parity.test.ts` / `test_section_line.py`). Its own file, fetched only when a link is made or opened; a
 * release cut before it has none, and then no link carries a place:
 *
 *   section_line (block INTEGER PRIMARY KEY, n INTEGER NOT NULL, blk0 INTEGER NOT NULL, m0 INTEGER NOT NULL,
 *                 b BLOB NOT NULL)          + INDEX section_line_head ON section_line (blk0, m0)
 *
 * One row per block of 4096 section handles (`h >> BLOCK_BITS`, as `section_akey`), stream handles only (a lake's
 * handle has no line). `n`: the slots used (the last stream block is short; slot i is handle `block << 12 | i`; slot 0
 * of block 0 — handle 0, no section — is a filler spanning nothing). `blk0` / `m0`: slot 0's stream key and start — the
 * block's head, so a place's block is ONE indexed read (the atlas's handles are sorted by stream key, then start: the
 * last head at or below the place).
 *
 * `b` (not deflated: the file ships gzipped) is columns of unsigned LEB128 varints; `zz` is zigzag for a signed value,
 * all of it ARITHMETIC (a stream key can be below −2^31, past 32-bit bit operations). A RUN is the slots in a row with
 * one stream key (slots are in strict (key, start) order):
 *
 *   R                                   the runs
 *   R × zz(key − previous run's key) × 8 + multi × 4 + closed × 2 + nonzero
 *                                       (first from 0); multi: 2+ slots; closed: the last slot's end is stored;
 *                                       nonzero: the first slot's start is not 0
 *   count, per multi run                its slots (they sum to n)
 *   starts                              per run: the first start (only when nonzero), then each next start − the last
 *   length, per closed run              its last slot's end − start
 *   E, then E × (slot gap, zz(end − next start))
 *                                       every other slot ends where the next starts but these (a lake between two
 *                                       pieces); slot gap = slot − the previous exception's − 1 (the first's from −1)
 *
 * A run not closed ends OPEN (`LINE_OPEN`): the stream has ONE section in this release (most streams), so its key alone
 * finds it; its place is its start (`lineMid`).
 *
 * `encodeLineBlock` is the writer's half, kept beside the reader so the format is pinned in one place (the pipeline's
 * writer is held to it; tests build fixture tables with it).
 */
export interface StreamPlace { readonly blk: number; readonly m: number }
/** A stream section's line: its stream key and its measure span [from, to), metres. */
export interface SectionLine { readonly blk: number; readonly from: number; readonly to: number }

/** The end of a stream's one section in this release: open, to the stream's end (no length is shipped for it). */
export const LINE_OPEN = 0xffffffff;

/** A section's middle, in whole metres: the place a link to it carries; an open section's (its stream's only one in this
 *  release), its start — a real point on the stream that this release resolves back to it. */
export const lineMid = (l: SectionLine): StreamPlace =>
  ({ blk: l.blk, m: l.to === LINE_OPEN ? l.from : Math.round((l.from + l.to) / 2) });

export interface LineBlock { readonly n: number; readonly blk: Float64Array; readonly from: Uint32Array; readonly to: Uint32Array }

export function decodeLineBlock(b: Uint8Array, n: number): LineBlock {
  let pos = 0;
  const uv = (): number => {
    let v = 0, mul = 1;
    for (;;) {
      if (pos >= b.length) throw new Error("section_line: a block ends inside a varint");
      const c = b[pos++]!;
      v += (c & 0x7f) * mul;
      if (c < 0x80) return v;
      mul *= 128;
    }
  };
  const r = uv(), keys: number[] = [], multi: boolean[] = [], closed: boolean[] = [], nz: boolean[] = [];
  let k = 0;
  for (let i = 0; i < r; i++) {
    const h = uv(), f = h % 8;
    k += unzz((h - f) / 8);
    keys.push(k); multi.push((f & 4) !== 0); closed.push((f & 2) !== 0); nz.push((f & 1) !== 0);
  }
  const counts = multi.map((m) => (m ? uv() : 1));
  if (counts.reduce((a, c) => a + c, 0) !== n) throw new Error(`section_line: a block of ${n} slots has other runs`);
  const blk = new Float64Array(n), from = new Uint32Array(n), to = new Uint32Array(n).fill(LINE_OPEN);
  let i = 0;
  for (let j = 0; j < r; j++) {
    for (let s = 0; s < counts[j]!; s++, i++) {
      blk[i] = keys[j]!;
      from[i] = s === 0 ? (nz[j] ? uv() : 0) : from[i - 1]! + uv();
    }
  }
  i = 0;
  for (let j = 0; j < r; j++) {
    const c = counts[j]!;
    for (let s = i; s < i + c - 1; s++) to[s] = from[s + 1]!;
    if (closed[j]) to[i + c - 1] = from[i + c - 1]! + uv();
    i += c;
  }
  let p = -1;
  for (let e = uv(); e > 0; e--) {
    p += 1 + uv();
    to[p] = from[p + 1]! + unzz(uv());
  }
  if (pos !== b.length) throw new Error(`section_line: ${b.length - pos} bytes after a block's last column`);
  return { n, blk, from, to };
}

const zz = (v: number): number => (v >= 0 ? 2 * v : -2 * v - 1);
const unzz = (u: number): number => (u % 2 === 0 ? u / 2 : -(u + 1) / 2);

/** The writer's half: `lines[i]` is slot i, in strict (key, start) order; only a run's last slot may end `LINE_OPEN`. */
export function encodeLineBlock(lines: readonly SectionLine[]): Uint8Array {
  const out: number[] = [];
  const uv = (v: number, into: number[] = out) => {
    if (!(v >= 0) || !Number.isSafeInteger(v)) throw new Error(`section_line: ${v} is not an unsigned varint`);
    while (v >= 0x80) { into.push((v % 128) | 0x80); v = Math.floor(v / 128); }
    into.push(v);
  };
  const runs: { k: number; i: number; c: number }[] = [];
  lines.forEach((l, i) => {
    const last = runs[runs.length - 1];
    if (last && last.k === l.blk) last.c++; else runs.push({ k: l.blk, i, c: 1 });
  });
  const heads: number[] = [], counts: number[] = [], starts: number[] = [], lengths: number[] = [];
  const exc: [number, number][] = [];
  let prev = 0;
  for (const { k, i, c } of runs) {
    const lo = lines[i]!.from, end = lines[i + c - 1]!, closed = end.to !== LINE_OPEN;
    for (let j = i; j < i + c - 1; j++)
      if (lines[j]!.to === LINE_OPEN) throw new Error(`section_line: stream ${k}: only a run's last section may be open`);
    uv(zz(k - prev) * 8 + (c > 1 ? 4 : 0) + (closed ? 2 : 0) + (lo ? 1 : 0), heads);
    prev = k;
    if (c > 1) uv(c, counts);
    if (lo) uv(lo, starts);
    for (let j = i + 1; j < i + c; j++) {
      uv(lines[j]!.from - lines[j - 1]!.from, starts);
      if (lines[j - 1]!.to !== lines[j]!.from) exc.push([j - 1, lines[j - 1]!.to - lines[j]!.from]);
    }
    if (closed) uv(end.to - end.from, lengths);
  }
  uv(runs.length);
  out.push(...heads, ...counts, ...starts, ...lengths);
  uv(exc.length);
  let p = -1;
  for (const [i, d] of exc) { uv(i - p - 1); uv(zz(d)); p = i; }
  return Uint8Array.from(out);
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
