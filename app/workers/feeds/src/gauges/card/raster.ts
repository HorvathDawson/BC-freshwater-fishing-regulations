// T487: A TINY RASTERISER FOR THE GAUGE SHARE CARDS — the worker has no canvas and no font engine, and the owner's rule
// is that a card costs almost no compute (owner 2026-11-08). A card is a palette image (one byte a pixel, at most 256
// colours: solid fills, pre-drawn letters from glyphs.generated.ts, anti-aliased strokes), encoded as an indexed PNG with
// the runtime's own zlib (CompressionStream "deflate"). A 1200 x 630 card draws and encodes in a few milliseconds.
import { FACES, type FaceKey } from "./glyphs.generated";

export type Rgb = readonly [number, number, number];

export function hex(c: string): Rgb {
  const m = /^#([0-9a-f]{6})$/i.exec(c);
  if (!m) throw new Error(`card: ${c} is not a #rrggbb colour`);
  const n = parseInt(m[1]!, 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

const LEVELS = 16;   // alpha steps a blended pixel is rounded to (keeps the palette small)

async function inflate(b64: string): Promise<Uint8Array> {
  const bin = atob(b64), bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  const out = new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream("deflate")));
  return new Uint8Array(await out.arrayBuffer());
}

const bitmaps = new Map<FaceKey, Promise<Uint8Array>>();
/** a face's bitmaps, inflated once per isolate */
export function faceBitmaps(k: FaceKey): Promise<Uint8Array> {
  let p = bitmaps.get(k);
  if (!p) bitmaps.set(k, (p = inflate(FACES[k].data)));
  return p;
}

export class Canvas {
  readonly px: Uint8Array;
  readonly palette: number[][] = [];
  /** characters asked for that no face holds (drawn as "?"): the run reports the card (no silent loss) */
  readonly missing = new Set<string>();
  private readonly index = new Map<number, number>();
  constructor(readonly w: number, readonly h: number, ground: Rgb) {
    this.px = new Uint8Array(w * h);
    this.colour(ground);
  }

  /** the palette entry of a colour (added when new; the nearest one when the palette is full) */
  colour(c: Rgb): number {
    const key = (c[0] << 16) | (c[1] << 8) | c[2];
    const hit = this.index.get(key);
    if (hit !== undefined) return hit;
    if (this.palette.length < 256) {
      this.palette.push([c[0], c[1], c[2]]);
      this.index.set(key, this.palette.length - 1);
      return this.palette.length - 1;
    }
    let best = 0, bd = Infinity;
    this.palette.forEach((p, i) => { const d = (p[0]! - c[0]) ** 2 + (p[1]! - c[1]) ** 2 + (p[2]! - c[2]) ** 2; if (d < bd) { bd = d; best = i; } });
    this.index.set(key, best);
    return best;
  }

  fill(x: number, y: number, w: number, h: number, c: Rgb): void {
    const i = this.colour(c);
    const x0 = Math.max(0, Math.round(x)), x1 = Math.min(this.w, Math.round(x + w));
    const y0 = Math.max(0, Math.round(y)), y1 = Math.min(this.h, Math.round(y + h));
    for (let yy = y0; yy < y1; yy++) this.px.fill(i, yy * this.w + x0, yy * this.w + x1);
  }

  /** colour `c` over pixel (x, y) at coverage `a` (0..1) */
  blend(x: number, y: number, c: Rgb, a: number): void {
    if (x < 0 || y < 0 || x >= this.w || y >= this.h || a <= 0) return;
    const q = Math.round(Math.min(1, a) * LEVELS) / LEVELS;
    if (q <= 0) return;
    const p = y * this.w + x;
    if (q >= 1) { this.px[p] = this.colour(c); return; }
    const b = this.palette[this.px[p]!]!;
    this.px[p] = this.colour([Math.round(b[0]! + (c[0] - b[0]!) * q), Math.round(b[1]! + (c[1] - b[1]!) * q), Math.round(b[2]! + (c[2] - b[2]!) * q)]);
  }

  /** a coverage mask (one float a pixel over the canvas) composited once: strokes that cross do not darken twice */
  composite(mask: Float32Array, c: Rgb): void {
    for (let p = 0; p < mask.length; p++) if (mask[p]! > 0) this.blend(p % this.w, (p / this.w) | 0, c, mask[p]!);
  }
}

/** text's width in a face, px */
export function measure(k: FaceKey, s: string): number {
  const f = FACES[k];
  let w = 0;
  for (const ch of s) {
    const i = f.chars.indexOf(ch);
    w += i >= 0 ? f.adv[i]! : f.adv[f.chars.indexOf("?")] ?? 0;
  }
  return w;
}

/** the longest start of `s` (whole words, then "…") that fits `max` px */
export function fit(k: FaceKey, s: string, max: number): string {
  if (measure(k, s) <= max) return s;
  const words = s.split(" ");
  while (words.length > 1) { words.pop(); const t = `${words.join(" ")}…`; if (measure(k, t) <= max) return t; }
  let t = s;
  while (t.length > 1 && measure(k, `${t}…`) > max) t = t.slice(0, -1);
  return `${t}…`;
}

/** split `s` into at most `lines` lines of `max` px (the last one cut with "…" when the rest does not fit) */
export function wrap(k: FaceKey, s: string, max: number, lines: number): string[] {
  const words = s.split(/\s+/).filter(Boolean), out: string[] = [];
  let cur = "";
  for (const w of words) {
    const next = cur ? `${cur} ${w}` : w;
    if (!cur || measure(k, next) <= max) cur = next;
    else { out.push(cur); cur = w; }
  }
  if (cur) out.push(cur);
  if (out.length <= lines) return out.map((l) => fit(k, l, max));
  return [...out.slice(0, lines - 1).map((l) => fit(k, l, max)), fit(k, out.slice(lines - 1).join(" "), max)];
}

/**
 * Draw `s` with its baseline at `y`, starting at `x`; returns the width drawn. A character the face does not hold is
 * drawn as "?" AND kept in `cv.missing`, which the card run reports.
 */
export async function text(cv: Canvas, k: FaceKey, s: string, x: number, y: number, c: Rgb): Promise<number> {
  const f = FACES[k], bm = await faceBitmaps(k);
  let pen = Math.round(x);
  const q = f.chars.indexOf("?");
  for (const ch of s) {
    let i = f.chars.indexOf(ch);
    if (i < 0) { cv.missing.add(ch); i = q; }
    const [l, t, w, h, off] = f.box[i]!;
    for (let yy = 0; yy < h; yy++)
      for (let xx = 0; xx < w; xx++) {
        const a = bm[off + yy * w + xx]!;
        if (a) cv.blend(Math.round(pen + l + xx), Math.round(y + t + yy), c, a / 255);
      }
    pen += f.adv[i]!;
  }
  return pen - Math.round(x);
}

export type Pt = readonly [number, number];

/** an anti-aliased polyline of `width` px into a coverage mask; `dash` = [on, off] px along the line */
export function stroke(cv: Canvas, pts: readonly Pt[], width: number, mask: Float32Array<ArrayBuffer> = new Float32Array(cv.w * cv.h), dash?: readonly [number, number]): Float32Array<ArrayBuffer> {
  const r = width / 2;
  let along = 0;
  const seg = (a: Pt, b: Pt) => {
    const x0 = Math.max(0, Math.floor(Math.min(a[0], b[0]) - r - 1)), x1 = Math.min(cv.w - 1, Math.ceil(Math.max(a[0], b[0]) + r + 1));
    const y0 = Math.max(0, Math.floor(Math.min(a[1], b[1]) - r - 1)), y1 = Math.min(cv.h - 1, Math.ceil(Math.max(a[1], b[1]) + r + 1));
    const dx = b[0] - a[0], dy = b[1] - a[1], len2 = dx * dx + dy * dy;
    for (let y = y0; y <= y1; y++)
      for (let x = x0; x <= x1; x++) {
        const px = x + 0.5, py = y + 0.5;
        const t = len2 ? Math.max(0, Math.min(1, ((px - a[0]) * dx + (py - a[1]) * dy) / len2)) : 0;
        const d = Math.hypot(px - (a[0] + t * dx), py - (a[1] + t * dy));
        const cov = Math.max(0, Math.min(1, r + 0.5 - d));
        const p = y * cv.w + x;
        if (cov > mask[p]!) mask[p] = cov;
      }
  };
  for (let i = 1; i < pts.length; i++) {
    const a = pts[i - 1]!, b = pts[i]!;
    if (!dash) { seg(a, b); continue; }
    const len = Math.hypot(b[0] - a[0], b[1] - a[1]), period = dash[0] + dash[1];
    let s = 0;
    while (s < len) {
      const phase = (along + s) % period;
      const on = phase < dash[0];
      const step = Math.min(len - s, on ? dash[0] - phase : period - phase);
      if (on && len) {
        const t0 = s / len, t1 = (s + step) / len;
        seg([a[0] + (b[0] - a[0]) * t0, a[1] + (b[1] - a[1]) * t0], [a[0] + (b[0] - a[0]) * t1, a[1] + (b[1] - a[1]) * t1]);
      }
      s += step || len;
    }
    along += len;
  }
  return mask;
}

/** fill from the line down to `bottom` (columns between the points), colour `c` at `a` */
export function fillUnder(cv: Canvas, pts: readonly Pt[], bottom: number, c: Rgb, a: number): void {
  for (let i = 1; i < pts.length; i++) {
    const p = pts[i - 1]!, q = pts[i]!;
    for (let x = Math.ceil(p[0]); x < Math.ceil(q[0]); x++) {
      const t = q[0] === p[0] ? 0 : (x - p[0]) / (q[0] - p[0]);
      const y = p[1] + (q[1] - p[1]) * t;
      for (let yy = Math.ceil(y); yy < bottom; yy++) cv.blend(x, yy, c, a);
    }
  }
}

/** fill between two lines that share their x positions (`upper` and `lower`, same length), colour `c` at `a`; the y of each
 * line is clamped to [top, bottom] so a band running off the chart's scale is cut at its edge */
export function fillBand(cv: Canvas, upper: readonly Pt[], lower: readonly Pt[], top: number, bottom: number, c: Rgb, a: number): void {
  for (let i = 1; i < upper.length; i++) {
    const p = upper[i - 1]!, q = upper[i]!, r = lower[i - 1]!, s = lower[i]!;
    for (let x = Math.ceil(p[0]); x < Math.ceil(q[0]); x++) {
      const t = q[0] === p[0] ? 0 : (x - p[0]) / (q[0] - p[0]);
      const y0 = Math.max(top, Math.min(bottom, p[1] + (q[1] - p[1]) * t)), y1 = Math.max(top, Math.min(bottom, r[1] + (s[1] - r[1]) * t));
      for (let yy = Math.ceil(Math.min(y0, y1)); yy < Math.max(y0, y1); yy++) cv.blend(x, yy, c, a);
    }
  }
}

// ── PNG (indexed colour) ────────────────────────────────────────────────────────────────────────────────────────────
const CRC = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c >>> 0; }
  return t;
})();
function crc32(b: Uint8Array): number {
  let c = 0xffffffff;
  for (let i = 0; i < b.length; i++) c = CRC[(c ^ b[i]!) & 255]! ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}
function chunk(type: string, data: Uint8Array): Uint8Array {
  const out = new Uint8Array(12 + data.length), v = new DataView(out.buffer);
  v.setUint32(0, data.length);
  for (let i = 0; i < 4; i++) out[4 + i] = type.charCodeAt(i);
  out.set(data, 8);
  v.setUint32(8 + data.length, crc32(out.subarray(4, 8 + data.length)));
  return out;
}
async function deflate(raw: Uint8Array<ArrayBuffer>): Promise<Uint8Array> {
  const s = new Blob([raw]).stream().pipeThrough(new CompressionStream("deflate"));
  return new Uint8Array(await new Response(s).arrayBuffer());
}

export async function png(cv: Canvas): Promise<Uint8Array> {
  const ihdr = new Uint8Array(13), v = new DataView(ihdr.buffer);
  v.setUint32(0, cv.w); v.setUint32(4, cv.h);
  ihdr[8] = 8; ihdr[9] = 3;                                      // 8-bit, indexed
  const plte = new Uint8Array(cv.palette.length * 3);
  cv.palette.forEach((c, i) => plte.set(c, i * 3));
  const raw = new Uint8Array((cv.w + 1) * cv.h);
  for (let y = 0; y < cv.h; y++) raw.set(cv.px.subarray(y * cv.w, (y + 1) * cv.w), y * (cv.w + 1) + 1);   // filter 0
  const parts = [new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), chunk("IHDR", ihdr), chunk("PLTE", plte),
                 chunk("IDAT", await deflate(raw)), chunk("IEND", new Uint8Array(0))];
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}
