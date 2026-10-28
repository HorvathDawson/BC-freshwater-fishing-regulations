/**
 * RGBA PIXELS AS A PNG `data:` URI (T74) — how the map's generated images (the hatches, the gauge pill, the route's
 * "you are here" pin: `images.ts`) reach MapLibre Native, whose binding takes images by URI (`Images`), never raw pixels
 * the way `map.addImage` does on the web. The same pixels, so the two maps draw the same marks.
 *
 * Uncompressed on purpose: the images are 32–48 px squares (≤ 9 KB of RGBA), so deflate's "stored" blocks cost a few
 * bytes over the pixels and need no compressor in the bundle. Straight (non-premultiplied) alpha, as PNG defines it
 * and as the generators write it.
 */

let crcTable: Uint32Array | null = null;
function crc32(b: Uint8Array, start: number, end: number): number {
  if (!crcTable) {
    crcTable = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      crcTable[n] = c >>> 0;
    }
  }
  let c = 0xffffffff;
  for (let i = start; i < end; i++) c = crcTable[(c ^ b[i]!) & 0xff]! ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function adler32(b: Uint8Array): number {
  let a = 1, s = 0;
  for (let i = 0; i < b.length; i++) { a = (a + b[i]!) % 65521; s = (s + a) % 65521; }
  return ((s << 16) | a) >>> 0;
}

const be32 = (out: number[], v: number) => out.push((v >>> 24) & 255, (v >>> 16) & 255, (v >>> 8) & 255, v & 255);

/** A zlib stream of `raw` in stored (uncompressed) deflate blocks. */
function zlibStored(raw: Uint8Array): number[] {
  const out = [0x78, 0x01];
  for (let i = 0; i < raw.length || i === 0; i += 65535) {
    const n = Math.min(65535, raw.length - i);
    out.push(i + n >= raw.length ? 1 : 0, n & 255, n >>> 8, ~n & 255, (~n >>> 8) & 255);
    for (let j = 0; j < n; j++) out.push(raw[i + j]!);
    if (n === 0) break;
  }
  be32(out, adler32(raw));
  return out;
}

function chunk(out: number[], type: string, data: ArrayLike<number>): void {
  be32(out, data.length);
  const start = out.length;
  for (let i = 0; i < 4; i++) out.push(type.charCodeAt(i));
  for (let i = 0; i < data.length; i++) out.push(data[i]!);
  be32(out, crc32(Uint8Array.from(out), start, out.length));
}

/** The PNG file for a `width` × `height` RGBA image. */
export function pngBytes(width: number, height: number, rgba: Uint8Array): Uint8Array {
  if (rgba.length !== width * height * 4) throw new Error(`png: ${rgba.length} bytes is not ${width}x${height} RGBA`);
  const raw = new Uint8Array(height * (width * 4 + 1));
  for (let y = 0; y < height; y++) {
    raw[y * (width * 4 + 1)] = 0;                       // filter: none
    raw.set(rgba.subarray(y * width * 4, (y + 1) * width * 4), y * (width * 4 + 1) + 1);
  }
  const out = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
  const ihdr: number[] = [];
  be32(ihdr, width); be32(ihdr, height);
  ihdr.push(8, 6, 0, 0, 0);                             // 8-bit, RGBA, deflate, no filter set, no interlace
  chunk(out, "IHDR", ihdr);
  chunk(out, "IDAT", zlibStored(raw));
  chunk(out, "IEND", []);
  return Uint8Array.from(out);
}

const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
function base64(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i += 3) {
    const n = (b[i]! << 16) | ((b[i + 1] ?? 0) << 8) | (b[i + 2] ?? 0);
    s += B64[(n >>> 18) & 63]! + B64[(n >>> 12) & 63]!
      + (i + 1 < b.length ? B64[(n >>> 6) & 63]! : "=") + (i + 2 < b.length ? B64[n & 63]! : "=");
  }
  return s;
}

/** The image as a `data:image/png;base64,…` URI. */
export const pngDataUri = (width: number, height: number, rgba: Uint8Array): string =>
  `data:image/png;base64,${base64(pngBytes(width, height, rgba))}`;
