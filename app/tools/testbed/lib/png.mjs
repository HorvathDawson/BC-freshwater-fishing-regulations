// (2026-10-30) A screenshot's pixels without a PNG library: Android's `screencap -p` writes 8-bit RGB/RGBA,
// non-interlaced, which is all this reads (zlib + the five row filters). For host-side checks that a region of the
// screen changed (a pan, a pinch) or holds a colour (the location dot).
import fs from 'node:fs';
import zlib from 'node:zlib';

/** { w, h, ch, px } — px is the raw rows, `ch` bytes a pixel (3 or 4). */
export function readPng(file) {
  const b = fs.readFileSync(file);
  let o = 8, w = 0, h = 0, ch = 4;
  const idat = [];
  while (o < b.length) {
    const len = b.readUInt32BE(o), type = b.toString('ascii', o + 4, o + 8), data = b.subarray(o + 8, o + 8 + len);
    if (type === 'IHDR') {
      w = data.readUInt32BE(0); h = data.readUInt32BE(4);
      if (data[8] !== 8 || data[12] !== 0) throw new Error(`png: bit depth ${data[8]} / interlace ${data[12]} not read`);
      ch = { 2: 3, 6: 4 }[data[9]];
      if (!ch) throw new Error(`png: colour type ${data[9]} not read`);
    } else if (type === 'IDAT') idat.push(data);
    else if (type === 'IEND') break;
    o += 12 + len;
  }
  const raw = zlib.inflateSync(Buffer.concat(idat));
  const stride = w * ch, px = Buffer.alloc(h * stride);
  for (let y = 0; y < h; y++) {
    const f = raw[y * (stride + 1)], src = y * (stride + 1) + 1, dst = y * stride;
    for (let x = 0; x < stride; x++) {
      const a = x >= ch ? px[dst + x - ch] : 0, up = y ? px[dst - stride + x] : 0, c = y && x >= ch ? px[dst - stride + x - ch] : 0;
      const v = raw[src + x];
      let r;
      if (f === 0) r = v; else if (f === 1) r = v + a; else if (f === 2) r = v + up; else if (f === 3) r = v + ((a + up) >> 1);
      else { const p = a + up - c, pa = Math.abs(p - a), pb = Math.abs(p - up), pc = Math.abs(p - c);
             r = v + (pa <= pb && pa <= pc ? a : pb <= pc ? up : c); }
      px[dst + x] = r & 255;
    }
  }
  return { w, h, ch, px };
}

const box = (img, r) => ({ x0: Math.max(0, r?.x ?? 0), y0: Math.max(0, r?.y ?? 0),
  x1: Math.min(img.w, (r?.x ?? 0) + (r?.w ?? img.w)), y1: Math.min(img.h, (r?.y ?? 0) + (r?.h ?? img.h)) });

/** The share of pixels (every 3rd each way) inside `r` whose colour moved by more than 40 (sum of |dR|+|dG|+|dB|). */
export function changed(a, b, r) {
  const { x0, y0, x1, y1 } = box(a, r);
  let n = 0, d = 0;
  for (let y = y0; y < y1; y += 3) for (let x = x0; x < x1; x += 3) {
    const i = (y * a.w + x) * a.ch, j = (y * b.w + x) * b.ch;
    n++;
    if (Math.abs(a.px[i] - b.px[j]) + Math.abs(a.px[i + 1] - b.px[j + 1]) + Math.abs(a.px[i + 2] - b.px[j + 2]) > 40) d++;
  }
  return n ? d / n : 0;
}

/** How many pixels inside `r` pass `test(r, g, b)`. */
export function count(img, r, test) {
  const { x0, y0, x1, y1 } = box(img, r);
  let n = 0;
  for (let y = y0; y < y1; y++) for (let x = x0; x < x1; x++) {
    const i = (y * img.w + x) * img.ch;
    if (test(img.px[i], img.px[i + 1], img.px[i + 2])) n++;
  }
  return n;
}

/**
 * (T723) The iOS software keyboard's top edge in a simulator screenshot (pixel row), or null when no keyboard is drawn.
 * The accessibility tree does not list the keyboard, so it is read off the pixels: its material is a light grey with a
 * blue cast (iOS 26 light: about 223,224,228), keys are white with grey gaps between rows. From the bottom up, a row is
 * "keyboard" when 30% or more of its middle is that grey; key rows (mostly white) are crossed as gaps up to 7% of the
 * height. Checked against a shot by eye (T723-kb-glossary.png: the row found is the keyboard's rounded top).
 */
export function keyboardTop(img) {
  const kb = (r, g, b) => r >= 195 && r <= 240 && b - r >= 1 && b - r <= 12 && Math.abs(g - r) <= 6;
  const x0 = Math.round(img.w * 0.1), x1 = Math.round(img.w * 0.9);
  const rowIn = (y) => {
    let n = 0, k = 0;
    for (let x = x0; x < x1; x += 4) { n++; const i = (y * img.w + x) * img.ch; if (kb(img.px[i], img.px[i + 1], img.px[i + 2])) k++; }
    return k / n >= 0.3;
  };
  let start = -1;
  for (let y = img.h - 1; y > img.h * 0.9; y--) if (rowIn(y)) { start = y; break; }
  if (start < 0) return null;
  let top = start, gap = 0;
  for (let y = start; y > img.h * 0.2; y--) {
    if (rowIn(y)) { top = y; gap = 0; } else if (++gap > img.h * 0.07) break;
  }
  return img.h - top < img.h * 0.15 ? null : top;   // under 15% of the screen is the home bar or a grey footer, not a keyboard
}
