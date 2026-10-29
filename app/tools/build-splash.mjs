#!/usr/bin/env node
/**
 * THE NATIVE SPLASH, DRAWN FROM THE LOADING FISH (T244, owner 2026-10-29).
 *
 *   node tools/build-splash.mjs            -> writes apps/mobile/assets/splash/splash-{light,dark}.png
 *   node tools/build-splash.mjs --check    -> exits 1 when either committed file differs (`pnpm splash`)
 *
 * The native splash (expo-splash-screen) must hand off to the in-app `FishSpinner` with no jump, so it is not new art:
 * it is the spinner's own frame 0 (the strip `pnpm spinner` bakes, `fish-sprite.generated.ts`), tinted `color.ui.live`,
 * over the same orbit ring in `color.ui.line` that `FishSpinner` draws as a bordered view — composited here at the pixel
 * size the app draws it (110 dp x 3 = 330 px, bilinear from the 192 px frame, as the Image stretch does), one file per
 * theme, on a transparent ground. The ground is `color.ui.card` per theme (what the boot screen paints), read by
 * `apps/mobile/app.config.js` from the same theme files; no colour is written down here or there.
 *
 * At t = 0 the spinner's ring is a full circle, its rotation is 0 and its strip is at frame 0 — so the first frame of the
 * live spinner IS this picture, to the pixel, and `splash.ts` hides the native one only once that frame is on screen.
 */
import { deflateSync, inflateSync as inflate } from "node:zlib";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));

/** The size the boot screen draws the spinner at (App.tsx), in dp, and the density the splash art is made at. */
const SPLASH_DP = 110;
const DENSITY = 3;
const PX = SPLASH_DP * DENSITY;

const CRC = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c;
  }
  return t;
})();
const crc32 = (buf) => {
  let c = -1;
  for (const b of buf) c = CRC[(c ^ b) & 0xff] ^ (c >>> 8);
  return (c ^ -1) >>> 0;
};
function chunk(type, data) {
  const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, "latin1"), data]);
  const crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(body));
  return Buffer.concat([len, body, crc]);
}
const SIG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);

/** RGBA 8-bit PNG from straight-alpha pixels. */
function pngRGBA(w, h, rgba) {
  const raw = Buffer.alloc(h * (1 + w * 4));
  for (let y = 0; y < h; y++) {
    raw[y * (1 + w * 4)] = 0;
    rgba.copy(raw, y * (1 + w * 4) + 1, y * w * 4, (y + 1) * w * 4);
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 6;
  return Buffer.concat([SIG, chunk("IHDR", ihdr), chunk("IDAT", deflateSync(raw, { level: 9 })), chunk("IEND", Buffer.alloc(0))]);
}

/** Frame 0 of the committed strip: its alpha channel, 0..1, `frame` x `frame`. */
function frameZero() {
  const src = readFileSync(here("../packages/ui-native/src/fish-sprite.generated.ts"), "utf8");
  const frame = Number(/frame:\s*(\d+)/.exec(src)?.[1]);
  const frames = Number(/frames:\s*(\d+)/.exec(src)?.[1]);
  const ring = Number(/radius:\s*([\d.]+)/.exec(src)?.[1]);
  const ringW = Number(/width:\s*([\d.]+)/.exec(src)?.[1]);
  const b64 = /data:image\/png;base64,([A-Za-z0-9+/=]+)/.exec(src)?.[1];
  if (!frame || !frames || !ring || !b64) throw new Error("build-splash: cannot read fish-sprite.generated.ts");
  const png = Buffer.from(b64, "base64");
  // the strip is our own writer's: 8-bit grey+alpha, one IDAT, filter 0 on every row (build-spinner.mjs `png`)
  let at = 8, w = 0, h = 0; const idat = [];
  while (at < png.length) {
    const len = png.readUInt32BE(at), type = png.toString("latin1", at + 4, at + 8);
    if (type === "IHDR") { w = png.readUInt32BE(at + 8); h = png.readUInt32BE(at + 12); }
    if (type === "IDAT") idat.push(png.subarray(at + 8, at + 8 + len));
    at += 12 + len;
  }
  const raw = inflate(Buffer.concat(idat));
  const a = new Float64Array(frame * frame);
  for (let y = 0; y < frame; y++) {
    const row = y * (1 + w * 2);
    if (raw[row] !== 0) throw new Error("build-splash: the strip is not filter-0; build-spinner.mjs changed its writer");
    for (let x = 0; x < frame; x++) a[y * frame + x] = raw[row + 2 + x * 2] / 255;
  }
  if (h !== frame) throw new Error("build-splash: strip height != frame");
  return { alpha: a, frame, ring, ringW };
}

const hex = (s) => [1, 3, 5].map((i) => parseInt(s.slice(i, i + 2), 16));
function theme(name) {
  const v = JSON.parse(readFileSync(here(`../packages/map/style/themes/${name}.json`), "utf8")).values;
  for (const k of ["color.ui.live", "color.ui.line"]) if (typeof v[k] !== "string") throw new Error(`build-splash: theme ${name} has no ${k}`);
  return { live: hex(v["color.ui.live"]), line: hex(v["color.ui.line"]) };
}

/** The splash picture for one theme: ring (line) under the fish (live), straight alpha, PX x PX. */
function draw(sprite, { live, line }) {
  const { alpha, frame, ring, ringW } = sprite;
  const out = Buffer.alloc(PX * PX * 4);
  // FishSpinner: ring View of diameter size*radius*2 with a border of max(size*width, 1) dp, centred; border-box.
  const R = SPLASH_DP * ring * DENSITY, bw = Math.max(SPLASH_DP * ringW, 1) * DENSITY, C = PX / 2;
  const SS = 4;
  const sample = (fx, fy) => {                       // bilinear on the 192 px frame, like the Image stretch
    const x0 = Math.floor(fx), y0 = Math.floor(fy), tx = fx - x0, ty = fy - y0;
    const at = (x, y) => alpha[Math.min(Math.max(y, 0), frame - 1) * frame + Math.min(Math.max(x, 0), frame - 1)];
    return at(x0, y0) * (1 - tx) * (1 - ty) + at(x0 + 1, y0) * tx * (1 - ty) + at(x0, y0 + 1) * (1 - tx) * ty + at(x0 + 1, y0 + 1) * tx * ty;
  };
  for (let y = 0; y < PX; y++) for (let x = 0; x < PX; x++) {
    let r = 0;
    for (let sy = 0; sy < SS; sy++) for (let sx = 0; sx < SS; sx++) {
      const d = Math.hypot(x + (sx + 0.5) / SS - C, y + (sy + 0.5) / SS - C);
      if (d <= R && d >= R - bw) r++;
    }
    const ringA = r / (SS * SS);
    const fishA = sample(((x + 0.5) * frame) / PX - 0.5, ((y + 0.5) * frame) / PX - 0.5);
    const a = fishA + ringA * (1 - fishA);           // fish over ring
    const o = (y * PX + x) * 4;
    if (a > 0) for (let k = 0; k < 3; k++) out[o + k] = Math.round((live[k] * fishA + line[k] * ringA * (1 - fishA)) / a);
    out[o + 3] = Math.round(a * 255);
  }
  return pngRGBA(PX, PX, out);
}

const sprite = frameZero();
const files = ["light", "dark"].map((name) => ({
  path: here(`../apps/mobile/assets/splash/splash-${name}.png`), bytes: draw(sprite, theme(name)),
}));

if (process.argv.includes("--check")) {
  const stale = files.filter((f) => !existsSync(f.path) || !readFileSync(f.path).equals(f.bytes));
  if (stale.length) {
    console.error(`splash art is stale — run \`node tools/build-splash.mjs\`: ${stale.map((f) => f.path).join(", ")}`);
    process.exit(1);
  }
  console.log("splash art: current");
} else {
  mkdirSync(here("../apps/mobile/assets/splash"), { recursive: true });
  for (const f of files) { writeFileSync(f.path, f.bytes); console.log(`wrote ${f.path} (${f.bytes.length} B)`); }
}
