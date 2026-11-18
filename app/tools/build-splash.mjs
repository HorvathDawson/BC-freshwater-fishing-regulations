#!/usr/bin/env node
/**
 * THE NATIVE SPLASH, DRAWN FROM THE LOADER (T244, owner 2026-10-29; T843, owner 2026-11-17: the coho, not the circling fish).
 *
 *   node tools/build-splash.mjs            -> writes apps/mobile/assets/splash/splash-{light,dark}.png and (T844) the web's
 *                                             opening picture, web-frame0-{light,dark}.png (the web sheet's frame 0)
 *   node tools/build-splash.mjs --check    -> exits 1 when either committed file differs (`pnpm splash`)
 *
 * The native splash (expo-splash-screen) must hand off to the boot screen's loader with no jump, so it is not new art:
 * it is the loader's own frame 0. Since T814 the boot screen's loader is the coho (`CohoLoader`, `COHO_WIDTH` = 192 dp
 * wide), and the splash had stayed the circling fish's frame 0, so a launch showed the old fish, then the coho (owner
 * 2026-11-17: "the old loading icon shows up before the new swimming fish one"). Now it is the coho sheet's frame 0 per
 * theme (light: coho-light, dark: coho-dark; the sheets are already printed in the theme's ink and red), at the pixel
 * size the app draws it (192 x 80 dp x 3 = 576 x 240 px, bilinear from the 288 x 120 px frame, as the Image stretch
 * does), on a transparent ground. The ground the coho sits on is `color.ui.coho.ground` = `color.ui.card`, what the boot
 * screen paints and what `apps/mobile/app.config.js` gives the splash; no colour is written down here or there.
 *
 * Android 12+ shows the splash icon inside a 192 dp circle: frame 0's farthest inked pixel is 92.6 dp from the centre,
 * inside the 96 dp radius; `--check` holds that (`CIRCLE_DP`), so a redrawn coho that would be clipped fails here.
 *
 * At t = 0 `CohoLoader`'s one Animated.Value is 0, so its sheet shows frame 0 — the first frame of the live loader IS
 * this picture, and `splash.ts` hides the native one only once that frame is on screen. The frame is read from the
 * committed lossless WebP by Python (`fish-loader/sheet_facts.py --frame0`): the decode is exact on every machine.
 */
import { execFileSync } from "node:child_process";
// (T80) fflate, not node:zlib, to write: zlib's deflate bytes differ by CPU (the Mac vs CI's x64); --check compares bytes
import { zlibSync } from "fflate";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));

/** The width the boot screen draws the coho at (`COHO_WIDTH`, ui-native/src/coho.ts), in dp, and the art's density. */
const SPLASH_DP = 192;
const DENSITY = 3;
/** Android 12+'s splash icon circle, in dp: everything inked must fall inside it. */
const CIRCLE_DP = 192;

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
  return Buffer.concat([SIG, chunk("IHDR", ihdr), chunk("IDAT", Buffer.from(zlibSync(raw, { level: 9 }))), chunk("IEND", Buffer.alloc(0))]);
}

/** The coho module's facts: the frame size and each theme's sheet file; (T844) `web`: the web's sheets' module. */
function coho(module = "coho.generated.ts") {
  const src = readFileSync(here(`../packages/ui-native/src/${module}`), "utf8");
  const fw = Number(/frame:\s*\{\s*width:\s*(\d+)/.exec(src)?.[1]);
  const fh = Number(/frame:\s*\{[^}]*height:\s*(\d+)/.exec(src)?.[1]);
  const imports = Object.fromEntries([...src.matchAll(/import (\w+) from "\.\/coho\/([\w.-]+)";/g)].map((m) => [m[1], m[2]]));
  const sheetOf = (theme) => {
    const v = new RegExp(`\\b${theme}:\\s*(\\w+),`).exec(src)?.[1];
    if (!v || !imports[v]) throw new Error(`build-splash: ${module} names no sheet for theme ${theme}`);
    return here(`../packages/ui-native/src/coho/${imports[v]}`);
  };
  if (!fw || !fh) throw new Error(`build-splash: cannot read the frame size from ${module}`);
  return { fw, fh, sheetOf };
}

/** Frame 0 of a sheet, straight-alpha RGBA, decoded by the repo's Python (lossless WebP: exact everywhere). */
function frameZero(sheet, fw, fh) {
  const py = here("../../.venv/bin/python");
  if (!existsSync(py)) throw new Error(`build-splash: no Python at ${py} — make the repo's .venv`);
  const b64 = execFileSync(py, [here("./fish-loader/sheet_facts.py"), "--frame0", String(fw), String(fh), sheet], { encoding: "utf8" }).trim();
  const px = Buffer.from(b64, "base64");
  if (px.length !== fw * fh * 4) throw new Error(`build-splash: frame 0 of ${sheet} is ${px.length} B, not ${fw}x${fh} RGBA`);
  return px;
}

/** The splash picture: frame 0 stretched to W x H px (bilinear on premultiplied colour), straight alpha. */
function draw(px, fw, fh) {
  const W = SPLASH_DP * DENSITY, H = Math.round((SPLASH_DP * fh / fw) * DENSITY);
  const out = Buffer.alloc(W * H * 4);
  const at = (x, y, k) => {
    const o = (Math.min(Math.max(y, 0), fh - 1) * fw + Math.min(Math.max(x, 0), fw - 1)) * 4;
    return k === 3 ? px[o + 3] / 255 : (px[o + k] * px[o + 3]) / 255;   // premultiplied
  };
  let far = 0;
  for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
    const fx = ((x + 0.5) * fw) / W - 0.5, fy = ((y + 0.5) * fh) / H - 0.5;
    const x0 = Math.floor(fx), y0 = Math.floor(fy), tx = fx - x0, ty = fy - y0;
    const lerp = (k) => at(x0, y0, k) * (1 - tx) * (1 - ty) + at(x0 + 1, y0, k) * tx * (1 - ty)
      + at(x0, y0 + 1, k) * (1 - tx) * ty + at(x0 + 1, y0 + 1, k) * tx * ty;
    const a = lerp(3), o = (y * W + x) * 4;
    if (a > 0) {
      for (let k = 0; k < 3; k++) out[o + k] = Math.min(255, Math.round(lerp(k) / a));
      out[o + 3] = Math.round(a * 255);
      if (out[o + 3] > 8) far = Math.max(far, Math.hypot(x + 0.5 - W / 2, y + 0.5 - H / 2) / DENSITY);
    }
  }
  if (far > CIRCLE_DP / 2) throw new Error(`build-splash: the coho reaches ${far.toFixed(1)} dp from the centre; Android 12+ clips the splash icon at ${CIRCLE_DP / 2} dp`);
  return { bytes: pngRGBA(W, H, out), far };
}

const { fw, fh, sheetOf } = coho();
const web = coho("cohoWeb.generated.ts");
if (web.fw !== fw || web.fh !== fh) throw new Error("build-splash: the web's coho frame is not the phones'");
const files = ["light", "dark"].flatMap((name) => [
  { path: here(`../apps/mobile/assets/splash/splash-${name}.png`), ...draw(frameZero(sheetOf(name), fw, fh), fw, fh) },
  // (T844) the web's opening picture: the WEB sheet's frame 0 at its own pixels (the web draws it at 1.5 px per CSS px
  // too), shown by the export's HTML shell (`tools/web-shell.mjs`) until that sheet has arrived: the same picture
  { path: here(`../apps/mobile/assets/splash/web-frame0-${name}.png`), bytes: pngRGBA(fw, fh, frameZero(web.sheetOf(name), fw, fh)), far: 0 },
]);

if (process.argv.includes("--check")) {
  const stale = files.filter((f) => !existsSync(f.path) || !readFileSync(f.path).equals(f.bytes));
  if (stale.length) {
    console.error(`splash art is stale — run \`node tools/build-splash.mjs\`: ${stale.map((f) => f.path).join(", ")}`);
    process.exit(1);
  }
  console.log("splash art: current");
} else {
  mkdirSync(here("../apps/mobile/assets/splash"), { recursive: true });
  for (const f of files) { writeFileSync(f.path, f.bytes); console.log(`wrote ${f.path} (${f.bytes.length} B${f.far ? `; reaches ${f.far.toFixed(1)} dp` : ""})`); }
}
