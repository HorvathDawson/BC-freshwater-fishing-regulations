/**
 * T406 — A WATER'S SHARE CARD AS A PNG. Facebook, X and most chat apps do not draw an SVG `og:image`, so a shared water link
 * showed no picture. The card (site.ts `shareSvg`, 1200 x 630) is rasterised at build time by @resvg/resvg-js (already the
 * icons' rasteriser, app/deps.md) in the app's own face (Archivo, from the app's font package): no system font is read, so
 * every machine draws the same pixels. Nothing is fetched; no service.
 */
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { Resvg } from "@resvg/resvg-js";
import { SHARE_FONT, SHARE_H, SHARE_W } from "./html";

const archivo = dirname(createRequire(import.meta.url).resolve("@expo-google-fonts/archivo/package.json"));
const FONT_FILES = [join(archivo, "400Regular/Archivo_400Regular.ttf"), join(archivo, "700Bold/Archivo_700Bold.ttf")];

/** The card drawn as a PNG at its own size. Throws on an SVG resvg cannot read. */
export function sharePng(svg: string): Uint8Array {
  const r = new Resvg(svg, { font: { fontFiles: FONT_FILES, loadSystemFonts: false, defaultFontFamily: SHARE_FONT }, fitTo: { mode: "original" } });
  const img = r.render();
  if (img.width !== SHARE_W || img.height !== SHARE_H) throw new Error(`seo: a share card drew ${img.width} x ${img.height}, not ${SHARE_W} x ${SHARE_H}`);
  return img.asPng();
}

/** The PNG's own width and height (its IHDR), to check a written card without decoding it. */
export function pngSize(png: Uint8Array): { width: number; height: number } | null {
  const sig = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
  if (png.length < 24 || sig.some((b, i) => png[i] !== b)) return null;
  const v = new DataView(png.buffer, png.byteOffset, png.byteLength);
  return { width: v.getUint32(16), height: v.getUint32(20) };
}
