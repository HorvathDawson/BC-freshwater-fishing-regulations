/**
 * T917 — THE STORED BYTES, made one way wherever they are made: a water page's gzip + brotli (T279: the site worker serves the
 * stored bytes) and a share card's PNG (T406). Plain JavaScript, so a job thread (jobs-worker.mjs) loads it without the module
 * runner; the TypeScript side (compressed.ts, share.ts) imports the same functions (bytes.d.mts). A change here changes every
 * page's bytes: the full build's file-by-file diff (T917) is the check.
 */
import { brotliCompressSync, constants, gzipSync } from "node:zlib";
import { Resvg } from "@resvg/resvg-js";

/** A page's two stored forms: gzip 9 and brotli 11 (sized to the page). */
export function compressPage(raw) {
  return {
    gz: gzipSync(raw, { level: 9 }),
    br: brotliCompressSync(raw, { params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_SIZE_HINT]: raw.length } }),
  };
}

/** A share card drawn as a PNG at its own size, in the given faces only (no system font). Throws on an SVG resvg cannot read. */
export function drawCard(svg, card) {
  const r = new Resvg(svg, { font: { fontFiles: card.fontFiles, loadSystemFonts: false, defaultFontFamily: card.font }, fitTo: { mode: "original" } });
  const img = r.render();
  if (img.width !== card.width || img.height !== card.height) throw new Error(`seo: a share card drew ${img.width} x ${img.height}, not ${card.width} x ${card.height}`);
  return img.asPng();
}
