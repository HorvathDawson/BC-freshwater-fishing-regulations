#!/usr/bin/env node
/**
 * T38 — THE HATCH SHEET'S TWO DRAWINGS, SHOT FROM THE PROTOTYPE'S OWN ART.
 *
 *   node tools/emit-adipose.mjs            -> writes apps/mobile/assets/fish/adipose-{wild,clipped}.webp, prints sha256
 *   node tools/emit-adipose.mjs --check    -> re-renders and exits 1 when either committed file differs
 *
 * The prototype's "Hatchery or wild?" sheet draws `FISHART.draw('RB', {marks:false, adiMark:true})` and the same with
 * `clipped:true` (regs-v11.html sheetHtml hatch; page v36 adiposeHtml). That drawing is evaluated here from
 * `design/regs-v10/fishart.js` (the file regs-v11.html loads) with a stub `window`, checked to be the same on a second
 * evaluation, and rasterised in headless Chrome (tools/regs-web/cdp.mjs) on a transparent canvas at 800×300:
 * an RGBA cut-out like the fish photos (D4 — no ground behind a picture). The colours live in the image, never in app
 * code (check-colours): this file only calls `draw`.
 *
 * Why a raster and not a react-native-svg port: tickets/T38-adipose-art-howtell.md "Decision".
 * Chrome is needed, so `--check` is NOT in `pnpm check`; `tools/emit-adipose.test.ts` pins the SVGs' sha256 instead,
 * so a fishart.js change fails the suite until this is rerun.
 */
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const app = fileURLToPath(new URL("..", import.meta.url));
export const FISHART_SOURCE = `${app}design/regs-v10/fishart.js`;
export const OUT = {
  wild: `${app}apps/mobile/assets/fish/adipose-wild.webp`,
  clipped: `${app}apps/mobile/assets/fish/adipose-clipped.webp`,
};
export const SIZE = { width: 800, height: 300 };
/** Chrome's WebP encoder quality: the drawings are flat shapes, 0.9 keeps edges clean at ~15 kB. */
const QUALITY = 0.9;

/**
 * fishart.js evaluated alone: its FISHART with `Math.random` stubbed (it only names the gradient / clip ids, which must
 * be stable for the drawing to be byte-for-byte reproducible), then the two drawings of the hatch sheet.
 */
export function adiposeSvgs(src) {
  const math = Object.create(Math, { random: { value: () => 0.5 } });
  const window = {};
  new Function("window", "Math", `${src}\n`)(window, math);
  const FISHART = window.FISHART;
  if (!FISHART?.draw) throw new Error("emit-adipose: fishart.js set no window.FISHART");
  const wild = FISHART.draw("RB", { marks: false, adiMark: true });
  const clipped = FISHART.draw("RB", { marks: false, adiMark: true, clipped: true });
  if (!wild || !clipped) throw new Error("emit-adipose: FISHART has no RB drawing");
  return { wild, clipped };
}

/** The page Chrome renders: each SVG drawn onto a transparent canvas, read back as WebP. */
async function rasterise(svgs) {
  const { open } = await import(pathToFileURL(`${app}tools/regs-web/cdp.mjs`).href);
  const dir = mkdtempSync(join(tmpdir(), "emit-adipose-"));
  const page = join(dir, "blank.html");
  writeFileSync(page, "<!doctype html><meta charset=utf-8><body></body>");
  const chrome = await open(pathToFileURL(page).href, { width: SIZE.width, height: SIZE.height });
  try {
    const out = {};
    for (const [k, svg] of Object.entries(svgs)) {
      const sized = svg.replace("<svg ", `<svg xmlns="http://www.w3.org/2000/svg" width="${SIZE.width}" height="${SIZE.height}" `);
      const b64 = await chrome.evaluate(`(async () => {
        const img = new Image();
        img.src = "data:image/svg+xml;base64," + btoa(unescape(encodeURIComponent(${JSON.stringify(sized)})));
        await img.decode();
        const c = document.createElement("canvas"); c.width = ${SIZE.width}; c.height = ${SIZE.height};
        c.getContext("2d").drawImage(img, 0, 0, ${SIZE.width}, ${SIZE.height});
        return c.toDataURL("image/webp", ${QUALITY}).split(",")[1];
      })()`);
      out[k] = Buffer.from(b64, "base64");
    }
    return out;
  } finally {
    await chrome.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

const sha = (b) => createHash("sha256").update(b).digest("hex");

async function main() {
  const src = readFileSync(FISHART_SOURCE, "utf8");
  const svgs = adiposeSvgs(src), again = adiposeSvgs(src);
  if (svgs.wild !== again.wild || svgs.clipped !== again.clipped) throw new Error("emit-adipose: draw() is not deterministic");
  const webp = await rasterise(svgs);
  if (process.argv.includes("--check")) {
    const stale = Object.keys(OUT).filter((k) => { try { return sha(readFileSync(OUT[k])) !== sha(webp[k]); } catch { return true; } });
    if (stale.length) { console.error(`adipose drawings stale (${stale.join(", ")}) — run node tools/emit-adipose.mjs`); process.exit(1); }
    console.log("✓ adipose drawings current");
    return;
  }
  for (const k of Object.keys(OUT)) {
    writeFileSync(OUT[k], webp[k]);
    console.log(`${sha(webp[k])}  ${OUT[k]}  (${webp[k].length} B; svg sha256 ${sha(svgs[k])})`);
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) await main();
