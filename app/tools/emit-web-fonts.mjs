#!/usr/bin/env node
/**
 * T409 — THE WATER PAGES' FONTS AS WOFF2, MADE FROM THE APP'S OWN TTF FILES.
 *
 *   node tools/emit-web-fonts.mjs        -> writes tools/seo/fonts/<Face>.woff2 + fonts.json
 *
 * A water page painted its pane with the app's faces as TTF (expo-google-fonts, ~36-47 kB each on the wire, six of them
 * before the first paint) at a path with an `@` in it, which the host answers with a 307 first. The same faces as WOFF2
 * are ~27-36 kB, served from `/assets/fonts/` (no redirect): Lighthouse mobile LCP -0.3-0.4 s on the sample pages.
 *
 * LOSSLESS: every glyph and table kept (fontTools re-saves the font with the woff2 flavour; no subsetting), so the page
 * draws exactly what the app draws (the T269 page-vs-tap pairs). `fonts.json` records, per face, the sha256 of the TTF it
 * was made from and of the WOFF2: the SEO build (tools/seo/build.ts `webFonts`) refuses a face with no WOFF2, a WOFF2 made
 * from another version of the app's TTF, or a WOFF2 that is not the one recorded — so a font package update fails the
 * build until this is rerun. Never hand-made.
 *
 * Needs Python with fontTools and brotli (`pip install fonttools brotli`); `FONT_PYTHON` names the interpreter (default
 * python3). Only the build tools use these files; the phone app is untouched.
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const app = fileURLToPath(new URL("..", import.meta.url));
export const FONT_DIR = `${app}tools/seo/fonts/`;
export const FONTS_RECORD = `${FONT_DIR}fonts.json`;
const sha = (b) => createHash("sha256").update(b).digest("hex");

/** The faces the app loads (`FACE` in packages/ui-native/src/type.ts, the one list). */
export function faces() {
  const src = readFileSync(`${app}packages/ui-native/src/type.ts`, "utf8");
  const block = /export const FACE = \{([\s\S]*?)\} as const/.exec(src);
  if (!block) throw new Error("emit-web-fonts: no FACE in packages/ui-native/src/type.ts");
  return [...new Set([...block[1].matchAll(/"(\w+_\d00\w*)"/g)].map((m) => m[1]))].sort();
}

const PACKAGE = { Archivo: "archivo", BricolageGrotesque: "bricolage-grotesque", JetBrainsMono: "jetbrains-mono" };

/** A face's TTF in the expo-google-fonts package the app requires (`Archivo_400Regular` → …/archivo/400Regular/…ttf). */
export function ttfOf(face) {
  const [family, weight] = face.split("_");
  const pkg = PACKAGE[family];
  if (!pkg || !weight) throw new Error(`emit-web-fonts: no package for the face ${face}`);
  const req = createRequire(`${app}apps/mobile/package.json`);
  return join(dirname(req.resolve(`@expo-google-fonts/${pkg}/package.json`)), weight, `${face}.ttf`);
}

const PY = `import sys
from fontTools.ttLib import TTFont
f = TTFont(sys.argv[1], recalcBBoxes=False, recalcTimestamp=False)
f.flavor = "woff2"
f.save(sys.argv[2], reorderTables=False)`;

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const py = process.env.FONT_PYTHON || "python3";
  try { execFileSync(py, ["-c", "import fontTools, brotli"], { stdio: "pipe" }); }
  catch { throw new Error(`emit-web-fonts: ${py} has no fontTools + brotli (pip install fonttools brotli; FONT_PYTHON=<python>)`); }
  mkdirSync(FONT_DIR, { recursive: true });
  const record = {};
  for (const face of faces()) {
    const ttf = ttfOf(face), out = `${FONT_DIR}${face}.woff2`;
    execFileSync(py, ["-c", PY, ttf, out], { stdio: "inherit" });
    const w = readFileSync(out);
    record[face] = { ttf_sha256: sha(readFileSync(ttf)), woff2_sha256: sha(w), bytes: w.length };
    console.log(`${face}: ${readFileSync(ttf).length} B ttf -> ${w.length} B woff2`);
  }
  writeFileSync(FONTS_RECORD, `${JSON.stringify(record, null, 1)}\n`);
}
