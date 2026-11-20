#!/usr/bin/env node
/**
 * (T814, owner 2026-11-17) THE COHO LOADER'S SPRITE SHEETS, baked from the owner's generator.
 *
 *   node tools/build-coho.mjs           -> `pnpm coho:build`: renders the sheets + writes coho.generated.ts (and, T844,
 *                                          the web's sheets + cohoWeb.generated.ts)
 *   node tools/build-coho.mjs --check   -> `pnpm coho` (in `pnpm check`): writes nothing, fails on any drift
 *
 * THE SOURCE OF TRUTH is `tools/fish-loader/fish_loader.py`, the owner's script, as he sent it: a two-block linocut
 * spawning coho, 80 frames, 4 s loop. Its drawing and motion are his; this tool only picks the app's options:
 *
 *   - FRAME WIDTH `FRAME_PX` (480 px; the height follows, 200 px). The loader is drawn `COHO_WIDTH` (192 dp) wide, so this
 *     is 2.5 px per dp (T852, owner 2026-11-18: "the loader fish icon should be higher resolution"): a Pixel 9 draws
 *     2.625 px per dp, so it is drawn at 1.05x, not the 1.75x upscale of T814's 288 px (1.5 px per dp), which looked soft.
 *     The sheet is decoded whole into memory: 80 x 480 x 200 x 4 B = 30.7 MB (288: 11.1 MB). Not 3 px per dp (576 x 240):
 *     44.2 MB decoded and 1.8 / 2.3 MB a file (light / dark), and no 8-column grid fits 4096 px (5 x 16 = 2880 x 3840);
 *     at 2.5 the grid stays 8 x 10, every cell a frame (T852 ticket, measured).
 *   - 8 COLUMNS x 10 rows (3840 x 2000 px): inside the 4096 px texture limit of every phone; a single row would be
 *     38 400 px wide. The grid is FULL (frames a multiple of the columns): the web steps it with CSS `steps(rows)` x
 *     `steps(columns)`, which would show an empty cell of a part-filled last row (checked below).
 *   - LOSSLESS WEBP: the same pixels as the PNG, a smaller file. Every target decodes it (iOS 14+, Android, browsers).
 *   - COLOURS from the tokens `color.ui.coho.ink` / `.red` per theme (never typed here: `tools/check-colours.mjs`). One
 *     sheet per distinct ink + red pair, named after the first theme that wears it; the module maps every theme to its
 *     sheet. The ground behind the loader (`color.ui.coho.ground`) is drawn by the component, never baked.
 *
 * (T844, owner 2026-11-17: "make way smaller for web") THE WEB'S SHEETS, `coho-<theme>-web.webp`, read only by the web
 * loader (`CohoLoader.web.tsx`, through `cohoWeb.generated.ts`, which nothing a phone bundles imports): the same drawing at
 * its own frame size (`WEB_FRAME_PX`, below), `WEB_FRAMES` frames over the same loop (10 fps, not 20) and each pixel put back on the line
 * between its two inks (`fish-loader/web_sheet.py`, `WEB_MIX` x `WEB_ALPHA` steps), so the lossless WebP packs it as a
 * palette: ~105 KB, not 908 KB. On Fast 3G the phones' sheet came in after the map, so the web's coho never swam.
 * (T852) `WEB_FRAME_PX` 384 x 160: 2 px per CSS px of the 192 px loader (it was the phones' 288, 1.5 px), ~146 KB a
 * sheet (from ~105 KB), 9.8 MB decoded; the phones' 480 would be ~188 KB, past the 150 KB the web's budget holds
 * (tools/web-shell.test.ts). Same aspect as the phones' frame, so `cohoHeight` and frame 0 (`pnpm splash`) still agree.
 *
 * WHY --check DOES NOT RE-DRAW. Re-rendering is byte-stable on one machine, but the drawing is floating point and the
 * Mac and CI's Linux were never compared (the T80 lesson: zlib's bytes already differed by CPU). So the check reads the
 * COMMITTED sheets back (`sheet_facts.py`: lossless WebP decodes exactly everywhere) and holds them to the tokens and the
 * module: each sheet's size, and the two colours it is printed in. A token changed without `pnpm coho:build` fails.
 */
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const PY = here("../../.venv/bin/python");
const GENERATOR = here("./fish-loader/fish_loader.py");
const FACTS = here("./fish-loader/sheet_facts.py");
const SHEET_DIR = here("../packages/ui-native/src/coho/");
const MODULE = here("../packages/ui-native/src/coho.generated.ts");
const WEB_MODULE = here("../packages/ui-native/src/cohoWeb.generated.ts");
const WEB_SHEET = here("./fish-loader/web_sheet.py");
const META = here("../packages/map/style/style.meta.json");

/** The app's options for the generator (see the header). */
const FRAME_PX = 480;
const COLUMNS = 8;
/** The generator's defaults the app relies on, restated so the module can say them without running Python. */
const FRAMES = 80, FPS = 20;
/** The design canvas's aspect (fish_loader.py VW x VH): the frame height follows the width. */
const VW = 732, VH = 305;
const FRAME_H = Math.round((FRAME_PX * VH) / VW);
/** (T852) The web's frame: 2 px per CSS px of the 192 px loader (see the header). Its aspect must be the phones'. */
const WEB_FRAME_PX = 384;
const WEB_FRAME_H = Math.round((WEB_FRAME_PX * VH) / VW);
if (WEB_FRAME_PX * FRAME_H !== FRAME_PX * WEB_FRAME_H) throw new Error("build-coho: the web's frame must have the phones' aspect");
const ROWS = Math.ceil(FRAMES / COLUMNS);
const LOOP_MS = Math.round((1000 * FRAMES) / FPS);
/** (T844) The web's sheets: half the frames over the same loop, the colours stepped (see the header). */
const WEB_FRAMES = 40, WEB_FPS = 10, WEB_MIX = 4, WEB_ALPHA = 16;
const WEB_ROWS = Math.ceil(WEB_FRAMES / COLUMNS);
if (Math.round((1000 * WEB_FRAMES) / WEB_FPS) !== LOOP_MS) throw new Error("build-coho: the web's loop must be the phones' loop");
/** (T852) Every sheet fits a phone's 4096 px texture limit, and fills its grid (the web's CSS steps show every cell). */
const TEXTURE_PX = 4096;
for (const [what, frames, rows, fw, fh] of [["phones'", FRAMES, ROWS, FRAME_PX, FRAME_H], ["web's", WEB_FRAMES, WEB_ROWS, WEB_FRAME_PX, WEB_FRAME_H]]) {
  if (frames % COLUMNS) throw new Error(`build-coho: the ${what} ${frames} frames leave the last of ${COLUMNS} columns part-filled`);
  if (fw * COLUMNS > TEXTURE_PX || fh * rows > TEXTURE_PX)
    throw new Error(`build-coho: the ${what} sheet is ${fw * COLUMNS} x ${fh * rows} px, past ${TEXTURE_PX}`);
}

const check = process.argv.includes("--check");
if (!existsSync(PY)) throw new Error(`build-coho: no Python at ${PY} — make the repo's .venv (pip install -r requirements.txt -c constraints.txt)`);

/** Every theme's coho ink + red, as the style build resolved them. */
const themes = JSON.parse(readFileSync(META, "utf8")).themes;
/** Light first, so the sheet every light-ground theme shares is named after it; the rest by name. */
const names = Object.keys(themes).sort((a, b) => (a === "light" ? -1 : b === "light" ? 1 : a.localeCompare(b)));
const pairOf = (t) => {
  const ink = themes[t]["color.ui.coho.ink"], red = themes[t]["color.ui.coho.red"];
  if (!ink || !red) throw new Error(`build-coho: theme ${t} has no color.ui.coho.ink / .red (pnpm style:build)`);
  return { ink: ink.toUpperCase(), red: red.toUpperCase() };
};
/** sheet name -> its pair; theme -> its sheet name. The first theme (by name) to wear a pair names its sheet. */
const sheets = new Map();
const sheetOf = {};
for (const t of names) {
  const p = pairOf(t);
  const found = [...sheets].find(([, q]) => q.ink === p.ink && q.red === p.red);
  const name = found ? found[0] : `coho-${t}`;
  if (!found) sheets.set(name, p);
  sheetOf[t] = name;
}

const ident = (s) => s.replace(/-(\w)/g, (_, c) => c.toUpperCase());
const moduleText = `/// <reference path="./assets.d.ts" />
/**
 * GENERATED by \`pnpm coho:build\` (tools/build-coho.mjs) from the owner's tools/fish-loader/fish_loader.py. Do not edit.
 *
 * (T814) The coho loader: ${FRAMES} frames of ${FRAME_PX}x${FRAME_H} px in a ${COLUMNS} x ${ROWS} sheet (row-major: frame i at
 * column i % ${COLUMNS}, row i / ${COLUMNS}), ${LOOP_MS} ms a loop. One lossless WebP per distinct \`color.ui.coho.ink\` + \`.red\`
 * pair, printed in those colours (never tinted); \`sheets\` names each theme's.
 */
${[...sheets.keys()].map((n) => `import ${ident(n)} from "./coho/${n}.webp";`).join("\n")}

export const COHO = {
  /** Frames in a loop. */
  frames: ${FRAMES},
  /** The sheet's grid. */
  columns: ${COLUMNS},
  rows: ${ROWS},
  /** One frame, in sheet px. The component draws it at its own width; the height follows. */
  frame: { width: ${FRAME_PX}, height: ${FRAME_H} },
  /** One loop (${FRAMES} frames at ${FPS} fps). */
  loopMs: ${LOOP_MS},
  /** Each theme's sheet. */
  sheets: {
${[...names].sort().map((t) => `    ${t}: ${ident(sheetOf[t])},`).join("\n")}
  },
} as const;
`;

const webModuleText = `/// <reference path="./assets.d.ts" />
/**
 * GENERATED by \`pnpm coho:build\` (tools/build-coho.mjs) from the owner's tools/fish-loader/fish_loader.py. Do not edit.
 *
 * (T844) The WEB's coho loader (owner 2026-11-17: "make way smaller for web"): ${WEB_FRAMES} frames of ${WEB_FRAME_PX}x${WEB_FRAME_H} px in a
 * ${COLUMNS} x ${WEB_ROWS} sheet, ${LOOP_MS} ms a loop (${WEB_FPS} fps), each pixel on ${WEB_MIX} steps between its two inks and ${WEB_ALPHA} of
 * alpha (\`fish-loader/web_sheet.py\`). Imported by \`CohoLoader.web.tsx\` only: a phone bundles \`coho.generated.ts\`'s.
 */
${[...sheets.keys()].map((n) => `import ${ident(n)}Web from "./coho/${n}-web.webp";`).join("\n")}

export const COHO_WEB = {
  /** Frames in a loop. */
  frames: ${WEB_FRAMES},
  /** The sheet's grid. */
  columns: ${COLUMNS},
  rows: ${WEB_ROWS},
  /** One frame, in sheet px (T852: 2 px per CSS px of the loader; the phones' frame's aspect). */
  frame: { width: ${WEB_FRAME_PX}, height: ${WEB_FRAME_H} },
  /** One loop (${WEB_FRAMES} frames at ${WEB_FPS} fps): the phones' loop. */
  loopMs: ${LOOP_MS},
  /** Each theme's sheet. */
  sheets: {
${[...names].sort().map((t) => `    ${t}: ${ident(sheetOf[t])}Web,`).join("\n")}
  },
} as const;
`;

const facts = (files) => execFileSync(PY, ["-I", FACTS, ...files], { encoding: "utf8" })
  .trim().split("\n").map((l) => JSON.parse(l));

if (check) {
  const bad = [];
  if (!existsSync(MODULE) || readFileSync(MODULE, "utf8") !== moduleText) bad.push(`${MODULE} differs from what the tokens make`);
  if (!existsSync(WEB_MODULE) || readFileSync(WEB_MODULE, "utf8") !== webModuleText) bad.push(`${WEB_MODULE} differs from what the tokens make`);
  // each sheet: [file, its pair, its rows, its frame]; the web's (T844) beside the phones'
  const all = [...sheets].flatMap(([n, p]) => [[join(SHEET_DIR, `${n}.webp`), p, ROWS, [FRAME_PX, FRAME_H]],
    [join(SHEET_DIR, `${n}-web.webp`), p, WEB_ROWS, [WEB_FRAME_PX, WEB_FRAME_H]]]);
  const missing = all.filter(([f]) => !existsSync(f));
  for (const [f] of missing) bad.push(`no sheet ${f}`);
  const known = new Set(all.map(([f]) => f.split("/").pop()));
  const stray = readdirSync(SHEET_DIR).filter((f) => !known.has(f));
  for (const f of stray) bad.push(`a sheet no theme wears: ${f}`);
  if (!missing.length) {
    for (const [i, got] of facts(all.map(([f]) => f)).entries()) {
      const [file, want, rows, [fw, fh]] = all[i];
      const ok = got.format === "WEBP" && got.width === fw * COLUMNS && got.height === fh * rows &&
        got.colours[0] === want.ink && got.colours[1] === want.red;
      if (!ok) bad.push(`${file}: ${JSON.stringify(got)}, wanted ${fw * COLUMNS}x${fh * rows} WEBP in ${want.ink} + ${want.red}`);
    }
  }
  if (bad.length) {
    console.error(`coho: ${bad.length} problem(s) — run \`pnpm coho:build\`\n  ${bad.join("\n  ")}`);
    process.exit(1);
  }
  console.log(`coho: ${sheets.size} sheet(s) and their web sheets match the tokens (${names.map((t) => `${t} -> ${sheetOf[t]}`).join(", ")})`);
} else {
  mkdirSync(SHEET_DIR, { recursive: true });
  const tmp = mkdtempSync(join(tmpdir(), "coho-"));
  try {
    for (const [name, { ink, red }] of sheets) {
      execFileSync(PY, ["-I", GENERATOR, "--sheet-only", "--format", "webp", "--width", String(FRAME_PX),
        "--cols", String(COLUMNS), "--frames", String(FRAMES), "--fps", String(FPS),
        "--ink", ink, "--accent", red, "--name", name, "--out", tmp], { stdio: "inherit" });
      const meta = JSON.parse(readFileSync(join(tmp, `${name}.json`), "utf8"));
      if (meta.frame_width !== FRAME_PX || meta.frame_height !== FRAME_H || meta.columns !== COLUMNS ||
          meta.rows !== ROWS || meta.loop_ms !== LOOP_MS)
        throw new Error(`build-coho: the generator made ${JSON.stringify(meta)}, not the grid this tool states`);
      copyFileSync(join(tmp, `${name}_sheet.webp`), join(SHEET_DIR, `${name}.webp`));
      // (T844) the web's: the same drawing, WEB_FRAMES frames over the same loop, then its colours stepped
      const web = `${name}-web-${WEB_FRAMES}`;
      execFileSync(PY, ["-I", GENERATOR, "--sheet-only", "--format", "webp", "--width", String(WEB_FRAME_PX),
        "--cols", String(COLUMNS), "--frames", String(WEB_FRAMES), "--fps", String(WEB_FPS),
        "--ink", ink, "--accent", red, "--name", web, "--out", tmp], { stdio: "inherit" });
      const wm = JSON.parse(readFileSync(join(tmp, `${web}.json`), "utf8"));
      if (wm.frame_width !== WEB_FRAME_PX || wm.frame_height !== WEB_FRAME_H || wm.columns !== COLUMNS || wm.rows !== WEB_ROWS ||
          wm.loop_ms !== LOOP_MS)
        throw new Error(`build-coho: the generator made ${JSON.stringify(wm)}, not the web grid this tool states`);
      execFileSync(PY, ["-I", WEB_SHEET, join(tmp, `${web}_sheet.webp`), join(SHEET_DIR, `${name}-web.webp`),
        "--ink", ink, "--red", red, "--mix", String(WEB_MIX), "--alpha", String(WEB_ALPHA)], { stdio: "inherit" });
    }
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
  for (const f of readdirSync(SHEET_DIR)) if (!sheets.has(f.replace(/(-web)?\.webp$/, ""))) unlinkSync(join(SHEET_DIR, f));
  writeFileSync(MODULE, moduleText);
  writeFileSync(WEB_MODULE, webModuleText);
  const kb = [...sheets.keys()].flatMap((n) => [`${n}.webp`, `${n}-web.webp`])
    .map((f) => `${f} ${(readFileSync(join(SHEET_DIR, f)).length / 1024).toFixed(0)} KB`);
  console.log(`coho: ${FRAMES} frames ${FRAME_PX}x${FRAME_H}, ${COLUMNS}x${ROWS}; ${kb.join(", ")}; decoded ` +
    `${((FRAMES * FRAME_PX * FRAME_H * 4) / 1e6).toFixed(1)} MB each`);
}
