#!/usr/bin/env node
/**
 * EVERY COLOUR IN THE APP LIVES IN ONE PLACE: the token set.
 *
 *   packages/map/style/tokens.json      what each colour MEANS
 *   packages/map/style/themes/*.json    its value in each theme
 *        │  pnpm style:build
 *        ▼
 *   style.json / style.meta.json        generated; `resolveTheme()` reads them
 *        │
 *        ├─ the map (layers, runtime-style, hatches, the gauge pill, the controls)
 *        └─ `palette` in packages/ui-native/src/theme.ts (every phone component)
 *
 * This gate fails on a colour written anywhere else — a hex, an `rgb()`/`hsl()`, or a CSS
 * named colour used as one. It exists because every copy that was typed out drifted: the
 * legend's stocking ramp was five colours the map never drew, the gauge pill's border read a
 * token that did not exist and fell back to a literal in every theme, and `controls.css`
 * carried the light theme's hexes as fallbacks a dark map would show the moment a variable
 * went missing.
 *
 * WHAT IS SCANNED: code — `.ts`, `.tsx`, `.mjs`, `.js`, `.css`, and the app's JSON — under
 * packages/, apps/, tools/ and conformance/. Comments are stripped first: a comment that
 * cites the hex a colour USED to be is history, not a colour, and the history is how the
 * next person learns why the value moved. JSON `$comment` strings are skipped for the same
 * reason. design/ is the mock-up archive (`riffle.html` is a picture to match, not code that
 * ships) and is not scanned.
 *
 * THE ALLOWLIST IS THE PALETTE AND NOTHING ELSE. Adding a path to it is the one way past
 * this gate, and it should read as what it is in review: a second place colours live.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const root = new URL("..", import.meta.url).pathname;

/** The palette: the only files allowed to hold a colour value. */
export const ALLOW = new Set([
  "packages/map/style/tokens.json",
  "packages/map/style/themes/light.json",
  "packages/map/style/themes/dark.json",
  "packages/map/style/themes/cvd.json",
  // GENERATED from the three above by `pnpm style:build`, hash-checked by `pnpm style`.
  "packages/map/style/style.json",
  "packages/map/style/style.meta.json",
]);

const ROOTS = ["packages", "apps", "tools", "conformance"];
const SKIP_DIR = new Set(["node_modules", "dist", ".expo", "android", "ios", "web-build"]);
const EXT = /\.(tsx?|mjs|cjs|js|css|json)$/;

/** CSS named colours. `transparent` and `currentColor` say "no colour of our own" and are fine. */
const NAMED = new Set(("aliceblue antiquewhite aqua aquamarine azure beige bisque black " +
  "blanchedalmond blue blueviolet brown burlywood cadetblue chartreuse chocolate coral " +
  "cornflowerblue cornsilk crimson cyan darkblue darkcyan darkgoldenrod darkgray darkgreen " +
  "darkgrey darkkhaki darkmagenta darkolivegreen darkorange darkorchid darkred darksalmon " +
  "darkseagreen darkslateblue darkslategray darkslategrey darkturquoise darkviolet deeppink " +
  "deepskyblue dimgray dimgrey dodgerblue firebrick floralwhite forestgreen fuchsia " +
  "gainsboro ghostwhite gold goldenrod gray green greenyellow grey honeydew hotpink " +
  "indianred indigo ivory khaki lavender lavenderblush lawngreen lemonchiffon lightblue " +
  "lightcoral lightcyan lightgoldenrodyellow lightgray lightgreen lightgrey lightpink " +
  "lightsalmon lightseagreen lightskyblue lightslategray lightslategrey lightsteelblue " +
  "lightyellow lime limegreen linen magenta maroon mediumaquamarine mediumblue mediumorchid " +
  "mediumpurple mediumseagreen mediumslateblue mediumspringgreen mediumturquoise " +
  "mediumvioletred midnightblue mintcream mistyrose moccasin navajowhite navy oldlace olive " +
  "olivedrab orange orangered orchid palegoldenrod palegreen paleturquoise palevioletred " +
  "papayawhip peachpuff peru pink plum powderblue purple rebeccapurple red rosybrown " +
  "royalblue saddlebrown salmon sandybrown seagreen seashell sienna silver skyblue slateblue " +
  "slategray slategrey snow springgreen steelblue tan teal thistle tomato turquoise violet " +
  "wheat white whitesmoke yellow yellowgreen").split(" "));

/**
 * A named colour is only a colour where something is being COLOURED. `namedFlavor("black")`
 * picks Protomaps' dark basemap and is a name, not a paint; `color: "black"` is a paint.
 */
const COLOUR_CONTEXT = /colou?r|background|border|fill|stroke|tint|shadow|tone|swatch|stop|halo|ink|paint/i;

/** Strip `//` and block comments, keeping strings (and line numbers) intact. */
export function stripComments(src) {
  let out = "", i = 0, q = null;
  while (i < src.length) {
    const c = src[i], n = src[i + 1];
    if (q) {
      out += c;
      if (c === "\\") { out += n ?? ""; i += 2; continue; }
      if (c === q) q = null;
      i++;
    } else if (c === '"' || c === "'" || c === "`") {
      q = c; out += c; i++;
    } else if (c === "/" && n === "/") {
      while (i < src.length && src[i] !== "\n") i++;
    } else if (c === "/" && n === "*") {
      i += 2;
      while (i < src.length && !(src[i] === "*" && src[i + 1] === "/")) {
        if (src[i] === "\n") out += "\n";
        i++;
      }
      i += 2;
    } else { out += c; i++; }
  }
  return out;
}

/** Drop every `"$…": "…"` member of a JSON document — they are prose, not values. */
function stripJsonComments(src) {
  return src.replace(/"\$[A-Za-z_]*"\s*:\s*"(?:[^"\\]|\\.)*"/g, (m) => m.replace(/[^\n]/g, " "));
}

const HEX = /(?<![\w&$/])#(?:[0-9a-fA-F]{8}|[0-9a-fA-F]{6}|[0-9a-fA-F]{3,4})(?![\w-])/g;
const FUNC = /\b(?:rgba?|hsla?)\(\s*[\d.]/g;
const QUOTED = /["'`]([a-zA-Z]+)["'`]/g;

/** Every colour literal in one file's text: `[{line, text}]`. */
export function findColours(src, path = "x.ts") {
  const code = path.endsWith(".json") ? stripJsonComments(src) : stripComments(src);
  const hits = [];
  code.split("\n").forEach((line, i) => {
    const found = [...line.matchAll(HEX), ...line.matchAll(FUNC)].map((m) => m[0]);
    if (COLOUR_CONTEXT.test(line) || path.endsWith(".css"))
      for (const m of line.matchAll(QUOTED)) if (NAMED.has(m[1].toLowerCase())) found.push(m[0]);
    if (path.endsWith(".css"))
      for (const m of line.matchAll(/:\s*([a-zA-Z]+)\s*[;!}]/g))
        if (NAMED.has(m[1].toLowerCase())) found.push(m[1]);
    for (const f of found) hits.push({ line: i + 1, text: f });
  });
  return hits;
}

function* walk(dir) {
  for (const name of readdirSync(dir)) {
    if (SKIP_DIR.has(name) || name.startsWith(".")) continue;
    const p = join(dir, name);
    if (statSync(p).isDirectory()) yield* walk(p);
    else if (EXT.test(name) && !name.endsWith(".tsbuildinfo")) yield p;
  }
}

/** Every colour written outside the palette, across the workspace. */
export function scan() {
  const out = [];
  for (const r of ROOTS)
    for (const p of walk(join(root, r))) {
      const rel = relative(root, p);
      if (ALLOW.has(rel)) continue;
      for (const h of findColours(readFileSync(p, "utf8"), rel)) out.push({ file: rel, ...h });
    }
  return out;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const hits = scan();
  if (hits.length) {
    for (const h of hits) console.error(`  ${h.file}:${h.line}  ${h.text}`);
    console.error(`✗ ${hits.length} colour literal(s) outside the palette. Add a token to ` +
                  `packages/map/style/tokens.json + themes/, run pnpm style:build, and read it ` +
                  `through resolveTheme() or \`palette\`.`);
    process.exit(1);
  }
  console.log(`✓ colours: every one is a token (palette: ${ALLOW.size} files)`);
}
