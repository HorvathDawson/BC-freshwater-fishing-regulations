#!/usr/bin/env node
/**
 * Enforces layers.json. This is the rule that would have prevented the v1 client drift
 * (4 shared-logic files, 1,041 lines diverged between webapp/ and mobile/).
 *
 * Deliberately dependency-free and ~60 lines: a boundary check nobody can read is a
 * boundary check nobody maintains.
 */
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";

// The app root by default. `CHECK_ROOT` points the gate at another tree: its own tests write their fixtures into a
// scratch copy OUTSIDE the app, never into packages/ where every other tree-scanning test walks (T156: a `_plat.ts`
// written here was deleted under check-colours / one-speaker mid-walk, ENOENT, in a parallel run).
const root = process.env.CHECK_ROOT ? process.env.CHECK_ROOT.replace(/\/?$/, "/") : new URL("..", import.meta.url).pathname;
const spec = JSON.parse(readFileSync(join(root, "layers.json"), "utf8"));
// Covers: `import x from "y"`, bare `import "y"`, `export … from "y"`,
// `require("y")`, and dynamic `import("y")`. The bare form matters — it is how a
// side-effecting platform import sneaks past a naive `from`-only regex.
//
// THE CLAUSE MAY NOT CONTAIN `(`, `)` OR `;`, which is what stops the lazy fill running off
// the end of a declaration and into the file. `export function DonorPanel(` starts a match
// that used to consume the entire component body looking for a ` from "`, and found one in
// JSX — `{cond ? "from" : "forecast from"}{" "}` — reporting the module as importing `}{`.
// A real import or re-export clause is specifiers and commas: no parentheses, no semicolon.
// Newlines are still allowed, because a long specifier list is routinely wrapped.
const CLAUSE = String.raw`[^;()]*?`;
const IMPORT_RE = new RegExp(
  [
    String.raw`(?:^|[\n;])\s*import\s+${CLAUSE}\sfrom\s*["']([^"']+)["']`,
    String.raw`(?:^|[\n;])\s*import\s*["']([^"']+)["']`,                      // import "y"
    String.raw`(?:^|[\n;])\s*export\s+${CLAUSE}\sfrom\s*["']([^"']+)["']`,
    String.raw`\brequire\s*\(\s*["']([^"']+)["']`,                           // require("y")
    String.raw`\bimport\s*\(\s*["']([^"']+)["']`,                            // import("y")
  ].join("|"),
  "g",
);

/** Block and line comments out, string literals left alone. */
function stripComments(src) {
  let out = "";
  let i = 0;
  let mode = "code";           // code | line | block | s | d | t
  while (i < src.length) {
    const c = src[i], n = src[i + 1];
    if (mode === "code") {
      if (c === "/" && n === "/") { mode = "line"; i += 2; continue; }
      if (c === "/" && n === "*") { mode = "block"; i += 2; continue; }
      if (c === "'") mode = "s";
      else if (c === '"') mode = "d";
      else if (c === "`") mode = "t";
      out += c; i++; continue;
    }
    if (mode === "line") {
      if (c === "\n") { mode = "code"; out += c; }
      i++; continue;
    }
    if (mode === "block") {
      if (c === "*" && n === "/") { mode = "code"; i += 2; continue; }
      if (c === "\n") out += c;          // keep line numbers honest
      i++; continue;
    }
    // inside a string: copy through, honouring escapes
    out += c;
    if (c === "\\") { out += src[i + 1] ?? ""; i += 2; continue; }
    if ((mode === "s" && c === "'") || (mode === "d" && c === '"') ||
        (mode === "t" && c === "`")) mode = "code";
    i++;
  }
  return out;
}

function walk(dir, out = []) {
  for (const name of readdirSync(dir)) {
    if (name === "node_modules" || name === "dist") continue;
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, out);
    else if (/\.(ts|tsx|js|jsx|mjs)$/.test(p)) out.push(p);
  }
  return out;
}

/**
 * MOST SPECIFIC LAYER WINS.
 *
 * A layer may sit inside another — `packages/data/src/sql/drivers` inside
 * `packages/data` — and when it does, only the inner rules apply. Without this, checking
 * every layer against every file it contains means an inner layer can only ever be
 * STRICTER than its parent, so the one thing a nested layer is for (a driver naming a
 * platform database that the library around it must never name) is impossible to express.
 *
 * Sorted longest-path-first so the first match is the most specific.
 */
const dirFor = (layer) => {
  // A package layer is named by its package root and its code lives in `src`. A nested
  // layer is named by the directory itself — `packages/data/src/sql/drivers` has no
  // `src` of its own, and appending one pointed the walk at nothing, so the parent's
  // rules kept applying and the nested layer had no effect at all.
  const withSrc = join(root, layer, "src");
  try { return statSync(withSrc).isDirectory() ? withSrc : join(root, layer); }
  catch { return join(root, layer); }
};
const layerRoots = Object.entries(spec.layers)
  .map(([layer, rules]) => ({ layer, rules, dir: dirFor(layer) }))
  .sort((a, b) => b.dir.length - a.dir.length);

const layerFor = (file) => layerRoots.find((l) => file.startsWith(l.dir + "/"));

let failed = 0;
for (const { layer, rules, dir } of layerRoots) {
  let files;
  try { files = walk(dir); } catch { continue; }   // layer not created yet
  files = files.filter((f) => layerFor(f)?.layer === layer);
  for (const file of files) {
    // Comments are stripped FIRST. The import regex matches `… from "x"` wherever it
    // appears, and a sentence in a doc comment that happens to contain that shape was
    // reported as an illegal import of a fragment of English. A boundary checker that
    // fails on prose teaches people to stop writing prose.
    const src = stripComments(readFileSync(file, "utf8"));
    const isTest = /\.(test|spec)\.[tj]sx?$/.test(file);
    const extra = isTest ? (spec.testOnlyImports ?? []) : [];
    for (const m of src.matchAll(IMPORT_RE)) {
      const dep = m.slice(1).find((g) => g !== undefined);
      if (!dep || dep.startsWith(".")) continue;             // relative = same layer, fine
      const bare = dep.startsWith("@") ? dep.split("/").slice(0, 2).join("/") : dep.split("/")[0];
      const banned = rules.banned.some((b) => bare === b || dep === b);
      const allowed = rules.mayImport.includes(bare) || extra.includes(bare);
      if (banned || !allowed) {
        console.error(
          `✗ ${relative(root, file)}\n    imports "${dep}"\n` +
          `    ${layer} may import: ${rules.mayImport.join(", ") || "(nothing external)"}\n` +
          `    why: ${rules.why}\n`
        );
        failed++;
      }
    }
  }
}

/*
 * (T1154) THE MAP ENGINE STAYS IN ITS FOLDER. The phone's map is a shell (`packages/map/src/Map.native.tsx`) over an
 * engine in `packages/map/src/native/<engine>/` (today `maplibre/`), and the shell reaches it through ONE file,
 * `native/engine.ts` (a build-time alias later, T1154 L4). layers.json keeps the SDK's import inside the engine's own
 * layer; this keeps the rest of the engine there too, which an import rule cannot see: outside an engine folder no
 * source file of packages/map/src may name a map SDK (in any string: an import, a require, a lazy load), a binding's
 * native name (`"MLRN…`), or React Native's device event bus (the binding's events), and none but `native/engine.ts`
 * may import from an engine folder. Comments are stripped first. Test files are the engine table's own harness (they
 * mock each SDK at its boundary and fire its events) and are not read. The web map (maplibre-gl) is out of scope.
 *
 * The engine folders are NAMED (`ENGINES`), never guessed from what `native/` holds: a helper folder beside them is
 * scanned like any other file. An engine folder or the map's source that is missing FAILS the gate, by name: a rule
 * with nothing to read must never pass for clean (W46).
 */
const MAP_SRC = join(root, "packages/map/src");
const ENGINE_HOME = join(MAP_SRC, "native");
const GATEWAY = join(ENGINE_HOME, "engine.ts");
const ENGINE_SDKS = ["@maplibre/maplibre-react-native", "@rnmapbox/maps"];
/** The phone map's engines, each a folder of `native/` (T1155 would add "mapbox"). */
const ENGINES = ["maplibre"];
const ENGINE_WORDS = [
  ...ENGINE_SDKS.map((sdk) => ({ re: new RegExp(`["'\`]${sdk.replace(/[/.]/g, "\\$&")}`), what: `names the map SDK "${sdk}"` })),
  { re: /["'`]MLRN/, what: 'names a binding\'s native name ("MLRN…")' },
  { re: /\bDeviceEventEmitter\b/, what: "uses DeviceEventEmitter (the binding's events)" },
  { re: /\bNativeEventEmitter\b/, what: "uses NativeEventEmitter (a native module's events)" },
  { re: /\bNativeModules\b/, what: "uses NativeModules (a native module by name)" },
  { re: /\brequireNativeComponent\b/, what: "uses requireNativeComponent (a native view by name)" },
];
const engines = ENGINES.map((n) => join(ENGINE_HOME, n));
for (const e of engines) {
  if (existsSync(e) && statSync(e).isDirectory()) continue;
  console.error(`✗ ${relative(root, e)}\n    the engine folder check-boundaries.mjs names (ENGINES) is missing: the engine rule cannot hold it (T1154)\n`);
  failed++;
}
const inEngine = (p) => engines.some((e) => p === e || p.startsWith(e + "/"));
const engineFiles = existsSync(MAP_SRC) ? walk(MAP_SRC) : [];
if (!existsSync(MAP_SRC)) {
  console.error(`✗ ${relative(root, MAP_SRC)}\n    the map's source is missing: the engine rule has nothing to read (T1154)\n`);
  failed++;
}
for (const file of engineFiles) {
  if (inEngine(file) || /\.(test|spec)\.[tj]sx?$/.test(file)) continue;
  const src = stripComments(readFileSync(file, "utf8"));
  const bad = ENGINE_WORDS.filter((w) => w.re.test(src)).map((w) => w.what);
  for (const m of src.matchAll(IMPORT_RE)) {
    const dep = m.slice(1).find((g) => g !== undefined);
    if (dep?.startsWith(".") && file !== GATEWAY && inEngine(join(dirname(file), dep)))
      bad.push(`imports "${dep}" from an engine folder`);
  }
  for (const what of bad) {
    console.error(
      `✗ ${relative(root, file)}\n    ${what}\n` +
      `    the map engine stays in packages/map/src/native/<engine>/; the shell reaches it only through native/engine.ts (T1154)\n`
    );
    failed++;
  }
}

if (failed) { console.error(`\n${failed} boundary violation(s). See layers.json.`); process.exit(1); }
console.log("✓ layer boundaries clean");
