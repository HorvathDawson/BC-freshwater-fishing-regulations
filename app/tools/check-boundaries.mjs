#!/usr/bin/env node
/**
 * Enforces layers.json. This is the rule that would have prevented the v1 client drift
 * (4 shared-logic files, 1,041 lines diverged between webapp/ and mobile/).
 *
 * Deliberately dependency-free and ~60 lines: a boundary check nobody can read is a
 * boundary check nobody maintains.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const root = new URL("..", import.meta.url).pathname;
const spec = JSON.parse(readFileSync(join(root, "layers.json"), "utf8"));
// Covers: `import x from "y"`, bare `import "y"`, `export … from "y"`,
// `require("y")`, and dynamic `import("y")`. The bare form matters — it is how a
// side-effecting platform import sneaks past a naive `from`-only regex.
const IMPORT_RE = new RegExp(
  [
    String.raw`(?:^|[\n;])\s*import\s+(?:(?!\bimport\b|\bexport\b)[\s\S])*?\sfrom\s*["']([^"']+)["']`,
    String.raw`(?:^|[\n;])\s*import\s*["']([^"']+)["']`,                      // import "y"
    String.raw`(?:^|[\n;])\s*export\s+(?:(?!\bimport\b|\bexport\b)[\s\S])*?\sfrom\s*["']([^"']+)["']`,
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
 * A layer may sit inside another — `packages/data/src/bundle/drivers` inside
 * `packages/data` — and when it does, only the inner rules apply. Without this, checking
 * every layer against every file it contains means an inner layer can only ever be
 * STRICTER than its parent, so the one thing a nested layer is for (a driver naming a
 * platform database that the library around it must never name) is impossible to express.
 *
 * Sorted longest-path-first so the first match is the most specific.
 */
const dirFor = (layer) => {
  // A package layer is named by its package root and its code lives in `src`. A nested
  // layer is named by the directory itself — `packages/data/src/bundle/drivers` has no
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
if (failed) { console.error(`\n${failed} boundary violation(s). See layers.json.`); process.exit(1); }
console.log("✓ layer boundaries clean");
