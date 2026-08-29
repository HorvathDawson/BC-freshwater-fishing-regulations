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

function walk(dir, out = []) {
  for (const name of readdirSync(dir)) {
    if (name === "node_modules" || name === "dist") continue;
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, out);
    else if (/\.(ts|tsx|js|jsx|mjs)$/.test(p)) out.push(p);
  }
  return out;
}

let failed = 0;
for (const [layer, rules] of Object.entries(spec.layers)) {
  const dir = join(root, layer, "src");
  let files;
  try { files = walk(dir); } catch { continue; }   // layer not created yet
  for (const file of files) {
    const src = readFileSync(file, "utf8");
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
