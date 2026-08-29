#!/usr/bin/env node
/**
 * Every top-level dependency needs a one-line justification in deps.md.
 *
 * v1's webapp accumulated chart.js, pdf-lib, pdfjs-dist, fuse.js, suncalc, pbf,
 * @mapbox/vector-tile — each defensible alone; together they are the bundle. This
 * does not block a dependency, it just makes adding one a deliberate act with a
 * reviewable diff.
 */
import { readFileSync, readdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const root = new URL("..", import.meta.url).pathname;
const noted = new Set(
  (existsSync(join(root, "deps.md")) ? readFileSync(join(root, "deps.md"), "utf8") : "")
    .split("\n")
    .map((l) => l.match(/^\s*[-*]\s*`([^`]+)`/)?.[1])
    .filter(Boolean),
);

const pkgs = ["package.json"];
for (const group of ["packages", "apps", "conformance"]) {
  const dir = join(root, group);
  if (!existsSync(dir)) continue;
  if (group === "conformance") pkgs.push("conformance/package.json");
  else for (const n of readdirSync(dir)) pkgs.push(`${group}/${n}/package.json`);
}

let missing = [];
for (const rel of pkgs) {
  const p = join(root, rel);
  if (!existsSync(p)) continue;
  const j = JSON.parse(readFileSync(p, "utf8"));
  for (const field of ["dependencies", "devDependencies"]) {
    for (const name of Object.keys(j[field] ?? {})) {
      if (name.startsWith("@app/")) continue;            // internal, always fine
      if (!noted.has(name)) missing.push(`${name}  (${rel})`);
    }
  }
}
if (missing.length) {
  console.error("✗ dependencies with no note in deps.md:\n  " + missing.join("\n  ") +
    "\n\n  Add a line to deps.md: - `name` — why it is here, and what it costs.");
  process.exit(1);
}
console.log(`✓ all dependencies justified in deps.md`);
