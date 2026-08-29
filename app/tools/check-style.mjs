#!/usr/bin/env node
/**
 * The style is generated and shared. Two ways that guarantee breaks:
 *   1. someone hand-edits style.json / style.meta.json   -> hash mismatch
 *   2. an app defines layers itself                      -> layers.json boundary ban
 */
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

const dir = new URL("../packages/map/style/", import.meta.url).pathname;
const want = readFileSync(dir + "style.sha256", "utf8").trim();
const got = createHash("sha256")
  .update(readFileSync(dir + "style.json") + readFileSync(dir + "style.meta.json"))
  .digest("hex");

if (got !== want) {
  console.error(
    `✗ the generated map style is stale or was hand-edited.\n` +
    `    recorded : ${want}\n    actual   : ${got}\n` +
    `  Edit layers.source.json / tokens.json / themes/, then: pnpm style:build`
  );
  process.exit(1);
}

const style = JSON.parse(readFileSync(dir + "style.json", "utf8"));
const meta = JSON.parse(readFileSync(dir + "style.meta.json", "utf8"));
for (const k of ["version", "sources", "layers"])
  if (!(k in style)) { console.error(`✗ style.json missing "${k}"`); process.exit(1); }

// every layer belongs to a declared group, and every highlightable layer exists
const groupIds = new Set(meta.groups.map((g) => g.id));
for (const l of style.layers) {
  const g = meta.layerGroup[l.id];
  if (!groupIds.has(g)) { console.error(`✗ layer "${l.id}" -> unknown group "${g}"`); process.exit(1); }
}
const ids = new Set(style.layers.map((l) => l.id));
for (const h of meta.highlightable) {
  if (!ids.has(h.id)) { console.error(`✗ highlightable layer "${h.id}" not in style`); process.exit(1); }
  if (!h.featureIdProperty) { console.error(`✗ "${h.id}" highlightable with no featureIdProperty`); process.exit(1); }
}
const defaults = meta.views.filter((v) => v.default);
if (defaults.length !== 1) { console.error(`✗ expected exactly 1 default view, got ${defaults.length}`); process.exit(1); }

console.log(`✓ style intact: ${style.layers.length} layers, ${meta.groups.length} groups, ` +
            `${meta.views.length} views, ${Object.keys(meta.themes).length} themes, sha ${got.slice(0, 12)}`);
