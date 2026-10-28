#!/usr/bin/env node
/**
 * One-off GENERATOR (re-run only if the registry's identity scheme changes): the v1 site's indexed pages -> today's water items.
 *
 * v1 (archive/webapp/scripts/prerender.mjs) served `/waterbody/<wbg>/`, wbg = the FWA watershed code with its trailing
 * "-000000" padding stripped (streams) or the waterbody_key (lakes). `app/seo/legacy-paths.txt` is the v1 sitemap as it was
 * live on 2026-07-25 (19,331 paths, fetched from https://canifishthis.ca/sitemap.xml). Today's items carry the same codes as
 * `wsc:` / `wbk:` refs in the atlas registry, so a code resolves to the item that holds it. A code the registry no longer
 * holds falls back to its longest parent watershed code (a tributary -> the stream it joins), else to its region when the id
 * says one (UNGAZ_..._R7), else to nothing (the build sends it to the home page).
 *
 *   node app/tools/seo/legacy-crosswalk.mjs [registry.json]   ->  app/seo/legacy-crosswalk.json
 * Output: { "<old id>": [itemId, ...candidates in order, "r<region>"?] | "<region>" | null }; item ids resolve through the committed slug map.
 */
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const APP = join(dirname(fileURLToPath(import.meta.url)), "../..");
const registry = process.argv[2] ?? join(APP, "../data/generated/atlas/full/registry.json");
const old = readFileSync(join(APP, "seo/legacy-paths.txt"), "utf8").split("\n").filter(Boolean);
const items = JSON.parse(readFileSync(registry, "utf8")).items;

const held = new Map();   // "wsc:<code>" / "wbk:<key>" -> item ids
const add = (k, id) => { const l = held.get(k); if (!l) held.set(k, [id]); else if (!l.includes(id)) l.push(id); };
const regionOf = new Map();
for (const it of items) {
  for (const ref of [it.id, ...it.ref_ids]) if (/^(wsc|wbk):/.test(ref)) add(ref, it.id);
  const mu = (it.mus ?? [])[0];
  if (mu) regionOf.set(it.id, /^\d+/.exec(mu)?.[0] ?? null);
}

const out = {}, stat = { exact: 0, parent: 0, region: 0, none: 0 };
for (const o of old.sort()) {
  const key = /^\d+$/.test(o) ? `wbk:${o}` : `wsc:${o}`;
  let hit = held.get(key), how = "exact";
  if (!hit && key.startsWith("wsc:")) {
    for (let p = o; !hit && p.includes("-"); ) { p = p.slice(0, p.lastIndexOf("-")); hit = held.get(`wsc:${p}`); how = "parent"; }
  }
  if (hit) {
    const rg = hit.map((id) => regionOf.get(id)).find(Boolean);   // the region to fall back to when no candidate has a page
    out[o] = rg ? [...hit, `r${rg}`] : hit; stat[how]++; continue;
  }
  const r = /_R(\d+)[A-Z]?$/.exec(o);
  if (r) { out[o] = r[1]; stat.region++; } else { out[o] = null; stat.none++; }
}
writeFileSync(join(APP, "seo/legacy-crosswalk.json"), `${JSON.stringify(out, null, 0).replace(/,"/g, ',\n"')}\n`);
console.log(JSON.stringify(stat), "of", old.length);
