#!/usr/bin/env node
/**
 * layers.source.json + tokens.json + themes/*  ->  style.json + style.meta.json
 *
 * Three things get called "the style". Kept apart on purpose:
 *   1. CATALOG  which layers exist, how they can be coloured, which toggle  -> baked
 *   2. THEME    the values behind named tokens                             -> runtime
 *   3. STATE    what is on, which view is picked, what is highlighted       -> runtime
 *
 * Only the catalog is baked, which is why a colour-mode switch, a dark theme and a
 * per-user palette all cost zero rebuilds.
 *
 * The semantic checks below are the point of this file — they encode correctness rules
 * from the plan that JSON Schema cannot express.
 */
import { readFileSync, writeFileSync, readdirSync } from "node:fs";
import { createHash } from "node:crypto";

const dir = new URL("../packages/map/style/", import.meta.url).pathname;
const src = JSON.parse(readFileSync(dir + "layers.source.json", "utf8"));
const tok = JSON.parse(readFileSync(dir + "tokens.json", "utf8"));
const tokens = tok.tokens, enums = tok.$enums ?? {};
const themes = readdirSync(dir + "themes")
  .filter((f) => f.endsWith(".json"))
  .map((f) => JSON.parse(readFileSync(dir + "themes/" + f, "utf8")));

const errs = [];
const err = (m) => errs.push(m);
const used = new Set();

function tokenValue(ref, where) {
  if (!ref || typeof ref !== "object" || !("token" in ref))
    return err(`${where}: expected {"token": "..."}, got ${JSON.stringify(ref)} — literals are not allowed`);
  if (!tokens[ref.token]) return err(`${where}: unknown token "${ref.token}"`);
  used.add(ref.token);
  const base = themes.find((t) => t.name === "light") ?? themes[0];
  return base?.values[ref.token];
}

// --- every theme must define every themeable token ---
for (const t of themes)
  for (const [name, def] of Object.entries(tokens))
    if (def.themeable && t.values[name] === undefined)
      err(`theme "${t.name}" is missing themeable token "${name}"`);

// --- no two colour tokens may share a value ---
// Each token exists to mean a DIFFERENT thing, so an identical hex is a semantic collision.
// It hid here for real: `color.status.open` and `color.flow.normal` were both #2e8b57, so the
// same green line meant "you may fish here" in the Regulations view and "normal water level"
// in Conditions. The enum-coverage rule cannot see it — the collision is ACROSS colour modes,
// not within one.
for (const t of themes) {
  const byValue = new Map();
  for (const [name, value] of Object.entries(t.values)) {
    if (typeof value !== "string" || !value.startsWith("#")) continue;
    const k = value.toLowerCase();
    if (byValue.has(k))
      err(`theme "${t.name}": ${byValue.get(k)} and ${name} are both ${value} — two meanings, ` +
          `one colour. A reader cannot tell them apart.`);
    else byValue.set(k, name);
  }
}

// --- external sources must be attributed ---
for (const [id, s] of Object.entries(src.sources ?? {}))
  if (s.external && !s.attribution)
    err(`source "${id}" is external but has no attribution — third-party tiles carry licence terms ours do not`);

// --- providers: where a colour mode's VALUES come from (not the tiles) ---
const providers = Object.fromEntries(
  Object.entries(src.providers ?? {}).filter(([k]) => !k.startsWith("$")));
for (const [id, p] of Object.entries(providers)) {
  if (!["bundle", "feed"].includes(p.kind)) err(`provider "${id}": kind must be bundle|feed`);
  if (!p.key) err(`provider "${id}": must declare the key it is joined on (section_id, item_id, …)`);
  if (p.kind === "feed" && !p.staleAfterMs)
    err(`provider "${id}": a live feed must declare staleAfterMs — the app has to know when to stop ` +
        `rendering a cached value as current`);
}

const groups = new Map((src.groups ?? []).map((g) => [g.id, g]));
const layersById = new Map();
const outLayers = [];

for (const l of src.layers ?? []) {
  const where = `layer "${l.id}"`;
  if (!groups.has(l.group)) err(`${where}: unknown group "${l.group}"`);
  if (!src.sources?.[l.source]) err(`${where}: unknown source "${l.source}"`);
  if (l.highlightable && !l.featureIdProperty)
    err(`${where}: highlightable layers need featureIdProperty — feature-state has nothing to key on`);
  if (!l.colorModes || !Object.keys(l.colorModes).length)
    err(`${where}: needs at least one colour mode`);

  for (const [mode, m] of Object.entries(l.colorModes ?? {})) {
    const w = `${where} mode "${mode}"`;

    // A non-static colouring is driven by data that is NOT in the tiles. Say where from.
    if (m.scale !== "static") {
      if (!m.data) {
        err(`${w}: needs a "data" binding — "${m.property}" is not a tile property, so the app ` +
            `cannot know what to fetch or how to join it`);
      } else {
        const prov = providers[m.data.provider];
        if (!prov) err(`${w}: unknown provider "${m.data.provider}"`);
        else {
          if (!m.data.field) err(`${w}: data binding must name the field to read`);
          if (prov.key !== l.featureIdProperty && !prov.fanout)
            err(`${w}: provider "${m.data.provider}" is keyed on ${prov.key} but the layer draws ` +
                `${l.featureIdProperty} — declare a fanout, or the values cannot reach the geometry`);
          if (prov.kind === "feed" && !m.missing)
            err(`${w}: reads a live feed, so it MUST define "missing" — a station can always be ` +
                `absent, offline or stale, and that must not look like a real reading`);
        }
      }
    }

    if (m.scale === "static") {
      tokenValue(m.color, w);
    } else if (m.scale === "categorical") {
      if (!m.property) err(`${w}: categorical needs a driving property`);
      for (const [k, v] of Object.entries(m.categories ?? {})) tokenValue(v, `${w} category "${k}"`);
      if (m.enum) {
        const members = enums[m.enum];
        if (!members) err(`${w}: unknown enum "${m.enum}"`);
        else {
          for (const member of members)
            if (!m.categories?.[member])
              err(`${w}: enum "${m.enum}" member "${member}" has no colour. ` +
                  `Every member must be coloured — this is how "unknown" and "default_only" ` +
                  `cannot be silently rendered as something they are not.`);
          for (const k of Object.keys(m.categories ?? {}))
            if (!members.includes(k)) err(`${w}: category "${k}" is not in enum "${m.enum}"`);
        }
      } else if (!m.missing) {
        err(`${w}: a categorical mode with no declared enum must define "missing"`);
      }
      if (m.missing) tokenValue(m.missing, `${w} missing`);
    } else if (m.scale === "continuous") {
      if (!Array.isArray(m.stops) || m.stops.length < 2) err(`${w}: needs >= 2 stops`);
      let prev = -Infinity;
      for (const [at, ref] of m.stops ?? []) {
        if (typeof at !== "number") err(`${w}: stop position must be a number`);
        else if (at <= prev) err(`${w}: stops must ascend (got ${at} after ${prev})`);
        prev = at;
        tokenValue(ref, `${w} stop ${at}`);
      }
      if (!m.missing)
        err(`${w}: continuous modes must define "missing" — a feature with no reading must ` +
            `never inherit a colour implying one`);
      else tokenValue(m.missing, `${w} missing`);
    } else {
      err(`${w}: unknown scale "${m.scale}"`);
    }
  }
  if (l.width) tokenValue(l.width, `${where} width`);
  layersById.set(l.id, l);

  const g = groups.get(l.group);
  outLayers.push({
    id: l.id, type: l.geometry, source: l.source,
    ...(l.sourceLayer ? { "source-layer": l.sourceLayer } : {}),
    ...(l.minzoom !== undefined ? { minzoom: l.minzoom } : {}),
    ...(l.maxzoom !== undefined ? { maxzoom: l.maxzoom } : {}),
    layout: { visibility: g?.defaultVisible ? "visible" : "none" },
  });
}

// --- views must name real layers and real modes; exactly one default ---
const defaults = (src.views ?? []).filter((v) => v.default);
if (defaults.length !== 1) err(`exactly one view must be marked default (found ${defaults.length})`);
for (const v of src.views ?? [])
  for (const [layerId, mode] of Object.entries(v.modes ?? {})) {
    const l = layersById.get(layerId);
    if (!l) err(`view "${v.id}": unknown layer "${layerId}"`);
    else if (!l.colorModes[mode]) err(`view "${v.id}": layer "${layerId}" has no mode "${mode}"`);
  }

if (errs.length) {
  console.error("✗ map style source is invalid:\n  " + errs.join("\n  "));
  process.exit(1);
}

const style = {
  $comment: "GENERATED by tools/build-style.mjs. Do not hand-edit.",
  version: 8, name: "canifishthis", sources: src.sources ?? {}, layers: outLayers,
};

const meta = {
  $comment: "GENERATED. The runtime reads this to build the layer/view UI and apply themes.",
  groups: src.groups ?? [],
  views: src.views ?? [],
  providers,
  layerGroup: Object.fromEntries((src.layers ?? []).map((l) => [l.id, l.group])),
  highlightable: (src.layers ?? []).filter((l) => l.highlightable)
    .map((l) => ({ id: l.id, featureIdProperty: l.featureIdProperty })),
  colorModes: Object.fromEntries((src.layers ?? []).map((l) => [l.id, l.colorModes])),
  widths: Object.fromEntries((src.layers ?? []).filter((l) => l.width).map((l) => [l.id, l.width.token])),
  themes: Object.fromEntries(themes.map((t) => [t.name, t.values])),
  tokens, enums,
};

const w = (n, o) => { const j = JSON.stringify(o, null, 2) + "\n"; writeFileSync(dir + n, j); return j; };
const all = w("style.json", style) + w("style.meta.json", meta);
writeFileSync(dir + "style.sha256", createHash("sha256").update(all).digest("hex") + "\n");

const unused = Object.keys(tokens).filter((t) => !used.has(t));
console.log(`✓ style built: ${outLayers.length} layers · ${groups.size} groups · ` +
  `${(src.views ?? []).length} views · ${themes.length} themes` +
  (unused.length ? `\n  note: unused tokens — ${unused.join(", ")}` : ""));
