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

/**
 * Our geometry vocabulary -> MapLibre's LAYER TYPE vocabulary. They are not the same words.
 *
 * `layers.source.json` says what a layer IS ("polygon"); a style says how it is DRAWN
 * ("fill"). Emitting our word straight into the style produced a style MapLibre refuses to
 * load at all — and, more quietly, one the adapter mis-painted: `baseAdapter` picks
 * `fill-color` for type "fill" and fell through to `circle-color` for every "polygon",
 * so administrative areas were being handed a paint property they do not have.
 *
 * Neither the tile-contract test nor the palette check could see this, because both compare
 * the style to OUR artifacts. Nothing compared it to the renderer's spec until it was run.
 */
const MAPLIBRE_TYPE = { line: "line", polygon: "fill", point: "circle" };

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

// --- colour tokens must be PERCEPTUALLY distinguishable, not merely different ---
//
// Exact-equality was the first version of this rule and it was too weak: it caught
// status.open == flow.normal, but missed color.highlight sitting ΔE 7.6 from
// status.restricted — a selected reach that looks like a restricted one.
//
// Threshold is ΔE76 >= 12 between tokens from DIFFERENT families. The target is 20; it is
// 12 today because the dark theme cannot reach 20 while `status.default_only` is a grey
// LINE colour competing with `water.mapped`. That is not a palette problem — it is the
// outcome/provenance conflation recorded in 13-build-plan §2.1. Raise this to 20 when
// default_only becomes a provenance chip.
//
// Steps WITHIN the flow ramp are exempt: a sequential ramp is meant to be ordered and
// close, and its members never encode different meanings.
const MIN_DELTA_E = 12;
const _lab = (hex) => {
  const h = hex.replace("#", "");
  const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16) / 255)
    .map((c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  const f = (t) => (t > 0.008856 ? Math.cbrt(t) : 7.787 * t + 16 / 116);
  const X = f((r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047);
  const Y = f(r * 0.2126 + g * 0.7152 + b * 0.0722);
  const Z = f((r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883);
  return [116 * Y - 16, 500 * (X - Y), 200 * (Y - Z)];
};
const deltaE = (a, b) => Math.hypot(...
  _lab(a).map((v, i) => v - _lab(b)[i]));
const family = (name) => name.split(".")[1];

for (const t of themes) {
  const cols = Object.entries(t.values)
    .filter(([, v]) => typeof v === "string" && v.startsWith("#"));
  for (let i = 0; i < cols.length; i++) {
    for (let j = i + 1; j < cols.length; j++) {
      const [n1, v1] = cols[i], [n2, v2] = cols[j];
      if (family(n1) === family(n2) && family(n1) === "flow") continue;   // sequential ramp
      const d = deltaE(v1, v2);
      if (d < MIN_DELTA_E)
        err(`theme "${t.name}": ${n1} (${v1}) and ${n2} (${v2}) are ΔE ${d.toFixed(1)} apart ` +
            `— under ${MIN_DELTA_E}, a reader cannot reliably tell them apart on a 1.4px line.`);
    }
  }
}

// --- every status colour must be legible as TEXT on its own theme's panel ---
//
// `status.restricted` shipped at #d98c00: 2.73:1 on white, which fails WCAG AA *and*
// AA-large, while being used as a pill fill under white text and as a 10pt status word.
// A density review caught it; nobody had run the numbers on the palette we ship.
//
// 3.0 is the AA-large floor and the minimum for a status WORD, which is what carries the
// answer once texture is ruled out as an instrument (see 13-build-plan §2.1).
const PANEL = { light: "#ffffff", dark: "#181d24" };
const MIN_CONTRAST = 3.0;
const _lum = (hex) => {
  const h = hex.replace("#", "");
  const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16) / 255)
    .map((c) => (c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
const contrast = (a, b) => {
  const [x, y] = [_lum(a), _lum(b)].sort((p, q) => q - p);
  return (x + 0.05) / (y + 0.05);
};
for (const t of themes) {
  const panel = PANEL[t.name];
  if (!panel) continue;
  for (const [name, value] of Object.entries(t.values)) {
    if (!name.startsWith("color.status.") || typeof value !== "string") continue;
    const c = contrast(value, panel);
    if (c < MIN_CONTRAST)
      err(`theme "${t.name}": ${name} (${value}) is ${c.toFixed(2)}:1 on the panel ` +
          `${panel} — under ${MIN_CONTRAST}:1 it is not legible as a status word.`);
  }
}

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
  if (s.external && !s.attribution) {
    err(`source "${id}" is external but has no attribution — third-party tiles carry licence terms ours do not`);
  } else if (s.external && /\b(TODO|TBD|FIXME|XXX|placeholder)\b/i.test(s.attribution)) {
    // Presence was not enough: the parcels source shipped `attribution: "TODO: name the
    // provider and licence before this ships"` and passed, because the guard only checked
    // that the field was non-empty. A placeholder is how a licensed layer reaches a store.
    err(`source "${id}" attribution is still a placeholder (${JSON.stringify(s.attribution)}) ` +
        `— name the provider and licence, or set external:false if it is ours`);
  }

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
/** Generated companion line layers: edge layer id -> the polygon layer it outlines. */
const edges = new Map();

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
  if (l.opacity) tokenValue(l.opacity, `${where} opacity`);
  if (l.dash) tokenValue(l.dash, `${where} dash`);
  if (l.width) {
    tokenValue(l.width, `${where} width`);
    // A fill has no width in MapLibre. Declaring one produced a style that threw on load
    // and took the entire recolouring pass down with it, so NOTHING on the map got its
    // colours — from one line of source that looked perfectly reasonable.
    if (l.geometry === "polygon")
      err(`${where}: a polygon has no width. MapLibre fills cannot be stroked; an outline ` +
           `needs a companion line layer over the same source-layer.`);
  }
  layersById.set(l.id, l);

  const g = groups.get(l.group);
  const geom = {
    source: l.source,
    ...(l.sourceLayer ? { "source-layer": l.sourceLayer } : {}),
    ...(l.minzoom !== undefined ? { minzoom: l.minzoom } : {}),
    ...(l.maxzoom !== undefined ? { maxzoom: l.maxzoom } : {}),
    layout: { visibility: g?.defaultVisible ? "visible" : "none" },
  };
  outLayers.push({ id: l.id, type: MAPLIBRE_TYPE[l.geometry], ...geom });

  /**
   * A COMPANION LINE LAYER over the same source-layer.
   *
   * MapLibre fills cannot be stroked, so a polygon that wants an edge needs a second layer
   * — which is exactly what v1 did for lakes and for every admin area
   * (`archive/webapp/src/map/styles.ts`: a `-fill` and a `-line` for each). Without it a
   * lake is a flat blob with no shoreline and a management unit has no visible boundary at
   * all, which is most of why the map read as unfinished.
   *
   * Generated rather than authored so the edge cannot drift from the fill it belongs to:
   * same source-layer, same zooms, same group, and it appears in every view.
   */
  if (l.outline) {
    const id = `${l.id}__edge`;
    tokenValue(l.outline.color, `${where} outline colour`);
    if (l.outline.width) tokenValue({ token: l.outline.width.token }, `${where} outline width`);
    outLayers.push({ id, type: "line", ...geom });
    edges.set(id, l);
  }
}


// --- a colour token may not cross semantic families ---
//
// Tokens are namespaced by MEANING: `status.*` is regulatory, `flow.*` is hydrological,
// `water.*` is neutral ("no data expressed") and may be used anywhere. A mode reading the
// `regs` provider may use status.*; one reading a feed like `gauges` may use flow.*.
//
// This exists because the discharge mode declared `missing: color.status.unknown`. About
// 19,250 of 19,700 waters have no gauge, so nearly the whole Conditions view rendered in
// the same violet that means "we could not parse this regulation" in the Regulations view
// — two unrelated unknowns, one colour. The ΔE rule cannot catch it: it is the SAME token,
// not two similar ones.
const FAMILY_FOR_PROVIDER = { regs: "status", gauges: "flow" };
for (const l of src.layers ?? []) {
  for (const [mode, m] of Object.entries(l.colorModes ?? {})) {
    const provider = m.data?.provider;
    const allowed = provider ? FAMILY_FOR_PROVIDER[provider] : null;
    const refs = [
      m.color, m.missing,
      ...Object.values(m.categories ?? {}),
      ...(m.stops ?? []).map(([, r]) => r),
    ].filter((r) => r && typeof r === "object" && r.token);
    for (const { token } of refs) {
      const fam = token.split(".")[1];
      if (fam === "water") continue;                    // neutral: always permitted
      if (allowed && fam !== allowed)
        err(`layer "${l.id}" mode "${mode}" reads provider "${provider}" but uses ` +
            `"${token}" — a ${fam}.* token. That colour already means something else in ` +
            `another view; use a ${allowed}.* token or a neutral water.* one.`);
      if (!allowed && fam === "flow")
        err(`layer "${l.id}" mode "${mode}" has no data provider but uses "${token}"`);
    }
  }
}

// --- views must name real layers and real modes; exactly one default ---
const defaults = (src.views ?? []).filter((v) => v.default);
if (defaults.length !== 1) err(`exactly one view must be marked default (found ${defaults.length})`);
/**
 * Fill in everything a generated edge layer needs, so it is a first-class layer rather
 * than something the runtime has to special-case: one static colour mode, an entry in
 * every view, and the same group as the fill it outlines (so one toggle moves both).
 */
for (const [id, l] of edges) {
  layersById.set(id, {
    ...l, id, geometry: "line",
    colorModes: { plain: { label: `${l.id} outline`, scale: "static", color: l.outline.color } },
  });
  for (const v of src.views ?? []) v.modes[id] = "plain";
}

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
  layerGroup: Object.fromEntries([
    ...(src.layers ?? []).map((l) => [l.id, l.group]),
    // an edge belongs to the group of the fill it outlines, so one toggle moves both
    ...[...edges].map(([id, l]) => [id, l.group]),
  ]),
  highlightable: (src.layers ?? []).filter((l) => l.highlightable)
    .map((l) => ({ id: l.id, featureIdProperty: l.featureIdProperty })),
  colorModes: Object.fromEntries([
    ...(src.layers ?? []).map((l) => [l.id, l.colorModes]),
    ...[...edges].map(([id]) => [id, layersById.get(id).colorModes]),
  ]),
  // WHICH PROPERTY carries the feature id, per layer. The style knew a property had been
  // named; nothing knew which one, so nothing could tell MapLibre to promote it.
  featureIds: Object.fromEntries((src.layers ?? [])
    .filter((l) => l.featureIdProperty)
    .map((l) => [l.id, l.featureIdProperty])),
  widths: Object.fromEntries([
    ...(src.layers ?? []).filter((l) => l.width)
      .map((l) => [l.id, l.width.by
        ? { token: l.width.token, by: l.width.by, mode: l.width.mode ?? "linear",
            ramp: l.width.ramp }
        : l.width.token]),
    ...[...edges].filter(([, l]) => l.outline.width)
      .map(([id, l]) => [id, l.outline.width.by
        ? { token: l.outline.width.token, by: l.outline.width.by,
            mode: l.outline.width.mode ?? "linear", ramp: l.outline.width.ramp }
        : l.outline.width.token]),
  ]),
  // Opacity rides the same rail as width: a token, applied at runtime, themeable. It is
  // NOT baked into the style's paint because a theme may want a different value — and
  // because wetland at full opacity buried every other layer on the map, which is the
  // failure that made this necessary.
  opacities: Object.fromEntries((src.layers ?? []).filter((l) => l.opacity)
    .map((l) => [l.id, l.opacity.token])),
  // A dash pattern. The under-lake layer's comment said "drawn dotted" for months while
  // the style had no way to express a dash, so it drew solid — and 303,932 solid routes
  // over every lake in the province is a spider web.
  dashes: Object.fromEntries((src.layers ?? []).filter((l) => l.dash)
    .map((l) => [l.id, l.dash.token])),
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
