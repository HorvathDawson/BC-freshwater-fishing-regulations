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
const MAPLIBRE_TYPE = { line: "line", polygon: "fill", point: "circle", raster: "raster" };

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

// --- a theme value may NAME another token: "@color.ui.ink" ---
//
// For two tokens that are one colour BY DESIGN — the shadow is the ink, the first donor is
// the live feed. Written as a second hex, the pair trips the duplicate guard below (rightly:
// it cannot tell intent from collision) or drifts the first time one is retuned. Resolved
// here, so the generated themes carry plain values and no reader ever sees an "@".
const aliased = new Set();
for (const t of themes)
  for (const [name, v] of Object.entries(t.values)) {
    if (typeof v !== "string" || !v.startsWith("@")) continue;
    let target = v.slice(1), hops = 0;
    while (typeof t.values[target] === "string" && t.values[target].startsWith("@") && hops++ < 8)
      target = t.values[target].slice(1);
    if (!tokens[target] || t.values[target] === undefined || hops >= 8)
      err(`theme "${t.name}": ${name} names "${v}", which is not a token with a value here`);
    else if (tokens[target].type !== tokens[name]?.type)
      err(`theme "${t.name}": ${name} names ${target}, a ${tokens[target].type}, not a ${tokens[name]?.type}`);
    else { t.values[name] = t.values[target]; aliased.add(`${t.name}:${name}`); }
  }

// --- every theme must define every themeable token ---
for (const t of themes)
  for (const [name, def] of Object.entries(tokens))
    if (def.themeable && t.values[name] === undefined)
      err(`theme "${t.name}" is missing themeable token "${name}"`);

// --- and a token that is NOT themeable must be the same in every theme ---
// `themeable: false` is a claim (the satellite swatch previews photographs, which a theme
// does not recolour). Unchecked, a theme could quietly disagree with it.
for (const [name, def] of Object.entries(tokens)) {
  if (def.themeable) continue;
  const seen = new Set(themes.map((t) => JSON.stringify(t.values[name])));
  if (seen.size > 1) err(`token "${name}" is not themeable but the themes disagree: ${[...seen].join(" / ")}`);
}

/**
 * THE APP'S CHROME (`color.ui.*`) is in these themes so there is one palette, but it is never
 * map paint: a card colour is not "on screen" beside a river the way a map token is. The two
 * map guards below skip it; the duplicate guard runs within it; the ui-native and cvd tests
 * hold its contrast and separation.
 */
const isUi = (name) => name.split(".")[1] === "ui";

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
//
// AND THE COMPARISON IS SCOPED TO WHAT IS ON SCREEN TOGETHER. Two colour modes of one
// layer are ALTERNATIVES — MapLibre paints exactly one of them — so the stream's plain
// blue and the flow ramp's blues are never both drawn, and a reader has no opportunity to
// confuse them. Comparing every token against every other treated the union of all views
// as though it were one picture, and the blue band is crowded enough that it made whole
// palettes unreachable: `water.mapped` at v1's #4A90E2 collides with `flow.f5` and with
// nothing a reader will ever see beside it.
//
// So a pair is checked when SOME VIEW can paint both. Chrome — a colour no colour mode
// references, like the paper, the mask or the highlight — is in every view by definition,
// and is still compared against everything, which is what catches "the selected reach
// looks like a restricted one".
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

/** Every token a colour mode can paint, so the rest are chrome and always present. */
const modeTokens = (m) => [
  m?.color, m?.missing, ...Object.values(m?.categories ?? {}),
  ...(m?.stops ?? []).map(([, r]) => r),
].filter((r) => r && typeof r === "object" && r.token).map((r) => r.token);

const srcById = new Map((src.layers ?? []).map((l) => [l.id, l]));
const inSomeMode = new Set((src.layers ?? [])
  .flatMap((l) => Object.values(l.colorModes ?? {}).flatMap(modeTokens)));
/** A generated edge or label appears in EVERY view, so its colour rides along in each. */
const companionTokens = (src.layers ?? []).flatMap((l) => [
  ...(l.outline ? [l.outline.color?.token] : []),
  ...(l.label ? [l.label.color?.token, l.label.halo?.token] : []),
]).filter(Boolean);

const viewSets = (src.views ?? []).map((v) => {
  const seen = new Set(companionTokens);
  for (const [layerId, mode] of Object.entries(v.modes ?? {}))
    for (const tok of modeTokens(srcById.get(layerId)?.colorModes?.[mode])) seen.add(tok);
  return seen;
});
const together = (a, b) => {
  const chromeA = !inSomeMode.has(a), chromeB = !inSomeMode.has(b);
  if (chromeA || chromeB) return true;                 // chrome is on screen in every view
  return viewSets.some((s) => s.has(a) && s.has(b));
};

for (const t of themes) {
  const cols = Object.entries(t.values)
    .filter(([n, v]) => typeof v === "string" && v.startsWith("#") && !isUi(n)
                        && !aliased.has(`${t.name}:${n}`));
  for (let i = 0; i < cols.length; i++) {
    for (let j = i + 1; j < cols.length; j++) {
      const [n1, v1] = cols[i], [n2, v2] = cols[j];
      if (family(n1) === family(n2) && family(n1) === "flow") continue;   // sequential ramp
      if (!together(n1, n2)) continue;                 // never drawn in the same picture
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
// The panel is the app's own card, read from the theme — it was a pair of hexes typed here,
// and the dark one (#181d24) was not the dark card (#15181B) the words actually sit on.
const PANEL_TOKEN = "color.ui.card";
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
  const panel = t.values[PANEL_TOKEN];
  if (typeof panel !== "string") { err(`theme "${t.name}" has no ${PANEL_TOKEN}`); continue; }
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
    if (aliased.has(`${t.name}:${name}`)) continue;     // one colour on purpose — see above
    // The map's tokens and the app's chrome are checked each within their own set: the
    // dark page and the paper under a missing tile are both near-black and both mean "ground".
    const k = (isUi(name) ? "ui:" : "map:") + value.toLowerCase();
    if (byValue.has(k))
      err(`theme "${t.name}": ${byValue.get(k)} and ${name} are both ${value} — two meanings, ` +
          `one colour. A reader cannot tell them apart.`);
    else byValue.set(k, name);
  }
}

// --- a raster source must say where its tiles are ---
for (const [id, s] of Object.entries(src.sources ?? {}))
  if (s.type === "raster" && !s.url && !(Array.isArray(s.tiles) && s.tiles.length))
    err(`source "${id}" is a raster with neither url nor tiles`);

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
/** Edge layers held back by `outline.drawAfter`: the layer they wait for -> the edges, in source order. */
const deferredEdges = new Map();
/**
 * Edge layer id -> the fill whose colour mode it takes (`outline.follow`). Emitted so the
 * RUNTIME can set the edge's mode whenever the fill's is set: a view lists both, but the
 * app sets per-layer modes and names only the fill, which left the edge plain forever.
 */
const follows = {};
/** Generated companion symbol layers: label layer id -> the layer it names. */
const labels = new Map();
/**
 * The symbol layers themselves, held back until every geometry layer has been emitted.
 *
 * TWO REASONS, and the second is the one that bites. Draw order is array order, so a label
 * emitted next to its own geometry is painted UNDER every layer after it — the lake sits
 * eighth and the streams tenth, so lake names would be crossed out by every river drawn
 * over them. And MapLibre decides symbol COLLISION priority by the same order: whichever
 * label layer comes first gets the ground, and the later one is dropped where they overlap.
 * v1 hit exactly this and fixed it the same way (see `fwaLabels` in
 * archive/webapp/src/map/styles.ts) — water names were losing to park labels.
 *
 * So they are appended at the end, ordered by a declared `priority` rather than by where
 * the layer happens to sit in the source. Lower goes first and therefore wins.
 */
const labelLayers = [];
/** Generated flow-arrow symbol layers: arrow layer id -> the line layer it marks. */
const arrows = new Map();
/**
 * WHICH WAY EACH TILE LAYER'S LINES RUN — the pipeline's word for it, never ours.
 *
 * A flow arrow is a glyph laid along the line in the direction the line was DIGITISED, so
 * whether it points downstream depends entirely on the tiles. `pipeline/deliver/tiles/
 * layers.py` declares it per layer (`flows`, measured on the live atlas) and writes it into
 * the tile contract; the glyph's turn is derived from that here, so the style cannot claim a
 * direction the tiles do not carry — and a layer whose lines have no stated direction
 * cannot have arrows at all.
 */
const tileContract = JSON.parse(readFileSync(
  new URL("../../pipeline/deliver/tiles/tile-contract.json", import.meta.url), "utf8"));
/**
 * The glyph, by the contract's `flows`. A line-placed ▶ points along the line as digitised,
 * so a line running mouth to source takes ◀. CHOSEN, NOT TURNED: `text-rotate: 180` on a ▶
 * turns it about a point off the glyph's centre, and every arrow sat a glyph-height beside
 * its river instead of on it (seen on the Vedder at z12, 2026-10-21).
 */
const ARROW_GLYPH = { mouth_to_source: "◀", source_to_mouth: "▶" };
/**
 * THE ONE STACK THAT HAS THE GLYPH. Protomaps' "Noto Sans Medium" ships a 9472-9727 range
 * with ONE glyph in it and no ▶ or ▲ — a symbol whose glyph is missing is silently not
 * placed, so an arrow in Medium draws nothing and says nothing (measured 2026-10-21: 0
 * rendered in Medium, 20+ in Regular on the same view). "Noto Sans Regular" carries the
 * geometric shapes and is packed with every release (`tiles/assets.py` BASEMAP_FONTS). Not
 * a source option: a font is part of the glyph choice, not a look to vary.
 */
const ARROW_FONT = ["Noto Sans Regular"];

/**
 * `field` with a trailing " [<id>]" cut off — see `trimId` on a label. `index-of` finds the FIRST " [", and every name
 * that carries an id carries exactly one bracket, at the end; a name that merely contains a bracket elsewhere and does
 * not end in "]" is drawn whole.
 */
const trimmedName = (field) => {
  const name = ["get", field];
  const at = ["index-of", " [", name];
  const n = ["length", name];
  return ["case",
    ["all", [">", at, 0], ["==", ["slice", name, ["-", n, 1]], "]"]],
    ["slice", name, 0, at],
    name];
};

for (const l of src.layers ?? []) {
  const where = `layer "${l.id}"`;
  if (!groups.has(l.group)) err(`${where}: unknown group "${l.group}"`);
  if (!src.sources?.[l.source]) err(`${where}: unknown source "${l.source}"`);
  if (l.highlightable && !l.featureIdProperty)
    err(`${where}: highlightable layers need featureIdProperty — feature-state has nothing to key on`);
  /*
   * A RASTER IS A PICTURE, NOT A GEOMETRY. Satellite imagery has no feature to colour, no
   * id to highlight and no source-layer: it is the ground itself. So it is exempt from the
   * colour-mode rule — and held to its own instead: it must draw an EXTERNAL raster source
   * (whose attribution the check below already requires), and it may carry nothing that
   * only a vector layer can mean.
   */
  if (l.geometry === "raster") {
    const s = src.sources?.[l.source];
    if (s && s.type !== "raster") err(`${where}: a raster layer must draw a raster source`);
    if (s && !s.external) err(`${where}: a raster source is imagery we did not make — mark it external`);
    for (const k of ["colorModes", "sourceLayer", "featureIdProperty", "highlightable",
                     "label", "outline", "width", "opacity"])
      if (l[k] !== undefined && l[k] !== false)
        err(`${where}: "${k}" means nothing on a raster layer`);
    layersById.set(l.id, l);
    outLayers.push({ id: l.id, type: "raster", source: l.source,
                     layout: { visibility: groups.get(l.group)?.defaultVisible ? "visible" : "none" } });
    continue;
  }
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
    } else if (m.scale === "categorical" || m.scale === "profile") {
      /*
       * A PROFILE MODE is categorical one step removed (colour-latency AP): the feature-state holds a profile LABEL and
       * the day decides which labels are which category, so the runtime builds the match (`colorExpression` with the
       * day's `dayProfiles`). The rules are a categorical's, tightened: it must be over an enum, colour every member,
       * AND define `missing` — a label the day does not place (none pushed yet, tidal, outside) must paint as no answer.
       */
      if (m.scale === "profile") {
        if (!m.enum) err(`${w}: a profile mode must be declared over an enum — the day's label sets are its members`);
        if (!m.missing) err(`${w}: a profile mode must define "missing" — a label the day does not place is no answer`);
      }
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
  if (l.opacity) {
    tokenValue({ token: l.opacity.token }, `${where} opacity`);
    /*
     * AN OPACITY THAT MOVES WITH ZOOM: `ramp` is [[zoom, factor]], the token's value times each factor, interpolated.
     * The closure hatch is a dense weave of a few pixels zoomed out and a texture you read through close in; one
     * opacity is wrong at one end or the other.
     */
    const r = l.opacity.ramp;
    if (r !== undefined) {
      if (!Array.isArray(r) || r.length < 2 || r.some((s) => !Array.isArray(s) || s.length !== 2
          || typeof s[0] !== "number" || typeof s[1] !== "number" || s[1] < 0 || s[1] > 1))
        err(`${where} opacity ramp: needs >= 2 [zoom, factor] stops, factors in 0..1`);
      else if (r.some(([z], i) => i > 0 && z <= r[i - 1][0]))
        err(`${where} opacity ramp: zooms must ascend`);
    }
  }
  if (l.pattern) tokenValue({ token: l.pattern.token }, `${where} pattern colour`);
  if (l.dash) tokenValue(l.dash, `${where} dash`);
  if (l.width) {
    tokenValue(l.width, `${where} width`);
    // A fill has no width in MapLibre. Declaring one produced a style that threw on load
    // and took the entire recolouring pass down with it, so NOTHING on the map got its
    // colours — from one line of source that looked perfectly reasonable.
    if (l.geometry === "polygon")
      err(`${where}: a polygon has no width. MapLibre fills cannot be stroked; an outline ` +
           `needs a companion line layer over the same source-layer.`);
    if (l.width.ramp && !l.width.by && l.width.mode !== "zoom")
      err(`${where}: a width ramp with no "by" must declare mode "zoom" — otherwise it is ` +
          `a ramp over an attribute that was never named, which reads as null everywhere.`);
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
  /*
   * A FILTER, when several style layers draw ONE source-layer.
   *
   * Land ownership ships as a single tile layer with an `owner` attribute, because it is
   * one dissolved fabric; but private land and Crown land are different pieces of advice
   * and a reader wants to see one without the other. Three style layers over one source
   * layer, each filtered, is how MapLibre expresses that — and it is the only way, since a
   * colour mode reads FEATURE-STATE and this value is in the tile.
   */
  /*
   * AN ICON POINT (T173, the fish obstacles): a point drawn as a picture chosen by a tile property, not a circle. The
   * images are generated per theme (`@app/map` images.ts, like the hatches), so the style only NAMES them; overlap is
   * off and the sort key ranks the property's values, so a crowded reach drops the lesser mark instead of stacking them.
   * The symbol sits where its circle would (above the lines, under the names), and a layer with no `icon` is unchanged.
   */
  if (l.icon) {
    const ic = l.icon;
    const iw = `${where} icon`;
    if (l.geometry !== "point") err(`${iw}: only a point can be an icon`);
    if (!ic.by || !ic.images || !ic.fallback) err(`${iw}: needs "by" (the tile property), "images" (value -> image) and "fallback"`);
    const values = Object.keys(ic.images ?? {});
    for (const v of ic.sortOrder ?? []) if (!values.includes(v)) err(`${iw}: sortOrder names "${v}", which has no image`);
    const size = ["interpolate", ["linear"], ["zoom"], ...(ic.size ?? []).flat()];
    outLayers.push({ id: l.id, type: "symbol", ...geom,
      ...(l.filter ? { filter: l.filter } : {}),
      layout: {
        ...geom.layout,
        "icon-image": ["match", ["get", ic.by], ...values.flatMap((v) => [v, ic.images[v]]), ic.fallback],
        "icon-size": size,
        "icon-allow-overlap": false,
        "icon-ignore-placement": false,
        "icon-padding": ic.padding ?? 2,
        "symbol-sort-key": ["match", ["get", ic.by],
          ...(ic.sortOrder ?? []).flatMap((v, i) => [v, i]), (ic.sortOrder ?? []).length],
      } });
  } else
  outLayers.push({ id: l.id, type: MAPLIBRE_TYPE[l.geometry], ...geom,
                   ...(l.filter ? { filter: l.filter } : {}) });

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
    if (l.outline.width?.ramp && !l.outline.width.by && l.outline.width.mode !== "zoom")
      err(`${where}: an outline width ramp with no "by" must declare mode "zoom"`);
    if (l.outline.dash) tokenValue(l.outline.dash, `${where} outline dash`);
    if (l.outline.opacity) tokenValue(l.outline.opacity, `${where} outline opacity`);
    /*
     * The edge inherits the fill's filter. Without it the outline is a different SHAPE from
     * the thing it outlines.
     *
     * AND IT MAY START LATER THAN ITS FILL. v1 drew every admin border from z11 and nothing
     * below, which is the right instinct: zoomed out, a province full of outlined polygons
     * is a mesh of competing lines and none of them is the answer to anything. The FILL is
     * what says "an area is here" at a distance — for a closure, a hatch — and the border
     * only earns its place once you are close enough to care exactly where it runs.
     * `max` of the two, so an edge can never draw before the fill it belongs to.
     */
    const edgeMinzoom = l.outline.minzoom !== undefined
      ? Math.max(l.outline.minzoom, l.minzoom ?? 0) : l.minzoom;
    const edge = { id, type: "line", ...geom,
                   ...(edgeMinzoom !== undefined ? { minzoom: edgeMinzoom } : {}),
                   ...(l.filter ? { filter: l.filter } : {}) };
    /*
     * `under: true` DRAWS THE EDGE BENEATH ITS FILL — a seam, not a border. Over an opaque fill it shows only where no
     * feature is drawn, which is how the Conditions field closes the slivers between basins that were simplified one at
     * a time, without moving any basin's edge (`basin` in layers.source.json).
     */
    /*
     * `drawAfter: "<layer>"` HOLDS THE EDGE BACK until that later layer (and its own edge) is drawn — the one way an edge
     * leaves its fill. A lake's shoreline carries the lake's status colour, so it is a WATER LINE, while its fill is a
     * fill: the region and unit boundaries go between the two (T83), over the fill so they stay traceable across a
     * lake, under the shoreline so the water's colour wins where they cross.
     */
    if (l.outline.drawAfter !== undefined) {
      if (l.outline.under === true) err(`${where}: an outline cannot be both "under" its fill and "drawAfter" a later layer`);
      const target = (src.layers ?? []).findIndex((x) => x.id === l.outline.drawAfter);
      if (target < 0) err(`${where}: outline.drawAfter names "${l.outline.drawAfter}", which is not a layer`);
      else if (target <= (src.layers ?? []).indexOf(l))
        err(`${where}: outline.drawAfter "${l.outline.drawAfter}" must come later in the source than the fill it outlines`);
      else (deferredEdges.get(l.outline.drawAfter) ?? deferredEdges.set(l.outline.drawAfter, []).get(l.outline.drawAfter)).push(edge);
    } else if (l.outline.under === true) outLayers.splice(outLayers.length - 1, 0, edge);
    else outLayers.push(edge);
    edges.set(id, l);
  }
  // The edges held back for this layer (`outline.drawAfter`) go directly over it and its own edge.
  for (const e of deferredEdges.get(l.id) ?? []) outLayers.push(e);

  /**
   * A COMPANION SYMBOL LAYER over the same source-layer — the water's own name.
   *
   * v1 drew these (`archive/webapp/src/map/styles.ts`) and losing them is most of why the
   * new map read as a diagram: a river map whose rivers have no names is a picture of
   * drainage, not somewhere you can find the Vedder. The names are already in the tile —
   * `stream.name` and `lake.name` were being shipped and nothing was drawing them.
   *
   * Generated rather than authored for the same reason the edge is: it must not drift
   * from the geometry it names. Same source-layer, same group (so one toggle moves both),
   * and everything a MapLibre symbol needs is derived here so no adapter has to invent it.
   *
   * LAYOUT IS BAKED, PAINT IS NOT. Placement, font, size and the zoom ladder never change
   * with the theme, so they belong in style.json where they cost nothing to apply. Colour
   * and halo are themeable and follow the same runtime path as every other paint value.
   */
  if (l.label) {
    const lab = l.label;
    const id = `${l.id}__label`;
    const lw = `${where} label`;
    tokenValue(lab.color, `${lw} colour`);
    tokenValue(lab.halo, `${lw} halo`);
    if (!lab.field) err(`${lw}: needs "field" — the tile property holding the name`);
    if (!["line", "point"].includes(lab.placement))
      err(`${lw}: placement must be "line" or "point"`);
    if (lab.sortBy && lab.placement !== "point")
      err(`${lw}: sortBy orders collision priority between POINT labels; a line label is ` +
          `placed along its own geometry and has nothing to sort against`);
    if (lab.showAbove && !lab.sortBy)
      err(`${lw}: showAbove filters on a magnitude, so it must be the same property ` +
          `sortBy ranks — otherwise the layer shows one set and prioritises another`);

    const size = ["interpolate", ["linear"], ["zoom"], ...(lab.size ?? []).flat()];
    /*
     * THE ZOOM LADDER, as a filter rather than a minzoom.
     *
     * `showAbove` says "at this zoom, only things bigger than this". Expressed as
     * `any(zoom >= showAll, ...steps)` so the big water appears early and everything
     * arrives together at `showAll`. Without the ladder a z8 tile of the Interior draws
     * every pothole's name at once and MapLibre drops them by collision, which means WHICH
     * lakes get named is decided by geometry order — a different set every pan.
     */
    const ladder = lab.showAbove
      ? ["any",
         [">=", ["zoom"], lab.showAll ?? (lab.minzoom ?? 0)],
         ...lab.showAbove.map(([z, min]) =>
           ["all", [">=", ["zoom"], z], [">=", ["get", lab.sortBy], min]])]
      : null;
    /*
     * AND IT INHERITS THE LAYER'S OWN FILTER. `park_closed` and `park` draw the same tile
     * layer split by `kind`; without this both label layers name EVERY park, so a national
     * park gets a crimson label and a green one on top of each other, and a provincial park
     * gets labelled as a closure. A label that does not name the same features as the shape
     * it belongs to is worse than no label.
     */
    const filter = ["all", ["has", lab.field], ["!=", ["get", lab.field], ""],
                    ...(l.filter ? [l.filter] : []),
                    ...(ladder ? [ladder] : [])];

    labelLayers.push({
      $priority: lab.priority ?? 0,
      id, type: "symbol", ...geom,
      ...(lab.minzoom !== undefined ? { minzoom: lab.minzoom } : {}),
      // A label may STOP as well as start. The region number is drawn while the region is
      // the thing on screen and hands over to the unit numbers at z7; without a maxzoom it
      // would keep drawing "3" across a valley the reader is looking at unit 3-17 in.
      ...(lab.maxzoom !== undefined ? { maxzoom: lab.maxzoom } : {}),
      filter,
      layout: {
        ...geom.layout,
        "symbol-placement": lab.placement,
        /*
         * `trimId`: THE NAME WITHOUT ITS SOURCE ID. The access and protected-area names carry the source's record id
         * in brackets ("Codd Wetland Ecological Conservancy Area [14448346]") — a key for the pipeline, noise on a
         * map (owner 2026-10-22: "makes names and labels universally well labelled"). Everything from the last " ["
         * on is dropped when the name ends in "]"; a name with no bracket is drawn as it is. In the style rather than
         * the tiles so it needs no rebuild; the pipeline can stop writing the id into `name` and this stays harmless.
         */
        "text-field": lab.trimId ? trimmedName(lab.field) : ["get", lab.field],
        "text-font": lab.font ?? ["Noto Sans Regular"],
        "text-size": size,
        ...(lab.letterSpacing !== undefined ? { "text-letter-spacing": lab.letterSpacing } : {}),
        ...(lab.maxWidth !== undefined ? { "text-max-width": lab.maxWidth } : {}),
        ...(lab.placement === "line"
          ? { "text-max-angle": lab.maxAngle ?? 25, "symbol-spacing": lab.spacing ?? 300 }
          : {}),
        // Negated: MapLibre places the LOWEST sort key first, and first placed wins the
        // collision. Biggest-first is the only ordering that is stable as you pan.
        ...(lab.sortBy ? { "symbol-sort-key": ["-", 0, ["get", lab.sortBy]] } : {}),
        "text-allow-overlap": false,
        "text-padding": lab.placement === "line" ? 6 : 3,
      },
    });
    labels.set(id, l);
  }

  /**
   * FLOW ARROWS — a companion symbol layer that says which way the water goes (owner
   * 2026-10-21, the Conditions map).
   *
   * A GLYPH, NOT AN IMAGE. The hatches and the gauge pill are pixels added with `addImage`,
   * which MapLibre Native has no API for (N2-F1: they do not draw on a phone). A glyph comes
   * from the font the labels already use, shipped with every release (`tiles/assets.py`
   * packs all 256 ranges), so the web and the phone draw the same arrow from one layer.
   *
   * DOWNSTREAM BY CONSTRUCTION. A line-placed glyph lies along the line as digitised (▶ points
   * from the first vertex to the last), so the glyph is CHOSEN from the tile contract
   * (`ARROW_GLYPH`), and `text-keep-upright: false` stops MapLibre flipping a
   * glyph on a line that runs right-to-left on screen — which would make every west-flowing
   * river's arrows lie.
   *
   * THE STREAM'S OWN COLOUR (`follow`): the arrow takes the line's colour mode, so it is
   * one expression over one feature-state and cannot disagree with the water it sits on.
   * The paper halo cuts it out of the line so the shape reads at a 1.4 px width.
   *
   * HIDDEN UNTIL A VIEW ASKS (`views[].show`). The arrows answer a Conditions question —
   * where does this water go, which gauge is downstream — and on the Map tab, the mini map
   * and the spot picker they would be noise over the regulations. Visibility "none" in the
   * style, so a map that never asks never draws them.
   */
  if (l.arrows) {
    const a = l.arrows;
    const id = `${l.id}__arrows`;
    const aw = `${where} arrows`;
    if (l.geometry !== "line") err(`${aw}: only a line has a direction to point along`);
    const flows = tileContract.layers?.[l.sourceLayer]?.flows;
    if (!(flows in ARROW_GLYPH))
      err(`${aw}: the tile contract gives "${l.sourceLayer}" no direction (flows: ${JSON.stringify(flows)}) — ` +
          `an arrow on a line of unknown direction is a guess drawn as a fact`);
    if (a.follow !== true) err(`${aw}: must follow its line's colour ("follow": true)`);
    tokenValue(a.halo, `${aw} halo`);
    const ladder = a.showAbove
      ? ["any", [">=", ["zoom"], a.showAll ?? (a.minzoom ?? 0)],
         ...a.showAbove.map(([z, min]) => ["all", [">=", ["zoom"], z], [">=", ["get", a.by], min]])]
      : null;
    if (a.showAbove && !a.by) err(`${aw}: showAbove needs "by", the tile attribute it ranks on`);
    labelLayers.push({
      // AFTER every name: a name placed first keeps its spot, and an arrow that would sit on
      // it is dropped (`text-ignore-placement` keeps the arrow from blocking anything).
      $priority: 1000,
      id, type: "symbol", ...geom,
      ...(a.minzoom !== undefined ? { minzoom: a.minzoom } : {}),
      ...(l.filter || ladder ? { filter: ["all", ...(l.filter ? [l.filter] : []), ...(ladder ? [ladder] : [])] } : {}),
      layout: {
        visibility: "none",
        "symbol-placement": "line",
        "symbol-spacing": a.spacing ?? 150,
        "text-field": ARROW_GLYPH[flows] ?? "▶",
        "text-font": ARROW_FONT,
        "text-size": ["interpolate", ["linear"], ["zoom"], ...(a.size ?? []).flat()],
        "text-keep-upright": false,
        "text-rotation-alignment": "map",
        "text-pitch-alignment": "map",
        "text-max-angle": a.maxAngle ?? 30,
        "text-allow-overlap": false,
        "text-ignore-placement": true,
        "text-padding": a.padding ?? 2,
      },
    });
    arrows.set(id, l);
  }
}


// --- a colour token may not cross semantic families ---
//
// Tokens are namespaced by MEANING: `status.*` is access and closure (static area layers),
// `flow.*` is hydrological, `water.*` is neutral ("no data expressed") and may be used
// anywhere. A mode reading a feed like `gauges` may use flow.*.
//
// This exists because the discharge mode once declared `missing: color.status.unknown`.
// About 19,250 of 19,700 waters have no gauge, so nearly the whole Conditions view rendered
// in the same violet that meant "we could not parse this regulation" in the (since removed)
// Regulations view — two unrelated unknowns, one colour. The ΔE rule cannot catch it: it is
// the SAME token, not two similar ones.
//
// THE GAUGES FEED HAS TWO SCALES (T52): `flow.*`, a percentile for the date, and `change.*`, the expected change over
// the next ten days — both hydrological readings of the same feed, in disjoint ranges of the one `standing` channel
// (0–100, 200–300), so a horizon switch is a relabel and never a second colour mode. Still never a status colour.
const FAMILY_FOR_PROVIDER = { gauges: ["flow", "change"] };
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
      if (allowed && !allowed.includes(fam))
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
  /*
   * `follow: true` MAKES THE OUTLINE ANSWER THE SAME QUESTION AS ITS FILL.
   *
   * Without it an edge has one static colour in every view — which is right for a park
   * boundary (the boundary is where the park is, whatever the water is doing) and wrong
   * for a lake in the Conditions view: the lake was filled by its flow percentile and
   * still ringed in plain blue, so a lake reading "much below normal" was drawn with a
   * normal-looking edge around it. Following means the edge inherits the fill's colour
   * modes verbatim and each view maps it to the SAME mode, so the two are painted from one
   * expression over one feature-state and cannot disagree.
   */
  const follow = l.outline.follow === true;
  const own = { plain: { label: `${l.id} outline`, scale: "static", color: l.outline.color } };
  /*
   * AND WHERE A FOLLOWED MODE FALLS BACK TO THE FILL'S PLAIN COLOUR, THE EDGE FALLS BACK
   * TO ITS OWN. Stocked paints a never-stocked lake (and one with no record) in
   * `color.lake.fill`, the plain water — that lake has no colour in this mode, and an
   * edge inheriting the fill's plain blue disappears into the fill exactly as it would in
   * plain. So the reference is swapped for the outline's colour: a lake the mode says
   * nothing about keeps today's neutral shoreline (owner 2026-10-20).
   */
  const fillPlain = l.colorModes?.plain?.scale === "static" ? l.colorModes.plain.color?.token : undefined;
  const swap = (r) => (r && typeof r === "object" && r.token === fillPlain ? l.outline.color : r);
  const followed = (m) => ({
    ...m,
    ...(m.color ? { color: swap(m.color) } : {}),
    ...(m.missing ? { missing: swap(m.missing) } : {}),
    ...(m.categories ? { categories: Object.fromEntries(
      Object.entries(m.categories).map(([k, r]) => [k, swap(r)])) } : {}),
    ...(m.stops ? { stops: m.stops.map(([at, r]) => [at, swap(r)]) } : {}),
  });
  layersById.set(id, {
    ...l, id, geometry: "line",
    // PLAIN STAYS THE AUTHORED COLOUR even when following, and that is not an exception
    // for its own sake: a lake's plain FILL is the water colour, so inheriting it would
    // paint the outline the same as the thing it outlines and the border would vanish.
    // The authored `outline.color` is the stream blue v1 used, which is the whole reason a
    // lake and the river through it read as one water. Only the data-driven modes follow.
    colorModes: follow
      ? { ...Object.fromEntries(Object.entries(l.colorModes ?? {}).map(([k, m]) => [k, followed(m)])),
          ...own }
      : own,
  });
  if (follow) follows[id] = l.id;
  for (const v of src.views ?? [])
    v.modes[id] = follow ? (v.modes[l.id] ?? "plain") : "plain";
}
/**
 * A name is a NAME IN EVERY VIEW. It does not change with the colouring, because it is
 * not answering the question the colouring answers — the Vedder is the Vedder whether it
 * is drawn open, closed or by percentile. One static mode, present in every view.
 */
for (const [id, l] of labels) {
  layersById.set(id, {
    ...l, id, geometry: "symbol",
    colorModes: { plain: { label: `${l.id} label`, scale: "static", color: l.label.color } },
  });
  for (const v of src.views ?? []) v.modes[id] = "plain";
}
/**
 * AN ARROW IS ITS LINE'S COLOUR — every mode of the line, verbatim, and each view maps it to
 * the line's own mode. Registered as a follower, so setting the stream's mode paints the
 * arrows in the same call (`setLayerMode`), exactly as a lake's outline follows its fill.
 */
for (const [id, l] of arrows) {
  layersById.set(id, { ...l, id, geometry: "symbol", colorModes: l.colorModes ?? {} });
  follows[id] = l.id;
  for (const v of src.views ?? []) v.modes[id] = v.modes[l.id] ?? "plain";
}

/*
 * A VIEW'S OWN LAYERS (`show`) — drawn by that view and by no other, the mirror of `hide`.
 *
 * Every layer named here is emitted hidden; the app turns it on by naming the view
 * (`shownLayers`), so a screen that asks for nothing draws nothing. The listed layer must
 * exist and must actually start hidden, or "only this view" is a claim the style breaks on
 * every other screen. And a layer that only ever draws when a view shows it must be shown
 * by SOME view, or it is a layer nothing can ever see.
 */
const outById = new Map(outLayers.concat(labelLayers).map((x) => [x.id, x]));
const shownAnywhere = new Set();
for (const v of src.views ?? [])
  for (const id of v.show ?? []) {
    shownAnywhere.add(id);
    const out = outById.get(id);
    if (!out) err(`view "${v.id}": shows unknown layer "${id}"`);
    else if (out.layout?.visibility !== "none")
      err(`view "${v.id}": shows "${id}", which the style already draws everywhere — a view's own layer must start hidden`);
  }
for (const id of arrows.keys())
  if (!shownAnywhere.has(id)) err(`layer "${id}": no view shows it, so it can never draw`);

/*
 * A HIDDEN LAYER TAKES ITS COMPANIONS WITH IT. Conditions hides `region` and `mu` — and drew their generated edges and
 * numbers anyway, because `hide` named the fills and an edge is a layer of its own: a grey boundary and a "3" across a
 * flow field the view had said it does not draw. A view hides a layer as a THING, so its edge, its name and its arrows
 * are hidden with it; written out here so the generated list is the whole truth and no adapter has to know the suffixes.
 */
for (const v of src.views ?? [])
  if (v.hide)
    v.hide = [...new Set(v.hide.flatMap((id) => [id, ...["__edge", "__label", "__arrows"]
      .map((s) => `${id}${s}`).filter((c) => outById.has(c))]))];

/*
 * THE FIELD (`field` on one polygon layer — the Conditions basins): the colour modes in which it is drawn at all, and the
 * layers it COVERS — each of which takes the field's maxzoom as its floor while the field is drawn (adapters/contract.ts
 * `zoomFor`). Emitted as {layer, handover, drawnIn, covers: {id: the floor it returns to}} so the adapter needs nothing
 * else: a generated layer's own minzoom, or for a layer the runtime adds (`covers.runtime`) the floor given here.
 */
let field = null;
for (const l of src.layers ?? []) {
  if (!l.field) continue;
  const where = `layer "${l.id}" field`;
  if (field) err(`${where}: a second field — the map has one`);
  if (l.geometry !== "polygon") err(`${where}: a field is an area`);
  if (typeof l.maxzoom !== "number") err(`${where}: needs a maxzoom — the zoom it hands over at`);
  const drawnIn = l.field.drawnIn ?? [];
  if (!drawnIn.length) err(`${where}: drawnIn names no mode, so it never draws`);
  for (const m of drawnIn) if (!l.colorModes?.[m]) err(`${where}: drawnIn names "${m}", which is not one of its modes`);
  const covers = {};
  for (const id of l.field.covers?.layers ?? []) {
    const out = outById.get(id);
    if (!out) err(`${where}: covers unknown layer "${id}"`);
    else covers[id] = out.minzoom ?? 0;
  }
  for (const [id, z] of Object.entries(l.field.covers?.runtime ?? {})) {
    if (outById.has(id)) err(`${where}: "${id}" is a style layer — list it under covers.layers`);
    if (typeof z !== "number") err(`${where}: runtime layer "${id}" needs the floor it returns to`);
    covers[id] = z;
  }
  /*
   * `solid`: the field is painted OPAQUE, each of its colours (and its seam's) pre-mixed on `solid.over` at the layer's
   * own opacity token — the colour the translucent field had on blank paper, with nothing showing through it.
   */
  let solid = null;
  if (l.field.solid) {
    tokenValue(l.field.solid.over, `${where} solid.over`);
    if (!l.opacity?.token) err(`${where}: solid needs the layer's opacity token — the strength it is mixed at`);
    solid = { over: l.field.solid.over.token, opacity: l.opacity?.token };
  }
  field = { layer: l.id, handover: l.maxzoom, drawnIn, covers, ...(solid ? { solid } : {}) };
  // ONE HANDOVER (T130): a covered layer takes its floor from the field alone. A per-mode floor on it is the handover
  // typed a second time — move one and the other opens a band drawing neither, or two answers at once.
  for (const id of Object.keys(covers))
    if (layersById.get(id)?.minzoomByMode)
      err(`${where}: "${id}" is covered by the field and also declares minzoomByMode — the field's maxzoom is its floor`);
}

/*
 * LAYERS THE RUNTIME ADDS, which a view may still switch.
 *
 * The gauge dots are not in this file and cannot be: they are drawn from a live GeoJSON
 * feed, not from the atlas, so they have no source-layer and no tile contract. But a view
 * still has to be able to say what they are ABOUT — flow, or water temperature — and that
 * belongs beside every other thing a view decides rather than in a second mechanism.
 *
 * Listed explicitly so a typo in a view is still an error. `runtime-style.ts` reads these
 * out of the same `modes` map it reads everything else from.
 */
const RUNTIME_LAYERS = new Set(src.$runtimeLayers ?? []);
for (const v of src.views ?? [])
  for (const [layerId, mode] of Object.entries(v.modes ?? {})) {
    if (RUNTIME_LAYERS.has(layerId)) continue;
    const l = layersById.get(layerId);
    if (!l) err(`view "${v.id}": unknown layer "${layerId}"`);
    else if (!l.colorModes[mode]) err(`view "${v.id}": layer "${layerId}" has no mode "${mode}"`);
  }

if (errs.length) {
  console.error("✗ map style source is invalid:\n  " + errs.join("\n  "));
  process.exit(1);
}

// Labels last, and among themselves in priority order. See `labelLayers`.
labelLayers.sort((a, b) => a.$priority - b.$priority);
for (const l of labelLayers) { const { $priority, ...rest } = l; outLayers.push(rest); }

const style = {
  $comment: "GENERATED by tools/build-style.mjs. Do not hand-edit.",
  version: 8, name: "canifishthis", sources: src.sources ?? {}, layers: outLayers,
};

const meta = {
  $comment: "GENERATED. The runtime reads this to build the layer/view UI and apply themes.",
  groups: src.groups ?? [],
  views: src.views ?? [],
  runtimeLayers: [...(src.$runtimeLayers ?? [])],
  providers,
  layerGroup: Object.fromEntries([
    ...(src.layers ?? []).map((l) => [l.id, l.group]),
    // an edge belongs to the group of the fill it outlines, so one toggle moves both
    ...[...edges].map(([id, l]) => [id, l.group]),
    // and a name belongs to the water it names, for exactly the same reason
    ...[...labels].map(([id, l]) => [id, l.group]),
    // and an arrow to the line it marks
    ...[...arrows].map(([id, l]) => [id, l.group]),
  ]),
  follows,
  highlightable: (src.layers ?? []).filter((l) => l.highlightable)
    .map((l) => ({ id: l.id, featureIdProperty: l.featureIdProperty })),
  colorModes: Object.fromEntries([
    ...(src.layers ?? []).map((l) => [l.id, l.colorModes]),
    ...[...edges].map(([id]) => [id, layersById.get(id).colorModes]),
    ...[...labels].map(([id]) => [id, layersById.get(id).colorModes]),
    ...[...arrows].map(([id]) => [id, layersById.get(id).colorModes]),
  ]),
  // WHICH PROPERTY carries the feature id, per layer. The style knew a property had been
  // named; nothing knew which one, so nothing could tell MapLibre to promote it.
  featureIds: Object.fromEntries((src.layers ?? [])
    .filter((l) => l.featureIdProperty)
    .map((l) => [l.id, l.featureIdProperty])),
  widths: Object.fromEntries([
    ...(src.layers ?? []).filter((l) => l.width)
      .map((l) => [l.id, (l.width.by || l.width.mode === "zoom")
        ? { token: l.width.token, ...(l.width.by ? { by: l.width.by } : {}),
            mode: l.width.mode ?? "linear", ramp: l.width.ramp }
        : l.width.token]),
    ...[...edges].filter(([, l]) => l.outline.width)
      .map(([id, l]) => [id, (l.outline.width.by || l.outline.width.mode === "zoom")
        ? { token: l.outline.width.token, ...(l.outline.width.by ? { by: l.outline.width.by } : {}),
            mode: l.outline.width.mode ?? "linear", ramp: l.outline.width.ramp }
        : l.outline.width.token]),
  ]),
  /** layer id -> [[zoom, factor]]: its opacity token times a zoom curve (`opacity.ramp`). Absent: the token, flat. */
  opacityRamps: Object.fromEntries((src.layers ?? []).filter((l) => l.opacity?.ramp)
    .map((l) => [l.id, l.opacity.ramp])),
  // Opacity rides the same rail as width: a token, applied at runtime, themeable. It is
  // NOT baked into the style's paint because a theme may want a different value — and
  // because wetland at full opacity buried every other layer on the map, which is the
  // failure that made this necessary.
  opacities: Object.fromEntries((src.layers ?? []).filter((l) => l.opacity)
    .map((l) => [l.id, l.opacity.token])),
  // A dash pattern. The under-lake layer's comment said "drawn dotted" for months while
  // the style had no way to express a dash, so it drew solid — and 303,932 solid routes
  // over every lake in the province is a spider web.
  dashes: Object.fromEntries([
    ...(src.layers ?? []).filter((l) => l.dash).map((l) => [l.id, l.dash.token]),
    // AND THE GENERATED EDGES. A dash declared on an outline is a dash on a companion LINE
    // layer, which is the only kind that can carry one — the fill it belongs to is a fill.
    // Declared and then dropped, the management-unit boundary drew solid and read as a
    // river, which is the exact failure the under-lake route already had once.
    ...[...edges].filter(([, l]) => l.outline.dash)
      .map(([id, l]) => [id, l.outline.dash.token]),
  ]),
  // A BORDER THAT IS NOT A HARD EDGE. v1 drew every admin boundary at 0.35, and at full
  // strength ours cut across the water it surrounds — a crisp line reads as a feature of
  // the ground rather than as an annotation on it.
  /*
   * PER-MODE ZOOM RANGES. A layer's `minzoom` is baked into the style; this is the zoom it
   * takes while it is being coloured a particular way.
   *
   * KEYED ON THE MODE AND NOT THE VIEW, because that is how the app actually switches: it
   * renders one view and sets each layer's colour mode, so a rule keyed on "the conditions
   * view" is a rule that never fires. The mode IS the question being asked — `standing` is
   * "what is the water doing" wherever it appears.
   *
   * The rivers' stand-down under the basin field is NOT one of these (T130): it is
   * `field.covers`, floored at the field's own maxzoom — the one handover number, which the
   * app reads as `FIELD_HANDOVER_Z` and the tile builder's `BASIN_HANDOVER_Z` is held to.
   * The `standing: 10` each river layer carried here was that number typed four more times.
   */
  /** The field and what it covers — see `field` above. null: the style has none. */
  field,
  minzoomByMode: Object.fromEntries(
    (src.layers ?? []).filter((l) => l.minzoomByMode)
      .map((l) => [l.id, l.minzoomByMode])),
  edgeOpacities: Object.fromEntries([...edges].filter(([, l]) => l.outline.opacity)
    .map(([id, l]) => [id, l.outline.opacity.token])),
  // A label's HALO — the only paint property no other layer type has. Text colour rides
  // the ordinary colorModes rail; the halo needs its own entry or a name is legible on
  // open ground and invisible the moment it crosses a road.
  labelHalos: Object.fromEntries([
    ...[...labels].map(([id, l]) => [id, { color: l.label.halo.token, width: l.label.haloWidth ?? 1.2 }]),
    // An arrow wears the same paper ring, which is what cuts it out of the line it sits on.
    ...[...arrows].map(([id, l]) => [id, { color: l.arrows.halo.token, width: l.arrows.haloWidth ?? 1.2 }]),
  ]),
  /*
   * A FILL PATTERN, by layer. The token names the COLOUR the pattern is woven from, not
   * an image: the image is generated at runtime from the resolved theme (src/hatch.ts),
   * the same way the gauge pill is, because a sprite sheet belongs to the basemap.
   *
   * It applies to STATIC colourings only, and that rule lives in paintFor. A pattern
   * overrides fill-color outright in MapLibre, so a patterned closure view would draw
   * "open" and "closed" as the same weave.
   */
  patterns: Object.fromEntries((src.layers ?? []).filter((l) => l.pattern)
    .map((l) => [l.id, { token: l.pattern.token,
                         ground: l.pattern.ground ?? 0.45,
                         stripe: l.pattern.stripe ?? 0.70,
                         darken: l.pattern.darken ?? 0.35,
                         spacing: l.pattern.spacing ?? 4,
                         weight: l.pattern.weight ?? 1.2,
                         cross: l.pattern.cross === true }])),
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
