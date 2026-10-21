// Colour-latency harness page. One colouring strategy per load (?mode=), the app's real style, the live atlas and
// release paint. Drives a fixed pan script and records, per pan: frames drawn with a wrongly coloured visible section,
// time from pan end to every visible section coloured, time to idle, colouring cost, long tasks, heap.
//
// modes:  none   no status at all (draw-time reference)
//         idle   TODAY: on 'idle' -> queryRenderedFeatures -> (next task, standing in for the React round trip)
//                -> setFeatureState for every visible id on stream+lake+wetland
//         A      per tile, synchronously in the tile's 'data' event: its ids -> setFeatureState (own layer only)
//         Ds     set-all SPARSE once per day: state only for non-base sections, the style's no-state fallback = base
//         Dfull  set-all FULL on the stream source-layer only (cost reference; lakes stay grey)
//         Bm     status in the style: ["match", ["id"], [own…], own, [closed…], closed, [none…], grey, base]
//         Bs     status in the style: ["step", ["id"], …] over the day's merged handle runs
//         E0/E1/E2  region around the user: state for every section in the viewport grown by 0/1/2 viewports each
//                side (z11 cell index), refreshed on every 'move', far cells dropped
//         E1A    E1 plus A as a safety net
//         AP / E1P / E1AP   the same, but the state is the section's PROFILE (index into the paint's 115 day-rows),
//                set once per section per session; the day lives in the STYLE as a ~115-label match on
//                ["feature-state","p"], so a day change is one setPaintProperty and no feature-state calls
// ?probe=1 adds a queryRenderedFeatures check on every rendered frame (slows frames; run without it for timings).
import { mvtIds } from "/mvt-ids.js";

const q = new URLSearchParams(location.search);
const MODE = q.get("mode") ?? "idle";
const PROBE = q.get("probe") === "1";
const D1 = Number(q.get("d1") ?? 294), D2 = Number(q.get("d2") ?? 120);
const CENTER = [Number(q.get("lon") ?? -121.0), Number(q.get("lat") ?? 52.0)];
const ZOOM = Number(q.get("z") ?? 11);
const N = 1957202;
const PSTATE = MODE.endsWith("P");
const T = () => performance.now();
const R = { mode: MODE, probe: PROBE, d1: D1, d2: D2, marks: {}, pans: [], longtasks: [], frames: [], idles: [],
            colour: [], notes: [], errors: [], mem: {}, expr: {} };
window.R = R;
const mark = (k) => { R.marks[k] = Math.round(T()); };
const mem = (k) => { R.mem[k] = performance.memory ? Math.round(performance.memory.usedJSHeapSize / 1048576) : null; };
new PerformanceObserver((l) => { for (const e of l.getEntries()) R.longtasks.push([Math.round(e.startTime), Math.round(e.duration)]); })
  .observe({ type: "longtask", buffered: true });

/** Colouring work, timed: every strategy's own code runs through here. */
let calls = 0;
function work(label, fn) {
  const t0 = T(), c0 = calls;
  const out = fn();
  R.colour.push([Math.round(t0), +(T() - t0).toFixed(2), calls - c0, label]);
  return out;
}

// ---- the status index (release paint), decoded as statusIndex.ts does --------------------------------------------
async function loadIndex() {
  const t0 = T();
  const res = await fetch("/paint.bin.gz");
  const raw = new Uint8Array(await new Response(res.body.pipeThrough(new DecompressionStream("gzip"))).arrayBuffer());
  const t1 = T();
  const len = new DataView(raw.buffer, raw.byteOffset).getUint32(5, true);
  const b = raw.subarray(9, 9 + len);
  let pos = 4 + 1 + 16;
  const v = () => { let n = 0, mul = 1, c; do { c = b[pos++]; n += (c & 0x7f) * mul; mul *= 128; } while (c & 0x80); return n; };
  const nP = v();
  const days = new Uint8Array(nP * 366);
  for (let p = 0; p < nP; p++) {
    let d = 0;
    for (let r = v(); r > 0; r--) { const x = v(), code = x & 7, l = Math.floor(x / 8); days.fill(code, p * 366 + d, p * 366 + d + l); d += l; }
  }
  const nRuns = v(), gaps = new Array(nRuns), counts = new Array(nRuns);
  for (let i = 0; i < nRuns; i++) gaps[i] = v();
  for (let i = 0; i < nRuns; i++) counts[i] = v();
  const bySection = nP < 255 ? new Uint8Array(N) : new Uint16Array(N);
  let end = 0;
  for (let i = 0; i < nRuns; i++) { const p = v(), s = end + gaps[i]; bySection.fill(p + 1, s, s + counts[i]); end = s + counts[i]; }
  R.marks.indexFetchMs = Math.round(t1 - t0);
  R.marks.indexDecodeMs = Math.round(T() - t1);
  return { bySection, days };
}
function codesFor(ix, day) {
  const t0 = T();
  const out = new Uint8Array(N), off = day - 1;
  for (let h = 0; h < N; h++) { const p = ix.bySection[h]; if (p) out[h] = ix.days[(p - 1) * 366 + off]; }
  R.marks[`codes${day}Ms`] = +(T() - t0).toFixed(1);
  return out;
}
const STATUS = ["base", "own", "closed", null, null];

// ---- map ----------------------------------------------------------------------------------------------------------
maplibregl.addProtocol("pmtiles", new pmtiles.Protocol().tile);
const styleText = (await (await fetch("/style.json")).text()).replaceAll("http://127.0.0.1:39301", location.origin);
const style = JSON.parse(styleText);
const TOK = style.metadata.statusTokens;
for (const l of style.layers) if (l.source === "imagery") l.layout = { ...(l.layout ?? {}), visibility: "none" };
// the status layers and the paint property holding the ["match", ["feature-state","status"], …]
const isStatusMatch = (x) => Array.isArray(x) && x[0] === "match" && JSON.stringify(x[1]) === '["feature-state","status"]';
const replaceIn = (x, rep) => isStatusMatch(x) ? rep : Array.isArray(x) ? x.map((y) => replaceIn(y, rep)) : x;
const SL = [];
for (const l of style.layers)
  for (const [k, val] of Object.entries(l.paint ?? {}))
    if (JSON.stringify(val).includes('["feature-state","status"]') && k.endsWith("color")) SL.push({ id: l.id, prop: k, orig: val });
R.statusLayers = SL.map((s) => `${s.id}.${s.prop}`);
const statusLayerIds = SL.map((s) => s.id);
function setStatusExpr(rep) { for (const s of SL) m.setPaintProperty(s.id, s.prop, replaceIn(s.orig, rep)); }
const sparseMatch = (fallback) => ["match", ["feature-state", "status"], "closed", TOK.closed, "own", TOK.own,
                                   "base", TOK.base, "none", TOK.missing, fallback];

mark("mapCreate");
const m = new maplibregl.Map({ container: "map", style, center: CENTER, zoom: ZOOM, attributionControl: false,
                               // the app keeps MapLibre's default 300 ms symbol fade, and `idle` waits for it
                               fadeDuration: Number(q.get("fade") ?? 300) });
window.__map = m;
m.on("error", (e) => R.errors.push(String(e.error?.message ?? e.error ?? e).slice(0, 200)));
m.on("idle", () => R.idles.push(Math.round(T())));
m.once("render", () => mark("firstRender"));

// ---- probe ------------------------------------------------------------------------------------------------------
let codes = null;          // today's code per handle (the in-memory id -> status lookup)
let exprLive = false;      // B: the status expression is installed
let sparseLive = false;    // Ds: the no-state fallback is base
function effective(f) {
  if (MODE === "Bm" || MODE === "Bs") return exprLive ? STATUS[codes[f.id]] ?? null : null;
  // P: the colour is days[p][today] through the installed profile expression; a feature is right iff it has its p
  if (PSTATE) return f.state?.p === undefined || !exprLive ? null : STATUS[profCode(f.state.p)] ?? null;
  const s = f.state?.status;
  if (MODE === "Ds") return s === undefined ? (sparseLive ? "base" : null) : s === "none" ? null : s;
  return s ?? null;
}
if (!PROBE) m.on("render", () => R.frames.push([Math.round(T()), -1, -1]));
if (PROBE) m.on("render", () => {
  if (!codes) return;
  const fs = m.queryRenderedFeatures({ layers: statusLayerIds });
  let wrong = 0, total = 0;
  for (const f of fs) {
    if (f.id === undefined) continue;
    total++;
    const want = STATUS[codes[f.id]] ?? null;
    if (want !== null && effective(f) !== want) {
      wrong++;
      if ((R.wrongSamples ??= []).length < 30 && Math.random() < 0.02)
        R.wrongSamples.push([Math.round(T()), f.id, f.layer.id, f.sourceLayer, JSON.stringify(f.state), want,
                             seenA.get(f.id) ?? null, f.tile ? `${f.tile.z}` : null]);
    }
  }
  R.frames.push([Math.round(T()), wrong, total]);
});

// ---- the strategies ---------------------------------------------------------------------------------------------
const set = (layer, id, status) => { calls++; m.setFeatureState({ source: "atlas", sourceLayer: layer, id }, { status }); };
const unset = (layer, id) => { calls++; m.removeFeatureState({ source: "atlas", sourceLayer: layer, id }); };
let IX = null, DAY = D1;
const profCode = (p) => (p ? IX.days[(p - 1) * 366 + DAY - 1] : 0);
const setP = (layer, id) => { calls++; m.setFeatureState({ source: "atlas", sourceLayer: layer, id }, { p: IX.bySection[id] }); };
/** one feature, whichever kind of state this mode keeps */
const apply = (layer, id) => (PSTATE ? setP(layer, id) : set(layer, id, STATUS[codes[id]] ?? null));
function profExpr(day) {
  const nP = IX.days.length / 366, own = [], closed = [], none = [];
  for (let p = 1; p <= nP; p++) {
    const c = IX.days[(p - 1) * 366 + day - 1];
    if (c === 1) own.push(p); else if (c === 2) closed.push(p); else if (c > 2) none.push(p);
  }
  const e = ["match", ["coalesce", ["feature-state", "p"], -1], -1, TOK.missing];
  if (own.length) e.push(own, TOK.own);
  if (closed.length) e.push(closed, TOK.closed);
  if (none.length) e.push(none, TOK.missing);
  e.push(TOK.base);
  return e;
}
function installProf(day, label) {
  const t0 = T();
  const e = profExpr(day);
  const t1 = T();
  setStatusExpr(e);
  const t2 = T();
  DAY = day;
  exprLive = true;
  R.expr[label] = { labels: e.length / 2 - 1, bytes: JSON.stringify(e).length, buildMs: +(t1 - t0).toFixed(2),
                    setPaintMs: +(t2 - t1).toFixed(2) };
  R.colour.push([Math.round(t0), +(t2 - t0).toFixed(2), 0, label]);
}
const sparseVal = (c) => (c === 1 ? "own" : c === 2 ? "closed" : "none");

// idle (today's path)
let lastVisible = [];
function idleReport() {
  const seen = new Set();
  const t0 = T();
  for (const f of m.queryRenderedFeatures())
    if (f.id !== undefined && (f.sourceLayer === "stream" || f.sourceLayer === "lake"
        || (f.sourceLayer === "wetland" && f.properties?.water === "stream"))) seen.add(f.id);
  R.colour.push([Math.round(t0), +(T() - t0).toFixed(2), 0, "idle-query"]);
  lastVisible = [...seen];
  setTimeout(() => work("idle-set", () => {
    for (const id of lastVisible) { const s = STATUS[codes[id]] ?? null; for (const l of ["stream", "lake", "wetland"]) set(l, id, s); }
  }), 0);
}

// A: per tile
const seenA = new Map();   // id -> layer
function tileIds(tile) {
  const vt = tile.latestFeatureIndex?.loadVTLayers?.();
  const out = {};
  if (vt) {
    for (const l of ["stream", "lake", "wetland"]) {
      const layer = vt[l];
      if (!layer) continue;
      const ids = out[l] = [];
      for (let i = 0; i < layer.length; i++) { const id = layer.feature(i).id; if (id !== undefined) ids.push(id); }
    }
  } else if (tile.latestRawTileData) return mvtIds(tile.latestRawTileData, ["stream", "lake", "wetland"]);
  return out;
}
function colourTile(tile) {
  work("A-tile", () => {
    const got = tileIds(tile);
    for (const [l, ids] of Object.entries(got))
      for (const id of ids) { if (seenA.get(id) === l) continue; seenA.set(id, l); apply(l, id); }
  });
}
function tilesLoaded() {
  // maplibre 5.24: style.tileManagers[src] holds _inViewTiles (getAllTiles) and _outOfViewCache (an LRU of {value})
  const tm = m.style.sourceCaches?.atlas ?? m.style.tileManagers?.atlas;
  if (!tm) return [];
  const out = tm._tiles ? Object.values(tm._tiles) : [...(tm._inViewTiles?.getAllTiles?.() ?? [])];
  for (const list of Object.values(tm._outOfViewCache?.data ?? {})) for (const e of list) if (e?.value) out.push(e.value);
  return out;
}

// E: region around the user, from a z11 cell index (ids per cell per layer)
let cells = null, cellZ = 11;
const colouredCells = new Set();
const cellOf = (lon, lat) => {
  const n = 2 ** cellZ, r = (lat * Math.PI) / 180;
  return [Math.floor(((lon + 180) / 360) * n), Math.floor(((1 - Math.log(Math.tan(r) + 1 / Math.cos(r)) / Math.PI) / 2) * n)];
};
function cellRange(margin) {
  const b = m.getBounds(), w = b.getEast() - b.getWest(), h = b.getNorth() - b.getSouth();
  const [x0, y0] = cellOf(b.getWest() - margin * w, b.getNorth() + margin * h);
  const [x1, y1] = cellOf(b.getEast() + margin * w, b.getSouth() - margin * h);
  return [x0, y0, x1, y1];
}
let missingCells = 0;
function regionTick(margin, label) {
  work(label, () => {
    const [x0, y0, x1, y1] = cellRange(margin);
    for (let x = x0; x <= x1; x++) for (let y = y0; y <= y1; y++) {
      const k = `${x}/${y}`;
      if (colouredCells.has(k)) continue;
      colouredCells.add(k);
      const c = cells[k];
      if (!c) { missingCells++; continue; }
      for (const [l, ids] of Object.entries(c)) for (const id of ids) apply(l, id);
    }
    // drop what is more than one further viewport away
    const [a0, b0, a1, b1] = cellRange(margin + 1);
    for (const k of [...colouredCells]) {
      const [x, y] = k.split("/").map(Number);
      if (x >= a0 && x <= a1 && y >= b0 && y <= b1) continue;
      colouredCells.delete(k);
      const c = cells[k];
      if (c) for (const [l, ids] of Object.entries(c)) for (const id of ids) unset(l, id);
    }
  });
}

// B: the day's status as one expression on ["id"]
function matchExpr(cs) {
  const own = [], closed = [], none = [];
  for (let h = 0; h < N; h++) { const c = cs[h]; if (c === 1) own.push(h); else if (c === 2) closed.push(h); else if (c > 2) none.push(h); }
  const e = ["match", ["id"]];
  if (own.length) e.push(own, TOK.own);
  if (closed.length) e.push(closed, TOK.closed);
  if (none.length) e.push(none, TOK.missing);
  e.push(TOK.base);
  return { e, ids: own.length + closed.length + none.length };
}
function stepExpr(cs) {
  // numeric classes out of the step (colour literals per stop cost a parsed Color each), then one small match
  const cls = [0, 1, 2, 3, 3];
  const s = ["step", ["id"], cls[cs[0]]];
  let pc = cls[cs[0]];
  const cap = Number(q.get("cap") ?? Infinity);   // ?cap=: stop after this many stops (scaling test only)
  for (let h = 1; h < N && s.length < 3 + 2 * cap; h++) { const c = cls[cs[h]]; if (c !== pc) { s.push(h, c); pc = c; } }
  const e = ["match", s, 1, TOK.own, 2, TOK.closed, 3, TOK.missing, TOK.base];
  return { e, stops: (s.length - 3) / 2 };
}
function installExpr(cs, label) {
  const t0 = T();
  const { e, ids, stops } = MODE === "Bm" ? matchExpr(cs) : stepExpr(cs);
  const t1 = T();
  const bytes = JSON.stringify(e).length;
  const t2 = T();
  setStatusExpr(e);
  const t3 = T();
  exprLive = true;
  R.expr[label] = { ids, stops, bytes, buildMs: Math.round(t1 - t0), stringifyMs: Math.round(t2 - t1),
                    setPaintMs: Math.round(t3 - t2) };
  R.colour.push([Math.round(t0), +(t3 - t0).toFixed(2), 0, label]);
}

// ---- setup per mode --------------------------------------------------------------------------------------------
async function setup() {
  const ix = await loadIndex();
  mark("indexReady");
  codes = codesFor(ix, D1);
  R.ix = ix;
  IX = ix;
  if (MODE === "none") return;
  if (PSTATE) installProf(D1, `day${D1}`);
  if (MODE === "idle") { m.on("idle", idleReport); if (m.loaded()) idleReport(); return; }
  if (MODE === "A" || MODE === "E1A" || MODE === "AP" || MODE === "E1AP") {
    m.on("sourcedata", (e) => { if (e.sourceId === "atlas" && e.tile && codes) colourTile(e.tile); });
    for (const t of tilesLoaded()) if (t.state === "loaded") colourTile(t);
  }
  if (MODE.startsWith("E")) {
    cells = (await (await fetch("/cells.json")).json()).cells;
    const margin = MODE === "E0" ? 0 : MODE === "E2" ? 2 : 1;
    regionTick(margin, "E-start");
    m.on("move", () => regionTick(margin, "E-move"));
  }
  if (MODE === "Ds") {
    work("Ds-setall", () => { for (let h = 0; h < N; h++) { const c = codes[h]; if (c) { const s = sparseVal(c); set("stream", h, s); set("lake", h, s); set("wetland", h, s); } } });
    const t0 = T();
    setStatusExpr(sparseMatch(TOK.base));
    sparseLive = true;
    R.colour.push([Math.round(t0), +(T() - t0).toFixed(2), 0, "Ds-flip"]);
  }
  if (MODE === "Dfull") work("Dfull-setall", () => { for (let h = 0; h < N; h++) set("stream", h, STATUS[codes[h]] ?? null); });
  if (MODE === "Bm" || MODE === "Bs") installExpr(codes, `day${D1}`);
}

async function dayChange() {
  const ix = R.ix;
  const next = codesFor(ix, D2);
  let diff = 0;
  for (let h = 0; h < N; h++) if (next[h] !== codes[h]) diff++;
  R.dayDiff = diff;
  const prev = codes;
  codes = next;
  if (MODE === "idle") work("idle-day", () => { for (const id of lastVisible) { const s = STATUS[codes[id]] ?? null; for (const l of ["stream", "lake", "wetland"]) set(l, id, s); } });
  if (PSTATE) { installProf(D2, `day${D2}`); return; }
  if (MODE === "A" || MODE === "E1A") work("A-day", () => { for (const [id, l] of seenA) set(l, id, STATUS[codes[id]] ?? null); });
  if (MODE.startsWith("E")) work("E-day", () => {
    for (const k of colouredCells) { const c = cells[k]; if (c) for (const [l, ids] of Object.entries(c)) for (const id of ids) set(l, id, STATUS[codes[id]] ?? null); }
  });
  if (MODE === "Ds") work("Ds-day", () => {
    for (let h = 0; h < N; h++) {
      if (next[h] === prev[h]) continue;
      for (const l of ["stream", "lake", "wetland"]) next[h] ? set(l, h, sparseVal(next[h])) : unset(l, h);
    }
  });
  if (MODE === "Dfull") work("Dfull-day", () => { for (let h = 0; h < N; h++) if (next[h] !== prev[h]) set("stream", h, STATUS[next[h]] ?? null); });
  if (MODE === "Bm" || MODE === "Bs") installExpr(codes, `day${D2}`);
}

// ---- the script --------------------------------------------------------------------------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function settled(after, maxMs = 20000) {
  const t0 = T();
  while (T() - t0 < maxMs) {
    if (R.idles.some((t) => t >= after) && !m.isMoving() && m.areTilesLoaded()) return true;
    await sleep(25);
  }
  R.notes.push(`not settled after ${maxMs} ms at ${Math.round(after)}`);
  return false;
}
async function step(label, go) {
  const tStart = T();
  const c0 = calls;
  await go();
  const tEnd = T();
  await settled(tEnd);
  await sleep(600);
  R.pans.push({ label, tStart: Math.round(tStart), tEnd: Math.round(tEnd), tNext: Math.round(T()), calls: calls - c0 });
}

async function main() {
  await new Promise((r) => m.once("load", r));
  mark("load");
  await setup();
  mark("setupDone");
  mem("afterSetup");
  await settled(R.marks.setupDone);
  await sleep(800);
  mark("startDone");
  const W = m.getContainer().clientWidth, H = m.getContainer().clientHeight;
  const dirs = [[1, 0], [0, 1], [-1, 0], [-1, 0], [0, -1], [0, -1], [1, 0], [1, 0], [0, 1]];
  const move = (f) => new Promise((r) => { m.once("moveend", r); f(); });
  for (const [i, [dx, dy]] of dirs.entries())
    await step(`pan${i + 1}(${dx},${dy})`, () => move(() => m.panBy([dx * W, dy * H], { duration: 400, easing: (t) => t })));
  await step("zoom-out", () => move(() => m.zoomTo(ZOOM - 1, { duration: 300 })));
  await step("zoom-in", () => move(() => m.zoomTo(ZOOM, { duration: 300 })));
  // a flick: three half-viewport pans back to back, no settling between
  await step("flick", async () => {
    await move(() => m.panBy([W / 2, 0], { duration: 150, easing: (t) => t }));
    await move(() => m.panBy([W / 2, H / 3], { duration: 150, easing: (t) => t }));
    await move(() => m.panBy([-W, -H / 3], { duration: 200, easing: (t) => t }));
  });
  mem("afterPans");
  mark("dayStart");
  const c0 = calls;
  await dayChange();
  mark("dayApplied");
  R.dayCalls = calls - c0;
  await sleep(50);
  await settled(R.marks.dayApplied);
  await sleep(600);
  mark("dayDone");
  mem("afterDay");
  R.missingCells = missingCells;
  R.totalCalls = calls;
  delete R.ix;
  window.DONE = true;
}
main().catch((e) => { R.errors.push(String(e?.stack ?? e).slice(0, 500)); window.DONE = true; });
