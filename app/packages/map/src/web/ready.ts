declare const __DEV__: boolean | undefined;
/**
 * WHEN A CALL CAN LAND on the web map (`Map.web.tsx` and its hooks in this folder): the protocol registered once, and
 * the waits for a style, a source or the first load — each a different event, for the reasons each says.
 */
import * as maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import { COPY_SOURCE } from "../water-copy";

/** An empty source, so the gauge layers exist before the first feed tick arrives. */
export const EMPTY_FC = '{"type":"FeatureCollection","features":[]}';

/**
 * `pmtiles://` has to be registered on maplibre ONCE per page, not per map. Registering it
 * again on every mount leaks a protocol handler and, worse, the second one wins while the
 * first map is still using it.
 */
let registered = false;
export function registerPMTiles() {
  if (registered) return;
  maplibregl.addProtocol("pmtiles", new Protocol().tile);
  registered = true;
}

/**
 * The maps whose style has loaded once. `isStyleLoaded()` is ALSO false while tiles load (GL JS counts the sources),
 * but paint, layout and feature-state calls need only the style itself, which never unloads here (the style is set once
 * per map). Without this a zoom out to the basin field — a long tile burst, exactly when the basin standings arrive —
 * waited on a `styledata` that never came, and the field stayed blank (N2 review web walk).
 */
export const styledOnce = new WeakSet<maplibregl.Map>();

/**
 * Run `fn` once the style can take layout and feature-state calls — now, or as soon as it
 * can. Returns the cleanup.
 *
 * NOT `isStyleLoaded() ? fn() : once("load", fn)`, the pattern this file uses elsewhere and
 * the one HANDOFF-ui.md records biting twice: `isStyleLoaded()` is briefly false on a map
 * that loaded long ago (every `setFilter` and tile burst), and `load` never fires again —
 * so the call was simply lost. `styledata` keeps arriving, so waiting on it cannot miss.
 */
export function whenStyled(m: maplibregl.Map, fn: () => void): () => void {
  if (m.isStyleLoaded()) { fn(); return () => {}; }
  const h = () => {
    if (!m.isStyleLoaded()) return;
    m.off("styledata", h); m.off("load", h);
    fn();
  };
  m.on("styledata", h); m.on("load", h);
  return () => { m.off("styledata", h); m.off("load", h); };
}

/**
 * Run `fn` once the route overlay's sources exist — now, almost always.
 *
 * NOT `whenStyled`: a GeoJSON source takes `setData` whenever it exists, and
 * `isStyleLoaded()` is false for as long as ANY tile is loading — a route map whose lines
 * arrived mid-burst waited for a `styledata` that a settled map never sends again, and the
 * route was never drawn (measured: the pins landed, the lines did not).
 */
export function whenTraced(m: maplibregl.Map, fn: () => void): () => void {
  if (m.getSource("trace")) { fn(); return () => {}; }
  const h = () => {
    if (!m.getSource("trace")) return;
    m.off("styledata", h);
    fn();
  };
  m.on("styledata", h);
  return () => { m.off("styledata", h); };
}

/**
 * Run `fn` now if this map's style has loaded ONCE, else on its `load` — for the look and the data. Not `whenStyled`:
 * its `isStyleLoaded()` also waits out every tile burst, and a `styledata` may never come; and not before `load`,
 * where the pill and hatches are added (adding them earlier raced the load handler: "gauge-pill already exists").
 */
export function afterLoad(m: maplibregl.Map, fn: () => void): () => void {
  if (styledOnce.has(m)) { fn(); return () => {}; }
  m.once("load", fn);
  return () => { m.off("load", fn); };
}

/** Run `fn` once the atlas source exists — the moment feature-state and paint calls can land on it. */
export function whenAtlas(m: maplibregl.Map, fn: () => void): () => void {
  if (m.getSource("atlas")) { fn(); return () => {}; }
  const h = () => {
    if (!m.getSource("atlas")) return;
    m.off("styledata", h);
    fn();
  };
  m.on("styledata", h);
  return () => { m.off("styledata", h); };
}
/** (T175) Run `fn` once the water copy's source exists — as `whenTraced`, for the same reason. */
export function whenCopy(m: maplibregl.Map, fn: () => void): () => void {
  if (m.getSource(COPY_SOURCE)) { fn(); return () => {}; }
  const h = () => {
    if (!m.getSource(COPY_SOURCE)) return;
    m.off("styledata", h);
    fn();
  };
  m.on("styledata", h);
  return () => { m.off("styledata", h); };
}

/** Development only: where the colouring's cost is recorded, for the colour-latency measurements. */
type ColourCost = { t: number; ms: number; calls: number; kind: string };
export const devCost = (c: ColourCost) => {
  if (typeof __DEV__ !== "undefined" && __DEV__)
    ((globalThis as { __colourCost?: ColourCost[] }).__colourCost ??= []).push(c);
};
/** Development only: take the public route even where 5.24's internals are there — to measure what it costs. */
export const forcePublic = () => typeof __DEV__ !== "undefined" && __DEV__
  && (globalThis as { __colourPublic?: boolean }).__colourPublic === true;
