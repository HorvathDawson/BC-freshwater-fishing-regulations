/**
 * ONE COLOURING ENGINE, IN THE REAL WEB MAP (owner 2026-10-21: "make sure other stuff between the 2 map pathways is the
 * same ... it should be a single pathway ideally"; "we need to do the same painting speed up for the conditions map
 * too"). `Map.web.tsx` over a recording maplibre-gl, mocked at its module boundary — the same seam as
 * `adapter-contract.render.test.tsx`.
 *
 *   one pass     a tile that arrives is labelled ONCE per section, in the tile's own `sourcedata` event, carrying BOTH
 *                channels' labels (`p` for the Map tab's status, `standing` for Conditions) in one feature-state call. A
 *                channel coloured by any other mechanism — a second engine, a pushed `data` loop, an `idle` listener —
 *                shows up here as a second call per section, or as a section without its value after the event.
 *   feed alone   a new gauge feed (new values, the SAME release labels, the SAME tiles) recolours with one engine call:
 *                feature-state on the loaded sections only — no paint property (which would make GL re-lay out every
 *                loaded tile), no clear, no map rebuilt, no tile fetched.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import type { StatusIndex } from "@app/core";
import type { MapProps } from "./map-props";
import type { StandingValues } from "./style";

const log: string[] = [];
let built = 0;
/** The tiles the fake map holds (in view), as GL 5.24 shapes them: a feature index that decodes the tile's layers. */
let held: unknown[] = [];
let stylesLoaded = true;
const tile = (ids: Record<string, number[]>) => ({
  state: "loaded",
  latestFeatureIndex: {
    loadVTLayers: () => Object.fromEntries(Object.entries(ids).map(([l, xs]) =>
      [l, { length: xs.length, feature: (i: number) => ({ id: xs[i] }) }])),
  },
});

type Handler = (e?: unknown) => void;
let gl: FakeGl | null = null;
class FakeGl {
  private on_ = new Map<string, Set<Handler>>();
  private json: { layers: { id: string; source?: string }[] };
  /** GL 5.24's internals as `tile-ids.web.ts` reads them. */
  style = { tileManagers: { atlas: { _inViewTiles: { getAllTiles: () => held }, _outOfViewCache: { data: {} } } } };
  touchZoomRotate = { disableRotation: () => {} };
  keyboard = { disableRotation: () => {} };
  constructor(opts: Record<string, unknown>) {
    built++; gl = this;
    this.json = opts.style as FakeGl["json"];
    queueMicrotask(() => this.fire("load"));
  }
  on(ev: string, fn: Handler) { (this.on_.get(ev) ?? this.on_.set(ev, new Set()).get(ev)!).add(fn); return this; }
  once(ev: string, fn: Handler) { return this.on(ev, fn); }
  off(ev: string, fn: Handler) { this.on_.get(ev)?.delete(fn); return this; }
  fire(ev: string, e?: unknown) { for (const fn of [...(this.on_.get(ev) ?? [])]) fn(e); }
  /** GL JS counts the SOURCES too: false through every tile burst, on a map that loaded long ago. */
  isStyleLoaded() { return stylesLoaded; }
  getStyle() { return this.json; }
  getLayer(id: string) { return this.json.layers.find((l) => l.id === id); }
  getSource() { return { setData: () => log.push("source setData") }; }
  setPaintProperty(id: string, prop: string) { log.push(`paint ${id} ${prop}`); }
  setLayoutProperty(id: string, prop: string, v: unknown) { if (id === "imagery") log.push(`layout ${id} ${prop} ${String(v)}`); }
  setLayerZoomRange() {} setFilter() {}
  addControl() {} hasImage() { return false; } addImage() {} removeImage() {} remove() {}
  getContainer() { return { clientWidth: 400, clientHeight: 800 }; }
  getCanvas() { return { style: { width: "400px", height: "800px" } }; }
  resize() {}
  getCenter() { return { lng: -122, lat: 49 }; }
  getZoom() { return 11; }
  getBounds() { return { getWest: () => -123, getSouth: () => 48, getEast: () => -121, getNorth: () => 50 }; }
  querySourceFeatures() { log.push("querySourceFeatures"); return []; }
  queryRenderedFeatures() { return []; }
  fitBounds() {}
  setFeatureState(t: { source: string; sourceLayer: string; id: unknown }, s: unknown) {
    log.push(`set ${t.source}/${t.sourceLayer} ${String(t.id)} ${JSON.stringify(s)}`);
  }
  removeFeatureState(t: { source: string; sourceLayer: string; id?: unknown }, key?: string) {
    log.push(`remove ${t.source}/${t.sourceLayer} ${String(t.id)} ${key ?? "*"}`);
  }
}
class Control { constructor(_o?: unknown) {} }
vi.mock("maplibre-gl", () => ({ Map: FakeGl, NavigationControl: Control, ScaleControl: Control,
                                Marker: class { setLngLat() { return this; } addTo() { return this; } remove() {} },
                                Popup: class { setText() { return this; } }, addProtocol: () => {} }));
vi.mock("pmtiles", () => ({ Protocol: class { tile = () => {}; } }));

const { Map: WebMap } = await import("./Map.web");

afterEach(() => { cleanup(); log.length = 0; held = []; built = 0; gl = null; stylesLoaded = true; });

/** A status index: section 10 reads profile 0, 11 profile 1; every other section is unlisted (base). */
const INDEX: StatusIndex = {
  handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 2, waters: 0, profiles: 2,
  profileOf: (s) => ({ 10: 0, 11: 1 } as Record<number, number>)[Number(s)] ?? null,
  profileCodeOn: (p) => (p === 0 ? "closed" : "own"),
  codeOn: () => "base", waterCodeOn: () => "base",
};
const DAY = new Date(2026, 5, 1);
const STATUS = { index: INDEX, day: DAY };
/** The release's classes: 10 and 11 share class 0, 12 is class 1, 13 has none (nothing speaks for it). */
const LABELS = { classOf: (s: number) => ({ 10: 0, 11: 0, 12: 1 } as Record<number, number>)[s] ?? null };
const values = (xs: number[], basins: Record<string, number> = {}): StandingValues =>
  ({ section: (k) => xs[k] ?? NaN, basins });
const FEED1 = values([42.5, -1]);
const FEED2 = values([80, 12]);

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
const MODES = { stream: "standing", lake: "standing" };
const props = (p: Partial<MapProps>): MapProps => ({
  at: AT, theme: "light", view: "plain", modes: MODES, initial: { lon: -122, lat: 49, zoom: 11 }, ...p,
});
const sections = () => log.filter((x) => / atlas\/(stream|lake|wetland) /.test(x));

describe("one colouring engine in the web map", () => {
  it("a tile's sections are labelled ONCE, in its load event, with every channel's label in one call", async () => {
    render(<WebMap {...props({ status: STATUS, standing: { labels: LABELS, values: FEED1 } })} />);
    await act(async () => { await Promise.resolve(); });
    log.length = 0;
    act(() => gl!.fire("sourcedata", { sourceId: "atlas", tile: tile({ stream: [10, 11, 13], lake: [12] }) }));
    expect(sections()).toEqual([
      'set atlas/stream 10 {"p":"p0","standing":42.5}',
      'set atlas/stream 11 {"p":"p1","standing":42.5}',
      'set atlas/stream 13 {"p":"b"}',
      'set atlas/lake 12 {"p":"b","standing":-1}',
    ]);
    // the same tile again (a cached tile revived, a re-parse): nothing — the de-dupe is the engine's, for both
    log.length = 0;
    act(() => gl!.fire("sourcedata", { sourceId: "atlas", tile: tile({ stream: [10, 11, 13], lake: [12] }) }));
    expect(sections()).toEqual([]);
  });

  it("A NEW FEED ALONE (same tiles, same release): one engine call, feature-state only — no paint, no clear, no reload", async () => {
    held = [tile({ stream: [10, 11, 13], lake: [12] })];
    const r = render(<WebMap {...props({ status: STATUS, standing: { labels: LABELS, values: FEED1 } })} />);
    // mount: each channel, installed in turn, labels the tiles the map already holds — through the same engine
    await waitFor(() => expect(sections()).toEqual([
      'set atlas/stream 10 {"p":"p0"}', 'set atlas/stream 11 {"p":"p1"}', 'set atlas/stream 13 {"p":"b"}',
      'set atlas/lake 12 {"p":"b"}',
      'set atlas/stream 10 {"standing":42.5}', 'set atlas/stream 11 {"standing":42.5}',
      'set atlas/lake 12 {"standing":-1}',
    ]));
    log.length = 0;
    r.rerender(<WebMap {...props({ status: STATUS, standing: { labels: LABELS, values: FEED2 } })} />);
    await act(async () => { await Promise.resolve(); });
    expect(log).toEqual([
      'set atlas/stream 10 {"standing":80}',
      'set atlas/stream 11 {"standing":80}',
      'set atlas/lake 12 {"standing":12}',
    ]);
    expect(built).toBe(1);                       // the same map: no style rebuilt, no tile source recreated
  });

  it("THE ROUTE MAP (DonorMap → MiniMap) through the same engine: only the route's sections; a horizon relabels them", async () => {
    // the route as classes (@app/ui `routeLabels`): its sections' places on the chain, nothing else
    const ROUTE = { classOf: (s: number) => ({ 11: 0, 12: 1 } as Record<number, number>)[s] ?? null };
    held = [tile({ stream: [10, 11, 13], lake: [12] })];
    const r = render(<WebMap {...props({ data: undefined, standing: { labels: ROUTE, values: values([55, 7]) } })} />);
    await waitFor(() => expect(sections()).toEqual(['set atlas/stream 11 {"standing":55}',
                                                    'set atlas/lake 12 {"standing":7}']));
    log.length = 0;
    r.rerender(<WebMap {...props({ data: undefined, standing: { labels: ROUTE, values: values([20]) } })} />);
    await act(async () => { await Promise.resolve(); });
    // feature-state only, the route only: a reach with nothing to say now is cleared, no paint, no clear-all
    expect(log).toEqual(['set atlas/stream 11 {"standing":20}', 'set atlas/lake 12 {"standing":null}']);
  });

  it("before any feed answers nothing is labelled; the status is not held back by it", async () => {
    render(<WebMap {...props({ status: STATUS, standing: { labels: LABELS, values: null } })} />);
    await act(async () => { await Promise.resolve(); });
    log.length = 0;
    act(() => gl!.fire("sourcedata", { sourceId: "atlas", tile: tile({ stream: [10, 12] }) }));
    expect(sections()).toEqual(['set atlas/stream 10 {"p":"p0"}', 'set atlas/stream 12 {"p":"b"}']);
  });

  it("A HORIZON CHANGE (new values, new-but-equal look props): feature-state only, ZERO paint or layout calls", async () => {
    held = [tile({ stream: [10, 11, 13], lake: [12] })];
    // what the Shell hands the map on a horizon click: the same look as fresh objects, and the next standings
    const look = () => ({ modes: { ...MODES }, groups: { water: true }, hide: [] as string[], show: [] as string[] });
    const r = render(<WebMap {...props({ ...look(), standing: { labels: LABELS, values: FEED1 } })} />);
    await waitFor(() => expect(sections().length).toBeGreaterThan(0));
    await act(async () => { await Promise.resolve(); });
    log.length = 0;
    r.rerender(<WebMap {...props({ ...look(), standing: { labels: LABELS, values: FEED2 } })} />);
    await act(async () => { await Promise.resolve(); });
    expect(log).toEqual([
      'set atlas/stream 10 {"standing":80}',
      'set atlas/stream 11 {"standing":80}',
      'set atlas/lake 12 {"standing":12}',
    ]);
    // and a real look change still repaints
    log.length = 0;
    r.rerender(<WebMap {...props({ ...look(), modes: { stream: "plain", lake: "standing" },
                                   standing: { labels: LABELS, values: FEED2 } })} />);
    await act(async () => { await Promise.resolve(); });
    expect(log.filter((x) => x.startsWith("paint stream ")).length).toBeGreaterThan(0);
  });

  it("THE GROUND goes with the look: satellite switched on mid tile burst, when no `styledata` may follow", async () => {
    const r = render(<WebMap {...props({ basemap: "map" })} />);
    await act(async () => { await Promise.resolve(); });
    log.length = 0;
    stylesLoaded = false;                        // a tile burst: GL says the style is not loaded, and stays quiet after
    r.rerender(<WebMap {...props({ basemap: "satellite" })} />);
    await act(async () => { await Promise.resolve(); });
    expect(log.filter((x) => x.startsWith("layout"))).toEqual(["layout imagery visibility visible"]);
  });
});
