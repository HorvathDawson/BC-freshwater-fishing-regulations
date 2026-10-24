/**
 * T175 IN THE REAL WEB MAP: `Map.web.tsx` over a recording maplibre-gl (the seam `one-engine.render.test.tsx` uses),
 * with an atlas that carries a small creek only from z12 — the owner's case: a Conditions map framed at z8 "doesn't show
 * the stream because it is too zoomed out".
 *
 *   held     the map asked to hold the creek (`waterLine`) steps in about the place being read until its tiles carry
 *            it, keeps the creek's line, gives it the states its atlas features hold, and draws it below z12
 *   cleared  another water: the copy and its states are gone at once, before anything of the new one is kept
 *   one way  a selection or a standing written for the creek on the atlas is written to the copy too
 *   reader   a pan by the reader stops the stepping in
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import type { MapProps } from "./map-props";

const log: string[] = [];
type Handler = (e?: unknown) => void;
let gl: FakeGl | null = null;
let ready = true;
/** The camera: the fake moves it on `easeTo`, as GL JS does once the ease ends. */
let cam = { lng: -121.6, lat: 49.1, zoom: 8 };
/** What the atlas features hold (GL keeps feature-state per source-layer and id). */
const atlasState = new Map<string, Record<string, unknown>>();

const CREEK = [
  { id: 501, properties: { ord: 1 }, geometry: { type: "LineString", coordinates: [[-121.5, 49.2], [-121.49, 49.21]] } },
  { id: 502, properties: { ord: 1 }, geometry: { type: "LineString", coordinates: [[-121.49, 49.21], [-121.47, 49.22]] } },
];
const RIVER = [{ id: 900, properties: { ord: 6 }, geometry: { type: "LineString", coordinates: [[-121.7, 49], [-121.5, 49.2]] } }];
/** The creek is in the stream tiles from z12; the river at every zoom. */
const streamTiles = () => (Math.floor(cam.zoom) >= 12 ? [...CREEK, ...RIVER] : RIVER);

class FakeGl {
  private on_ = new Map<string, Set<Handler>>();
  private json: { layers: { id: string; source?: string; minzoom?: number }[] };
  touchZoomRotate = { disableRotation: () => {} };
  keyboard = { disableRotation: () => {} };
  constructor(opts: Record<string, unknown>) {
    gl = this;
    this.json = opts.style as FakeGl["json"];
    queueMicrotask(() => this.fire("load"));
  }
  on(ev: string, fn: Handler) { (this.on_.get(ev) ?? this.on_.set(ev, new Set()).get(ev)!).add(fn); return this; }
  once(ev: string, fn: Handler) { return this.on(ev, fn); }
  off(ev: string, fn: Handler) { this.on_.get(ev)?.delete(fn); return this; }
  fire(ev: string, e?: unknown) { for (const fn of [...(this.on_.get(ev) ?? [])]) fn(e); }
  isStyleLoaded() { return true; }
  loaded() { return ready; }
  getStyle() { return this.json; }
  getLayer(id: string) { return this.json.layers.find((l) => l.id === id); }
  getSource(id: string) {
    return { setData: (d: { features?: { id: number }[] }) =>
      log.push(`data ${id} [${(d.features ?? []).map((f) => f.id).join(",")}]`) };
  }
  setPaintProperty() {} setLayoutProperty() {} setFilter() {}
  setLayerZoomRange(id: string, lo: number, hi: number) { if (id.startsWith("water-copy")) log.push(`range ${id} ${lo}-${hi}`); }
  addControl() {} hasImage() { return false; } addImage() {} removeImage() {} remove() {}
  getContainer() { return { clientWidth: 400, clientHeight: 400 }; }
  getCanvas() { return { style: { width: "400px", height: "400px" } }; }
  resize() {}
  getCenter() { return { lng: cam.lng, lat: cam.lat }; }
  getZoom() { return cam.zoom; }
  getBounds() { return { getWest: () => -123, getSouth: () => 48, getEast: () => -121, getNorth: () => 50 }; }
  querySourceFeatures(_s: string, o: { sourceLayer: string }) {
    return o.sourceLayer === "stream" ? streamTiles().map((f) => ({ ...f })) : [];
  }
  queryRenderedFeatures() { return []; }
  fitBounds() {}
  easeTo(o: { center: [number, number]; zoom: number }) {
    log.push(`ease z${o.zoom}`);
    cam = { lng: o.center[0], lat: o.center[1], zoom: o.zoom };
    // our own move: no `originalEvent`, as GL JS sends it for a programmatic ease
    this.fire("movestart", {});
    queueMicrotask(() => this.fire("idle"));
  }
  getFeatureState(t: { source: string; sourceLayer: string; id: number }) {
    return atlasState.get(`${t.sourceLayer}/${t.id}`) ?? {};
  }
  setFeatureState(t: { source: string; sourceLayer?: string; id: unknown }, s: Record<string, unknown>) {
    if (t.source === "atlas") atlasState.set(`${t.sourceLayer}/${t.id}`, { ...atlasState.get(`${t.sourceLayer}/${t.id}`), ...s });
    if (t.source === "water-copy" || (t.source === "atlas" && t.sourceLayer === "stream"))
      log.push(`set ${t.source} ${String(t.id)} ${JSON.stringify(s)}`);
  }
  removeFeatureState(t: { source: string; sourceLayer?: string; id?: unknown }, key?: string) {
    if (t.source === "water-copy") log.push(`remove ${t.source} ${String(t.id)} ${key ?? "*"}`);
  }
}
class Control { constructor(_o?: unknown) {} }
vi.mock("maplibre-gl", () => ({ Map: FakeGl, NavigationControl: Control, ScaleControl: Control,
                                Marker: class { setLngLat() { return this; } addTo() { return this; } remove() {} },
                                Popup: class { setText() { return this; } }, addProtocol: () => {} }));
vi.mock("pmtiles", () => ({ Protocol: class { tile = () => {}; } }));

const { Map: WebMap } = await import("./Map.web");

afterEach(() => { cleanup(); log.length = 0; gl = null; cam = { lng: -121.6, lat: 49.1, zoom: 8 }; atlasState.clear(); ready = true; });

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
const props = (p: Partial<MapProps>): MapProps => ({
  at: AT, theme: "light", view: "conditions", modes: { stream: "standing", lake: "standing" },
  initial: { lon: cam.lng, lat: cam.lat, zoom: cam.zoom }, ...p,
});
const settle = async () => { for (let i = 0; i < 20; i++) await act(async () => { await Promise.resolve(); }); };

describe("T175: a creek's own line at every zoom, in the web map", () => {
  it("framed at z8, it steps in to the creek's first zoom about the place read, keeps it, and draws it below z12", async () => {
    atlasState.set("stream/501", { standing: 31 });
    render(<WebMap {...props({ highlight: [501],
                               waterLine: { sections: [501, 502], at: { lat: 49.205, lon: -121.495 } } })} />);
    await settle();
    await waitFor(() => expect(log.filter((x) => x.startsWith("data water-copy"))).toEqual(["data water-copy [501,502]"]));
    expect(log.filter((x) => x.startsWith("ease"))).toEqual(["ease z9", "ease z10", "ease z11", "ease z12"]);
    // the place being read stays where it was on screen as the frame closed in on it
    expect(Math.abs(cam.lng - (-121.495 + (-121.6 + 121.495) / 16))).toBeLessThan(1e-6);
    // the copy wears what the atlas features hold (the standing the engine gave, the selection), and stops at z12
    expect(log.filter((x) => x.startsWith("set water-copy")).sort()).toEqual([
      'set water-copy 501 {"standing":31,"selected":true}',
    ]);
    expect(log.filter((x) => x.startsWith("range"))).toEqual([
      "range water-copy-casing 0-12", "range water-copy-line 0-12"]);
  });

  it("a standing the engine labels the creek with after it is kept goes to the copy too — and the river's never", async () => {
    cam = { lng: -121.6, lat: 49.1, zoom: 12 };
    const labels = { classOf: (s: number) => (s === 900 ? 1 : 0) };
    const feed = (a: number, b: number) => ({ labels, values: { section: (k: number) => (k === 0 ? a : b), basins: {} } });
    const r = render(<WebMap {...props({ waterLine: { sections: [501, 502] }, standing: feed(31, 12) })} />);
    await settle();
    await waitFor(() => expect(log).toContain("data water-copy [501,502]"));
    expect(log.filter((x) => x.startsWith("set water-copy")).sort()).toEqual([
      'set water-copy 501 {"standing":31}', 'set water-copy 502 {"standing":31}']);
    log.length = 0;
    // a new feed: the engine relabels the loaded sections — the creek's copy with them, the river only on the atlas
    r.rerender(<WebMap {...props({ waterLine: { sections: [501, 502] }, standing: feed(64, 7) })} />);
    await settle();
    expect(log.filter((x) => x.includes(" 900 ") || x.startsWith("set water-copy")).sort()).toEqual([
      'set atlas 900 {"standing":7}', 'set water-copy 501 {"standing":64}', 'set water-copy 502 {"standing":64}']);
  });

  it("another water: the creek's copy and states are dropped at once, before the new water is kept", async () => {
    cam = { lng: -121.6, lat: 49.1, zoom: 12 };
    const r = render(<WebMap {...props({ highlight: [501], waterLine: { sections: [501, 502] } })} />);
    await settle();
    await waitFor(() => expect(log).toContain("data water-copy [501,502]"));
    log.length = 0;
    r.rerender(<WebMap {...props({ highlight: [900], waterLine: { sections: [900] } })} />);
    await settle();
    const after = log.filter((x) => x.startsWith("data water-copy") || x.startsWith("remove water-copy"));
    // (the old selection is cleared through the mirror first — the highlight changes before the copy's key does)
    expect(after).toEqual(["remove water-copy 501 selected", "remove water-copy undefined *", "data water-copy []",
                           "data water-copy [900]"]);
  });

  it("THE WATER decides when to stop: a lit route down the river (in the tiles at z8) does not stop it short of the creek", async () => {
    render(<WebMap {...props({ highlight: [900], waterLine: { sections: [501, 502], at: { lat: 49.205, lon: -121.495 } } })} />);
    await settle();
    await waitFor(() => expect(log.filter((x) => x.startsWith("data water-copy")).at(-1)).toBe("data water-copy [501,502,900]"));
    expect(log.filter((x) => x.startsWith("ease"))).toEqual(["ease z9", "ease z10", "ease z11", "ease z12"]);
  });

  it("the reader's own pan or zoom stops the stepping in", async () => {
    ready = false;                                   // still loading: the first settle is the `idle` below
    render(<WebMap {...props({ waterLine: { sections: [501, 502] } })} />);
    await act(async () => { await Promise.resolve(); });
    act(() => gl!.fire("movestart", { originalEvent: {} }));
    ready = true;
    act(() => gl!.fire("idle"));
    await settle();
    expect(log.filter((x) => x.startsWith("ease") || x.startsWith("data water-copy"))).toEqual([]);
  });

  it("a map that only LIGHTS a water (no waterLine) keeps its copy but never moves the camera", async () => {
    render(<WebMap {...props({ highlight: [501, 502] })} />);
    await settle();
    expect(log.filter((x) => x.startsWith("ease") || x.startsWith("data water-copy"))).toEqual([]);
    cam = { lng: -121.6, lat: 49.1, zoom: 12 };
    act(() => gl!.fire("idle"));
    await settle();
    expect(log.filter((x) => x.startsWith("data water-copy"))).toEqual(["data water-copy [501,502]"]);
  });
});
