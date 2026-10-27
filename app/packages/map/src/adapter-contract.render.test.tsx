/**
 * N2 — ONE CONTRACT, BOTH MAPS. The same op script (mount with status data and a selection, change the selection, tap,
 * settle) is run against `Map.web.tsx` over a recording maplibre-gl and against `Map.native.tsx` over a recording
 * `@maplibre/maplibre-react-native`, each mocked AT ITS BOUNDARY (the SDK module), and the two command logs must be
 * the same strings: which feature-states were set and removed on which source-layer with which id (and its TYPE), which
 * boxes were queried, and what the app was told a tap and the screen hold.
 *
 * The native fake answers like Android does (N1): ids are STRINGS, a feature with no id is "", and the press event's
 * `nativeEvent` is gone once the handler yields — so reading it after an `await` fails here as it does on a phone.
 *
 * Not proven here: the renderers themselves (no WebGL, no MapLibre Native). That is the emulator run (n2/README.md).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useEffect, useImperativeHandle, type ReactNode } from "react";
import { MAP_GESTURES } from "@app/core";
import type { MapProps } from "./map-props";
import { maplibreGestures, nativeMapGestures } from "./gestures";

/** Every SDK call that matters to the contract, in order, as one string each. */
const log: string[] = [];
/** The log without the water copy's mirrored writes (T175): the atlas's own commands. */
const atlasLog = () => log.filter((x) => !x.includes("water-copy"));
const fmt = (id: unknown) => `${typeof id}:${String(id)}`;
/** What a query answers: by the layers it asked for, and whether the asker is the phone (string ids) or the web. */
let answer: (layers: readonly string[], phone: boolean) => { id?: unknown }[] = () => [];

// ---- maplibre-gl, recorded -------------------------------------------------------------------------------------------
type Handler = (e?: unknown) => void;
const web: { map: FakeGl | null; opts: Record<string, unknown> | null; rotationOff: boolean;
             keyboardRotationOff: boolean; tilesIn: boolean; fitOpts: unknown[]; canvasH: number } =
  { map: null, opts: null, rotationOff: false, keyboardRotationOff: false, tilesIn: true, fitOpts: [], canvasH: 800 };
class FakeGl {
  private on_ = new Map<string, Set<Handler>>();
  private style: { layers: { id: string; source?: string }[]; sources: Record<string, unknown> };
  touchZoomRotate = { disableRotation: () => { web.rotationOff = true; } };
  keyboard = { disableRotation: () => { web.keyboardRotationOff = true; } };
  constructor(opts: Record<string, unknown>) {
    web.opts = opts; web.map = this;
    this.style = opts.style as FakeGl["style"];
    // the style loads after the constructor returns, as in GL JS
    queueMicrotask(() => this.fire("load"));
  }
  on(ev: string, fn: Handler) { (this.on_.get(ev) ?? this.on_.set(ev, new Set()).get(ev)!).add(fn); return this; }
  once(ev: string, fn: Handler) { return this.on(ev, fn); }
  off(ev: string, fn: Handler) { this.on_.get(ev)?.delete(fn); return this; }
  fire(ev: string, e?: unknown) { for (const fn of [...(this.on_.get(ev) ?? [])]) fn(e); }
  // GL JS counts the SOURCES too: false through every tile burst, on a map that loaded long ago
  isStyleLoaded() { return web.tilesIn; }
  getStyle() { return this.style; }
  getLayer(id: string) { return this.style.layers.find((l) => l.id === id); }
  getSource() { return { setData: () => {} }; }
  setPaintProperty() {} setLayoutProperty() {} setLayerZoomRange() {} setFilter() {}
  addControl() {} hasImage() { return false; } addImage() {} removeImage() {} remove() {}
  getContainer() { return { clientWidth: 400, clientHeight: 800 }; }
  /** The canvas as last sized: a test sets `web.canvasH` to leave it stale (its box changed, the observer not yet run). */
  getCanvas() { return { style: { width: "400px", height: `${web.canvasH}px` } }; }
  resize() { if (web.canvasH !== 800) log.push(`resize ${web.canvasH}->800`); web.canvasH = 800; }
  getCenter() { return { lng: -122, lat: 49 }; }
  /** Screen x: a degree of longitude east of -122.2 is 1000 px (so -121.83788 sits at 362 of the 400 px map). */
  project(l: { lng: number; lat: number }) { return { x: Math.round((l.lng + 122.2) * 1000), y: 400 }; }
  /** Its inverse, and a degree of latitude north of 49 is 1000 px up from y 400. */
  unproject([x, y]: [number, number]) { return { lng: x / 1000 - 122.2, lat: 49 + (400 - y) / 1000 }; }
  /** the style and every tile in (T175's water copy asks a settled map at once) */
  loaded() { return true; }
  getZoom() { return 9; }
  getBounds() { return { getWest: () => -123, getSouth: () => 48, getEast: () => -121, getNorth: () => 50 }; }
  querySourceFeatures() { return []; }
  fitBounds(b: unknown, o?: unknown) { log.push(`fit ${JSON.stringify(b)}`); web.fitOpts.push(o); }
  easeTo(o: { center: [number, number]; zoom: number; duration: number }) {
    log.push(`ease ${JSON.stringify(o.center)} z${o.zoom} ${o.duration}ms`);
  }
  setFeatureState(t: { source: string; sourceLayer: string; id: unknown }, s: unknown) {
    log.push(`set ${t.source}/${t.sourceLayer} ${fmt(t.id)} ${JSON.stringify(s)}`);
  }
  removeFeatureState(t: { source: string; sourceLayer: string; id?: unknown }, key?: string) {
    log.push(`remove ${t.source}/${t.sourceLayer} ${fmt(t.id)} ${key ?? "*"}`);
  }
  queryRenderedFeatures(a?: unknown, b?: { layers?: string[] }) {
    const box = Array.isArray(a) ? a : null;
    const layers = (box ? b?.layers : (a as { layers?: string[] } | undefined)?.layers) ?? [];
    log.push(`query ${box ? JSON.stringify(box) : "viewport"} ${layers.join(",")}`);
    return answer(layers, false);
  }
}
class Control { constructor(_o?: unknown) {} }
class Marker {
  /** A custom element (the boundary marks, P15) is put in the document, as GL JS puts it over the map. */
  private el: HTMLElement | null;
  private at: { lng: number; lat: number } = { lng: 0, lat: 0 };
  constructor(o?: { element?: HTMLElement }) {
    this.el = o?.element ?? null;
    this.el?.classList.add("maplibregl-marker");          // as GL JS does: its stylesheet then places it absolutely
  }
  setLngLat(l: [number, number]) { this.at = { lng: l[0], lat: l[1] }; return this; } setPopup() { return this; }
  getLngLat() { return this.at; }
  getElement() { return this.el!; }
  addTo() { if (this.el) document.body.appendChild(this.el); return this; }
  remove() { this.el?.remove(); }
}
vi.mock("maplibre-gl", () => ({ Map: FakeGl, NavigationControl: Control, ScaleControl: Control, Marker,
                                Popup: class { setText() { return this; } }, addProtocol: () => {} }));
vi.mock("pmtiles", () => ({ Protocol: class { tile = () => {}; } }));

// ---- @maplibre/maplibre-react-native, recorded ----------------------------------------------------------------------
type Press = { nativeEvent: { point: [number, number]; lngLat: [number, number] } | null };
const native: { props: Record<string, unknown> | null; camera: Record<string, unknown> | null;
                geojson: Record<string, unknown>; holdRender: boolean } =
  { props: null, camera: null, geojson: {}, holdRender: false };
vi.mock("@maplibre/maplibre-react-native", () => ({
  Map: ({ ref, children, ...props }: { ref?: unknown; children?: ReactNode } & Record<string, unknown>) => {
    native.props = props;
    useImperativeHandle(ref as never, () => ({
      queryRenderedFeatures: async (a?: unknown, b?: { layers?: string[] }) => {
        const box = Array.isArray(a) ? a : null;
        const layers = (box ? b?.layers : (a as { layers?: string[] } | undefined)?.layers) ?? [];
        log.push(`query ${box ? JSON.stringify(box) : "viewport"} ${layers.join(",")}`);
        return answer(layers, true);
      },
      getBounds: async () => [-123, 48, -121, 50],
    }));
    // a style finishes loading after the view mounts, and again after every style swap, as on a phone
    const loaded = props.onDidFinishLoadingStyle as (() => void) | undefined;
    const drawn = props.onDidFinishRenderingMapFully as (() => void) | undefined;
    useEffect(() => { loaded?.(); if (!native.holdRender) drawn?.(); }, [props.mapStyle]);
    return <>{children}</>;
  },
  Camera: ({ ref, ...props }: { ref?: unknown } & Record<string, unknown>) => {
    native.camera = props;
    useImperativeHandle(ref as never, () => ({
      fitBounds: (b: unknown) => log.push(`fit ${JSON.stringify(b)}`),
      easeTo: (o: { center: [number, number]; zoom: number; duration: number }) =>
        log.push(`ease ${JSON.stringify(o.center)} z${o.zoom} ${o.duration}ms`),
    }));
    return null;
  },
  VectorSource: ({ id, ref }: { id: string; ref?: unknown }) => {
    useImperativeHandle(ref as never, () => ({
      setFeatureState: async (t: { id: unknown; sourceLayer: string }, s: unknown) => {
        log.push(`set ${id}/${t.sourceLayer} ${fmt(t.id)} ${JSON.stringify(s)}`);
      },
      removeFeatureState: async (t: { id?: unknown; sourceLayer: string }, key?: string) => {
        log.push(`remove ${id}/${t.sourceLayer} ${fmt(t.id)} ${key ?? "*"}`);
      },
      querySourceFeatures: async () => [],
    }));
    return null;
  },
  GeoJSONSource: ({ id, data }: { id: string; data: unknown }) => { native.geojson[id] = data; return null; },
  // a view annotation draws its children over the map
  Marker: ({ children }: { children?: ReactNode }) => <>{children}</>,
}));

const { Map: WebMap } = await import("./Map.web");
const { Map: NativeMap } = await import("./Map.native");

afterEach(() => {
  cleanup();
  log.length = 0;
  answer = () => [];
  web.map = null; web.opts = null; web.rotationOff = false; web.keyboardRotationOff = false; web.tilesIn = true;
  web.fitOpts = []; web.canvasH = 800;
  native.props = null; native.camera = null; native.geojson = {}; native.holdRender = false;
});

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
/** One object for the whole test: a NEW data object is new data, and both maps re-apply it (as they should). */
const DATA = { stream: { 311274: { status: "open" } }, lake: { 1566515: { status: "closed" } } };
/**
 * What the WEB map may be pushed: never a section's status, standing, stocked or survey — those are the colouring
 * engine's channels there (one pathway, owner 2026-10-21; `withoutEngineKeys` refuses them). Any other key carries the
 * same contract through the same `data` path (`other` here: no colour mode reads it).
 */
const WEB_DATA = { stream: { 311274: { other: 12 } }, lake: { 1566515: { other: "sheet" } } };
const INITIAL = { lon: -122, lat: 49, zoom: 9 };
const LIT = [311274];
const base = (p: Partial<MapProps> = {}): MapProps => ({
  at: AT, theme: "light", view: "plain", initial: INITIAL, data: DATA, highlight: LIT, ...p,
});

/** The two maps, driven the same way: mount, re-render, tap at a point, settle. */
const PLATFORMS = {
  web: {
    Map: WebMap,
    data: WEB_DATA,
    pushed: ['set atlas/stream number:311274 {"other":12}', 'set atlas/lake number:1566515 {"other":"sheet"}'],
    tap: (x: number, y: number, lon: number, lat: number) => {
      act(() => web.map!.fire("click", { point: { x, y }, lngLat: { lng: lon, lat } }));
    },
    settle: () => { act(() => web.map!.fire("idle")); },
    // the browser's own long press (Chrome on Android) or a right click: `contextmenu` at the point
    hold: (x: number, y: number, _lon: number, _lat: number) => {
      act(() => web.map!.fire("contextmenu", { point: { x, y }, originalEvent: { preventDefault: () => {} } }));
    },
  },
  native: {
    Map: NativeMap,
    data: DATA,
    pushed: ['set atlas/stream number:311274 {"status":"open"}', 'set atlas/lake number:1566515 {"status":"closed"}'],
    tap: (x: number, y: number, lon: number, lat: number) => {
      const e: Press = { nativeEvent: { point: [x, y], lngLat: [lon, lat] } };
      act(() => { (native.props!.onPress as (e: Press) => void)(e); });
      e.nativeEvent = null;              // React Native recycles the event once the handler yields (N1 §3)
    },
    // MapLibre Native's own long press, recycled once the handler yields like a tap
    hold: (x: number, y: number, lon: number, lat: number) => {
      const e: Press = { nativeEvent: { point: [x, y], lngLat: [lon, lat] } };
      act(() => { (native.props!.onLongPress as (e: Press) => void)(e); });
      e.nativeEvent = null;
    },
    // a camera that stopped, then the frame with the tiles in: the order a phone reports them
    settle: () => {
      act(() => {
        (native.props!.onRegionDidChange as (e: unknown) => void)(
          { nativeEvent: { center: [-122, 49], zoom: 9, bearing: 0, pitch: 0, bounds: [-123, 48, -121, 50],
                           animated: false, userInteraction: true } });
        (native.props!.onDidFinishRenderingMapFully as () => void)();
      });
    },
  },
} as const;

/**
 * The selection, once per SOURCE-layer (lake_flowing draws the lake features, wetland_flowing the wetland ones, and a
 * value a feature holds is not sent twice: `onceSink`).
 */
const selected = (id: number, on: boolean) => ["wetland", "lake", "stream"]
  .map((sl) => (on ? `set atlas/${sl} number:${id} {"selected":true}` : `remove atlas/${sl} number:${id} selected`));

describe.each(Object.entries(PLATFORMS))("the map adapter contract: %s", (_name, P) => {
  it("pushed colours and the selection go to feature-state by the section's NUMBER id, on its source-layer", async () => {
    const r = render(<P.Map {...base({ data: P.data })} />);
    // on mount the pushed data and the selection are independent effects: which lands first is the renderer's business
    // (the water copy's own writes — T175, `water-copy.render.test.tsx` — are not the atlas's contract)
    await waitFor(() => expect(atlasLog().sort()).toEqual([...P.pushed, ...selected(311274, true)].sort()));
    log.length = 0;
    r.rerender(<P.Map {...base({ data: P.data, highlight: [1566515] })} />);
    // the old selection's `selected` key cleared (only that key, per feature: the status colours stay), then the new
    // one set
    await waitFor(() => expect(atlasLog()).toEqual([...selected(311274, false), ...selected(1566515, true)]));
  });

  it("P15: the rest of the water is `water`, set and cleared with the selection", async () => {
    const water = (id: number, on: boolean) => ["wetland", "lake", "stream"]
      .map((sl) => (on ? `set atlas/${sl} number:${id} {"water":true}` : `remove atlas/${sl} number:${id} water`));
    const r = render(<P.Map {...base({ data: undefined, water: [1235781] })} />);
    await waitFor(() => expect(atlasLog().sort()).toEqual([...selected(311274, true), ...water(1235781, true)].sort()));
    log.length = 0;
    r.rerender(<P.Map {...base({ data: undefined, highlight: [1235781], water: [311274] })} />);
    // every key of the last highlight cleared, per feature (a key a feature does not hold is not sent), then the new
    await waitFor(() => expect(atlasLog().sort()).toEqual([
      ...selected(311274, false), ...water(1235781, false), ...selected(1235781, true), ...water(311274, true)].sort()));
  });

  it("P15: a boundary mark is a finger-sized button with its label, and a tap names it", async () => {
    const press = vi.fn();
    render(<P.Map {...base({ data: undefined, onPressMark: press, marks: [
      { lon: -121.83788, lat: 49.071839, label: "Tamihi Rapids Bridge", a11y: "Boundary: Tamihi Rapids Bridge", on: false },
      { lon: -121.710609, lat: 49.078392, label: null, a11y: "Boundary: Slesse", on: true }] })} />);
    const b = await screen.findByLabelText("Boundary: Tamihi Rapids Bridge");
    expect([b.textContent, screen.getByLabelText("Boundary: Slesse").textContent]).toEqual(["Tamihi Rapids Bridge", ""]);
    fireEvent.click(screen.getByLabelText("Boundary: Slesse"));
    expect(press.mock.calls).toEqual([[1]]);
  });

  it("a tap asks a 24 px box, streams first, and answers the section as a number (Android sends a string)", async () => {
    const press = vi.fn();
    const point = vi.fn();
    answer = (layers, phone) => (layers.includes("stream") ? [{ id: "" }, { id: phone ? "311274" : 311274 }] : []);
    render(<P.Map {...base({ data: undefined, highlight: undefined, onPressFeature: press, onMapPoint: point })} />);
    await waitFor(() => expect(native.props ?? web.map).toBeTruthy());
    log.length = 0;
    P.tap(100, 200, -122.3, 49.1);
    await waitFor(() => expect(press.mock.calls).toEqual([["stream", 311274, 49.1, -122.3]]));
    expect({ log, point: point.mock.calls }).toEqual({
      log: ["query [[88,188],[112,212]] stream,lake_flowing,wetland_flowing"],
      point: [[49.1, -122.3]],
    });
  });

  it("no stream under the finger: still water next; nothing at all: no answer", async () => {
    const press = vi.fn();
    answer = (layers, phone) => (layers.includes("lake") ? [{ id: phone ? "1566515" : 1566515 }] : []);
    render(<P.Map {...base({ data: undefined, highlight: undefined, onPressFeature: press })} />);
    await waitFor(() => expect(native.props ?? web.map).toBeTruthy());
    log.length = 0;
    P.tap(50, 60, -121.9, 49.4);
    await waitFor(() => expect(press.mock.calls).toEqual([["lake", 1566515, 49.4, -121.9]]));
    expect(log).toEqual(["query [[38,48],[62,72]] stream,lake_flowing,wetland_flowing",
                         "query [[38,48],[62,72]] lake,wetland"]);
    answer = () => [{ id: "" }, {}];
    P.tap(10, 10, -121, 49);
    await waitFor(() => expect(log.length).toBe(4));
    await act(async () => {});
    expect(press.mock.calls.length).toBe(1);
  });

  it("a long press answers WHERE, once, and asks the map nothing (owner 2026-10-21: a spot is dropped there)", async () => {
    const held = vi.fn();
    const press = vi.fn();
    render(<P.Map {...base({ data: undefined, highlight: undefined, onLongPress: held, onPressFeature: press })} />);
    await waitFor(() => expect(native.props ?? web.map).toBeTruthy());
    log.length = 0;
    P.hold(362, 300, -121.838, 49.1);
    await act(async () => {});
    expect({ held: held.mock.calls.map(([lat, lon]) => [lat.toFixed(4), lon.toFixed(4)]), log, press: press.mock.calls })
      .toEqual({ held: [["49.1000", "-121.8380"]], log: [], press: [] });
  });

  it("a long press still answers on a map rebuilt for new tile endpoints (the release's tiles replacing the dev ones)", async () => {
    const held = vi.fn();
    const r = render(<P.Map {...base({ data: undefined, highlight: undefined, onLongPress: held })} />);
    await waitFor(() => expect(native.props ?? web.map).toBeTruthy());
    const first = web.map;
    r.rerender(<P.Map {...base({ data: undefined, highlight: undefined, onLongPress: held,
                                 at: { ...AT, atlas: `${AT.atlas}?release` } })} />);
    await waitFor(() => expect(native.props !== null || (web.map !== null && web.map !== first)).toBe(true));
    P.hold(362, 300, -121.838, 49.1);
    await act(async () => {});
    expect(held.mock.calls.map(([lat, lon]) => [lat.toFixed(4), lon.toFixed(4)])).toEqual([["49.1000", "-121.8380"]]);
  });

  it("what is on screen: the visible water's sections, as numbers, once each", async () => {
    const seen = vi.fn();
    answer = (layers, phone) => (layers.length === 4
      ? [{ id: phone ? "311274" : 311274 }, { id: phone ? "1566515" : 1566515 }, { id: phone ? "311274" : 311274 },
         { id: "" }]
      : []);
    render(<P.Map {...base({ data: undefined, highlight: undefined, onVisible: seen })} />);
    await waitFor(() => expect(native.props ?? web.map).toBeTruthy());
    P.settle();
    await waitFor(() => expect(seen).toHaveBeenLastCalledWith([311274, 1566515]));
    expect(log.filter((l) => l.startsWith("query"))[0]).toBe("query viewport stream,lake,lake_flowing,wetland_flowing");
  });
});

describe("web only", () => {
  it("a finger held still 500 ms drops it; its click is not a tap; a pan or a second finger drops nothing", async () => {
    vi.useFakeTimers();
    try {
      const held = vi.fn();
      const tapped = vi.fn();
      render(<WebMap {...base({ data: undefined, highlight: undefined, onLongPress: held, onMapPoint: tapped })} />);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      const touch = (ev: string, x: number, y: number, n = 1) =>
        act(() => web.map!.fire(ev, { point: { x, y }, originalEvent: { touches: { length: n } } }));
      touch("touchstart", 362, 400);
      touch("touchmove", 366, 403);                    // 5 px: still a press
      act(() => { vi.advanceTimersByTime(500); });
      touch("touchend", 366, 403);
      act(() => web.map!.fire("click", { point: { x: 366, y: 403 }, lngLat: { lng: -121.834, lat: 48.997 } }));
      touch("touchstart", 100, 400);
      touch("touchmove", 110, 400);                    // a pan
      act(() => { vi.advanceTimersByTime(800); });
      touch("touchstart", 100, 400);
      touch("touchmove", 100, 400, 2);                 // a pinch
      act(() => { vi.advanceTimersByTime(800); });
      act(() => web.map!.fire("click", { point: { x: 100, y: 400 }, lngLat: { lng: -122.1, lat: 49 } }));
      expect({ held: held.mock.calls.map(([lat, lon]) => [lat.toFixed(3), lon.toFixed(3)]), tapped: tapped.mock.calls })
        .toEqual({ held: [["49.000", "-121.838"]], tapped: [[49, -122.1]] });
    } finally {
      vi.useRealTimers();
    }
  });

  it("the lift that ends a long press is cancelled, so the browser makes no click of it on the sheet now under the " +
     "finger; a tap's lift is left alone", async () => {
    vi.useFakeTimers();
    try {
      render(<WebMap {...base({ data: undefined, highlight: undefined, onLongPress: () => {} })} />);
      await act(async () => { await vi.advanceTimersByTimeAsync(0); });
      const lifts: string[] = [];
      const press = (ms: number) => {
        act(() => web.map!.fire("touchstart", { point: { x: 200, y: 400 }, originalEvent: { touches: { length: 1 } } }));
        act(() => { vi.advanceTimersByTime(ms); });
        act(() => web.map!.fire("touchend", { point: { x: 200, y: 400 },
                                              originalEvent: { touches: { length: 0 },
                                                               preventDefault: () => lifts.push(`${ms} ms`) } }));
      };
      press(120);
      press(600);
      press(120);
      expect(lifts).toEqual(["600 ms"]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("SATELLITE ONLINE-ONLY: an imagery tile's failure goes to onImageryError with its status, not to onError; " +
     "any other source's error still goes to onError", async () => {
    const imagery: (number | undefined)[] = [];
    const errors: string[] = [];
    render(<WebMap {...base({ onImageryError: (s) => imagery.push(s), onError: (e) => errors.push(e.message) })} />);
    await act(async () => { await Promise.resolve(); });
    errors.length = 0;          // the fake map's own look-pass complaints are not this test's subject
    act(() => {
      web.map!.fire("error", { sourceId: "imagery", error: Object.assign(new Error("Failed to fetch"), {}) });
      web.map!.fire("error", { sourceId: "imagery", error: Object.assign(new Error("Forbidden"), { status: 403 }) });
      web.map!.fire("error", { sourceId: "atlas", error: new Error("atlas broke") });
    });
    expect({ imagery, errors }).toEqual({ imagery: [undefined, 403], errors: ["atlas broke"] });
  });

  it("pushed data that arrives mid tile burst is applied (no `load` fires again on a zoom out)", async () => {
    const r = render(<WebMap {...base({ highlight: undefined, data: WEB_DATA })} />);
    await waitFor(() => expect(log.length).toBe(2));
    log.length = 0;
    web.tilesIn = false;                  // zoomed out: a long tile burst, and no `load` will ever fire again
    r.rerender(<WebMap {...base({ highlight: undefined,
                                  data: { ...WEB_DATA, lake: { 1566515: { other: "digitised" } } } })} />);
    await waitFor(() => expect(log).toEqual(['set atlas/lake number:1566515 {"other":"digitised"}']));
  });

  it("ONE PATHWAY: a pushed status, standing or lake fact is REFUSED on the web and reported; the rest lands", async () => {
    const errors: string[] = [];
    render(<WebMap {...base({ highlight: undefined, onError: (e) => errors.push(e.message), data: {
      stream: { 311274: { status: "open", other: 3 } },
      lake: { 1566515: { standing: 40 }, 1566516: { stocked: "recent", survey: "sheet" } },
      basin: { CHWK: { standing: 5 } } } })} />);
    await waitFor(() => expect(log).toEqual(['set atlas/stream number:311274 {"other":3}']));
    await act(async () => {});
    expect(log).toEqual(['set atlas/stream number:311274 {"other":3}']);
    const why = "the web map colours sections through the colouring engine only "
      + "(MapProps.status, MapProps.standing, MapProps.lakes, MapProps.salmon)";
    expect(errors).toEqual([`pushed p/standing/status/stocked/survey/sp/salmon on "stream" refused: ${why}`,
                            `pushed p/standing/status/stocked/survey/sp/salmon on "lake" refused: ${why}`,
                            `pushed p/standing/status/stocked/survey/sp/salmon on "basin" refused: ${why}`]);
  });

  it("P15 review: a mark's element is left to GL JS's absolute placement — nothing inline stacks it in normal flow", async () => {
    // maplibre-gl.css: `.maplibregl-marker{position:absolute;top:0;left:0}`; GL JS then only sets a translate. An inline
    // `position` on the element beats the class, so the marks stacked 44 px apart (a mark drawn 44 px × its index low).
    const css = document.createElement("style");
    css.textContent = ".maplibregl-marker{position:absolute;top:0;left:0}";
    document.head.appendChild(css);
    try {
      render(<WebMap {...base({ data: undefined, marks: [
        { lon: -121.83788, lat: 49.071839, label: "Tamihi Rapids Bridge", a11y: "Boundary: Tamihi Rapids Bridge", on: false },
        { lon: -121.70, lat: 49.02, label: "Slesse Creek", a11y: "Boundary: Slesse Creek", on: true }] })} />);
      await waitFor(() => expect(document.querySelectorAll(".maplibregl-marker").length).toBe(2));
      for (const root of document.querySelectorAll<HTMLElement>(".maplibregl-marker")) {
        expect(root.style.position).toBe("");
        expect(getComputedStyle(root).position).toBe("absolute");
      }
      // the label still sits over its own ring: its containing block is the 44 px button inside the root
      const b = screen.getByLabelText("Boundary: Slesse Creek");
      expect([b.style.position, b.querySelector("span")!.style.position]).toEqual(["relative", "absolute"]);
    } finally { css.remove(); }
  });

  it("P15: a mark's label near the map's edge is moved in, and again when the map moves", async () => {
    render(<WebMap {...base({ data: undefined, marks: [
      { lon: -121.83788, lat: 49.071839, label: "Tamihi Rapids Bridge", a11y: "Boundary: Tamihi Rapids Bridge", on: false }] })} />);
    const span = () => screen.getByLabelText("Boundary: Tamihi Rapids Bridge").querySelector("span")!;
    await waitFor(() => expect(span().style.transform).toBe("translateX(calc(-50% + -62px))"));    // x 362 of 400
    const project = web.map!.project.bind(web.map);
    web.map!.project = (l) => ({ ...project(l), x: 30 });
    act(() => web.map!.fire("move"));
    expect(span().style.transform).toBe("translateX(calc(-50% + 70px))");
  });
});

describe("overzoom: the camera past the tiles, every source at its own deepest zoom", () => {
  it("web: maplibre-gl is constructed with the camera to z18 and each source stopping where its archive stops", () => {
    render(<WebMap {...base()} />);
    const o = web.opts!;
    const sources = (o.style as { sources: Record<string, { maxzoom?: number }> }).sources;
    expect({ minZoom: o.minZoom, maxZoom: o.maxZoom, maxBounds: o.maxBounds,
             atlas: sources.atlas?.maxzoom, basemap: sources.basemap?.maxzoom, imagery: sources.imagery?.maxzoom })
      .toEqual({ minZoom: 4, maxZoom: 18, maxBounds: [[-147.1, 40.2], [-106.1, 68]],
                 // Esri World Imagery's own z19 (owner 2026-10-21): the camera never outruns it; holes 404 to the parent
                 atlas: 13, basemap: 14, imagery: 19 });
  });
  it("web: a fit goes no closer than the atlas's z14, though the camera may", async () => {
    render(<WebMap {...base({ fit: { key: "pond", bbox: [-121.9502, 49.1999, -121.9498, 49.2001] } })} />);
    await waitFor(() => expect(web.fitOpts.length).toBe(1));
    expect(web.fitOpts).toEqual([{ padding: 48, maxZoom: 14, duration: 450 }]);
  });
  it("native: a fit to a pond is widened to what z14 shows — the binding's fitBounds has no maxZoom", async () => {
    render(<NativeMap {...base({ fit: { key: "pond", bbox: [-121.9502, 49.1999, -121.9498, 49.2001] } })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("fit"))).toBe(true));
    const fits = log.filter((l) => l.startsWith("fit"))
      .map((l) => (JSON.parse(l.slice(4)) as number[]).map((v) => Math.round(v * 1e4) / 1e4));
    // a 400 x 800 phone less 48 px padding: 304 x 704 px of z14 (0.0130° of longitude, 0.0198° of latitude at 49.2°N),
    // centred on the pond
    expect(fits).toEqual([[-121.9565, 49.1901, -121.9435, 49.2099]]);
  });
  it("(T48) a fit to a CAMERA puts it back exactly (search's cancel), the same command on both maps, once per key", async () => {
    const fit = { key: "restore:1", bbox: null, camera: { lon: -121.85, lat: 49.15, zoom: 9.4 } };
    const w = render(<WebMap {...base({ fit })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("ease"))).toBe(true));
    w.rerender(<WebMap {...base({ fit: { ...fit } })} />);
    const onWeb = log.filter((l) => l.startsWith("ease") || l.startsWith("fit"));
    cleanup();
    log.length = 0;
    const n = render(<NativeMap {...base({ fit })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("ease"))).toBe(true));
    n.rerender(<NativeMap {...base({ fit: { ...fit } })} />);
    const onPhone = log.filter((l) => l.startsWith("ease") || l.startsWith("fit"));
    expect({ onWeb, onPhone }).toEqual({ onWeb: ["ease [-121.85,49.15] z9.4 450ms"],
                                         onPhone: ["ease [-121.85,49.15] z9.4 450ms"] });
  });
  it("(T48) web: a camera put back measures its box FIRST — eased on a stale canvas, it lands half the change off", async () => {
    // Search hides the legend strip, so the map is taller while it is open; closing brings the strip back. GL JS's
    // easeTo pins the centre to the canvas's middle AS IT WAS when the ease began, so easing before the canvas follows
    // its (now shorter) box put the reader's map back ~33 px low — and each search drifted it further.
    web.canvasH = 866;
    render(<WebMap {...base({ fit: { key: "restore:1", bbox: null, camera: { lon: -121.85, lat: 49.15, zoom: 9.4 } } })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("ease"))).toBe(true));
    expect(log.filter((l) => l.startsWith("ease") || l.startsWith("resize")))
      .toEqual(["resize 866->800", "ease [-121.85,49.15] z9.4 450ms"]);
  });
});

describe("the gesture policy reaches each renderer", () => {
  it("web: maplibre-gl is constructed with it, and pinch- and keyboard-rotation are turned off after", () => {
    render(<WebMap {...base()} />);
    const o = web.opts!;
    const got = Object.fromEntries(Object.keys(maplibreGestures()).map((k) => [k, o[k]]));
    expect({ got, rotationOff: web.rotationOff, keyboardRotationOff: web.keyboardRotationOff })
      .toEqual({ got: maplibreGestures(), rotationOff: true, keyboardRotationOff: true });
  });
  it("native: the binding's switches are the policy's — no rotate, no tilt, no compass", async () => {
    render(<NativeMap {...base()} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    const p = native.props!;
    expect(Object.fromEntries(Object.keys(nativeMapGestures()).map((k) => [k, p[k]])))
      .toEqual({ dragPan: true, touchZoom: true, doubleTapZoom: true, doubleTapHoldZoom: true, touchRotate: false,
                 touchPitch: false, compass: false });
    expect(nativeMapGestures()).toEqual(Object.fromEntries(Object.keys(nativeMapGestures()).map((k) => [k, p[k]])));
    expect(MAP_GESTURES.rotate || MAP_GESTURES.tilt).toBe(false);
  });
});

describe("native only", () => {
it("no listener, no long press: the binding is handed no handler (its long press stays the SDK's no-op)", async () => {
  render(<NativeMap {...base({ data: undefined, highlight: undefined })} />);
  await waitFor(() => expect(native.props).toBeTruthy());
  expect(native.props!.onLongPress).toBeUndefined();
});

  it("(T145 review) a tap in a spot's 44 px target is the spot's alone: the map neither reports the point (it would " +
     "clear the selection the tap makes) nor opens the water under it; just outside, both as ever", async () => {
    const press = vi.fn();
    const point = vi.fn();
    answer = (layers) => (layers.includes("stream") ? [{ id: "311274" }] : []);
    const z = 14, east = (px: number) => -122 + (px / (512 * 2 ** z)) * 360;
    render(<NativeMap {...base({ data: undefined, highlight: undefined, initial: { lon: -122, lat: 49, zoom: z },
                                 onPressFeature: press, onMapPoint: point,
                                 spots: { points: [{ id: "s1", lat: 49, lon: -122, title: "Tamihi run" }],
                                          selected: "s1", onOpen: () => {}, onList: () => {},
                                          face: () => null } })} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    log.length = 0;
    PLATFORMS.native.tap(200, 400, -122, 49);          // on the mark
    PLATFORMS.native.tap(220, 400, east(20), 49);      // in its target (half 22)
    await act(async () => {});
    const onSpot = { point: point.mock.calls.length, press: press.mock.calls.length, queries: log.length };
    PLATFORMS.native.tap(230, 400, east(30), 49);      // off it
    await waitFor(() => expect(press.mock.calls.length).toBe(1));
    expect([onSpot, point.mock.calls]).toEqual([{ point: 0, press: 0, queries: 0 }, [[49, east(30)]]]);
  });

  it("is handed the style with nothing promoted, and a camera held to the province's zooms and bounds", async () => {
    render(<NativeMap {...base()} />);
    await waitFor(() => expect(native.camera).toBeTruthy());
    const style = native.props!.mapStyle as { sources: { atlas: Record<string, unknown> } };
    expect({ promote: "promoteId" in style.sources.atlas, url: style.sources.atlas.url, camera: native.camera })
      .toEqual({
        promote: false, url: "pmtiles://file:///d/atlas.pmtiles",
        // OVERZOOM: the camera to z18, four past the tiles (`CAMERA_MAX_ZOOM`)
        camera: { initialViewState: { center: [-122, 49], zoom: 9 }, minZoom: 4, maxZoom: 18,
                  maxBounds: [-147.1, 40.2, -106.1, 68] },
      });
  });
  it("a style swap (a theme change) drops feature-state on a phone: status and selection are put back", async () => {
    const r = render(<NativeMap {...base()} />);
    const status = ['set atlas/stream number:311274 {"status":"open"}',
                    'set atlas/lake number:1566515 {"status":"closed"}'];
    await waitFor(() => expect(log).toEqual([...status, ...selected(311274, true)]));
    const style1 = native.props!.mapStyle;
    log.length = 0;
    r.rerender(<NativeMap {...base({ theme: "dark" })} />);
    // nothing to remove: the swap took every state with it (`forget`), so the selection is simply set again
    await waitFor(() => expect(log).toEqual([...status, ...selected(311274, true)]));
    expect(native.props!.mapStyle).not.toBe(style1);
  });
  it("queries nothing until the map has drawn (Android SIGSEGVs in MapRenderer::actor() before that)", async () => {
    native.holdRender = true;
    const press = vi.fn();
    const seen = vi.fn();
    answer = () => [{ id: "311274" }];
    render(<NativeMap {...base({ data: undefined, highlight: undefined, onPressFeature: press, onVisible: seen })} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    PLATFORMS.native.tap(100, 200, -122.3, 49.1);
    act(() => {
      (native.props!.onRegionDidChange as (e: unknown) => void)({ nativeEvent: { center: [-122, 49], zoom: 9 } });
    });
    await act(async () => {});
    expect({ queries: log.filter((l) => l.startsWith("query")), press: press.mock.calls.length,
             seen: seen.mock.calls.length }).toEqual({ queries: [], press: 0, seen: 0 });
    // the first full render opens the gate, and reports what is on screen
    act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });
    await waitFor(() => expect(seen.mock.calls).toEqual([[[311274]]]));
  });
  it("sends no feature-state until the map has drawn (Android SIGSEGVs in MapRenderer::actor() from setFeatureState)", async () => {
    native.holdRender = true;
    render(<NativeMap {...base()} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    await act(async () => {});
    expect(log).toEqual([]);
    act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });
    await waitFor(() => expect([...log].sort()).toEqual([
      'set atlas/stream number:311274 {"status":"open"}', 'set atlas/lake number:1566515 {"status":"closed"}',
      ...selected(311274, true)].sort()));
  });
  it("asks what is on screen once per camera move: our own feature-state redraw asks nothing", async () => {
    const seen = vi.fn();
    answer = () => [{ id: "5" }, { id: "6" }];
    render(<NativeMap {...base({ data: undefined, highlight: undefined, onVisible: seen })} />);
    await waitFor(() => expect(seen.mock.calls).toEqual([[[5, 6]]]));
    const queries = () => log.filter((l) => l.startsWith("query viewport")).length;
    const before = queries();
    // the map redraws (status colours landed): no camera moved, so no query and no report
    act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });
    await act(async () => {});
    expect(queries()).toBe(before);
    answer = () => [{ id: "7" }];
    PLATFORMS.native.settle();
    await waitFor(() => expect(seen.mock.calls).toEqual([[[5, 6]], [[7]]]));
    expect(queries()).toBe(before + 1);
  });
  it("a status value already on a feature is not sent again (one bridge call per feature, per value)", async () => {
    const r = render(<NativeMap {...base({ highlight: undefined })} />);
    await waitFor(() => expect(log.filter((l) => l.startsWith("set")).length).toBe(2));
    log.length = 0;
    // the same statuses, as a new object (every settle re-sends the visible set), plus one new section
    r.rerender(<NativeMap {...base({ highlight: undefined,
                                     data: { ...DATA, stream: { ...DATA.stream, 42: { status: "open" } } } })} />);
    await waitFor(() => expect(log).toEqual(['set atlas/stream number:42 {"status":"open"}']));
  });
  it("the basin field (keyed by basin_id, no MVT id) goes into the style, never to feature-state", async () => {
    const basin = { "B-1": { standing: 30 } };
    const r = render(<NativeMap {...base({ view: "conditions", modes: { stream: "standing", basin: "standing" },
                                           highlight: undefined, data: { basin } })} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    await act(async () => {});
    const fill = () => JSON.stringify((native.props!.mapStyle as { layers: { id: string; paint?: unknown }[] })
      .layers.find((l) => l.id === "basin")!.paint);
    expect(fill()).toContain('["literal",{"B-1":30}]');
    expect(log.filter((l) => l.includes("basin"))).toEqual([]);
    r.rerender(<NativeMap {...base({ view: "conditions", modes: { stream: "standing", basin: "standing" },
                                     highlight: undefined, data: { basin: { "B-1": { standing: 70 } } } })} />);
    await waitFor(() => expect(fill()).toContain('["literal",{"B-1":70}]'));
  });
  it("draws the web's scale bar — metric, bottom-right — not the binding's (top-left, device units on Android)", async () => {
    const r = render(<NativeMap {...base()} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    // the binding's own is ON but parked off screen: off, the map left a black hole over the next screen (Android)
    expect([native.props!.scaleBar, native.props!.scaleBarPosition]).toEqual([true, { top: -10000, left: -10000 }]);
    // z9 at 49°N: 92 px ≈ 9.2 km → "5 km" (`metricScale`), and it follows the camera
    expect(r.getByLabelText("scale").textContent).toBe("5 km");
    act(() => {
      (native.props!.onRegionDidChange as (e: unknown) => void)({ nativeEvent: { center: [-122, 49], zoom: 14 } });
    });
    expect(r.getByLabelText("scale").textContent).toBe("200 m");
    r.rerender(<NativeMap {...base({ bare: true })} />);
    expect(r.queryByLabelText("scale")).toBeNull();
  });
  it("reports where the camera settled, read off the event before anything else", async () => {
    const moved = vi.fn();
    render(<NativeMap {...base({ data: undefined, highlight: undefined, onMoved: moved })} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    PLATFORMS.native.settle();
    expect(moved.mock.calls).toEqual([[{ lon: -122, lat: 49, zoom: 9 }]]);
  });
  it("frames a subject: a fit moves the camera to the box, once per key", async () => {
    const fit = { key: "a", bbox: [-123, 49, -122, 50] as const };
    const r = render(<NativeMap {...base({ data: undefined, highlight: undefined, fit })} />);
    await waitFor(() => expect(log.filter((l) => l.startsWith("fit"))).toEqual(["fit [-123,49,-122,50]"]));
    r.rerender(<NativeMap {...base({ data: undefined, highlight: undefined, fit: { ...fit } })} />);
    await act(async () => {});
    expect(log.filter((l) => l.startsWith("fit"))).toEqual(["fit [-123,49,-122,50]"]);
  });
  it("new gauge readings go to the style's own gauges source, not a style swap", async () => {
    const g1 = '{"type":"FeatureCollection","features":[]}';
    const g2 = '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-122,49]},"properties":{"label":"1 m³/s"}}]}';
    const r = render(<NativeMap {...base({ gauges: g1 })} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    const style1 = native.props!.mapStyle;
    r.rerender(<NativeMap {...base({ gauges: g2 })} />);
    await act(async () => {});
    expect({ same: native.props!.mapStyle === style1, data: native.geojson.gauges })
      .toEqual({ same: true, data: JSON.parse(g2) });
  });
});
