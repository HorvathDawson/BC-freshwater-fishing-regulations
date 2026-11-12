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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useEffect, useImperativeHandle, type ReactNode } from "react";
import { MAP_GESTURES, setFailureSink } from "@app/core";
import type { MapProps } from "./map-props";
import type { StatusIndex } from "@app/core";
import { maplibreGestures, nativeMapGestures } from "./gestures";
import { colour } from "./chrome";
import { MARK_LABEL, MARK_RING, MARK_SOURCE, PIN_DOT, PIN_SOURCE } from "./point-marks";

/** Every SDK call that matters to the contract, in order, as one string each. */
const log: string[] = [];
/** The log without the water copy's mirrored writes (T175): the atlas's own commands. */
const atlasLog = () => log.filter((x) => !x.includes("water-copy"));
const fmt = (id: unknown) => `${typeof id}:${String(id)}`;
/** What a query answers: by the layers it asked for, and whether the asker is the phone (string ids) or the web. */
let answer: (layers: readonly string[], phone: boolean) => { id?: unknown; properties?: unknown }[] = () => [];

// ---- maplibre-gl, recorded -------------------------------------------------------------------------------------------
type Handler = (e?: unknown) => void;
const web: { map: FakeGl | null; opts: Record<string, unknown> | null; rotationOff: boolean;
             keyboardRotationOff: boolean; tilesIn: boolean; fitOpts: unknown[]; canvasH: number;
             /** (T317) what each runtime GeoJSON source was last given (`addSource` or `setData`) */
             data: Record<string, unknown> } =
  { map: null, opts: null, rotationOff: false, keyboardRotationOff: false, tilesIn: true, fitOpts: [], canvasH: 800, data: {} };
/** (T674) where the fake web camera is: a test moves it, then fires `moveend`. */
const webCam = { lng: -122, lat: 49 };
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
  getSource(id: string) { return { setData: (d: unknown) => { web.data[id] = d; } }; }
  /** (T317) the point marks' runtime sources and layers, over the style's */
  addSource(id: string, s: { data: unknown }) { web.data[id] = s.data; this.style.sources[id] = s; }
  addLayer(l: { id: string; source?: string }) { this.style.layers.push(l); }
  removeLayer(id: string) { this.style.layers = this.style.layers.filter((l) => l.id !== id); }
  setPaintProperty() {} setLayoutProperty() {} setLayerZoomRange() {} setFilter() {}
  addControl() {} hasImage() { return false; } addImage() {} removeImage() {} remove() {}
  getContainer() { return { clientWidth: 400, clientHeight: 800 }; }
  /** the element whose cursor the hover sets (GL JS's canvas container) */
  container = { style: { cursor: "" } };
  getCanvasContainer() { return this.container; }
  isMoving() { return false; }
  /** The canvas as last sized: a test sets `web.canvasH` to leave it stale (its box changed, the observer not yet run). */
  getCanvas() { return { style: { width: "400px", height: `${web.canvasH}px` } }; }
  resize() { if (web.canvasH !== 800) log.push(`resize ${web.canvasH}->800`); web.canvasH = 800; }
  getCenter() { return { ...webCam }; }
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
                geojson: Record<string, unknown>; holdRender: boolean; watched?: string[];
                layers?: Record<string, { type: string; paint?: unknown }>;
                images?: Record<string, unknown>; fitOpts?: unknown[];
                onLog?: (e: { level: string; tag: string; message: string }) => boolean } =
  { props: null, camera: null, geojson: {}, holdRender: false };
vi.mock("@maplibre/maplibre-react-native", () => ({
  // (T418) MapLibre Native's log: the map installs one handler (the style-asset watch)
  LogManager: { onLog: (h: (e: { level: string; tag: string; message: string }) => boolean) => { native.onLog = h; } },
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
      fitBounds: (b: unknown, o?: unknown) => { (native.fitOpts ??= []).push(o); log.push(`fit ${JSON.stringify(b)}`); },
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
      // (T74) the patched binding's per-tile path
      setFeatureStates: async (sourceLayer: string, ids: string[], states: unknown[]) => {
        ids.forEach((fid, i) => log.push(`set ${id}/${sourceLayer} ${fid} ${JSON.stringify(states[i])}`));
      },
      watchTiles: async (layers: string[]) => { native.watched = layers; },
      nativeTag: () => 7,
    }));
    return null;
  },
  Images: ({ images }: { images: Record<string, unknown> }) => { native.images = images; return null; },
  GeoJSONSource: ({ id, data, children }: { id: string; data: unknown; children?: ReactNode }) => {
    native.geojson[id] = data;
    return <>{children}</>;
  },
  // (T303) a style layer the map adds over the style (the location dot's), recorded by id
  Layer: ({ id, type, paint }: { id: string; type: string; paint?: unknown }) => {
    (native.layers ??= {})[id] = { type, paint };
    return null;
  },
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
  web.fitOpts = []; web.canvasH = 800; web.data = {}; webCam.lng = -122; webCam.lat = 49;
  native.props = null; native.camera = null; native.geojson = {}; native.holdRender = false; native.layers = {}; native.fitOpts = [];
});

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
/** One object for the whole test: a NEW data object is new data, and both maps re-apply it (as they should). */
/**
 * What a map may be pushed: never a section's status, standing, stocked or survey — those are the colouring engine's
 * channels on BOTH maps (one pathway, owner 2026-10-21; T74 for the phone; `withoutEngineKeys` refuses them). Any
 * other key carries the same contract through the same `data` path (`other` here: no colour mode reads it).
 */
const DATA = { stream: { 311274: { other: 12 } }, lake: { 1566515: { other: "sheet" } } };
const WEB_DATA = DATA;
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
    pushed: ['set atlas/stream number:311274 {"other":12}', 'set atlas/lake number:1566515 {"other":"sheet"}'],
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
const unset = (sl: string, id: number, key: string) => (platform === "native"
  // (T345) a phone unsets a key by writing it null (`adapters/native.ts` `raw.remove`: MapLibre Native's removals)
  ? `set atlas/${sl} number:${id} {"${key}":null}` : `remove atlas/${sl} number:${id} ${key}`);
let platform = "";
const selected = (id: number, on: boolean) => ["wetland", "lake", "stream"]
  .map((sl) => (on ? `set atlas/${sl} number:${id} {"selected":true}` : unset(sl, id, "selected")));

describe.each(Object.entries(PLATFORMS))("the map adapter contract: %s", (_name, P) => {
  beforeEach(() => { platform = _name; });
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
      .map((sl) => (on ? `set atlas/${sl} number:${id} {"water":true}` : unset(sl, id, "water")));
    const r = render(<P.Map {...base({ data: undefined, water: [1235781] })} />);
    await waitFor(() => expect(atlasLog().sort()).toEqual([...selected(311274, true), ...water(1235781, true)].sort()));
    log.length = 0;
    r.rerender(<P.Map {...base({ data: undefined, highlight: [1235781], water: [311274] })} />);
    // every key of the last highlight cleared, per feature (a key a feature does not hold is not sent), then the new
    await waitFor(() => expect(atlasLog().sort()).toEqual([
      ...selected(311274, false), ...water(1235781, false), ...selected(1235781, true), ...water(311274, true)].sort()));
  });

  it("(T499) the same highlight handed again in NEW arrays (a caller's re-render) writes nothing: no frame unlit", async () => {
    const r = render(<P.Map {...base({ data: undefined, highlight: [311274], water: [1235781], near: [1566515], selection: "dim" })} />);
    await waitFor(() => expect(atlasLog().some((x) => x.includes('{"part":true}'))).toBe(true));
    await act(async () => { await new Promise((d) => setTimeout(d, 50)); });
    log.length = 0;
    for (let i = 0; i < 3; i++)
      r.rerender(<P.Map {...base({ data: undefined, highlight: [311274], water: [1235781], near: [1566515], selection: "dim" })} />);
    await act(async () => { await new Promise((d) => setTimeout(d, 50)); });
    // before T499 each re-render cleared every lit key (null writes on a phone) and set it again: the Fraser's blink
    expect(atlasLog()).toEqual([]);
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

  it("a mouse over what a click would open shows the hand, asked by the click's own hit test; elsewhere the grab", async () => {
  const press = vi.fn();
  answer = (layers) => (layers.includes("stream") ? [{ id: 311274 }] : []);
  render(<WebMap {...base({ data: undefined, highlight: undefined, onPressFeature: press })} />);
  await waitFor(() => expect(web.map).toBeTruthy());
  const hover = (x: number, y: number, buttons = 0) =>
    act(() => web.map!.fire("mousemove", { point: { x, y }, lngLat: { lng: -122, lat: 49 }, originalEvent: { buttons } }));
  log.length = 0;
  hover(100, 200);
  await waitFor(() => expect(web.map!.container.style.cursor).toBe("pointer"));
  expect(log).toContain("query [[88,188],[112,212]] stream,lake_flowing,wetland_flowing");
  answer = () => [];
  hover(10, 10);
  await waitFor(() => expect(web.map!.container.style.cursor).toBe(""));
  // a drag (a button held) asks nothing; the map leaving the mouse clears it
  answer = (layers) => (layers.includes("stream") ? [{ id: 311274 }] : []);
  log.length = 0;
  hover(100, 200, 1);
  await act(async () => { await new Promise((r) => setTimeout(r, 40)); });
  expect([log, web.map!.container.style.cursor]).toEqual([[], ""]);
  expect(press).not.toHaveBeenCalled();
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

  it("(T345) a highlight moved mid tile burst is applied: a step's camera fly must not leave the old part lit", async () => {
    const r = render(<WebMap {...base({ data: undefined, highlight: [311274], water: [1235781] })} />);
    await waitFor(() => expect(log.length).toBeGreaterThan(0));
    log.length = 0;
    web.tilesIn = false;                  // ▲ / ▼: the camera flies to the new part, its tiles load, no `styledata` follows
    r.rerender(<WebMap {...base({ data: undefined, highlight: [1235781], water: [311274] })} />);
    await waitFor(() => expect(log.filter((l) => l.startsWith("set atlas/stream")).sort()).toEqual([
      'set atlas/stream number:1235781 {"selected":true}', 'set atlas/stream number:311274 {"water":true}']));
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
    const why = "the map colours sections through the colouring engine only "
      + "(MapProps.status, MapProps.standing, MapProps.lakes, MapProps.salmon, MapProps.method, MapProps.runs)";
    expect(errors).toEqual([`pushed p/standing/status/stocked/survey/sp/salmon/gm/rt on "stream" refused: ${why}`,
                            `pushed p/standing/status/stocked/survey/sp/salmon/gm/rt on "lake" refused: ${why}`,
                            `pushed p/standing/status/stocked/survey/sp/salmon/gm/rt on "basin" refused: ${why}`]);
  });

  it("T317: the marks are map layers — a ring each, a label each labelled one — and no DOM marker moves with the map", async () => {
    render(<WebMap {...base({ data: undefined, marks: [
      { lon: -121.83788, lat: 49.071839, label: "Tamihi Rapids Bridge", a11y: "Boundary: Tamihi Rapids Bridge", on: false },
      { lon: -121.70, lat: 49.02, label: null, a11y: "Boundary: Slesse Creek", on: true }] })} />);
    await waitFor(() => expect(web.map!.getLayer(MARK_LABEL)).toBeTruthy());
    expect([web.map!.getLayer(MARK_RING)?.source, web.map!.getLayer(MARK_LABEL)?.source]).toEqual([MARK_SOURCE, MARK_SOURCE]);
    const fc = web.data[MARK_SOURCE] as { features: { properties: Record<string, unknown> }[] };
    expect(fc.features.map((f) => f.properties)).toEqual([
      { i: 0, kind: "ring", on: false }, { i: 0, kind: "label", label: "Tamihi Rapids Bridge" }, { i: 1, kind: "ring", on: true }]);
    expect(document.querySelectorAll(".maplibregl-marker").length).toBe(0);
  });

  it("P15/T317: a mark's label near the map's edge is moved in where the camera settles — never per frame", async () => {
    render(<WebMap {...base({ data: undefined, marks: [
      { lon: -121.83788, lat: 49.071839, label: "Tamihi Rapids Bridge", a11y: "Boundary: Tamihi Rapids Bridge", on: false }] })} />);
    type Fc = { features: { properties: { kind: string }; geometry: { coordinates: number[] } }[] };
    const labelAt = () => (web.data[MARK_SOURCE] as Fc | undefined)?.features.find((f) => f.properties.kind === "label")
      ?.geometry.coordinates;
    // x 362 of 400: 62 px in, the label's point at x 300 (`unproject`: lng = x / 1000 - 122.2)
    await waitFor(() => expect(labelAt()?.[0]).toBeCloseTo(-121.9, 6));
    const project = web.map!.project.bind(web.map);
    web.map!.project = (l) => ({ ...project(l), x: 30 });
    act(() => web.map!.fire("move"));
    expect(labelAt()?.[0]).toBeCloseTo(-121.9, 6);
    act(() => web.map!.fire("moveend"));
    await waitFor(() => expect(labelAt()?.[0]).toBeCloseTo(-122.1, 6));      // 70 px the other way: x 100
  });

  it("T317: the marker and a panel's pins are one GeoJSON source with the pin image per tone, not DOM markers", async () => {
    render(<WebMap {...base({ data: undefined, marker: { lat: 49.1, lon: -122.1 },
                             pins: [{ lat: 49.2, lon: -122.2, tone: colour("light", "color.ui.spot"), title: "Hope" }] })} />);
    await waitFor(() => expect(web.data[PIN_SOURCE]).toBeTruthy());
    const fc = web.data[PIN_SOURCE] as { features: { properties: Record<string, unknown> }[] };
    expect(fc.features.map((f) => [f.properties.kind, f.properties.tone, f.properties.img])).toEqual([
      ["pin", colour("light", "color.ui.spot"), `t317-pin-${colour("light", "color.ui.spot").slice(1)}`], ["marker", colour("light", "color.ui.accent"), `t317-pin-${colour("light", "color.ui.accent").slice(1)}`]]);
    expect(document.querySelectorAll(".maplibregl-marker").length).toBe(0);
  });
});

describe.each(Object.entries(PLATFORMS))("T317 point marks: %s", (_name, P) => {
  it("a tap within 22 px of a mark is the mark's (its layers asked), and opens no water under it", async () => {
    const press = vi.fn();
    const feature = vi.fn();
    answer = (layers) => (layers.includes(MARK_RING) ? [{ properties: { i: 1 } }] : [{ id: 311274 }]);
    render(<P.Map {...base({ data: undefined, highlight: undefined, onPressMark: press, onPressFeature: feature, marks: [
      { lon: -121.83788, lat: 49.071839, label: "Tamihi Rapids Bridge", a11y: "Boundary: Tamihi Rapids Bridge", on: false },
      { lon: -121.710609, lat: 49.078392, label: null, a11y: "Boundary: Slesse", on: true }] })} />);
    await waitFor(() => expect(native.props ?? web.map).toBeTruthy());
    if (_name === "web") await waitFor(() => expect(web.map!.getLayer(MARK_RING)).toBeTruthy());
    log.length = 0;
    P.tap(100, 200, -122.3, 49.1);
    await waitFor(() => expect(press.mock.calls).toEqual([[1]]));
    await act(async () => {});
    expect(feature.mock.calls).toEqual([]);
    expect(log).toContain(`query [[78,178],[122,222]] ${MARK_RING},${MARK_LABEL}`);
  });
});

describe("native only (T317)", () => {
  it("the marker, pins and marks are GeoJSON sources with layers, not view annotations", async () => {
    render(<NativeMap {...base({ data: undefined, marker: { lat: 49.1, lon: -122.1 }, pins: [{ lat: 49.2, lon: -122.2 }],
                                marks: [{ lon: -121.8, lat: 49.07, label: "Bridge", a11y: "Boundary: Bridge", on: true }] })} />);
    await waitFor(() => expect(native.layers?.[MARK_LABEL]).toBeTruthy());
    expect(Object.keys(native.layers!).filter((k) => k.startsWith("t317-")).sort())
      .toEqual([MARK_LABEL, MARK_RING, PIN_DOT].sort());
    const pins = native.geojson[PIN_SOURCE] as { features: { properties: { kind: string } }[] };
    const marks = native.geojson[MARK_SOURCE] as { features: { properties: { kind: string } }[] };
    expect([pins.features.map((f) => f.properties.kind), marks.features.map((f) => f.properties.kind)])
      .toEqual([["pin", "marker"], ["ring", "label"]]);
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
                 atlas: 13, basemap: 13, imagery: 19 });
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
  it("(T400) native: a fit EASES — the binding's fitBounds defaults to a 'fly' arc, out and back in, in 450 ms", async () => {
    render(<NativeMap {...base({ fit: { key: "pond", bbox: [-121.9502, 49.1999, -121.9498, 49.2001] } })} />);
    await waitFor(() => expect(native.fitOpts?.length).toBe(1));
    expect(native.fitOpts).toEqual([{ padding: { top: 48, right: 48, bottom: 48, left: 48 }, duration: 450, easing: "ease" }]);
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
it("(T621, G15) zoomControls puts + / − on a bare phone map, one level a press; without it, none", async () => {
  const { unmount } = render(<NativeMap {...base({ data: undefined, highlight: undefined, bare: true })} />);
  await waitFor(() => expect(native.props).toBeTruthy());
  expect(screen.queryByLabelText("Zoom in")).toBeNull();
  unmount();
  render(<NativeMap {...base({ data: undefined, highlight: undefined, bare: true, zoomControls: true })} />);
  log.length = 0;
  fireEvent.click(await screen.findByLabelText("Zoom in"));
  fireEvent.click(screen.getByLabelText("Zoom out"));
  expect(log).toEqual(["ease [-122,49] z10 250ms", "ease [-122,49] z8 250ms"]);
});

it("(T621, G19) a tap on a titled pin shows its words, as the web's popup does; another tap puts them away", async () => {
  answer = (layers) => (layers.includes("t317-pins-dot") ? [{ properties: { title: "Upstream gauge", i: "0" } }] : []);
  render(<NativeMap {...base({ data: undefined, highlight: undefined, onPressFeature: vi.fn(),
                               pins: [{ lat: 49.1, lon: -122.3, title: "Upstream gauge" }] })} />);
  await waitFor(() => expect(native.props).toBeTruthy());
  await act(async () => { (native.props!.onDidFinishRenderingMapFully as (() => void) | undefined)?.(); });
  PLATFORMS.native.tap(100, 200, -122.3, 49.1);
  expect(await screen.findByText("Upstream gauge")).toBeTruthy();
  answer = () => [];
  PLATFORMS.native.tap(300, 500, -122.0, 49.0);
  await waitFor(() => expect(screen.queryByText("Upstream gauge")).toBeNull());
});

it("no listener, no long press: the binding is handed no handler (its long press stays the SDK's no-op)", async () => {
  render(<NativeMap {...base({ data: undefined, highlight: undefined })} />);
  await waitFor(() => expect(native.props).toBeTruthy());
  expect(native.props!.onLongPress).toBeUndefined();
});

  it("(T145 review) a tap in a spot's 44 px target is the spot's alone: the map neither reports the point (it would " +
     "clear the selection the tap makes) nor opens the water under it — (T316) it opens the spot; just outside, both " +
     "as ever", async () => {
    const press = vi.fn();
    const open = vi.fn();
    const point = vi.fn();
    answer = (layers) => (layers.includes("stream") ? [{ id: "311274" }] : []);
    const z = 14, east = (px: number) => -122 + (px / (512 * 2 ** z)) * 360;
    render(<NativeMap {...base({ data: undefined, highlight: undefined, initial: { lon: -122, lat: 49, zoom: z },
                                 onPressFeature: press, onMapPoint: point,
                                 spots: { points: [{ id: "s1", lat: 49, lon: -122, title: "Tamihi run" }],
                                          selected: "s1", onOpen: open, onList: () => {},
                                          colours: { mark: colour("light", "color.ui.spot"), ground: colour("light", "color.ui.spot-ground") } } })} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    log.length = 0;
    PLATFORMS.native.tap(200, 400, -122, 49);          // on the mark
    PLATFORMS.native.tap(220, 400, east(20), 49);      // in its target (half 22)
    await act(async () => {});
    const onSpot = { point: point.mock.calls.length, press: press.mock.calls.length, queries: log.length };
    PLATFORMS.native.tap(230, 400, east(30), 49);      // off it
    await waitFor(() => expect(press.mock.calls.length).toBe(1));
    expect([onSpot, point.mock.calls, open.mock.calls]).toEqual([{ point: 0, press: 0, queries: 0 }, [[49, east(30)]],
                                                                 [["s1"], ["s1"]]]);
  });

  it("is handed the style with nothing promoted, and a camera held to the province's zooms and bounds", async () => {
    render(<NativeMap {...base()} />);
    await waitFor(() => expect(native.camera).toBeTruthy());
    const style = native.props!.mapStyle as { sources: { atlas: Record<string, unknown> } };
    expect({ promote: "promoteId" in style.sources.atlas, url: style.sources.atlas.url, camera: native.camera })
      .toEqual({
        promote: false, url: "pmtiles://file:///d/atlas.pmtiles",
        // OVERZOOM: the camera to z18, four past the tiles (`CAMERA_MAX_ZOOM`)
        // (T303) and never MapLibre's own location tracking: following is ours (the camera to each fix)
        camera: { initialViewState: { center: [-122, 49], zoom: 9 }, minZoom: 4, maxZoom: 18,
                  maxBounds: [-147.1, 40.2, -106.1, 68] },
      });
  });
  it("(T303) draws your location itself from the fix, follows each new fix, and a hand pan stops following", async () => {
    const onRelease = vi.fn();
    const me = (p: Partial<NonNullable<MapProps["me"]>> = {}) =>
      ({ on: true, fix: { lat: 49.1, lon: -121.95, accuracy: 20 }, following: false, centre: 1, onRelease, ...p });
    const r = render(<NativeMap {...base({ me: me() })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("ease [-121.95,49.1]"))).toBe(true));
    const pt = JSON.parse(native.geojson.me as string);
    expect(pt.features.map((f: { geometry: unknown }) => f.geometry))
      .toEqual([{ type: "Point", coordinates: [-121.95, 49.1] }]);
    // the circle under the dot, both in the theme's location blue; the circle's radius grows with the zoom
    expect(Object.keys(native.layers!).filter((k) => k.startsWith("me-"))).toEqual(["me-accuracy", "me-dot"]);
    const acc = native.layers!["me-accuracy"]!.paint as Record<string, unknown>;
    const dot = native.layers!["me-dot"]!.paint as Record<string, unknown>;
    expect([acc["circle-color"], dot["circle-color"], (acc["circle-radius"] as unknown[]).slice(0, 3)])
      .toEqual([colour("light", "color.ui.me"), colour("light", "color.ui.me"), ["interpolate", ["exponential", 2], ["zoom"]]]);
    // not following: a new fix moves the dot, not the map
    log.length = 0;
    r.rerender(<NativeMap {...base({ me: me({ fix: { lat: 49.2, lon: -121.9 } }) })} />);
    await act(async () => {});
    expect(log.filter((l) => l.startsWith("ease"))).toEqual([]);
    expect(JSON.parse(native.geojson.me as string).features[0].geometry.coordinates).toEqual([-121.9, 49.2]);
    // following: each new fix brings the camera
    r.rerender(<NativeMap {...base({ me: me({ fix: { lat: 49.2, lon: -121.9 }, following: true, centre: 2 }) })} />);
    r.rerender(<NativeMap {...base({ me: me({ fix: { lat: 49.3, lon: -121.8 }, following: true, centre: 2 }) })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("ease [-121.8,49.3]"))).toBe(true));
    // our own ease is not the reader's: following holds; a hand pan releases it
    const will = native.props!.onRegionWillChange as (e: unknown) => void;
    act(() => will({ nativeEvent: { userInteraction: false } }));
    expect(onRelease).not.toHaveBeenCalled();
    act(() => will({ nativeEvent: { userInteraction: true } }));
    expect(onRelease).toHaveBeenCalledTimes(1);
    // off: no dot
    r.rerender(<NativeMap {...base({ me: me({ on: false }) })} />);
    await act(async () => {});
    expect(JSON.parse(native.geojson.me as string).features).toEqual([]);
  });
  it("a style swap (a theme change) drops feature-state on a phone: status and selection are put back", async () => {
    const r = render(<NativeMap {...base()} />);
    const status = ['set atlas/stream number:311274 {"other":12}',
                    'set atlas/lake number:1566515 {"other":"sheet"}'];
    await waitFor(() => expect(log).toEqual([...status, ...selected(311274, true)]));
    const style1 = native.props!.mapStyle;
    log.length = 0;
    r.rerender(<NativeMap {...base({ theme: "dark" })} />);
    // nothing to remove: the swap took every state with it (`forget`), so the selection is simply set again
    await waitFor(() => expect(log).toEqual([...status, ...selected(311274, true)]));
    expect(native.props!.mapStyle).not.toBe(style1);
  });
  it("(T508) says it is drawn whole only after a full render that follows the lit sections written AND the fit's camera " +
     "at rest — never at its first draw (a held pane is shown on this word)", async () => {
    native.holdRender = true;
    const drawn = vi.fn();
    const full = () => act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });
    const rest = () => act(() => {
      (native.props!.onRegionDidChange as (e: unknown) => void)({ nativeEvent: { center: [-122, 49], zoom: 12 } });
    });
    const fit = { key: "part", bbox: [-122.1, 48.9, -121.9, 49.1] as const };
    let t = 1_800_000_000_000;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => t);
    const r = render(<NativeMap {...base({ fit, onDrawn: drawn })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("fit "))).toBe(true));
    full();                                            // the first draw: the gate opens, the selection is written after it
    await waitFor(() => expect(log.some((l) => l.includes('"selected"'))).toBe(true));
    expect(drawn).not.toHaveBeenCalled();             // the fit is still on its way
    rest();                                            // (emulator) the camera it STARTS from, reported as the fit is sent
    full();                                            // a full render mid-flight: still not whole
    expect(drawn).not.toHaveBeenCalled();
    t += 500;                                          // the glide's time is up
    rest();                                            // the camera arrives…
    expect(drawn).not.toHaveBeenCalled();
    full();                                            // …and the map draws what is there: whole
    expect(drawn).toHaveBeenCalledTimes(1);
    // a new part to light: not whole again until it is written and drawn
    drawn.mockClear();
    r.rerender(<NativeMap {...base({ fit, onDrawn: drawn, highlight: [5] })} />);
    await act(async () => {});
    expect(drawn).not.toHaveBeenCalled();
    full();
    expect(drawn).toHaveBeenCalledTimes(1);
    // a listener that arrives after the map was drawn whole hears it at once
    const late = vi.fn();
    r.rerender(<NativeMap {...base({ fit, onDrawn: late, highlight: [5] })} />);
    await act(async () => {});
    expect(late).toHaveBeenCalledTimes(1);
    clock.mockRestore();
  });
  it("(T508, emulator) no second 'map fully rendered' after the fit's glide: a camera at rest after a frame with every " +
     "tile in is whole; the frames are heard only while someone waits", async () => {
    native.holdRender = true;
    const drawn = vi.fn();
    const fit = { key: "part", bbox: [-122.1, 48.9, -121.9, 49.1] as const };
    let t = 1_800_000_000_000;
    const clock = vi.spyOn(Date, "now").mockImplementation(() => t);
    const r = render(<NativeMap {...base({ fit, onDrawn: drawn })} />);
    await waitFor(() => expect(log.some((l) => l.startsWith("fit "))).toBe(true));
    act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });   // the one "fully rendered"
    await waitFor(() => expect(log.some((l) => l.includes('"selected"'))).toBe(true));
    const frame = (full: boolean) => act(() => {
      ((full ? native.props!.onDidFinishRenderingFrameFully : native.props!.onDidFinishRenderingFrame) as () => void)();
    });
    frame(true);                                       // mid-glide, every tile in: not at rest
    frame(false);                                      // the last frame before rest still loading
    t += 500;
    act(() => { (native.props!.onRegionDidChange as (e: unknown) => void)({ nativeEvent: { center: [-122, 49], zoom: 12 } }); });
    expect(drawn).not.toHaveBeenCalled();
    frame(true);                                       // the tiles in, at rest: whole
    expect(drawn).toHaveBeenCalledTimes(1);
    // nobody waiting: the frames are not sent across the bridge
    r.rerender(<NativeMap {...base({ fit })} />);
    await act(async () => {});
    expect(native.props!.onDidFinishRenderingFrameFully).toBeUndefined();
    clock.mockRestore();
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
      'set atlas/stream number:311274 {"other":12}', 'set atlas/lake number:1566515 {"other":"sheet"}',
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
  it("(T348) a settle with no full render after it still asks, once: the legend's count follows a pan over held tiles", async () => {
    const seen = vi.fn();
    answer = () => [{ id: "5" }];
    render(<NativeMap {...base({ data: undefined, highlight: undefined, onVisible: seen })} />);
    await waitFor(() => expect(seen.mock.calls).toEqual([[[5]]]));
    const queries = () => log.filter((l) => l.startsWith("query viewport")).length;
    const before = queries();
    answer = () => [{ id: "8" }];
    vi.useFakeTimers();
    try {
      act(() => {
        (native.props!.onRegionDidChange as (e: unknown) => void)({ nativeEvent: { center: [-121, 50], zoom: 10, userInteraction: true } });
      });
      expect(queries()).toBe(before);                          // not at once: a full render may yet come
      await act(async () => { vi.advanceTimersByTime(800); });
    } finally { vi.useRealTimers(); }
    await waitFor(() => expect(seen.mock.calls).toEqual([[[5]], [[8]]]));
    // the full render that follows asks nothing more: still one query for the move
    act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });
    await act(async () => {});
    expect(queries()).toBe(before + 1);
  });
  it("a status value already on a feature is not sent again (one bridge call per feature, per value)", async () => {
    const r = render(<NativeMap {...base({ highlight: undefined })} />);
    await waitFor(() => expect(log.filter((l) => l.startsWith("set")).length).toBe(2));
    log.length = 0;
    // the same statuses, as a new object (every settle re-sends the visible set), plus one new section
    r.rerender(<NativeMap {...base({ highlight: undefined,
                                     data: { ...DATA, stream: { ...DATA.stream, 42: { other: 1 } } } })} />);
    await waitFor(() => expect(log).toEqual(['set atlas/stream number:42 {"other":1}']));
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
  it("draws the web's scale bar — metric, bottom-LEFT — not the binding's (top-left, device units on Android), and the credits bottom-right (T424)", async () => {
    const r = render(<NativeMap {...base()} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    // (T424) the binding's own attribution is off: the credits are the web's component, a chip bottom-right
    expect(native.props!.attribution).toBe(false);
    const scale = getComputedStyle(r.getByLabelText("scale")), chip = getComputedStyle(r.getByTestId("map-credits"));
    expect([scale.left, scale.right, chip.right, chip.left]).toEqual(["10px", "auto", "8px", "auto"]);
    expect(r.getByTestId("map-credits-chip").textContent).toBe("©");
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
    expect(r.queryByTestId("map-credits")).toBeNull();
  });
  it("(T418) a style asset that would not load (MapLibre's log) is REPORTED once per asset, and still logged", async () => {
    const sent: unknown[] = [];
    const undo = setFailureSink((e) => sent.push(e));
    try {
      render(<NativeMap {...base({ data: undefined, highlight: undefined })} />);
      await waitFor(() => expect(native.onLog).toBeTruthy());
      // the 0.5.0 phone's lines (2026-11-04 17:07), twice over: one report per asset
      const lines = ["{anifishthis.app}[Style]: Failed to load source outside: ", "{anifishthis.app}[Style]: Failed to load sprite: ",
                     "{RenderThread 16}[Style]: Failed to load glyph range 0-255 for font stack Noto Sans Regular:( )"];
      for (const message of [...lines, ...lines])
        expect(native.onLog!({ level: "error", tag: "Mbgl", message })).toBe(false);   // printed as before
      native.onLog!({ level: "warn", tag: "Mbgl", message: "Failed to load sprite: " });  // a warning: not this
      expect(sent.map((e) => (e as Error).message)).toEqual([
        `map: a style asset failed to load (source:outside): ${lines[0]!.trim()}`,
        `map: a style asset failed to load (sprite): ${lines[1]!.trim()}`,
        `map: a style asset failed to load (glyphs:Noto Sans Regular): ${lines[2]!.trim()}`,
      ]);
    } finally { undo(); }
  });
  it("reports where the camera settled, read off the event before anything else", async () => {
    const moved = vi.fn();
    render(<NativeMap {...base({ data: undefined, highlight: undefined, onMoved: moved })} />);
    await waitFor(() => expect(native.props).toBeTruthy());
    PLATFORMS.native.settle();
    expect(moved.mock.calls).toEqual([[{ lon: -122, lat: 49, zoom: 9, bounds: [-123, 48, -121, 50] }]]);
  });
  it("frames a subject: a fit moves the camera to the box, once per key", async () => {
    const fit = { key: "a", bbox: [-123, 49, -122, 50] as const };
    const r = render(<NativeMap {...base({ data: undefined, highlight: undefined, fit })} />);
    await waitFor(() => expect(log.filter((l) => l.startsWith("fit"))).toEqual(["fit [-123,49,-122,50]"]));
    r.rerender(<NativeMap {...base({ data: undefined, highlight: undefined, fit: { ...fit } })} />);
    await act(async () => {});
    expect(log.filter((l) => l.startsWith("fit"))).toEqual(["fit [-123,49,-122,50]"]);
  });
  it("(T287) a new status index with the same day's profiles (an applied update) swaps no style", async () => {
    const index = (): StatusIndex => ({
      handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 2, waters: 0, profiles: 2,
      profileOf: (s) => ({ 10: 0, 11: 1 } as Record<number, number>)[Number(s)] ?? null,
      profileCodeOn: (p) => (p === 0 ? "closed" : "own"), codeOn: () => "base", waterCodeOn: () => "base",
    });
    const day = new Date(2026, 5, 1);
    const r = render(<NativeMap {...base({ status: { index: index(), day } })} />);
    await waitFor(() => expect(JSON.stringify(native.props?.mapStyle)).toContain("closed"));
    const style1 = native.props!.mapStyle;
    r.rerender(<NativeMap {...base({ status: { index: index(), day } })} />);
    await act(async () => {});
    expect(native.props!.mapStyle).toBe(style1);
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

/**
 * (T674) A SPOT OFF THE MAP IS NOT IN THE ACCESSIBILITY TREE. Each mark has a 1 px button for a screen reader (T316); it
 * was there for EVERY saved spot wherever the camera was, so with the map on Coquitlam a reader still reached "Open spot
 * <a spot at Hope>" at the map's corner (emulator, T603: `spot-mark:spot:…` bounds [0,136][3,139]). Only the marks the
 * camera holds have one, on both maps, and it follows the camera.
 */
describe.each(["web", "native"] as const)("T674 a spot's button only while its mark is on the map: %s", (name) => {
  const HERE = { id: "s1", lat: 49, lon: -122, title: "Tamihi run" };
  /** 0.56 degrees east at zoom 9: about 408 px from the middle of a 400 px map. */
  const HOPE = { id: "s2", lat: 49, lon: -121.44, title: "Hope bar" };
  const spots = { points: [HERE, HOPE], selected: null, onOpen: () => {}, onList: () => {},
                  colours: { mark: colour("light", "color.ui.spot"), ground: colour("light", "color.ui.spot-ground") } };
  const said = () => screen.queryAllByRole("button").map((b) => b.getAttribute("aria-label")).filter((l) => l?.includes("spot"));
  const moveTo = (lon: number, lat: number) => {
    if (name === "web") { webCam.lng = lon; webCam.lat = lat; act(() => web.map!.fire("moveend")); return; }
    act(() => { (native.props!.onRegionDidChange as (e: unknown) => void)(
      { nativeEvent: { center: [lon, lat], zoom: 9, bearing: 0, pitch: 0, animated: false, userInteraction: true } }); });
  };

  it("the mark the camera holds is said; the one off the map is not, until the camera goes there", async () => {
    const M = PLATFORMS[name].Map;
    render(<M {...base({ data: undefined, highlight: undefined, spots })} />);
    if (name === "native") {
      await waitFor(() => expect(native.props).toBeTruthy());
      // before the map knows its own size nothing is said to be on it
      expect(said()).toEqual([]);
      act(() => { (native.props!.onLayout as (e: unknown) => void)({ nativeEvent: { layout: { width: 400, height: 800 } } }); });
    }
    await waitFor(() => expect(said()).toEqual(["Open spot Tamihi run"]));
    moveTo(HOPE.lon, HOPE.lat);
    await waitFor(() => expect(said()).toEqual(["Open spot Hope bar"]));
  });
});
