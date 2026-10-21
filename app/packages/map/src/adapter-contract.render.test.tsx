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
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { useEffect, useImperativeHandle, type ReactNode } from "react";
import { MAP_GESTURES } from "@app/core";
import type { MapProps } from "./map-props";
import { maplibreGestures, nativeMapGestures } from "./gestures";

/** Every SDK call that matters to the contract, in order, as one string each. */
const log: string[] = [];
const fmt = (id: unknown) => `${typeof id}:${String(id)}`;
/** What a query answers: by the layers it asked for, and whether the asker is the phone (string ids) or the web. */
let answer: (layers: readonly string[], phone: boolean) => { id?: unknown }[] = () => [];

// ---- maplibre-gl, recorded -------------------------------------------------------------------------------------------
type Handler = (e?: unknown) => void;
const web: { map: FakeGl | null; opts: Record<string, unknown> | null; rotationOff: boolean;
             keyboardRotationOff: boolean } = { map: null, opts: null, rotationOff: false, keyboardRotationOff: false };
class FakeGl {
  private on_ = new Map<string, Set<Handler>>();
  private style: { layers: { id: string; source?: string }[]; sources: Record<string, unknown> };
  touchZoomRotate = { disableRotation: () => { web.rotationOff = true; } };
  keyboard = { disableRotation: () => { web.keyboardRotationOff = true; } };
  constructor(opts: Record<string, unknown>) {
    web.opts = opts; web.map = this;
    this.style = opts.style as FakeGl["style"];
  }
  on(ev: string, fn: Handler) { (this.on_.get(ev) ?? this.on_.set(ev, new Set()).get(ev)!).add(fn); return this; }
  once(ev: string, fn: Handler) { return this.on(ev, fn); }
  off(ev: string, fn: Handler) { this.on_.get(ev)?.delete(fn); return this; }
  fire(ev: string, e?: unknown) { for (const fn of [...(this.on_.get(ev) ?? [])]) fn(e); }
  isStyleLoaded() { return true; }
  getStyle() { return this.style; }
  getLayer(id: string) { return this.style.layers.find((l) => l.id === id); }
  getSource() { return { setData: () => {} }; }
  setPaintProperty() {} setLayoutProperty() {} setLayerZoomRange() {} setFilter() {}
  addControl() {} hasImage() { return false; } addImage() {} removeImage() {} remove() {}
  getContainer() { return { clientWidth: 400, clientHeight: 800 }; }
  getCenter() { return { lng: -122, lat: 49 }; }
  getZoom() { return 9; }
  getBounds() { return { getWest: () => -123, getSouth: () => 48, getEast: () => -121, getNorth: () => 50 }; }
  querySourceFeatures() { return []; }
  fitBounds(b: unknown) { log.push(`fit ${JSON.stringify(b)}`); }
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
  constructor(_o?: unknown) {}
  setLngLat() { return this; } addTo() { return this; } remove() {} setPopup() { return this; }
}
vi.mock("maplibre-gl", () => ({ Map: FakeGl, NavigationControl: Control, ScaleControl: Control, Marker,
                                Popup: class { setText() { return this; } }, addProtocol: () => {} }));
vi.mock("pmtiles", () => ({ Protocol: class { tile = () => {}; } }));

// ---- @maplibre/maplibre-react-native, recorded ----------------------------------------------------------------------
type Press = { nativeEvent: { point: [number, number]; lngLat: [number, number] } | null };
const native: { props: Record<string, unknown> | null; camera: Record<string, unknown> | null;
                geojson: Record<string, unknown> } = { props: null, camera: null, geojson: {} };
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
    useEffect(() => { loaded?.(); }, [props.mapStyle]);
    return <>{children}</>;
  },
  Camera: ({ ref, ...props }: { ref?: unknown } & Record<string, unknown>) => {
    native.camera = props;
    useImperativeHandle(ref as never, () => ({ fitBounds: (b: unknown) => log.push(`fit ${JSON.stringify(b)}`) }));
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
  Marker: () => null,
}));

const { Map: WebMap } = await import("./Map.web");
const { Map: NativeMap } = await import("./Map.native");

afterEach(() => {
  cleanup();
  log.length = 0;
  answer = () => [];
  web.map = null; web.opts = null; web.rotationOff = false; web.keyboardRotationOff = false;
  native.props = null; native.camera = null; native.geojson = {};
});

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
/** One object for the whole test: a NEW data object is new data, and both maps re-apply it (as they should). */
const DATA = { stream: { 311274: { status: "open" } }, lake: { 1566515: { status: "closed" } } };
const INITIAL = { lon: -122, lat: 49, zoom: 9 };
const LIT = [311274];
const base = (p: Partial<MapProps> = {}): MapProps => ({
  at: AT, theme: "light", view: "plain", initial: INITIAL, data: DATA, highlight: LIT, ...p,
});

/** The two maps, driven the same way: mount, re-render, tap at a point, settle. */
const PLATFORMS = {
  web: {
    Map: WebMap,
    tap: (x: number, y: number, lon: number, lat: number) => {
      act(() => web.map!.fire("click", { point: { x, y }, lngLat: { lng: lon, lat } }));
    },
    settle: () => { act(() => web.map!.fire("idle")); },
  },
  native: {
    Map: NativeMap,
    tap: (x: number, y: number, lon: number, lat: number) => {
      const e: Press = { nativeEvent: { point: [x, y], lngLat: [lon, lat] } };
      act(() => { (native.props!.onPress as (e: Press) => void)(e); });
      e.nativeEvent = null;              // React Native recycles the event once the handler yields (N1 §3)
    },
    settle: () => {
      act(() => {
        (native.props!.onRegionDidChange as (e: unknown) => void)(
          { nativeEvent: { center: [-122, 49], zoom: 9, bearing: 0, pitch: 0, bounds: [-123, 48, -121, 50],
                           animated: false, userInteraction: true } });
      });
    },
  },
} as const;

/** The selection on every highlightable layer, through its own SOURCE-layer (lake_flowing is lake's). */
const selected = (id: number, on: boolean) => [
  ["wetland", "wetland"], ["wetland_flowing", "wetland"], ["lake", "lake"], ["lake_flowing", "lake"],
  ["stream", "stream"],
].map(([, sl]) => (on ? `set atlas/${sl} number:${id} {"selected":true}` : `remove atlas/${sl} number:${id} selected`));

describe.each(Object.entries(PLATFORMS))("the map adapter contract: %s", (_name, P) => {
  it("status colours and the selection go to feature-state by the section's NUMBER id, on its source-layer", async () => {
    const r = render(<P.Map {...base()} />);
    await waitFor(() => expect(log).toEqual([
      'set atlas/stream number:311274 {"status":"open"}',
      'set atlas/lake number:1566515 {"status":"closed"}',
      ...selected(311274, true),
    ]));
    log.length = 0;
    r.rerender(<P.Map {...base({ highlight: [1566515] })} />);
    // per layer: the old selection's `selected` key cleared (only that key, per feature: the status colours stay),
    // then the new one set
    await waitFor(() => expect(log).toEqual(
      [0, 1, 2, 3, 4].flatMap((i) => [selected(311274, false)[i]!, selected(1566515, true)[i]!])));
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
  it("is handed the style with nothing promoted, and a camera held to the province's zooms and bounds", async () => {
    render(<NativeMap {...base()} />);
    await waitFor(() => expect(native.camera).toBeTruthy());
    const style = native.props!.mapStyle as { sources: { atlas: Record<string, unknown> } };
    expect({ promote: "promoteId" in style.sources.atlas, url: style.sources.atlas.url, camera: native.camera })
      .toEqual({
        promote: false, url: "pmtiles://file:///d/atlas.pmtiles",
        camera: { initialViewState: { center: [-122, 49], zoom: 9 }, minZoom: 4, maxZoom: 14,
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
    await waitFor(() => expect(log).toEqual([
      ...status,
      ...[0, 1, 2, 3, 4].flatMap((i) => [selected(311274, false)[i]!, selected(311274, true)[i]!]),
    ]));
    expect(native.props!.mapStyle).not.toBe(style1);
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
