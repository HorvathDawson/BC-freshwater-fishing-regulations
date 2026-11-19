/**
 * (T876) THE LABEL HOLD ON THE PHONE'S MAP: `Map.native.tsx` over a recording `@maplibre/maplibre-react-native` (the
 * seam of `adapter-contract.render.test.tsx`). The camera's reports are fired as the binding fires them; what is read is
 * the visibility each ADOPTED label layer is handed (`Layer` with the style's own id, under its own source).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { Children, cloneElement, isValidElement, useEffect, useImperativeHandle, type ReactNode } from "react";
import type { MapProps } from "./map-props";
import { LOST_END_MS, SETTLE_MS } from "./label-hold";

/** A source hands its children its id, as the binding does (`cloneReactChildrenWithProps`). */
const withSource = (children: ReactNode, id: string) =>
  Children.map(children, (c) => (isValidElement(c) ? cloneElement(c, { source: id } as never) : c));
const native: { props: Record<string, unknown> | null; layers: Record<string, { source?: string; layout?: Record<string, unknown> }>;
                mounts: Record<string, number>; unmounts: string[] } = { props: null, layers: {}, mounts: {}, unmounts: [] };
vi.mock("@maplibre/maplibre-react-native", () => ({
  LogManager: { onLog: () => {} },
  Map: ({ ref, children, ...props }: { ref?: unknown; children?: ReactNode } & Record<string, unknown>) => {
    native.props = props;
    useImperativeHandle(ref as never, () => ({
      queryRenderedFeatures: async () => [], getBounds: async () => [-123, 48, -121, 50],
      project: async () => [200, 400], unproject: async () => [-122, 49],
    }));
    const loaded = props.onDidFinishLoadingStyle as (() => void) | undefined;
    useEffect(() => { loaded?.(); }, [props.mapStyle]);  // eslint-disable-line react-hooks/exhaustive-deps
    return <>{children}</>;
  },
  Camera: ({ ref }: { ref?: unknown }) => {
    useImperativeHandle(ref as never, () => ({ fitBounds: () => {}, easeTo: () => {} }));
    return null;
  },
  VectorSource: ({ id, ref, children }: { id: string; ref?: unknown; children?: ReactNode }) => {
    useImperativeHandle(ref as never, () => ({
      setFeatureState: async () => {}, removeFeatureState: async () => {}, querySourceFeatures: async () => [],
      setFeatureStates: async () => {}, watchTiles: async () => {}, nativeTag: () => 7,
    }));
    return <>{withSource(children, id)}</>;
  },
  Images: () => null,
  GeoJSONSource: ({ id, children }: { id: string; children?: ReactNode }) => <>{withSource(children, id)}</>,
  Layer: ({ id, source, layout }: { id: string; source?: string; layout?: Record<string, unknown> }) => {
    native.layers[id] = { source, layout };
    useEffect(() => {
      native.mounts[id] = (native.mounts[id] ?? 0) + 1;
      return () => { native.unmounts.push(id); };
    }, []);  // eslint-disable-line react-hooks/exhaustive-deps
    return null;
  },
  Marker: ({ children }: { children?: ReactNode }) => <>{children}</>,
}));

const { Map: NativeMap } = await import("./Map.native");
const { setLab, LAB_DEFAULT } = await import("./lab");

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
const props = (p: Partial<MapProps> = {}): MapProps => ({
  at: AT, theme: "light", view: "plain", initial: { lon: -122, lat: 49, zoom: 10 }, ...p,
});
type Ev = { center?: [number, number]; zoom?: number; userInteraction?: boolean };
const fire = (name: "onRegionWillChange" | "onRegionIsChanging" | "onRegionDidChange", e: Ev) =>
  act(() => { (native.props![name] as (e: { nativeEvent: Ev }) => void)({ nativeEvent: { center: [-122, 49], ...e } }); });
const vis = (id: string) => native.layers[id]?.layout?.visibility;
/** One of each kind the hold takes, and the gauge pill it leaves. */
const HELD = ["stream__label", "lake__label", "places_locality", "roads_labels_major", "park__label", "mu__label", "route-arrows"];
const shown = () => HELD.map(vis);
const ALL_VISIBLE = HELD.map(() => "visible");
const ALL_HIDDEN = HELD.map(() => "none");
const later = (ms: number) => act(() => { vi.advanceTimersByTime(ms); });

beforeEach(() => { vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] }); });
afterEach(() => {
  cleanup(); vi.useRealTimers(); setLab(LAB_DEFAULT);
  native.props = null; native.layers = {}; native.mounts = {}; native.unmounts = [];
});

describe("labels wait while the zoom moves, on a phone (T876)", () => {
  it("adopts every label layer under its own source, at the style's own visibility; the gauge pill is not held", () => {
    render(<NativeMap {...props()} />);
    expect(shown()).toEqual(ALL_VISIBLE);
    expect(HELD.map((id) => native.layers[id]!.source)).toEqual(["atlas", "atlas", "basemap", "basemap", "atlas", "mus", "atlas"]);
    // a layer the style itself does not draw is adopted at none, never shown by the hold
    expect(vis("obstacle")).toBe("none");
    expect(native.layers["gauge-label"]).toBeUndefined();
    // the map's own layers (the location dot) are not adopted labels
    expect(Object.keys(native.layers).filter((id) => id.startsWith("trace-"))).toEqual([]);
  });

  it("a pinch hides them once the zoom leaves where it started; at rest they come back after the settle", () => {
    render(<NativeMap {...props()} />);
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.005 });
    expect(shown()).toEqual(ALL_VISIBLE);
    fire("onRegionIsChanging", { zoom: 10.4 });
    expect(shown()).toEqual(ALL_HIDDEN);
    fire("onRegionDidChange", { zoom: 11.2, userInteraction: true });
    later(SETTLE_MS - 10);
    expect(shown()).toEqual(ALL_HIDDEN);
    later(10);
    expect(shown()).toEqual(ALL_VISIBLE);
  });

  it("a quick run of pinches keeps them away between pinches: the next pinch before the settle cancels it", () => {
    render(<NativeMap {...props()} />);
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 11 });
    fire("onRegionDidChange", { zoom: 11.3 });
    later(SETTLE_MS - 50);
    fire("onRegionWillChange", { zoom: 11.3, userInteraction: true });
    later(100);
    expect(shown()).toEqual(ALL_HIDDEN);
    fire("onRegionIsChanging", { zoom: 10.2 });
    fire("onRegionDidChange", { zoom: 10 });
    later(SETTLE_MS);
    expect(shown()).toEqual(ALL_VISIBLE);
  });

  it("a fit's glide keeps the labels (it runs behind a held pane; Android calls it user interaction)", async () => {
    render(<NativeMap {...props({ fit: { key: "part", bbox: [-122.1, 48.9, -121.9, 49.1] } })} />);
    await act(async () => {});
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 11.5 });
    fire("onRegionIsChanging", { zoom: 13 });
    expect(shown()).toEqual(ALL_VISIBLE);
    // the next move after the fit is the reader's: held as any
    later(2000);
    fire("onRegionDidChange", { zoom: 13 });
    fire("onRegionWillChange", { zoom: 13, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 12 });
    expect(shown()).toEqual(ALL_HIDDEN);
  });

  it("a pan that keeps its zoom keeps its labels", () => {
    render(<NativeMap {...props()} />);
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    for (let i = 0; i < 5; i++) fire("onRegionIsChanging", { zoom: 10, center: [-122 + i / 100, 49] });
    fire("onRegionDidChange", { zoom: 10 });
    expect(shown()).toEqual(ALL_VISIBLE);
    later(LOST_END_MS * 2);
    expect(shown()).toEqual(ALL_VISIBLE);
  });

  it("never left hidden: a lost 'did change' gives them back after the last camera report", () => {
    render(<NativeMap {...props()} />);
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.6 });
    later(LOST_END_MS - 100);
    fire("onRegionIsChanging", { zoom: 10.9 });          // still moving: the clock starts again
    later(LOST_END_MS - 100);
    expect(shown()).toEqual(ALL_HIDDEN);
    later(100);
    expect(shown()).toEqual(ALL_VISIBLE);
  });

  it("the hold switched off in the lab: the labels are given back at once and never held; nothing is unmounted", () => {
    render(<NativeMap {...props()} />);
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.6 });
    expect(shown()).toEqual(ALL_HIDDEN);
    act(() => { setLab({ holdLabels: false }); });
    expect(shown()).toEqual(ALL_VISIBLE);
    fire("onRegionWillChange", { zoom: 10.6, userInteraction: true });
    expect(native.props!.onRegionIsChanging).toBeUndefined();
    fire("onRegionDidChange", { zoom: 12 });
    later(LOST_END_MS);
    expect(shown()).toEqual(ALL_VISIBLE);
    // unmounting an adopted layer removes it from the style: a re-render never does
    expect(native.unmounts).toEqual([]);
  });

  it("a style swap keeps the same adopted layers mounted, at the new style's visibility, and the hold over it", () => {
    const r = render(<NativeMap {...props()} />);
    const ids = Object.keys(native.mounts).sort();
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.6 });
    r.rerender(<NativeMap {...props({ theme: "dark" })} />);
    expect(shown()).toEqual(ALL_HIDDEN);
    // a kind switched off in the lab is a style whose layer is at none: the hold gives back none for it
    act(() => { setLab({ waterNames: false }); });
    fire("onRegionDidChange", { zoom: 10.6 });
    later(SETTLE_MS);
    expect(shown()).toEqual(["none", "none", ...ALL_VISIBLE.slice(2)]);
    expect(Object.keys(native.mounts).sort()).toEqual(ids);
    expect(Object.values(native.mounts).every((n) => n === 1)).toBe(true);
    expect(native.unmounts).toEqual([]);
  });

  it("a map opened dark adopts the light style's own labels when it turns light (the ids grow, none is dropped)", () => {
    const r = render(<NativeMap {...props({ theme: "dark" })} />);
    expect(native.layers.pois).toBeUndefined();
    r.rerender(<NativeMap {...props({ theme: "light" })} />);
    expect(vis("pois")).toBe("visible");
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.6 });
    expect(vis("pois")).toBe("none");
    r.rerender(<NativeMap {...props({ theme: "dark" })} />);
    fire("onRegionDidChange", { zoom: 10.6 });
    later(SETTLE_MS);
    expect(vis("pois")).toBe("none");               // the dark style has none: kept, at none, never unmounted
    expect(native.unmounts).toEqual([]);
  });

  it("with the hold off nothing reads a camera frame", () => {
    render(<NativeMap {...props()} />);
    expect(native.props!.onRegionIsChanging).toBeTypeOf("function");
    act(() => { setLab({ holdLabels: false }); });
    expect(native.props!.onRegionIsChanging).toBeUndefined();
  });

  it("the map is said drawn whole only once the labels are back (a test-bed shot on 'whole' has its names)", () => {
    const drawn = vi.fn();
    render(<NativeMap {...props({ onDrawn: drawn })} />);
    const fully = () => act(() => { (native.props!.onDidFinishRenderingFrameFully as () => void)(); });
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.6 });
    act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });
    fire("onRegionDidChange", { zoom: 11 });
    fully();
    later(SETTLE_MS - 10);
    fully();
    expect(drawn).not.toHaveBeenCalled();
    later(10);                                       // the labels come back …
    expect(shown()).toEqual(ALL_VISIBLE);
    fully();                                         // … and their full frame is the whole one
    expect(drawn).toHaveBeenCalledTimes(1);
  });

  it("a release that draws no frame: the last full frame stands for it a moment later", () => {
    const drawn = vi.fn();
    render(<NativeMap {...props({ onDrawn: drawn })} />);
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.6 });
    act(() => { (native.props!.onDidFinishRenderingMapFully as () => void)(); });
    fire("onRegionDidChange", { zoom: 11 });
    act(() => { (native.props!.onDidFinishRenderingFrameFully as () => void)(); });
    later(SETTLE_MS);
    expect(drawn).not.toHaveBeenCalled();
    later(400);
    expect(drawn).toHaveBeenCalledTimes(1);
  });

  it("unmounting the map leaves no timer behind", () => {
    const r = render(<NativeMap {...props()} />);
    fire("onRegionWillChange", { zoom: 10, userInteraction: true });
    fire("onRegionIsChanging", { zoom: 10.6 });
    r.unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});
