/**
 * (T409, owner 2026-11-05) THE WEB MAP IS BUILT WHEN IT IS FIRST SEEN. `Map.web.tsx` over a counting maplibre-gl (the
 * seam of `one-engine.render.test.tsx`), inside a box like the Shell's warm map behind a water's own page (`BEHIND`:
 * opacity 0). Below desktop width nothing is built until the box is shown; at desktop width it is built at once; and the
 * map built on being shown is the map a visible mount of the same props builds (one load path).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { MapProps } from "./map-props";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { BUILD_AT_ONCE_FROM } from "./web/firstSeen";

const built: { center: unknown; zoom: unknown; style: string }[] = [];
const log: string[] = [];
type Handler = (e?: unknown) => void;
class FakeGl {
  private on_ = new globalThis.Map<string, Set<Handler>>();
  private json: { layers: { id: string }[] };
  touchZoomRotate = { disableRotation: () => {} };
  keyboard = { disableRotation: () => {} };
  constructor(o: Record<string, unknown>) {
    built.push({ center: o.center, zoom: o.zoom, style: JSON.stringify(o.style) });
    this.json = o.style as FakeGl["json"];
    queueMicrotask(() => this.fire("load"));
  }
  on(ev: string, fn: Handler) { (this.on_.get(ev) ?? this.on_.set(ev, new Set()).get(ev)!).add(fn); return this; }
  once(ev: string, fn: Handler) { return this.on(ev, fn); }
  off(ev: string, fn: Handler) { this.on_.get(ev)?.delete(fn); return this; }
  fire(ev: string, e?: unknown) { for (const fn of [...(this.on_.get(ev) ?? [])]) fn(e); }
  isStyleLoaded() { return true; }
  getStyle() { return this.json; }
  getLayer(id: string) { return this.json.layers.find((l) => l.id === id); }
  getSource() { return { setData: () => log.push("setData") }; }
  setPaintProperty(id: string, prop: string) { log.push(`paint ${id} ${prop}`); }
  setLayoutProperty(id: string, prop: string, v: unknown) { log.push(`layout ${id} ${prop} ${String(v)}`); }
  setLayerZoomRange() {} setFilter(id: string) { log.push(`filter ${id}`); }
  addControl() {} hasImage() { return false; } addImage() {} removeImage() {} remove() {}
  getContainer() { return { clientWidth: 400, clientHeight: 800 }; }
  getCanvasContainer() { return { style: { cursor: "" } }; } isMoving() { return false; }
  getCanvas() { return { style: { width: "400px", height: "800px" } }; }
  resize() {}
  getCenter() { return { lng: -122, lat: 49 }; }
  loaded() { return true; }
  getZoom() { return 11; }
  getBounds() { return { getWest: () => -123, getSouth: () => 48, getEast: () => -121, getNorth: () => 50 }; }
  querySourceFeatures() { return []; }
  queryRenderedFeatures() { return []; }
  fitBounds(b: unknown) { log.push(`fit ${JSON.stringify(b)}`); }
  jumpTo(c: unknown) { log.push(`jump ${JSON.stringify(c)}`); }
  setFeatureState(t: { source: string; id: unknown }, s: unknown) { log.push(`set ${t.source} ${String(t.id)} ${JSON.stringify(s)}`); }
  removeFeatureState(t: { source: string; id?: unknown }) { log.push(`remove ${t.source} ${String(t.id)}`); }
}
class Control { constructor(_o?: unknown) {} }
vi.mock("maplibre-gl", () => ({ Map: FakeGl, NavigationControl: Control, ScaleControl: Control,
                                Marker: class { setLngLat() { return this; } addTo() { return this; } remove() {} },
                                Popup: class { setText() { return this; } }, addProtocol: () => {} }));
vi.mock("pmtiles", () => ({ Protocol: class { tile = () => {}; } }));

const { Map: WebMap } = await import("./Map.web");

const width = (w: number) =>
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
afterEach(() => { cleanup(); built.length = 0; log.length = 0; width(0); });

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
const props = (p: Partial<MapProps> = {}): MapProps => ({
  at: AT, theme: "light", view: "plain", modes: { stream: "standing", lake: "standing" },
  initial: { lon: -121.9, lat: 49.1, zoom: 12 }, ...p,
});
/** the Shell's box: behind the water (opacity 0) while a water is in front, the screen once it is left */
const Box = ({ behind, p }: { behind: boolean; p: MapProps }) => (
  <div data-testid="box" style={behind ? { position: "absolute", inset: 0, opacity: 0 } : { flex: 1 }}>
    <WebMap {...p} />
  </div>);
const settle = () => act(async () => { await Promise.resolve(); await Promise.resolve(); });

describe("the web map is built when it is first seen (T409)", () => {
  it("phone width, behind the water: nothing built (an empty box in its place); shown: built once, and stays built", async () => {
    width(390);
    const { rerender, container } = render(<Box behind p={props()} />);
    await settle();
    expect([built.length, !!container.querySelector("[data-map-unbuilt]")]).toEqual([0, true]);
    rerender(<Box behind={false} p={props()} />);
    await settle();
    expect([built.length, !!container.querySelector("[data-map-unbuilt]")]).toEqual([1, false]);
    rerender(<Box behind p={props()} />);   // hidden again (a second water opened): the map is kept, never rebuilt
    rerender(<Box behind={false} p={props()} />);
    await settle();
    expect(built.length).toBe(1);
  });

  it("shown by a CLASS change (react-native-web restyles by class), not only by an inline style", async () => {
    width(390);
    const css = document.createElement("style");
    css.textContent = ".behind { opacity: 0; }";
    document.head.appendChild(css);
    try {
      const { container } = render(<div className="behind"><WebMap {...props()} /></div>);
      await settle();
      expect(built.length).toBe(0);
      await act(async () => { (container.firstChild as HTMLElement).className = "front"; });
      await settle();
      expect(built.length).toBe(1);
    } finally { css.remove(); }
  });

  it("desktop width: built at once, even behind the water (owner's call)", async () => {
    width(1440);
    render(<Box behind p={props()} />);
    await settle();
    expect(built.length).toBe(1);
  });

  it("a map in sight is built at once at any width (every other map: as before)", async () => {
    width(390);
    render(<Box behind={false} p={props()} />);
    await settle();
    expect(built.length).toBe(1);
  });

  it("a phone that widens to desktop while the map is unseen builds it then", async () => {
    width(390);
    render(<Box behind p={props()} />);
    await settle();
    expect(built.length).toBe(0);
    width(1440);
    await act(async () => { window.dispatchEvent(new Event("resize")); });
    await settle();
    expect(built.length).toBe(1);
  });

  it("ONE LOAD PATH: the map built on being shown is the map a visible mount of the same (latest) props builds", async () => {
    width(390);
    const before = props({ theme: "light", initial: { lon: -121.9, lat: 49.1, zoom: 12 } });
    const after = props({ theme: "dark", view: "plain", initial: { lon: -127.5, lat: 54.3, zoom: 9 } });
    // reached by back: mounted unseen with one set of props, changed while unseen, then shown
    const { rerender } = render(<Box behind p={before} />);
    await settle();
    rerender(<Box behind p={after} />);
    rerender(<Box behind={false} p={after} />);
    await settle();
    const shown = { built: [...built], log: [...log] };
    cleanup(); built.length = 0; log.length = 0;
    // reached by a tap: mounted visible with those props
    render(<Box behind={false} p={after} />);
    await settle();
    expect(shown.built).toHaveLength(1);
    expect(shown).toEqual({ built, log });
  });

  it("the width it builds at once from is @app/ui's desktop breakpoint (read as text: this package may not import it)", () => {
    const layout = readFileSync(join(__dirname, "../../ui/src/layout.ts"), "utf8");
    const desktop = /LAYOUT_BREAKPOINTS = \{[^}]*desktop: (\d+)/.exec(layout)?.[1];
    expect(BUILD_AT_ONCE_FROM).toBe(Number(desktop));
  });
});
