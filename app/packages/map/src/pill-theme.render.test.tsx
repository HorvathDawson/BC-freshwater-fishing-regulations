/**
 * (T181) THE GAUGE PILL FOLLOWS THE THEME. `Map.web.tsx` over a recording maplibre-gl (the seam of
 * `one-engine.render.test.tsx`): the generated images — the gauge label's pill and the route's "you are here" pin — are
 * painted from the theme's tokens, and a light / dark / colour-blind switch must repaint them in the NEW theme. They
 * were re-added on every switch (`useLook` calls the map's `pill` re-adder) but by a closure that had captured the theme
 * the map was BUILT with, so a map opened light kept a light pill under dark text after a switch to dark.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { colour } from "./chrome";
import { hereImage } from "./here-mark";
import { pillImage } from "./pill";
import type { MapProps } from "./map-props";
import { HERE_IMAGE } from "./trace-overlay";

type Handler = (e?: unknown) => void;
type Img = { data: Uint8Array };
/** The last image added under each id. */
const added = new Map<string, Img>();
let gl: FakeGl | null = null;
class FakeGl {
  private on_ = new Map<string, Set<Handler>>();
  private json: { layers: { id: string }[] };
  touchZoomRotate = { disableRotation: () => {} };
  keyboard = { disableRotation: () => {} };
  constructor(opts: Record<string, unknown>) {
    this.json = opts.style as FakeGl["json"];
    gl = this;
    queueMicrotask(() => this.fire("load"));
  }
  on(ev: string, fn: Handler) { (this.on_.get(ev) ?? this.on_.set(ev, new Set()).get(ev)!).add(fn); return this; }
  once(ev: string, fn: Handler) { return this.on(ev, fn); }
  off(ev: string, fn: Handler) { this.on_.get(ev)?.delete(fn); return this; }
  fire(ev: string, e?: unknown) { for (const fn of [...(this.on_.get(ev) ?? [])]) fn(e); }
  isStyleLoaded() { return true; }
  getStyle() { return this.json; }
  getLayer(id: string) { return this.json.layers.find((l) => l.id === id); }
  getSource() { return { setData: () => {} }; }
  setPaintProperty() {} setLayoutProperty() {} setLayerZoomRange() {} setFilter() {}
  addControl() {} remove() {}
  hasImage(id: string) { return added.has(id); }
  addImage(id: string, img: Img) { added.set(id, img); }
  removeImage(id: string) { added.delete(id); }
  getContainer() { return { clientWidth: 400, clientHeight: 800 }; }
  getCanvas() { return { style: { width: "400px", height: "800px" } }; }
  resize() {}
  getCenter() { return { lng: -122, lat: 49 }; }
  loaded() { return true; }
  getZoom() { return 11; }
  getBounds() { return { getWest: () => -123, getSouth: () => 48, getEast: () => -121, getNorth: () => 50 }; }
  querySourceFeatures() { return []; }
  queryRenderedFeatures() { return []; }
  fitBounds() {}
  setFeatureState() {} removeFeatureState() {}
}
class Control { constructor(_o?: unknown) {} }
vi.mock("maplibre-gl", () => ({ Map: FakeGl, NavigationControl: Control, ScaleControl: Control,
                                Marker: class { setLngLat() { return this; } addTo() { return this; } remove() {} },
                                Popup: class { setText() { return this; } }, addProtocol: () => {} }));
vi.mock("pmtiles", () => ({ Protocol: class { tile = () => {}; } }));

const { Map: WebMap } = await import("./Map.web");

afterEach(() => { cleanup(); added.clear(); gl = null; });

const AT = { atlas: "file:///d/atlas.pmtiles", basemap: "file:///d/basemap.pmtiles" };
const props = (theme: string): MapProps => ({
  at: AT, theme, view: "plain", modes: { stream: "status", lake: "status" }, initial: { lon: -122, lat: 49, zoom: 11 },
});
/** The pill and the pin as `images.ts` paints them in `theme`. */
const pillIn = (theme: string) => pillImage(colour(theme, "color.outside"), colour(theme, "color.ui.line")).data;
const pinIn = (theme: string) => hereImage(colour(theme, "color.ui.accent"), colour(theme, "color.outside")).data;
const same = (a: Uint8Array | undefined, b: Uint8Array) => !!a && a.length === b.length && a.every((x, i) => x === b[i]);
const flush = () => act(async () => { await Promise.resolve(); await Promise.resolve(); });

describe("(T181) the web map's generated images follow a theme switch", () => {
  it("the themes paint different pills (else the switch below proves nothing)", () => {
    expect(same(pillIn("light"), pillIn("dark"))).toBe(false);
    expect(same(pinIn("light"), pinIn("dark"))).toBe(false);
  });

  it("a map built light, switched to dark, then cvd: the pill and the pin are repainted in each new theme", async () => {
    const r = render(<WebMap {...props("light")} />);
    await flush();
    expect(same(added.get("gauge-pill")?.data, pillIn("light"))).toBe(true);

    r.rerender(<WebMap {...props("dark")} />);
    await flush();
    expect(same(added.get("gauge-pill")?.data, pillIn("dark"))).toBe(true);
    expect(same(added.get(HERE_IMAGE)?.data, pinIn("dark"))).toBe(true);

    r.rerender(<WebMap {...props("cvd")} />);
    await flush();
    expect(same(added.get("gauge-pill")?.data, pillIn("cvd"))).toBe(true);
  });

  it("an image MapLibre asks for after a switch (`styleimagemissing`) comes back in the current theme too", async () => {
    const r = render(<WebMap {...props("light")} />);
    await flush();
    r.rerender(<WebMap {...props("dark")} />);
    await flush();
    added.clear();
    act(() => gl!.fire("styleimagemissing", { id: "gauge-pill" }));
    expect(same(added.get("gauge-pill")?.data, pillIn("dark"))).toBe(true);
  });
});
