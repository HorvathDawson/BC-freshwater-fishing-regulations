/**
 * (T1252) WHAT GL 6 CHANGED UNDER THE WEB MAP, pinned on the map `useMapInstance` builds:
 *  - `zoomLevelsToOverscale` is passed as `undefined` (GL 6 defaults it to 4 and slices z13 tiles from z14 up);
 *  - GL's worker file failing to load (its `error` event) is SAID on the map (reason "worker") and REPORTED once a
 *    page, from whichever map meets it first — through `onError`, or `reportFailure` without one — as `MapWorkerMissing`;
 *  - the no-WebGL error GL 6 raises on the `error` event (a lost context it cannot restore) is the browser
 *    (`MapUnavailable`, the map says so), never passed on as a defect.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, renderHook } from "@testing-library/react";
import type { MapProps } from "../map-props";
import type { WebMapRefs } from "./refs";

type Handler = (e?: unknown) => void;
let gl: FakeGl | null = null;
class FakeGl {
  opts: Record<string, unknown>;
  private on_ = new globalThis.Map<string, Set<Handler>>();
  touchZoomRotate = { disableRotation: () => {} };
  keyboard = { disableRotation: () => {} };
  constructor(o: Record<string, unknown>) { this.opts = o; gl = this; }
  setMissingStyleImageResolver() {}
  on(ev: string, fn: Handler) { (this.on_.get(ev) ?? this.on_.set(ev, new Set()).get(ev)!).add(fn); return this; }
  once(ev: string, fn: Handler) { return this.on(ev, fn); }
  off(ev: string, fn: Handler) { this.on_.get(ev)?.delete(fn); return this; }
  fire(ev: string, e?: unknown) { for (const fn of [...(this.on_.get(ev) ?? [])]) fn(e); }
  addControl() {} remove() {} resize() {}
  getSource() { return undefined; }
  getContainer() { return { clientWidth: 400, clientHeight: 800 }; }
  getCanvas() { return { style: { width: "400px", height: "800px" } }; }
  getCanvasContainer() { return { style: { cursor: "" } }; }
}
class Control { constructor(_o?: unknown) {} }
vi.mock("maplibre-gl", () => ({ Map: FakeGl, NavigationControl: Control, ScaleControl: Control, addProtocol: () => {},
                                setWorkerUrl: () => {}, getVersion: () => "6.19.0" }));
vi.mock("pmtiles", () => ({ Protocol: class { tile = () => {}; } }));
/** a fresh module per test: what was reported "once a page" is per page, and each test is a page */
let useMapInstance: typeof import("./useMapInstance").useMapInstance;
let setFailureSink: typeof import("@app/core").setFailureSink;   // the fresh module's own core, where it reports
beforeEach(async () => {
  vi.resetModules();
  ({ useMapInstance } = await import("./useMapInstance"));
  ({ setFailureSink } = await import("@app/core"));
});

afterEach(cleanup);

const at = { atlas: "a", basemap: "b", glyphs: "g", sprite: "s", assets: "x" } as unknown as MapProps["at"];
const refs = () => ({ host: { current: document.createElement("div") }, map: { current: null }, renderer: { current: null },
                      adapter: { current: null }, copy: { current: null }, colouring: { current: null }, pill: { current: null },
                      refit: { current: null } }) as unknown as WebMapRefs;
const mount = (onError?: (e: Error) => void, said: (string | null)[] = []) => {
  renderHook(() => useMapInstance(refs(), { at, theme: "light", modes: undefined, gauges: undefined, bare: true,
                                            initial: { lon: -123, lat: 49, zoom: 6 }, onError } as never, (u) => { said.push(u); }));
  return gl!;
};
/** what GL 6's actor fires when the worker file 404s (src/util/actor.ts createActors) */
const workerMissing = { type: "error", error: new Error("Worker failed to load. Check that the worker URL is correct.") };
/** what GL 6's `_contextRestored` fires when it cannot make a context again (src/ui/map.ts) */
const gpuLost = { type: "error", error: Object.assign(new Error("WebGL2 is required to display this map."),
                                                      { name: "GPUInitializationError", statusMessage: "context lost" }) };

describe("T1252 the GL 6 map", () => {
  it("overzooms past a source's deepest zoom, never slices (zoomLevelsToOverscale: undefined, set, not left out)", () => {
    const m = mount(() => {});
    expect(Object.prototype.hasOwnProperty.call(m.opts, "zoomLevelsToOverscale")).toBe(true);
    expect(m.opts.zoomLevelsToOverscale).toBeUndefined();
  });

  it("GL's worker failing to load: the map says so in its own words' reason, reported through onError ONCE (a pool of 3)", () => {
    const errors: Error[] = [];
    const said: (string | null)[] = [];
    const m = mount((e) => { errors.push(e); }, said);
    for (let i = 0; i < 3; i++) m.fire("error", workerMissing);   // GL subscribes each worker of its pool (3 on Safari)
    expect(said).toEqual([null, "worker", "worker", "worker"]);
    expect(errors.map((e) => `${e.name}: ${e.message}`))
      .toEqual(["MapWorkerMissing: GL's worker failed to load (/maplibre-gl-6.19.0/maplibre-gl-worker.mjs)"]);
    expect(errors[0]!.cause).toBe(workerMissing.error);
  });

  it("a map with no onError (MiniMap) says so and reports it, once a page with every other map", () => {
    const failures: unknown[] = [];
    const said: (string | null)[] = [];
    const unsink = setFailureSink((e) => { failures.push(e); });
    mount(undefined, said).fire("error", workerMissing);
    mount(undefined, said).fire("error", workerMissing);
    const errors: Error[] = [];
    mount((e) => { errors.push(e); }).fire("error", workerMissing);   // the screen map, on the same page
    unsink();
    expect(said).toEqual([null, "worker", null, "worker"]);
    expect(failures.map((e) => (e as Error).name)).toEqual(["MapWorkerMissing"]);
    expect(errors).toEqual([]);
  });

  it("a context GL cannot restore is the browser: the map says it is unavailable, reported as MapUnavailable", () => {
    const errors: Error[] = [];
    const said: (string | null)[] = [];
    mount((e) => { errors.push(e); }, said).fire("error", gpuLost);
    expect(said).toEqual([null, "webgl"]);
    expect(errors.map((e) => `${e.name}: ${e.message}`)).toEqual(["MapUnavailable: Failed to initialize WebGL (context lost)"]);
  });

  it("any other map error is passed on as it was", () => {
    const errors: Error[] = [];
    const e = new Error("layers.x: bad");
    mount((x) => { errors.push(x); }).fire("error", { type: "error", error: e });
    expect(errors).toEqual([e]);
  });
});
