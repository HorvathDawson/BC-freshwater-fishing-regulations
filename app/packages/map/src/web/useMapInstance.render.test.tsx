/**
 * (T1005, crash kind 1bd22eea73a9dfb8) A BROWSER WITH NO WEBGL: MapLibre's constructor throws; the map is told it is
 * unavailable (it says so) and ONE stable error is reported, instead of an uncaught error out of the effect.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { setFailureSink } from "@app/core";
import { cleanup, renderHook } from "@testing-library/react";
import type { MapProps } from "../map-props";
import type { WebMapRefs } from "./refs";

vi.mock("maplibre-gl", () => ({
  // (T1252) GL 6's no-WebGL error: a GPUInitializationError carrying the browser's reason
  Map: class { constructor() { throw Object.assign(new Error("WebGL2 is required to display this map."),
                                                   { name: "GPUInitializationError", statusMessage: "no GPU" }); } },
  addProtocol: () => {}, setWorkerUrl: () => {}, getVersion: () => "6.19.0",
}));
const { useMapInstance } = await import("./useMapInstance");

afterEach(cleanup);

describe("T1005 useMapInstance with no WebGL", () => {
  it("says the map is unavailable and reports one MapUnavailable error", () => {
    const refs = { host: { current: document.createElement("div") }, map: { current: null }, renderer: { current: null },
                   adapter: { current: null }, copy: { current: null }, colouring: { current: null }, pill: { current: null },
                   refit: { current: null } } as unknown as WebMapRefs;
    const errors: Error[] = [];
    const said: (string | null)[] = [];
    const at = { atlas: "a", basemap: "b", glyphs: "g", sprite: "s", assets: "x" } as unknown as MapProps["at"];
    renderHook(() => useMapInstance(refs, { at, theme: "light", modes: undefined, gauges: undefined, initial: { lon: -123, lat: 49, zoom: 6 },
                                            onError: (e: Error) => { errors.push(e); } } as never, (u) => { said.push(u); }));
    expect(said).toEqual(["webgl"]);
    expect(errors.map((e) => `${e.name}: ${e.message}`)).toEqual(["MapUnavailable: Failed to initialize WebGL (no GPU)"]);
    expect(refs.map.current).toBeNull();
  });

  it("a map with no onError of its own (MiniMap) still reports it, once a page, and says it", () => {
    const failures: unknown[] = [];
    const unsink = setFailureSink((e) => { failures.push(e); });
    const said: (string | null)[] = [];
    const at = { atlas: "a", basemap: "b", glyphs: "g", sprite: "s", assets: "x" } as unknown as MapProps["at"];
    for (let i = 0; i < 2; i++) {
      const refs = { host: { current: document.createElement("div") }, map: { current: null }, renderer: { current: null },
                     adapter: { current: null }, copy: { current: null }, colouring: { current: null }, pill: { current: null },
                     refit: { current: null } } as unknown as WebMapRefs;
      renderHook(() => useMapInstance(refs, { at, theme: "light", modes: undefined, gauges: undefined,
                                              initial: { lon: -123, lat: 49, zoom: 6 } } as never, (u) => { said.push(u); }));
    }
    unsink();
    expect(said).toEqual(["webgl", "webgl"]);
    expect(failures.map((e) => `${(e as Error).name}: ${(e as Error).message}`))
      .toEqual(["MapUnavailable: Failed to initialize WebGL (no GPU)"]);
  });
});
