import { afterEach, describe, expect, it, vi } from "vitest";
import { camProbe, parseCameraLink } from "./native/camProbe";

describe("(T245) the test bed's camera link", () => {
  it("reads lon, lat and zoom", () => {
    expect(parseCameraLink("canifishthis://testbed/camera?lon=-121.9&lat=49.1&zoom=12"))
      .toEqual({ lon: -121.9, lat: 49.1, zoom: 12 });
  });
  it("is null for any other link, or a missing / unreadable number", () => {
    expect(parseCameraLink("canifishthis://water/123")).toBeNull();
    expect(parseCameraLink("canifishthis://testbed/camera?lon=-121.9&lat=49.1")).toBeNull();
    expect(parseCameraLink("canifishthis://testbed/camera?lon=x&lat=49.1&zoom=8")).toBeNull();
  });
});

describe("(T312) the camera probe is silent outside the test bed", () => {
  afterEach(() => { delete (globalThis as { __panProbe?: boolean }).__panProbe; vi.restoreAllMocks(); });
  it("logs nothing and subscribes nothing without the pan probe", () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    camProbe.settled({ lon: 0, lat: 0, zoom: 1, user: true });
    camProbe.move("fit", 0);
    const go = vi.fn();
    camProbe.mount(go)();
    expect(log).not.toHaveBeenCalled();
  });
  it("logs each settle and app move with the pan probe on", () => {
    (globalThis as { __panProbe?: boolean }).__panProbe = true;
    const log = vi.spyOn(console, "log").mockImplementation(() => {});
    camProbe.settled({ lon: -122, lat: 49, zoom: 10, user: true });
    camProbe.move("refine", 2);
    expect(log.mock.calls.map((c) => String(c[0]).split(" ")[0])).toEqual(["[cam]", "[move]"]);
    expect(String(log.mock.calls[1]?.[0])).toContain('"why":"refine","round":2');
  });
});
