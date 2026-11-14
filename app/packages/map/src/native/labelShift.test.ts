import { describe, expect, it } from "vitest";
import { nativeLabelShift, type Projector } from "./labelShift";
import type { MapMark } from "../map-props";

const MARKS: MapMark[] = [{ lon: -121.9, lat: 49.1, label: "Vedder Crossing", a11y: "Vedder Crossing", on: false },
                          { lon: -121.5, lat: 49.2, label: null, a11y: "a mark", on: false }];

/** A binding map that counts the calls it was sent; `fail` rejects them. */
function fakeMap(fail?: Error) {
  const calls: string[] = [];
  const map: Projector = {
    project: async ([lon, lat]) => { calls.push(`project ${lon},${lat}`); if (fail) throw fail; return [395, 300]; },
    unproject: async ([x, y]) => { calls.push(`unproject ${x},${y}`); if (fail) throw fail; return [-121.95, 49.1]; },
  };
  return { map, calls };
}

describe("(T734) the marks' label shift asks only a map that is there", () => {
  it("a map not ready (not styled, or not drawn yet): no native call at all, nothing reported", async () => {
    const { map, calls } = fakeMap(), reported: unknown[] = [];
    expect(await nativeLabelShift(MARKS, 400, map, () => false, (e) => reported.push(e))).toBeUndefined();
    expect(calls).toEqual([]);
    expect(reported).toEqual([]);
  });

  it("no map (the ref not set, or unmounted): no call", async () => {
    expect(await nativeLabelShift(MARKS, 400, null, () => true, () => {})).toBeUndefined();
  });

  it("a map that unmounts between a mark's two calls is not asked the second", async () => {
    const { map, calls } = fakeMap();
    let mounted = true;
    const wrapped: Projector = { project: async (p) => { const r = await map.project(p); mounted = false; return r; },
                                 unproject: map.unproject };
    const at = await nativeLabelShift(MARKS, 400, wrapped, () => mounted, () => {});
    expect(calls).toEqual(["project -121.9,49.1"]);
    expect(at).toEqual([null, null]);
  });

  it("a ready map: the label near the right edge is moved in (project, then unproject)", async () => {
    const { map, calls } = fakeMap();
    const at = await nativeLabelShift(MARKS, 400, map, () => true, () => {});
    expect(calls).toHaveLength(2);
    expect(calls[0]).toBe("project -121.9,49.1");
    expect(calls[1]).toMatch(/^unproject \d+(\.\d+)?,300$/);
    expect(at).toEqual([[-121.95, 49.1], null]);
  });

  it("a ready map's call that rejects is REPORTED (never swallowed), and its label left unmeasured", async () => {
    const boom = new Error("project: the map is not ready");
    const { map } = fakeMap(boom), reported: unknown[] = [];
    const at = await nativeLabelShift(MARKS, 400, map, () => true, (e) => reported.push(e));
    expect(reported).toEqual([boom]);
    expect(at).toEqual([null, null]);
  });
});
