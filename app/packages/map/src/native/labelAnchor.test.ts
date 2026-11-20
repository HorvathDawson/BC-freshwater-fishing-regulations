import { describe, expect, it } from "vitest";
import { nativeLabelAnchors, type Projector } from "./labelAnchor";
import type { MapMark } from "../map-props";

const MARKS: MapMark[] = [{ lon: -121.9, lat: 49.1, label: "Vedder Crossing", a11y: "Vedder Crossing", on: false },
                          { lon: -121.5, lat: 49.2, label: null, a11y: "a mark", on: false }];

/** A binding map that counts the calls it was sent; `fail` rejects them. */
function fakeMap(fail?: Error, x = 395) {
  const calls: string[] = [];
  const map: Projector = {
    project: async ([lon, lat]) => { calls.push(`project ${lon},${lat}`); if (fail) throw fail; return [x, 300]; },
  };
  return { map, calls };
}

describe("(T734) the marks' label anchors ask only a map that is there", () => {
  it("a map not ready (not styled, or not drawn yet): no native call at all, nothing reported", async () => {
    const { map, calls } = fakeMap(), reported: unknown[] = [];
    expect(await nativeLabelAnchors(MARKS, 400, map, () => false, (e) => reported.push(e))).toBeUndefined();
    expect(calls).toEqual([]);
    expect(reported).toEqual([]);
  });

  it("no map (the ref not set, or unmounted): no call", async () => {
    expect(await nativeLabelAnchors(MARKS, 400, null, () => true, () => {})).toBeUndefined();
  });

  it("a map that unmounts after the first check is not asked: each call checks again", async () => {
    const { map, calls } = fakeMap();
    let checks = 0;
    const at = await nativeLabelAnchors(MARKS, 400, map, () => ++checks === 1, () => {});
    expect(calls).toEqual([]);
    expect(at).toEqual([null, null]);
  });

  it("a ready map: one project per labelled mark, never an unproject (T929: the label's point is the mark's)", async () => {
    const { map, calls } = fakeMap();
    expect(await nativeLabelAnchors(MARKS, 400, map, () => true, () => {})).toEqual(["bottom-right", null]);
    expect(calls).toEqual(["project -121.9,49.1"]);
    expect(await nativeLabelAnchors(MARKS, 400, fakeMap(undefined, 200).map, () => true, () => {})).toEqual(["bottom", null]);
  });

  it("a ready map's call that rejects is REPORTED (never swallowed), and its label left unmeasured", async () => {
    const boom = new Error("project: the map is not ready");
    const { map } = fakeMap(boom), reported: unknown[] = [];
    const at = await nativeLabelAnchors(MARKS, 400, map, () => true, (e) => reported.push(e));
    expect(reported).toEqual([boom]);
    expect(at).toEqual([null, null]);
  });
});
