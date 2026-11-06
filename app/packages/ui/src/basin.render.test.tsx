/**
 * (T376) The watershed group a Conditions view is about: the map's (T417: the one most on screen, asked at rest, on
 * Conditions only) and a
 * water's own (its tap, else its own point) — never one for the other, and a failure said, never an empty answer.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { Basin, ItemId, SectionId, WaterSource } from "@app/data";
import { useViewBasin, useWaterBasin, waterBasin } from "./basin";

afterEach(cleanup);

const CHWK: Basin = { id: "CHWK", name: "Chilliwack River" };
const LFRA: Basin = { id: "LFRA", name: "Lower Fraser" };
type Box = readonly [number, number, number, number];
/**
 * West of lon 0: Chilliwack; 0 to 10: Lower Fraser; past 10: no group. Every point and box asked is recorded. A box is
 * answered by the larger overlap (`BasinGrid.mostIn` in miniature); a box of one point, by that point.
 */
const source = (asked: { lon: number; lat: number }[] = [], extra: Partial<WaterSource> = {}, boxes: Box[] = []) => ({
  basinAt: async (p: { lon: number; lat: number }) => { asked.push({ lon: p.lon, lat: p.lat });
                                                         return p.lon < 0 ? CHWK : p.lon < 10 ? LFRA : null; },
  basinIn: async (b: Box) => {
    boxes.push(b);
    const [w, , e] = b;
    if (w === e) return w < 0 ? CHWK : w < 10 ? LFRA : null;
    const c = Math.max(0, Math.min(e, 0) - w), l = Math.max(0, Math.min(e, 10) - Math.max(w, 0));
    return c === 0 && l === 0 ? null : c >= l ? CHWK : LFRA;
  },
  waterPoint: async (item: ItemId) => (item === ("gnis:1" as ItemId) ? { lon: 5, lat: 49 } : null),
  ...extra,
} as unknown as WaterSource);

describe("waterBasin: the water's own place, never the map's centre", () => {
  it("a tap on the water: the group the tap is in", async () => {
    expect(await waterBasin(source(), { item: "gnis:1" as ItemId, tapSid: 7 as SectionId, at: { lon: -1, lat: 49 } }))
      .toEqual(CHWK);
  });
  it("opened without a tap: the group of the water's own point", async () => {
    expect(await waterBasin(source(), { item: "gnis:1" as ItemId, tapSid: null, at: null })).toEqual(LFRA);
  });
  it("its place in no group: null (a fact, not a failure)", async () => {
    expect(await waterBasin(source([], { waterPoint: async () => ({ lon: 20, lat: 49 }) } as Partial<WaterSource>),
                            { item: "gnis:1" as ItemId, tapSid: null, at: null })).toBeNull();
  });
  it("THROWS where it has no place: a reach without its tap point, a water without geometry, a source without groups",
     async () => {
    await expect(waterBasin(source(), { item: null, tapSid: 7 as SectionId, at: null })).rejects.toThrow("without the point");
    await expect(waterBasin(source(), { item: "gnis:2" as ItemId, tapSid: null, at: null })).rejects.toThrow("no geometry");
    await expect(waterBasin({} as WaterSource, { item: null, tapSid: 7 as SectionId, at: { lon: 0, lat: 0 } }))
      .rejects.toThrow("carries no watershed groups");
  });
  it("useWaterBasin: ready with the group, failed with the reason", async () => {
    const ok = renderHook(() => useWaterBasin(source(), { item: "gnis:1" as ItemId, tapSid: null, at: null }));
    await waitFor(() => expect(ok.result.current).toEqual({ state: "ready", value: LFRA, error: null }));
    const bad = renderHook(() => useWaterBasin(source(), { item: "gnis:2" as ItemId, tapSid: null, at: null }));
    await waitFor(() => expect(bad.result.current.state).toBe("failed"));
  });
});

describe("useViewBasin (T417): the group most on the map, asked at rest", () => {
  it("asks the start (a point: the map has not drawn yet), then each rest by its bounds; renders only on a new group",
     async () => {
    const boxes: Box[] = [];
    const s = source([], {}, boxes);
    let renders = 0;
    const h = renderHook(() => { renders++; return useViewBasin(s, true, { lon: 1, lat: 49 }); });
    await waitFor(() => expect(h.result.current.viewBasin).toEqual(LFRA));
    expect(boxes).toEqual([[1, 49, 1, 49]]);
    const before = renders;
    act(() => h.result.current.noteView({ lon: 2, lat: 49, bounds: [1, 48, 3, 50] }));      // the same group: no render
    await waitFor(() => expect(boxes).toHaveLength(2));
    expect(boxes.at(-1)).toEqual([1, 48, 3, 50]);
    expect(renders).toBe(before);
    act(() => h.result.current.noteView({ lon: -3, lat: 49, bounds: [-4, 48, -2, 50] }));
    await waitFor(() => expect(h.result.current.viewBasin).toEqual(CHWK));
    act(() => h.result.current.noteView({ lon: 30, lat: 49, bounds: [29, 48, 31, 50] }));  // nothing on screen: null
    await waitFor(() => expect(h.result.current.viewBasin).toBeNull());
  });
  it("the owner's case: centred on the sea beside a group, the group on screen answers — never none", async () => {
    const s = source();
    const h = renderHook(() => useViewBasin(s, true, { lon: -3, lat: 49 }));
    await waitFor(() => expect(h.result.current.viewBasin).toEqual(CHWK));
    // centre lon 11 (the sea), the Lower Fraser reaching in from the west edge
    act(() => h.result.current.noteView({ lon: 11, lat: 49, bounds: [8, 48, 14, 50] }));
    await waitFor(() => expect(h.result.current.viewBasin).toEqual(LFRA));
    // centre in Chilliwack (lon -0.5) is the Chilliwack only when it holds more of the screen
    act(() => h.result.current.noteView({ lon: 1, lat: 49, bounds: [-3, 48, 5, 50] }));     // 3 vs 5 wide: LFRA stays
    act(() => h.result.current.noteView({ lon: -0.5, lat: 49, bounds: [-4, 48, 3, 50] }));  // 4 vs 3: Chilliwack
    await waitFor(() => expect(h.result.current.viewBasin).toEqual(CHWK));
  });
  it("off Conditions nothing is asked and the answer is withdrawn; back on, the last view noted is asked", async () => {
    const boxes: Box[] = [];
    const s = source([], {}, boxes);
    const h = renderHook(({ on }) => useViewBasin(s, on, { lon: 1, lat: 49 }), { initialProps: { on: false } });
    act(() => h.result.current.noteView({ lon: -3, lat: 49, bounds: [-4, 48, -2, 50] }));
    expect(boxes).toEqual([]);
    expect(h.result.current.viewBasin).toBeUndefined();
    h.rerender({ on: true });
    await waitFor(() => expect(h.result.current.viewBasin).toEqual(CHWK));
    expect(boxes).toEqual([[-4, 48, -2, 50]]);
    h.rerender({ on: false });
    await waitFor(() => expect(h.result.current.viewBasin).toBeUndefined());
  });
  it("a source or release without groups: the error, never null", async () => {
    const h = renderHook(() => useViewBasin({} as WaterSource, true, { lon: 1, lat: 49 }));
    await waitFor(() => expect(h.result.current.viewBasinError?.message).toMatch("carries no watershed groups"));
    expect(h.result.current.viewBasin).toBeUndefined();
  });
});
