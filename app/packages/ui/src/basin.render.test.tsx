/**
 * (T376) The watershed group a Conditions view is about: the map centre's (asked at rest, on Conditions only) and a
 * water's own (its tap, else its own point) — never one for the other, and a failure said, never an empty answer.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { Basin, ItemId, SectionId, WaterSource } from "@app/data";
import { useCentreBasin, useWaterBasin, waterBasin } from "./basin";

afterEach(cleanup);

const CHWK: Basin = { id: "CHWK", name: "Chilliwack River" };
const LFRA: Basin = { id: "LFRA", name: "Lower Fraser" };
/** West of lon 0: Chilliwack; 0 to 10: Lower Fraser; past 10: no group. Every point asked is recorded. */
const source = (asked: { lon: number; lat: number }[] = [], extra: Partial<WaterSource> = {}) => ({
  basinAt: async (p: { lon: number; lat: number }) => { asked.push({ lon: p.lon, lat: p.lat });
                                                         return p.lon < 0 ? CHWK : p.lon < 10 ? LFRA : null; },
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

describe("useCentreBasin: the group under the map's centre, asked at rest", () => {
  it("asks the start on Conditions, then each rest; re-renders only when the group changes", async () => {
    const asked: { lon: number; lat: number }[] = [];
    const s = source(asked);
    let renders = 0;
    const h = renderHook(() => { renders++; return useCentreBasin(s, true, { lon: 1, lat: 49 }); });
    await waitFor(() => expect(h.result.current.centreBasin).toEqual(LFRA));
    const before = renders, n = asked.length;
    act(() => h.result.current.noteCentre({ lon: 2, lat: 49 }));        // the same group: no new render
    await waitFor(() => expect(asked).toHaveLength(n + 1));
    expect(asked.at(-1)).toEqual({ lon: 2, lat: 49 });
    expect(renders).toBe(before);
    act(() => h.result.current.noteCentre({ lon: -3, lat: 49 }));
    await waitFor(() => expect(h.result.current.centreBasin).toEqual(CHWK));
    act(() => h.result.current.noteCentre({ lon: 30, lat: 49 }));       // the sea: null, not undefined
    await waitFor(() => expect(h.result.current.centreBasin).toBeNull());
  });
  it("off Conditions nothing is asked and the answer is withdrawn; back on, the last centre noted is asked", async () => {
    const asked: { lon: number; lat: number }[] = [];
    const s = source(asked);
    const h = renderHook(({ on }) => useCentreBasin(s, on, { lon: 1, lat: 49 }), { initialProps: { on: false } });
    act(() => h.result.current.noteCentre({ lon: -3, lat: 49 }));
    expect(asked).toEqual([]);
    expect(h.result.current.centreBasin).toBeUndefined();
    h.rerender({ on: true });
    await waitFor(() => expect(h.result.current.centreBasin).toEqual(CHWK));
    expect(asked).toEqual([{ lon: -3, lat: 49 }]);
    h.rerender({ on: false });
    await waitFor(() => expect(h.result.current.centreBasin).toBeUndefined());
  });
  it("a source or release without groups: the error, never null", async () => {
    const h = renderHook(() => useCentreBasin({} as WaterSource, true, { lon: 1, lat: 49 }));
    await waitFor(() => expect(h.result.current.centreBasinError?.message).toMatch("carries no watershed groups"));
    expect(h.result.current.centreBasin).toBeUndefined();
  });
});
