/**
 * THE LOOK IS KEYED ON THE LOOK ALONE (engine polish, 2026-10-21): a horizon change moves the standings, never a
 * layer's mode — so `useMapModes` must hand the map the SAME `modes` and `activeGroups` objects across it. A new object
 * re-ran the map's whole look pass (139 `setPaintProperty` per horizon click, measured).
 */
import { describe, expect, it } from "vitest";
import { renderHook } from "@testing-library/react";
import { useMapModes, type MapModeInput } from "./mapModes";

const GROUPS = { water: true };
const base: MapModeInput = {
  tab: "conditions", flowParam: "discharge", horizon: 0, zoomedOut: false, paired: true, statusOk: true,
  perTile: true, water: "regulations", groups: GROUPS, basemap: "satellite", imageryUp: true,
};

describe("useMapModes", () => {
  it("a horizon change keeps the same `modes` and `activeGroups` objects; the horizon itself moves", () => {
    const r = renderHook((i: MapModeInput) => useMapModes(i), { initialProps: base });
    const first = r.result.current;
    r.rerender({ ...base, horizon: 3 });
    const next = r.result.current;
    expect(next.standingHorizon).toBe(3);
    expect(next.modes).toBe(first.modes);
    expect(next.activeGroups).toBe(first.activeGroups);
  });

  it("a change that moves a mode is a new `modes` object, with the new modes", () => {
    const r = renderHook((i: MapModeInput) => useMapModes(i), { initialProps: base });
    const first = r.result.current.modes;
    r.rerender({ ...base, flowParam: "temperature" });
    expect(r.result.current.modes).not.toBe(first);
    expect(r.result.current.modes).toEqual({
      stream: "plain", lake: "plain", lake_flowing: "plain", wetland_flowing: "plain", gauges: "temperature",
      basin: "plain",
    });
  });

  it("new-but-equal groups (a caller's fresh object) keep the same `activeGroups`", () => {
    const r = renderHook((i: MapModeInput) => useMapModes(i), { initialProps: base });
    const first = r.result.current.activeGroups;
    r.rerender({ ...base, groups: { water: true } });
    expect(r.result.current.activeGroups).toBe(first);
    r.rerender({ ...base, groups: { water: false } });
    expect(r.result.current.activeGroups).toEqual({ water: false });
  });

  it("losing the imagery moves only the ground: the same `modes` and `activeGroups`, the drawn map, the reason", () => {
    const r = renderHook((i: MapModeInput) => useMapModes(i), { initialProps: base });
    const first = r.result.current;
    r.rerender({ ...base, imageryUp: false });
    const next = r.result.current;
    expect({ modes: next.modes === first.modes, groups: next.activeGroups === first.activeGroups,
             before: first.basemap, after: next.basemap }).toEqual({ modes: true, groups: true, before: "satellite",
                                                                    after: "map" });
    r.rerender({ ...base, imageryUp: true });
    expect(r.result.current.basemap).toBe("satellite");
  });
});
