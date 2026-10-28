/**
 * (T211) THE STANDARD / SALMON TOGGLE, from the hooks' side: the toggle moves the map's MODES and nothing else (owner
 * 2026-10-27: "toggling Standard/Salmon should be instant — no re-fetch/re-tile").
 *
 *   modes      Salmon paints every water through the salmon channel (`salmon`, per tile), only on the Map tab under
 *              the Regulations colouring; Standard is the day's status
 *   no fetch   the release's salmon is asked ONCE per source, before any toggle (after the day's status is in), and the
 *              feed handed to the map is the SAME object whichever way the toggle is set — so a toggle reaches no
 *              engine, no labels, no network
 */
import { describe, expect, it } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { useState } from "react";
import type { SalmonFacts } from "@app/core";
import type { WaterSource } from "@app/data";
import { mapModes, type MapModeInput } from "./mapModes";
import { useSalmonColour, type RegsLayer } from "./salmonColour";

const FACTS = { handles: "0".repeat(16), chains: 1, chainOf: () => 0,
                payload: { chains: [{ m: [0, 3] }] } } as unknown as SalmonFacts;
const source = (asked: string[]) =>
  ({ salmonFacts: async () => { asked.push("salmonFacts"); return FACTS; } } as unknown as WaterSource);
const DAY = new Date(2026, 6, 20, 12);

const base: MapModeInput = {
  tab: "map", flowParam: "discharge", horizon: 0, zoomedOut: false, paired: true, statusOk: true,
  water: "regulations", groups: {}, basemap: "map", imageryUp: true,
};

describe("mapModes: Standard / Salmon", () => {
  it("Salmon paints every water by DFO's salmon; Standard by the day's status — the toggle changes only the modes", () => {
    const std = mapModes(base), sal = mapModes({ ...base, salmon: true });
    expect([std.modes.stream, sal.modes.stream, sal.modes.lake, sal.modes.lake_flowing, sal.modes.wetland_flowing])
      .toEqual(["profile", "salmon", "salmon", "salmon", "salmon"]);
    const { modes: _a, ...restStd } = std, { modes: _b, ...restSal } = sal;
    expect(restSal).toEqual(restStd);
  });

  it("never on Conditions, under a lake measure, or off a proven pair", () => {
    expect([mapModes({ ...base, salmon: true, tab: "conditions" }).modes.stream,
            mapModes({ ...base, salmon: true, water: "stocked" }).modes.lake,
            mapModes({ ...base, salmon: true, paired: false }).modes.stream]).toEqual(["standing", "stocked", "plain"]);
  });
});

describe("useSalmonColour: fetched once, before the toggle, and the same feed whichever way it is set", () => {
  it("not asked until the day's status is in on the Map tab; then once; a toggle asks nothing and changes no feed", async () => {
    const asked: string[] = [];
    const src = source(asked);
    const { result, rerender } = renderHook((p: { statusOk: boolean }) => {
      const [paint, setPaint] = useState<RegsLayer>("standard");
      const s = useSalmonColour({ source: src, paired: true, statusOk: p.statusOk, mapTab: true, day: DAY });
      return { paint, setPaint, s };
    }, { initialProps: { statusOk: false } });
    await new Promise((r) => setTimeout(r, 0));
    expect([asked, result.current.s.feed]).toEqual([[], null]);    // first paint is not competed with
    rerender({ statusOk: true });
    await waitFor(() => expect(result.current.s.feed?.facts).toBe(FACTS));
    const feed = result.current.s.feed;
    for (const p of ["salmon", "standard", "salmon"] as const) {
      act(() => result.current.setPaint(p));
      expect(result.current.s.feed).toBe(feed);
    }
    expect(asked).toEqual(["salmonFacts"]);
  });

  it("leaving the Map tab keeps what was read: coming back asks nothing", async () => {
    const asked: string[] = [];
    const src = source(asked);
    const { result, rerender } = renderHook((p: { mapTab: boolean }) =>
      useSalmonColour({ source: src, paired: true, statusOk: true, mapTab: p.mapTab, day: DAY }), { initialProps: { mapTab: true } });
    await waitFor(() => expect(result.current.facts).toBe(FACTS));
    rerender({ mapTab: false });
    rerender({ mapTab: true });
    expect(asked).toEqual(["salmonFacts"]);
  });
});
