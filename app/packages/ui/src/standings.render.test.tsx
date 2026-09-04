/**
 * The Conditions colouring: section -> panel -> percentile.
 *
 * The failure that matters is painting a colour where there is no data. A reach nothing can
 * speak for must come back ABSENT, not zero — zero is the bottom of the scale, so it would
 * render every ungauged creek in BC as a river in drought. That is most of the province.
 *
 * These invariants were written for a `useStandings` that joined through `section_gauge`,
 * one station per reach. That hook is gone: it left a reach grey whenever its one station
 * went quiet, while a tap on the same reach answered confidently from the rest of its
 * panel. The invariants did not change with the join, so they are kept and re-pointed.
 */
import { describe, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { usePanelStandings } from "./panel";
import type { Panel, RegsSource, SectionId } from "@app/data";

const A = "111:0" as SectionId, B = "222:0" as SectionId;

const panel = (...donors: [string, number][]): Panel => ({
  areaKm2: 100,
  members: donors.map(([station, areaKm2]) =>
    ({ station: station as never, role: "up" as const, areaKm2, years: 40,
       regulated: false })),
});

const src = (panels: Record<string, Panel>) => ({
  panelsFor: async (secs: readonly SectionId[]) =>
    new Map(secs.filter((s) => panels[s]).map((s) => [s, panels[s]!])),
} as unknown as RegsSource);

const feed = (pct: Record<string, number | null>) => ({
  index: async () => ({
    stations: Object.fromEntries(
      Object.entries(pct).map(([k, v]) => [k, { percentile: v }])),
  }),
});

describe("usePanelStandings", () => {
  it("joins a reach to the percentile its panel produces", async () => {
    const { result } = renderHook(() =>
      usePanelStandings(src({ [A]: panel(["08A", 100]) }), feed({ "08A": 0.42 }), [A]));
    await waitFor(() => expect(result.current.get(A)).toBeCloseTo(0.42, 6));
  });

  it("leaves a reach with no panel ABSENT, never zero", async () => {
    // Zero is the bottom of the colour scale. Most of BC has no gauge entitled to speak for
    // it, so this single decision is the difference between an honest map and a
    // province-wide drought.
    const { result } = renderHook(() =>
      usePanelStandings(src({ [A]: panel(["08A", 100]) }), feed({ "08A": 0.42 }), [A, B]));
    await waitFor(() => expect(result.current.has(A)).toBe(true));
    expect(result.current.has(B)).toBe(false);
  });

  it("still colours a reach whose nearest gauge has gone quiet", async () => {
    // THE REASON THIS HOOK EXISTS. Under the old join the reach went grey; the panel
    // answers from the donor that is still reporting.
    const { result } = renderHook(() =>
      usePanelStandings(src({ [A]: panel(["quiet", 100], ["08B", 130]) }),
                        feed({ "quiet": null, "08B": 0.2 }), [A]));
    await waitFor(() => expect(result.current.get(A)).toBeCloseTo(0.2, 6));
  });

  it("marks a reach whose whole panel is silent, rather than dropping it", async () => {
    // -0.01 is the sentinel the style reserves: "somebody measures here, and today it
    // cannot tell you". Absent would say nobody is measuring at all.
    const { result } = renderHook(() =>
      usePanelStandings(src({ [A]: panel(["08A", 100]) }), feed({ "08A": null }), [A]));
    await waitFor(() => expect(result.current.get(A)).toBe(-0.01));
  });

  it("colours nothing when the feed is unreachable", async () => {
    const dead = { index: async () => null };
    const { result } = renderHook(() =>
      usePanelStandings(src({ [A]: panel(["08A", 100]) }), dead, [A]));
    await waitFor(() => expect(result.current.size).toBe(0));
  });

  it("colours nothing when there is no feed at all", async () => {
    const { result } = renderHook(() =>
      usePanelStandings(src({ [A]: panel(["08A", 100]) }), undefined, [A]));
    await waitFor(() => expect(result.current.size).toBe(0));
  });

  it("asks the bundle only about what is on screen", async () => {
    // Holding the whole table client-side to colour ~300 features would be most of the
    // bundle in memory.
    const panelsFor = vi.fn(async () => new Map());
    renderHook(() => usePanelStandings({ panelsFor } as unknown as RegsSource,
                                       feed({}), [A, B]));
    await waitFor(() => expect(panelsFor).toHaveBeenCalledWith([A, B]));
  });

  it("does not query at all with nothing on screen", async () => {
    const panelsFor = vi.fn(async () => new Map());
    renderHook(() => usePanelStandings({ panelsFor } as unknown as RegsSource, feed({}), []));
    await new Promise((r) => setTimeout(r, 10));
    expect(panelsFor).not.toHaveBeenCalled();
  });
});
