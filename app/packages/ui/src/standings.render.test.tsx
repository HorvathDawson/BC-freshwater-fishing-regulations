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
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { clearPanelCache, usePanelStandings } from "./panel";
import type { Panel, RegsSource, SectionId } from "@app/data";

const A = "111:0" as SectionId, B = "222:0" as SectionId;

const panel = (...donors: [string, number][]): Panel => ({
  areaKm2: 100,
  members: donors.map(([station, areaKm2]) =>
    ({ station: station as never, role: "up" as const, areaKm2, years: 40,
       regulated: false, sameRiver: true })),
});

const src = (panels: Record<string, Panel>, lakes: Record<string, string> = {}) => ({
  panelsFor: async (secs: readonly SectionId[]) =>
    new Map(secs.filter((s) => panels[s]).map((s) => [s, panels[s]!])),
  // A lake is not a panel — its stage is set by its outlet and its own storage, so nothing
  // transfers to it. It is coloured by a station IN it, or not at all.
  lakeStationsFor: async (secs: readonly SectionId[]) =>
    new Map(secs.filter((s) => lakes[s]).map((s) => [s, lakes[s]!])),
} as unknown as RegsSource);

const feed = (pct: Record<string, number | null>) => ({
  index: async () => ({
    stations: Object.fromEntries(
      Object.entries(pct).map(([k, v]) => [k, { percentile: v }])),
  }),
});

// The panel cache is module state — a bundle does not change while the app runs — so each
// test has to start from empty or it inherits the last one's viewport.
beforeEach(clearPanelCache);

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
    const lakeStationsFor = vi.fn(async () => new Map());
    renderHook(() => usePanelStandings(
      { panelsFor, lakeStationsFor } as unknown as RegsSource, feed({}), [A, B]));
    await waitFor(() => expect(panelsFor).toHaveBeenCalledWith([A, B]));
  });

  it("reads a section's panel once, however often the viewport moves over it", async () => {
    // The bundle does not change while the app runs, but the viewport does — constantly,
    // and the query key is the viewport. Every pan used to re-read panels the client
    // already had, over SQLite in WebAssembly on range requests, and the colour arrived a
    // beat behind the map.
    const panelsFor = vi.fn(async (secs: readonly SectionId[]) =>
      new Map(secs.map((s) => [s, panel(["08A", 100])])));
    const lakeStationsFor = vi.fn(async () => new Map());
    const source = { panelsFor, lakeStationsFor } as unknown as RegsSource;
    const { result, rerender } = renderHook(
      ({ secs }) => usePanelStandings(source, feed({ "08A": 0.42 }), secs),
      { initialProps: { secs: [A] as SectionId[] } });
    await waitFor(() => expect(result.current.get(A)).toBeDefined());
    rerender({ secs: [A, B] });                 // panned: one new reach, one already known
    await waitFor(() => expect(result.current.get(B)).toBeDefined());
    // Two calls, and the second asked ONLY for the reach it had not seen.
    expect(panelsFor).toHaveBeenCalledTimes(2);
    expect(panelsFor).toHaveBeenLastCalledWith([B]);
  });

  it("remembers that a section has NO panel, and stops asking", async () => {
    // "Nothing qualifies here" is an answer. Re-asking for it on every pan is the same
    // waste as re-asking for a panel — and most of the province is this case.
    const panelsFor = vi.fn(async () => new Map());
    const lakeStationsFor = vi.fn(async () => new Map());
    const source = { panelsFor, lakeStationsFor } as unknown as RegsSource;
    const { rerender } = renderHook(
      ({ q }) => usePanelStandings(source, feed({}), [A], "both", q as never),
      { initialProps: { q: 0 } });
    await waitFor(() => expect(panelsFor).toHaveBeenCalledTimes(1));
    rerender({ q: 1 });                          // same reach, different question
    await new Promise((r) => setTimeout(r, 20));
    expect(panelsFor).toHaveBeenCalledTimes(1);
  });

  it("does not query at all with nothing on screen", async () => {
    const panelsFor = vi.fn(async () => new Map());
    const lakeStationsFor = vi.fn(async () => new Map());
    renderHook(() => usePanelStandings(
      { panelsFor, lakeStationsFor } as unknown as RegsSource, feed({}), []));
    await new Promise((r) => setTimeout(r, 10));
    expect(panelsFor).not.toHaveBeenCalled();
  });
});
