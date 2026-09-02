/**
 * The Conditions colouring: section -> station -> percentile.
 *
 * The failure that matters is painting a colour where there is no data. A reach with no
 * gauge must come back ABSENT, not zero — zero is the bottom of the scale, so it would
 * render every ungauged creek in BC as a river in drought. That is 97.6% of the province.
 */
import { describe, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useStandings } from "./hooks";
import type { RegsSource, SectionId, StationId } from "@app/data";

const A = "111:0" as SectionId, B = "222:0" as SectionId;

const src = (stations: Record<string, string>) => ({
  stationsFor: async (secs: readonly SectionId[]) =>
    new Map(secs.filter((s) => stations[s]).map((s) => [s, stations[s] as StationId])),
} as unknown as RegsSource);

const feed = (pct: Record<string, number | null>) => ({
  index: async () => ({
    stations: Object.fromEntries(
      Object.entries(pct).map(([k, v]) => [k, { percentile: v }])),
  }),
});

describe("useStandings", () => {
  it("joins a reach to its station's percentile", async () => {
    const { result } = renderHook(() =>
      useStandings(src({ [A]: "08A" }), feed({ "08A": 0.42 }), [A]));
    await waitFor(() => expect(result.current.get(A)).toBe(0.42));
  });

  it("leaves an ungauged reach ABSENT, never zero", async () => {
    // Zero is the bottom of the colour scale. 97.6% of BC has no gauge, so this single
    // decision is the difference between an honest map and a province-wide drought.
    const { result } = renderHook(() =>
      useStandings(src({ [A]: "08A" }), feed({ "08A": 0.42 }), [A, B]));
    await waitFor(() => expect(result.current.has(A)).toBe(true));
    expect(result.current.has(B)).toBe(false);
  });

  it("leaves a gauged reach absent when the station reports no percentile", async () => {
    const { result } = renderHook(() =>
      useStandings(src({ [A]: "08A" }), feed({ "08A": null }), [A]));
    await waitFor(() => expect(result.current.size).toBe(0));
  });

  it("colours nothing when the feed is unreachable", async () => {
    const dead = { index: async () => null };
    const { result } = renderHook(() => useStandings(src({ [A]: "08A" }), dead, [A]));
    await waitFor(() => expect(result.current.size).toBe(0));
  });

  it("colours nothing when there is no feed at all", async () => {
    const { result } = renderHook(() =>
      useStandings(src({ [A]: "08A" }), undefined, [A]));
    await waitFor(() => expect(result.current.size).toBe(0));
  });

  it("asks the bundle only about what is on screen", async () => {
    // The table is 558,746 rows. Holding it client-side to colour ~300 features would be
    // most of the bundle in memory.
    const stationsFor = vi.fn(async () => new Map());
    renderHook(() => useStandings({ stationsFor } as unknown as RegsSource,
                                  feed({}), [A, B]));
    await waitFor(() => expect(stationsFor).toHaveBeenCalledWith([A, B]));
  });

  it("does not query at all with nothing on screen", async () => {
    const stationsFor = vi.fn(async () => new Map());
    renderHook(() => useStandings({ stationsFor } as unknown as RegsSource, feed({}), []));
    await new Promise((r) => setTimeout(r, 10));
    expect(stationsFor).not.toHaveBeenCalled();
  });
});
