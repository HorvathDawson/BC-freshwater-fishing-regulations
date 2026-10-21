/**
 * THE LAKE MODES' VALUES reach the map the way the Conditions standings do (`useConditionsColour`): from the release,
 * once per source, only on a proven pair and only to a map that labels each tile itself. A new release (a new source) is
 * new facts; a new day is the same facts with another day; nothing else changes them.
 */
import { describe, expect, it } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import type { LakeFacts } from "@app/core";
import type { RegsSource } from "@app/data";
import { useLakeColour } from "./lakeColour";

const facts = (tag: string): LakeFacts => ({
  handles: tag, asof: 46314, sections: 1, survey: () => null, stockedDay: () => null,
});
const source = (f: LakeFacts | null, asked: string[] = []) =>
  ({ lakeFacts: async () => { asked.push("lakeFacts"); return f; } } as unknown as RegsSource);
const DAY = new Date(2026, 9, 21, 15, 30);

describe("useLakeColour", () => {
  it("the release's facts and today's day number, once ready, on a proven pair to a per-tile map", async () => {
    const f = facts("a"), asked: string[] = [], src = source(f, asked);
    const { result } = renderHook(() => useLakeColour({ source: src, perTile: true, paired: true, today: DAY }));
    expect(result.current).toBeNull();                         // not asked yet is not "no lake is surveyed"
    await waitFor(() => expect(result.current).toEqual({ facts: f, day: 46314 }));
    expect(asked).toEqual(["lakeFacts"]);
  });

  it("nothing off a proven pair, to a map that takes pushed data, or from a release that ships none", async () => {
    const asked: string[] = [], src = source(facts("a"), asked), empty = source(null);
    const off = renderHook(() => useLakeColour({ source: src, perTile: true, paired: false, today: DAY }));
    const native = renderHook(() => useLakeColour({ source: src, perTile: false, paired: true, today: DAY }));
    const none = renderHook(() => useLakeColour({ source: empty, perTile: true, paired: true, today: DAY }));
    await new Promise((r) => setTimeout(r, 0));
    expect([off.result.current, native.result.current, none.result.current, asked]).toEqual([null, null, null, []]);
  });

  it("a new release is new facts; the same release on the same day is the SAME object (the map does nothing)", async () => {
    const a = facts("a"), b = facts("b");
    let src = source(a);
    const { result, rerender } = renderHook(() => useLakeColour({ source: src, perTile: true, paired: true, today: DAY }));
    await waitFor(() => expect(result.current?.facts).toBe(a));
    const first = result.current;
    rerender();
    expect(result.current).toBe(first);
    src = source(b);
    rerender();
    await waitFor(() => expect(result.current).toEqual({ facts: b, day: 46314 }));
  });
});
