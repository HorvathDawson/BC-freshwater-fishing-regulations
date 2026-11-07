/**
 * (T382) The page's sun line: the tapped point when the frame has one, else the water's own point, on the chosen day,
 * on the B.C. clock — the whole composed line asserted. No point, no line.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import type { ItemId, WaterSource } from "@app/data";
import { useDaylight, useSunLine } from "./sun";

afterEach(cleanup);

const VEDDER = { lat: 49.1, lon: -121.96 };
const asked: ItemId[] = [];
const source = { waterPoint: async (item: ItemId) => { asked.push(item); return item === ("gnis:1" as ItemId) ? VEDDER : null; } } as
  Pick<WaterSource, "waterPoint">;
const NOV4 = { y: 2026, m: 11, d: 4 };

describe("useSunLine", () => {
  it("the tapped point, never asking the water's", () => {
    asked.length = 0;
    const { result } = renderHook(() => useSunLine(source, "gnis:1" as ItemId, { lat: 49.2827, lon: -123.1207 }, NOV4));
    expect(result.current).toBe("Sunrise 08:05 PT · sunset 17:46 PT");
    expect(asked).toEqual([]);
  });
  it("no tap: the water's own point, on the chosen day", async () => {
    asked.length = 0;
    const { result, rerender } = renderHook(({ day }) => useSunLine(source, "gnis:1" as ItemId, null, day), { initialProps: { day: NOV4 } });
    await waitFor(() => expect(result.current).toBe("Sunrise 08:00 PT · sunset 17:42 PT"));
    rerender({ day: { y: 2026, m: 6, d: 21 } });
    expect(result.current).toBe("Sunrise 05:03 PT · sunset 21:16 PT");
    expect(asked).toEqual(["gnis:1"]);
  });
  it("an unnamed reach takes its tap; with no point known there is no line", async () => {
    expect(renderHook(() => useSunLine(source, null, VEDDER, NOV4)).result.current).toBe("Sunrise 08:00 PT · sunset 17:42 PT");
    expect(renderHook(() => useSunLine(source, null, null, NOV4)).result.current).toBeNull();
    expect(renderHook(() => useSunLine({}, "gnis:1" as ItemId, null, NOV4)).result.current).toBeNull();
    const none = renderHook(() => useSunLine(source, "gnis:2" as ItemId, null, NOV4));
    await waitFor(() => expect(asked).toContain("gnis:2"));
    expect(none.result.current).toBeNull();
  });
});

describe("useDaylight (T473)", () => {
  it("the hours the rule's offsets leave open, at the point and on the day; no point, no function", () => {
    const { result } = renderHook(() => useDaylight(source, null, VEDDER, NOV4));
    expect(result.current!(60, 60)).toBe("07:00–18:42 PT");
    expect(renderHook(() => useDaylight(source, null, null, NOV4)).result.current).toBeNull();
  });
});
