/**
 * (T731, owner 2026-11-16: "we should fix anything that doesnt seem desirable") THE BAR KEEPS ITS HEIGHT ACROSS A STEP
 * INTO ANOTHER WATER: the Vedder has one part, so no "Show all N parts" row (and no exit line): the bar drawn under the
 * map was 122 px shorter than the Chilliwack's and the map grew by that much. A water opened fresh carries no floor.
 */
import { describe, expect, it } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { useBarFloor } from "./useBarFloor";

const lay = (r: { current: { onLayout: (e: never) => void } }, h: number) =>
  act(() => r.current.onLayout({ nativeEvent: { layout: { height: h } } } as never));

describe("(T731) useBarFloor", () => {
  it("a fresh open has no floor; a step into another water floors the bar at the height just left", () => {
    const { result, rerender } = renderHook(({ k }: { k: string | null }) => useBarFloor(k), { initialProps: { k: "gnis:8634" as string | null } });
    expect(result.current.minHeight).toBeUndefined();
    lay(result, 420);
    rerender({ k: "gnis:8634" });                       // a step inside one water: still no floor
    expect(result.current.minHeight).toBeUndefined();
    rerender({ k: null });                              // the next water is read: nothing to say yet
    rerender({ k: "gnis:8639" });                       // the Vedder (one part): drawn no shorter
    expect(result.current.minHeight).toBe(420);
    lay(result, 420);
    rerender({ k: "gnis:8634" });                       // and back
    expect(result.current.minHeight).toBe(420);
  });
});
