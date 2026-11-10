/** (T620 G10) Runs opens on the pentad of the day the MAP is on, and follows the map's day; scrubbing stays its own. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, renderHook } from "@testing-library/react";
import { pentadOf } from "@app/core";
import { useRunTiming } from "./runTiming";

afterEach(cleanup);
const NOW = new Date(2026, 10, 10);
const LATER = new Date(2026, 11, 2);   // three weeks out

describe("Runs' day", () => {
  it("opens on the map's picked day, not today", () => {
    const { result } = renderHook(() => useRunTiming({ source: null, wanted: true, paired: true, tiles: null,
                                                       now: NOW, mapDay: LATER }));
    expect(result.current.pentad).toBe(pentadOf(LATER));
    expect(result.current.today).toBe(pentadOf(NOW));
    expect(pentadOf(LATER)).not.toBe(pentadOf(NOW));
  });
  it("no picked day: today; a day picked later moves the slider, a scrub in between is kept until then", () => {
    const { result, rerender } = renderHook(({ d }: { d: Date | null }) =>
      useRunTiming({ source: null, wanted: true, paired: true, tiles: null, now: NOW, mapDay: d }), { initialProps: { d: null as Date | null } });
    expect(result.current.pentad).toBe(pentadOf(NOW));
    act(() => result.current.setPentad(10));
    expect(result.current.pentad).toBe(10);
    rerender({ d: LATER });
    expect(result.current.pentad).toBe(pentadOf(LATER));
  });
});
