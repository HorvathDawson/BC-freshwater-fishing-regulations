/** (T624, replaces T620 G10) Runs' day is its own: opens on today, nothing the map does moves it, scrubbing is Runs' alone. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, renderHook } from "@testing-library/react";
import { pentadOf } from "@app/core";
import { useRunTiming } from "./runTiming";

afterEach(cleanup);
const NOW = new Date(2026, 10, 10);

describe("Runs' day", () => {
  it("opens on today and holds a scrub; a new render (the map's day moving elsewhere) never moves it", () => {
    const { result, rerender } = renderHook(() => useRunTiming({ source: null, wanted: true, paired: true, tiles: null, now: NOW }));
    expect(result.current.pentad).toBe(pentadOf(NOW));
    act(() => result.current.setPentad(10));
    rerender();
    expect(result.current.pentad).toBe(10);
    expect(result.current.today).toBe(pentadOf(NOW));
  });
});
