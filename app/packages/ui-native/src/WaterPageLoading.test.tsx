/**
 * (T910, owner 2026-11-19 "build all 3 on webapp") THE REGULATIONS PAGE LOADS LATER ON THE WEB: its code is in the later
 * chunk, and until it is in the page draws its own loading face — the water frame (back, the view strip) and the
 * spinner — never an empty box; the same face it draws while its water's sheet comes. (The source checks — what imports
 * the page — are tools/water-page-lazy.test.ts: the boundaries gate keeps node:fs out of the packages.)
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, WaterSource } from "@app/data";
import { WaterPageLoading } from "./WaterPageLoading";
import { WaterScreen } from "./WaterScreen";
import { SPINNER_TIMING } from "./LoadingState";
import { LIGHT } from "./theme";

afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("(T910) the Regulations page's loading face", () => {
  it("draws the water frame at once and the spinner after the spinner's delay — never an empty box", () => {
    vi.useFakeTimers();
    render(<WaterPageLoading palette={LIGHT} />);
    expect(screen.getByTestId("water-frame-header")).toBeTruthy();
    expect(screen.getByTestId("loading-held")).toBeTruthy();          // the spinner's place, held for its delay
    act(() => { vi.advanceTimersByTime(SPINNER_TIMING.showAfterMs + 1); });
    expect(screen.getByTestId("loading-state")).toBeTruthy();
    expect(screen.getByLabelText(/loading/i)).toBeTruthy();
  });

  it("is the face of both waits: the page's code coming (the web's part) and its water's sheet coming (WaterScreen)", () => {
    vi.useFakeTimers();
    const base = makeFixtureSource();
    // a water whose sheet never comes: WaterScreen stays on its sheet wait
    const source = new Proxy(base, { get: (t, k) => (k === "water" ? () => new Promise(() => {}) : Reflect.get(t, k)) }) as WaterSource;
    const page = render(<WaterScreen source={source} item={"gnis:8634" as ItemId} palette={LIGHT} />);
    const code = render(<WaterPageLoading palette={LIGHT} />);
    expect(code.container.innerHTML).toBe(page.container.innerHTML);
    act(() => { vi.advanceTimersByTime(SPINNER_TIMING.showAfterMs + 1); });
    expect(code.container.querySelector('[data-testid="loading-state"]')).not.toBeNull();
    expect(code.container.innerHTML).toBe(page.container.innerHTML);
  });

});
