/**
 * (T248) The first-run walkthrough: once per device, skippable, sequenced behind the first-run notice, re-openable; and the
 * Help page drawn from the legend tokens.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { HELP_WORDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, helpVM } from "@app/core";
import { tidalColours } from "@app/map";
import { HelpPage } from "./menu/HelpPage";
import { useWalkthrough, WalkthroughSheet, type WalkthroughStore } from "./Walkthrough";
import { DARK, LIGHT } from "./theme";

afterEach(cleanup);
const memory = (v: string | null = null): WalkthroughStore & { v: string | null } => {
  const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } };
  return s;
};
const w = WALKTHROUGH_WORDS;

describe("the walkthrough (T248)", () => {
  it("opens on a new device and walks the cards; the last one stores the version and closes it", () => {
    const store = memory();
    const { result } = renderHook(() => useWalkthrough(store, false));
    expect(result.current.open).toBe(true);
    const { rerender } = render(<WalkthroughSheet state={result.current} palette={LIGHT} />);
    for (let i = 0; i < w.cards.length - 1; i++) {
      expect(screen.getByTestId(`walk-card-${w.cards[i]!.key}`)).toBeTruthy();
      act(() => { fireEvent.click(screen.getByText(w.next)); });
      rerender(<WalkthroughSheet state={result.current} palette={LIGHT} />);
    }
    expect(screen.getByTestId("walk-card-offline")).toBeTruthy();
    expect(screen.queryByText(w.skip)).toBeNull();
    expect(store.v).toBeNull();
    act(() => { fireEvent.click(screen.getByText(w.done)); });
    expect([store.v, result.current.open]).toEqual([WALKTHROUGH_VERSION, false]);
  });

  it("is skippable from the first card, and a skip counts as seen", () => {
    const store = memory();
    const { result } = renderHook(() => useWalkthrough(store, false));
    render(<WalkthroughSheet state={result.current} palette={LIGHT} />);
    act(() => { fireEvent.click(screen.getByText(w.skip)); });
    expect([store.v, result.current.open]).toEqual([WALKTHROUGH_VERSION, false]);
  });

  it("stays down once seen, and with no store (a test, a screen alone)", () => {
    expect(renderHook(() => useWalkthrough(memory(WALKTHROUGH_VERSION), false)).result.current.open).toBe(false);
    expect(renderHook(() => useWalkthrough(null, false)).result.current.open).toBe(false);
    expect(renderHook(() => useWalkthrough(memory("walkthrough-0"), false)).result.current.open).toBe(true);
  });

  it("waits for the first-run notice: held while it is up, there the moment it is gone — never both", () => {
    const store = memory();
    const { result, rerender } = renderHook(({ blocked }) => useWalkthrough(store, blocked), { initialProps: { blocked: true } });
    expect(result.current.open).toBe(false);
    rerender({ blocked: false });
    expect(result.current.open).toBe(true);
  });

  it("re-opens on request, from the Help page, even on a device that has seen it", () => {
    const store = memory(WALKTHROUGH_VERSION);
    const { result } = renderHook(() => useWalkthrough(store, false));
    act(() => result.current.reopen());
    expect(result.current.open).toBe(true);
    act(() => result.current.finish());
    expect(result.current.open).toBe(false);
  });

  it("draws the app's own pieces: the mode strip, the date strip, the status lines, the sync icon and bell", () => {
    const store = memory();
    const { result } = renderHook(() => useWalkthrough(store, false));
    const { rerender } = render(<WalkthroughSheet state={result.current} palette={LIGHT} />);
    expect(screen.getByTestId("walk-modes").textContent).toBe("StandardSalmonRun timing");
    const seen = ["walk-days", "walk-tap", "walk-offline"];
    for (const id of seen) {
      act(() => { fireEvent.click(screen.getByText(w.next)); });
      rerender(<WalkthroughSheet state={result.current} palette={LIGHT} />);
      expect(screen.getByTestId(id), id).toBeTruthy();
    }
    expect(screen.getByTestId("inseason-count")).toBeTruthy();
  });
});

describe("the Help page (T248)", () => {
  it("draws every section and row of helpVM, in order", () => {
    render(<HelpPage palette={LIGHT} theme="light" />);
    expect(screen.getAllByTestId(/^help-section-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(helpVM().map((s) => `help-section-${s.key}`));
    expect(screen.getAllByTestId(/^help-row-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(helpVM().flatMap((s) => s.rows.map((r) => `help-row-${r.key}`)));
    expect(screen.getByText(HELP_WORDS.lead)).toBeTruthy();
  });

  it("swatches wear the map's own colours in each theme — resolved, not copied", () => {
    for (const [palette, theme] of [[LIGHT, "light"], [DARK, "dark"]] as const) {
      const { unmount } = render(<HelpPage palette={palette} theme={theme} />);
      const closed = screen.getByTestId("help-row-status-closed").querySelector("div[style*='height: 4px']") as HTMLElement;
      expect(closed.style.backgroundColor).toBe(hexToRgb(palette.waterStatus.closed));
      expect(screen.getByTestId("help-tidal").style.backgroundColor).toBe(hexToRgb(tidalColours(theme).fill));
      expect(screen.getByTestId("help-edge-dashed")).toBeTruthy();
      expect(screen.getByTestId("help-edge-solid")).toBeTruthy();
      unmount();
    }
  });

  it("the button shows the walkthrough again; with no handler there is no button", () => {
    let n = 0;
    const { rerender } = render(<HelpPage palette={LIGHT} theme="light" onWalkthrough={() => { n++; }} />);
    fireEvent.click(screen.getByText(w.again));
    expect(n).toBe(1);
    rerender(<HelpPage palette={LIGHT} theme="light" />);
    expect(screen.queryByTestId("help-walkthrough")).toBeNull();
  });
});

function hexToRgb(hex: string): string {
  const m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex);
  if (!m) return hex;
  return `rgb(${parseInt(m[1]!, 16)}, ${parseInt(m[2]!, 16)}, ${parseInt(m[3]!, 16)})`;
}
