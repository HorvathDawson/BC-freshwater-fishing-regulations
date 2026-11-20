/**
 * T915 (owner 2026-11-19: "make calendar popup look better on desktop (good for mobile but desktop doesnt like mobile
 * popup)") — the browser's "Which day?": the phone's bottom sheet on a phone and a narrow window, exactly as before; at
 * the desktop layout class (>= 1280) a popover card hung under the button that opened it. Escape, a click outside and
 * Done close it, focus goes back to the button, and the keyboard starts on the chosen day.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { dayRange, setFailureSink } from "@app/core";
import { LayoutWidthContext } from "@app/ui";
import { DaySheet } from "./DaySheet";
import { LIGHT } from "../theme";

const NOW = new Date(2026, 10, 19, 9, 30);
const RANGE = dayRange(NOW, null);

let button: HTMLElement;
beforeEach(() => {
  // the page's date pill, where a desktop page draws it (jsdom lays nothing out: its box is given)
  button = document.createElement("div");
  button.setAttribute("data-testid", "regs-date");
  button.tabIndex = 0;
  button.getBoundingClientRect = () => ({ left: 300, top: 100, width: 120, height: 44, right: 420, bottom: 144, x: 300, y: 100, toJSON: () => ({}) });
  document.body.appendChild(button);
});
afterEach(() => { cleanup(); button.remove(); });

function mount(width: number | null, open = true) {
  const onClose = vi.fn(), onDay = vi.fn();
  const el = (o: boolean) => {
    const sheet = <DaySheet open={o} onClose={onClose} palette={LIGHT} day={null} onDay={onDay} range={RANGE} now={NOW} anchor="regs-date" />;
    return width === null ? sheet : <LayoutWidthContext.Provider value={width}>{sheet}</LayoutWidthContext.Provider>;
  };
  const r = render(el(open));
  return { onClose, onDay, close: () => r.rerender(el(false)) };
}

describe("T915: the day calendar on the web", () => {
  it("a phone and a narrow window keep the bottom sheet (no provider, 390, 900, 1279)", () => {
    for (const w of [null, 390, 900, 1279]) {
      mount(w);
      expect([w, !!screen.queryByTestId("sheet-panel"), !!screen.queryByTestId("day-popover")]).toEqual([w, true, false]);
      cleanup();
    }
  });

  it("desktop: a square card under the button, its left edge on the button's, the app's lift; no scrim, no sheet", () => {
    mount(1440);
    expect(screen.queryByTestId("sheet-panel")).toBeNull();
    const card = screen.getByTestId("day-popover");
    const st = getComputedStyle(card);
    expect([st.position, st.top, st.left, st.width]).toEqual(["absolute", "150px", "300px", "344px"]);
    expect(card.style.boxShadow || st.boxShadow).toContain("3px 3px 0");
    expect(card.getAttribute("style")).not.toMatch(/radius/);
    expect(card.textContent).toContain("Which day?");
    expect(card.querySelector('[data-testid="day-calendar"]')).not.toBeNull();
    // the area outside the card is clear (it only listens for the click that closes it)
    expect(getComputedStyle(screen.getByTestId("day-popover-outside")).backgroundColor).toMatch(/^(|transparent|rgba\(0, 0, 0, 0\))$/);
  });

  it("near the window's right edge the card stays inside the window", () => {
    button.getBoundingClientRect = () => ({ left: 900, top: 100, width: 120, height: 44, right: 1020, bottom: 144, x: 900, y: 100, toJSON: () => ({}) });
    mount(1440);                                        // jsdom's window: 1024 wide
    expect(getComputedStyle(screen.getByTestId("day-popover")).left).toBe(`${1024 - 344 - 16}px`);
  });

  it("the keyboard starts on the chosen day, and the click-outside area is no tab stop", async () => {
    mount(1440);
    await act(() => new Promise((r) => requestAnimationFrame(() => r(null))));
    const cell = screen.getByRole("button", { name: "Thursday, November 19, today, chosen" });
    expect(document.activeElement).toBe(cell);
    expect(screen.getByTestId("day-popover-outside").getAttribute("tabindex")).toBe("-1");
  });

  it("Escape, a click outside and Done each close it", () => {
    const { onClose } = mount(1440);
    act(() => { fireEvent.keyUp(document, { key: "Escape" }); });
    expect(onClose).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByTestId("day-popover-outside"));
    expect(onClose).toHaveBeenCalledTimes(2);
    fireEvent.click(screen.getByRole("button", { name: "Done" }));
    expect(onClose).toHaveBeenCalledTimes(3);
  });

  it("closed, focus goes back to the button that opened it", async () => {
    const { close } = mount(1440);
    act(() => close());
    await act(() => new Promise((r) => requestAnimationFrame(() => r(null))));
    expect(document.activeElement).toBe(button);
  });

  it("no button on the page: the card is still drawn, and the failure is reported", () => {
    button.remove();
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      mount(1440);
      expect(screen.getByTestId("day-popover")).toBeTruthy();
      // a resize while it is open measures again: still one report, not one per resize
      // (react-native-web's window size is the root element's client box)
      const size = (w: number) => act(() => {
        Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
        window.dispatchEvent(new Event("resize"));
      });
      try { size(1500); size(1600); } finally {
        delete (document.documentElement as { clientWidth?: number }).clientWidth;   // jsdom's own again
        window.dispatchEvent(new Event("resize"));
      }
      expect(seen.map(String)).toEqual([expect.stringContaining('day popover: its button "regs-date" is not on the page')]);
    } finally { undo(); }
  });
});
