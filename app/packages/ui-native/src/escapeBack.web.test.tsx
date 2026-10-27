/** T214 item 2: Escape is Back on the desktop web, only where nothing else owns the key. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
import { useEscapeBack } from "./escapeBack.web";
import { isTouchDevice } from "./touchDevice.web";

afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

function Probe({ back, active }: { back: () => boolean; active: boolean }) { useEscapeBack(back, active); return null; }

describe("useEscapeBack", () => {
  it("Escape calls the one back while active; any other key, or inactive (a layer is open, the root), does not", () => {
    const back = vi.fn(() => true);
    const r = render(<Probe back={back} active />);
    fireEvent.keyDown(window, { key: "Enter" });
    expect(back).not.toHaveBeenCalled();
    fireEvent.keyDown(window, { key: "Escape" });
    expect(back).toHaveBeenCalledTimes(1);
    r.rerender(<Probe back={back} active={false} />);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(back).toHaveBeenCalledTimes(1);
  });
  it("leaves an Escape alone that a field, a dialog or another handler already took", () => {
    const back = vi.fn(() => true);
    render(<Probe back={back} active />);
    const input = document.body.appendChild(document.createElement("input"));
    fireEvent.keyDown(input, { key: "Escape" });                       // typed in a field: the field's
    expect(back).not.toHaveBeenCalled();
    const taken = new KeyboardEvent("keydown", { key: "Escape", cancelable: true, bubbles: true });
    window.addEventListener("keydown", (e) => e.preventDefault(), { once: true, capture: true });
    window.dispatchEvent(taken);                                       // already taken by another handler
    expect(back).not.toHaveBeenCalled();
    const dialog = document.body.appendChild(document.createElement("div"));
    dialog.setAttribute("aria-modal", "true");
    fireEvent.keyDown(window, { key: "Escape" });                      // a dialog over the page: its own Escape
    expect(back).not.toHaveBeenCalled();
    dialog.remove();
    fireEvent.keyDown(window, { key: "Escape" });
    expect(back).toHaveBeenCalledTimes(1);
  });
});

describe("isTouchDevice (web)", () => {
  it("is the primary pointer: coarse swipes, a mouse does not", () => {
    vi.stubGlobal("matchMedia", (q: string) => ({ matches: q === "(pointer: coarse)" }));
    expect(isTouchDevice()).toBe(true);
    vi.stubGlobal("matchMedia", () => ({ matches: false }));
    expect(isTouchDevice()).toBe(false);
  });
});
