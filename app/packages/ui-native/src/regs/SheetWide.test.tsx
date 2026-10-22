/**
 * T45 — a regs sheet on a wide screen is a centred dialog sized to its content, not a phone-height bottom sheet stretched
 * across the window (owner: "on desktop the sheet spans the whole 1280 px window … capped?"). The layout class comes the
 * way the app gives it: the shell's width through `LayoutWidthContext` (no provider: a phone). Back and Escape close it
 * (G1: the one back; Escape reaches the Modal's onRequestClose, which is the back layer's close).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { RegsPage, SheetKey, SheetVM } from "@app/core";
import { BackLayerContext, LayoutWidthContext, type BackLayers } from "@app/ui";
import { SheetHost } from "./SheetHost";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const sheetPage = (f: unknown): RegsPage => { const p = f as Frozen; return { ...p, sheet: (k: SheetKey) => p.sheets[k] ?? null } as RegsPage; };
const VEDDER = sheetPage(vedder1010);

function host(width: number | null, open: SheetKey = "pos:pos", layers: BackLayers | null = null) {
  const onClose = vi.fn();
  const sheet = <SheetHost page={VEDDER} open={open} onOpen={() => {}} onClose={onClose} palette={LIGHT} />;
  const inner = width === null ? sheet : <LayoutWidthContext.Provider value={width}>{sheet}</LayoutWidthContext.Provider>;
  render(layers ? <BackLayerContext.Provider value={layers}>{inner}</BackLayerContext.Provider> : inner);
  shown();
  return { onClose };
}
/**
 * react-native-web's Modal turns active (role dialog, Escape heard) when its opening animation ends; jsdom runs no
 * animation, so the end is sent by hand to the Modal's animated box (the portal's first child, under document.body).
 */
function shown() {
  const modal = document.querySelector('[aria-modal="true"]');
  const portal = [...document.body.children].find((c) => modal && c.contains(modal));
  const box = portal?.firstElementChild;
  // React listens for the prefixed name where the DOM has no AnimationEvent (jsdom): send both
  if (box) act(() => { for (const t of ["animationend", "webkitAnimationEnd"]) box.dispatchEvent(new Event(t, { bubbles: true })); });
}
/** what the open sheet is drawn as: the bottom sheet (its grabber and drag) or the dialog, with its measure */
function shape() {
  const dialog = screen.queryByTestId("sheet-dialog");
  const s = dialog ? getComputedStyle(dialog) : null;
  return { dialog: !!dialog, maxWidth: s?.maxWidth ?? null, border: s?.borderTopWidth ?? null,
           title: screen.getByRole("heading").textContent, done: screen.getAllByLabelText("Done").length };
}

describe("T45 a sheet on a wide screen", () => {
  it("a phone (no width, 390, 899): the bottom sheet as before; a tablet and a desktop (900, 1024, 1440): the dialog, 640 at most", () => {
    const seen: Record<string, unknown> = {};
    for (const w of [null, 390, 899, 900, 1024, 1440]) { host(w); seen[String(w)] = shape(); cleanup(); }
    const sheet = { dialog: false, maxWidth: null, border: null, title: "Possession", done: 1 };
    const dialog = { dialog: true, maxWidth: "640px", border: "1.5px", title: "Possession", done: 1 };
    expect(seen).toEqual({ null: sheet, 390: sheet, 899: sheet, 900: dialog, 1024: dialog, 1440: dialog });
  });

  it("the dialog's words are the sheet's words, in order", () => {
    host(390, "src:r0");
    const phone = document.body.textContent;
    cleanup();
    host(1440, "src:r0");
    expect(document.body.textContent).toBe(phone);
  });

  it("Escape, Done and the scrim each close the dialog once", () => {
    const h = host(1440);
    act(() => { fireEvent.keyUp(document, { key: "Escape" }); });
    act(() => { screen.getByLabelText("Done").click(); screen.getByLabelText("Close").click(); });
    expect(h.onClose).toHaveBeenCalledTimes(3);
  });

  it("G1: the dialog is a back layer while open; the one back (the browser's, Escape) closes it, not the water", () => {
    const log: string[] = [];
    let close: (() => void) | null = null;
    const layers: BackLayers = { register: (c) => { log.push("register"); close = c; return () => { log.push("unregister"); }; },
      back: () => { log.push("back"); close?.(); return true; } };
    const h = host(1440, "pos:pos", layers);
    act(() => { fireEvent.keyUp(document, { key: "Escape" }); });
    // the host here keeps the key open (its onClose is a spy), so the layer registers again: still open afterwards
    expect([log, h.onClose.mock.calls.length]).toEqual([["register", "back", "unregister", "register"], 1]);
  });

  it("no drag on the dialog (a window has no bottom edge to pull it to): the grabber is the bottom sheet's alone", () => {
    host(390);
    const phoneBars = screen.getByTestId("sheet-handle").children.length;
    cleanup();
    host(1440);
    expect([phoneBars, screen.getByTestId("sheet-handle").children.length]).toEqual([2, 1]);
  });
});
