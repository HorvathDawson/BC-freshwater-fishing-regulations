/**
 * (T842, owner on 0.7.12: "the keyboard covers vedder in the walk through") THE PICK STEP ON A PHONE, WITH THE CARD'S REAL
 * SIZE: the keyboard is up from typing "ved", the Vedder River row sits low in the list. The step closes the keyboard,
 * scrolls the row up into view, and the card's box never meets the row's ring. Its own file: it gives the page a
 * ResizeObserver and element sizes (jsdom lays nothing out), which react-native-web keeps for every later test in a file.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { useState } from "react";
import { act, render, renderHook, screen, waitFor } from "@testing-library/react";
import { Keyboard } from "react-native";
import { WALKTHROUGH_VERSION } from "@app/core";
import { GuideCard, panes, useGuide, type GuideActs, type GuideFacts, type WalkthroughStore } from "./Walkthrough";
import { useCoachRegistry, type Rect } from "./CoachMarks";
import { LIGHT } from "./theme";

const PHONE = { w: 390, h: 844 }, KEYBOARD_TOP = 500, CARD_H = 210;
const meets = (a: Rect, b: Rect) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
const px = (v: string) => parseFloat(v || "0");

/** a layout for the two views that matter: the walk's layer (the phone's window) and its card (the card's real size) */
function sizeOf(el: HTMLElement): { w: number; h: number } {
  const id = el.getAttribute("data-testid") ?? "";
  if (id === "guide") return PHONE;
  if (id.startsWith("guide-step-")) return { w: px(el.style.width), h: CARD_H };
  return { w: 0, h: 0 };
}
const saved: Record<string, unknown> = {};
beforeAll(() => {
  saved.ro = (window as unknown as { ResizeObserver?: unknown }).ResizeObserver;
  (window as unknown as { ResizeObserver: unknown }).ResizeObserver = class {
    constructor(private cb: (e: { target: Element }[]) => void) {}
    observe(target: Element) { setTimeout(() => this.cb([{ target }]), 0); }
    unobserve() {}
    disconnect() {}
  };
  for (const k of ["offsetWidth", "offsetHeight"] as const)
    Object.defineProperty(HTMLElement.prototype, k, { configurable: true,
      get(this: HTMLElement) { const s = sizeOf(this); return k === "offsetWidth" ? s.w : s.h; } });
  Object.assign(window, { innerWidth: PHONE.w, innerHeight: PHONE.h });
});
afterAll(() => {
  (window as unknown as { ResizeObserver?: unknown }).ResizeObserver = saved.ro;
  delete (HTMLElement.prototype as unknown as Record<string, unknown>).offsetWidth;
  delete (HTMLElement.prototype as unknown as Record<string, unknown>).offsetHeight;
});

describe("(T842) the pick step: the keyboard closed, the row in view, the card off it", () => {
  it("closes the keyboard, scrolls the row up, and the card's box never meets the row's ring", async () => {
    // iOS Safari's keyboard: the visual viewport shrinks to its top edge
    const vv = Object.assign(new EventTarget(), { height: KEYBOARD_TOP, offsetTop: 0, scale: 1 });
    Object.defineProperty(window, "visualViewport", { value: vv, configurable: true });
    const dismiss = vi.spyOn(Keyboard, "dismiss").mockImplementation(() => {
      act(() => { Object.assign(vv, { height: PHONE.h }); vv.dispatchEvent(new Event("resize")); });
    });
    try {
      // the results list under the field: its view from 120 to the foot; the row 790 down (under the keyboard, its foot off the screen), its arrow
      const scroll = { y: 0 }, asked: number[] = [];
      const page = { by: (dy: number) => { asked.push(dy); scroll.y = Math.max(0, scroll.y + dy); },
                     view: async () => ({ x: 0, y: 120, w: PHONE.w, h: PHONE.h - 120 }) };
      const reg = renderHook(() => useCoachRegistry()).result.current;
      reg.set("search:hit", { measureInWindow: (cb) => cb(0, 790 - scroll.y, PHONE.w, 70) }, null, page);
      reg.set("search:go", { measureInWindow: (cb) => cb(330, 805 - scroll.y, 44, 40) }, null, page);
      const store: WalkthroughStore = { read: () => `guide-at:${WALKTHROUGH_VERSION}/pick`, write: () => {} };
      const facts: GuideFacts = { water: { item: "w-vedder", name: "Vedder River" }, searching: true, query: "ved",
                                  selected: null, open: null, onMap: false };
      const acts: GuideActs = { search: () => {}, select: () => {}, openWater: () => {}, view: () => {} };
      function Walk() {
        const [f] = useState(facts);
        const guide = useGuide(store, false, f, acts);
        return <GuideCard guide={guide} facts={f} palette={LIGHT} registry={reg} top={68} bottom={0} />;
      }
      render(<Walk />);
      expect(screen.getByTestId("guide-step-pick")).toBeTruthy();
      expect(dismiss).toHaveBeenCalledTimes(1);                           // the keyboard closed as the step came up
      // the row scrolled up into the list's view, with the card's room beside it
      await waitFor(() => expect(asked.length).toBeGreaterThan(0), { timeout: 3000 });
      expect(scroll.y).toBeGreaterThan(0);
      const ring = await screen.findByTestId("guide-ring", {}, { timeout: 3000 });
      await waitFor(() => expect(px(ring.style.top)).toBeLessThan(790), { timeout: 3000 });
      const card = screen.getByTestId("guide-step-pick");
      const c = { x: px(card.style.left), y: px(card.style.top), w: px(card.style.width), h: CARD_H };
      const r = { x: px(ring.style.left), y: px(ring.style.top), w: px(ring.style.width), h: px(ring.style.height) };
      expect(meets(c, r), `card ${JSON.stringify(c)} ring ${JSON.stringify(r)}`).toBe(false);
      expect(c.y + c.h).toBeLessThanOrEqual(PHONE.h);                    // on the screen
      expect(r.y + r.h).toBeLessThanOrEqual(PHONE.h);                    // and the row wholly in view
      // the pointer is on the card's edge toward the row
      expect(screen.getByTestId(c.y > r.y ? "guide-pointer-below" : "guide-pointer-above")).toBeTruthy();
    } finally {
      dismiss.mockRestore();
      delete (window as unknown as { visualViewport?: unknown }).visualViewport;
    }
  });
});

describe("(T842) the dim round the highlight", () => {
  it("covers the screen but the hole, wherever its scroll carries it, so only the control takes a tap", () => {
    const win = PHONE, hole = { x: 20, y: 300, w: 200, h: 60 };
    const ps = panes(hole, win).map((p) => ({ x: p.left, y: p.top, w: p.width, h: p.height }));
    for (const p of ps) expect(meets(p, hole), JSON.stringify(p)).toBe(false);
    // carried up to a window's height either way, every point of the screen off the hole is still covered
    for (const dy of [-win.h, -200, 0, 200, win.h]) for (let x = 0; x < win.w; x += 13) for (let y = 0; y < win.h; y += 13) {
      const inHole = x >= hole.x && x < hole.x + hole.w && y >= hole.y + dy && y < hole.y + hole.h + dy;
      const covered = ps.some((p) => x >= p.x && x < p.x + p.w && y >= p.y + dy && y < p.y + p.h + dy);
      expect(covered, `${x},${y} carried ${dy}`).toBe(!inHole);
    }
  });
});
