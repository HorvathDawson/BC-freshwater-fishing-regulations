/**
 * (T984, owner 2026-11-21: "only a walk through not modify the state") The guided walkthrough only points and describes: once
 * per device, skippable at any step, resumed where it was left, re-openable; Next, Back and Skip move the tour alone; the
 * dim takes every touch. (`Walkthrough.t984.test.tsx` has the stores-before-and-after test and the highlight's time budget.)
 * Also the Help page drawn from the legend tokens (T248), and the card's geometry (T456, T824).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, renderHook, screen, waitFor, act } from "@testing-library/react";
import { HELP_WORDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, helpVM } from "@app/core";
import { notFishingColours, tidalColours } from "@app/map";
import { HelpPage } from "./menu/HelpPage";
import { GUIDE_STEPS, GuideCard, useGuide, type Guide, type WalkthroughStore } from "./Walkthrough";
import { insideWindow, unionRect, useCoachRegistry, type CoachRegistry, type Rect } from "./CoachMarks";
import { CARD_GAP, footRoom, placeGuideCard, revealBy } from "./guideLayout";
import { DARK, LIGHT, SPACE } from "./theme";

// (T402) a ceiling, not a delay: waitFor returns as soon as it holds
const WAIT = 10_000;
afterEach(cleanup);
const memory = (v: string | null = null): WalkthroughStore & { v: string | null } => {
  const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } };
  return s;
};
const w = WALKTHROUGH_WORDS;
const keyAt = () => screen.getByTestId(/^guide-step-/).getAttribute("data-testid")!.slice("guide-step-".length);
const press = (label: string) => act(() => { fireEvent.click(screen.getByText(label)); });
/** (T842) the stored line of a walk left at step `k` (it names the walk's version) */
const atStep = (k: string) => `guide-at:${WALKTHROUGH_VERSION}/${k}`;

function useRegistry(at: Record<string, [number, number, number, number]> = {}): CoachRegistry {
  const reg = renderHook(() => useCoachRegistry()).result.current;
  for (const [k, r] of Object.entries(at)) reg.set(k, { measureInWindow: (cb) => cb(...r) });
  return reg;
}
/** every control a step highlights, drawn somewhere (a phone's places, roughly) */
const EVERY: Record<string, [number, number, number, number]> = Object.fromEntries(
  [...new Set(GUIDE_STEPS.flatMap((s) => s.targets))].map((k, i) => [k, [16, 120 + (i % 8) * 60, 200, 40]]));

/** The walk over nothing: it takes no facts and no moves. */
function Harness({ store, blocked = false, reg, out }: {
  store: WalkthroughStore | null; blocked?: boolean; reg: CoachRegistry; out: { guide?: Guide };
}) {
  const guide = useGuide(store, blocked);
  out.guide = guide;
  return <GuideCard guide={guide} palette={LIGHT} registry={reg} top={60} bottom={64} />;
}

describe("the guided walkthrough (T293, T984)", { timeout: 30_000 }, () => {
  it("(T984) is five cards, every one only a look: search, the map, the day, the colour mode, you're all set", () => {
    expect(GUIDE_STEPS.map((s) => s.key)).toEqual(["search", "map", "days", "modes", "done"]);
    // every highlighted control is one the Map tab draws; no step is a DO step (no `kind`, no move)
    for (const s of GUIDE_STEPS) { expect(s).not.toHaveProperty("kind"); expect(s.body.length, s.key).toBeGreaterThan(10); }
    for (const s of GUIDE_STEPS) for (const n of s.names ?? []) expect(s.body, `${s.key}: ${n}`).toContain(n);
  });

  it("opens on a new device once nothing blocks it, and walks to the end with Next alone", async () => {
    const store = memory(), out: { guide?: Guide } = {};
    const reg = useRegistry(EVERY);
    const { rerender } = render(<Harness store={store} blocked reg={reg} out={out} />);
    expect(screen.queryByTestId("guide")).toBeNull();                 // the first-run notice is up
    rerender(<Harness store={store} reg={reg} out={out} />);
    expect(keyAt()).toBe("search");
    expect(screen.getByTestId("guide-count").textContent).toBe(`${w.phases.find} · ${w.step(1, GUIDE_STEPS.length)}`);
    const seen: string[] = [];
    for (let k = 0; k < 30 && screen.queryByTestId("guide"); k++) {
      const key = keyAt();
      seen.push(key);
      press(key === "done" ? w.done : w.next);
      if (key !== "done") expect(keyAt()).not.toBe(key);               // on at once: nothing to wait for
    }
    expect(screen.queryByTestId("guide")).toBeNull();
    expect(store.v).toBe(WALKTHROUGH_VERSION);                           // seen, and no resume line left
    expect(seen).toEqual(GUIDE_STEPS.map((s) => s.key));                 // every one, none passed
  });

  it("the card's buttons are Skip, Back and Next, nothing else", () => {
    render(<Harness store={memory(atStep("days"))} reg={useRegistry(EVERY)} out={{}} />);
    expect(screen.getAllByRole("button").map((b) => b.textContent)).toEqual([w.skip, w.back, w.next]);
  });

  it("is skippable at any step; a skip counts as seen and forgets the step", () => {
    const store = memory(atStep("map"));
    render(<Harness store={store} reg={useRegistry()} out={{}} />);
    expect(keyAt()).toBe("map");                                        // resumed where it was left
    act(() => { fireEvent.click(screen.getByTestId("guide-skip")); });
    expect(screen.queryByTestId("guide")).toBeNull();
    expect([store.v, guideResumeAt(store.v)]).toEqual([WALKTHROUGH_VERSION, null]);
  });

  it("a step kept from an older walk (an old device's resume line) starts this walk from the top", () => {
    render(<Harness store={memory("walkthrough-5\nguide-at:obstacles")} reg={useRegistry()} out={{}} />);
    expect(keyAt()).toBe("search");
    cleanup();
    render(<Harness store={memory("guide-at:pick")} reg={useRegistry()} out={{}} />);
    expect(keyAt()).toBe("search");
  });

  it("keeps the step it is on, so a walk left when the app closed resumes there", () => {
    const store = memory();
    render(<Harness store={store} reg={useRegistry()} out={{}} />);
    press(w.next);
    expect(guideResumeAt(store.v)).toBe("map");
  });

  it("Back steps back; from the first step it leaves the walk", () => {
    const store = memory(atStep("days"));
    render(<Harness store={store} reg={useRegistry()} out={{}} />);
    press(w.back);
    expect(keyAt()).toBe("map");
    press(w.back);
    expect(keyAt()).toBe("search");
    act(() => { fireEvent.click(screen.getByText(w.skip)); });
    expect(screen.queryByTestId("guide")).toBeNull();
  });

  it("(T984) a step whose control is not drawn shows its card with nothing highlighted, never waiting or passed", async () => {
    const drawn = Object.fromEntries(Object.entries(EVERY).filter(([k]) => k !== "days"));
    render(<Harness store={memory(atStep("days"))} reg={useRegistry(drawn)} out={{}} />);
    await act(async () => { await new Promise((r) => setTimeout(r, 500)); });        // past any wait a missing control could cause
    expect([keyAt(), screen.queryByTestId("guide-ring")]).toEqual(["days", null]);
    // still dimmed whole: the tour never lets a touch through to the app
    expect(screen.getAllByTestId("guide-dim").length).toBe(1);
  });

  it("highlights the control a step is about, and keeps the card beside it", async () => {
    const reg = useRegistry({ search: [14, 10, 362, 46] });
    render(<Harness store={memory(atStep("search"))} reg={reg} out={{}} />);
    const ring = await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    expect([ring.style.left, ring.style.top, ring.style.width]).toEqual(["6px", "2px", "378px"]);
    expect(parseFloat(screen.getByTestId("guide-step-search").style.top)).toBeGreaterThan(10 + 46);
  });

  it("(T984) the dim takes every touch, the hole round the control too: nothing reaches the app", async () => {
    render(<Harness store={memory(atStep("search"))} reg={useRegistry({ search: [14, 10, 362, 46] })} out={{}} />);
    await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    for (const el of [...screen.getAllByTestId("guide-dim"), screen.getByTestId("guide-hole")]) {
      expect((el as unknown as { onstartshouldsetresponder?: unknown }).onstartshouldsetresponder ?? true).toBeTruthy();
    }
    expect(screen.getAllByTestId("guide-dim").length).toBe(4);           // four panes round a hole, and the hole's own
  });

  it("(T456) the card follows a highlight that moves (a sheet sliding, the page scrolling) and stays off it", async () => {
    const at = { y: 120 };
    const reg = useRegistry();
    reg.set("map", { measureInWindow: (cb) => cb(16, at.y, 300, 120) });
    render(<Harness store={memory(atStep("map"))} reg={reg} out={{}} />);
    await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    /** the card's box (jsdom lays out nothing: its height is 0, so its top-left corner is what can land in the ring) */
    const apart = () => {
      const c = screen.getByTestId("guide-step-map").style, r = screen.getByTestId("guide-ring").style;
      const [cl, ct, cw] = [parseFloat(c.left), parseFloat(c.top), parseFloat(c.width)];
      const [rl, rt, rw, rh] = [parseFloat(r.left), parseFloat(r.top), parseFloat(r.width), parseFloat(r.height)];
      return cl >= rl + rw || cl + cw <= rl || ct >= rt + rh || ct <= rt;
    };
    expect(apart()).toBe(true);
    const was = screen.getByTestId("guide-step-map").style.top;
    at.y = 520;                                                            // scrolled: the control is now low on the screen
    await waitFor(() => expect(parseFloat(screen.getByTestId("guide-ring").style.top)).toBeGreaterThan(500), { timeout: WAIT });
    expect(apart()).toBe(true);
    expect(screen.getByTestId("guide-step-map").style.top).not.toBe(was);   // it moved with the ring
  });

  it("(T824, owner: \"glitchy with keyboard\") a phone browser's keyboard: the card keeps above it, and goes back down after", async () => {
    // iOS Safari's keyboard shrinks the visual viewport, not the page: the card at the foot was left under the keyboard
    const saved = [window.innerWidth, window.innerHeight];
    Object.assign(window, { innerWidth: 390, innerHeight: 844 });
    act(() => { window.dispatchEvent(new Event("resize")); });
    const vv = Object.assign(new EventTarget(), { height: 844, offsetTop: 0, scale: 1 });
    Object.defineProperty(window, "visualViewport", { value: vv, configurable: true });
    try {
      // "days" with its control not drawn: the card sits at the foot, over the tab bar (64)
      render(<Harness store={memory(atStep("days"))} reg={useRegistry()} out={{}} />);
      const top = () => parseFloat(screen.getByTestId("guide-step-days").style.top);
      await waitFor(() => expect(top()).toBe(844 - 64 - SPACE.s3), { timeout: WAIT });
      act(() => { Object.assign(vv, { height: 500 }); vv.dispatchEvent(new Event("resize")); });
      expect(top()).toBe(500 - SPACE.s3);                                   // above the keyboard's top edge, at once
      act(() => { Object.assign(vv, { height: 844 }); vv.dispatchEvent(new Event("resize")); });
      expect(top()).toBe(844 - 64 - SPACE.s3);
    } finally {
      delete (window as unknown as { visualViewport?: unknown }).visualViewport;
      Object.assign(window, { innerWidth: saved[0], innerHeight: saved[1] });
      act(() => { window.dispatchEvent(new Event("resize")); });
    }
  });

  it("is a non-modal dialog whose step is said as it comes up", () => {
    render(<Harness store={memory()} reg={useRegistry()} out={{}} />);
    const card = screen.getByTestId("guide-step-search");
    expect([card.getAttribute("role"), card.getAttribute("aria-modal")]).toEqual(["dialog", "false"]);
    expect(screen.getByText(GUIDE_STEPS[0]!.title).closest("[aria-live='polite']")).toBeTruthy();
  });

  it("stays down once seen, and with no store; re-opens on request at the first step", () => {
    const out: { guide?: Guide } = {};
    render(<Harness store={memory(WALKTHROUGH_VERSION)} reg={useRegistry()} out={out} />);
    expect(screen.queryByTestId("guide")).toBeNull();
    act(() => out.guide!.reopen());
    expect(keyAt()).toBe("search");
    cleanup();
    render(<Harness store={null} reg={useRegistry()} out={{}} />);
    expect(screen.queryByTestId("guide")).toBeNull();
  });
});

/**
 * (T456, the tester's screenshot: the day chips under the card) THE CARD NEVER MEETS THE RING, at a phone's size and a
 * desktop's, for every place and size a ring can take that leaves room for the card at all.
 */
describe("(T456) the card never covers what it is about", () => {
  const meets = (a: Rect, b: Rect) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
  const SIZES = [{ name: "phone", win: { w: 390, h: 844 }, room: { top: 8, bottom: 64 } },
                 { name: "phone, water page", win: { w: 390, h: 844 }, room: { top: 8, bottom: 0 } },
                 // (T824) the keyboard up: what it covers is kept at the foot
                 { name: "phone, keyboard up", win: { w: 390, h: 844 }, room: { top: 8, bottom: footRoom(64, 508, { y: 0, h: 844 }) } },
                 { name: "desktop", win: { w: 1280, h: 800 }, room: { top: 68, bottom: 0 } }];
  for (const { name, win, room } of SIZES) it(`${name}: every ring, every card height`, () => {
    let n = 0;
    for (const cardH of [150, 220, 300]) for (const w0 of [44, 120, 240, win.w - 32]) for (const h of [24, 44, 120, 260])
      for (let y = room.top; y + h <= win.h - room.bottom; y += 13) for (const x of [0, 16, (win.w - w0) / 2, win.w - w0 - 16]) {
        const ring = { x, y, w: w0, h }, card = { w: Math.min(360, win.w - 32), h: cardH };
        const p = placeGuideCard(ring, win, card, room);
        // a side with room for the card (over it, under it, or beside it on a desktop); else the page scrolls first
        const roomy = win.h - room.bottom - (y + h) >= cardH + CARD_GAP || y - room.top >= cardH + CARD_GAP
          || (win.w >= 900 && (win.w - (x + w0) >= card.w + CARD_GAP || x >= card.w + CARD_GAP));
        if (!roomy) continue;
        n++;
        expect(p.covers, `${JSON.stringify(ring)} card ${cardH}`).toBe(false);
        expect(meets({ x: p.left, y: p.top, w: card.w, h: cardH }, ring), `${JSON.stringify(ring)} card ${cardH}`).toBe(false);
        expect(p.top).toBeGreaterThanOrEqual(room.top);
        expect(p.top + cardH).toBeLessThanOrEqual(win.h - room.bottom);
      }
    expect(n).toBeGreaterThan(1000);
  });
  it("a ring under a fixed header, or below the page's foot, is scrolled into the page's view with room for the card", () => {
    const win = { w: 390, h: 844 }, room = { top: 8, bottom: 0 }, view = { x: 0, y: 240, w: 390, h: 604 }, card = { w: 358, h: 220 };
    for (const ring of [{ x: 16, y: 100, w: 300, h: 60 }, { x: 16, y: 900, w: 300, h: 80 }, { x: 16, y: 700, w: 300, h: 200 }]) {
      const dy = revealBy(ring, view, win, card, room);
      const after = { ...ring, y: ring.y - dy };
      expect(after.y, JSON.stringify(ring)).toBeGreaterThanOrEqual(view.y);
      expect(after.y + after.h).toBeLessThanOrEqual(view.y + view.h);
      expect(placeGuideCard(after, win, card, room).covers).toBe(false);
      expect(revealBy(after, view, win, card, room)).toBe(0);              // once there, it stays
    }
  });
});

describe("coach-mark geometry (T257)", () => {
  it("keeps the spotlight inside the window, ring and all (a tab at the foot)", () => {
    expect(insideWindow({ x: 174, y: 788, w: 132, h: 60 }, { w: 390, h: 844 })).toEqual({ x: 174, y: 788, w: 132, h: 54 });
    expect(insideWindow({ x: -6, y: -6, w: 50, h: 50 }, { w: 390, h: 844 })).toEqual({ x: 2, y: 2, w: 42, h: 42 });
  });
  it("unions the rects it is given", () => {
    expect(unionRect([{ x: 10, y: 10, w: 10, h: 10 }, { x: 30, y: 5, w: 10, h: 10 }])).toEqual({ x: 10, y: 5, w: 30, h: 15 });
    expect(unionRect([])).toBeNull();
  });
});

describe("the Help page (T248)", () => {
  it("draws every section and row of helpVM, in order", () => {
    render(<HelpPage palette={LIGHT} theme="light" />);
    expect(screen.getAllByTestId(/^help-section-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(helpVM("web").map((s) => `help-section-${s.key}`));
    expect(screen.getAllByTestId(/^help-row-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(helpVM("web").flatMap((s) => s.rows.map((r) => `help-row-${r.key}`)));
    // (T873) no intro paragraph: the page opens on the map's key
    expect(screen.getAllByRole("heading")[0]!.textContent).toBe(HELP_WORDS.sections.colours);
  });

  it("swatches wear the map's own colours in each theme — resolved, not copied", () => {
    for (const [palette, theme] of [[LIGHT, "light"], [DARK, "dark"]] as const) {
      const { unmount } = render(<HelpPage palette={palette} theme={theme} />);
      const closed = screen.getByTestId("help-row-status-closed").querySelector("div[style*='height: 4px']") as HTMLElement;
      expect(closed.style.backgroundColor).toBe(hexToRgb(palette.waterStatus.closed));
      expect(screen.getByTestId("help-tidal").style.backgroundColor).toBe(hexToRgb(tidalColours(theme).fill));
      expect(screen.getByTestId("help-notfishing").style.backgroundColor).toBe(hexToRgb(notFishingColours(theme).fill));
      expect(screen.getByTestId("help-edge-dashed")).toBeTruthy();
      expect(screen.getByTestId("help-edge-solid")).toBeTruthy();
      unmount();
    }
  });

  it("(T873) the map's key, then offline and updates as one-line facts, then the rest (the web's words here)", () => {
    let off = 0;
    render(<HelpPage palette={LIGHT} theme="light" onOffline={() => { off++; }} />);
    const order = screen.getAllByTestId(/^help-(section-|facts$)/).map((e) => e.getAttribute("data-testid"));
    expect(order).toEqual(["help-section-colours", "help-section-lines", "help-section-salmon", "help-section-runs",
                           "help-facts", "help-section-sync", "help-section-badges"]);
    for (const t of HELP_WORDS.facts.web) expect(screen.getByText(t)).toBeTruthy();
    expect(screen.queryByTestId("help-offline")).toBeNull();            // a browser has no Offline maps page
    expect(screen.queryByTestId(/^help-tour-/)).toBeNull();               // (owner 2026-11-08) the old mode hints are gone
    expect(off).toBe(0);
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

describe("(T824) the room the keyboard takes", () => {
  it("keeps what the keyboard covers of the layer, or the tab bar's room, whichever is more", () => {
    expect(footRoom(64, null, { y: 0, h: 844 })).toBe(64);                  // no keyboard
    expect(footRoom(64, 508, { y: 0, h: 844 })).toBe(336);                  // iOS: the window does not shrink for it
    expect(footRoom(64, 820, { y: 0, h: 844 })).toBe(64);                   // a keyboard lower than the tab bar's top
    expect(footRoom(0, 600, { y: 0, h: 600 })).toBe(0);                     // Android's resize: the layer ends at the keyboard
    expect(footRoom(0, 508, { y: 47, h: 797 })).toBe(336);                  // a layer under the status bar, in window points
  });
});
