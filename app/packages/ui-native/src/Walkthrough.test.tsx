/**
 * (T293) The guided walkthrough — one hands-on walk on a real water: once per device, skippable at any step, resumed where
 * it was left, re-openable; DO steps move on when the app gets there and "Show me" does them; Back steps back and out;
 * and (T248) the Help page drawn from the legend tokens, with its short pages.
 *
 * (T456) The rework: the highlighted control moves the walk on (the page's own "‹ Map" included, never a back of the
 * walk's), the card never meets the highlight (phone and desktop, a highlight that moves), every way off a step's screen
 * pauses the walk behind a chip that puts the app back, and the words name only controls drawn inside the highlight.
 *
 * (T824, owner on iPhone 2026-11-16) Ten steps, not 41 (the tests that walked the closed stretch, Layers and the spot
 * steps went with them); the card keeps above the keyboard.
 *
 * (T842, owner on 0.7.12) Six steps (the "Back to the map" and Trout / Salmon tests went with theirs: the last card says
 * them); the highlight rides its page's scroll (an Animated value), never a re-measure a step behind; the pick step
 * closes the keyboard (`Walkthrough.t842.test.tsx` has the card against the row with real card sizes).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { useState, type ReactNode } from "react";
import { act, cleanup, fireEvent, render, renderHook, screen, waitFor, within } from "@testing-library/react";
import { Animated, Keyboard } from "react-native";
import { GLANCE_WORDS, GUIDE_WATER, HELP_WORDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, helpVM, type GlanceRowVM } from "@app/core";
import { BackLayerContext } from "@app/ui";
import { notFishingColours, tidalColours } from "@app/map";
import { HelpPage } from "./menu/HelpPage";
import { GUIDE_STEPS, GuideCard, PAUSE_AFTER_MS, useGuide, type Guide, type GuideActs, type GuideFacts,
         type WalkthroughStore } from "./Walkthrough";
import { CoachContext, insideWindow, unionRect, useCoachRegistry, type CoachRegistry, type Rect } from "./CoachMarks";
import { CARD_GAP, footRoom, placeGuideCard, revealBy } from "./guideLayout";
import { GuideControlContext, useGuideControlHost } from "./guideControls";
import { ViewTabs } from "./Faces";
import { OtherFish } from "./regs/KeepBlocks";
import { makeReleaseSource } from "@app/data/release";
import { openRegsFixtureWithLines, sectionNames } from "@app/data/release/node";
import { openRegsStore } from "@app/data/regs";
import type { ItemId } from "@app/data";
const { WaterScreen } = await import("./WaterScreen");
// (T402) a ceiling, not a delay: waitFor returns as soon as it holds; 2 s ran out under the full parallel check
const WAIT = 10_000;
import { DARK, LIGHT, SPACE } from "./theme";

afterEach(cleanup);
const memory = (v: string | null = null): WalkthroughStore & { v: string | null } => {
  const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } };
  return s;
};
const w = WALKTHROUGH_WORDS;
const WATER = { item: "w-vedder", name: "Vedder River" };
/** the pretend app: `away` is another tab in front (the walk's screens are the Map tab's and a water's) */
interface Pretend extends GuideFacts { readonly away?: boolean }
const BROWSE: Pretend = { water: WATER, searching: false, query: "", selected: null, open: null, dayPicked: false, controls: {} };
const REGS: Pretend = { ...BROWSE, open: { item: WATER.item, view: "regs" } };
const COND: Pretend = { ...BROWSE, open: { item: WATER.item, view: "conditions" } };
const keyAt = () => screen.getByTestId(/^guide-step-/).getAttribute("data-testid")!.slice("guide-step-".length);
const press = (label: string) => act(() => { fireEvent.click(screen.getByText(label)); });
const stepOf = (k: string) => GUIDE_STEPS.find((s) => s.key === k)!;
const sleep = (ms: number) => act(() => new Promise<void>((d) => setTimeout(d, ms)));
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

/** The walk over a pretend app: the acts move the facts as the Shell's route would. */
function Harness({ store, start = BROWSE, blocked = false, reg, log, out }: {
  store: WalkthroughStore | null; start?: Pretend; blocked?: boolean; reg: CoachRegistry; log: string[];
  out: { guide?: Guide; set?: (f: (x: Pretend) => Pretend) => void };
}) {
  const [f, setF] = useState(start);
  const acts: GuideActs = {
    search: () => { log.push("search"); setF((x) => ({ ...x, away: false, open: null, searching: true, query: WATER.name })); },
    select: () => { log.push("select"); setF((x) => ({ ...x, selected: WATER.item })); },
    openWater: (v) => { log.push(`open:${v}`); setF((x) => ({ ...x, away: false, searching: false, open: { item: WATER.item, view: v } })); },
    view: (v) => { log.push(`view:${v}`); setF((x) => ({ ...x, open: x.open && { ...x.open, view: v } })); },
    toMap: () => { log.push("toMap"); setF((x) => ({ ...x, away: false, open: null, searching: false })); },
    pickDay: () => { log.push("day"); setF((x) => ({ ...x, dayPicked: true })); },
    control: (k, v) => { log.push(`${k}=${String(v)}`); setF((x) => ({ ...x, controls: { ...x.controls, [k]: v } })); return true; },
  };
  const facts: GuideFacts = { ...f, onMap: !f.away && !f.open && !f.searching };
  const guide = useGuide(store, blocked, facts, acts);
  out.guide = guide; out.set = setF;
  return <GuideCard guide={guide} facts={facts} acts={acts} palette={LIGHT} registry={reg} top={60} bottom={64} />;
}

describe("the guided walkthrough (T293, T456, T824, T842)", { timeout: 30_000 }, () => {
  it("(T842) is the owner's six steps, in his order: search, open, open or closed, a day, the river, you're all set", () => {
    // (T824) was 29 steps here (the web's), 41 on a phone, then 10; (T842, owner 2026-11-17, his pick) six
    expect([...new Set(GUIDE_STEPS.map((s) => s.phase))]).toEqual(["find", "rules", "conditions", "end"]);
    expect(GUIDE_STEPS.map((s) => s.key)).toEqual(["search", "pick", "answer", "day", "conditions", "done"]);
    expect(GUIDE_STEPS.map((s) => s.title)).toEqual(["Search for it", "Open its page", "Open or closed", "Pick another day",
                                                     "How is the river?", "You're all set"]);
  });

  it("(T456) no step only describes a control: a step that highlights a control asks for its tap, or reads a line", () => {
    // the reading steps highlight lines, not controls; every other highlighted step is a DO step the reader's tap moves on
    const reading = ["answer"];
    for (const s of GUIDE_STEPS.filter((x) => x.targets.length > 0 && !reading.includes(x.key))) expect(s.kind, s.key).toBe("do");
    // a step that names a control by its label says that label
    for (const s of GUIDE_STEPS) for (const n of s.names ?? []) expect(s.body, `${s.key}: ${n}`).toContain(n);
  });

  it("opens on a new device once nothing blocks it, names the water, and walks to the end with real taps and Show me", async () => {
    const store = memory(), log: string[] = [], out: { guide?: Guide } = {};
    const reg = useRegistry(EVERY);
    const { rerender } = render(<Harness store={store} blocked reg={reg} log={log} out={out} />);
    expect(screen.queryByTestId("guide")).toBeNull();                 // the first-run notice is up
    rerender(<Harness store={store} reg={reg} log={log} out={out} />);
    expect(keyAt()).toBe("search");
    expect(screen.getByTestId("guide-body").textContent).toContain("the Vedder River");
    expect(screen.getByTestId("guide-body").textContent).toContain("highlights what to tap");
    expect(screen.getByTestId("guide-count").textContent).toBe(`${w.phases.find} · ${w.step(1, GUIDE_STEPS.length)}`);
    const seen: string[] = [];
    for (let k = 0; k < 30 && screen.queryByTestId("guide"); k++) {
      await act(async () => {});                                          // settle the last move's effects first: under load a DO step read early offers Next, not Show me
      const key = keyAt(), s = stepOf(key);
      seen.push(key);
      const label = screen.queryByText(w.showMe) ? w.showMe : screen.queryByText(w.done) ? w.done : w.next;
      press(label);
      if (label === w.showMe) await waitFor(() => expect(keyAt()).not.toBe(key), { timeout: WAIT });
      else if (s.key !== "done") expect(keyAt()).not.toBe(key);
    }
    expect(screen.queryByTestId("guide")).toBeNull();
    expect(store.v).toBe(WALKTHROUGH_VERSION);                           // seen, and no resume line left
    expect(seen).toEqual(GUIDE_STEPS.map((s) => s.key));                 // every one of the six, none passed
    // each DO step done by Show me, through the app's own moves
    // (T842) the first step is the search itself: the walk starts with the map in front for it
    expect(log).toEqual(["toMap", "search", "open:regs", "day", "view:conditions"]);
  }, 40_000);

  it("a DO step done by the reader ticks, then moves on by itself", async () => {
    const out: { guide?: Guide; set?: (f: (x: Pretend) => Pretend) => void } = {};
    render(<Harness store={memory(atStep("search"))} reg={useRegistry()} log={[]} out={out} />);
    expect(keyAt()).toBe("search");
    act(() => out.set!((x) => ({ ...x, searching: true, query: "ve" })));
    expect(screen.getByText(`${w.tick} ${stepOf("search").title}`)).toBeTruthy();
    await waitFor(() => expect(keyAt()).toBe("pick"), { timeout: WAIT });
  });

  it("(T842, owner: \"the keyboard covers vedder\") the pick step closes the keyboard: reached from the search, resumed, or stepped back to", async () => {
    const dismiss = vi.spyOn(Keyboard, "dismiss");
    try {
      const out: { guide?: Guide; set?: (f: (x: Pretend) => Pretend) => void } = {};
      render(<Harness store={memory(atStep("search"))} reg={useRegistry()} log={[]} out={out} />);
      expect(dismiss).not.toHaveBeenCalled();                            // typing in search: the keyboard stays
      act(() => out.set!((x) => ({ ...x, searching: true, query: "ved" })));
      await waitFor(() => expect(keyAt()).toBe("pick"), { timeout: WAIT });
      expect(dismiss).toHaveBeenCalledTimes(1);
      cleanup(); dismiss.mockClear();
      render(<Harness store={memory(atStep("pick"))} reg={useRegistry()} log={[]} out={{}} start={{ ...BROWSE, searching: true, query: "ved" }} />);
      expect([keyAt(), dismiss.mock.calls.length]).toEqual(["pick", 1]);
      cleanup(); dismiss.mockClear();
      render(<Harness store={memory(atStep("answer"))} reg={useRegistry()} log={[]} out={{}} start={REGS} />);
      expect(dismiss).not.toHaveBeenCalled();                            // a page with no list: nothing closed
      press(w.back);
      expect([keyAt(), dismiss.mock.calls.length]).toEqual(["pick", 1]);
    } finally { dismiss.mockRestore(); }
  });

  it("is skippable at any step; a skip counts as seen and forgets the step", () => {
    const store = memory(atStep("answer"));
    render(<Harness store={store} reg={useRegistry()} log={[]} out={{}} start={REGS} />);
    expect(keyAt()).toBe("answer");                                     // resumed where it was left
    act(() => { fireEvent.click(screen.getByTestId("guide-skip")); });
    expect(screen.queryByTestId("guide")).toBeNull();
    expect([store.v, guideResumeAt(store.v)]).toEqual([WALKTHROUGH_VERSION, null]);
  });

  it("a step kept from an older walk (an old device's resume line) starts this walk from the top", () => {
    render(<Harness store={memory("walkthrough-5\nguide-at:obstacles")} reg={useRegistry()} log={[]} out={{}} />);
    expect(keyAt()).toBe("search");
    cleanup();
    // (T842) the 10-step walk's "answer" was its step 4; this walk's is step 3: not resumed, step 1
    render(<Harness store={memory("guide-at:answer")} reg={useRegistry()} log={[]} out={{}} start={REGS} />);
    expect(keyAt()).toBe("search");
  });

  it("keeps the step it is on, so a walk left when the app closed resumes there", () => {
    const store = memory();
    render(<Harness store={store} reg={useRegistry()} log={[]} out={{}} />);
    press(w.next);
    expect(guideResumeAt(store.v)).toBe("pick");
  });

  it("Back steps back, putting the app where the step expects it; from the first step it leaves the walk", () => {
    const store = memory(atStep("done")), log: string[] = [];
    render(<Harness store={store} reg={useRegistry()} log={log} out={{}} start={COND} />);
    expect(keyAt()).toBe("done");
    press(w.back);
    expect(keyAt()).toBe("conditions");
    expect(screen.getByText(`${w.tick} ${stepOf("conditions").title}`)).toBeTruthy();   // done already
    press(w.back);
    expect(keyAt()).toBe("day");
    expect(log).toEqual(["view:regs"]);                                 // its Regulations back in front
    cleanup();
    const first = memory();
    const out2: { guide?: Guide } = {};
    render(<Harness store={first} reg={useRegistry()} log={[]} out={out2} />);
    act(() => out2.guide!.back());                                      // the card's Back, on the first step
    expect([screen.queryByTestId("guide"), first.v]).toEqual([null, WALKTHROUGH_VERSION]);
  });

  it("(T456) the walk is not a layer on the app's back: the system back and a page's ‹ do their own job", () => {
    const layers: (() => void)[] = [];
    const stack = { register: (close: () => void) => { layers.push(close); return () => { layers.splice(layers.indexOf(close), 1); }; },
                    back: () => { layers.at(-1)?.(); return true; } };
    render(<BackLayerContext.Provider value={stack}>
      <Harness store={memory(atStep("answer"))} reg={useRegistry(EVERY)} log={[]} out={{}} start={REGS} />
    </BackLayerContext.Provider>);
    expect(keyAt()).toBe("answer");
    expect(layers).toHaveLength(0);
  });

  it("(T824) a step whose control is not drawn shows its card with nothing highlighted, never passed unseen", async () => {
    const drawn = Object.fromEntries(Object.entries(EVERY).filter(([k]) => k !== "regs:date"));
    render(<Harness store={memory(atStep("day"))} reg={useRegistry(drawn)} log={[]} out={{}} start={REGS} />);
    await sleep(2400);                                                    // past the wait an optional step gets
    expect([keyAt(), screen.queryByTestId("guide-ring")]).toEqual(["day", null]);
    // (T842) nothing to cut out: no dim either, so the reader can still reach the control some other way
    expect(screen.queryByTestId("guide-dim")).toBeNull();
  });

  it("highlights the control a step is about, and keeps the card beside it", async () => {
    const reg = useRegistry({ search: [14, 10, 362, 46] });
    render(<Harness store={memory(atStep("search"))} reg={reg} log={[]} out={{}} />);
    const ring = await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    expect([ring.style.left, ring.style.top, ring.style.width]).toEqual(["6px", "2px", "378px"]);
    expect(parseFloat(screen.getByTestId("guide-step-search").style.top)).toBeGreaterThan(10 + 46);
  });

  it("(T842, owner: \"the box has a weird delay tracking the box\") the highlight rides its page's scroll value: no re-measure while it moves", async () => {
    // it was re-measured on each scroll event (a round trip each): it trailed the row it rings
    const y = new Animated.Value(0), told = new Set<() => void>();
    const scroll = { off: 0, at: 0 };
    const page = { by: () => {}, view: async () => null, y, offset: () => scroll.off, scrolledAt: () => scroll.at,
                   onScroll: (f: () => void) => { told.add(f); return () => { told.delete(f); }; } };
    let measured = 0;
    const reg = renderHook(() => useCoachRegistry()).result.current;
    reg.set("search", { measureInWindow: (cb) => { measured += 1; cb(14, 300 - scroll.off, 362, 46); } }, null, page);
    render(<Harness store={memory(atStep("search"))} reg={reg} log={[]} out={{}} />);
    const ring = await screen.findByTestId("guide-ring", {}, { timeout: 2000 });
    const spot = screen.getByTestId("guide-spot");
    expect(ring.style.top).toBe(`${300 - 8}px`);
    expect(told.size).toBe(1);                                            // the walk hears the page move
    const before = measured;
    for (const off of [40, 80, 120]) {
      // one scroll frame: the page's own scroll sets its value (the UI thread's job on a phone) and tells the walk
      act(() => { scroll.off = off; scroll.at = Date.now(); y.setValue(off); for (const f of told) f(); });
      expect(spot.style.transform, `scrolled ${off}`).toBe(`translateY(${-off}px)`);   // moved with it, in the same frame
      expect(ring.style.top).toBe(`${300 - 8}px`);                        // not re-placed
      await sleep(40);                                                    // a check's tick passes: the page is moving
    }
    expect(measured, "measured while the page moved").toBe(before);
    await sleep(300);                                                     // the page stops: checked once, where it was carried
    expect(measured).toBeGreaterThan(before);
    expect([ring.style.top, spot.style.transform]).toEqual([`${300 - 8}px`, "translateY(-120px)"]);
  });

  it("(T824) after the keyboard (or anything that moves things), a second of quick looks: the highlight is on its control within frames", async () => {
    const at = { y: 10 };
    const reg = useRegistry();
    reg.set("search", { measureInWindow: (cb) => cb(14, at.y, 362, 46) });
    render(<Harness store={memory(atStep("search"))} reg={reg} log={[]} out={{}} />);
    await screen.findByTestId("guide-ring", {}, { timeout: 2000 });
    at.y = 300;                                                           // the step just came up: a sliding page moves it
    // (T842) hidden while it moves, drawn where two checks two frames apart agree
    await waitFor(() => expect(screen.getByTestId("guide-ring").style.top).toBe(`${300 - 8}px`), { timeout: 300 });
  });

  it("(T466) a highlight inside a scrolled page at phone size: the page is scrolled once, and the highlight follows it there", async () => {
    // a 390 × 844 phone: the gauge's headline is laid out below the page's visible body
    const win = { w: 390, h: 844 };
    const saved = [window.innerWidth, window.innerHeight];
    Object.assign(window, { innerWidth: win.w, innerHeight: win.h });
    act(() => { window.dispatchEvent(new Event("resize")); });
    try {
      const scroll = { y: 0 }, asked: number[] = [];
      const page = { by: (dy: number) => { asked.push(dy); scroll.y = Math.max(0, scroll.y + dy); },
                     view: async () => ({ x: 0, y: 300, w: 390, h: 480 }) };          // the page, under its header
      const reg = renderHook(() => useCoachRegistry()).result.current;
      reg.set("regs:answer", { measureInWindow: (cb) => cb(16, 1010 - scroll.y, 358, 96) }, null, page);
      render(<Harness store={memory(atStep("answer"))} reg={reg} log={[]} out={{}} start={REGS} />);
      expect(screen.getByTestId("guide-step-answer")).toBeTruthy();
      await waitFor(() => expect(asked.length).toBeGreaterThan(0), { timeout: WAIT });
      const ring = await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
      await waitFor(() => expect(parseFloat(ring.style.top)).toBeGreaterThanOrEqual(300 - 8), { timeout: WAIT });
      const top = parseFloat(ring.style.top), h = parseFloat(ring.style.height);
      expect(top + h).toBeLessThanOrEqual(300 + 480 + 8);
      expect(scroll.y).toBeGreaterThan(0);                                   // scrolled DOWN to it, never left off-screen
      await sleep(600);
      expect(asked.length, "one scroll, not one a frame").toBe(1);
    } finally {
      Object.assign(window, { innerWidth: saved[0], innerHeight: saved[1] });
      act(() => { window.dispatchEvent(new Event("resize")); });
    }
  });

  it("(T456) the card follows a highlight that moves (a sheet sliding, the page scrolling) and stays off it", async () => {
    const at = { y: 120 };
    const reg = useRegistry();
    reg.set("regs:answer", { measureInWindow: (cb) => cb(16, at.y, 300, 120) });
    render(<Harness store={memory(atStep("answer"))} reg={reg} log={[]} out={{}} start={REGS} />);
    await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    /** the card's box (jsdom lays out nothing: its height is 0, so its top-left corner is what can land in the ring) */
    const apart = () => {
      const c = screen.getByTestId("guide-step-answer").style, r = screen.getByTestId("guide-ring").style;
      const [cl, ct, cw] = [parseFloat(c.left), parseFloat(c.top), parseFloat(c.width)];
      const [rl, rt, rw, rh] = [parseFloat(r.left), parseFloat(r.top), parseFloat(r.width), parseFloat(r.height)];
      return cl >= rl + rw || cl + cw <= rl || ct >= rt + rh || ct <= rt;
    };
    expect(apart()).toBe(true);
    const was = screen.getByTestId("guide-step-answer").style.top;
    at.y = 520;                                                            // scrolled: the control is now low on the screen
    await waitFor(() => expect(parseFloat(screen.getByTestId("guide-ring").style.top)).toBeGreaterThan(500), { timeout: WAIT });
    expect(apart()).toBe(true);
    expect(screen.getByTestId("guide-step-answer").style.top).not.toBe(was);   // it moved with the ring
  });

  it("(T824, owner: \"glitchy with keyboard\") a phone browser's keyboard: the card keeps above it, and goes back down after", async () => {
    // iOS Safari's keyboard shrinks the visual viewport, not the page: the card at the foot was left under the keyboard
    const saved = [window.innerWidth, window.innerHeight];
    Object.assign(window, { innerWidth: 390, innerHeight: 844 });
    act(() => { window.dispatchEvent(new Event("resize")); });
    const vv = Object.assign(new EventTarget(), { height: 844, offsetTop: 0, scale: 1 });
    Object.defineProperty(window, "visualViewport", { value: vv, configurable: true });
    try {
      // "day" with its date not drawn: the card sits at the foot, over the tab bar (64)
      render(<Harness store={memory(atStep("day"))} reg={useRegistry()} log={[]} out={{}} start={REGS} />);
      const top = () => parseFloat(screen.getByTestId("guide-step-day").style.top);
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
    render(<Harness store={memory()} reg={useRegistry()} log={[]} out={{}} />);
    const card = screen.getByTestId("guide-step-search");
    expect([card.getAttribute("role"), card.getAttribute("aria-modal")]).toEqual(["dialog", "false"]);
    expect(screen.getByText(GUIDE_STEPS[0]!.title).closest("[aria-live='polite']")).toBeTruthy();
  });

  it("stays down once seen, and with no store; re-opens on request at the first step", () => {
    const out: { guide?: Guide } = {};
    render(<Harness store={memory(WALKTHROUGH_VERSION)} reg={useRegistry()} log={[]} out={out} />);
    expect(screen.queryByTestId("guide")).toBeNull();
    act(() => out.guide!.reopen());
    expect(keyAt()).toBe("search");
    cleanup();
    render(<Harness store={null} reg={useRegistry()} log={[]} out={{}} />);
    expect(screen.queryByTestId("guide")).toBeNull();
  });
});

/**
 * (T456) GUARDRAILS: every way off a step's own screen has ONE result. The step's own control (‹ Map on "Back to the map")
 * moves the walk on; any other — the page's ‹ or the system back (the water closes, the map in front), a tab (another tab
 * in front) — pauses the walk behind "Resume walkthrough", and the chip puts the app back on that step.
 */
describe("(T456) every exit from a step's screen", { timeout: 60_000 }, () => {
  /** where each step is shown */
  const startOf = (k: string): Pretend => {
    const s = stepOf(k);
    if (k === "pick") return { ...BROWSE, searching: true, query: "ved" };
    if (s.phase === "rules" || k === "conditions") return REGS;
    if (s.phase === "conditions") return COND;
    return BROWSE;
  };
  const EXITS: Record<string, (x: Pretend) => Pretend> = {
    "the page's ‹ / the system back": (x) => ({ ...x, open: null, searching: false }),
    "a tab": (x) => ({ ...x, away: true, open: null, searching: false }),
  };
  const withScreen = GUIDE_STEPS.filter((s) => s.key !== "done");
  for (const s of withScreen) for (const [exit, leave] of Object.entries(EXITS)) {
    it(`${s.key}: ${exit}`, async () => {
      const log: string[] = [], out: { guide?: Guide; set?: (f: (x: Pretend) => Pretend) => void } = {};
      const start = startOf(s.key);
      render(<Harness store={memory(atStep(s.key))} reg={useRegistry(EVERY)} log={log} out={out} start={start} />);
      expect(keyAt()).toBe(s.key);
      log.length = 0;
      act(() => out.set!(leave));
      if (exit !== "a tab" && ["search", "pick"].includes(s.key)) {
        // the map IS these steps' screen: the card stays up
        await sleep(PAUSE_AFTER_MS + 200);
        expect([screen.queryByTestId("guide-paused"), keyAt()]).toEqual([null, s.key]);
        return;
      }
      await waitFor(() => expect(screen.getByTestId("guide-paused")).toBeTruthy(), { timeout: WAIT });
      expect(screen.queryByTestId(/^guide-step-/)).toBeNull();            // the card stepped aside
      act(() => { fireEvent.click(screen.getByTestId("guide-resume")); });
      expect(keyAt()).toBe(s.key);                                        // the same step, the app put back for it
      expect(log.length, `${s.key} resume set nothing up`).toBeGreaterThan(0);
      await sleep(PAUSE_AFTER_MS + 200);
      expect(screen.queryByTestId("guide-paused")).toBeNull();            // and it stays up: its screen is back
    });
  }
  it("the chip's Skip ends the walk (seen)", async () => {
    const store = memory(atStep("answer")), out: { set?: (f: (x: Pretend) => Pretend) => void } = {};
    render(<Harness store={store} reg={useRegistry(EVERY)} log={[]} out={out} start={REGS} />);
    act(() => out.set!((x) => ({ ...x, open: null })));
    await waitFor(() => expect(screen.getByTestId("guide-paused")).toBeTruthy(), { timeout: WAIT });
    act(() => { fireEvent.click(screen.getByTestId("guide-end")); });
    expect([screen.queryByTestId("guide"), store.v]).toEqual([null, WALKTHROUGH_VERSION]);
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

/** (T456, rule 46) the walk's water in the regs fixture release, named by what it IS: its stream (key 356363467) from 0 m */
const lined = openRegsFixtureWithLines();
const source = makeReleaseSource(lined);
const sections = sectionNames(lined);
const DAY = new Date(2026, 10, 6, 12);
async function vedder(): Promise<ItemId> {
  const sid = await sections.stream(356363467, 0);
  const r = await (await (await openRegsStore(lined)).core()).get(
    "SELECT i.item_id, i.name FROM item_section s JOIN item i ON i.ord = s.ord WHERE s.sid = ? LIMIT 1", sid);
  expect(r?.name).toBe(GUIDE_WATER.name);
  return String(r!.item_id) as ItemId;
}

/**
 * (T456) EVERY STEP'S RING IS LAID OUT WHEN ITS CARD SHOWS: on the walk's water's Regulations (the release's own page), each
 * rules step's control is drawn and registered (the folds' steps are optional: a page with no folds passes them).
 */
describe("(T456) the Vedder River's Regulations draw every rules step's control", { timeout: 60_000 }, () => {
  it("each ring target is registered on the page", async () => {
    const reg = renderHook(() => useCoachRegistry()).result.current;
    render(<CoachContext.Provider value={reg}>
      <WaterScreen source={source} loader={lined} item={await vedder()} section={null} part={null} day={DAY} palette={LIGHT} />
    </CoachContext.Provider>);
    await waitFor(() => expect(screen.getByTestId("regs-view")).toBeTruthy(), { timeout: 30_000 });
    const rules = GUIDE_STEPS.filter((s) => s.phase === "rules");
    await waitFor(() => {
      const missing = rules.filter((s) => !s.optional).flatMap((s) => s.targets).filter((t) => reg.get(t) === null);
      expect(missing).toEqual([]);
    }, { timeout: 30_000 });
    // (T810 cutover) retired: `regs:rule` registered ("the release's group card: the rule a step reads"). No Keep tab draws
    // a group card since the cutover, and no step targets `regs:rule` (help.ts GUIDE_STEPS; the steps' own targets are
    // held just above); the trout fold's spot is `regs:trout` (the "(T810 O) the arrow folds" case below)
  });
});

/**
 * (T456, "+1d to +10d" named chips no longer drawn) A STEP NAMES ONLY WHAT IS IN ITS RING: each step's named labels are
 * inside the view its target registers — its words, or its spoken name — drawn by the real component.
 */
describe("(T456) the words name only controls drawn inside the highlight", { timeout: 60_000 }, () => {
  function Coached({ reg, children }: { reg: CoachRegistry; children: ReactNode }) {
    return <CoachContext.Provider value={reg}>{children}</CoachContext.Provider>;
  }
  /** the words and spoken names inside a registered view */
  const said = (reg: CoachRegistry, k: string) => {
    const el = reg.get(k) as unknown as HTMLElement | null;
    if (!el) return null;
    return [el.textContent ?? "", el.getAttribute("aria-label") ?? "",
            ...[...el.querySelectorAll("[aria-label]")].map((e) => e.getAttribute("aria-label") ?? "")].join(" | ").toLowerCase();
  };
  const fold = (key: string, name: string): { key: string; row: GlanceRowVM; body: () => ReactNode } =>
    ({ key, row: { thumb: "CT", name, value: null, tone: "plain", fold: true } as unknown as GlanceRowVM, body: () => null });
  const DRAWN: [string, () => ReactNode][] = [
    ["view:conditions", () => <ViewTabs palette={LIGHT} views={["regs", "conditions", "map"]} on="regs" onView={() => {}} />],
  ];
  it("each named label is inside its step's highlighted control", () => {
    const reg = renderHook(() => useCoachRegistry()).result.current;
    render(<Coached reg={reg}>{DRAWN.map(([k, el]) => <div key={k}>{el()}</div>)}</Coached>);
    const checked: string[] = [];
    for (const s of GUIDE_STEPS) {
      if (!s.names?.length) continue;
      const inside = s.targets.map((t) => said(reg, t)).filter((x): x is string => x !== null).join(" | ");
      expect(inside, `${s.key}: ${s.targets.join(", ")} not drawn`).not.toBe("");
      for (const n of s.names) expect(inside, `${s.key} names "${n}"`).toContain(n.toLowerCase());
      checked.push(s.key);
    }
    expect(checked).toEqual(["conditions"]);
  });
  it("the walk sees a screen's own control, and Show me sets it the reader's way", () => {
    const host = renderHook(() => useGuideControlHost());
    const reg = renderHook(() => useCoachRegistry()).result.current;
    render(<GuideControlContext.Provider value={host.result.current.sink}><Coached reg={reg}>
      <OtherFish rows={[]} keys={[]} palette={LIGHT} actions={{} as never} folds={[fold("salmon", "Salmon")]} />
    </Coached></GuideControlContext.Provider>);
    host.rerender();
    expect(host.result.current.values["regs:salmon"]).toBe(false);
    act(() => { host.result.current.set("regs:salmon", true); });
    host.rerender();
    expect(host.result.current.values["regs:salmon"]).toBe(true);
    expect(screen.getByText(GLANCE_WORDS.hide)).toBeTruthy();             // the fold opened, as a tap opens it
  });
  it("(T810 O) the arrow folds (dev switch): the walk still sees and sets the salmon and trout folds (the steps' words are T842's)", () => {
    const host = renderHook(() => useGuideControlHost());
    const reg = renderHook(() => useCoachRegistry()).result.current;
    const arrow = (key: string, name: string) => ({ ...fold(key, name), arrow: true });
    render(<GuideControlContext.Provider value={host.result.current.sink}><Coached reg={reg}>
      <OtherFish rows={[]} keys={[]} palette={LIGHT} actions={{} as never} folds={[arrow("salmon", "Salmon"), arrow("trout", "Trout and char")]} />
    </Coached></GuideControlContext.Provider>);
    host.rerender();
    expect([host.result.current.values["regs:salmon"], host.result.current.values["regs:trout"]]).toEqual([false, false]);
    act(() => { host.result.current.set("regs:trout", true); });
    host.rerender();
    expect([host.result.current.values["regs:trout"], screen.queryByText(GLANCE_WORDS.show), screen.queryByText(GLANCE_WORDS.hide)])
      .toEqual([true, null, null]);
    expect(within(screen.getByTestId("glance:trout")).getByRole("button").getAttribute("aria-expanded")).toBe("true");
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
      expect(screen.getByTestId("help-notfishing").style.backgroundColor).toBe(hexToRgb(notFishingColours(theme).fill));
      expect(screen.getByTestId("help-edge-dashed")).toBeTruthy();
      expect(screen.getByTestId("help-edge-solid")).toBeTruthy();
      unmount();
    }
  });

  it("(T293) has short pages first: the walkthrough, syncing and updates, offline (the web's words here)", () => {
    let off = 0;
    render(<HelpPage palette={LIGHT} theme="light" onOffline={() => { off++; }} />);
    expect(screen.getAllByTestId(/^help-page-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(["help-page-walkthrough", "help-page-updates", "help-page-offline"]);
    expect(screen.getByText(HELP_WORDS.updates.web[0]!)).toBeTruthy();
    expect(screen.getByText(HELP_WORDS.offline.web[0]!)).toBeTruthy();
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
