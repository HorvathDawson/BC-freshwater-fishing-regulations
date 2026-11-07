/**
 * (T293) The guided walkthrough — one hands-on walk on a real water: once per device, skippable at any step, resumed where
 * it was left, re-openable; DO steps move on when the app gets there and "Show me" does them; Back steps back and out;
 * and (T248) the Help page drawn from the legend tokens, with its short pages.
 *
 * (T456) The rework: the ringed control moves the walk on (the page's own "‹ Map" included, never a back of the walk's),
 * the card never meets the ring (phone and desktop, a ring that moves), every way off a step's screen pauses the walk
 * behind a chip that puts the app back, and the words name only controls drawn inside the ring.
 */
import { afterEach, describe, expect, it } from "vitest";
import { useState, type ReactNode } from "react";
import { act, cleanup, fireEvent, render, renderHook, screen, waitFor } from "@testing-library/react";
import { GLANCE_WORDS, GUIDE_WATER, HELP_WORDS, TAB_TOUR_WORDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, helpVM, layersVM,
         LAYER_DEFAULTS, type GlanceRowVM } from "@app/core";
import { BackLayerContext, horizonChips } from "@app/ui";
import { notFishingColours, tidalColours } from "@app/map";
import { HelpPage } from "./menu/HelpPage";
import { GUIDE_STEPS, GuideCard, PAUSE_AFTER_MS, useGuide, useTabTour, type Guide, type GuideActs, type GuideFacts,
         type WalkthroughStore } from "./Walkthrough";
import { CoachContext, insideWindow, placeCallout, unionRect, useCoachRegistry, type CoachRegistry, type Rect } from "./CoachMarks";
import { CARD_GAP, placeGuideCard, revealBy } from "./guideLayout";
import { GuideControlContext, useGuideControlHost } from "./guideControls";
import { ViewTabs, FaceBar } from "./Faces";
import { HorizonPicker } from "./HorizonPicker";
import { ChartControls } from "./ChartControls";
import { TabBar } from "./TabBar";
import { MapRail } from "./MapRail";
import { LayersSheet } from "./LayersSheet";
import { OtherFish } from "./regs/KeepBlocks";
import { makeReleaseSource } from "@app/data/release";
import { openRegsFixtureWithLines, sectionNames } from "@app/data/release/node";
import { openRegsStore } from "@app/data/regs";
import type { ItemId } from "@app/data";
const { WaterScreen } = await import("./WaterScreen");
// (T402) a ceiling, not a delay: waitFor returns as soon as it holds; 2 s ran out under the full parallel check
const WAIT = 10_000;
import { DARK, LIGHT } from "./theme";

afterEach(cleanup);
const memory = (v: string | null = null): WalkthroughStore & { v: string | null } => {
  const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } };
  return s;
};
const w = WALKTHROUGH_WORDS;
const WATER = { item: "w-vedder", name: "Vedder River" };
/** the pretend app: `away` is another tab in front (the walk's screens are the Map tab's and a water's) */
interface Pretend extends GuideFacts { readonly away?: boolean }
const BROWSE: Pretend = { water: WATER, searching: false, query: "", selected: null, open: null, adding: false, viewing: null, spots: 0,
                          layersOpen: false, ground: "ground.map", obstacles: false, layersWas: null, dayPicked: false, controls: {} };
const REGS: Pretend = { ...BROWSE, open: { item: WATER.item, view: "regs" } };
const COND: Pretend = { ...BROWSE, open: { item: WATER.item, view: "conditions" } };
const keyAt = () => screen.getByTestId(/^guide-step-/).getAttribute("data-testid")!.slice("guide-step-".length);
const press = (label: string) => act(() => { fireEvent.click(screen.getByText(label)); });
const stepOf = (k: string) => GUIDE_STEPS.find((s) => s.key === k)!;

function useRegistry(at: Record<string, [number, number, number, number]> = {}): CoachRegistry {
  const reg = renderHook(() => useCoachRegistry()).result.current;
  for (const [k, r] of Object.entries(at)) reg.set(k, { measureInWindow: (cb) => cb(...r) });
  return reg;
}
/** every control a step rings, drawn somewhere (a phone's places, roughly) */
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
    addSpot: () => { log.push("add"); setF((x) => ({ ...x, open: null, adding: true })); },
    cancelAdd: () => { log.push("cancel"); setF((x) => ({ ...x, adding: false })); },
    toSpots: () => { log.push("spots"); setF((x) => ({ ...x, viewing: null })); },
    openSpot: () => { log.push("openSpot"); setF((x) => ({ ...x, viewing: "s1" })); },
    openLayers: () => { log.push("layers"); setF((x) => ({ ...x, away: false, open: null, layersOpen: true, layersWas: x.layersWas ?? { ground: x.ground!, obstacles: !!x.obstacles } })); },
    otherGround: () => { log.push("ground"); setF((x) => ({ ...x, ground: x.ground === "ground.map" ? "ground.satellite" : "ground.map" })); },
    obstaclesOn: () => { log.push("obstacles"); setF((x) => ({ ...x, obstacles: true })); },
    restoreLayers: () => { log.push("restore"); setF((x) => ({ ...x, layersOpen: false, ground: x.layersWas?.ground ?? x.ground, obstacles: x.layersWas?.obstacles ?? x.obstacles, layersWas: null })); },
    toMap: () => { log.push("toMap"); setF((x) => ({ ...x, away: false, open: null, searching: false, layersOpen: false })); },
    leaveWater: () => { log.push("leave"); setF((x) => ({ ...x, open: null })); },
    pickDay: () => { log.push("day"); setF((x) => ({ ...x, dayPicked: true })); },
    control: (k, v) => { log.push(`${k}=${String(v)}`); setF((x) => ({ ...x, controls: { ...x.controls, [k]: v } })); return true; },
  };
  const facts: GuideFacts = { ...f, onMap: !f.away && !f.open && !f.searching && !f.adding && f.viewing === null };
  const guide = useGuide(store, blocked, facts, acts);
  out.guide = guide; out.set = setF;
  return <GuideCard guide={guide} facts={facts} acts={acts} palette={LIGHT} registry={reg} top={60} bottom={64} />;
}

describe("the guided walkthrough (T293, T456)", { timeout: 30_000 }, () => {
  it("is the owner's walk, in his order: find it, read its rules and its conditions, back to the map, its layers", () => {
    // (T302) this is the web's walk (jsdom is the web): no spot steps — spots live in the phone app
    expect([...new Set(GUIDE_STEPS.map((s) => s.phase))]).toEqual(["find", "rules", "conditions", "layers", "end"]);
    expect(GUIDE_STEPS.map((s) => s.key)).toEqual(["welcome", "search", "pick", "map", "regs", "answer", "day", "trout", "rule",
      "salmon", "both", "part", "conditions", "reading", "forecast", "gauge", "quantity", "span", "change", "leave", "layers",
      "ground", "obstacles", "restore", "done"]);
  });

  it("(T456) no step only describes a control: a step that rings a control asks for its tap, or reads a line", () => {
    // the reading steps ring lines, not controls; every other ringed step is a DO step the reader's tap moves on
    const reading = ["answer", "rule", "both", "part", "reading", "gauge", "change", "done"];
    for (const s of GUIDE_STEPS.filter((x) => x.targets.length > 0 && !reading.includes(x.key))) expect(s.kind, s.key).toBe("do");
    // a step that names a control by its label says that label
    for (const s of GUIDE_STEPS) for (const n of s.names ?? []) expect(s.body, `${s.key}: ${n}`).toContain(n);
  });

  it("opens on a new device once nothing blocks it, names the water, and walks to the end with real taps and Show me", async () => {
    const store = memory(), log: string[] = [], out: { guide?: Guide } = {};
    const reg = useRegistry(EVERY);
    const { rerender } = render(<Harness store={store} blocked reg={reg} log={log} out={out} start={{ ...BROWSE, spots: 1 }} />);
    expect(screen.queryByTestId("guide")).toBeNull();                 // the first-run notice is up
    rerender(<Harness store={store} reg={reg} log={log} out={out} start={{ ...BROWSE, spots: 1 }} />);
    expect(keyAt()).toBe("welcome");
    expect(screen.getByTestId("guide-body").textContent).toContain("the Vedder River");
    expect(screen.getByTestId("guide-count").textContent).toBe(`${w.phases.find} · ${w.step(1, GUIDE_STEPS.length)}`);
    for (let k = 0; k < 60 && screen.queryByTestId("guide"); k++) {
      await act(async () => {});                                          // settle the last move's effects first: under load a DO step read early offers Next, not Show me
      const key = keyAt(), s = stepOf(key);
      const label = screen.queryByText(w.showMe) ? w.showMe : screen.queryByText(w.done) ? w.done : w.next;
      press(label);
      if (label === w.showMe) await waitFor(() => expect(keyAt()).not.toBe(key), { timeout: WAIT });
      else if (s.key !== "done") expect(keyAt()).not.toBe(key);
    }
    expect(screen.queryByTestId("guide")).toBeNull();
    expect(store.v).toBe(WALKTHROUGH_VERSION);                           // seen, and no resume line left
    // each DO step done by Show me, through the app's own moves; "regs" was passed (the water opened on its Regulations)
    expect(log).toEqual(["search", "select", "open:regs", "day", "regs:trout=true", "regs:salmon=true", "view:conditions",
      "cond:horizons=1", "cond:quantity=level", "cond:span=year", "leave", "layers", "ground", "obstacles", "restore"]);
  }, 40_000);

  it("a DO step done by the reader ticks, then moves on by itself", async () => {
    const out: { guide?: Guide; set?: (f: (x: Pretend) => Pretend) => void } = {};
    render(<Harness store={memory("guide-at:search")} reg={useRegistry()} log={[]} out={out} />);
    expect(keyAt()).toBe("search");
    act(() => out.set!((x) => ({ ...x, searching: true, query: "ve" })));
    expect(screen.getByText(`${w.tick} ${stepOf("search").title}`)).toBeTruthy();
    await waitFor(() => expect(keyAt()).toBe("pick"), { timeout: WAIT });
  });

  it("(T456) the forecast step moves on when the reader picks a day in the panel's own control", async () => {
    const out: { set?: (f: (x: Pretend) => Pretend) => void } = {};
    render(<Harness store={memory("guide-at:forecast")} reg={useRegistry(EVERY)} log={[]} out={out} start={{ ...COND, controls: { "cond:horizons": 0 } }} />);
    expect(keyAt()).toBe("forecast");
    act(() => out.set!((x) => ({ ...x, controls: { ...x.controls, "cond:horizons": 3 } })));
    await waitFor(() => expect(keyAt()).toBe("gauge"), { timeout: WAIT });
  });

  it("is skippable at any step; a skip counts as seen and forgets the step", () => {
    const store = memory("guide-at:answer");
    render(<Harness store={store} reg={useRegistry()} log={[]} out={{}} start={REGS} />);
    expect(keyAt()).toBe("answer");                                     // resumed where it was left
    act(() => { fireEvent.click(screen.getByTestId("guide-skip")); });
    expect(screen.queryByTestId("guide")).toBeNull();
    expect([store.v, guideResumeAt(store.v)]).toEqual([WALKTHROUGH_VERSION, null]);
  });

  it("keeps the step it is on, so a walk left when the app closed resumes there", () => {
    const store = memory();
    render(<Harness store={store} reg={useRegistry()} log={[]} out={{}} />);
    press(w.next);
    expect(guideResumeAt(store.v)).toBe("search");
  });

  it("Back steps back, putting the app where the step expects it; from the first step it leaves the walk", () => {
    const store = memory("guide-at:reading"), log: string[] = [];
    render(<Harness store={store} reg={useRegistry()} log={log} out={{}} start={COND} />);
    expect(keyAt()).toBe("reading");
    press(w.back);
    expect(keyAt()).toBe("conditions");
    expect(screen.getByText(`${w.tick} ${stepOf("conditions").title}`)).toBeTruthy();   // done already
    press(w.back);
    expect(keyAt()).toBe("part");
    expect(log).toEqual(["view:regs"]);                                 // its Regulations back in front
    cleanup();
    const first = memory();
    const out2: { guide?: Guide } = {};
    render(<Harness store={first} reg={useRegistry()} log={[]} out={out2} />);
    act(() => out2.guide!.back());                                      // the card's Back, on the first step
    expect([screen.queryByTestId("guide"), first.v]).toEqual([null, WALKTHROUGH_VERSION]);
  });

  it("(T456, the tester's report) on \"Back to the map\" the page's own ‹ Map leaves the page and the walk goes ON", async () => {
    const log: string[] = [], out: { set?: (f: (x: Pretend) => Pretend) => void } = {};
    // the app's one back, as the Shell provides it: the walk must not be a layer on it (it was: ‹ Map stepped the walk back)
    const layers: (() => void)[] = [];
    const stack = { register: (close: () => void) => { layers.push(close); return () => { layers.splice(layers.indexOf(close), 1); }; },
                    back: () => { layers.at(-1)?.(); return true; } };
    render(<BackLayerContext.Provider value={stack}>
      <Harness store={memory("guide-at:leave")} reg={useRegistry(EVERY)} log={log} out={out} start={COND} />
    </BackLayerContext.Provider>);
    expect(keyAt()).toBe("leave");
    expect(layers).toHaveLength(0);                                       // nothing of the walk's sits on the back
    // the reader presses "‹ Map": the page closes itself (its own move, once)
    act(() => { log.push("page:close"); out.set!((x) => ({ ...x, open: null })); });
    expect(screen.getByText(`${w.tick} ${stepOf("leave").title}`)).toBeTruthy();
    await waitFor(() => expect(keyAt()).toBe("layers"), { timeout: WAIT });
    expect(log).toEqual(["page:close"]);                                  // the walk ran nothing behind the reader's back
  });

  it("(owner 2026-10-29) Layers: open, switch the ground, fish obstacles on — then Next puts them back as they were", async () => {
    const log: string[] = [], out: { set?: (f: (x: Pretend) => Pretend) => void } = {};
    render(<Harness store={memory("guide-at:layers")} reg={useRegistry(EVERY)} log={log} out={out} start={{ ...BROWSE, obstacles: false }} />);
    expect(log).toEqual(["layers"]);                                     // resumed on it: the sheet is opened for it
    await waitFor(() => expect(keyAt()).toBe("ground"), { timeout: WAIT });
    press(w.showMe);
    await waitFor(() => expect(keyAt()).toBe("obstacles"), { timeout: WAIT });
    press(w.showMe);
    await waitFor(() => expect(keyAt()).toBe("restore"), { timeout: WAIT });
    press(w.next);
    expect(keyAt()).toBe("done");
    expect(log).toEqual(["layers", "ground", "obstacles", "restore"]);
  });

  it("(T456) an optional step whose control this water does not draw is passed, never shown pointing at nothing", async () => {
    // "trout" and "rule" ring nothing here (no regs:trout / regs:rule drawn): passed on to "salmon"
    const drawn = Object.fromEntries(Object.entries(EVERY).filter(([k]) => !["regs:trout", "regs:rule", "regs:ruleline"].includes(k)));
    render(<Harness store={memory("guide-at:day")} reg={useRegistry(drawn)} log={[]} out={{}} start={{ ...REGS, dayPicked: true }} />);
    expect(keyAt()).toBe("day");
    press(w.doneStep);
    await waitFor(() => expect(keyAt()).toBe("salmon"), { timeout: WAIT });
  });

  it("rings the control a step is about, and keeps the card beside it", async () => {
    const reg = useRegistry({ search: [14, 10, 362, 46] });
    render(<Harness store={memory("guide-at:search")} reg={reg} log={[]} out={{}} />);
    const ring = await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    expect([ring.style.left, ring.style.top, ring.style.width]).toEqual(["6px", "2px", "378px"]);
    expect(parseFloat(screen.getByTestId("guide-step-search").style.top)).toBeGreaterThan(10 + 46);
  });

  it("(T456) the card follows a ring that moves (a sheet sliding, the page scrolling) and stays off it", async () => {
    const at = { y: 120 };
    const reg = useRegistry();
    reg.set("cond:horizons", { measureInWindow: (cb) => cb(16, at.y, 300, 120) });
    render(<Harness store={memory("guide-at:forecast")} reg={reg} log={[]} out={{}} start={COND} />);
    await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    /** the card's box (jsdom lays out nothing: its height is 0, so its top-left corner is what can land in the ring) */
    const apart = () => {
      const c = screen.getByTestId("guide-step-forecast").style, r = screen.getByTestId("guide-ring").style;
      const [cl, ct, cw] = [parseFloat(c.left), parseFloat(c.top), parseFloat(c.width)];
      const [rl, rt, rw, rh] = [parseFloat(r.left), parseFloat(r.top), parseFloat(r.width), parseFloat(r.height)];
      return cl >= rl + rw || cl + cw <= rl || ct >= rt + rh || ct <= rt;
    };
    expect(apart()).toBe(true);
    const was = screen.getByTestId("guide-step-forecast").style.top;
    at.y = 520;                                                            // scrolled: the control is now low on the screen
    await waitFor(() => expect(parseFloat(screen.getByTestId("guide-ring").style.top)).toBeGreaterThan(500), { timeout: WAIT });
    expect(apart()).toBe(true);
    expect(screen.getByTestId("guide-step-forecast").style.top).not.toBe(was);   // it moved with the ring
  });

  it("is a non-modal dialog whose step is said as it comes up", () => {
    render(<Harness store={memory()} reg={useRegistry()} log={[]} out={{}} />);
    const card = screen.getByTestId("guide-step-welcome");
    expect([card.getAttribute("role"), card.getAttribute("aria-modal")]).toEqual(["dialog", "false"]);
    expect(screen.getByText(GUIDE_STEPS[0]!.title).closest("[aria-live='polite']")).toBeTruthy();
  });

  it("stays down once seen, and with no store; re-opens on request at the first step", () => {
    const out: { guide?: Guide } = {};
    render(<Harness store={memory(WALKTHROUGH_VERSION)} reg={useRegistry()} log={[]} out={out} />);
    expect(screen.queryByTestId("guide")).toBeNull();
    act(() => out.guide!.reopen());
    expect(keyAt()).toBe("welcome");
    cleanup();
    render(<Harness store={null} reg={useRegistry()} log={[]} out={{}} />);
    expect(screen.queryByTestId("guide")).toBeNull();
  });
});

/**
 * (T456) GUARDRAILS: every way off a step's own screen has ONE result. The step's own control (‹ Map on "Back to the map")
 * moves the walk on; any other — the page's ‹ or the system back (the water closes, the map in front), a tab (another tab
 * in front), a sheet closed — pauses the walk behind "Resume walkthrough", and the chip puts the app back on that step.
 */
describe("(T456) every exit from a step's screen", { timeout: 60_000 }, () => {
  /** where each step is shown */
  const startOf = (k: string): Pretend => {
    const s = stepOf(k);
    if (k === "pick" || k === "map") return { ...BROWSE, searching: true, query: "ved", selected: k === "map" ? WATER.item : null };
    if (s.phase === "rules" || k === "conditions") return REGS;
    if (s.phase === "conditions") return COND;
    if (s.phase === "layers" && k !== "layers") return { ...BROWSE, layersOpen: true, layersWas: { ground: "ground.map", obstacles: false } };
    return BROWSE;
  };
  const EXITS: Record<string, (x: Pretend) => Pretend> = {
    "the page's ‹ / the system back": (x) => ({ ...x, open: null, searching: false, layersOpen: false }),
    "a tab": (x) => ({ ...x, away: true, open: null, searching: false, layersOpen: false }),
  };
  const withScreen = GUIDE_STEPS.filter((s) => !["welcome", "done"].includes(s.key));
  for (const s of withScreen) for (const [exit, leave] of Object.entries(EXITS)) {
    it(`${s.key}: ${exit}`, async () => {
      const log: string[] = [], out: { guide?: Guide; set?: (f: (x: Pretend) => Pretend) => void } = {};
      const start = startOf(s.key);
      render(<Harness store={memory(`guide-at:${s.key}`)} reg={useRegistry(EVERY)} log={log} out={out} start={start} />);
      expect(keyAt()).toBe(s.key);
      log.length = 0;
      act(() => out.set!(leave));
      if (s.key === "leave") {
        // its own control (the water closes): on, never paused
        await waitFor(() => expect(keyAt()).toBe("layers"), { timeout: WAIT });
        return;
      }
      if (exit !== "a tab" && ["search", "map", "layers"].includes(s.key)) {
        // the map IS these steps' screen: the card stays up
        await act(() => new Promise((d) => setTimeout(d, PAUSE_AFTER_MS + 200)));
        expect([screen.queryByTestId("guide-paused"), keyAt()]).toEqual([null, s.key]);
        return;
      }
      await waitFor(() => expect(screen.getByTestId("guide-paused")).toBeTruthy(), { timeout: WAIT });
      expect(screen.queryByTestId(/^guide-step-/)).toBeNull();            // the card stepped aside
      act(() => { fireEvent.click(screen.getByTestId("guide-resume")); });
      expect(keyAt()).toBe(s.key);                                        // the same step, the app put back for it
      expect(log.length, `${s.key} resume set nothing up`).toBeGreaterThan(0);
      await act(() => new Promise((d) => setTimeout(d, PAUSE_AFTER_MS + 200)));
      expect(screen.queryByTestId("guide-paused")).toBeNull();            // and it stays up: its screen is back
    });
  }
  it("a sheet closed under a Layers step pauses; Resume opens it again", async () => {
    const log: string[] = [], out: { set?: (f: (x: Pretend) => Pretend) => void } = {};
    render(<Harness store={memory("guide-at:obstacles")} reg={useRegistry(EVERY)} log={log} out={out} start={startOf("obstacles")} />);
    act(() => out.set!((x) => ({ ...x, layersOpen: false })));
    await waitFor(() => expect(screen.getByTestId("guide-paused")).toBeTruthy(), { timeout: WAIT });
    act(() => { fireEvent.click(screen.getByTestId("guide-resume")); });
    expect([keyAt(), log]).toEqual(["obstacles", ["layers"]]);
  });
  it("the chip's Skip ends the walk (seen)", async () => {
    const store = memory("guide-at:answer"), out: { set?: (f: (x: Pretend) => Pretend) => void } = {};
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
    expect(reg.get("regs:rule")).not.toBeNull();                          // the release's group card: the rule a step reads
  });
});

/**
 * (T456, "+1d to +10d" named chips no longer drawn) A STEP NAMES ONLY WHAT IS IN ITS RING: each step's named labels are
 * inside the view its target registers — its words, or its spoken name — drawn by the real component.
 */
describe("(T456) the words name only controls drawn inside the ring", { timeout: 60_000 }, () => {
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
    ["view:regs", () => <ViewTabs palette={LIGHT} views={["regs", "conditions", "map"]} on="regs" onView={() => {}} />],
    ["water:back", () => <FaceBar palette={LIGHT} face="regulations" onFace={() => {}} onBack={() => {}} backLabel="Map" />],
    ["regs:trout", () => <OtherFish rows={[]} keys={[]} palette={LIGHT} actions={{} as never}
                                    folds={[fold("salmon", "Salmon"), fold("group:0", "Trout and char")]} />],
    ["cond:horizons", () => <HorizonPicker palette={LIGHT} chips={horizonChips(0, "discharge")} onPick={() => {}} testID="h" coach="cond:horizons" />],
    ["cond:quantity", () => <ChartControls palette={LIGHT} value="discharge" label="Quantity" testID="q" coach="cond:quantity" onPick={() => {}}
                                           options={[["discharge", "Flow"], ["level", "Level"]]} />],
    ["cond:span", () => <ChartControls palette={LIGHT} value="72h" label="Span" testID="s" coach="cond:span" onPick={() => {}}
                                       options={[["72h", "72 h"], ["year", "Year"]]} />],
    ["layers", () => <MapRail palette={LIGHT} onLayers={() => {}} />],
    ["layers:ground", () => <LayersSheet open onClose={() => {}} palette={LIGHT} onChoose={() => {}}
                                         vm={layersVM("map", LAYER_DEFAULTS, { carries: { stocked: false, surveyed: false }, online: true })} />],
    ["tab:more", () => <TabBar active="map" onChange={() => {}} palette={LIGHT} />],
  ];
  it("each named label is inside its step's ringed control", async () => {
    const reg = renderHook(() => useCoachRegistry()).result.current;
    const item = await vedder();
    render(<Coached reg={reg}>{DRAWN.map(([k, el]) => <div key={k}>{el()}</div>)}
      <WaterScreen source={source} loader={lined} item={item} section={null} part={null} day={DAY} palette={LIGHT} />
    </Coached>);
    await waitFor(() => expect(reg.get("regs:rule")).not.toBeNull(), { timeout: 30_000 });
    const checked: string[] = [];
    for (const s of GUIDE_STEPS) {
      if (!s.names?.length) continue;
      const inside = s.targets.map((t) => said(reg, t)).filter((x): x is string => x !== null).join(" | ");
      expect(inside, `${s.key}: ${s.targets.join(", ")} not drawn`).not.toBe("");
      for (const n of s.names) expect(inside, `${s.key} names "${n}"`).toContain(n.toLowerCase());
      checked.push(s.key);
    }
    // the rule's ring is the head and the first rule's own line, never the whole tier under it (the phone's ring was 2145 px)
    expect(said(reg, "regs:ruleline")).not.toContain("of your 2 trout and char");
    // salmon's Details is the same fold row as trout's (its own key)
    expect(said(reg, "regs:salmon")).toContain(GLANCE_WORDS.show.toLowerCase());
    expect(checked).toEqual(GUIDE_STEPS.filter((s) => s.names?.length).map((s) => s.key));
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
});

describe("coach-mark geometry (T257)", () => {
  it("the callout sits under a control near the top, over one near the foot, inside the gutters on a phone", () => {
    const top = placeCallout({ x: 10, y: 60, w: 100, h: 40 }, { w: 390, h: 844 });
    expect([top.below, top.top, top.left, top.width]).toEqual([true, 114, 16, 358]);
    const foot = placeCallout({ x: 100, y: 780, w: 80, h: 50 }, { w: 390, h: 844 });
    expect([foot.below, foot.bottom]).toEqual([false, 844 - 780 + 14]);
    const wide = placeCallout({ x: 1000, y: 10, w: 44, h: 44 }, { w: 1280, h: 800 });
    expect([wide.width, wide.left]).toEqual([360, 1022 - 180]);
    const edge = placeCallout({ x: 1230, y: 10, w: 44, h: 44 }, { w: 1280, h: 800 });
    expect(edge.left).toBe(1280 - 360 - 16);
  });
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
    let mode = "", off = 0;
    render(<HelpPage palette={LIGHT} theme="light" onTabTour={(k) => { mode = k; }} onOffline={() => { off++; }} />);
    expect(screen.getAllByTestId(/^help-page-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(["help-page-walkthrough", "help-page-updates", "help-page-offline"]);
    expect(screen.getByText(HELP_WORDS.updates.web[0]!)).toBeTruthy();
    expect(screen.getByText(HELP_WORDS.offline.web[0]!)).toBeTruthy();
    expect(screen.queryByTestId("help-offline")).toBeNull();            // a browser has no Offline maps page
    fireEvent.click(screen.getByText(TAB_TOUR_WORDS.again.salmon));
    expect([mode, off]).toEqual(["salmon", 0]);
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

describe("the mode hints (T276, T293)", () => {
  const mem = (text: string | null = null): WalkthroughStore & { text: string | null } => {
    const m = { text, read: () => m.text, write: (t: string) => { m.text = t; } };
    return m;
  };
  it("open once, on the tab's first opening, and are remembered beside the welcome tour's line", () => {
    const store = mem(WALKTHROUGH_VERSION);
    const { result, rerender } = renderHook(({ on }) => useTabTour(store, "runs", on, false), { initialProps: { on: false } });
    expect(result.current.open).toBe(false);          // the Runs tab is not in front
    rerender({ on: true });
    expect(result.current.open).toBe(true);
    act(() => result.current.finish());
    expect(result.current.open).toBe(false);
    expect(store.text!.split("\n")).toEqual([WALKTHROUGH_VERSION, "tour-runs-1"]);
    const again = renderHook(() => useTabTour(store, "runs", true, false));
    expect(again.result.current.open).toBe(false);    // seen
    act(() => again.result.current.reopen());          // More → Help
    expect(again.result.current.open).toBe(true);
  });
  it("(T293) Runs and Salmon one step each; the Conditions map a short tour that moves the camera out, then in", () => {
    expect([TAB_TOUR_WORDS.runs.length, TAB_TOUR_WORDS.salmon.length]).toEqual([1, 1]);
    expect(TAB_TOUR_WORDS.conditions.map((c) => [c.key, c.camera ?? null]))
      .toEqual([["watershed", "watershed"], ["gauge", "gauge"], ["colours", null], ["tap", null], ["change", null]]);
    expect(TAB_TOUR_WORDS.salmon[0]!.targets).toEqual(["modes"]);
  });
  it("wait while something else holds the screen, and never open with no store", () => {
    expect(renderHook(() => useTabTour(mem(), "conditions", true, true)).result.current.open).toBe(false);
    expect(renderHook(() => useTabTour(null, "conditions", true, false)).result.current.open).toBe(false);
  });
});
