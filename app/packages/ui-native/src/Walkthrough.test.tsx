/**
 * (T293) The guided walkthrough — one hands-on walk on a real water: once per device, skippable at any step, resumed where
 * it was left, re-openable; DO steps move on when the app gets there and "Show me" does them; Back steps back and out;
 * and (T248) the Help page drawn from the legend tokens, with its short pages.
 */
import { afterEach, describe, expect, it } from "vitest";
import { useState } from "react";
import { act, cleanup, fireEvent, render, renderHook, screen, waitFor } from "@testing-library/react";
import { HELP_WORDS, TAB_TOUR_WORDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, guideResumeAt, helpVM } from "@app/core";
import { notFishingColours, tidalColours } from "@app/map";
import { HelpPage } from "./menu/HelpPage";
import { GUIDE_STEPS, GuideCard, useGuide, useTabTour, type Guide, type GuideActs, type GuideFacts,
         type WalkthroughStore } from "./Walkthrough";
import { insideWindow, placeCallout, unionRect, useCoachRegistry, type CoachRegistry } from "./CoachMarks";
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
const BROWSE: GuideFacts = { water: WATER, searching: false, query: "", selected: null, open: null, adding: false, viewing: null, spots: 0,
                             layersOpen: false, ground: "ground.map", obstacles: false, layersWas: null };
const keyAt = () => screen.getByTestId(/^guide-step-/).getAttribute("data-testid")!.slice("guide-step-".length);
const press = (label: string) => act(() => { fireEvent.click(screen.getByText(label)); });

function useRegistry(at: Record<string, [number, number, number, number]> = {}): CoachRegistry {
  const reg = renderHook(() => useCoachRegistry()).result.current;
  for (const [k, r] of Object.entries(at)) reg.set(k, { measureInWindow: (cb) => cb(...r) });
  return reg;
}

/** The walk over a pretend app: the acts move the facts as the Shell's route would. */
function Harness({ store, start = BROWSE, blocked = false, reg, log, out }: {
  store: WalkthroughStore | null; start?: GuideFacts; blocked?: boolean; reg: CoachRegistry; log: string[];
  out: { guide?: Guide; set?: (f: (x: GuideFacts) => GuideFacts) => void };
}) {
  const [f, setF] = useState(start);
  const acts: GuideActs = {
    search: () => { log.push("search"); setF((x) => ({ ...x, searching: true, query: WATER.name })); },
    select: () => { log.push("select"); setF((x) => ({ ...x, selected: WATER.item })); },
    openWater: (v) => { log.push(`open:${v}`); setF((x) => ({ ...x, searching: false, open: { item: WATER.item, view: v } })); },
    view: (v) => { log.push(`view:${v}`); setF((x) => ({ ...x, open: x.open && { ...x.open, view: v } })); },
    addSpot: () => { log.push("add"); setF((x) => ({ ...x, open: null, adding: true })); },
    cancelAdd: () => { log.push("cancel"); setF((x) => ({ ...x, adding: false })); },
    toSpots: () => { log.push("spots"); setF((x) => ({ ...x, viewing: null })); },
    openSpot: () => { log.push("openSpot"); setF((x) => ({ ...x, viewing: "s1" })); },
    openLayers: () => { log.push("layers"); setF((x) => ({ ...x, open: null, layersOpen: true, layersWas: x.layersWas ?? { ground: x.ground!, obstacles: !!x.obstacles } })); },
    otherGround: () => { log.push("ground"); setF((x) => ({ ...x, ground: x.ground === "ground.map" ? "ground.satellite" : "ground.map" })); },
    obstaclesOn: () => { log.push("obstacles"); setF((x) => ({ ...x, obstacles: true })); },
    restoreLayers: () => { log.push("restore"); setF((x) => ({ ...x, layersOpen: false, ground: x.layersWas?.ground ?? x.ground, obstacles: x.layersWas?.obstacles ?? x.obstacles, layersWas: null })); },
  };
  const guide = useGuide(store, blocked, f, acts);
  out.guide = guide; out.set = setF;
  return <GuideCard guide={guide} facts={f} acts={acts} palette={LIGHT} registry={reg} top={60} bottom={64} />;
}

describe("the guided walkthrough (T293)", { timeout: 30_000 }, () => {
  it("is the owner's walk, in his order: find it, its rules, its conditions, a spot and its coming days", () => {
    // (T302) this is the web's walk (jsdom is the web): no spot steps — spots live in the phone app
    expect([...new Set(GUIDE_STEPS.map((s) => s.phase))]).toEqual(["find", "rules", "conditions", "layers", "end"]);
    expect(GUIDE_STEPS.map((s) => s.key)).toEqual(["welcome", "search", "pick", "map", "regs", "answer", "day", "keep", "gear",
      "part", "book", "conditions", "reading", "forecast", "change", "layers", "ground", "obstacles", "restore", "done"]);
  });

  it("opens on a new device once nothing blocks it, names the water, and walks to the end with real taps and Show me", async () => {
    const store = memory(), log: string[] = [], out: { guide?: Guide } = {};
    const reg = useRegistry();
    const { rerender } = render(<Harness store={store} blocked reg={reg} log={log} out={out} start={{ ...BROWSE, spots: 1 }} />);
    expect(screen.queryByTestId("guide")).toBeNull();                 // the first-run notice is up
    rerender(<Harness store={store} reg={reg} log={log} out={out} start={{ ...BROWSE, spots: 1 }} />);
    expect(keyAt()).toBe("welcome");
    expect(screen.getByTestId("guide-body").textContent).toContain("the Vedder River");
    expect(screen.getByTestId("guide-count").textContent).toBe(`${w.phases.find} · ${w.step(1, GUIDE_STEPS.length)}`);
    for (let k = 0; k < 40 && screen.queryByTestId("guide"); k++) {
      await act(async () => {});                                          // settle the last move's effects first: under load a DO step read early offers Next, not Show me
      const key = keyAt(), s = GUIDE_STEPS.find((x) => x.key === key)!;
      const label = screen.queryByText(w.showMe) ? w.showMe : screen.queryByText(w.done) ? w.done : w.next;
      press(label);
      if (label === w.showMe) await waitFor(() => expect(keyAt()).not.toBe(key), { timeout: WAIT });
      else if (s.key !== "done") expect(keyAt()).not.toBe(key);
    }
    expect(screen.queryByTestId("guide")).toBeNull();
    expect(store.v).toBe(WALKTHROUGH_VERSION);                           // seen, and no resume line left
    // each DO step done by Show me, through the app's own moves; "regs" was passed (the water opened on its Regulations)
    expect(log).toEqual(["search", "select", "open:regs", "view:conditions", "layers", "ground", "obstacles", "restore"]);
   }, 20_000);

  it("a DO step done by the reader ticks, then moves on by itself", async () => {
    const out: { guide?: Guide; set?: (f: (x: GuideFacts) => GuideFacts) => void } = {};
    render(<Harness store={memory("guide-at:search")} reg={useRegistry()} log={[]} out={out} />);
    expect(keyAt()).toBe("search");
    act(() => out.set!((x) => ({ ...x, searching: true, query: "ve" })));
    expect(screen.getByText(`${w.tick} ${GUIDE_STEPS[1]!.title}`)).toBeTruthy();
    await waitFor(() => expect(keyAt()).toBe("pick"), { timeout: WAIT });
  });

  it("is skippable at any step; a skip counts as seen and forgets the step", () => {
    const store = memory("guide-at:keep");
    render(<Harness store={store} reg={useRegistry()} log={[]} out={{}} start={{ ...BROWSE, open: { item: WATER.item, view: "regs" } }} />);
    expect(keyAt()).toBe("keep");                                       // resumed where it was left
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
    render(<Harness store={store} reg={useRegistry()} log={log} out={{}} start={{ ...BROWSE, open: { item: WATER.item, view: "conditions" } }} />);
    expect(keyAt()).toBe("reading");
    press(w.back);
    expect(keyAt()).toBe("conditions");
    expect(screen.getByText(`${w.tick} ${GUIDE_STEPS.find((s) => s.key === "conditions")!.title}`)).toBeTruthy();   // done already
    press(w.back);
    expect(keyAt()).toBe("book");
    expect(log).toEqual(["view:regs"]);                                 // its Regulations back in front
    cleanup();
    const first = memory();
    render(<Harness store={first} reg={useRegistry()} log={[]} out={{}} />);
    const out2: { guide?: Guide } = {};
    cleanup();
    render(<Harness store={first} reg={useRegistry()} log={[]} out={out2} />);
    act(() => out2.guide!.back());                                      // the system back, on the first step
    expect([screen.queryByTestId("guide"), first.v]).toEqual([null, WALKTHROUGH_VERSION]);
  });

  it("(owner 2026-10-29) Layers: open, switch the ground, fish obstacles on — then Next puts them back as they were", async () => {
    const log: string[] = [], out: { set?: (f: (x: GuideFacts) => GuideFacts) => void } = {};
    render(<Harness store={memory("guide-at:layers")} reg={useRegistry()} log={log} out={out} start={{ ...BROWSE, obstacles: false }} />);
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

  it("rings the control a step is about, and keeps the card beside it", async () => {
    const reg = useRegistry({ search: [14, 10, 362, 46] });
    render(<Harness store={memory("guide-at:search")} reg={reg} log={[]} out={{}} />);
    const ring = await screen.findByTestId("guide-ring", {}, { timeout: WAIT });
    expect([ring.style.left, ring.style.top, ring.style.width]).toEqual(["6px", "2px", "378px"]);
    expect(parseFloat(screen.getByTestId("guide-step-search").style.top)).toBeGreaterThan(10 + 46);
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
