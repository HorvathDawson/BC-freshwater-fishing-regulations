/**
 * The one back, MOUNTED (G1): `useBack` under React, with its effects, batching and re-renders — the conditions the
 * history sync really runs in. Each platform input is modelled as its glue drives it: Android's `BackHandler` calls
 * the back function and reads the boolean; the browser traverses its history (later, as browsers do) and fires
 * `popstate`.
 */
import { afterEach, describe, expect, it } from "vitest";
import { useState } from "react";
import { act, cleanup, render } from "@testing-library/react";
import type { ItemId } from "@app/data";
import { BackLayerContext, historySync, useBack, useBackLayer, type Back, type HistoryPort,
         type SystemBack } from "./gestures";
import { useRoute, type Route, type RouteAction } from "./route";

afterEach(cleanup);
const VEDDER = "gnis:3062" as ItemId, FRASER = "gnis:39325" as ItemId, HARRISON = "gnis:5000" as ItemId;

function browser() {
  const entries: (number | null)[] = [null];
  let i = 0;
  const listeners = new Set<() => void>();
  const queue: number[] = [];
  const port: HistoryPort = {
    index: () => entries[i] ?? null,
    push: (n) => { entries.splice(i + 1); entries.push(n); i = entries.length - 1; },
    go: (d) => { queue.push(d); },
    listen: (f) => { listeners.add(f); return () => { listeners.delete(f); }; },
  };
  const flush = () => act(() => {
    while (queue.length) {
      const j = i + queue.shift()!;
      if (j < 0 || j >= entries.length || j === i) continue;
      i = j;
      for (const f of [...listeners]) f();
    }
  });
  return { port, flush, userBack: () => { queue.push(-1); flush(); }, state: () => ({ entries: [...entries], at: i }) };
}

/** Android's BackHandler, as the glue wires it: a press calls back() and the boolean decides who handles it. */
function androidBack() {
  let handler: (() => boolean) | null = null;
  const system: SystemBack = (back) => { handler = back; return { sync: () => {}, dispose: () => { handler = null; } }; };
  return { system, press: () => { let handled = false; act(() => { handled = handler!(); }); return handled; } };
}

function Probe({ system, out }: { system: SystemBack; out: { go?: (a: RouteAction) => void; b?: Back; r?: Route } }) {
  const [route, go] = useRoute();
  out.go = go; out.r = route;
  out.b = useBack(route, go, system);
  return null;
}
const frames = (r: Route) => (r.mode === "water" ? r.stack.map((f) => `${f.item}/${f.view}`) : [`browse:${r.tab}`]);

describe("useBack, mounted", () => {
  it("Android's system back pops exactly one frame, and at the browse root hands the press to Android", () => {
    const a = androidBack();
    const out: Parameters<typeof Probe>[0]["out"] = {};
    render(<Probe system={a.system} out={out} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); out.go!({ type: "flowInto", item: FRASER, part: 0 }); });
    const seen = [a.press(), frames(out.r!), a.press(), frames(out.r!), a.press(), frames(out.r!)];
    expect(seen).toEqual([true, [`${VEDDER}/regs`], true, ["browse:map"], false, ["browse:map"]]);
  });

  it("the ‹ and the browser's back, interleaved through a deep push: one frame each, history in step", () => {
    const b = browser();
    const system: SystemBack = (back) => historySync(b.port, back);
    const out: Parameters<typeof Probe>[0]["out"] = {};
    render(<Probe system={system} out={out} />);
    act(() => {
      out.go!({ type: "openWater", item: VEDDER });
      out.go!({ type: "view", view: "map" });
      out.go!({ type: "flowInto", item: FRASER, part: 1 });
      out.go!({ type: "view", view: "conditions" });
      out.go!({ type: "flowInto", item: HARRISON, part: 0 });
    });
    const seen: unknown[] = [{ frames: frames(out.r!), depth: out.b!.depth, history: b.state() }];
    act(() => { out.b!.back(); }); b.flush();                       // ‹
    seen.push({ frames: frames(out.r!), depth: out.b!.depth, history: b.state() });
    b.userBack();                                                    // the browser's back
    seen.push({ frames: frames(out.r!), depth: out.b!.depth, history: b.state() });
    act(() => { out.b!.back(); }); b.flush();                       // ‹
    seen.push({ frames: frames(out.r!), depth: out.b!.depth, history: b.state() });
    expect(seen).toEqual([
      { frames: [`${VEDDER}/map`, `${FRASER}/conditions`, `${HARRISON}/regs`], depth: 3,
        history: { entries: [null, 1, 2, 3], at: 3 } },
      { frames: [`${VEDDER}/map`, `${FRASER}/conditions`], depth: 2, history: { entries: [null, 1, 2, 3], at: 2 } },
      // the views are lateral: back leaves a water whatever view is in front
      { frames: [`${VEDDER}/map`], depth: 1, history: { entries: [null, 1, 2, 3], at: 1 } },
      { frames: ["browse:map"], depth: 0, history: { entries: [null, 1, 2, 3], at: 0 } },
    ]);
  });

  it("a push after the browser's back truncates the forward entries: no stranded entry", () => {
    const b = browser();
    const system: SystemBack = (back) => historySync(b.port, back);
    const out: Parameters<typeof Probe>[0]["out"] = {};
    render(<Probe system={system} out={out} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); out.go!({ type: "flowInto", item: FRASER, part: 0 }); });
    b.userBack();
    act(() => { out.go!({ type: "flowInto", item: HARRISON, part: 0 }); });
    expect({ frames: frames(out.r!), history: b.state() })
      .toEqual({ frames: [`${VEDDER}/regs`, `${HARRISON}/regs`], history: { entries: [null, 1, 2], at: 2 } });
  });
});

/**
 * A SHEET IS A BACK LAYER (G1 follow-up). A sheet (Layers) is not a route frame, but back must close it before it
 * pops anything: on Android the Modal's own `onRequestClose` did; on the web the browser's back went under it (and
 * at the browse root left the app with the sheet open). Now an open sheet registers with the one back
 * (`useBackLayer`), which counts it in the depth (one history entry on the web) and closes it first.
 */
interface Out { go?: (a: RouteAction) => void; b?: Back; r?: Route; setOpen?: (o: boolean) => void; open?: boolean;
                requestClose?: () => void }
function FakeSheet({ out }: { out: Out }) {
  const [open, setOpen] = useState(false);
  out.setOpen = setOpen; out.open = open;
  out.requestClose = useBackLayer(open, () => setOpen(false));
  return null;
}
function WithSheet({ system, out }: { system: SystemBack; out: Out }) {
  const [route, go] = useRoute();
  out.go = go; out.r = route;
  out.b = useBack(route, go, system);
  return <BackLayerContext.Provider value={out.b.layers}><FakeSheet out={out} /></BackLayerContext.Provider>;
}
const look = (out: Out, b?: ReturnType<typeof browser>) =>
  ({ frames: frames(out.r!), sheet: out.open, depth: out.b!.depth, ...(b ? { history: b.state() } : {}) });

describe("an open sheet: back closes it first, on every platform", () => {
  it("web: the browser's back closes the sheet and leaves the route; the next back pops the route", () => {
    const b = browser();
    const out: Out = {};
    render(<WithSheet system={(back) => historySync(b.port, back)} out={out} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); });
    act(() => { out.setOpen!(true); });
    const seen = [look(out, b)];
    b.userBack(); seen.push(look(out, b));
    b.userBack(); seen.push(look(out, b));
    expect(seen).toEqual([
      { frames: [`${VEDDER}/regs`], sheet: true, depth: 2, history: { entries: [null, 1, 2], at: 2 } },
      { frames: [`${VEDDER}/regs`], sheet: false, depth: 1, history: { entries: [null, 1, 2], at: 1 } },
      { frames: ["browse:map"], sheet: false, depth: 0, history: { entries: [null, 1, 2], at: 0 } },
    ]);
  });

  it("web, at the browse root: the browser's back closes the sheet instead of leaving the app", () => {
    const b = browser();
    const out: Out = {};
    render(<WithSheet system={(back) => historySync(b.port, back)} out={out} />);
    act(() => { out.setOpen!(true); });
    b.userBack();
    expect(look(out, b))
      .toEqual({ frames: ["browse:map"], sheet: false, depth: 0, history: { entries: [null, 1], at: 0 } });
  });

  it("web: a sheet closed by Done (or the scrim, or a drag) drops its entry — none stranded", () => {
    const b = browser();
    const out: Out = {};
    render(<WithSheet system={(back) => historySync(b.port, back)} out={out} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); });
    act(() => { out.setOpen!(true); });
    act(() => { out.setOpen!(false); }); b.flush();               // Done
    const closed = look(out, b);
    act(() => { out.go!({ type: "flowInto", item: FRASER, part: 0 }); });   // a push reuses the dropped entry's slot
    const pushed = look(out, b);
    b.userBack();
    expect([closed, pushed, look(out, b)]).toEqual([
      { frames: [`${VEDDER}/regs`], sheet: false, depth: 1, history: { entries: [null, 1, 2], at: 1 } },
      { frames: [`${VEDDER}/regs`, `${FRASER}/regs`], sheet: false, depth: 2, history: { entries: [null, 1, 2], at: 2 } },
      { frames: [`${VEDDER}/regs`], sheet: false, depth: 1, history: { entries: [null, 1, 2], at: 1 } },
    ]);
  });

  it("Android: the system back closes the sheet first (and says it handled the press), then pops the route", () => {
    const a = androidBack();
    const out: Out = {};
    render(<WithSheet system={a.system} out={out} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); });
    act(() => { out.setOpen!(true); });
    const seen: unknown[] = [];
    seen.push([a.press(), look(out)]);
    seen.push([a.press(), look(out)]);
    seen.push([a.press(), look(out)]);
    expect(seen).toEqual([
      [true, { frames: [`${VEDDER}/regs`], sheet: false, depth: 1 }],
      [true, { frames: ["browse:map"], sheet: false, depth: 0 }],
      [false, { frames: ["browse:map"], sheet: false, depth: 0 }],
    ]);
  });

  it("Android: the Modal's onRequestClose is the same one back — it closes the sheet and pops nothing else", () => {
    const a = androidBack();
    const out: Out = {};
    render(<WithSheet system={a.system} out={out} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); });
    act(() => { out.setOpen!(true); });
    act(() => { out.requestClose!(); });
    expect(look(out)).toEqual({ frames: [`${VEDDER}/regs`], sheet: false, depth: 1 });
  });

  it("with no back to register with (a sheet outside the shell), its close is its own", () => {
    const out: Out = {};
    render(<FakeSheet out={out} />);
    act(() => { out.setOpen!(true); });
    act(() => { out.requestClose!(); });
    expect(out.open).toBe(false);
  });
});

/**
 * A LAYER THAT TAKES A STEP (back, everywhere): one back does one thing to a surface — a capture steps back one step,
 * an edit asks before it drops anything — and the surface may stay open. It is then registered again, on top, so the
 * next back reaches it again rather than going under it; only when it closes does back reach the route.
 */
function Stepper({ out, steps }: { out: Out & { step?: number }; steps: number }) {
  const [step, setStep] = useState(steps);
  out.step = step;
  useBackLayer(step > 0, () => setStep((s) => s - 1));
  return null;
}
function WithStepper({ system, out, steps }: { system: SystemBack; out: Out & { step?: number }; steps: number }) {
  const [route, go] = useRoute();
  out.go = go; out.r = route;
  out.b = useBack(route, go, system);
  return <BackLayerContext.Provider value={out.b.layers}><Stepper out={out} steps={steps} /></BackLayerContext.Provider>;
}

describe("a layer that takes one step per back stays registered until it closes", () => {
  it("Android: two steps, then the route, then Android's own (false)", () => {
    const a = androidBack();
    const out: Out & { step?: number } = {};
    render(<WithStepper system={a.system} out={out} steps={2} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); });
    const seen: string[] = [];
    for (let i = 0; i < 4; i++) {
      const handled = a.press();
      seen.push(`${handled} step ${out.step} ${frames(out.r!).join()} depth ${out.b!.depth}`);
    }
    expect(seen.join(" | ")).toBe(
      `true step 1 ${VEDDER}/regs depth 2 | true step 0 ${VEDDER}/regs depth 1 | true step 0 browse:map depth 0 | ` +
      "false step 0 browse:map depth 0");
  });

  it("web: each browser back takes one step and the entry is put back while the surface is still open", () => {
    const b = browser();
    const out: Out & { step?: number } = {};
    render(<WithStepper system={(back) => historySync(b.port, back)} out={out} steps={2} />);
    const seen: unknown[] = [{ step: out.step, history: b.state() }];
    b.userBack(); seen.push({ step: out.step, history: b.state() });
    b.userBack(); seen.push({ step: out.step, history: b.state() });
    expect(seen).toEqual([
      { step: 2, history: { entries: [null, 1], at: 1 } },
      { step: 1, history: { entries: [null, 1], at: 1 } },
      { step: 0, history: { entries: [null, 1], at: 0 } },
    ]);
  });
});
