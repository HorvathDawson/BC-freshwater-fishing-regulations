/**
 * The one back, MOUNTED (G1): `useBack` under React, with its effects, batching and re-renders — the conditions the
 * history sync really runs in. Each platform input is modelled as its glue drives it: Android's `BackHandler` calls
 * the back function and reads the boolean; the browser traverses its history (later, as browsers do) and fires
 * `popstate`.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { ItemId } from "@app/data";
import { historySync, useBack, type Back, type HistoryPort, type SystemBack } from "./gestures";
import { useRoute, type Route, type RouteAction } from "./route";

afterEach(cleanup);
const VEDDER = "gnis:3062" as ItemId, FRASER = "gnis:39325" as ItemId;

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
    act(() => { out.go!({ type: "openWater", item: VEDDER }); out.go!({ type: "view", view: "map" }); });
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
    });
    const seen: unknown[] = [{ frames: frames(out.r!), depth: out.b!.depth, history: b.state() }];
    act(() => { out.b!.back(); }); b.flush();                       // ‹
    seen.push({ frames: frames(out.r!), depth: out.b!.depth, history: b.state() });
    b.userBack();                                                    // the browser's back
    seen.push({ frames: frames(out.r!), depth: out.b!.depth, history: b.state() });
    act(() => { out.b!.back(); }); b.flush();                       // ‹
    seen.push({ frames: frames(out.r!), depth: out.b!.depth, history: b.state() });
    b.userBack();                                                    // the browser's back, out of water mode
    seen.push({ frames: frames(out.r!), depth: out.b!.depth, history: b.state() });
    expect(seen).toEqual([
      { frames: [`${VEDDER}/map`, `${FRASER}/conditions`], depth: 4, history: { entries: [null, 1, 2, 3, 4], at: 4 } },
      { frames: [`${VEDDER}/map`, `${FRASER}/regs`], depth: 3, history: { entries: [null, 1, 2, 3, 4], at: 3 } },
      { frames: [`${VEDDER}/map`], depth: 2, history: { entries: [null, 1, 2, 3, 4], at: 2 } },
      { frames: [`${VEDDER}/regs`], depth: 1, history: { entries: [null, 1, 2, 3, 4], at: 1 } },
      { frames: ["browse:map"], depth: 0, history: { entries: [null, 1, 2, 3, 4], at: 0 } },
    ]);
  });

  it("a push after the browser's back truncates the forward entries: no stranded entry", () => {
    const b = browser();
    const system: SystemBack = (back) => historySync(b.port, back);
    const out: Parameters<typeof Probe>[0]["out"] = {};
    render(<Probe system={system} out={out} />);
    act(() => { out.go!({ type: "openWater", item: VEDDER }); out.go!({ type: "view", view: "map" }); });
    b.userBack();
    act(() => { out.go!({ type: "view", view: "conditions" }); });
    expect({ frames: frames(out.r!), history: b.state() })
      .toEqual({ frames: [`${VEDDER}/conditions`], history: { entries: [null, 1, 2], at: 2 } });
  });
});
