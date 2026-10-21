/**
 * The one back rule across every input (G1), and the browser's history kept in step with the route.
 *
 * The history here is a model of the browser's own: a list of entries and a position, `pushState` truncating the
 * forward entries, and `go` TRAVERSING LATER (a queued task, as browsers do) and then firing one `popstate`. That
 * asynchrony is what makes a double pop possible, so the model keeps it.
 */
import { describe, expect, it } from "vitest";
import type { ItemId } from "@app/data";
import { backDepth, historySync, partSwipeAction, type HistoryPort } from "./gestures";
import { HOME, reduce, type Route, type RouteAction } from "./route";

const VEDDER = "gnis:3062" as ItemId, FRASER = "gnis:39325" as ItemId, HARRISON = "gnis:5000" as ItemId;

function browser() {
  const entries: (number | null)[] = [null];
  let i = 0;
  let left = false;
  const listeners = new Set<() => void>();
  const queue: number[] = [];
  const port: HistoryPort = {
    index: () => entries[i] ?? null,
    push: (n) => { entries.splice(i + 1); entries.push(n); i = entries.length - 1; },
    go: (d) => { queue.push(d); },
    listen: (f) => { listeners.add(f); return () => { listeners.delete(f); }; },
  };
  /** Let the queued traversals happen, each with its popstate. */
  const flush = () => {
    while (queue.length) {
      const j = i + queue.shift()!;
      if (j < 0) { left = true; continue; }          // back past the page's first entry: the browser leaves the app
      if (j >= entries.length || j === i) continue;
      i = j;
      for (const f of [...listeners]) f();
    }
  };
  return {
    port, flush,
    /** The browser's own back button (or a phone browser's edge swipe). */
    userBack: () => { queue.push(-1); flush(); },
    userForward: () => { queue.push(1); flush(); },
    get state() { return { entries: [...entries], at: i, left }; },
  };
}

/** The app: a route, the one back function, and the effect that tells the history the depth after each change. */
function app(b: ReturnType<typeof browser>) {
  let route: Route = HOME;
  let pops = 0;
  const back = () => {
    const next = reduce(route, { type: "back" });
    if (next === route) return false;
    pops++;
    route = next;
    binding.sync(backDepth(route));
    return true;
  };
  const binding = historySync(b.port, back);
  binding.sync(backDepth(route));
  return {
    go: (...as: RouteAction[]) => { for (const a of as) { route = reduce(route, a); binding.sync(backDepth(route)); } },
    back,
    get route() { return route; },
    get pops() { return pops; },
    frames: () => (route.mode === "water" ? route.stack.map((f) => `${f.item}/${f.view}`) : [`browse:${route.tab}`]),
  };
}

describe("backDepth: how many backs the route holds, counted by the reducer that pops it", () => {
  it("browse root 0; a drill 1; a water on a drill 2; its map view 3; a water flowed into 4", () => {
    const rows: [string, RouteAction[]][] = [
      ["root", []],
      ["drill", [{ type: "condSection", section: 5 as never }]],
      ["water", [{ type: "condSection", section: 5 as never }, { type: "openWater", item: VEDDER }]],
      ["map", [{ type: "condSection", section: 5 as never }, { type: "openWater", item: VEDDER },
               { type: "view", view: "map" }]],
      ["flow", [{ type: "condSection", section: 5 as never }, { type: "openWater", item: VEDDER },
                { type: "view", view: "map" }, { type: "flowInto", item: FRASER, part: 0 }]],
    ];
    expect(rows.map(([k, as]) => [k, backDepth(as.reduce(reduce, HOME as Route))]))
      .toEqual([["root", 0], ["drill", 1], ["water", 2], ["map", 3], ["flow", 4]]);
  });
});

describe("one back from each input pops exactly one frame", () => {
  const deep: RouteAction[] = [{ type: "openWater", item: VEDDER }, { type: "view", view: "map" },
                               { type: "flowInto", item: FRASER, part: 2 }];

  it("the ‹ button (and Android's BackHandler, and the iOS edge swipe — all the same back())", () => {
    const b = browser(); const a = app(b);
    a.go(...deep);
    expect(a.back()).toBe(true);
    b.flush();
    expect({ frames: a.frames(), pops: a.pops, history: b.state })
      .toEqual({ frames: [`${VEDDER}/map`], pops: 1, history: { entries: [null, 1, 2, 3], at: 2, left: false } });
  });

  it("the browser's back button", () => {
    const b = browser(); const a = app(b);
    a.go(...deep);
    b.userBack();
    expect({ frames: a.frames(), pops: a.pops, history: b.state })
      .toEqual({ frames: [`${VEDDER}/map`], pops: 1, history: { entries: [null, 1, 2, 3], at: 2, left: false } });
  });

  it("at the browse root back pops nothing and says so (Android then leaves the app)", () => {
    const b = browser(); const a = app(b);
    expect({ popped: a.back(), pops: a.pops, history: b.state })
      .toEqual({ popped: false, pops: 0, history: { entries: [null], at: 0, left: false } });
  });
});

describe("the browser's history stays in step with the route", () => {
  it("a deep push sequence, then ‹, browser back, ‹ and browser back again, all the way out", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "openWater", item: VEDDER }, { type: "view", view: "map" },
         { type: "flowInto", item: FRASER, part: 2 }, { type: "flowInto", item: HARRISON, part: 0 });
    const seen: unknown[] = [{ frames: a.frames(), history: b.state }];
    a.back(); b.flush(); seen.push({ frames: a.frames(), history: b.state });
    b.userBack(); seen.push({ frames: a.frames(), history: b.state });
    a.back(); b.flush(); seen.push({ frames: a.frames(), history: b.state });
    b.userBack(); seen.push({ frames: a.frames(), history: b.state });
    b.userBack(); seen.push({ frames: a.frames(), history: b.state });
    expect(seen).toEqual([
      { frames: [`${VEDDER}/map`, `${FRASER}/regs`, `${HARRISON}/regs`],
        history: { entries: [null, 1, 2, 3, 4], at: 4, left: false } },
      { frames: [`${VEDDER}/map`, `${FRASER}/regs`], history: { entries: [null, 1, 2, 3, 4], at: 3, left: false } },
      { frames: [`${VEDDER}/map`], history: { entries: [null, 1, 2, 3, 4], at: 2, left: false } },
      { frames: [`${VEDDER}/regs`], history: { entries: [null, 1, 2, 3, 4], at: 1, left: false } },
      { frames: ["browse:map"], history: { entries: [null, 1, 2, 3, 4], at: 0, left: false } },
      // back at the root is the browser's: it leaves the app, and the route is not asked
      { frames: ["browse:map"], history: { entries: [null, 1, 2, 3, 4], at: 0, left: true } },
    ]);
    expect(a.pops).toBe(4);
  });

  it("a push while the ‹'s traversal is still in flight waits for it (no entry removed by the late traversal)", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "openWater", item: VEDDER }, { type: "view", view: "map" });
    a.back();                                      // go(-1) queued, not yet traversed
    a.go({ type: "view", view: "conditions" });    // depth back to 2 before the popstate
    b.flush();
    expect({ frames: a.frames(), history: b.state })
      .toEqual({ frames: [`${VEDDER}/conditions`], history: { entries: [null, 1, 2], at: 2, left: false } });
    b.userBack();
    expect({ frames: a.frames(), history: b.state, pops: a.pops })
      .toEqual({ frames: [`${VEDDER}/regs`], history: { entries: [null, 1, 2], at: 1, left: false }, pops: 2 });
  });

  it("a tab change that drops two steps drops two entries in one traversal", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "condSection", section: 9 as never }, { type: "addSpot" });
    const d = backDepth(a.route);
    a.go({ type: "tab", tab: "map" });
    b.flush();
    expect({ d, history: b.state, frames: a.frames(), pops: a.pops })
      .toEqual({ d: 2, history: { entries: [null, 1, 2], at: 0, left: false }, frames: ["browse:map"], pops: 0 });
  });

  it("the browser's forward is refused: a popped frame is gone, so history returns to where the route is", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "openWater", item: VEDDER }, { type: "view", view: "map" });
    b.userBack();
    b.userForward();
    expect({ frames: a.frames(), history: b.state, pops: a.pops })
      .toEqual({ frames: [`${VEDDER}/regs`], history: { entries: [null, 1, 2], at: 1, left: false }, pops: 1 });
  });

  it("the browser going back two entries at once pops two frames", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "openWater", item: VEDDER }, { type: "view", view: "map" });
    b.port.go(-2); b.flush();
    expect({ frames: a.frames(), history: b.state, pops: a.pops })
      .toEqual({ frames: ["browse:map"], history: { entries: [null, 1, 2], at: 0, left: false }, pops: 2 });
  });
});

describe("a part swipe is a part action on the neighbour, in step order", () => {
  const water = (part: number | null, view: "regs" | "map" | "conditions" = "regs"): Route =>
    ({ mode: "water", from: HOME, stack: [{ item: VEDDER, part, tapSid: null, view }] });
  const order = [4, 0, 2];                         // part_ix in step order, mouth first

  it("next is upstream, prev is downstream; nothing past either end; nothing without a part or an order", () => {
    const rows: [string, Route, readonly number[] | null, "next" | "prev"][] = [
      ["mouth next", water(4), order, "next"], ["mouth prev", water(4), order, "prev"],
      ["middle next", water(0), order, "next"], ["middle prev", water(0), order, "prev"],
      ["top next", water(2), order, "next"], ["top prev", water(2, "map"), order, "prev"],
      ["conditions view", water(0, "conditions"), order, "next"],
      ["no part yet", water(null), order, "next"], ["part not in order", water(7), order, "next"],
      ["no order", water(0), null, "next"], ["browse", HOME, order, "next"],
    ];
    expect(rows.map(([k, r, o, s]) => [k, partSwipeAction(r, o, s)])).toEqual([
      ["mouth next", { type: "part", part: 0 }], ["mouth prev", null],
      ["middle next", { type: "part", part: 2 }], ["middle prev", { type: "part", part: 4 }],
      ["top next", null], ["top prev", { type: "part", part: 0 }],
      ["conditions view", { type: "part", part: 2 }],
      ["no part yet", null], ["part not in order", null], ["no order", null], ["browse", null],
    ]);
  });
});
