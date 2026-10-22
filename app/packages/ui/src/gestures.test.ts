/**
 * The one back rule across every input (G1), and the browser's history kept in step with the route.
 *
 * The history here is a model of the browser's own: a list of entries and a position, `pushState` truncating the
 * forward entries, and `go` TRAVERSING LATER (a queued task, as browsers do) and then firing one `popstate`. That
 * asynchrony is what makes a double pop possible, so the model keeps it.
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { backDepth, edgeBackDepth, historySync, viewSwipeAction, type HistoryPort } from "./gestures";
import { HOME, STACK_CAP, reduce, type Route, type RouteAction } from "./route";

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
  it("browse root 0; the Conditions tab 1 (back goes to the Map tab first); a water tapped there 2; its map view still 2 (views are lateral); a flow 3", () => {
    const tap: RouteAction[] = [{ type: "tab", tab: "conditions" },
                                { type: "openWater", item: VEDDER, tapSid: 5 as never, view: "conditions" }];
    const rows: [string, RouteAction[]][] = [
      ["root", []],
      ["conditions tab", tap.slice(0, 1)],
      ["water", tap],
      ["map", [...tap, { type: "view", view: "map" }]],
      ["flow", [...tap, { type: "view", view: "map" }, { type: "flowInto", item: FRASER, part: 0 }]],
    ];
    expect(rows.map(([k, as]) => [k, backDepth(as.reduce(reduce, HOME as Route))]))
      .toEqual([["root", 0], ["conditions tab", 1], ["water", 2], ["map", 2], ["flow", 3]]);
  });

  it("search is one back (to the Map tab); its eye two (back returns to search first); a water opened there, three", () => {
    const shown: RouteAction[] = [{ type: "tab", tab: "search" }, { type: "show", item: VEDDER }];
    expect([backDepth(shown.slice(0, 1).reduce(reduce, HOME as Route)), backDepth(shown.reduce(reduce, HOME as Route)),
            backDepth([...shown, { type: "openWater", item: VEDDER } as RouteAction].reduce(reduce, HOME as Route))])
      .toEqual([1, 2, 3]);
  });
});

describe("edgeBackDepth: the iOS edge swipe has no tab-to-tab step (iOS tabs are not history)", () => {
  it("a tab root has nothing to swipe back to; a water or spot over it does; open layers count", () => {
    const at = (as: RouteAction[]) => as.reduce(reduce, HOME as Route);
    const rows: [string, Route, number][] = [
      ["map root", HOME, 0],
      ["search root", at([{ type: "tab", tab: "search" }]), 0],
      ["spots root", at([{ type: "tab", tab: "spots" }]), 0],
      ["a spot", at([{ type: "openSpot", id: "s1" }]), 1],
      ["search's eye", at([{ type: "tab", tab: "search" }, { type: "show", item: VEDDER }]), 1],
      ["a water from conditions", at([{ type: "tab", tab: "conditions" }, { type: "openWater", item: VEDDER }]), 1],
      ["a water from the map", at([{ type: "openWater", item: VEDDER }]), 1],
    ];
    const layers = 1;
    expect(rows.map(([k, r, n]) => `${k}: ${edgeBackDepth(r, backDepth(r))}/${n} +sheet ${edgeBackDepth(r, backDepth(r) + layers)}`))
      .toEqual(["map root: 0/0 +sheet 1", "search root: 0/0 +sheet 1", "spots root: 0/0 +sheet 1",
                "a spot: 1/1 +sheet 2", "search's eye: 1/1 +sheet 2",
                "a water from conditions: 1/1 +sheet 2", "a water from the map: 1/1 +sheet 2"]);
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
      .toEqual({ frames: [`${VEDDER}/map`], pops: 1, history: { entries: [null, 1, 2], at: 1, left: false } });
  });

  it("the browser's back button", () => {
    const b = browser(); const a = app(b);
    a.go(...deep);
    b.userBack();
    expect({ frames: a.frames(), pops: a.pops, history: b.state })
      .toEqual({ frames: [`${VEDDER}/map`], pops: 1, history: { entries: [null, 1, 2], at: 1, left: false } });
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
    expect(seen).toEqual([
      { frames: [`${VEDDER}/map`, `${FRASER}/regs`, `${HARRISON}/regs`],
        history: { entries: [null, 1, 2, 3], at: 3, left: false } },
      { frames: [`${VEDDER}/map`, `${FRASER}/regs`], history: { entries: [null, 1, 2, 3], at: 2, left: false } },
      // the views are lateral: back from the first water's map view leaves the water
      { frames: [`${VEDDER}/map`], history: { entries: [null, 1, 2, 3], at: 1, left: false } },
      { frames: ["browse:map"], history: { entries: [null, 1, 2, 3], at: 0, left: false } },
      // back at the root is the browser's: it leaves the app, and the route is not asked
      { frames: ["browse:map"], history: { entries: [null, 1, 2, 3], at: 0, left: true } },
    ]);
    expect(a.pops).toBe(3);
  });

  it("a push while the ‹'s traversal is still in flight waits for it (no entry removed by the late traversal)", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "openWater", item: VEDDER }, { type: "flowInto", item: FRASER, part: 0 });
    a.back();                                                // go(-1) queued, not yet traversed
    a.go({ type: "flowInto", item: HARRISON, part: 1 });    // depth back to 2 before the popstate
    b.flush();
    expect({ frames: a.frames(), history: b.state })
      .toEqual({ frames: [`${VEDDER}/regs`, `${HARRISON}/regs`], history: { entries: [null, 1, 2], at: 2, left: false } });
    b.userBack();
    expect({ frames: a.frames(), history: b.state, pops: a.pops })
      .toEqual({ frames: [`${VEDDER}/regs`], history: { entries: [null, 1, 2], at: 1, left: false }, pops: 2 });
  });

  it("a tab change that drops three steps drops three entries in one traversal", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "tab", tab: "search" }, { type: "show", item: VEDDER }, { type: "addSpot" });
    const d = backDepth(a.route);
    a.go({ type: "tab", tab: "map" });
    b.flush();
    expect({ d, history: b.state, frames: a.frames(), pops: a.pops })
      .toEqual({ d: 3, history: { entries: [null, 1, 2, 3], at: 0, left: false }, frames: ["browse:map"], pops: 0 });
  });

  it("the browser's forward is refused: a popped frame is gone, so history returns to where the route is", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "openWater", item: VEDDER }, { type: "flowInto", item: FRASER, part: 0 });
    b.userBack();
    b.userForward();
    expect({ frames: a.frames(), history: b.state, pops: a.pops })
      .toEqual({ frames: [`${VEDDER}/regs`], history: { entries: [null, 1, 2], at: 1, left: false }, pops: 1 });
  });

  it("the browser going back two entries at once pops two frames", () => {
    const b = browser(); const a = app(b);
    a.go({ type: "openWater", item: VEDDER }, { type: "flowInto", item: FRASER, part: 0 });
    b.port.go(-2); b.flush();
    expect({ frames: a.frames(), history: b.state, pops: a.pops })
      .toEqual({ frames: ["browse:map"], history: { entries: [null, 1, 2], at: 0, left: false }, pops: 2 });
  });
});

describe("at the stack cap the history does not grow", () => {
  it("a push past STACK_CAP drops the oldest frame: the depth holds at the cap and no entry is added", () => {
    const b = browser(); const a = app(b);
    const items = Array.from({ length: STACK_CAP + 2 }, (_, i) => `gnis:${9000 + i}` as ItemId);
    a.go({ type: "openWater", item: items[0]! });
    for (let i = 1; i < STACK_CAP; i++) a.go({ type: "flowInto", item: items[i]!, part: 0 });
    const full = { depth: backDepth(a.route), history: b.state };
    a.go({ type: "flowInto", item: items[STACK_CAP]!, part: 0 });
    a.go({ type: "flowInto", item: items[STACK_CAP + 1]!, part: 0 });
    b.flush();
    const past = { depth: backDepth(a.route), history: b.state, first: a.frames()[0], last: a.frames().at(-1) };
    b.userBack();
    const after = { depth: backDepth(a.route), history: b.state, last: a.frames().at(-1), pops: a.pops };
    const entries = [null, 1, 2, 3, 4, 5, 6, 7, 8];
    expect({ cap: STACK_CAP, full, past, after }).toEqual({
      cap: 8,
      full: { depth: 8, history: { entries, at: 8, left: false } },
      past: { depth: 8, history: { entries, at: 8, left: false },
              first: "gnis:9002/regs", last: "gnis:9009/regs" },
      after: { depth: 7, history: { entries, at: 7, left: false }, last: "gnis:9008/regs", pops: 1 },
    });
  });
});

describe("a view swipe moves between a water's views, Regulations · Conditions · Map", () => {
  const water = (view: "regs" | "map" | "conditions"): Route =>
    ({ mode: "water", from: HOME, stack: [{ item: VEDDER, part: 2, tapSid: null, at: null, view }] });
  const AT = { lat: 49.1, lon: -121.9 };
  const reach: Route = { mode: "water", from: { ...HOME, tab: "conditions" },
                         stack: [{ item: null, part: null, tapSid: 77 as SectionId, at: AT, view: "conditions" }] };

  it("finger left is the view to the right; nothing past either end; the part never moves", () => {
    const rows: [string, Route, "next" | "prev"][] = [
      ["regs next", water("regs"), "next"], ["regs prev", water("regs"), "prev"],
      ["conditions next", water("conditions"), "next"], ["conditions prev", water("conditions"), "prev"],
      ["map next", water("map"), "next"], ["map prev", water("map"), "prev"],
    ];
    expect(rows.map(([k, r, s]) => [k, viewSwipeAction(r, s)])).toEqual([
      ["regs next", { type: "view", view: "conditions" }], ["regs prev", null],
      ["conditions next", { type: "view", view: "map" }], ["conditions prev", { type: "view", view: "regs" }],
      ["map next", null], ["map prev", { type: "view", view: "conditions" }],
    ]);
  });

  it("an unnamed reach offers Conditions alone: nothing either way; nothing anywhere in browse", () => {
    const rows: [string, Route, "next" | "prev"][] = [
      ["reach prev", reach, "prev"], ["reach next", reach, "next"],
      ["conditions map", { ...HOME, tab: "conditions" }, "prev"], ["browse map", HOME, "prev"],
    ];
    expect(rows.map(([k, r, s]) => [k, viewSwipeAction(r, s)])).toEqual([
      ["reach prev", null], ["reach next", null], ["conditions map", null], ["browse map", null],
    ]);
  });
});
