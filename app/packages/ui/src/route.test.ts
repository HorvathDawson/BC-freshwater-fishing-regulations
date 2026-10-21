import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { HOME, STACK_CAP, bottomBarShown, reduce, waterFrame, type Route, type RouteAction } from "./route";

const VEDDER = "gnis:3062" as ItemId, FRASER = "gnis:39325" as ItemId, HARRISON = "gnis:5000" as ItemId;
const run = (r: Route, ...as: RouteAction[]) => as.reduce(reduce, r);

describe("route (P13): one value, one back rule", () => {
  it("browse/map -> open a water -> back is the same browse state, whole", () => {
    const browse = run(HOME, { type: "tab", tab: "conditions" }, { type: "condSection", section: 77 as SectionId, at: { lat: 49.1, lon: -121.9 } });
    const water = reduce(browse, { type: "openWater", item: VEDDER, tapSid: 77 as SectionId });
    expect(water.mode).toBe("water");
    expect(bottomBarShown(water)).toBe(false);
    expect(reduce(water, { type: "back" })).toBe(browse);
  });

  it("water regs -> map view -> back -> regs -> back -> browse", () => {
    const w = run(HOME, { type: "openWater", item: VEDDER }, { type: "view", view: "map" });
    const regs = reduce(w, { type: "back" });
    expect(regs.mode === "water" && regs.stack.map((f) => f.view)).toEqual(["regs"]);
    expect(reduce(regs, { type: "back" })).toEqual(HOME);
  });

  it("flowing into two waters, back twice returns to the first water at its part", () => {
    const w = run(HOME, { type: "openWater", item: VEDDER, part: 1 },
                  { type: "flowInto", item: FRASER, part: 4 }, { type: "flowInto", item: HARRISON, part: 0 });
    const back2 = run(w, { type: "back" }, { type: "back" });
    expect(back2.mode === "water" && back2.stack).toEqual([{ item: VEDDER, part: 1, tapSid: null, at: null, view: "regs" }]);
  });

  it("the water stack is capped at 8, dropping the oldest", () => {
    let r: Route = reduce(HOME, { type: "openWater", item: VEDDER });
    for (let i = 0; i < 12; i++) r = reduce(r, { type: "flowInto", item: `w:${i}` as ItemId, part: 0 });
    expect(r.mode === "water" && r.stack.length).toBe(STACK_CAP);
    expect(r.mode === "water" && r.stack[0]!.item).toBe("w:4");
  });

  it("a tab, a spot or a Conditions drill is impossible in water mode: there is no bar", () => {
    const w = reduce(HOME, { type: "openWater", item: VEDDER });
    for (const a of [{ type: "tab", tab: "spots" }, { type: "openSpot", id: "s1" },
                     { type: "condSection", section: 1 as SectionId }] as RouteAction[]) {
      expect(reduce(w, a)).toBe(w);
    }
  });

  it("in browse mode back closes what is open, innermost first, then stays", () => {
    const r = run(HOME, { type: "openSpot", id: "s1" }, { type: "addSpot" });
    expect(bottomBarShown(r)).toBe(false);
    const a = reduce(r, { type: "back" });
    expect(a).toEqual({ ...HOME, tab: "spots" });
    expect(reduce(HOME, { type: "back" })).toBe(HOME);
  });

  it("search's eye SHOWS the water on the main map; back returns to search", () => {
    const search = reduce(HOME, { type: "tab", tab: "search" });
    const shown = reduce(search, { type: "show", item: VEDDER });
    expect(shown).toEqual({ mode: "browse", tab: "map", cond: null, spot: null, adding: false, shown: VEDDER });
    expect(bottomBarShown(shown)).toBe(true);
    expect(reduce(shown, { type: "back" })).toEqual(search);
  });

  it("a water opened from the shown map comes back to it, and the next back to search", () => {
    const search = reduce(HOME, { type: "tab", tab: "search" });
    const shown = reduce(search, { type: "show", item: VEDDER });
    const water = reduce(shown, { type: "openWater", item: VEDDER });
    expect(reduce(water, { type: "back" })).toBe(shown);
    expect(run(water, { type: "back" }, { type: "back" })).toEqual(search);
  });

  it("a tab change forgets the shown water — back on a map the reader chose stays", () => {
    const shown = run(HOME, { type: "tab", tab: "search" }, { type: "show", item: VEDDER });
    const map = reduce(shown, { type: "tab", tab: "map" });
    expect(map).toEqual(HOME);
    expect(reduce(map, { type: "back" })).toBe(map);
  });

  it("show means nothing in water mode: there is no search under it", () => {
    const w = reduce(HOME, { type: "openWater", item: VEDDER });
    expect(reduce(w, { type: "show", item: FRASER })).toBe(w);
  });

  it("a tab change clears the drill, the spot and the add flow (the old TabBar reset), and part/view mean nothing in browse", () => {
    const r = run(HOME, { type: "condSection", section: 5 as SectionId }, { type: "tab", tab: "map" });
    expect(r).toEqual(HOME);
    expect(reduce(HOME, { type: "view", view: "map" })).toBe(HOME);
    expect(reduce(HOME, { type: "part", part: 2 })).toBe(HOME);
  });
});

describe("waterFrame (T01): what the water screen is handed — the water, its part, the section TAPPED", () => {
  it("null while browsing", () => {
    expect(waterFrame(HOME)).toBeNull();
  });

  it("a map tap keeps its section; a search open has none; a water flowed into has none, at its part", () => {
    const tap = reduce(HOME, { type: "openWater", item: VEDDER, tapSid: 77 as SectionId });
    expect(waterFrame(tap)).toEqual({ item: VEDDER, part: null, tapSid: 77, at: null, view: "regs" });
    expect(waterFrame(reduce(HOME, { type: "openWater", item: VEDDER }))).toEqual({ item: VEDDER, part: null, tapSid: null, at: null, view: "regs" });
    const flowed = reduce(tap, { type: "flowInto", item: FRASER, part: 4 });
    expect(waterFrame(flowed)).toEqual({ item: FRASER, part: 4, tapSid: null, at: null, view: "regs" });
    // back to the first water: its own tap again, never the second water's
    expect(waterFrame(reduce(flowed, { type: "back" }))).toEqual({ item: VEDDER, part: null, tapSid: 77, at: null, view: "regs" });
  });

  it("a tap on another water from inside water mode is that water's tap", () => {
    const w = run(HOME, { type: "openWater", item: VEDDER, tapSid: 77 as SectionId },
                  { type: "openWater", item: HARRISON, tapSid: 12 as SectionId });
    expect(waterFrame(w)).toEqual({ item: HARRISON, part: null, tapSid: 12, at: null, view: "regs" });
  });
});

describe("the views are lateral: the strip or a swipe moves them, back returns to the regs, then where it came from", () => {
  const AT = { lat: 49.1, lon: -121.9 };
  it("a Conditions reach -> its Regulations keeps the tap; Conditions -> Map -> back -> back is that same reach", () => {
    const reach = run(HOME, { type: "condSection", section: 77 as SectionId, at: AT });
    const regs = reduce(reach, { type: "openWater", item: VEDDER, tapSid: 77 as SectionId, at: AT });
    expect(waterFrame(regs)).toEqual({ item: VEDDER, part: null, tapSid: 77, at: AT, view: "regs" });
    const views = [regs];
    for (const v of ["conditions", "map", "conditions"] as const) views.push(reduce(views.at(-1)!, { type: "view", view: v }));
    expect(views.map((r) => waterFrame(r)!.view)).toEqual(["regs", "conditions", "map", "conditions"]);
    const back1 = reduce(views.at(-1)!, { type: "back" });
    expect(waterFrame(back1)).toEqual({ item: VEDDER, part: null, tapSid: 77, at: AT, view: "regs" });
    expect(reduce(back1, { type: "back" })).toBe(reach);
  });

  it("a tap point without its section is dropped (it would place \"you are here\" on no reach)", () => {
    expect(waterFrame(reduce(HOME, { type: "openWater", item: VEDDER, at: AT })))
      .toEqual({ item: VEDDER, part: null, tapSid: null, at: null, view: "regs" });
  });
});
