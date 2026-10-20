import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { HOME, STACK_CAP, bottomBarShown, reduce, type Route, type RouteAction } from "./route";

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
    expect(back2.mode === "water" && back2.stack).toEqual([{ item: VEDDER, part: 1, tapSid: null, view: "regs" }]);
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

  it("a tab change clears the drill, the spot and the add flow (the old TabBar reset), and part/view mean nothing in browse", () => {
    const r = run(HOME, { type: "condSection", section: 5 as SectionId }, { type: "tab", tab: "map" });
    expect(r).toEqual(HOME);
    expect(reduce(HOME, { type: "view", view: "map" })).toBe(HOME);
    expect(reduce(HOME, { type: "part", part: 2 })).toBe(HOME);
  });
});
