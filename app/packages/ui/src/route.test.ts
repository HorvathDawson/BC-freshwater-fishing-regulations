import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { HOME, STACK_CAP, bottomBarShown, frameViews, reduce, waterFrame, type Route, type RouteAction } from "./route";

const VEDDER = "gnis:3062" as ItemId, FRASER = "gnis:39325" as ItemId, HARRISON = "gnis:5000" as ItemId;
const run = (r: Route, ...as: RouteAction[]) => as.reduce(reduce, r);

describe("route (P13): one value, one back rule", () => {
  it("browse/conditions -> open a water -> back is the same browse state, whole", () => {
    const browse = run(HOME, { type: "tab", tab: "conditions" });
    const water = reduce(browse, { type: "openWater", item: VEDDER, tapSid: 77 as SectionId, view: "conditions" });
    expect(water.mode).toBe("water");
    expect(bottomBarShown(water)).toBe(false);
    expect(reduce(water, { type: "back" })).toBe(browse);
  });

  it("the views are lateral: water regs -> map view -> back is browse, whole (back never walks the strip)", () => {
    const w = run(HOME, { type: "openWater", item: VEDDER }, { type: "view", view: "map" });
    expect(reduce(w, { type: "back" })).toBe(HOME);
    const c = run(HOME, { type: "openWater", item: VEDDER, view: "conditions" }, { type: "view", view: "regs" });
    expect(reduce(c, { type: "back" })).toBe(HOME);
  });

  it("flowing into two waters, back twice returns to the first water at its part", () => {
    const w = run(HOME, { type: "openWater", item: VEDDER, part: 1 },
                  { type: "flowInto", item: FRASER, part: 4 }, { type: "flowInto", item: HARRISON, part: 0 });
    const back2 = run(w, { type: "view", view: "map" }, { type: "back" }, { type: "back" });
    expect(back2.mode === "water" && back2.stack).toEqual([{ item: VEDDER, part: 1, tapSid: null, at: null, view: "regs" }]);
  });

  it("the water stack is capped at 8, dropping the oldest", () => {
    let r: Route = reduce(HOME, { type: "openWater", item: VEDDER });
    for (let i = 0; i < 12; i++) r = reduce(r, { type: "flowInto", item: `w:${i}` as ItemId, part: 0 });
    expect(r.mode === "water" && r.stack.length).toBe(STACK_CAP);
    expect(r.mode === "water" && r.stack[0]!.item).toBe("w:4");
  });

  it("a tab or a spot is impossible in water mode: there is no bar", () => {
    const w = reduce(HOME, { type: "openWater", item: VEDDER });
    for (const a of [{ type: "tab", tab: "spots" }, { type: "openSpot", id: "s1" }, { type: "search", open: true },
                     { type: "addSpot", at: { lat: 49, lon: -122 } }] as RouteAction[]) {
      expect(reduce(w, a)).toBe(w);
    }
  });

  it("in browse mode back closes what is open, innermost first, then stays", () => {
    const r = run(HOME, { type: "openSpot", id: "s1" }, { type: "addSpot" });
    expect(bottomBarShown(r)).toBe(false);
    const a = reduce(r, { type: "back" });
    expect(a).toEqual(HOME);
    expect(reduce(HOME, { type: "back" })).toBe(HOME);
  });

  it("back on another tab's root goes to the Map tab (the start tab); back on the Map tab stays (Android leaves)", () => {
    const tabs = ["map", "conditions", "spots", "more"] as const;
    const after = tabs.map((t) => {
      const at = reduce(HOME, { type: "tab", tab: t });
      const b = reduce(at, { type: "back" });
      return `${t} -> ${b === at ? "stays" : b.mode === "browse" ? b.tab : b.mode}`;
    });
    expect(after.join(" | ")).toBe("map -> stays | conditions -> map | spots -> map | more -> map");
  });

  it("the spots are the maps' rail's (owner 2026-10-21): a spot opens over the map it was picked on, back returns there", () => {
    const cond = reduce(HOME, { type: "tab", tab: "conditions" });
    const spot = reduce(cond, { type: "openSpot", id: "s1" });
    expect([spot, bottomBarShown(spot), reduce(spot, { type: "back" })]).toEqual([
      { mode: "browse", tab: "conditions", spot: "s1", adding: null, search: false }, true, cond]);
  });

  it("adding a spot keeps the point it was started at (a long press, the map's middle, the reader's location)", () => {
    const at = { lat: 49.0974, lon: -121.9675 };
    const add = reduce(HOME, { type: "addSpot", at });
    const plain = reduce(HOME, { type: "addSpot" });
    expect([add, plain, bottomBarShown(add), reduce(add, { type: "back" })]).toEqual([
      { mode: "browse", tab: "map", spot: null, adding: { at }, search: false },
      { mode: "browse", tab: "map", spot: null, adding: { at: null }, search: false },
      false, HOME]);
  });

  it("(T48) search opens OVER either map, the bar still up; back closes it first, then the tab rule", () => {
    const cond = reduce(HOME, { type: "tab", tab: "conditions" });
    const onMap = reduce(HOME, { type: "search", open: true });
    const onCond = reduce(cond, { type: "search", open: true });
    expect([onMap, onCond, bottomBarShown(onMap), reduce(onMap, { type: "back" }), reduce(onCond, { type: "back" }),
            run(onCond, { type: "back" }, { type: "back" })]).toEqual([
      { mode: "browse", tab: "map", spot: null, adding: null, search: true },
      { mode: "browse", tab: "conditions", spot: null, adding: null, search: true },
      true, HOME, cond, HOME]);
  });

  it("(T48) search is a map's: never over Spots or More, and a tab change closes it", () => {
    const spots = reduce(HOME, { type: "tab", tab: "spots" });
    const more = reduce(HOME, { type: "tab", tab: "more" });
    const open = reduce(HOME, { type: "search", open: true });
    expect([reduce(spots, { type: "search", open: true }), reduce(more, { type: "search", open: true }),
            reduce(open, { type: "tab", tab: "conditions" }), reduce(open, { type: "search", open: false }),
            reduce(HOME, { type: "search", open: false })])
      .toEqual([spots, more, { ...HOME, tab: "conditions" }, HOME, HOME]);
  });

  it("(T48) a water opened from search comes back to search as it was, and the next back closes it", () => {
    const open = reduce(HOME, { type: "search", open: true });
    const water = reduce(open, { type: "openWater", item: VEDDER });
    expect([reduce(water, { type: "back" }), run(water, { type: "back" }, { type: "back" })]).toEqual([open, HOME]);
  });

  it("(T48) a spot opened or added while searching is over the map: back returns to the map with search closed", () => {
    const open = reduce(HOME, { type: "search", open: true });
    expect([reduce(open, { type: "openSpot", id: "s1" }), reduce(open, { type: "addSpot" })]).toEqual([
      { ...HOME, spot: "s1" }, { ...HOME, adding: { at: null } }]);
  });

  it("(T48) Spots is a tab again: a spot opens over it and back returns to it, then to the Map tab", () => {
    const spots = reduce(HOME, { type: "tab", tab: "spots" });
    const spot = reduce(spots, { type: "openSpot", id: "s1" });
    expect([spot, run(spot, { type: "back" }), run(spot, { type: "back" }, { type: "back" })]).toEqual([
      { mode: "browse", tab: "spots", spot: "s1", adding: null, search: false }, spots, HOME]);
  });

  it("search means nothing in water mode: there is no bar", () => {
    const w = reduce(HOME, { type: "openWater", item: VEDDER });
    expect(reduce(w, { type: "search", open: true })).toBe(w);
  });

  it("a tab change clears the spot and the add flow (the old TabBar reset), and part/view mean nothing in browse", () => {
    const r = run(HOME, { type: "openSpot", id: "s1" }, { type: "addSpot" }, { type: "tab", tab: "map" });
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

describe("the views are lateral: the strip or a swipe moves them, back leaves the water for where it came from", () => {
  const AT = { lat: 49.1, lon: -121.9 };
  it("a Conditions-map tap -> Regulations -> Map -> Conditions keeps the tap; back is that same Conditions map", () => {
    const condMap = reduce(HOME, { type: "tab", tab: "conditions" });
    const reach = reduce(condMap, { type: "openWater", item: VEDDER, tapSid: 77 as SectionId, at: AT, view: "conditions" });
    expect(waterFrame(reach)).toEqual({ item: VEDDER, part: null, tapSid: 77, at: AT, view: "conditions" });
    const views = [reach];
    for (const v of ["regs", "map", "conditions"] as const) views.push(reduce(views.at(-1)!, { type: "view", view: v }));
    expect(views.map((r) => waterFrame(r)!.view)).toEqual(["conditions", "regs", "map", "conditions"]);
    expect(views.map((r) => reduce(r, { type: "back" }))).toEqual([condMap, condMap, condMap, condMap]);
    expect(reduce(views.at(-1)!, { type: "back" })).toBe(condMap);
  });

  it("a tap point without its section is dropped (it would place \"you are here\" on no reach)", () => {
    expect(waterFrame(reduce(HOME, { type: "openWater", item: VEDDER, at: AT })))
      .toEqual({ item: VEDDER, part: null, tapSid: null, at: null, view: "regs" });
  });
});

describe("an unnamed reach (no registry item) opens on its Conditions only", () => {
  const AT = { lat: 49.2, lon: -121.5 };
  const condMap = reduce(HOME, { type: "tab", tab: "conditions" });
  it("opened on Conditions it is a frame with no item; it offers Conditions alone, and back is the Conditions map", () => {
    const r = reduce(condMap, { type: "openWater", item: null, tapSid: 501 as SectionId, at: AT, view: "conditions" });
    expect(waterFrame(r)).toEqual({ item: null, part: null, tapSid: 501, at: AT, view: "conditions" });
    expect(frameViews(waterFrame(r)!)).toEqual(["conditions"]);
    expect(["regs", "map"].map((v) => reduce(r, { type: "view", view: v as "regs" | "map" }))).toEqual([r, r]);
    expect(reduce(r, { type: "part", part: 1 })).toBe(r);
    expect(reduce(r, { type: "back" })).toBe(condMap);
  });

  it("refused off Conditions, and refused with no section to stand on", () => {
    expect([reduce(HOME, { type: "openWater", item: null, tapSid: 501 as SectionId, view: "regs" }),
            reduce(HOME, { type: "openWater", item: null, tapSid: 501 as SectionId }),
            reduce(condMap, { type: "openWater", item: null, view: "conditions" })]).toEqual([HOME, HOME, condMap]);
  });

  it("a named water offers all three views", () => {
    expect(frameViews({ item: VEDDER })).toEqual(["regs", "conditions", "map"]);
  });
});
