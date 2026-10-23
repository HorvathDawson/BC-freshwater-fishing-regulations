import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { backDepth } from "./gestures";
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

  it("(T58) a split lake's areas are one water: either frame's group matches; a group of one is not kept", () => {
    const A = "wbk:-21" as ItemId, B = "wbk:-22" as ItemId, AREAS = [A, B];
    const lake = (item: ItemId, group?: readonly ItemId[]): RouteAction =>
      ({ type: "openWater", item, part: 0, ...(group ? { group } : {}) });
    const stack = (r: Route) => (r.mode === "water" ? r.stack.map((f) => `${f.item}${f.group ? `[${f.group.length}]` : ""}`) : []);
    expect([
      stack(run(HOME, lake(A), lake(VEDDER), lake(B, AREAS))),            // the new frame knows the areas
      stack(run(HOME, lake(A, AREAS), lake(VEDDER), lake(B))),            // the held frame knows them
      stack(run(HOME, lake(A), lake(VEDDER), lake(B))),                   // neither: two waters, as before
      stack(run(HOME, lake(VEDDER, [VEDDER]), lake(FRASER), lake(VEDDER))),
    ]).toEqual([
      [`${B}[2]`], [`${B}`], [A, VEDDER, B], [VEDDER],
    ]);
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

  it("(owner 2026-10-22) back from More returns to the tab it was opened from, then the Map tab; More again keeps it", () => {
    const walk = (from: "map" | "conditions" | "spots") => {
      let r = run(HOME, { type: "tab", tab: from }, { type: "tab", tab: "more" }, { type: "tab", tab: "more" });
      const out: string[] = [];
      for (let i = 0; i < 3; i++) {
        const b = reduce(r, { type: "back" });
        out.push(b === r ? "stays" : b.mode === "browse" ? b.tab : b.mode);
        r = b;
      }
      return `${from}: ${out.join(" -> ")}`;
    };
    expect([walk("map"), walk("conditions"), walk("spots")]).toEqual([
      "map: map -> stays -> stays", "conditions: conditions -> map -> stays", "spots: spots -> map -> stays"]);
    // the Map tab is the default and unsaid: More from the map is the plain More route
    expect(run(HOME, { type: "tab", tab: "more" })).toEqual({ ...HOME, tab: "more" });
    // leaving More forgets it
    expect(run(HOME, { type: "tab", tab: "spots" }, { type: "tab", tab: "more" }, { type: "tab", tab: "conditions" }))
      .toEqual({ ...HOME, tab: "conditions" });
  });

  it("the spots are the maps' rail's (owner 2026-10-21): a spot opens over the map it was picked on, back returns there", () => {
    const cond = reduce(HOME, { type: "tab", tab: "conditions" });
    const spot = reduce(cond, { type: "openSpot", id: "s1" });
    expect([spot, bottomBarShown(spot), reduce(spot, { type: "back" })]).toEqual([
      { mode: "browse", tab: "conditions", spot: "s1", adding: null, search: false }, true, cond]);
  });

  it("adding a spot keeps the point it was started at (a long press, a tap in pick mode, the reader's location)", () => {
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

  it("(T48) a water opened by a TAP on the map while searching (a phone's half sheet) comes back to search as it was, " +
     "and the next back closes it", () => {
    const open = reduce(HOME, { type: "search", open: true });
    const water = reduce(open, { type: "openWater", item: VEDDER, tapSid: 77 as SectionId });
    expect([reduce(water, { type: "back" }), run(water, { type: "back" }, { type: "back" })]).toEqual([open, HOME]);
  });

  it("(owner 2026-10-22 late) a water opened FROM A SEARCH RESULT consumes the search: back, and × from any water above " +
     "it, land on the map it was searched over with search closed; the same back depth as search alone", () => {
    const rows = (["map", "conditions"] as const).map((tab) => {
      const open = run(HOME, { type: "tab", tab }, { type: "search", open: true });
      const water = reduce(open, { type: "openWater", item: VEDDER, view: "regs", fromSearch: true });
      const deeper = reduce(water, { type: "flowInto", item: FRASER, part: 0 });
      return [water, reduce(water, { type: "back" }), run(deeper, { type: "back" }, { type: "back" }),
              reduce(deeper, { type: "close" }), backDepth(open), backDepth(water)];
    });
    const landed = (tab: "map" | "conditions") => ({ ...HOME, tab });
    expect(rows).toEqual((["map", "conditions"] as const).map((tab) => [
      { mode: "water", from: landed(tab), stack: [{ item: VEDDER, part: null, tapSid: null, at: null, view: "regs" }] },
      landed(tab), landed(tab), landed(tab), tab === "map" ? 1 : 2, tab === "map" ? 1 : 2,
    ]));
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

/**
 * T53 (owner 2026-10-22): an unnamed stream HAS regulations (its region's base plus any override reaching it, answered
 * by the pipeline per section), so its frame offers Regulations and Conditions; only the water Map stays named-only.
 */
describe("an unnamed reach (no registry item) offers Regulations and Conditions, never the water Map", () => {
  const AT = { lat: 49.2, lon: -121.5 };
  const condMap = reduce(HOME, { type: "tab", tab: "conditions" });
  it("opened on Conditions it is a frame with no item; Regulations is a view of it, Map is not; back is the Conditions map", () => {
    const r = reduce(condMap, { type: "openWater", item: null, tapSid: 501 as SectionId, at: AT, view: "conditions" });
    expect(waterFrame(r)).toEqual({ item: null, part: null, tapSid: 501, at: AT, view: "conditions" });
    expect(frameViews(waterFrame(r)!)).toEqual(["regs", "conditions"]);
    const regs = reduce(r, { type: "view", view: "regs" });
    expect([waterFrame(regs), reduce(r, { type: "view", view: "map" })]).toEqual([
      { item: null, part: null, tapSid: 501, at: AT, view: "regs" }, r]);
    expect(reduce(r, { type: "part", part: 1 })).toBe(r);
    // a view move replaces the frame, so back from Regulations is still the Conditions map (back steps out one layer)
    expect([reduce(r, { type: "back" }), reduce(regs, { type: "back" }), reduce(regs, { type: "close" })])
      .toEqual([condMap, condMap, condMap]);
  });

  it("opened from the Map tab it is the same frame with Regulations in front; refused on Map, and with no section", () => {
    expect([reduce(HOME, { type: "openWater", item: null, tapSid: 501 as SectionId, view: "regs" }),
            reduce(HOME, { type: "openWater", item: null, tapSid: 501 as SectionId }),
            reduce(HOME, { type: "openWater", item: null, tapSid: 501 as SectionId, view: "map" }),
            reduce(condMap, { type: "openWater", item: null, view: "conditions" })]).toEqual([
      { mode: "water", from: HOME, stack: [{ item: null, part: null, tapSid: 501, at: null, view: "regs" }] },
      { mode: "water", from: HOME, stack: [{ item: null, part: null, tapSid: 501, at: null, view: "regs" }] },
      HOME, condMap]);
  });

  it("a named water offers all three views", () => {
    expect(frameViews({ item: VEDDER })).toEqual(["regs", "conditions", "map"]);
  });
});

/**
 * (Owner 2026-10-22 late) BACK STEPS OUT ONE LAYER, it does not undo moves: a move within one water (its views, its
 * parts, a split lake's other closure area, the same water opened again) REPLACES the frame; a DIFFERENT water opened
 * from inside is one layer — and a water already under it is gone back to, not stacked again; close leaves every
 * water at once, for exactly where the first one was opened from.
 */
describe("back steps out one layer: moves inside a water replace, a different water is one layer, close leaves all", () => {
  const CHW = "gnis:8634" as ItemId, LAKE_A = "wbk:-21" as ItemId, LAKE_B = "wbk:-22" as ItemId;
  const frame = (item: ItemId | null, part: number | null, view: "regs" | "map" | "conditions", tapSid: number | null = null) =>
    ({ item, part, tapSid: tapSid as SectionId | null, at: null, view });
  const from = { ...HOME, tab: "conditions" as const, search: true };
  const opened = reduce(from, { type: "openWater", item: CHW, part: 1, view: "map" });

  it("N moves inside one water: ONE frame, the last move's; ONE back is where the water was opened from", () => {
    const moved = run(opened, { type: "part", part: 2 }, { type: "part", part: 3 }, { type: "view", view: "regs" },
                      { type: "view", view: "conditions" }, { type: "part", part: 0 }, { type: "view", view: "map" },
                      { type: "openWater", item: CHW, part: 2, view: "regs" });
    expect([moved, reduce(moved, { type: "back" })]).toEqual([
      { mode: "water", from, stack: [frame(CHW, 2, "regs")] }, from]);
  });

  it("the same water opened again from inside it (a tap on its own section) replaces its frame, tap and all", () => {
    const r = reduce(opened, { type: "openWater", item: CHW, tapSid: 9 as SectionId, at: { lat: 49, lon: -121 }, view: "regs" });
    expect(r).toEqual({ mode: "water", from,
                        stack: [{ item: CHW, part: null, tapSid: 9, at: { lat: 49, lon: -121 }, view: "regs" }] });
  });

  it("a split lake's other closure area is the same water: a part move, never a layer", () => {
    const lake = reduce(HOME, { type: "openWater", item: LAKE_A, part: 0, view: "map" });
    const r = run(lake, { type: "part", part: 0, item: LAKE_B }, { type: "part", part: 1, item: LAKE_A });
    expect([r, reduce(r, { type: "back" })]).toEqual([
      { mode: "water", from: HOME, stack: [frame(LAKE_A, 1, "map")] }, HOME]);
  });

  it("a tributary chain A -> B: back is A, back again is the main view", () => {
    const b = run(opened, { type: "openWater", item: VEDDER, tapSid: 5 as SectionId, view: "regs" },
                  { type: "view", view: "map" }, { type: "part", part: 1 });
    const back1 = reduce(b, { type: "back" });
    expect([b, back1, reduce(back1, { type: "back" })]).toEqual([
      { mode: "water", from, stack: [frame(CHW, 1, "map"), frame(VEDDER, 1, "map", 5)] },
      { mode: "water", from, stack: [frame(CHW, 1, "map")] },
      from]);
  });

  it("a water already under the top is gone back to, not stacked again: A -> B -> A is A alone, at the new move", () => {
    const r = run(opened, { type: "flowInto", item: VEDDER, part: 0 }, { type: "openWater", item: FRASER, view: "regs" },
                  { type: "openWater", item: CHW, part: 3, view: "map" });
    const f = run(opened, { type: "openWater", item: VEDDER, part: 0, view: "map" }, { type: "flowInto", item: CHW, part: 2 });
    expect([r, f]).toEqual([
      { mode: "water", from, stack: [frame(CHW, 3, "map")] },
      { mode: "water", from, stack: [frame(CHW, 2, "regs")] }]);
  });

  it("an unnamed reach is its section: the same reach replaces, another reach is a layer", () => {
    const reach = reduce(HOME, { type: "openWater", item: null, tapSid: 7 as SectionId, view: "conditions" });
    const same = reduce(reach, { type: "openWater", item: null, tapSid: 7 as SectionId, view: "conditions" });
    const other = reduce(reach, { type: "openWater", item: null, tapSid: 8 as SectionId, view: "conditions" });
    expect([same, other.mode === "water" && other.stack.map((s) => s.tapSid)]).toEqual([
      { mode: "water", from: HOME, stack: [frame(null, null, "conditions", 7)] }, [7, 8]]);
  });

  it("(owner 2026-10-22 late, 'one system') a cross into the water this one continues as / from REPLACES the top " +
     "frame: to and fro never stacks, ONE back leaves the system for where it was opened from; under a tributary it " +
     "replaces only the top; a water already under the top is gone back to; nothing in browse", () => {
    const cross = (item: ItemId, part: number): RouteAction => ({ type: "cross", item, part });
    const over = run(opened, cross(VEDDER, 0), cross(CHW, 1), cross(VEDDER, 0), cross(CHW, 1));
    const tribUnder = run(reduce(from, { type: "openWater", item: FRASER, view: "regs" }),
                          { type: "flowInto", item: CHW, part: 1 }, cross(VEDDER, 0), cross(CHW, 1));
    const backTo = run(opened, { type: "flowInto", item: FRASER, part: 0 }, cross(CHW, 3));
    expect([over, reduce(over, { type: "back" }), tribUnder, reduce(tribUnder, { type: "back" }), backTo,
            reduce(from, cross(VEDDER, 0)) === from]).toEqual([
      { mode: "water", from, stack: [frame(CHW, 1, "map")] }, from,
      { mode: "water", from, stack: [frame(FRASER, null, "regs"), frame(CHW, 1, "map")] },
      { mode: "water", from, stack: [frame(FRASER, null, "regs")] },
      { mode: "water", from, stack: [frame(CHW, 3, "map")] },
      true]);
  });

  it("(owner 2026-10-22 late, 'one system') a cross, THEN a tributary opened from inside: the tributary is one layer " +
     "over the water crossed into; back is that water, back again leaves the system", () => {
    const cross = (item: ItemId, part: number): RouteAction => ({ type: "cross", item, part });
    const trib = run(opened, cross(VEDDER, 0), { type: "openWater", item: HARRISON, tapSid: 4 as SectionId, view: "regs" });
    const back1 = reduce(trib, { type: "back" });
    expect([trib, back1, reduce(back1, { type: "back" })]).toEqual([
      { mode: "water", from, stack: [frame(VEDDER, 0, "map"), frame(HARRISON, null, "regs", 4)] },
      { mode: "water", from, stack: [frame(VEDDER, 0, "map")] },
      from]);
  });

  it("close leaves every water at once, for where the first was opened from (search and all); nothing in browse", () => {
    const deep = run(opened, { type: "openWater", item: VEDDER, view: "regs" }, { type: "flowInto", item: FRASER, part: 0 });
    expect([deep.mode === "water" && deep.stack.length, reduce(deep, { type: "close" }), reduce(opened, { type: "close" }),
            reduce(from, { type: "close" }) === from]).toEqual([3, from, from, true]);
  });
});
