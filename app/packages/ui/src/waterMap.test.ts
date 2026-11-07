/**
 * P15: the water map's state and view model over THE RELEASE'S OWN TABLES — the committed regs fixture release (T10),
 * whose water file holds the Chilliwack, Vedder, Cowichan, Kootenay Lake's closure areas and the Zymoetz exactly as
 * release r-1bb06fa28daf wrote them (`part_geo`, `part_mark`, `part_section`, `water_exit`).
 */
import { beforeAll, describe, expect, it } from "vitest";
import type { WaterGroup } from "@app/core";
import { loadWaterGroup, openRegsStore, type RegsStore } from "@app/data/regs";
import { openRegsFixtureWithLines, sectionNames, type SectionNames } from "@app/data/release/node";
import type { ItemId } from "@app/data";
import { HOME, reduce, type Route } from "./route";
import {
  crossAction, exitAction, partAction, showRegsActions, waterMapStart, waterMapStartIx, waterMapStep, waterMapView,
  type WaterMapAction,
  type WaterMapState,
} from "./waterMap";

let st: RegsStore;
let sec: SectionNames;
beforeAll(async () => { const loader = openRegsFixtureWithLines(); st = await openRegsStore(loader); sec = sectionNames(loader); });

const run = (g: WaterGroup, s: WaterMapState, ...as: WaterMapAction[]) => as.reduce((x, a) => waterMapStep(g, x, a), s);
const view = (g: WaterGroup, s: WaterMapState) => waterMapView(g, s)!;
/** What the bar says, whole. */
const bar = (g: WaterGroup, s: WaterMapState) => {
  const v = view(g, s);
  return `${v.where} | ${v.current.name} | ${v.down.label}${v.down.disabled ? " (off)" : ""} | ${v.up.label}`
    + `${v.up.disabled ? " (off)" : ""}${v.exit ? ` | ${v.exit.label}` : ""}`;
};
const sids = async (ord: number, partIx: number) =>
  (await (await st.water()).all("SELECT sid FROM part_section WHERE ord = ? AND part_ix = ? ORDER BY sid", ord, partIx))
    .map((r) => Number(r.sid));

describe("the Chilliwack: ▼ / ▲ through its four parts, the ends disabled, the mouth's exit", () => {
  it("from the mouth part to the top and back", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    let s = waterMapStart(waterMapStartIx(g, "gnis:8634", 1));     // part_ix 1 is the mouth part (rank 0)
    expect(s.at).toBe(0);
    expect(bar(g, s)).toBe("Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge"
      // ▼ at the mouth part goes on into the Vedder (`water_continue`); "Flows into" stays
      + " | Downstream into Vedder River | Upstream | Flows into Vedder River ›");
    expect(view(g, s).exit).toEqual({ label: "Flows into Vedder River ›", item: "gnis:3062", part: 0 });
    const seen = [bar(g, s)];
    for (let k = 0; k < 4; k += 1) { s = run(g, s, { type: "up" }); seen.push(bar(g, s)); }
    expect(seen.slice(1)).toEqual([
      "Part 2 of 4 · Chilliwack River | Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence | Downstream | Upstream",
      "Part 3 of 4 · Chilliwack River | 100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary | Downstream | Upstream",
      "Part 4 of 4 · Chilliwack River | Chilliwack River Ecological Reserve boundary → Source | Downstream | Highest part (off)",
      // ▲ at the top changes nothing
      "Part 4 of 4 · Chilliwack River | Chilliwack River Ecological Reserve boundary → Source | Downstream | Highest part (off)",
    ]);
    s = run(g, s, { type: "down" }, { type: "down" }, { type: "down" }, { type: "down" });
    expect(s).toEqual(waterMapStart(0));
  });

  it("selected is exactly the part's part_section rows; the rest of the water is `water`", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const ord = (await st.itemOrd("gnis:8634"))!;
    for (const [at, partIx] of [[0, 1], [1, 2], [2, 0], [3, 3]] as const) {
      const v = view(g, waterMapStart(at));
      expect([at, v.selected]).toEqual([at, await sids(ord, partIx)]);
      expect([...v.selected, ...v.water].sort((a, b) => a - b)).toEqual(
        (await (await st.water()).all("SELECT sid FROM part_section WHERE ord = ? ORDER BY sid", ord)).map((r) => Number(r.sid)));
    }
  });

  it("the marks: a ring at each placed boundary, the current part's two ends labelled", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const marks = (s: WaterMapState) => view(g, s).marks.map((m) => `${m.ix} ${m.label ?? "(ring)"}${m.on ? " (open)" : ""}`);
    expect(marks(waterMapStart(1))).toEqual([
      "0 (ring)", "1 Tamihi Rapids Bridge", "2 fishing boundary signs 100 m below the Slesse Creek confluence"]);
    expect(view(g, waterMapStart(1)).marks[1]).toEqual({ ix: 1, lon: -121.83788, lat: 49.071839,
      label: "Tamihi Rapids Bridge", a11y: "Boundary: Tamihi Rapids Bridge", on: false });
  });

  it("tapping a mark opens its words with the parts above and below it; a second tap closes it", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const s = run(g, waterMapStart(0), { type: "mark", ix: 2 });
    expect(view(g, s).pop).toEqual({
      title: "fishing boundary signs 100 m below the Slesse Creek confluence", close: "Close",
      upLabel: "Upstream", up: [{ ix: 2, item: "gnis:8634", partIx: 0, note: "Chilliwack River",
        name: "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary" }],
      downLabel: "Downstream", down: { ix: 1, item: "gnis:8634", partIx: 2, note: "Chilliwack River",
        name: "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence" } });
    expect(view(g, s).marks.map((m) => [m.ix, m.on, m.label !== null])).toEqual([[0, false, false], [1, false, true], [2, true, true]]);
    expect(run(g, s, { type: "mark", ix: 2 }).mark).toBeNull();
    expect(run(g, s, { type: "mark", ix: null }).mark).toBeNull();
    expect(run(g, s, { type: "mark", ix: 9 })).toBe(s);
    // a step closes it
    expect(run(g, s, { type: "up" })).toEqual(waterMapStart(1));
  });

  it("a tap on a part's section selects it; a section of another water changes nothing", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const s = waterMapStart(0);
    // a section of the Chilliwack's top part, and one of the Vedder's, read from the fixture (handles move with each re-cut)
    expect(run(g, s, { type: "section", sid: g.parts[3]!.sids[0]! })).toEqual(waterMapStart(3));
    const vedder = await loadWaterGroup(st, "gnis:3062");
    expect(run(g, s, { type: "section", sid: vedder.parts[0]!.sids[0]! })).toBe(s);
    expect(run(g, s, { type: "part", ix: 2 })).toEqual(waterMapStart(2));
    expect(run(g, s, { type: "part", ix: 4 })).toBe(s);
  });

  it("⤢ frames the whole river, and back to the part", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const part = view(g, waterMapStart(1));
    expect([part.whole, part.bounds, part.fitKey]).toEqual([
      { label: "⤢ Whole river", aria: "Whole river", on: false }, [-121.837917, 49.066457, -121.710461, 49.084975], "gnis:8634:2"]);
    const whole = view(g, run(g, waterMapStart(1), { type: "whole" }));
    expect([whole.whole, whole.bounds, whole.fitKey]).toEqual([
      { label: "This part", aria: "This part", on: true }, [-121.966394, 48.999855, -121.39612, 49.114995], "gnis:8634:whole"]);
    expect(run(g, run(g, waterMapStart(1), { type: "whole" }), { type: "whole" })).toEqual(waterMapStart(1));
  });

  it("Show regulations for this part names the part's item and part_ix", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect(view(g, waterMapStart(2)).regs).toEqual({ label: "Show regulations for this part", item: "gnis:8634", partIx: 0 });
  });
});

describe("(T150) the parts of this water, listed under the map — the header's Parts sheet moved here", () => {
  it("the Chilliwack: closed, the toggle says how many; open, every part by name and place, the part shown marked", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect(view(g, waterMapStart(1)).list).toEqual({ title: "Parts of this water", toggle: "Show all 4 parts", open: false,
      rows: [
        { ix: 0, item: "gnis:8634", partIx: 1, note: "", here: false,
          name: "Vedder Crossing Bridge → Tamihi Rapids Bridge",
          aria: "Vedder Crossing Bridge → Tamihi Rapids Bridge, part 1 of 4 · Chilliwack River" },
        { ix: 1, item: "gnis:8634", partIx: 2, note: "", here: true,
          name: "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence",
          aria: "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence, part 2 of 4 · Chilliwack River" },
        { ix: 2, item: "gnis:8634", partIx: 0, note: "", here: false,
          name: "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary",
          aria: "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary, part 3 of 4 · Chilliwack River" },
        { ix: 3, item: "gnis:8634", partIx: 3, note: "", here: false,
          name: "Chilliwack River Ecological Reserve boundary → Source",
          aria: "Chilliwack River Ecological Reserve boundary → Source, part 4 of 4 · Chilliwack River" }] });
    const open = run(g, waterMapStart(1), { type: "list" });
    expect([open, view(g, open).list!.toggle, view(g, open).list!.open]).toEqual([
      { ...waterMapStart(1), list: true }, "Hide the list of parts", true]);
    // a pick is the part step: the map moves there and the list closes; toggling again closes it
    expect(run(g, open, { type: "part", ix: 3 })).toEqual(waterMapStart(3));
    expect(run(g, open, { type: "list" })).toEqual(waterMapStart(1));
    // opening the list closes an open mark or chooser (one card over the map at a time)
    expect(run(g, waterMapStart(1), { type: "mark", ix: 1 }, { type: "list" })).toEqual({ ...waterMapStart(1), list: true });
  });

  it("a split lake: its closure areas are the list (each its own water, named beside), the area shown marked", async () => {
    const g = await loadWaterGroup(st, "wbk:-20");
    const at = waterMapStartIx(g, "wbk:-20", 0);
    const l = view(g, waterMapStart(at)).list!;
    expect([l.rows.length > 1, l.rows.filter((r) => r.here).map((r) => r.item), l.toggle])
      .toEqual([true, ["wbk:-20"], `Show all ${g.parts.length} parts`]);
    expect(l.rows.every((r) => r.aria === `${r.name}, part ${r.ix + 1} of ${g.parts.length}${r.note ? ` · ${r.note}` : ""}`)).toBe(true);
  });

  it("a water of one part has no list, and the toggle action is a no-op there", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const one = { ...g, parts: g.parts.slice(0, 1) } as WaterGroup;
    expect([view(one, waterMapStart(0)).list, run(one, waterMapStart(0), { type: "list" })]).toEqual([null, waterMapStart(0)]);
  });
});

// (T67, r-0ed3c2b2a0a7) the Zymoetz's canyon side channel is no part of its own any more: E15's spurious mark is gone
// and the channel joins the part above the canyon. A side-channel piece of the Skeena's Ecological Reserve is a part of
// its own, and the Reserve's part is a fork upstream.
// (T183, r-98c43091c440) Skeena section 2 starts at the Exchamsiks cut: the Reserve below the cut lost it (with the side
// channel piece 360216962:96, one part again, part 5, the lowest on the map) and the mainstem from the Reserve up to the
// Exchamsiks joined part 0 ("3 stretches"). The part of its own is now the Exchamsiks host channel ABOVE the cut
// (360216952:1017, part 6, still section 2): the Reserve's ▲ is part 0 (the main stem) or that channel.
describe("a fork upstream: the Skeena's Ecological Reserve", () => {
  it("▲ at the fork opens a chooser, the main stem first; a choice steps there", async () => {
    const g = await loadWaterGroup(st, "gnis:2936");
    // (T204) part 0 ("3 stretches") is three entries: the Reserve (part 5) is index 1, between its stretches 1 and 2
    const reserve = waterMapStart(1);
    expect([view(g, reserve).current.name, view(g, reserve).up])
      .toEqual(["Within Skeena River Ecological Reserve", { label: "Upstream", aria: "Upstream", disabled: false, fork: true, cross: null }]);
    const open = run(g, reserve, { type: "up" });
    expect(open).toEqual({ ...reserve, choosing: "up" });
    expect(view(g, open).chooser).toEqual({ title: "Which way upstream?", close: "Close", options: [
      { ix: 2, item: "gnis:2936", partIx: 0, name: "3 stretches · stretch 2 of 3", note: "Skeena River" },
      { ix: 12, item: "gnis:2936", partIx: 6, name: "Side channel", note: "Skeena River" }] });
    expect(run(g, open, { type: "choose", to: 2 })).toEqual(waterMapStart(2));
    expect(run(g, open, { type: "choose", to: 12 })).toEqual(waterMapStart(12));
    // not one of the ways up: nothing
    expect(run(g, open, { type: "choose", to: 0 })).toBe(open);
    expect(run(g, open, { type: "closeChooser" })).toEqual(reserve);
    // the host channel above the cut is the highest part, and its own ▼ is a fork (section 2 and the Reserve meet it at
    // the Exchamsiks cut)
    const side = view(g, waterMapStart(12));
    expect([side.current.name, side.up.disabled, side.down.fork]).toEqual(["Side channel", true, true]);
  });
});

// (T278, owner T166 2026-10-28: no parts on tidal water) the release keeps each tidal cut's piece as an unkeyed part
// (the Skeena's part 9, below `split:skeena_river__tidal_boundary`); the water map never lists it
describe("a tidal piece is never a part of the water map", () => {
  it("the Skeena: the store's part 9 is tidal, and no entry of its group is part 9", async () => {
    const ord = (await st.itemOrd("gnis:2936"))!;
    expect((await st.parts(ord)).filter((p) => p.tidal).map((p) => [p.partIx, p.akey])).toEqual([[9, null]]);
    const g = await loadWaterGroup(st, "gnis:2936");
    expect(g.parts.filter((p) => p.item === "gnis:2936" && p.partIx === 9)).toEqual([]);
    expect(g.marks.every((m) => [m.down, ...m.up].every((i) => g.parts[i]!.partIx !== 9))).toBe(true);
  });
});

describe("every part is reached by ▼ / ▲ (P15 review: the Zymoetz's lake part was not)", () => {
  /** Every state one press of ▼ or ▲ (and, at a fork, each choice) leads to from part `at`. */
  const presses = (g: WaterGroup, at: number): number[] => (["down", "up"] as const).flatMap((type) => {
    const s = waterMapStep(g, waterMapStart(at), { type });
    if (s.choosing) return view(g, s).chooser!.options.map((o) => waterMapStep(g, s, { type: "choose", to: o.ix }).at);
    return [s.at];
  }).filter((j) => j !== at);

  it("over every fixture water, each part is stepped INTO from another, and all are reached from the mouth", async () => {
    const missed: string[] = [];
    for (const id of ["gnis:8634", "gnis:3062", "gnis:12227", "wbk:-20", "gnis:3137"]) {
      const g = await loadWaterGroup(st, id);
      const into = new Set(g.parts.flatMap((_, i) => presses(g, i)));
      const seen = new Set([0]);
      const queue = [0];
      while (queue.length) for (const j of presses(g, queue.shift()!)) if (!seen.has(j)) { seen.add(j); queue.push(j); }
      g.parts.forEach((_, j) => {
        if (g.parts.length > 1 && !into.has(j)) missed.push(`${id} part ${j} (${view(g, waterMapStart(j)).current.name})`);
        if (!seen.has(j)) missed.push(`${id} part ${j}: not reached from the mouth`);
      });
    }
    expect(missed).toEqual([]);
  });

  it("(T204) the Zymoetz's source part is two stretches either side of the lake part: ▼ walks them in stem order", async () => {
    const g = await loadWaterGroup(st, "gnis:3137");
    // 3: the source part's lower stretch, 4: "McDonell Lake (3000 m downstream) → McDonell Lake outlet", 5: its upper
    expect([3, 4, 5].map((i) => [g.parts[i]!.partIx, g.parts[i]!.stretch ?? 0])).toEqual([[3, 0], [2, 0], [3, 1]]);
    const top = waterMapStart(5);
    expect(view(g, top).down).toEqual({ label: "Downstream", aria: "Downstream", disabled: false, fork: false, cross: null });
    expect(run(g, top, { type: "down" })).toEqual(waterMapStart(4));
    expect(run(g, waterMapStart(4), { type: "down" })).toEqual(waterMapStart(3));
  });
});

describe("across waters: the Vedder goes on from the Chilliwack (`water_continue`)", () => {
  it("the Vedder: ▲ is live and SAYS it goes into the Chilliwack (owner 2026-10-21 late); ▼ is its end", async () => {
    const g = await loadWaterGroup(st, "gnis:3062");
    const v = view(g, waterMapStart(0));
    expect([v.up, v.down, bar(g, waterMapStart(0))]).toEqual([
      { label: "Upstream into Chilliwack River", aria: "Upstream into Chilliwack River", disabled: false,
        fork: false, cross: { item: "gnis:8634", part: 1 } },
      { label: "Lowest part", aria: "Lowest part", disabled: true, fork: false, cross: null },
      "Part 1 of 1 | Vedder River | Lowest part (off) | Upstream into Chilliwack River"]);
    // ▲ is not a step inside the group: the state does not move (the caller opens the Chilliwack)
    expect(run(g, waterMapStart(0), { type: "up" })).toEqual(waterMapStart(0));
  });

  it("the Chilliwack: ▼ at its lowest part names the Vedder's part 0, and 'Flows into' stays; no other part crosses", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect([view(g, waterMapStart(0)).down, view(g, waterMapStart(0)).exit?.label]).toEqual([
      { label: "Downstream into Vedder River", aria: "Downstream into Vedder River", disabled: false, fork: false,
        cross: { item: "gnis:3062", part: 0 } }, "Flows into Vedder River ›"]);
    expect(g.parts.map((_, i) => [view(g, waterMapStart(i)).down.cross, view(g, waterMapStart(i)).up.cross]))
      .toEqual([[{ item: "gnis:3062", part: 0 }, null], [null, null], [null, null], [null, null]]);
  });

  it("a water with no continuation gets no cross-water step (the Cowichan, the KLWA lake, the Zymoetz)", async () => {
    for (const id of ["gnis:12227", "wbk:-20", "gnis:3137"]) {
      const g = await loadWaterGroup(st, id);
      expect([id, g.parts.flatMap((_, i) => [view(g, waterMapStart(i)).down.cross, view(g, waterMapStart(i)).up.cross])
        .filter((c) => c !== null)]).toEqual([id, []]);
    }
  });
});

describe("Cowichan, Vedder and the Kootenay Lake West Arm", () => {
  it("Cowichan: four parts in a line; at the mouth no exit (the sea)", async () => {
    const g = await loadWaterGroup(st, "gnis:12227");
    expect(g.parts.map((_, i) => bar(g, waterMapStart(i)))).toEqual([
      "Part 1 of 4 · Cowichan River | Mouth → CNR Mile 66 Trestle | Lowest part (off) | Upstream",
      "Part 2 of 4 · Cowichan River | CNR Mile 66 Trestle → CNR Bridge (mile 70.2) | Downstream | Upstream",
      "Part 3 of 4 · Cowichan River | CNR Bridge (mile 70.2) → Greendale Trestle | Downstream | Upstream",
      "Part 4 of 4 · Cowichan River | Greendale Trestle → Cowichan Lake outlet | Downstream | Highest part (off)"]);
  });

  it("Vedder: one part; its mouth is its end (its ▲ goes on into the Chilliwack, above)", async () => {
    const g = await loadWaterGroup(st, "gnis:3062");
    expect(bar(g, waterMapStart(0))).toBe("Part 1 of 1 | Vedder River | Lowest part (off) | Upstream into Chilliwack River");
  });

  it("KLWA: the lake's three closure areas in the prototype's order, stepped by their exits, out into the Kootenay River; no placed marks", async () => {
    const g = await loadWaterGroup(st, "wbk:-20");
    expect(waterMapStartIx(g, "wbk:-20", 0)).toBe(2);
    expect(g.parts.map((_, i) => bar(g, waterMapStart(i)))).toEqual([
      "Part 1 of 3 | Kootenay Lake — Lower West Arm | Lowest part (off) | Upstream | Flows into Kootenay River ›",
      "Part 2 of 3 | Kootenay Lake — Upper West Arm | Downstream | Upstream",
      "Part 3 of 3 | Kootenay Lake — Main Body | Downstream | Highest part (off)"]);
    expect([view(g, waterMapStart(2)).marks, view(g, waterMapStart(2)).whole.label]).toEqual([[], "⤢ Whole lake"]);
    // Show regulations for a closure area names THAT item
    expect(view(g, waterMapStart(0)).regs).toEqual({ label: "Show regulations for this part", item: "wbk:-22", partIx: 0 });
  });
});

describe("leaving the map, through the route (P13)", () => {
  const CHW = "gnis:8634" as ItemId;
  const onMap: Route = { mode: "water", from: HOME,
                         stack: [{ item: CHW, part: 1, tapSid: null, at: null, view: "map" }] };
  const top = (r: Route) => (r.mode === "water" ? r.stack[r.stack.length - 1] : null);

  it("Show regulations for this part: the Regulations view of this water on that part", () => {
    const r = showRegsActions(CHW, CHW, 3).reduce(reduce, onMap);
    expect(r).toEqual({ ...onMap, stack: [{ item: CHW, part: 3, tapSid: null, at: null, view: "regs" }] });
  });

  it("(owner 2026-10-22 late) a split lake's other closure area is the same water: the frame moves there, no layer; " +
     "back leaves the water", () => {
    const r = showRegsActions(CHW, "wbk:-21" as ItemId, 0).reduce(reduce, onMap);
    expect([r, reduce(r, { type: "back" })]).toEqual([
      { ...onMap, stack: [{ item: "wbk:-21", part: 0, tapSid: null, at: null, view: "regs" }] }, HOME]);
  });

  it("the exit opens the receiving water at the part the mouth meets; back returns to the map", () => {
    const r = reduce(onMap, exitAction("gnis:3062" as ItemId, 0));
    expect([top(r), reduce(r, { type: "back" })]).toEqual([
      { item: "gnis:3062", part: 0, tapSid: null, at: null, view: "regs" }, onMap]);
    expect(top(reduce(onMap, exitAction("gnis:3062" as ItemId, null)))?.part).toBeNull();
  });

  it("ONE WATER SCREEN: a step on the map is the frame's part, so Regulations opens on the stepped part", async () => {
    const stepped = reduce(onMap, partAction(CHW, CHW, 2));
    expect(stepped).toEqual({ ...onMap, stack: [{ item: CHW, part: 2, tapSid: null, at: null, view: "map" }] });
    expect(top(reduce(stepped, { type: "view", view: "regs" }))).toEqual(
      { item: CHW, part: 2, tapSid: null, at: null, view: "regs" });
    // (T369 D1/D2) a tap lies in ONE part: a step to another part (or a split lake's other area) leaves it behind, so
    // the stepped frame is exactly the part's — what a reload of its address draws; the same part again keeps it
    const TAPPED = await sec.stream(355999283, 0);   // a Chilliwack section (355999283:0)
    const tapped: Route = { ...onMap, stack: [{ item: CHW, part: 1, tapSid: TAPPED as never, at: { lat: 49, lon: -121 }, view: "map" }] };
    expect([top(reduce(tapped, partAction(CHW, CHW, 2))), top(reduce(tapped, partAction(CHW, "wbk:-21" as ItemId, 0))),
            top(reduce(tapped, partAction(CHW, CHW, 1)))])
      .toEqual([{ item: CHW, part: 2, tapSid: null, at: null, view: "map" },
                { item: "wbk:-21", part: 0, tapSid: null, at: null, view: "map" },
                { item: CHW, part: 1, tapSid: TAPPED, at: { lat: 49, lon: -121 }, view: "map" }]);
  });

  it("across waters (owner 2026-10-22 late: a river that changes its name is ONE system): ▲ from the Vedder is the " +
     "Chilliwack's lowest part with its Map in front, IN PLACE of the Vedder; back leaves the system", () => {
    const onVedder: Route = { mode: "water", from: HOME,
                              stack: [{ item: "gnis:3062" as ItemId, part: 0, tapSid: null, at: null, view: "map" }] };
    const r = reduce(onVedder, crossAction(CHW, 1));
    expect([crossAction(CHW, 1), r, reduce(r, { type: "back" }), reduce(onMap, crossAction("gnis:3062" as ItemId, 0))])
      .toEqual([
        { type: "cross", item: CHW, part: 1 },
        { mode: "water", from: HOME, stack: [{ item: CHW, part: 1, tapSid: null, at: null, view: "map" }] },
        HOME,
        { ...onMap, stack: [{ item: "gnis:3062", part: 0, tapSid: null, at: null, view: "map" }] }]);
  });

  it("(owner 2026-10-22 late) ▲ / ▼ back and forth across the boundary never stacks: one back from either side leaves", () => {
    const onVedder: Route = { mode: "water", from: HOME,
                              stack: [{ item: "gnis:3062" as ItemId, part: 0, tapSid: null, at: null, view: "map" }] };
    const r = [crossAction(CHW, 1), crossAction("gnis:3062" as ItemId, 0), crossAction(CHW, 1)].reduce(reduce, onVedder);
    expect([r, reduce(r, { type: "back" })]).toEqual([
      { mode: "water", from: HOME, stack: [{ item: CHW, part: 1, tapSid: null, at: null, view: "map" }] }, HOME]);
  });

  it("'Flows into' from a true tributary stays a layer: back returns to the tributary", () => {
    const onTrib: Route = { mode: "water", from: HOME,
                            stack: [{ item: "gnis:11481" as ItemId, part: 0, tapSid: null, at: null, view: "map" }] };
    const r = reduce(onTrib, exitAction(CHW, 2));
    expect([r, reduce(r, { type: "back" })]).toEqual([
      { ...onTrib, stack: [...onTrib.stack, { item: CHW, part: 2, tapSid: null, at: null, view: "regs" }] }, onTrib]);
  });
});

// (T345) the Coquitlam on the fixture (its rows exactly as release r-4df5bccc9360 wrote them): the two parts either side
// of the Lougheed Highway Bridge cut (blk 356364584 at 7028.62) light DISJOINT sections that meet at the bridge mark —
// Mary Hill → Lougheed is sections 316504 (312 → 6590, the gauge 08MH002 cut) and 316505 (6590 → 7028); Lougheed → the
// watershed boundary is 316506 (7028 → 16309) alone. And its mouth meets the Fraser's TIDAL part (16), so "Flows into"
// is said, never a way into a part no Fraser map lists (which opened the Fraser on "2 stretches · stretch 1 of 2").
// (T441) the Coquitlam's sections as [stream key, start metre] (the fixture's `lines`), never as ids: Mary Hill → Lougheed
// is two sections (312 → 6590, the gauge 08MH002 cut, and 6590 → 7028), Lougheed → the watershed boundary one (7028 →
// 16309), and the rest the lake-outlet part's
const COQ_MARY_HILL = [[356364584, 312], [356364584, 6590]] as const;
const COQ_LOUGHEED = [[356364584, 7028]] as const;
const COQ_UPPER = [[355994322, 0], [355999215, 0], [356000341, 0], [356001729, 0], [356001891, 0], [356364584, 16309],
                   [356364584, 32218], [356364584, 36910], [356364584, 46864]] as const;
describe("(T345) the Coquitlam: parts meet at the Lougheed bridge; the tidal Fraser is said, not opened", () => {
  it("each part lights its own sections only, and the Lougheed mark sits between the two parts that meet there", async () => {
    const g = await loadWaterGroup(st, "gnis:19986");
    const parts = g.parts.map((_, i) => { const v = view(g, waterMapStart(i)); return [v.current.name, v.selected]; });
    expect(parts).toEqual([
      ["Mary Hill Bypass Bridge → Lougheed Highway Bridge", await sec.streams(COQ_MARY_HILL)],
      ["Lougheed Highway Bridge → Coquitlam Watershed boundary", await sec.streams(COQ_LOUGHEED)],
      ["Coquitlam Watershed boundary → A lake outlet", await sec.streams(COQ_UPPER)],
    ]);
    // never one section in two parts, and every part's `water` is exactly the others' sections
    const all = g.parts.flatMap((p) => p.sids);
    expect(new Set(all).size).toBe(all.length);
    g.parts.forEach((_, i) => expect([...view(g, waterMapStart(i)).water].sort((a, b) => a - b))
      .toEqual(g.parts.flatMap((p, j) => (j === i ? [] : p.sids)).sort((a, b) => a - b)));
    const lougheed = g.marks.find((m) => m.label === "Lougheed Highway Bridge")!;
    expect([lougheed.down, lougheed.up]).toEqual([0, [1]]);
    // the list's pick lights the same part a step does
    expect(view(g, run(g, { ...waterMapStart(1), list: true }, { type: "part", ix: 0 })).selected).toEqual(await sec.streams(COQ_MARY_HILL));
  });

  it("the mouth part says 'Flows into the tidal Fraser River' and offers nothing to open", async () => {
    const g = await loadWaterGroup(st, "gnis:19986");
    expect(view(g, waterMapStart(0)).exit).toEqual(
      { label: "Flows into the tidal Fraser River", item: "gnis:39325", part: 16, tidal: true });
    // the receiving part IS the Fraser's tidal piece, which its map does not list
    const fraser = await loadWaterGroup(st, "gnis:39325");
    expect(fraser.parts.some((p) => p.partIx === 16)).toBe(false);
  });
});

describe("(T443, owner on 0.6.7) two marks with the same words a few steps apart draw ONE label", () => {
  it("the Fraser's Landstrom Bar sign zone: both ends of the zone named once, at every part they bound", async () => {
    const g = await loadWaterGroup(st, "gnis:39325");
    const zone = g.marks.flatMap((m, ix) => (m.label === "Landstrom Bar sign zone boundary" ? [ix] : []));
    expect(zone).toHaveLength(2);
    // every part that has one of the two marks at an end: the part around the zone and the zone itself
    const parts = [...new Set(zone.flatMap((ix) => [g.marks[ix]!.down, ...g.marks[ix]!.up]))];
    expect(parts.length).toBeGreaterThanOrEqual(2);
    for (const at of parts) {
      const v = view(g, waterMapStart(at));
      const shown = v.marks.filter((m) => zone.includes(m.ix));
      expect(shown).toHaveLength(2);                                    // both rings drawn
      expect(shown.filter((m) => m.label !== null)).toHaveLength(1);    // one label
      expect(shown.every((m) => m.a11y.length > 0)).toBe(true);         // each ring keeps its words for a screen reader
    }
    // the open mark keeps its own label (the other end goes quiet)
    const open = view(g, { ...waterMapStart(parts[0]!), mark: zone[1]! });
    expect(open.marks.find((m) => m.ix === zone[1])!.label).not.toBeNull();
    expect(open.marks.find((m) => m.ix === zone[0])!.label).toBeNull();
  });

  it("MUTATION PIN: labels a kilometre apart, or with other words, are both drawn", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const v = view(g, waterMapStart(1));
    expect(v.marks.filter((m) => m.label !== null).length).toBe(2);   // part 2's two ends, different words
  });
});

