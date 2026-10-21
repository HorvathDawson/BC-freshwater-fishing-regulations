/**
 * P15: the water map's state and view model over THE RELEASE'S OWN TABLES — the committed regs fixture release (T10),
 * whose water file holds the Chilliwack, Vedder, Cowichan, Kootenay Lake's closure areas and the Zymoetz exactly as
 * release r-1bb06fa28daf wrote them (`part_geo`, `part_mark`, `part_section`, `water_exit`).
 */
import { beforeAll, describe, expect, it } from "vitest";
import type { WaterGroup } from "@app/core";
import { loadWaterGroup, openRegsStore, type RegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import type { ItemId } from "@app/data";
import { HOME, reduce, type Route } from "./route";
import {
  exitAction, showRegsActions, waterMapStart, waterMapStartIx, waterMapStep, waterMapView, type WaterMapAction,
  type WaterMapState,
} from "./waterMap";

let st: RegsStore;
beforeAll(async () => { st = await openRegsStore(openRegsFixture()); });

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
      + " | Lowest part (off) | ▲ Upstream | Flows into Vedder River ›");
    expect(view(g, s).exit).toEqual({ label: "Flows into Vedder River ›", item: "gnis:3062", part: 0 });
    const seen = [bar(g, s)];
    for (let k = 0; k < 4; k += 1) { s = run(g, s, { type: "up" }); seen.push(bar(g, s)); }
    expect(seen.slice(1)).toEqual([
      "Part 2 of 4 · Chilliwack River | Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence | ▼ Downstream | ▲ Upstream",
      "Part 3 of 4 · Chilliwack River | 100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary | ▼ Downstream | ▲ Upstream",
      "Part 4 of 4 · Chilliwack River | Chilliwack River Ecological Reserve boundary → Source | ▼ Downstream | Highest part (off)",
      // ▲ at the top changes nothing
      "Part 4 of 4 · Chilliwack River | Chilliwack River Ecological Reserve boundary → Source | ▼ Downstream | Highest part (off)",
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
      title: "fishing boundary signs 100 m below the Slesse Creek confluence", close: "Close", closeGlyph: "✕",
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
    expect(run(g, s, { type: "section", sid: 1235785 })).toEqual(waterMapStart(3));
    expect(run(g, s, { type: "section", sid: 62410 })).toBe(s);          // the Vedder River's
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

describe("a side channel: the Zymoetz's canyon", () => {
  it("▲ below the fork opens a chooser, the main stem first; a choice steps there", async () => {
    const g = await loadWaterGroup(st, "gnis:3137");
    const canyon = waterMapStart(1);
    expect(view(g, canyon).up).toEqual({ label: "▲ Upstream", disabled: false, fork: true });
    const open = run(g, canyon, { type: "up" });
    expect(open).toEqual({ ...canyon, choosing: true });
    expect(view(g, open).chooser).toEqual({ title: "Which way upstream?", close: "Close", closeGlyph: "✕", options: [
      { ix: 3, item: "gnis:3137", partIx: 0, name: "Zymoetz Canyon upper entrance → Limonite Creek confluence", note: "Zymoetz River" },
      { ix: 2, item: "gnis:3137", partIx: 5, name: "Side channel", note: "Zymoetz River" }] });
    expect(run(g, open, { type: "choose", to: 2 })).toEqual(waterMapStart(2));
    expect(run(g, open, { type: "choose", to: 3 })).toEqual(waterMapStart(3));
    // not one of the ways up: nothing
    expect(run(g, open, { type: "choose", to: 0 })).toBe(open);
    expect(run(g, open, { type: "closeChooser" })).toEqual(canyon);
    // the side channel's own ▲ is the main stem, no chooser
    expect(run(g, waterMapStart(2), { type: "up" })).toEqual(waterMapStart(3));
  });
});

describe("Cowichan, Vedder and the Kootenay Lake West Arm", () => {
  it("Cowichan: four parts in a line; at the mouth no exit (the sea)", async () => {
    const g = await loadWaterGroup(st, "gnis:12227");
    expect(g.parts.map((_, i) => bar(g, waterMapStart(i)))).toEqual([
      "Part 1 of 4 · Cowichan River | Mouth → CNR Mile 66 Trestle | Lowest part (off) | ▲ Upstream",
      "Part 2 of 4 · Cowichan River | CNR Mile 66 Trestle → CNR Bridge (mile 70.2) | ▼ Downstream | ▲ Upstream",
      "Part 3 of 4 · Cowichan River | CNR Bridge (mile 70.2) → Greendale Trestle | ▼ Downstream | ▲ Upstream",
      "Part 4 of 4 · Cowichan River | Greendale Trestle → Cowichan Lake outlet | ▼ Downstream | Highest part (off)"]);
  });

  it("Vedder: one part, both ends disabled", async () => {
    const g = await loadWaterGroup(st, "gnis:3062");
    expect(bar(g, waterMapStart(0))).toBe("Part 1 of 1 | Vedder River | Lowest part (off) | Highest part (off)");
  });

  it("KLWA: the lake's three closure areas in the prototype's order, stepped by their exits, out into the Kootenay River; no placed marks", async () => {
    const g = await loadWaterGroup(st, "wbk:-20");
    expect(waterMapStartIx(g, "wbk:-20", 0)).toBe(2);
    expect(g.parts.map((_, i) => bar(g, waterMapStart(i)))).toEqual([
      "Part 1 of 3 | Kootenay Lake — Lower West Arm | Lowest part (off) | ▲ Upstream | Flows into Kootenay River ›",
      "Part 2 of 3 | Kootenay Lake — Upper West Arm | ▼ Downstream | ▲ Upstream",
      "Part 3 of 3 | Kootenay Lake — Main Body | ▼ Downstream | Highest part (off)"]);
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

  it("a split lake's other closure area opens that water on top; back returns to the map", () => {
    const r = showRegsActions(CHW, "wbk:-21" as ItemId, 0).reduce(reduce, onMap);
    expect([top(r), reduce(r, { type: "back" })]).toEqual([
      { item: "wbk:-21", part: 0, tapSid: null, at: null, view: "regs" }, onMap]);
  });

  it("the exit opens the receiving water at the part the mouth meets; back returns to the map", () => {
    const r = reduce(onMap, exitAction("gnis:3062" as ItemId, 0));
    expect([top(r), reduce(r, { type: "back" })]).toEqual([
      { item: "gnis:3062", part: 0, tapSid: null, at: null, view: "regs" }, onMap]);
    expect(top(reduce(onMap, exitAction("gnis:3062" as ItemId, null)))?.part).toBeNull();
  });
});
