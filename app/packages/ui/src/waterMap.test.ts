/**
 * P15: the water map's state and view model over THE RELEASE'S OWN TABLES — the committed regs fixture release (T10),
 * whose water file holds the Chilliwack, Vedder, Cowichan, Kootenay Lake's closure areas and the Zymoetz exactly as
 * release r-1bb06fa28daf wrote them (`part_geo`, `part_mark`, `part_section`, `water_exit`).
 */
import { beforeAll, describe, expect, it } from "vitest";
import { markLabel, partName, partRowAria, partWhere, type WaterGroup } from "@app/core";
import { loadWaterGroup, openRegsStore, type RegsStore } from "@app/data/regs";
import { openRegsFixtureWithLines, sectionNames, type SectionNames } from "@app/data/release/node";
import type { ItemId } from "@app/data";
import { HOME, reduce, type Route } from "./route";
import {
  crossAction, exitAction, markTap, partAction, showRegsActions, waterMapStart, waterMapStartIx, waterMapStep, waterMapView,
  type WaterMapAction,
  type WaterMapState,
} from "./waterMap";

let st: RegsStore;
let sec: SectionNames;
// (T667) parts are named by their ENDS, never typed as numbers (`part_ix` is river order: one more cut renumbers the ones
// above it): the Chilliwack's five parts in river order, and the three Skeena parts the tests name
const CH_ENDS = [["continues:gnis:3062", "chilliwack_vedder_rivers__vedder_crossing_bridge_200_m_upstream"],
  ["chilliwack_vedder_rivers__vedder_crossing_bridge_200_m_upstream", "chilliwack_vedder_rivers__tamihi_rapids_bridge"],
  ["chilliwack_vedder_rivers__tamihi_rapids_bridge", "chilliwack_vedder_rivers__boundary_signs_below_slesse_creek"],
  ["chilliwack_vedder_rivers__boundary_signs_below_slesse_creek", "area:CHILLIWACK RIVER ECOLOGICAL RESERVE"],
  ["area:CHILLIWACK RIVER ECOLOGICAL RESERVE", "source"]] as const;
const SKEENA_HOST_CHANNEL = 360216952;   // the Exchamsiks host channel's FWA stream key (a key is a name; its part number is not)
let CH: number[];
let SK_RESERVE: number, SK_RESERVE_UP: number, SK_EXCHAMSIKS_UP: number, SK_SIDE: number;
beforeAll(async () => {
  const loader = openRegsFixtureWithLines(); st = await openRegsStore(loader); sec = sectionNames(loader);
  CH = await Promise.all(CH_ENDS.map(([lo, hi]) => sec.part("gnis:8634", lo, hi)));
  // (T678 junctions, r-adc4e248a163) the main stem just above the Reserve is a part of its own: the false junction that
  // joined it to the stretch below the Reserve is gone (same rule key as that stretch)
  SK_RESERVE = await sec.part("gnis:2936", "area:SKEENA RIVER ECOLOGICAL RESERVE@km63.5",
    "area:SKEENA RIVER ECOLOGICAL RESERVE@near:skeena_river__exchamsiks_river_into_skeena_river");
  SK_RESERVE_UP = await sec.part("gnis:2936", "area:SKEENA RIVER ECOLOGICAL RESERVE@near:skeena_river__exchamsiks_river_into_skeena_river",
    "skeena_river__exchamsiks_river_into_skeena_river");
  // (r-46e4355a7277) the run above the Exchamsiks ends at the Lakelse confluence now (a new part end; it ran to the Zymagotitz)
  SK_EXCHAMSIKS_UP = await sec.part("gnis:2936", "skeena_river__exchamsiks_river_into_skeena_river",
    "skeena_river__lakelse_river_into_skeena_river");
  SK_SIDE = await sec.partAt("gnis:2936", SKEENA_HOST_CHANNEL, 1017);   // the Exchamsiks host channel above the cut: a part with no ends
});

const run = (g: WaterGroup, s: WaterMapState, ...as: WaterMapAction[]) => as.reduce((x, a) => waterMapStep(g, x, a), s);
const view = (g: WaterGroup, s: WaterMapState) => waterMapView(g, s)!;
/** (T780) an end's words without the review note a fixture cut before T780 still carries (the release since says none). */
const plainEnd = (s: string) => s.replace(/, approximate location \([a-z]+ confidence\)$/, "");
/** What the bar says, whole. */
const bar = (g: WaterGroup, s: WaterMapState) => {
  const v = view(g, s);
  return `${v.where} | ${v.current.name} | ${v.down.label}${v.down.disabled ? " (off)" : ""} | ${v.up.label}`
    + `${v.up.disabled ? " (off)" : ""}${v.exit ? ` | ${v.exit.label}` : ""}`;
};
/** (T768) A bar's lead as the group says it ("Part k of N · Water | name"): counts and names come from the water's group
 *  (the one content pin is conformance waterGroup.fixture.test.ts), never typed here. */
const lead = (g: WaterGroup, i: number) => `${partWhere(g, i)} | ${partName(g, i)}`;
const sids = async (ord: number, partIx: number) =>
  (await (await st.water()).all("SELECT sid FROM part_section WHERE ord = ? AND part_ix = ? ORDER BY sid", ord, partIx))
    .map((r) => Number(r.sid));

// (T517) the Chilliwack's lowest rule set is cut in two by the DFO salmon chain 200 m above the Vedder Crossing bridge:
// five parts in river order (part_ix = the order), the lowest two one run of one rule set
describe("the Chilliwack: ▼ / ▲ through its five parts, the ends disabled, the mouth's exit", () => {
  it("from the mouth part to the top and back", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    let s = waterMapStart(waterMapStartIx(g, "gnis:8634", 0));     // part_ix 0 is the mouth part (river order)
    expect(s.at).toBe(0);
    expect(bar(g, s)).toBe(lead(g, 0)
      // ▼ at the mouth part goes on into the Vedder (`water_continue`); "Flows into" stays
      + " | Downstream into Vedder River | Upstream | Flows into Vedder River ›");
    // (T517) the mouth meets the Vedder's upper part (the Vedder is two parts of one run too)
    expect(view(g, s).exit).toEqual({ label: "Flows into Vedder River ›", item: "gnis:3062", part: 1 });
    const seen = [bar(g, s)];
    const top = g.parts.length - 1;
    for (let k = 0; k < top + 1; k += 1) { s = run(g, s, { type: "up" }); seen.push(bar(g, s)); }
    // one ▲ per part above the mouth part, then the top again: ▲ at the top changes nothing
    expect(seen.slice(1)).toEqual([
      ...Array.from({ length: top }, (_, i) => i + 1).map((i) =>
        `${lead(g, i)} | Downstream | ${i === top ? "Highest part (off)" : "Upstream"}`),
      `${lead(g, top)} | Downstream | Highest part (off)`,
    ]);
    s = run(g, s, ...Array.from({ length: top }, () => ({ type: "down" }) as const));
    expect(s).toEqual(waterMapStart(0));
  });

  it("selected is exactly the part's part_section rows; the rest of the water is `water`", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const ord = (await st.itemOrd("gnis:8634"))!;
    for (const [at, partIx] of g.parts.map((p, i) => [i, p.partIx] as const)) {
      const v = view(g, waterMapStart(at));
      expect([at, v.selected]).toEqual([at, await sids(ord, partIx)]);
      expect([...v.selected, ...v.water].sort((a, b) => a - b)).toEqual(
        (await (await st.water()).all("SELECT sid FROM part_section WHERE ord = ? ORDER BY sid", ord)).map((r) => Number(r.sid)));
    }
  });

  it("the marks: a ring at each placed boundary, the current part's two ends labelled", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const marks = (s: WaterMapState) => view(g, s).marks.map((m) => `${m.ix} ${m.label ?? "(ring)"}${m.on ? " (open)" : ""}`);
    // a ring at every placed boundary; a part's two ends carry the words the group says (markLabel), never typed here
    const said = (at: number) => g.marks.map((m, ix) =>
      `${ix} ${m.down === at || m.up.includes(at) ? markLabel(g, m) ?? "(ring)" : "(ring)"}`);
    expect(marks(waterMapStart(2))).toEqual(said(2));
    expect(marks(waterMapStart(1))).toEqual(said(1));
    expect(said(1).filter((x) => !x.endsWith("(ring)")).length).toBeGreaterThan(0);   // the check is not vacuous
    const m1 = g.marks[1]!;
    expect(view(g, waterMapStart(1)).marks[1]).toEqual({ ix: 1, lon: m1.lon, lat: m1.lat,
      label: markLabel(g, m1), a11y: `Boundary: ${markLabel(g, m1)}`, on: false });
  });

  it("tapping a mark opens its words with the parts above and below it; a second tap closes it", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const m = 2, mk = g.marks[m]!;
    const s = run(g, waterMapStart(0), { type: "mark", ix: m });
    // (T517) a mark says the same words as the ends of the parts it joins
    expect(mk.up).toEqual([m + 1]);
    expect(view(g, s).pop).toEqual({
      title: markLabel(g, mk), close: "Close",
      upLabel: "Upstream", up: [{ ix: m + 1, item: "gnis:8634", partIx: g.parts[m + 1]!.partIx, note: "Chilliwack River",
        name: partName(g, m + 1) }],
      downLabel: "Downstream", down: { ix: m, item: "gnis:8634", partIx: g.parts[m]!.partIx, note: "Chilliwack River",
        name: partName(g, m) } });
    expect(view(g, s).marks.map((m) => [m.ix, m.on, m.label !== null]))
      .toEqual([[0, false, true], [1, false, false], [2, true, true], [3, false, false]]);
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
    const top = g.parts.length - 1;
    expect(run(g, s, { type: "section", sid: g.parts[top]!.sids[0]! })).toEqual(waterMapStart(top));
    const vedder = await loadWaterGroup(st, "gnis:3062");
    expect(run(g, s, { type: "section", sid: vedder.parts[0]!.sids[0]! })).toBe(s);
    expect(run(g, s, { type: "part", ix: 2 })).toEqual(waterMapStart(2));
    expect(run(g, s, { type: "part", ix: g.parts.length })).toBe(s);
  });

  it("⤢ frames the whole river, and back to the part", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const part = view(g, waterMapStart(2));
    expect([part.whole, part.bounds, part.fitKey]).toEqual([
      { label: "⤢ Whole river", aria: "Whole river", on: false }, part.bounds, "gnis:8634:2"]);
    // the part's frame holds the part's own box (the frame may be padded; never smaller)
    const [w, so, e, n] = g.parts[2]!.box!, [fw, fs, fe, fn] = part.bounds!;
    expect([fw <= w, fs <= so, fe >= e, fn >= n]).toEqual([true, true, true, true]);
    const whole = view(g, run(g, waterMapStart(2), { type: "whole" }));
    expect([whole.whole, whole.bounds, whole.fitKey]).toEqual([
      { label: "This part", aria: "This part", on: true }, whole.bounds, "gnis:8634:whole"]);
    // the whole river's frame holds every part's box
    const [ww, ws, we, wn] = whole.bounds!;
    expect(g.parts.every((p) => p.box === null || (ww <= p.box[0] && ws <= p.box[1] && we >= p.box[2] && wn >= p.box[3]))).toBe(true);
    expect(run(g, run(g, waterMapStart(2), { type: "whole" }), { type: "whole" })).toEqual(waterMapStart(2));
  });

  it("Show regulations for this part names the part's item and part_ix", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect(view(g, waterMapStart(3)).regs).toEqual({ label: "Show regulations for this part", item: "gnis:8634", partIx: CH[3] });
    // where the map's index is not the part's (the Skeena: its tidal part 0 is no entry), the part's own part_ix
    const sk = await loadWaterGroup(st, "gnis:2936");
    expect(view(sk, waterMapStart(SK_EXCHAMSIKS_UP - 1)).regs).toEqual({ label: "Show regulations for this part", item: "gnis:2936", partIx: SK_EXCHAMSIKS_UP });
  });
});

describe("(T150) the parts of this water, listed under the map — the header's Parts sheet moved here", () => {
  it("the Chilliwack: closed, the toggle says how many; open, every part by name and place, the part shown marked", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect(view(g, waterMapStart(1)).list).toEqual({ title: "Parts of this water", toggle: `Show all ${g.parts.length} parts`, open: false,
      rows: g.parts.map((p, ix) => ({ ix, item: "gnis:8634", partIx: p.partIx, note: "", here: ix === 1,
        name: partName(g, ix), aria: partRowAria(g, ix) })) });
    const open = run(g, waterMapStart(1), { type: "list" });
    expect([open, view(g, open).list!.toggle, view(g, open).list!.open]).toEqual([
      { ...waterMapStart(1), list: true }, "Hide the list of parts", true]);
    // a pick is the part step: the map moves there and the list closes; toggling again closes it
    expect(run(g, open, { type: "part", ix: g.parts.length - 1 })).toEqual(waterMapStart(g.parts.length - 1));
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
    // (T875) the list's place is held by a quiet line, so the bar is as tall as a water of many parts; never on one of many
    expect([view(one, waterMapStart(0)).onePart, view(g, waterMapStart(0)).onePart]).toEqual(["This river is one part", null]);
  });
});

// (T750, owner 2026-11-14 on staging, the Skeena's part 2 of 18: "navigating up the skeena it gives a branching option. we
// should only have up or downastream that access all components. we just need to choose the most sensible next item. for
// example here which ever has a lower downstream item maybe.") The Reserve's part forks upstream in the marks (the main
// stem above it, and the km-less side channel that leaves it further up and rejoins at the Exchamsiks): ▲ takes the
// next part in the water's one line (the release's river order, the parts list's order), the main stem, whose
// downstream end is the lower; the next ▲ the side channel; the next the Exchamsiks stretch. No chooser, ever.
describe("(T750) no fork: the Skeena's Ecological Reserve steps up one line", () => {
  it("▲ from the Reserve: the main stem above it, then the side channel, then above the Exchamsiks; ▼ the same way back", async () => {
    const g = await loadWaterGroup(st, "gnis:2936");
    const ix = (partIx: number) => waterMapStartIx(g, "gnis:2936", partIx);
    const reserve = waterMapStart(ix(SK_RESERVE));
    expect([view(g, reserve).current.name, view(g, reserve).up]).toEqual([
      "Skeena River Ecological Reserve boundary (km 63.5) → Skeena River Ecological Reserve boundary near the Exchamsiks River confluence",
      { label: "Upstream", aria: "Upstream", disabled: false, cross: null }]);
    const ups = [run(g, reserve, { type: "up" }), run(g, reserve, { type: "up" }, { type: "up" }),
                 run(g, reserve, { type: "up" }, { type: "up" }, { type: "up" })];
    expect(ups).toEqual([waterMapStart(ix(SK_RESERVE_UP)), waterMapStart(ix(SK_SIDE)), waterMapStart(ix(SK_EXCHAMSIKS_UP))]);
    expect(ups.map((s) => view(g, s).current.name)).toEqual([
      "Skeena River Ecological Reserve boundary near the Exchamsiks River confluence → Exchamsiks River confluence",
      // (T768) the third is the run above the Exchamsiks, whatever part end it runs up to in this release
      "Skeena River", partName(g, ix(SK_EXCHAMSIKS_UP))]);
    expect(partName(g, ix(SK_EXCHAMSIKS_UP))).toMatch(/^Exchamsiks River confluence → /);
    // and back down the same line
    expect(run(g, ups[2]!, { type: "down" }, { type: "down" }, { type: "down" })).toEqual(reserve);
    // the line IS the parts list's order (one order, every screen)
    expect(view(g, reserve).list!.rows.map((r) => r.ix)).toEqual(g.parts.map((_, i) => i));
  });
});

// (T750) ONE LINE THROUGH EVERY PART, on waters cut from the real release (the committed regs fixture): the Skeena (its
// side channel), the Fraser (the Landstrom Bar sign zone inside a stretch), the Columbia and the Thompson (lakes in the
// line), the West Road (Blackwater) (a braid beside a stretch: a fork in the marks both ways), the Kootenay (border
// crossings). ▲ pressed from the lowest part n-1 times visits every part exactly once, in the release's river order
// (`part_ix` ascending, the parts list's order), stopping at the top; ▼ from there walks the same line back.
describe("(T750) ▲ from the lowest part walks every part once; ▼ walks it back", () => {
  const WATERS = { Skeena: "gnis:2936", Fraser: "gnis:39325", Columbia: "gnis:37414", Thompson: "gnis:39492",
                   "West Road (Blackwater)": "gnis:26104", Kootenay: "gnis:14097" } as const;
  for (const [name, id] of Object.entries(WATERS)) {
    it(`${name}`, async () => {
      const g = await loadWaterGroup(st, id);
      const n = g.parts.length;
      expect(n).toBeGreaterThan(2);
      const up: number[] = [];
      let s = waterMapStart(0);
      for (let k = 0; k < n; k += 1) { up.push(s.at); s = run(g, s, { type: "up" }); }
      // the top: ▲ stops there ("Highest part"), every part seen once
      expect([s.at, view(g, s).up.disabled, view(g, s).up.label]).toEqual([n - 1, true, "Highest part"]);
      expect(up).toEqual(g.parts.map((_, i) => i));
      expect(new Set(up).size).toBe(n);
      // the order is the release's river order, and the parts list's
      const ord = (await st.itemOrd(id))!;
      const listed = (await st.parts(ord)).filter((p) => !p.tidal).map((p) => p.partIx).sort((a, b) => a - b);
      expect(up.map((i) => g.parts[i]!.partIx)).toEqual(listed);
      expect(view(g, s).list!.rows.map((r) => r.ix)).toEqual(up);
      const down: number[] = [];
      for (let k = 0; k < n; k += 1) { down.push(s.at); s = run(g, s, { type: "down" }); }
      expect(down).toEqual([...up].reverse());
      expect([s.at, view(g, s).down.disabled]).toEqual([0, true]);
    });
  }
});

// (T278, owner T166 2026-10-28: no parts on tidal water) the release keeps each tidal cut's piece as an unkeyed part
// (the Skeena's part 0 in river order since T517, below `split:skeena_river__tidal_boundary`); the water map never lists it
describe("a tidal piece is never a part of the water map", () => {
  it("the Skeena: the store's part 0 is tidal, and no entry of its group is part 0", async () => {
    const ord = (await st.itemOrd("gnis:2936"))!;
    expect((await st.parts(ord)).filter((p) => p.tidal).map((p) => [p.partIx, p.akey])).toEqual([[0, null]]);
    const g = await loadWaterGroup(st, "gnis:2936");
    expect(g.parts.filter((p) => p.item === "gnis:2936" && p.partIx === 0)).toEqual([]);
    expect(g.marks.every((m) => [m.down, ...m.up].every((i) => g.parts[i]!.partIx !== 0))).toBe(true);
  });
});

describe("every part is reached by ▼ / ▲ (P15 review: the Zymoetz's lake part was not)", () => {
  /** Every state one press of ▼ or ▲ leads to from part `at`. */
  const presses = (g: WaterGroup, at: number): number[] => (["down", "up"] as const)
    .map((type) => waterMapStep(g, waterMapStart(at), { type }).at).filter((j) => j !== at);

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

  it("(T517) the Zymoetz's runs either side of the lake part are parts of their own, in river order: ▼ walks them in stem order", async () => {
    const g = await loadWaterGroup(st, "gnis:3137");
    // 3: "Limonite Creek confluence → McDonell Lake (3000 m downstream)", 4: "McDonell Lake (3000 m downstream) → McDonell
    // Lake outlet", 5: "McDonell Lake → Source" — each one part, part_ix the river order
    expect([3, 4, 5].map((i) => [g.parts[i]!.partIx, g.parts[i]!.pieces])).toEqual([[3, [3]], [4, [4]], [5, [5]]]);
    const top = waterMapStart(5);
    expect(view(g, top).down).toEqual({ label: "Downstream", aria: "Downstream", disabled: false, cross: null });
    expect(run(g, top, { type: "down" })).toEqual(waterMapStart(4));
    expect(run(g, waterMapStart(4), { type: "down" })).toEqual(waterMapStart(3));
  });
});

describe("across waters: the Vedder goes on from the Chilliwack (`water_continue`)", () => {
  it("the Vedder: ▲ from its top part is live and SAYS it goes into the Chilliwack (owner 2026-10-21 late); ▼ from its mouth part is its end", async () => {
    const g = await loadWaterGroup(st, "gnis:3062");
    // (T517) two parts of one run (the DFO salmon chain cuts it at the cable car crossing): ▲ from the mouth part is a step
    const v0 = view(g, waterMapStart(0)), v1 = view(g, waterMapStart(1));
    expect([v0.up, v0.down, v1.up, v1.down]).toEqual([
      { label: "Upstream", aria: "Upstream", disabled: false, cross: null },
      { label: "Lowest part", aria: "Lowest part", disabled: true, cross: null },
      { label: "Upstream into Chilliwack River", aria: "Upstream into Chilliwack River", disabled: false,
        cross: { item: "gnis:8634", part: 0 } },
      { label: "Downstream", aria: "Downstream", disabled: false, cross: null }]);
    expect(run(g, waterMapStart(0), { type: "up" })).toEqual(waterMapStart(1));
    // ▲ off the top is not a step inside the group: the state does not move (the caller opens the Chilliwack)
    expect(run(g, waterMapStart(1), { type: "up" })).toEqual(waterMapStart(1));
  });

  it("the Chilliwack: ▼ at its lowest part names the Vedder's top part (1), and 'Flows into' stays; no other part crosses", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect([view(g, waterMapStart(0)).down, view(g, waterMapStart(0)).exit?.label]).toEqual([
      { label: "Downstream into Vedder River", aria: "Downstream into Vedder River", disabled: false,
        cross: { item: "gnis:3062", part: 1 } }, "Flows into Vedder River ›"]);
    expect(g.parts.map((_, i) => [view(g, waterMapStart(i)).down.cross, view(g, waterMapStart(i)).up.cross]))
      .toEqual([[{ item: "gnis:3062", part: 1 }, null], [null, null], [null, null], [null, null], [null, null]]);
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
  // (T768) HOW MANY parts and what each is called is pinned in ONE place (`conformance/src/regs/waterGroup.fixture.test.ts`,
  // where a new part end is seen and attributed once: r-46e4355a7277 added Skutz Falls). Here: what the bar SAYS for
  // every part, composed from the group, and that the parts are one line from the mouth to the lake outlet
  it("Cowichan: its parts in one line from the mouth to the lake outlet (T517: cut by the salmon chain); at the mouth no exit (the sea)", async () => {
    const g = await loadWaterGroup(st, "gnis:12227"), n = g.parts.length;
    expect(n).toBeGreaterThanOrEqual(5);
    expect(g.parts.map((_, i) => bar(g, waterMapStart(i)))).toEqual(g.parts.map((_, i) =>
      `Part ${i + 1} of ${n} · Cowichan River | ${partName(g, i)} | ${i === 0 ? "Lowest part (off)" : "Downstream"} | `
      + (i === n - 1 ? "Highest part (off)" : "Upstream")));
    expect([g.parts[0]!.lo, g.parts[n - 1]!.hi, g.parts.slice(1).map((p, i) => p.lo === g.parts[i]!.hi)])
      .toEqual(["mouth", "lake_outlet:wbk:329101302", g.parts.slice(1).map(() => true)]);
    expect(new Set(g.parts.map((_, i) => partName(g, i))).size).toBe(n);   // every part by its own name
    // (T780, owner 2026-11-20) the mark says the place's name alone: a release since T780 ships no ", approximate
    // location (… confidence)" in its end words; the committed fixture (cut before it) still does, read here without it
    // until the release re-cuts the fixture (`plainEnd`)
    expect(plainEnd(view(g, waterMapStart(0)).marks[0]!.label!)).toBe("White triangle boundary sign located in Sandy Pool Regional Park");
  });

  it("Vedder: two parts of one run (T517); its mouth flows into the Sumas (its top part's ▲ goes on into the Chilliwack, above)", async () => {
    const g = await loadWaterGroup(st, "gnis:3062");
    // (T517) the fixture's core names every water a kept water flows into, so the Vedder's mouth links the Sumas
    expect(g.parts.map((_, i) => bar(g, waterMapStart(i)))).toEqual([
      `${lead(g, 0)} | Lowest part (off) | Upstream | Flows into Sumas River ›`,
      `${lead(g, 1)} | Downstream | Upstream into Chilliwack River`]);
  });

  it("KLWA: the lake's three closure areas in the prototype's order, stepped by their exits, out into the Kootenay River; no placed marks", async () => {
    const g = await loadWaterGroup(st, "wbk:-20");
    expect(waterMapStartIx(g, "wbk:-20", 0)).toBe(2);
    expect(g.parts.map((_, i) => bar(g, waterMapStart(i)))).toEqual([
      `${lead(g, 0)} | Lowest part (off) | Upstream | Flows into Kootenay River ›`,
      `${lead(g, 1)} | Downstream | Upstream`,
      `${lead(g, 2)} | Downstream | Highest part (off)`]);
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
    // (T517) the Vedder's top part (1) meets the Chilliwack's lowest (0)
    const onVedder: Route = { mode: "water", from: HOME,
                              stack: [{ item: "gnis:3062" as ItemId, part: 1, tapSid: null, at: null, view: "map" }] };
    const r = reduce(onVedder, crossAction(CHW, 0));
    expect([crossAction(CHW, 0), r, reduce(r, { type: "back" }), reduce(onMap, crossAction("gnis:3062" as ItemId, 1))])
      .toEqual([
        { type: "cross", item: CHW, part: 0 },
        { mode: "water", from: HOME, stack: [{ item: CHW, part: 0, tapSid: null, at: null, view: "map" }] },
        HOME,
        { ...onMap, stack: [{ item: "gnis:3062", part: 1, tapSid: null, at: null, view: "map" }] }]);
  });

  it("(owner 2026-10-22 late) ▲ / ▼ back and forth across the boundary never stacks: one back from either side leaves", () => {
    const onVedder: Route = { mode: "water", from: HOME,
                              stack: [{ item: "gnis:3062" as ItemId, part: 1, tapSid: null, at: null, view: "map" }] };
    const r = [crossAction(CHW, 0), crossAction("gnis:3062" as ItemId, 1), crossAction(CHW, 0)].reduce(reduce, onVedder);
    expect([r, reduce(r, { type: "back" })]).toEqual([
      { mode: "water", from: HOME, stack: [{ item: CHW, part: 0, tapSid: null, at: null, view: "map" }] }, HOME]);
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
// watershed boundary is 316506 (7028 → 16309) alone. And its mouth meets the Fraser's TIDAL part (0 in river order), so "Flows into"
// is said, never a way into a part no Fraser map lists (which opened the Fraser on "2 stretches · stretch 1 of 2").
// (T441) the Coquitlam's sections as [stream key, start metre] (the fixture's `lines`), never as ids: Mary Hill → Lougheed
// is two sections (312 → 6590, the gauge 08MH002 cut, and 6590 → 7028), Lougheed → the watershed boundary one (7028 →
// 16309), and the rest the lake-outlet part's — (T678 D1, r-0a9930fb8826) with the Coquitlam's one folded pond
// (lake:329292598), a section of that part with no line, named by the part (`sec.lake`)
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
      ["Coquitlam Watershed boundary → Source",   // (T697 / P55) the top end reads its real boundary
       [...await sec.streams(COQ_UPPER), await sec.lake("gnis:19986", await sec.partAt("gnis:19986", 356364584, 16309))]],
    ]);
    // never one section in two parts, and every part's `water` is exactly the others' sections
    const all = g.parts.flatMap((p) => p.sids);
    expect(new Set(all).size).toBe(all.length);
    g.parts.forEach((_, i) => expect([...view(g, waterMapStart(i)).water].sort((a, b) => a - b))
      .toEqual(g.parts.flatMap((p, j) => (j === i ? [] : p.sids)).sort((a, b) => a - b)));
    const lougheed = g.marks.find((m) => markLabel(g, m) === "Lougheed Highway Bridge")!;
    expect([lougheed.down, lougheed.up]).toEqual([0, [1]]);
    // the list's pick lights the same part a step does
    expect(view(g, run(g, { ...waterMapStart(1), list: true }, { type: "part", ix: 0 })).selected).toEqual(await sec.streams(COQ_MARY_HILL));
  });

  it("the mouth part says 'Flows into the tidal Fraser River' and offers nothing to open", async () => {
    const g = await loadWaterGroup(st, "gnis:19986");
    expect(view(g, waterMapStart(0)).exit).toEqual(
      { label: "Flows into the tidal Fraser River", item: "gnis:39325", part: 0, tidal: true });
    // the receiving part IS the Fraser's tidal piece, which its map does not list
    const fraser = await loadWaterGroup(st, "gnis:39325");
    expect(fraser.parts.some((p) => p.partIx === 0)).toBe(false);
  });
});

// (T985 #2, owner 2026-11-21: "Label the mouth end") the in-app report: the Chilliwack's mouth part labelled only its upper
// end (Tamihi Rapids Bridge). The fixture release was cut before the mouth's place (`water_exit.lon` / `lat`), so the
// group is given one here: a release that carries it is a group whose exit has `at`
describe("(T985) the mouth: 'Flows into <water>' at the downstream end of every water with an exit", () => {
  const AT = [-121.96, 49.1] as const;
  const withMouth = (g: WaterGroup): WaterGroup => ({ ...g, exit: { ...g.exit!, at: AT } });

  it("the Chilliwack's mouth part: its upper end and its mouth both labelled; a ring alone at the mouth one part up", async () => {
    const g = withMouth(await loadWaterGroup(st, "gnis:8634"));
    const labels = (at: number) => view(g, waterMapStart(at)).marks.filter((m) => m.label !== null).map((m) => m.label);
    const top = markLabel(g, g.marks.find((m) => m.down === 0)!);
    expect(labels(0)).toEqual([top, "Flows into Vedder River"]);
    expect(view(g, waterMapStart(0)).marks.at(-1)).toEqual({ ix: -1, lon: -121.96, lat: 49.1, label: "Flows into Vedder River",
      a11y: "Mouth: Flows into Vedder River", on: false, mouth: { into: { item: "gnis:3062", part: 1 } } });
    expect(view(g, waterMapStart(1)).marks.at(-1)).toMatchObject({ ix: -1, label: null, a11y: "Mouth: Flows into Vedder River" });
    expect(labels(1)).not.toContain("Flows into Vedder River");
  });

  it("a tap on the mouth opens the receiving water at the part it meets (the bar's link); a boundary's tap opens its words", async () => {
    const g = withMouth(await loadWaterGroup(st, "gnis:8634"));
    const v = view(g, waterMapStart(0));
    expect([markTap(v.marks.at(-1)!), markTap(v.marks[0]!)]).toEqual([
      { open: { item: "gnis:3062", part: 1 } }, { act: { type: "mark", ix: 0 } }]);
    // the mouth is no boundary: a "mark" action naming it changes nothing
    expect(run(g, waterMapStart(0), { type: "mark", ix: -1 })).toEqual(waterMapStart(0));
  });

  it("on tidal water: 'Flows into the tidal Fraser River', and a tap opens nothing", async () => {
    const g = withMouth(await loadWaterGroup(st, "gnis:19986"));
    const m = view(g, waterMapStart(0)).marks.at(-1)!;
    expect([m.label, m.a11y, m.mouth, markTap(m)]).toEqual(["Flows into the tidal Fraser River",
      "Mouth: Flows into the tidal Fraser River", { into: null }, null]);
  });

  it("a release cut before the mouth's place draws no mouth; a water with no exit has none", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect(view(g, waterMapStart(0)).marks.some((m) => m.mouth)).toBe(false);
    const none = { ...g, exit: null };
    expect(view(none, waterMapStart(0)).marks.some((m) => m.mouth)).toBe(false);
  });
});

describe("(T443, owner on 0.6.7) two marks with the same words a few steps apart draw ONE label", () => {
  it("the Fraser's Landstrom Bar sign zone: both ends of the zone named once, at every part they bound", async () => {
    const g = await loadWaterGroup(st, "gnis:39325");
    // (T517) the zone's two ends are one end token said at two places: "… (km 156.4)" and "… (km 156.5)"
    const zone = g.marks.flatMap((m, ix) => (markLabel(g, m)?.startsWith("Landstrom Bar sign zone boundary (km ") ? [ix] : []));
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

  it("(T747) two marks with the SAME words (the same end token, place and all) are one label at any distance", async () => {
    // the Skeena's part 1 meets parts 2 and 5 at two marks 4.2 km apart, both "… Ecological Reserve boundary (km 63.5)":
    // framed wide, two labels a few pixels apart (doubled text). Made here from the Chilliwack: one of part 2's ends
    // said again 4 km east, bounding the same part
    const g = await loadWaterGroup(st, "gnis:8634");
    const v0 = view(g, waterMapStart(1));
    const end = v0.marks.find((m) => m.label !== null)!;
    const src = g.marks[end.ix]!;
    const twin = { ...src, lon: src.lon! + 0.055 };
    const g2 = { ...g, marks: [...g.marks, twin] };
    const v = view(g2, waterMapStart(1));
    const both = v.marks.filter((m) => m.ix === end.ix || m.ix === g.marks.length);
    expect([both.length, both.filter((m) => m.label !== null).length, both.every((m) => m.a11y.length > 0)]).toEqual([2, 1, true]);
    // MUTATION PIN: the same end said at ANOTHER place (other words) 4 km away is its own label
    const tok = `${src.token!.replace(/@.*$/, "")}@km999.9`;
    const other = { ...twin, token: tok };
    const v3 = view({ ...g, ends: { ...g.ends, [tok]: g.ends[src.token!]! }, marks: [...g.marks, other] }, waterMapStart(1));
    expect(v3.marks.filter((m) => m.ix === end.ix || m.ix === g.marks.length).filter((m) => m.label !== null)).toHaveLength(2);
  });

  it("MUTATION PIN: labels a kilometre apart, or of other ends, are both drawn", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    const v = view(g, waterMapStart(1));
    expect(v.marks.filter((m) => m.label !== null).length).toBe(2);   // part 2's two ends, different words
  });
});

