/**
 * T228 (owner 2026-10-28): pieces of one part merge on the map only when the salmon regulations match too — and the
 * live feed decides that, so a feed that opens salmon on one of two pieces splits them and reverting joins them.
 */
import { describe, expect, it } from "vitest";
import { markLabel, mergeNav, partName, stepsUp, waterGroup, type ItemNavRows } from "./waterNav";
import { salmonTimelineKey, type SalmonFacts } from "./salmon";

// one river: part 0 (mouth) cut by DFO chains into stretches 0 (sids 1, 2), 1 (sid 3), 2 (sid 4); part 1 (no-access
// land, sids 5, 6) above it
const RIVER: ItemNavRows = {
  ord: 1, item: "gnis:1", name: "Test River", kind: "stream",
  parts: [{ partIx: 0, label: "Mouth → Watershed boundary", short: "Mouth → Watershed boundary" },
          { partIx: 1, label: "Watershed boundary → Source", short: "Watershed boundary → Source" }],
  geo: [{ partIx: 0, rank: 0, box: [0, 0, 3, 1], mid: [1, 0.5] }, { partIx: 1, rank: 1, box: [3, 0, 5, 1], mid: [4, 0.5] }],
  marks: [
    { lon: 1, lat: 0.5, label: "Sandy Pool sign", down: 0, up: [0], downStretch: 0, upStretches: [1] },
    { lon: 2, lat: 0.5, label: "Canyon Bridge", down: 0, up: [0], downStretch: 1, upStretches: [2] },
    { lon: 3, lat: 0.5, label: "Watershed boundary", down: 0, up: [1], downStretch: 2, upStretches: [0] },
  ],
  sections: [1, 2, 3, 4].map((sid) => ({ sid, partIx: 0 })).concat([5, 6].map((sid) => ({ sid, partIx: 1 }))),
  stretches: [
    { partIx: 0, stretch: 0, rank: 0, box: [0, 0, 1, 1], mid: [0.5, 0.5], sids: [1, 2] },
    { partIx: 0, stretch: 1, rank: 1, box: [1, 0, 2, 1], mid: [1.5, 0.5], sids: [3] },
    { partIx: 0, stretch: 2, rank: 2, box: [2, 0, 3, 1], mid: [2.5, 0.5], sids: [4] },
    { partIx: 1, stretch: 0, rank: 3, box: [3, 0, 5, 1], mid: [4, 0.5], sids: [5, 6] },
  ],
  exit: null,
};

const feed = (open: ReadonlySet<number>) => (sid: number) => (open.has(sid) ? "open" : "closed");

describe("mergeNav (T228)", () => {
  it("joins every piece of a part when salmon reads alike, never across the no-access part", () => {
    const g = mergeNav(waterGroup([RIVER]), feed(new Set()));
    expect(g.parts.map((p) => [p.partIx, p.sids])).toEqual([[0, [1, 2, 3, 4]], [1, [5, 6]]]);
    expect(g.parts[0]!.box).toEqual([0, 0, 3, 1]);
    expect(g.parts[0]!.stretches).toBeUndefined();
    expect(g.marks.map((m) => [m.down, m.up, m.label])).toEqual([[0, [1], "Watershed boundary"]]);
    expect(stepsUp(g, 0)).toEqual([1]);
  });

  it("a feed opening salmon on the middle piece splits it out; reverting joins them again", () => {
    const raw = waterGroup([RIVER]);
    const open = mergeNav(raw, feed(new Set([3])));
    expect(open.parts.map((p) => p.sids)).toEqual([[1, 2], [3], [4], [5, 6]]);
    // a salmon piece is named by its own ends (the named splits), never "stretch 2 of 3"
    expect([0, 1, 2].map((i) => partName(open, i))).toEqual([
      "Mouth → Sandy Pool sign", "Sandy Pool sign → Canyon Bridge", "Canyon Bridge → Watershed boundary"]);
    expect(markLabel(open, open.marks[0]!)).toBe("Sandy Pool sign");
    expect(markLabel(open, open.marks[1]!)).toBe("Canyon Bridge");
    const back = mergeNav(raw, feed(new Set()));
    expect(back.parts.map((p) => p.sids)).toEqual([[1, 2, 3, 4], [5, 6]]);
  });

  it("opening the top two pieces alike keeps them one, apart from the mouth piece", () => {
    const g = mergeNav(waterGroup([RIVER]), feed(new Set([3, 4])));
    expect(g.parts.map((p) => p.sids)).toEqual([[1, 2], [3, 4], [5, 6]]);
    expect(g.marks.map((m) => [m.down, m.up])).toEqual([[0, [1]], [1, [2]]]);
  });

  it("with no salmon read, the part's chain-cut pieces are one (the part as before the cut)", () => {
    expect(mergeNav(waterGroup([RIVER]), null).parts.map((p) => p.sids)).toEqual([[1, 2, 3, 4], [5, 6]]);
  });
});

// (T228 follow-up, owner 2026-10-28) the key is the salmon REGULATIONS' content, not the open/closed word
const OPEN2 = { s: "open", t: "Coho: 2 per day" };
const OPEN4H = { s: "open", t: "Coho: 4 per day, hatchery-marked only" };
const CLOSED = { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 };
/** Facts whose chain per section is `chainOf`, chain k's coho verdict all year `coho[k]` (other species closed). */
function salmonFacts(coho: readonly object[], chainOf: (sid: number) => number,
                     verdicts: readonly object[] = [CLOSED, OPEN2, OPEN4H]): SalmonFacts {
  const ix = (v: object) => verdicts.indexOf(v);
  const chains = coho.map((v) => ({ r: [], w: "", l: [], sp: [[0, ix(v)], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 1] }));
  return { handles: "", chains: chains.length, chainOf, payload: { verdicts, gears: [], chains } } as unknown as SalmonFacts;
}
// sections 1, 2 on chain 0; 3 on chain 1; 4 on chain 2
const chainOf = (sid: number) => (sid === 3 ? 1 : sid === 4 ? 2 : 0);
const pieces = (f: SalmonFacts) => mergeNav(waterGroup([RIVER]), (sid) => salmonTimelineKey(f, sid)).parts.map((p) => p.sids);

describe("the salmon key is the regulations' content (T228 follow-up)", () => {
  it("a limit difference splits though both are open", () => {
    const v = [CLOSED, OPEN2, { s: "open", t: "Coho: 4 per day" }];
    expect(pieces(salmonFacts([v[1]!, v[2]!, v[1]!], chainOf, v))).toEqual([[1, 2], [3], [4], [5, 6]]);
  });
  it("hatchery-marked only vs any coho splits", () => {
    expect(pieces(salmonFacts([OPEN2, OPEN4H, OPEN2], chainOf))).toEqual([[1, 2], [3], [4], [5, 6]]);
  });
  it("identical content joins, even across two chains and with case or spacing apart", () => {
    const loud = { s: "open", t: "coho:  2 PER day" };
    const f = salmonFacts([OPEN2, OPEN2, loud], chainOf, [CLOSED, OPEN2, OPEN4H, loud]);
    expect(pieces(f)).toEqual([[1, 2, 3, 4], [5, 6]]);
  });
  it("a feed change re-groups: the middle piece goes hatchery-only, then back", () => {
    expect(pieces(salmonFacts([OPEN2, OPEN4H, OPEN2], chainOf))).toEqual([[1, 2], [3], [4], [5, 6]]);
    expect(pieces(salmonFacts([OPEN2, OPEN2, OPEN2], chainOf))).toEqual([[1, 2, 3, 4], [5, 6]]);
  });
});
