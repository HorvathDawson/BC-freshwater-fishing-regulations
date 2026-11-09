/**
 * T228 (owner 2026-10-28): pieces of one part merge on the map only when the salmon regulations match too — and the
 * live feed decides that, so a feed that opens salmon on one of two pieces splits them and reverting joins them.
 * (T517) A piece is now a PART of its own: one connected run of one rule set (`grp`, `run`) the DFO salmon chain cuts
 * is several parts, each shipping its two end tokens (`lo`, `hi`); every name — a piece's, a joined span's — is the
 * one name function's (`spanName`) over them.
 * ONE mode (owner 2026-11-08): they join only where the live salmon reads them alike; with no reading, never.
 */
import { describe, expect, it } from "vitest";
import { entryNames, markLabel, mergeNav, partAt, partName, stepsUp, waterGroup, type ItemNavRows } from "./waterNav";
import { salmonTimelineKey, type SalmonFacts } from "./salmon";

// one river: parts 0 (sids 1, 2), 1 (sid 3), 2 (sid 4) are one run of one rule set (grp 0, run 0) cut by DFO chains;
// part 3 (no-access land, sids 5, 6; grp 1) above it. (T517, owner 2026-11-09) each part ships its two END TOKENS; the
// release's `display.ends` says them (`spanName`)
const RUN = "Mouth → Watershed boundary";
const ENDS = { mouth: ["Mouth"], sp: ["Sandy Pool sign"], cb: ["Canyon Bridge"], ws: ["Watershed boundary"], source: ["Source"] };
const piece = (partIx: number, lo: string, hi: string) => ({ partIx, label: RUN, short: null, grp: 0, run: 0, lo, hi });
const RIVER: ItemNavRows = {
  ord: 1, item: "gnis:1", name: "Test River", kind: "stream", tidal: [],
  parts: [piece(0, "mouth", "sp"), piece(1, "sp", "cb"), piece(2, "cb", "ws"),
          { partIx: 3, label: "Watershed boundary → Source", short: null, lo: "ws", hi: "source", grp: 1, run: 0 }],
  geo: [{ partIx: 0, box: [0, 0, 1, 1], mid: [0.5, 0.5] }, { partIx: 1, box: [1, 0, 2, 1], mid: [1.5, 0.5] },
        { partIx: 2, box: [2, 0, 3, 1], mid: [2.5, 0.5] }, { partIx: 3, box: [3, 0, 5, 1], mid: [4, 0.5] }],
  marks: [
    { lon: 1, lat: 0.5, token: "sp", down: 0, up: [1] },
    { lon: 2, lat: 0.5, token: "cb", down: 1, up: [2] },
    { lon: 3, lat: 0.5, token: "ws", down: 2, up: [3] },
  ],
  sections: [[1, 0], [2, 0], [3, 1], [4, 2], [5, 3], [6, 3]].map(([sid, partIx]) => ({ sid: sid!, partIx: partIx! })),
  exit: null,
};

const feed = (open: ReadonlySet<number>) => (sid: number) => (open.has(sid) ? "open" : "closed");

describe("mergeNav (T228, T517)", () => {
  it("joins every piece of a run when salmon reads alike, named by the whole run; never across the no-access part", () => {
    const g = mergeNav(waterGroup([RIVER], ENDS), feed(new Set()));
    expect(g.parts.map((p) => [p.partIx, p.pieces, p.sids])).toEqual([[0, [0, 1, 2], [1, 2, 3, 4]], [3, [3], [5, 6]]]);
    expect(g.parts[0]!.box).toEqual([0, 0, 3, 1]);
    expect([0, 1].map((i) => partName(g, i))).toEqual([RUN, "Watershed boundary → Source"]);
    expect(g.marks.map((m) => [m.down, m.up, markLabel(g, m)])).toEqual([[0, [1], "Watershed boundary"]]);
    expect(stepsUp(g, 0)).toEqual([1]);
  });

  it("a feed opening salmon on the middle piece splits it out; reverting joins them again", () => {
    const open = mergeNav(waterGroup([RIVER], ENDS), feed(new Set([3])));
    expect(open.parts.map((p) => p.sids)).toEqual([[1, 2], [3], [4], [5, 6]]);
    // a salmon piece is named by its own ends (the named splits), never "stretch 2 of 3"
    expect([0, 1, 2].map((i) => partName(open, i))).toEqual([
      "Mouth → Sandy Pool sign", "Sandy Pool sign → Canyon Bridge", "Canyon Bridge → Watershed boundary"]);
    expect(markLabel(open, open.marks[0]!)).toBe("Sandy Pool sign");
    expect(markLabel(open, open.marks[1]!)).toBe("Canyon Bridge");
    const back = mergeNav(waterGroup([RIVER], ENDS), feed(new Set()));
    expect(back.parts.map((p) => p.sids)).toEqual([[1, 2, 3, 4], [5, 6]]);
  });

  it("opening the top two pieces alike keeps them one, apart from the mouth piece, named by their outer ends", () => {
    const g = mergeNav(waterGroup([RIVER], ENDS), feed(new Set([3, 4])));
    expect(g.parts.map((p) => p.sids)).toEqual([[1, 2], [3, 4], [5, 6]]);
    expect(g.parts.map((p) => p.pieces)).toEqual([[0], [1, 2], [3]]);
    expect(g.marks.map((m) => [m.down, m.up])).toEqual([[0, [1]], [1, [2]]]);
    expect([0, 1].map((i) => partName(g, i))).toEqual(["Mouth → Sandy Pool sign", "Sandy Pool sign → Watershed boundary"]);
  });

  it("(T517) a frame on ANY piece of a joined entry finds that entry: partAt by its pieces, never -1", () => {
    const g = mergeNav(waterGroup([RIVER], ENDS), feed(new Set([3, 4])));
    expect([0, 1, 2, 3].map((k) => partAt(g, "gnis:1", k))).toEqual([0, 1, 1, 2]);
    expect(partAt(g, "gnis:1", 9)).toBe(-1);
  });

  it("(T517) every part of the water is named as the entry holding it is (`entryNames`: the header's and sheet's names)", () => {
    const g = mergeNav(waterGroup([RIVER], ENDS), feed(new Set([3, 4])));
    expect([...entryNames(g, "gnis:1")]).toEqual([
      [0, "Mouth → Sandy Pool sign"], [1, "Sandy Pool sign → Watershed boundary"], [2, "Sandy Pool sign → Watershed boundary"],
      [3, "Watershed boundary → Source"]]);
    const all = mergeNav(waterGroup([RIVER], ENDS), feed(new Set()));
    expect([...entryNames(all, "gnis:1")]).toEqual([[0, RUN], [1, RUN], [2, RUN], [3, "Watershed boundary → Source"]]);
  });

  it("with no salmon read, nothing joins across a part: every piece is its own entry, by its own name (one mode)", () => {
    const g = mergeNav(waterGroup([RIVER], ENDS), null);
    expect(g.parts.map((p) => p.sids)).toEqual([[1, 2], [3], [4], [5, 6]]);
    expect(g.parts.map((_, i) => partName(g, i))).toEqual([
      "Mouth → Sandy Pool sign", "Sandy Pool sign → Canyon Bridge", "Canyon Bridge → Watershed boundary",
      "Watershed boundary → Source"]);
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
const pieces = (f: SalmonFacts) => mergeNav(waterGroup([RIVER], ENDS), (sid) => salmonTimelineKey(f, sid)).parts.map((p) => p.sids);

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
