/**
 * (T1140, owner 2026-11-25) A WATER OF SEVERAL PARTS SAYS WHICH PART OF HOW MANY, on every head, in the Map tab's own words
 * and count: `entryPlaces` over the SAME `mergeNav(group, live salmon key)` the map draws, so "Part n of m" on the
 * Regulations and Conditions heads is the map bar's `partWhere` for that state — joins included (T517). And "Other parts of
 * this water have different rules" only where another entry has another rule set, another water of the group, or another
 * salmon reading: two runs of one rule set are not "different".
 */
import { describe, expect, it } from "vitest";
import { entryPlaces, mergeNav, partWhere, WATER_MAP_WORDS, waterGroup, type ItemNavRows } from "./waterNav";

const ENDS = { mouth: ["Mouth"], sp: ["Sandy Pool sign"], cb: ["Canyon Bridge"], ws: ["Watershed boundary"], source: ["Source"] };
const piece = (partIx: number, lo: string, hi: string, grp = 0, run = 0) => ({ partIx, label: "", grp, run, lo, hi });
/** one river: parts 0-2 one run of rule set 0 (the DFO chain cuts it), part 3 rule set 1 above it */
const river = (top: { grp: number; run: number } = { grp: 1, run: 0 }): ItemNavRows => ({
  ord: 1, item: "gnis:1", name: "Test River", kind: "stream", tidal: [],
  parts: [piece(0, "mouth", "sp"), piece(1, "sp", "cb"), piece(2, "cb", "ws"), piece(3, "ws", "source", top.grp, top.run)],
  geo: [0, 1, 2, 3].map((partIx) => ({ partIx, box: [partIx, 0, partIx + 1, 1] as const, mid: [partIx + 0.5, 0.5] as const })),
  marks: [{ lon: 1, lat: 0.5, token: "sp", down: 0, up: [1] }, { lon: 2, lat: 0.5, token: "cb", down: 1, up: [2] },
          { lon: 3, lat: 0.5, token: "ws", down: 2, up: [3] }],
  sections: [[1, 0], [2, 0], [3, 1], [4, 2], [5, 3], [6, 3]].map(([sid, partIx]) => ({ sid: sid!, partIx: partIx! })),
  exit: null,
});
const feed = (open: ReadonlySet<number>) => (sid: number) => (open.has(sid) ? "open" : "closed");
/** the map bar's count for the entry holding part `k` ("Part 2 of 3"), without its " · <water>" note */
const barSays = (g: ReturnType<typeof waterGroup>, k: number) =>
  partWhere(g, g.parts.findIndex((p) => p.pieces.includes(k))).split(" · ")[0];

describe("(T1140) entryPlaces: which part of how many, as the Map tab counts it", () => {
  it("after the live salmon joins the run: the run is part 1 of 2 for every piece of it, the rule set above part 2 of 2", () => {
    const key = feed(new Set());
    const g = mergeNav(waterGroup([river()], ENDS), key);
    const places = entryPlaces(g, "gnis:1", key);
    expect([...places]).toEqual([[0, { n: 1, of: 2, differs: true }], [1, { n: 1, of: 2, differs: true }],
                                 [2, { n: 1, of: 2, differs: true }], [3, { n: 2, of: 2, differs: true }]]);
    // the words are the map bar's, part for part
    for (const k of [0, 1, 2, 3]) expect(WATER_MAP_WORDS.partOf(places.get(k)!.n, places.get(k)!.of)).toBe(barSays(g, k));
  });

  it("a feed that splits the run: the count follows the map (4 entries), and the bar agrees for every part", () => {
    const key = feed(new Set([3]));
    const g = mergeNav(waterGroup([river()], ENDS), key);
    const places = entryPlaces(g, "gnis:1", key);
    expect([...places].map(([k, p]) => [k, p.n, p.of])).toEqual([[0, 1, 4], [1, 2, 4], [2, 3, 4], [3, 4, 4]]);
    for (const k of [0, 1, 2, 3]) expect(WATER_MAP_WORDS.partOf(places.get(k)!.n, places.get(k)!.of)).toBe(barSays(g, k));
  });

  it("'different rules' is never said of entries of one rule set and one salmon reading (two runs a lake parts)", () => {
    // the top part has the run's rule set but is another connected run: with salmon read alike, the same rules
    const key = feed(new Set());
    const g = mergeNav(waterGroup([river({ grp: 0, run: 1 })], ENDS), key);
    expect(g.parts.length).toBe(2);
    expect([...entryPlaces(g, "gnis:1", key).values()].map((p) => p.differs)).toEqual([false, false, false, false]);
    // a salmon reading that differs between them IS different rules
    const opened = feed(new Set([5, 6]));
    const g2 = mergeNav(waterGroup([river({ grp: 0, run: 1 })], ENDS), opened);
    expect([...entryPlaces(g2, "gnis:1", opened).values()].map((p) => p.differs)).toEqual([true, true, true, true]);
    // and with no salmon read (nothing joins), only the rule sets speak: one rule set, nothing "different"
    const raw = mergeNav(waterGroup([river({ grp: 0, run: 1 })], ENDS), null);
    expect([...entryPlaces(raw, "gnis:1", null).values()].map((p) => [p.of, p.differs])).toEqual(
      [[4, false], [4, false], [4, false], [4, false]]);
  });

  it("a water the map shows as ONE entry has no places: its heads stay as they were", () => {
    const one: ItemNavRows = { ...river(), parts: [piece(0, "mouth", "sp"), piece(1, "sp", "cb")],
                               geo: river().geo.slice(0, 2), marks: river().marks.slice(0, 1),
                               sections: [{ sid: 1, partIx: 0 }, { sid: 2, partIx: 1 }] };
    const key = feed(new Set());
    const g = mergeNav(waterGroup([one], ENDS), key);
    expect(g.parts.length).toBe(1);
    expect(partWhere(g, 0)).toBe("Whole water");
    expect(entryPlaces(g, "gnis:1", key).size).toBe(0);
  });
});
