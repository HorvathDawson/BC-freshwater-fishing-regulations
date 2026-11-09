/**
 * (T369, owner 2026-11-03) A LINK ADDRESSES A PLACE, NOT A PART NUMBER — across a RE-SPLIT. Two releases of one river,
 * held as plain reads (`PlaceReads`): in the second a section is split and a new part cut, so every part index above
 * it moves. The place a link made in the first resolves in the second to the part that now holds it; the old `part=`
 * index would name another part (it opens the whole water, said: startLink.test.ts).
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import type { NavRows, PartLine, PartRow } from "@app/data/regs";
import type { SectionLine } from "@app/data/regs/place";
import { placeOf, resolvePartPlace, resolvePlace, resolveStartLink, type PlaceReads } from "./place";
import { parseStartLink, routeSearch } from "./startLink";
import { HOME, reduce, waterFrame, type WaterFrame, type WaterRoute } from "./route";
import { carryRoute } from "./carry";
import { openWater } from "./openWater";

const BLK = 360000001, RIVER = "gnis:1" as ItemId, ORD = 7;
/** A release: each section's handle, line and part. `runs`: its `part_line` (T388; every water/2 release has it),
 *  by default each part's sections on BLK merged (`runsOf`). */
interface Sec { h: number; from: number; to: number; part: number | null }
/** The runs of a release's sections: each part's sections on BLK merged, + `extra`. */
const runsOf = (secs: Sec[], extra: PartLine[] = []): PartLine[] => {
  const out: PartLine[] = [];
  for (const s of secs) {
    if (s.part === null) continue;
    const last = out[out.length - 1];
    if (last && last.partIx === s.part && last.m1 === s.from) out[out.length - 1] = { ...last, m1: s.to };
    else out.push({ partIx: s.part, blk: BLK, m0: s.from, m1: s.to });
  }
  return [...out, ...extra];
};
function release(secs: Sec[], mids: Record<number, number>, runs: PartLine[] = runsOf(secs)): PlaceReads {
  const line = (h: number): SectionLine | null => {
    const s = secs.find((x) => x.h === h);
    return s ? { blk: BLK, from: s.from, to: s.to } : null;
  };
  const parts = [...new Set(secs.flatMap((s) => (s.part === null ? [] : [s.part])))];
  const nav: NavRows = {
    geo: parts.map((p) => ({ partIx: p, midSid: mids[p]!, mid: [0, 0], box: null })),
    marks: [], exit: null, continuesAs: null, continuesFrom: null,
    sections: secs.filter((s) => s.part !== null).map((s) => ({ sid: s.h, partIx: s.part! })),
  };
  return {
    hasLines: async () => true,
    itemOrd: async (id) => (id === RIVER ? ORD : null),
    parts: async () => parts.map((p) => ({ partIx: p, tidal: false }) as unknown as PartRow),
    navRows: async () => nav,
    sectionPart: async (_o, sid) => secs.find((s) => s.h === sid)?.part ?? null,
    partLines: async () => runs,
    sectionLine: async (h) => line(h),
    sectionAt: async (blk, m) => (blk === BLK ? secs.find((s) => s.from <= m && m < s.to)?.h ?? null : null),
  };
}
const itemFor = (secs: Sec[]) => ({ itemForSection: async (sid: SectionId) => (secs.find((s) => s.h === sid)?.part !== null ? RIVER : null) });

// release A: three parts, mouth up
const A: Sec[] = [{ h: 10, from: 0, to: 1000, part: 0 }, { h: 11, from: 1000, to: 3000, part: 1 },
                  { h: 12, from: 3000, to: 5000, part: 2 }, { h: 13, from: 5000, to: 6000, part: null }];
// release B: section 11 split at 1800 and a new part cut there — part 1 is now [1000, 1800), the old part 1's upper
// half (where its middle was) part 2, the old part 2 part 3; handles renumbered
const B: Sec[] = [{ h: 20, from: 0, to: 1000, part: 0 }, { h: 21, from: 1000, to: 1800, part: 1 },
                  { h: 22, from: 1800, to: 3000, part: 2 }, { h: 23, from: 3000, to: 5000, part: 3 },
                  { h: 24, from: 5000, to: 6000, part: null }];

describe("(T369) a place across a re-split", () => {
  const relA = release(A, { 0: 10, 1: 11, 2: 12 }), relB = release(B, { 0: 20, 1: 21, 2: 22, 3: 23 });

  it("A's part 1 is the middle of its run; in B that place is part 2 (the index 1 is another part now)", async () => {
    const frame = waterFrame(reduce(HOME, openWater({ item: RIVER, part: 1, view: "map" })));
    const place = (await placeOf(relA, frame))!;
    expect(place).toEqual({ blk: BLK, m: 2000 });
    expect(await resolvePlace(relA, itemFor(A), place)).toEqual({ sid: 11, item: RIVER, part: 1 });
    expect(await resolvePlace(relB, itemFor(B), place)).toEqual({ sid: 22, item: RIVER, part: 2 });
    // the whole round trip: A's address, opened on B
    const url = routeSearch(reduce(HOME, openWater({ item: RIVER, part: 1, view: "map" })), "", place);
    expect(url).toBe(`?regs=${RIVER}&blk=${BLK}&m=2000&view=map`);
    const opened = await resolveStartLink(parseStartLink(url)!, relB, itemFor(B));
    expect([waterFrame(opened.route), opened.notice]).toEqual([
      waterFrame(reduce(HOME, openWater({ item: RIVER, part: 2, view: "map" }))), null]);
  });

  it("a place past the water's sections is an unnamed reach by `lines`; a named link no part holds is gone, said", async () => {
    const reach = (await resolvePlace(relB, itemFor(B), { blk: BLK, m: 5500 }))!;
    expect(reach).toEqual({ sid: 24, item: null, part: null });
    const gone = await resolveStartLink(parseStartLink(`?regs=${RIVER}&blk=${BLK}&m=9000&view=map`)!, relB, itemFor(B));
    expect([waterFrame(gone.route), gone.notice]).toEqual([
      waterFrame(reduce(HOME, openWater({ item: RIVER, part: null, view: "map", whole: true }))), "place-gone"]);
  });

  it("no place where none is needed: browsing, a whole water, a water of one part; a release with no lines says so", async () => {
    const one = release([{ h: 1, from: 0, to: 10, part: 0 }], { 0: 1 });
    expect(await Promise.all([
      placeOf(relA, null),
      placeOf(relA, waterFrame(reduce(HOME, openWater({ item: RIVER, view: "map", whole: true })))),
      placeOf(one, waterFrame(reduce(HOME, openWater({ item: RIVER, part: 0, view: "map" }))))])).toEqual([null, null, null]);
    // an unnamed reach's place is the tapped section's middle, from `lines`: a release with none says so
    const none: PlaceReads = { ...relA, hasLines: async () => false };
    await expect(placeOf(none, waterFrame(reduce(HOME, openWater({ item: null, tapSid: 13 as SectionId })))))
      .rejects.toThrow("this release has no section_line");
  });
});

describe("(T386, owner 2026-11-04) an unnamed reach open across a data update: carried by its place", () => {
  const relA = release(A, { 0: 10, 1: 11, 2: 12 }), relB = release(B, { 0: 20, 1: 21, 2: 22, 3: 23 });
  const unnamed = (sid: number): WaterRoute => ({ mode: "water", from: HOME,
    stack: [{ item: null, part: null, tapSid: sid as SectionId, at: { lat: 49, lon: -122 }, view: "regs" }] });

  it("still unnamed in the new release: the section there, the frame kept", async () => {
    const a = await carryRoute(unnamed(13), relA, relB, { was: relA, now: relB, itemFor: itemFor(B) });
    expect(a).toEqual({ type: "carry", stack: [
      { item: null, part: null, tapSid: 24, at: { lat: 49, lon: -122 }, view: "regs" }] });
  });

  it("now a named water's part: opened on it; no section spans it any more: dropped; no place reads: dropped", async () => {
    const named: Sec[] = [...B.slice(0, 4), { h: 24, from: 5000, to: 6000, part: 3 }];
    const relN = release(named, { 0: 20, 1: 21, 2: 22, 3: 23 });
    expect(await carryRoute(unnamed(13), relA, relN, { was: relA, now: relN, itemFor: itemFor(named) })).toEqual({ type: "carry",
      stack: [{ item: RIVER, part: 3, tapSid: 24, at: { lat: 49, lon: -122 }, view: "regs" }] });
    const gone = release(B.slice(0, 4), { 0: 20, 1: 21, 2: 22, 3: 23 });
    expect(await carryRoute(unnamed(13), relA, gone, { was: relA, now: gone, itemFor: itemFor(B) }))
      .toEqual({ type: "carry", stack: [] });
    // MUTATION PIN: without the place reads the old rule (dropped) is what runs
    expect(await carryRoute(unnamed(13), relA, relB)).toEqual({ type: "carry", stack: [] });
  });
});

/**
 * (T388, owner 2026-11-04) A NAMED WATER NEVER FETCHES `lines`: its places are made from and opened by the water file's
 * own `part_line`. Every release below has the runs, and every `lines` read THROWS and is counted — so a named place that
 * touched `lines` fails here, not only in the browser walk.
 */
describe("(T388) a named water's place comes from part_line, never lines", () => {
  const SIDE = 360000002;
  /** `rel` with every `lines` read refused and counted. */
  const noLines = (rel: PlaceReads) => {
    const asked: string[] = [];
    const refuse = (what: string) => async (): Promise<never> => { asked.push(what); throw new Error(`lines read: ${what}`); };
    return { asked, reads: { ...rel, hasLines: refuse("hasLines"), sectionLine: refuse("sectionLine"),
                             sectionAt: refuse("sectionAt") } as PlaceReads };
  };
  // part 1 also has a side channel on its own blue line, shorter than its main stem
  const relA = release(A, { 0: 10, 1: 11, 2: 12 }, runsOf(A, [{ partIx: 1, blk: SIDE, m0: 0, m1: 300 }]));
  const relB = release(B, { 0: 20, 1: 21, 2: 22, 3: 23 });
  const AT = { lat: 49.2, lon: -122.1 };

  it("writing: a part's place is the middle of its longest run; a TAPPED frame says its part's, not the tap's", async () => {
    const { asked, reads } = noLines(relA);
    const part = waterFrame(reduce(HOME, openWater({ item: RIVER, part: 1, view: "map" })));
    const tapped = waterFrame(reduce(HOME, openWater({ item: RIVER, part: 1, view: "regs", tapSid: 11 as SectionId, at: AT })));
    // a tap whose part is not set yet: the tapped section's part (12 → part 2)
    const loose: WaterFrame = { ...waterFrame(reduce(HOME, openWater({ item: RIVER, part: 2, view: "regs" })))!,
                                part: null, tapSid: 12 as SectionId, at: AT };
    expect(await Promise.all([placeOf(reads, part), placeOf(reads, tapped), placeOf(reads, loose)]))
      .toEqual([{ blk: BLK, m: 2000 }, { blk: BLK, m: 2000 }, { blk: BLK, m: 4000 }]);
    expect(asked).toEqual([]);
  });

  it("one part: no place, tapped or not", async () => {
    const one = noLines(release([{ h: 1, from: 0, to: 10, part: 0 }], { 0: 1 }, []));
    expect(await Promise.all([
      placeOf(one.reads, waterFrame(reduce(HOME, openWater({ item: RIVER, part: 0, view: "map" })))),
      placeOf(one.reads, waterFrame(reduce(HOME, openWater({ item: RIVER, part: 0, view: "regs", tapSid: 1 as SectionId, at: AT }))))]))
      .toEqual([null, null]);
    expect(one.asked).toEqual([]);
  });

  it("opening: the part of THAT water whose run holds the place — across a re-split too — with no lines read", async () => {
    const a = noLines(relA), b = noLines(relB);
    const url = `?regs=${RIVER}&blk=${BLK}&m=2000&view=map`;
    const here = await resolveStartLink(parseStartLink(url)!, a.reads, itemFor(A));
    const there = await resolveStartLink(parseStartLink(url)!, b.reads, itemFor(B));
    expect([waterFrame(here.route), waterFrame(there.route), here.notice, there.notice]).toEqual([
      waterFrame(reduce(HOME, openWater({ item: RIVER, part: 1, view: "map" }))),
      waterFrame(reduce(HOME, openWater({ item: RIVER, part: 2, view: "map" }))), null, null]);
    // the side channel is part 1's too
    expect(await resolvePartPlace(a.reads, RIVER, ORD, await relA.partLines(ORD), { blk: SIDE, m: 150 }))
      .toEqual({ sid: 11, item: RIVER, part: 1 });
    expect([a.asked, b.asked]).toEqual([[], []]);
  });

  it("a tapped link reopens the tap on its part's midpoint section, at its `here=` point", async () => {
    const { asked, reads } = noLines(relA);
    const r = reduce(HOME, openWater({ item: RIVER, part: 1, view: "regs", tapSid: 10 as SectionId, at: AT }));
    const url = routeSearch(r, "", await placeOf(reads, waterFrame(r)));
    expect(url).toBe(`?regs=${RIVER}&blk=${BLK}&m=2000&here=49.20000,-122.10000`);
    const opened = await resolveStartLink(parseStartLink(url)!, reads, itemFor(A));
    expect(waterFrame(opened.route)).toEqual(
      waterFrame(reduce(HOME, openWater({ item: RIVER, part: 1, view: "regs", tapSid: 11 as SectionId, at: AT }))));
    expect(asked).toEqual([]);
  });

  it("a place no run of THAT water holds is 'not on this map' — never another water, never the unnamed reach there", async () => {
    const { asked, reads } = noLines(relB);
    const whole = waterFrame(reduce(HOME, openWater({ item: RIVER, part: null, view: "map", whole: true })));
    // 5500 is section 24, an unnamed reach in B: an unnamed link (`?blk=&m=`, below) opens it by `lines`; the named link does not
    for (const m of [5500, 9000]) {
      const gone = await resolveStartLink(parseStartLink(`?regs=${RIVER}&blk=${BLK}&m=${m}&view=map`)!, reads, itemFor(B));
      expect([m, waterFrame(gone.route), gone.notice]).toEqual([m, whole, "place-gone"]);
    }
    expect(asked).toEqual([]);
  });

  it("an UNNAMED reach is unchanged: its place and its link still read lines", async () => {
    const { asked, reads } = noLines(relB);
    const reach = waterFrame(reduce(HOME, openWater({ item: null, part: null, view: "regs", tapSid: 24 as SectionId, at: AT })));
    await expect(placeOf(reads, reach)).rejects.toThrow("lines read");
    await expect(resolveStartLink(parseStartLink(`?blk=${BLK}&m=5500`)!, reads, itemFor(B))).rejects.toThrow("lines read");
    expect(asked).toEqual(["hasLines", "sectionAt"]);
    expect(await placeOf(relB, reach)).toEqual({ blk: BLK, m: 5500 });
  });
});
