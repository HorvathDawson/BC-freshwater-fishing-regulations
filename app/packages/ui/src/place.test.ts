/**
 * (T369, owner 2026-11-03) A LINK ADDRESSES A PLACE, NOT A PART NUMBER — across a RE-SPLIT. Two releases of one river,
 * held as plain reads (`PlaceReads`): in the second a section is split and a new part cut, so every part index above
 * it moves. The place a link made in the first resolves in the second to the part that now holds it; the old `part=`
 * index would name another part (it opens the whole water, said: startLink.test.ts).
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import type { NavRows, PartRow } from "@app/data/regs";
import type { SectionLine } from "@app/data/regs/place";
import { placeOf, resolvePlace, resolveStartLink, type PlaceReads } from "./place";
import { parseStartLink, routeSearch } from "./startLink";
import { HOME, reduce, waterFrame, type WaterRoute } from "./route";
import { carryRoute } from "./carry";
import { openWater } from "./openWater";

const BLK = 360000001, RIVER = "gnis:1" as ItemId, ORD = 7;
/** A release: each section's handle, line and part (`stretch`: [part, stretch] for a part of two stretches). */
interface Sec { h: number; from: number; to: number; part: number | null; stretch?: number }
function release(secs: Sec[], mids: Record<number, number>, stretchMids: Record<string, number> = {}): PlaceReads {
  const line = (h: number): SectionLine | null => {
    const s = secs.find((x) => x.h === h);
    return s ? { blk: BLK, from: s.from, to: s.to } : null;
  };
  const parts = [...new Set(secs.flatMap((s) => (s.part === null ? [] : [s.part])))];
  const nav: NavRows = {
    geo: parts.map((p) => ({ partIx: p, rank: p, midSid: mids[p]!, mid: [0, 0], box: null })),
    marks: [], exit: null, continuesAs: null, continuesFrom: null,
    sections: secs.filter((s) => s.part !== null).map((s) => ({ sid: s.h, partIx: s.part! })),
    stretches: Object.entries(stretchMids).map(([k, mid]) => {
      const [p, st] = k.split(":").map(Number);
      return { partIx: p!, stretch: st!, rank: st!, midSid: mid, mid: [0, 0] as const, box: null,
               sids: secs.filter((s) => s.part === p && (s.stretch ?? 0) === st).map((s) => s.h) };
    }),
  };
  return {
    hasLines: async () => true,
    itemOrd: async (id) => (id === RIVER ? ORD : null),
    parts: async () => parts.map((p) => ({ partIx: p, tidal: false }) as unknown as PartRow),
    navRows: async () => nav,
    sectionPart: async (_o, sid) => secs.find((s) => s.h === sid)?.part ?? null,
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

  it("A's part 1 is the middle of its midpoint section; in B that place is part 2 (the index 1 is another part now)", async () => {
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

  it("a place past the water's sections is an unnamed reach in either release; a place no section spans is gone, said", async () => {
    const reach = (await resolvePlace(relB, itemFor(B), { blk: BLK, m: 5500 }))!;
    expect(reach).toEqual({ sid: 24, item: null, part: null });
    const gone = await resolveStartLink(parseStartLink(`?regs=${RIVER}&blk=${BLK}&m=9000&view=map`)!, relB, itemFor(B));
    expect([waterFrame(gone.route), gone.notice]).toEqual([
      waterFrame(reduce(HOME, openWater({ item: RIVER, part: null, view: "map", whole: true }))), "place-gone"]);
  });

  it("a stretch: its own midpoint section, and back to the same stretch", async () => {
    const S: Sec[] = [{ h: 1, from: 0, to: 1000, part: 0 }, { h: 2, from: 1000, to: 2000, part: 0, stretch: 1 },
                      { h: 3, from: 2000, to: 3000, part: 1 }];
    const rel = release(S, { 0: 1, 1: 3 }, { "0:0": 1, "0:1": 2 });
    const frame = waterFrame(reduce(HOME, openWater({ item: RIVER, part: 0, stretch: 1, view: "map" })));
    const place = (await placeOf(rel, frame))!;
    expect([place, await resolvePlace(rel, itemFor(S), place)]).toEqual([
      { blk: BLK, m: 1500 }, { sid: 2, item: RIVER, part: 0, stretch: 1 }]);
  });

  it("no place where none is needed: browsing, a whole water, a water of one part; a release with no lines says so", async () => {
    const one = release([{ h: 1, from: 0, to: 10, part: 0 }], { 0: 1 });
    expect(await Promise.all([
      placeOf(relA, null),
      placeOf(relA, waterFrame(reduce(HOME, openWater({ item: RIVER, view: "map", whole: true })))),
      placeOf(one, waterFrame(reduce(HOME, openWater({ item: RIVER, part: 0, view: "map" }))))])).toEqual([null, null, null]);
    const none: PlaceReads = { ...relA, hasLines: async () => false };
    await expect(placeOf(none, waterFrame(reduce(HOME, openWater({ item: RIVER, part: 1 })))))
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
      { item: null, part: null, stretch: undefined, tapSid: 24, at: { lat: 49, lon: -122 }, view: "regs" }] });
  });

  it("now a named water's part: opened on it; no section spans it any more: dropped; no place reads: dropped", async () => {
    const named: Sec[] = [...B.slice(0, 4), { h: 24, from: 5000, to: 6000, part: 3 }];
    const relN = release(named, { 0: 20, 1: 21, 2: 22, 3: 23 });
    expect(await carryRoute(unnamed(13), relA, relN, { was: relA, now: relN, itemFor: itemFor(named) })).toEqual({ type: "carry",
      stack: [{ item: RIVER, part: 3, stretch: undefined, tapSid: 24, at: { lat: 49, lon: -122 }, view: "regs" }] });
    const gone = release(B.slice(0, 4), { 0: 20, 1: 21, 2: 22, 3: 23 });
    expect(await carryRoute(unnamed(13), relA, gone, { was: relA, now: gone, itemFor: itemFor(B) }))
      .toEqual({ type: "carry", stack: [] });
    // MUTATION PIN: without the place reads the old rule (dropped) is what runs
    expect(await carryRoute(unnamed(13), relA, relB)).toEqual({ type: "carry", stack: [] });
  });
});
