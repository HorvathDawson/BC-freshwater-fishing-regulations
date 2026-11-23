/**
 * (T672 W4, owner 2026-11-12) the meta description names EVERY region a water runs through.
 * (T1042) and it holds the page's own facts: page-specific, in range, no day word, no licence words.
 */
import { describe, expect, it } from "vitest";
import { descriptionHasFacts, descriptionOf, factsOf, FACTLESS_TAIL, openWindows, waterRegions } from "./html";
import type { Item, PartModel, Section, WaterModel } from "./model";

const m = (regions: number[], focus: string | null = null, parts: PartModel[] = []) => ({ name: "Fraser River", regions, region: regions[0] ?? 0, focus, parts }) as unknown as WaterModel;
const it_ = (text: string, o: Partial<Item> = {}): Item => ({ text, book: [], ...o });
type Run = { bits: number | null; days: number };
/** a part with a year strip: runs of (bits, days); closed = bit 1 */
const part = (sections: Section[], runs: Run[] | null = [{ bits: 0, days: 365 }], marks: PartModel["marks"] = []): PartModel => ({
  partIx: 0, label: "x", status: "open", today: "", sections, marks, yearBits: runs,
  year: runs ? runs.map((r) => ({ closed: r.bits === null ? null : (r.bits & 1) === 1, days: r.days })) : null });
const boats = (t: string): Section => ({ title: "Boats", items: [it_(t)] });
const licence: Section = { title: "Licence", items: [it_("If you are a resident of B.C. and disabled: You may qualify for a basic freshwater angling licence at the reduced fee of $1.14.")] };
const dayRows: Section = { title: "Other fish", items: [it_("Kokanee · release"), it_("Whitefish · 15 a day")] };
const gear: Section = { title: "Gear", items: [it_("Hooks: One fly")] };

describe("descriptionOf regions", () => {
  it("a multi-region water names all of them, ascending, once each", () => {
    expect(waterRegions(m([7, 2, 5, 3, 2], "2"))).toBe("Regions 2, 3, 5, 7");
    expect(descriptionOf(m([2, 3, 5, 7], "2"), "Fraser River", "Quesnel")).toContain("(near Quesnel, Regions 2, 3, 5, 7)");
  });
  it("a one-region water keeps its own code", () => {
    expect(waterRegions(m([7], "7a"))).toBe("Region 7A");
    expect(descriptionOf(m([7], "7a"))).toContain("(Region 7A)");
  });
  it("a one-region water without a focus says the hub's number", () => expect(waterRegions(m([4]))).toBe("Region 4"));
});

describe("openWindows (T1042)", () => {
  const w = (...r: [number | null, number][]) => openWindows(part([], r.map(([bits, days]) => ({ bits, days }))).year);
  it("reads the strip's open runs as calendar windows, the new year's two ends joined", () => {
    expect(w([0, 120], [1, 61], [0, 184])).toEqual(["Jul 1–Apr 30"]);
    expect(w([1, 151], [0, 44], [1, 48], [0, 91], [1, 31])).toEqual(["Jun 1–Jul 14", "Sep 1–Nov 30"]);
    expect(w([0, 365])).toEqual(["all year"]);
    expect(w([1, 365])).toEqual([]);
  });
  it("a run the strip does not know, or no strip, gives no windows", () => {
    expect(w([0, 100], [null, 265])).toBeNull();
    expect(openWindows(null)).toBeNull();
  });
});

describe("descriptionOf facts (T1042, owner 2026-11-23: season windows)", () => {
  const A = m([2], "2", [part([boats("No powered boats on parts"), dayRows, gear, licence], [{ bits: 0, days: 120 }, { bits: 1, days: 61 }, { bits: 0, days: 184 }])]);
  const B = m([2], "2", [part([licence], [{ bits: 1, days: 151 }, { bits: 0, days: 214 }])]);
  it("is page-specific: two waters in the same place with different seasons read differently", () => {
    expect(descriptionOf(A)).not.toBe(descriptionOf(B));
    expect(descriptionOf(A)).toBe("Fraser River fishing regulations, B.C. (Region 2): Open Jul 1–Apr 30; No powered boats on parts.");
    expect(descriptionOf(B)).toContain("Open Jun 1–Dec 31");
  });
  it("holds only what is true on every day: never a day's Other fish rows or Hooks line, never the licence", () => {
    const d = descriptionOf(A);
    expect(d).not.toMatch(/Kokanee|Whitefish|Hooks|One fly|licence|\$1\.14|today|Oct 27/i);
    expect(factsOf(A)).toEqual(["Open Jul 1–Apr 30", "No powered boats on parts"]);
  });
  it("is in range: at most 155 characters; a long name drops 'fishing regulations' before a fact, and a fact that does not fit is left out whole", () => {
    expect(descriptionOf(A).length).toBeLessThanOrEqual(155);
    const long = m([2], "2", [part([boats("x".repeat(200))], [{ bits: 0, days: 365 }])]);
    expect(descriptionOf(long)).toBe("Fraser River fishing regulations, B.C. (Region 2): Open all year.");
    const name = "A".repeat(110);   // "<name> fishing regulations, B.C. (Region 2): Open all year." is 164 characters, without "fishing regulations" 144
    expect(descriptionOf(long, name)).toBe(`${name}, B.C. (Region 2): Open all year.`);
  });
  it("on a water in parts, only what every part says", () => {
    const two = m([2], "2", [part([boats("No powered boats")], [{ bits: 0, days: 365 }]), part([boats("No powered boats")], [{ bits: 0, days: 100 }, { bits: 1, days: 265 }])]);
    expect(factsOf(two)).toEqual(["No powered boats"]);
    const same = m([2], "2", [part([boats("No powered boats")]), part([boats("Speed limit 20 km/h")])]);
    expect(factsOf(same)).toEqual(["Open all year"]);
  });
  it("a mark goes in only when it holds on EVERY open day (the strip's bit on every open run)", () => {
    const fly = [{ bit: 4, label: "fly fishing only" }, { bit: 16, label: "bait ban" }];
    const all = m([2], "2", [part([], [{ bits: 4, days: 200 }, { bits: 1, days: 65 }, { bits: 4 | 16, days: 100 }], fly)]);
    expect(factsOf(all)).toContain("fly fishing only");
    expect(factsOf(all)).not.toContain("bait ban");   // only on part of the open year
    const some = m([2], "2", [part([], [{ bits: 4, days: 200 }, { bits: 0, days: 165 }], fly)]);
    expect(factsOf(some)).not.toContain("fly fishing only");
  });
  it("a page with no fact says so in whole words, never cut, and the build counts it", () => {
    const none = m([2], "2", [part([licence], null)]);
    expect(factsOf(none)).toEqual([]);
    expect(descriptionHasFacts(none)).toBe(false);
    expect(descriptionOf(none)).toBe(`Fraser River fishing regulations, B.C. (Region 2): ${FACTLESS_TAIL}`);
    const d = descriptionOf(none, "B".repeat(70));
    expect(d.length).toBeLessThanOrEqual(155);
    expect(d).toMatch(/from the B\.C\. synopsis\.$/);   // a longer name takes the shorter whole sentence, not a cut one
    expect(descriptionHasFacts(A)).toBe(true);
  });
});
