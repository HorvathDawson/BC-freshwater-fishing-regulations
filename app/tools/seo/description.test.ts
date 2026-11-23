/**
 * (T672 W4, owner 2026-11-12) the meta description names EVERY region a water runs through.
 * (T1042) and it holds the page's own facts: page-specific, in range, no day word, no licence words.
 */
import { describe, expect, it } from "vitest";
import { descriptionOf, factsOf, FACTLESS_TAIL, waterRegions } from "./html";
import type { Item, PartModel, Section, WaterModel } from "./model";

const m = (regions: number[], focus: string | null = null, parts: PartModel[] = []) => ({ name: "Fraser River", regions, region: regions[0] ?? 0, focus, parts }) as unknown as WaterModel;
const it_ = (text: string, o: Partial<Item> = {}): Item => ({ text, book: [], ...o });
const part = (sections: Section[]): PartModel => ({ partIx: 0, label: "x", status: "open", today: "", sections, year: null, marks: [] });
const keep = (limit: string): Section => ({ title: "Other fish", items: [it_("Kokanee · release"), it_(limit), it_("Burbot · until Oct 31, 2 a day")] });
const trout: Section = { title: "Trout and char", items: [it_("Trout and char", { head: true }), it_("Keep 2 trout and char today"), it_("2 a day")] };
const gear: Section = { title: "Gear", items: [it_("Today, Oct 27", { dated: true }), it_("Lines: 1 line"), it_("Hooks: One hook, lure or fly, Single barbless hook, no trebles")] };
const licence: Section = { title: "Licence", items: [it_("If you are a resident of B.C. and disabled: You may qualify for a basic freshwater angling licence at the reduced fee of $1.14.")] };

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

describe("descriptionOf facts (T1042)", () => {
  const A = m([2], "2", [part([trout, keep("Whitefish · 15 a day"), gear, licence])]);
  const B = m([2], "2", [part([keep("Whitefish · release"), licence])]);
  it("is page-specific: two waters with the same place and different rules read differently", () => {
    expect(descriptionOf(A)).not.toBe(descriptionOf(B));
    expect(descriptionOf(A)).toBe("Fraser River fishing regulations, B.C. (Region 2): Kokanee · release; Whitefish · 15 a day; Hooks: One hook, lure or fly, Single barbless hook, no trebles.");
    expect(descriptionOf(B)).toContain("Whitefish · release");
  });
  it("is in range: at most 155 characters, and more than the name and place alone", () => {
    for (const w of [A, B]) { const d = descriptionOf(w); expect(d.length).toBeLessThanOrEqual(155); expect(d.length).toBeGreaterThan(40); }
    const long = m([2], "2", [part([{ title: "Boats", items: [it_("x".repeat(200))] }, keep("Whitefish · 15 a day"), gear])]);
    expect(descriptionOf(long).length).toBeLessThanOrEqual(155);
    expect(descriptionOf(long)).toContain("Kokanee · release");
    expect(descriptionOf(long)).not.toContain("xxx");   // a whole fact or none, never a cut one
  });
  it("never says a day word, a dated line or the licence", () => {
    const d = descriptionOf(A);
    expect(d).not.toMatch(/today|Oct 27|until|licence|\$1\.14/i);
    expect(factsOf(A).join(" ")).not.toContain("Burbot");   // its line names a date
  });
  it("on a water in parts, only what every part says", () => {
    const two = m([2], "2", [part([keep("Whitefish · 15 a day"), gear]), part([keep("Whitefish · release"), gear])]);
    expect(factsOf(two)).toEqual(["Kokanee · release", "Hooks: One hook, lure or fly, Single barbless hook, no trebles"]);
  });
  it("a page with no fact that fits says so, and the build counts it", () => {
    expect(descriptionOf(m([2], "2"))).toContain(FACTLESS_TAIL);
  });
});
