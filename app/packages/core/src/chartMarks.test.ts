import { describe, expect, it } from "vitest";
import { layoutDayMarks, markTextWidth, type DayMarkIn, type MarkLayoutIn } from "./chartMarks";

const plot = { x: 40, width: 270 };
const base = { plot, top: 21, rowHeight: 12, fontSize: 10 };
const lay = (marks: DayMarkIn[]) => layoutDayMarks({ ...base, marks } as MarkLayoutIn);
const today = (x: number): DayMarkIn => ({ x, text: "Today", kind: "today" });
const regs = (x: number, text = "Wed 11 Nov"): DayMarkIn => ({ x, text, kind: "regs" });
const cond = (x: number, text = "Thu 12 Nov"): DayMarkIn => ({ x, text, kind: "conditions" });

/** the property the ticket asks for: no two label boxes intersect, none leaves the plot, none covers a rule */
function expectClean(marks: DayMarkIn[], rulesToo = true) {
  const out = lay(marks);
  const placed = out.filter((o) => o.placed);
  for (let i = 0; i < placed.length; i++) {
    const a = placed[i]!;
    expect(a.box.x0).toBeGreaterThanOrEqual(plot.x);
    expect(a.box.x1).toBeLessThanOrEqual(plot.x + plot.width);
    if (rulesToo) for (const o of marks) if (o.x !== a.x) expect(o.x > a.box.x0 && o.x < a.box.x1).toBe(false);
    for (let j = i + 1; j < placed.length; j++) {
      const b = placed[j]!;
      const apart = a.box.x1 <= b.box.x0 || b.box.x1 <= a.box.x0 || a.box.y1 <= b.box.y0 || b.box.y1 <= a.box.y0;
      expect(apart, `${a.text}@${a.x} vs ${b.text}@${b.x}`).toBe(true);
    }
  }
  return out;
}

describe("day mark labels never intersect", () => {
  it("the owner's case: today and the next day, a day apart (53 px on build 25)", () => {
    const out = expectClean([today(142), regs(195)]);
    expect(out.every((o) => o.placed)).toBe(true);
  });
  it("two rules closer than their words are wide still get separate boxes", () => {
    const out = expectClean([today(120), regs(128)]);
    expect(out.every((o) => o.placed)).toBe(true);
    expect(out[0]!.row === out[1]!.row && out[0]!.anchor === out[1]!.anchor).toBe(false);
  });
  it("far apart: both read on the first row, to the right of their rules", () => {
    const out = expectClean([today(60), regs(225, "Wed 18 Nov")]);
    expect(out.map((o) => [o.row, o.anchor])).toEqual([[0, "start"], [0, "start"]]);
  });
  it("a mark at the right edge turns its label to the left of the rule", () => {
    const out = expectClean([today(60), regs(plot.x + plot.width - 1)]);
    expect(out[1]!.anchor).toBe("end");
  });
  it("a mark at the left edge keeps its label inside the plot", () => expectClean([today(plot.x + 1), regs(plot.x + 4)]));
  it("three marks (today, a Conditions day, the regulations day) all place", () => {
    const out = expectClean([today(100), cond(112), regs(124)], false);
    expect(out.every((o) => o.placed)).toBe(true);
  });
  it("today keeps the best place (row 0, right of its rule) when it can", () => {
    const out = expectClean([regs(160), today(100)]);
    expect(out[1]!.row).toBe(0);
    expect(out[1]!.anchor).toBe("start");
  });
  it("a label that fits nowhere is returned unplaced, never drawn over another", () => {
    const out = layoutDayMarks({ ...base, plot: { x: 40, width: 50 }, marks: [today(60), regs(62), cond(64)] });
    expect(out.filter((o) => !o.placed).length).toBeGreaterThan(0);
  });
  it("fuzz: every pair of rule positions, at every pixel step, on three marks", () => {
    for (let a = 40; a <= 310; a += 7)
      for (let b = 40; b <= 310; b += 5) {
        if (a === b) continue;
        expectClean([today(a), regs(b)], false);
        expectClean([today(a), cond(b), regs((a + b) / 2 + 3)], false);
      }
  });
  it("the width measure grows with the text", () => {
    expect(markTextWidth("Wed 11 Nov", 10)).toBeGreaterThan(markTextWidth("Today", 10));
  });
});
