import { describe, expect, it } from "vitest";
import type { PartModel, WaterModel } from "./model";
import { regionYear, rowWords, yearCardHtml } from "./regionYear";

const part = (year: PartModel["year"]): PartModel =>
  ({ year, marks: [], partIx: 0, label: "x", status: "open", today: "", sections: [] });
const water = (id: string, kind: string, ...parts: PartModel[]): WaterModel =>
  ({ ord: 0, id, name: id, kind, regions: [2], region: 2, focus: "2", status: "open", answer: "", parts, neighbours: [], mid: null, book: [] });

describe("T406 part 2: the region's year bar, folded from the water strips", () => {
  // a stream closed Jan 1 - Jun 30 (181 days), open after; a stream open all year; a lake open all year; a lake with no strip
  const ws = [
    water("a", "stream", part([{ closed: true, days: 181 }, { closed: false, days: 184 }])),
    water("b", "stream", part([{ closed: false, days: 365 }])),
    water("c", "lake", part([{ closed: false, days: 365 }])),
    water("d", "wetland", part(null)),
  ];
  const rows = regionYear(ws);

  it("counts each kind's open and closed part-days per week", () => {
    expect(rows.map((r) => [r.label, r.parts])).toEqual([["Streams", 2], ["Lakes", 1]]);
    expect([rows[0]!.open[0], rows[0]!.closed[0], rows[0]!.open[30], rows[0]!.closed[30]]).toEqual([7, 7, 14, 0]);
    // the last week holds the year's 365th day
    expect(rows[0]!.open[51]! + rows[0]!.closed[51]!).toBe(16);
  });

  it("says each row in whole words", () => {
    expect(rows.map(rowWords)).toEqual([
      "Streams (2): 100% open at most; fewest open in the week of Jan 1, 50%.",
      "Lakes (1): every one open all year.",
    ]);
  });

  it("refuses a strip that does not cover the book year", () => {
    expect(() => regionYear([water("e", "stream", part([{ closed: false, days: 300 }]))])).toThrow(/300 days/);
  });

  it("draws nothing for a region with no strip, and a named, labelled card otherwise", () => {
    expect(yearCardHtml("Region 2", [])).toBe("");
    const html = yearCardHtml("Region 2", rows);
    expect(html).toMatch(/^<section class="card yb" aria-labelledby="yb-h"><h2 id="yb-h">When waters are open in Region 2<\/h2><svg viewBox="0 0 364 102" role="img" aria-label="Streams \(2\): 100% open/);
    expect((html.match(/<rect class="o"/g) ?? []).length).toBe(104);
  });
});
