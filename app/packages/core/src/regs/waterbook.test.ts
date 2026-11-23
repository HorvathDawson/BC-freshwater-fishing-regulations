import { describe, expect, it } from "vitest";
import type { RegsInputs } from "./inputs";
import { waterPagesOf } from "./waterbook";

/** `row`: the rule's entry is a water table's row (display `RuleFacts.row`, T916); every rank-0 rule here is one unless said */
const rule = (baseRank: number, book: [number, number][] | null, missing = false, row = baseRank === 0, book_box: unknown[] = []) =>
  ({ missing, baseRank, facts: book ? { book, book_box, ...(row ? { row: true } : {}) } : null });
const inp = (rules: ReturnType<typeof rule>[]) => ({ ruleset: { reach: rules.map((_, i) => i), trib: [] },
  rules: { get: (i: number) => rules[i] } }) as unknown as RegsInputs;
const box = (pdf: number, y: number) => ({ pdf, box: [10, y, 200, y + 12] as [number, number, number, number], size: [612, 792] as [number, number] });

describe("waterPagesOf (T474, T991 round 3)", () => {
  it("is the pages of the water's own (rank 0) entries, not the region's general pages, in page order, once each", () => {
    expect(waterPagesOf(inp([rule(3, [[23, 25]]), rule(0, [[59, 61]]), rule(0, [[21, 23]]), rule(0, [[21, 23]])])))
      .toEqual([{ t: "book", printed: 21, pdf: 23 }, { t: "book", printed: 59, pdf: 61 }]);
  });
  it("a water with no entry of its own: none, never a guessed page", () => {
    expect(waterPagesOf(inp([rule(3, [[23, 25]]), rule(4, [[9, 11]])]))).toEqual([]);
    expect(waterPagesOf(inp([rule(0, null, true)]))).toEqual([]);
  });
  // Sweltzer River (owner 2026-11-19, "Has page 7 link at top (which is wrong)"): its own row is p.26; the B.C. White Sturgeon
  // Conservation Licence (printed p.7, the licences page) binds the whole Fraser watershed and sits at rank 0 like a row
  it("(T916) a B.C. or zone table's line bound to a water at rank 0 is not this water's page", () => {
    expect(waterPagesOf(inp([rule(0, [[26, 28]]), rule(0, [[7, 9]], false, false)]))).toEqual([{ t: "book", printed: 26, pdf: 28 }]);
  });
  it("(owner 2026-11-22: \"make sure highlighting is still there\") each page carries its rows' boxes on that PDF page only", () => {
    const line = waterPagesOf(inp([rule(0, [[26, 28], [27, 29]], false, true, [box(28, 100), box(29, 50)]), rule(0, [[26, 28]], false, true, [box(28, 300)])]));
    expect(line).toEqual([{ t: "book", printed: 26, pdf: 28, box: [box(28, 100), box(28, 300)] }, { t: "book", printed: 27, pdf: 29, box: [box(29, 50)] }]);
  });
});
