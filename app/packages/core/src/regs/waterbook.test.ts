import { describe, expect, it } from "vitest";
import type { RegsInputs } from "./inputs";
import { waterBookOf } from "./waterbook";

/** `row`: the rule's entry is a water table's row (display `RuleFacts.row`, T916); every rank-0 rule here is one unless said */
const rule = (baseRank: number, book: [number, number][] | null, missing = false, row = baseRank === 0) =>
  ({ missing, baseRank, facts: book ? { book, ...(row ? { row: true } : {}) } : null });
const inp = (rules: ReturnType<typeof rule>[]) => ({ ruleset: { reach: rules.map((_, i) => i), trib: [] },
  rules: { get: (i: number) => rules[i] } }) as unknown as RegsInputs;

describe("waterBookOf (T474)", () => {
  it("is the pages of the water's own (rank 0) entries, not the region's general pages", () => {
    const line = waterBookOf(inp([rule(3, [[23, 25]]), rule(0, [[21, 23]]), rule(0, [[21, 23]])]));
    expect(line).toEqual([{ t: "text", s: "This water in the rule book" }, { t: "text", s: " " }, { t: "book", printed: 21, pdf: 23, bare: true }]);
  });
  it("a water with no entry of its own: no line, never a guessed page", () => {
    expect(waterBookOf(inp([rule(3, [[23, 25]]), rule(4, [[9, 11]])]))).toEqual([]);
    expect(waterBookOf(inp([rule(0, null, true)]))).toEqual([]);
  });
});

describe("waterBookOf: only the water's own row (T916)", () => {
  // Sweltzer River (owner 2026-11-19, "Has page 7 link at top (which is wrong)"): its own row is p.26; the B.C. White Sturgeon
  // Conservation Licence (printed p.7, the licences page) binds the whole Fraser watershed and sits at rank 0 like a row
  it("a B.C. or zone table's line bound to a water at rank 0 is not this water's page", () => {
    const line = waterBookOf(inp([rule(0, [[26, 28]]), rule(0, [[7, 9]], false, false)]));
    expect(line).toEqual([{ t: "text", s: "This water in the rule book" }, { t: "text", s: " " }, { t: "book", printed: 26, pdf: 28, bare: true }]);
  });
  it("its own row already linked on the page: no line, never the province page instead", () => {
    const page = { keep: [{ line: [{ t: "book", printed: 26, pdf: 28 }] }] };
    expect(waterBookOf(inp([rule(0, [[26, 28]]), rule(0, [[7, 9]], false, false)]), page)).toEqual([]);
  });
});

describe("waterBookOf dedupe (T60)", () => {
  const i = inp([rule(0, [[22, 24]]), rule(0, [[59, 61]])]);
  it("leaves out a page the screen's lines already link, keeps the rest", () => {
    const page = { keep: [{ line: [{ t: "text", s: "x" }, { t: "book", printed: 22, pdf: 24 }] }] };
    expect(waterBookOf(i, page)).toEqual([{ t: "text", s: "This water in the rule book" }, { t: "text", s: " " }, { t: "book", printed: 59, pdf: 61, bare: true }]);
  });
  it("every page already linked: no line (the rule's own link is the one action)", () => {
    const page = [[{ t: "book", printed: 22, pdf: 24 }], { a: { t: "book", printed: 59, pdf: 61 } }];
    expect(waterBookOf(i, page)).toEqual([]);
  });
  it("(T919 b) a page a section's source line links counts as linked (Elaho River: Gear's sources p.23, not again at the foot)", () => {
    const page = { gear: { src: { who: "B.C. rules ›", aria: "", open: "srcs:gear", keys: [], pages: [{ printed: 22, pdf: 24 }] } } };
    expect(waterBookOf(i, page)).toEqual([{ t: "text", s: "This water in the rule book" }, { t: "text", s: " " }, { t: "book", printed: 59, pdf: 61, bare: true }]);
  });
  it("(T919 b) a per-row gear source (not drawn since T900) links nothing", () => {
    const page = { gear: { lines: [{ src: { who: "", aria: "", open: "srcs:gl|hook", keys: [], pages: [{ printed: 22, pdf: 24 }] } }] } };
    expect(waterBookOf(i, page)).toHaveLength(5);   // both pages still on the line
  });
});
