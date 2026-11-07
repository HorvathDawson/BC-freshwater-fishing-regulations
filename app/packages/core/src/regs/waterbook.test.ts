import { describe, expect, it } from "vitest";
import type { RegsInputs } from "./inputs";
import { waterBookOf } from "./waterbook";

const rule = (baseRank: number, book: [number, number][] | null, missing = false) => ({ missing, baseRank, facts: book ? { book } : null });
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
});
