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
