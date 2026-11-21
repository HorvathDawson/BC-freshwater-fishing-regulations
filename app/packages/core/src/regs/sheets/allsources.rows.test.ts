/** (T990) the every-source sheet gathers the trout table's rules and the card rows' deciding rules: one row for a rule in both. */
import { describe, expect, it, vi } from "vitest";

const rule = (key: string) => ({ key, missing: false, f: {}, export: { verbatim: `words ${key}` }, facts: { title: key, book: [[40, 42]] } });
const R: Record<string, ReturnType<typeof rule>> = { "a::1": rule("a::1"), "d::1": rule("d::1"), "e::1": rule("e::1") };

vi.mock("../decided", () => ({
  WINS: ["governs", "adds"],
  // two card rows: the first decided by a::1 (also in the trout table) and lost to by e::1; the second by d::1
  rowSources: (row: { n: number }) => (row.n === 0
    ? [{ r: R["a::1"], role: "governs" }, { r: R["e::1"], role: "replaced" }] : [{ r: R["d::1"], role: "adds" }]),
}));

import { allSourcesSheet } from "./allsources";
import type { SheetCtx } from "../model";

describe("the every-source sheet's trout and card-row gathering (T990)", () => {
  it("a rule in both a card row and the trout table is listed once; an overruled rule is not a source", () => {
    const byIx: Record<number, unknown> = { 0: R["a::1"] };
    const ctx = {
      inp: { rules: { byKey: () => null, get: (n: number) => byIx[n] } },
      page: { gear: null, methods: null, keep: { trout: { kind: "table", src: [0] } }, header: { name: "Water" }, day: { month: 7, day: 15 } },
      today: { month: 7, day: 15 }, env: { card: { rows: [{ n: 0 }, { n: 1 }] }, settled: {} },
    } as unknown as SheetCtx;
    const g = (allSourcesSheet(ctx).blocks[0] as unknown as { groups: { rows: { key: string }[] }[] }).groups;
    expect(g.flatMap((x) => x.rows.map((r) => r.key))).toEqual(["a::1", "d::1"]);
  });
});
