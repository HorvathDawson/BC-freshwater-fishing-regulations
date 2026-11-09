/**
 * T529: /changes compares WHAT APPLIES WHERE (rule content + metres along the stream), never how a water is cut into parts
 * or what the parts are called. Mutation-style: each "same" case is paired with the one change that must show.
 */
import { describe, expect, it } from "vitest";
import type { RegsStore } from "@app/data/regs";
import { mergeSpans, rulesKey } from "./model";

type Part = { partIx: number; ruleset: number; grp?: number; label?: string };
type Line = { partIx: number; blk: number; m0: number; m1: number };

/** a one-water store: parts (each keyed to a rule set), their stream lines, and rule sets of rule ids with a field value */
function store(parts: Part[], lines: Line[], sets: Record<number, { rule: string; value: number }[]>): RegsStore {
  const rules = new Map<number, unknown>();
  const rix = new Map<string, number>();
  for (const s of Object.values(sets)) for (const r of s) {
    const key = `${r.rule}|${r.value}`;
    if (rix.has(key)) continue;
    const ix = rix.size; rix.set(key, ix);
    rules.set(ix, { rix: ix, facts: {}, export: { entry_id: "e1", rule_id: r.rule, type: "quota", family: "trout", fields: { n: r.value },
                                                   prov: { rank: 1 } } });
  }
  const fake = {
    parts: async () => parts.map((p) => ({ itemOrd: 1, partIx: p.partIx, akey: p.ruleset, facts: { label: p.label ?? "" }, sections: 1,
                                           grp: p.grp ?? p.partIx, run: 0, tidal: false })),
    rules: async () => rules,
    partLines: async () => lines,
    akey: async (a: number) => ({ ruleset: a, kind: "k", tidal: false, steelhead: null, steelheadWater: null, steelheadRules: null }),
    ruleset: async (s: number) => ({ sections: 1, reach: sets[s]!.map((r) => rix.get(`${r.rule}|${r.value}`)!), trib: [] }),
  };
  return fake as unknown as RegsStore;
}

const A = [{ rule: "r1", value: 2 }], B = [{ rule: "r2", value: 0 }];

describe("rulesKey (T529): what applies where, however the water is cut", () => {
  // two rule sets on one stream: A on [0, 5000), B on [5000, 9000)
  const base = store([{ partIx: 0, ruleset: 1 }, { partIx: 1, ruleset: 2 }],
    [{ partIx: 0, blk: 7, m0: 0, m1: 5000 }, { partIx: 1, blk: 7, m0: 5000, m1: 9000 }], { 1: A, 2: B });

  it("a water re-cut into different parts with the same rules is not a change", async () => {
    // A split into two parts at 2000 m (a length cut / connected-run re-cut), reindexed, groups renumbered
    const recut = store([{ partIx: 0, ruleset: 1, grp: 9 }, { partIx: 1, ruleset: 2, grp: 3 }, { partIx: 2, ruleset: 1, grp: 4 }],
      [{ partIx: 0, blk: 7, m0: 0, m1: 2000 }, { partIx: 2, blk: 7, m0: 2000, m1: 5000 }, { partIx: 1, blk: 7, m0: 5000, m1: 9000 }],
      { 1: A, 2: B });
    expect(await rulesKey(recut, 1)).toBe(await rulesKey(base, 1));
    // mutation: the same re-cut, but the second A piece ends 500 m short — where A applies moved
    const short = store([{ partIx: 0, ruleset: 1 }, { partIx: 1, ruleset: 2 }, { partIx: 2, ruleset: 1 }],
      [{ partIx: 0, blk: 7, m0: 0, m1: 2000 }, { partIx: 2, blk: 7, m0: 2000, m1: 4500 }, { partIx: 1, blk: 7, m0: 4500, m1: 9000 }],
      { 1: A, 2: B });
    expect(await rulesKey(short, 1)).not.toBe(await rulesKey(base, 1));
  });

  it("a renamed part with the same rules is not a change", async () => {
    const renamed = store([{ partIx: 0, ruleset: 1, label: "Mouth to Slesse Creek" }, { partIx: 1, ruleset: 2, label: "above the falls" }],
      [{ partIx: 0, blk: 7, m0: 0, m1: 5000 }, { partIx: 1, blk: 7, m0: 5000, m1: 9000 }], { 1: A, 2: B });
    expect(await rulesKey(renamed, 1)).toBe(await rulesKey(base, 1));
  });

  it("a changed rule on one stretch is a change", async () => {
    const changed = store([{ partIx: 0, ruleset: 1 }, { partIx: 1, ruleset: 2 }],
      [{ partIx: 0, blk: 7, m0: 0, m1: 5000 }, { partIx: 1, blk: 7, m0: 5000, m1: 9000 }],
      { 1: A, 2: [{ rule: "r2", value: 1 }] });
    expect(await rulesKey(changed, 1)).not.toBe(await rulesKey(base, 1));
    // and a stretch that moves to the other rule set
    const swapped = store([{ partIx: 0, ruleset: 1 }, { partIx: 1, ruleset: 2 }],
      [{ partIx: 0, blk: 7, m0: 0, m1: 6000 }, { partIx: 1, blk: 7, m0: 6000, m1: 9000 }], { 1: A, 2: B });
    expect(await rulesKey(swapped, 1)).not.toBe(await rulesKey(base, 1));
  });

  it("a one-part water (no stream lines) is its rule content", async () => {
    const one = store([{ partIx: 0, ruleset: 1 }], [], { 1: A });
    expect(await rulesKey(one, 1)).toBe(await rulesKey(store([{ partIx: 0, ruleset: 1, label: "x" }], [], { 1: A }), 1));
    expect(await rulesKey(one, 1)).not.toBe(await rulesKey(store([{ partIx: 0, ruleset: 1 }], [], { 1: [{ rule: "r1", value: 3 }] }), 1));
  });
});

describe("mergeSpans (T529)", () => {
  it("joins touching and overlapping stretches of one stream, never across streams or gaps", () => {
    expect(mergeSpans([[7, 2000, 5000], [7, 0, 2000], [8, 0, 10], [7, 5100, 6000], [7, 5500, 5800]]))
      .toEqual([[7, 0, 5000], [7, 5100, 6000], [8, 0, 10]]);
  });
});
