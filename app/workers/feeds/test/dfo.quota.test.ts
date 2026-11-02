// T332: a species' numbers are a node of the GAME-FISH QUOTA TREE (the answers model's TreeTier), in the Worker as in the
// Python (pipeline/regs/dfo_salmon/quota.py). Held to the words T320 gave every open cell of the tracked DFO pages
// (pipeline/tests/fixtures/dfo_salmon/t320_words.json — recorded before the change, read by the Python test too), so the
// TS port, the Python and the T320 words agree on every saved cell; the full feed is byte-compared in dfo.parity.test.ts.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { Interner, locationCalendar, type Verdict } from "../src/dfo/salmon";
import { speciesTree, walk, words, type QuotaTier } from "../src/dfo/quota";
import { toRules } from "../src/dfo/typed";
import { REPO } from "./dfo.scenario";

const T320 = JSON.parse(readFileSync(join(REPO, "pipeline/tests/fixtures/dfo_salmon/t320_words.json"), "utf-8")) as Record<string, string | null>;
const SPECIES = ["CH", "CO", "SK", "PK", "CM"];

function openVerdict(species: string, sp: string, cell: string): Verdict {
  const rules = toRules({ species, dates: "Apr 1 to Mar 31", limits_gear: cell } as never, "x", 0) as never[];
  const verdicts = new Interner<unknown>();
  const cal = locationCalendar(rules, verdicts, new Interner<unknown>());
  const runs = cal.sp[SPECIES.indexOf(sp)]!;
  for (let k = 1; k < runs.length; k += 2) {
    const v = runs[k]! >= 0 ? (verdicts.items[runs[k]!] as Verdict) : null;
    if (v?.s === "open") return v;
  }
  throw new Error(`${cell}: opens no day`);
}

describe("(T332) DFO quotas in the game-fish quota tree", () => {
  it("every open cell of the tracked pages says T320's words, through the tree", () => {
    const got: Record<string, string | null> = {};
    for (const key of Object.keys(T320)) {
      const [species, sp, cell] = key.split("|") as [string, string, string];
      const v = openVerdict(species, sp, cell);
      got[key] = v.tree ? words(v.tree) : null;
      if (v.tree) for (const x of walk(v.tree)) if (x.node === "tier") expect(x.tier === "size").toBe(x.over_cm !== undefined);
    }
    expect(got).toEqual(T320);
  });

  it("the shapes: hatchery kind folded, the wild kind its own number, a monthly size tier's period", () => {
    const tree = (cell: string) => openVerdict("Coho", "CO", cell).tree as QuotaTier;
    const wild = tree("2 per day, 1 of which can be unmarked — FN1013");
    expect(wild.children.map((k) => [k.node, k.n, "name" in k ? k.name : null, "folded" in k ? k.folded : undefined]))
      .toEqual([["kind", 2, "coho", true], ["kind", 1, "unmarked (wild) coho", undefined]]);
    const mo = tree("4 per day only 1 over 65 cm. Monthly Quota = 1 over 65 cm.");
    expect(mo.children.map((t) => [t.node, (t as QuotaTier).tier, t.n, (t as QuotaTier).over_cm, (t as QuotaTier).period]))
      .toEqual([["tier", "size", 1, 65, undefined], ["tier", "size", 1, 65, "monthly"]]);
    const mx = tree("1 per day, maximum size 35 cm");
    expect(mx.children[0]).toMatchObject({ node: "kind", sizes: { cm: [[0, 35]], in: [[0, 14]] }, folded: true });
  });

  it("two opening rules with different numbers on one day are not reconciled: u, no tree", () => {
    const a = { rule_id: "a", species: ["CO"], take: 4 }, b = { rule_id: "b", species: ["CO"], take: 2 };
    expect(speciesTree([a, b] as never, new Map())).toEqual({ u: 1 });
    expect(speciesTree([a, { ...a, rule_id: "c" }] as never, new Map())?.tree?.n).toBe(4);
  });
});
