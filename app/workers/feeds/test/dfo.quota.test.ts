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

// T349 (owner 2026-11-02): the page's default sentences, the shared (aggregate) tier, non-retention closed
import { statementLocators } from "../src/dfo/feed";
import { bucket, markShared, sharedScope } from "../src/dfo/quota";

describe("T349 page defaults and shared quotas", () => {
  it("reads Region 2's two default sentences as closed page-default locators", () => {
    const locs = statementLocators("2", [
      "There is no fishing for salmon in waters of Region 2 flowing into Areas 28 and 29 except for the opportunities listed in the table below.",
      "Unless otherwise stated in the table below, the daily limit for waters in Region 2 flowing into Areas 13, 15, and 16 is zero (0).",
    ]);
    expect(locs.map((l) => l.fingerprint)).toEqual(["page:2:areas-28-29", "page:2:areas-13-15-16"]);
    for (const l of locs) expect(l.rules[0]).toMatchObject({ species: ["SA"], take: 0, may_target: false });
  });
  it("shares R5B's count across Management Units 5-6 to 5-11", () => {
    const scope = sharedScope({ specific_area: "All lakes or streams or part thereof in the aggregate (combined total), in Management Units 5-6 to 5-11 in Region 5B" });
    expect(scope).toBe("Management Units 5-6 to 5-11");
    const [r] = markShared([{ rule_id: "a.r0", type: "retention_limit", species: ["CH"], take: 4, verbatim: "4 per day" }], scope);
    const q = speciesTree([r as never], new Map());
    expect(q?.tree).toMatchObject({ tier: "total", whose: scope, n: 4 });
  });
  it("makes the Squamish hatchery coho note an aggregate tier over the watershed", () => {
    const b = bucket([{ id: "d", k: "daily", t: "…", n: 4 },
      { id: "a", k: "aggregate", t: "Squamish watershed: …", n: 1, sp: ["CO"], hatchery: true, w: [{ n: "Squamish River", ws: true, key: "squamish river+ws" }] } as never]);
    expect((b?.shared as { tree: QuotaTier }[])[0]!.tree).toMatchObject({ tier: "total", n: 1, whose: "the Squamish River watershed", origin: "hatchery" });
  });
});
