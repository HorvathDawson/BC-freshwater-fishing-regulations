// T332: a species' numbers are a node of the GAME-FISH QUOTA TREE (the answers model's TreeTier), in the Worker as in the
// Python (pipeline/regs/dfo_salmon/quota.py). Held to the words T320 gave every open cell of the tracked DFO pages
// (pipeline/tests/fixtures/dfo_salmon/t320_words.json — recorded before the change, read by the Python test too), so the
// TS port, the Python and the T320 words agree on every saved cell; (T431) the full-feed byte comparison was retired with T350 (cascade resolved in Python only).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { Interner, locationCalendar, type Verdict } from "../src/dfo/salmon";
import { bucket, headerAnswers, headerClosure, sharedBuckets, sizes, speciesTree, walk, words, type QuotaTier } from "../src/dfo/quota";
import { parseHeader } from "../src/dfo/header";
import { toRules } from "../src/dfo/typed";
import { REPO } from "./dfo.scenario";
import { REG } from "./wording.fixture";

const T320 = JSON.parse(readFileSync(join(REPO, "pipeline/tests/fixtures/dfo_salmon/t320_words.json"), "utf-8")) as Record<string, string | null>;
const SPECIES = ["CH", "CO", "SK", "PK", "CM"];

function openVerdict(species: string, sp: string, cell: string): Verdict {
  const rules = toRules({ species, dates: "Apr 1 to Mar 31", limits_gear: cell } as never, "x", 0, REG) as never[];
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

describe("(T349 / T350) non-retention is closed; release only where DFO says catch and release", () => {
  const verdictOf = (cell: string): string | undefined => {
    const rules = toRules({ species: "Coho", dates: "Apr 1 to Mar 31", limits_gear: cell } as never, "x", 0, REG) as never[];
    const verdicts = new Interner<unknown>();
    const runs = locationCalendar(rules, verdicts, new Interner<unknown>()).sp[SPECIES.indexOf("CO")]!;
    return runs[1]! >= 0 ? (verdicts.items[runs[1]!] as Verdict).s : undefined;
  };
  it("plain non-retention answers closed", () => expect(verdictOf("Non-retention")).toBe("closed"));
  it("catch and release in DFO's words answers release", () =>
    expect(verdictOf("Open for salmon catch and release")).toBe("release"));
});

// (T356, T358) the header notes (lengths as LengthBands) and what they resolve to — bucket, the region's `shared` and
// `sizes` — are the Python's (pipeline/tests/fixtures/dfo_salmon/t356_header.json, written and checked by
// pipeline/tests/test_dfo_header.py) on every fixture page; the closure rule carries its note id
describe("(T356) the header's answers, as the Python resolves them", () => {
  const FIX = JSON.parse(readFileSync(join(REPO, "pipeline/tests/fixtures/dfo_salmon/t356_header.json"), "utf-8")) as
    Record<string, { notes: never[]; bucket?: unknown; shared: unknown[]; sizes: unknown[] }>;
  for (const [slug, want] of Object.entries(FIX)) it(`region ${slug}`, () => {
    const notes = parseHeader(readFileSync(join(REPO, `pipeline/tests/fixtures/dfo_salmon/region${slug}-eng.html`), "utf-8"), slug);
    // (T343) `headerAnswers` gives the notes back with each folded note marked, as the Python does
    expect(JSON.parse(JSON.stringify(headerAnswers(notes)))).toEqual(want);
    expect([bucket(notes) ?? undefined, sharedBuckets(notes), sizes(notes)]).toEqual([want.bucket, want.shared, want.sizes]);
    const closed = (notes as { k: string; id?: string }[]).find((n) => n.k === "closed");
    if (closed) expect(headerClosure(notes, slug)![0]).toMatchObject({ header_note: closed.id });
  });
});
