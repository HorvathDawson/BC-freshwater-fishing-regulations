/**
 * standing.ts and book.ts, store-free, over hand-built inputs (real rule rows from testing/ruleRows.ts where a rule's words
 * matter). The 134-state parity against the golden is app/conformance/src/regs/standing.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { CardModel } from "./card";
import type { DisplayFrame, ExportRule, RegsInputs, RuleFacts, RuleIndex, RuleView, RowsValue } from "./inputs";
import { plain } from "./line";
import { ruleView } from "./rules";
import type { Settled } from "./settle";
import { bookLine, ruleBook, withBook } from "./book";
import { inheritedOf, possessionOf, possessionRules, possessionValue, whereNotOf, whereWords } from "./standing";
import { BOOK } from "./testing/speciesBook";
import { RULE_ROWS } from "./testing/ruleRows";

/** A real rule row with fields / facts changed. */
const view = (rix: number, fields: Record<string, unknown> = {}, facts: Partial<RuleFacts> = {}, base = rix): RuleView => {
  const [e, f] = RULE_ROWS[base]!;
  const exp: ExportRule = { ...e, rule_id: base === rix ? e.rule_id : `${e.rule_id}#${rix}`, fields: { ...e.fields, ...fields } };
  return ruleView(rix, exp, f && { ...f, ...facts }, BOOK, "reach");
};
const indexOf = (views: readonly RuleView[]): RuleIndex => {
  const m = new Map(views.map((v) => [v.rix, v]));
  return { get: (rix) => m.get(rix)!, has: (rix) => m.has(rix), size: m.size };
};
const OPEN = { status: "own", closing: [], strip: 0 } as unknown as DisplayFrame;
const settled = (o: Partial<Settled>): Settled => ({ s: 0, status: OPEN, standing: [], ...o }) as unknown as Settled;

describe("book.ts: a rule's Rule book pages as runs", () => {
  it("one link per page, ONE space between two links; a sentence then its links as separate elements", () => {
    expect(bookLine([[9, 11], [13, 15]])).toEqual([{ t: "book", printed: 9, pdf: 11 }, { t: "text", s: " " }, { t: "book", printed: 13, pdf: 15 }]);
    expect(plain(withBook([{ t: "text", s: "No fishing" }], bookLine([[9, 11], [13, 15]])))).toBe("No fishing Rule book p.9 ↗ Rule book p.13 ↗");
    expect(withBook([{ t: "text", s: "x" }], [])).toEqual([{ t: "text", s: "x" }]);
    expect(ruleBook({ facts: null } as unknown as RuleView)).toEqual([]);
  });
});

describe("inheritedOf: the display frame's lifts, each with the lifting rule's book and the caution", () => {
  const by = view(3351, {}, { book: [[36, 38]] });
  const inp = (inherited: unknown, display: Partial<DisplayFrame> = {}) =>
    ({ frames: { display: { ...OPEN, inherited, ...display } }, rules: indexOf([by]) }) as unknown as RegsInputs;
  it("label, the data's sentence + ' ' + Rule book p.N, the caution or null", () => {
    const vm = inheritedOf(inp([{ says: "Open Apr 1–Jun 14: an exemption.", by: 3351, lifts: [1], caution: "Nearer row is silent." },
      { says: "Second.", by: 3351, lifts: [2] }]));
    expect(vm && { label: vm.label, lines: vm.lines.map((l) => [plain(l.says), l.caution]) }).toEqual({ label: "By another water’s rule",
      lines: [["Open Apr 1–Jun 14: an exemption. Rule book p.36 ↗", "Nearer row is silent."], ["Second. Rule book p.36 ↗", null]] });
  });
  it("none, an empty list, a tidal frame or no frames → null", () => {
    expect(inheritedOf(inp(null))).toBeNull();
    expect(inheritedOf(inp([]))).toBeNull();
    expect(inheritedOf(inp([{ says: "x", by: 3351, lifts: [] }], { status: "tidal" } as Partial<DisplayFrame>))).toBeNull();
    expect(inheritedOf({ frames: null } as unknown as RegsInputs)).toBeNull();
  });
});

describe("whereNotOf: the day's standing rules with a `where` heading", () => {
  const w = "Where you can’t fish";
  const fishway = view(3351, {}, { where: w, plain: "No fishing within 23 m (75 ft) of a fishway.", book: [[9, 11]] });
  const bare = view(9001, {}, { where: w, plain: null, book: [[9, 11]] }, 3351);
  const elsewhere = view(9002, {}, { where: null }, 3351);
  it("the data's sentence with 'No fishing: ' and no stop; no sentence → the verbatim lower-cased", () => {
    expect(whereWords(fishway)).toBe("No fishing: within 23 m (75 ft) of a fishway");
    expect(whereWords(bare)).toBe("No fishing: within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap");
  });
  it("heading, scope null for the plain heading, each item with its book and rule key; rules with no `where` are not here", () => {
    const vm = whereNotOf(settled({ standing: [fishway, elsewhere, bare] }))!;
    expect([vm.heading, vm.scope, ...vm.items.map((i) => [plain(i.text), i.rule])]).toEqual([w, null,
      ["No fishing: within 23 m (75 ft) of a fishway Rule book p.9 ↗", fishway.key],
      ["No fishing: within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap Rule book p.9 ↗", bare.key]]);
  });
  it("only the FIRST heading's group (the prototype's first .caveats block); another heading is the scope", () => {
    const near = view(9003, {}, { where: "Near the dam", plain: "No fishing near the dam.", book: [] }, 3351);
    const vm = whereNotOf(settled({ standing: [near, fishway] }))!;
    expect([vm.heading, vm.scope, vm.items.map((i) => plain(i.text))]).toEqual([w, "Near the dam", ["No fishing: near the dam"]]);
  });
  it("drawn on a closed day too; never with no status (no key) or on a tidal day; none → null", () => {
    expect(whereNotOf(settled({ standing: [fishway], status: { status: "closed", closing: [], strip: 1 } as unknown as DisplayFrame }))).not.toBeNull();
    expect(whereNotOf(settled({ standing: [fishway], status: null }))).toBeNull();
    expect(whereNotOf(settled({ standing: [fishway], status: { status: "tidal" } as unknown as DisplayFrame }))).toBeNull();
    expect(whereNotOf(settled({ standing: [elsewhere] }))).toBeNull();
  });
});

describe("possession: the rules the stored answers give the role 'possession', and the row", () => {
  const all = view(3079, { species: [] }, { plain: "Possession: 2 days’ quota. Fish at home don’t count." });
  const lt = view(2281, { per_daily: 3 }, { plain: "Lake trout: 3 days’ quota." });
  const same = view(9010, { species: ["RB"], per_daily: 2 }, { plain: "Rainbow trout: 2 days’ quota." }, 3079);
  const R = indexOf([all, lt, same]);
  const dec = (roles: [number, string, number | null][]) => ({ roles });
  const card = (fish: RowsValue["fish"], s = 0) => ({
    inp: { rules: R, year: { rowsBySeg: [{ fish } as unknown as RowsValue] } } as unknown as RegsInputs,
    card: { ctx: { s }, R: Object.fromEntries(Object.keys(fish).map((k) => [k, {}])) } as unknown as CardModel,
  });
  it("collects the role over every fish, hatchery then wild, once per rule; the one naming no fish leads; same days dropped", () => {
    const { inp, card: c } = card({
      LT: { hatchery: dec([[2281, "possession", null], [5, "governs", null]]), wild: dec([[3079, "possession", null]]) },
      RB: { hatchery: dec([[9010, "possession", null], [3079, "possession", null]]), wild: null },
    } as unknown as RowsValue["fish"]);
    expect(possessionRules(c, inp).map((r) => r.rix)).toEqual([3079, 2281]);   // 9010 says 2 days, as the lead does
  });
  it("no rule naming no fish: the most days leads", () => {
    const { inp, card: c } = card({ RB: { hatchery: dec([[9010, "possession", null], [2281, "possession", null]]), wild: null } } as unknown as RowsValue["fish"]);
    expect(possessionRules(c, inp).map((r) => r.rix)).toEqual([2281, 9010]);
  });
  it("the value words: both prototype patterns over the data's sentence; anything else → null", () => {
    expect(possessionValue("Possession: 2 days’ quota. Fish at home don’t count.")).toBe("2 days’ quota");
    expect(possessionValue("Possession: twice the daily quota (fish at home don’t count).")).toBe("twice the daily quota");
    expect(possessionValue("Possession: 2 days’ quota")).toBe("2 days’ quota");
    expect(possessionValue("Have no more than 2 lake trout in possession.")).toBeNull();
  });
  it("the row: label, value, the whole sentence, the lead rule, its term and sheet — only on an open, keyed, non-tidal day", () => {
    const { inp, card: c } = card({ RB: { hatchery: dec([[3079, "possession", null], [2281, "possession", null]]), wild: null } } as unknown as RowsValue["fish"]);
    expect(possessionOf(c, inp, settled({}))).toEqual({ label: "Possession", value: "2 days’ quota",
      says: "Possession: 2 days’ quota. Fish at home don’t count. Lake trout: 3 days’ quota.", rule: all.key, term: "possession_quota", open: "pos:pos" });
    for (const status of [null, { status: "closed" }, { status: "tidal" }])
      expect(possessionOf(c, inp, settled({ status: status as unknown as DisplayFrame }))).toBeNull();
  });
  it("a rule with no sentence, or no rule with the role → null", () => {
    const bare = view(9011, { species: [] }, { plain: null }, 3079);
    const { inp, card: c } = card({ RB: { hatchery: dec([[9011, "possession", null]]), wild: null } } as unknown as RowsValue["fish"]);
    (inp.rules as { get: (n: number) => RuleView }).get = (n) => (n === 9011 ? bare : R.get(n));
    expect(possessionOf(c, inp, settled({}))).toBeNull();
    const none = card({ RB: { hatchery: dec([[3079, "governs", null]]), wild: null } } as unknown as RowsValue["fish"]);
    expect(possessionOf(none.card, none.inp, settled({}))).toBeNull();
  });
});
