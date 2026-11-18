/**
 * standing.ts and book.ts, store-free, over hand-built inputs (real rule rows from testing/ruleRows.ts where a rule's words
 * matter). The 134-state parity against the golden is app/conformance/src/regs/standing.parity.test.ts (retired, T439).
 */
import { describe, expect, it } from "vitest";
import type { DisplayFrame, ExportRule, RegsInputs, RuleFacts, RuleIndex, RuleView, RowsValue } from "./inputs";
import { plain } from "./line";
import { ruleView } from "./rules";
import type { Settled } from "./settle";
import { bookLine, ruleBook, withBook } from "./book";
import { inheritedOf, possessionOf, whereNotOf, whereWords } from "./standing";
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
  it("drawn on an open day; never on a closed day (owner 2026-11-04), with no status (no key) or on a tidal day; none → null", () => {
    expect(whereNotOf(settled({ standing: [fishway], status: { status: "own" } as unknown as DisplayFrame }))).not.toBeNull();
    expect(whereNotOf(settled({ standing: [fishway], status: { status: "closed", closing: [], strip: 1 } as unknown as DisplayFrame }))).toBeNull();
    expect(whereNotOf(settled({ standing: [fishway], status: null }))).toBeNull();
    expect(whereNotOf(settled({ standing: [fishway], status: { status: "tidal" } as unknown as DisplayFrame }))).toBeNull();
    expect(whereNotOf(settled({ standing: [elsewhere] }))).toBeNull();
  });
});

describe("(T810 S) possession: the rows frame's page_possession (F0, contract 2.11), drawn as Python chose it", () => {
  const all = view(3079, { species: [] }, { plain: "Possession: 2 days’ quota. Fish at home don’t count." });
  const lt = view(2281, { per_daily: 3 }, { plain: "Lake trout: 3 days’ quota." });
  const cap = view(4001, { species: ["WP"] }, { plain: "Have no more than 3 walleye in possession. Fish at home don’t count." }, 2281);
  const R = indexOf([all, lt, cap]);
  type PP = RowsValue["page_possession"];
  const inp = (pp: PP, seg = 0) => ({ rules: R, year: { rowsBySeg: Object.assign([], { [seg]: { page_possession: pp } }) } }) as unknown as RegsInputs;
  const LEAD = { says: all.facts!.plain!, value: "2 days’ quota", src: all.rix };
  it("the row: label, the lead's value words, every sentence (lead first), the lines kept under it, the lead's rule", () => {
    const pp: PP = { lead: LEAD, also: [{ says: lt.facts!.plain!, src: lt.rix }, { says: cap.facts!.plain!, src: cap.rix }] };
    expect(possessionOf(inp(pp), settled({}))).toEqual({ label: "Possession", value: "2 days’ quota",
      says: "Possession: 2 days’ quota. Fish at home don’t count. Lake trout: 3 days’ quota. Have no more than 3 walleye in possession. Fish at home don’t count.",
      also: ["Lake trout: 3 days’ quota.", "Have no more than 3 walleye in possession. Fish at home don’t count."],
      rule: all.key, term: "possession_quota", open: "pos:pos" });
  });
  it("(T670 K1) a water's own limits alone (no lead): no value word, the lines say it, the first line's rule opens", () => {
    const pp: PP = { lead: null, also: [{ says: cap.facts!.plain!, src: cap.rix }] };
    expect(possessionOf(inp(pp), settled({}))).toEqual({ label: "Possession", value: "", says: cap.facts!.plain!,
      also: [cap.facts!.plain!], rule: cap.key, term: "possession_quota", open: "pos:pos" });
  });
  it("the value words are the payload's, never cut from the sentence (the old regex is gone)", () => {
    const pp: PP = { lead: { says: "Possession: twice the daily quota (fish at home don’t count).", value: "twice the daily quota", src: all.rix }, also: [] };
    expect(possessionOf(inp(pp), settled({}))!.value).toBe("twice the daily quota");
  });
  it("the card's own segment's frame; null where it says nothing, or on a closed, tidal or unknown day", () => {
    const pp: PP = { lead: LEAD, also: [] };
    expect(possessionOf(inp(pp, 1), settled({ s: 1 }))!.value).toBe("2 days’ quota");
    expect(possessionOf(inp(pp, 1), settled({ s: 0 }))).toBeNull();
    expect(possessionOf(inp(null), settled({}))).toBeNull();
    expect(possessionOf(inp(pp), settled({ s: -1 }))).toBeNull();
    for (const status of [null, { status: "closed" }, { status: "tidal" }])
      expect(possessionOf(inp(pp), settled({ status: status as unknown as DisplayFrame }))).toBeNull();
  });
});
