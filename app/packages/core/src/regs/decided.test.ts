/**
 * decided.ts ("How this was decided"), store-free, over hand-built rules, rows answers and lifts. The 134-state parity is
 * app/conformance/src/regs/plainrow.parity.test.ts (every non-pool plain row's ladder).
 */
import { describe, expect, it } from "vitest";
import type { CardModel, HydRow } from "./card";
import { ladderOf, rowSources, shortLabel, withRestates, type LadderEnv } from "./decided";
import type { ExportRule, RegsInputs, RuleFacts, RuleIndex, RuleView, RowsValue } from "./inputs";
import { plain } from "./line";
import { noRule, ruleView } from "./rules";
import type { Settled } from "./settle";
import { BOOK } from "./testing/speciesBook";

interface R { rank?: number; name?: string; label?: string; plain?: string | null; type?: string; fields?: Record<string, unknown>;
  kind?: RuleFacts["kind"]; closure?: true; subsets?: RuleFacts["subsets"]; book?: [number, number][] }
const rule = (rix: number, o: R = {}): RuleView => {
  const exp: ExportRule = { entry_id: `e${rix}`, rule_id: `r${rix}`, type: o.type ?? "retention_limit", family: "retention",
    label: o.label ?? `Rule ${rix}`, verbatim: o.label ?? `Rule ${rix}`, fields: { species: ["RB"], ...o.fields },
    prov: { rank: o.rank ?? 3, entry_name: o.name } };
  const facts = { kind: o.kind ?? "pool", title: "", plain: o.plain === undefined ? `Sentence ${rix}.` : o.plain, closure: o.closure ?? null,
    subsets: o.subsets ?? null, book: o.book ?? [[13, 15]], applies: "" } as RuleFacts;
  return ruleView(rix, exp, facts, BOOK, "reach");
};
const indexOf = (vs: readonly RuleView[]): RuleIndex => {
  const m = new Map(vs.map((v) => [v.rix, v]));
  return { get: (ix) => m.get(ix) ?? noRule(ix), has: (ix) => m.has(ix), size: m.size };
};
type Roles = [number, string, number | null][];
const env = (rules: RuleView[], fish: Record<string, { H?: Roles; W?: Roles }>, o: { lifted?: [number, number][]; kind?: string; name?: string } = {}) => {
  const wire = Object.fromEntries(Object.entries(fish).map(([S, x]) => [S, { hatchery: x.H ? { roles: x.H } : null, wild: x.W ? { roles: x.W } : null }]));
  const inp = { rules: indexOf(rules), year: { rowsBySeg: [{ fish: wire } as unknown as RowsValue] },
    item: { name: o.name ?? "Elk River", kind: o.kind ?? "stream" }, place: null } as unknown as RegsInputs;
  const card = { ctx: { s: 0 }, R: Object.fromEntries(Object.keys(fish).map((S) => [S, {}])) } as unknown as CardModel;
  const settled = { lifted: new Map(o.lifted ?? []) } as unknown as Settled;
  const e: LadderEnv = { inp, card, settled, seams: { rowTitle: () => "Rainbow trout" } };
  return e;
};
const row = (o: Partial<HydRow> & { restates?: unknown; week?: unknown } = {}): HydRow =>
  ({ kind: "release", pool: null, win: null, members: ["RB"], allMembers: ["RB"], ...o, raw: { restates: o.restates ?? null, week: o.week ?? null } }) as unknown as HydRow;
const keys = (l: readonly { r: RuleView; role: string }[]) => l.map((x) => [x.r.rix, x.role]);
const flat = (l: ReturnType<typeof ladderOf>) => [
  ...l.sections.flatMap((s) => [`Q ${s.q}`, `= ${s.result}`, ...s.steps.flatMap((st) => [`L ${st.level}`, ...st.lines.map((x) => `${plain(x.text)} | ${x.tag} | ${plain(x.book)}`)])]),
  ...l.lost.map((x) => `X ${x.text} | ${x.status}`)];

describe("rowSources: every stored role of the row's fish, once per rule, deciding roles first, then by level", () => {
  it("gathers hatchery then wild over the members; a rule's deciding role wins over its losing one; then rank ascending", () => {
    const a = rule(1, { rank: 3 }), b = rule(2, { rank: 0 }), c = rule(3, { rank: 4 }), d = rule(4, { rank: -1 });
    const e = env([a, b, c, d], { RB: { H: [[1, "replaced", 2], [3, "possession", null]], W: [[1, "governs", null], [2, "governs", null]] },
      CT: { H: [[4, "moot", 2]] } });
    expect(keys(rowSources(row({ allMembers: ["RB", "CT"] }), e.card, e.settled, e.inp))).toEqual([[2, "governs"], [1, "governs"], [3, "possession"], [4, "moot"]]);
  });
  it("adds the day's stored lifts of a rule naming one of the row's fish (role 'lifted', by the lifter); not another fish's", () => {
    const a = rule(1), lifter = rule(2, { rank: 0 }), other = rule(5, { fields: { species: ["KO"] } });
    const e = env([a, lifter, other], { RB: { H: [[2, "governs", null]] } }, { lifted: [[1, 2], [5, 2]] });
    const s = rowSources(row(), e.card, e.settled, e.inp);
    expect(keys(s)).toEqual([[2, "governs"], [1, "lifted"]]);
    expect(s[1]!.by?.rix).toBe(2);
  });
  it("a fish the card does not hold, or no rows frame → nothing", () => {
    const e = env([rule(1)], { RB: { H: [[1, "governs", null]] } });
    expect(rowSources(row({ allMembers: ["KO"] }), e.card, e.settled, e.inp)).toEqual([]);
    expect(rowSources(row(), { ...e.card, ctx: { s: -1 } } as unknown as CardModel, e.settled, e.inp)).toEqual([]);
  });
});

describe("ladderOf, the Keep tab's form", () => {
  it("the four questions in order, the result on the first, levels high → low with the data's names, SAYS tags and book pages", () => {
    const region = rule(1, { rank: 3, name: "Region 4" }), area = rule(2, { rank: 2, name: "Elk River and all its tributaries upstream of Highway 3 crossing" }),
      water = rule(3, { rank: 0, kind: "gate", closure: true }), size = rule(4, { rank: 3, name: "Region 4" }), pos = rule(5, { rank: 3, name: "Region 4", book: [[34, 36]] }),
      rec = rule(6, { rank: 0, fields: { record_retention: true } }), stop = rule(7, { rank: 4, type: "stop_fishing_after_quota" });
    const e = env([region, area, water, size, pos, rec, stop], { RB: { H: [[3, "governs", null], [1, "contains", null], [2, "narrows", null], [4, "floor", null],
      [5, "possession", null], [6, "duty", null], [7, "duty", null]] } });
    expect(flat(ladderOf(row({ kind: "closed" }), e))).toEqual([
      "Q What you can keep", "= Closed here.",
      "L Region 4", "Sentence 1. | Daily limit | Rule book p.13 ↗",
      "L A named area", "Sentence 2. | Lower daily limit | Rule book p.13 ↗",
      "L Elk River", "Sentence 3. | Closed | Rule book p.13 ↗",
      "Q Size", "= null", "L Region 4", "Sentence 4. | Size | Rule book p.13 ↗",
      "Q Other rules", "= null", "L All of B.C.", "Sentence 7. | Stop fishing | Rule book p.13 ↗",
      "L Region 4", "Sentence 5. | Carry limit | Rule book p.34 ↗",
      "L Elk River", "Sentence 6. | Record it | Rule book p.13 ↗",   // the pipeline's role "duty"; record_retention words the tag
    ]);
  });
  it("a record_retention rule keeps its STORED role (owner rule 2026-10-21: the app never re-classifies a role)", () => {
    const rec = rule(6, { rank: 0, fields: { record_retention: true } });
    const e = env([rec], { RB: { H: [[6, "limit", null]] } });
    expect(flat(ladderOf(row(), e))).toEqual(["Q Other limits", "= null", "L Elk River", "Sentence 6. | Record it | Rule book p.13 ↗"]);
  });
  it("a short named-area name is its level; rank 3 with no name is 'The region'; a release gate says Release", () => {
    const a = rule(1, { rank: 2, name: "Fraser River" }), b = rule(2, { rank: 3, kind: "gate" });
    const e = env([a, b], { RB: { H: [[1, "governs", null], [2, "also", null]] } });
    expect(flat(ladderOf(row(), e)).filter((x) => x.startsWith("L ") || x.includes("|"))).toEqual([
      "L The region", "Sentence 2. | Also applies | Rule book p.13 ↗", "L Fraser River", "Sentence 1. | Daily limit | Rule book p.13 ↗"]);
  });
  it("lost: 'Replaced by' the replacing rule's level; agrees, a winner's sentence again, or the same short label are not listed", () => {
    const win = rule(1, { rank: 0 }), region = rule(2, { rank: 3, name: "Region 4" }), bc = rule(3, { rank: 4 }), area = rule(4, { rank: 2 });
    const r5 = rule(5), r6 = rule(6), r7 = rule(7), same = rule(8, { plain: "Sentence 1." }), twin = rule(9, { label: "Rule 2", plain: null }), r10 = rule(10);
    const e = env([win, region, bc, area, r5, r6, r7, same, twin, r10], { RB: { H: [[1, "governs", null], [5, "replaced", 1], [6, "replaced", 2],
      [7, "lifted", 3], [10, "moot", 4], [8, "replaced", 2], [9, "replaced", 2], [4, "agrees", 1], [3, "falls", null]] } }, { kind: "lake" });
    expect(ladderOf(row(), e).lost.map((x) => `${x.text} | ${x.status}`)).toEqual([
      "Sentence 5. | Replaced by this lake’s rule", "Sentence 6. | Replaced by the Region 4 rule", "Sentence 7. | Replaced by a B.C.-wide rule",
      "Sentence 10. | Doesn’t matter today", "Sentence 3. | Not needed here"]);
    const e2 = env([win, r5, area], { RB: { H: [[1, "governs", null], [5, "replaced", 4]] } });
    expect(ladderOf(row(), e2).lost.map((x) => x.status)).toEqual(["Replaced by a rule above"]);
  });
  it("a line for fewer fish than the row says the data's subset sentence (or its 'for …'); none in the data → a missing line", () => {
    const a = rule(1, { subsets: [{ fish: ["RB"], plain: "Rainbow only." } as never] }), b = rule(2, { subsets: [{ fish: ["CT"], for: "for cutthroat", plain: null } as never] }), c = rule(3);
    const e = env([a, b, c], { RB: { H: [[1, "governs", null]] }, CT: { H: [[2, "floor", null], [3, "limit", null]] } });
    expect(flat(ladderOf(row({ allMembers: ["RB", "CT"], members: ["RB", "CT"] }), e)).filter((x) => x.includes("|"))).toEqual([
      "Rainbow only. | Daily limit | Rule book p.13 ↗", "Sentence 2. for cutthroat | Size | Rule book p.13 ↗",
      "Missing from the answers file: the sentence for e3::r3 said for CT (display.rules[].subsets) | null | "]);
  });
  it("no stored role at all: the row's own winning rule governs", () => {
    const w = rule(1, { rank: 0 });
    expect(flat(ladderOf(row({ win: w }), env([w], {})))).toEqual(["Q What you can keep", "= Release every one here.", "L Elk River", "Sentence 1. | Daily limit | Rule book p.13 ↗"]);
  });
});

describe("ladderOf with members (T36's other: sheet) and the restates notes", () => {
  it("one section, the deciding rules for those fish in QORDER (no possession), the given result, nothing lost", () => {
    const a = rule(1), b = rule(2), c = rule(3), d = rule(4);
    const e = env([a, b, c, d], { RB: { H: [[2, "floor", null], [1, "governs", null], [3, "possession", null], [4, "replaced", 1]] } });
    const l = ladderOf(row(), e, ["RB"], "Keep up to 2");
    expect([l.sections.length, l.sections[0]!.q, l.sections[0]!.result, l.lost]).toEqual([1, null, "Keep up to 2", []]);
    expect(flat(l).filter((x) => x.includes("|"))).toEqual(["Sentence 1. | Daily limit | Rule book p.13 ↗", "Sentence 2. | Size | Rule book p.13 ↗"]);
  });
  it("a restating rule's note goes under the first winning line of the rule it restates; a week row keeps its notes", () => {
    const a = rule(1), b = rule(2);
    const e = env([a, b], { RB: { H: [[1, "governs", null]] } });
    const r = row({ restates: [{ r: 2, of: 1, says: "Zone B also prints this." }] });
    expect(withRestates(ladderOf(r, e), r, e.inp).sections[0]!.steps[0]!.lines[0]!.notes).toEqual(["Zone B also prints this."]);
    const wk = row({ restates: [{ r: 2, of: 1, says: "x" }], week: { says: "w" } });
    expect(withRestates(ladderOf(wk, e), wk, e.inp).sections[0]!.steps[0]!.lines[0]!.notes).toBeUndefined();
  });
});

describe("shortLabel (page v36): the label shortened for the ladder", () => {
  it("drops the row title, says 'a day', shortens the size and possession words", () => {
    expect(shortLabel("Rainbow trout — 2 per day (none under 30 cm)", "Rainbow trout")).toBe("2 a day 30 cm+");
    expect(shortLabel("Trout — 5 per day, all species combined (no more than 1 over 50 cm)", "Char")).toBe("Trout: 5 a day: max 1 over 50 cm");
    expect(shortLabel("All game fish — possession quota is 2 daily quotas", "")).toBe("All game fish: hold up to 2 days’ limits");
  });
});
