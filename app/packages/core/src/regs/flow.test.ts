/**
 * (T883, proposal) flow.ts: every fish sheet's "How this was decided" — the steps in level order with a replaced rule in
 * its place, the answer whole, the other rules, the folded ones. Built on stub rules (rank, provenance, kind), so each
 * case below is the shape of a real row, not a release's data: Cultus Lake's kokanee (Region 2's 5 a day replaced by the
 * lake's release; B.C.'s possession replaced by Region 2's) and a trout payload's plain list.
 */
import { describe, expect, it } from "vitest";
import type { LadderItem } from "./decided";
import { downLine, flowOf, partStep, restateStep, withNotes, withWeek } from "./flow";
import type { RuleIndex, RuleView } from "./inputs";
import type { TroutRowVM, TroutVM } from "./model";
import type { FishAnswer } from "../answers.generated";
import { troutFlow, troutFrom } from "./sheets/trout";
import { plain, text } from "./line";

const rule = (key: string, rank: number, entry: string, region: string | null, says: string, page: number): RuleView => ({
  key, rank, baseRank: rank, kind: "limit", missing: false, part: null, via: null, f: {},
  export: { label: says, verbatim: says, prov: { entry_name: entry, region } }, facts: { plain: says, book: [[page, page + 2]] },
}) as unknown as RuleView;

const BC_POS = rule("bcpos", 4, "", null, "Possession: 2 days’ quota.", 9);
const R2_KO = rule("r2ko", 3, "Region 2", "2", "Keep up to 5 kokanee a day.", 21);
const R2_POS = rule("r2pos", 3, "Region 2", "2", "Possession: 2 days’ quota. Fish at home don’t count.", 21);
const LAKE = rule("lake", 0, "Cultus Lake", "2", "Release every kokanee.", 23);
const item = (r: RuleView, role: string, by: RuleView | null = null): LadderItem => ({ r, role, by, spp: new Set() });
const says = (it: LadderItem) => [text(it.r.facts!.plain!)];
const shape = (f: ReturnType<typeof flowOf>) => ({
  steps: f.steps.map((s) => [s.kicker, plain(s.says), s.tag, s.tone, plain(s.book)]),
  result: f.result, other: f.other.map((s) => [s.kicker, plain(s.says), s.tag]), quiet: f.quiet.map((s) => [s.kicker, s.tag]),
  heads: [f.otherHead, f.quietHead],
});

describe("flowOf", () => {
  it("Cultus Lake kokanee: Region 2's limit, replaced in its place; the lake's release; the answer; possession aside", () => {
    // rowSources order: deciding roles first — the flow re-reads them by level
    const items = [item(LAKE, "governs"), item(R2_POS, "possession"), item(R2_KO, "replaced", LAKE), item(BC_POS, "replaced", R2_POS)];
    expect(shape(flowOf(items, says, "Cultus Lake", "Release every one here."))).toEqual({
      steps: [
        ["Region 2 · base rule", "Keep up to 5 kokanee a day.", "Replaced by Cultus Lake", "lost", "p.21 ↗"],
        ["Cultus Lake · this water’s rule", "Release every kokanee.", null, "won", "p.23 ↗"],
      ],
      result: "So: Release every one here.",
      other: [["Region 2 · base rule", "Possession: 2 days’ quota. Fish at home don’t count.", null]],
      quiet: [["All of B.C.", "Replaced by Region 2"]],
      heads: ["Other rules", "Rules that don’t apply here (1)"],
    });
  });

  it("a step under the same kicker as the one before draws only its page; a lost rule repeating a won one is dropped", () => {
    const R2_B = rule("r2b", 3, "Region 2", "2", "No bait.", 22), R2_DUP = rule("r2dup", 3, "Region 2", "2", "Release every kokanee.", 21);
    const f = flowOf([item(R2_KO, "governs"), item(R2_B, "also"), item(R2_DUP, "replaced", LAKE)], says, "Cultus Lake", null);
    expect(f.steps.map((s) => [s.kicker, s.repeat ?? false, plain(s.says), s.tag])).toEqual([
      ["Region 2 · base rule", false, "Release every kokanee.", "Replaced by Cultus Lake"],
      ["Region 2 · base rule", true, "Keep up to 5 kokanee a day.", null], ["Region 2 · base rule", true, "No bait.", "Also applies"],
    ]);
    // a lost rule saying what a deciding one says is not drawn
    const same = flowOf([item(R2_KO, "governs"), item(rule("r2same", 3, "Region 2", "2", "Keep up to 5 kokanee a day.", 21), "replaced", LAKE)], says, "Cultus Lake", null);
    expect(same.steps.map((s) => s.tone)).toEqual(["won"]);
    expect(f.result).toBeNull();
  });

  it("withWeek: the day-parts stand where the rules they cite stood, the viewed day's tagged; the answer is the week's status", () => {
    const WK = rule("wk", 0, "Cultus Lake", "2", "Keep 5 on weekends.", 23);
    const f = flowOf([item(R2_KO, "replaced", WK), item(WK, "governs"), item(R2_POS, "possession")], says, "Cultus Lake", "Keep 5 a day here.");
    const parts = [partStep(WK, [text("Sat–Sun: "), text("Keep up to 5")], "Cultus Lake", "Today"), partStep(WK, [text("Mon–Fri: "), text("Release every one")], "Cultus Lake", null)];
    const w = withWeek(f, parts, "Weekends: keep 5 · Weekdays: release");
    expect(w.steps.map((s) => [s.kicker, s.repeat ?? false, plain(s.says), s.tag, s.today ?? false])).toEqual([
      ["Region 2 · base rule", false, "Keep up to 5 kokanee a day.", "Replaced by Cultus Lake", false],
      ["Cultus Lake · this water’s rule", false, "Sat–Sun: Keep up to 5", "Today", true],
      ["Cultus Lake · this water’s rule", true, "Mon–Fri: Release every one", null, false],
    ]);
    expect([w.result, w.other.length]).toEqual(["So: Weekends: keep 5 · Weekdays: release", 1]);
  });

  it("withNotes: a restates note under the first step of the rule it restates, once", () => {
    const f = withNotes(flowOf([item(LAKE, "governs")], says, "Cultus Lake", null), new Map([["lake", ["Counts toward the 5 for both arms."]]]));
    expect(f.steps.map((s) => s.notes)).toEqual([["Counts toward the 5 for both arms."]]);
  });

  // ── the independent review of c38c96e35, one test per defect ──
  const UPPER = rule("upper", 0, "Kootenay Lake — Upper West Arm", "4", "No more than 5 from both arms.", 37);
  const LOWER = rule("lower", 0, "Kootenay Lake — Lower West Arm", "4", "Keep up to 5.", 37);
  it("review 2: a step is named against the SHEET's water — another water's own rule is a listed water, never 'this water'", () => {
    expect(partStep(UPPER, [text("x")], "Kootenay Lake — Lower West Arm", null).kicker).toBe("Kootenay Lake — Upper West Arm · listed water");
    expect(restateStep(UPPER, LOWER, [text("x")], "Kootenay Lake — Lower West Arm").kicker).toBe("Kootenay Lake — Upper West Arm · listed water");
    expect(partStep(LOWER, [text("x")], "Kootenay Lake — Lower West Arm", null).kicker).toBe("Kootenay Lake — Lower West Arm · this water’s rule");
  });
  it("review 2 / owner ~23:40: a rule that comes down from another water's row to its tributaries is named that way", () => {
    const TRIB = { ...rule("chill", 1, "Chilliwack River", "2", "No fishing Jun 1–Jun 30.", 22), via: "trib", baseRank: 0 } as unknown as RuleView;
    const f = flowOf([item(TRIB, "governs")], says, "Foley Creek", null);
    expect(f.steps.map((s) => s.kicker)).toEqual(["Chilliwack River · its tributaries"]);
    expect(partStep(TRIB, [text("x")], "Foley Creek", null).kicker).toBe("Chilliwack River · its tributaries");
  });
  it("review 4: within one level the replaced rule reads first and the deciding one last, just above the answer", () => {
    const OLD = rule("old", 0, "Cultus Lake", "2", "Keep up to 2 kokanee.", 23);
    const f = flowOf([item(LAKE, "governs"), item(OLD, "replaced", LAKE)], says, "Cultus Lake", "Release every one here.");
    expect(f.steps.map((s) => [plain(s.says), s.tone])).toEqual([["Keep up to 2 kokanee.", "lost"], ["Release every kokanee.", "won"]]);
  });
  it("review 5: two deciding rules with the same words are drawn once", () => {
    const TWIN = rule("twin", 3, "Region 2", "2", "Keep up to 5 kokanee a day.", 21);
    const f = flowOf([item(R2_KO, "governs"), item(TWIN, "governs")], says, "Cultus Lake", null);
    expect(f.steps.map((s) => plain(s.says))).toEqual(["Keep up to 5 kokanee a day."]);
  });
  it("review 6: a restatement is its own quiet step, tagged with the rule it restates, never a deciding dot", () => {
    const s = restateStep(UPPER, LOWER, [text("No more than 5 from both arms.")], "Kootenay Lake — Lower West Arm");
    expect([s.tone, s.tag]).toEqual(["note", "Restates Kootenay Lake — Lower West Arm’s rule"]);
    const w = withWeek(flowOf([item(LOWER, "governs")], says, "Kootenay Lake — Lower West Arm", null),
      [partStep(LOWER, [text("Sat–Sun: Keep up to 5")], "Kootenay Lake — Lower West Arm", null), s], null);
    expect(w.steps.map((x) => x.tone)).toEqual(["note", "won"]);   // the deciding part last, above the answer
  });
});

describe("troutFlow (T883: a trout row's rail from its whole list and the fish's stored roles)", () => {
  const HK = rule("hk", 3, "Region 2", "2", "Keep up to 4 trout and char a day", 21);
  const H2 = rule("h2", 3, "Region 2", "2", "At most 2 hatchery trout and char a day from streams", 21);
  const WR = rule("wr", 3, "Region 2", "2", "Release wild trout and char from streams", 21);
  const RS = new Map([[1, HK], [2, H2], [3, WR], [4, R2_POS]]);
  const rules = { get: (ix: number) => RS.get(ix)! } as unknown as Pick<RuleIndex, "get">;
  const row = (src: number[]) => ({ src, origin: null, sheet: { head: "Keep · counts toward the 2" } }) as unknown as TroutRowVM;
  const ans = (lead: "hatchery" | "wild" | null, h: [number, string, number | null][], w: [number, string, number | null][]) =>
    ({ lead, hatchery: { roles: h }, wild: { roles: w } }) as unknown as FishAnswer;
  const a = ans("hatchery", [[1, "governs", null], [2, "narrows", null], [4, "possession", null]], [[3, "governs", null], [1, "moot", 3], [4, "possession", null]]);
  it("every rule of row.src with its role; a rule only one origin holds, or holds differently, says whose; the answer is the head", () => {
    const f = troutFlow(row([1, 3, 4, 2]), a, rules, "Mamquam River")!;
    expect(f.result).toBe("So: Keep · counts toward the 2");
    expect(f.steps.map((s) => [plain(s.says), s.tag])).toEqual([["Keep up to 4 trout and char a day", "Hatchery ones"],
      ["Release wild trout and char from streams", "Wild ones"], ["At most 2 hatchery trout and char a day from streams", "Hatchery ones: Lowers the number"]]);
    expect(f.other.map((s) => [s.rule, s.tag])).toEqual([["r2pos", null]]);   // the same role for both: said once, no origin
  });
  it("re-review a: a rule the two origins give different roles is never drawn with one origin's role for both", () => {
    const CAP = rule("cap", 3, "Region 4", "4", "At most 2 trout over 50 cm a day", 34), ONE = rule("one", 3, "Region 4", "4", "At most 1 trout over 50 cm a day", 34);
    const R = new Map([[1, HK], [6, CAP], [7, ONE]]);
    const f = troutFlow({ src: [1, 6, 7], origin: null, sheet: { head: "Keep" } } as unknown as TroutRowVM,
      ans("hatchery", [[1, "governs", null], [6, "replaced", 7], [7, "limit", null]], [[1, "governs", null], [6, "limit", null]]),
      { get: (ix: number) => R.get(ix)! } as never, "Lois River")!;
    const capSteps = [...f.steps, ...f.quiet].filter((s) => s.rule === "cap");
    expect(capSteps.length).toBeGreaterThan(0);
    expect(capSteps.every((s) => /^(Hatchery|Wild) ones/.test(s.tag ?? ""))).toBe(true);
    expect(capSteps.map((s) => s.tag)).toContain("Wild ones: A cap inside the total");
  });
  it("re-review b: a head naming a number no step carries keeps the plain list", () => {
    const head = (h: string) => ({ src: [1, 2, 4], origin: null, sheet: { head: h } }) as unknown as TroutRowVM;
    expect(troutFlow(head("Keep · counts toward the 2"), a, rules, "")).not.toBeNull();
    expect(troutFlow(head("Keep · counts toward the 4 and the 6"), a, rules, "")).toBeNull();
  });
  it("re-review minor: a rule replaced by its own source's rule says 'Replaced', never 'Replaced by' its own level", () => {
    const R4A = rule("r4a", 3, "Region 4", "4", "Keep 5.", 34), R4B = rule("r4b", 3, "Region 4", "4", "Keep 2.", 34);
    const f = flowOf([item(R4A, "replaced", R4B), item(R4B, "governs")], says, "Glenn Lake", null);
    expect(f.steps.map((s) => s.tag)).toEqual(["Replaced", null]);
  });
  it("a rule with no stored role, or no answer: no rail (the sheet keeps its Sources list)", () => {
    expect(troutFlow(row([1, 2, 5 as number]), a, { get: (ix: number) => RS.get(ix) ?? rule("x", 3, "Region 2", "2", "x", 21) } as never, "")).toBeNull();
    expect(troutFlow(row([1]), null, rules, "")).toBeNull();
  });
});

describe("(T892) the table's row says when its answer comes down from another water's row to its tributaries", () => {
  const TRIB = { ...rule("koot", 1, "Kootenay River", "4", "Release every burbot.", 36), via: "trib", baseRank: 0 } as unknown as RuleView;
  const ELK = { ...rule("elk", 1, "Elk River", "4", "Keep 1 bull trout.", 36), via: "trib", baseRank: 0 } as unknown as RuleView;
  const TWIN = { ...rule("twin", 1, "Twin Lakes", "4", "Keep 2 bull trout.", 36), via: "trib", baseRank: 0 } as unknown as RuleView;
  const R4 = rule("r4", 3, "Region 4", "4", "Keep up to 5 burbot a day.", 34);
  const OWN = rule("own", 0, "Anchor Creek", "4", "Release every burbot.", 36);
  const down = (f: ReturnType<typeof flowOf>) => downLine(f.down ?? []);
  it("FlowVM.down + downLine: the water the sheet's deciding kicker names; a water's own or region row says nothing", () => {
    const f = flowOf([item(TRIB, "governs"), item(R4, "replaced", TRIB)], says, "Anchor Creek", null);
    expect(f.steps.filter((s) => s.tone === "won").map((s) => s.kicker)).toEqual(["Kootenay River · its tributaries"]);
    expect(down(f)).toBe("Kootenay River’s rule for its tributaries");
    expect(down(flowOf([item(OWN, "governs"), item(R4, "replaced", OWN)], says, "Anchor Creek", null))).toBeNull();
    expect(down(flowOf([item(R4, "governs")], says, "Anchor Creek", null))).toBeNull();
  });
  it("a tributary rule that was replaced, or only a possession rule, decides nothing here: no line", () => {
    expect(down(flowOf([item(R4, "governs"), item(TRIB, "replaced", R4)], says, "Anchor Creek", null))).toBeNull();
    expect(down(flowOf([item(R4, "governs"), item(TRIB, "possession")], says, "Anchor Creek", null))).toBeNull();
  });
  it("review 3: a tributary rule the rail drops (a region rule with the same words decides) is not named on the row either", () => {
    const SAME = rule("r4same", 3, "Region 4", "4", "Release every burbot.", 34);
    const f = flowOf([item(SAME, "governs"), item(TRIB, "governs")], says, "Anchor Creek", null);
    expect(f.steps.map((s) => s.kicker)).toEqual(["Region 4 · base rule"]);
    expect(down(f)).toBeNull();
  });
  it("review 1: a Bass row whose smallmouth come down from Duck Lake and whose largemouth are the region's: no line", () => {
    const DUCK = { ...rule("duck", 1, "Duck Lake", "4", "Keep up to 3 smallmouth bass.", 36), via: "trib", baseRank: 0 } as unknown as RuleView;
    const forFish = (r: RuleView, role: string, spp: string[]): LadderItem => ({ r, role, by: null, spp: new Set(spp) });
    const half = flowOf([forFish(DUCK, "governs", ["SMB"]), forFish(R4, "governs", ["LMB"])], says, "Cory Creek", null, ["LMB", "SMB"]);
    expect(half.steps.map((s) => s.kicker)).toContain("Duck Lake · its tributaries");   // the sheet says it, for the smallmouth
    expect(down(half)).toBeNull();
    const whole = flowOf([forFish(DUCK, "governs", ["SMB", "LMB"])], says, "Cory Creek", null, ["LMB", "SMB"]);
    expect(down(whole)).toBe("Duck Lake’s rule for its tributaries");
  });
  it("two waters: both named, once each; review 5: a name ending in s takes only ’", () => {
    expect(down(flowOf([item(TRIB, "governs"), item(ELK, "narrows")], says, "Anchor Creek", null)))
      .toBe("Kootenay River’s and Elk River’s rules for their tributaries");
    expect(down(flowOf([item(TWIN, "governs")], says, "Twin Creek", null))).toBe("Twin Lakes’ rule for its tributaries");
  });
});

describe("(T892) troutFrom: the trout fold's tributary line", () => {
  const DOWN = { ...rule("bc", 1, "Bella Coola River", "5", "Release wild trout and char.", 50), via: "trib", baseRank: 0 } as unknown as RuleView;
  const R5 = rule("r5", 3, "Region 5", "5", "Keep up to 4 trout and char a day", 48);
  const RS = new Map([[1, DOWN], [2, R5]]);
  const rules = { get: (ix: number) => RS.get(ix)! } as unknown as Pick<RuleIndex, "get">;
  const trow = (key: string, src: number[]) => ({ key, fish: key, src, origin: null, sheet: { head: "Release" } }) as unknown as TroutRowVM;
  const table = (...rows: TroutRowVM[]) => ({ boxes: [{ rows }] }) as unknown as TroutVM;
  const ans = (roles: [number, string, number | null][]) => ({ lead: "wild", hatchery: null, wild: { roles } }) as unknown as FishAnswer;
  const down = ans([[1, "governs", null], [2, "replaced", 1]]), own = ans([[2, "governs", null]]);
  it("every row from the same water's tributary rule: the fold says it, whole", () => {
    expect(troutFrom(table(trow("RB", [1, 2]), trow("CT", [1, 2])), () => down, rules, "Nygaard Creek"))
      .toBe("Bella Coola River’s rule for its tributaries");
  });
  it("only some rows (the others the region's): no fold line — the table's rows are Python's words (T810)", () => {
    expect(troutFrom(table(trow("DV", [1, 2]), trow("RB", [2])), (f) => (f === "DV" ? down : own), rules, "Bryce Creek")).toBeNull();
    expect(troutFrom(table(trow("RB", [2])), () => own, rules, "Mamquam River")).toBeNull();
  });
});
