/**
 * (T883, proposal) flow.ts: every fish sheet's "How this was decided" — the steps in level order with a replaced rule in
 * its place, the answer whole, the other rules, the folded ones. Built on stub rules (rank, provenance, kind), so each
 * case below is the shape of a real row, not a release's data: Cultus Lake's kokanee (Region 2's 5 a day replaced by the
 * lake's release; B.C.'s possession replaced by Region 2's) and a trout payload's plain list.
 */
import { describe, expect, it } from "vitest";
import type { LadderItem } from "./decided";
import { flowOf, plainFlow, ruleStep, withNotes, withWeek } from "./flow";
import type { RuleView } from "./inputs";
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
      ["Region 2 · base rule", false, "Keep up to 5 kokanee a day.", null], ["Region 2 · base rule", true, "No bait.", "Also applies"],
      ["Region 2 · base rule", true, "Release every kokanee.", "Replaced by Cultus Lake"],
    ]);
    expect(f.result).toBeNull();
  });

  it("plainFlow (the trout payload's list): kept in its order, kicker once, the answer whole", () => {
    const step = (k: string, s: string) => ({ kicker: k, says: [text(s)], tag: null, tone: "won" as const, book: [], rule: null });
    const f = plainFlow([step("Region 2 · base rule", "A"), step("Region 2 · base rule", "B"), step("Mamquam River · this water’s rule", "C")], "Keep 2");
    expect(f.steps.map((s) => [s.repeat ?? false, plain(s.says)])).toEqual([[false, "A"], [true, "B"], [false, "C"]]);
    expect(f.result).toBe("So: Keep 2");
  });

  it("withWeek: the day-parts stand where the rules they cite stood, today's tagged; the answer is the week's status", () => {
    const WK = rule("wk", 0, "Cultus Lake", "2", "Keep 5 on weekends.", 23);
    const f = flowOf([item(R2_KO, "replaced", WK), item(WK, "governs"), item(R2_POS, "possession")], says, "Cultus Lake", "Keep 5 a day here.");
    const parts = [ruleStep(WK, [text("Sat–Sun: "), text("Keep up to 5")], "Today", true), ruleStep(WK, [text("Mon–Fri: "), text("Release every one")])];
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
});
