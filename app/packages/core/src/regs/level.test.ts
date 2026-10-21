/**
 * The one page v36 levelTag (and openRule's sub) every presenter shares: T30's levelWord / downWater and T36's rule sheet
 * sub are built on it, so the level words live once (WORDS.LEVEL_TAG_*).
 */
import { describe, expect, it } from "vitest";
import * as regs from "./index";
import type { ExportRule, RuleFacts, RuleView } from "./inputs";
import { levelTag, ruleSub } from "./level";
import { levelWord } from "./gear";
import { ruleView } from "./rules";
import { BOOK } from "./testing/speciesBook";
import { GEAR } from "./words";

const exp = (entry_id: string, rank: number, entry_name?: string): ExportRule => ({ entry_id, rule_id: "r1", type: "", family: "", label: "L",
  verbatim: "", fields: {}, prov: { rank, ...(entry_name ? { entry_name } : {}) } });
const rule = (entry_id: string, rank: number, entry_name?: string, via: "reach" | "trib" = "reach"): RuleView =>
  ruleView(0, exp(entry_id, rank, entry_name), { kind: "gear", title: "", book: [], applies: "" } as RuleFacts, BOOK, via);

describe("levelTag / ruleSub (page v36 levelTag, openRule's sub)", () => {
  it("names each rank, Region N from the rule's own entry id", () => {
    expect([rule("zp:g", 4), rule("z2:s", 3), rule("zx:a", 3), rule("na:x", 2), rule("w:d", 1), rule("fed:x", -1), rule("w:v", 0)].map(levelTag))
      .toEqual(["B.C. rule", "Region 2 rule", "Region rule", "Named area rule", "Downstream rule", "Federal or park rule", "This water’s rule"]);
  });
  it("the sub joins the entry name, and collapses \"Region 2 rule · Region 2\" to \"Region 2\"", () => {
    expect([ruleSub(rule("w:d", 1, "Duncan River")), ruleSub(rule("z2:s", 3, "Region 2")), ruleSub(rule("zp:g", 4))])
      .toEqual(["Downstream rule · Duncan River", "Region 2", "B.C. rule"]);
  });
  it("levelWord is built on it and reads the same either side of the collapse", () => {
    expect([levelWord(rule("z2:s", 3, "Region 2")), levelWord(rule("z2:s", 3, "Streams")), levelWord(rule("zp:g", 4))])
      .toEqual(["Region 2", "Region 2", "B.C. rule"]);
  });
  it("there is one levelTag in the barrel and one set of level words", () => {
    expect(regs.levelTag).toBe(levelTag);
    expect("ruleSheetSub" in regs).toBe(false);
    expect(Object.keys(GEAR).filter((k) => /LEVEL|REGION/.test(k))).toEqual([]);
  });
});
