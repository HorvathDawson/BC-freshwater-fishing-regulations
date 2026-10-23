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
  // T109 step 1: Region 7 is printed as two zones, and their rows carry z7a:/z7b: ids. `/^z(\d+)/` read them as
  // "Region 7 rule" while Python says "Region 7A". The 23 zone entries of r-36fbc5326506 (ui-rules-export.json), whole subs.
  it("z7a/z7b read as Region 7A/7B on all 23 zone entries (T109)", () => {
    const ZONE7: [string, string][] = [["z7a:bait_ban_streams", "Every stream in Zone A"], ["z7a:ice_fishing_huts", "Lakes of Zone A"],
      ["z7a:possession_exceptions", "Zone A"], ["z7a:sara_sturgeon", "Upper Fraser and Nechako"], ["z7a:set_lining", "Lakes of Zone A"],
      ["z7a:single_barbless_hook", "Every stream in Zone A"], ["z7a:species_quotas", "Zone A"], ["z7a:spring_stream_closure", "Every stream in Zone A"],
      ["z7a:stocked_lakes", "Lakes of Zone A"], ["z7a:tagging_program", "Zone A"], ["z7a:trout_char_quota", "Zone A"],
      ["z7b:arctic_grayling", "Zone B"], ["z7b:bait", "Zone B"], ["z7b:bull_trout_dolly_varden", "Zone B"], ["z7b:ice_fishing_huts", "Lakes of Zone B"],
      ["z7b:peace_river_navigation", "Peace River"], ["z7b:possession_exceptions", "Zone B"], ["z7b:set_lining", "Zone B"],
      ["z7b:single_barbless_hook", "Every stream in Zone B"], ["z7b:site_c_tagging", "Zone B"], ["z7b:species_quotas", "Zone B"],
      ["z7b:thin_ice", "Inga and Sundance lakes"], ["z7b:trout_char_quota", "Zone B"]];
    expect(ZONE7.length).toBe(23);
    expect(ZONE7.map(([id, name]) => ruleSub(rule(id, 3, name)))).toEqual([
      "Region 7A rule · Every stream in Zone A", "Region 7A rule · Lakes of Zone A", "Region 7A rule · Zone A",
      "Region 7A rule · Upper Fraser and Nechako", "Region 7A rule · Lakes of Zone A", "Region 7A rule · Every stream in Zone A",
      "Region 7A rule · Zone A", "Region 7A rule · Every stream in Zone A", "Region 7A rule · Lakes of Zone A", "Region 7A rule · Zone A",
      "Region 7A rule · Zone A",
      "Region 7B rule · Zone B", "Region 7B rule · Zone B", "Region 7B rule · Zone B", "Region 7B rule · Lakes of Zone B",
      "Region 7B rule · Peace River", "Region 7B rule · Zone B", "Region 7B rule · Zone B", "Region 7B rule · Every stream in Zone B",
      "Region 7B rule · Zone B", "Region 7B rule · Zone B", "Region 7B rule · Inga and Sundance lakes", "Region 7B rule · Zone B"]);
    // the level word (gear tab) and the collapse read the zone too; a plain region and a two-digit-looking id are unchanged
    expect([levelWord(rule("z7a:species_quotas", 3, "Zone A")), ruleSub(rule("z7b:x", 3, "Region 7B")), levelTag(rule("z7:x", 3)),
      levelTag(rule("z6:x", 3)), levelTag(rule("zp:g", 3))])
      .toEqual(["Region 7A", "Region 7B", "Region 7 rule", "Region 6 rule", "Region rule"]);
  });
  it("there is one levelTag in the barrel and one set of level words", () => {
    expect(regs.levelTag).toBe(levelTag);
    expect("ruleSheetSub" in regs).toBe(false);
    expect(Object.keys(GEAR).filter((k) => /LEVEL|REGION/.test(k))).toEqual([]);
  });
});
