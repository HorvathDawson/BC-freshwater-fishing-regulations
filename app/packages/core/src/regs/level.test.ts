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

/** The rule as the store ships it: `prov.region` is the pipeline's parse of the entry id (`entry_id.region_of`, T109) —
 * stated here as the export states it, never read off the id by the app. */
const SHIPPED_REGION: Record<string, string | null> = {
  "zp:g": null, "z2:s": "2", "zx:a": null, "z7:x": "7", "z6:x": "6", "z7b:x": "7b", "z7a:daily_quotas": "7a" };
const exp = (entry_id: string, rank: number, entry_name?: string): ExportRule => ({ entry_id, rule_id: "r1", type: "", family: "", label: "L",
  verbatim: "", fields: {}, prov: { rank, ...(entry_name ? { entry_name } : {}),
    region: entry_id in SHIPPED_REGION ? SHIPPED_REGION[entry_id]! : null } });
const rule = (entry_id: string, rank: number, entry_name?: string, via: "reach" | "trib" = "reach",
  region?: string): RuleView => {
  const e = exp(entry_id, rank, entry_name);
  return ruleView(0, region === undefined ? e : { ...e, prov: { ...e.prov, region } },
    { kind: "gear", title: "", book: [], applies: "" } as RuleFacts, BOOK, via);
};

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
  // "Region 7 rule" while Python says "Region 7A". The 23 zone entries of r-36fbc5326506 (ui-rules-export.json), whole subs;
  // 20 since the daily-quota merge (2026-10-26: trout/char + other quotas [+ 7B grayling] = zN:daily_quotas).
  it("z7a/z7b read as Region 7A/7B on all 20 zone entries (T109)", () => {
    const ZONE7: [string, string][] = [["z7a:bait_ban_streams", "Every stream in Zone A"], ["z7a:ice_fishing_huts", "Lakes of Zone A"],
      ["z7a:possession_exceptions", "Zone A"], ["z7a:sara_sturgeon", "Upper Fraser and Nechako"], ["z7a:set_lining", "Lakes of Zone A"],
      ["z7a:single_barbless_hook", "Every stream in Zone A"], ["z7a:daily_quotas", "Zone A"], ["z7a:spring_stream_closure", "Every stream in Zone A"],
      ["z7a:stocked_lakes", "Lakes of Zone A"], ["z7a:tagging_program", "Zone A"],
      ["z7b:daily_quotas", "Zone B"], ["z7b:bait", "Zone B"], ["z7b:bull_trout_dolly_varden", "Zone B"], ["z7b:ice_fishing_huts", "Lakes of Zone B"],
      ["z7b:peace_river_navigation", "Peace River"], ["z7b:possession_exceptions", "Zone B"], ["z7b:set_lining", "Zone B"],
      ["z7b:single_barbless_hook", "Every stream in Zone B"], ["z7b:site_c_tagging", "Zone B"],
      ["z7b:thin_ice", "Inga and Sundance lakes"]];
    expect(ZONE7.length).toBe(20);
    // what the pipeline ships for these ids (`entry_id.region_of`): the zone letter
    expect(ZONE7.map(([id, name]) => ruleSub(rule(id, 3, name, "reach", id.slice(1, 3))))).toEqual([
      "Region 7A rule · Every stream in Zone A", "Region 7A rule · Lakes of Zone A", "Region 7A rule · Zone A",
      "Region 7A rule · Upper Fraser and Nechako", "Region 7A rule · Lakes of Zone A", "Region 7A rule · Every stream in Zone A",
      "Region 7A rule · Zone A", "Region 7A rule · Every stream in Zone A", "Region 7A rule · Lakes of Zone A", "Region 7A rule · Zone A",
      "Region 7B rule · Zone B", "Region 7B rule · Zone B", "Region 7B rule · Zone B", "Region 7B rule · Lakes of Zone B",
      "Region 7B rule · Peace River", "Region 7B rule · Zone B", "Region 7B rule · Zone B", "Region 7B rule · Every stream in Zone B",
      "Region 7B rule · Zone B", "Region 7B rule · Inga and Sundance lakes"]);
    // the level word (gear tab) and the collapse read the zone too; a plain region and a two-digit-looking id are unchanged
    expect([levelWord(rule("z7a:daily_quotas", 3, "Zone A")), ruleSub(rule("z7b:x", 3, "Region 7B")), levelTag(rule("z7:x", 3)),
      levelTag(rule("z6:x", 3)), levelTag(rule("zp:g", 3))])
      .toEqual(["Region 7A", "Region 7B", "Region 7 rule", "Region 6 rule", "Region rule"]);
  });
  it("reads the region the pipeline shipped, never the entry id (T109)", () => {
    // MUTATION: a levelTag that parses the id again says "Region 7A rule" for the first and "Region rule" for the second
    expect([levelTag(rule("z7a:x", 3, undefined, "reach", "7b")), levelTag(rule("zp:odd", 3, undefined, "reach", "5"))])
      .toEqual(["Region 7B rule", "Region 5 rule"]);
    expect(levelTag(rule("z7a:x", 3, undefined, "reach"))).toBe("Region rule");      // nothing shipped: no region named
  });
  it("there is one levelTag in the barrel and one set of level words", () => {
    expect(regs.levelTag).toBe(levelTag);
    expect("ruleSheetSub" in regs).toBe(false);
    expect(Object.keys(GEAR).filter((k) => /LEVEL|REGION/.test(k))).toEqual([]);
  });
});
