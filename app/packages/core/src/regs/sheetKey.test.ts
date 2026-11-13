import { describe, expect, it } from "vitest";
import { appKeyOf, goldenKeyOf, parseSheetKey } from "./sheetKey";

describe("parseSheetKey", () => {
  it.each([
    "hatch", "pos:pos", "rule:zp:conduct::conduct.r1", "rule:r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8",
    "gl:bait_ban", "gl:bait_ban,single_barbless_hook", "sp:RB", "srcs:bait", "srcs:ways", "srcs:always", "srcs:boats", "srcs:gl|mode",
    "srcs:gl|lines", "srcs:gl|hook", "srcs:gl|weight", "other:o0", "other:o12", "src:r3",
  ])("%s is a key", (k) => { expect(parseSheetKey(k)).toBe(k); });
  // (owner 2026-11-04) the licence sources sheet is removed: `licsrc:` is no key; (T698, owner 2026-11-12 "Remove the
  // screen") nor is `parts`, the Parts sheet
  it.each(["parts", "date", "kind:o0", "licsrc:adult", "licsrc:u16_alien", "licsrc:child", "srcs:gl|bait", "srcs:fish", "other:0", "rule:", "gl:", "src:o1", ""])("%j is refused", (k) => {
    expect(parseSheetKey(k)).toBeNull();
  });
});

describe("appKeyOf / goldenKeyOf (the golden embeds the state id; the app's keys never do)", () => {
  it.each([
    ["other:vedder_1010|o0", "other:o0"], ["src:vedder_1010|r0", "src:r0"], ["pos:vedder_1010|pos", "pos:pos"],
    ["srcs:bait|vedder_1010", "srcs:bait"], ["srcs:gl|vedder_1010|hook", "srcs:gl|hook"],
    ["rule:zp:conduct::conduct.r1", "rule:zp:conduct::conduct.r1"],
    ["gl:bait_ban", "gl:bait_ban"], ["sp:ST", "sp:ST"], ["hatch", "hatch"],
  ] as const)("%s ⇄ %s", (g, a) => {
    expect(appKeyOf(g, "vedder_1010")).toBe(a);
    expect(goldenKeyOf(a, "vedder_1010")).toBe(g);
  });
  it("a key of another state is not this state's", () => {
    expect(() => appKeyOf("other:klwa_1010|o0", "vedder_1010")).toThrow(/not a sheet key/);
  });
  it("(T698) the golden's Parts sheet key maps to nothing (owner 2026-11-12: that screen is removed)", () => {
    expect(() => appKeyOf("parts", "bc1_501")).toThrow(/not a sheet key/);
  });
  it("the golden's licence sources key maps to nothing (owner 2026-11-04: that sheet is removed)", () => {
    expect(() => appKeyOf("licsrc:lic|vedder_1010|u16_res", "vedder_1010")).toThrow(/not a sheet key/);
  });
});

