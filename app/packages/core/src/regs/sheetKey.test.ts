import { describe, expect, it } from "vitest";
import { appKeyOf, goldenKeyOf, parseSheetKey, profileKeyOf } from "./sheetKey";

describe("parseSheetKey", () => {
  it.each([
    "hatch", "parts", "pos:pos", "rule:zp:conduct::conduct.r1", "rule:r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8",
    "gl:bait_ban", "gl:bait_ban,single_barbless_hook", "sp:RB", "srcs:bait", "srcs:ways", "srcs:always", "srcs:boats", "srcs:gl|mode",
    "srcs:gl|lines", "srcs:gl|hook", "srcs:gl|weight", "licsrc:adult", "licsrc:u16_alien", "other:o0", "other:o12", "src:r3",
  ])("%s is a key", (k) => { expect(parseSheetKey(k)).toBe(k); });
  it.each(["date", "kind:o0", "licsrc:child", "srcs:gl|bait", "srcs:fish", "other:0", "rule:", "gl:", "src:o1", ""])("%j is refused", (k) => {
    expect(parseSheetKey(k)).toBeNull();
  });
});

describe("appKeyOf / goldenKeyOf (the golden embeds the state id; the app's keys never do)", () => {
  it.each([
    ["other:vedder_1010|o0", "other:o0"], ["src:vedder_1010|r0", "src:r0"], ["pos:vedder_1010|pos", "pos:pos"],
    ["srcs:bait|vedder_1010", "srcs:bait"], ["srcs:gl|vedder_1010|hook", "srcs:gl|hook"],
    ["licsrc:lic|vedder_1010|u16_res", "licsrc:u16_res"], ["rule:zp:conduct::conduct.r1", "rule:zp:conduct::conduct.r1"],
    ["gl:bait_ban", "gl:bait_ban"], ["sp:ST", "sp:ST"], ["hatch", "hatch"], ["parts", "parts"],
  ] as const)("%s ⇄ %s", (g, a) => {
    expect(appKeyOf(g, "vedder_1010")).toBe(a);
    expect(goldenKeyOf(a, "vedder_1010")).toBe(g);
  });
  it("a key of another state is not this state's", () => {
    expect(() => appKeyOf("other:klwa_1010|o0", "vedder_1010")).toThrow(/not a sheet key/);
  });
});

describe("profileKeyOf", () => {
  it("each of the six readers", () => {
    expect([
      profileKeyOf({ age: "adult", residency: "resident" }), profileKeyOf({ age: "adult", residency: "non_resident" }),
      profileKeyOf({ age: "adult", residency: "non_resident_alien" }), profileKeyOf({ age: "u16", residency: "resident" }),
      profileKeyOf({ age: "u16", residency: "non_resident" }), profileKeyOf({ age: "u16", residency: "non_resident_alien" }),
    ]).toEqual(["adult", "adult_non", "adult_alien", "u16_res", "u16_non", "u16_alien"]);
  });
});
